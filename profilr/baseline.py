"""Versioned, concurrency-safe baseline and comparison storage.

Schema overview:

* ``profiles``          — immutable profile snapshots, keyed by content hash.
* ``baseline_versions`` — monotonically increasing versions per table;
                          ``UNIQUE(table_name, input_identity)`` makes a retry
                          of the same batch return the existing version instead
                          of duplicating it.
* ``baseline_heads``    — movable pointer to the "current" version per table;
                          rollback moves this pointer, history stays immutable.
* ``baseline_events``   — audit trail (save / rollback / legacy migration).
* ``specs``             — comparison specs (比较口径), content-addressed.
* ``comparisons``       — one row per (table, baseline_version,
                          current_version, spec_id); status lifecycle
                          running → complete / failed, with interrupted rows
                          recovered on open so no half-finished state lingers.

Writes run inside ``BEGIN IMMEDIATE`` transactions guarded by a process-local
lock, so concurrent saves allocate distinct versions and readers never see a
partially written version.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class BaselineError(Exception):
    """Base error for baseline store operations."""


class BaselineNotFoundError(BaselineError):
    """The table has no baseline versions at all."""


class VersionNotFoundError(BaselineError):
    """A requested version does not exist for the table."""


class ComparisonNotFoundError(BaselineError):
    """A requested comparison record does not exist."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(payload) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


SCHEMA = """
CREATE TABLE IF NOT EXISTS profiles (
    profile_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    input_identity TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS baseline_versions (
    table_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
    input_identity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    PRIMARY KEY (table_name, version),
    UNIQUE (table_name, input_identity)
);
CREATE TABLE IF NOT EXISTS baseline_heads (
    table_name TEXT PRIMARY KEY,
    version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS baseline_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    event TEXT NOT NULL,
    from_version INTEGER,
    to_version INTEGER,
    detail TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS specs (
    spec_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS comparisons (
    comparison_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    baseline_version INTEGER NOT NULL,
    current_version INTEGER NOT NULL,
    spec_id TEXT NOT NULL,
    status TEXT NOT NULL,
    metrics_json TEXT,
    alerts_json TEXT,
    error TEXT,
    parent_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class BaselineStore:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate_legacy()
        self.recovered_comparisons = self._recover_interrupted()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "BaselineStore":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()

    @contextmanager
    def _transaction(self):
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except Exception:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    # ------------------------------------------------------------------
    # recovery & migration
    # ------------------------------------------------------------------
    def _recover_interrupted(self) -> int:
        """Mark comparisons that were 'running' when the process died.

        A crash between start and finish leaves a half-finished row; on the
        next open it is flagged 'interrupted' so callers can recompute it
        instead of trusting partial output.
        """
        with self._transaction() as conn:
            cursor = conn.execute(
                "UPDATE comparisons SET status='interrupted', "
                "error=coalesce(error, '进程中断，未完成计算'), updated_at=? "
                "WHERE status='running'",
                (_now(),),
            )
            return cursor.rowcount

    def _migrate_legacy(self) -> None:
        """Import rows from the original ``baselines`` table, if present."""
        exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='baselines'"
        ).fetchone()
        if not exists:
            return
        rows = self.conn.execute(
            "SELECT table_name, version, payload FROM baselines ORDER BY table_name, version"
        ).fetchall()
        with self._transaction() as conn:
            for row in rows:
                table, version, payload = row["table_name"], row["version"], row["payload"]
                identity = f"legacy://{table}/v{version}"
                profile_id = "prof-" + _hash(identity + "|" + payload)
                conn.execute(
                    "INSERT OR IGNORE INTO profiles VALUES (?, ?, ?, ?, ?)",
                    (profile_id, table, identity, payload, _now()),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO baseline_versions "
                    "(table_name, version, profile_id, input_identity, status, created_at) "
                    "VALUES (?, ?, ?, ?, 'active', ?)",
                    (table, version, profile_id, identity, _now()),
                )
                head = conn.execute(
                    "SELECT version FROM baseline_heads WHERE table_name=?", (table,)
                ).fetchone()
                if head is None or head["version"] < version:
                    conn.execute(
                        "INSERT INTO baseline_heads VALUES (?, ?) "
                        "ON CONFLICT(table_name) DO UPDATE SET version=excluded.version",
                        (table, version),
                    )
                conn.execute(
                    "INSERT INTO baseline_events "
                    "(table_name, event, from_version, to_version, detail, created_at) "
                    "VALUES (?, 'legacy_migrated', NULL, ?, ?, ?)",
                    (table, version, identity, _now()),
                )

    # ------------------------------------------------------------------
    # versions
    # ------------------------------------------------------------------
    def save_profile(self, table_name: str, profile: dict, input_identity: str | None = None) -> dict:
        """Save a profile snapshot as the next version.

        Idempotent: the same ``input_identity`` submitted again (e.g. a retried
        batch) returns the existing version with ``created=False`` rather than
        allocating a new one.
        """
        identity = input_identity or f"profile-hash:{_hash(_canonical(profile))}"
        profile_id = "prof-" + _hash(identity + "|" + _canonical(profile))
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT version, profile_id FROM baseline_versions "
                "WHERE table_name=? AND input_identity=?",
                (table_name, identity),
            ).fetchone()
            if existing:
                return {
                    "table": table_name,
                    "version": existing["version"],
                    "profile_id": existing["profile_id"],
                    "input_identity": identity,
                    "created": False,
                }
            version = conn.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM baseline_versions WHERE table_name=?",
                (table_name,),
            ).fetchone()[0]
            conn.execute(
                "INSERT OR IGNORE INTO profiles VALUES (?, ?, ?, ?, ?)",
                (profile_id, table_name, identity, _canonical(profile), _now()),
            )
            conn.execute(
                "INSERT INTO baseline_versions "
                "(table_name, version, profile_id, input_identity, status, created_at) "
                "VALUES (?, ?, ?, ?, 'active', ?)",
                (table_name, version, profile_id, identity, _now()),
            )
            conn.execute(
                "INSERT INTO baseline_heads VALUES (?, ?) "
                "ON CONFLICT(table_name) DO UPDATE SET version=excluded.version",
                (table_name, version),
            )
            conn.execute(
                "INSERT INTO baseline_events "
                "(table_name, event, from_version, to_version, detail, created_at) "
                "VALUES (?, 'save', NULL, ?, ?, ?)",
                (table_name, version, identity, _now()),
            )
            return {
                "table": table_name,
                "version": version,
                "profile_id": profile_id,
                "input_identity": identity,
                "created": True,
            }

    def save(self, table_name: str, profile: dict) -> int:
        """Backward-compatible wrapper returning just the version number."""
        return self.save_profile(table_name, profile)["version"]

    def head(self, table_name: str) -> dict | None:
        row = self.conn.execute(
            "SELECT v.table_name, v.version, v.profile_id, v.input_identity, v.created_at "
            "FROM baseline_heads h "
            "JOIN baseline_versions v ON v.table_name=h.table_name AND v.version=h.version "
            "WHERE h.table_name=?",
            (table_name,),
        ).fetchone()
        return dict(row) if row else None

    def versions(self, table_name: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT v.table_name, v.version, v.profile_id, v.input_identity, v.status, "
            "v.created_at, (h.version IS NOT NULL) AS is_head "
            "FROM baseline_versions v "
            "LEFT JOIN baseline_heads h ON h.table_name=v.table_name AND h.version=v.version "
            "WHERE v.table_name=? ORDER BY v.version",
            (table_name,),
        ).fetchall()
        return [dict(row) for row in rows]

    def load(self, table_name: str, version: int | None = None) -> dict | None:
        """Load the profile snapshot for a version (default: current head)."""
        if version is None:
            row = self.conn.execute(
                "SELECT p.payload FROM baseline_heads h "
                "JOIN baseline_versions v ON v.table_name=h.table_name AND v.version=h.version "
                "JOIN profiles p ON p.profile_id=v.profile_id "
                "WHERE h.table_name=?",
                (table_name,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT p.payload FROM baseline_versions v "
                "JOIN profiles p ON p.profile_id=v.profile_id "
                "WHERE v.table_name=? AND v.version=?",
                (table_name, version),
            ).fetchone()
        return json.loads(row["payload"]) if row else None

    def rollback(self, table_name: str, to_version: int) -> dict:
        """Move the head pointer back to an earlier version (history is kept)."""
        with self._transaction() as conn:
            target = conn.execute(
                "SELECT version FROM baseline_versions WHERE table_name=? AND version=?",
                (table_name, to_version),
            ).fetchone()
            if target is None:
                raise VersionNotFoundError(f"表 {table_name!r} 不存在版本 {to_version}，无法回退")
            head = conn.execute(
                "SELECT version FROM baseline_heads WHERE table_name=?", (table_name,)
            ).fetchone()
            previous = head["version"] if head else None
            if previous == to_version:
                return {"table": table_name, "head": to_version, "previous_head": previous, "changed": False}
            conn.execute(
                "UPDATE baseline_heads SET version=? WHERE table_name=?",
                (to_version, table_name),
            )
            conn.execute(
                "INSERT INTO baseline_events "
                "(table_name, event, from_version, to_version, detail, created_at) "
                "VALUES (?, 'rollback', ?, ?, ?, ?)",
                (table_name, previous, to_version, f"head {previous} -> {to_version}", _now()),
            )
            return {"table": table_name, "head": to_version, "previous_head": previous, "changed": True}

    def events(self, table_name: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id, table_name, event, from_version, to_version, detail, created_at "
            "FROM baseline_events WHERE table_name=? ORDER BY id",
            (table_name,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # specs & comparisons
    # ------------------------------------------------------------------
    def ensure_spec(self, spec: dict) -> str:
        spec_id = "spec-" + _hash(_canonical(spec))[:16]
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO specs VALUES (?, ?, ?)",
                (spec_id, _canonical(spec), _now()),
            )
        return spec_id

    def get_spec(self, spec_id: str) -> dict | None:
        row = self.conn.execute("SELECT payload FROM specs WHERE spec_id=?", (spec_id,)).fetchone()
        return json.loads(row["payload"]) if row else None

    @staticmethod
    def comparison_id(table_name: str, baseline_version: int, current_version: int, spec_id: str) -> str:
        key = _canonical({
            "table": table_name,
            "baseline_version": baseline_version,
            "current_version": current_version,
            "spec_id": spec_id,
        })
        return "cmp-" + _hash(key)

    def get_comparison(self, comparison_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM comparisons WHERE comparison_id=?", (comparison_id,)
        ).fetchone()
        return dict(row) if row else None

    def start_comparison(
        self,
        comparison_id: str,
        table_name: str,
        baseline_version: int,
        current_version: int,
        spec_id: str,
        parent_id: str | None = None,
    ) -> None:
        """Record a comparison as running; replaces interrupted/failed rows."""
        with self._transaction() as conn:
            conn.execute(
                "DELETE FROM comparisons WHERE comparison_id=? AND status IN ('interrupted', 'failed')",
                (comparison_id,),
            )
            conn.execute(
                "INSERT OR REPLACE INTO comparisons "
                "(comparison_id, table_name, baseline_version, current_version, spec_id, "
                "status, metrics_json, alerts_json, error, parent_id, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'running', NULL, NULL, NULL, ?, ?, ?)",
                (comparison_id, table_name, baseline_version, current_version, spec_id,
                 parent_id, _now(), _now()),
            )

    def finish_comparison(self, comparison_id: str, metrics: dict, alerts: list) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE comparisons SET status='complete', metrics_json=?, alerts_json=?, "
                "error=NULL, updated_at=? WHERE comparison_id=?",
                (_canonical(metrics), _canonical(alerts), _now(), comparison_id),
            )

    def fail_comparison(self, comparison_id: str, error: str) -> None:
        with self._transaction() as conn:
            conn.execute(
                "UPDATE comparisons SET status='failed', error=?, updated_at=? WHERE comparison_id=?",
                (error, _now(), comparison_id),
            )
