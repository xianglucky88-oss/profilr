"""Lifecycle-aware alert store built on SQLite.

The legacy :func:`profilr.notify.merge_alerts` only deduplicates delta values
within a single batch.  This module keeps one record per alert *identity* across
batches and drives a small state machine so operators can distinguish new
drift, persistent drift, suppressed drift and recovered drift.

Design decisions
----------------
Identity
    The natural key ``(table_name, field, metric)`` identifies a drift alert.
    It intentionally does not include baseline version, threshold or payload
    hashes, so the same drift stays the same alert across versions.
    ``alert_id`` is a deterministic SHA-256 hash of the natural key and is
    therefore stable and portable between environments.

Lifecycle states
    ``new``        first time the identity has ever been observed over threshold
    ``active``     the drift is still over threshold in subsequent batches
    ``suppressed`` operator-muted until ``suppress_until``; the window, not
                   configuration changes, controls when it can fire again
    ``recovered``  the latest observation is below threshold (or the alert was
                   resolved while suppressed)

    Acknowledgment is a timestamp annotation (``acknowledged_at``), not a
    status: confirming an alert does not hide ongoing drift.

Threshold changes
    Changing the threshold never reopens a suppressed alert.  Reopening is
    driven only by (a) the suppression window expiring while a fresh
    observation is still over threshold, or (b) a recovered identity drifting
    again.  The threshold used for every observation is snapshotted on the row
    and in the event log, so historical decisions stay explainable.

Duplicate batches and concurrency
    Every ingestion carries a ``batch_id`` (caller supplied, otherwise derived
    deterministically from the batch contents) and ``ingest_batches`` enforces
    uniqueness, so replaying a batch is a no-op that returns the original
    summary.  All writes happen inside a single ``BEGIN IMMEDIATE`` transaction
    and the natural key has a UNIQUE constraint: concurrent writers serialize
    on SQLite's write lock and update the same row instead of inserting
    duplicates.

Baseline version drift
    Each alert snapshots ``baseline_version`` (the baseline compared against)
    and ``observed_version`` (the profile batch producing the observation).
    Query results additionally carry ``latest_baseline_version``,
    ``baseline_stale`` and ``baseline_missing`` flags.  Alerts are never
    rewritten when baselines change; the next ingestion simply evaluates
    against the then-current baseline and naturally recovers or reopens them.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

#: Alert has never been seen before (first episode, first over-threshold batch).
STATUS_NEW = "new"
#: Alert is still over threshold in a later batch (persistent drift).
STATUS_ACTIVE = "active"
#: Operator muted the alert until ``suppress_until``.
STATUS_SUPPRESSED = "suppressed"
#: Latest observation dropped below threshold (drift gone for now).
STATUS_RECOVERED = "recovered"

STATUSES = (STATUS_NEW, STATUS_ACTIVE, STATUS_SUPPRESSED, STATUS_RECOVERED)
_OPEN_STATUSES = (STATUS_NEW, STATUS_ACTIVE, STATUS_SUPPRESSED)
_TIME_FIELDS = ("first_seen_at", "last_seen_at", "recovered_at")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    alert_id          TEXT PRIMARY KEY,
    table_name        TEXT NOT NULL,
    field             TEXT NOT NULL,
    metric            TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('new','active','suppressed','recovered')),
    latest_delta      REAL NOT NULL,
    threshold         REAL NOT NULL,
    occurrence_count  INTEGER NOT NULL,
    first_seen_at     TEXT NOT NULL,
    last_seen_at      TEXT NOT NULL,
    acknowledged_at   TEXT,
    suppressed_at     TEXT,
    suppress_until    TEXT,
    recovered_at      TEXT,
    baseline_version  INTEGER,
    observed_version  INTEGER,
    reopen_count      INTEGER NOT NULL DEFAULT 0,
    UNIQUE(table_name, field, metric)
);
CREATE TABLE IF NOT EXISTS alert_events (
    event_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    from_status      TEXT,
    to_status        TEXT,
    delta            REAL,
    threshold        REAL,
    baseline_version INTEGER,
    observed_version INTEGER,
    batch_id         TEXT,
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_alert ON alert_events(alert_id, event_id);
CREATE INDEX IF NOT EXISTS idx_alerts_table ON alerts(table_name);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
CREATE TABLE IF NOT EXISTS ingest_batches (
    batch_id         TEXT PRIMARY KEY,
    table_name       TEXT NOT NULL,
    observed_version INTEGER,
    ingested_at      TEXT NOT NULL,
    alert_opened     INTEGER NOT NULL,
    alert_updated    INTEGER NOT NULL,
    alert_recovered  INTEGER NOT NULL
);
"""


def utcnow() -> str:
    """Return an ISO-8601 UTC timestamp (seconds precision, lexically sortable)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def alert_id_for(table_name: str, field: str, metric: str) -> str:
    """Stable identity for an alert, independent of version or threshold."""
    payload = json.dumps([table_name, field, metric], separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _derive_batch_id(table_name: str, observed_version: int | None, observations: list[dict]) -> str:
    payload = [
        table_name,
        observed_version,
        sorted(
            (str(o.get("field")), str(o.get("metric")), float(o.get("delta", 0.0)))
            for o in observations
        ),
    ]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]


class AlertStore:
    """Persist alert lifecycle records and serve read-only lifecycle queries."""

    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False lets the threaded HTTP server share one store;
        # write serialization is guaranteed by BEGIN IMMEDIATE below.
        self.conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(_SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ write

    def ingest_observations(
        self,
        table_name: str,
        observations: list[dict],
        *,
        baseline_version: int | None = None,
        observed_version: int | None = None,
        threshold: float = 0.1,
        batch_id: str | None = None,
        now: str | None = None,
    ) -> dict:
        """Fold one batch of drift observations into the lifecycle records.

        ``observations`` is the raw drift output (every evaluated
        field/metric pair with its ``delta``), not a pre-filtered list: pairs
        below threshold are what allow open alerts to recover.  Fields absent
        from the batch are left untouched, because a missing field cannot be
        distinguished from a pipeline that skipped it.

        Returns an ingestion summary.  Replaying the same ``batch_id`` is
        idempotent and returns the original counters with ``duplicate=True``.
        """
        now = now or utcnow()
        if batch_id is None:
            batch_id = _derive_batch_id(table_name, observed_version, observations)

        summary = {
            "batch_id": batch_id,
            "table": table_name,
            "duplicate": False,
            "opened": 0,
            "updated": 0,
            "recovered": 0,
            "suppressed_hits": 0,
        }

        self.conn.execute("BEGIN IMMEDIATE")
        try:
            prior = self.conn.execute(
                "SELECT * FROM ingest_batches WHERE batch_id=?", (batch_id,)
            ).fetchone()
            if prior is not None:
                # Duplicate batch: do not touch any alert row or event log.
                self.conn.execute("COMMIT")
                summary.update(
                    duplicate=True,
                    opened=prior["alert_opened"],
                    updated=prior["alert_updated"],
                    recovered=prior["alert_recovered"],
                )
                return summary

            for obs in observations:
                self._apply_observation(
                    table_name, obs, threshold,
                    baseline_version, observed_version, batch_id, now, summary,
                )

            self.conn.execute(
                "INSERT INTO ingest_batches VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id, table_name, observed_version, now,
                    summary["opened"], summary["updated"], summary["recovered"],
                ),
            )
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        return summary

    def _apply_observation(
        self, table_name, obs, default_threshold,
        baseline_version, observed_version, batch_id, now, summary,
    ) -> None:
        field = obs["field"]
        metric = obs["metric"]
        delta = float(obs["delta"])
        obs_threshold = float(obs.get("threshold", default_threshold))
        aid = alert_id_for(table_name, field, metric)
        row = self.conn.execute(
            "SELECT * FROM alerts WHERE alert_id=?", (aid,)
        ).fetchone()
        over_threshold = delta >= obs_threshold

        if row is None:
            if not over_threshold:
                return  # nothing has ever alerted here; ignore quiet observations
            self.conn.execute(
                """INSERT INTO alerts (alert_id, table_name, field, metric, status,
                       latest_delta, threshold, occurrence_count, first_seen_at,
                       last_seen_at, baseline_version, observed_version, reopen_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 0)""",
                (aid, table_name, field, metric, STATUS_NEW, delta, obs_threshold,
                 now, now, baseline_version, observed_version),
            )
            self._event(aid, "opened", None, STATUS_NEW, delta, obs_threshold,
                        baseline_version, observed_version, batch_id, now)
            summary["opened"] += 1
            return

        status = row["status"]
        if over_threshold:
            self._apply_over_threshold(
                row, delta, obs_threshold, baseline_version,
                observed_version, batch_id, now, summary,
            )
        elif status in _OPEN_STATUSES:
            # Drift dropped below threshold; suppression does not block recovery.
            self.conn.execute(
                """UPDATE alerts SET status=?, latest_delta=?, threshold=?,
                       last_seen_at=?, recovered_at=?, suppress_until=NULL,
                       baseline_version=?, observed_version=?
                   WHERE alert_id=?""",
                (STATUS_RECOVERED, delta, obs_threshold, now, now,
                 baseline_version, observed_version, aid),
            )
            self._event(aid, "recovered", status, STATUS_RECOVERED, delta,
                        obs_threshold, baseline_version, observed_version, batch_id, now)
            summary["recovered"] += 1
        # A recovered identity observed quietly again needs no state change.

    def _apply_over_threshold(
        self, row, delta, obs_threshold, baseline_version,
        observed_version, batch_id, now, summary,
    ) -> None:
        aid = row["alert_id"]
        status = row["status"]

        if status == STATUS_RECOVERED:
            # Same identity, new drift episode: reopen as active (not "new",
            # which is reserved for never-before-seen identities).
            self.conn.execute(
                """UPDATE alerts SET status=?, latest_delta=?, threshold=?,
                       occurrence_count=occurrence_count+1, reopen_count=reopen_count+1,
                       last_seen_at=?, recovered_at=NULL,
                       baseline_version=?, observed_version=?
                   WHERE alert_id=?""",
                (STATUS_ACTIVE, delta, obs_threshold, now,
                 baseline_version, observed_version, aid),
            )
            self._event(aid, "reopened", STATUS_RECOVERED, STATUS_ACTIVE, delta,
                        obs_threshold, baseline_version, observed_version, batch_id, now)
            summary["updated"] += 1
            return

        if status == STATUS_SUPPRESSED and row["suppress_until"] and now < row["suppress_until"]:
            # Inside the suppression window: keep muting, but record that the
            # drift is still happening so the window decision stays auditable.
            self.conn.execute(
                """UPDATE alerts SET latest_delta=?, threshold=?,
                       occurrence_count=occurrence_count+1, last_seen_at=?,
                       baseline_version=?, observed_version=?
                   WHERE alert_id=?""",
                (delta, obs_threshold, now, baseline_version, observed_version, aid),
            )
            self._event(aid, "repeated", STATUS_SUPPRESSED, STATUS_SUPPRESSED, delta,
                        obs_threshold, baseline_version, observed_version, batch_id, now)
            summary["suppressed_hits"] += 1
            return

        if status == STATUS_SUPPRESSED:
            # Window expired and drift persists: the alert fires again.
            new_status = STATUS_ACTIVE
            self.conn.execute(
                """UPDATE alerts SET status=?, latest_delta=?, threshold=?,
                       occurrence_count=occurrence_count+1, last_seen_at=?,
                       suppress_until=NULL, baseline_version=?, observed_version=?
                   WHERE alert_id=?""",
                (new_status, delta, obs_threshold, now,
                 baseline_version, observed_version, aid),
            )
            self._event(aid, "reactivated", STATUS_SUPPRESSED, new_status, delta,
                        obs_threshold, baseline_version, observed_version, batch_id, now)
            summary["updated"] += 1
            return

        # new -> active on the second noisy batch; active stays active.
        new_status = STATUS_ACTIVE if status == STATUS_NEW else STATUS_ACTIVE
        self.conn.execute(
            """UPDATE alerts SET status=?, latest_delta=?, threshold=?,
                   occurrence_count=occurrence_count+1, last_seen_at=?,
                   baseline_version=?, observed_version=?
               WHERE alert_id=?""",
            (new_status, delta, obs_threshold, now,
             baseline_version, observed_version, aid),
        )
        self._event(aid, "repeated", status, new_status, delta, obs_threshold,
                    baseline_version, observed_version, batch_id, now)
        summary["updated"] += 1

    def acknowledge(self, alert_id: str, now: str | None = None) -> dict | None:
        """Stamp the latest acknowledgment time without changing status."""
        now = now or utcnow()
        row = self.conn.execute(
            "SELECT status FROM alerts WHERE alert_id=?", (alert_id,)
        ).fetchone()
        if row is None:
            return None
        self.conn.execute(
            "UPDATE alerts SET acknowledged_at=? WHERE alert_id=?", (now, alert_id)
        )
        self._event(alert_id, "acknowledged", row["status"], row["status"],
                    None, None, None, None, None, now)
        self.conn.commit()
        return self.get_alert(alert_id)

    def suppress(
        self, alert_id: str, until: str, now: str | None = None
    ) -> dict | None:
        """Mute an alert until the ISO-8601 timestamp ``until``."""
        now = now or utcnow()
        if until <= now:
            raise ValueError("suppress window must end in the future")
        row = self.conn.execute(
            "SELECT status FROM alerts WHERE alert_id=?", (alert_id,)
        ).fetchone()
        if row is None:
            return None
        self.conn.execute(
            "UPDATE alerts SET status=?, suppressed_at=?, suppress_until=? "
            "WHERE alert_id=?",
            (STATUS_SUPPRESSED, now, until, alert_id),
        )
        self._event(alert_id, "suppressed", row["status"], STATUS_SUPPRESSED,
                    None, None, None, None, None, now)
        self.conn.commit()
        return self.get_alert(alert_id)

    def list_events(self, alert_id: str) -> list[dict]:
        """Return the full audit trail for one alert (oldest first)."""
        rows = self.conn.execute(
            "SELECT * FROM alert_events WHERE alert_id=? ORDER BY event_id",
            (alert_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------- read

    def table_exists(self, table_name: str) -> bool:
        """True when the table has a baseline or at least one alert record."""
        found = self.conn.execute(
            "SELECT 1 FROM baselines WHERE table_name=? LIMIT 1", (table_name,)
        ).fetchone()
        if found is not None:
            return True
        found = self.conn.execute(
            "SELECT 1 FROM alerts WHERE table_name=? LIMIT 1", (table_name,)
        ).fetchone()
        return found is not None

    def get_alert(self, alert_id: str, now: str | None = None) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM alerts WHERE alert_id=?", (alert_id,)
        ).fetchone()
        if row is None:
            return None
        versions = self._baseline_versions([row["table_name"]])
        return self._enrich(dict(row), versions, now or utcnow())

    def query_alerts(
        self,
        *,
        table_name: str | None = None,
        field: str | None = None,
        metric: str | None = None,
        status: str | Iterable[str] | None = None,
        since: str | None = None,
        until: str | None = None,
        time_field: str = "last_seen_at",
        limit: int = 50,
        offset: int = 0,
        now: str | None = None,
    ) -> dict:
        """Filter alerts by table/field/metric/status/time window, paginated.

        Returns ``{"items": [...], "pagination": {...}}``.  Time filters apply
        to ``time_field`` (``first_seen_at``, ``last_seen_at`` or
        ``recovered_at``); the default is ``last_seen_at``.
        """
        if not isinstance(limit, int) or limit < 1 or limit > 200:
            raise ValueError("limit must be an integer between 1 and 200")
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        if time_field not in _TIME_FIELDS:
            raise ValueError(f"time_field must be one of {_TIME_FIELDS}")

        statuses = self._normalize_status(status)
        where, params = self._build_where(
            table_name, field, metric, statuses, since, until, time_field
        )

        total = self.conn.execute(
            f"SELECT count(*) AS n FROM alerts WHERE {where}", params
        ).fetchone()["n"]
        rows = self.conn.execute(
            f"""SELECT * FROM alerts WHERE {where}
                ORDER BY last_seen_at DESC, alert_id ASC
                LIMIT ? OFFSET ?""",
            (*params, limit, offset),
        ).fetchall()

        tables = {r["table_name"] for r in rows}
        versions = self._baseline_versions(tables)
        current = now or utcnow()
        items = [self._enrich(dict(r), versions, current) for r in rows]
        return {
            "items": items,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "count": total,
                "returned": len(items),
                "has_more": offset + len(items) < total,
            },
        }

    @staticmethod
    def _normalize_status(status: str | Iterable[str] | None) -> list[str]:
        if status is None:
            return []
        if isinstance(status, str):
            statuses = [s.strip() for s in status.split(",") if s.strip()]
        else:
            statuses = list(status)
        for value in statuses:
            if value not in STATUSES:
                raise ValueError(f"unknown status {value!r}; valid: {', '.join(STATUSES)}")
        return statuses

    @staticmethod
    def _build_where(table_name, field, metric, statuses, since, until, time_field):
        clauses, params = [], []
        if table_name is not None:
            clauses.append("table_name = ?")
            params.append(table_name)
        if field is not None:
            clauses.append("field = ?")
            params.append(field)
        if metric is not None:
            clauses.append("metric = ?")
            params.append(metric)
        if statuses:
            clauses.append(f"status IN ({', '.join('?' for _ in statuses)})")
            params.extend(statuses)
        if since is not None:
            clauses.append(f"{time_field} >= ?")
            params.append(since)
        if until is not None:
            clauses.append(f"{time_field} <= ?")
            params.append(until)
        return " AND ".join(clauses) if clauses else "1=1", params

    def _baseline_versions(self, tables: Iterable[str]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for table_name in tables:
            rows = self.conn.execute(
                "SELECT version FROM baselines WHERE table_name=?", (table_name,)
            ).fetchall()
            versions = [r["version"] for r in rows]
            result[table_name] = {
                "versions": set(versions),
                "latest": max(versions, default=None),
            }
        return result

    @staticmethod
    def _enrich(row: dict, baseline_info: dict, now: str) -> dict:
        info = baseline_info.get(row["table_name"], {"versions": set(), "latest": None})
        baseline_version = row["baseline_version"]
        latest = info["latest"]
        row["latest_baseline_version"] = latest
        # Snapshot was taken against an older baseline: deltas use the old basis.
        row["baseline_stale"] = (
            baseline_version is not None
            and latest is not None
            and baseline_version < latest
        )
        # The referenced baseline row no longer exists; the alert remains
        # authoritative for lifecycle, but its delta cannot be re-derived.
        row["baseline_missing"] = (
            baseline_version is not None and baseline_version not in info["versions"]
        )
        row["suppression_active"] = (
            row["status"] == STATUS_SUPPRESSED
            and bool(row["suppress_until"])
            and now < row["suppress_until"]
        )
        return row

    def _event(
        self, alert_id, event_type, from_status, to_status, delta, threshold,
        baseline_version, observed_version, batch_id, now,
    ) -> None:
        self.conn.execute(
            """INSERT INTO alert_events (alert_id, event_type, from_status, to_status,
                   delta, threshold, baseline_version, observed_version, batch_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (alert_id, event_type, from_status, to_status, delta, threshold,
             baseline_version, observed_version, batch_id, now),
        )
