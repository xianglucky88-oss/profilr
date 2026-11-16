"""SQLite-backed baseline versions."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


class BaselineStore:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS baselines "
            "(table_name TEXT, version INTEGER, payload TEXT, "
            "PRIMARY KEY(table_name, version))"
        )
        self.conn.commit()

    def save(self, table_name: str, profile: dict) -> int:
        # BEGIN IMMEDIATE serializes concurrent writers so two processes cannot
        # compute the same next version and hit the PRIMARY KEY conflict.
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            current = self.conn.execute(
                "SELECT coalesce(max(version), 0) FROM baselines WHERE table_name=?",
                (table_name,),
            ).fetchone()[0]
            version = current + 1
            self.conn.execute(
                "INSERT INTO baselines VALUES (?, ?, ?)",
                (table_name, version, json.dumps(profile)),
            )
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        return version

    def load(self, table_name: str, version: int | None = None) -> dict | None:
        if version is None:
            row = self.conn.execute(
                "SELECT payload FROM baselines WHERE table_name=? "
                "ORDER BY version DESC LIMIT 1",
                (table_name,),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT payload FROM baselines WHERE table_name=? AND version=?",
                (table_name, version),
            ).fetchone()
        return json.loads(row[0]) if row else None

    def table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM baselines WHERE table_name=? LIMIT 1", (table_name,)
        ).fetchone()
        return row is not None

    def list_tables(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT table_name FROM baselines ORDER BY table_name"
        ).fetchall()
        return [r[0] for r in rows]
