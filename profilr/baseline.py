"""SQLite-backed baseline versions."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


class BaselineStore:
    def __init__(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("CREATE TABLE IF NOT EXISTS baselines (table_name TEXT, version INTEGER, payload TEXT, PRIMARY KEY(table_name, version))")
        self.conn.commit()

    def save(self, table_name: str, profile: dict) -> int:
        current = self.conn.execute("SELECT coalesce(max(version), 0) FROM baselines WHERE table_name=?", (table_name,)).fetchone()[0]
        version = current + 1
        self.conn.execute("INSERT INTO baselines VALUES (?, ?, ?)", (table_name, version, json.dumps(profile)))
        self.conn.commit()
        return version

    def load(self, table_name: str, version: int | None = None) -> dict | None:
        if version is None:
            row = self.conn.execute("SELECT payload FROM baselines WHERE table_name=? ORDER BY version DESC LIMIT 1", (table_name,)).fetchone()
        else:
            row = self.conn.execute("SELECT payload FROM baselines WHERE table_name=? AND version=?", (table_name, version)).fetchone()
        return json.loads(row[0]) if row else None
