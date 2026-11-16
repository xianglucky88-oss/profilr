"""Read-only HTTP endpoints plus minimal alert lifecycle actions.

No FastAPI dependency is required at runtime; the standard-library
``http.server`` is used so the offline demo stays self-contained.

Routing
-------
GET  /health
GET  /tables                          known tables (baselines + alert rows)
GET  /baseline/<table>?version=N      baseline payload (default: latest)
GET  /alerts?table=&field=&metric=&status=&since=&until=&time_field=&limit=&offset=
GET  /alerts/<alert_id>               single lifecycle record
GET  /alerts/<alert_id>/events        audit trail for one alert
POST /alerts/<alert_id>/acknowledge   stamp acknowledged_at (status unchanged)
POST /alerts/<alert_id>/suppress      body {"until": "ISO-8601 timestamp"}

Error convention: every non-2xx response is JSON like
``{"error": "...", "status": 404}``.  A table-scoped query against a table
that has neither baselines nor alert rows returns 404; an existing table with
no matching alerts returns 200 with an empty ``items`` list.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .alerting import STATUSES, AlertStore
from .baseline import BaselineStore


def serve(db_path: str, port: int = 9800) -> None:
    baseline_store = BaselineStore(db_path)
    alert_store = AlertStore(db_path)

    class Handler(BaseHTTPRequestHandler):
        # ------------------------------------------------------------ helpers

        def _send_json(self, payload, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, status: int, message: str) -> None:
            self._send_json({"error": message, "status": status}, status)

        @staticmethod
        def _first(query: dict, key: str) -> str | None:
            values = query.get(key)
            return values[0] if values else None

        @staticmethod
        def _int(query: dict, key: str, default: int) -> int:
            raw = Handler._first(query, key)
            if raw is None:
                return default
            try:
                return int(raw)
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be an integer")

        # ---------------------------------------------------------------- GET

        def do_GET(self):  # noqa: N802
            parts = urlsplit(self.path)
            path, query = parts.path, parse_qs(parts.query)

            if path == "/health":
                return self._send_json({"status": "ok"})

            if path == "/tables":
                tables = set(baseline_store.list_tables())
                rows = alert_store.conn.execute(
                    "SELECT DISTINCT table_name FROM alerts"
                ).fetchall()
                tables.update(r[0] for r in rows)
                return self._send_json({"tables": sorted(tables)})

            if path.startswith("/baseline/"):
                return self._handle_baseline(path, query)

            if path == "/alerts":
                return self._handle_query_alerts(query)

            if path.startswith("/alerts/"):
                remainder = path[len("/alerts/"):]
                segments = remainder.split("/")
                if len(segments) == 1:
                    return self._handle_get_alert(segments[0])
                if len(segments) == 2 and segments[1] == "events":
                    return self._handle_alert_events(segments[0])

            return self._send_error(404, f"unknown path: {path}")

        def _handle_baseline(self, path: str, query: dict) -> None:
            table = path.split("/baseline/", 1)[1]
            try:
                version = self._int(query, "version", None)  # type: ignore[arg-type]
            except ValueError as exc:
                return self._send_error(400, str(exc))
            if not baseline_store.table_exists(table) and not alert_store.table_exists(table):
                return self._send_error(404, f"table not found: {table}")
            payload = baseline_store.load(table, version)
            if payload is None:
                return self._send_error(
                    404, f"baseline version not found for table {table}"
                )
            return self._send_json(
                {"table": table, "version": version, "baseline": payload}
                if version is not None
                else {"table": table, "baseline": payload}
            )

        def _handle_query_alerts(self, query: dict) -> None:
            table = self._first(query, "table")
            if table is not None and not alert_store.table_exists(table):
                # Explicit table filter on an unknown table: 404 rather than a
                # silently empty page, so callers can tell "no drift" from
                # "wrong table name".
                return self._send_error(404, f"table not found: {table}")
            try:
                limit = self._int(query, "limit", 50)
                offset = self._int(query, "offset", 0)
            except ValueError as exc:
                return self._send_error(400, str(exc))

            status = self._first(query, "status")
            if status:
                invalid = [s for s in status.split(",") if s and s not in STATUSES]
                if invalid:
                    return self._send_error(
                        400,
                        f"unknown status {invalid[0]!r}; valid: {', '.join(STATUSES)}",
                    )
            try:
                result = alert_store.query_alerts(
                    table_name=table,
                    field=self._first(query, "field"),
                    metric=self._first(query, "metric"),
                    status=status,
                    since=self._first(query, "since"),
                    until=self._first(query, "until"),
                    time_field=self._first(query, "time_field") or "last_seen_at",
                    limit=limit,
                    offset=offset,
                )
            except ValueError as exc:
                return self._send_error(400, str(exc))
            result["filters"] = {
                "table": table,
                "field": self._first(query, "field"),
                "metric": self._first(query, "metric"),
                "status": status.split(",") if status else None,
                "since": self._first(query, "since"),
                "until": self._first(query, "until"),
            }
            return self._send_json(result)

        def _handle_get_alert(self, alert_id: str) -> None:
            alert = alert_store.get_alert(alert_id)
            if alert is None:
                return self._send_error(404, f"alert not found: {alert_id}")
            return self._send_json(alert)

        def _handle_alert_events(self, alert_id: str) -> None:
            if alert_store.get_alert(alert_id) is None:
                return self._send_error(404, f"alert not found: {alert_id}")
            return self._send_json(
                {"alert_id": alert_id, "events": alert_store.list_events(alert_id)}
            )

        # --------------------------------------------------------------- POST

        def do_POST(self):  # noqa: N802
            parts = urlsplit(self.path)
            path = parts.path
            segments = path.strip("/").split("/")
            # /alerts/<id>/<action>
            if len(segments) != 3 or segments[0] != "alerts":
                return self._send_error(404, f"unknown path: {path}")
            _, alert_id, action = segments

            if alert_store.get_alert(alert_id) is None:
                return self._send_error(404, f"alert not found: {alert_id}")

            if action == "acknowledge":
                return self._send_json(alert_store.acknowledge(alert_id))

            if action == "suppress":
                try:
                    body = self._read_json_body()
                except ValueError as exc:
                    return self._send_error(400, str(exc))
                until = body.get("until")
                if not isinstance(until, str):
                    return self._send_error(
                        400, "body must be JSON with a string 'until' timestamp"
                    )
                try:
                    updated = alert_store.suppress(alert_id, until)
                except ValueError as exc:
                    return self._send_error(400, str(exc))
                return self._send_json(updated)

            return self._send_error(404, f"unknown action: {action}")

        def _read_json_body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ValueError(f"invalid JSON body: {exc}") from exc
            if not isinstance(body, dict):
                raise ValueError("request body must be a JSON object")
            return body

        def log_message(self, *_):
            return

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
