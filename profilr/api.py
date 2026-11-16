"""Read-only HTTP endpoint exposing versioned baselines and comparisons.

Routes (all GET, JSON):

* ``/baseline/{table}``           — current head version + profile snapshot
* ``/baseline/{table}/versions``  — version list with head marker
* ``/baseline/{table}/events``    — audit trail (saves, rollbacks, migrations)
* ``/baseline/{table}/compare?from=&to=`` — reproducible comparison envelope
* ``/comparison/{comparison_id}`` — stored comparison result

Every comparison response states exactly which two versions were compared and
carries per-alert reasons, so consumers can see why an alert fired.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .baseline import BaselineError, BaselineStore, ComparisonNotFoundError
from .pipeline import compare_versions, get_comparison_result, list_versions


def serve(db_path: str, port: int = 9800) -> None:
    store = BaselineStore(db_path)

    class Handler(BaseHTTPRequestHandler):
        def _send(self, payload, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]
            try:
                if len(parts) == 2 and parts[0] == "baseline":
                    table = parts[1]
                    head = store.head(table)
                    if head is None:
                        return self._send({"error": f"表 {table!r} 还没有任何基线版本"}, 404)
                    return self._send({"table": table, "head": head, "profile": store.load(table)})
                if len(parts) == 3 and parts[0] == "baseline" and parts[2] == "versions":
                    return self._send(list_versions(store, parts[1]))
                if len(parts) == 3 and parts[0] == "baseline" and parts[2] == "events":
                    return self._send({"table": parts[1], "events": store.events(parts[1])})
                if len(parts) == 3 and parts[0] == "baseline" and parts[2] == "compare":
                    query = parse_qs(parsed.query)
                    def _int(name):
                        values = query.get(name)
                        return int(values[0]) if values and values[0] else None
                    return self._send(compare_versions(store, parts[1], _int("from"), _int("to")))
                if len(parts) == 2 and parts[0] == "comparison":
                    return self._send(get_comparison_result(store, parts[1]))
            except ComparisonNotFoundError as exc:
                return self._send({"error": str(exc)}, 404)
            except BaselineError as exc:
                return self._send({"error": str(exc)}, 400)
            return self._send({"error": "not found"}, 404)

        def log_message(self, *_):
            return

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
