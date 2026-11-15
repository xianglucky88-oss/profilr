"""Minimal read-only HTTP endpoint without requiring FastAPI at runtime."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .baseline import BaselineStore


def serve(db_path: str, port: int = 9800) -> None:
    store = BaselineStore(db_path)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path.startswith("/baseline/"):
                table = self.path.split("/baseline/", 1)[1]
                body = json.dumps(store.load(table)).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def log_message(self, *_):
            return

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
