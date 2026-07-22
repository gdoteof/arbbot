"""Dashboard server: stdlib-only, port 4748 (task board's neighbor).

    GET /               -> the instrument panel (self-contained HTML)
    GET /api/summary    -> aggregated JSON (?range=today|7d|all)

Read-only over local files; binds 127.0.0.1 only.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from arbbot.dash.data import DashboardData

INDEX = Path(__file__).parent / "index.html"


def make_handler(data: DashboardData):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # quiet
            pass

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/":
                self._send(200, INDEX.read_bytes(), "text/html; charset=utf-8")
            elif url.path == "/api/summary":
                q = parse_qs(url.query)
                rng = (q.get("range") or ["today"])[0]
                if rng not in ("today", "7d", "all"):
                    rng = "today"
                fees = (q.get("fees") or ["scanned"])[0]
                try:
                    body = json.dumps(data.summary(rng, fees)).encode()
                    self._send(200, body, "application/json")
                except Exception as e:
                    self._send(500, json.dumps({"error": str(e)}).encode(),
                               "application/json")
            else:
                self._send(404, b"not found", "text/plain")

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=4748)
    ap.add_argument("--scan-dir", default="data/scan")
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--health", default="data/health.jsonl")
    ap.add_argument("--registry", default="config/registry.yaml")
    args = ap.parse_args()
    data = DashboardData(args.scan_dir, args.raw_dir, args.health, args.registry)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(data))
    print(f"arbbot dashboard: http://127.0.0.1:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
