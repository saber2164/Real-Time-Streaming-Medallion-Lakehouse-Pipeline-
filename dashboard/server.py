"""
dashboard/server.py

Read-only stakeholder dashboard for the Medallion lakehouse.

The dashboard is deliberately dumb: it serves one HTML page and the JSON
snapshot that ``spark/gold_refresh.py`` writes. It has no Spark, no Delta
reader, no database and no third-party Python packages, so it cannot drift from
the pipeline's business logic and it starts in milliseconds.

The page itself is the *same* file that is published on GitHub Pages
(``docs/dashboard.html``): served here it talks to ``/api/metrics``, served
statically it falls back to the committed snapshot. One page, two hosts.

Endpoints
---------
GET /              dashboard page (docs/dashboard.html)
GET /api/metrics   latest metrics snapshot (JSON, no-store)
GET /healthz       200 when the server is up (used by Docker/CI healthchecks)
GET /assets/*      fonts + stylesheets (read-only, same origin)

Configuration (environment)
---------------------------
METRICS_PATH   default /lakehouse/dashboard/metrics.json
PORT           default 8501
HOST           default 0.0.0.0 (the compose port binding is localhost-only)
"""

import json
import logging
import os
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_PATH = os.path.join(HERE, "index.html")
ASSETS_DIR = os.path.join(HERE, "assets")
METRICS_PATH = os.getenv("METRICS_PATH", "/lakehouse/dashboard/metrics.json")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8501"))

CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".woff2": "font/woff2",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
    ".txt": "text/plain; charset=utf-8",
}

# The published page (docs/dashboard.html) is shared between two hosts:
# GitHub Pages (static snapshot) and this server (live metrics). The page
# decides which one it is from this marker, so it never has to probe a URL
# that does not exist and spam the console with 404s.
DEPLOY_MARKER = b'content="static"'
DEPLOY_LIVE = b'content="live"'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard")

_cache_lock = threading.Lock()
_cache = {"mtime_ns": None, "raw": None, "payload": None}


def _waiting_payload(message):
    return {
        "schema_version": 1,
        "status": "waiting",
        "generated_at": None,
        "message": message,
        "kpis": {},
        "pipeline": {},
        "charts": {
            "events_by_minute": [],
            "revenue_by_hour": [],
            "top_sellers": [],
            "payment_mix": [],
        },
    }


def load_metrics():
    """Return the latest metrics dict, cached by file mtime.

    A missing or half-written file is never an error: both are normal while the
    pipeline is starting up, and the UI has an explicit waiting state.
    """
    try:
        stat = os.stat(METRICS_PATH)
    except FileNotFoundError:
        return _waiting_payload(
            "Waiting for the first Gold refresh. Start the pipeline and the "
            "gold-refresh service."
        )
    except OSError as exc:
        return _waiting_payload(f"Cannot read metrics: {exc}")

    with _cache_lock:
        if _cache["mtime_ns"] == stat.st_mtime_ns and _cache["payload"] is not None:
            return _cache["payload"]
        try:
            with open(METRICS_PATH, "rb") as handle:
                raw = handle.read()
            payload = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Ignoring unreadable metrics file: %s", exc)
            return _waiting_payload("Metrics are being written; retrying…")
        _cache.update(mtime_ns=stat.st_mtime_ns, raw=raw, payload=payload)
        return payload


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "MedallionDashboard/1.0"

    def _send(self, status, body, content_type, cache_control="no-store"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache_control)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        path = self.path.split("?", 1)[0]

        if path in ("/", "/index.html"):
            try:
                with open(INDEX_PATH, "rb") as handle:
                    body = handle.read()
            except OSError as exc:
                self._send(
                    500,
                    f"dashboard index.html unavailable: {exc}".encode(),
                    "text/plain; charset=utf-8",
                )
                return
            # Tell the page it is being served by the live metrics server.
            body = body.replace(DEPLOY_MARKER, DEPLOY_LIVE)
            self._send(200, body, "text/html; charset=utf-8", "no-cache")
            return

        if path == "/api/metrics":
            body = json.dumps(load_metrics(), separators=(",", ":")).encode()
            self._send(200, body, "application/json; charset=utf-8")
            return

        if path.startswith("/assets/"):
            # Static assets (fonts, stylesheets) that the page needs. Read-only
            # and path-traversal safe: the resolved path must stay in ASSETS_DIR.
            rel = os.path.normpath(path[len("/assets/"):]).lstrip("/")
            target = os.path.realpath(os.path.join(ASSETS_DIR, rel))
            root = os.path.realpath(ASSETS_DIR)
            if not target.startswith(root + os.sep) or not os.path.isfile(target):
                self._send(404, b"not found", "text/plain; charset=utf-8")
                return
            ext = os.path.splitext(target)[1].lower()
            with open(target, "rb") as handle:
                body = handle.read()
            self._send(
                200,
                body,
                CONTENT_TYPES.get(ext, "application/octet-stream"),
                "public, max-age=3600",
            )
            return

        if path == "/healthz":
            body = json.dumps(
                {
                    "status": "ok",
                    "metrics_present": os.path.exists(METRICS_PATH),
                }
            ).encode()
            self._send(200, body, "application/json; charset=utf-8")
            return

        if path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return

        self._send(404, b"not found", "text/plain; charset=utf-8")

    def log_message(self, fmt, *args):
        # Keep the container log useful without per-request noise from polling.
        if self.path.startswith("/api/") or self.path == "/healthz":
            return
        logger.info("%s - %s", self.address_string(), fmt % args)


def main():
    if not os.path.exists(INDEX_PATH):
        logger.error("Missing dashboard page: %s", INDEX_PATH)
        return 1

    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)

    def _shutdown(_signum, _frame):
        logger.info("Shutting down dashboard server")
        # shutdown() must not run on the serve_forever thread.
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info(
        "Dashboard listening on http://%s:%s (metrics=%s)", HOST, PORT, METRICS_PATH
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
