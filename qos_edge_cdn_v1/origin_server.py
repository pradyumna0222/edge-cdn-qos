"""Origin server for the QoS-aware Edge CDN prototype."""

import os
import threading
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("CDN_DATA_DIR", str(BASE_DIR / "cdn_images"))).expanduser().resolve()

app = Flask(__name__)

# Lightweight request counters. These observe normal Flask responses without altering
# content selection, file handling, cache behavior, or existing endpoint URLs.
_metrics_lock = threading.Lock()
_origin_metrics = {
    "content_requests": 0,
    "content_responses": 0,
    "content_errors": 0,
    "bytes_sent": 0,
    "catalog_requests": 0,
    "catalog_responses": 0,
}


@app.after_request
def record_origin_traffic(response):
    path = request.path
    with _metrics_lock:
        if path.startswith("/files/"):
            _origin_metrics["content_requests"] += 1
            if 200 <= response.status_code < 400:
                _origin_metrics["content_responses"] += 1
                try:
                    _origin_metrics["bytes_sent"] += int(response.headers.get("Content-Length", "0") or 0)
                except (TypeError, ValueError):
                    pass
            else:
                _origin_metrics["content_errors"] += 1
        elif path == "/files":
            _origin_metrics["catalog_requests"] += 1
            if 200 <= response.status_code < 400:
                _origin_metrics["catalog_responses"] += 1
    return response



def list_assets():
    if not DATA_DIR.is_dir():
        return []
    return sorted(
        path.name for path in DATA_DIR.iterdir()
        if path.is_file() and not path.name.startswith(".")
    )


@app.get("/")
def home():
    return "QoS Edge CDN origin server is running. See /health and /files."


@app.get("/health")
def health():
    exists = DATA_DIR.is_dir()
    payload = {
        "service": "origin",
        "status": "ok" if exists else "error",
        "data_directory": str(DATA_DIR),
        "data_directory_exists": exists,
        "asset_count": len(list_assets()),
    }
    return jsonify(payload), (200 if exists else 503)


@app.get("/metrics")
def metrics():
    """Expose authoritative origin content-request and response counters."""
    with _metrics_lock:
        snapshot = dict(_origin_metrics)
    snapshot["service"] = "origin"
    snapshot["status"] = "ok"
    return jsonify(snapshot)


@app.get("/files")
def files():
    if not DATA_DIR.is_dir():
        return jsonify({"error": "CDN data directory does not exist", "data_directory": str(DATA_DIR)}), 503
    return jsonify(list_assets())


@app.get("/files/<path:filename>")
def get_file(filename):
    # Only serve files directly inside the configured asset directory.
    if filename in {"", ".", ".."} or "/" in filename or "\\" in filename:
        return jsonify({"error": "Invalid filename"}), 400
    if not DATA_DIR.is_dir():
        return jsonify({"error": "CDN data directory does not exist", "data_directory": str(DATA_DIR)}), 503
    return send_from_directory(str(DATA_DIR), filename, as_attachment=False)


if __name__ == "__main__":
    print(f"[ORIGIN] Asset directory: {DATA_DIR}")
    app.run(host="127.0.0.1", port=int(os.environ.get("ORIGIN_PORT", "5000")), debug=False, threaded=True)
