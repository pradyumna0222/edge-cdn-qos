"""Reusable edge node with an independent disk cache and basic QoS metrics."""

import argparse
import os
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PORTS = {"edge1": 8001, "edge2": 8002, "edge3": 8003}
DEFAULT_ORIGIN_URL = os.environ.get("ORIGIN_URL", "http://127.0.0.1:5000").rstrip("/")
DEFAULT_CACHE_ROOT = Path(os.environ.get("CDN_CACHE_DIR", str(BASE_DIR / "cache"))).expanduser().resolve()
DEFAULT_CACHE_CAPACITY = int(os.environ.get("EDGE_CACHE_MAX_BYTES", str(128 * 1024 * 1024)))


def create_edge_app(edge_id, port=None):
    if edge_id not in DEFAULT_PORTS:
        raise ValueError(f"Unknown edge id: {edge_id}. Choose edge1, edge2, or edge3.")

    origin_url = DEFAULT_ORIGIN_URL
    cache_dir = DEFAULT_CACHE_ROOT / edge_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_capacity = max(0, DEFAULT_CACHE_CAPACITY)

    app = Flask(f"qos_{edge_id}")
    metric_lock = threading.Lock()
    cache_lock = threading.Lock()
    metrics = {
        "requests": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "origin_fetches": 0,
        "origin_errors": 0,
        "request_errors": 0,
        "cache_evictions": 0,
        "bytes_served": 0,
    }

    def increment(name, amount=1):
        with metric_lock:
            metrics[name] += amount

    def cache_file_list():
        return sorted(
            path.name for path in cache_dir.iterdir()
            if path.is_file() and not path.name.startswith(".tmp-")
        )

    def cache_size_bytes():
        total = 0
        for path in cache_dir.iterdir():
            if path.is_file() and not path.name.startswith(".tmp-"):
                try:
                    total += path.stat().st_size
                except OSError:
                    pass
        return total

    def put_in_cache(filename, content):
        """Atomically cache an object and evict least-recently-used files as needed."""
        if cache_capacity <= 0 or len(content) > cache_capacity:
            return False

        destination = cache_dir / filename
        with cache_lock:
            if destination.is_file():
                try:
                    os.utime(destination, None)
                    return True
                except OSError:
                    pass

            temp_path = cache_dir / f".tmp-{uuid.uuid4().hex}"
            try:
                temp_path.write_bytes(content)
                candidates = []
                total = 0
                for path in cache_dir.iterdir():
                    if not path.is_file() or path.name.startswith(".tmp-") or path == destination:
                        continue
                    try:
                        stat = path.stat()
                        total += stat.st_size
                        candidates.append((stat.st_mtime, path, stat.st_size))
                    except OSError:
                        continue

                needed = total + len(content) - cache_capacity
                for _, victim, victim_size in sorted(candidates, key=lambda item: item[0]):
                    if needed <= 0:
                        break
                    try:
                        victim.unlink(missing_ok=True)
                        total -= victim_size
                        needed -= victim_size
                        increment("cache_evictions")
                    except OSError:
                        continue

                os.replace(temp_path, destination)
                os.utime(destination, None)
                return True
            except OSError:
                return False
            finally:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass

    def set_response_headers(response, cache_status, elapsed_ms, cache_stored=None):
        response.headers["X-Edge-ID"] = edge_id
        response.headers["X-Cache"] = cache_status
        response.headers["X-Edge-Time-Ms"] = f"{elapsed_ms:.3f}"
        if cache_stored is not None:
            response.headers["X-Cache-Stored"] = "1" if cache_stored else "0"
        return response

    @app.get("/")
    def home():
        return f"QoS Edge CDN node {edge_id} is running. See /health and /metrics."

    @app.get("/health")
    def health():
        with metric_lock:
            metric_snapshot = dict(metrics)
        return jsonify({
            "service": "edge",
            "edge_id": edge_id,
            "status": "ok",
            "cache_directory": str(cache_dir),
            "cache_bytes": cache_size_bytes(),
            "cache_capacity_bytes": cache_capacity,
            "cached_files": cache_file_list(),
            "metrics": metric_snapshot,
        })

    @app.get("/metrics")
    def get_metrics():
        with metric_lock:
            metric_snapshot = dict(metrics)
        return jsonify({
            "edge_id": edge_id,
            "metrics": metric_snapshot,
            "cache_bytes": cache_size_bytes(),
            "cache_capacity_bytes": cache_capacity,
            "cached_files": cache_file_list(),
        })

    @app.get("/files/<path:filename>")
    def get_file(filename):
        started = time.perf_counter()
        increment("requests")

        if filename in {"", ".", ".."} or "/" in filename or "\\" in filename:
            increment("request_errors")
            return set_response_headers(jsonify({"error": "Invalid filename"}), "BYPASS", (time.perf_counter() - started) * 1000), 400

        cached_path = cache_dir / filename
        if cached_path.is_file():
            try:
                os.utime(cached_path, None)
                size = cached_path.stat().st_size
                increment("cache_hits")
                increment("bytes_served", size)
                response = send_from_directory(str(cache_dir), filename, as_attachment=False)
                return set_response_headers(response, "HIT", (time.perf_counter() - started) * 1000, cache_stored=True)
            except OSError:
                # A concurrent eviction may remove a file; handle that as a cache miss.
                pass

        increment("cache_misses")
        increment("origin_fetches")
        origin_file_url = f"{origin_url}/files/{quote(filename, safe='')}"
        try:
            upstream = requests.get(origin_file_url, timeout=(2.0, 20.0))
        except requests.RequestException as exc:
            increment("origin_errors")
            increment("request_errors")
            response = jsonify({"error": "Origin server request failed", "detail": str(exc)})
            response.status_code = 502
            return set_response_headers(response, "MISS", (time.perf_counter() - started) * 1000, cache_stored=False)

        if upstream.status_code != 200:
            if upstream.status_code >= 500:
                increment("origin_errors")
            response = Response(
                upstream.content,
                status=upstream.status_code,
                content_type=upstream.headers.get("Content-Type", "application/octet-stream"),
            )
            return set_response_headers(response, "MISS", (time.perf_counter() - started) * 1000, cache_stored=False)

        content = upstream.content
        increment("bytes_served", len(content))
        cached = put_in_cache(filename, content)
        elapsed_ms = (time.perf_counter() - started) * 1000

        if cached:
            response = send_from_directory(str(cache_dir), filename, as_attachment=False)
        else:
            response = Response(content, status=200, content_type=upstream.headers.get("Content-Type", "application/octet-stream"))
        return set_response_headers(response, "MISS", elapsed_ms, cache_stored=cached)

    app.config["EDGE_ID"] = edge_id
    app.config["EDGE_PORT"] = port or DEFAULT_PORTS[edge_id]
    app.config["EDGE_CACHE_DIR"] = str(cache_dir)
    app.config["EDGE_ORIGIN_URL"] = origin_url
    return app


def main():
    parser = argparse.ArgumentParser(description="Run one QoS Edge CDN node")
    parser.add_argument("--id", required=True, choices=sorted(DEFAULT_PORTS), help="Edge ID")
    parser.add_argument("--port", type=int, default=None, help="Override the default port for this edge")
    args = parser.parse_args()

    port = args.port or DEFAULT_PORTS[args.id]
    app = create_edge_app(args.id, port)
    print(f"[{args.id.upper()}] Listening on http://127.0.0.1:{port}")
    print(f"[{args.id.upper()}] Origin: {DEFAULT_ORIGIN_URL}")
    print(f"[{args.id.upper()}] Cache: {app.config['EDGE_CACHE_DIR']} (capacity {DEFAULT_CACHE_CAPACITY} bytes)")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
