"""QoS-aware gateway: chooses a healthy edge using latency, load, and cache locality."""

import os
import random
import threading
import time
from urllib.parse import quote

import requests
from flask import Flask, Response, jsonify, request

app = Flask(__name__)

ORIGIN_URL = os.environ.get("ORIGIN_URL", "http://127.0.0.1:5000").rstrip("/")
EDGE_URLS = {
    "edge1": os.environ.get("EDGE1_URL", "http://127.0.0.1:8001").rstrip("/"),
    "edge2": os.environ.get("EDGE2_URL", "http://127.0.0.1:8002").rstrip("/"),
    "edge3": os.environ.get("EDGE3_URL", "http://127.0.0.1:8003").rstrip("/"),
}
HEALTH_TTL_SECONDS = max(0.25, float(os.environ.get("EDGE_HEALTH_TTL_SECONDS", "1.5")))
PROBE_TIMEOUT_SECONDS = max(0.1, float(os.environ.get("EDGE_PROBE_TIMEOUT_SECONDS", "0.6")))
EDGE_TIMEOUT = (1.5, 20.0)
# A short lease makes dashboard overload simulation self-clearing if the dashboard exits.
SIMULATED_OVERLOAD_LEASE_SECONDS = 12.0
SIMULATED_FAILURE_LEASE_SECONDS = 12.0
# During overload, the affected edge remains usable but receives only 0.30x normal
# selection weight. With three otherwise equal edges this is about 13% vs 43.5% each.
OVERLOADED_EDGE_WEIGHT = 0.30

state_lock = threading.Lock()
probe_lock = threading.Lock()
edge_state = {
    edge_id: {
        "healthy": None,
        "last_probe": 0.0,
        "probe_ms": 100.0,
        "ewma_ms": None,
        "active": 0,
        "failure_streak": 0,
        "cached_files": set(),
        "requests": 0,
        "simulated_overloaded": False,
        "overload_until": 0.0,
        "simulated_down": False,
        "down_until": 0.0,
    }
    for edge_id in EDGE_URLS
}
router_metrics = {
    "requests": 0,
    "successful_responses": 0,
    "client_errors": 0,
    "upstream_failures": 0,
    "origin_fallbacks": 0,
    "origin_requests": 0,
    "origin_responses": 0,
    "origin_errors": 0,
    "bytes_returned": 0,
}


def copy_response(upstream, routed_edge=None, router_time_ms=None, body=None):
    """Copy upstream metadata, optionally relaying a response body without buffering."""
    response_body = upstream.content if body is None else body
    response = Response(
        response_body,
        status=upstream.status_code,
        content_type=upstream.headers.get("Content-Type", "application/octet-stream"),
        direct_passthrough=(body is not None),
    )
    for header in ("Content-Disposition", "Content-Length", "ETag", "Last-Modified", "Cache-Control", "Accept-Ranges", "Content-Range", "X-Edge-ID", "X-Cache", "X-Edge-Time-Ms", "X-Cache-Stored"):
        if header in upstream.headers:
            response.headers[header] = upstream.headers[header]
    if routed_edge:
        response.headers["X-Routed-Edge"] = routed_edge
    if router_time_ms is not None:
        response.headers["X-Router-Time-Ms"] = f"{router_time_ms:.3f}"
    return response

def increment_metric(name, amount=1):
    with state_lock:
        router_metrics[name] += amount


def refresh_health(force=False):
    """Refresh cached edge health and cache inventories, without probing on every request."""
    now = time.monotonic()
    with probe_lock:
        for edge_id, base_url in EDGE_URLS.items():
            with state_lock:
                last_probe = edge_state[edge_id]["last_probe"]
                if not force and now - last_probe < HEALTH_TTL_SECONDS:
                    continue
                edge_state[edge_id]["last_probe"] = now

            started = time.perf_counter()
            try:
                response = requests.get(f"{base_url}/health", timeout=PROBE_TIMEOUT_SECONDS)
                elapsed_ms = (time.perf_counter() - started) * 1000
                response.raise_for_status()
                payload = response.json()
                healthy = payload.get("status") == "ok"
                cached_files = set(payload.get("cached_files", []))
                with state_lock:
                    state = edge_state[edge_id]
                    state["healthy"] = healthy
                    state["probe_ms"] = elapsed_ms
                    state["cached_files"] = cached_files
                    if healthy and state["ewma_ms"] is None:
                        state["ewma_ms"] = elapsed_ms
                    if healthy:
                        state["failure_streak"] = 0
            except (requests.RequestException, ValueError, TypeError):
                with state_lock:
                    state = edge_state[edge_id]
                    state["healthy"] = False
                    state["cached_files"] = set()


def choose_edge_order(filename, excluded=None, preferred_edge=None):
    """Build a QoS candidate order, applying a reduced probability to an overloaded edge.

    In normal mode, a healthy requested-edge hint is preferred, while automatic requests
    use latency + active load + cache locality. During overload, hints are intentionally
    ignored and weighted randomized scheduling is used so traffic actually shifts away
    from the degraded edge rather than pinning hinted clients to it.
    """
    excluded = excluded or set()
    refresh_health()
    with state_lock:
        candidates = []
        now = time.monotonic()
        for edge_id, edge in edge_state.items():
            if edge["simulated_overloaded"] and now >= edge["overload_until"]:
                edge["simulated_overloaded"] = False
                edge["overload_until"] = 0.0
            if edge["simulated_down"] and now >= edge["down_until"]:
                edge["simulated_down"] = False
                edge["down_until"] = 0.0
            if edge_id in excluded or edge["healthy"] is not True or edge["simulated_down"]:
                continue
            latency = edge["ewma_ms"] if edge["ewma_ms"] is not None else edge["probe_ms"]
            score = latency + (edge["active"] * 15.0) + (edge["failure_streak"] * 150.0)
            if filename in edge["cached_files"]:
                score -= 10000.0
            candidates.append((score, edge_id))

        candidates.sort(key=lambda item: (item[0], item[1]))
        ordered = [edge_id for _, edge_id in candidates]
        overloaded_present = any(edge_state[edge_id]["simulated_overloaded"] for edge_id in ordered)

        if overloaded_present and ordered:
            # Weighted random first choice plus a weighted fallback order. A single
            # overloaded edge gets 0.30 weight; two healthy peers each get 1.0 weight.
            remaining = list(ordered)
            weighted_order = []
            while remaining:
                weights = [OVERLOADED_EDGE_WEIGHT if edge_state[e]["simulated_overloaded"] else 1.0
                           for e in remaining]
                selected = random.choices(remaining, weights=weights, k=1)[0]
                weighted_order.append(selected)
                remaining.remove(selected)
            ordered = weighted_order
        elif preferred_edge in ordered:
            ordered.remove(preferred_edge)
            ordered.insert(0, preferred_edge)
        return ordered


def mark_edge_started(edge_id):
    with state_lock:
        edge_state[edge_id]["active"] += 1
        edge_state[edge_id]["requests"] += 1


def mark_edge_finished(edge_id, elapsed_ms, status_code=None, transport_error=False):
    with state_lock:
        state = edge_state[edge_id]
        state["active"] = max(0, state["active"] - 1)
        if transport_error or (status_code is not None and status_code >= 500):
            state["failure_streak"] += 1
            state["healthy"] = False
            state["last_probe"] = 0.0
        else:
            state["healthy"] = True
            state["failure_streak"] = 0
            old = state["ewma_ms"]
            state["ewma_ms"] = elapsed_ms if old is None else (0.75 * old + 0.25 * elapsed_ms)


def fetch_origin(filename):
    url = f"{ORIGIN_URL}/files/{quote(filename, safe='')}"
    increment_metric("origin_requests")
    try:
        forward_headers = {}
        for header_name in ("Range", "If-Range"):
            if request.headers.get(header_name):
                forward_headers[header_name] = request.headers[header_name]
        upstream = requests.get(url, headers=forward_headers, timeout=(2.0, 60.0))
        increment_metric("origin_responses")
        return copy_response(upstream, routed_edge="origin-fallback")
    except requests.RequestException as exc:
        increment_metric("origin_errors")
        return jsonify({"error": "No healthy edge was available and the origin request failed", "detail": str(exc)}), 502


@app.get("/")
def home():
    return "QoS-aware Edge CDN router is running. Use /health, /metrics, or /files/<filename>."


@app.get("/health")
def health():
    refresh_health()
    with state_lock:
        edges = {}
        for edge_id, state in edge_state.items():
            edges[edge_id] = {
                "url": EDGE_URLS[edge_id],
                "healthy": state["healthy"],
                "probe_latency_ms": round(state["probe_ms"], 3),
                "ewma_latency_ms": round(state["ewma_ms"], 3) if state["ewma_ms"] is not None else None,
                "active_requests": state["active"],
                "cached_file_count": len(state["cached_files"]),
                "failure_streak": state["failure_streak"],
                "simulated_overloaded": bool(state["simulated_overloaded"] and time.monotonic() < state["overload_until"]),
                "simulated_down": bool(state["simulated_down"] and time.monotonic() < state["down_until"]),
                "overload_weight": OVERLOADED_EDGE_WEIGHT if state["simulated_overloaded"] and time.monotonic() < state["overload_until"] else 1.0,
                "dispatch_available": state["healthy"] is True and not (state["simulated_down"] and time.monotonic() < state["down_until"]),
            }
    all_healthy = any(info["healthy"] and info["dispatch_available"] for info in edges.values())
    return jsonify({"service": "qos-router", "status": "ok" if all_healthy else "degraded", "edges": edges}), (200 if all_healthy else 503)


@app.get("/metrics")
def metrics():
    refresh_health()
    with state_lock:
        counts = dict(router_metrics)
        edges = {}
        for edge_id, state in edge_state.items():
            edges[edge_id] = {
                "healthy": state["healthy"],
                "ewma_latency_ms": round(state["ewma_ms"], 3) if state["ewma_ms"] is not None else None,
                "active_requests": state["active"],
                "requests_routed": state["requests"],
                "cached_file_count": len(state["cached_files"]),
                "failure_streak": state["failure_streak"],
                "simulated_overloaded": bool(state["simulated_overloaded"] and time.monotonic() < state["overload_until"]),
                "simulated_down": bool(state["simulated_down"] and time.monotonic() < state["down_until"]),
                "overload_weight": OVERLOADED_EDGE_WEIGHT if state["simulated_overloaded"] and time.monotonic() < state["overload_until"] else 1.0,
                "dispatch_available": state["healthy"] is True and not (state["simulated_down"] and time.monotonic() < state["down_until"]),
            }
    return jsonify({"router": counts, "edges": edges})


@app.post("/admin/simulated-overload")
def simulated_overload():
    """Local-demo control: mark an edge as degraded; do not fully exclude it."""
    if request.remote_addr not in {"127.0.0.1", "::1"}:
        return jsonify({"ok": False, "error": "Localhost-only control endpoint"}), 403
    payload = request.get_json(silent=True) or {}
    edge_id = str(payload.get("edge_id", "")).lower()
    enabled = bool(payload.get("enabled", False))
    if edge_id not in edge_state:
        return jsonify({"ok": False, "error": "edge_id must be edge1, edge2, or edge3"}), 400
    with state_lock:
        edge = edge_state[edge_id]
        if enabled and edge["simulated_down"] and time.monotonic() < edge["down_until"]:
            return jsonify({"ok": False, "error": f"{edge_id} is simulated down; restore it before overloading"}), 409
        edge["simulated_overloaded"] = enabled
        edge["overload_until"] = time.monotonic() + SIMULATED_OVERLOAD_LEASE_SECONDS if enabled else 0.0
    return jsonify({"ok": True, "edge_id": edge_id, "simulated_overloaded": enabled,
                    "selection_weight": OVERLOADED_EDGE_WEIGHT if enabled else 1.0,
                    "lease_seconds": SIMULATED_OVERLOAD_LEASE_SECONDS if enabled else 0,
                    "message": "Edge remains available with reduced routing weight" if enabled else "Normal routing weight restored"})


@app.post("/admin/simulated-failure")
def simulated_failure():
    """Local-demo control: simulate an edge outage at the router without killing its process."""
    if request.remote_addr not in {"127.0.0.1", "::1"}:
        return jsonify({"ok": False, "error": "Localhost-only control endpoint"}), 403
    payload = request.get_json(silent=True) or {}
    edge_id = str(payload.get("edge_id", "")).lower()
    enabled = bool(payload.get("enabled", False))
    if edge_id not in edge_state:
        return jsonify({"ok": False, "error": "edge_id must be edge1, edge2, or edge3"}), 400
    with state_lock:
        edge = edge_state[edge_id]
        if enabled:
            edge["simulated_overloaded"] = False
            edge["overload_until"] = 0.0
        edge["simulated_down"] = enabled
        edge["down_until"] = time.monotonic() + SIMULATED_FAILURE_LEASE_SECONDS if enabled else 0.0
    return jsonify({"ok": True, "edge_id": edge_id, "simulated_down": enabled,
                    "lease_seconds": SIMULATED_FAILURE_LEASE_SECONDS if enabled else 0,
                    "message": "Edge marked unavailable for router dispatch" if enabled else "Edge restored to router dispatch"})


@app.get("/files/<path:filename>")
def get_file(filename):
    started = time.perf_counter()
    increment_metric("requests")

    if filename in {"", ".", ".."} or "/" in filename or "\\" in filename:
        increment_metric("client_errors")
        return jsonify({"error": "Invalid filename"}), 400

    raw_hint = str(request.args.get("edge_hint", "")).lower()
    requested_edge = raw_hint if raw_hint in EDGE_URLS else None
    attempted = set()
    while True:
        candidates = choose_edge_order(filename, attempted, preferred_edge=requested_edge)
        if not candidates:
            break
        edge_id = candidates[0]
        attempted.add(edge_id)
        base_url = EDGE_URLS[edge_id]
        url = f"{base_url}/files/{quote(filename, safe='')}"
        mark_edge_started(edge_id)
        attempt_started = time.perf_counter()
        try:
            # Preserve byte-range headers so HTML5 video players can stream and seek.
            forward_headers = {}
            for header_name in ("Range", "If-Range"):
                if request.headers.get(header_name):
                    forward_headers[header_name] = request.headers[header_name]
            upstream = requests.get(url, headers=forward_headers, timeout=(2.0, 60.0), stream=True)
            elapsed_ms = (time.perf_counter() - attempt_started) * 1000
            if 200 <= upstream.status_code < 300 and upstream.headers.get("X-Cache-Stored") == "1":
                with state_lock:
                    edge_state[edge_id]["cached_files"].add(filename)
            elif upstream.headers.get("X-Cache-Stored") == "0":
                with state_lock:
                    edge_state[edge_id]["cached_files"].discard(filename)
            if upstream.status_code >= 500:
                upstream.close()
                mark_edge_finished(edge_id, elapsed_ms, status_code=upstream.status_code)
                increment_metric("upstream_failures")
                continue

            if 200 <= upstream.status_code < 300:
                increment_metric("successful_responses")
            elif 400 <= upstream.status_code < 500:
                increment_metric("client_errors")

            total_ms = (time.perf_counter() - started) * 1000

            def relay_body(response_to_relay=upstream, response_edge=edge_id, request_started=attempt_started):
                received_bytes = 0
                transport_error = False
                try:
                    for chunk in response_to_relay.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            received_bytes += len(chunk)
                            yield chunk
                except requests.RequestException:
                    transport_error = True
                    increment_metric("upstream_failures")
                finally:
                    response_to_relay.close()
                    full_elapsed_ms = (time.perf_counter() - request_started) * 1000
                    mark_edge_finished(response_edge, full_elapsed_ms,
                                       status_code=response_to_relay.status_code,
                                       transport_error=transport_error)
                    if 200 <= response_to_relay.status_code < 300:
                        increment_metric("bytes_returned", received_bytes)

            client_response = copy_response(upstream, routed_edge=edge_id,
                                            router_time_ms=total_ms, body=relay_body())
            client_response.headers["X-Requested-Edge"] = requested_edge or "auto"
            if requested_edge and requested_edge != edge_id:
                with state_lock:
                    preferred = edge_state[requested_edge]
                    overload = bool(preferred["simulated_overloaded"] and time.monotonic() < preferred["overload_until"])
                    simulated_down = bool(preferred["simulated_down"] and time.monotonic() < preferred["down_until"])
                    preferred_healthy = preferred["healthy"] is True
                client_response.headers["X-Redirected-From"] = requested_edge
                client_response.headers["X-Redirect-Reason"] = "simulated-overload" if overload else ("simulated-edge-outage" if simulated_down else ("edge-unavailable" if not preferred_healthy else "QoS-failover"))
            return client_response
        except requests.RequestException:
            elapsed_ms = (time.perf_counter() - attempt_started) * 1000
            mark_edge_finished(edge_id, elapsed_ms, transport_error=True)
            increment_metric("upstream_failures")
            # Remove the cached health result so a later request probes the failed node again.
            with state_lock:
                edge_state[edge_id]["last_probe"] = 0.0

    # If every edge is unavailable or returned a server error, try the origin directly.
    increment_metric("origin_fallbacks")
    response = fetch_origin(filename)
    total_ms = (time.perf_counter() - started) * 1000
    if isinstance(response, tuple):
        body, status = response
        body.headers["X-Router-Time-Ms"] = f"{total_ms:.3f}"
        body.headers["X-Requested-Edge"] = requested_edge or "auto"
        if requested_edge:
            body.headers["X-Redirected-From"] = requested_edge
            body.headers["X-Redirect-Reason"] = "no-healthy-edge"
        return body, status
    response.headers["X-Router-Time-Ms"] = f"{total_ms:.3f}"
    response.headers["X-Requested-Edge"] = requested_edge or "auto"
    if requested_edge:
        response.headers["X-Redirected-From"] = requested_edge
        response.headers["X-Redirect-Reason"] = "no-healthy-edge"
    if 200 <= response.status_code < 300:
        increment_metric("successful_responses")
        try:
            increment_metric("bytes_returned", int(response.headers.get("Content-Length", "0")))
        except ValueError:
            pass
    return response


if __name__ == "__main__":
    print("[ROUTER] Listening on http://127.0.0.1:9000")
    print("[ROUTER] Edge URLs:", EDGE_URLS)
    app.run(host="127.0.0.1", port=int(os.environ.get("ROUTER_PORT", "9000")), debug=False, threaded=True)
