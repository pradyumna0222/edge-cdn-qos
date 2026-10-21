"""Live multi-client dashboard for the QoS Edge CDN v1 demo.

Run after the origin, three edge nodes and QoS router have started:
    python live_dashboard.py
Then open http://127.0.0.1:9100
"""
import atexit
import copy
import os
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
from flask import Flask, Response, jsonify, render_template_string, request

app = Flask(__name__)
ROUTER_URL = os.environ.get("ROUTER_URL", "http://127.0.0.1:9000").rstrip("/")
ORIGIN_URL = os.environ.get("ORIGIN_URL", "http://127.0.0.1:5000").rstrip("/")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "9100"))
EDGE_URLS = {
    "edge1": os.environ.get("EDGE1_URL", "http://127.0.0.1:8001").rstrip("/"),
    "edge2": os.environ.get("EDGE2_URL", "http://127.0.0.1:8002").rstrip("/"),
    "edge3": os.environ.get("EDGE3_URL", "http://127.0.0.1:8003").rstrip("/"),
}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".avif"}
lock = threading.RLock()
events = deque(maxlen=500)
state = {
    "running": False, "requested_clients": 25, "active_clients": 0,
    "started_at": None, "stopped_at": None, "completed": 0,
    "successes": 0, "failures": 0, "bytes": 0, "total_latency_ms": 0.0,
    "latencies": deque(maxlen=5000), "client_threads": [], "stop_event": threading.Event(),
    "client_pool": None, "files": [], "next_client_id": 1,
    "assignment_counts": {"auto": 0, "edge1": 0, "edge2": 0, "edge3": 0},
    "redirected_requests": 0, "redirects_by_edge": {"edge1": 0, "edge2": 0, "edge3": 0},
    "overload_generated": 0, "overload_errors": 0, "overload_bytes": 0,
    "overload_inflight": {"edge1": 0, "edge2": 0, "edge3": 0},
    "overload_baseline": {"edge1": 0, "edge2": 0, "edge3": 0},
    "overload_window_counts": {"edge1": 0, "edge2": 0, "edge3": 0},
}

# Health and metrics are collected independently every 3 seconds, not on every UI refresh.
monitor = {
    "last_heartbeat": None,
    "router_health": {"status": "unreachable", "edges": {}},
    "router_metrics": {"router": {}, "edges": {}},
    "origin_health": {"status": "unreachable", "asset_count": 0},
    "edge_details": {},
    "edge_history": deque(maxlen=60),
    "origin_history": deque(maxlen=60),
    "last_origin_totals": None,
    "last_edge_request_counts": {},
    "origin_metrics_source": "waiting",
}
overload_manager = {"edge_id": None, "stop_event": None, "pool": None, "futures": []}
failure_manager = {"edge_id": None}
HEARTBEAT_SECONDS = 3.0
OVERLOAD_WORKERS = 3
OVERLOAD_PAUSE_SECONDS = 0.15


def add_event(**item):
    item.setdefault("time", datetime.now().strftime("%H:%M:%S.%f")[:-3])
    with lock:
        events.appendleft(item)


def is_image(filename):
    return Path(filename).suffix.lower() in IMAGE_EXTENSIONS


def valid_filename(filename):
    return bool(filename) and filename not in {".", ".."} and "/" not in filename and "\\" not in filename


def available_files():
    try:
        r = requests.get(f"{ORIGIN_URL}/files", timeout=2.5)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            return []
        return sorted({str(x) for x in data if isinstance(x, str) and valid_filename(x)})
    except (requests.RequestException, ValueError, TypeError):
        return []


def catalog_contains(filename):
    with lock:
        current = list(state["files"])
    if not current:
        current = available_files()
        with lock:
            state["files"] = current
    return filename in current


def choose_client_file(files):
    """Use images for most simulated requests, while retaining mixed-content tests."""
    images = [name for name in files if is_image(name)]
    others = [name for name in files if not is_image(name)]
    if images and others and random.random() < 0.88:
        return random.choice(images)
    return random.choice(images or files)


def record_result(client, filename, edge, status, elapsed_ms, cache, size, detail,
                  requested_edge=None, redirected_from=None, redirect_reason=None, selection_roll=None):
    ok = 200 <= int(status or 0) < 300
    with lock:
        state["completed"] += 1
        state["successes"] += int(ok)
        state["failures"] += int(not ok)
        state["bytes"] += int(size or 0)
        state["total_latency_ms"] += float(elapsed_ms or 0)
        state["latencies"].append(float(elapsed_ms or 0))
        if client.startswith("Client ") and requested_edge in state["assignment_counts"]:
            state["assignment_counts"][requested_edge] += 1
        if redirected_from in state["redirects_by_edge"]:
            state["redirected_requests"] += 1
            state["redirects_by_edge"][redirected_from] += 1
    add_event(client=client, filename=filename, edge=edge, status=status or "ERROR",
              latency_ms=round(float(elapsed_ms or 0), 2), cache=cache or "—", bytes=int(size or 0),
              requested_edge=requested_edge or "—", redirected_from=redirected_from or "",
              redirect_reason=redirect_reason or "", selection_roll=selection_roll,
              detail=detail or ("OK" if ok else "Request failed"))


def client_loop(client_id, stop_event):
    """Generate HTTP requests with a reproducible-to-explain random edge hint.

    random.randint(0, 3): 0 = auto QoS; 1/2/3 = prefer edge1/edge2/edge3.
    All requests still pass through the QoS router; the router redirects hints away
    from simulated-overloaded or unhealthy edges.
    """
    session = requests.Session()
    try:
        while not stop_event.is_set():
            with lock:
                choices = list(state["files"])
            if not choices:
                choices = available_files()
                with lock:
                    state["files"] = choices
            if not choices:
                add_event(client=f"Client {client_id}", filename="—", edge="—", status="WAITING",
                          latency_ms=0, cache="—", bytes=0, detail="Origin has no listed content or is unavailable")
                stop_event.wait(2)
                continue

            filename = choose_client_file(choices)
            selection_roll = random.randint(0, 3)
            preferred = None if selection_roll == 0 else f"edge{selection_roll}"
            requested_edge = preferred or "auto"
            started = time.perf_counter()
            status_code = 0
            size = 0
            edge = "unknown"
            cache = "unknown"
            error = ""
            redirected_from = None
            redirect_reason = None
            try:
                params = {"edge_hint": preferred} if preferred else None
                response = session.get(f"{ROUTER_URL}/files/{quote(filename, safe='')}",
                                       params=params, timeout=(2, 25))
                status_code = response.status_code
                size = len(response.content)
                edge = response.headers.get("X-Routed-Edge", "unknown")
                cache = response.headers.get("X-Cache", "not reported")
                redirected_from = response.headers.get("X-Redirected-From")
                redirect_reason = response.headers.get("X-Redirect-Reason")
                response.close()
                if status_code >= 400:
                    error = f"HTTP {status_code}"
                elif redirected_from:
                    error = f"Redirected from {redirected_from}: {redirect_reason or 'QoS failover'}"
            except requests.RequestException as exc:
                error = str(exc)[:120]
            elapsed = (time.perf_counter() - started) * 1000
            record_result(f"Client {client_id}", filename, edge, status_code, elapsed, cache, size, error,
                          requested_edge=requested_edge, redirected_from=redirected_from,
                          redirect_reason=redirect_reason, selection_roll=selection_roll)
            stop_event.wait(random.uniform(0.25, 1.0))
    finally:
        session.close()


def start_simulation(client_count=25):
    client_count = max(1, min(100, int(client_count)))
    with lock:
        if state["running"]:
            return False, "Simulation is already running"
        state["stop_event"] = threading.Event()
        stop_event = state["stop_event"]
        state.update({"running": True, "requested_clients": client_count, "active_clients": client_count,
                      "started_at": datetime.now().isoformat(timespec="seconds"), "stopped_at": None,
                      "completed": 0, "successes": 0, "failures": 0, "bytes": 0,
                      "total_latency_ms": 0.0, "latencies": deque(maxlen=5000), "files": available_files(),
                      "assignment_counts": {"auto": 0, "edge1": 0, "edge2": 0, "edge3": 0},
                      "redirected_requests": 0, "redirects_by_edge": {"edge1": 0, "edge2": 0, "edge3": 0}})
        events.clear()
        pool = ThreadPoolExecutor(max_workers=client_count, thread_name_prefix="cdn-client")
        state["client_pool"] = pool
        state["client_threads"] = [pool.submit(client_loop, i + 1, stop_event) for i in range(client_count)]
    add_event(client="SYSTEM", filename="—", edge="—", status="STARTED", latency_ms=0, cache="—", bytes=0,
              detail=f"Started {client_count} concurrent simulated clients")
    return True, f"Started {client_count} simulated clients"


def stop_simulation():
    with lock:
        if not state["running"]:
            return False, "Simulation is not running"
        state["running"] = False
        state["active_clients"] = 0
        state["stopped_at"] = datetime.now().isoformat(timespec="seconds")
        stop_event = state["stop_event"]
        pool = state["client_pool"]
        stop_event.set()
    if pool:
        pool.shutdown(wait=False, cancel_futures=True)
    add_event(client="SYSTEM", filename="—", edge="—", status="STOPPED", latency_ms=0, cache="—", bytes=0,
              detail="Simulation stopped")
    return True, "Simulation stopped"


def overload_worker(edge_id, stop_event, worker_number):
    """Bounded synthetic HTTP load aimed directly at one edge for classroom testing."""
    session = requests.Session()
    base_url = EDGE_URLS[edge_id]
    try:
        while not stop_event.is_set():
            with lock:
                choices = list(state["files"])
            if not choices:
                choices = available_files()
            images = [name for name in choices if is_image(name)]
            target = random.choice(images or choices) if choices else None
            if not target:
                stop_event.wait(0.5)
                continue
            with lock:
                state["overload_inflight"][edge_id] += 1
            try:
                response = session.get(f"{base_url}/files/{quote(target, safe='')}", timeout=(1, 5))
                payload_size = len(response.content)
                with lock:
                    state["overload_generated"] += 1
                    state["overload_bytes"] += payload_size
                    if response.status_code >= 400:
                        state["overload_errors"] += 1
                response.close()
            except requests.RequestException:
                with lock:
                    state["overload_generated"] += 1
                    state["overload_errors"] += 1
            finally:
                with lock:
                    state["overload_inflight"][edge_id] = max(0, state["overload_inflight"][edge_id] - 1)
            stop_event.wait(OVERLOAD_PAUSE_SECONDS)
    finally:
        session.close()


def read_router_edge_counts():
    try:
        payload = requests.get(f"{ROUTER_URL}/metrics", timeout=1.5).json()
        edges = payload.get("edges", {}) or {}
        return {edge: int((edges.get(edge) or {}).get("requests_routed", 0) or 0) for edge in EDGE_URLS}
    except Exception:
        return {edge: 0 for edge in EDGE_URLS}


def start_overload(edge_id):
    if edge_id not in EDGE_URLS:
        return False, "Choose edge1, edge2, or edge3"
    with lock:
        previous = overload_manager["edge_id"]
        failed_edge = failure_manager["edge_id"]
    if failed_edge:
        stop_failure()
    if previous:
        stop_overload()
    baseline = read_router_edge_counts()
    try:
        response = requests.post(f"{ROUTER_URL}/admin/simulated-overload",
                                 json={"edge_id": edge_id, "enabled": True}, timeout=2)
        response.raise_for_status()
    except requests.RequestException as exc:
        try:
            message = exc.response.json().get("error", str(exc)) if exc.response is not None else str(exc)
        except Exception:
            message = str(exc)
        return False, f"Could not activate router overload steering: {str(message)[:140]}"
    stop_event = threading.Event()
    pool = ThreadPoolExecutor(max_workers=OVERLOAD_WORKERS, thread_name_prefix="cdn-overload")
    futures = [pool.submit(overload_worker, edge_id, stop_event, i + 1) for i in range(OVERLOAD_WORKERS)]
    with lock:
        overload_manager.update({"edge_id": edge_id, "stop_event": stop_event, "pool": pool, "futures": futures})
        state["overload_baseline"] = dict(baseline)
        state["overload_window_counts"] = {edge: 0 for edge in EDGE_URLS}
    add_event(client="SYSTEM", filename="—", edge=edge_id, status="OVERLOAD_ON", latency_ms=0,
              cache="—", bytes=0, detail=f"Synthetic HTTP load started ({OVERLOAD_WORKERS} bounded workers); router reduces this edge's selection weight to 0.30")
    return True, f"Synthetic load enabled for {edge_id}; its routing share is reduced, not set to zero"


def stop_overload():
    with lock:
        edge_id = overload_manager["edge_id"]
        stop_event = overload_manager["stop_event"]
        pool = overload_manager["pool"]
        overload_manager.update({"edge_id": None, "stop_event": None, "pool": None, "futures": []})
    if not edge_id:
        return True, "No overload simulation is active"
    if stop_event:
        stop_event.set()
    try:
        requests.post(f"{ROUTER_URL}/admin/simulated-overload",
                      json={"edge_id": edge_id, "enabled": False}, timeout=1.5)
    except requests.RequestException:
        # The router's 12-second lease also clears a stale override if the dashboard exits.
        pass
    if pool:
        pool.shutdown(wait=False, cancel_futures=True)
    add_event(client="SYSTEM", filename="—", edge=edge_id, status="OVERLOAD_OFF", latency_ms=0,
              cache="—", bytes=0, detail="Synthetic HTTP load stopped; normal QoS routing restored")
    return True, f"Overload simulation stopped for {edge_id}; normal routing weights restored"


def start_failure(edge_id):
    if edge_id not in EDGE_URLS:
        return False, "Choose edge1, edge2, or edge3"
    with lock:
        existing_failure = failure_manager["edge_id"]
        existing_overload = overload_manager["edge_id"]
    if existing_overload:
        stop_overload()
    if existing_failure:
        stop_failure()
    try:
        response = requests.post(f"{ROUTER_URL}/admin/simulated-failure",
                                 json={"edge_id": edge_id, "enabled": True}, timeout=2)
        response.raise_for_status()
    except requests.RequestException as exc:
        try:
            message = exc.response.json().get("error", str(exc)) if exc.response is not None else str(exc)
        except Exception:
            message = str(exc)
        return False, f"Could not simulate edge outage: {str(message)[:140]}"
    with lock:
        failure_manager["edge_id"] = edge_id
    add_event(client="SYSTEM", filename="—", edge=edge_id, status="EDGE_DOWN", latency_ms=0,
              cache="—", bytes=0, detail="Simulated outage: router will stop dispatching new requests to this edge; edge process itself stays running")
    return True, f"{edge_id} is simulated unavailable; requests will fail over to healthy peers"


def stop_failure():
    with lock:
        edge_id = failure_manager["edge_id"]
        failure_manager["edge_id"] = None
    if not edge_id:
        return True, "No simulated outage is active"
    try:
        requests.post(f"{ROUTER_URL}/admin/simulated-failure",
                      json={"edge_id": edge_id, "enabled": False}, timeout=1.5)
    except requests.RequestException:
        pass
    add_event(client="SYSTEM", filename="—", edge=edge_id, status="EDGE_RESTORED", latency_ms=0,
              cache="—", bytes=0, detail="Simulated outage removed; edge is eligible for QoS routing again")
    return True, f"{edge_id} restored to router dispatch"


def collect_heartbeat():
    router_health = {"status": "unreachable", "edges": {}}
    router_metrics = {"router": {}, "edges": {}}
    origin_health = {"status": "unreachable", "asset_count": 0}
    edge_details = {}
    try:
        response = requests.get(f"{ROUTER_URL}/health", timeout=1.2)
        router_health = response.json()
    except Exception as exc:
        router_health = {"status": "unreachable", "error": str(exc)[:100], "edges": {}}
    try:
        response = requests.get(f"{ROUTER_URL}/metrics", timeout=1.2)
        router_metrics = response.json()
    except Exception:
        pass
    try:
        response = requests.get(f"{ORIGIN_URL}/health", timeout=1.2)
        origin_health = response.json()
    except Exception as exc:
        origin_health = {"status": "unreachable", "error": str(exc)[:100], "asset_count": 0}
    origin_direct_metrics = None
    try:
        response = requests.get(f"{ORIGIN_URL}/metrics", timeout=1.2)
        response.raise_for_status()
        candidate = response.json()
        if candidate.get("service") == "origin" and "content_requests" in candidate:
            origin_direct_metrics = candidate
    except Exception:
        pass
    for edge_id, base_url in EDGE_URLS.items():
        try:
            response = requests.get(f"{base_url}/health", timeout=0.8)
            response.raise_for_status()
            payload = response.json()
            payload["heartbeat_http_ok"] = True
            edge_details[edge_id] = payload
        except Exception as exc:
            edge_details[edge_id] = {"status": "unreachable", "cached_files": [], "metrics": {},
                                     "heartbeat_http_ok": False, "error": str(exc)[:100]}
    now = datetime.now().strftime("%H:%M:%S")
    with lock:
        router_edges = (router_health.get("edges") or {})
        metric_edges = (router_metrics.get("edges") or {})
        request_delta = {}
        for edge_id, detail in edge_details.items():
            edge_m = detail.get("metrics", {}) or {}
            current = int(edge_m.get("requests", 0) or 0)
            previous = int(monitor["last_edge_request_counts"].get(edge_id, current))
            request_delta[edge_id] = max(0, current - previous)
            monitor["last_edge_request_counts"][edge_id] = current
            detail["router_active_requests"] = int((router_edges.get(edge_id) or {}).get("active_requests", 0) or 0)
            detail["router_requests_routed"] = int((metric_edges.get(edge_id) or {}).get("requests_routed", 0) or 0)
            detail["simulated_overloaded"] = bool((router_edges.get(edge_id) or {}).get("simulated_overloaded", False))
            detail["dashboard_overload_inflight"] = int(state["overload_inflight"].get(edge_id, 0))
            detail["interval_requests"] = request_delta[edge_id]
        edge_origin_requests = sum(int((d.get("metrics") or {}).get("origin_fetches", 0) or 0) for d in edge_details.values())
        edge_origin_errors = sum(int((d.get("metrics") or {}).get("origin_errors", 0) or 0) for d in edge_details.values())
        router_counts = router_metrics.get("router", {}) or {}
        router_origin_requests = int(router_counts.get("origin_requests", router_counts.get("origin_fallbacks", 0)) or 0)
        router_origin_responses = int(router_counts.get("origin_responses", max(0, router_origin_requests - int(router_counts.get("origin_errors", 0) or 0))) or 0)
        router_origin_errors = int(router_counts.get("origin_errors", 0) or 0)
        if origin_direct_metrics is not None:
            # Authoritative origin-side request accounting. This catches every content GET,
            # including requests from edges on misses and router direct-origin fallback.
            origin_totals = {
                "requests": int(origin_direct_metrics.get("content_requests", 0) or 0),
                "responses": int(origin_direct_metrics.get("content_responses", 0) or 0),
                "errors": int(origin_direct_metrics.get("content_errors", 0) or 0),
                "bytes": int(origin_direct_metrics.get("bytes_sent", 0) or 0),
                "catalog_requests": int(origin_direct_metrics.get("catalog_requests", 0) or 0),
                "edge_cache_miss_fetches": edge_origin_requests,
                "router_fallback_requests": router_origin_requests,
            }
            origin_source = "origin-side /metrics"
        else:
            # Compatible fallback if the user's older origin server lacks /metrics.
            origin_totals = {
                "requests": edge_origin_requests + router_origin_requests,
                "responses": max(0, edge_origin_requests - edge_origin_errors) + router_origin_responses,
                "errors": edge_origin_errors + router_origin_errors,
                "bytes": 0,
                "catalog_requests": 0,
                "edge_cache_miss_fetches": edge_origin_requests,
                "router_fallback_requests": router_origin_requests,
            }
            origin_source = "edge/router estimate (origin /metrics unavailable)"
        prev_origin = monitor["last_origin_totals"]
        if prev_origin is None:
            origin_delta = {"requests": 0, "responses": 0, "errors": 0, "bytes": 0}
        else:
            origin_delta = {k: max(0, origin_totals.get(k, 0) - prev_origin.get(k, 0)) for k in ("requests", "responses", "errors", "bytes")}
        monitor["last_origin_totals"] = dict(origin_totals)
        cache_hits = sum(int((d.get("metrics") or {}).get("cache_hits", 0) or 0) for d in edge_details.values())
        cache_misses = sum(int((d.get("metrics") or {}).get("cache_misses", 0) or 0) for d in edge_details.values())
        cache_metrics = {
            "hits": cache_hits,
            "misses": cache_misses,
            "hit_ratio_percent": round(100.0 * cache_hits / (cache_hits + cache_misses), 2) if cache_hits + cache_misses else 0.0,
            "cached_objects": sum(len(d.get("cached_files") or []) for d in edge_details.values()),
            "cache_bytes": sum(int(d.get("cache_bytes", 0) or 0) for d in edge_details.values()),
            "capacity_bytes": sum(int(d.get("cache_capacity_bytes", 0) or 0) for d in edge_details.values()),
            "evictions": sum(int((d.get("metrics") or {}).get("cache_evictions", 0) or 0) for d in edge_details.values()),
            "origin_fetches": edge_origin_requests,
            "origin_errors": edge_origin_errors,
        }
        monitor["edge_history"].append({"time": now, **request_delta})
        monitor["origin_history"].append({"time": now, **origin_delta})
        monitor.update({"last_heartbeat": now, "router_health": router_health, "router_metrics": router_metrics,
                        "origin_health": origin_health, "edge_details": edge_details,
                        "origin_metrics": origin_totals, "origin_interval": origin_delta,
                        "origin_metrics_source": origin_source,
                        "cache_metrics": cache_metrics})
        active_overload = overload_manager["edge_id"]
        active_failure = failure_manager["edge_id"]
        # Windowed routed-attempt counts from the moment overload simulation started.
        if active_overload:
            baselines = state.get("overload_baseline", {})
            state["overload_window_counts"] = {
                edge: max(0, int((metric_edges.get(edge) or {}).get("requests_routed", 0) or 0) - int(baselines.get(edge, 0) or 0))
                for edge in EDGE_URLS
            }
    # Renew the router's overload lease. If the dashboard disappears, the router clears it after 12 sec.
    if active_overload:
        # Check again before renewal; if Stop was clicked concurrently, do not renew.
        with lock:
            still_active = overload_manager["edge_id"] == active_overload
        if still_active:
            try:
                requests.post(f"{ROUTER_URL}/admin/simulated-overload",
                              json={"edge_id": active_overload, "enabled": True}, timeout=1.0)
            except requests.RequestException:
                pass
            with lock:
                stopped_during_renewal = overload_manager["edge_id"] != active_overload
            if stopped_during_renewal:
                try:
                    requests.post(f"{ROUTER_URL}/admin/simulated-overload",
                                  json={"edge_id": active_overload, "enabled": False}, timeout=1.0)
                except requests.RequestException:
                    pass
    # Renew the simulated-outage lease as well; if this dashboard exits, the router
    # automatically restores the edge after 12 seconds.
    if active_failure:
        with lock:
            still_active = failure_manager["edge_id"] == active_failure
        if still_active:
            try:
                requests.post(f"{ROUTER_URL}/admin/simulated-failure",
                              json={"edge_id": active_failure, "enabled": True}, timeout=1.0)
            except requests.RequestException:
                pass
            with lock:
                stopped_during_renewal = failure_manager["edge_id"] != active_failure
            if stopped_during_renewal:
                try:
                    requests.post(f"{ROUTER_URL}/admin/simulated-failure",
                                  json={"edge_id": active_failure, "enabled": False}, timeout=1.0)
                except requests.RequestException:
                    pass


def heartbeat_loop():
    while True:
        try:
            collect_heartbeat()
        except Exception as exc:
            add_event(client="HEARTBEAT", filename="—", edge="—", status="ERROR", latency_ms=0,
                      cache="—", bytes=0, detail=str(exc)[:120])
        time.sleep(HEARTBEAT_SECONDS)


@app.post("/api/overload")
def api_overload():
    payload = request.get_json(silent=True) or {}
    if not bool(payload.get("enabled", False)):
        ok, message = stop_overload()
        return jsonify({"ok": ok, "message": message})
    edge_id = str(payload.get("edge_id", "")).lower()
    ok, message = start_overload(edge_id)
    return jsonify({"ok": ok, "message": message}), (200 if ok else 502)


@app.post("/api/failure")
def api_failure():
    payload = request.get_json(silent=True) or {}
    if not bool(payload.get("enabled", False)):
        ok, message = stop_failure()
        return jsonify({"ok": ok, "message": message})
    edge_id = str(payload.get("edge_id", "")).lower()
    ok, message = start_failure(edge_id)
    return jsonify({"ok": ok, "message": message}), (200 if ok else 502)


@app.get("/")
def index():
    return render_template_string(HTML)


@app.get("/api/content")
def api_content():
    """Return the origin's real catalog so the dashboard gallery follows cdn_images."""
    files = available_files()
    with lock:
        state["files"] = files
    assets = [{"name": name, "kind": "image" if is_image(name) else "file",
               "preview_url": f"/api/preview/{quote(name, safe='')}" if is_image(name) else None,
               "access_url": f"/api/access/{quote(name, safe='')}"} for name in files]
    image_count = sum(1 for asset in assets if asset["kind"] == "image")
    return jsonify({"ok": bool(files), "origin_url": ORIGIN_URL, "count": len(assets),
                    "image_count": image_count, "assets": assets,
                    "message": "" if files else "Origin returned no files. Check that the origin is running and cdn_images contains content."})


@app.get("/api/preview/<path:filename>")
def api_preview(filename):
    """Proxy image previews from origin; gallery previews are distinct from CDN traffic."""
    if not valid_filename(filename) or not is_image(filename):
        return jsonify({"error": "Preview is available for image files only"}), 400
    if not catalog_contains(filename):
        return jsonify({"error": "File is not in the origin catalog"}), 404
    try:
        upstream = requests.get(f"{ORIGIN_URL}/files/{quote(filename, safe='')}", timeout=(2, 8))
        content_type = upstream.headers.get("Content-Type", "application/octet-stream")
        if upstream.status_code != 200:
            return jsonify({"error": f"Origin returned HTTP {upstream.status_code}"}), upstream.status_code
        if not content_type.lower().startswith("image/"):
            return jsonify({"error": "The origin file is not served as an image"}), 415
        response = Response(upstream.content, content_type=content_type)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Gallery-Preview"] = "origin-preview"
        return response
    except requests.RequestException as exc:
        return jsonify({"error": f"Origin preview failed: {str(exc)[:120]}"}), 502


@app.get("/api/access/<path:filename>")
def api_access(filename):
    """Request selected content through the QoS router and return its bytes to the UI."""
    if not valid_filename(filename) or not catalog_contains(filename):
        return jsonify({"error": "File is not in the origin catalog"}), 404
    started = time.perf_counter()
    try:
        upstream = requests.get(f"{ROUTER_URL}/files/{quote(filename, safe='')}", timeout=(2, 25))
        elapsed = (time.perf_counter() - started) * 1000
        content = upstream.content
        edge = upstream.headers.get("X-Routed-Edge", "unknown")
        cache = upstream.headers.get("X-Cache", "not reported")
        detail = "Gallery request succeeded" if upstream.status_code < 400 else f"HTTP {upstream.status_code}"
        record_result("Gallery", filename, edge, upstream.status_code, elapsed, cache, len(content), detail)
        response = Response(content, status=upstream.status_code,
                            content_type=upstream.headers.get("Content-Type", "application/octet-stream"))
        for header in ("X-Routed-Edge", "X-Cache", "X-Router-Time-Ms", "X-Edge-Time-Ms", "Content-Disposition", "X-Requested-Edge", "X-Redirected-From", "X-Redirect-Reason"):
            if header in upstream.headers:
                response.headers[header] = upstream.headers[header]
        response.headers["X-Dashboard-Latency-Ms"] = f"{elapsed:.2f}"
        response.headers["Cache-Control"] = "no-store"
        return response
    except requests.RequestException as exc:
        elapsed = (time.perf_counter() - started) * 1000
        record_result("Gallery", filename, "unknown", "ERROR", elapsed, "—", 0, str(exc)[:120])
        return jsonify({"error": f"CDN request failed: {str(exc)[:150]}"}), 502


@app.get("/api/state")
def api_state():
    with lock:
        latencies = list(state["latencies"])
        count = state["completed"]
        snapshot = {k: state[k] for k in ("running", "requested_clients", "active_clients", "started_at", "stopped_at", "completed", "successes", "failures", "bytes", "assignment_counts", "redirected_requests", "redirects_by_edge", "overload_generated", "overload_errors", "overload_bytes", "overload_inflight", "overload_window_counts")}
        snapshot["average_latency_ms"] = round(state["total_latency_ms"] / count, 2) if count else 0
        snapshot["success_rate"] = round(state["successes"] * 100 / count, 1) if count else 0
        snapshot["p95_latency_ms"] = round(sorted(latencies)[min(len(latencies) - 1, int(len(latencies) * 0.95))], 2) if latencies else 0
        snapshot["events"] = list(events)[:80]
        snapshot["content_count"] = len(state["files"])
        snapshot["image_count"] = sum(1 for x in state["files"] if is_image(x))
        snapshot["overload_edge"] = overload_manager["edge_id"]
        snapshot["failed_edge"] = failure_manager["edge_id"]
        snapshot["last_heartbeat"] = monitor["last_heartbeat"]
        snapshot["router_health"] = copy.deepcopy(monitor["router_health"])
        snapshot["router_metrics"] = copy.deepcopy(monitor["router_metrics"])
        snapshot["origin_health"] = copy.deepcopy(monitor["origin_health"])
        snapshot["edge_details"] = copy.deepcopy(monitor["edge_details"])
        snapshot["cache_metrics"] = copy.deepcopy(monitor.get("cache_metrics", {}))
        snapshot["origin_metrics_source"] = monitor.get("origin_metrics_source", "waiting")
        snapshot["origin_metrics"] = copy.deepcopy(monitor.get("origin_metrics", {}))
        snapshot["origin_interval"] = copy.deepcopy(monitor.get("origin_interval", {}))
        snapshot["origin_history"] = list(monitor["origin_history"])
        snapshot["edge_history"] = list(monitor["edge_history"])
        snapshot["heartbeat_seconds"] = HEARTBEAT_SECONDS
    return jsonify(snapshot)


@app.post("/api/start")
def api_start():
    payload = request.get_json(silent=True) or {}
    ok, message = start_simulation(payload.get("clients", 25))
    return jsonify({"ok": ok, "message": message}), (200 if ok else 409)


@app.post("/api/stop")
def api_stop():
    ok, message = stop_simulation()
    return jsonify({"ok": ok, "message": message}), (200 if ok else 409)


HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>QoS Edge CDN · Live Control Room</title>
<style>
:root{color-scheme:light;--bg:#f3f6fb;--panel:#fff;--ink:#17243a;--muted:#687992;--line:#dbe4ef;--navy:#122540;--blue:#2563eb;--cyan:#0891b2;--green:#07845e;--amber:#b76e09;--red:#c9364e;--shadow:0 5px 22px #203b5b0a}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 Inter,Segoe UI,Arial,sans-serif}button,input,select{font:inherit}header{background:var(--navy);color:#fff;padding:21px clamp(16px,3vw,40px);display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap}.brand{display:flex;align-items:center;gap:13px}.logo{height:46px;width:46px;border-radius:12px;background:#1c3e62;border:1px solid #416184;display:grid;place-items:center;font-size:23px}.title{font-size:21px;font-weight:800;letter-spacing:-.25px}.sub{font-size:12px;color:#bdcce0}.headRight{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.statuspill{display:flex;align-items:center;gap:8px;border:1px solid #416184;border-radius:99px;padding:8px 12px;font-size:12px;color:#e2ecf8}.dot{height:8px;width:8px;border-radius:50%;background:#e7a43b;display:inline-block}.wrap{max-width:1600px;margin:auto;padding:22px clamp(12px,2.3vw,32px) 40px;display:grid;gap:17px}.sectionTitle{font-size:17px;font-weight:800;margin:0}.sectionSub{font-size:12px;color:var(--muted);margin-top:3px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:17px;min-width:0;box-shadow:var(--shadow)}.sectionhead{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap;margin-bottom:15px}.eyebrow{font-size:10px;text-transform:uppercase;letter-spacing:1.2px;color:var(--blue);font-weight:850;margin-bottom:6px}.intro{display:flex;justify-content:space-between;align-items:center;gap:18px;flex-wrap:wrap}.introText{max-width:790px}.introText h1{font-size:25px;margin:0 0 5px;letter-spacing:-.6px}.introText p{color:var(--muted);margin:0;max-width:820px}.controls{display:flex;align-items:center;gap:9px;flex-wrap:wrap}button{border:1px solid transparent;border-radius:9px;padding:10px 14px;font-weight:750;cursor:pointer;background:var(--blue);color:white;transition:transform .12s,opacity .12s}button:hover{transform:translateY(-1px)}button:disabled{opacity:.45;cursor:not-allowed;transform:none}button.secondary{background:#f5f8fc;color:#263952;border-color:var(--line)}button.stop{background:var(--red)}button.small{padding:7px 10px;font-size:12px}select,input[type=search]{border:1px solid #cbd7e6;background:white;color:var(--ink);border-radius:9px;padding:10px 11px}.metrics{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px}.metric{padding:14px 15px}.label{font-size:12px;color:var(--muted)}.value{font-size:27px;font-weight:820;letter-spacing:-.8px;margin-top:6px}.hint{font-size:11px;color:var(--muted);margin-top:4px}.blue{color:var(--blue)}.green{color:var(--green)}.amber{color:var(--amber)}.red{color:var(--red)}.muted{color:var(--muted)}.pill{display:inline-flex;align-items:center;gap:5px;border-radius:99px;padding:4px 8px;font-size:10px;font-weight:800;background:#e8f8f1;color:var(--green);white-space:nowrap}.pill.down{background:#fff0f1;color:var(--red)}.pill.wait{background:#fff6e4;color:var(--amber)}.topologyPanel{padding:18px}.topology{display:grid;grid-template-columns:minmax(180px,.9fr) 32px minmax(210px,1fr) 32px minmax(430px,1.85fr) 32px minmax(150px,.75fr);gap:9px;align-items:stretch}.flowArrow{display:grid;place-items:center;color:#8da3bd;font-size:24px;font-weight:800}.flowCard{border:1px solid var(--line);border-radius:12px;padding:14px;background:#fbfdff;min-width:0}.flowCard.routerCard{background:#edf6ff;border-color:#bcd8fc}.flowCard.originCard{background:#f0fbf7;border-color:#bdebd8}.flowHead{display:flex;align-items:center;gap:9px;margin-bottom:10px}.flowIcon{height:35px;width:35px;display:grid;place-items:center;border-radius:10px;background:#e6effc;font-size:18px;flex-shrink:0}.routerCard .flowIcon{background:#d8eaff}.originCard .flowIcon{background:#d9f5e8}.flowName{font-size:14px;font-weight:850}.flowPort{font-size:11px;color:var(--muted)}.flowDesc{font-size:12px;color:var(--muted);margin-bottom:10px}.clientGrid{display:grid;grid-template-columns:repeat(5,1fr);gap:5px}.clientChip{border:1px solid #dce7f5;color:#38516f;background:#fff;border-radius:6px;text-align:center;padding:5px 0;font-size:10px;font-weight:750}.clientChip.busy{background:#e7f3ff;border-color:#8abaf5;color:#1456ac}.flowStat{font-size:12px;margin-top:6px}.flowStat strong{font-size:14px}.serverNodes{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}.node{border:1px solid var(--line);border-radius:10px;background:#fff;padding:11px;min-width:0}.nodeHead{display:flex;align-items:center;justify-content:space-between;gap:5px;flex-wrap:wrap;margin-bottom:9px}.nodeName{font-size:12px;font-weight:850}.nodePort{font-size:10px;color:var(--muted)}.nodeStats{display:grid;gap:5px;font-size:11px;color:var(--muted)}.nodeStats span{display:flex;justify-content:space-between;gap:5px}.nodeStats b{color:var(--ink);font-weight:750;text-align:right}.cachebar{height:6px;background:#eaf0f7;border-radius:50px;overflow:hidden;margin-top:10px}.cachebar span{display:block;height:100%;background:var(--blue);border-radius:50px;width:0;transition:width .2s}.cachedNames{margin-top:7px;color:#6b7b90;font-size:10px;line-height:1.45;word-break:break-word}.originCount{font-size:23px;font-weight:850;margin:3px 0;color:#087650}.originCount small{font-size:12px;font-weight:600;color:var(--muted)}.contentPanel{padding:18px}.catalogTop{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:13px}.catalogFilters{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.gallery{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px}.assetCard{border:1px solid var(--line);border-radius:12px;overflow:hidden;background:white;min-width:0;display:flex;flex-direction:column;transition:border-color .15s,box-shadow .15s}.assetCard:hover{border-color:#9ebae1;box-shadow:0 5px 18px #1c3c620d}.thumb{height:145px;background:linear-gradient(135deg,#edf4fb,#f8fafc);display:grid;place-items:center;position:relative;overflow:hidden}.thumb img{height:100%;width:100%;object-fit:contain}.thumb .fileGlyph{font-size:41px;color:#7187a2}.typeBadge{position:absolute;left:8px;top:8px;border-radius:5px;padding:4px 6px;font-size:9px;font-weight:850;background:#ffffffeb;color:#40556d;border:1px solid #dce4ed}.assetInfo{padding:11px;display:flex;flex-direction:column;gap:7px;flex:1}.assetName{font-weight:800;font-size:12px;overflow-wrap:anywhere}.assetNote{font-size:10px;color:var(--muted);min-height:15px}.assetAction{margin-top:auto;display:flex;gap:6px}.assetAction button{width:100%;font-size:11px;padding:8px 7px}.fetchState{font-size:10px;color:var(--green);min-height:13px}.emptyState{padding:36px 16px;text-align:center;color:var(--muted);border:1px dashed #cad8e8;border-radius:12px;grid-column:1/-1}.twocol{display:grid;grid-template-columns:1fr 1fr;gap:16px}.chartbox{height:190px;position:relative}.chartbox canvas{height:100%;width:100%}.legend{display:flex;gap:12px;flex-wrap:wrap;font-size:11px;color:var(--muted);margin-top:6px}.legend span{display:inline-flex;align-items:center;gap:6px}.legend i{width:8px;height:8px;border-radius:2px;display:inline-block}.routeBars{display:grid;gap:12px}.routeLabel{display:flex;justify-content:space-between;gap:10px;font-size:12px;margin-bottom:5px}.tablewrap{overflow:auto;max-height:420px}table{border-collapse:collapse;width:100%;font-size:12px;white-space:nowrap}th{text-align:left;background:#f5f8fc;color:#61748d;font-size:10px;text-transform:uppercase;letter-spacing:.6px;position:sticky;top:0;z-index:1}td,th{padding:10px 11px;border-bottom:1px solid #ebf0f6}tbody tr:hover{background:#f9fbfe}.contentCell{display:flex;align-items:center;gap:7px}.miniThumb{height:28px;width:36px;border:1px solid var(--line);background:#f4f7fb;border-radius:5px;object-fit:contain}.tag{border-radius:5px;padding:4px 6px;font-size:10px;font-weight:850;background:#e8f8f1;color:var(--green)}.tag.bad{background:#fff0f1;color:var(--red)}.tag.wait{background:#fff6e4;color:var(--amber)}.footer{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;font-size:11px;color:var(--muted);padding:2px 3px}.toast{position:fixed;right:18px;bottom:18px;background:#142944;color:#fff;padding:13px 16px;border-radius:10px;box-shadow:0 12px 35px #10203733;max-width:360px;z-index:10;display:none}.toast.show{display:block}.modalBackdrop{position:fixed;inset:0;background:#15243caa;z-index:8;display:none;place-items:center;padding:16px}.modalBackdrop.show{display:grid}.modal{background:white;color:var(--ink);padding:16px;border-radius:14px;max-width:900px;max-height:90vh;width:100%;display:grid;gap:12px}.modal img{max-width:100%;max-height:65vh;object-fit:contain;margin:auto}.modalHead{display:flex;justify-content:space-between;align-items:center;gap:10px}.smallNote{font-size:11px;color:var(--muted)}@media(max-width:1250px){.topology{grid-template-columns:1fr 24px 1fr;}.topology .arrow-origin{display:none}.topology .edgesWrap{grid-column:1/-1}.topology .originCard{grid-column:1/-1}.topology .originArrow{display:none}.metrics{grid-template-columns:repeat(3,minmax(0,1fr))}.gallery{grid-template-columns:repeat(4,minmax(0,1fr))}}@media(max-width:760px){.wrap{padding:12px}.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.topology{grid-template-columns:1fr}.flowArrow{transform:rotate(90deg);min-height:22px}.topology .edgesWrap,.topology .originCard{grid-column:auto}.topology .arrow-origin,.topology .originArrow{display:grid}.serverNodes{grid-template-columns:1fr}.gallery{grid-template-columns:repeat(2,minmax(0,1fr))}.twocol{grid-template-columns:1fr}.introText h1{font-size:21px}.thumb{height:125px}}@media(min-width:761px) and (max-width:1000px){.gallery{grid-template-columns:repeat(3,minmax(0,1fr))}}

.cacheGrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.cacheStat{border:1px solid var(--line);border-radius:10px;background:#fbfdff;padding:12px}.cacheStat .value{font-size:22px}.panelNote{font-size:11px;color:var(--muted);margin-top:10px}.controlGroup{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.controlGroup label{font-size:12px;color:var(--muted);font-weight:700}.overloadState{border:1px solid #f5c3c8;background:#fff5f5;border-radius:10px;padding:12px;font-size:12px;line-height:1.6}.overloadState.off{border-color:#bdebd8;background:#f0fbf7}routeRoll{font-size:10px;display:inline-block;background:#eef4fc;color:#334e71;border-radius:5px;padding:3px 5px}.hbText{font-size:11px;color:#e2ecf8}.node.overloaded{border-color:#e7a4ac;background:#fff7f7}.node.overloaded .nodeName{color:var(--red)}.statRow{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:12px}.statRow .cacheStat .value{font-size:20px}.chartLegend{display:flex;gap:12px;flex-wrap:wrap;font-size:11px;color:var(--muted);margin-top:5px}.chartLegend i{width:9px;height:9px;border-radius:2px;display:inline-block;margin-right:4px}
@media(max-width:1000px){.architectureDiagram{grid-template-columns:1fr 24px 1fr}.archEdges{grid-column:1/-1}.archFallback{grid-column:1/-1}.cacheGrid{grid-template-columns:repeat(2,minmax(0,1fr))}}@media(max-width:600px){.architectureDiagram{grid-template-columns:1fr}.archArrow{transform:rotate(90deg)}.cacheGrid,.statRow{grid-template-columns:1fr}}

.architectureDiagram{display:grid;grid-template-columns:minmax(170px,1fr) 32px minmax(190px,1fr) 32px minmax(280px,1.5fr);gap:10px;align-items:center}
.archBox{border:1px solid var(--line);border-radius:12px;background:#fbfdff;padding:14px;display:flex;gap:10px;align-items:flex-start;min-width:0;min-height:108px}
.archBox.routerCard{background:#edf6ff;border-color:#bcd8fc}.archIcon{width:36px;height:36px;border-radius:10px;display:grid;place-items:center;background:#e6effc;font-size:20px;flex-shrink:0}.archMeta{font-size:11px;color:var(--muted);margin-top:4px}.archArrow{font-size:25px;color:#8da3bd;text-align:center;font-weight:800}.archNumbers{display:grid;gap:3px;font-size:11px;margin-top:8px}.archNumbers span{display:flex;justify-content:space-between;gap:12px}.archGroupTitle{font-size:11px;font-weight:850;text-transform:uppercase;color:var(--muted);letter-spacing:.7px;margin-bottom:7px}.archEdges{display:grid;gap:6px}.archEdge{display:grid;grid-template-columns:9px auto 45px auto;gap:7px;align-items:center;padding:8px 9px;border:1px solid var(--line);border-radius:8px;background:#fff;font-size:11px}.archEdge small{color:var(--muted)}.archEdge span:last-child{justify-self:end;font-weight:800;color:var(--green)}.archEdgeDot{height:8px;width:8px;border-radius:50%;background:#b8c4d3}.archFallback{grid-column:1/-1;display:flex;align-items:center;justify-content:center;gap:13px;flex-wrap:wrap;background:#f0fbf7;border:1px dashed #a9dfc9;border-radius:9px;padding:10px;font-size:11px;color:#245a49}.archMiniGrid{display:grid;grid-template-columns:repeat(5,24px);gap:3px;margin-top:7px}.archMiniGrid .clientChip{padding:3px 0;font-size:8px}.node.down{border-color:#e7a4ac;background:#fff0f1}.node.down .nodeName{color:var(--red)}.node.overloaded{border-color:#e7c88d;background:#fffaf0}.node.overloaded .nodeName{color:var(--amber)}

</style>
</head>
<body>
<header><div class="brand"><div class="logo">◈</div><div><div class="title">QoS Edge CDN <span style="color:#8bbaff">/</span> Live Control Room</div><div class="sub">Multi-client simulation · Origin content · Intelligent edge routing · Cache visibility</div></div></div><div class="headRight"><span class="statuspill"><span class="dot" id="liveDot"></span><span id="liveText">Connecting to local services…</span></span><span class="statuspill">Dashboard · :9100</span><span class="statuspill"><span class="dot" id="heartbeatDot"></span><span class="hbText">Heartbeat: <b id="heartbeatAt">waiting</b> · 3 sec</span></span></div></header>
<div class="wrap">
<section class="intro"><div class="introText"><div class="eyebrow">Live network observability</div><h1>Watch requests move through your CDN</h1><p>The gallery below is discovered from the Origin Server. Click <b>Request via CDN</b> on any image to send a real request through the QoS router, or start 25 concurrent simulated clients to create live traffic.</p></div><div class="controls"><label for="clients" class="muted">Simulated clients</label><select id="clients"><option>5</option><option>10</option><option selected>25</option><option>50</option><option>75</option><option>100</option></select><button id="startBtn" onclick="startSim()">▶ Start simulation</button><button id="stopBtn" class="stop" onclick="stopSim()" disabled>■ Stop</button></div></section>
<section class="metrics"><div class="panel metric"><div class="label">Completed requests</div><div class="value" id="mCompleted">0</div><div class="hint">Simulation + gallery requests</div></div><div class="panel metric"><div class="label">Active clients</div><div class="value blue" id="mClients">0 / 25</div><div class="hint">Concurrent workers</div></div><div class="panel metric"><div class="label">Success rate</div><div class="value green" id="mSuccess">—</div><div class="hint">Successful HTTP responses</div></div><div class="panel metric"><div class="label">Mean latency</div><div class="value" id="mMean">—</div><div class="hint">End-to-end request time</div></div><div class="panel metric"><div class="label">p95 latency</div><div class="value amber" id="mP95">—</div><div class="hint">95% of recent requests at/below</div></div><div class="panel metric"><div class="label">Transferred data</div><div class="value" id="mBytes">0 B</div><div class="hint">Response payload bytes</div></div></section>
<section class="panel topologyPanel"><div class="sectionhead"><div><div class="sectionTitle">CDN architecture</div><div class="sectionSub">HTTP request path: client → QoS router → selected edge. Origin is contacted on cache miss or when no edge can serve.</div></div><span class="pill wait" id="systemPill">Waiting for telemetry</span></div>
 <div class="architectureDiagram">
  <div class="archBox clientsBox"><div class="archIcon">👥</div><div><b>Clients</b><div class="archMeta"><span id="clientFlowCount">25</span> configured · <span id="archActiveClients">0</span> active</div><div class="archMiniGrid" id="clientChips"></div></div></div>
  <div class="archArrow">→</div>
  <div class="archBox routerCard"><div class="archIcon">⇄</div><div><b>QoS Router</b><div class="archMeta">HTTP · port 9000</div><div class="archNumbers"><span>Routed <b id="rRequests">0</b></span><span>Fallbacks <b id="rFallbacks">0</b></span><span>Errors <b id="rFailures">0</b></span></div></div></div>
  <div class="archArrow">→</div>
  <div class="archEdgeGroup"><div class="archGroupTitle">Edge cache pool</div><div class="archEdges">
   <div class="archEdge"><span class="archEdgeDot" id="archDot-edge1"></span><b>Edge 1</b><small>:8001</small><span id="archStatus-edge1">checking</span></div>
   <div class="archEdge"><span class="archEdgeDot" id="archDot-edge2"></span><b>Edge 2</b><small>:8002</small><span id="archStatus-edge2">checking</span></div>
   <div class="archEdge"><span class="archEdgeDot" id="archDot-edge3"></span><b>Edge 3</b><small>:8003</small><span id="archStatus-edge3">checking</span></div>
  </div></div>
  <div class="archFallback"><span>↘ Cache miss / all edges unavailable</span><b>Origin Server · HTTP :5000</b><span id="originPill" class="pill wait">Checking origin</span><span><b id="originImages">—</b> images · <b id="originAssets">—</b> files</span></div>
 </div>
 <div id="routerState" class="smallNote" style="margin-top:12px">Waiting for router health…</div>
</section>
<section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Edge health and load</div><div class="sectionSub">3-second heartbeat: availability, latency, request load, cache inventory, overload and simulated outage.</div></div><span class="pill" id="edgeHeartbeatStatus">Heartbeat pending</span></div><div class="serverNodes" id="nodes"><div class="node">Loading edge nodes…</div></div></section>
<section class="panel contentPanel"><div class="catalogTop"><div><div class="sectionTitle">Origin content library</div><div class="sectionSub">Actual files discovered from the origin's <code>/files</code> endpoint. Images appear as thumbnails.</div></div><div class="catalogFilters"><input id="searchAssets" type="search" placeholder="Search images or files…" oninput="renderGallery()"><select id="typeFilter" onchange="renderGallery()"><option value="all">All content</option><option value="image">Images only</option><option value="file">Other files</option></select><button class="secondary small" onclick="loadContent()">↻ Refresh library</button></div></div><div class="smallNote" id="catalogStatus" style="margin-bottom:12px">Loading content from origin…</div><div id="gallery" class="gallery"><div class="emptyState">Connecting to origin and reading the content catalog…</div></div><div class="smallNote" style="margin-top:12px">Gallery thumbnails are direct origin previews so you can see the content. Click <b>Request via CDN</b> to send the selected file through the router and update request metrics/cache state.</div></section>
<div class="twocol"><section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Request latency trend</div><div class="sectionSub">Recent completed requests from the live feed.</div></div></div><div class="chartbox"><canvas id="latencyChart"></canvas></div><div class="legend"><span><i style="background:var(--blue)"></i>End-to-end latency (ms)</span><span id="trendCount">0 samples</span></div></section><section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Requests handled by each edge</div><div class="sectionSub">Router's cumulative routing counters since startup.</div></div></div><div id="routeBars" class="routeBars"><div class="muted">Waiting for router metrics…</div></div><div style="border-top:1px solid var(--line);margin-top:18px;padding-top:13px;display:grid;grid-template-columns:1fr 1fr;gap:12px"><div><div class="label">Router bytes returned</div><div class="value" style="font-size:20px" id="rBytes">0 B</div></div><div><div class="label">Origin asset count</div><div class="value" style="font-size:20px" id="contentCount">0</div></div></div></section></div>
 <div class="twocol">
 <section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Caching panel</div><div class="sectionSub">Aggregated from the three edge servers' real cache metrics, refreshed every 3 seconds.</div></div><span class="pill" id="cacheHeartbeatPill">Waiting for heartbeat</span></div>
 <div class="cacheGrid"><div class="cacheStat"><div class="label">Cache hit ratio</div><div class="value green" id="cacheHitRatio">0%</div><div class="hint">Hits / (hits + misses)</div></div><div class="cacheStat"><div class="label">Cache hits</div><div class="value" id="cacheHits">0</div><div class="hint">Served from edge storage</div></div><div class="cacheStat"><div class="label">Cache misses</div><div class="value amber" id="cacheMisses">0</div><div class="hint">Caused an origin fetch</div></div><div class="cacheStat"><div class="label">Cached objects</div><div class="value" id="cacheObjects">0</div><div class="hint">Sum across all edges</div></div></div>
 <div class="statRow"><div class="cacheStat"><div class="label">Cache storage used</div><div class="value" id="cacheBytes">0 B</div></div><div class="cacheStat"><div class="label">LRU evictions</div><div class="value" id="cacheEvictions">0</div></div><div class="cacheStat"><div class="label">Edge origin fetches</div><div class="value" id="cacheOriginFetches">0</div></div></div>
 <div class="panelNote">A cache hit avoids another origin request. A cache miss triggers the edge's HTTP fetch from the origin. Metrics count the actual counters reported by edge nodes.</div></section>
 <section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Origin server traffic</div><div class="sectionSub">Authoritative origin-side HTTP content GET counters from origin_server.py. Cache hits stay at the edge and do not reach the origin.</div></div><span class="pill" id="originTrafficPill">Counting HTTP traffic</span></div>
 <div class="cacheGrid"><div class="cacheStat"><div class="label">Requests to origin</div><div class="value blue" id="originRequests">0</div></div><div class="cacheStat"><div class="label">HTTP responses received</div><div class="value green" id="originResponses">0</div></div><div class="cacheStat"><div class="label">Origin errors</div><div class="value red" id="originErrors">0</div></div><div class="cacheStat"><div class="label">Last 3 sec requests</div><div class="value" id="originInterval">0</div></div></div>
 <div class="statRow"><div class="cacheStat"><div class="label">Origin bytes served</div><div class="value" id="originBytes">0 B</div></div><div class="cacheStat"><div class="label">Catalog requests</div><div class="value" id="originCatalogRequests">0</div></div><div class="cacheStat"><div class="label">Edge cache-miss fetches</div><div class="value" id="edgeMissFetches">0</div></div></div><div class="chartbox" style="margin-top:10px"><canvas id="originChart"></canvas></div><div class="chartLegend"><span><i style="background:#2563eb"></i>Requests to origin (per 3 sec)</span><span><i style="background:#07845e"></i>Responses received (per 3 sec)</span></div><div class="panelNote">The origin exposes a read-only /metrics endpoint. It counts actual content GETs, responses and bytes at the main server; catalog-list calls are tracked separately. If content counters remain zero while cache hits rise, the edges are serving cached files and no origin fetch is required.</div></section></div>
 <section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Runtime overload and QoS redirection</div><div class="sectionSub">Start bounded synthetic HTTP load against one edge. The router reduces that edge’s selection weight so most new routed requests shift to healthy peers while the target remains available.</div></div><span class="pill wait" id="overloadPill">No overload active</span></div>
 <div class="controls controlGroup"><label for="overloadTarget">Target edge</label><select id="overloadTarget"><option value="edge1">Edge 1 · :8001</option><option value="edge2">Edge 2 · :8002</option><option value="edge3">Edge 3 · :8003</option></select><button id="overloadStartBtn" onclick="startOverloadUi()">⚠ Overload selected edge</button><button id="overloadStopBtn" class="secondary" onclick="stopOverloadUi()" disabled>Stop overload</button></div>
 <div id="overloadState" class="overloadState off" style="margin-top:12px">No overload simulation is active. Normal QoS routing is enabled.</div>
 <div class="statRow"><div class="cacheStat"><div class="label">Synthetic load requests</div><div class="value" id="overloadGenerated">0</div></div><div class="cacheStat"><div class="label">Synthetic load in flight</div><div class="value" id="overloadInflight">0</div></div><div class="cacheStat"><div class="label">Requests redirected</div><div class="value amber" id="redirectedCount">0</div></div></div>
 <div class="sectionSub" style="margin-top:12px">Each client request uses <code>randint(0, 3)</code>: 0 = automatic QoS; 1–3 = preferred edge. During overload, weighted steering targets roughly 13% of routed attempts to the degraded edge and 43.5% to each healthy peer if all three are available. Actual counts depend on how much traffic runs.</div><div id="assignmentBars" class="routeBars" style="margin-top:12px"><div class="muted">Waiting for random assignment samples…</div></div>
 <div class="trafficWindow" style="margin-top:16px;border-top:1px solid var(--line);padding-top:13px"><div class="sectionhead"><div><div class="sectionTitle">Requests during this overload test</div><div class="sectionSub">Windowed router-routed attempt counts since overload started, not totals from before the experiment.</div></div><b id="overloadWindowTotal">0 total</b></div><div id="overloadTrafficBars" class="routeBars"><div class="muted">Start overload to collect window counts.</div></div><div class="panelNote">For approximately 3,000 router-routed attempts, the target overloaded edge should receive about 390 (13%) and the two healthy peers about 1,305 each (43.5%). This is an expected weighted distribution, not a hard-coded count; allow enough time for clients to generate the traffic.</div></div>
 </section>
 <section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Simulate an edge outage</div><div class="sectionSub">Temporarily stop routing to the selected edge and show failover. This is a router-level outage simulation; it does not kill the Python edge process.</div></div><span class="pill" id="failurePill">No outage active</span></div>
 <div class="controls controlGroup"><label for="failureTarget">Target edge</label><select id="failureTarget"><option value="edge1">Edge 1 · :8001</option><option value="edge2">Edge 2 · :8002</option><option value="edge3">Edge 3 · :8003</option></select><button id="failureStartBtn" class="stop" onclick="startFailureUi()">⚡ Simulate edge crash</button><button id="failureStopBtn" class="secondary" onclick="stopFailureUi()" disabled>Restore edge</button></div>
 <div id="failureState" class="overloadState off" style="margin-top:12px">No simulated outage active. All healthy edges are eligible for routing.</div>
 </section>
 <section class="panel"><div class="sectionhead"><div><div class="sectionTitle">Live request feed</div><div class="sectionSub">A row appears whenever a simulated client or gallery button requests content.</div></div><span class="pill" id="feedCount">0 events</span></div><div class="tablewrap"><table><thead><tr><th>Time</th><th>Client</th><th>Content</th><th>Requested route</th><th>Selected edge</th><th>Status</th><th>Latency</th><th>Cache</th><th>Bytes</th></tr></thead><tbody id="feed"><tr><td colspan="9" class="muted">Start the simulation or request an image from the content library.</td></tr></tbody></table></div></section>

<div class="footer"><span>Local demo · Dashboard :9100 · Router :9000 · Edges :8001 / :8002 / :8003 · Origin :5000</span><span>25 simulated clients = concurrent workers on one laptop, not 25 separate devices.</span></div>
</div>
<div id="toast" class="toast"></div><div id="modalBackdrop" class="modalBackdrop" onclick="closeModal(event)"><div class="modal"><div class="modalHead"><b id="modalTitle">Image preview</b><button class="secondary small" onclick="closeModal()">Close</button></div><img id="modalImage" alt="Selected origin image"><div id="modalCaption" class="smallNote"></div></div></div>
<script>
const $=id=>document.getElementById(id);let history=[],previousCompleted=0,contentAssets=[],lastKnownCount=0;const colors={edge1:'#0891b2',edge2:'#2563eb',edge3:'#07845e','origin-fallback':'#b76e09',unknown:'#91a4bd'};
function fmtBytes(n){n=Number(n||0);if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(2)+' MB'}function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}function isImageName(n){return /\.(jpg|jpeg|png|gif|webp|bmp|avif)$/i.test(n)}function toast(msg){const el=$('toast');el.textContent=msg;el.classList.add('show');setTimeout(()=>el.classList.remove('show'),2800)}
async function post(path,body){let r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});let j=await r.json();if(!r.ok)throw new Error(j.message||'Request failed');return j}async function startSim(){try{await post('/api/start',{clients:Number($('clients').value)});await refresh()}catch(e){toast(e.message)}}async function stopSim(){try{await post('/api/stop');await refresh()}catch(e){toast(e.message)}}
async function loadContent(){const status=$('catalogStatus');status.textContent='Reading the origin content catalog…';try{const r=await fetch('/api/content',{cache:'no-store'});const data=await r.json();contentAssets=data.assets||[];lastKnownCount=contentAssets.length;status.textContent=data.ok?`${data.image_count} images · ${data.count} total files · Source: ${data.origin_url}/files`:(data.message||'No content found.');$('contentCount').textContent=data.count||0;$('originImages').textContent=data.image_count??0;$('originAssets').textContent=data.count??0;renderGallery();}catch(e){status.textContent='Could not load origin content. Confirm the origin server is running on port 5000.';$('gallery').innerHTML='<div class="emptyState">Origin content is unavailable. Start <code>origin_server.py</code>, then click Refresh library.</div>'}}
function renderGallery(){const q=$('searchAssets').value.trim().toLowerCase(),filter=$('typeFilter').value;const filtered=contentAssets.filter(a=>(!q||a.name.toLowerCase().includes(q))&&(filter==='all'||a.kind===filter));$('gallery').innerHTML=filtered.length?filtered.map(a=>{const image=a.kind==='image';const thumb=image?`<img loading="lazy" src="${esc(a.preview_url)}" alt="${esc(a.name)}" onerror="this.style.display='none';this.nextElementSibling.style.display='grid'"><span class="fileGlyph" style="display:none">▧</span>`:`<span class="fileGlyph">${/\.(pptx?|pdf)$/i.test(a.name)?'▤':'▧'}</span>`;return `<article class="assetCard"><div class="thumb">${thumb}<span class="typeBadge">${image?'IMAGE':(a.name.split('.').pop()||'FILE').toUpperCase()}</span></div><div class="assetInfo"><div class="assetName" title="${esc(a.name)}">${esc(a.name)}</div><div class="assetNote">${image?'Origin image preview':'Origin file'}</div><div class="fetchState" id="assetState-${encodeURIComponent(a.name)}"></div><div class="assetAction"><button onclick="accessAsset(${JSON.stringify(a.name).replace(/</g,'\\u003c')},${image})">↗ Request via CDN</button></div></div></article>`}).join(''):'<div class="emptyState">No matching files. Try another search or change the content filter.</div>'}
async function accessAsset(filename,image){const stateEl=$('assetState-'+encodeURIComponent(filename));if(stateEl)stateEl.textContent='Requesting through router…';try{const r=await fetch('/api/access/'+encodeURIComponent(filename),{cache:'no-store'});const blob=await r.blob();const edge=r.headers.get('X-Routed-Edge')||'unknown';const cache=r.headers.get('X-Cache')||'not reported';const latency=r.headers.get('X-Dashboard-Latency-Ms')||'—';if(!r.ok){let msg='HTTP '+r.status;try{const j=JSON.parse(await blob.text());msg=j.error||msg}catch{}if(stateEl)stateEl.textContent=msg;toast(filename+': '+msg);await refresh();return}if(stateEl)stateEl.textContent=`✓ ${edge} · ${latency} ms · ${cache}`;toast(`${filename} served by ${edge} in ${latency} ms (${cache})`);if(image){const url=URL.createObjectURL(blob);$('modalTitle').textContent=filename;$('modalImage').src=url;$('modalCaption').textContent=`Delivered through the QoS router · ${edge} · ${latency} ms · ${fmtBytes(blob.size)} · Cache: ${cache}`;$('modalBackdrop').classList.add('show');$('modalImage').dataset.blobUrl=url}else{const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=filename;document.body.appendChild(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),2000)}await refresh()}catch(e){if(stateEl)stateEl.textContent='Request failed';toast('CDN request failed: '+e.message)}}
function closeModal(event){if(event&&event.target!==$('modalBackdrop')&&event.target!==$('modalBackdrop').firstElementChild)return;const old=$('modalImage').dataset.blobUrl;if(old)URL.revokeObjectURL(old);$('modalImage').removeAttribute('src');$('modalBackdrop').classList.remove('show')}
function renderNodes(health,metrics,details){const edges=(health&&health.edges)||{},me=(metrics&&metrics.edges)||{};details=details||{};$('nodes').innerHTML=['edge1','edge2','edge3'].map(id=>{const e=edges[id]||{},m=me[id]||{},d=details[id]||{},down=!!(e.simulated_down||m.simulated_down),overloaded=!!(e.simulated_overloaded||m.simulated_overloaded),ok=!down&&(e.healthy===true||d.status==='ok'),load=Number(e.active_requests??m.active_requests??0)+Number(d.dashboard_overload_inflight||0),lat=e.ewma_latency_ms??e.probe_latency_ms,files=d.cached_files||[],cache=Math.max(e.cached_file_count??m.cached_file_count??0,files.length),status=down?'SIMULATED DOWN':(overloaded?'OVERLOADED · LOW WEIGHT':(ok?'HEALTHY':'UNREACHABLE'));$('archDot-'+id).style.background=down?'#c9364e':overloaded?'#b76e09':ok?'#07845e':'#a3afbf';$('archStatus-'+id).textContent=down?'DOWN':overloaded?'DEGRADED':ok?'UP':'UNREACHABLE';$('archStatus-'+id).style.color=down?'#c9364e':overloaded?'#b76e09':ok?'#07845e':'#c9364e';return `<div class="node ${down?'down':(overloaded?'overloaded':'')}"><div class="nodeHead"><div><div class="nodeName">${id.toUpperCase()}</div><div class="nodePort">127.0.0.1:${id==='edge1'?8001:id==='edge2'?8002:8003}</div></div><span class="pill ${down?'down':(overloaded?'wait':(ok?'':'down'))}">${status}</span></div><div class="nodeStats"><span>Probe latency <b>${lat==null?'—':Number(lat).toFixed(1)+' ms'}</b></span><span>Routed in-flight <b>${Number(e.active_requests??m.active_requests??0)}</b></span><span>Direct load workers <b>${Number(d.dashboard_overload_inflight||0)}</b></span><span>Routed requests <b>${Number(m.requests_routed||0).toLocaleString()}</b></span><span>Cache objects <b>${cache}</b></span><span>Failure streak <b>${e.failure_streak??0}</b></span></div><div class="cachebar"><span style="width:${Math.min(100,cache*4)}%"></span></div><div class="cachedNames"><b>Cached files:</b> ${files.length?files.slice(0,4).map(esc).join(', ')+(files.length>4?` +${files.length-4} more`:''):'No files cached yet'}</div></div>`}).join('')}

function renderBars(metrics){const es=(metrics&&metrics.edges)||{},vals=['edge1','edge2','edge3'].map(id=>[id,Number((es[id]||{}).requests_routed||0)]),total=vals.reduce((a,x)=>a+x[1],0),max=Math.max(1,...vals.map(x=>x[1]));$('routeBars').innerHTML=vals.map(([id,n])=>`<div><div class="routeLabel"><span style="color:${colors[id]};font-weight:800">${id.toUpperCase()}</span><b>${n.toLocaleString()} requests · ${total?Math.round(n*100/total):0}%</b></div><div class="cachebar" style="height:9px"><span style="width:${n/max*100}%;background:${colors[id]}"></span></div></div>`).join('')}
function renderOverloadWindow(counts){counts=counts||{};const rows=['edge1','edge2','edge3'].map(id=>[id,Number(counts[id]||0)]),total=rows.reduce((a,x)=>a+x[1],0);$('overloadWindowTotal').textContent=total.toLocaleString()+' total';$('overloadTrafficBars').innerHTML=rows.map(([id,n])=>`<div><div class="routeLabel"><span style="color:${colors[id]};font-weight:800">${id.toUpperCase()}</span><b>${n.toLocaleString()} · ${total?(n*100/total).toFixed(1):'0.0'}%</b></div><div class="cachebar" style="height:9px"><span style="width:${total?n*100/total:0}%;background:${colors[id]}"></span></div></div>`).join('')}

function renderFeed(items){$('feedCount').textContent=items.length+' recent events';$('feed').innerHTML=items.length?items.map(e=>{const ok=Number(e.status)>=200&&Number(e.status)<300,system=['STARTED','STOPPED','OVERLOAD_ON','OVERLOAD_OFF'].includes(String(e.status)),img=isImageName(e.filename);const thumb=img?'<span aria-hidden="true">🖼️</span>':'<span aria-hidden="true">📄</span>';const route=e.requested_edge&&e.requested_edge!=='—'?e.requested_edge:'—';const routeText=e.redirected_from?`${route} → redirect`:`${route}${e.selection_roll!==undefined&&e.selection_roll!==null?' (r'+e.selection_roll+')':''}`;return `<tr><td class="muted">${esc(e.time)}</td><td>${esc(e.client)}</td><td><div class="contentCell">${thumb}<span>${esc(e.filename)}</span></div></td><td><span class="routeRoll">${esc(routeText)}</span>${e.redirected_from?`<div class="smallNote">from ${esc(e.redirected_from)} · ${esc(e.redirect_reason||'failover')}</div>`:''}</td><td><b style="color:${colors[e.edge]||'#17243a'}">${esc(e.edge)}</b></td><td><span class="tag ${ok||system?'':(e.status==='WAITING'?'wait':'bad')}">${esc(e.status)}</span></td><td>${Number(e.latency_ms||0).toFixed(1)} ms</td><td>${esc(e.cache||'—')}</td><td>${fmtBytes(Number(e.bytes||0))}</td></tr>`}).join(''):'<tr><td colspan="9" class="muted">No requests yet. Start the simulation or request content from the gallery.</td></tr>'}
function drawChart(){const c=$('latencyChart'),ctx=c.getContext('2d'),dpr=window.devicePixelRatio||1,w=c.clientWidth,h=c.clientHeight;if(!w||!h)return;c.width=w*dpr;c.height=h*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);const vals=history.slice(-60),max=Math.max(50,...vals)*1.15;ctx.font='10px Segoe UI';ctx.strokeStyle='#e2e9f2';ctx.fillStyle='#687992';ctx.lineWidth=1;for(let i=0;i<5;i++){const y=10+(h-30)*i/4;ctx.beginPath();ctx.moveTo(34,y);ctx.lineTo(w-8,y);ctx.stroke();ctx.fillText(Math.round(max*(1-i/4)),2,y+3)}if(vals.length<2){ctx.fillText('Waiting for request samples…',40,h/2);return}ctx.beginPath();vals.forEach((v,i)=>{const x=34+(w-44)*i/(vals.length-1),y=10+(h-30)*(1-v/max);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)});ctx.strokeStyle='#2563eb';ctx.lineWidth=2;ctx.stroke()}
function drawOriginChart(){const c=$('originChart');if(!c)return;const ctx=c.getContext('2d'),dpr=window.devicePixelRatio||1,w=c.clientWidth,h=c.clientHeight;if(!w||!h)return;c.width=w*dpr;c.height=h*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,w,h);const rows=(window.originHistory||[]).slice(-30);const max=Math.max(1,...rows.map(x=>Math.max(Number(x.requests||0),Number(x.responses||0))));ctx.font='10px Segoe UI';ctx.strokeStyle='#e2e9f2';ctx.fillStyle='#687992';for(let i=0;i<4;i++){const y=10+(h-30)*i/3;ctx.beginPath();ctx.moveTo(28,y);ctx.lineTo(w-8,y);ctx.stroke();ctx.fillText(String(Math.round(max*(1-i/3))),2,y+3)}if(!rows.length||rows.every(x=>!x.requests&&!x.responses)){ctx.fillText('Waiting for origin fetch samples…',36,h/2);return}const chartW=w-42,step=chartW/Math.max(1,rows.length),bw=Math.max(2,Math.min(9,step*.28));rows.forEach((v,i)=>{const x=32+i*step+step*.12,rh=(h-30)*Number(v.requests||0)/max,sh=(h-30)*Number(v.responses||0)/max;ctx.fillStyle='#2563eb';ctx.fillRect(x,h-20-rh,bw,rh);ctx.fillStyle='#07845e';ctx.fillRect(x+bw+2,h-20-sh,bw,sh)});}
async function startOverloadUi(){try{const edge=$('overloadTarget').value;const result=await post('/api/overload',{edge_id:edge,enabled:true});toast(result.message);await refresh()}catch(e){toast(e.message)}}async function stopOverloadUi(){try{const result=await post('/api/overload',{enabled:false});toast(result.message);await refresh()}catch(e){toast(e.message)}}async function startFailureUi(){try{const edge=$('failureTarget').value;const result=await post('/api/failure',{edge_id:edge,enabled:true});toast(result.message);await refresh()}catch(e){toast(e.message)}}async function stopFailureUi(){try{const result=await post('/api/failure',{enabled:false});toast(result.message);await refresh()}catch(e){toast(e.message)}}
async function refresh(){try{const r=await fetch('/api/state',{cache:'no-store'}),s=await r.json(),rh=s.router_health||{},rm=s.router_metrics||{},router=rm.router||{},oh=s.origin_health||{};const availableEdges=Object.values(rh.edges||{}).filter(e=>(e.dispatch_available!==undefined?e.dispatch_available:e.healthy)).length;$('liveText').textContent=rh.status==='ok'?`Router online · ${availableEdges}/3 edges available`:'Router '+(rh.status||'unavailable');$('liveDot').style.background=rh.status==='ok'?'#17b67c':'#d94a5c';$('mCompleted').textContent=Number(s.completed||0).toLocaleString();$('mClients').textContent=(s.running?s.active_clients:0)+' / '+s.requested_clients;$('mSuccess').textContent=s.completed?s.success_rate+'%':'—';$('mMean').textContent=s.completed?s.average_latency_ms+' ms':'—';$('mP95').textContent=s.completed?s.p95_latency_ms+' ms':'—';$('mBytes').textContent=fmtBytes(s.bytes);$('startBtn').disabled=s.running;$('stopBtn').disabled=!s.running;$('clients').disabled=s.running;$('clientFlowCount').textContent=s.requested_clients||25;$('archActiveClients').textContent=s.running?s.active_clients:0;$('routerState').textContent='Status: '+(rh.status||'unavailable')+' · routing policy uses health, latency, load and cache locality';$('rRequests').textContent=(router.requests||0).toLocaleString();$('rFallbacks').textContent=(router.origin_fallbacks||0).toLocaleString();$('rFailures').textContent=(router.upstream_failures||0).toLocaleString();$('rBytes').textContent=fmtBytes(router.bytes_returned||0);$('heartbeatAt').textContent=s.last_heartbeat||'waiting';$('heartbeatDot').style.background=s.last_heartbeat?'#17b67c':'#e7a43b';$('edgeHeartbeatStatus').textContent=s.last_heartbeat?'Last heartbeat '+s.last_heartbeat:'Heartbeat pending';const cm=s.cache_metrics||{};$('cacheHitRatio').textContent=(cm.hit_ratio_percent||0)+'%';$('cacheHits').textContent=Number(cm.hits||0).toLocaleString();$('cacheMisses').textContent=Number(cm.misses||0).toLocaleString();$('cacheObjects').textContent=Number(cm.cached_objects||0).toLocaleString();$('cacheBytes').textContent=fmtBytes(cm.cache_bytes||0)+' / '+fmtBytes(cm.capacity_bytes||0);$('cacheEvictions').textContent=Number(cm.evictions||0).toLocaleString();$('cacheOriginFetches').textContent=Number(cm.origin_fetches||0).toLocaleString();$('cacheHeartbeatPill').textContent='Heartbeat '+(s.last_heartbeat||'pending');const om=s.origin_metrics||{},oi=s.origin_interval||{};$('originRequests').textContent=Number(om.requests||0).toLocaleString();$('originResponses').textContent=Number(om.responses||0).toLocaleString();$('originErrors').textContent=Number(om.errors||0).toLocaleString();$('originBytes').textContent=fmtBytes(om.bytes||0);$('originCatalogRequests').textContent=Number(om.catalog_requests||0).toLocaleString();$('edgeMissFetches').textContent=Number(om.edge_cache_miss_fetches||0).toLocaleString();$('originInterval').textContent=Number(oi.requests||0).toLocaleString();$('originTrafficPill').textContent=(s.origin_metrics_source||'origin counters')+' · '+(s.last_heartbeat||'pending');window.originHistory=s.origin_history||[];drawOriginChart();$('overloadGenerated').textContent=Number(s.overload_generated||0).toLocaleString();$('overloadInflight').textContent=Object.values(s.overload_inflight||{}).reduce((a,b)=>a+Number(b||0),0);$('redirectedCount').textContent=Number(s.redirected_requests||0).toLocaleString();const activeOverload=s.overload_edge;$('overloadStartBtn').disabled=!!activeOverload||!!s.failed_edge;$('overloadStopBtn').disabled=!activeOverload;$('overloadTarget').disabled=!!activeOverload||!!s.failed_edge;$('overloadPill').className='pill '+(activeOverload?'wait':'');$('overloadPill').textContent=activeOverload?'DEGRADED '+activeOverload.toUpperCase():'No overload active';$('overloadState').className='overloadState '+(activeOverload?'':'off');$('overloadState').textContent=activeOverload?`${activeOverload.toUpperCase()} is under bounded synthetic HTTP load and remains eligible at 0.30x routing weight. Healthy peers get most new routed requests. ${Number(s.redirected_requests||0)} preferred-edge requests have been redirected since simulation start.`:'No overload simulation is active. Traffic uses normal QoS selection. Overload uses three bounded HTTP workers and a reduced routing weight; it does not stop the edge.';renderOverloadWindow(s.overload_window_counts||{});const failedEdge=s.failed_edge;$('failureStartBtn').disabled=!!failedEdge||!!activeOverload;$('failureStopBtn').disabled=!failedEdge;$('failureTarget').disabled=!!failedEdge||!!activeOverload;$('failurePill').className='pill '+(failedEdge?'down':'');$('failurePill').textContent=failedEdge?'SIMULATED DOWN · '+failedEdge.toUpperCase():'No outage active';$('failureState').className='overloadState '+(failedEdge?'':'off');$('failureState').textContent=failedEdge?`${failedEdge.toUpperCase()} is excluded from router dispatch. The actual edge process remains running for safe testing; incoming requests are redirected to the healthy peers or origin fallback.`:'No simulated outage active. Choose an edge and click Simulate edge crash to test failover; click Restore edge to return it to service.';const assignments=s.assignment_counts||{};$('assignmentBars').innerHTML=['auto','edge1','edge2','edge3'].map(k=>{const n=Number(assignments[k]||0),tot=Object.values(assignments).reduce((a,b)=>a+Number(b||0),0);return `<div><div class="routeLabel"><span>${k==='auto'?'Auto QoS':k.toUpperCase()}</span><b>${n} (${tot?Math.round(n*100/tot):0}%)</b></div><div class="cachebar" style="height:8px"><span style="width:${tot?n*100/tot:0}%;background:${colors[k]||'#64748b'}"></span></div></div>`}).join('');$('contentCount').textContent=s.content_count??lastKnownCount;$('originImages').textContent=s.image_count??0;$('originAssets').textContent=s.content_count??lastKnownCount;const originOk=oh.status==='ok';$('originPill').className='pill '+(originOk?'':'down');$('originPill').textContent=originOk?`● ONLINE · ${oh.asset_count??lastKnownCount} files`:'● ORIGIN UNAVAILABLE';const healthy=Object.values(rh.edges||{}).filter(e=>(e.dispatch_available!==undefined?e.dispatch_available:e.healthy)).length;$('systemPill').className='pill '+(healthy===3&&originOk?'':'wait');$('systemPill').textContent=healthy===3&&originOk?'Origin + 3 edges dispatchable':`${healthy}/3 edges dispatchable`;
renderNodes(rh,rm,s.edge_details||{});renderBars(rm);renderFeed(s.events||[]);$('clientChips').innerHTML=Array.from({length:Math.min(25,s.requested_clients||25)},(_,i)=>`<span class="clientChip ${s.running?'busy':''}">C${String(i+1).padStart(2,'0')}</span>`).join('')+(s.requested_clients>25?`<span class="clientChip">+${s.requested_clients-25}</span>`:'');if(s.completed<previousCompleted){history=[];previousCompleted=0;drawChart()}const delta=s.completed-previousCompleted;if(delta>0){const newEvents=(s.events||[]).filter(e=>e.client&&((e.client.startsWith('Client'))||e.client==='Gallery')&&typeof e.latency_ms==='number');newEvents.slice(0,Math.min(delta,newEvents.length)).reverse().forEach(e=>history.push(Number(e.latency_ms)||0));history=history.slice(-500);previousCompleted=s.completed;$('trendCount').textContent=history.length+' samples';drawChart()}}catch(e){$('liveText').textContent='Dashboard telemetry unavailable';$('liveDot').style.background='#d94a5c'}}
loadContent();refresh();setInterval(refresh,1400);window.addEventListener('resize',drawChart);document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal()});
</script>
</body></html>'''

# Start the 3-second collector once per dashboard process. It does not modify the origin or edge code.
threading.Thread(target=heartbeat_loop, name="cdn-heartbeat", daemon=True).start()
atexit.register(stop_overload)
atexit.register(stop_failure)


if __name__ == "__main__":
    print(f"[DASHBOARD] Open http://127.0.0.1:{DASHBOARD_PORT}")
    print(f"[DASHBOARD] Router URL: {ROUTER_URL}")
    print(f"[DASHBOARD] Origin URL: {ORIGIN_URL}")
    print("[DASHBOARD] Heartbeat interval: 3 seconds; HTTP-only local lab")
    app.run(host="127.0.0.1", port=DASHBOARD_PORT, debug=False, threaded=True)
