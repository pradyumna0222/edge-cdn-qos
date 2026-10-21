# Live Multi-Client QoS Edge CDN Demo

This adds a browser dashboard and concurrent simulated clients to the existing v1 project. It does not replace `origin_server.py`, `edge_node.py`, or `qos_router.py`.

## What it does
- Starts with 25 concurrent simulated clients (adjustable to 5, 10, 25, 50, 75, or 100 in the dashboard).
- Each client repeatedly requests a file through `http://127.0.0.1:9000`.
- Shows recent requests, selected edge, status, latency, cache header when available, payload bytes, edge health, active requests, router counters, and latency trend.
- Dashboard API runs on `http://127.0.0.1:9100`.

## Windows setup
1. Extract the project ZIP to a new folder such as `C:\Users\Pradyumna\Desktop\qos_edge_cdn_v1`.
2. Put your existing content files in the `cdn_images` folder. The bundled `Edge_CDN_Presentation.pptx` is enough for a first smoke test; adding the 1.jpg–21.jpg assets gives more realistic content variety.
3. Ensure Python is installed and `python` works in PowerShell.
4. Double-click `START_LIVE_DEMO.bat`. It installs requirements, opens the origin, three edge servers, router, and dashboard in separate windows, then opens the browser dashboard.
5. If Windows Firewall asks about Python, allow private-network access only if needed. For this one-laptop demo, use localhost.
6. On the dashboard, select 25 clients and click **Start simulation**. Watch the live request feed and edge counters. Click **Stop** to end the simulation.

## Manual start (if the batch file is blocked)
Open six PowerShell terminals in the project folder and run one command per terminal:

```powershell
python -m pip install -r requirements.txt
python .\origin_server.py
python .\edge_node.py --id edge1
python .\edge_node.py --id edge2
python .\edge_node.py --id edge3
python .\qos_router.py
python .\live_dashboard.py
```

Open `http://127.0.0.1:9100` in your browser. The commands after installation must each run in their own terminal.

## API endpoints
- `GET /api/state` — dashboard telemetry and recent event feed.
- `POST /api/start` JSON `{"clients":25}` — start simulated clients.
- `POST /api/stop` — stop simulation.

## Notes / interpretation
- This is a local demonstration. The 25 simulated clients are concurrent request loops on one machine, not 25 independent network devices.
- Dashboard metrics are collected from the simulator and the existing router endpoints. Cache hit/miss truth is available in edge health counters; the per-request cache column displays `X-Cache` only if the edge sends that header.
- Request routing uses the existing v1 QoS router policy. The dashboard visualizes its behavior; it does not claim a research improvement by itself.
- Keep the simulation at 25 first. Increase client count gradually and watch failures and p95 latency before using larger loads.
