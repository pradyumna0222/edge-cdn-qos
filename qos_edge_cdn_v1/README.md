# QoS-Aware Edge CDN — v1 foundation

This is a clean, portable foundation for the existing three-edge CDN. It keeps the original idea of three separate edge nodes but removes hardcoded LAN addresses and uses a portable asset path. Run this version in a **separate folder first**; keep the original project unchanged until the smoke test passes.

## What v1 includes

- Origin server with a configurable `cdn_images` directory and health/file-list endpoints.
- Three independent edge processes, each with its own disk cache.
- Per-edge cache hit/miss and origin-fetch metrics.
- Bounded disk cache with least-recently-used (LRU) eviction.
- A router/gateway that chooses a healthy edge using measured latency, active-request load, and cached-object locality.
- Retry of unavailable edges and direct-origin fallback if all edges fail.
- A benchmark that records p50/p95-ready request samples, response status, cache result, routed edge, payload size, SHA-256, and throughput.

The current routing and caching policy is a baseline, not a claim of research novelty. Next iterations can replace its simple score with the proposed joint edge-selection/cache-placement algorithm.

## 1. Install

Open PowerShell in this folder:

```powershell
py -m pip install -r requirements.txt
```

If `py` is not available but `python` works, use `python -m pip install -r requirements.txt`.

## 2. Add your images

Copy `1.jpg` through `21.jpg`, `thankyou.jpg`, and any other demo assets directly into `cdn_images`. The included `Edge_CDN_Presentation.pptx` is a ready-made smoke-test asset.

Default data path: this project's `cdn_images` folder. If your assets live elsewhere, set `CDN_DATA_DIR` to that **asset folder's absolute path before starting the origin**, for example:

```powershell
$env:CDN_DATA_DIR = "C:\Users\YourName\Desktop\CDN_Project\cdn_images"
python .\origin_server.py
```

Set the same variable in a different origin terminal only if that terminal needs the same custom path. The edges and router call the origin over `http://127.0.0.1:5000` by default.

## 3. Start the five services

Stop the old servers first so the ports are free. Start each command in its own PowerShell terminal, with the working directory set to this folder.

Terminal 1 — origin, port 5000:

```powershell
python .\origin_server.py
```

Terminal 2 — Edge 1, port 8001:

```powershell
python .\edge_node.py --id edge1
```

Terminal 3 — Edge 2, port 8002:

```powershell
python .\edge_node.py --id edge2
```

Terminal 4 — Edge 3, port 8003:

```powershell
python .\edge_node.py --id edge3
```

Terminal 5 — QoS router, port 9000:

```powershell
python .\qos_router.py
```

Do not also start the old `app.py` or `edge_server\edge1.py` / `edge2.py` / `edge3.py` on the same ports while this version is running.

## 4. Verify in PowerShell

Check origin health:

```powershell
Invoke-RestMethod http://127.0.0.1:5000/health
```

Check router and all three edge statuses:

```powershell
Invoke-RestMethod http://127.0.0.1:9000/health
```

Download the included smoke-test asset through the router:

```powershell
Invoke-WebRequest http://127.0.0.1:9000/files/Edge_CDN_Presentation.pptx -OutFile .\smoke-test.pptx
Get-Item .\smoke-test.pptx | Select-Object Name, Length
```

Run the same download again. The first request to a particular edge should show `X-Cache: MISS`; a repeated request routed to an edge holding the object should show `X-Cache: HIT`. The router may choose a different warm edge depending on current health/latency and cache inventory.

Inspect metrics:

```powershell
Invoke-RestMethod http://127.0.0.1:9000/metrics | ConvertTo-Json -Depth 6
Invoke-RestMethod http://127.0.0.1:8001/metrics | ConvertTo-Json -Depth 6
```

## 5. Run a small benchmark first

Use a file that exists in `cdn_images`. The included presentation works immediately:

```powershell
python .\qos_benchmark.py --base-url http://127.0.0.1:9000 --files Edge_CDN_Presentation.pptx --runs 5 --output .\smoke-benchmark.csv
```

After confirming that returns 200 on all runs, benchmark the image set:

```powershell
python .\qos_benchmark.py --base-url http://127.0.0.1:9000 --files 1.jpg 2.jpg 3.jpg 4.jpg 5.jpg 6.jpg 7.jpg 8.jpg 9.jpg 10.jpg 11.jpg 12.jpg 13.jpg 14.jpg 15.jpg 16.jpg 17.jpg 18.jpg 19.jpg 20.jpg 21.jpg --runs 10 --output .\qos-benchmark.csv
```

Only include filenames that really exist in the asset directory; 404s are expected for missing files and should not be treated as successful benchmark samples.

## Design note

v1 uses cache-on-miss with a per-edge byte limit and LRU eviction. The router prefers a healthy edge that already holds the requested file, otherwise it scores nodes using smoothed request latency and active load. Health and cache inventory are refreshed periodically to avoid probing all edges for every single request. This is deliberately simple and measurable so it can serve as a comparison baseline for a future joint QoS routing and cache-placement policy.
