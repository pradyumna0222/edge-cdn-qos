@echo off
setlocal
cd /d "%~dp0"
echo Starting QoS Edge CDN live dashboard...
python -m pip install -r requirements.txt
start "CDN Origin :5000" /D "%~dp0" cmd /k python origin_server.py
timeout /t 2 /nobreak >nul
start "CDN Edge 1 :8001" /D "%~dp0" cmd /k python edge_node.py --id edge1
start "CDN Edge 2 :8002" /D "%~dp0" cmd /k python edge_node.py --id edge2
start "CDN Edge 3 :8003" /D "%~dp0" cmd /k python edge_node.py --id edge3
timeout /t 3 /nobreak >nul
start "CDN QoS Router :9000" /D "%~dp0" cmd /k python qos_router.py
timeout /t 3 /nobreak >nul
start "CDN Live Dashboard :9100" /D "%~dp0" cmd /k python live_dashboard.py
timeout /t 2 /nobreak >nul
start "" http://127.0.0.1:9100
echo.
echo Services launched. Keep the service windows open.
echo Dashboard: http://127.0.0.1:9100
pause
