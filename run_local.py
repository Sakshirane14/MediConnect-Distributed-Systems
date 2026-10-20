from __future__ import annotations
import os
import sys
import subprocess
import time
import signal
import urllib.request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON = os.path.join(BASE_DIR, "venv/bin/python")
if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable  # fall back to whatever interpreter is running this script

processes = []

def cleanup(sig=None, frame=None):
    print("\nShutting down all MediConnect nodes & services...")
    for p in processes:
        try:
            p.terminate()
        except Exception:
            pass
    sys.exit(0)

signal.signal(signal.SIGINT, cleanup)
signal.signal(signal.SIGTERM, cleanup)

def start_distributor(distributor_id, name, port, db_name):
    env = os.environ.copy()
    env["DISTRIBUTOR_ID"] = distributor_id
    env["DISTRIBUTOR_NAME"] = name
    env["GRPC_PORT"] = str(port)
    env["GRPC_ADDRESS"] = f"localhost:{port}"      # how Main API will reach me
    env["MAIN_API_URL"] = "http://localhost:8000"  # where I self-register
    env["DB_PATH"] = os.path.join(BASE_DIR, "data", db_name)
    env["PYTHONPATH"] = os.path.join(BASE_DIR, "distributor_service")
    p = subprocess.Popen(
        [VENV_PYTHON, "-m", "app.server"],
        cwd=os.path.join(BASE_DIR, "distributor_service"),
        env=env
    )
    processes.append(p)
    print(f"  [Node] {distributor_id} ({name}) gRPC Server on port {port} (DB: {db_name})")

def start_storage(region, port):
    env = os.environ.copy()
    env["REGION_NAME"] = region
    env["STORAGE_DIR"] = os.path.join(BASE_DIR, "data", f"storage_{region.lower()}")
    p = subprocess.Popen(
        [VENV_PYTHON, "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", str(port)],
        cwd=os.path.join(BASE_DIR, "storage_service"),
        env=env,
    )
    processes.append(p)
    print(f"  [Storage] DFS region {region} on port {port}")

def start_main_api():
    env = os.environ.copy()
    env["MAIN_DB_PATH"] = os.path.join(BASE_DIR, "data/main_api.db")
    env["JWT_SECRET"] = "mediconnect-dev-secret-2026"
    env["PYTHONPATH"] = os.path.join(BASE_DIR, "main_api")
    p = subprocess.Popen(
        [VENV_PYTHON, "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"],
        cwd=os.path.join(BASE_DIR, "main_api"),
        env=env
    )
    processes.append(p)
    print("  [Middleware] Main API running at http://localhost:8000")

def start_frontend():
    p = subprocess.Popen(
        [VENV_PYTHON, "-m", "http.server", "8080"],
        cwd=os.path.join(BASE_DIR, "frontend")
    )
    processes.append(p)
    print("  [Frontend] Web Portal running at http://localhost:8080")

def wait_for_main_api(timeout=15):
    for _ in range(timeout * 2):
        try:
            urllib.request.urlopen("http://localhost:8000/health", timeout=1)
            return True
        except Exception:
            time.sleep(0.5)
    return False

if __name__ == "__main__":
    print("===============================================================")
    print("   MediConnect — Distributed Pharmacy Procurement Network")
    print("   (local dev orchestrator — no Docker, no RabbitMQ)")
    print("===============================================================")

    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "main_api", "uploads"), exist_ok=True)

    # Main API must be up FIRST so distributors' self-registration on
    # startup succeeds immediately instead of waiting for their retry loop.
    # DFS storage regions first, so Main API can replicate to them immediately.
    start_storage("Pune", 9101)
    start_storage("Mumbai", 9102)
    start_storage("Delhi", 9103)

    start_main_api()
    if wait_for_main_api():
        print("  Main API is ready — starting distributor nodes...")
    else:
        print("  WARNING: Main API did not respond in time; distributors will retry registration.")

    start_distributor("DIS001", "HealthMed Distributors", 50051, "distributor_dis001.db")
    start_distributor("DIS002", "CareSupply Healthcare", 50052, "distributor_dis002.db")
    start_distributor("DIS003", "MediWholesale", 50053, "distributor_dis003.db")
    time.sleep(1)
    start_frontend()

    print("===============================================================")
    print("   ALL SERVICES RUNNING!")
    print("   -> Frontend Web Portal: http://localhost:8080")
    print("   -> Main API & Swagger:  http://localhost:8000/docs")
    print("   -> To demo SCALABILITY, run in a new terminal:")
    print("        python add_distributor_d.py")
    print("===============================================================")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()
