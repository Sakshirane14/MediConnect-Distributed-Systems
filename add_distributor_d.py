"""
FA-1 Scalability demo (local dev / no Docker).

Run this in a NEW terminal while run_local.py (with Distributor A/B/C
already running) is still up. It starts a 4th distributor node using the
exact same reusable distributor_service image/code, on a new port. The
node self-registers with Main API on startup — no Main API restart, no
code changes.

Afterwards:
  - GET /api/network/status will show "registered_nodes": 4
  - Searching any medicine will automatically include Distributor D

Usage:
    python add_distributor_d.py
Press Ctrl+C to stop Distributor D again (and watch it disappear from
subsequent /api/network/status calls once its next expected heartbeat
is missed, or immediately if you also refresh distributor health checks).
"""
import os
import sys
import subprocess
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON = os.path.join(BASE_DIR, "venv/bin/python")
if not os.path.exists(VENV_PYTHON):
    VENV_PYTHON = sys.executable

env = os.environ.copy()
env["DISTRIBUTOR_ID"] = "DIS004"
env["DISTRIBUTOR_NAME"] = "QuickPharma Wholesale"
env["GRPC_PORT"] = "50054"
env["GRPC_ADDRESS"] = "localhost:50054"
env["MAIN_API_URL"] = "http://localhost:8000"
env["DB_PATH"] = os.path.join(BASE_DIR, "data", "distributor_dis004.db")
env["PYTHONPATH"] = os.path.join(BASE_DIR, "distributor_service")

print("=" * 60)
print("  Adding Distributor D (DIS004) — QuickPharma Wholesale")
print("  Same distributor_service codebase, new env vars, new port.")
print("=" * 60)

p = subprocess.Popen(
    [VENV_PYTHON, "-m", "app.server"],
    cwd=os.path.join(BASE_DIR, "distributor_service"),
    env=env,
)

try:
    p.wait()
except KeyboardInterrupt:
    print("\nStopping Distributor D...")
    p.terminate()
