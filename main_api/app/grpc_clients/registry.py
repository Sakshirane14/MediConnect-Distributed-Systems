from __future__ import annotations
import os
import time
import logging
import threading

logger = logging.getLogger(__name__)

IN_DOCKER = os.path.exists('/.dockerenv') or os.environ.get('IN_DOCKER') == 'true'

# ── Dynamic Distributor Registry (FA-1 §9 Service Registration/Discovery) ──
# In-memory, thread-safe registry of currently-known distributor nodes.
# Distributor services call POST /internal/distributors/register on
# startup (see distributor_service/app/server.py) to add/refresh their
# entry here. Main API's search/health code (distributor_client.py)
# iterates over whatever is CURRENTLY registered — it contains zero
# distributor-specific logic, which is what makes "add Distributor D at
# runtime, no Main API code changes" actually true rather than a claim.
#
# Pre-seeded with A/B/C so the demo works immediately without waiting on
# the first registration heartbeat; self-registration simply refreshes/
# confirms these and is how D gets added live.
_lock = threading.Lock()
_registry: dict[str, dict] = {
    "DIS001": {
        "name": "HealthMed Distributors",
        "grpc_address": "distributor-a:50051" if IN_DOCKER else "localhost:50051",
        "registered_at": time.time(),
    },
    "DIS002": {
        "name": "CareSupply Healthcare",
        "grpc_address": "distributor-b:50051" if IN_DOCKER else "localhost:50052",
        "registered_at": time.time(),
    },
    "DIS003": {
        "name": "MediWholesale",
        "grpc_address": "distributor-c:50051" if IN_DOCKER else "localhost:50053",
        "registered_at": time.time(),
    },
}


def register_distributor(distributor_id: str, name: str, grpc_address: str) -> None:
    with _lock:
        is_new = distributor_id not in _registry
        _registry[distributor_id] = {
            "name": name,
            "grpc_address": grpc_address,
            "registered_at": time.time(),
        }
    if is_new:
        logger.info(f"[Registry] NEW distributor registered: {distributor_id} ({name}) @ {grpc_address}")
    else:
        logger.info(f"[Registry] Distributor re-registered (heartbeat): {distributor_id}")


def get_registry() -> dict[str, dict]:
    with _lock:
        return dict(_registry)


def get_distributor(distributor_id: str) -> dict | None:
    with _lock:
        return _registry.get(distributor_id)


def unregister_distributor(distributor_id: str) -> bool:
    with _lock:
        if distributor_id in _registry:
            del _registry[distributor_id]
            return True
        return False


# Backwards-compat alias some older code/imports may still reference.
PHARMACY_REGISTRY = _registry
