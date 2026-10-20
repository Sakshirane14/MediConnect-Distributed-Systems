from __future__ import annotations
"""
'Distributed Network Status' page backend — FA-1 §35-37.
Real, live registry + health-check data (never faked): this is the single
page that makes Fault Tolerance and Scalability visually obvious to a
faculty evaluator watching the demo. Extended with FA-1 Unit 3
(Synchronization): logical clocks, clock-offset estimation, Bully
election, and a distributed event log.
"""
import logging
from fastapi import APIRouter, Depends
from ..auth.auth import get_current_user
from ..grpc_clients.registry import get_registry
from ..grpc_clients.distributor_client import health_check_distributor
from ..sync import lamport, election, snapshot

router = APIRouter(prefix="/api/network", tags=["network"])
logger = logging.getLogger(__name__)


@router.get("/status")
async def network_status(current_user: dict = Depends(get_current_user)):
    registry = get_registry()
    nodes = []
    online_count = 0
    online_ids = []
    for distributor_id, info in registry.items():
        health = await health_check_distributor(distributor_id)
        is_online = health.get("status") == "healthy"
        if is_online:
            online_count += 1
            online_ids.append(distributor_id)
        offset = lamport.get_clock_offsets().get(distributor_id)
        nodes.append({
            "distributor_id": distributor_id,
            "name": info["name"],
            "grpc_address": info["grpc_address"],
            "status": "online" if is_online else "offline",
            "response_ms": health.get("response_ms"),
            "lamport_clock": health.get("lamport_clock"),
            "clock_offset_ms": offset["offset_ms"] if offset else None,
        })

    # FA-1 Unit 3 — Bully Algorithm: re-elect automatically the moment the
    # current coordinator is found to be offline. This is the algorithm's
    # actual trigger condition, checked here rather than on a separate
    # background timer, so it's always consistent with what this very
    # response just observed.
    auto_election = election.ensure_coordinator(online_ids)

    return {
        "registered_nodes": len(registry),
        "online": online_count,
        "offline": len(registry) - online_count,
        "nodes": nodes,
        "coordinator": election.get_current_coordinator(),
        "last_election": election.get_last_election(),
        "auto_election_just_ran": auto_election is not None,
        "main_lamport_clock": lamport.get_lamport_value(),
        "vector_clock": lamport.get_vector_clock(),
    }


@router.post("/election")
async def trigger_election(current_user: dict = Depends(get_current_user)):
    """Manual 'Trigger Election' demo button — FA-1 Unit 3 Election
    Algorithms. Runs the Bully algorithm against whichever nodes are
    online right now, regardless of whether the current coordinator is
    still alive (useful for demonstrating the algorithm on demand, not
    only when a node has actually failed)."""
    registry = get_registry()
    online_ids = []
    for distributor_id in registry:
        health = await health_check_distributor(distributor_id)
        if health.get("status") == "healthy":
            online_ids.append(distributor_id)
    return election.run_election(online_ids, triggered_by="manual (demo button)")


@router.get("/events")
async def get_event_log(limit: int = 50, current_user: dict = Depends(get_current_user)):
    """FA-1 Unit 3 — the shared, Lamport-ordered Distributed Event Log.
    Every entry carries the logical clock (and vector-clock snapshot) at
    which it was recorded, not just a wall-clock timestamp."""
    return {"events": lamport.get_recent_events(limit)}


@router.post("/snapshot")
async def create_snapshot(current_user: dict = Depends(get_current_user)):
    """FA-1 Unit 3 — Global State (Chandy-Lamport-inspired). Captures Main
    API's own state plus every online distributor node's inventory, all
    within one async call, tagged with the logical clock position at
    which the snapshot was taken."""
    return await snapshot.take_global_snapshot(triggered_by="manual (demo button)")


@router.get("/snapshots")
async def list_snapshots(limit: int = 10, current_user: dict = Depends(get_current_user)):
    return {"snapshots": snapshot.get_snapshot_history(limit)}
