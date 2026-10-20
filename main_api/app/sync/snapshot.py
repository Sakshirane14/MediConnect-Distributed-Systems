from __future__ import annotations
"""
FA-1 Unit 3 — Global State / the Chandy-Lamport Snapshot Algorithm.

The real Chandy-Lamport algorithm captures a consistent global state of a
distributed system WITHOUT stopping it, by having each process record its
own state and then send "marker" messages along every outgoing channel;
a process records its state the first time it sees a marker, and then
also records the messages that arrive on each channel before that
channel's own marker (the in-flight/"in-transit" messages).

Honest simplification: this system has no long-lived channels with
in-flight application messages to drain in that sense (every gRPC call is
a single request/response, not a stream), so there is nothing analogous
to "in-transit messages on a channel" to capture. What we DO faithfully
reproduce is the core, actually-hard part of the concept: capturing each
independent process's local state at approximately the same instant, and
tagging the result with the logical clock (not wall-clock) position at
which it was captured, which is what makes it a meaningful DISTRIBUTED
snapshot rather than just "everyone's current wall-clock state" — under
message reordering or clock drift, wall-clock time can be misleading
about what "at the same moment" even means, and the Lamport timestamp on
this snapshot is what actually pins it to a specific point in the
system's causal history.
"""
import time
import uuid
from collections import deque

from . import lamport
from ..grpc_clients.registry import get_registry
from ..grpc_clients.distributor_client import search_all_distributors
from ..db import get_all_orders

_snapshot_history: deque = deque(maxlen=10)


async def take_global_snapshot(triggered_by: str = "manual") -> dict:
    snapshot_id = f"SNAP{uuid.uuid4().hex[:8]}"
    captured_at = time.time()
    main_lamport_at_capture = lamport.tick_local()  # the snapshot's own position in the causal order

    # Main API's local state
    orders = await get_all_orders()
    status_counts: dict[str, int] = {}
    for o in orders:
        status_counts[o["status"]] = status_counts.get(o["status"], 0) + 1

    main_state = {
        "lamport_clock": main_lamport_at_capture,
        "vector_clock": lamport.get_vector_clock(),
        "total_orders": len(orders),
        "orders_by_status": status_counts,
        "registered_nodes": len(get_registry()),
    }

    # Each distributor node's local state, captured "at the same moment"
    # (i.e. within this one async call, back-to-back with no intervening
    # awaits that would let significant time pass between nodes).
    all_medicines = await search_all_distributors("")  # fans out to every registered node concurrently
    nodes_state: dict[str, dict] = {}
    for did, info in get_registry().items():
        node_medicines = [m for m in all_medicines if m.get("distributor_id") == did]
        reachable = any(m.get("status") != "unavailable" and m.get("status") != "timeout" for m in node_medicines) if node_medicines else False
        nodes_state[did] = {
            "name": info["name"],
            "reachable_at_capture": reachable,
            "medicine_count": len([m for m in node_medicines if m.get("medicine_id")]),
            "inventory": [m for m in node_medicines if m.get("medicine_id")],
            "known_lamport_clock": lamport.get_vector_clock().get(did),
        }

    snapshot = {
        "snapshot_id": snapshot_id,
        "captured_at": captured_at,
        "triggered_by": triggered_by,
        "main_api": main_state,
        "nodes": nodes_state,
    }
    _snapshot_history.appendleft(snapshot)
    lamport.record_event("snapshot", snapshot_id, f"Global snapshot captured ({triggered_by}) at logical time {main_lamport_at_capture}")
    return snapshot


def get_snapshot_history(limit: int = 10) -> list[dict]:
    return list(_snapshot_history)[:limit]
