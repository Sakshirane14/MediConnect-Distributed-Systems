from __future__ import annotations
"""
FA-1 Unit 3 — Logical Clocks (Lamport's Algorithm) + Vector Clocks, on the
Main API side. See distributor_service/app/clock.py for the per-node half
of this; this module is Main API's own clock plus the bookkeeping needed
to show the whole system's causal history in one place for the demo.

Vector clock note (an honest simplification): a textbook vector clock
gives every process a component for every OTHER process, updated via
gossip between them. Here, distributor nodes never talk to each other —
only to Main API — so there is no path for node B to ever learn node A's
clock value directly. What we track instead is the vector *as observed
by Main API*: one component per known node ID plus "MAIN", where
component X is updated to max(current, X's_last_reported_lamport_clock)
every time Main API talks to X, and MAIN's own component increments on
every local Main API event. This is a faithful, simplified version of a
vector clock for a hub-and-spoke topology (which is what this system
actually is), not a claim that the nodes maintain full vector clocks
themselves — they only maintain a scalar Lamport clock each.
"""
import threading
import time
import uuid
from collections import deque

_lock = threading.Lock()
_lamport_value = 0
_vector_clock: dict[str, int] = {"MAIN": 0}

# Recent cross-service events, newest first. Capped so this stays an
# in-memory demo aid, not an unbounded log — FA-1 doesn't need durability
# here, just visibility into "what happened, in what causal order".
_event_log: deque = deque(maxlen=200)

# Per-node estimated physical clock offset (FA-1 Unit 3 — Clock
# Synchronization, Cristian's Algorithm). offset_ms > 0 means that node's
# clock is estimated to be AHEAD of Main API's.
_clock_offsets: dict[str, dict] = {}


def tick_local() -> int:
    """Rule 1 of Lamport's algorithm: a purely local Main API event."""
    global _lamport_value
    with _lock:
        _lamport_value += 1
        _vector_clock["MAIN"] = _lamport_value
        return _lamport_value


def observe_node_clock(distributor_id: str, remote_lamport: int) -> int:
    """Rule 2: Main API just heard back from `distributor_id` carrying
    its Lamport value. Applies the standard receive rule to Main API's
    own clock AND records the node's reported value as that node's vector
    component (see the module docstring for what this vector actually
    represents)."""
    global _lamport_value
    with _lock:
        _lamport_value = max(_lamport_value, remote_lamport) + 1
        _vector_clock["MAIN"] = _lamport_value
        _vector_clock[distributor_id] = remote_lamport
        return _lamport_value


def record_clock_offset(distributor_id: str, server_unix_time: float, rtt_ms: float) -> dict:
    """FA-1 Unit 3 — Cristian's Algorithm. Classic formula: the node's
    clock at the moment Main API's request arrived is estimated as
    server_unix_time (which the node stamped right before replying), and
    by the time we receive the reply roughly rtt_ms/2 has passed since
    then — so we adjust by half the round trip to estimate the node's
    CURRENT clock, then compare to our own current time."""
    now = time.time()
    estimated_node_time_now = server_unix_time + (rtt_ms / 2000.0)
    offset_ms = round((estimated_node_time_now - now) * 1000, 1)
    entry = {"distributor_id": distributor_id, "offset_ms": offset_ms, "rtt_ms": rtt_ms, "measured_at": now}
    with _lock:
        _clock_offsets[distributor_id] = entry
    return entry


def get_clock_offsets() -> dict:
    with _lock:
        return dict(_clock_offsets)


def get_lamport_value() -> int:
    with _lock:
        return _lamport_value


def get_vector_clock() -> dict:
    with _lock:
        return dict(_vector_clock)


def record_event(event_type: str, ref_id: str, description: str, service: str = "main-api") -> dict:
    """Append one entry to the shared distributed event log, stamped with
    Main API's current Lamport clock and vector-clock snapshot. Called
    from order/requirement/image-requirement lifecycle code so the FA-1
    'Distributed Event Log' panel shows a genuine, causally-ordered trace
    of what happened across independently-clocked services — not just a
    wall-clock-sorted activity feed."""
    clock = tick_local()
    entry = {
        "event_id": f"EVT{uuid.uuid4().hex[:8]}",
        "lamport_clock": clock,
        "vector_clock": get_vector_clock(),
        "service": service,
        "event_type": event_type,
        "ref_id": ref_id,
        "description": description,
        "wall_clock_time": time.time(),
    }
    with _lock:
        _event_log.appendleft(entry)
    return entry


def get_recent_events(limit: int = 50) -> list[dict]:
    with _lock:
        return list(_event_log)[:limit]
