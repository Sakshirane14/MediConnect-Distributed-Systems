from __future__ import annotations
"""
FA-1 Unit 3 — Election Algorithms (the Bully Algorithm).

Every distributor node has a numeric rank derived from its ID (DIS001=1,
DIS002=2, ...). The Bully algorithm's rule is simple: the highest-ranked
process that is currently alive becomes coordinator, and any process that
notices the coordinator is unreachable triggers a new election.

Honest simplification: a textbook Bully implementation has each process
independently send ELECTION/OK/COORDINATOR messages directly to its
peers over the network, with real timeouts. This system's distributor
nodes only ever talk to Main API, never to each other (see lamport.py's
docstring for the same topology note) — so Main API plays the role of
the network here, computing the same result the message-passing protocol
would converge on and recording an explicit trace of the messages that
WOULD be exchanged, for demo/viva clarity. The RANKING RULE and the
TRIGGER CONDITION (coordinator unreachable -> re-election) are the real
algorithm; only the transport is centralized rather than peer-to-peer.
"""
import re
import time
import threading
from . import lamport

_lock = threading.Lock()
_current_coordinator: str | None = None
_last_election: dict | None = None


def _rank(distributor_id: str) -> int:
    """DIS001 -> 1, DIS004 -> 4, ... — the Bully algorithm's ordering key.
    Falls back to a hash for any ID that doesn't match the DISNNN pattern
    so this never crashes on an unexpected node name."""
    m = re.search(r"(\d+)$", distributor_id)
    return int(m.group(1)) if m else hash(distributor_id) % 10_000


def get_current_coordinator() -> str | None:
    with _lock:
        return _current_coordinator


def get_last_election() -> dict | None:
    with _lock:
        return _last_election


def run_election(online_ids: list[str], triggered_by: str) -> dict:
    """Runs (a centralized simulation of) the Bully algorithm and updates
    the current coordinator. Always safe to call — an empty online_ids
    list just means "no coordinator possible right now"."""
    global _current_coordinator, _last_election

    old_coordinator = _current_coordinator
    trace: list[str] = [f"Election triggered ({triggered_by}). Previous coordinator: {old_coordinator or 'none'}."]

    if not online_ids:
        winner = None
        trace.append("No distributor nodes are currently online — no coordinator can be elected.")
    else:
        ranked = sorted(online_ids, key=_rank)
        winner = max(ranked, key=_rank)
        for node in ranked:
            higher = [n for n in ranked if _rank(n) > _rank(node)]
            if higher:
                trace.append(f"{node} --ELECTION--> {', '.join(higher)}")
        trace.append(f"No node ranked higher than {winner} is online, so {winner} declares itself coordinator.")
        for node in ranked:
            if node != winner:
                trace.append(f"{winner} --COORDINATOR--> {node}")

    with _lock:
        _current_coordinator = winner
        _last_election = {
            "coordinator": winner,
            "previous_coordinator": old_coordinator,
            "trace": trace,
            "triggered_by": triggered_by,
            "timestamp": time.time(),
        }

    lamport.record_event(
        "election", winner or "none",
        f"Bully election ({triggered_by}) -> coordinator: {winner or 'none'} (was: {old_coordinator or 'none'})",
    )
    return _last_election


def ensure_coordinator(online_ids: list[str]) -> dict | None:
    """Call this whenever the network status is checked. If the current
    coordinator has gone offline (or none has ever been elected and nodes
    ARE online), automatically triggers the Bully algorithm's defining
    behaviour: re-election on coordinator failure. Returns the election
    result if one just ran, else None (nothing needed to change)."""
    coordinator = get_current_coordinator()
    if coordinator in online_ids:
        return None  # current coordinator still alive - Bully algorithm requires no action
    if not online_ids:
        return None  # nothing to elect from
    return run_election(online_ids, triggered_by="auto (coordinator unreachable)")
