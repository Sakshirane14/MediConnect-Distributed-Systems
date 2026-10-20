from __future__ import annotations
"""
FA-1 Unit 3 — Logical Clocks (Lamport's Algorithm).

Each distributor node keeps its OWN monotonically-increasing counter —
there is no shared/global clock anywhere in this system, which is exactly
the problem logical clocks exist to solve. Two rules, per Lamport (1978):

  1. Local event (handling any request on its own):        clock += 1
  2. Receive event (a request arrives carrying a sender's
     clock value):     clock = max(local_clock, sender_clock) + 1

Applying rule 2 on every incoming gRPC call is what keeps this node's
clock causally consistent with Main API's clock, even though the two
processes never share memory and could be on different machines with
unsynchronized physical clocks.
"""
import threading


class LamportClock:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()  # gRPC's asyncio server can still run handlers concurrently enough to matter

    def tick(self) -> int:
        """Rule 1 — a purely local event."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_clock: int) -> int:
        """Rule 2 — an event caused by a message from another process."""
        with self._lock:
            self._value = max(self._value, received_clock) + 1
            return self._value

    @property
    def value(self) -> int:
        with self._lock:
            return self._value
