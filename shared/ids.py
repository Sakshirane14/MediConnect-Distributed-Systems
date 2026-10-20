from __future__ import annotations
"""
ID generation helpers.
Names vs Identifiers vs Addresses (FA-1 §Names, Identifiers, Addresses):
  - Identifier: stable, unique key (PHA001) — never changes
  - Name:       human-readable label ("MedPlus Pharmacy A")
  - Address:    where to reach it now (pharmacy-a:50051) — can change
"""
import time
import threading

_counters: dict[str, int] = {}
_lock = threading.Lock()


def _next(prefix: str, width: int = 3) -> str:
    with _lock:
        _counters[prefix] = _counters.get(prefix, 0) + 1
        return f"{prefix}{_counters[prefix]:0{width}d}"


def pharmacy_id() -> str:   return _next("PHA")
def medicine_id() -> str:   return _next("MED")
def user_id() -> str:       return _next("USR")
def order_id() -> str:      return _next("ORD")
def event_id() -> str:      return _next("EVT")
def notification_id() -> str: return _next("NTF")
def staff_id() -> str:      return _next("STF")
def document_id() -> str:   return _next("DOC")
