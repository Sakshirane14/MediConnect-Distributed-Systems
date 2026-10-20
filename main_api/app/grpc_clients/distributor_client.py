from __future__ import annotations
import asyncio
import time
import logging
from typing import Any
import grpc
import grpc.aio
from .registry import get_registry, get_distributor
from ..sync import lamport

logger = logging.getLogger(__name__)

try:
    from ..generated import pharmacy_pb2, pharmacy_pb2_grpc
except ImportError:
    pharmacy_pb2 = None
    pharmacy_pb2_grpc = None

GRPC_TIMEOUT = 2.0

# Channels are created lazily and cached per address, since the set of
# distributors is dynamic (a newly-registered Distributor D has no
# pre-opened channel yet — see scalability requirement, FA-1 §8-10).
_channels: dict[str, grpc.aio.Channel] = {}
_stubs: dict[str, Any] = {}


def _get_stub(grpc_address: str):
    if grpc_address not in _stubs:
        channel = grpc.aio.insecure_channel(grpc_address)
        _channels[grpc_address] = channel
        _stubs[grpc_address] = pharmacy_pb2_grpc.DistributorServiceStub(channel)
        logger.info(f"gRPC channel opened (lazy): {grpc_address}")
    return _stubs[grpc_address]


async def init_channels() -> None:
    """Pre-warm channels for all currently-registered distributors. Not
    required for correctness (channels are also created lazily) but avoids
    a first-request delay for the initially-known nodes."""
    for distributor_id, info in get_registry().items():
        _get_stub(info["grpc_address"])


async def close_channels() -> None:
    for channel in _channels.values():
        await channel.close()


async def _call_distributor(distributor_id: str, rpc_name: str, request: Any) -> Any:
    info = get_distributor(distributor_id)
    if info is None:
        raise RuntimeError(f"{distributor_id} is not registered")
    stub = _get_stub(info["grpc_address"])
    method = getattr(stub, rpc_name)
    return await asyncio.wait_for(method(request), timeout=GRPC_TIMEOUT)


async def search_all_distributors(query: str) -> list[dict]:
    """
    Fan out SearchMedicine concurrently to every currently-registered
    distributor node (FA-1 §14: parallel, not sequential). A failing or
    slow node contributes a single 'unavailable' result and does not
    block or fail the aggregate — FA-1 fault-tolerance / partial-response
    aggregation (§29-31).
    """
    registry_snapshot = get_registry()  # iterate a snapshot so registrations mid-fanout don't cause surprises

    async def _search_one(distributor_id: str, info: dict) -> list[dict]:
        start = time.perf_counter()
        try:
            req = pharmacy_pb2.SearchMedicineRequest(query=query)
            resp = await _call_distributor(distributor_id, "SearchMedicine", req)
            elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
            results = []
            for r in resp.results:
                results.append({
                    "distributor_id":   r.distributor_id,
                    "distributor_name": info["name"],
                    "medicine_id":      r.medicine_id,
                    "name":             r.name,
                    "available":        r.available,
                    "quantity":         r.quantity,
                    "price":            round(r.price, 2),
                    "status":           r.status,
                    "response_ms":      elapsed_ms,
                })
            return results
        except asyncio.TimeoutError:
            elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
            logger.warning(f"{distributor_id}: SearchMedicine TIMEOUT after {elapsed_ms}ms")
            return [{
                "distributor_id": distributor_id, "distributor_name": info["name"],
                "medicine_id": None, "name": query, "available": False, "quantity": 0,
                "price": 0.0, "status": "timeout", "response_ms": elapsed_ms, "error": "Timed out",
            }]
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
            logger.warning(f"{distributor_id}: SearchMedicine failed - {exc}")
            return [{
                "distributor_id": distributor_id, "distributor_name": info["name"],
                "medicine_id": None, "name": query, "available": False, "quantity": 0,
                "price": 0.0, "status": "unavailable", "response_ms": elapsed_ms, "error": str(exc),
            }]

    tasks = [_search_one(did, info) for did, info in registry_snapshot.items()]
    all_results = await asyncio.gather(*tasks, return_exceptions=True)

    merged = []
    for result in all_results:
        if isinstance(result, Exception):
            logger.error(f"Unhandled gather exception: {result}")
        elif isinstance(result, list):
            merged.extend(result)
    return merged


async def get_medicine_from_distributor(distributor_id: str, medicine_id: str) -> dict | None:
    try:
        req = pharmacy_pb2.GetMedicineDetailsRequest(medicine_id=medicine_id)
        resp = await _call_distributor(distributor_id, "GetMedicineDetails", req)
        info = get_distributor(distributor_id) or {}
        return {
            "medicine_id":         resp.medicine_id,
            "name":                resp.name,
            "description":         resp.description,
            "price":               round(resp.price, 2),
            "quantity":            resp.quantity,
            "available":           resp.available,
            "distributor_id":      resp.distributor_id,
            "distributor_name":    info.get("name", distributor_id),
            "low_stock_threshold": resp.low_stock_threshold,
        }
    except Exception as exc:
        logger.warning(f"{distributor_id}: GetMedicineDetails failed - {exc}")
        return None


async def place_order_at_distributor(distributor_id: str, order_id: str, medicine_id: str,
                                     quantity: int, pharmacy_id: str) -> dict:
    """
    Ask the chosen distributor to atomically accept the order against its
    own stock. This IS the concurrency-safety checkpoint (FA-1 §17) —
    it happens at the actual owning node, not centrally in Main API.
    Piggybacks Main API's current Lamport clock so the node can apply the
    receive rule before entering its critical section (Unit 3).
    """
    try:
        my_clock = lamport.tick_local()
        req = pharmacy_pb2.PlaceOrderRequest(
            order_id=order_id, medicine_id=medicine_id, quantity=quantity, pharmacy_id=pharmacy_id,
            sender_lamport_clock=my_clock,
        )
        resp = await _call_distributor(distributor_id, "PlaceOrder", req)
        lamport.observe_node_clock(distributor_id, resp.lamport_clock)
        return {
            "success":      resp.success,
            "new_quantity": resp.new_quantity,
            "message":      resp.message,
            "node_lamport_clock": resp.lamport_clock,
        }
    except Exception as exc:
        logger.warning(f"{distributor_id}: PlaceOrder failed - {exc}")
        return {"success": False, "new_quantity": 0, "message": f"Distributor unavailable: {exc}", "node_lamport_clock": None}


async def update_stock_at_distributor(distributor_id: str, medicine_id: str,
                                      new_quantity: int, staff_id: str) -> dict:
    try:
        req = pharmacy_pb2.UpdateStockRequest(
            medicine_id=medicine_id, new_quantity=new_quantity, staff_id=staff_id
        )
        resp = await _call_distributor(distributor_id, "UpdateStock", req)
        return {"success": resp.success, "message": resp.message}
    except Exception as exc:
        return {"success": False, "message": str(exc)}


async def health_check_distributor(distributor_id: str) -> dict:
    start = time.perf_counter()
    try:
        my_clock = lamport.tick_local()
        req = pharmacy_pb2.HealthCheckRequest(sender_lamport_clock=my_clock)
        resp = await _call_distributor(distributor_id, "HealthCheck", req)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        lamport.observe_node_clock(distributor_id, resp.lamport_clock)
        lamport.record_clock_offset(distributor_id, resp.server_unix_time, elapsed_ms)
        return {
            "status": resp.status, "distributor_id": resp.distributor_id, "response_ms": elapsed_ms,
            "lamport_clock": resp.lamport_clock,
        }
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 1)
        return {"status": "unavailable", "distributor_id": distributor_id, "response_ms": elapsed_ms, "error": str(exc)}
