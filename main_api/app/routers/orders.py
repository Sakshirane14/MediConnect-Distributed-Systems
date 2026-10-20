from __future__ import annotations
"""
Procurement order lifecycle (FA-1 B2B flow, pharmacy -> distributor):
  REQUESTED -> APPROVED -> DISPATCHED -> RECEIVED
  REQUESTED -> REJECTED

The atomic stock deduction (concurrency-safety checkpoint, FA-1 §17)
happens at APPROVE time via a gRPC call INTO the chosen distributor's own
node (`place_order_at_distributor`) -- not centrally in Main API. This is
what makes the concurrency guarantee a genuine distributed-systems property
rather than a shortcut.
"""
import logging
import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from ..auth.auth import get_current_user
from ..grpc_clients.distributor_client import place_order_at_distributor
from ..grpc_clients.registry import get_distributor
from ..db import (
    create_order, get_order_by_id, get_orders_for_pharmacy, get_orders_for_distributor,
    get_all_orders, update_order_status, upsert_pharmacy_stock,
)
from ..mq.publisher import publish_event
from ..ws.stock_ws import broadcast_update
from ..sync import lamport

router = APIRouter(prefix="/api/orders", tags=["orders"])
logger = logging.getLogger(__name__)


def _order_id() -> str:
    return f"ORD{uuid.uuid4().int % 10**6:06d}"

def _event_id() -> str:
    return f"EVT{uuid.uuid4().int % 10**6:06d}"

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _require_role(current_user: dict, *roles: str):
    if current_user.get("role") not in roles:
        raise HTTPException(status_code=403, detail=f"Requires role: {' or '.join(roles)}")


class CreateOrderRequest(BaseModel):
    distributor_id: str
    medicine_id: str
    medicine_name: str
    quantity: int = 1
    unit_price: float = 0.0


@router.post("")
async def place_procurement_order(
    req: CreateOrderRequest,
    current_user: dict = Depends(get_current_user),
):
    """Pharmacy Staff: place a procurement order with a specific distributor
    (chosen from the "Find Suppliers" search results). Publishes
    `order_created` asynchronously to RabbitMQ."""
    _require_role(current_user, "pharmacy_staff")
    pharmacy_id = current_user.get("pharmacy_id")
    if not pharmacy_id:
        raise HTTPException(status_code=400, detail="Your account has no pharmacy_id assigned.")
    if req.quantity <= 0:
        raise HTTPException(status_code=400, detail="quantity must be positive.")
    if get_distributor(req.distributor_id) is None:
        raise HTTPException(status_code=404, detail=f"Distributor {req.distributor_id} is not currently registered.")

    order_id = _order_id()
    await create_order(
        order_id, pharmacy_id, req.distributor_id, req.medicine_id, req.medicine_name,
        req.quantity, req.unit_price, current_user["user_id"],
    )
    lamport.record_event("order_created", order_id, f"Order {order_id} placed by {pharmacy_id} with {req.distributor_id} for {req.quantity}x {req.medicine_name}")

    event = {
        "event_id": _event_id(), "order_id": order_id, "pharmacy_id": pharmacy_id,
        "distributor_id": req.distributor_id, "medicine_id": req.medicine_id,
        "medicine_name": req.medicine_name, "quantity": req.quantity, "timestamp": _now(),
    }
    await publish_event("order_created", event)

    try:
        await broadcast_update("orders:" + pharmacy_id, {"type": "order_created", "order_id": order_id, "status": "requested"})
        await broadcast_update("orders:" + req.distributor_id, {"type": "order_created", "order_id": order_id, "status": "requested"})
    except Exception as e:
        logger.warning(f"WebSocket broadcast failed: {e}")

    return {"success": True, "order_id": order_id, "status": "requested"}


@router.get("")
async def list_orders(
    distributor_id: str = Query(None),
    status: str = Query(None),
    current_user: dict = Depends(get_current_user),
):
    """Pharmacy Staff see their own orders. Distributor Staff see orders
    placed WITH THEIR distributor only."""
    if current_user.get("role") == "pharmacy_staff":
        orders = await get_orders_for_pharmacy(current_user.get("pharmacy_id"))
        if status:
            orders = [o for o in orders if o["status"] == status]
        return {"orders": orders}

    if current_user.get("role") == "distributor_staff":
        did = current_user.get("distributor_id")
        orders = await get_orders_for_distributor(did, status)
        return {"orders": orders}

    orders = await get_all_orders(status)
    return {"orders": orders}


@router.get("/{order_id}")
async def get_order(order_id: str, current_user: dict = Depends(get_current_user)):
    order = await get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found.")
    if current_user.get("role") == "pharmacy_staff" and order["pharmacy_id"] != current_user.get("pharmacy_id"):
        raise HTTPException(status_code=403, detail="Forbidden.")
    if current_user.get("role") == "distributor_staff" and order["distributor_id"] != current_user.get("distributor_id"):
        raise HTTPException(status_code=403, detail="Forbidden.")
    return order


@router.post("/{order_id}/approve")
async def approve_order(order_id: str, current_user: dict = Depends(get_current_user)):
    """Distributor Staff: approve a pharmacy's order.

    FA-1 concurrency-safety checkpoint (§17): stock is atomically decremented
    via gRPC `PlaceOrder` INTO the owning distributor node's own database --
    a single `UPDATE ... WHERE quantity >= ?` there. Two pharmacies racing
    to buy the last units from the SAME distributor can never together take
    more than physically exists.
    """
    _require_role(current_user, "distributor_staff")
    order = await get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found.")
    if order["distributor_id"] != current_user.get("distributor_id"):
        raise HTTPException(status_code=403, detail="This order belongs to a different distributor.")
    if order["status"] != "requested":
        raise HTTPException(status_code=400, detail=f"Order is already '{order['status']}'.")

    if order["medicine_id"].startswith("IMGREQ-"):
        # Image Requirement order (FA-1 §8 Distributed Multimedia): there's
        # no structured SKU to reserve stock against — the pharmacist
        # uploaded a photo, not a medicine_id, so this distributor already
        # committed to fulfilling it manually when it won the claim race.
        # Skip the gRPC ReserveStock call and go straight to approved.
        result = {"success": True, "new_quantity": None}
        lamport.record_event("mutex", order_id, f"Image-requirement order {order_id} approved without stock reservation (no SKU).")
    else:
        # FA-1 Unit 3 — Mutual Exclusion: this gRPC call is the "request
        # entry" into the critical section (that distributor node's own
        # atomic stock UPDATE). We log both sides of it to the shared event
        # log so the request/grant-or-deny sequence is visible even though
        # the actual exclusion enforcement lives inside PlaceOrder itself.
        lamport.record_event("mutex_request", order_id, f"Order {order_id}: requesting exclusive access to {order['medicine_id']} @ {order['distributor_id']} (qty={order['quantity']})")
        result = await place_order_at_distributor(
            order["distributor_id"], order_id, order["medicine_id"], order["quantity"], order["pharmacy_id"]
        )
        lamport.record_event(
            "mutex_grant" if result["success"] else "mutex_deny",
            order_id,
            f"Order {order_id}: {'GRANTED' if result['success'] else 'DENIED'} — {result['message']}",
        )
    if not result["success"]:
        raise HTTPException(status_code=409, detail=result["message"])

    await update_order_status(order_id, "approved", "approved_timestamp")

    event = {
        "event_id": _event_id(), "order_id": order_id, "pharmacy_id": order["pharmacy_id"],
        "distributor_id": order["distributor_id"], "medicine_id": order["medicine_id"],
        "medicine_name": order["medicine_name"], "quantity": order["quantity"], "timestamp": _now(),
    }
    await publish_event("order_approved", event)

    try:
        await broadcast_update("orders:" + order["pharmacy_id"], {"type": "order_updated", "order_id": order_id, "status": "approved"})
        await broadcast_update("orders:" + order["distributor_id"], {"type": "order_updated", "order_id": order_id, "status": "approved"})
    except Exception as e:
        logger.warning(f"WebSocket broadcast failed: {e}")

    return {"success": True, "order_id": order_id, "status": "approved", "distributor_remaining_stock": result["new_quantity"]}


@router.post("/{order_id}/reject")
async def reject_order(order_id: str, current_user: dict = Depends(get_current_user)):
    """Distributor Staff: reject a pharmacy's order. No stock movement occurs."""
    _require_role(current_user, "distributor_staff")
    order = await get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found.")
    if order["distributor_id"] != current_user.get("distributor_id"):
        raise HTTPException(status_code=403, detail="This order belongs to a different distributor.")
    if order["status"] != "requested":
        raise HTTPException(status_code=400, detail=f"Order is already '{order['status']}'.")

    await update_order_status(order_id, "rejected")

    event = {
        "event_id": _event_id(), "order_id": order_id, "pharmacy_id": order["pharmacy_id"],
        "distributor_id": order["distributor_id"], "medicine_id": order["medicine_id"],
        "medicine_name": order["medicine_name"], "quantity": order["quantity"], "timestamp": _now(),
    }
    await publish_event("order_rejected", event)

    try:
        await broadcast_update("orders:" + order["pharmacy_id"], {"type": "order_updated", "order_id": order_id, "status": "rejected"})
        await broadcast_update("orders:" + order["distributor_id"], {"type": "order_updated", "order_id": order_id, "status": "rejected"})
    except Exception as e:
        logger.warning(f"WebSocket broadcast failed: {e}")

    return {"success": True, "order_id": order_id, "status": "rejected"}


@router.post("/{order_id}/dispatch")
async def dispatch_order(order_id: str, current_user: dict = Depends(get_current_user)):
    """Distributor Staff: mark an approved order as physically dispatched."""
    _require_role(current_user, "distributor_staff")
    order = await get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found.")
    if order["distributor_id"] != current_user.get("distributor_id"):
        raise HTTPException(status_code=403, detail="This order belongs to a different distributor.")
    if order["status"] != "approved":
        raise HTTPException(status_code=400, detail=f"Order must be 'approved' before dispatch (currently '{order['status']}').")

    await update_order_status(order_id, "dispatched", "dispatched_timestamp")

    event = {
        "event_id": _event_id(), "order_id": order_id, "pharmacy_id": order["pharmacy_id"],
        "distributor_id": order["distributor_id"], "medicine_id": order["medicine_id"],
        "medicine_name": order["medicine_name"], "quantity": order["quantity"], "timestamp": _now(),
    }
    await publish_event("order_dispatched", event)

    try:
        await broadcast_update("orders:" + order["pharmacy_id"], {"type": "order_updated", "order_id": order_id, "status": "dispatched"})
        await broadcast_update("orders:" + order["distributor_id"], {"type": "order_updated", "order_id": order_id, "status": "dispatched"})
    except Exception as e:
        logger.warning(f"WebSocket broadcast failed: {e}")

    return {"success": True, "order_id": order_id, "status": "dispatched"}


@router.post("/{order_id}/receive")
async def receive_order(order_id: str, current_user: dict = Depends(get_current_user)):
    """Pharmacy Staff: mark a dispatched order as received. Increases the
    pharmacy's own LOCAL inventory (central table in Main API — pharmacies
    are clients of the distributor network in this architecture, not
    independent gRPC nodes themselves, FA-1 §45)."""
    _require_role(current_user, "pharmacy_staff")
    order = await get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found.")
    if order["pharmacy_id"] != current_user.get("pharmacy_id"):
        raise HTTPException(status_code=403, detail="Forbidden.")
    if order["status"] != "dispatched":
        raise HTTPException(status_code=400, detail=f"Order must be 'dispatched' before it can be received (currently '{order['status']}').")

    new_qty = await upsert_pharmacy_stock(
        order["pharmacy_id"], order["medicine_id"], order["medicine_name"], order["quantity"]
    )
    await update_order_status(order_id, "received", "received_timestamp")

    event = {
        "event_id": _event_id(), "order_id": order_id, "pharmacy_id": order["pharmacy_id"],
        "distributor_id": order["distributor_id"], "medicine_id": order["medicine_id"],
        "medicine_name": order["medicine_name"], "quantity": order["quantity"],
        "new_pharmacy_quantity": new_qty, "timestamp": _now(),
    }
    await publish_event("order_received", event)

    try:
        await broadcast_update("orders:" + order["pharmacy_id"], {"type": "order_updated", "order_id": order_id, "status": "received"})
        await broadcast_update("orders:" + order["distributor_id"], {"type": "order_updated", "order_id": order_id, "status": "received"})
        await broadcast_update("stock:" + order["pharmacy_id"], {
            "medicine_id": order["medicine_id"], "new_quantity": new_qty,
        })
    except Exception as e:
        logger.warning(f"WebSocket broadcast failed: {e}")

    return {"success": True, "order_id": order_id, "status": "received", "new_pharmacy_quantity": new_qty}
