from __future__ import annotations
"""
Medicine Requirement List — a pharmacy bundles several needed medicines
into one requirement and broadcasts it to every currently-registered
distributor node. Each distributor independently evaluates it against its
OWN live inventory (via gRPC) and submits its own response — genuinely
independent computation, not rows filtered from one shared table.
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from ..auth.auth import get_current_user
from ..grpc_clients.distributor_client import get_medicine_from_distributor, health_check_distributor
from ..grpc_clients.registry import get_registry, get_distributor
from ..db import (
    create_requirement, get_requirement, get_requirement_items,
    get_requirements_for_pharmacy, get_all_open_requirements,
    save_distributor_response, get_responses_for_requirement, has_distributor_responded,
    save_notification, update_requirement_status, get_response_items, create_order,
)
from ..mq.publisher import publish_event
from ..ws.stock_ws import broadcast_update

router = APIRouter(prefix="/api/requirements", tags=["requirements"])
logger = logging.getLogger(__name__)


def _requirement_id() -> str:
    return f"REQ{uuid.uuid4().int % 10**6:06d}"

def _event_id() -> str:
    return f"EVT{uuid.uuid4().int % 10**6:06d}"

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RequirementItemIn(BaseModel):
    medicine_id: str
    medicine_name: str
    requested_quantity: int
    priority: str = "Normal"

class CreateRequirementRequest(BaseModel):
    items: list[RequirementItemIn]
    attachment_document_id: str | None = None


@router.post("")
async def create_requirement_list(req: CreateRequirementRequest, current_user: dict = Depends(get_current_user)):
    """Pharmacy Staff: publish a Medicine Requirement List to the whole
    distributor network."""
    if current_user.get("role") != "pharmacy_staff":
        raise HTTPException(status_code=403, detail="Requires role: pharmacy_staff")
    if not req.items:
        raise HTTPException(status_code=400, detail="At least one medicine is required.")

    pharmacy_id = current_user.get("pharmacy_id")
    requirement_id = _requirement_id()
    items = [i.dict() for i in req.items]
    await create_requirement(requirement_id, pharmacy_id, current_user["user_id"], items, req.attachment_document_id)

    event = {
        "event_id": _event_id(), "requirement_id": requirement_id,
        "pharmacy_id": pharmacy_id, "medicine_count": len(items), "timestamp": _now(),
    }
    await publish_event("requirement_created", event)

    # Every currently-registered distributor node is notified (topic-per-
    # distributor keeps this consistent with the rest of the WS design).
    # Notifications are saved directly here (rather than solely relying on
    # the async RabbitMQ consumer) because only Main API's in-process
    # registry knows exactly which distributors are currently registered.
    for distributor_id in get_registry():
        try:
            notification_id = f"NTF{uuid.uuid4().int % 10**6:06d}"
            await save_notification(
                notification_id, "distributor", distributor_id,
                f"New requirement {requirement_id} from {pharmacy_id}: {len(items)} medicine(s) requested.",
            )
            await broadcast_update("requirements:" + distributor_id, {"type": "new_requirement", "requirement_id": requirement_id})
            await broadcast_update("notifications:" + distributor_id, {"type": "notification", "notification_id": notification_id})
        except Exception as e:
            logger.warning(f"Notify/broadcast failed for {distributor_id}: {e}")

    return {"success": True, "requirement_id": requirement_id, "status": "open", "item_count": len(items)}


@router.get("")
async def list_requirements(current_user: dict = Depends(get_current_user)):
    if current_user.get("role") == "pharmacy_staff":
        requirements = await get_requirements_for_pharmacy(current_user.get("pharmacy_id"))
    elif current_user.get("role") == "distributor_staff":
        requirements = await get_all_open_requirements()
        did = current_user.get("distributor_id")
        for r in requirements:
            r["i_have_responded"] = await has_distributor_responded(r["requirement_id"], did)
    else:
        requirements = await get_all_open_requirements()

    for r in requirements:
        r["items"] = await get_requirement_items(r["requirement_id"])
        r["responses"] = await get_responses_for_requirement(r["requirement_id"])
        r["response_count"] = len(r["responses"])
        r["distributor_count"] = len(get_registry())

    return {"requirements": requirements}


@router.get("/{requirement_id}")
async def get_requirement_detail(requirement_id: str, current_user: dict = Depends(get_current_user)):
    requirement = await get_requirement(requirement_id)
    if not requirement:
        raise HTTPException(status_code=404, detail="Requirement not found.")
    if current_user.get("role") == "pharmacy_staff" and requirement["pharmacy_id"] != current_user.get("pharmacy_id"):
        raise HTTPException(status_code=403, detail="Forbidden.")

    requirement["items"] = await get_requirement_items(requirement_id)
    requirement["responses"] = await get_responses_for_requirement(requirement_id)
    requirement["distributor_count"] = len(get_registry())

    # Live per-distributor response + REAL network health. Health checks are
    # concurrent so one slow/offline node does not serially delay the modal.
    registry = get_registry()
    responded_ids = {r["distributor_id"] for r in requirement["responses"]}
    registry_items = list(registry.items())
    health_results = await asyncio.gather(
        *(health_check_distributor(did) for did, _ in registry_items),
        return_exceptions=True,
    )
    distributor_status = []
    for (did, info), health in zip(registry_items, health_results):
        if isinstance(health, Exception):
            health = {"status": "unavailable", "response_ms": None, "error": str(health)}
        raw_status = str(health.get("status", "unavailable")).lower()
        online = raw_status in {"healthy", "online", "ok"}
        distributor_status.append({
            "distributor_id": did,
            "name": info["name"],
            "responded": did in responded_ids,
            "online": online,
            "network_status": "online" if online else ("timeout" if "timeout" in raw_status else "offline"),
            "response_ms": health.get("response_ms"),
        })
    requirement["distributor_status"] = distributor_status
    return requirement


@router.post("/{requirement_id}/respond")
async def respond_to_requirement(requirement_id: str, current_user: dict = Depends(get_current_user)):
    """Distributor Staff: independently evaluate this requirement against
    OUR OWN live inventory (fetched via gRPC, per item) and submit a
    response. This is what makes each distributor's answer genuinely
    independent rather than filtered rows from a shared table."""
    if current_user.get("role") != "distributor_staff":
        raise HTTPException(status_code=403, detail="Requires role: distributor_staff")
    distributor_id = current_user.get("distributor_id")
    if get_distributor(distributor_id) is None:
        raise HTTPException(status_code=503, detail="Your distributor node is not currently registered/online.")

    requirement = await get_requirement(requirement_id)
    if not requirement:
        raise HTTPException(status_code=404, detail="Requirement not found.")
    items = await get_requirement_items(requirement_id)
    if not items:
        raise HTTPException(status_code=400, detail="Requirement has no items.")

    response_items = []
    can_fulfil_all = True
    total_price = 0.0

    for item in items:
        details = await get_medicine_from_distributor(distributor_id, item["medicine_id"])
        if details is None:
            available_qty, price, can_fulfil = 0, 0.0, False
        else:
            available_qty = details["quantity"]
            price = details["price"]
            can_fulfil = available_qty >= item["requested_quantity"]
        if not can_fulfil:
            can_fulfil_all = False
        billable_qty = min(available_qty, item["requested_quantity"])
        total_price += billable_qty * price
        response_items.append({
            "medicine_id": item["medicine_id"],
            "available_quantity": available_qty,
            "price": price,
            "can_fulfil": can_fulfil,
        })

    await save_distributor_response(requirement_id, distributor_id, can_fulfil_all, round(total_price, 2), response_items)

    event = {
        "event_id": _event_id(), "requirement_id": requirement_id, "distributor_id": distributor_id,
        "pharmacy_id": requirement["pharmacy_id"], "can_fulfil_all": can_fulfil_all,
        "total_price": round(total_price, 2), "timestamp": _now(),
    }
    await publish_event("distributor_response_submitted", event)

    try:
        await broadcast_update("requirement:" + requirement_id, {
            "type": "response_submitted", "distributor_id": distributor_id, "can_fulfil_all": can_fulfil_all,
        })
    except Exception as e:
        logger.warning(f"WS broadcast failed: {e}")

    return {
        "success": True, "requirement_id": requirement_id, "distributor_id": distributor_id,
        "can_fulfil_all": can_fulfil_all, "total_price": round(total_price, 2), "items": response_items,
    }


class AcceptResponseRequest(BaseModel):
    distributor_id: str


@router.post("/{requirement_id}/accept")
async def accept_requirement_response(requirement_id: str, req: AcceptResponseRequest,
                                       current_user: dict = Depends(get_current_user)):
    """Pharmacy Staff (owning): accept ONE distributor's response to a
    requirement list. This is the step that was previously missing —
    without it, a response was just a quote that went nowhere:

      * it never created an order, so there was nothing to approve/
        dispatch/receive, and
      * the requirement stayed 'open' forever, so it kept showing up as
        pending in every OTHER distributor's queue even after the
        pharmacy had effectively already chosen someone.

    Accepting creates one real procurement order per requested item
    (quantity capped at what that distributor said it can supply) and
    flips the requirement to 'fulfilled', which removes it from
    get_all_open_requirements() — the query every distributor dashboard
    uses to list pending requirements.
    """
    if current_user.get("role") != "pharmacy_staff":
        raise HTTPException(status_code=403, detail="Requires role: pharmacy_staff")

    requirement = await get_requirement(requirement_id)
    if not requirement:
        raise HTTPException(status_code=404, detail="Requirement not found.")
    if requirement["pharmacy_id"] != current_user.get("pharmacy_id"):
        raise HTTPException(status_code=403, detail="Forbidden.")
    if requirement["status"] != "open":
        raise HTTPException(status_code=400, detail=f"Requirement is already '{requirement['status']}'.")

    distributor_id = req.distributor_id
    if not await has_distributor_responded(requirement_id, distributor_id):
        raise HTTPException(status_code=400, detail=f"{distributor_id} has not responded to this requirement yet.")

    response_items = await get_response_items(requirement_id, distributor_id)
    if not response_items:
        raise HTTPException(status_code=400, detail="That distributor's response has no items.")

    requested_items = {i["medicine_id"]: i for i in await get_requirement_items(requirement_id)}
    pharmacy_id = requirement["pharmacy_id"]
    created_order_ids = []

    for item in response_items:
        medicine_id = item["medicine_id"]
        requested = requested_items.get(medicine_id)
        if not requested:
            continue
        quantity = min(item["available_quantity"], requested["requested_quantity"])
        if quantity <= 0:
            continue  # this distributor couldn't supply this particular line at all

        order_id = f"ORD{uuid.uuid4().int % 10**6:06d}"
        await create_order(
            order_id, pharmacy_id, distributor_id, medicine_id, requested["medicine_name"],
            quantity, item["price"], current_user["user_id"],
        )
        created_order_ids.append(order_id)

        event = {
            "event_id": _event_id(), "order_id": order_id, "pharmacy_id": pharmacy_id,
            "distributor_id": distributor_id, "medicine_id": medicine_id,
            "medicine_name": requested["medicine_name"], "quantity": quantity, "timestamp": _now(),
        }
        await publish_event("order_created", event)
        try:
            await broadcast_update("orders:" + pharmacy_id, {"type": "order_created", "order_id": order_id, "status": "requested"})
            await broadcast_update("orders:" + distributor_id, {"type": "order_created", "order_id": order_id, "status": "requested"})
        except Exception as e:
            logger.warning(f"WebSocket broadcast failed: {e}")

    if not created_order_ids:
        raise HTTPException(status_code=400, detail=f"{distributor_id} cannot currently supply any item on this requirement.")

    await update_requirement_status(requirement_id, "fulfilled", distributor_id)

    # Tell every OTHER distributor this requirement is no longer open —
    # their dashboards will also stop listing it on next refresh because
    # get_all_open_requirements() now filters status='open', but a live
    # push makes the demo (and the actual UX) snappier.
    for did in get_registry():
        try:
            await broadcast_update("requirements:" + did, {"type": "requirement_closed", "requirement_id": requirement_id})
        except Exception:
            pass

    return {
        "success": True, "requirement_id": requirement_id, "status": "fulfilled",
        "accepted_distributor_id": distributor_id, "order_ids": created_order_ids,
    }
