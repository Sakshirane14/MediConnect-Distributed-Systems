from __future__ import annotations
"""Pharmacy-side endpoints: local inventory (central table — pharmacies are
clients of the distributor network in this architecture, FA-1 §45) and
notifications."""
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from ..auth.auth import get_current_user
from ..db import get_pharmacy_inventory, set_pharmacy_stock, get_notifications, mark_all_notifications_read

router = APIRouter(prefix="/api/pharmacy", tags=["pharmacy"])
logger = logging.getLogger(__name__)


def _require_own_pharmacy(current_user: dict, pharmacy_id: str):
    if current_user.get("role") != "pharmacy_staff" or current_user.get("pharmacy_id") != pharmacy_id:
        raise HTTPException(status_code=403, detail="Only that pharmacy's own staff may access this.")


@router.get("/{pharmacy_id}/inventory")
async def get_inventory(pharmacy_id: str, current_user: dict = Depends(get_current_user)):
    _require_own_pharmacy(current_user, pharmacy_id)
    items = await get_pharmacy_inventory(pharmacy_id)
    return {"pharmacy_id": pharmacy_id, "medicines": items}


class StockUpdateRequest(BaseModel):
    new_quantity: int

@router.put("/{pharmacy_id}/inventory/{medicine_id}")
async def update_inventory(pharmacy_id: str, medicine_id: str, req: StockUpdateRequest,
                           current_user: dict = Depends(get_current_user)):
    """Manual local stock correction (e.g. after a physical stock count)."""
    _require_own_pharmacy(current_user, pharmacy_id)
    ok = await set_pharmacy_stock(pharmacy_id, medicine_id, req.new_quantity)
    if not ok:
        raise HTTPException(status_code=404, detail="Medicine not found in this pharmacy's local inventory.")
    return {"success": True, "new_quantity": req.new_quantity}


@router.get("/{pharmacy_id}/notifications")
async def get_pharmacy_notifications(pharmacy_id: str, current_user: dict = Depends(get_current_user)):
    _require_own_pharmacy(current_user, pharmacy_id)
    notifications = await get_notifications(pharmacy_id)
    return {"pharmacy_id": pharmacy_id, "notifications": notifications}


@router.post("/{pharmacy_id}/notifications/read-all")
async def mark_pharmacy_notifications_read(pharmacy_id: str, current_user: dict = Depends(get_current_user)):
    _require_own_pharmacy(current_user, pharmacy_id)
    count = await mark_all_notifications_read(pharmacy_id)
    return {"success": True, "marked_read": count}
