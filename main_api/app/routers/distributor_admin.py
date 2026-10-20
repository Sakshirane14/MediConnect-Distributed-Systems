from __future__ import annotations
"""Distributor-side endpoints: own inventory (via gRPC into that node),
notifications, and manual stock correction."""
import logging
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from ..auth.auth import get_current_user
from ..grpc_clients.distributor_client import search_all_distributors, update_stock_at_distributor
from ..grpc_clients.registry import get_distributor
from ..db import get_notifications, mark_all_notifications_read

router = APIRouter(prefix="/api/distributors", tags=["distributor-admin"])
logger = logging.getLogger(__name__)


def _require_own_distributor(current_user: dict, distributor_id: str):
    if current_user.get("role") != "distributor_staff" or current_user.get("distributor_id") != distributor_id:
        raise HTTPException(status_code=403, detail="Only that distributor's own staff may access this.")


@router.get("/{distributor_id}/inventory")
async def get_distributor_inventory(distributor_id: str, current_user: dict = Depends(get_current_user)):
    _require_own_distributor(current_user, distributor_id)
    if get_distributor(distributor_id) is None:
        raise HTTPException(status_code=404, detail="Distributor not currently registered/online.")
    all_results = await search_all_distributors("")  # empty query -> matches everything (LIKE '%%')
    medicines = [r for r in all_results if r.get("distributor_id") == distributor_id]
    return {"distributor_id": distributor_id, "medicines": medicines}


class StockUpdateRequest(BaseModel):
    new_quantity: int

@router.put("/{distributor_id}/inventory/{medicine_id}")
async def update_medicine_stock(distributor_id: str, medicine_id: str, req: StockUpdateRequest,
                                current_user: dict = Depends(get_current_user)):
    _require_own_distributor(current_user, distributor_id)
    result = await update_stock_at_distributor(distributor_id, medicine_id, req.new_quantity, current_user["user_id"])
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/{distributor_id}/notifications")
async def get_distributor_notifications(distributor_id: str, current_user: dict = Depends(get_current_user)):
    _require_own_distributor(current_user, distributor_id)
    notifications = await get_notifications(distributor_id)
    return {"distributor_id": distributor_id, "notifications": notifications}


@router.post("/{distributor_id}/notifications/read-all")
async def mark_distributor_notifications_read(distributor_id: str, current_user: dict = Depends(get_current_user)):
    _require_own_distributor(current_user, distributor_id)
    count = await mark_all_notifications_read(distributor_id)
    return {"success": True, "marked_read": count}
