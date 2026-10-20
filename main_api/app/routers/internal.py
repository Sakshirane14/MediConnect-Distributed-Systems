from __future__ import annotations
from fastapi import APIRouter
from pydantic import BaseModel
from ..db import save_notification
from ..grpc_clients.registry import register_distributor
from ..ws.stock_ws import broadcast_update

router = APIRouter(prefix="/internal", tags=["internal"])

class NotificationPayload(BaseModel):
    notification_id: str
    recipient_type: str
    recipient_id: str
    message: str

@router.post("/notifications")
async def create_notification(payload: NotificationPayload):
    await save_notification(
        payload.notification_id,
        payload.recipient_type,
        payload.recipient_id,
        payload.message,
    )
    await broadcast_update(
        "notifications:" + payload.recipient_id,
        {"type": "notification", "notification_id": payload.notification_id},
    )
    return {"ok": True}


class DistributorRegistration(BaseModel):
    distributor_id: str
    name: str
    grpc_address: str

@router.post("/distributors/register")
async def register_distributor_node(payload: DistributorRegistration):
    """
    FA-1 §9 Service Registration/Discovery. Called by each distributor
    node on startup (and periodically as a heartbeat). This is the single
    integration point that makes 'add Distributor D at runtime with zero
    Main API code changes' literally true — see grpc_clients/registry.py.
    """
    register_distributor(payload.distributor_id, payload.name, payload.grpc_address)
    return {"ok": True, "registered": payload.distributor_id}

