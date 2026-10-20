from __future__ import annotations
"""
Stream-Oriented Communication (FA-1 Unit 2 §4) — generic topic pub/sub over
WebSockets. Two logical topic families share the same subscriber registry:

  stock:{medicine_id}   -> live stock quantity/status changes
  orders:{pharmacy_id}  -> live order-status changes for one pharmacy
  orders:DISTRIBUTOR    -> live order-status changes across the whole network

Dashboards subscribe to the topic(s) relevant to them and get pushed
updates with no polling / no page refresh required.
"""
import json
import logging
from collections import defaultdict
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)

_subscribers: dict[str, set] = defaultdict(set)


async def _handle_topic_socket(websocket: WebSocket, topic: str) -> None:
    await websocket.accept()
    _subscribers[topic].add(websocket)
    logger.info(f"WS subscribed: {topic} (total={len(_subscribers[topic])})")
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        _subscribers[topic].discard(websocket)
        logger.info(f"WS unsubscribed: {topic}")
    except Exception as e:
        _subscribers[topic].discard(websocket)
        logger.warning(f"WS error on {topic}: {e}")


@router.websocket("/ws/stock/{medicine_id}")
async def stock_websocket(websocket: WebSocket, medicine_id: str):
    await _handle_topic_socket(websocket, "stock:" + medicine_id)


@router.websocket("/ws/orders/{scope}")
async def orders_websocket(websocket: WebSocket, scope: str):
    """scope is either a pharmacy_id (PHA001...) or a distributor_id (DIS001...)."""
    await _handle_topic_socket(websocket, "orders:" + scope)


@router.websocket("/ws/requirements/{distributor_id}")
async def requirements_feed_websocket(websocket: WebSocket, distributor_id: str):
    """A distributor dashboard subscribes here to learn about brand-new
    requirement lists the moment a pharmacy publishes one."""
    await _handle_topic_socket(websocket, "requirements:" + distributor_id)


@router.websocket("/ws/requirement/{requirement_id}")
async def requirement_status_websocket(websocket: WebSocket, requirement_id: str):
    """A pharmacy dashboard subscribes here (per open requirement) to see
    'Distributor A -> Responded' update live as each distributor
    independently submits its response — no page refresh needed."""
    await _handle_topic_socket(websocket, "requirement:" + requirement_id)


@router.websocket("/ws/notifications/{recipient_id}")
async def notifications_websocket(websocket: WebSocket, recipient_id: str):
    """Push a lightweight signal whenever a new notification is persisted.
    The dashboard then reloads its authenticated notification list."""
    await _handle_topic_socket(websocket, "notifications:" + recipient_id)


@router.websocket("/ws/image-requirements")
async def image_requirements_broadcast_websocket(websocket: WebSocket):
    """Single global broadcast channel — every distributor dashboard
    subscribes here (no per-distributor scope) so that when a pharmacist
    uploads a photo requirement, EVERY distributor sees it appear/disappear
    at the same instant. This is what makes the 'first to claim wins' flow
    a genuine, visible race rather than something inferred from separate
    per-node feeds."""
    await _handle_topic_socket(websocket, "image-requirements:broadcast")


async def broadcast_update(topic: str, payload: dict) -> None:
    subscribers = set(_subscribers.get(topic, set()))
    if not subscribers:
        return
    message = json.dumps(payload)
    dead = set()
    for ws in subscribers:
        try:
            await ws.send_text(message)
        except Exception:
            dead.add(ws)
    _subscribers[topic] -= dead
    logger.info(f"WS broadcast to {len(subscribers)} client(s) on {topic}")


# Backwards-compatible alias used by pharmacy_admin.py for direct stock edits.
async def broadcast_stock_update(medicine_id: str, payload: dict) -> None:
    await broadcast_update("stock:" + medicine_id, payload)
