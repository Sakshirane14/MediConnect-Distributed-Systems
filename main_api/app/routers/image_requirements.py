from __future__ import annotations
"""
Image Requirement — FA-1 §8 Distributed Multimedia Systems.

A pharmacist uploads a PHOTO of a needs list (handwritten note, printed
list, whatever's on hand) instead of typing structured items. The image
itself is the multimedia payload: captured on the pharmacy's device,
stored on the Main API, and transferred across the network to every
distributor node's dashboard.

It is broadcast to every currently-registered distributor at once, and
whichever distributor's staff clicks "Accept This Order" FIRST wins it —
a genuine distributed-systems race condition, resolved with a single
atomic SQL UPDATE (see db.claim_image_requirement), not a lock the app
happens to hold in memory. Once claimed, a normal procurement order is
created and flows through the exact same approve -> dispatch -> receive
lifecycle as any other order.
"""
import os
import uuid
import logging
import mimetypes
from pathlib import Path
from datetime import datetime, timezone
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends, Query, Header
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
from jose import JWTError

from ..auth.auth import get_current_user, decode_token
from ..db import (
    create_image_requirement, get_image_requirement, get_image_requirements_for_pharmacy,
    get_image_requirements_for_distributor, claim_image_requirement, create_order, get_user_by_id,
    record_replicas, get_replica_regions, get_all_replicas,
)
from ..mq.publisher import publish_event
from ..ws.stock_ws import broadcast_update
from ..sync import lamport
from ..dfs import storage as dfs

router = APIRouter(prefix="/api/image-requirements", tags=["image-requirements"])
logger = logging.getLogger(__name__)

UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "uploads")))
MAX_SIZE = 5 * 1024 * 1024
ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp"}

# Broadcast: ALL distributor dashboards subscribe to this single shared
# WebSocket topic (unlike the per-distributor "requirements:{id}" topic
# used by the structured requirement-list flow), because every
# distributor genuinely needs to see this at the same time to make the
# "first to claim" race real.
BROADCAST_TOPIC = "image-requirements:broadcast"


def _image_requirement_id() -> str:
    return f"IMGREQ{uuid.uuid4().int % 10**6:06d}"

def _order_id() -> str:
    return f"ORD{uuid.uuid4().int % 10**6:06d}"

def _event_id() -> str:
    return f"EVT{uuid.uuid4().int % 10**6:06d}"

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.post("")
async def upload_image_requirement(
    file: UploadFile = File(...),
    note: str = Form(""),
    current_user: dict = Depends(get_current_user),
):
    """Pharmacy Staff: upload a photo of a needs list. Broadcast to every
    distributor the moment it's saved."""
    if current_user.get("role") != "pharmacy_staff":
        raise HTTPException(status_code=403, detail="Requires role: pharmacy_staff")
    pharmacy_id = current_user.get("pharmacy_id")

    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type '{file.content_type}'. Upload a photo: JPEG, PNG, or WebP.",
        )

    content = await file.read()
    if len(content) > MAX_SIZE:
        raise HTTPException(status_code=413, detail=f"Image too large ({len(content) // 1024}KB). Maximum allowed: 5MB.")
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    ext = Path(file.filename or "requirement.jpg").suffix or ".jpg"
    unique_name = f"IMGREQ_{uuid.uuid4().hex[:10]}{ext}"
    # FA-1 Unit 4 — Distributed File System: copy the photo to EVERY storage
    # region at once instead of keeping it on Main API's own disk.
    stored_regions = await dfs.replicate_file(unique_name, content)
    if stored_regions:
        await record_replicas(unique_name, stored_regions)
    else:
        # Every region is down: degrade gracefully (keep the upload working)
        # by falling back to Main API's local disk, like before the DFS existed.
        logger.warning(f"DFS: no storage region reachable - storing {unique_name} locally as fallback")
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        (UPLOAD_DIR / unique_name).write_bytes(content)

    image_requirement_id = _image_requirement_id()
    await create_image_requirement(image_requirement_id, pharmacy_id, current_user["user_id"], unique_name, note)

    event = {
        "event_id": _event_id(), "image_requirement_id": image_requirement_id,
        "pharmacy_id": pharmacy_id, "timestamp": _now(),
    }
    await publish_event("image_requirement_uploaded", event)

    try:
        await broadcast_update(BROADCAST_TOPIC, {
            "type": "image_requirement_created", "image_requirement_id": image_requirement_id,
            "pharmacy_id": pharmacy_id,
        })
    except Exception as e:
        logger.warning(f"WebSocket broadcast failed: {e}")

    logger.info(f"Image requirement {image_requirement_id} uploaded by {pharmacy_id} ({unique_name}, {len(content)} bytes)")
    return {"success": True, "image_requirement_id": image_requirement_id, "replicated_to": stored_regions}


@router.get("")
async def list_image_requirements(current_user: dict = Depends(get_current_user)):
    role = current_user.get("role")
    if role == "pharmacy_staff":
        items = await get_image_requirements_for_pharmacy(current_user.get("pharmacy_id"))
    elif role == "distributor_staff":
        items = await get_image_requirements_for_distributor(current_user.get("distributor_id"))
    else:
        raise HTTPException(status_code=403, detail="Unknown role.")
    replicas = await get_all_replicas()
    for it in items:
        it["replicas"] = replicas.get(it["image_filename"], [])
    # Live region status so the UI can show "Pune OK / Mumbai offline" per photo.
    return {"image_requirements": items, "regions": await dfs.storage_health()}


async def _resolve_user_from_header_or_query(authorization: str | None, token_qs: str | None) -> dict:
    """Plain <img src="..."> tags can't attach an Authorization header, so
    this endpoint also accepts ?token=... in the URL as a fallback — same
    JWT, just carried a different way. Everything else in the app still
    uses the normal header-based get_current_user."""
    raw_token = None
    if authorization and authorization.lower().startswith("bearer "):
        raw_token = authorization[7:]
    elif token_qs:
        raw_token = token_qs
    if not raw_token:
        raise HTTPException(status_code=401, detail="Missing authentication token.")
    try:
        payload = decode_token(raw_token)
        user_id = payload.get("sub")
        user = await get_user_by_id(user_id) if user_id else None
        if not user:
            raise HTTPException(status_code=401, detail="Invalid authentication token.")
        return user
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication token.")


@router.get("/{image_requirement_id}/image")
async def get_image_requirement_photo(
    image_requirement_id: str,
    token: str | None = Query(None),
    authorization: str | None = Header(None),
):
    """Serves the actual uploaded photo. Any pharmacy_staff (their own
    upload only) or any distributor_staff (broadcast — every distributor
    needs to be able to view it to decide whether to claim it) may view
    it."""
    current_user = await _resolve_user_from_header_or_query(authorization, token)
    item = await get_image_requirement(image_requirement_id)
    if not item:
        raise HTTPException(status_code=404, detail="Image requirement not found.")
    role = current_user.get("role")
    if role == "pharmacy_staff" and item["pharmacy_id"] != current_user.get("pharmacy_id"):
        raise HTTPException(status_code=403, detail="Forbidden.")
    if role not in ("pharmacy_staff", "distributor_staff"):
        raise HTTPException(status_code=403, detail="Forbidden.")

    filename = item["image_filename"]
    regions = await get_replica_regions(filename)
    if regions:
        got = await dfs.fetch_file(filename, regions)
        if got is None:
            raise HTTPException(status_code=503, detail="Every storage region holding this photo is currently unreachable.")
        content, region = got
        media_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        return Response(content=content, media_type=media_type, headers={"X-Served-From-Region": region})

    # Legacy photo uploaded before the DFS existed (or while every region was down).
    path = UPLOAD_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Image file missing on server.")
    return FileResponse(path)


@router.post("/{image_requirement_id}/claim")
async def claim_image_requirement_order(image_requirement_id: str, current_user: dict = Depends(get_current_user)):
    """Distributor Staff: 'Accept This Order'. First one in wins — see
    db.claim_image_requirement for why this is actually race-safe and not
    just 'usually fine'. The resulting order uses a placeholder
    medicine_id (IMGREQ-...) since there's no structured item list to
    reserve stock against; orders.py special-cases that prefix to skip the
    gRPC stock-reservation step at approval time and go straight to
    'approved' — everything AFTER that point (dispatch, receive, chat) is
    the exact same flow as any other order."""
    if current_user.get("role") != "distributor_staff":
        raise HTTPException(status_code=403, detail="Requires role: distributor_staff")
    distributor_id = current_user.get("distributor_id")

    item = await get_image_requirement(image_requirement_id)
    if not item:
        raise HTTPException(status_code=404, detail="Image requirement not found.")
    if item["status"] != "open":
        winner = item.get("claimed_by_distributor_id") or "another distributor"
        raise HTTPException(status_code=409, detail=f"Already claimed by {winner}.")

    order_id = _order_id()
    lamport.record_event("mutex_request", image_requirement_id, f"{distributor_id}: requesting exclusive claim on {image_requirement_id}")
    won = await claim_image_requirement(image_requirement_id, distributor_id, order_id)
    if not won:
        # Someone else's claim committed first between our read above and
        # this UPDATE — that gap is exactly the race window, and losing it
        # is the correct, expected outcome for the loser.
        fresh = await get_image_requirement(image_requirement_id)
        winner = fresh.get("claimed_by_distributor_id") if fresh else "another distributor"
        lamport.record_event("mutex_deny", image_requirement_id, f"{distributor_id}: DENIED — {winner} claimed {image_requirement_id} first")
        raise HTTPException(status_code=409, detail=f"Too late — already claimed by {winner}.")
    lamport.record_event("mutex_grant", image_requirement_id, f"{distributor_id}: GRANTED exclusive claim on {image_requirement_id} -> order {order_id}")

    await create_order(
        order_id, item["pharmacy_id"], distributor_id,
        f"IMGREQ-{image_requirement_id}", f"Image requirement {image_requirement_id} (see attached photo)",
        1, 0.0, current_user["user_id"],
    )

    event = {
        "event_id": _event_id(), "image_requirement_id": image_requirement_id, "order_id": order_id,
        "pharmacy_id": item["pharmacy_id"], "distributor_id": distributor_id, "timestamp": _now(),
    }
    await publish_event("image_requirement_claimed", event)

    try:
        await broadcast_update(BROADCAST_TOPIC, {
            "type": "image_requirement_claimed", "image_requirement_id": image_requirement_id,
            "claimed_by_distributor_id": distributor_id,
        })
        await broadcast_update("orders:" + item["pharmacy_id"], {"type": "order_created", "order_id": order_id, "status": "requested"})
        await broadcast_update("orders:" + distributor_id, {"type": "order_created", "order_id": order_id, "status": "requested"})
    except Exception as e:
        logger.warning(f"WebSocket broadcast failed: {e}")

    logger.info(f"Image requirement {image_requirement_id} claimed by {distributor_id} -> order {order_id}")
    return {"success": True, "image_requirement_id": image_requirement_id, "order_id": order_id, "status": "claimed"}
