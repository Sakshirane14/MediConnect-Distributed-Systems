from __future__ import annotations
"""
WebRTC signaling relay - FA-1 Distributed Multimedia Systems, P2P call
between Distributor and Pharmacy Staff.

This server is intentionally "dumb": it just tracks which two WebSocket
connections are in a room and relays whatever signaling message one peer
sends to the other. All SDP offer/answer/ICE-candidate logic lives in the
browser (frontend/js/webrtc.js). Business identity (login) and WebRTC peer
identity are deliberately separate - the `peer` query param is a random
per-tab session id (see getPeerSessionId() in webrtc.js), not the logged-in
user id, so two tabs on the same laptop are always distinct peers even if
they happen to be logged in as the same account.
"""
import json
import logging
from collections import defaultdict
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(tags=["webrtc"])
logger = logging.getLogger(__name__)

# room_id -> list of (websocket, peer_session_id)
_rooms: dict[str, list] = defaultdict(list)


@router.websocket("/ws/call/{room_id}")
async def signaling_websocket(websocket: WebSocket, room_id: str, peer: str = "unknown"):
    await websocket.accept()
    room = _rooms[room_id]

    if len(room) >= 2:
        logger.info(f"[WebRTC] Room {room_id} full - rejecting peer={peer}")
        await websocket.send_text(json.dumps({"type": "error", "message": "Room full (max 2 participants)."}))
        await websocket.close()
        return

    room.append((websocket, peer))
    peer_index = len(room) - 1
    logger.info(f"[WebRTC] room={room_id} peer={peer} joined as index={peer_index} (room size={len(room)})")

    if len(room) == 2:
        # Second peer just joined: tell the FIRST peer to become the
        # initiator (create the offer). Tell the SECOND peer (itself) that
        # it should simply wait for that incoming offer.
        first_ws, first_peer = room[0]
        try:
            await first_ws.send_text(json.dumps({"type": "peer_joined", "room_id": room_id, "peer": peer}))
            logger.info(f"[WebRTC] room={room_id} notified initiator peer={first_peer}")
        except Exception as e:
            logger.warning(f"[WebRTC] Failed to notify initiator: {e}")
        await websocket.send_text(json.dumps({"type": "waiting", "room_id": room_id, "note": "offer incoming"}))
    else:
        await websocket.send_text(json.dumps({"type": "waiting", "room_id": room_id}))

    try:
        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            msg_type = msg.get("type", "")
            logger.info(f"[WebRTC] room={room_id} peer={peer} -> {msg_type}")

            other_peers = [ws for ws, pid in room if ws is not websocket]
            for peer_ws in other_peers:
                try:
                    await peer_ws.send_text(raw)
                except Exception as e:
                    logger.warning(f"[WebRTC] Failed to relay {msg_type} in room {room_id}: {e}")
    except WebSocketDisconnect:
        room[:] = [(ws, pid) for ws, pid in room if ws is not websocket]
        logger.info(f"[WebRTC] room={room_id} peer={peer} left (remaining={len(room)})")
        for peer_ws, pid in room:
            try:
                await peer_ws.send_text(json.dumps({"type": "peer_left", "room_id": room_id}))
            except Exception:
                pass
        if not room:
            del _rooms[room_id]
    except Exception as e:
        logger.error(f"[WebRTC] signaling error in room {room_id}: {e}")
        room[:] = [(ws, pid) for ws, pid in room if ws is not websocket]
