from __future__ import annotations
"""Shared constants across all services (reference/documentation module —
not imported by the running services, each of which keeps its own copy
so they remain independently deployable nodes)."""
from enum import Enum

# ── Timeouts ──────────────────────────────────────────────────────────────────
GRPC_TIMEOUT_SECONDS = 2.0          # per-pharmacy gRPC call deadline
RABBITMQ_RECONNECT_DELAY = 3        # seconds between reconnect attempts

# ── Stock thresholds ──────────────────────────────────────────────────────────
LOW_STOCK_THRESHOLD = 5             # qty <= this → "low_stock" badge

# ── Business document upload (Purchase Order / Invoice / Delivery Note) ───────
MAX_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MB
ALLOWED_MIME_TYPES = {"image/jpeg", "image/png", "image/webp", "application/pdf"}

# ── Status strings ────────────────────────────────────────────────────────────
class MedicineStatus(str, Enum):
    AVAILABLE    = "available"
    LOW_STOCK    = "low_stock"
    OUT_OF_STOCK = "out_of_stock"

class PharmacyStatus(str, Enum):
    ONLINE  = "online"
    OFFLINE = "offline"

class OrderStatus(str, Enum):
    REQUESTED  = "requested"
    APPROVED   = "approved"
    REJECTED   = "rejected"
    DISPATCHED = "dispatched"
    RECEIVED   = "received"

# ── RabbitMQ (Message-Oriented Communication, FA-1 Unit 2 §3) ────────────────
# One direct exchange, one routing key per lifecycle event. The notification
# service binds a single queue to every routing key below, so approving,
# rejecting, dispatching, requesting and receiving stock are each their own
# asynchronous, independently-consumable event.
RABBITMQ_EXCHANGE      = "mediconnect.events"
RABBITMQ_QUEUE         = "notification_queue"
RABBITMQ_EXCHANGE_TYPE = "direct"
RABBITMQ_ROUTING_KEYS = [
    "order_created",
    "order_approved",
    "order_rejected",
    "order_dispatched",
    "order_received",
    "requirement_created",
    "distributor_response_submitted",
]

# ── Pharmacy registry (ID → gRPC address) ────────────────────────────────────
# Address: where to physically reach the entity right now.
# Identifier: stable key used throughout the system.
# Name: human-readable label.
# NOTE: main_api/app/grpc_clients/registry.py is the live copy actually used
# by the running Main API (it additionally resolves docker vs. local
# addresses) — this is kept in sync for documentation/viva purposes.
PHARMACY_REGISTRY: dict[str, dict] = {
    "PHA001": {"name": "Pharmacy A", "grpc_address": "pharmacy-a:50051"},
    "PHB002": {"name": "Pharmacy B", "grpc_address": "pharmacy-b:50051"},
    "PHC003": {"name": "Pharmacy C", "grpc_address": "pharmacy-c:50051"},
}
