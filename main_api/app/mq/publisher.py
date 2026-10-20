from __future__ import annotations
"""
RabbitMQ publisher — FA-1 Unit 2 §3 Message-Oriented Communication.

One direct exchange ("mediconnect.events"). Each replenishment lifecycle
transition publishes to its own routing key so the notification-service
consumer can react differently per event type, and so the flow is easy to
demonstrate/explain independently of the request/response gRPC path.
"""
import asyncio
import json
import logging
import os

logger = logging.getLogger(__name__)

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE_NAME = "mediconnect.events"

VALID_ROUTING_KEYS = {
    "order_created",
    "order_approved",
    "order_rejected",
    "order_dispatched",
    "order_received",
    "requirement_created",
    "distributor_response_submitted",
}

_connection = None
_channel = None
_exchange = None
_connect_lock = asyncio.Lock()

async def init_rabbitmq() -> None:
    await _ensure_rabbitmq()


async def _ensure_rabbitmq() -> bool:
    """Connect/reconnect lazily so a RabbitMQ startup race does not disable
    publishing for the lifetime of Main API."""
    global _connection, _channel, _exchange
    if _exchange is not None and _connection is not None and not getattr(_connection, "is_closed", False):
        return True
    async with _connect_lock:
        if _exchange is not None and _connection is not None and not getattr(_connection, "is_closed", False):
            return True
        try:
            import aio_pika
            _connection = await aio_pika.connect_robust(RABBITMQ_URL, reconnect_interval=5)
            _channel = await _connection.channel()
            _exchange = await _channel.declare_exchange(
                EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True
            )
            logger.info(f"RabbitMQ connected: {RABBITMQ_URL}")
            return True
        except Exception as e:
            logger.warning(f"RabbitMQ connect failed (non-fatal): {e}")
            _connection = _channel = _exchange = None
            return False

async def close_rabbitmq() -> None:
    global _connection
    if _connection:
        try:
            await _connection.close()
        except Exception:
            pass

async def publish_event(routing_key: str, event: dict) -> None:
    global _connection, _channel, _exchange
    """Publish a B2B replenishment lifecycle event. Non-fatal on failure —
    the request/response flow (gRPC + REST) never blocks on RabbitMQ being
    up (FA-1 fault-tolerance principle: async notification is best-effort)."""
    if routing_key not in VALID_ROUTING_KEYS:
        logger.warning(f"Unknown routing key '{routing_key}' - publishing anyway")
    if not await _ensure_rabbitmq():
        logger.warning(f"RabbitMQ not available - skipping publish of '{routing_key}'")
        return
    try:
        import aio_pika
        body = json.dumps(event).encode()
        message = aio_pika.Message(
            body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await _exchange.publish(message, routing_key=routing_key)
        logger.info(f"Published event {event.get('event_id')} [{routing_key}] to {EXCHANGE_NAME}")
    except Exception as e:
        logger.warning(f"Failed to publish RabbitMQ event '{routing_key}' (non-fatal): {e}")
        _connection = _channel = _exchange = None
