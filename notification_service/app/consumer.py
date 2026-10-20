from __future__ import annotations
"""
RabbitMQ notification consumer.
FA-1: Message-Oriented Communication — decoupled async event processing.

Binds one queue to all 5 procurement-order lifecycle routing keys and
persists a notification for the relevant recipient (pharmacy or
distributor) via the Main API's internal endpoint. Runs independently —
the order lifecycle (gRPC + REST) never waits on this consumer.
"""
import asyncio
import json
import logging
import os
import uuid

import aio_pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE_NAME = "mediconnect.events"
QUEUE_NAME = "notification_queue"
ROUTING_KEYS = [
    "order_created",
    "order_approved",
    "order_rejected",
    "order_dispatched",
    "order_received",
    "requirement_created",
    "distributor_response_submitted",
]
MAIN_API_URL = os.environ.get("MAIN_API_URL", "http://main-api:8000")


def _build_notifications(routing_key: str, body: dict) -> list[tuple[str, str, str]]:
    """Return list of (recipient_type, recipient_id, message) for this event."""
    order_id = body.get("order_id", "?")
    pharmacy_id = body.get("pharmacy_id", "?")
    distributor_id = body.get("distributor_id", "?")
    med_name = body.get("medicine_name", "Medicine")
    qty = body.get("quantity", 1)

    if routing_key == "order_created":
        return [("distributor", distributor_id, f"New order {order_id}: {qty}x {med_name} from {pharmacy_id}.")]
    if routing_key == "order_approved":
        return [("pharmacy", pharmacy_id, f"Order {order_id} approved by {distributor_id}: {qty}x {med_name}. Awaiting dispatch.")]
    if routing_key == "order_rejected":
        return [("pharmacy", pharmacy_id, f"Order {order_id} was rejected by {distributor_id}: {qty}x {med_name}.")]
    if routing_key == "order_dispatched":
        return [("pharmacy", pharmacy_id, f"Order {order_id} dispatched by {distributor_id}: {qty}x {med_name} is on its way.")]
    if routing_key == "order_received":
        new_qty = body.get("new_pharmacy_quantity", "?")
        return [("distributor", distributor_id, f"Order {order_id} received by {pharmacy_id}: {qty}x {med_name} (their new stock: {new_qty}).")]
    if routing_key == "requirement_created":
        # Main API already fans this out directly to every registered
        # distributor (it alone knows the live registry) — this event is
        # still published/consumed here for the async audit trail /
        # message-oriented-communication demo, but does not duplicate
        # notifications.
        return []
    if routing_key == "distributor_response_submitted":
        requirement_id = body.get("requirement_id", "?")
        fulfil = "can fulfil in full" if body.get("can_fulfil_all") else "can partially fulfil"
        return [("pharmacy", pharmacy_id, f"{distributor_id} responded to {requirement_id}: {fulfil} (approx ₹{body.get('total_price', 0)}).")]
    return []


async def handle_message(message: aio_pika.IncomingMessage) -> None:
    async with message.process():
        try:
            routing_key = message.routing_key
            body = json.loads(message.body.decode())
            event_id = body.get("event_id", "?")
            order_id = body.get("order_id", "?")

            logger.info(f"[EVENT {event_id}] {routing_key} for order {order_id}: {body}")

            targets = _build_notifications(routing_key, body)

            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                for recipient_type, recipient_id, msg_text in targets:
                    ntf_id = f"NTF{uuid.uuid4().int % 10**6:06d}"
                    try:
                        response = await client.post(
                            f"{MAIN_API_URL}/internal/notifications",
                            json={
                                "notification_id": ntf_id,
                                "recipient_type": recipient_type,
                                "recipient_id": recipient_id,
                                "message": msg_text,
                            },
                        )
                        response.raise_for_status()
                    except Exception as e:
                        logger.warning(f"Failed to persist notification to Main API: {e}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")


async def main() -> None:
    logger.info("Notification service starting...")

    while True:
        try:
            connection = await aio_pika.connect_robust(
                RABBITMQ_URL,
                reconnect_interval=5,
            )
            break
        except Exception as e:
            logger.warning(f"RabbitMQ not ready, retrying in 5s: {e}")
            await asyncio.sleep(5)

    channel = await connection.channel()
    await channel.set_qos(prefetch_count=10)

    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    for key in ROUTING_KEYS:
        await queue.bind(exchange, routing_key=key)

    logger.info(f"Consuming from {QUEUE_NAME} on exchange {EXCHANGE_NAME}, keys={ROUTING_KEYS}")
    await queue.consume(handle_message)

    try:
        await asyncio.Future()  # run forever
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
