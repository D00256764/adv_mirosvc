"""
CQRS - Read-side Projection Consumer

Maintains order_summary:{order_id} in Redis as an optimised read model.
The GET /orders/{id} endpoint reads from here, never from the write model.

Every domain event updates the projection so the read model stays eventually
consistent with the write side.  The projection consumer runs in a separate
consumer group so it sees all events independently of the saga listener.
"""
from __future__ import annotations

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis

from events import Event, EventType

logger = logging.getLogger(__name__)

_PROJECTION_TOPICS = [
    EventType.ORDER_CREATED.value,
    EventType.INVENTORY_RESERVED.value,
    EventType.INVENTORY_INSUFFICIENT.value,
    EventType.PAYMENT_SUCCEEDED.value,
    EventType.PAYMENT_FAILED.value,
    EventType.ORDER_COMPLETED.value,
    EventType.ORDER_CANCELLED.value,
]

_STATUS_MAP: dict[EventType, str] = {
    EventType.ORDER_CREATED: "PENDING",
    EventType.INVENTORY_RESERVED: "INVENTORY_RESERVED",
    EventType.INVENTORY_INSUFFICIENT: "CANCELLED",
    EventType.PAYMENT_SUCCEEDED: "PAYMENT_PROCESSING",
    EventType.PAYMENT_FAILED: "CANCELLED",
    EventType.ORDER_COMPLETED: "COMPLETED",
    EventType.ORDER_CANCELLED: "CANCELLED",
}


async def run_projection_consumer(redis: Redis, kafka_bootstrap: str) -> None:
    consumer = AIOKafkaConsumer(
        *_PROJECTION_TOPICS,
        bootstrap_servers=kafka_bootstrap,
        group_id="order-projection-group",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Projection consumer started")
    try:
        async for msg in consumer:
            try:
                event = Event.from_bytes(msg.value)
                await _update_projection(redis, event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"Projection consumer error: {exc}")
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


async def _update_projection(redis: Redis, event: Event) -> None:
    key = f"order_summary:{event.order_id}"

    if event.event_type == EventType.ORDER_CREATED:
        summary = {
            "order_id": event.order_id,
            "customer_id": event.payload.get("customer_id"),
            "items": event.payload.get("items", []),
            "total": event.payload.get("total"),
            "status": "PENDING",
            "event_log": [event.event_type.value],
        }
    else:
        raw = await redis.get(key)
        if not raw:
            return
        summary = json.loads(raw)
        summary["status"] = _STATUS_MAP.get(event.event_type, summary["status"])
        summary["event_log"].append(event.event_type.value)

    await redis.set(key, json.dumps(summary))
    logger.info(f"Projection {event.order_id} → {summary['status']}")


async def get_order_summary(redis: Redis, order_id: str) -> dict | None:
    raw = await redis.get(f"order_summary:{order_id}")
    return json.loads(raw) if raw else None
