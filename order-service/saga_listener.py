"""
Saga Choreography — Order Service listener

This service participates in the saga as both initiator (POST /orders emits
order.created) and a downstream compensating step.

Happy path:   payment.succeeded  → update order COMPLETED
Failure path: payment.failed     → update order CANCELLED
              inventory.insufficient → update order CANCELLED

The compensating write is itself wrapped in the outbox so order.completed /
order.cancelled also flow through Kafka consistently.
"""
from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis

from events import Event, EventType
from models import OrderStatus
from outbox import update_order_status_with_outbox

logger = logging.getLogger(__name__)

_SAGA_TOPICS = [
    EventType.PAYMENT_SUCCEEDED.value,
    EventType.PAYMENT_FAILED.value,
    EventType.INVENTORY_INSUFFICIENT.value,
]


async def run_saga_listener(redis: Redis, kafka_bootstrap: str) -> None:
    consumer = AIOKafkaConsumer(
        *_SAGA_TOPICS,
        bootstrap_servers=kafka_bootstrap,
        group_id="order-saga-group",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Saga listener started")
    try:
        async for msg in consumer:
            try:
                event = Event.from_bytes(msg.value)
                await _handle(redis, event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"Saga listener error: {exc}")
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()


async def _handle(redis: Redis, event: Event) -> None:
    order_id = event.order_id

    if event.event_type == EventType.PAYMENT_SUCCEEDED:
        completion = Event(
            event_type=EventType.ORDER_COMPLETED,
            order_id=order_id,
            payload={"reason": "payment succeeded"},
        )
        await update_order_status_with_outbox(redis, order_id, OrderStatus.COMPLETED, completion)
        logger.info(f"Order {order_id} → COMPLETED")

    elif event.event_type in (EventType.PAYMENT_FAILED, EventType.INVENTORY_INSUFFICIENT):
        cancellation = Event(
            event_type=EventType.ORDER_CANCELLED,
            order_id=order_id,
            payload={"reason": event.event_type.value},
        )
        await update_order_status_with_outbox(redis, order_id, OrderStatus.CANCELLED, cancellation)
        logger.info(f"Order {order_id} → CANCELLED (trigger: {event.event_type})")
