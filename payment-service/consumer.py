"""
Payment Service

Listens for:
  inventory.reserved > attempt payment > publish payment.succeeded or payment.failed

Payment rule: orders with total > 500 are rejected so you can test the
compensating-action path without any extra setup.

The payment record and outbox entry are written atomically so the outcome
is never lost even if the service crashes mid-publish.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis

from events import Event, EventType

logger = logging.getLogger(__name__)


async def _process_payment(total: float) -> bool:
    """Approve if total <= 500, reject otherwise (deterministic for demos)."""
    await asyncio.sleep(0.05)  # simulate I/O latency
    return total <= 500.0


async def handle_inventory_reserved(redis: Redis, event: Event) -> None:
    if await redis.exists(f"processed:{event.event_id}"):
        logger.info(f"Duplicate event {event.event_id}, skipping")
        return

    total: float = event.payload.get("total", 0.0)
    items: list = event.payload.get("items", [])
    order_id = event.order_id

    success = await _process_payment(total)

    outcome = Event(
        event_type=EventType.PAYMENT_SUCCEEDED if success else EventType.PAYMENT_FAILED,
        order_id=order_id,
        payload={
            "total": total,
            "items": items,
            **({"reason": "total exceeds limit"} if not success else {}),
        },
    )

    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(f"payment:{order_id}", outcome.model_dump_json())
        pipe.lpush("outbox:pending", outcome.to_bytes())
        pipe.set(f"processed:{event.event_id}", 1, ex=86400)
        await pipe.execute()

    if success:
        logger.info(f"Payment succeeded for order {order_id} (total: {total})")
    else:
        logger.warning(f"Payment FAILED for order {order_id} (total: {total} > 500)")


async def run_payment_consumer(redis: Redis, kafka_bootstrap: str) -> None:
    consumer = AIOKafkaConsumer(
        EventType.INVENTORY_RESERVED.value,
        bootstrap_servers=kafka_bootstrap,
        group_id="payment-service-group",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Payment consumer started")
    try:
        async for msg in consumer:
            try:
                event = Event.from_bytes(msg.value)
                if event.event_type == EventType.INVENTORY_RESERVED:
                    await handle_inventory_reserved(redis, event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"Payment consumer error: {exc}")
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
