"""
Transactional Outbox Pattern - Order Service

write_order_with_outbox/update_order_status_with_outbox each use Redis
MULTI/EXEC (pipeline transaction=True) so the business write and the outbox
entry are committed atomically; either both land or neither does.

run_outbox_publisher polls outbox:pending, moves each entry to
outbox:processing atomically via LMOVE, publishes to Kafka, then removes it.
On crash, the recovery step at startup drains outbox:processing back into
outbox:pending so no events are lost.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from redis.asyncio import Redis

from events import Event
from models import Order, OrderStatus

logger = logging.getLogger(__name__)


async def write_order_with_outbox(redis: Redis, order: Order, event: Event) -> None:
    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(f"order:{order.order_id}", order.model_dump_json())
        pipe.lpush("outbox:pending", event.to_bytes())
        await pipe.execute()


async def update_order_status_with_outbox(
    redis: Redis, order_id: str, new_status: OrderStatus, event: Event
) -> None:
    raw = await redis.get(f"order:{order_id}")
    if not raw:
        logger.warning(f"Order {order_id} not found for status update")
        return
    order = Order.model_validate_json(raw)
    order.status = new_status
    order.updated_at = datetime.now(timezone.utc).isoformat()

    async with redis.pipeline(transaction=True) as pipe:
        pipe.set(f"order:{order.order_id}", order.model_dump_json())
        pipe.lpush("outbox:pending", event.to_bytes())
        await pipe.execute()


async def run_outbox_publisher(redis: Redis, producer: AIOKafkaProducer) -> None:
    # Recover events left in processing from a previous crash
    recovered = 0
    while await redis.lmove("outbox:processing", "outbox:pending", "RIGHT", "LEFT"):
        recovered += 1
    if recovered:
        logger.info(f"Recovered {recovered} events from outbox:processing after restart")

    logger.info("Outbox publisher started")
    while True:
        try:
            # Atomic move: pop oldest from pending, push to processing
            raw = await redis.lmove("outbox:pending", "outbox:processing", "RIGHT", "LEFT")
            if raw:
                event = Event.from_bytes(raw)
                await producer.send_and_wait(event.event_type.value, value=raw)
                await redis.lrem("outbox:processing", 1, raw)
                logger.info(f"Published {event.event_type} for order {event.order_id}")
            else:
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Outbox publisher error: {exc}")
            await asyncio.sleep(1)
