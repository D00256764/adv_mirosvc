"""
Outbox publisher for Inventory Service; identical recovery + publish loop
as used in every service. Each service has its own Redis instance and its
own outbox:pending/outbox:processing lists.
"""
from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaProducer
from redis.asyncio import Redis

from events import Event

logger = logging.getLogger(__name__)


async def run_outbox_publisher(redis: Redis, producer: AIOKafkaProducer) -> None:
    recovered = 0
    while await redis.lmove("outbox:processing", "outbox:pending", "RIGHT", "LEFT"):
        recovered += 1
    if recovered:
        logger.info(f"Recovered {recovered} events from outbox:processing after restart")

    logger.info("Inventory outbox publisher started")
    while True:
        try:
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
