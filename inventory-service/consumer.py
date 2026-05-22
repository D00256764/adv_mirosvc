"""
Inventory Service 

Listens for:
  order.created > check stock > reserve > publish inventory.reserved or inventory.insufficient
  payment.failed > release reservation > publish inventory.released

Each incoming event_id is recorded in Redis with a 24-hour TTL
so duplicate deliveries are silently skipped.

Atomic writes use Redis MULTI/EXEC pipelines so stock changes and the outbox
entry are always committed together.
"""
from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis

from events import Event, EventType

logger = logging.getLogger(__name__)

_SEED_STOCK: dict[str, int] = {
    "shoes": 100,
    "leggings": 50,
    "yoga-mat": 200,
}


async def seed_inventory(redis: Redis) -> None:
    for product_id, qty in _SEED_STOCK.items():
        if not await redis.exists(f"stock:{product_id}"):
            await redis.set(f"stock:{product_id}", qty)
    logger.info("Inventory seeded: %s", list(_SEED_STOCK.keys()))


async def _already_processed(redis: Redis, event_id: str) -> bool:
    if await redis.exists(f"processed:{event_id}"):
        return True
    return False


async def _mark_processed(pipe, event_id: str) -> None:
    pipe.set(f"processed:{event_id}", 1, ex=86400)


async def handle_order_created(redis: Redis, event: Event) -> None:
    if await _already_processed(redis, event.event_id):
        logger.info(f"Duplicate event {event.event_id}, skipping")
        return

    items: list[dict] = event.payload.get("items", [])

    for item in items:
        raw_stock = await redis.get(f"stock:{item['product_id']}")
        available = int(raw_stock) if raw_stock else 0
        if available < item["quantity"]:
            insufficient = Event(
                event_type=EventType.INVENTORY_INSUFFICIENT,
                order_id=event.order_id,
                payload={
                    "product_id": item["product_id"],
                    "available": available,
                    "required": item["quantity"],
                },
            )
            async with redis.pipeline(transaction=True) as pipe:
                pipe.lpush("outbox:pending", insufficient.to_bytes())
                await _mark_processed(pipe, event.event_id)
                await pipe.execute()
            logger.warning(f"Insufficient stock for order {event.order_id}")
            return

    reserved = Event(
        event_type=EventType.INVENTORY_RESERVED,
        order_id=event.order_id,
        payload={"items": items, "total": event.payload.get("total")},
    )
    async with redis.pipeline(transaction=True) as pipe:
        for item in items:
            pipe.decrby(f"stock:{item['product_id']}", item["quantity"])
            pipe.set(f"reservation:{event.order_id}:{item['product_id']}", item["quantity"])
        pipe.lpush("outbox:pending", reserved.to_bytes())
        await _mark_processed(pipe, event.event_id)
        await pipe.execute()
    logger.info(f"Inventory reserved for order {event.order_id}")


async def handle_payment_failed(redis: Redis, event: Event) -> None:
    if await _already_processed(redis, event.event_id):
        return

    items: list[dict] = event.payload.get("items", [])

    # Compensating transaction: restore stock
    release = Event(
        event_type=EventType.INVENTORY_RELEASED,
        order_id=event.order_id,
        payload={"items": items},
    )
    async with redis.pipeline(transaction=True) as pipe:
        for item in items:
            pipe.incrby(f"stock:{item['product_id']}", item["quantity"])
            pipe.delete(f"reservation:{event.order_id}:{item['product_id']}")
        pipe.lpush("outbox:pending", release.to_bytes())
        await _mark_processed(pipe, event.event_id)
        await pipe.execute()
    logger.info(f"Inventory released (compensating) for order {event.order_id}")


async def run_inventory_consumer(redis: Redis, kafka_bootstrap: str) -> None:
    consumer = AIOKafkaConsumer(
        EventType.ORDER_CREATED.value,
        EventType.PAYMENT_FAILED.value,
        bootstrap_servers=kafka_bootstrap,
        group_id="inventory-service-group",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Inventory consumer started")
    try:
        async for msg in consumer:
            try:
                event = Event.from_bytes(msg.value)
                if event.event_type == EventType.ORDER_CREATED:
                    await handle_order_created(redis, event)
                elif event.event_type == EventType.PAYMENT_FAILED:
                    await handle_payment_failed(redis, event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"Inventory consumer error: {exc}")
    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
