import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis

from consumer import run_inventory_consumer, seed_inventory
from outbox import run_outbox_publisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

redis: Redis
producer: AIOKafkaProducer
_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis, producer

    redis = Redis.from_url(REDIS_URL, decode_responses=False)
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()

    await seed_inventory(redis)

    _tasks.append(asyncio.create_task(run_inventory_consumer(redis, KAFKA_BOOTSTRAP)))
    _tasks.append(asyncio.create_task(run_outbox_publisher(redis, producer)))

    yield

    for t in _tasks:
        t.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    await producer.stop()
    await redis.aclose()


app = FastAPI(title="Inventory Service", lifespan=lifespan)


@app.get("/stock/{product_id}")
async def get_stock(product_id: str):
    raw = await redis.get(f"stock:{product_id}")
    if raw is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"product_id": product_id, "stock": int(raw)}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "inventory-service"}
