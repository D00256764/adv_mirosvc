import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis

from consumer import run_payment_consumer
from outbox import run_outbox_publisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6381")
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

    _tasks.append(asyncio.create_task(run_payment_consumer(redis, KAFKA_BOOTSTRAP)))
    _tasks.append(asyncio.create_task(run_outbox_publisher(redis, producer)))

    yield

    for t in _tasks:
        t.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    await producer.stop()
    await redis.aclose()


app = FastAPI(title="Payment Service", lifespan=lifespan)


@app.get("/payments/{order_id}")
async def get_payment(order_id: str):
    raw = await redis.get(f"payment:{order_id}")
    if not raw:
        raise HTTPException(status_code=404, detail="Payment not found")
    return json.loads(raw)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "payment-service"}
