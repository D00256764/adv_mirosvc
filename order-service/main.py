import asyncio
import logging
import os
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis

from events import Event, EventType
from models import CreateOrderRequest, Order
from outbox import run_outbox_publisher, write_order_with_outbox
from projection import get_order_summary, run_projection_consumer
from saga_listener import run_saga_listener

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

redis: Redis
producer: AIOKafkaProducer
_tasks: list[asyncio.Task] = []


async def _start_kafka_producer(max_attempts: int = 30, delay_s: float = 5.0) -> AIOKafkaProducer:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        kafka = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
        try:
            await kafka.start()
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return kafka
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Kafka not ready (attempt %s/%s): %s",
                attempt,
                max_attempts,
                exc,
            )
            try:
                await kafka.stop()
            except Exception:
                pass
            await asyncio.sleep(delay_s)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP}") from last_error


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis, producer

    redis = Redis.from_url(REDIS_URL, decode_responses=False)
    producer = await _start_kafka_producer()

    _tasks.append(asyncio.create_task(run_outbox_publisher(redis, producer)))
    _tasks.append(asyncio.create_task(run_saga_listener(redis, KAFKA_BOOTSTRAP)))
    _tasks.append(asyncio.create_task(run_projection_consumer(redis, KAFKA_BOOTSTRAP)))

    yield

    for t in _tasks:
        t.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    await producer.stop()
    await redis.aclose()


app = FastAPI(title="Order Service", lifespan=lifespan)


@app.post("/orders", status_code=201)
async def create_order(body: CreateOrderRequest):
    order = Order(customer_id=body.customer_id, items=body.items)
    event = Event(
        event_type=EventType.ORDER_CREATED,
        order_id=order.order_id,
        payload={
            "customer_id": order.customer_id,
            "items": [i.model_dump() for i in order.items],
            "total": order.total,
        },
    )
    await write_order_with_outbox(redis, order, event)
    logger.info(f"Order {order.order_id} created")
    return {"order_id": order.order_id, "status": order.status, "total": order.total}


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    """CQRS read endpoint — returns the optimised read projection."""
    summary = await get_order_summary(redis, order_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Order not found")
    return summary


@app.get("/health")
async def health():
    return {"status": "ok", "service": "order-service"}
