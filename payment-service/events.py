from __future__ import annotations

from enum import Enum
from typing import Any
import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field


class EventType(str, Enum):
    ORDER_CREATED = "order.created"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_INSUFFICIENT = "inventory.insufficient"
    INVENTORY_RELEASED = "inventory.released"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    ORDER_COMPLETED = "order.completed"
    ORDER_CANCELLED = "order.cancelled"


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    order_id: str
    payload: dict[str, Any] = {}
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> "Event":
        return cls.model_validate_json(data)
