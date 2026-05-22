from __future__ import annotations

from enum import Enum
import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class OrderItem(BaseModel):
    product_id: str
    quantity: int
    unit_price: float


class CreateOrderRequest(BaseModel):
    customer_id: str
    items: list[OrderItem]


class Order(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    items: list[OrderItem]
    status: OrderStatus = OrderStatus.PENDING
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    updated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def total(self) -> float:
        return sum(item.quantity * item.unit_price for item in self.items)
