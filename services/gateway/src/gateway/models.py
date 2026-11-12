"""HTTP request/response models."""

from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    quantity: int = Field(ge=1, le=1000)
    unit_price: Decimal = Field(gt=0, max_digits=10, decimal_places=2)


class OrderRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=64)
    currency: str = Field(default="EUR", pattern=r"^[A-Z]{3}$")
    items: list[OrderItem] = Field(min_length=1, max_length=100)


class OrderAccepted(BaseModel):
    order_id: str
    status_url: str


class Step(BaseModel):
    task: str
    task_id: str
    state: str


class OrderStatus(BaseModel):
    order_id: str
    status: Literal["PENDING", "PROCESSING", "SUCCESS", "FAILURE"]
    steps: list[Step]
    result: Any = None
    error: str | None = None
