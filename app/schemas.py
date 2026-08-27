"""Pydantic-схемы HTTP API."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Запрос на создание платежа."""

    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    currency: Currency
    description: str | None = None
    metadata: dict | None = None
    webhook_url: HttpUrl


class PaymentCreated(BaseModel):
    """Ответ о принятом в обработку платеже."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    """Полное представление платежа."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    meta: dict | None = Field(default=None, serialization_alias="metadata")
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
