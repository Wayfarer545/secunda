import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

JSON_VARIANT = sa.JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    """Базовый класс моделей."""

    metadata = sa.MetaData(naming_convention=NAMING_CONVENTION)


class PaymentStatus(StrEnum):
    """Статус платежа."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Currency(StrEnum):
    """Валюта платежа."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class Payment(Base):
    """Платёж."""

    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(sa.Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(sa.Numeric(12, 2))
    currency: Mapped[Currency] = mapped_column(sa.Enum(Currency, native_enum=False, length=3))
    description: Mapped[str | None] = mapped_column(sa.Text)
    meta: Mapped[dict | None] = mapped_column("metadata", JSON_VARIANT)
    status: Mapped[PaymentStatus] = mapped_column(
        sa.Enum(
            PaymentStatus,
            native_enum=False,
            length=16,
            values_callable=lambda e: [m.value for m in e],
        ),
        default=PaymentStatus.PENDING,
        index=True,
    )
    idempotency_key: Mapped[str] = mapped_column(sa.String(255), unique=True)
    webhook_url: Mapped[str] = mapped_column(sa.String(2048))
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))


class OutboxMessage(Base):
    """Сообщение transactional outbox для публикации в брокер."""

    __tablename__ = "outbox"
    __table_args__ = (
        sa.Index("ix_outbox_unpublished", "id", postgresql_where=sa.text("published_at IS NULL")),
    )

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer(), "sqlite"), sa.Identity(), primary_key=True
    )
    routing_key: Mapped[str] = mapped_column(sa.String(255))
    payload: Mapped[dict] = mapped_column(JSON_VARIANT)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
