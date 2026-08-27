"""Идемпотентность создания платежа и записи в outbox."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.names import RK_PAYMENTS_NEW
from app.models import OutboxMessage
from app.schemas import PaymentCreate
from app.services.payments import create_payment


def _payment_data() -> PaymentCreate:
    return PaymentCreate(
        amount="10.50",
        currency="USD",
        description="заказ 42",
        metadata={"order_id": "42"},
        webhook_url="https://example.com/hook",
    )


async def test_duplicate_key_returns_same_payment(session: AsyncSession) -> None:
    """Повтор ключа возвращает тот же платёж с created=False и одной записью outbox."""
    first, created_first = await create_payment(session, _payment_data(), "key-1")
    second, created_second = await create_payment(session, _payment_data(), "key-1")

    assert created_first is True
    assert created_second is False
    assert second.id == first.id

    outbox = (await session.scalars(select(OutboxMessage))).all()
    assert len(outbox) == 1
    assert outbox[0].routing_key == RK_PAYMENTS_NEW
    assert outbox[0].payload["payment_id"] == str(first.id)
    assert "occurred_at" in outbox[0].payload


async def test_different_keys_create_separate_payments(session: AsyncSession) -> None:
    """Разные ключи создают независимые платежи и записи outbox."""
    first, _ = await create_payment(session, _payment_data(), "key-1")
    second, _ = await create_payment(session, _payment_data(), "key-2")

    assert first.id != second.id
    outbox = (await session.scalars(select(OutboxMessage))).all()
    assert len(outbox) == 2
    assert {row.payload["payment_id"] for row in outbox} == {str(first.id), str(second.id)}
