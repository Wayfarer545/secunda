"""Операции над платежами."""

from datetime import UTC, datetime
from uuid import UUID

from loguru import logger
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.names import RK_PAYMENTS_NEW
from app.models import OutboxMessage, Payment
from app.schemas import PaymentCreate


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> tuple[Payment, bool]:
    """Создаёт платёж и outbox-сообщение; при повторе ключа возвращает существующий платёж."""
    payment = Payment(
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        meta=data.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(data.webhook_url),
    )
    session.add(payment)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            raise
        logger.info("Повтор Idempotency-Key {}: возвращён платёж {}", idempotency_key, existing.id)
        return existing, False

    session.add(
        OutboxMessage(
            routing_key=RK_PAYMENTS_NEW,
            payload={
                "payment_id": str(payment.id),
                "occurred_at": datetime.now(UTC).isoformat(),
            },
        )
    )
    await session.commit()
    logger.info("Создан платёж {}", payment.id)
    return payment, True


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    """Возвращает платёж по идентификатору."""
    return await session.get(Payment, payment_id)
