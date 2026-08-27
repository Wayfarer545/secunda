"""Публикация накопленных outbox-сообщений релеем."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.names import EXCHANGE_PAYMENTS, RK_PAYMENTS_NEW
from app.models import OutboxMessage
from app.relay.runner import publish_batch
from tests.conftest import FakeBroker


async def _seed_pending(session: AsyncSession, count: int) -> list[OutboxMessage]:
    """Кладёт в outbox count неотправленных сообщений."""
    rows = [
        OutboxMessage(routing_key=RK_PAYMENTS_NEW, payload={"payment_id": f"p-{i}"})
        for i in range(count)
    ]
    session.add_all(rows)
    await session.commit()
    return rows


async def _pending_ids(session: AsyncSession) -> list[int]:
    """Идентификаторы неопубликованных строк по возрастанию."""
    stmt = (
        select(OutboxMessage.id)
        .where(OutboxMessage.published_at.is_(None))
        .order_by(OutboxMessage.id)
    )
    return list(await session.scalars(stmt))


async def test_publishes_pending_in_id_order(
    session: AsyncSession, fake_broker: FakeBroker
) -> None:
    """Неотправленные строки публикуются по возрастанию id и помечаются published_at."""
    await _seed_pending(session, 3)

    published = await publish_batch(fake_broker, session, batch_size=10)

    assert published == 3
    assert [c["payload"]["payment_id"] for c in fake_broker.published] == ["p-0", "p-1", "p-2"]
    assert all(c["exchange"] == EXCHANGE_PAYMENTS for c in fake_broker.published)
    assert all(c["routing_key"] == RK_PAYMENTS_NEW for c in fake_broker.published)
    assert all(c["headers"] == {"x-attempt": 1} for c in fake_broker.published)
    assert await _pending_ids(session) == []


async def test_skips_already_published(session: AsyncSession, fake_broker: FakeBroker) -> None:
    """Уже опубликованные строки не публикуются повторно."""
    rows = await _seed_pending(session, 3)
    rows[0].published_at = datetime.now(UTC)
    rows[1].published_at = datetime.now(UTC)
    await session.commit()

    published = await publish_batch(fake_broker, session, batch_size=10)

    assert published == 1
    assert [c["payload"]["payment_id"] for c in fake_broker.published] == ["p-2"]


async def test_respects_batch_size(session: AsyncSession, fake_broker: FakeBroker) -> None:
    """За тик публикуется не больше batch_size строк, следующий тик добирает остаток."""
    await _seed_pending(session, 5)

    assert await publish_batch(fake_broker, session, batch_size=2) == 2
    assert [c["payload"]["payment_id"] for c in fake_broker.published] == ["p-0", "p-1"]
    assert len(await _pending_ids(session)) == 3

    # publish_batch сам открывает транзакцию, сессии нужен чистый статус
    await session.rollback()
    assert await publish_batch(fake_broker, session, batch_size=2) == 2
    assert [c["payload"]["payment_id"] for c in fake_broker.published[2:]] == ["p-2", "p-3"]


async def test_publish_failure_keeps_row_pending(
    session: AsyncSession, fake_broker: FakeBroker
) -> None:
    """При сбое публикации упавшая строка остаётся pending, успехи до неё зафиксированы."""
    await _seed_pending(session, 3)
    fake_broker.fail_on_call = 2

    published = await publish_batch(fake_broker, session, batch_size=10)

    assert published == 1
    assert [c["payload"]["payment_id"] for c in fake_broker.published] == ["p-0"]
    await session.rollback()
    assert len(await _pending_ids(session)) == 2

    fake_broker.fail_on_call = None
    await session.rollback()
    assert await publish_batch(fake_broker, session, batch_size=10) == 2
    assert await _pending_ids(session) == []
