"""Outbox-релей: публикует накопленные события в брокер."""

import asyncio
import signal
from contextlib import suppress
from datetime import UTC, datetime

from dishka import AsyncContainer
from faststream.rabbit import RabbitBroker
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.topology import declare_topology, exchange_payments
from app.config import Settings
from app.di import make_container
from app.logging import setup_logging
from app.models import OutboxMessage


async def publish_batch(broker: RabbitBroker, session: AsyncSession, batch_size: int) -> int:
    """Публикует пачку неотправленных сообщений outbox, возвращает число опубликованных."""
    async with session.begin():
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = (await session.scalars(stmt)).all()

        published = 0
        for row in rows:
            try:
                await broker.publish(
                    row.payload,
                    exchange=exchange_payments,
                    routing_key=row.routing_key,
                    headers={"x-attempt": 1},
                    persist=True,
                    content_type="application/json",
                )
            except Exception:
                # фиксируем уже опубликованное, остаток подберёт следующий тик
                logger.exception("Ошибка публикации outbox id={}", row.id)
                break
            row.published_at = datetime.now(UTC)
            published += 1

    return published


async def run_loop(
    broker: RabbitBroker,
    container: AsyncContainer,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    """Цикл публикации: тик каждые outbox_poll_interval секунд до сигнала остановки."""
    while not stop.is_set():
        try:
            async with container() as request_container:
                session = await request_container.get(AsyncSession)
                published = await publish_batch(broker, session, settings.outbox_batch_size)
            if published:
                logger.info("Опубликовано {} событий из outbox", published)
        except Exception:
            logger.exception("Сбой тика релея")
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval)


def _install_stop_handlers(stop: asyncio.Event) -> None:
    """Вешает обработчики SIGINT/SIGTERM; на Windows остаётся KeyboardInterrupt."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)


async def main() -> None:
    """Точка входа релея: подключение к брокеру и цикл публикации до сигнала остановки."""
    settings = Settings()
    setup_logging(settings)
    container = make_container(settings)
    stop = asyncio.Event()
    _install_stop_handlers(stop)

    broker = RabbitBroker(settings.rabbitmq_url)
    try:
        async with broker:
            await declare_topology(broker)
            logger.info("Outbox-релей запущен, интервал {} с", settings.outbox_poll_interval)
            await run_loop(broker, container, settings, stop)
    finally:
        await container.close()
    logger.info("Outbox-релей остановлен")
