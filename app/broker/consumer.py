"""Консьюмер платежей. Запуск: uv run faststream run app.broker.consumer:app."""

import logging
from datetime import UTC, datetime

import httpx
from dishka_faststream import FromDishka, inject, setup_dishka
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker.messages import PaymentEvent
from app.broker.names import RK_DLQ, RK_RETRY_2S, RK_RETRY_4S
from app.broker.topology import (
    declare_topology,
    exchange_dlx,
    exchange_payments,
    exchange_retry,
    queue_payments_new,
)
from app.config import Settings
from app.di import make_container
from app.logging import reset_stdlib_handlers, setup_logging
from app.models import Payment, PaymentStatus
from app.services.gateway import emulate_gateway
from app.services.webhook import send_webhook

settings = Settings()
setup_logging(settings)
reset_stdlib_handlers("faststream")

# свой логгер без хендлеров: записи брокера идут через перехват в loguru,
# иначе faststream создаст собственный с propagate=False и чужим форматом
broker = RabbitBroker(settings.rabbitmq_url, logger=logging.getLogger("faststream.broker"))
app = FastStream(broker)
setup_dishka(make_container(settings), app=app, finalize_container=True)


@app.on_startup
async def _prepare_topology() -> None:
    await broker.connect()
    await declare_topology(broker)


async def _route_failure(
    broker: RabbitBroker, event: PaymentEvent, attempt: int, exc: Exception
) -> None:
    """Уводит событие в retry-очередь либо в DLQ по номеру попытки."""
    payload = event.model_dump(mode="json")
    if attempt >= 3:
        await broker.publish(
            payload,
            exchange=exchange_dlx,
            routing_key=RK_DLQ,
            headers={"x-attempt": attempt, "x-last-error": str(exc)[:500]},
            persist=True,
            content_type="application/json",
        )
        logger.error("Платёж {}: попытки исчерпаны, ушло в DLQ ({})", event.payment_id, exc)
        return

    delay, routing_key = (2, RK_RETRY_2S) if attempt == 1 else (4, RK_RETRY_4S)
    await broker.publish(
        payload,
        exchange=exchange_retry,
        routing_key=routing_key,
        headers={"x-attempt": attempt + 1},
        persist=True,
        content_type="application/json",
    )
    logger.warning("Платёж {}: повтор через {}с ({})", event.payment_id, delay, exc)


async def process_payment(
    event: PaymentEvent,
    attempt: int,
    session: AsyncSession,
    settings: Settings,
    http_client: httpx.AsyncClient,
    broker: RabbitBroker,
) -> None:
    """Проводит платёж через шлюз и доставляет вебхук, идемпотентно к повторной доставке."""
    logger.info("Получено событие payment_id={}, попытка {}", event.payment_id, attempt)

    try:
        # лок строки намеренно держится на время работы шлюза: конкурентные доставки
        # одного платежа сериализуются, двойной вызов шлюза дороже недолгого лока
        payment = await session.get(Payment, event.payment_id, with_for_update=True)
        if payment is None:
            logger.warning("Платёж {} не найден, событие уходит в DLQ", event.payment_id)
            await broker.publish(
                event.model_dump(mode="json"),
                exchange=exchange_dlx,
                routing_key=RK_DLQ,
                headers={"x-attempt": attempt, "x-last-error": "платёж не найден"},
                persist=True,
                content_type="application/json",
            )
            return

        if payment.status == PaymentStatus.PENDING:
            result = await emulate_gateway(settings)
            payment.status = result
            payment.processed_at = datetime.now(UTC)
            await session.commit()
            logger.info("Шлюз вернул {} для платежа {}", result.value, payment.id)
        else:
            logger.info(
                "Платёж {} уже в статусе {}, шлюз пропущен (повторная доставка)",
                payment.id,
                payment.status.value,
            )

        if payment.webhook_delivered_at is None:
            await send_webhook(http_client, payment, settings.webhook_timeout)
            payment.webhook_delivered_at = datetime.now(UTC)
            await session.commit()
        else:
            logger.info("Вебхук по платежу {} уже доставлен, пропуск", payment.id)
    except Exception as exc:  # любые инфраструктурные сбои (БД, сеть, вебхук) уводим в retry
        await _route_failure(broker, event, attempt, exc)


@broker.subscriber(
    queue_payments_new,
    exchange_payments,
    ack_policy=AckPolicy.REJECT_ON_ERROR,
    channel=Channel(prefetch_count=10),
)
@inject
async def handle_payment(
    event: PaymentEvent,
    msg: RabbitMessage,
    session: FromDishka[AsyncSession],
    settings: FromDishka[Settings],
    http_client: FromDishka[httpx.AsyncClient],
) -> None:
    """Тонкий адаптер подписки: извлекает номер попытки и делегирует обработку."""
    attempt = int(msg.headers.get("x-attempt", 1))
    await process_payment(event, attempt, session, settings, http_client, broker)
