"""Логика обработки платёжного события консьюмером."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker import consumer
from app.broker.messages import PaymentEvent
from app.broker.names import EXCHANGE_DLX, EXCHANGE_RETRY, RK_DLQ, RK_RETRY_2S, RK_RETRY_4S
from app.config import Settings
from app.models import Payment, PaymentStatus
from app.services.webhook import WebhookDeliveryError
from tests.conftest import FakeBroker


class GatewayStub:
    """Управляемый шлюз: фиксирует вызовы и возвращает заданный статус."""

    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.result = result
        self.calls = 0

    async def __call__(self, settings: Settings) -> PaymentStatus:
        """Возвращает подготовленный исход без задержек."""
        self.calls += 1
        return self.result


class WebhookStub:
    """Управляемый вебхук: фиксирует вызовы, при необходимости падает."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def __call__(self, client: Any, payment: Payment, timeout: float) -> None:
        """Имитирует доставку либо поднимает подготовленную ошибку."""
        self.calls += 1
        if self.error is not None:
            raise self.error


Wiring = Callable[[GatewayStub, WebhookStub], None]


@pytest.fixture
def wire(monkeypatch: pytest.MonkeyPatch) -> Wiring:
    """Подставляет стабы шлюза и вебхука в модуль консьюмера."""

    def _wire(gateway: GatewayStub, webhook: WebhookStub) -> None:
        monkeypatch.setattr(consumer, "emulate_gateway", gateway)
        monkeypatch.setattr(consumer, "send_webhook", webhook)

    return _wire


async def _run(
    payment_id: UUID,
    session: AsyncSession,
    settings: Settings,
    broker: FakeBroker,
    attempt: int = 1,
) -> None:
    """Прогоняет событие через process_payment."""
    event = PaymentEvent(payment_id=payment_id, occurred_at=datetime.now(UTC))
    await consumer.process_payment(event, attempt, session, settings, None, broker)


async def test_pending_payment_succeeded(
    session: AsyncSession,
    settings: Settings,
    fake_broker: FakeBroker,
    wire: Wiring,
    payment_factory: Callable[..., Payment],
) -> None:
    """Успех шлюза: статус succeeded, вебхук доставлен, ретраев нет."""
    payment = payment_factory()
    session.add(payment)
    await session.commit()
    gateway, webhook = GatewayStub(PaymentStatus.SUCCEEDED), WebhookStub()
    wire(gateway, webhook)

    await _run(payment.id, session, settings, fake_broker)

    await session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.webhook_delivered_at is not None
    assert gateway.calls == 1
    assert webhook.calls == 1
    assert fake_broker.published == []


async def test_pending_payment_failed_is_business_outcome(
    session: AsyncSession,
    settings: Settings,
    fake_broker: FakeBroker,
    wire: Wiring,
    payment_factory: Callable[..., Payment],
) -> None:
    """Отказ шлюза — бизнес-исход: статус failed, вебхук уходит, ретрая нет."""
    payment = payment_factory()
    session.add(payment)
    await session.commit()
    gateway, webhook = GatewayStub(PaymentStatus.FAILED), WebhookStub()
    wire(gateway, webhook)

    await _run(payment.id, session, settings, fake_broker)

    await session.refresh(payment)
    assert payment.status == PaymentStatus.FAILED
    assert payment.processed_at is not None
    assert webhook.calls == 1
    assert fake_broker.published == []


async def test_redelivery_skips_gateway(
    session: AsyncSession,
    settings: Settings,
    fake_broker: FakeBroker,
    wire: Wiring,
    payment_factory: Callable[..., Payment],
) -> None:
    """Повторная доставка при терминальном статусе: шлюз не вызывается, вебхук уходит."""
    payment = payment_factory(status=PaymentStatus.SUCCEEDED, processed_at=datetime.now(UTC))
    session.add(payment)
    await session.commit()
    gateway, webhook = GatewayStub(), WebhookStub()
    wire(gateway, webhook)

    await _run(payment.id, session, settings, fake_broker)

    await session.refresh(payment)
    assert gateway.calls == 0
    assert webhook.calls == 1
    assert payment.webhook_delivered_at is not None
    assert fake_broker.published == []


async def test_redelivery_after_webhook_skips_everything(
    session: AsyncSession,
    settings: Settings,
    fake_broker: FakeBroker,
    wire: Wiring,
    payment_factory: Callable[..., Payment],
) -> None:
    """Полностью обработанный платёж: ни шлюз, ни вебхук не вызываются."""
    payment = payment_factory(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
        webhook_delivered_at=datetime.now(UTC),
    )
    session.add(payment)
    await session.commit()
    gateway, webhook = GatewayStub(), WebhookStub()
    wire(gateway, webhook)

    await _run(payment.id, session, settings, fake_broker)

    assert gateway.calls == 0
    assert webhook.calls == 0
    assert fake_broker.published == []


@pytest.mark.parametrize(
    ("attempt", "expected_rk", "next_attempt"),
    [(1, RK_RETRY_2S, 2), (2, RK_RETRY_4S, 3)],
)
async def test_webhook_failure_goes_to_retry(
    session: AsyncSession,
    settings: Settings,
    fake_broker: FakeBroker,
    wire: Wiring,
    payment_factory: Callable[..., Payment],
    attempt: int,
    expected_rk: str,
    next_attempt: int,
) -> None:
    """Сбой вебхука уводит событие в retry-очередь со счётчиком попыток."""
    payment = payment_factory()
    session.add(payment)
    await session.commit()
    gateway, webhook = GatewayStub(), WebhookStub(error=WebhookDeliveryError("ответ 500"))
    wire(gateway, webhook)

    await _run(payment.id, session, settings, fake_broker, attempt=attempt)

    assert len(fake_broker.published) == 1
    call = fake_broker.published[0]
    assert call["exchange"] == EXCHANGE_RETRY
    assert call["routing_key"] == expected_rk
    assert call["headers"] == {"x-attempt": next_attempt}
    assert call["payload"]["payment_id"] == str(payment.id)
    await session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None


async def test_webhook_failure_on_last_attempt_goes_to_dlq(
    session: AsyncSession,
    settings: Settings,
    fake_broker: FakeBroker,
    wire: Wiring,
    payment_factory: Callable[..., Payment],
) -> None:
    """Третий сбой вебхука отправляет событие в DLQ с текстом ошибки."""
    payment = payment_factory()
    session.add(payment)
    await session.commit()
    gateway, webhook = GatewayStub(), WebhookStub(error=WebhookDeliveryError("ответ 500"))
    wire(gateway, webhook)

    await _run(payment.id, session, settings, fake_broker, attempt=3)

    assert len(fake_broker.published) == 1
    call = fake_broker.published[0]
    assert call["exchange"] == EXCHANGE_DLX
    assert call["routing_key"] == RK_DLQ
    assert call["headers"]["x-attempt"] == 3
    assert "ответ 500" in call["headers"]["x-last-error"]


async def test_missing_payment_goes_to_dlq(
    session: AsyncSession,
    settings: Settings,
    fake_broker: FakeBroker,
    wire: Wiring,
) -> None:
    """Событие о несуществующем платеже уходит в DLQ без вызова шлюза и вебхука."""
    gateway, webhook = GatewayStub(), WebhookStub()
    wire(gateway, webhook)

    await _run(uuid4(), session, settings, fake_broker)

    assert gateway.calls == 0
    assert webhook.calls == 0
    assert len(fake_broker.published) == 1
    call = fake_broker.published[0]
    assert call["exchange"] == EXCHANGE_DLX
    assert call["routing_key"] == RK_DLQ
    assert call["headers"]["x-last-error"] == "платёж не найден"
