"""HTTP API платежей через ASGI-клиент."""

from collections.abc import Callable
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxMessage, Payment

PAYMENTS_URL = "/api/v1/payments"


def _payment_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "amount": "10.50",
        "currency": "USD",
        "description": "заказ 42",
        "metadata": {"order_id": "42"},
        "webhook_url": "https://example.com/hook",
    }
    body.update(overrides)
    return body


async def test_create_payment(
    client: httpx.AsyncClient, session: AsyncSession, auth_headers: dict[str, str]
) -> None:
    """Валидный POST: 202, платёж и outbox-запись в БД."""
    resp = await client.post(
        PAYMENTS_URL, json=_payment_body(), headers={**auth_headers, "Idempotency-Key": "idem-1"}
    )

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"
    assert body["created_at"] is not None
    payment_id = UUID(body["payment_id"])

    payment = await session.get(Payment, payment_id)
    assert payment is not None
    assert payment.amount == Decimal("10.50")
    outbox = (await session.scalars(select(OutboxMessage))).all()
    assert len(outbox) == 1
    assert outbox[0].payload["payment_id"] == str(payment_id)


async def test_create_payment_idempotent_replay(
    client: httpx.AsyncClient, session: AsyncSession, auth_headers: dict[str, str]
) -> None:
    """Повторный POST с тем же Idempotency-Key: 202, тот же id, одна запись outbox."""
    headers = {**auth_headers, "Idempotency-Key": "idem-1"}
    first = await client.post(PAYMENTS_URL, json=_payment_body(), headers=headers)
    second = await client.post(PAYMENTS_URL, json=_payment_body(), headers=headers)

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    outbox = (await session.scalars(select(OutboxMessage))).all()
    assert len(outbox) == 1


async def test_get_payment(
    client: httpx.AsyncClient,
    session: AsyncSession,
    auth_headers: dict[str, str],
    payment_factory: Callable[..., Payment],
) -> None:
    """GET существующего платежа: полная карточка с ключом metadata, без idempotency_key."""
    payment = payment_factory()
    session.add(payment)
    await session.commit()

    resp = await client.get(f"{PAYMENTS_URL}/{payment.id}", headers=auth_headers)

    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == str(payment.id)
    assert body["amount"] == "100.00"
    assert body["currency"] == "USD"
    assert body["metadata"] == {"order_id": "42"}
    assert body["status"] == "pending"
    assert body["webhook_url"] == "https://example.com/hook"
    assert "meta" not in body
    assert "idempotency_key" not in body


async def test_get_missing_payment(
    client: httpx.AsyncClient, auth_headers: dict[str, str]
) -> None:
    """GET несуществующего id: 404 с русским описанием."""
    resp = await client.get(f"{PAYMENTS_URL}/{uuid4()}", headers=auth_headers)

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Платёж не найден"


async def test_missing_api_key(client: httpx.AsyncClient) -> None:
    """Запрос без X-API-Key: 401."""
    resp = await client.post(
        PAYMENTS_URL, json=_payment_body(), headers={"Idempotency-Key": "idem-1"}
    )

    assert resp.status_code == 401
    assert resp.json()["detail"] == "Неверный или отсутствующий API-ключ"


async def test_wrong_api_key(client: httpx.AsyncClient) -> None:
    """Запрос с неверным ключом: 401."""
    resp = await client.post(
        PAYMENTS_URL,
        json=_payment_body(),
        headers={"X-API-Key": "wrong-key", "Idempotency-Key": "idem-1"},
    )

    assert resp.status_code == 401


async def test_missing_idempotency_key(
    client: httpx.AsyncClient, auth_headers: dict[str, str]
) -> None:
    """POST без Idempotency-Key: 422."""
    resp = await client.post(PAYMENTS_URL, json=_payment_body(), headers=auth_headers)

    assert resp.status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        _payment_body(amount="0"),
        _payment_body(amount="-5"),
        _payment_body(currency="GBP"),
    ],
)
async def test_invalid_body(
    client: httpx.AsyncClient, auth_headers: dict[str, str], body: dict[str, Any]
) -> None:
    """Нулевая и отрицательная сумма, неизвестная валюта: 422."""
    resp = await client.post(
        PAYMENTS_URL, json=body, headers={**auth_headers, "Idempotency-Key": "idem-1"}
    )

    assert resp.status_code == 422
