import httpx
from loguru import logger

from app.models import Payment


class WebhookDeliveryError(Exception):
    """Ошибка доставки вебхука."""


async def send_webhook(client: httpx.AsyncClient, payment: Payment, timeout: float) -> None:
    """Отправляет вебхук о результате платежа, при неудаче поднимает WebhookDeliveryError."""
    body = {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        "metadata": payment.meta,
    }
    try:
        response = await client.post(payment.webhook_url, json=body, timeout=timeout)
    except httpx.HTTPError as exc:
        raise WebhookDeliveryError(f"транспортная ошибка: {exc}") from exc

    if not response.is_success:
        raise WebhookDeliveryError(f"ответ {response.status_code}")

    logger.info("Вебхук доставлен: payment_id={}, статус {}", payment.id, payment.status.value)
