import asyncio
import random

from app.config import Settings
from app.models import PaymentStatus


async def emulate_gateway(settings: Settings) -> PaymentStatus:
    """Эмулирует внешний платёжный шлюз: случайная задержка и вероятностный исход."""
    delay = random.uniform(settings.gateway_delay_min, settings.gateway_delay_max)
    await asyncio.sleep(delay)
    if random.random() < settings.gateway_success_rate:
        return PaymentStatus.SUCCEEDED
    return PaymentStatus.FAILED
