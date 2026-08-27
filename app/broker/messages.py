from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class PaymentEvent(BaseModel):
    """Событие о платеже, публикуемое через outbox."""

    payment_id: UUID
    occurred_at: datetime
