"""HTTP API платежей."""

from typing import Annotated
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.security import require_api_key
from app.schemas import PaymentCreate, PaymentCreated, PaymentDetail
from app.services import payments as payments_service

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    route_class=DishkaRoute,
    dependencies=[Depends(require_api_key)],
)


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: Annotated[str, Header()],
    session: FromDishka[AsyncSession],
) -> PaymentCreated:
    """Принимает платёж в обработку; идемпотентно по заголовку Idempotency-Key."""
    payment, _ = await payments_service.create_payment(session, data, idempotency_key)
    return PaymentCreated(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}")
async def get_payment(payment_id: UUID, session: FromDishka[AsyncSession]) -> PaymentDetail:
    """Возвращает платёж по идентификатору."""
    payment = await payments_service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Платёж не найден")
    return PaymentDetail.model_validate(payment)
