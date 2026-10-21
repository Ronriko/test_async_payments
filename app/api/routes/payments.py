from uuid import UUID

from fastapi import APIRouter, Depends, status

from app.api.dependencies import IdempotencyKey, PaymentServiceDep, require_api_key
from app.models import Payment
from app.schemas.payments import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentRead

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={
        status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse},
    },
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={status.HTTP_413_CONTENT_TOO_LARGE: {"model": ErrorResponse}},
)
async def create_payment(
    payload: PaymentCreate, idempotency_key: IdempotencyKey, service: PaymentServiceDep
) -> Payment:
    return await service.create(payload, idempotency_key)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse}},
)
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> Payment:
    return await service.get(payment_id)
