from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from app.domain.enums import Currency, PaymentStatus

PAYMENT_CREATED = "payment.created"


class PaymentCreatedEvent(BaseModel):
    payment_id: UUID


class WebhookNotification(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None
