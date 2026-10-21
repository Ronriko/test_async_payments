import hashlib
import json
import logging
from decimal import Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.webhook import is_private_literal
from app.domain.exceptions import (
    IdempotencyKeyReusedError,
    PaymentNotFoundError,
    WebhookUrlNotAllowedError,
)
from app.models import Payment
from app.repositories import OutboxRepository, PaymentRepository
from app.schemas.events import PAYMENT_CREATED, PaymentCreatedEvent
from app.schemas.payments import PaymentCreate

logger = logging.getLogger(__name__)


def request_fingerprint(data: PaymentCreate) -> str:
    body = data.model_dump(mode="json")
    body["amount"] = str(data.amount.quantize(Decimal("0.01")))
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession, *, allow_private_webhooks: bool) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)
        self._allow_private_webhooks = allow_private_webhooks

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        if not self._allow_private_webhooks and is_private_literal(data.webhook_url.host or ""):
            raise WebhookUrlNotAllowedError
        fingerprint = request_fingerprint(data)
        async with self._session.begin():
            payment = await self._payments.insert_if_absent(
                {
                    "amount": data.amount,
                    "currency": data.currency,
                    "description": data.description,
                    "payment_metadata": data.metadata,
                    "webhook_url": str(data.webhook_url),
                    "idempotency_key": idempotency_key,
                    "request_hash": fingerprint,
                }
            )
            if payment is None:
                return await self._replay(idempotency_key, fingerprint)
            event = PaymentCreatedEvent(payment_id=payment.id)
            self._outbox.add(PAYMENT_CREATED, event.model_dump(mode="json"))
        logger.info("payment created", extra={"payment_id": str(payment.id)})
        return payment

    async def get(self, payment_id: UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _replay(self, idempotency_key: str, fingerprint: str) -> Payment:
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing.request_hash != fingerprint:
            raise IdempotencyKeyReusedError
        logger.info("idempotent replay", extra={"payment_id": str(existing.id)})
        return existing
