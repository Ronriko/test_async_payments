import logging
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.gateway import PaymentGateway
from app.clients.webhook import WebhookClient
from app.domain.enums import PaymentStatus
from app.domain.exceptions import PaymentNotFoundError
from app.models import Payment
from app.repositories import PaymentRepository
from app.schemas.events import WebhookNotification

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Charges a pending payment once and notifies the client; safe to run again for any message."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhook: WebhookClient,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhook = webhook

    async def process(self, payment_id: UUID) -> None:
        payment = await self._load(payment_id)
        if payment.status is PaymentStatus.PENDING:
            status = await self._gateway.charge(payment.id, payment.amount, payment.currency)
            payment = await self._store_result(payment_id, status)
        await self._notify(payment)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _store_result(self, payment_id: UUID, status: PaymentStatus) -> Payment:
        async with self._session_factory() as session, session.begin():
            repository = PaymentRepository(session)
            payment = await repository.complete_if_pending(payment_id, status, datetime.now(UTC))
            if payment is not None:
                logger.info(
                    "payment processed", extra={"payment_id": str(payment_id), "status": status}
                )
                return payment
            # a duplicate delivery got here first: report its result, never overwrite it
            logger.info("payment already processed", extra={"payment_id": str(payment_id)})
            existing = await repository.get(payment_id)
        if existing is None:
            raise PaymentNotFoundError(payment_id)
        return existing

    async def _notify(self, payment: Payment) -> None:
        notification = WebhookNotification(
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )
        await self._webhook.send(payment.webhook_url, notification)
        logger.info(
            "webhook delivered", extra={"payment_id": str(payment.id), "status": payment.status}
        )
