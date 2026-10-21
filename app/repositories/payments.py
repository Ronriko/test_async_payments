from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import PaymentStatus
from app.models import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, values: dict[str, Any]) -> Payment | None:
        """Insert a payment unless its idempotency key exists; None means a concurrent twin won."""
        stmt = (
            insert(Payment)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        result = await self._session.scalars(stmt, execution_options={"populate_existing": True})
        return result.one_or_none()

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return (await self._session.scalars(stmt)).one()

    async def complete_if_pending(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> Payment | None:
        """Store the gateway result; None means another consumer has already stored one."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=processed_at)
            .returning(Payment)
        )
        result = await self._session.scalars(stmt, execution_options={"populate_existing": True})
        return result.one_or_none()
