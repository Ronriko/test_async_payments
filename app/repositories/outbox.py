from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event_type: str, payload: dict[str, Any]) -> None:
        self._session.add(OutboxEvent(event_type=event_type, payload=payload))

    async def lock_due_batch(self, limit: int) -> Sequence[OutboxEvent]:
        # SKIP LOCKED lets several relays share the table without publishing an event twice
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), OutboxEvent.next_attempt_at <= func.now())
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()

    async def mark_published(self, event_ids: Sequence[int]) -> None:
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(event_ids))
            .values(published_at=func.now(), last_error=None)
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)

    async def delete_published_before(self, age: timedelta) -> int:
        stmt = delete(OutboxEvent).where(OutboxEvent.published_at < func.now() - age)
        result = await self._session.execute(stmt)
        return int(getattr(result, "rowcount", 0) or 0)

    async def mark_failed(self, event_id: int, error: str, retry_in: timedelta) -> None:
        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                attempts=OutboxEvent.attempts + 1,
                last_error=error,
                next_attempt_at=func.now() + retry_in,
            )
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)
