import asyncio
import contextlib
import logging
import time
from datetime import timedelta
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.repositories import OutboxRepository

CLEANUP_INTERVAL_SECONDS = 3600.0
STOP_TIMEOUT_SECONDS = 15.0
MAX_BACKOFF_EXPONENT = 16

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def publish(self, event_id: int, event_type: str, payload: dict[str, Any]) -> None: ...


class OutboxRelay:
    """Moves committed outbox rows to the broker; at-least-once, consumers dedupe."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = settings.outbox_batch_size
        self._poll_interval = settings.outbox_poll_interval_seconds
        self._max_backoff = settings.outbox_max_backoff_seconds
        self._retention = timedelta(hours=settings.outbox_retention_hours)
        self._stopping = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._stopping.clear()
        self._task = asyncio.create_task(self.run(), name="outbox-relay")

    async def stop(self) -> None:
        """Let the current batch commit what the broker already confirmed, then exit."""
        self._stopping.set()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(self._task, timeout=STOP_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning("outbox relay did not stop in time and was cancelled")

    async def run(self) -> None:
        next_cleanup = time.monotonic()
        while not self._stopping.is_set():
            try:
                published = await self.publish_due_batch()
                if time.monotonic() >= next_cleanup:
                    await self.delete_old_events()
                    next_cleanup = time.monotonic() + CLEANUP_INTERVAL_SECONDS
            except Exception:
                logger.exception("outbox relay iteration failed")
                published = 0
            if published < self._batch_size:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=self._poll_interval)

    async def publish_due_batch(self) -> int:
        async with self._session_factory() as session, session.begin():
            outbox = OutboxRepository(session)
            events = await outbox.lock_due_batch(self._batch_size)
            published: list[int] = []
            for event in events:
                if self._stopping.is_set():
                    break
                try:
                    await self._publisher.publish(event.id, event.event_type, event.payload)
                except Exception as exc:
                    retry_in = self._backoff(event.attempts)
                    await outbox.mark_failed(event.id, f"{type(exc).__name__}: {exc}", retry_in)
                    logger.exception(
                        "outbox publish failed",
                        extra={"event_id": event.id, "retry_in_seconds": retry_in.total_seconds()},
                    )
                    break  # the broker is most likely down, the rest of the batch waits for it
                published.append(event.id)
            if published:
                await outbox.mark_published(published)
        if published:
            logger.info("outbox events published", extra={"count": len(published)})
        return len(published)

    async def delete_old_events(self) -> None:
        async with self._session_factory() as session, session.begin():
            deleted = await OutboxRepository(session).delete_published_before(self._retention)
        if deleted:
            logger.info("old outbox events deleted", extra={"count": deleted})

    def _backoff(self, attempts: int) -> timedelta:
        exponent = min(attempts, MAX_BACKOFF_EXPONENT)
        return timedelta(seconds=min(self._max_backoff, 2.0**exponent))
