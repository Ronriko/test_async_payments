import asyncio
from collections import Counter
from datetime import timedelta
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.models import OutboxEvent
from app.repositories import OutboxRepository
from app.services.outbox_relay import OutboxRelay


class FakePublisher:
    def __init__(self, *, fail: bool = False, delay: float = 0.0) -> None:
        self.fail = fail
        self.delay = delay
        self.published: list[int] = []
        self.first_published = asyncio.Event()

    async def publish(self, event_id: int, event_type: str, payload: dict[str, Any]) -> None:
        await asyncio.sleep(self.delay)
        if self.fail:
            raise ConnectionError("broker is down")
        self.published.append(event_id)
        self.first_published.set()


async def add_events(session_factory: async_sessionmaker[AsyncSession], count: int) -> None:
    async with session_factory() as session, session.begin():
        outbox = OutboxRepository(session)
        for number in range(count):
            outbox.add("payment.created", {"number": number})


async def load_events(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list((await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id))).all())


async def test_due_events_are_published_and_marked(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    await add_events(session_factory, 3)
    publisher = FakePublisher()

    published = await OutboxRelay(session_factory, publisher, settings).publish_due_batch()

    events = await load_events(session_factory)
    assert published == 3
    assert publisher.published == [event.id for event in events]
    assert all(event.published_at is not None for event in events)


async def test_failed_publish_is_postponed_and_retried_later(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    await add_events(session_factory, 2)
    publisher = FakePublisher(fail=True)
    relay = OutboxRelay(session_factory, publisher, settings)

    assert await relay.publish_due_batch() == 0
    first, second = await load_events(session_factory)
    assert first.attempts == 1
    assert first.last_error == "ConnectionError: broker is down"
    assert first.published_at is None
    assert first.next_attempt_at - first.created_at >= timedelta(seconds=1)
    assert second.attempts == 0  # the batch stops at the first failure

    publisher.fail = False
    async with session_factory() as session, session.begin():
        await session.execute(update(OutboxEvent).values(next_attempt_at=OutboxEvent.created_at))
    assert await relay.publish_due_batch() == 2


def test_backoff_grows_and_stays_capped(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    relay = OutboxRelay(session_factory, FakePublisher(), settings)
    cap = timedelta(seconds=settings.outbox_max_backoff_seconds)

    assert [relay._backoff(n).total_seconds() for n in range(4)] == [1, 2, 4, 8]
    assert relay._backoff(10_000) == cap  # 2.0 ** 10_000 would overflow


async def test_stop_lets_the_running_batch_commit(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    await add_events(session_factory, 5)
    publisher = FakePublisher(delay=0.05)
    relay = OutboxRelay(session_factory, publisher, settings)

    await relay.start()
    await asyncio.wait_for(publisher.first_published.wait(), timeout=5)
    await relay.stop()

    events = await load_events(session_factory)
    published = [event.id for event in events if event.published_at is not None]
    assert published == publisher.published  # nothing confirmed by the broker is rolled back
    assert 0 < len(published) < 5


async def test_concurrent_relays_publish_every_event_once(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    await add_events(session_factory, 60)
    publisher = FakePublisher(delay=0.005)
    small_batches = settings.model_copy(update={"outbox_batch_size": 10})
    relays = [OutboxRelay(session_factory, publisher, small_batches) for _ in range(3)]

    for _ in range(10):
        await asyncio.gather(*(relay.publish_due_batch() for relay in relays))

    assert len(publisher.published) == 60
    assert Counter(publisher.published).most_common(1)[0][1] == 1


async def test_old_published_events_are_deleted(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    await add_events(session_factory, 2)
    relay = OutboxRelay(session_factory, FakePublisher(), settings)
    await relay.publish_due_batch()
    old_id = (await load_events(session_factory))[0].id
    async with session_factory() as session, session.begin():
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == old_id)
            .values(published_at=OutboxEvent.published_at - timedelta(days=30))
        )

    await relay.delete_old_events()

    assert [event.id for event in await load_events(session_factory)] == [old_id + 1]
