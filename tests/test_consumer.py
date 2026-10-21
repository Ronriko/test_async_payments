import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest
from faststream.rabbit import (
    RabbitBroker,
    RabbitExchange,
    RabbitMessage,
    RabbitQueue,
    TestRabbitBroker,
)
from pydantic import ValidationError

from app.broker.consumer import register_payment_consumer, retry_delay
from app.broker.topology import ATTEMPT_HEADER, ERROR_HEADER, Topology, build_topology
from app.core.config import Settings
from app.domain.exceptions import PaymentNotFoundError, WebhookDeliveryError


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls.append(payment_id)
        if self.error is not None:
            raise self.error


@dataclass
class Forwarded:
    queue: str
    headers: dict[str, Any]
    body: dict[str, Any]
    expiration: object


@dataclass
class Harness:
    broker: RabbitBroker
    topology: Topology
    forwarded: list[Forwarded] = field(default_factory=list)

    async def publish(self, payment_id: str, attempt: int) -> None:
        await self.broker.publish(
            {"payment_id": payment_id},
            exchange=self.topology.exchange,
            routing_key=self.topology.queue.name,
            headers={ATTEMPT_HEADER: attempt},
        )


def build(settings: Settings, processor: FakeProcessor) -> Harness:
    harness = Harness(RabbitBroker(), build_topology(settings))
    topology = harness.topology
    register_payment_consumer(harness.broker, topology, processor, settings)

    def capture(queue: RabbitQueue, exchange: RabbitExchange) -> None:
        @harness.broker.subscriber(queue, exchange)
        async def forwarded(body: dict[str, Any], message: RabbitMessage) -> None:
            raw = message.raw_message
            harness.forwarded.append(Forwarded(queue.name, message.headers, body, raw.expiration))

    for retry_queue in topology.retry_queues:
        capture(retry_queue, topology.exchange)
    capture(topology.dead_letter_queue, topology.dead_letter_exchange)
    return harness


async def test_message_is_handed_to_the_processor(settings: Settings) -> None:
    processor = FakeProcessor()
    harness = build(settings, processor)
    payment_id = uuid.uuid4()

    async with TestRabbitBroker(harness.broker):
        await harness.publish(str(payment_id), attempt=1)

    assert processor.calls == [payment_id]
    assert harness.forwarded == []


@pytest.mark.parametrize("attempt", [1, 2])
async def test_transient_failure_schedules_an_exponentially_delayed_retry(
    settings: Settings, attempt: int
) -> None:
    harness = build(settings, FakeProcessor(error=RuntimeError("db is down")))
    payment_id = str(uuid.uuid4())

    async with TestRabbitBroker(harness.broker):
        await harness.publish(payment_id, attempt=attempt)

    [retry] = harness.forwarded
    assert retry.queue == f"{settings.payments_queue}.retry.{attempt}"
    assert retry.headers[ATTEMPT_HEADER] == attempt + 1
    assert retry.headers[ERROR_HEADER] == "RuntimeError: db is down"
    assert retry.body == {"payment_id": payment_id}
    assert retry.expiration == retry_delay(settings, attempt)
    assert retry_delay(settings, 2) == 2 * retry_delay(settings, 1)


@pytest.mark.parametrize(
    ("error", "attempt"),
    [
        (RuntimeError("still down"), 3),
        (PaymentNotFoundError(uuid.UUID(int=0)), 1),
        (WebhookDeliveryError("receiver answered 404", permanent=True), 1),
    ],
)
async def test_final_failure_moves_the_message_to_the_dlq_with_its_error(
    settings: Settings, error: Exception, attempt: int
) -> None:
    harness = build(settings, FakeProcessor(error=error))

    async with TestRabbitBroker(harness.broker):
        await harness.publish(str(uuid.uuid4()), attempt=attempt)

    [dead] = harness.forwarded
    assert dead.queue == settings.dead_letter_queue
    assert dead.headers[ATTEMPT_HEADER] == attempt
    assert dead.headers[ERROR_HEADER] == f"{type(error).__name__}: {error}"
    assert dead.expiration is None


async def test_malformed_message_is_rejected_without_processing(settings: Settings) -> None:
    processor = FakeProcessor()
    harness = build(settings, processor)

    async with TestRabbitBroker(harness.broker):
        with pytest.raises(ValidationError, match="payment_id"):
            await harness.broker.publish(
                {"unexpected": 1},
                exchange=harness.topology.exchange,
                routing_key=harness.topology.queue.name,
            )

    assert processor.calls == []
    assert harness.forwarded == []
