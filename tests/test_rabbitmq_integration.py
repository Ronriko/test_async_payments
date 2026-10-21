import asyncio
import json
import re
import uuid
from typing import Any

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import main, worker
from app.broker.consumer import register_payment_consumer
from app.broker.publisher import RabbitEventPublisher
from app.broker.topology import (
    ATTEMPT_HEADER,
    ERROR_HEADER,
    Topology,
    build_topology,
    declare_topology,
)
from app.core.config import Settings
from app.models import OutboxEvent
from app.repositories import OutboxRepository
from app.services.outbox_relay import OutboxRelay

pytestmark = pytest.mark.rabbitmq


class AlwaysFailingProcessor:
    def __init__(self) -> None:
        self.calls = 0

    async def process(self, payment_id: uuid.UUID) -> None:
        self.calls += 1
        raise RuntimeError("gateway timeout")


def isolated(settings: Settings, **overrides: Any) -> Settings:
    suffix = uuid.uuid4().hex[:8]
    names = {
        "payments_exchange": f"test.payments.{suffix}",
        "payments_queue": f"test.payments.new.{suffix}",
        "dead_letter_exchange": f"test.payments.dlx.{suffix}",
        "dead_letter_queue": f"test.payments.dlq.{suffix}",
        "retry_base_delay_seconds": 0.2,
    }
    return settings.model_copy(update={**names, **overrides})


async def next_dead_letter(broker: RabbitBroker, topology: Topology) -> AbstractIncomingMessage:
    queue = await broker.declare_queue(topology.dead_letter_queue)
    for _ in range(150):
        message = await queue.get(no_ack=True, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.1)
    pytest.fail("nothing reached the DLQ")


async def delete_topology(settings: Settings) -> None:
    topology = build_topology(settings)
    async with RabbitBroker(str(settings.rabbitmq_url)) as broker:
        for queue in (topology.queue, topology.dead_letter_queue, *topology.retry_queues):
            await (await broker.declare_queue(queue)).delete(if_unused=False, if_empty=False)
        for exchange in (topology.exchange, topology.dead_letter_exchange):
            await (await broker.declare_exchange(exchange)).delete()


async def test_failing_message_reaches_the_dlq_after_max_attempts(settings: Settings) -> None:
    test_settings = isolated(settings)
    broker = RabbitBroker(
        str(settings.rabbitmq_url), default_channel=Channel(on_return_raises=True)
    )
    topology = build_topology(test_settings)
    processor = AlwaysFailingProcessor()
    register_payment_consumer(broker, topology, processor, test_settings)

    try:
        async with broker:
            await declare_topology(broker, topology)
            await broker.start()
            publisher = RabbitEventPublisher(broker, topology)
            await publisher.publish(1, "payment.created", {"payment_id": str(uuid.uuid4())})
            dead = await next_dead_letter(broker, topology)
            await publisher.publish(2, "payment.created", {"unexpected": "body"})
            rejected = await next_dead_letter(broker, topology)
    finally:
        await delete_topology(test_settings)

    assert processor.calls == test_settings.max_attempts
    assert dead.headers[ATTEMPT_HEADER] == test_settings.max_attempts
    assert dead.headers[ERROR_HEADER] == "RuntimeError: gateway timeout"
    # a body the handler cannot even parse goes the native way: reject, then the DLX
    deaths = rejected.headers["x-death"]
    assert isinstance(deaths, list)
    assert [death["reason"] for death in deaths if isinstance(death, dict)] == ["rejected"]


async def test_payment_travels_from_the_api_to_the_webhook(settings: Settings) -> None:
    received: list[dict[str, Any]] = []

    async def receiver(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        length = re.search(rb"(?i)content-length: *(\d+)", head)
        assert length is not None
        received.append(json.loads(await reader.readexactly(int(length.group(1)))))
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(receiver, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    e2e = isolated(
        settings,
        gateway_min_delay_seconds=0,
        gateway_max_delay_seconds=0,
        gateway_success_rate=1.0,
        webhook_allow_private_networks=True,
        outbox_poll_interval_seconds=0.1,
    )
    consumer_app, api_app = worker.create_app(e2e), main.create_app(e2e)
    headers = {"X-API-Key": e2e.api_key.get_secret_value(), "Idempotency-Key": str(uuid.uuid4())}
    body = {
        "amount": "42.00",
        "currency": "EUR",
        "description": "e2e",
        "webhook_url": f"http://127.0.0.1:{port}/hook",
    }

    try:
        async with (
            server,
            consumer_app.start_lifespan_context(),
            api_app.router.lifespan_context(api_app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api_app), base_url="http://testserver"
            ) as client,
        ):
            payment_id = (await client.post("/api/v1/payments", json=body, headers=headers)).json()[
                "payment_id"
            ]
            for _ in range(100):
                details = (
                    await client.get(f"/api/v1/payments/{payment_id}", headers=headers)
                ).json()
                if details["status"] != "pending" and received:
                    break
                await asyncio.sleep(0.1)
    finally:
        await delete_topology(e2e)

    assert details["status"] == "succeeded"
    assert received == [
        {
            "payment_id": payment_id,
            "status": "succeeded",
            "amount": "42.00",
            "currency": "EUR",
            "processed_at": details["processed_at"],
        }
    ]


async def test_event_without_a_route_stays_in_the_outbox(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    unbound = isolated(settings)
    broker = worker.create_broker(unbound)
    topology = build_topology(unbound)
    async with session_factory() as session, session.begin():
        OutboxRepository(session).add("payment.created", {"payment_id": str(uuid.uuid4())})

    try:
        async with broker:
            # no subscriber, so nothing is bound to payments.new: a mandatory publish comes back
            await declare_topology(broker, topology)
            relay = OutboxRelay(session_factory, RabbitEventPublisher(broker, topology), unbound)
            published = await relay.publish_due_batch()
    finally:
        await delete_topology(unbound)

    async with session_factory() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert published == 0
    assert event.published_at is None
    assert event.attempts == 1
