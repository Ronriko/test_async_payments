import asyncio
import json
import uuid
from decimal import Decimal

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients import webhook as webhook_module
from app.clients.webhook import WebhookClient
from app.domain.enums import Currency, PaymentStatus
from app.domain.exceptions import PaymentNotFoundError, WebhookDeliveryError
from app.models import Payment
from app.schemas.events import WebhookNotification
from app.schemas.payments import PaymentCreate
from app.services.payments import PaymentService
from app.services.processing import PaymentProcessor

WEBHOOK_URL = "https://shop.example.com/hook"


class FakeGateway:
    def __init__(self, *outcomes: PaymentStatus, barrier: asyncio.Barrier | None = None) -> None:
        self._outcomes = list(outcomes)
        self._barrier = barrier
        self.calls: list[uuid.UUID] = []

    async def charge(
        self, idempotency_key: uuid.UUID, amount: Decimal, currency: Currency
    ) -> PaymentStatus:
        self.calls.append(idempotency_key)
        outcome = self._outcomes.pop(0)
        if self._barrier is not None:
            await self._barrier.wait()
        return outcome


class Receiver:
    def __init__(self, *statuses: int) -> None:
        self._statuses = list(statuses)
        self.bodies: list[dict[str, object]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.bodies.append(json.loads(request.content))
        return httpx.Response(self._statuses.pop(0) if self._statuses else 200)


async def create_payment(session_factory: async_sessionmaker[AsyncSession]) -> Payment:
    data = PaymentCreate.model_validate(
        {"amount": "25.00", "currency": "USD", "description": "test", "webhook_url": WEBHOOK_URL}
    )
    async with session_factory() as session:
        service = PaymentService(session, allow_private_webhooks=False)
        return await service.create(data, str(uuid.uuid4()))


def make_processor(
    session_factory: async_sessionmaker[AsyncSession], gateway: FakeGateway, receiver: Receiver
) -> PaymentProcessor:
    client = httpx.AsyncClient(transport=httpx.MockTransport(receiver))
    webhook = WebhookClient(client, timeout_seconds=5, allow_private_networks=True)
    return PaymentProcessor(session_factory, gateway, webhook)


async def load(session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


@pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_gateway_outcome_is_stored_and_sent_to_the_client(
    session_factory: async_sessionmaker[AsyncSession], outcome: PaymentStatus
) -> None:
    payment = await create_payment(session_factory)
    receiver = Receiver(200)
    processor = make_processor(session_factory, FakeGateway(outcome), receiver)

    await processor.process(payment.id)

    stored = await load(session_factory, payment.id)
    assert stored.status is outcome
    assert stored.processed_at is not None
    assert receiver.bodies == [
        {
            "payment_id": str(payment.id),
            "status": outcome.value,
            "amount": "25.00",
            "currency": "USD",
            "processed_at": stored.processed_at.isoformat().replace("+00:00", "Z"),
        }
    ]


@pytest.mark.parametrize(("status_code", "permanent"), [(500, False), (429, False), (400, True)])
async def test_failed_webhook_keeps_the_payment_result(
    session_factory: async_sessionmaker[AsyncSession], status_code: int, permanent: bool
) -> None:
    payment = await create_payment(session_factory)
    processor = make_processor(
        session_factory, FakeGateway(PaymentStatus.SUCCEEDED), Receiver(status_code)
    )

    with pytest.raises(WebhookDeliveryError) as error:
        await processor.process(payment.id)

    assert error.value.permanent is permanent
    assert (await load(session_factory, payment.id)).status is PaymentStatus.SUCCEEDED


async def test_redelivery_does_not_charge_again_but_resends_the_webhook(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    receiver = Receiver(500, 200)
    processor = make_processor(session_factory, gateway, receiver)
    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)

    await processor.process(payment.id)

    assert gateway.calls == [payment.id]
    assert [body["status"] for body in receiver.bodies] == ["succeeded", "succeeded"]


async def test_concurrent_duplicates_keep_the_first_stored_result(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await create_payment(session_factory)
    receiver = Receiver()
    # both deliveries are inside the gateway at once, so the second write must lose the race
    barrier = asyncio.Barrier(2)
    gateway = FakeGateway(PaymentStatus.SUCCEEDED, PaymentStatus.FAILED, barrier=barrier)
    processor = make_processor(session_factory, gateway, receiver)

    await asyncio.gather(processor.process(payment.id), processor.process(payment.id))

    stored = await load(session_factory, payment.id)
    assert gateway.calls == [payment.id, payment.id]
    assert [body["status"] for body in receiver.bodies] == [stored.status.value] * 2


async def test_unknown_payment_is_a_permanent_error(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    processor = make_processor(session_factory, FakeGateway(), Receiver())

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())


@pytest.mark.parametrize("url", ["http://127.0.0.1:8000/hook", "http://localhost/hook"])
async def test_private_webhook_targets_are_refused(url: str) -> None:
    receiver = Receiver()
    client = WebhookClient(
        httpx.AsyncClient(transport=httpx.MockTransport(receiver)),
        timeout_seconds=5,
        allow_private_networks=False,
    )
    notification = WebhookNotification(
        payment_id=uuid.uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("1.00"),
        currency=Currency.RUB,
        processed_at=None,
    )

    with pytest.raises(WebhookDeliveryError) as error:
        await client.send(url, notification)

    assert error.value.permanent
    assert receiver.bodies == []


async def test_ssrf_check_resolves_the_name_httpx_connects_to(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checked: list[str] = []

    async def record(host: str) -> None:
        checked.append(host)

    monkeypatch.setattr(webhook_module, "ensure_public_host", record)
    client = WebhookClient(
        httpx.AsyncClient(transport=httpx.MockTransport(Receiver())),
        timeout_seconds=5,
        allow_private_networks=False,
    )
    notification = WebhookNotification(
        payment_id=uuid.uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("1.00"),
        currency=Currency.RUB,
        processed_at=None,
    )

    # "faß" in punycode: the decoded name would resolve as "fass" (IDNA 2003), not this one
    await client.send("http://xn--fa-hia.example/hook", notification)

    assert checked == ["xn--fa-hia.example"]
