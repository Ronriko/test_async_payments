import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from pydantic import PostgresDsn
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.main import create_app
from app.models import OutboxEvent, Payment
from app.repositories import PaymentRepository
from app.schemas.payments import MAX_METADATA_BYTES, MAX_METADATA_DEPTH


class SqlStateError(Exception):
    """Stands in for an asyncpg error, which carries the server's SQLSTATE code."""

    def __init__(self, sqlstate: str) -> None:
        super().__init__(f"sqlstate {sqlstate}")
        self.sqlstate = sqlstate


BODY: dict[str, Any] = {
    "amount": "10.50",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://shop.example.com/webhooks/payments",
}


def idem() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_payment_returns_202_and_writes_outbox_event(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post("/api/v1/payments", json=BODY, headers=idem())

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    async with session_factory() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.event_type == "payment.created"
    assert event.payload == {"payment_id": body["payment_id"]}
    assert event.published_at is None


async def test_get_payment_returns_details(client: httpx.AsyncClient) -> None:
    created = (await client.post("/api/v1/payments", json=BODY, headers=idem())).json()

    response = await client.get(f"/api/v1/payments/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "10.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "status": "pending",
        "webhook_url": BODY["webhook_url"],
        "created_at": created["created_at"],
        "processed_at": None,
    }


@pytest.mark.parametrize("api_key", [None, "wrong-key"])
@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid.uuid4()}")],
)
async def test_requests_without_valid_api_key_are_rejected(
    client: httpx.AsyncClient, method: str, path: str, api_key: str | None
) -> None:
    headers = {"Idempotency-Key": "k"}
    if api_key is not None:
        headers["X-API-Key"] = api_key
    request = client.build_request(method, path, json=BODY, headers=headers)
    if api_key is None:
        del request.headers["X-API-Key"]

    response = await client.send(request)

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "ApiKey"


async def test_missing_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=BODY)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


async def test_repeated_request_returns_the_same_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = idem()
    first = await client.post("/api/v1/payments", json=BODY, headers=headers)
    second = await client.post("/api/v1/payments", json={**BODY, "amount": "10.5"}, headers=headers)

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_reused_key_with_another_body_is_rejected(client: httpx.AsyncClient) -> None:
    headers = idem()
    await client.post("/api/v1/payments", json=BODY, headers=headers)

    response = await client.post(
        "/api/v1/payments", json={**BODY, "amount": "99.00"}, headers=headers
    )

    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["type"] == "idempotency_key_reused"
    assert error["loc"] == ["header", "Idempotency-Key"]


async def test_concurrent_requests_with_one_key_create_one_payment(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = idem()

    responses = await asyncio.gather(
        *(client.post("/api/v1/payments", json=BODY, headers=headers) for _ in range(20))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


@pytest.mark.parametrize(
    "override",
    [
        {"amount": "0"},
        {"amount": "-1.00"},
        {"amount": "10.555"},
        {"amount": "12345678901.00"},
        {"currency": "GBP"},
        {"description": ""},
        {"description": "x" * 501},
        {"description": "nul\x00byte"},
        {"metadata": {"key": "nul\x00byte"}},
        {"metadata": {"blob": "x" * MAX_METADATA_BYTES}},
        {"webhook_url": "ftp://example.com/hook"},
        {"webhook_url": "not a url"},
        {"unexpected": "field"},
    ],
)
async def test_invalid_payload_is_rejected(
    client: httpx.AsyncClient,
    session_factory: async_sessionmaker[AsyncSession],
    override: dict[str, Any],
) -> None:
    response = await client.post("/api/v1/payments", json={**BODY, **override}, headers=idem())

    assert response.status_code == 422
    assert await count(session_factory, Payment) == 0


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:9000/hook",
        "http://localhost/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/hook",
        "http://10.0.0.5/hook",
    ],
)
async def test_private_webhook_url_is_rejected(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], url: str
) -> None:
    body = {**BODY, "webhook_url": url}

    response = await client.post("/api/v1/payments", json=body, headers=idem())

    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert error["type"] == "webhook_url_not_allowed"
    assert error["loc"] == ["body", "webhook_url"]
    assert await count(session_factory, Payment) == 0


@pytest.mark.parametrize("number", ["NaN", "Infinity", "1e400"])
async def test_non_finite_metadata_numbers_are_rejected(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], number: str
) -> None:
    body = json.dumps({**BODY, "metadata": {"value": "PLACEHOLDER"}}).replace(
        '"PLACEHOLDER"', number
    )
    headers = {**idem(), "Content-Type": "application/json"}

    response = await client.post("/api/v1/payments", content=body, headers=headers)

    assert response.status_code == 422
    assert await count(session_factory, Payment) == 0


@pytest.mark.parametrize("depth", [MAX_METADATA_DEPTH, 300, 600])
async def test_deeply_nested_metadata_is_rejected(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], depth: int
) -> None:
    nested: Any = "leaf"
    for _ in range(depth):
        nested = [nested]

    response = await client.post(
        "/api/v1/payments", json={**BODY, "metadata": {"deep": nested}}, headers=idem()
    )

    assert response.status_code == 422
    assert "nested deeper" in response.json()["detail"][0]["msg"]
    assert await count(session_factory, Payment) == 0


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (DBAPIError("INSERT", {}, SqlStateError("57P03")), 503),  # the server is starting up
        (DBAPIError("INSERT", {}, SqlStateError("53300")), 503),  # too many connections
        (DBAPIError("INSERT", {}, SqlStateError("23502")), 500),  # a real bug, not an outage
        (PoolTimeoutError("pool exhausted"), 503),
    ],
)
async def test_database_errors_are_mapped_by_sqlstate(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected: int,
) -> None:
    async def failing_insert(*_: object) -> None:
        raise error

    monkeypatch.setattr(PaymentRepository, "insert_if_absent", failing_insert)

    response = await client.post("/api/v1/payments", json=BODY, headers=idem())

    assert response.status_code == expected


@pytest.mark.parametrize(
    ("override", "stored"),
    [
        ({"metadata": None}, {"metadata": {}}),
        ({"currency": "rub"}, {"currency": "RUB"}),
        (
            {"metadata": {"text": "\\u0000 is not a NUL"}},
            {"metadata": {"text": "\\u0000 is not a NUL"}},
        ),
    ],
)
async def test_lenient_inputs_are_normalised(
    client: httpx.AsyncClient, override: dict[str, Any], stored: dict[str, Any]
) -> None:
    created = await client.post("/api/v1/payments", json={**BODY, **override}, headers=idem())
    assert created.status_code == 202

    details = (await client.get(f"/api/v1/payments/{created.json()['payment_id']}")).json()

    assert {key: details[key] for key in stored} == stored


async def test_oversized_body_is_rejected_before_parsing(
    client: httpx.AsyncClient, settings: Settings
) -> None:
    oversized = b"x" * (settings.max_request_body_bytes + 1)

    async def chunks() -> AsyncIterator[bytes]:
        for start in range(0, len(oversized), 8192):
            yield oversized[start : start + 8192]

    declared = await client.post("/api/v1/payments", content=oversized, headers=idem())
    streamed = await client.post("/api/v1/payments", content=chunks(), headers=idem())

    assert declared.status_code == streamed.status_code == 413
    assert streamed.json() == {"detail": "Request body is too large"}


async def test_unknown_payment_returns_404(client: httpx.AsyncClient) -> None:
    payment_id = uuid.uuid4()

    response = await client.get(f"/api/v1/payments/{payment_id}")

    assert response.status_code == 404
    assert response.json() == {"detail": f"Payment {payment_id} not found"}


async def test_malformed_payment_id_returns_422(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/payments/not-a-uuid")

    assert response.status_code == 422


async def test_health_endpoints(client: httpx.AsyncClient) -> None:
    assert (await client.get("/health")).json() == {"status": "ok"}
    assert (await client.get("/health/ready")).json() == {"status": "ok"}


async def test_database_outage_returns_503(settings: Settings) -> None:
    unreachable = PostgresDsn("postgresql+asyncpg://payments:secret@127.0.0.1:1/payments")
    app = create_app(settings.model_copy(update={"database_url": unreachable}))
    headers = {"X-API-Key": settings.api_key.get_secret_value()}
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver", headers=headers
        ) as broken_client,
    ):
        ready = await broken_client.get("/health/ready")
        created = await broken_client.post("/api/v1/payments", json=BODY, headers=idem())

    assert ready.status_code == created.status_code == 503
    assert created.json() == {"detail": "Service temporarily unavailable"}
