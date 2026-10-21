import httpx
import pytest
from pydantic import AmqpDsn

from app.core.config import Settings
from app.worker import create_app, create_http_client


async def test_unreachable_broker_fails_the_start_up(settings: Settings) -> None:
    """The process must exit, so that the restart policy retries it, instead of idling."""
    unreachable = AmqpDsn("amqp://guest:guest@127.0.0.1:1/")
    app = create_app(settings.model_copy(update={"rabbitmq_url": unreachable}))

    with pytest.raises(ConnectionError):
        async with app.start_lifespan_context():
            pass


async def test_webhook_client_uses_the_configured_timeout(settings: Settings) -> None:
    slow_receivers = settings.model_copy(update={"webhook_timeout_seconds": 30.0})

    async with create_http_client(slow_receivers) as client:
        # httpx would otherwise cut every phase at its own 5 s default inside the 30 s deadline
        assert client.timeout == httpx.Timeout(30.0)
