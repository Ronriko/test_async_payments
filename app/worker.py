"""Consumer service: the payments.new subscriber plus the outbox relay, served by uvicorn."""

import logging
from http.cookiejar import CookieJar, DefaultCookiePolicy

import httpx
from faststream.asgi import AsgiFastStream, make_ping_asgi
from faststream.rabbit import Channel, RabbitBroker

from app.broker.consumer import register_payment_consumer
from app.broker.publisher import RabbitEventPublisher
from app.broker.topology import build_topology, declare_topology
from app.clients.gateway import EmulatedGateway
from app.clients.webhook import WebhookClient
from app.core.config import Settings, get_settings
from app.core.database import create_engine, create_session_factory
from app.core.logging import setup_logging
from app.services.outbox_relay import OutboxRelay
from app.services.processing import PaymentProcessor

GRACEFUL_TIMEOUT_SECONDS = 30.0


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        str(settings.rabbitmq_url),
        # mandatory publishes that no queue accepts must fail instead of vanishing
        default_channel=Channel(on_return_raises=True),
        graceful_timeout=GRACEFUL_TIMEOUT_SECONDS,
        # outside the "faststream" logger tree, which has its own colored handler
        logger=logging.getLogger("app.broker"),
    )


def create_http_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=settings.webhook_timeout_seconds,
        # receivers must not be able to plant cookies that leak to other receivers
        cookies=CookieJar(policy=DefaultCookiePolicy(allowed_domains=[])),
    )


def create_app(settings: Settings | None = None) -> AsgiFastStream:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    broker = create_broker(settings)
    topology = build_topology(settings)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    http_client = create_http_client(settings)
    webhook = WebhookClient(
        http_client,
        settings.webhook_timeout_seconds,
        allow_private_networks=settings.webhook_allow_private_networks,
    )
    processor = PaymentProcessor(session_factory, EmulatedGateway(settings), webhook)
    register_payment_consumer(broker, topology, processor, settings)
    relay = OutboxRelay(session_factory, RabbitEventPublisher(broker, topology), settings)

    async def prepare_broker() -> None:
        # on_startup runs before uvicorn reports start-up, so a failure here exits the process
        await broker.connect()
        await declare_topology(broker, topology)

    async def close_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return AsgiFastStream(
        broker,
        logger=logging.getLogger("app.worker"),
        asgi_routes=[("/health", make_ping_asgi(broker, timeout=5.0, include_in_schema=False))],
        on_startup=[prepare_broker],
        after_startup=[relay.start],
        on_shutdown=[relay.stop],
        after_shutdown=[close_resources],
    )
