import logging
from datetime import timedelta
from typing import Protocol
from uuid import UUID

from aio_pika.abc import HeadersType
from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitMessage, RabbitQueue

from app.broker.topology import ATTEMPT_HEADER, ERROR_HEADER, Topology
from app.core.config import Settings
from app.domain.exceptions import PaymentNotFoundError, WebhookDeliveryError
from app.schemas.events import PaymentCreatedEvent

logger = logging.getLogger(__name__)


class Processor(Protocol):
    async def process(self, payment_id: UUID) -> None: ...


def attempt_of(message: RabbitMessage) -> int:
    value = message.headers.get(ATTEMPT_HEADER, 1)
    return value if isinstance(value, int) and value >= 1 else 1


def is_permanent(exc: Exception) -> bool:
    if isinstance(exc, WebhookDeliveryError):
        return exc.permanent
    return isinstance(exc, PaymentNotFoundError)


def retry_delay(settings: Settings, failed_attempt: int) -> float:
    return settings.retry_base_delay_seconds * 2.0 ** (failed_attempt - 1)


def register_payment_consumer(
    broker: RabbitBroker, topology: Topology, processor: Processor, settings: Settings
) -> None:
    @broker.subscriber(
        topology.queue,
        topology.exchange,
        channel=Channel(prefetch_count=settings.consumer_prefetch),
        # anything the handler does not handle (an invalid body, a failed publish below) rejects
        # the message without requeue, and RabbitMQ dead-letters it to the DLQ
        ack_policy=AckPolicy.REJECT_ON_ERROR,
        title="payments.new",
    )
    async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        attempt = attempt_of(message)
        context = {"payment_id": str(event.payment_id), "attempt": attempt}

        async def forward(
            exchange: RabbitExchange,
            queue: RabbitQueue,
            headers: HeadersType,
            expiration: timedelta | None = None,
        ) -> None:
            await broker.publish(
                event,
                exchange=exchange,
                routing_key=queue.name,
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                headers=headers,
                expiration=expiration,
                persist=True,
                mandatory=True,
            )

        try:
            await processor.process(event.payment_id)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if is_permanent(exc) or attempt >= settings.max_attempts:
                logger.exception("payment message moved to DLQ", extra=context)
                await forward(
                    topology.dead_letter_exchange,
                    topology.dead_letter_queue,
                    {ATTEMPT_HEADER: attempt, ERROR_HEADER: error},
                )
                return
            delay = retry_delay(settings, attempt)
            await forward(
                topology.exchange,
                topology.retry_queue_for(attempt),
                {ATTEMPT_HEADER: attempt + 1, ERROR_HEADER: error},
                expiration=timedelta(seconds=delay),
            )
            logger.warning(
                "payment processing failed, retry scheduled",
                extra={**context, "retry_in_seconds": delay, "error": error},
            )
