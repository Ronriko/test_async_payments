from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import Settings

ATTEMPT_HEADER = "attempt"
ERROR_HEADER = "error"  # the failure that sent the message to a retry queue or to the DLQ


@dataclass(frozen=True)
class Topology:
    """payments.new -> (reject) -> payments.dlx -> payments.dlq; retries wait in *.retry.N."""

    exchange: RabbitExchange
    queue: RabbitQueue
    dead_letter_exchange: RabbitExchange
    dead_letter_queue: RabbitQueue
    retry_queues: tuple[RabbitQueue, ...]

    def retry_queue_for(self, failed_attempt: int) -> RabbitQueue:
        return self.retry_queues[failed_attempt - 1]


def build_topology(settings: Settings) -> Topology:
    exchange = RabbitExchange(settings.payments_exchange, type=ExchangeType.DIRECT, durable=True)
    dead_letter_exchange = RabbitExchange(
        settings.dead_letter_exchange, type=ExchangeType.DIRECT, durable=True
    )
    queue = RabbitQueue(
        settings.payments_queue,
        durable=True,
        arguments={
            "x-dead-letter-exchange": settings.dead_letter_exchange,
            "x-dead-letter-routing-key": settings.dead_letter_queue,
        },
    )
    # a message waits here for its per-message TTL, then is dead-lettered back to payments.new
    retry_queues = tuple(
        RabbitQueue(
            f"{settings.payments_queue}.retry.{attempt}",
            durable=True,
            arguments={
                "x-dead-letter-exchange": settings.payments_exchange,
                "x-dead-letter-routing-key": settings.payments_queue,
            },
        )
        for attempt in range(1, settings.max_attempts)
    )
    return Topology(
        exchange=exchange,
        queue=queue,
        dead_letter_exchange=dead_letter_exchange,
        dead_letter_queue=RabbitQueue(settings.dead_letter_queue, durable=True),
        retry_queues=retry_queues,
    )


async def declare_topology(broker: RabbitBroker, topology: Topology) -> None:
    """Declare what no subscriber declares itself: the DLQ and the retry queues with bindings."""
    exchange = await broker.declare_exchange(topology.exchange)
    dead_letter_exchange = await broker.declare_exchange(topology.dead_letter_exchange)
    dead_letter_queue = await broker.declare_queue(topology.dead_letter_queue)
    await dead_letter_queue.bind(dead_letter_exchange, routing_key=topology.dead_letter_queue.name)
    for retry_queue in topology.retry_queues:
        declared = await broker.declare_queue(retry_queue)
        await declared.bind(exchange, routing_key=retry_queue.name)
