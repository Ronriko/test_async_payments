from typing import Any

from faststream.rabbit import RabbitBroker

from app.broker.topology import ATTEMPT_HEADER, Topology

PUBLISH_CONFIRM_TIMEOUT_SECONDS = 10.0


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, topology: Topology) -> None:
        self._broker = broker
        self._topology = topology

    async def publish(self, event_id: int, event_type: str, payload: dict[str, Any]) -> None:
        await self._broker.publish(
            payload,
            exchange=self._topology.exchange,
            routing_key=self._topology.queue.name,
            message_id=f"outbox-{event_id}",
            message_type=event_type,
            headers={ATTEMPT_HEADER: 1},
            persist=True,
            mandatory=True,
            timeout=PUBLISH_CONFIRM_TIMEOUT_SECONDS,
        )
