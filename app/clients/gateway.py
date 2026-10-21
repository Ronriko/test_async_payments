import asyncio
import random
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from app.core.config import Settings
from app.domain.enums import Currency, PaymentStatus


class PaymentGateway(Protocol):
    async def charge(
        self, idempotency_key: UUID, amount: Decimal, currency: Currency
    ) -> PaymentStatus: ...


class EmulatedGateway:
    """Stand-in for an external payment provider: 2-5 s latency, 90% approvals by default."""

    def __init__(self, settings: Settings) -> None:
        self._min_delay = settings.gateway_min_delay_seconds
        self._max_delay = settings.gateway_max_delay_seconds
        self._success_rate = settings.gateway_success_rate

    async def charge(
        self, idempotency_key: UUID, amount: Decimal, currency: Currency
    ) -> PaymentStatus:
        # a real provider gets idempotency_key so that a retried call cannot charge twice
        await asyncio.sleep(random.uniform(self._min_delay, self._max_delay))
        if random.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
