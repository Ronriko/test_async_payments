from uuid import UUID


class DomainError(Exception):
    """Base class for errors that the API maps to a 4xx response."""


class PaymentNotFoundError(DomainError):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")
        self.payment_id = payment_id


class IdempotencyKeyReusedError(DomainError):
    def __init__(self) -> None:
        super().__init__("Idempotency-Key was already used with a different request body")


class WebhookUrlNotAllowedError(DomainError):
    def __init__(self) -> None:
        super().__init__("webhook_url must point to a public address")


class WebhookDeliveryError(Exception):
    """The receiver did not confirm the notification; permanent errors are not worth retrying."""

    def __init__(self, message: str, *, permanent: bool = False) -> None:
        super().__init__(message)
        self.permanent = permanent
