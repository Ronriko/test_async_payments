import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.domain.enums import Currency, PaymentStatus

MAX_METADATA_BYTES = 16 * 1024
MAX_METADATA_DEPTH = 32

Amount = Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2, examples=["1500.00"])]


def metadata_problem(value: object) -> str | None:
    """Walks without recursion, so a deeply nested document cannot crash the check itself."""
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, str) and "\x00" in item:
            return "must not contain NUL characters"
        if isinstance(item, dict | list):
            if depth > MAX_METADATA_DEPTH:
                return f"must not be nested deeper than {MAX_METADATA_DEPTH} levels"
            children = [*item.keys(), *item.values()] if isinstance(item, dict) else item
            stack.extend((child, depth + 1) for child in children)
    return None


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("currency", mode="before")
    @classmethod
    def currency_upper(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @field_validator("metadata", mode="before")
    @classmethod
    def metadata_null_is_empty(cls, value: object) -> object:
        return {} if value is None else value

    # PostgreSQL rejects NUL, NaN and Infinity in text and jsonb: a 422 here, not a 500 later
    @field_validator("description")
    @classmethod
    def description_without_nul(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("must not contain NUL characters")
        return value

    @field_validator("metadata")
    @classmethod
    def metadata_fits_storage(cls, value: dict[str, Any]) -> dict[str, Any]:
        problem = metadata_problem(value)
        if problem is not None:
            raise ValueError(problem)
        try:
            encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
        except ValueError as exc:
            raise ValueError("numbers must be finite") from exc
        if len(encoded.encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"must not exceed {MAX_METADATA_BYTES} bytes as JSON")
        return value


class PaymentAccepted(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(validation_alias="payment_metadata")
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class ErrorResponse(BaseModel):
    detail: str


class HealthResponse(BaseModel):
    status: str
