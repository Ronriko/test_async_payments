from functools import lru_cache

from pydantic import AmqpDsn, Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """The part migrations need; they must not depend on the API key or the broker."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: PostgresDsn
    db_pool_size: int = Field(default=10, ge=1, le=100)
    db_max_overflow: int = Field(default=10, ge=0, le=100)


class Settings(DatabaseSettings):
    api_key: SecretStr = Field(min_length=16)
    rabbitmq_url: AmqpDsn
    log_level: str = "INFO"
    max_request_body_bytes: int = Field(default=64 * 1024, ge=1024)

    payments_exchange: str = "payments"
    payments_queue: str = "payments.new"
    dead_letter_exchange: str = "payments.dlx"
    dead_letter_queue: str = "payments.dlq"
    consumer_prefetch: int = Field(default=10, ge=1, le=1000)
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=2.0, gt=0)

    gateway_min_delay_seconds: float = Field(default=2.0, ge=0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    webhook_allow_private_networks: bool = False

    outbox_batch_size: int = Field(default=100, ge=1, le=1000)
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_max_backoff_seconds: float = Field(default=60.0, gt=0)
    outbox_retention_hours: int = Field(default=168, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
