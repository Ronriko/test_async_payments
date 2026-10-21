import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as sqlalchemy_async_engine

from app.core.config import Settings, get_settings
from app.core.database import create_engine, create_session_factory
from app.main import create_app

API_KEY = "test-api-key-0123456789"
ROOT = Path(__file__).resolve().parent.parent


async def _ensure_database(url: str) -> None:
    target = make_url(url)
    maintenance = sqlalchemy_async_engine(
        target.set(database="postgres"), isolation_level="AUTOCOMMIT"
    )
    try:
        async with maintenance.connect() as connection:
            exists = await connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": target.database},
            )
            if not exists:
                await connection.execute(text(f'CREATE DATABASE "{target.database}"'))
    finally:
        await maintenance.dispose()


class SuiteDatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    test_database_url: PostgresDsn | None = None


def suite_database_url(settings: Settings) -> PostgresDsn:
    """The suite drops and truncates tables, so it refuses anything but a dedicated *_test DB."""
    url = SuiteDatabaseSettings().test_database_url
    if url is None or url == settings.database_url or not (url.path or "").endswith("_test"):
        pytest.exit("Set TEST_DATABASE_URL to a dedicated database named *_test", returncode=2)
    return url


@pytest.fixture(scope="session")
def base_settings() -> Settings:
    settings = get_settings()
    # only the connections come from the environment; tuning in a developer's .env must not leak in
    defaults = {
        name: field.get_default(call_default_factory=True)
        for name, field in Settings.model_fields.items()
        if not field.is_required()
    }
    return settings.model_copy(
        update={
            **defaults,
            "database_url": suite_database_url(settings),
            "api_key": SecretStr(API_KEY),
            "log_level": "WARNING",
        }
    )


@pytest.fixture(scope="session", autouse=True)
def migrated_database(base_settings: Settings) -> None:
    url = str(base_settings.database_url)
    asyncio.run(_ensure_database(url))
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["database_url"] = url
    config.attributes["configure_logger"] = False
    command.downgrade(config, "base")
    command.upgrade(config, "head")


@pytest.fixture
def settings(base_settings: Settings) -> Settings:
    return base_settings


@pytest.fixture
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(settings)
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture(autouse=True)
async def clean_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox RESTART IDENTITY"))


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers={"X-API-Key": API_KEY},
        ) as http_client,
    ):
        yield http_client
