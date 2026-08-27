"""Общие фикстуры: in-memory БД, тестовый DI-контейнер, фейковый брокер."""

from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from typing import Any
from uuid import uuid4

import httpx
import pytest
from dishka import Provider, Scope, from_context, make_async_container, provide
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from app.api.main import create_app
from app.config import Settings
from app.models import Base, Currency, Payment

TEST_API_KEY = "test-api-key"


class FakeBroker:
    """Двойник RabbitBroker: копит publish-вызовы, умеет падать на заданном по счёту вызове."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []
        self.calls = 0
        self.fail_on_call: int | None = None

    async def publish(
        self,
        payload: Any,
        *,
        exchange: Any = None,
        routing_key: str = "",
        headers: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Запоминает вызов либо имитирует обрыв соединения."""
        self.calls += 1
        if self.fail_on_call == self.calls:
            raise ConnectionError("обрыв соединения с брокером")
        self.published.append(
            {
                "payload": payload,
                "exchange": getattr(exchange, "name", exchange),
                "routing_key": routing_key,
                "headers": headers or {},
            }
        )


class OverridesProvider(Provider):
    """DI-провайдер тестов: вместо реальной БД отдаёт общую тестовую сессию."""

    scope = Scope.APP

    settings = from_context(provides=Settings, scope=Scope.APP)

    def __init__(self, session: AsyncSession) -> None:
        super().__init__()
        self._session = session

    @provide(scope=Scope.REQUEST)
    async def session(self) -> AsyncSession:
        return self._session


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """Свежая in-memory БД со схемой на каждый тест."""
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """Сессия к тестовой БД."""
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        yield session


@pytest.fixture
def settings() -> Settings:
    """Настройки тестов без чтения .env."""
    return Settings(_env_file=None, api_key=SecretStr(TEST_API_KEY))


@pytest.fixture
def fake_broker() -> FakeBroker:
    """Фейковый брокер для проверки публикаций."""
    return FakeBroker()


@pytest.fixture
def payment_factory() -> Callable[..., Payment]:
    """Фабрика платежей с валидными значениями по умолчанию."""

    def make(**overrides: Any) -> Payment:
        params: dict[str, Any] = {
            "amount": Decimal("100.00"),
            "currency": Currency.USD,
            "description": "тестовый платёж",
            "meta": {"order_id": "42"},
            "idempotency_key": uuid4().hex,
            "webhook_url": "https://example.com/hook",
        }
        params.update(overrides)
        return Payment(**params)

    return make


@pytest.fixture
def auth_headers() -> dict[str, str]:
    """Заголовок авторизации с тестовым ключом."""
    return {"X-API-Key": TEST_API_KEY}


@pytest.fixture
async def client(session: AsyncSession, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """ASGI-клиент реального приложения с тестовым DI-контейнером."""
    container = make_async_container(OverridesProvider(session), context={Settings: settings})
    app = create_app(container)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        yield http
    await container.close()
