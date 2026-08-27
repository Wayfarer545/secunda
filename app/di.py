from collections.abc import AsyncIterable

import httpx
from dishka import (
    AsyncContainer,
    Provider,
    Scope,
    from_context,
    make_async_container,
    provide,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.db import new_engine, new_sessionmaker


class AppProvider(Provider):
    """Провайдер зависимостей приложения."""

    scope = Scope.APP

    settings = from_context(provides=Settings, scope=Scope.APP)

    @provide
    async def engine(self, settings: Settings) -> AsyncIterable[AsyncEngine]:
        engine = new_engine(settings)
        yield engine
        await engine.dispose()

    @provide
    def sessionmaker(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return new_sessionmaker(engine)

    @provide(scope=Scope.REQUEST)
    async def session(
        self, sessionmaker: async_sessionmaker[AsyncSession]
    ) -> AsyncIterable[AsyncSession]:
        async with sessionmaker() as session:
            yield session

    @provide
    async def http_client(self) -> AsyncIterable[httpx.AsyncClient]:
        async with httpx.AsyncClient() as client:
            yield client


def make_container(settings: Settings) -> AsyncContainer:
    """Создаёт DI-контейнер приложения."""
    return make_async_container(AppProvider(), context={Settings: settings})
