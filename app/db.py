from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import Settings


def new_engine(settings: Settings) -> AsyncEngine:
    """Создаёт асинхронный движок БД."""
    return create_async_engine(settings.database_url)


def new_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Создаёт фабрику асинхронных сессий."""
    return async_sessionmaker(engine, expire_on_commit=False)
