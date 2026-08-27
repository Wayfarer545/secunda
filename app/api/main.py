"""Сборка FastAPI-приложения."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from dishka import AsyncContainer
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from app.api.payments import router as payments_router
from app.config import Settings
from app.di import make_container
from app.logging import setup_logging


def create_app(container: AsyncContainer | None = None) -> FastAPI:
    """Создаёт приложение с маршрутами; DI-контейнер строится сам либо приходит извне."""
    settings = Settings()
    setup_logging(settings)
    if container is None:
        container = make_container(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await container.close()

    docs_enabled = settings.app_env != "prod"
    app = FastAPI(
        title="Secunda",
        description="Сервис асинхронного процессинга платежей",
        version="0.1.0",
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )
    app.include_router(payments_router)

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Проверка живости сервиса."""
        return {"status": "ok"}

    setup_dishka(container, app)
    return app


app = create_app()
