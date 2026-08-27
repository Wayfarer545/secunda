from typing import Literal, Self

from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEV_API_KEY = "dev-api-key"


class Settings(BaseSettings):
    """Настройки приложения из переменных окружения и .env."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_env: Literal["dev", "prod"] = "dev"
    database_url: str = "postgresql+asyncpg://secunda:secunda@localhost:5432/secunda"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: SecretStr = SecretStr(DEV_API_KEY)
    log_level: str | None = None
    webhook_timeout: float = 10.0
    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 100
    gateway_success_rate: float = 0.9
    gateway_delay_min: float = 2.0
    gateway_delay_max: float = 5.0

    @property
    def resolved_log_level(self) -> str:
        """Уровень логирования: явный либо DEBUG для dev / INFO для prod."""
        if self.log_level:
            return self.log_level.upper()
        return "DEBUG" if self.app_env == "dev" else "INFO"

    @model_validator(mode="after")
    def _forbid_default_key_in_prod(self) -> Self:
        key = self.api_key.get_secret_value()
        if self.app_env == "prod" and key in ("", DEV_API_KEY):
            raise ValueError("В prod запрещён запуск с пустым или дефолтным API_KEY")
        return self
