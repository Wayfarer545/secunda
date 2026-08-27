import logging
import sys

from loguru import logger

from app.config import Settings

LOG_FORMAT = (
    "<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | "
    "<cyan>{module}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

QUIET_LOGGERS = ("sqlalchemy.engine", "aio_pika", "aiormq", "dishka", "httpx", "httpcore")


class InterceptHandler(logging.Handler):
    """Перенаправляет записи stdlib logging в loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = logging.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging(settings: Settings) -> None:
    """Настраивает loguru и перехват stdlib-логов."""
    logger.remove()
    logger.add(sys.stderr, format=LOG_FORMAT, level=settings.resolved_log_level)

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    for name in QUIET_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)


def reset_stdlib_handlers(*prefixes: str) -> None:
    """Снимает чужие хендлеры с логгеров, возвращая их в общий конвейер loguru.

    Нужен для faststream: его CLI навешивает собственные хендлеры уже после
    setup_logging, в обход перехвата.
    """
    for name, obj in logging.root.manager.loggerDict.items():
        if isinstance(obj, logging.Logger) and name.startswith(prefixes):
            obj.handlers.clear()
            obj.propagate = True
