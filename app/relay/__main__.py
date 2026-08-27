"""Точка входа outbox-релея. Запуск: python -m app.relay."""

import asyncio
from contextlib import suppress

from app.relay.runner import main

if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main())
