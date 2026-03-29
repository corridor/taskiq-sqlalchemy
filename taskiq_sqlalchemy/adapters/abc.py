from abc import ABC, abstractmethod
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine


class DialectAdapter(ABC):
    """
    Abstract interface for database-specific pub/sub operations.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def broker_startup(self) -> None:
        return

    async def broker_shutdown(self) -> None:
        return

    async def worker_startup(self) -> None:
        return

    async def worker_shutdown(self) -> None:
        return

    async def client_startup(self) -> None:
        return

    async def client_shutdown(self) -> None:
        return

    @abstractmethod
    async def notify(self, channel: str, payload: str) -> None:
        """
        Send a pub/sub notification on *channel* with *payload*.

        Must not block; may be called from any coroutine.
        """

    @abstractmethod
    def listen(self, channel: str) -> AsyncGenerator[str, None]:
        """
        Return an async generator that yields string payloads as they arrive
        on *channel*.  The generator runs indefinitely; the broker cancels it
        on shutdown.
        """
