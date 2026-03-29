import typing as t

import anyio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from taskiq_sqlalchemy.adapters.abc import DialectAdapter
from taskiq_sqlalchemy.models import TaskiqQueueMixin


class PollingAdapter(DialectAdapter):
    """
    A pure-SQL adapter that polls the queue table instead of using native
    pub/sub.  Used when no specific adapter is registered for a dialect.

    Not ideal for high-throughput scenarios, but means the library works on
    *any* SQLAlchemy-supported database out of the box.
    """

    POLL_INTERVAL_SECS = 2
    POLL_RESULT_LIMIT = 25

    def __init__(self, engine: AsyncEngine, queue_cls: type[TaskiqQueueMixin]) -> None:
        super().__init__(engine)
        self.queue_cls = queue_cls
        self._stop_event = anyio.Event()

    async def worker_shutdown(self) -> None:
        self._stop_event.set()

    async def notify(self, channel: str, payload: str) -> None:
        # No-op for polling mode — the broker writes to the queue table directly
        # and the poller reads it.  notify() is still called by the broker but
        # is a no-op here because the poll loop does the wakeup.
        pass

    def listen(self, channel: str) -> t.AsyncGenerator[str, None]:
        return self._poll_gen(channel)

    async def _poll_gen(self, channel: str) -> t.AsyncGenerator[str, None]:
        while not self._stop_event.is_set():
            async with self.engine.begin() as conn:
                result = await conn.execute(
                    sa.select(self.queue_cls.task_id)
                    .filter_by(channel=channel)
                    .limit(self.POLL_RESULT_LIMIT)
                    .with_for_update(skip_locked=True)
                )
                rows = result.fetchall()
                for row in rows:
                    yield str(row.task_id)
            if not rows:
                with anyio.move_on_after(self.POLL_INTERVAL_SECS):
                    await self._stop_event.wait()
