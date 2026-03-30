"""taskiq_sqlalchemy.broker

SQLAlchemy-backed TaskIQ broker.

The broker owns the queue table and delegates all pub/sub to the DialectAdapter.
"""

import logging
import typing as t

import sqlalchemy as sa
from taskiq import AckableMessage, AsyncBroker, BrokerMessage, TaskiqEvents, TaskiqState

from taskiq_sqlalchemy.adapters import resolve_adapter
from taskiq_sqlalchemy.adapters.abc import DialectAdapter
from taskiq_sqlalchemy.manager import SQLAlchemyManager


logger = logging.getLogger(__name__)


class SQLAlchemyBroker(AsyncBroker):
    """
    TaskIQ broker backed by any SQLAlchemy async engine.
    """

    _adapter: DialectAdapter

    def __init__(
        self,
        manager: SQLAlchemyManager,
        *,
        channel_name: str = "taskiq",
        adapter: t.Optional[DialectAdapter] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.manager = manager
        self.channel_name = channel_name
        self._adapter = adapter or resolve_adapter(manager)

        self.add_event_handler(TaskiqEvents.WORKER_STARTUP, self.on_worker_startup)
        self.add_event_handler(TaskiqEvents.WORKER_SHUTDOWN, self.on_worker_shutdown)
        self.add_event_handler(TaskiqEvents.CLIENT_STARTUP, self.on_client_startup)
        self.add_event_handler(TaskiqEvents.CLIENT_SHUTDOWN, self.on_client_shutdown)

    async def startup(self) -> None:
        await super().startup()
        await self._adapter.broker_startup()
        logger.info(
            "SQLAlchemyBroker started (dialect=%s, channel=%r)",
            self.manager.engine.dialect.name,
            self.channel_name,
        )

    async def shutdown(self) -> None:
        await self._adapter.broker_shutdown()
        await super().shutdown()
        logger.info("SQLAlchemyBroker shut down")

    async def kick(self, message: BrokerMessage) -> None:
        """
        Persist the message then notify listening workers.
        The two steps are deliberately *not* in the same transaction:
        we want the row committed before we send the notify so that workers
        that wake up immediately can actually read it.
        """
        serialised = message.model_dump_json().encode()

        async with self.manager.engine.begin() as conn:
            await conn.execute(
                sa.insert(self.manager.queue_cls).values(
                    task_id=message.task_id,
                    channel=self.channel_name,
                    task_name=message.task_name,
                    message=serialised,
                ),
            )

        # Notify outside the transaction so the row is visible to workers
        await self._adapter.notify(self.channel_name, message.task_id)
        logger.debug("Kicked task %s (%s)", message.task_id, message.task_name)

    async def listen(self) -> t.AsyncGenerator[AckableMessage, None]:
        """
        Yield ``AckableMessage`` objects as they arrive.

        For Postgres (and other native-pub/sub dialects) this is push-based.
        For polling adapters each yielded item triggers a table read.
        """
        async for task_id in self._adapter.listen(self.channel_name):
            message = await self._fetch_message(task_id)
            if message is not None:
                yield message

    async def _fetch_message(self, task_id: str) -> t.Optional[AckableMessage]:
        """
        Fetch and deserialise one message, then delete it from the queue.
        """
        async with self.manager.engine.begin() as conn:
            if self.manager.engine.dialect.name == "mysql":
                # MySQL does not support DELETE .. RETURNING
                # SELECT FOR UPDATE SKIP LOCKED atomically claims the row.
                result = await conn.execute(
                    sa.select(self.manager.queue_cls)
                    .filter_by(task_id=task_id, channel=self.channel_name)
                    .with_for_update(skip_locked=True)
                )
                row = result.first()
                if row is None:
                    # Another worker already locked/deleted it
                    return None
                await conn.execute(
                    sa.delete(self.manager.queue_cls).filter_by(task_id=task_id, channel=self.channel_name)
                )
            else:
                result = await conn.execute(
                    sa.delete(self.manager.queue_cls)
                    .filter_by(task_id=task_id, channel=self.channel_name)
                    .returning(self.manager.queue_cls.message)
                )
                row = result.first()
                if row is None:
                    # Another worker already claimed it
                    return None

        async def ack() -> None:
            # No-op: the row was already deleted when claimed.
            return None

        try:
            broker_message = BrokerMessage.model_validate_json(row.message)
            return AckableMessage(data=broker_message.message, ack=ack)
        except Exception:
            logger.exception("Failed to deserialise message for task %s", task_id)
            return None

    async def on_worker_startup(self, state: TaskiqState) -> None:
        await self._adapter.worker_startup()

    async def on_worker_shutdown(self, state: TaskiqState) -> None:
        await self._adapter.worker_shutdown()

    async def on_client_startup(self, state: TaskiqState) -> None:
        await self._adapter.client_startup()

    async def on_client_shutdown(self, state: TaskiqState) -> None:
        await self._adapter.client_shutdown()
