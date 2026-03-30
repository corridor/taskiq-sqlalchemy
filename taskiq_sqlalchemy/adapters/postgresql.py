import logging
import math
import typing as t

import anyio
import anyio.abc
import sqlalchemy as sa
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from sqlalchemy.ext.asyncio import AsyncEngine

from taskiq_sqlalchemy.adapters.abc import DialectAdapter


logger = logging.getLogger(__name__)


class PostgresDialectAdapter(DialectAdapter):
    """
    Postgres LISTEN/NOTIFY via a dedicated asyncpg connection.

    We intentionally hold *one* raw asyncpg connection for listening from the
    SQLAlchemy pool. That connection must not be returned to the pool while
    the listener is active.
    """

    _listener_conn: t.Optional[sa.PoolProxiedConnection]

    # send/receive for the subscribed channel.
    _send_stream: t.Optional[MemoryObjectSendStream[str]]
    _recv_stream: t.Optional[MemoryObjectReceiveStream[str]]

    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine)
        self._listener_conn = None
        self._send_stream = None
        self._recv_stream = None
        self._stop_event = anyio.Event()

    async def worker_startup(self) -> None:
        self._listener_conn = await self.engine.raw_connection()
        logger.debug("PostgresDialectAdapter: raw asyncpg connection acquired")

    async def worker_shutdown(self) -> None:
        self._stop_event.set()

        if self._send_stream is not None:
            await self._send_stream.aclose()

        if self._listener_conn is not None:
            try:
                self._listener_conn.close()
            except Exception:
                logger.exception(
                    "PostgresDialectAdapter: error closing listener conn",
                )
            self._listener_conn = None

    async def notify(self, channel: str, payload: str) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                sa.text("SELECT pg_notify(:channel, :payload)"),
                {"channel": channel, "payload": payload},
            )

    def listen(self, channel: str) -> t.AsyncGenerator[str, None]:
        return self._listen_gen(channel)

    async def _listen_gen(self, channel: str) -> t.AsyncGenerator[str, None]:
        if self._listener_conn is None or self._listener_conn.driver_connection is None:
            raise RuntimeError("Connection not available for listen()")

        # Unbounded buffer: asyncpg delivers notifications synchronously, so we
        # must never block the callback.  If the consumer is slow the buffer
        # grows; that's acceptable — tasks are short-lived string IDs.
        self._send_stream, self._recv_stream = anyio.create_memory_object_stream[str](
            max_buffer_size=math.inf,
        )

        def _callback(conn: object, pid: int, channel_: str, payload: str) -> None:
            # Called synchronously by asyncpg on the event-loop thread.
            # send_nowait() is safe here — it never suspends.
            try:
                self._send_stream.send_nowait(payload)
            except anyio.WouldBlock:
                # Should never happen with max_buffer_size=inf, but guard
                # defensively to prevent a silent drop crashing the callback.
                logger.warning(
                    "PostgresDialectAdapter: notification buffer full on channel %r; payload %r dropped",
                    channel_,
                    payload,
                )

        await self._listener_conn.driver_connection.add_listener(channel, _callback)
        logger.debug("PostgresDialectAdapter: listening on channel %r", channel)

        try:
            async with self._recv_stream:
                async for payload in self._recv_stream:
                    if self._stop_event.is_set():
                        return
                    yield payload
        finally:
            # Always remove the asyncpg listener and clean up, even if the
            # generator is garbage-collected or cancelled mid-iteration.
            if self._listener_conn is not None:
                try:
                    await self._listener_conn.driver_connection.remove_listener(
                        channel,
                        _callback,
                    )
                except Exception:
                    logger.debug(
                        "PostgresDialectAdapter: could not remove listener for %r (connection may already be closed)",
                        channel,
                        exc_info=True,
                    )
            await self._send_stream.aclose()
