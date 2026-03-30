"""taskiq_sqlalchemy.adapters.mssql

MSSQLDialectAdapter — pub/sub for Microsoft SQL Server using Service Broker.
"""

import logging
import typing as t

import anyio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from taskiq_sqlalchemy.adapters.abc import DialectAdapter


logger = logging.getLogger(__name__)

# Service Broker object name limits
_MAX_SB_NAME = 128
_SB_QUEUE_PREFIX = "taskiq_sb_"
_SB_SERVICE_PREFIX = "taskiq_svc_"

# How long WAITFOR RECEIVE blocks before timing out (milliseconds).
# Keep short so the stop-event check is responsive.
_WAITFOR_TIMEOUT_MS = 500


def _sb_queue_name(channel: str) -> str:
    """Derive a safe Service Broker queue name from a channel name."""
    raw = f"{_SB_QUEUE_PREFIX}{channel}"
    return raw[:_MAX_SB_NAME]


def _sb_service_name(channel: str) -> str:
    """Derive a safe Service Broker service name from a channel name."""
    raw = f"{_SB_SERVICE_PREFIX}{channel}"
    return raw[:_MAX_SB_NAME]


class MSSQLDialectAdapter(DialectAdapter):
    """
    SQL Server Service Broker adapter for taskiq-sqlalchemy.

    One long-lived conversation handle is maintained per channel.
    """

    # channel → GUID conversation handle (str)
    _conv_handles: dict[str, str]
    _stop_event: anyio.Event

    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine)
        self._conv_handles = {}
        self._stop_event = anyio.Event()

    async def client_startup(self) -> None:
        """Ensure the default 'taskiq' channel SB objects exist on startup."""
        await self.ensure_queue("taskiq")

    async def broker_shutdown(self) -> None:
        self._stop_event.set()

    async def worker_startup(self) -> None:
        """Open a long-lived conversation handle for the default channel."""
        await self.ensure_queue("taskiq")
        await self._open_conversation("taskiq")

    async def worker_shutdown(self) -> None:
        """End all open conversations and signal the listen() loops to stop."""
        self._stop_event.set()
        for channel in list(self._conv_handles):
            await self._end_conversation(channel)

    async def ensure_queue(self, channel: str) -> None:
        """
        Create the Service Broker queue and service for *channel* if they do
        not already exist.

        Safe to call multiple times — uses ``IF NOT EXISTS`` guards via
        ``sys.service_queues`` / ``sys.services``.
        """
        queue_name = _sb_queue_name(channel)
        service_name = _sb_service_name(channel)

        async with self.engine.begin() as conn:
            # Create the queue
            await conn.execute(
                sa.text(
                    "IF NOT EXISTS ("
                    "  SELECT 1 FROM sys.service_queues"
                    "  WHERE name = :queue_name"
                    ") "
                    "EXEC('CREATE QUEUE [{queue_name}]')".replace("{queue_name}", queue_name)
                ),
                {"queue_name": queue_name},
            )

            # Create the service bound to the queue
            await conn.execute(
                sa.text(
                    "IF NOT EXISTS ("
                    "  SELECT 1 FROM sys.services"
                    "  WHERE name = :service_name"
                    ") "
                    "EXEC('CREATE SERVICE [{service_name}]"
                    " ON QUEUE [{queue_name}]"
                    " ([DEFAULT])')".replace("{service_name}", service_name).replace("{queue_name}", queue_name)
                ),
                {
                    "service_name": service_name,
                    "queue_name": queue_name,
                },
            )

        logger.info(
            "MSSQLDialectAdapter: ensured SB queue %r and service %r",
            queue_name,
            service_name,
        )

    async def _open_conversation(self, channel: str) -> str:
        """
        Begin a Service Broker dialog conversation and cache the handle.

        A conversation is required before ``SEND ON CONVERSATION`` can be
        called.  We open one per channel and reuse it for all ``notify()``
        calls on that channel.
        """
        if channel in self._conv_handles:
            return self._conv_handles[channel]

        service_name = _sb_service_name(channel)

        async with self.engine.begin() as conn:
            result = await conn.execute(
                sa.text(
                    "DECLARE @conv_handle UNIQUEIDENTIFIER; "
                    "BEGIN DIALOG CONVERSATION @conv_handle "
                    "  FROM SERVICE :service_name "
                    "  TO SERVICE :service_name "
                    "  ON CONTRACT [DEFAULT] "
                    "  WITH ENCRYPTION = OFF; "
                    "SELECT @conv_handle AS conv_handle;"
                ),
                {"service_name": service_name},
            )
            row = result.fetchone()

        if row is None:
            raise RuntimeError(f"MSSQLDialectAdapter: failed to open conversation for channel {channel!r}")

        handle = str(row.conv_handle)
        self._conv_handles[channel] = handle
        logger.debug(
            "MSSQLDialectAdapter: opened conversation %r on channel %r",
            handle,
            channel,
        )
        return handle

    async def _end_conversation(self, channel: str) -> None:
        """End the cached conversation for *channel* and remove it."""
        handle = self._conv_handles.pop(channel, None)
        if handle is None:
            return

        try:
            async with self.engine.begin() as conn:
                await conn.execute(
                    sa.text("END CONVERSATION :conv_handle WITH CLEANUP;"),
                    {"conv_handle": handle},
                )
        except Exception:
            logger.exception("MSSQLDialectAdapter: could not end conversation %r (may already be closed)", handle)
        else:
            logger.debug(
                "MSSQLDialectAdapter: ended conversation %r on channel %r",
                handle,
                channel,
            )

    async def notify(self, channel: str, payload: str) -> None:
        """
        Send a Service Broker message on *channel* carrying *payload*.

        The payload is the ``task_id`` string, encoded as UTF-8.
        The message is sent inside the same transaction that committed the
        ``taskiq_queue`` row — but since ``kick()`` commits the queue row
        first and then calls ``notify()``, we open a new short transaction
        here.  The SEND is committed immediately so the worker's WAITFOR
        RECEIVE sees it without delay.

        If no conversation handle exists for the channel yet, one is opened
        on the first call.  This covers the case where ``notify()`` is
        called from a client process that never called ``worker_startup()``.
        """
        # Acquire or open a conversation handle
        handle = await self._open_conversation(channel)

        async with self.engine.begin() as conn:
            await conn.execute(
                sa.text("SEND ON CONVERSATION :conv_handle MESSAGE TYPE [DEFAULT] (:payload);"),
                {
                    "conv_handle": handle,
                    "payload": payload.encode(),
                },
            )

        logger.debug(
            "MSSQLDialectAdapter: sent task_id=%r on channel %r (conv=%r)",
            payload,
            channel,
            handle,
        )

    def listen(self, channel: str) -> t.AsyncGenerator[str, None]:
        return self._listen_gen(channel)

    async def _listen_gen(self, channel: str) -> t.AsyncGenerator[str, None]:
        """
        WAITFOR RECEIVE loop.

        Blocks at most ``_WAITFOR_TIMEOUT_MS`` milliseconds per iteration,
        then checks the stop event.  When a message arrives it is committed
        (removing it from the SB queue) and the payload (task_id) is yielded.
        """
        queue_name = _sb_queue_name(channel)

        logger.debug(
            "MSSQLDialectAdapter: starting WAITFOR RECEIVE loop on queue %r",
            queue_name,
        )

        while not self._stop_event.is_set():
            async with self.engine.begin() as conn:
                result = await conn.execute(
                    sa.text(
                        "WAITFOR ("
                        "  RECEIVE TOP(1)"
                        "    conversation_handle,"
                        "    message_body"
                        "  FROM [{queue_name}]"
                        "), TIMEOUT :timeout_ms;".replace("{queue_name}", queue_name)
                    ),
                    {"timeout_ms": _WAITFOR_TIMEOUT_MS},
                )
                row = result.fetchone()
                # The transaction commits here, removing the message from the
                # SB queue and preventing re-delivery.

            if row is None:
                # Timeout — no message arrived; loop back and check stop event.
                continue

            # Decode the RAW message body back to a task_id string.
            raw_body: bytes = row.message_body
            payload = raw_body.decode()
            logger.debug(
                "MSSQLDialectAdapter: received task_id=%r from queue %r",
                payload,
                queue_name,
            )
            yield payload

        logger.debug(
            "MSSQLDialectAdapter: WAITFOR loop exited for queue %r",
            queue_name,
        )
