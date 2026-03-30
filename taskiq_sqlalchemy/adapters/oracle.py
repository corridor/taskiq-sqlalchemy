"""taskiq_sqlalchemy.oracle_adapter

OracleDialectAdapter — pub/sub for Oracle Database using Advanced Queuing (AQ).
"""

import logging
import typing as t

import anyio
import anyio.to_thread
import oracledb
import sqlalchemy as sa
from oracledb import DEQ_FIRST_MSG, DEQ_NO_WAIT
from sqlalchemy.ext.asyncio import AsyncEngine

from taskiq_sqlalchemy.adapters.abc import DialectAdapter


logger = logging.getLogger(__name__)

# Maximum characters in an Oracle AQ queue name (safe for all Oracle versions)
_MAX_QUEUE_NAME = 24
_QUEUE_TABLE_SUFFIX = "_QT"  # AQ requires a separate queue table object


def _oracle_queue_name(channel: str) -> str:
    """
    Derive a safe Oracle AQ queue name from a taskiq channel name.

    Rules:
      - Uppercase (Oracle identifiers are case-insensitive but uppercase by convention)
      - Max _MAX_QUEUE_NAME characters for the queue name itself
      - Queue table name = queue name + _QT (Oracle AQ requirement)
    """
    raw = channel.upper().replace("-", "_").replace(".", "_")
    # Reserve room for the _QT suffix on the queue table name
    return raw[:_MAX_QUEUE_NAME]


class OracleDialectAdapter(DialectAdapter):
    """
    Oracle Advanced Queuing (AQ) adapter for taskiq-sqlalchemy.
    """

    def __init__(
        self,
        engine: AsyncEngine,
    ) -> None:
        super().__init__(engine)

        self._stop_event = anyio.Event()

    async def broker_startup(self) -> None:
        if oracledb.is_thin_mode() is False:
            raise RuntimeError(
                "OracleDialectAdapter requires python-oracledb Thin mode. "
                "Do not call oracledb.init_oracle_client() before using this adapter. "
                "Asyncio support is only available in Thin mode.",
            )

        await self.ensure_queue("taskiq")

    async def broker_shutdown(self) -> None:
        self._stop_event.set()

    async def ensure_queue(self, channel: str) -> None:
        """
        Create the AQ queue table and queue if they do not already exist.

        Called automatically by listen() and notify() on first use.
        Safe to call multiple times — uses IF NOT EXISTS semantics via
        exception handling on the DBMS_AQADM calls.

        This is the Oracle equivalent of the Postgres CREATE TABLE IF NOT EXISTS
        in _create_table(). It must run before any enqueue or dequeue.
        """
        queue_name = _oracle_queue_name(channel)
        queue_table = queue_name + _QUEUE_TABLE_SUFFIX

        async with self.engine.begin() as conn:
            await conn.execute(
                sa.text("""
                DECLARE
                    v_qt_count  NUMBER;
                    v_q_count   NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO v_qt_count
                    FROM user_queue_tables
                    WHERE queue_table = :queue_table;

                    IF v_qt_count = 0 THEN
                        DBMS_AQADM.CREATE_QUEUE_TABLE(
                            queue_table        => :queue_table,
                            queue_payload_type => 'RAW'
                        );
                    END IF;

                    SELECT COUNT(*) INTO v_q_count
                    FROM user_queues
                    WHERE name = :queue_name;

                    IF v_q_count = 0 THEN
                        DBMS_AQADM.CREATE_QUEUE(
                            queue_name  => :queue_name,
                            queue_table => :queue_table,
                            max_retries => :max_retries
                        );
                        DBMS_AQADM.START_QUEUE(
                            queue_name => :queue_name
                        );
                    END IF;
                END;
                """),
                {
                    "queue_table": queue_table,
                    "queue_name": queue_name,
                    "max_retries": 5,
                },
            )
            logger.info(
                "OracleDialectAdapter: ensured AQ queue table %r and queue %r",
                queue_table,
                queue_name,
            )

    async def purge_queue(self, channel: str) -> None:
        """Purge all messages in the queue."""
        queue_name = _oracle_queue_name(channel)
        queue_table = queue_name + _QUEUE_TABLE_SUFFIX

        async with self.engine.begin() as conn:
            await conn.execute(
                sa.text("""
                DECLARE
                    v_qt_count  NUMBER;
                    v_purge_options DBMS_AQADM.AQ$_PURGE_OPTIONS_T;
                BEGIN
                    SELECT COUNT(*) INTO v_qt_count
                    FROM user_queue_tables
                    WHERE queue_table = :queue_table;

                    IF v_qt_count > 0 THEN
                        v_purge_options.block := TRUE;
                        v_purge_options.delivery_mode := DBMS_AQADM.PERSISTENT_OR_BUFFERED;
                        DBMS_AQADM.PURGE_QUEUE_TABLE(
                            queue_table     => :queue_table,
                            purge_condition => NULL,
                            purge_options   => v_purge_options
                        );
                    END IF;
                END;
                """),
                {
                    "queue_table": queue_table,
                },
            )
            logger.info("OracleDialectAdapter: purged AQ queue table %r", queue_table)

    async def notify(self, channel: str, payload: str) -> None:
        """
        Enqueue a message into the Oracle AQ queue for this channel.
        """
        queue_name = _oracle_queue_name(channel)

        async with self.engine.begin() as conn:
            driver_connection = (await conn.get_raw_connection()).driver_connection
            queue = await anyio.to_thread.run_sync(driver_connection.queue, queue_name)
            # ENQ_IMMEDIATE: commit on enqueue without waiting for connection commit.
            # This ensures the signal is visible to dequeuing workers immediately.
            queue.enqoptions.visibility = oracledb.ENQ_IMMEDIATE
            await queue.enqone(
                driver_connection.msgproperties(payload=payload.encode()),
            )
            logger.debug(
                "OracleDialectAdapter: enqueued task_id=%r on queue %r",
                payload,
                queue_name,
            )

    def listen(self, channel: str) -> t.AsyncGenerator[str, None]:
        return self._listen_gen(channel)

    async def _listen_gen(self, channel: str) -> t.AsyncGenerator[str, None]:
        """
        Blocking dequeue loop on the dedicated listener connection.

        deqone() with DEQ_WAIT_FOREVER blocks at the network level until
        Oracle delivers a message — no polling, no sleep(). This is the async
        equivalent of asyncpg's add_listener callback model: the coroutine
        suspends at `await queue.deqone()` and resumes only when a message
        arrives.

        We use a finite dequeue_wait instead of DEQ_WAIT_FOREVER to allow
        clean shutdown: if dequeue_wait=5, the worst-case shutdown delay is
        5 seconds. When stop_event is set, the generator exits.

        After dequeue we commit immediately (DEQ_ON_COMMIT default) so that
        the message is removed from the AQ queue. The actual task processing
        happens via the broker's _fetch_message() on the queue TABLE — AQ is
        only the transport/signal layer.
        """

        queue_name = _oracle_queue_name(channel)

        async with self.engine.connect() as conn:
            driver_connection = (await conn.get_raw_connection()).driver_connection

            queue = await anyio.to_thread.run_sync(driver_connection.queue, queue_name)

            queue.deqoptions.wait = DEQ_NO_WAIT
            queue.deqoptions.navigation = DEQ_FIRST_MSG

            logger.debug(
                "OracleDialectAdapter: starting dequeue loop on queue %r",
                queue_name,
            )

            while not self._stop_event.is_set():
                try:
                    message = await queue.deqone()
                except Exception as exc:
                    # ORA-25228: timeout/end-of-wait with no message available.
                    # This is the normal "no messages yet" signal when using a
                    # finite wait. Loop back and check stop_event.
                    if "ORA-25228" in str(exc):
                        await anyio.sleep(0.5)
                        continue
                    # Any other error - log and re-raise to let the broker decide
                    # whether to restart the listener.
                    logger.exception(
                        "OracleDialectAdapter: dequeue error on queue %r",
                        queue_name,
                    )
                    raise

                if message is None:
                    # deqone() returned None — finite wait expired, no message.
                    await anyio.sleep(0.5)
                    continue

                # Commit to remove the message from the AQ queue.
                await conn.commit()

                # Decode the RAW payload back to a string (task_id)
                payload = message.payload.decode()
                logger.debug(
                    "OracleDialectAdapter: dequeued task_id=%r from queue %r",
                    payload,
                    queue_name,
                )
                yield payload

            logger.debug(
                "OracleDialectAdapter: dequeue loop exited for queue %r",
                queue_name,
            )
