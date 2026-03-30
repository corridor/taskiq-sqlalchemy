"""tests/broker/adapters/test_oracle.py

Tests for ``OracleDialectAdapter`` in isolation.

Architecture of Oracle AQ
--------------------------
* ``ensure_queue(channel)`` creates a RAW queue table + queue via DBMS_AQADM.
  Idempotent: ORA-24001 / ORA-24006 are silently ignored.
* ``notify(channel, payload)`` enqueues a RAW message with ENQ_IMMEDIATE.
* ``listen(channel)`` dequeues in a loop with DEQ_NO_WAIT; sleeps 0.5 s when
  empty; exits when ``_stop_event`` is set.
"""

import typing as t
from unittest.mock import MagicMock, patch

import anyio
import anyio.to_thread
import oracledb
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from taskiq_sqlalchemy.adapters.oracle import OracleDialectAdapter, _oracle_queue_name


_ORA_URL = "oracle+oracledb://taskiq_user:gOxN5hbl7geTwgvS@localhost:1521/?service_name=taskiq"


@pytest.fixture
async def ora_engine() -> t.AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(_ORA_URL)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def ora_adapter(
    ora_engine: AsyncEngine,
) -> t.AsyncGenerator[OracleDialectAdapter, None]:
    adapter = OracleDialectAdapter(ora_engine)
    await adapter.ensure_queue("test_ora_channel")
    try:
        yield adapter
    finally:
        await adapter.purge_queue("test_ora_channel")
        adapter._stop_event.set()


@pytest.mark.parametrize(
    ("channel", "expected"),
    [
        ("taskiq", "TASKIQ"),
        ("my-channel", "MY_CHANNEL"),
        ("my.channel.name", "MY_CHANNEL_NAME"),
        ("a" * 30, "A" * 24),  # truncated to _MAX_QUEUE_NAME = 24
        ("ALREADY_UPPER", "ALREADY_UPPER"),
    ],
)
def test_oracle_queue_name(channel: str, expected: str) -> None:
    """_oracle_queue_name: uppercase, separator replacement, truncation."""
    assert _oracle_queue_name(channel) == expected


@pytest.mark.anyio
async def test_broker_startup_raises_in_thick_mode() -> None:
    """
    broker_startup() must raise RuntimeError when oracledb is NOT in thin mode.
    Verified via mock — no real thick Oracle client required.
    """
    adapter = OracleDialectAdapter(MagicMock())

    with (
        patch(
            "taskiq_sqlalchemy.adapters.oracle.oracledb.is_thin_mode",
            return_value=False,
        ),
        pytest.raises(RuntimeError, match="Thin mode"),
    ):
        await adapter.broker_startup()


@pytest.mark.oracle
@pytest.mark.anyio
async def test_ensure_queue_idempotent(
    ora_adapter: OracleDialectAdapter,
) -> None:
    """Calling ensure_queue() a second time must not raise ORA-24001/ORA-24006."""
    await ora_adapter.ensure_queue("test_ora_channel")


@pytest.mark.oracle
@pytest.mark.anyio
async def test_notify_enqueues_message(
    ora_adapter: OracleDialectAdapter,
    ora_engine: AsyncEngine,
) -> None:
    """
    notify() must place a RAW message on the AQ queue.
    Verified by performing a raw dequeue immediately after.
    """
    channel = "test_ora_channel"
    payload = "task-id-oracle-notify"
    queue_name = _oracle_queue_name(channel)

    await ora_adapter.notify(channel, payload)

    async with ora_engine.begin() as conn:
        raw = await conn.get_raw_connection()
        driver_conn = raw.driver_connection
        queue = await anyio.to_thread.run_sync(driver_conn.queue, queue_name)
        queue.deqoptions.wait = oracledb.DEQ_NO_WAIT
        queue.deqoptions.navigation = oracledb.DEQ_FIRST_MSG
        msg = await queue.deqone()

    assert msg is not None, "Expected a message in the AQ queue after notify()"
    assert msg.payload.decode() == payload


@pytest.mark.oracle
@pytest.mark.anyio
async def test_listen_yields_notified_payload(
    ora_adapter: OracleDialectAdapter,
) -> None:
    """listen() must yield the payload enqueued by notify()."""
    channel = "test_ora_channel"
    payload = "task-id-listen-test"
    received: list[str] = []

    await ora_adapter.notify(channel, payload)

    async def _collect() -> None:
        async for p in ora_adapter.listen(channel):
            received.append(p)
            ora_adapter._stop_event.set()
            return

    with anyio.fail_after(10.0):
        await _collect()

    assert received == [payload]


@pytest.mark.oracle
@pytest.mark.anyio
async def test_listen_stops_on_stop_event(
    ora_engine: AsyncEngine,
) -> None:
    """
    A pre-set _stop_event causes listen() to exit immediately
    without yielding anything (empty queue).
    """
    adapter = OracleDialectAdapter(ora_engine)
    await adapter.ensure_queue("test_ora_channel")
    adapter._stop_event.set()

    collected: list[str] = []
    with anyio.fail_after(5.0):
        collected.extend([p async for p in adapter.listen("test_ora_channel")])

    assert collected == []
