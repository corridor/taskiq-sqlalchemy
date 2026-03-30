"""tests/broker/adapters/test_mssql.py

Tests for ``MSSQLDialectAdapter`` in isolation.
"""

import typing as t
from unittest.mock import MagicMock

import anyio
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from taskiq_sqlalchemy.adapters import resolve_adapter
from taskiq_sqlalchemy.adapters.mssql import (
    MSSQLDialectAdapter,
    _MAX_SB_NAME,
    _sb_queue_name,
    _sb_service_name,
)
from taskiq_sqlalchemy.manager import SQLAlchemyManager


_MSSQL_URL = "mssql+aioodbc://sa:gOxN5hbl7geTwgvS@localhost:1433/taskiq?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"


@pytest.fixture
async def mssql_engine() -> t.AsyncGenerator[AsyncEngine, None]:

    engine = create_async_engine(_MSSQL_URL)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def mssql_channel(task_id: str) -> str:
    """
    Derive a test-unique SB channel name from the test's task_id UUID.

    SB names are capped at 128 chars. We take the last 16 hex digits of the
    UUID (24 chars total with "mssql_test_" prefix).
    """
    short = task_id.replace("-", "")[-16:].upper()
    return f"mssql_test_{short}"


@pytest.fixture
async def mssql_adapter(
    mssql_engine: AsyncEngine,
    mssql_channel: str,
) -> t.AsyncGenerator[MSSQLDialectAdapter, None]:
    adapter = MSSQLDialectAdapter(mssql_engine)
    await adapter.ensure_queue(mssql_channel)
    try:
        yield adapter
    finally:
        adapter._stop_event.set()
        for channel in list(adapter._conv_handles):
            await adapter._end_conversation(channel)


@pytest.mark.parametrize(
    ("channel", "expected"),
    [
        ("taskiq", "taskiq_sb_taskiq"),
        ("my-channel", "taskiq_sb_my-channel"),
        # Truncation: result must be ≤ _MAX_SB_NAME chars
        ("a" * 200, "taskiq_sb_" + "a" * (_MAX_SB_NAME - len("taskiq_sb_"))),
    ],
)
def test_sb_queue_name(channel: str, expected: str) -> None:
    """_sb_queue_name: prefix + channel, truncated to _MAX_SB_NAME."""
    result = _sb_queue_name(channel)
    assert result == expected
    assert len(result) <= _MAX_SB_NAME


@pytest.mark.parametrize(
    ("channel", "expected"),
    [
        ("taskiq", "taskiq_svc_taskiq"),
        ("my-channel", "taskiq_svc_my-channel"),
        ("a" * 200, "taskiq_svc_" + "a" * (_MAX_SB_NAME - len("taskiq_svc_"))),
    ],
)
def test_sb_service_name(channel: str, expected: str) -> None:
    """_sb_service_name: prefix + channel, truncated to _MAX_SB_NAME."""
    result = _sb_service_name(channel)
    assert result == expected
    assert len(result) <= _MAX_SB_NAME


def test_resolve_adapter_returns_mssql() -> None:
    """resolve_adapter() must return MSSQLDialectAdapter for the mssql dialect."""
    engine = MagicMock()
    engine.dialect.name = "mssql"

    manager = SQLAlchemyManager.__new__(SQLAlchemyManager)
    manager._engine = engine
    manager.queue_cls = MagicMock()
    manager.result_cls = MagicMock()
    manager.schedule_cls = MagicMock()

    adapter = resolve_adapter(manager)
    assert isinstance(adapter, MSSQLDialectAdapter)


@pytest.mark.mssql
@pytest.mark.anyio
async def test_ensure_queue_idempotent(
    mssql_adapter: MSSQLDialectAdapter,
    mssql_channel: str,
) -> None:
    """Calling ensure_queue() a second time must not raise."""
    await mssql_adapter.ensure_queue(mssql_channel)


@pytest.mark.mssql
@pytest.mark.anyio
async def test_notify_sends_message(
    mssql_adapter: MSSQLDialectAdapter,
    mssql_engine: AsyncEngine,
    mssql_channel: str,
) -> None:
    """
    notify() must place a message on the SB queue that RECEIVE can read.
    """
    payload = "task-id-mssql-notify"
    queue_name = _sb_queue_name(mssql_channel)

    await mssql_adapter.notify(mssql_channel, payload)

    async with mssql_engine.begin() as conn:
        result = await conn.execute(
            sa.text(f"WAITFOR (  RECEIVE TOP(1) message_body  FROM [{queue_name}]), TIMEOUT 3000;")
        )
        row = result.fetchone()

    assert row is not None, "Expected a message in the SB queue after notify()"
    assert row.message_body.decode("utf-8") == payload


@pytest.mark.mssql
@pytest.mark.anyio
async def test_listen_yields_notified_payload(
    mssql_adapter: MSSQLDialectAdapter,
    mssql_channel: str,
) -> None:
    """listen() must yield the payload sent by notify()."""
    payload = "task-id-listen-test"
    received: list[str] = []

    await mssql_adapter.notify(mssql_channel, payload)

    async def _collect() -> None:
        async for p in mssql_adapter.listen(mssql_channel):
            received.append(p)
            mssql_adapter._stop_event.set()
            return

    with anyio.fail_after(10.0):
        await _collect()

    assert received == [payload]


@pytest.mark.mssql
@pytest.mark.anyio
async def test_listen_stops_on_stop_event(
    mssql_engine: AsyncEngine,
    mssql_channel: str,
) -> None:
    """
    A pre-set _stop_event causes listen() to exit immediately
    without yielding anything (empty queue, short timeout).
    """
    adapter = MSSQLDialectAdapter(mssql_engine)
    await adapter.ensure_queue(mssql_channel)
    adapter._stop_event.set()

    collected: list[str] = []
    with anyio.fail_after(5.0):
        collected.extend([p async for p in adapter.listen(mssql_channel)])

    assert collected == []


@pytest.mark.mssql
@pytest.mark.anyio
async def test_worker_shutdown_ends_conversations(
    mssql_engine: AsyncEngine,
    mssql_channel: str,
) -> None:
    """worker_shutdown() must end all cached conversations and clear _conv_handles."""
    adapter = MSSQLDialectAdapter(mssql_engine)
    await adapter.ensure_queue(mssql_channel)
    await adapter.worker_startup()

    assert len(adapter._conv_handles) >= 1

    await adapter.worker_shutdown()

    assert adapter._conv_handles == {}
