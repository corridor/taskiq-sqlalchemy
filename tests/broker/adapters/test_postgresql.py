"""tests/broker/adapters/test_postgresql.py

Tests for ``PostgresDialectAdapter`` in isolation.
"""

import asyncio
import typing as t

import anyio
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from taskiq_sqlalchemy.adapters.postgresql import PostgresDialectAdapter


pytestmark = [pytest.mark.anyio, pytest.mark.postgresql]

_PG_URL = "postgresql+asyncpg://taskiq_user:taskiq_pwd@localhost:5432/taskiq"


@pytest.fixture
async def pg_engine() -> t.AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(_PG_URL, echo=False)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def pg_adapter(
    pg_engine: AsyncEngine,
) -> t.AsyncGenerator[PostgresDialectAdapter, None]:
    adapter = PostgresDialectAdapter(pg_engine)
    await adapter.worker_startup()
    try:
        yield adapter
    finally:
        await adapter.worker_shutdown()


async def test_notify_sends_pg_notify(pg_engine: AsyncEngine) -> None:
    """
    notify() must deliver a payload via pg_notify.
    We verify by holding a raw asyncpg LISTEN and asserting the notification
    arrives within a short timeout.
    """
    channel = "test_notify_channel"
    payload = "hello-from-notify"
    received: list[str] = []

    # Acquire a raw asyncpg connection to LISTEN
    raw_conn = await pg_engine.raw_connection()
    driver_conn = raw_conn.driver_connection

    def _on_notification(conn: object, pid: int, ch: str, p: str) -> None:
        received.append(p)

    await driver_conn.add_listener(channel, _on_notification)

    try:
        adapter = PostgresDialectAdapter(pg_engine)
        await adapter.notify(channel, payload)

        # Give Postgres a moment to deliver the notification
        with anyio.fail_after(3.0):
            while payload not in received:
                with anyio.move_on_after(0.05):
                    await anyio.Event().wait()
    finally:
        await driver_conn.remove_listener(channel, _on_notification)
        raw_conn.close()

    assert payload in received


async def test_listen_receives_notification(pg_adapter: PostgresDialectAdapter) -> None:
    """
    After worker_startup(), listen() must yield the payload sent by notify().
    """
    channel = "test_listen_channel"
    payload = "task-id-xyz"
    received: list[str] = []

    async def _collect() -> None:
        async for p in pg_adapter.listen(channel):
            received.append(p)
            return  # stop after first message

    # Start listener in background, then notify
    async with anyio.create_task_group() as tg:
        tg.start_soon(_collect)
        # Small delay to let the LISTEN register before we notify
        await asyncio.sleep(0.1)
        await pg_adapter.notify(channel, payload)

        with anyio.fail_after(5.0):
            while not received:
                with anyio.move_on_after(0.05):
                    await anyio.Event().wait()
        tg.cancel_scope.cancel()

    assert received == [payload]


async def test_worker_shutdown_releases_connection(
    pg_engine: AsyncEngine,
) -> None:
    """After worker_shutdown(), _listener_conn is None."""
    adapter = PostgresDialectAdapter(pg_engine)
    await adapter.worker_startup()
    assert adapter._listener_conn is not None

    await adapter.worker_shutdown()
    assert adapter._listener_conn is None
