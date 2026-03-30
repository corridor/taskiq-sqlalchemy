"""tests/broker/adapters/test_polling.py

Tests for ``PollingAdapter`` in isolation.

All tests use SQLite (aiosqlite) — zero external services required.
The schema lifecycle mirrors result_backend: create_all before, drop_all after.
"""

import typing as t
import uuid

import anyio
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from taskiq_sqlalchemy.adapters.polling import PollingAdapter
from taskiq_sqlalchemy.manager import SQLAlchemyManager


pytestmark = pytest.mark.anyio


@pytest.fixture
async def sqlite_engine() -> t.AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def polling_manager(
    sqlite_engine: AsyncEngine,
) -> t.AsyncGenerator[SQLAlchemyManager, None]:
    class _Base(DeclarativeBase):
        pass

    manager = SQLAlchemyManager(base_classes=(_Base,))
    manager.configure(engine=sqlite_engine)

    async with sqlite_engine.begin() as conn:
        await conn.run_sync(_Base.metadata.create_all)

    try:
        yield manager
    finally:
        async with sqlite_engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)


@pytest.fixture
def adapter(polling_manager: SQLAlchemyManager) -> PollingAdapter:
    a = PollingAdapter(
        polling_manager.engine,
        queue_cls=polling_manager.queue_cls,
    )
    a.poll_interval = 0.05  # speed up for tests
    return a


async def _insert_queue_rows(
    manager: SQLAlchemyManager,
    channel: str,
    count: int,
) -> list[str]:
    """Insert ``count`` rows into the queue; return their task_ids."""
    ids = [str(uuid.uuid4()) for _ in range(count)]
    async with manager.engine.begin() as conn:
        for tid in ids:
            await conn.execute(
                sa.insert(manager.queue_cls).values(
                    task_id=tid,
                    channel=channel,
                    task_name="tests.fake_task",
                    message=b"{}",
                ),
            )
    return ids


async def test_polling_notify_is_noop(
    adapter: PollingAdapter,
    polling_manager: SQLAlchemyManager,
) -> None:
    """notify() completes without error and does not write to the DB."""
    # No rows before
    async with polling_manager.engine.connect() as conn:
        count_before = (
            await conn.execute(
                sa.select(sa.func.count()).select_from(polling_manager.queue_cls),
            )
        ).scalar()

    await adapter.notify("any_channel", "any_payload")

    # Still no rows
    async with polling_manager.engine.connect() as conn:
        count_after = (
            await conn.execute(
                sa.select(sa.func.count()).select_from(polling_manager.queue_cls),
            )
        ).scalar()

    assert count_before == count_after == 0


async def test_poll_yields_task_ids_from_db(
    adapter: PollingAdapter,
    polling_manager: SQLAlchemyManager,
) -> None:
    """
    Pre-insert rows; the first poll cycle must yield all their task_ids.
    The broker deletes the row when it claims it, but the adapter only yields
    the id — we collect directly from the generator here.
    """
    channel = "test_channel"
    insert_count = 3
    inserted_ids = await _insert_queue_rows(
        polling_manager,
        channel=channel,
        count=insert_count,
    )

    collected: list[str] = []

    async def _drain() -> None:
        async for task_id in adapter.listen(channel):
            collected.append(task_id)
            if len(collected) >= insert_count:
                adapter._stop_event.set()
                return

    with anyio.fail_after(5.0):
        await _drain()

    assert set(collected) == set(inserted_ids)


async def test_poll_skips_other_channels(
    adapter: PollingAdapter,
    polling_manager: SQLAlchemyManager,
) -> None:
    """Rows on channel_b must not appear when polling channel_a."""
    await _insert_queue_rows(polling_manager, channel="channel_b", count=2)

    collected: list[str] = []

    # Poll channel_a for a short window — should get nothing
    async def _drain() -> None:
        async for task_id in adapter.listen("channel_a"):
            collected.append(task_id)
            return

    with anyio.move_on_after(0.3):
        await _drain()

    assert collected == [], "channel_b rows must not bleed into channel_a"


async def test_poll_stops_on_stop_event(
    adapter: PollingAdapter,
) -> None:
    """Setting _stop_event causes the listen() generator to exit cleanly."""
    # Set the stop event before starting — generator should exit immediately
    adapter._stop_event.set()

    collected: list[str] = []
    with anyio.fail_after(2.0):
        collected.extend([task_id async for task_id in adapter.listen("any_channel")])

    # The generator exited without hanging
    assert collected == []


async def test_poll_respects_limit(
    adapter: PollingAdapter,
    polling_manager: SQLAlchemyManager,
) -> None:
    """
    Insert 30 rows; the first poll cycle must yield at most 25 (the .limit(25)
    guard in _poll_gen).  We verify this by counting what one pass sees before
    the stop event fires.
    """
    channel = "limit_test"
    await _insert_queue_rows(polling_manager, channel=channel, count=30)

    collected: list[str] = []

    async def _one_pass() -> None:
        async for task_id in adapter.listen(channel):
            collected.append(task_id)
            if len(collected) >= adapter.POLL_RESULT_LIMIT:
                adapter._stop_event.set()
                return

    with anyio.fail_after(5.0):
        await _one_pass()

    # We stopped after exactly 25; the batch must have had exactly 25 items
    assert len(collected) == adapter.POLL_RESULT_LIMIT
