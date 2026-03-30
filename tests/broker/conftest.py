"""tests/broker/conftest.py

Fixtures for broker tests.
"""

import asyncio
from typing import AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from taskiq import BrokerMessage

from taskiq_sqlalchemy.adapters.abc import DialectAdapter
from taskiq_sqlalchemy.broker import SQLAlchemyBroker
from taskiq_sqlalchemy.manager import SQLAlchemyManager


class FakeAdapter(DialectAdapter):
    """
    Test double for DialectAdapter.

    ``notify_calls`` captures every (channel, payload) pair sent via notify().
    ``_inbound`` is an asyncio.Queue; tests push strings into it to simulate
    incoming messages that listen() will yield.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine)
        self.notify_calls: list[tuple[str, str]] = []
        self._inbound: asyncio.Queue[str] = asyncio.Queue()

    async def notify(self, channel: str, payload: str) -> None:
        self.notify_calls.append((channel, payload))

    def listen(self, channel: str) -> AsyncGenerator[str, None]:
        return self._listen_gen(channel)

    async def _listen_gen(self, channel: str) -> AsyncGenerator[str, None]:
        while True:
            yield await self._inbound.get()

    def feed(self, task_id: str) -> None:
        """Push a task_id into the inbound queue (simulates a notify)."""
        self._inbound.put_nowait(task_id)


# Map of (pytest param id) → (SQLAlchemy async URL)
_ENGINE_PARAMS: list = [
    pytest.param(
        "sqlite+aiosqlite:///:memory:",
        id="sqlite+aiosqlite",
    ),
    pytest.param(
        "postgresql+asyncpg://taskiq_user:taskiq_pwd@localhost:5432/taskiq",
        id="postgresql+asyncpg",
        marks=pytest.mark.postgresql,
    ),
    pytest.param(
        "postgresql+psycopg://taskiq_user:taskiq_pwd@localhost:5432/taskiq",
        id="postgresql+psycopg",
        marks=pytest.mark.postgresql,
    ),
    pytest.param(
        "oracle+oracledb://taskiq_user:taskiq_pwd@localhost:1521/?service_name=taskiq",
        id="oracle+oracledb",
        marks=pytest.mark.oracle,
    ),
]


@pytest.fixture(params=_ENGINE_PARAMS)
async def async_engine(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[AsyncEngine, None]:
    """One AsyncEngine per dialect/driver; skipped if DB unreachable."""
    url: str = request.param
    engine = create_async_engine(url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def manager_with_schema(
    async_engine: AsyncEngine,
) -> AsyncGenerator[SQLAlchemyManager, None]:
    """
    Creates all taskiq tables (queue + result + schedule), yields a configured
    manager, then drops all tables — giving every test a clean slate.
    """

    class _Base(DeclarativeBase):
        pass

    manager = SQLAlchemyManager(base_classes=(_Base,))
    manager.configure(engine=async_engine)

    async with async_engine.begin() as conn:
        await conn.run_sync(_Base.metadata.create_all)

    try:
        yield manager
    finally:
        async with async_engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)


@pytest.fixture
def fake_adapter(async_engine: AsyncEngine) -> FakeAdapter:
    """Fresh FakeAdapter bound to the same engine as the current test."""
    return FakeAdapter(async_engine)


@pytest.fixture
async def broker(
    manager_with_schema: SQLAlchemyManager,
    fake_adapter: FakeAdapter,
) -> AsyncGenerator[SQLAlchemyBroker, None]:
    """
    Broker wired with the FakeAdapter — suitable for kick/fetch tests that
    don't need real pub/sub delivery.
    """
    b = SQLAlchemyBroker(
        manager_with_schema,
        channel_name="test_channel",
        adapter=fake_adapter,
    )
    await b.startup()
    try:
        yield b
    finally:
        await b.shutdown()


@pytest.fixture
async def polling_broker(
    manager_with_schema: SQLAlchemyManager,
) -> AsyncGenerator[SQLAlchemyBroker, None]:
    """
    Broker wired with the real PollingAdapter — for end-to-end listen() tests.
    Works with any engine (SQLite always, Postgres when available).
    """
    from taskiq_sqlalchemy.adapters.polling import PollingAdapter

    adapter = PollingAdapter(
        manager_with_schema.engine,
        queue_cls=manager_with_schema.queue_cls,
    )
    # Speed up polling for tests
    adapter.poll_interval = 0.05

    b = SQLAlchemyBroker(
        manager_with_schema,
        channel_name="test_channel",
        adapter=adapter,
    )
    await b.startup()
    try:
        yield b
    finally:
        await b.shutdown()


@pytest.fixture
def broker_message(task_id: str) -> BrokerMessage:
    """A minimal valid BrokerMessage."""
    return BrokerMessage(
        task_id=task_id,
        task_name="tests.fake_task",
        message=b'{"args": [], "kwargs": {}}',
        labels={},
    )
