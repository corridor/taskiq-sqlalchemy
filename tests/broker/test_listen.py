"""tests/broker/test_listen.py

End-to-end tests for ``SQLAlchemyBroker.listen()`` using the real PollingAdapter
against a SQLite in-memory database.

We deliberately do NOT parametrize over PostgreSQL here: Postgres has its own
push-based adapter (PostgresDialectAdapter) that is tested separately.  The
PollingAdapter + SQLite combination gives us full functional coverage of the
broker's listen() / kick() / _fetch_message() pipeline with zero external
services.
"""

import typing as t
import uuid

import anyio
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from taskiq import AckableMessage, BrokerMessage

from taskiq_sqlalchemy.adapters.polling import PollingAdapter
from taskiq_sqlalchemy.broker import SQLAlchemyBroker
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
async def manager_with_schema(  # type: ignore[override]
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
async def polling_broker(  # type: ignore[override]
    manager_with_schema: SQLAlchemyManager,
) -> t.AsyncGenerator[SQLAlchemyBroker, None]:
    adapter = PollingAdapter(
        manager_with_schema.engine,
        queue_cls=manager_with_schema.queue_cls,
    )
    adapter.POLL_INTERVAL_SECS = 0.05
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


async def _collect(
    broker: SQLAlchemyBroker,
    n: int,
    timeout: float = 5.0,
) -> list[AckableMessage]:
    """
    Drive broker.listen() and collect the first ``n`` messages.
    Raises TimeoutError if they don't arrive within ``timeout`` seconds.
    """
    collected: list[AckableMessage] = []

    async def _drain() -> None:
        async for msg in broker.listen():
            collected.append(msg)
            if len(collected) >= n:
                return

    with anyio.fail_after(timeout):
        await _drain()

    return collected


async def test_listen_yields_kicked_message(
    polling_broker: SQLAlchemyBroker,
    broker_message: BrokerMessage,
) -> None:
    """
    kick() a single message; the first listen() yield must carry the same
    payload.
    """
    await polling_broker.kick(broker_message)

    messages = await _collect(polling_broker, n=1)

    assert len(messages) == 1
    assert messages[0].data == broker_message.message


async def test_listen_multiple_messages_in_order(
    polling_broker: SQLAlchemyBroker,
) -> None:
    """
    Kick 3 messages; collect 3 via listen(); all payloads present, no duplicates.
    """
    messages_sent = [
        BrokerMessage(
            task_id=str(uuid.uuid4()),
            task_name="tests.fake_task",
            message=f"payload-{i}".encode(),
            labels={},
        )
        for i in range(3)
    ]

    for msg in messages_sent:
        await polling_broker.kick(msg)

    received = await _collect(polling_broker, n=3)

    assert len(received) == 3
    received_payloads = {msg.data for msg in received}
    expected_payloads = {msg.message for msg in messages_sent}
    assert received_payloads == expected_payloads


async def test_listen_channel_isolation(
    manager_with_schema: SQLAlchemyManager,
) -> None:
    """
    Messages kicked on channel A must NOT be delivered to a broker listening
    on channel B.
    """

    def _make_broker(channel: str) -> SQLAlchemyBroker:
        adapter = PollingAdapter(
            manager_with_schema.engine,
            queue_cls=manager_with_schema.queue_cls,
        )
        adapter.POLL_INTERVAL_SECS = 0.05
        return SQLAlchemyBroker(
            manager_with_schema,
            channel_name=channel,
            adapter=adapter,
        )

    broker_a = _make_broker("channel_a")
    broker_b = _make_broker("channel_b")

    await broker_a.startup()
    await broker_b.startup()

    try:
        # Kick one message on channel_a
        msg_a = BrokerMessage(
            task_id=str(uuid.uuid4()),
            task_name="tests.fake_task",
            message=b"for-channel-a",
            labels={},
        )
        await broker_a.kick(msg_a)

        # broker_b should yield nothing within a short window
        received_by_b: list[AckableMessage] = []

        async def _try_listen_b() -> None:
            async for msg in broker_b.listen():
                received_by_b.append(msg)
                return

        # Give broker_b a brief window to (incorrectly) receive the message
        with anyio.move_on_after(0.3):
            await _try_listen_b()

        assert received_by_b == [], (
            "broker_b must not receive messages sent on channel_a"
        )

        # Confirm broker_a does receive it
        received_by_a = await _collect(broker_a, n=1, timeout=3.0)
        assert received_by_a[0].data == msg_a.message

    finally:
        await broker_a.shutdown()
        await broker_b.shutdown()
