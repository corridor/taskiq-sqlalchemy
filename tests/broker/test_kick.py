"""tests/broker/test_kick.py

Tests for ``SQLAlchemyBroker.kick()``.
"""

import uuid

import pytest
import sqlalchemy as sa
from taskiq import BrokerMessage

from taskiq_sqlalchemy.broker import SQLAlchemyBroker
from taskiq_sqlalchemy.manager import SQLAlchemyManager

from .conftest import FakeAdapter


pytestmark = pytest.mark.anyio


async def test_kick_inserts_row(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    broker_message: BrokerMessage,
) -> None:
    """kick() persists a row in taskiq_queue with the expected field values."""
    await broker.kick(broker_message)

    async with manager_with_schema.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(manager_with_schema.queue_cls).where(
                    manager_with_schema.queue_cls.task_id == broker_message.task_id,
                ),
            )
        ).fetchone()

    assert row is not None, "Expected a row in taskiq_queue after kick()"
    assert row.task_id == broker_message.task_id
    assert row.task_name == broker_message.task_name
    assert row.channel == broker.channel_name
    assert row.message is not None


async def test_kick_serializes_broker_message(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    broker_message: BrokerMessage,
) -> None:
    """The bytes stored in message column round-trip to the original BrokerMessage."""
    await broker.kick(broker_message)

    async with manager_with_schema.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(manager_with_schema.queue_cls).where(
                    manager_with_schema.queue_cls.task_id == broker_message.task_id,
                ),
            )
        ).fetchone()

    assert row is not None
    recovered = BrokerMessage.model_validate_json(row.message)
    assert recovered.task_id == broker_message.task_id
    assert recovered.task_name == broker_message.task_name
    assert recovered.message == broker_message.message


async def test_kick_calls_adapter_notify(
    broker: SQLAlchemyBroker,
    fake_adapter: FakeAdapter,
    broker_message: BrokerMessage,
) -> None:
    """notify() is called exactly once with the correct channel and task_id."""
    assert fake_adapter.notify_calls == []

    await broker.kick(broker_message)

    assert len(fake_adapter.notify_calls) == 1
    channel, payload = fake_adapter.notify_calls[0]
    assert channel == broker.channel_name
    assert payload == broker_message.task_id


async def test_kick_notify_outside_transaction(
    manager_with_schema: SQLAlchemyManager,
    fake_adapter: FakeAdapter,
    broker_message: BrokerMessage,
) -> None:
    """
    The row must be visible in the DB at the moment notify() fires.

    We verify this by subclassing FakeAdapter to run a DB read *inside*
    notify() and asserting the row already exists.
    """
    row_visible_during_notify = False

    class _SpyAdapter(FakeAdapter):
        async def notify(self, channel: str, payload: str) -> None:
            nonlocal row_visible_during_notify
            async with self.engine.connect() as conn:
                row = (
                    await conn.execute(
                        sa.select(manager_with_schema.queue_cls).where(
                            manager_with_schema.queue_cls.task_id == payload,
                        ),
                    )
                ).fetchone()
                row_visible_during_notify = row is not None
            await super().notify(channel, payload)

    spy = _SpyAdapter(manager_with_schema.engine)
    b = SQLAlchemyBroker(
        manager_with_schema,
        channel_name="test_channel",
        adapter=spy,
    )
    await b.startup()
    try:
        await b.kick(broker_message)
    finally:
        await b.shutdown()

    assert row_visible_during_notify, "Row must be committed to DB before notify() is called"


async def test_kick_multiple_messages(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    fake_adapter: FakeAdapter,
) -> None:
    """Kicking N messages creates exactly N rows with distinct task_ids."""

    message_count = 5
    messages = [
        BrokerMessage(
            task_id=str(uuid.uuid4()),
            task_name="tests.fake_task",
            message=b'{"args": [], "kwargs": {}}',
            labels={},
        )
        for _ in range(message_count)
    ]

    for msg in messages:
        await broker.kick(msg)

    async with manager_with_schema.engine.connect() as conn:
        rows = (
            await conn.execute(
                sa.select(manager_with_schema.queue_cls).where(
                    manager_with_schema.queue_cls.channel == broker.channel_name,
                ),
            )
        ).fetchall()

    assert len(rows) == message_count
    stored_ids = {row.task_id for row in rows}
    expected_ids = {msg.task_id for msg in messages}
    assert stored_ids == expected_ids
    # notify() called once per message
    assert len(fake_adapter.notify_calls) == message_count
