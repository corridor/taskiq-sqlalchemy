"""tests/broker/test_fetch_message.py

Tests for ``SQLAlchemyBroker._fetch_message()``.
"""

import asyncio
import typing as t

import pytest
import sqlalchemy as sa
from taskiq import BrokerMessage

from taskiq_sqlalchemy.broker import SQLAlchemyBroker
from taskiq_sqlalchemy.manager import SQLAlchemyManager

pytestmark = pytest.mark.anyio


async def _insert_queue_row(
    manager: SQLAlchemyManager,
    *,
    task_id: str,
    channel: str,
    message_bytes: bytes,
    task_name: str = "tests.fake_task",
) -> None:
    async with manager.engine.begin() as conn:
        await conn.execute(
            sa.insert(manager.queue_cls).values(
                task_id=task_id,
                channel=channel,
                task_name=task_name,
                message=message_bytes,
            )
        )


def _valid_message_bytes(broker_message: BrokerMessage) -> bytes:
    return broker_message.model_dump_json().encode()


async def test_fetch_message_returns_ackable(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    broker_message: BrokerMessage,
) -> None:
    """_fetch_message returns AckableMessage with correct payload; row is gone."""
    raw = _valid_message_bytes(broker_message)
    await _insert_queue_row(
        manager_with_schema,
        task_id=broker_message.task_id,
        channel=broker.channel_name,
        message_bytes=raw,
    )

    result = await broker._fetch_message(broker_message.task_id)

    assert result is not None, "Expected an AckableMessage"
    assert result.data == broker_message.message

    # Row must have been deleted
    async with manager_with_schema.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(manager_with_schema.queue_cls).where(
                    manager_with_schema.queue_cls.task_id == broker_message.task_id
                )
            )
        ).fetchone()
    assert row is None, "Row should be deleted after _fetch_message"


async def test_fetch_message_missing_returns_none(
    broker: SQLAlchemyBroker,
    task_id: str,
) -> None:
    """Returns None for a task_id that is not in the queue."""
    result = await broker._fetch_message(task_id)
    assert result is None


async def test_fetch_message_wrong_channel_returns_none(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    broker_message: BrokerMessage,
) -> None:
    """A row on a different channel is invisible to this broker."""
    raw = _valid_message_bytes(broker_message)
    await _insert_queue_row(
        manager_with_schema,
        task_id=broker_message.task_id,
        channel="other_channel",  # different from broker.channel_name
        message_bytes=raw,
    )

    result = await broker._fetch_message(broker_message.task_id)
    assert result is None, "Should not claim a row on a different channel"


async def test_fetch_message_ack_is_noop(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    broker_message: BrokerMessage,
) -> None:
    """Calling ack() after _fetch_message completes without error."""
    raw = _valid_message_bytes(broker_message)
    await _insert_queue_row(
        manager_with_schema,
        task_id=broker_message.task_id,
        channel=broker.channel_name,
        message_bytes=raw,
    )

    result = await broker._fetch_message(broker_message.task_id)
    assert result is not None
    # Must not raise
    await result.ack()


async def test_fetch_message_bad_json_returns_none(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    task_id: str,
) -> None:
    """A row with invalid JSON in message returns None instead of raising."""
    await _insert_queue_row(
        manager_with_schema,
        task_id=task_id,
        channel=broker.channel_name,
        message_bytes=b"this is not json {{{",
    )

    result = await broker._fetch_message(task_id)
    assert result is None, "Corrupt message should be swallowed and return None"


async def test_fetch_message_atomic_double_claim(
    broker: SQLAlchemyBroker,
    manager_with_schema: SQLAlchemyManager,
    broker_message: BrokerMessage,
) -> None:
    """
    Two concurrent _fetch_message() calls for the same task_id:
    exactly one must succeed and the other must return None.

    This tests the DELETE … RETURNING atomicity guarantee.
    """
    raw = _valid_message_bytes(broker_message)
    await _insert_queue_row(
        manager_with_schema,
        task_id=broker_message.task_id,
        channel=broker.channel_name,
        message_bytes=raw,
    )

    results: t.Sequence[t.Any] = await asyncio.gather(
        broker._fetch_message(broker_message.task_id),
        broker._fetch_message(broker_message.task_id),
    )

    non_none = [r for r in results if r is not None]
    assert len(non_none) == 1, (
        f"Exactly one claim should succeed; got {len(non_none)} non-None results"
    )
