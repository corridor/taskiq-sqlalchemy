import typing as t

import pytest
import sqlalchemy as sa
from taskiq import TaskiqResult
from taskiq.serializers.pickle import PickleSerializer

from taskiq_sqlalchemy.manager import SQLAlchemyManager
from taskiq_sqlalchemy.result_backend import SQLAlchemyResultBackend

pytestmark = pytest.mark.anyio


async def test_set_result_stores_row(
    result_backend: SQLAlchemyResultBackend[t.Any],
    manager_and_backend: tuple[SQLAlchemyManager, SQLAlchemyResultBackend[t.Any]],
    task_id: str,
    simple_result: TaskiqResult[str],
) -> None:
    """A row with the correct task_id is written to the database."""
    manager, _ = manager_and_backend

    await result_backend.set_result(task_id=task_id, result=simple_result)

    async with manager.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(manager.result_cls).where(
                    manager.result_cls.task_id == task_id
                )
            )
        ).fetchone()

    assert row is not None, "Expected a row in the DB after set_result"
    assert row.task_id == task_id
    assert row.is_err is False
    assert row.result is not None  # bytes, non-empty


async def test_set_result_error_flag(
    result_backend: SQLAlchemyResultBackend[t.Any],
    manager_and_backend: tuple[SQLAlchemyManager, SQLAlchemyResultBackend[t.Any]],
    task_id: str,
    error_result: TaskiqResult[str],
) -> None:
    """``is_err=True`` is correctly persisted for a failed task."""
    manager, _ = manager_and_backend

    await result_backend.set_result(task_id=task_id, result=error_result)

    async with manager.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(manager.result_cls).where(
                    manager.result_cls.task_id == task_id
                )
            )
        ).fetchone()

    assert row is not None
    assert row.is_err is True


async def test_set_result_idempotent_overwrite(
    result_backend: SQLAlchemyResultBackend[t.Any],
    manager_and_backend: tuple[SQLAlchemyManager, SQLAlchemyResultBackend[t.Any]],
    task_id: str,
    simple_result: TaskiqResult[str],
    error_result: TaskiqResult[str],
) -> None:
    """
    Calling set_result twice for the same task_id must not raise.
    The second write wins (upsert for PostgreSQL; delete+insert for others).
    """
    manager, _ = manager_and_backend

    await result_backend.set_result(task_id=task_id, result=simple_result)
    # Second call — must not raise
    await result_backend.set_result(task_id=task_id, result=error_result)

    # Verify only one row exists and it reflects the second write
    async with manager.engine.connect() as conn:
        rows = (
            await conn.execute(
                sa.select(manager.result_cls).where(
                    manager.result_cls.task_id == task_id
                )
            )
        ).fetchall()

    assert len(rows) == 1, "Exactly one row should exist after two set_result calls"
    assert rows[0].is_err is True, "Latest write (error_result) should have won"


async def test_set_result_serialization_roundtrip(
    result_backend: SQLAlchemyResultBackend[t.Any],
    manager_and_backend: tuple[SQLAlchemyManager, SQLAlchemyResultBackend[t.Any]],
    task_id: str,
    simple_result: TaskiqResult[str],
) -> None:
    """
    Raw bytes stored in the DB can be deserialized back to the original
    TaskiqResult using the default PickleSerializer.
    """
    manager, _ = manager_and_backend
    serializer = PickleSerializer()

    await result_backend.set_result(task_id=task_id, result=simple_result)

    async with manager.engine.connect() as conn:
        row = (
            await conn.execute(
                sa.select(manager.result_cls).where(
                    manager.result_cls.task_id == task_id
                )
            )
        ).fetchone()

    assert row is not None
    recovered: TaskiqResult[str] = serializer.loadb(row.result)
    assert recovered.return_value == simple_result.return_value
    assert recovered.is_err == simple_result.is_err
    assert recovered.execution_time == simple_result.execution_time
