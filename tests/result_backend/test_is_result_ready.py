import typing as t

import pytest
from taskiq import TaskiqResult

from taskiq_sqlalchemy.result_backend import SQLAlchemyResultBackend

pytestmark = pytest.mark.anyio


async def test_is_result_ready_false_before_set(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
) -> None:
    """is_result_ready returns False when no result has been stored yet."""
    ready = await result_backend.is_result_ready(task_id=task_id)
    assert ready is False


async def test_is_result_ready_true_after_set(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    simple_result: TaskiqResult[str],
) -> None:
    """is_result_ready returns True immediately after set_result."""
    assert (await result_backend.is_result_ready(task_id=task_id)) is False

    await result_backend.set_result(task_id=task_id, result=simple_result)

    assert (await result_backend.is_result_ready(task_id=task_id)) is True


async def test_is_result_ready_false_after_consumed(
    keep_results_false_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    simple_result: TaskiqResult[str],
) -> None:
    """
    After the result is consumed via get_result (keep_results=False),
    is_result_ready returns False because the row has been deleted.
    """
    backend = keep_results_false_backend

    await backend.set_result(task_id=task_id, result=simple_result)
    assert await backend.is_result_ready(task_id=task_id)

    # Consume — deletes the row
    await backend.get_result(task_id=task_id)

    assert not await backend.is_result_ready(task_id=task_id)


async def test_is_result_ready_independent_task_ids(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    another_task_id: str,
    simple_result: TaskiqResult[str],
    error_result: TaskiqResult[str],
) -> None:
    """
    Setting a result for task_id_a must not affect the readiness state of
    task_id_b, and vice-versa.
    """
    # Neither is ready yet
    assert not await result_backend.is_result_ready(task_id=task_id)
    assert not await result_backend.is_result_ready(task_id=another_task_id)

    # Store only the first
    await result_backend.set_result(task_id=task_id, result=simple_result)

    assert await result_backend.is_result_ready(task_id=task_id)
    assert not await result_backend.is_result_ready(task_id=another_task_id)

    # Store the second
    await result_backend.set_result(task_id=another_task_id, result=error_result)

    assert await result_backend.is_result_ready(task_id=task_id)
    assert await result_backend.is_result_ready(task_id=another_task_id)
