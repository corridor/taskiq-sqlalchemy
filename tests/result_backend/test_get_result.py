import types
import typing as t
import uuid

import pytest
from taskiq import TaskiqResult

from taskiq_sqlalchemy.result_backend import SQLAlchemyResultBackend


pytestmark = pytest.mark.anyio


async def test_get_result_returns_stored_value(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    simple_result: TaskiqResult[str],
) -> None:
    """get_result returns the same TaskiqResult that was stored."""
    await result_backend.set_result(task_id=task_id, result=simple_result)

    recovered = await result_backend.get_result(task_id=task_id)

    assert recovered.return_value == simple_result.return_value
    assert recovered.is_err == simple_result.is_err
    assert recovered.execution_time == simple_result.execution_time


async def test_get_result_missing_raises(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
) -> None:
    """ValueError is raised when no result exists for the given task_id."""
    with pytest.raises(ValueError, match="No result found"):
        await result_backend.get_result(task_id=task_id)


async def test_get_result_logs_stripped_by_default(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    result_with_logs: TaskiqResult[str],
) -> None:
    """``result.log`` is None when with_logs=False (the default)."""
    await result_backend.set_result(task_id=task_id, result=result_with_logs)

    recovered = await result_backend.get_result(task_id=task_id, with_logs=False)

    assert recovered.log is None, "Log should be stripped when with_logs=False"
    assert recovered.return_value == result_with_logs.return_value


async def test_get_result_logs_preserved_when_requested(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    result_with_logs: TaskiqResult[str],
) -> None:
    """``result.log`` is preserved when with_logs=True."""
    await result_backend.set_result(task_id=task_id, result=result_with_logs)

    recovered = await result_backend.get_result(task_id=task_id, with_logs=True)

    assert recovered.log == result_with_logs.log, "Log should be present when with_logs=True"


async def test_get_result_keep_results_false_deletes_row(
    keep_results_false_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    simple_result: TaskiqResult[str],
) -> None:
    """
    With keep_results=False the row is deleted after the first successful
    get_result call; a subsequent call raises ValueError.
    """
    backend = keep_results_false_backend

    await backend.set_result(task_id=task_id, result=simple_result)

    # First read — should succeed
    recovered = await backend.get_result(task_id=task_id)
    assert recovered.return_value == simple_result.return_value

    # Second read — row must have been deleted
    with pytest.raises(ValueError, match="No result found"):
        await backend.get_result(task_id=task_id)


async def test_get_result_keep_results_true_keeps_row(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
    simple_result: TaskiqResult[str],
) -> None:
    """
    With keep_results=True (default) repeated get_result calls always succeed.
    """
    await result_backend.set_result(task_id=task_id, result=simple_result)

    first = await result_backend.get_result(task_id=task_id)
    second = await result_backend.get_result(task_id=task_id)

    assert first.return_value == second.return_value == simple_result.return_value


async def test_get_result_complex_object_roundtrip(
    result_backend: SQLAlchemyResultBackend[t.Any],
    task_id: str,
) -> None:
    """
    A complex Python object (SimpleNamespace with a UUID field) survives
    the pickle serialization round-trip through the database.
    """
    original_value = types.SimpleNamespace(job_id=uuid.uuid4(), score=3.14)
    complex_result = TaskiqResult(
        is_err=False,
        log=None,
        return_value=original_value,
        execution_time=0.2,
    )

    await result_backend.set_result(task_id=task_id, result=complex_result)
    recovered = await result_backend.get_result(task_id=task_id, with_logs=False)

    assert recovered.return_value.job_id == original_value.job_id
    assert recovered.return_value.score == original_value.score
    assert recovered.is_err is False
