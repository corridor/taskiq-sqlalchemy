import typing as t
import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from taskiq import TaskiqResult

from taskiq_sqlalchemy.manager import SQLAlchemyManager
from taskiq_sqlalchemy.result_backend import SQLAlchemyResultBackend


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
) -> t.AsyncGenerator[AsyncEngine, None]:
    """
    Yields one ``AsyncEngine`` per dialect/driver combination.

    Tests are **skipped** (not failed) when the backing service is unreachable
    or the required driver package is not installed.  This keeps the local dev
    loop fast (SQLite always works) while giving full coverage in CI.
    """
    url: str = request.param
    engine = create_async_engine(url)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def manager_and_backend(
    async_engine: AsyncEngine,
) -> t.AsyncGenerator[tuple[SQLAlchemyManager, SQLAlchemyResultBackend[t.Any]], None]:
    """
    Creates the taskiq_result table, yields a ready (manager, backend) pair,
    then drops the table — giving every test a clean slate.

    The ``DeclarativeBase`` is created fresh per test so that the ORM metadata
    is not shared between parametrized runs (avoids SQLAlchemy mapper conflicts
    when the same engine is reused across tests with different table names).
    """

    class _Base(DeclarativeBase):
        pass

    manager = SQLAlchemyManager(base_classes=(_Base,))
    manager.configure(engine=async_engine)

    # Create schema
    async with async_engine.begin() as conn:
        await conn.run_sync(_Base.metadata.create_all)

    backend: SQLAlchemyResultBackend[t.Any] = SQLAlchemyResultBackend(manager)

    try:
        yield manager, backend
    finally:
        # Drop schema — even if the test raised
        async with async_engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)


@pytest.fixture
async def result_backend(
    manager_and_backend: tuple[SQLAlchemyManager, SQLAlchemyResultBackend[t.Any]],
) -> SQLAlchemyResultBackend[t.Any]:
    """Convenience fixture: just the backend (most tests don't need the manager)."""
    _, backend = manager_and_backend
    return backend


@pytest.fixture
async def keep_results_false_backend(
    async_engine: AsyncEngine,
) -> t.AsyncGenerator[SQLAlchemyResultBackend[t.Any], None]:
    """
    A backend with ``keep_results=False``, created against the same engine
    as the parametrized ``async_engine`` fixture.
    """

    class _Base(DeclarativeBase):
        pass

    manager = SQLAlchemyManager(base_classes=(_Base,))
    manager.configure(engine=async_engine)

    async with async_engine.begin() as conn:
        await conn.run_sync(_Base.metadata.create_all)

    backend: SQLAlchemyResultBackend[t.Any] = SQLAlchemyResultBackend(
        manager,
        keep_results=False,
    )

    try:
        yield backend
    finally:
        async with async_engine.begin() as conn:
            await conn.run_sync(_Base.metadata.drop_all)


@pytest.fixture
def another_task_id() -> str:
    """A second, distinct UUID string."""
    return str(uuid.uuid4())


@pytest.fixture
def simple_result() -> TaskiqResult[str]:
    """Successful result with a plain string return value."""
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value="hello world",
        execution_time=0.42,
    )


@pytest.fixture
def result_with_logs() -> TaskiqResult[str]:
    """Successful result that carries log output."""
    return TaskiqResult(
        is_err=False,
        log="some log line",
        return_value="with logs",
        execution_time=0.1,
    )


@pytest.fixture
def error_result() -> TaskiqResult[str]:
    """A result representing a failed task (``is_err=True``)."""
    return TaskiqResult(
        is_err=True,
        log="Traceback (most recent call last): ...",
        return_value="",
        execution_time=0.05,
    )
