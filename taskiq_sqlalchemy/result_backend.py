"""taskiq_sqlalchemy.result_backend

Pure SQLAlchemy result backend
Works on any SQLAlchemy async engine (Postgres, Oracle, MSSQL, SQLite, ...).
"""

import logging
import typing as t

import sqlalchemy as sa
from sqlalchemy.sql.dml import Insert
from taskiq import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.result import TaskiqResult
from taskiq.serializers.pickle import PickleSerializer

from taskiq_sqlalchemy.manager import SQLAlchemyManager
from taskiq_sqlalchemy.models import TaskiqResultMixin


_ReturnType = t.TypeVar("_ReturnType")
logger = logging.getLogger(__name__)


class SQLAlchemyResultBackend(AsyncResultBackend[_ReturnType]):
    """
    Stores and retrieves task results in a SQL table.
    """

    def __init__(
        self,
        manager: SQLAlchemyManager,
        *,
        keep_results: bool = True,
        serializer: t.Optional[TaskiqSerializer] = None,
    ) -> None:
        self.manager = manager
        self.keep_results = keep_results
        self.serializer = serializer or PickleSerializer()

    @classmethod
    async def build_upsert_statement(
        cls,
        dialect: str,
        result_cls: type[TaskiqResultMixin],
        values: dict,
        update: dict,
    ) -> Insert:
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert  # noqa: PLC0415

            return (
                insert(result_cls)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=[result_cls.task_id],
                    set_=update,
                )
            )

        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert  # noqa: PLC0415

            return (
                insert(result_cls)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=[result_cls.task_id],
                    set_=update,
                )
            )

        if dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert  # noqa: PLC0415

            return insert(result_cls).values(**values).on_duplicate_key_update(**update)

        raise NotImplementedError(f"Upsert not supported for {dialect}")

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        async_engine = self.manager.engine
        serialised = self.serializer.dumpb(result)
        async with async_engine.begin() as conn:
            # Upsert pattern: try insert, update on conflict.
            # SQLAlchemy Core provides dialect-agnostic on_conflict helpers
            # only for Postgres. For other dialects we fall back
            # to a delete-then-insert, which is safe because set_result is
            # called at most once per task_id.
            dialect = async_engine.dialect.name

            result_cls = self.manager.result_cls
            if dialect in ("postgresql", "sqlite", "myssql"):
                stmt = await self.build_upsert_statement(
                    dialect,
                    result_cls,
                    values={"task_id": task_id, "result": serialised, "is_err": result.is_err},
                    update={
                        "result": serialised,
                        "is_err": result.is_err,
                    },
                )
            else:
                # Generic fallback: delete then insert (safe, task results are
                # written exactly once per task_id in normal operation)
                await conn.execute(
                    sa.delete(self.manager.result_cls).where(
                        self.manager.result_cls.task_id == task_id,
                    ),
                )
                stmt = sa.insert(self.manager.result_cls).values(
                    task_id=task_id,
                    result=serialised,
                    is_err=result.is_err,
                )
            await conn.execute(stmt)

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,  # noqa: FBT001, FBT002 -- Keep same signature as parent
    ) -> TaskiqResult[_ReturnType]:
        async with self.manager.engine.begin() as conn:
            row = (
                await conn.execute(
                    sa.select(self.manager.result_cls).where(
                        self.manager.result_cls.task_id == task_id,
                    ),
                )
            ).fetchone()

            if row is None:
                raise ValueError(f"No result found for task_id={task_id!r}")

            if not self.keep_results:
                await conn.execute(
                    sa.delete(self.manager.result_cls).where(
                        self.manager.result_cls.task_id == task_id,
                    ),
                )

        result: TaskiqResult[_ReturnType] = self.serializer.loadb(row.result)
        if not with_logs:
            result.log = None
        return result

    async def is_result_ready(self, task_id: str) -> bool:
        async with self.manager.engine.connect() as conn:
            stmt = sa.select(sa.literal(value=True)).where(
                sa.exists().where(self.manager.result_cls.task_id == task_id),
            )
            return bool(await conn.scalar(stmt))
