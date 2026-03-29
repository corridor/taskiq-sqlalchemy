import datetime
import typing as t

import sqlalchemy as sa
from sqlalchemy.dialects import oracle
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import expression


class BaseMixin:
    id: Mapped[int] = mapped_column(
        (
            sa.BigInteger()
            # Sqlite doesn't allow BIGINT to be used as a primary key with autoincrement.
            # See: https://stackoverflow.com/questions/18835740
            .with_variant(sa.Integer, "sqlite")
            .with_variant(oracle.NUMBER(38), "oracle")
        ),
        sa.Identity(),
        primary_key=True,
    )

    created_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime, server_default=sa.func.now()
    )


class TaskiqQueueMixin(BaseMixin):
    __tablename__ = "taskiq_queue"

    task_id: Mapped[str] = mapped_column(sa.String(255), index=True)
    task_name: Mapped[str] = mapped_column(sa.String(255))

    channel: Mapped[str] = mapped_column(sa.String(255), index=True)
    message: Mapped[bytes] = mapped_column(sa.LargeBinary)


class TaskiqResultMixin(BaseMixin):
    __tablename__ = "taskiq_result"

    task_id: Mapped[str] = mapped_column(sa.String(255), unique=True)

    result: Mapped[t.Optional[bytes]] = mapped_column(sa.LargeBinary)
    is_err: Mapped[bool] = mapped_column(
        sa.Boolean(name="bool_is_err"), server_default=expression.false()
    )


class TaskiqScheduleMixin(BaseMixin):
    __tablename__ = "taskiq_schedule"

    task_name: Mapped[str] = mapped_column(sa.String(255))

    schedule: Mapped[t.Any] = mapped_column(sa.JSON().with_variant(sa.CLOB(), "oracle"))

    updated_at: Mapped[datetime.datetime] = mapped_column(
        sa.DateTime,
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
    )
