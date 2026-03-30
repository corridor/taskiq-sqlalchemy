import typing as t

from sqlalchemy.ext.asyncio import AsyncEngine

from taskiq_sqlalchemy.models import (
    TaskiqQueueMixin,
    TaskiqResultMixin,
    TaskiqScheduleMixin,
)


class SQLAlchemyManager:
    _engine: t.Optional[AsyncEngine]

    queue_cls: type[TaskiqQueueMixin]
    result_cls: type[TaskiqResultMixin]
    schedule_cls: type[TaskiqScheduleMixin]

    def __init__(
        self,
        base_classes: t.Sequence[t.Any] = (),
        queue_cls: t.Optional[type[TaskiqQueueMixin]] = None,
        result_cls: t.Optional[type[TaskiqResultMixin]] = None,
        schedule_cls: t.Optional[type[TaskiqScheduleMixin]] = None,
    ) -> None:
        self._engine = None

        if queue_cls is None:
            if len(base_classes) == 0:
                raise ValueError(
                    "base_classes and queue_cls cannot be empty at the same time",
                )

            class TaskiqQueue(*base_classes, TaskiqQueueMixin):
                pass

            self.queue_cls = TaskiqQueue
        else:
            self.queue_cls = queue_cls

        if result_cls is None:
            if len(base_classes) == 0:
                raise ValueError(
                    "base_classes and result_cls cannot be empty at the same time",
                )

            class TaskiqResult(*base_classes, TaskiqResultMixin):
                pass

            self.result_cls = TaskiqResult
        else:
            self.result_cls = result_cls

        if schedule_cls is None:
            if len(base_classes) == 0:
                raise ValueError("base_classes and schedule_cls cannot be empty at the same time")

            class TaskiqSchedule(*base_classes, TaskiqScheduleMixin):
                pass

            self.schedule_cls = TaskiqSchedule
        else:
            self.schedule_cls = schedule_cls

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("This manager is not bound to any SQLAlchemy engine")
        return self._engine

    def configure(self, engine: AsyncEngine) -> None:
        self._engine = engine
