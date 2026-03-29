import typing as t

from sqlalchemy.ext.asyncio import AsyncEngine

from taskiq_sqlalchemy.models import (
    TaskiqQueueMixin,
    TaskiqResultMixin,
    TaskiqScheduleMixin,
)


class SQLAlchemyManager:
    engine: t.Optional[AsyncEngine]

    queue_cls: t.Optional[type[TaskiqQueueMixin]]
    result_cls: t.Optional[type[TaskiqResultMixin]]
    schedule_cls: t.Optional[type[TaskiqScheduleMixin]]

    def __init__(
        self,
        queue_cls: t.Optional[type[TaskiqQueueMixin]] = None,
        result_cls: t.Optional[type[TaskiqResultMixin]] = None,
        schedule_cls: t.Optional[type[TaskiqScheduleMixin]] = None,
    ) -> None:
        self.engine = None

        self.queue_cls = queue_cls
        self.result_cls = result_cls
        self.schedule_cls = schedule_cls

    def register_tables(self, base_classes: t.Sequence[t.Any]) -> None:

        if self.queue_cls is None:

            class TaskiqQueue(*base_classes, TaskiqQueueMixin):
                pass

            self.queue_cls = TaskiqQueue

        if self.result_cls is None:

            class TaskiqResult(*base_classes, TaskiqResultMixin):
                pass

            self.result_cls = TaskiqResult

        if self.schedule_cls is None:

            class TaskiqSchedule(*base_classes, TaskiqScheduleMixin):
                pass

            self.schedule_cls = TaskiqSchedule

    def configure(self, engine: AsyncEngine) -> None:
        self.engine = engine
