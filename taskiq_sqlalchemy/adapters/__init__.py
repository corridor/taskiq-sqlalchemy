import typing as t

from taskiq_sqlalchemy.adapters.oracle import OracleDialectAdapter
from taskiq_sqlalchemy.adapters.polling import PollingAdapter
from taskiq_sqlalchemy.adapters.postgresql import PostgresDialectAdapter
from taskiq_sqlalchemy.manager import SQLAlchemyManager


def resolve_adapter(
    manager: SQLAlchemyManager,
) -> t.Union[PostgresDialectAdapter, OracleDialectAdapter, PollingAdapter]:
    if manager.engine.dialect.name == "postgresql":
        from taskiq_sqlalchemy.adapters.postgresql import PostgresDialectAdapter  # noqa: PLC0415

        return PostgresDialectAdapter(manager.engine)
    if manager.engine.dialect.name == "oracle":
        from taskiq_sqlalchemy.adapters.oracle import OracleDialectAdapter  # noqa: PLC0415

        return OracleDialectAdapter(manager.engine)

    from taskiq_sqlalchemy.adapters.polling import PollingAdapter  # noqa: PLC0415

    return PollingAdapter(manager.engine, queue_cls=manager.queue_cls)
