import typing as t

from taskiq_sqlalchemy.adapters.mssql import MSSQLDialectAdapter
from taskiq_sqlalchemy.adapters.oracle import OracleDialectAdapter
from taskiq_sqlalchemy.adapters.polling import PollingAdapter
from taskiq_sqlalchemy.adapters.postgresql import PostgresDialectAdapter
from taskiq_sqlalchemy.manager import SQLAlchemyManager


def resolve_adapter(
    manager: SQLAlchemyManager,
) -> t.Union[PostgresDialectAdapter, OracleDialectAdapter, MSSQLDialectAdapter, PollingAdapter]:
    dialect = manager.engine.dialect.name

    if dialect == "postgresql":
        from taskiq_sqlalchemy.adapters.postgresql import PostgresDialectAdapter  # noqa: PLC0415

        return PostgresDialectAdapter(manager.engine)

    if dialect == "oracle":
        from taskiq_sqlalchemy.adapters.oracle import OracleDialectAdapter  # noqa: PLC0415

        return OracleDialectAdapter(manager.engine)

    if dialect == "mssql":
        from taskiq_sqlalchemy.adapters.mssql import MSSQLDialectAdapter  # noqa: PLC0415

        return MSSQLDialectAdapter(manager.engine)

    from taskiq_sqlalchemy.adapters.polling import PollingAdapter  # noqa: PLC0415

    return PollingAdapter(manager.engine, queue_cls=manager.queue_cls)
