from taskiq_sqlalchemy.manager import SQLAlchemyManager


def resolve_adapter(manager: SQLAlchemyManager):
    if manager.engine.dialect.name == "postgresql":
        from taskiq_sqlalchemy.adapters.postgresql import PostgresDialectAdapter

        return PostgresDialectAdapter(manager.engine)
    if manager.engine.dialect.name == "oracle":
        from taskiq_sqlalchemy.adapters.oracle import OracleDialectAdapter

        return OracleDialectAdapter(manager.engine)

    from taskiq_sqlalchemy.adapters.polling import PollingAdapter

    return PollingAdapter(manager.engine, queue_cls=manager.queue_cls)
