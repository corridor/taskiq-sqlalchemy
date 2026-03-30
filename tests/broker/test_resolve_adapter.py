from unittest.mock import MagicMock

from taskiq_sqlalchemy.adapters import resolve_adapter
from taskiq_sqlalchemy.adapters.oracle import OracleDialectAdapter
from taskiq_sqlalchemy.adapters.polling import PollingAdapter
from taskiq_sqlalchemy.adapters.postgresql import PostgresDialectAdapter
from taskiq_sqlalchemy.manager import SQLAlchemyManager


def _mock_manager(dialect_name: str) -> SQLAlchemyManager:
    """Build a SQLAlchemyManager whose engine reports the given dialect name."""
    engine = MagicMock()
    engine.dialect.name = dialect_name

    queue_cls = MagicMock()

    manager = SQLAlchemyManager.__new__(SQLAlchemyManager)
    manager._engine = engine
    manager.queue_cls = queue_cls
    manager.result_cls = MagicMock()
    manager.schedule_cls = MagicMock()
    return manager


def test_resolve_postgres_adapter() -> None:
    """postgresql dialect resolves to PostgresDialectAdapter."""
    manager = _mock_manager("postgresql")
    adapter = resolve_adapter(manager)
    assert isinstance(adapter, PostgresDialectAdapter)


def test_resolve_oracle_adapter() -> None:
    """oracle dialect resolves to OracleDialectAdapter."""
    manager = _mock_manager("oracle")
    adapter = resolve_adapter(manager)
    assert isinstance(adapter, OracleDialectAdapter)


def test_resolve_polling_adapter_for_sqlite() -> None:
    """sqlite dialect falls back to PollingAdapter."""
    manager = _mock_manager("sqlite")
    adapter = resolve_adapter(manager)
    assert isinstance(adapter, PollingAdapter)


def test_resolve_polling_adapter_for_unknown_dialect() -> None:
    """Any unrecognised dialect (e.g. mssql) also falls back to PollingAdapter."""
    manager = _mock_manager("mysql")
    adapter = resolve_adapter(manager)
    assert isinstance(adapter, PollingAdapter)
