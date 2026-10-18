# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Supplied migration connections retain their caller's transaction ownership."""

import runpy
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import sqlalchemy as sa
from alembic.config import Config

from alembic import command, context

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def environment():
    """Load the real environment without opening a database connection."""
    with (
        patch.object(
            context,
            "config",
            SimpleNamespace(
                config_file_name=None,
                attributes={},
                get_section=lambda *_args: {},
            ),
            create=True,
        ),
        patch("logging.config.fileConfig"),
        patch.object(context, "is_offline_mode", return_value=True),
        patch.object(context, "configure"),
        patch.object(context, "begin_transaction", return_value=nullcontext()),
        patch.object(context, "run_migrations"),
    ):
        namespace = runpy.run_path(str(ROOT / "alembic/env.py"))
    return namespace["run_migrations_online"].__globals__


def test_supplied_connection_is_used_without_async_engine_or_ownership_changes(environment):
    connection = Mock()
    environment["config"].attributes["connection"] = connection
    migrate = Mock()
    environment["do_run_migrations"] = migrate
    with patch.object(environment["asyncio"], "run") as run_async:
        environment["run_migrations_online"]()
    migrate.assert_called_once_with(connection)
    run_async.assert_not_called()
    assert connection.mock_calls == []


@pytest.mark.parametrize("attributes", ({}, {"connection": None}))
def test_absent_connection_retains_standalone_async_path(environment, attributes):
    environment["config"].attributes = attributes
    migrate = Mock()
    environment["do_run_migrations"] = migrate
    run_async = AsyncMock()
    environment["run_async_migrations"] = run_async
    environment["run_migrations_online"]()
    run_async.assert_awaited_once_with()
    migrate.assert_not_called()


def test_supplied_connection_failure_propagates_without_cleanup(environment):
    connection = Mock()
    environment["config"].attributes["connection"] = connection
    environment["do_run_migrations"] = Mock(side_effect=RuntimeError("migration failed"))
    with pytest.raises(RuntimeError, match="migration failed"):
        environment["run_migrations_online"]()
    assert connection.mock_calls == []


@pytest.mark.parametrize("fail", (False, True))
def test_real_alembic_upgrade_retains_outer_transaction_and_rolls_back(tmp_path, monkeypatch, fail):
    """Exercise a real revision DAG; adapt only PostgreSQL session setup for SQLite."""
    (tmp_path / "versions").mkdir()
    (tmp_path / "env.py").write_bytes((ROOT / "alembic/env.py").read_bytes())
    (tmp_path / "versions/001_probe.py").write_text(
        "from alembic import op\n"
        "revision = '001_probe'\n"
        "down_revision = None\n"
        "def upgrade():\n"
        "    op.execute('CREATE TABLE migration_probe (value INTEGER)')\n"
        "    op.execute('INSERT INTO migration_probe VALUES (7)')\n"
        + ("    raise RuntimeError('revision failed')\n" if fail else "")
    )
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(tmp_path))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "main")
    monkeypatch.setenv("DB_SCHEMA", "main")
    engine = sa.create_engine("sqlite://")
    setup_statements = []
    try:
        with engine.connect() as connection:

            @sa.event.listens_for(connection, "before_cursor_execute", retval=True)
            def adapt_session_setup(_connection, _cursor, statement, parameters, _context, _many):
                if statement in ('create schema if not exists "main"', 'SET search_path TO "main",public'):
                    setup_statements.append(statement)
                    return "SELECT 1", ()
                return statement, parameters

            connection.exec_driver_sql("BEGIN")
            transaction = connection.get_transaction()
            config.attributes["connection"] = connection
            expected_result = pytest.raises(RuntimeError, match="revision failed") if fail else nullcontext()
            with patch("logging.config.fileConfig"), expected_result:
                command.upgrade(config, "head")
            if not fail:
                assert (
                    connection.exec_driver_sql("SELECT version_num FROM main.alembic_version").scalar() == "001_probe"
                )
            assert transaction.is_active
            assert not connection.closed
            assert connection.exec_driver_sql("SELECT value FROM migration_probe").scalar() == 7
            transaction.rollback()
            assert not sa.inspect(connection).has_table("migration_probe")
            assert not sa.inspect(connection).has_table("alembic_version", schema="main")
    finally:
        engine.dispose()
    assert setup_statements == ['create schema if not exists "main"', 'SET search_path TO "main",public']
