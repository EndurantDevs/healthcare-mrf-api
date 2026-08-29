# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib.util
import io
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest

from db.models import ImportRun


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260829090000_import_run_idempotency_scope.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "migration_import_run_idempotency_scope",
        MIGRATION_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Context:
    as_sql = False

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


class _Bind:
    def __init__(self, fail_on: str | None = None):
        self.fail_on = fail_on
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str):
        self.statements.append(statement)
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError("fixture index build failed")


class _Operations:
    def __init__(self, fail_on: str | None = None):
        self.context = _Context()
        self.bind = _Bind(fail_on)

    def get_context(self):
        return self.context

    def get_bind(self):
        return self.bind


def _install_index_states(monkeypatch, migration, operations, states):
    calls = []
    state_iterator = iter(states)

    def has_matching_index(
        actual_operations,
        index_name,
        table_name,
        columns,
        **options,
    ):
        calls.append((index_name, tuple(columns), options))
        assert actual_operations is operations
        assert table_name == migration.TABLE_NAME
        state = next(state_iterator)
        if isinstance(state, Exception):
            raise state
        return state

    monkeypatch.setattr(migration, "has_matching_index", has_matching_index)
    return calls


def _assert_exact_checks(migration, calls):
    assert calls
    for _name, _columns, options in calls:
        assert options["schema"] == "fixture"
        assert options["unique"] is True
        assert str(options["postgresql_where"]) == migration.ACTIVE_PREDICATE


def test_migration_contract_keeps_global_and_composite_indexes():
    migration = _load_migration()
    model_indexes = {
        index["name"]: index
        for index in ImportRun.__my_additional_indexes__
    }

    assert migration.revision == "20260829090000_import_run_idempotency_scope"
    assert (
        migration.down_revision
        == "20260828120000_hospital_price_modifier_payer_identity"
    )
    assert model_indexes[migration.LEGACY_INDEX_NAME]["index_elements"] == (
        migration.LEGACY_INDEX_COLUMNS
    )
    assert model_indexes[migration.INDEX_NAME]["index_elements"] == (
        migration.INDEX_COLUMNS
    )
    for index_name in (migration.LEGACY_INDEX_NAME, migration.INDEX_NAME):
        assert model_indexes[index_name]["unique"] is True
        assert model_indexes[index_name]["where"] == migration.ACTIVE_PREDICATE


@pytest.mark.parametrize("initial_state", (False, "invalid"))
def test_upgrade_repairs_composite_after_verifying_global(
    monkeypatch,
    initial_state,
):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    first_composite_state = (
        RuntimeError(
            f"existing_schema_index_invalid:fixture.{migration.INDEX_NAME}"
        )
        if initial_state == "invalid"
        else False
    )
    calls = _install_index_states(
        monkeypatch,
        migration,
        operations,
        (True, first_composite_state, True),
    )
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()

    assert operations.bind.statements == [
        migration._drop_index_sql("fixture", migration.INDEX_NAME),
        migration._create_index_sql(
            "fixture",
            migration.INDEX_NAME,
            migration.INDEX_COLUMNS,
        ),
    ]
    assert calls[0][:2] == (
        migration.LEGACY_INDEX_NAME,
        migration.LEGACY_INDEX_COLUMNS,
    )
    _assert_exact_checks(migration, calls)


@pytest.mark.parametrize("initial_state", (False, "invalid"))
def test_upgrade_repairs_global_before_adopting_composite(
    monkeypatch,
    initial_state,
):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    first_global_state = (
        RuntimeError(
            "existing_schema_index_invalid:"
            f"fixture.{migration.LEGACY_INDEX_NAME}"
        )
        if initial_state == "invalid"
        else False
    )
    calls = _install_index_states(
        monkeypatch,
        migration,
        operations,
        (first_global_state, True, True),
    )
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()

    assert operations.bind.statements == [
        migration._drop_index_sql("fixture", migration.LEGACY_INDEX_NAME),
        migration._create_index_sql(
            "fixture",
            migration.LEGACY_INDEX_NAME,
            migration.LEGACY_INDEX_COLUMNS,
        ),
    ]
    _assert_exact_checks(migration, calls)


@pytest.mark.parametrize("index_name", ("LEGACY_INDEX_NAME", "INDEX_NAME"))
def test_wrong_shape_named_index_fails_closed(monkeypatch, index_name):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    actual_name = getattr(migration, index_name)
    mismatch = RuntimeError(
        f"existing_schema_index_mismatch:fixture.{actual_name}"
    )
    states = (
        (mismatch,)
        if index_name == "LEGACY_INDEX_NAME"
        else (True, mismatch)
    )
    _install_index_states(monkeypatch, migration, operations, states)
    monkeypatch.setattr(migration, "op", operations)

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration.upgrade()

    assert operations.bind.statements == []


def test_matching_indexes_are_adopted_without_ddl(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    calls = _install_index_states(
        monkeypatch,
        migration,
        operations,
        (True, True),
    )
    monkeypatch.setattr(migration, "op", operations)

    migration.upgrade()

    assert operations.bind.statements == []
    _assert_exact_checks(migration, calls)


def test_failed_composite_build_keeps_global_index(monkeypatch):
    migration = _load_migration()
    create_composite = migration._create_index_sql(
        "fixture",
        migration.INDEX_NAME,
        migration.INDEX_COLUMNS,
    )
    operations = _Operations(fail_on=create_composite)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    _install_index_states(
        monkeypatch,
        migration,
        operations,
        (True, False),
    )
    monkeypatch.setattr(migration, "op", operations)

    with pytest.raises(RuntimeError, match="fixture index build failed"):
        migration.upgrade()

    assert operations.bind.statements == [
        migration._drop_index_sql("fixture", migration.INDEX_NAME),
        create_composite,
    ]
    assert migration._drop_index_sql(
        "fixture",
        migration.LEGACY_INDEX_NAME,
    ) not in operations.bind.statements


def test_downgrade_restores_global_before_dropping_composite(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    calls = _install_index_states(
        monkeypatch,
        migration,
        operations,
        (False, True),
    )
    monkeypatch.setattr(migration, "op", operations)

    migration.downgrade()

    assert operations.bind.statements == [
        migration._drop_index_sql("fixture", migration.LEGACY_INDEX_NAME),
        migration._create_index_sql(
            "fixture",
            migration.LEGACY_INDEX_NAME,
            migration.LEGACY_INDEX_COLUMNS,
        ),
        migration._drop_index_sql("fixture", migration.INDEX_NAME),
    ]
    _assert_exact_checks(migration, calls)


def test_offline_sql_preserves_global_while_preparing_composite(monkeypatch):
    migration = _load_migration()
    output_buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output_buffer},
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", Operations(context))

    migration.upgrade()

    sql = output_buffer.getvalue()
    global_create = migration._create_index_sql(
        "fixture",
        migration.LEGACY_INDEX_NAME,
        migration.LEGACY_INDEX_COLUMNS,
    )
    composite_drop = migration._drop_index_sql("fixture", migration.INDEX_NAME)
    composite_create = migration._create_index_sql(
        "fixture",
        migration.INDEX_NAME,
        migration.INDEX_COLUMNS,
    )
    assert sql.index(global_create) < sql.index(composite_drop) < sql.index(
        composite_create
    )
    assert migration._drop_index_sql(
        "fixture",
        migration.LEGACY_INDEX_NAME,
    ) not in sql
    assert "CONCURRENTLY" in sql


def test_offline_downgrade_keeps_composite_without_exact_validation(
    monkeypatch,
):
    migration = _load_migration()
    output_buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output_buffer},
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", Operations(context))

    migration.downgrade()

    sql = output_buffer.getvalue()
    assert migration._create_index_sql(
        "fixture",
        migration.LEGACY_INDEX_NAME,
        migration.LEGACY_INDEX_COLUMNS,
    ) in sql
    assert migration._drop_index_sql("fixture", migration.INDEX_NAME) not in sql
