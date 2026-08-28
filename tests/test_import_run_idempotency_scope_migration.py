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


def _assert_exact_checks(migration, calls, index_name, columns):
    assert calls
    assert all(call[:2] == (index_name, columns) for call in calls)
    for _name, _columns, options in calls:
        assert options["schema"] == "fixture"
        assert options["unique"] is True
        assert str(options["postgresql_where"]) == migration.ACTIVE_PREDICATE


def test_migration_contract_matches_model():
    migration = _load_migration()
    model_index = next(
        index
        for index in ImportRun.__my_additional_indexes__
        if index["name"] == migration.INDEX_NAME
    )

    assert migration.revision == "20260829090000_import_run_idempotency_scope"
    assert (
        migration.down_revision
        == "20260828120000_hospital_price_modifier_payer_identity"
    )
    assert model_index["index_elements"] == migration.INDEX_COLUMNS
    assert model_index["unique"] is True
    assert model_index["where"] == migration.ACTIVE_PREDICATE


@pytest.mark.parametrize(
    ("method_name", "target_name", "target_columns", "obsolete_name"),
    (
        ("upgrade", "INDEX_NAME", "INDEX_COLUMNS", "LEGACY_INDEX_NAME"),
        ("downgrade", "LEGACY_INDEX_NAME", "LEGACY_INDEX_COLUMNS", "INDEX_NAME"),
    ),
)
@pytest.mark.parametrize("initial_state", (False, "invalid"))
def test_online_transition_recovers_target_then_verifies_before_drop(
    monkeypatch,
    method_name,
    target_name,
    target_columns,
    obsolete_name,
    initial_state,
):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    target_name = getattr(migration, target_name)
    target_columns = getattr(migration, target_columns)
    obsolete_name = getattr(migration, obsolete_name)
    first_state = (
        RuntimeError(f"existing_schema_index_invalid:fixture.{target_name}")
        if initial_state == "invalid"
        else False
    )
    calls = _install_index_states(
        monkeypatch,
        migration,
        operations,
        (first_state, True),
    )
    monkeypatch.setattr(migration, "op", operations)

    getattr(migration, method_name)()

    assert operations.bind.statements == [
        migration._drop_index_sql("fixture", target_name),
        migration._create_index_sql("fixture", target_name, target_columns),
        migration._drop_index_sql("fixture", obsolete_name),
    ]
    _assert_exact_checks(migration, calls, target_name, target_columns)


@pytest.mark.parametrize(
    ("method_name", "target_name", "target_columns", "obsolete_name"),
    (
        ("upgrade", "INDEX_NAME", "INDEX_COLUMNS", "LEGACY_INDEX_NAME"),
        ("downgrade", "LEGACY_INDEX_NAME", "LEGACY_INDEX_COLUMNS", "INDEX_NAME"),
    ),
)
def test_online_retry_adopts_verified_target_before_obsolete_drop(
    monkeypatch,
    method_name,
    target_name,
    target_columns,
    obsolete_name,
):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    target_name = getattr(migration, target_name)
    target_columns = getattr(migration, target_columns)
    obsolete_name = getattr(migration, obsolete_name)
    calls = _install_index_states(
        monkeypatch,
        migration,
        operations,
        (True,),
    )
    monkeypatch.setattr(migration, "op", operations)

    getattr(migration, method_name)()

    assert operations.bind.statements == [
        migration._drop_index_sql("fixture", obsolete_name)
    ]
    _assert_exact_checks(migration, calls, target_name, target_columns)


def test_wrong_shape_target_fails_closed(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    mismatch = RuntimeError(
        f"existing_schema_index_mismatch:fixture.{migration.INDEX_NAME}"
    )
    _install_index_states(monkeypatch, migration, operations, (mismatch,))
    monkeypatch.setattr(migration, "op", operations)

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration.upgrade()

    assert operations.bind.statements == []


def test_failed_downgrade_build_keeps_composite_index(monkeypatch):
    migration = _load_migration()
    create_legacy = migration._create_index_sql(
        "fixture",
        migration.LEGACY_INDEX_NAME,
        migration.LEGACY_INDEX_COLUMNS,
    )
    operations = _Operations(fail_on=create_legacy)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    _install_index_states(monkeypatch, migration, operations, (False,))
    monkeypatch.setattr(migration, "op", operations)

    with pytest.raises(RuntimeError, match="fixture index build failed"):
        migration.downgrade()

    assert operations.bind.statements == [
        migration._drop_index_sql("fixture", migration.LEGACY_INDEX_NAME),
        create_legacy,
    ]
    assert all(
        migration.INDEX_NAME not in statement
        for statement in operations.bind.statements
    )


@pytest.mark.parametrize(
    ("method_name", "target_name", "target_columns", "obsolete_name"),
    (
        ("upgrade", "INDEX_NAME", "INDEX_COLUMNS", "LEGACY_INDEX_NAME"),
        ("downgrade", "LEGACY_INDEX_NAME", "LEGACY_INDEX_COLUMNS", "INDEX_NAME"),
    ),
)
def test_offline_sql_preserves_obsolete_index_until_target_build(
    monkeypatch,
    method_name,
    target_name,
    target_columns,
    obsolete_name,
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
    target_name = getattr(migration, target_name)
    target_columns = getattr(migration, target_columns)
    obsolete_name = getattr(migration, obsolete_name)

    getattr(migration, method_name)()

    sql = output_buffer.getvalue()
    target_drop = migration._drop_index_sql("fixture", target_name)
    target_create = migration._create_index_sql(
        "fixture",
        target_name,
        target_columns,
    )
    obsolete_drop = migration._drop_index_sql("fixture", obsolete_name)
    assert sql.index(target_drop) < sql.index(target_create) < sql.index(obsolete_drop)
    assert "CONCURRENTLY" in sql
