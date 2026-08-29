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
    / "20260829100000_activate_import_run_idempotency_scope.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "migration_activate_import_run_idempotency_scope",
        MIGRATION_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Bind:
    def __init__(self, fail_on: str | None = None):
        self.fail_on = fail_on
        self.statements: list[str] = []

    def exec_driver_sql(self, statement: str):
        self.statements.append(statement)
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError("fixture index build failed")


class _Context:
    as_sql = False

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


class _Operations:
    def __init__(self, fail_on: str | None = None):
        self.bind = _Bind(fail_on)
        self.context = _Context()

    def get_bind(self):
        return self.bind

    def get_context(self):
        return self.context


def _install_index_states(
    monkeypatch,
    migration,
    operations,
    shape_states,
    named_states,
):
    shape_state_iterator = iter(shape_states)
    named_state_iterator = iter(named_states)

    def has_matching_index(
        actual_operations,
        _index_name,
        _table_name,
        _columns,
        **_options,
    ):
        assert actual_operations is operations
        state = next(shape_state_iterator)
        if isinstance(state, Exception):
            raise state
        return state

    monkeypatch.setattr(migration, "has_matching_index", has_matching_index)
    monkeypatch.setattr(
        migration,
        "_named_index_state",
        lambda _schema, _index_name: next(named_state_iterator),
    )


def _configure(
    monkeypatch,
    migration,
    operations,
    shape_states,
    named_states,
):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", operations)
    _install_index_states(
        monkeypatch,
        migration,
        operations,
        shape_states,
        named_states,
    )


TARGET_LIVE = ("fixture", "import_run", True, True, True)
TARGET_INVALID = ("fixture", "import_run", False, False, False)
TARGET_NONLIVE = ("fixture", "import_run", True, True, False)
FOREIGN_LIVE = ("fixture", "other_table", True, True, True)


def test_activation_contract_matches_current_model():
    migration = _load_migration()
    model_index_by_name = {
        index["name"]: index for index in ImportRun.__my_additional_indexes__
    }

    assert migration.revision == (
        "20260829100000_activate_import_run_idempotency_scope"
    )
    assert migration.down_revision == "20260829090000_import_run_idempotency_scope"
    assert migration.LEGACY_INDEX_NAME not in model_index_by_name
    assert model_index_by_name[migration.INDEX_NAME]["index_elements"] == (
        migration.INDEX_COLUMNS
    )


@pytest.mark.parametrize(
    ("named_state", "shape_states", "error"),
    (
        (None, (), "required_index_missing"),
        (TARGET_NONLIVE, (), "required_index_missing"),
        (TARGET_LIVE, (False,), "required_index_missing"),
        (
            TARGET_LIVE,
            (
                RuntimeError(
                    "existing_schema_index_invalid:fixture."
                    "import_run_importer_active_idempotency_idx"
                ),
            ),
            "existing_schema_index_invalid",
        ),
        (
            TARGET_LIVE,
            (
                RuntimeError(
                    "existing_schema_index_mismatch:fixture."
                    "import_run_importer_active_idempotency_idx"
                ),
            ),
            "existing_schema_index_mismatch",
        ),
        (FOREIGN_LIVE, (), "existing_schema_index_mismatch"),
    ),
)
def test_upgrade_requires_exact_live_composite(
    monkeypatch,
    named_state,
    shape_states,
    error,
):
    migration = _load_migration()
    operations = _Operations()
    _configure(
        monkeypatch,
        migration,
        operations,
        shape_states,
        (named_state,),
    )

    with pytest.raises(RuntimeError, match=error):
        migration.upgrade()

    assert operations.bind.statements == []


@pytest.mark.parametrize(
    ("legacy_state", "legacy_shape", "expected_statements"),
    (
        (TARGET_LIVE, (True,), ("drop",)),
        (None, (), ()),
        (TARGET_INVALID, (), ("drop",)),
        (TARGET_NONLIVE, (), ("drop",)),
    ),
)
def test_upgrade_drops_only_proven_target_legacy_after_composite_proof(
    monkeypatch,
    legacy_state,
    legacy_shape,
    expected_statements,
):
    migration = _load_migration()
    operations = _Operations()
    _configure(
        monkeypatch,
        migration,
        operations,
        (True, *legacy_shape),
        (TARGET_LIVE, legacy_state),
    )

    migration.upgrade()

    expected_sql_by_kind = {
        "drop": migration._drop_legacy_index_sql("fixture"),
    }
    assert operations.bind.statements == [
        expected_sql_by_kind[statement] for statement in expected_statements
    ]


def test_upgrade_rejects_foreign_same_name_legacy(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    _configure(
        monkeypatch,
        migration,
        operations,
        (True,),
        (TARGET_LIVE, FOREIGN_LIVE),
    )

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration.upgrade()

    assert operations.bind.statements == []


def test_upgrade_rejects_wrong_shape_legacy(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    mismatch = RuntimeError(
        "existing_schema_index_mismatch:fixture.import_run_active_idempotency_idx"
    )
    _configure(
        monkeypatch,
        migration,
        operations,
        (True, mismatch),
        (TARGET_LIVE, TARGET_LIVE),
    )

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration.upgrade()

    assert operations.bind.statements == []


@pytest.mark.parametrize(
    ("named_states", "shape_states", "expected_statements"),
    (
        ((None, None, TARGET_LIVE), (True,), ("create",)),
        (
            (TARGET_INVALID, TARGET_INVALID, TARGET_LIVE),
            (True,),
            ("drop", "create"),
        ),
        (
            (TARGET_NONLIVE, TARGET_NONLIVE, TARGET_LIVE),
            (True,),
            ("drop", "create"),
        ),
    ),
)
def test_downgrade_rebuilds_and_verifies_global_without_dropping_composite(
    monkeypatch,
    named_states,
    shape_states,
    expected_statements,
):
    migration = _load_migration()
    operations = _Operations()
    _configure(
        monkeypatch,
        migration,
        operations,
        shape_states,
        named_states,
    )

    migration.downgrade()

    expected_sql_by_kind = {
        "drop": migration._drop_legacy_index_sql("fixture"),
        "create": migration._create_legacy_index_sql("fixture"),
    }
    assert operations.bind.statements == [
        expected_sql_by_kind[statement] for statement in expected_statements
    ]
    assert all(migration.INDEX_NAME not in sql for sql in operations.bind.statements)


def test_downgrade_adopts_exact_live_global_without_ddl(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    _configure(
        monkeypatch,
        migration,
        operations,
        (True, True),
        (TARGET_LIVE, TARGET_LIVE),
    )

    migration.downgrade()

    assert operations.bind.statements == []


def test_downgrade_rejects_wrong_shape_global(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    mismatch = RuntimeError(
        "existing_schema_index_mismatch:fixture.import_run_active_idempotency_idx"
    )
    _configure(
        monkeypatch,
        migration,
        operations,
        (mismatch,),
        (TARGET_LIVE,),
    )

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration.downgrade()

    assert operations.bind.statements == []


def test_downgrade_rejects_foreign_same_name_global(monkeypatch):
    migration = _load_migration()
    operations = _Operations()
    _configure(
        monkeypatch,
        migration,
        operations,
        (),
        (FOREIGN_LIVE,),
    )

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration.downgrade()

    assert operations.bind.statements == []


def test_failed_downgrade_build_keeps_composite(monkeypatch):
    migration = _load_migration()
    create_legacy = migration._create_legacy_index_sql("fixture")
    operations = _Operations(fail_on=create_legacy)
    _configure(
        monkeypatch,
        migration,
        operations,
        (),
        (None, None, TARGET_INVALID),
    )

    with pytest.raises(RuntimeError, match="fixture index build failed"):
        migration.downgrade()

    assert operations.bind.statements == [
        create_legacy,
        migration._drop_legacy_index_sql("fixture"),
    ]
    assert all(migration.INDEX_NAME not in sql for sql in operations.bind.statements)


def _offline_operations(monkeypatch):
    migration = _load_migration()
    output_buffer = io.StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output_buffer},
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", Operations(context))
    return migration, output_buffer


def test_offline_upgrade_requires_live_index_validation(monkeypatch):
    migration, output_buffer = _offline_operations(monkeypatch)

    with pytest.raises(
        RuntimeError,
        match="offline_activation_requires_live_index_validation",
    ):
        migration.upgrade()

    assert output_buffer.getvalue() == ""


def test_offline_downgrade_restores_only_global_index(monkeypatch):
    migration, output_buffer = _offline_operations(monkeypatch)

    migration.downgrade()

    sql = output_buffer.getvalue()
    assert migration._create_legacy_index_sql("fixture") in sql
    assert migration.INDEX_NAME not in sql
