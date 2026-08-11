# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa

from db.models import FHIRFormularySourceAcquisitionLease


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / ("20260811030000_fhir_formulary_source_acquisition_lease.py")
)


def _migration():
    module_spec = importlib.util.spec_from_file_location(
        "fhir_formulary_source_acquisition_lease_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _normalized_sql(value: object) -> str:
    return " ".join(str(value).split())


def _assert_column_parity(
    migration_columns_by_name: dict[str, sa.Column],
    model_columns_by_name: dict[str, sa.Column],
    expected_column_names: tuple[str, ...],
) -> None:
    for column_name in expected_column_names:
        model_column = model_columns_by_name[column_name]
        migration_column = migration_columns_by_name[column_name]
        assert type(model_column.type) is type(migration_column.type)
        assert model_column.nullable is migration_column.nullable
        model_default = (
            None
            if model_column.server_default is None
            else _normalized_sql(model_column.server_default.arg)
        )
        migration_default = (
            None
            if migration_column.server_default is None
            else _normalized_sql(migration_column.server_default.arg)
        )
        assert model_default == migration_default


def _assert_foreign_key_parity(model_table: sa.Table) -> None:
    foreign_key = next(
        constraint
        for constraint in model_table.constraints
        if isinstance(constraint, sa.ForeignKeyConstraint)
    )
    assert foreign_key.name == ("fhir_formulary_source_acquisition_lease_source_fkey")
    assert foreign_key.ondelete == "RESTRICT"
    assert tuple(element.target_fullname for element in foreign_key.elements) == (
        "mrf.fhir_formulary_source.source_id",
    )


def _assert_state_check_parity(
    create_arguments: tuple[object, ...],
    model_table: sa.Table,
) -> None:
    migration_check = next(
        argument
        for argument in create_arguments
        if isinstance(argument, sa.CheckConstraint)
    )
    model_check = next(
        constraint
        for constraint in model_table.constraints
        if isinstance(constraint, sa.CheckConstraint)
    )
    assert (
        migration_check.name
        == model_check.name
        == ("fhir_formulary_source_acquisition_lease_state_check")
    )
    assert _normalized_sql(migration_check.sqltext) == _normalized_sql(
        model_check.sqltext
    )
    assert "INTERVAL '1 hour'" in _normalized_sql(migration_check.sqltext)


def test_source_acquisition_lease_is_the_linear_reusable_head(monkeypatch) -> None:
    migration = _migration()
    operation = Mock()
    operation.create_table = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "lease_test")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    assert migration.revision == (
        "20260811030000_fhir_formulary_source_acquisition_lease"
    )
    assert migration.down_revision == (
        "20260811020000_provider_directory_rooted_graph_acquisition"
    )
    operation.create_table.assert_called_once()
    assert operation.create_table.call_args.args[0] == (
        "fhir_formulary_source_acquisition_lease"
    )
    assert operation.create_table.call_args.kwargs["schema"] == "lease_test"
    executed_sql = " ".join(
        _normalized_sql(call.args[0]) for call in operation.execute.call_args_list
    )
    assert "SECURITY DEFINER SET search_path = pg_catalog" in executed_sql
    assert "REVOKE ALL ON TABLE" in executed_sql
    assert executed_sql.count("ENABLE ALWAYS TRIGGER") == 2


def test_source_acquisition_lease_model_has_full_migration_parity(
    monkeypatch,
) -> None:
    """The ORM carries every 101300 column, constraint, and server default."""

    migration = _migration()
    operation = Mock()
    operation.create_table = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(migration, "op", operation)
    migration.upgrade()

    create_arguments = operation.create_table.call_args.args[1:]
    migration_columns_by_name = {
        argument.name: argument
        for argument in create_arguments
        if isinstance(argument, sa.Column)
    }
    model_table = FHIRFormularySourceAcquisitionLease.__table__
    model_columns_by_name = {column.name: column for column in model_table.columns}
    expected_column_names = (
        "source_id",
        "lease_generation",
        "lease_token",
        "lease_expires_at",
        "lease_heartbeat_at",
        "claimed_at",
        "created_at",
        "updated_at",
    )

    assert tuple(model_columns_by_name) == expected_column_names
    assert tuple(migration_columns_by_name) == expected_column_names
    _assert_column_parity(
        migration_columns_by_name,
        model_columns_by_name,
        expected_column_names,
    )

    assert tuple(column.name for column in model_table.primary_key.columns) == (
        "source_id",
    )
    _assert_foreign_key_parity(model_table)
    _assert_state_check_parity(create_arguments, model_table)


def test_source_acquisition_lease_guard_fences_every_transition() -> None:
    migration = _migration()
    guard_sql = _normalized_sql(migration._guard_function_sql("lease_test"))
    install_sql = " ".join(
        _normalized_sql(statement)
        for statement in migration._guard_install_statements("lease_test")
    )

    assert "action_name = 'claim'" in guard_sql
    assert "OLD.lease_expires_at <= clock_timestamp()" in guard_sql
    assert "NEW.lease_generation <> OLD.lease_generation + 1" in guard_sql
    assert guard_sql.count("INTERVAL '1 hour'") == 2
    assert "action_name = 'heartbeat'" in guard_sql
    assert "action_generation IS DISTINCT FROM OLD.lease_generation" in guard_sql
    assert "action_token IS DISTINCT FROM OLD.lease_token" in guard_sql
    assert "action_name = 'release'" in guard_sql
    assert "NEW.lease_token IS NOT NULL" in guard_sql
    assert "TG_OP IN ('DELETE', 'TRUNCATE')" in guard_sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in install_sql
    assert "BEFORE TRUNCATE" in install_sql


def test_source_acquisition_lease_downgrade_fences_only_a_live_owner() -> None:
    downgrade_sql = _normalized_sql(_migration()._downgrade_fence_sql("lease_test"))

    assert "lease_token IS NOT NULL" in downgrade_sql
    assert "lease_expires_at > clock_timestamp()" in downgrade_sql
    assert "downgrade_blocked" in downgrade_sql
