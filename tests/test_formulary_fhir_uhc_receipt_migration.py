# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa

from db.models import FHIRFormularyUHCAdmissionReceipt


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260810040000_fhir_formulary_uhc_admission_receipt.py"
)


def _migration():
    module_spec = importlib.util.spec_from_file_location(
        "fhir_formulary_uhc_admission_receipt_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _constraint(name: str):
    return next(
        constraint
        for constraint in FHIRFormularyUHCAdmissionReceipt.__table__.constraints
        if constraint.name == name
    )


def test_receipt_migration_is_linear_and_default_empty(monkeypatch) -> None:
    """The receipt is one linear dormant table with guarded installation."""

    migration = _migration()
    operation = Mock()
    operation.create_table = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "receipt_test")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    assert migration.revision == (
        "20260810040000_fhir_formulary_uhc_admission_receipt"
    )
    assert migration.down_revision == (
        "20260810030000_fhir_formulary_source_artifact"
    )
    operation.create_table.assert_called_once()
    assert operation.create_table.call_args.args[0] == (
        "fhir_formulary_uhc_admission_receipt"
    )
    assert operation.create_table.call_args.kwargs["schema"] == "receipt_test"
    executed_sql = " ".join(
        " ".join(str(call.args[0]).split())
        for call in operation.execute.call_args_list
    )
    assert "SECURITY DEFINER SET search_path = pg_catalog" in executed_sql
    assert "REVOKE ALL ON FUNCTION" in executed_sql
    assert "REVOKE ALL ON TABLE" in executed_sql
    assert executed_sql.count("ENABLE ALWAYS TRIGGER") == 2
    assert "BEFORE TRUNCATE" in executed_sql


def test_receipt_model_binds_admission_observation_and_file_set() -> None:
    """Model parity retains every source and generic-admission foreign key."""

    table = FHIRFormularyUHCAdmissionReceipt.__table__
    assert tuple(column.name for column in table.primary_key.columns) == (
        "receipt_id",
    )
    foreign_key_by_name = {
        constraint.name: constraint
        for constraint in table.constraints
        if isinstance(constraint, sa.ForeignKeyConstraint)
    }
    assert set(foreign_key_by_name) == {
        "fhir_formulary_uhc_admission_receipt_admission_fkey",
        "fhir_formulary_uhc_admission_receipt_observation_fkey",
        "fhir_formulary_uhc_admission_receipt_set_fkey",
    }
    assert tuple(
        foreign_key.target_fullname
        for foreign_key in foreign_key_by_name[
            "fhir_formulary_uhc_admission_receipt_admission_fkey"
        ].elements
    ) == (
        "mrf.fhir_formulary_twin_admission.source_id",
        "mrf.fhir_formulary_twin_admission.candidate_dataset_id",
    )
    assert _constraint(
        "fhir_formulary_uhc_admission_receipt_values_check"
    ) is not None


def test_receipt_guard_checks_exact_census_and_locked_downgrade() -> None:
    """The database validates linked evidence and blocks destructive rollback."""

    migration = _migration()
    guard_sql = " ".join(migration._guard_function_sql("receipt_test").split())
    install_sql = " ".join(
        " ".join(statement.split())
        for statement in migration._guard_install_statements("receipt_test")
    )
    downgrade_sql = " ".join(
        migration._downgrade_fence_sql("receipt_test").split()
    )

    assert "expected_file_count IS DISTINCT FROM 48" in guard_sql
    assert "artifact_count IS DISTINCT FROM 48" in guard_sql
    assert "verified_artifact_count IS DISTINCT FROM 48" in guard_sql
    assert "cs_artifact_count IS DISTINCT FROM 24" in guard_sql
    assert "ifp_artifact_count IS DISTINCT FROM 24" in guard_sql
    assert "max_artifact_verified_at > admission_cutoff_at" in guard_sql
    assert "fhir_formulary_source_artifact_set_sha256" in guard_sql
    assert "observed_artifact_set_sha256 IS DISTINCT FROM" in guard_sql
    assert "expected_receipt_id IS DISTINCT FROM NEW.receipt_id" in guard_sql
    assert "pg_catalog.chr(31)" in guard_sql
    assert "admission_list_count IS DISTINCT FROM NEW.plan_count" in guard_sql
    assert "FOR SHARE OF observation, artifact_set" in guard_sql
    assert "FOR SHARE OF admission" in guard_sql
    assert "FOR SHARE OF artifact" in guard_sql
    assert "TG_OP <> 'INSERT'" in guard_sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in install_sql
    assert "BEFORE TRUNCATE" in install_sql
    assert "downgrade_blocked" in downgrade_sql


def test_receipt_check_rejects_infinite_or_out_of_range_source_time() -> None:
    """Receipt timestamps must be finite and inside the reviewed source range."""

    values_constraint = _constraint(
        "fhir_formulary_uhc_admission_receipt_values_check"
    )
    constraint_sql = str(values_constraint.sqltext)

    assert "isfinite(max_last_updated_at)" in constraint_sql
    assert "2000-01-01" in constraint_sql
    assert "2101-01-01" in constraint_sql


def test_receipt_model_uses_the_guarded_database_timestamp_default() -> None:
    """ORM parity retains the transaction-timestamp receipt boundary."""

    recorded_at = FHIRFormularyUHCAdmissionReceipt.__table__.c.recorded_at

    assert recorded_at.server_default is not None
    assert "transaction_timestamp()" in str(recorded_at.server_default.arg)
