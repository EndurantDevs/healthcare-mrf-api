# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Schema and model contracts for FHIR formulary twin evidence."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

from db.models import FHIRFormularyTwinAdmission
from db.models import FHIRFormularyTwinAttempt
from db.models.formulary_fhir_admission import _schema as _model_schema


ROOT = Path(__file__).resolve().parents[1]
ATTEMPT_PATH = ROOT / "alembic" / "versions" / (
    "20260808110000_fhir_formulary_twin_attempt.py"
)
ADMISSION_PATH = ROOT / "alembic" / "versions" / (
    "20260808120000_fhir_formulary_twin_admission.py"
)
GUARDS_PATH = ROOT / "alembic" / "versions" / (
    "20260808130000_fhir_formulary_publication_guards.py"
)
ATTEMPT_COLUMNS = (
    "source_id",
    "baseline_dataset_id",
    "baseline_run_id",
    "candidate_dataset_id",
    "candidate_run_id",
    "cutoff_at",
    "source_configuration_hash",
    "acquisition_contract_hash",
    "baseline_evidence_hash",
    "candidate_evidence_hash",
    "matched",
    "attempted_at",
)
ADMISSION_COLUMNS = (
    "source_id",
    "baseline_dataset_id",
    "baseline_run_id",
    "candidate_dataset_id",
    "candidate_run_id",
    "predecessor_dataset_id",
    "cutoff_at",
    "source_configuration_hash",
    "acquisition_contract_hash",
    "list_count",
    "alias_count",
    "medication_count",
    "coverage_hash",
    "membership_hash",
    "alternative_count",
    "alternative_hash",
    "baseline_verified_at",
    "candidate_verified_at",
    "admitted_at",
)


def _load_migration(path: Path, module_name: str):
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _upgrade_sql(migration, monkeypatch) -> str:
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "formulary_test")
    monkeypatch.setenv("DB_SCHEMA", "formulary_test")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return " ".join(" ".join(statements).split())


def _downgrade_sql(migration, monkeypatch) -> list[str]:
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    return statements


def _constraint(model, constraint_name: str):
    return next(
        constraint
        for constraint in model.__table__.constraints
        if constraint.name == constraint_name
    )


def _foreign_key_signature(model, constraint_name: str) -> tuple[tuple, tuple]:
    constraint = _constraint(model, constraint_name)
    assert isinstance(constraint, sa.ForeignKeyConstraint)
    return (
        tuple(column.name for column in constraint.columns),
        tuple(element.target_fullname for element in constraint.elements),
    )


def test_migrations_form_one_linear_head_and_install_no_rows(monkeypatch):
    attempt = _load_migration(ATTEMPT_PATH, "fhir_twin_attempt_migration")
    admission = _load_migration(ADMISSION_PATH, "fhir_twin_admission_migration")
    guards = _load_migration(GUARDS_PATH, "fhir_publication_guards_migration")
    attempt_sql = _upgrade_sql(attempt, monkeypatch)
    admission_sql = _upgrade_sql(admission, monkeypatch)
    guards_sql = _upgrade_sql(guards, monkeypatch)

    assert attempt.revision == "20260808110000_fhir_formulary_twin_attempt"
    assert attempt.down_revision == (
        "20260808100000_public_evidence_reference_roots"
    )
    assert admission.revision == "20260808120000_fhir_formulary_twin_admission"
    assert admission.down_revision == "20260808110000_fhir_formulary_twin_attempt"
    assert guards.revision == "20260808130000_fhir_formulary_publication_guards"
    assert guards.down_revision == "20260808120000_fhir_formulary_twin_admission"
    assert 'CREATE TABLE "formulary_test"."fhir_formulary_twin_attempt"' in attempt_sql
    assert (
        'CREATE TABLE "formulary_test"."fhir_formulary_twin_admission"'
        in admission_sql
    )
    assert "INSERT INTO" not in attempt_sql + admission_sql + guards_sql


def test_attempt_migration_burns_cross_role_roots_and_freezes_datasets(monkeypatch):
    migration = _load_migration(ATTEMPT_PATH, "fhir_twin_attempt_guards")
    sql = _upgrade_sql(migration, monkeypatch)

    for column_name in ATTEMPT_COLUMNS:
        assert column_name in sql
    assert "fhir_formulary_twin_attempt_baseline_key" in sql
    assert "fhir_formulary_twin_attempt_candidate_key" in sql
    assert "fhir_formulary_twin_attempt_binding_key" in sql
    assert "matched = (baseline_evidence_hash = candidate_evidence_hash)" in sql
    assert sql.index("FOR UPDATE") < sql.index("pg_advisory_xact_lock")
    assert "pg_advisory_xact_lock" in sql
    assert "baseline_dataset_id IN" in sql
    assert "candidate_dataset_id IN" in sql
    assert "fhir_formulary_twin_dataset_guard" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql
    assert "existing_row.matched = NEW.matched THEN RETURN NEW" in sql
    assert "fhir_formulary_twin_attempt_downgrade_forbidden" in " ".join(
        " ".join(statement.split())
        for statement in _downgrade_sql(migration, monkeypatch)
    )


def test_admission_migration_fails_closed_and_allows_only_exact_intents(monkeypatch):
    migration = _load_migration(GUARDS_PATH, "fhir_twin_publication_guards")
    sql = _upgrade_sql(migration, monkeypatch)

    assert "fhir_formulary_preexisting_current_invalid" in sql
    assert "candidate_row.publish_requested = false" in sql
    assert "candidate_row.seed_eligible = true" in sql
    assert "candidate_row.publish_requested IS DISTINCT FROM true" in sql
    assert "candidate_row.seed_eligible IS DISTINCT FROM false" in sql
    assert "fhir_formulary_current_intent_invalid" in sql
    assert "fhir_formulary_twin_admission_required" in sql
    assert "OLD.status = 'verified' AND NEW.status = 'published'" in sql
    assert "DEFERRABLE INITIALLY DEFERRED" in sql
    assert "fhir_formulary_current_immutable" in sql
    assert "source_metadata -> 'synthetic' IS DISTINCT FROM 'true'::jsonb" in sql
    assert "source_enabled IS DISTINCT FROM true" in sql
    assert "fhir_formulary_current_source_immutable" in sql
    assert "to_jsonb(NEW) - 'enabled' - 'updated_at'" in sql
    assert "OLD.status = 'published'" in sql


def test_admission_migration_binds_exact_evidence(monkeypatch):
    migration = _load_migration(ADMISSION_PATH, "fhir_twin_admission_evidence")
    sql = _upgrade_sql(migration, monkeypatch)

    for column_name in ADMISSION_COLUMNS:
        assert column_name in sql
    assert "fhir_formulary_twin_admission_attempt_fkey" in sql
    assert "fhir_formulary_twin_attempt_not_matched" in sql
    assert "fhir_formulary_twin_proof_mismatch" in sql


def test_publication_guards_freeze_graph_and_reject_late_content(monkeypatch):
    migration = _load_migration(GUARDS_PATH, "fhir_content_graph_guards")
    sql = _upgrade_sql(migration, monkeypatch)

    assert "guard_fhir_formulary_cow_immutable" in sql
    assert "BEFORE UPDATE OR DELETE OR TRUNCATE" in sql
    assert "fhir_formulary_checkpoint_truncate_guard" in sql
    assert "owner_status IS DISTINCT FROM 'building'" in sql
    assert "fhir_formulary_late_dataset_content" in sql
    assert "AFTER INSERT" in sql
    assert "fhir_formulary_late_alias_content" in sql
    assert "ORDER BY owner.source_id, owner.dataset_id FOR SHARE OF owner" in sql
    for table_name in (
        "fhir_formulary_coverage_plan",
        "fhir_formulary_dataset_alias",
        "fhir_formulary_alias_membership",
        "fhir_formulary_alternative",
    ):
        assert table_name in sql


def test_attempt_model_matches_exact_source_qualified_ownership():
    schema = FHIRFormularyTwinAttempt.__table__.schema
    dataset = f"{schema}.fhir_formulary_dataset"

    assert tuple(FHIRFormularyTwinAttempt.__table__.c.keys()) == ATTEMPT_COLUMNS
    assert tuple(FHIRFormularyTwinAttempt.__table__.primary_key.columns.keys()) == (
        "source_id",
        "baseline_dataset_id",
        "candidate_dataset_id",
    )
    assert _foreign_key_signature(
        FHIRFormularyTwinAttempt,
        "fhir_formulary_twin_attempt_baseline_fkey",
    ) == (
        ("source_id", "baseline_dataset_id", "baseline_run_id"),
        (f"{dataset}.source_id", f"{dataset}.dataset_id", f"{dataset}.run_id"),
    )
    assert _foreign_key_signature(
        FHIRFormularyTwinAttempt,
        "fhir_formulary_twin_attempt_candidate_fkey",
    ) == (
        ("source_id", "candidate_dataset_id", "candidate_run_id"),
        (f"{dataset}.source_id", f"{dataset}.dataset_id", f"{dataset}.run_id"),
    )


def test_admission_model_is_bound_to_exact_matched_attempt():
    schema = FHIRFormularyTwinAdmission.__table__.schema
    attempt = f"{schema}.fhir_formulary_twin_attempt"

    assert tuple(FHIRFormularyTwinAdmission.__table__.c.keys()) == ADMISSION_COLUMNS
    assert _foreign_key_signature(
        FHIRFormularyTwinAdmission,
        "fhir_formulary_twin_admission_attempt_fkey",
    ) == (
        (
            "source_id",
            "baseline_dataset_id",
            "baseline_run_id",
            "candidate_dataset_id",
            "candidate_run_id",
        ),
        (
            f"{attempt}.source_id",
            f"{attempt}.baseline_dataset_id",
            f"{attempt}.baseline_run_id",
            f"{attempt}.candidate_dataset_id",
            f"{attempt}.candidate_run_id",
        ),
    )
    assert FHIRFormularyTwinAttempt.__table__.c.attempted_at.server_default
    assert FHIRFormularyTwinAdmission.__table__.c.admitted_at.server_default


@pytest.mark.parametrize(
    "migration_path",
    (ATTEMPT_PATH, ADMISSION_PATH, GUARDS_PATH),
)
def test_schema_alias_conflicts_fail_closed(monkeypatch, migration_path):
    migration = _load_migration(migration_path, migration_path.stem)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="must match"):
        migration.upgrade()
    with pytest.raises(RuntimeError, match="must match"):
        _model_schema()
