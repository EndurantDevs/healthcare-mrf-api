# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

from db.models import FHIRFormularyUHCAdmissionReceipt


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260814010000_fhir_formulary_uhc_selected_receipt.py"
)


def _migration():
    module_spec = importlib.util.spec_from_file_location(
        "fhir_formulary_uhc_selected_receipt_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_selected_receipt_migration_is_additive_and_linear(monkeypatch) -> None:
    """The selected receipt extends the current linear migration head."""

    migration = _migration()
    operation = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "receipt_test")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    assert migration.revision == (
        "20260814010000_fhir_formulary_uhc_selected_receipt"
    )
    assert migration.down_revision == (
        "20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance"
    )
    assert [
        call.args[1].name for call in operation.add_column.call_args_list
    ] == [
        "expected_file_count",
        "excluded_file_count",
        "selected_source_file_ids",
        "exclusion_code",
    ]
    operation.alter_column.assert_called_once()
    operation.drop_constraint.assert_called_once()
    operation.create_check_constraint.assert_called_once()


def test_selected_receipt_guard_binds_canonical_verified_selection() -> None:
    """The guard retains 48 census proof while hashing only selected rows."""

    migration = _migration()
    guard_sql = " ".join(migration._guard_function_sql("receipt_test").split())
    hash_sql = " ".join(
        migration._selection_hash_function_sql("receipt_test").split()
    )
    install_sql = " ".join(
        " ".join(statement.split())
        for statement in migration._install_guard_statements("receipt_test")
    )

    assert "artifact_count IS DISTINCT FROM 48" in guard_sql
    assert "cs_artifact_count IS DISTINCT FROM 24" in guard_sql
    assert "ifp_artifact_count IS DISTINCT FROM 24" in guard_sql
    assert "selected_verified_artifact_count IS DISTINCT FROM" in guard_sql
    assert "canonical_selected_source_file_ids IS DISTINCT FROM" in guard_sql
    assert "ORDER BY pg_catalog.convert_to(artifact.family" in guard_sql
    assert "fhir_formulary_source_artifact_selection_sha256" in guard_sql
    assert "NEW.file_count = 48" in guard_sql
    assert "NEW.file_count < 48" in guard_sql
    assert "pg_catalog.array_to_string" in guard_sql
    assert "pg_catalog.cardinality(candidate_selected_source_file_ids)" in hash_sql
    assert "artifact.status = 'verified'" in hash_sql
    assert "artifact.source_file_id = ANY" in hash_sql
    assert "SECURITY DEFINER SET search_path = pg_catalog" in hash_sql
    assert "REVOKE ALL ON FUNCTION" in install_sql
    assert install_sql.count("ENABLE ALWAYS TRIGGER") == 2


def test_selected_receipt_model_keeps_ids_private_and_counts_exact() -> None:
    """ORM parity persists coverage but excludes private identity arrays."""

    table = FHIRFormularyUHCAdmissionReceipt.__table__
    assert {
        "expected_file_count",
        "excluded_file_count",
        "selected_source_file_ids",
        "exclusion_code",
    }.issubset(table.c.keys())
    assert FHIRFormularyUHCAdmissionReceipt.EXCLUDE_FIELDS == (
        "selected_source_file_ids",
    )
    values_check = next(
        constraint
        for constraint in table.constraints
        if constraint.name
        == "fhir_formulary_uhc_admission_receipt_values_check"
    )
    constraint_sql = str(values_check.sqltext)
    assert "file_count BETWEEN 1 AND 48" in constraint_sql
    assert "excluded_file_count = expected_file_count - file_count" in constraint_sql
    assert "cardinality(selected_source_file_ids) = file_count" in constraint_sql
    assert "exclusion_code = 'not_selected'" in constraint_sql


def test_selected_receipt_downgrade_blocks_only_partial_evidence() -> None:
    """A downgrade may discard derived full metadata but never a partial proof."""

    migration = _migration()
    fence_sql = " ".join(
        migration._partial_downgrade_fence_sql("receipt_test").split()
    )

    assert "WHERE excluded_file_count > 0" in fence_sql
    assert "selected_receipt_downgrade_blocked" in fence_sql
