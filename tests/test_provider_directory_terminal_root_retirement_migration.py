# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Migration contracts for narrow legacy terminal-root retirement."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as evidence,
)
from db import migration_provider_directory_terminal_root_retirement_guards as guards
from process.provider_directory_terminal_root_retirement_contract import (
    REQUIRED_CHILD_RELATIONS,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / ("20260810090000_provider_directory_terminal_root_retirement.py")
)
REPAIR_MIGRATION_PATH = (
    ROOT
    / "alembic/versions"
    / (
        "20260810100000_provider_directory_terminal_root_retirement_resource_count_repair.py"
    )
)


class _OperationsRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: object) -> None:
        self.statements.append(str(statement))


def _migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_terminal_root_retirement_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _repair_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_terminal_root_retirement_resource_count_repair",
        REPAIR_MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _normalized(value: str) -> str:
    return " ".join(value.split())


def test_revision_and_resource_count_repair_form_additive_chain() -> None:
    migration = _migration()
    repair = _repair_migration()
    assert migration.revision == (
        "20260810090000_provider_directory_terminal_root_retirement"
    )
    assert migration.down_revision == (
        "20260810080000_provider_directory_uhc_flex_practitioner_publication"
    )
    assert repair.revision == (
        "20260810100000_provider_directory_terminal_root_retirement_resource_count_repair"
    )
    assert repair.down_revision == (
        "20260809040000_ptg_import_wave_ordinary_cutover"
    )


def test_evidence_inventory_is_closed_and_hashes_indirect_outputs() -> None:
    relation_sql = evidence.relation_evidence_function_sql("terminal_test")
    evidence_sql = evidence.evidence_function_sql("terminal_test")
    assert set(evidence._relation_specs("terminal_test")) == set(
        REQUIRED_CHILD_RELATIONS
    )
    assert (
        'JOIN "terminal_test"."provider_directory_bulk_acquisition_checkpoint" AS bulk'
        in relation_sql
    )
    assert "bulk.dataset_id = candidate_dataset_id" in relation_sql
    assert "pg_catalog.to_jsonb(row) - 'payload_bytes'" in relation_sql
    assert "pg_catalog.sha256(row.payload_bytes)" in relation_sql
    assert "SET TimeZone = 'UTC'" in relation_sql
    assert "SET TimeZone = 'UTC'" in evidence_sql
    assert "pg_catalog.sum(grouped.row_count)" in evidence_sql
    assert "SELECT pg_catalog.count(*)::bigint AS actual_count" not in evidence_sql
    assert "pg_catalog.max(finished_at) AT TIME ZONE 'UTC'" in evidence_sql
    assert "ancestor.depth < 128" in evidence_sql
    assert "NOT child.run_id = ANY(ancestor.path)" in evidence_sql
    assert "AND child.importer = 'provider-directory-fhir'" not in evidence_sql
    assert "IS DISTINCT FROM 'provider-directory-fhir'" in evidence_sql
    assert "count(DISTINCT lineage_walk.depth)" in evidence_sql
    assert "lineage_edges.child_count > 1" in evidence_sql
    assert "lineage_edges.invalid_edge" in evidence_sql
    assert "lineage_edges.run_id = parent.import_run_id" in evidence_sql
    assert "lineage_shape.is_linear" in evidence_sql
    for field_name in (
        "actual_resource_count",
        "parent_resource_count",
        "proof_shard_count",
        "proof_row_count",
        "prior_status",
        "lineage_finished_at",
        "terminal_run_count",
    ):
        assert f"'{field_name}'" in evidence_sql


def test_eligibility_is_narrow_v1_terminal_and_root_anchored() -> None:
    sql = _normalized(guards.eligible_function_sql("terminal_test"))
    assert "parent.status IN ('acquiring', 'acquisition_retired')" in sql
    assert "(lineage_walk.import_row).retry_of_run_id IS NULL" in sql
    assert "lineage_edges.child_count > 1" in sql
    assert "lineage_edges.run_id = parent.import_run_id" in sql
    assert "lineage_shape.is_linear" in sql
    assert "AND child.importer = 'provider-directory-fhir'" not in sql
    assert "parent.completion_proof_required_version IS NULL" in sql
    assert "parent.completion_proof_json IS NULL" in sql
    assert "parent.completion_proof_sha256 IS NULL" in sql
    assert "? 'resource_hash_contract'" in sql
    assert "resource_hash_contract' = 'transport_bound_v1'" in sql
    assert "minimum_age BETWEEN 900 AND 604800" in sql
    assert "'canceled', 'cancelled', 'dead_letter', 'failed'" in sql
    assert "pg_catalog.transaction_timestamp() - pg_catalog.make_interval" in sql
    assert "AT TIME ZONE" not in sql
    assert evidence.EVIDENCE_FUNCTION not in sql


def test_marker_validation_separates_transition_snapshot_from_replay() -> None:
    marker_sql = _normalized(guards.marker_function_sql("terminal_test"))
    parent_sql = _normalized(guards.parent_guard_function_sql("terminal_test"))
    assert "WHEN candidate_dataset_id IS NULL THEN TRUE" in marker_sql
    for driftable_field in (
        "predecessor_identity_sha256",
        "source_identity_sha256",
        "target_identity_sha256",
    ):
        assert driftable_field in marker_sql
    assert "provider_directory_endpoint_dataset_previous_reference" in marker_sql
    assert "-> 'row_count' = '0'::jsonb" in marker_sql
    assert f'{guards.MARKER_FUNCTION}"(NULL, marker)' in parent_sql
    assert "(marker -> 'evidence') IS DISTINCT FROM" in parent_sql
    assert parent_sql.count(evidence.EVIDENCE_FUNCTION) == 1
    assert guards.VALID_FUNCTION not in parent_sql


def test_parent_guard_allows_only_status_and_marker_transition() -> None:
    sql = _normalized(guards.parent_guard_function_sql("terminal_test"))
    assert "OLD.status <> 'acquiring'" in sql
    assert "'status', 'publication_metadata_json'" in sql
    assert "OLD.publication_metadata_json" in sql
    assert "predecessor.status = 'published'" in sql
    assert "predecessor.is_current IS TRUE" in sql
    assert "competing.status IN ('acquiring', 'incomplete')" in sql
    assert "terminal_root_retirement_reference_forbidden" in sql
    assert "terminal_root_retirement_immutable" in sql
    assert "IF OLD.status = 'acquisition_retired' THEN RAISE EXCEPTION" in sql


def test_child_and_run_guards_cover_late_attachments() -> None:
    child_sql = _normalized(guards.child_guard_function_sql("terminal_test"))
    run_sql = _normalized(guards.import_run_guard_function_sql("terminal_test"))
    migration = _migration()
    assert set(guards.CHILD_TRIGGER_SUFFIXES) == (
        set(REQUIRED_CHILD_RELATIONS)
        - {"provider_directory_endpoint_dataset_previous_reference"}
    )
    assert "provider_directory_bulk_output_checkpoint" in child_sql
    assert "WHERE checkpoint_id = OLD.checkpoint_id" in child_sql
    assert "OLD.previous_dataset_id" in child_sql
    assert "NEW.previous_dataset_id" in child_sql
    assert "NEW.retry_of_run_id" in run_sql
    assert guards.RUN_RETIRED_FUNCTION in run_sql
    assert "CONSTRAINT TRIGGER" not in "\n".join(
        migration._function_sqls("terminal_test")
    )


def test_upgrade_is_fail_closed_and_enables_always_triggers(monkeypatch) -> None:
    migration = _migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.upgrade()
    sql = "\n".join(recorder.statements)
    assert "terminal_root_retirement_adoption_blocked" in sql
    assert "status = 'acquisition_retired'" in sql
    assert "ENABLE ALWAYS TRIGGER" in sql
    assert "REVOKE ALL ON FUNCTION" in sql
    assert "CONSTRAINT TRIGGER" not in sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql


def test_downgrade_refuses_used_status_or_marker(monkeypatch) -> None:
    migration = _migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.downgrade()
    sql = "\n".join(recorder.statements)
    assert "terminal_root_retirement_downgrade_blocked" in sql
    assert "status = 'acquisition_retired'" in sql
    assert guards.MARKER in sql
    assert "DROP TRIGGER IF EXISTS" in sql
    assert "DROP FUNCTION IF EXISTS" in sql


def test_resource_count_repair_is_fenced_replacement(monkeypatch) -> None:
    migration = _repair_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    migration.upgrade()

    sql = "\n".join(recorder.statements)
    normalized_sql = _normalized(sql)
    assert recorder.statements[0] == (
        'LOCK TABLE "terminal_test"."provider_directory_endpoint_dataset" '
        "IN SHARE ROW EXCLUSIVE MODE;"
    )
    assert (
        "provider_directory_terminal_root_retirement_resource_count_repair_used" in sql
    )
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 1
    assert "pg_catalog.sum(grouped.row_count)" in sql
    assert "SELECT pg_catalog.count(*)::bigint AS actual_count" not in sql
    assert (
        normalized_sql.count(
            "provider_directory_terminal_root_retirement_evidence_function_changed"
        )
        == 2
    )
    assert "pg_catalog.to_regprocedure" in sql
    assert "TimeZone=UTC" in sql
    assert "pg_catalog.sha256" in sql
    assert "pg_catalog.btrim(" in sql
    assert "function_row.prosrc" in sql
    assert "'[[:space:]]+'" in sql
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 1


def test_resource_count_repair_downgrade_preserves_corrected_body(
    monkeypatch,
) -> None:
    migration = _repair_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration.downgrade()

    assert recorder.statements == []


def test_resource_count_repair_rejects_conflicting_schema_env(monkeypatch) -> None:
    migration = _repair_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")

    with pytest.raises(RuntimeError, match="must match"):
        migration._schema()


@pytest.mark.parametrize(
    "function_sql",
    (
        "SELECT 1\n    $function$;",
        "AS $function$\nSELECT 1",
    ),
)
def test_resource_count_repair_rejects_unknown_body_delimiters(
    monkeypatch,
    function_sql: str,
) -> None:
    migration = _repair_migration()
    monkeypatch.setattr(
        migration.evidence,
        "evidence_function_sql",
        lambda _schema: function_sql,
    )

    with pytest.raises(RuntimeError, match="function body changed"):
        migration._function_body_sha256("terminal_test", corrected=True)


def test_resource_count_repair_rejects_unknown_body_and_ddl(monkeypatch) -> None:
    migration = _repair_migration()
    unknown_body = "AS $function$\nSELECT 1\n    $function$;"
    monkeypatch.setattr(
        migration.evidence,
        "evidence_function_sql",
        lambda _schema: unknown_body,
    )

    with pytest.raises(RuntimeError, match="count SQL changed"):
        migration._function_body_sha256("terminal_test", corrected=True)
    with pytest.raises(RuntimeError, match="function DDL changed"):
        migration._replacement_sql("terminal_test")
