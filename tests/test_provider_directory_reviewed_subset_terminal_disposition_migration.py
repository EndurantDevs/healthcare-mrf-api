# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for the reviewed-subset terminal disposition."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260810010000_provider_directory_reviewed_subset_terminal_disposition.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_subset_terminal_disposition_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _Recorder:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(str(statement))


def _capture(monkeypatch, operation):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_disposition_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    statements = [
        " ".join(statement.split()) for statement in recorder.statements
    ]
    return migration, recorder, statements, " ".join(statements)


def _assert_additive_upgrade_shape(migration, statements, normalized_sql) -> None:
    """Check revision lineage and the exact additive object inventory."""

    assert migration.revision == (
        "20260810010000_provider_directory_reviewed_subset_terminal_disposition"
    )
    assert migration.down_revision == (
        "20260810000000_provider_directory_reviewed_subset_bounded_drift"
    )
    assert statements[0].startswith("LOCK TABLE")
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert normalized_sql.count("CREATE FUNCTION") == 1
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 2
    assert normalized_sql.count("CREATE CONSTRAINT TRIGGER") == 2
    assert normalized_sql.count("ENABLE ALWAYS TRIGGER") == 2
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 1
    assert "provider_directory_subset_terminal_disposition_valid" in normalized_sql
    assert migration._DATASET_CONSTRAINT in normalized_sql
    assert migration._CHECKPOINT_CONSTRAINT in normalized_sql
    assert "SECURITY DEFINER SET search_path = pg_catalog" in normalized_sql
    assert "provider_directory_subset_terminal_disposition_adoption_blocked" in (
        normalized_sql
    )
    assert "pending_reviewed_subset_acquisition" in normalized_sql
    assert "required_root_count', 1" in normalized_sql
    assert migration._MARKER in normalized_sql
    assert migration._LEGACY_MARKER in normalized_sql
    assert all(
        len(name.encode("utf-8")) <= 63
        for name in (
            migration._DATASET_CONSTRAINT,
            migration._CHECKPOINT_CONSTRAINT,
        )
    )


def _assert_upgrade_has_no_evidence_dml(
    recorder,
    statements,
) -> None:
    """Reject evidence writes and accidental multi-command simple DDL."""

    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )
    replacement_statements = [
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    ]
    assert all(
        "provider_directory_subset_abandonment_valid" not in statement.split(
            "(", 1
        )[0]
        for statement in replacement_statements
    )
    simple_statements = [
        statement
        for statement in recorder.statements
        if statement.lstrip().startswith(
            (
                "CREATE CONSTRAINT TRIGGER",
                "ALTER TABLE",
                "REVOKE ALL",
            )
        )
    ]
    assert len(simple_statements) == 5
    assert all(statement.strip().count(";") == 1 for statement in simple_statements)


def test_upgrade_is_additive_closed_and_has_no_evidence_dml(monkeypatch):
    """Install only the closed validator and its two deferred aliases."""

    migration, recorder, statements, normalized_sql = _capture(
        monkeypatch,
        "upgrade",
    )
    _assert_additive_upgrade_shape(migration, statements, normalized_sql)
    _assert_upgrade_has_no_evidence_dml(recorder, statements)


def test_validator_binds_exact_partition_lineage_counts_and_hashes():
    """Bind the marker to exact lineage, proof, count, and resource evidence."""

    migration = _load_migration()
    validation_sql = " ".join(
        migration._valid_function_sql("terminal_disposition_test").split()
    )

    for marker_field in migration._MARKER_FIELDS:
        assert marker_field in validation_sql
    for disposition_field in migration._RESOURCE_DISPOSITION_FIELDS:
        assert disposition_field in validation_sql
    for literal in (
        migration._CONTRACT,
        migration._REASON,
        migration._STABLE_COMPLETE,
        migration._COUNT_DRIFT,
        migration._RETRYABLE_HTTP_500,
        migration._BLOCKED_CENSUS_DRIFT,
        migration._RETRYABLE_HTTP_500_ERROR,
        "provider_directory_reviewed_root_policy_v1",
        "requires_twin_root_verification",
        "completion_proof_v1",
        "resource_diagnostics",
        "resource_hash_contract",
        "transport_neutral_v2",
        "reused_from_checkpoint",
        "server_issued_subset_coverage",
        "terminal_page_start_offset",
        "logical_window_end_offset",
        "provider_directory_subset_payload_sha256",
        "provider_directory_dataset_proof_shard",
        "provider_directory_bulk_acquisition_checkpoint",
    ):
        assert literal in validation_sql
    assert "disposition_count_complete <> 2" in validation_sql
    assert "disposition_count_drift <> 1" in validation_sql
    assert "disposition_count_retryable <> 4" in validation_sql
    assert "terminal_page_delta <> 1" in validation_sql
    assert "shared_proof_identity" in validation_sql
    assert "LEAST" in validation_sql
    assert "ARRAY['run_id', 'observed_at', 'resources']" not in validation_sql
    assert "checkpoint_row.owner_run_id IS DISTINCT FROM" in validation_sql
    assert "proof_resource_count IS DISTINCT FROM" in validation_sql
    assert "candidate_metadata ? 'provider_directory_reviewed_subset_abandonment_v1'" in (
        validation_sql
    )
    assert "EXCEPTION WHEN OTHERS THEN RETURN FALSE" in validation_sql
    assert "provider_directory_source" not in validation_sql


def test_transition_binds_current_manual_profile_and_import_envelopes():
    """Bind mutable source state only while the terminal seal is created."""

    migration = _load_migration()
    guard_sql = " ".join(
        migration._dataset_guard_sql("terminal_disposition_test").split()
    )
    manual_resources = (
        "ARRAY['InsurancePlan', 'PractitionerRole', 'Practitioner', "
        "'Organization', 'Location', 'HealthcareService', "
        "'OrganizationAffiliation']::text[]"
    )

    assert manual_resources in guard_sql
    for literal in (
        "provider_directory_current_version_census_start_urls",
        "provider_directory_subset_canonical_sha256",
        "contract_identity",
        "source_scope_sha256",
        "start_url_hash IS DISTINCT FROM",
        "ARRAY['run_id', 'observed_at', 'resources']::text[]",
        "pg_input_is_valid",
        "ARRAY['acquisition_root_run_id', 'terminal_run_id', 'source_ids'",
    ):
        assert literal in guard_sql
    assert "endpoint-serving" not in guard_sql


def test_clean_downgrade_is_fenced_then_restores_predecessor_guards(
    monkeypatch,
):
    """Fence evidence before dropping aliases and restoring old guard bodies."""

    migration, _recorder, statements, normalized_sql = _capture(
        monkeypatch,
        "downgrade",
    )

    fence_position = normalized_sql.index(
        "provider_directory_subset_terminal_disposition_downgrade_blocked"
    )
    drop_position = normalized_sql.index("DROP TRIGGER")
    restore_position = normalized_sql.index("CREATE OR REPLACE FUNCTION")
    assert fence_position < drop_position < restore_position
    assert normalized_sql.count("DROP TRIGGER") == 2
    assert normalized_sql.count("DROP FUNCTION") == 1
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 2
    assert migration._MARKER in normalized_sql
    assert migration._LEGACY_MARKER in normalized_sql
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


def test_v1_validator_renderer_is_not_changed_by_successor():
    """Leave the predecessor expired-cursor validator byte-for-byte intact."""

    migration = _load_migration()
    abandonment = migration._abandonment()
    original = abandonment._valid_function_sql("terminal_disposition_test")

    assert migration._LEGACY_MARKER in original
    assert migration._MARKER not in original
    assert migration._VALID not in original
    assert "expired_server_cursor" in original
