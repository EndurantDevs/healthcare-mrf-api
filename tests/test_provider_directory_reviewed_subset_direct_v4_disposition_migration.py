# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for the thin reviewed direct-v4 disposition successor."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260810110000_provider_directory_reviewed_subset_direct_v4_disposition.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_subset_direct_v4_migration",
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
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "terminal_v4_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    statements = [" ".join(value.split()) for value in recorder.statements]
    return migration, statements, " ".join(statements)


def test_upgrade_adds_one_helper_and_replaces_only_existing_dispatchers(
    monkeypatch,
):
    """Keep the successor additive and preserve the checkpoint guard."""

    migration, statements, normalized_sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260810110000_provider_directory_reviewed_subset_direct_v4_disposition"
    )
    assert migration.down_revision == (
        "20260810100000_provider_directory_terminal_root_retirement_"
        "resource_count_repair"
    )
    assert statements[0].startswith("LOCK TABLE")
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert normalized_sql.count("CREATE FUNCTION") == 1
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 2
    assert migration._DIRECT_VALID in normalized_sql
    assert migration._VALID in normalized_sql
    assert "guard_provider_directory_subset_abandonment_dataset" in normalized_sql
    assert "guard_provider_directory_subset_abandonment_checkpoint" not in (
        " ".join(
            statement
            for statement in statements
            if statement.startswith("CREATE OR REPLACE FUNCTION")
        )
    )
    assert "CREATE TRIGGER" not in normalized_sql
    assert "DROP TRIGGER" not in normalized_sql
    assert "CREATE TABLE" not in normalized_sql.replace(
        "CREATE TEMP TABLE", ""
    )
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


def test_private_validator_binds_exact_marker_and_durable_evidence():
    """Bind the frozen profile to the retained checkpoints, rows, and proofs."""

    migration = _load_migration()
    validation_sql = " ".join(
        migration._direct_valid_sql("terminal_v4_test").split()
    )

    assert migration._MARKER_SHA256 in validation_sql
    assert migration._CONTRACT in validation_sql
    assert migration._REASON in validation_sql
    assert "candidate_metadata_sha256" in validation_sql
    assert "source_diagnostics_sha256" in validation_sql
    assert "direct_lineage" in validation_sql
    assert "terminal_page_entry_count" in validation_sql
    assert "jsonb_object_keys" in validation_sql
    assert "resource_counts_json" in validation_sql
    assert "provider_directory_dataset_proof_shard" in validation_sql
    assert "provider_directory_pagination_checkpoint" in validation_sql
    assert "provider_directory_bulk_acquisition_checkpoint" in validation_sql
    assert "EXCEPTION WHEN OTHERS THEN RETURN FALSE" in validation_sql
    assert "provider_directory_source" not in validation_sql
    for resource_type in migration._DRIFT_RESOURCE_TYPES:
        assert resource_type in validation_sql


def test_transition_binds_source_and_zero_lineage_only_at_seal_time():
    """Keep replay source-independent while closing the initial mutation."""

    migration = _load_migration()
    guard_sql = " ".join(
        migration._dataset_guard_sql("terminal_v4_test").split()
    )

    for literal in (
        migration._MARKER_SHA256,
        "provider_directory_configured_endpoint_id",
        "provider_directory_candidate_status",
        "provider_directory_current_version_census_start_urls",
        "provider_directory_supported_resources",
        "NULLIF(source.endpoint_id, '') IS NOT NULL",
        "source_import_sha256",
        "source_diagnostics_sha256",
        "previous_dataset_id = NEW.dataset_id",
        "retry_of_run_id IS NOT NULL",
        "import_row.run_id IN (",
        "import_row.retry_of_run_id IN (",
    ):
        assert literal in guard_sql
    assert guard_sql.index(migration._CONTRACT) < guard_sql.index(
        "OLD.status <> 'failed'"
    )
    assert "RETURN NEW" in guard_sql


def test_upgrade_and_downgrade_fence_only_inner_v2_contract(monkeypatch):
    """Allow historical v1 evidence while fencing exact v2 adoption/reversal."""

    migration, _upgrade_statements, upgrade_sql = _capture(
        monkeypatch,
        "upgrade",
    )
    _migration, downgrade_statements, downgrade_sql = _capture(
        monkeypatch,
        "downgrade",
    )

    assert migration._CONTRACT in upgrade_sql
    assert "provider_directory_subset_terminal_v4_adoption_blocked" in upgrade_sql
    assert "provider_directory_subset_terminal_v4_downgrade_blocked" in (
        downgrade_sql
    )
    assert "DROP FUNCTION" in downgrade_sql
    assert migration._DIRECT_VALID in downgrade_sql
    assert not any("DROP TRIGGER" in value for value in downgrade_statements)
    assert downgrade_sql.count("CREATE OR REPLACE FUNCTION") == 2


def test_helper_is_owner_only_and_object_identity_is_preserved(monkeypatch):
    """Fence helper ACLs and preserve all pre-existing function/trigger OIDs."""

    migration, _statements, normalized_sql = _capture(monkeypatch, "upgrade")

    assert "ALTER FUNCTION %I.%I(text) OWNER TO %I" in normalized_sql
    assert "REVOKE ALL ON FUNCTION %I.%I(text) FROM PUBLIC" in normalized_sql
    assert "function_acl.grantee <> helper.proowner" in normalized_sql
    assert "provider_directory_subset_terminal_v4_identity_changed" in normalized_sql
    assert migration._DATASET_CONSTRAINT in normalized_sql
    assert migration._CHECKPOINT_CONSTRAINT in normalized_sql
