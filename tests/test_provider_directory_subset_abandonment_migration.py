# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for reviewed Provider Directory subset abandonment."""

from __future__ import annotations

import importlib.util
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260808220000_provider_directory_subset_abandonment.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_subset_abandonment_migration",
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
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "subset_abandonment_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    normalized_sql = " ".join(
        " ".join(statement.split()) for statement in recorder.statements
    )
    return migration, recorder, normalized_sql


def _abandonment_trigger_names(migration):
    return (
        migration._DATASET_ROW_TRIGGER,
        migration._DATASET_CONSTRAINT,
        migration._DATASET_TRUNCATE,
        migration._RESOURCE_INSERT,
        migration._RESOURCE_UPDATE,
        migration._RESOURCE_DELETE,
        migration._PROOF_INSERT,
        migration._PROOF_UPDATE,
        migration._PROOF_DELETE,
        migration._PROOF_TRUNCATE,
        migration._BULK_INSERT,
        migration._BULK_UPDATE,
        migration._BULK_DELETE,
        migration._BULK_TRUNCATE,
        migration._CHECKPOINT_ROW_TRIGGER,
        migration._CHECKPOINT_CONSTRAINT,
        migration._CHECKPOINT_TRUNCATE,
    )


def test_upgrade_installs_additive_fail_closed_abandonment_guards(monkeypatch):
    """Install only hardened additive abandonment functions and triggers."""

    migration, recorder, normalized_sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260808220000_provider_directory_subset_abandonment"
    )
    assert migration.down_revision == (
        "20260808210000_provider_directory_subset_payload_guard_repair"
    )
    assert normalized_sql.startswith("LOCK TABLE")
    assert normalized_sql.count("CREATE OR REPLACE FUNCTION") == 4
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 4
    assert normalized_sql.count("CREATE TRIGGER") == 15
    assert normalized_sql.count("CREATE CONSTRAINT TRIGGER") == 2
    assert normalized_sql.count("ENABLE ALWAYS TRIGGER") == 17
    assert "SECURITY DEFINER SET search_path = pg_catalog" in normalized_sql
    assert "provider_directory_subset_abandonment_adoption_forbidden" in (
        normalized_sql
    )
    assert "provider_directory_subset_abandonment_valid" in normalized_sql
    assert "server_issued_traversal_subset" in normalized_sql
    assert "pending_two_matching_reviewed_subset_acquisitions" in normalized_sql
    assert "pg_try_advisory_xact_lock" in normalized_sql
    assert "provider-directory-pagination:" in normalized_sql
    assert "FOR SHARE OF dataset" in normalized_sql
    assert "provider_directory_reviewed_subset_activation_v1" in normalized_sql
    assert all(
        len(trigger_name.encode("utf-8")) <= 63
        for trigger_name in _abandonment_trigger_names(migration)
    )
    simple_statements = [
        statement
        for statement in recorder.statements
        if statement.lstrip().startswith(
            (
                "CREATE TRIGGER",
                "CREATE CONSTRAINT TRIGGER",
                "ALTER TABLE",
                "REVOKE ALL",
            )
        )
    ]
    assert len(simple_statements) == 38
    assert all(statement.strip().count(";") == 1 for statement in simple_statements)


def test_validator_binds_closed_marker_and_retained_evidence_counts():
    migration = _load_migration()
    validation_sql = " ".join(
        migration._valid_function_sql("subset_abandonment_test").split()
    )

    for marker_field in (
        "contract_version",
        "reason_code",
        "source_scope_sha256",
        "resource_types",
        "terminal_error_codes",
        "checkpoint_count",
        "pages_processed",
        "rows_processed",
        "resource_count",
        "proof_shard_count",
        "proof_row_count",
    ):
        assert marker_field in validation_sql
    assert "acquisition_abandoned" in validation_sql
    assert "completion_proof_required_version = 3" in validation_sql
    assert "completion_proof_json IS NULL" in validation_sql
    assert "completion_proof_sha256 IS NULL" in validation_sql
    assert "provider_directory_bulk_acquisition_checkpoint" in validation_sql
    assert "jsonb_each_text" in validation_sql
    assert "IS DISTINCT FROM checkpoint.rows_processed" in validation_sql
    assert "COALESCE(proof_summary.proof_row_count, 0)" in validation_sql


def test_downgrade_blocks_sealed_evidence_and_drops_only_new_objects(
    monkeypatch,
):
    _migration, recorder, normalized_sql = _capture(monkeypatch, "downgrade")

    assert "provider_directory_subset_abandonment_downgrade_blocked" in (normalized_sql)
    assert normalized_sql.count("DROP TRIGGER") == 17
    assert normalized_sql.count("DROP FUNCTION") == 4
    assert "provider_directory_subset_payload_guard_repair" not in (
        " ".join(
            statement
            for statement in recorder.statements
            if statement.lstrip().startswith(("DROP TRIGGER", "DROP FUNCTION"))
        )
    )
