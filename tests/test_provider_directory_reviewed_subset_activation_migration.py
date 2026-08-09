# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contracts for explicit reviewed subset activation."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260808200000_provider_directory_reviewed_subset_activation.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_subset_activation_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _Recorder:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(str(statement))


def _capture(monkeypatch, operation):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "subset_activation_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    normalized_sql = " ".join(
        " ".join(statement.split()) for statement in recorder.statements
    )
    return migration, recorder, normalized_sql


def _assert_upgrade_identity(migration, normalized_sql):
    """Assert the activation revision and object installation identity."""

    assert migration.revision == (
        "20260808200000_provider_directory_reviewed_subset_activation"
    )
    assert migration.down_revision == (
        "20260808190000_provider_directory_subset_completion_proof"
    )
    assert normalized_sql.startswith("LOCK TABLE")
    assert migration._ACTIVATION_VALID_FUNCTION in normalized_sql
    assert migration._SOURCE_GUARD_FUNCTION in normalized_sql
    assert migration._DATASET_GUARD_FUNCTION in normalized_sql
    assert migration._SOURCE_GUARD_TRIGGER in normalized_sql
    assert migration._SOURCE_TRUNCATE_TRIGGER in normalized_sql
    assert migration._DATASET_GUARD_TRIGGER in normalized_sql
    assert migration._DATASET_TRUNCATE_TRIGGER in normalized_sql


def _assert_upgrade_guard_contract(migration, normalized_sql):
    """Assert the installed guards retain the required trust boundaries."""

    assert all(
        len(trigger_name.encode("utf-8")) <= 63
        for trigger_name in (
            migration._SOURCE_GUARD_TRIGGER,
            migration._SOURCE_TRUNCATE_TRIGGER,
            migration._DATASET_GUARD_TRIGGER,
            migration._DATASET_TRUNCATE_TRIGGER,
        )
    )
    assert "DEFERRABLE INITIALLY DEFERRED" in normalized_sql
    assert normalized_sql.count("ENABLE ALWAYS TRIGGER") == 4
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 3
    assert "provider_directory_reviewed_subset_activation_isolation_invalid" in (
        normalized_sql
    )
    assert "pg_try_advisory_xact_lock" in normalized_sql
    assert "LOCK TABLE" in normalized_sql
    assert "IN SHARE MODE" in normalized_sql
    assert "FOR UPDATE OF active_source" in normalized_sql
    assert "affected_active_sources AS MATERIALIZED" in normalized_sql
    assert "affected_source_ids" in normalized_sql
    assert "affected_endpoint_ids" in normalized_sql
    assert "provider_directory_reviewed_subset_activation_source_invalid" in (
        normalized_sql
    )
    assert "provider_directory_reviewed_subset_activation_dataset_invalid" in (
        normalized_sql
    )
    assert "verification_source_scope_sha256" in normalized_sql
    assert "IS NOT DISTINCT FROM" in normalized_sql
    assert "provider-directory-fhir-reviewed-subset-source-contract-v1" in (
        normalized_sql
    )
    assert "server_issued_subset_replay_evidence_sha256" in normalized_sql
    assert "server_issued_subset_coverage" in normalized_sql
    assert "verification_baseline" in normalized_sql
    assert "verification_mismatch" in normalized_sql
    assert "observed_columns IS DISTINCT FROM" in normalized_sql


def _assert_upgrade_simple_ddl(recorder):
    """Assert asyncpg receives every simple DDL command separately."""

    simple_ddl_statements = [
        statement
        for statement in recorder.statements
        if statement.lstrip().startswith(
            ("CREATE CONSTRAINT TRIGGER", "CREATE TRIGGER", "ALTER TABLE", "REVOKE ALL")
        )
    ]
    assert len(simple_ddl_statements) == 11
    assert all(
        statement.strip().count(";") == 1
        for statement in simple_ddl_statements
    )


def test_upgrade_installs_hardened_activation_guards(monkeypatch):
    """Install the complete hardened activation contract without DDL bundles."""

    migration, recorder, normalized_sql = _capture(monkeypatch, "upgrade")

    _assert_upgrade_identity(migration, normalized_sql)
    _assert_upgrade_guard_contract(migration, normalized_sql)
    _assert_upgrade_simple_ddl(recorder)


def test_activation_validator_binds_closed_marker_source_and_exact_twins():
    migration = _load_migration()
    validation_sql = " ".join(
        migration._activation_valid_function_sql("subset_activation_test").split()
    )

    assert "contract_version" in validation_sql
    assert "source_contract_sha256" in validation_sql
    assert "completion_proof_sha256" in validation_sql
    assert "verification_source_scope_sha256" in validation_sql
    assert "baseline_acquisition_root_run_id" in validation_sql
    assert "mismatch_fields" in validation_sql
    assert "acquisition_root_run_id')" in validation_sql
    assert "provider_directory_subset_canonical_sha256" in validation_sql
    assert "provider_directory_configured_endpoint_id" in validation_sql
    assert "provider_directory_manual_only" in validation_sql
    assert "provider_directory_acquisition_enabled" in validation_sql
    assert "pg_catalog.count(*)" in validation_sql
    assert ") = 2" in validation_sql
    assert "candidate.status = 'superseded'" in validation_sql
    assert "candidate.superseded_at IS NOT NULL" in validation_sql


def test_downgrade_is_fail_closed_and_leaves_predecessor_shapes(monkeypatch):
    migration, recorder, normalized_sql = _capture(monkeypatch, "downgrade")

    assert "provider_directory_reviewed_subset_activation_downgrade_blocked" in (
        normalized_sql
    )
    assert normalized_sql.count("DROP TRIGGER") == 4
    assert normalized_sql.count("DROP FUNCTION") == 3
    assert "provider_directory_subset_published_source_guard" in normalized_sql
    assert "provider_directory_subset_completion_proof_pair_valid" in (
        normalized_sql
    )
    assert normalized_sql.endswith("$migration$;")
    dropped_ddl_statements = [
        statement
        for statement in recorder.statements
        if statement.lstrip().startswith(("DROP TRIGGER", "DROP FUNCTION"))
    ]
    assert len(dropped_ddl_statements) == 7
    assert all(
        statement.strip().count(";") == 1
        for statement in dropped_ddl_statements
    )
