# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DDL contract for the exact reviewed v5 HTTP-410 disposition."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions"
    / "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_v5_http410_disposition_migration",
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
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "v5_http410_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    direct = migration._direct()
    terminal = direct._terminal()
    monkeypatch.setattr(direct, "op", recorder)
    monkeypatch.setattr(terminal, "op", recorder)
    getattr(migration, operation)()
    normalized_statements = [
        " ".join(statement.split()) for statement in recorder.statements
    ]
    return migration, normalized_statements, " ".join(normalized_statements)


def test_upgrade_is_linear_additive_and_bounded(monkeypatch):
    migration, statements, sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition"
    )
    assert migration.down_revision == "20260811110000_address_formatted_display"
    assert "FOR attempt IN 1..150 LOOP" in statements[0]
    assert "ACCESS EXCLUSIVE MODE NOWAIT" in statements[0]
    assert "pg_sleep(0.2)" in statements[0]
    for relation_name in (
        "provider_directory_endpoint_dataset",
        "provider_directory_dataset_resource",
        "provider_directory_source",
        "provider_directory_dataset_proof_shard",
        "provider_directory_pagination_checkpoint",
        "provider_directory_bulk_acquisition_checkpoint",
        "import_run",
    ):
        assert statements[0].count(relation_name) == 1
    assert sql.count("CREATE FUNCTION") == 1
    assert sql.count("CREATE OR REPLACE FUNCTION") == 2
    assert "CREATE CONSTRAINT TRIGGER" not in sql
    assert "ALTER TABLE" not in sql
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )


def test_v3_validator_binds_the_exact_retained_packet():
    migration = _load_migration()
    validation_sql = " ".join(migration._v5_valid_sql("v5_test").split())

    for literal in (
        migration._CONTRACT,
        migration._REASON,
        migration._CAMPAIGN,
        migration._MARKER_SHA256,
        migration._HELPER,
        "terminal_http_410",
        "verified_complete",
        "candidate_metadata_sha256",
        "source_diagnostics_sha256",
        "checkpoint_retry_count",
        "previous_reference_count",
        "provider_directory_dataset_proof_shard",
        "provider_directory_bulk_acquisition_checkpoint",
    ):
        assert literal in validation_sql
    assert "terminal_census_drift" not in validation_sql
    assert "terminal_page_entry_count" not in validation_sql
    assert validation_sql.count("IS DISTINCT FROM 'null'::jsonb") == 3
    assert "ARRAY['HealthcareService']::text[]" in validation_sql
    assert "page_delta' <> '1'::jsonb" not in validation_sql
    assert "page_delta' <> '0'::jsonb" in validation_sql
    assert "EXCEPTION WHEN OTHERS THEN RETURN FALSE" in validation_sql


def test_transition_binds_terminal_window_source_and_failed_root():
    migration = _load_migration()
    transition_sql = " ".join(migration._v5_transition_sql("v5_test").split())

    for literal in (
        "OLD.status <> 'failed'",
        "NEW.status <> 'acquisition_abandoned'",
        migration._CONTRACT,
        migration._CAMPAIGN,
        migration._MARKER_SHA256,
        "provider-directory-fhir-server-issued-traversal-subset-v5",
        "terminal-logical-window-covers-advertised-pre",
        "provider_directory_current_version_census_start_urls",
        "provider_directory_subset_canonical_sha256",
        "provider_directory_configured_endpoint_id",
        "last_resource_import",
        "source_import_sha256",
        "retry_of_run_id",
        "NEW.resource_count IS DISTINCT FROM OLD.resource_count",
    ):
        assert literal in transition_sql
    assert "provider_directory_subset_terminal_v4_transition_invalid" not in (
        transition_sql
    )


def test_shared_dispatch_preserves_v2_and_v1_order():
    migration = _load_migration()
    direct = migration._direct()
    shared_sql = " ".join(migration._shared_valid_sql("v5_test").split())
    guard_sql = " ".join(migration._dataset_guard_sql("v5_test").split())

    assert shared_sql.index(migration._CONTRACT) < shared_sql.index(direct._CONTRACT)
    assert shared_sql.index(direct._CONTRACT) < shared_sql.index(
        "healthporta.provider-directory.reviewed-subset-terminal-disposition.v1"
    )
    assert guard_sql.index(migration._CONTRACT) < guard_sql.index(direct._CONTRACT)
    assert migration._v5_transition_sql("v5_test").strip() in (
        migration._dataset_guard_sql("v5_test")
    )


def test_upgrade_fences_helper_acl_bodies_and_object_identity(monkeypatch):
    migration, _statements, sql = _capture(monkeypatch, "upgrade")

    for literal in (
        "CREATE TEMP TABLE provider_directory_terminal_v5_identity_snapshot",
        "provider_directory_v5_http410_shape_changed",
        "provider_directory_v5_http410_identity_changed",
        "ALTER FUNCTION %I.%I(text) OWNER TO %I",
        "REVOKE ALL ON FUNCTION %I.%I(text) FROM PUBLIC",
        "search_path=pg_catalog",
        "helper.proowner = shared.proowner",
        "helper_acl.grantee <> helper.proowner",
        "trigger_row.tgfoid",
    ):
        assert literal in sql
    assert sql.count(migration._HELPER) >= 6
    assert "provider_directory_v5_http410_adoption_blocked" in sql


def test_clean_downgrade_blocks_v3_then_restores_exact_v2(monkeypatch):
    migration, statements, sql = _capture(monkeypatch, "downgrade")
    direct = migration._direct()

    assert "provider_directory_v5_http410_downgrade_blocked" in sql
    assert any(
        statement == " ".join(direct._shared_valid_sql("v5_http410_test").split())
        for statement in statements
    )
    assert any(
        statement == " ".join(direct._dataset_guard_sql("v5_http410_test").split())
        for statement in statements
    )
    assert f'DROP FUNCTION "v5_http410_test"."{migration._HELPER}"(text);' in (
        statements
    )
    assert "provider_directory_v5_http410_shape_changed" in sql
    assert "provider_directory_subset_terminal_v4_shape_changed" in sql


def test_predecessor_renderers_are_not_modified():
    migration = _load_migration()
    direct = migration._direct()
    schema = "v5_test"

    assert migration._direct()._shared_valid_sql(schema) == direct._shared_valid_sql(
        schema
    )
    assert migration._direct()._dataset_guard_sql(schema) == direct._dataset_guard_sql(
        schema
    )
    assert migration._direct()._direct_valid_sql(schema) == direct._direct_valid_sql(
        schema
    )
