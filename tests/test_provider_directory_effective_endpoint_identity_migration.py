# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""DDL contract for configured Provider Directory endpoint identity."""

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260809010000_provider_directory_effective_endpoint_identity.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_effective_endpoint_identity_migration",
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


def _normalized(value: str) -> str:
    return " ".join(value.split())


def _capture(monkeypatch, operation):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "effective_endpoint_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, operation)()
    normalized_statements = [
        _normalized(statement) for statement in recorder.statements
    ]
    return migration, recorder, normalized_statements


def _assert_replaced_function_contract(statements):
    replacements = [
        statement
        for statement in statements
        if statement.startswith("CREATE OR REPLACE FUNCTION")
    ]
    assert len(replacements) == 4
    assert {
        replacement.split("(", 1)[0].rsplit(".", 1)[-1].strip('"')
        for replacement in replacements
    } == {
        "guard_tin_npi_connector_endpoint_dataset",
        "guard_provider_directory_subset_published_source",
        "provider_directory_reviewed_subset_activation_valid",
        "guard_provider_directory_reviewed_subset_activation_source",
    }
    assert all("SECURITY DEFINER" in replacement for replacement in replacements)
    assert all(
        "SET search_path = pg_catalog" in replacement
        for replacement in replacements
    )
    assert not any(
        "guard_provider_directory_reviewed_subset_activation_dataset" in replacement
        for replacement in replacements
    )
    assert not any(
        "provider_directory_subset_abandonment" in replacement
        for replacement in replacements
    )


def _assert_no_recreation_or_evidence_dml(statements):
    top_level_statements = [statement.upper() for statement in statements]
    assert not any(
        statement.startswith(
            (
                "CREATE TRIGGER",
                "CREATE CONSTRAINT TRIGGER",
                "DROP TRIGGER",
                "CREATE FUNCTION",
                "DROP FUNCTION",
                "ALTER TABLE",
            )
        )
        for statement in top_level_statements
    )
    assert not any(
        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in top_level_statements
    )
    revokes = [
        statement
        for statement in statements
        if statement.startswith("REVOKE ALL ON FUNCTION")
    ]
    assert len(revokes) == 4
    assert all(statement.strip().count(";") == 1 for statement in revokes)


def _assert_pre_and_post_shapes(normalized_sql):
    assert (
        "guard_provider_directory_reviewed_subset_activation_dataset"
        in normalized_sql
    )
    expected_twice = (
        "provider_directory_reviewed_subset_activation_trigger_changed",
        "tin_npi_connector_endpoint_dataset_guard_changed",
        "provider_directory_subset_source_guard_function_changed",
        "provider_directory_reviewed_subset_activation_function_changed",
        "provider_directory_subset_abandonment_shape_changed",
    )
    assert all(
        normalized_sql.count(error_code) == 2
        for error_code in expected_twice
    )


def _assert_adoption_fences(normalized_sql):
    assert "provider_directory_effective_endpoint_terminal_adoption_invalid" in (
        normalized_sql
    )
    assert "provider_directory_effective_endpoint_publication_adoption_invalid" in (
        normalized_sql
    )
    assert "provider_directory_effective_endpoint_activation_adoption_invalid" in (
        normalized_sql
    )


def test_upgrade_replaces_only_four_functions_and_preserves_installed_shapes(
    monkeypatch,
):
    """Replace function bodies while retaining every installed trigger shape."""

    migration, recorder, statements = _capture(monkeypatch, "upgrade")
    normalized_sql = " ".join(statements)

    assert migration.revision == (
        "20260809010000_provider_directory_effective_endpoint_identity"
    )
    assert migration.down_revision == (
        "20260809000000_provider_directory_subset_abandonment"
    )
    assert statements[0] == (
        'LOCK TABLE "effective_endpoint_test".'
        '"provider_directory_endpoint_dataset", '
        '"effective_endpoint_test"."provider_directory_dataset_resource", '
        '"effective_endpoint_test"."provider_directory_source" '
        "IN ACCESS EXCLUSIVE MODE;"
    )
    _assert_replaced_function_contract(statements)
    _assert_no_recreation_or_evidence_dml(statements)
    _assert_pre_and_post_shapes(normalized_sql)
    _assert_adoption_fences(normalized_sql)
    assert recorder.statements


def test_corrected_subset_contract_hashes_configured_but_serves_physical_alias():
    migration = _load_migration()
    subset = migration._subset()
    terminal_sql = _normalized(
        subset._subset_source_sql(
            "effective_endpoint_test",
            require_verified=False,
            dataset_alias="terminal_dataset",
            use_configured_endpoint_identity=True,
            require_physical_match=False,
        )
    )
    published_sql = _normalized(
        subset._subset_source_sql(
            "effective_endpoint_test",
            require_verified=True,
            dataset_alias="published_dataset",
            use_configured_endpoint_identity=True,
            require_physical_match=True,
        )
    )

    configured_projection = (
        "'endpoint_id', current_source.metadata_json::jsonb ->> "
        "'provider_directory_configured_endpoint_id'"
    )
    assert configured_projection in terminal_sql
    assert configured_projection in published_sql
    assert "NULLIF(current_source.endpoint_id, '') IS NOT NULL" in terminal_sql
    assert (
        "current_source.endpoint_id = terminal_dataset.endpoint_id"
        not in terminal_sql
    )
    assert (
        "current_source.endpoint_id = published_dataset.endpoint_id"
        in published_sql
    )
    assert (
        "current_source.metadata_json::jsonb ->> "
        "'provider_directory_configured_endpoint_id' = "
        "terminal_dataset.endpoint_id"
    ) in terminal_sql


def test_activation_uses_configured_identity_and_allows_only_final_cutover():
    migration = _load_migration()
    activation = migration._activation()
    validation_sql = _normalized(
        activation._activation_valid_function_sql(
            "effective_endpoint_test",
            use_configured_endpoint_identity=True,
            replace_existing=True,
        )
    )
    source_guard_sql = _normalized(
        activation._source_guard_function_sql(
            "effective_endpoint_test",
            allow_effective_endpoint_cutover=True,
            replace_existing=True,
        )
    )

    assert validation_sql.startswith("CREATE OR REPLACE FUNCTION")
    assert (
        "->> 'endpoint_id' = active_source.metadata_json::jsonb ->> "
        "'provider_directory_configured_endpoint_id'"
    ) in validation_sql
    assert (
        "'endpoint_id', active_source.metadata_json::jsonb ->> "
        "'provider_directory_configured_endpoint_id'"
    ) in validation_sql
    assert "NULLIF(current_source.endpoint_id, '') IS NOT NULL" in validation_sql
    assert "current_source.endpoint_id = candidate.endpoint_id" not in validation_sql

    assert source_guard_sql.startswith("CREATE OR REPLACE FUNCTION")
    assert "NEW.endpoint_id IS NOT DISTINCT FROM OLD.endpoint_id" in source_guard_sql
    assert "NEW.endpoint_id IS DISTINCT FROM OLD.endpoint_id" in source_guard_sql
    assert (
        "to_jsonb(NEW) - ARRAY['endpoint_id', 'updated_at']::text[]"
        in source_guard_sql
    )
    assert "NEW.updated_at IS NOT DISTINCT FROM" in source_guard_sql
    assert "pg_catalog.transaction_timestamp()" in source_guard_sql
    assert (
        "NEW.endpoint_id = new_metadata ->> "
        "'provider_directory_configured_endpoint_id'"
    ) in source_guard_sql
    assert "NEW.endpoint_id = new_metadata ->" in source_guard_sql
    assert "AS activation_candidate" in source_guard_sql
    assert "-> 'candidate' ->> 'dataset_id'" in source_guard_sql
    assert "activation_candidate.endpoint_id = NEW.endpoint_id" in source_guard_sql
    assert "activation_candidate.status = 'published'" in source_guard_sql
    assert "activation_candidate.is_current IS TRUE" in source_guard_sql
    assert "activation_candidate.validated_at IS NOT NULL" in source_guard_sql
    assert "activation_candidate.published_at IS NOT NULL" in source_guard_sql
    assert "activation_candidate.superseded_at IS NULL" in source_guard_sql


def test_historical_generator_defaults_retain_physical_endpoint_contract():
    migration = _load_migration()
    subset = migration._subset()
    activation = migration._activation()

    subset_sql = _normalized(
        subset._subset_source_sql(
            "effective_endpoint_test",
            require_verified=False,
            dataset_alias="legacy_dataset",
        )
    )
    activation_sql = _normalized(
        activation._activation_valid_function_sql("effective_endpoint_test")
    )
    source_guard_sql = _normalized(
        activation._source_guard_function_sql("effective_endpoint_test")
    )

    assert "'endpoint_id', current_source.endpoint_id" in subset_sql
    assert "current_source.endpoint_id = legacy_dataset.endpoint_id" in subset_sql
    assert activation_sql.startswith("CREATE FUNCTION")
    assert "->> 'endpoint_id' = active_source.endpoint_id" in activation_sql
    assert "'endpoint_id', active_source.endpoint_id" in activation_sql
    assert source_guard_sql.startswith("CREATE FUNCTION")
    assert "AS activation_candidate" not in source_guard_sql


def test_downgrade_keeps_configured_identity_bodies(monkeypatch):
    _migration, recorder, statements = _capture(monkeypatch, "downgrade")

    assert statements == []
    assert recorder.statements == []
