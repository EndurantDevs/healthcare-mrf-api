# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lifecycle, cutover, and cleanup contracts for the connector migration."""

from __future__ import annotations

import pytest

from tests.tin_npi_connector_migration_support import (
    capture_upgrade,
    load_connector_migration,
)


def test_pointer_cutover_is_monotonic_cas_with_source_relation_fence(monkeypatch):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    _assert_pointer_cas_contract(normalized_sql)
    _assert_source_fence_contract(normalized_sql)
    _assert_token_policy_fence(normalized_sql)
    assert "healthporta.tin_npi_pointer_generation_key" in normalized_sql
    assert "tin_npi_connector_pointer_action_invalid" in normalized_sql
    assert "REVOKE ALL ON FUNCTION" in normalized_sql
    assert "FROM PUBLIC" in normalized_sql


def _assert_pointer_cas_contract(normalized_sql):
    assert (
        "pointer_key, pointer_version, generation_key, published_at "
        ") VALUES (1, 0, NULL, NULL)"
    ) in normalized_sql
    assert "NEW.pointer_version <> OLD.pointer_version + 1" in normalized_sql
    assert "tin_npi_connector_pointer_cas_conflict" in normalized_sql
    assert (
        "generation_key IS NOT DISTINCT FROM expected_generation_key"
    ) in normalized_sql
    assert "expected_source_vector_id" in normalized_sql
    assert "assert_tin_npi_connector_source_fence" in normalized_sql
    source_fence_call = (
        'PERFORM "tin_connector"."assert_tin_npi_connector_source_fence"'
        "(target_generation_key)"
    )
    assert normalized_sql.count(source_fence_call) == 1
    assert normalized_sql.index(source_fence_call) < normalized_sql.index(
        'CREATE FUNCTION "tin_connector"."rollback_tin_npi_connector_generation"'
    )


def _assert_source_fence_contract(normalized_sql):
    relation_names = (
        "provider_directory_api_endpoint",
        "provider_directory_source",
        "provider_directory_endpoint_dataset",
    )
    for relation_name in relation_names:
        assert relation_name in normalized_sql
    endpoint_lock = (
        'LOCK TABLE "tin_connector"."provider_directory_api_endpoint" '
        "IN EXCLUSIVE MODE"
    )
    source_lock = (
        'LOCK TABLE "tin_connector"."provider_directory_source" IN EXCLUSIVE MODE'
    )
    dataset_lock = (
        'LOCK TABLE "tin_connector"."provider_directory_endpoint_dataset" '
        "IN EXCLUSIVE MODE"
    )
    relation_lock = "LOCK TABLE %I.%I IN ACCESS SHARE MODE"
    assert normalized_sql.index(endpoint_lock) < normalized_sql.index(source_lock)
    assert normalized_sql.index(source_lock) < normalized_sql.index(dataset_lock)
    assert normalized_sql.index(dataset_lock) < normalized_sql.index(relation_lock)
    fence_markers = (
        "tin_npi_connector_fhir_source_scope_changed",
        "tin_npi_connector_fhir_dataset_changed",
        "tin_npi_connector_fhir_current_dataset_changed",
        "source_summary_v1",
        "healthporta.provider-directory.source-summary.v1",
        "organization_resources",
        "tin_npi_connector_source_relation_changed",
    )
    for fence_marker in fence_markers:
        assert fence_marker in normalized_sql


def _assert_token_policy_fence(normalized_sql):
    assert "ptg2_provider_tax_identity_manifest" in normalized_sql
    assert "all-retained-ptg-tax-policy-descriptors.v1" in normalized_sql
    assert "tin_npi_connector_token_policy_scope_changed" in normalized_sql
    assert (
        'CREATE FUNCTION "tin_connector".'
        '"assert_tin_npi_connector_token_policy_fence"( '
        "target_generation_key bigint, require_exact_scope boolean )"
    ) in normalized_sql
    assert (
        'PERFORM "tin_connector"."assert_tin_npi_connector_token_policy_fence"'
        "( target_generation_key, FALSE )"
    ) in normalized_sql


def test_live_rows_are_immutable_truncate_guarded_and_gc_is_batched(monkeypatch):
    _, sql_statements, normalized_sql = capture_upgrade(monkeypatch)

    truncate_triggers = [
        statement
        for statement in sql_statements
        if "_truncate_guard" in statement
        and "guard_tin_npi_connector_truncate" in statement
        and "CREATE TRIGGER" in statement
    ]
    assert len(truncate_triggers) == 7
    lifecycle_markers = (
        "tin_npi_connector_truncate_forbidden",
        "tin_npi_connector_child_immutable",
        "generation_state IN ('failed', 'retired')",
        "healthporta.tin_npi_gc_generation_key",
        "current_user <> generation_owner",
        "OLD.gc_after > clock_timestamp()",
        "OLD.build_lease_expires_at > clock_timestamp()",
        "target_state NOT IN ('failed', 'retired')",
        "target_state = 'retired'",
        "target_state = 'failed'",
        "tin_npi_connector_generation_not_collectable",
    )
    for lifecycle_marker in lifecycle_markers:
        assert lifecycle_marker in normalized_sql
    assert normalized_sql.count("FOR UPDATE SKIP LOCKED") == 2
    assert normalized_sql.count("LIMIT batch_size") == 2
    assert (
        "IF batch_size IS NULL OR batch_size < 1 OR batch_size > 100000"
    ) in normalized_sql
    _assert_evidence_first_gc(normalized_sql)


def _assert_evidence_first_gc(normalized_sql):
    evidence_delete = (
        'DELETE FROM "tin_connector"."tin_npi_connector_evidence" WHERE ctid IN'
    )
    lookup_delete = (
        'DELETE FROM "tin_connector"."tin_npi_connector_lookup" WHERE ctid IN'
    )
    assert evidence_delete in normalized_sql
    assert lookup_delete in normalized_sql
    assert normalized_sql.index(evidence_delete) < normalized_sql.index(lookup_delete)
    assert (
        "IF remaining_evidence_rows THEN deleted_lookup_rows := 0; "
        "generation_removed := FALSE"
    ) in normalized_sql


def test_published_dataset_resources_are_database_immutable(monkeypatch):
    _, sql_statements, normalized_sql = capture_upgrade(monkeypatch)

    resource_triggers = [
        " ".join(statement.split())
        for statement in sql_statements
        if "tin_npi_connector_dataset_resource_" in statement
        and "CREATE TRIGGER" in statement
    ]
    assert len(resource_triggers) == 4
    assert any("AFTER INSERT" in statement for statement in resource_triggers)
    assert any("AFTER UPDATE" in statement for statement in resource_triggers)
    assert any("AFTER DELETE" in statement for statement in resource_triggers)
    assert any("BEFORE TRUNCATE" in statement for statement in resource_triggers)
    guard_markers = (
        "REFERENCING NEW TABLE AS new_rows",
        "REFERENCING OLD TABLE AS old_rows",
        "tin_npi_connector_dataset_resource_parent_immutable",
        "FOR SHARE OF dataset",
        "tin_npi_connector_endpoint_dataset_transition_invalid",
        "tin_npi_connector_endpoint_dataset_delete_forbidden",
        "tin_npi_connector_endpoint_dataset_guard_changed",
        "trigger_row.tgenabled = 'A'",
        "trigger_row.tgtype = expected.trigger_type",
        "trigger_row.tgtype = 31",
        "trigger_row.tgattr = ''::int2vector",
        "trigger_row.tgoldtable IS NOT DISTINCT FROM",
        "tin_npi_connector_dataset_resource_guard_changed",
    )
    for guard_marker in guard_markers:
        assert guard_marker in normalized_sql
    assert normalized_sql.count("ENABLE ALWAYS TRIGGER") == 5
    assert (
        "BEFORE INSERT OR UPDATE OR DELETE ON "
        '"tin_connector"."provider_directory_endpoint_dataset"'
    ) in normalized_sql
    assert (
        'LOCK TABLE "tin_connector"."provider_directory_dataset_resource" '
        "IN SHARE MODE"
    ) in normalized_sql
    _assert_guard_functions_revoked(normalized_sql)


def _assert_guard_functions_revoked(normalized_sql):
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"guard_tin_npi_connector_dataset_resource" FROM PUBLIC'
    ) in normalized_sql
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"guard_tin_npi_connector_endpoint_dataset" FROM PUBLIC'
    ) in normalized_sql


def test_expired_builds_have_owner_only_abandonment_and_batched_recovery(
    monkeypatch,
):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    abandonment_markers = (
        '"abandon_tin_npi_connector_generation"',
        "SECURITY DEFINER SET search_path = pg_catalog",
        "healthporta.tin_npi_abandon_generation_key",
        "OLD.build_lease_expires_at <= clock_timestamp()",
        "current_user = generation_owner",
        "target_build_lease_expires_at > clock_timestamp()",
        "tin_npi_connector_generation_not_abandonable",
        '"retire_tin_npi_connector_generation"( '
        "target_generation_key bigint, retain_until timestamptz )",
        "healthporta.tin_npi_retire_generation_key",
        "retain_until < clock_timestamp() + interval '24 hours'",
        "tin_npi_connector_generation_not_retirable",
    )
    for abandonment_marker in abandonment_markers:
        assert abandonment_marker in normalized_sql
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"abandon_tin_npi_connector_generation" FROM PUBLIC'
    ) in normalized_sql
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"retire_tin_npi_connector_generation" FROM PUBLIC'
    ) in normalized_sql


def test_connector_downgrade_refuses_data_then_drops_in_order(monkeypatch):
    migration = load_connector_migration()
    sql_statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tin_connector")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", sql_statements.append)

    migration.downgrade()

    assert "tin_npi_connector_downgrade_requires_empty_inactive_foundation" in (
        sql_statements[0]
    )
    assert "tin_npi_connector_token_policy" in sql_statements[0]
    assert "tin_npi_connector_identifier_policy" in sql_statements[0]
    _assert_downgrade_trigger_order(sql_statements)
    _assert_downgrade_table_order(sql_statements)
    function_drops = sql_statements[13:]
    assert all(
        statement.startswith('DROP FUNCTION IF EXISTS "tin_connector".')
        for statement in function_drops
    )
    assert function_drops[-1].endswith('"tin_npi_connector_valid_npi"(bigint);')


def _assert_downgrade_trigger_order(sql_statements):
    trigger_suffixes = ("insert", "update", "delete", "truncate")
    expected_triggers = [
        (
            f'DROP TRIGGER IF EXISTS "tin_npi_connector_dataset_resource_'
            f'{trigger_suffix}_guard" ON "tin_connector".'
            '"provider_directory_dataset_resource";'
        )
        for trigger_suffix in trigger_suffixes
    ]
    assert sql_statements[1:5] == expected_triggers
    assert sql_statements[5] == (
        'DROP TRIGGER IF EXISTS "tin_npi_connector_endpoint_dataset_guard" '
        'ON "tin_connector"."provider_directory_endpoint_dataset";'
    )


def _assert_downgrade_table_order(sql_statements):
    expected_table_drops = [
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_current";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_evidence";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_lookup";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_generation_policy";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_generation";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_identifier_policy";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_token_policy";',
    ]
    assert sql_statements[6:13] == expected_table_drops


def test_connector_schema_env_alias_conflict_fails_closed(monkeypatch):
    migration = load_connector_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="DB_SCHEMA and HLTHPRT_DB_SCHEMA"):
        migration.upgrade()
