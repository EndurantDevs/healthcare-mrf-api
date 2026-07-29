# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260729110000_tin_npi_connector.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "tin_npi_connector_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _upgrade_statements(monkeypatch) -> tuple[object, list[str], str]:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tin_connector")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    normalized = " ".join(" ".join(statements).split())
    return migration, statements, normalized


def test_connector_migration_descends_from_current_merged_head(monkeypatch):
    migration, statements, normalized = _upgrade_statements(monkeypatch)

    assert migration.revision == "20260729110000_tin_npi_connector"
    assert migration.down_revision == "20260729100000_ptg2_candidate_audit_hold"
    assert len(statements) == 86
    assert normalized.count('CREATE TABLE "tin_connector".') == 7
    for table_name in (
        "tin_npi_connector_token_policy",
        "tin_npi_connector_identifier_policy",
        "tin_npi_connector_generation",
        "tin_npi_connector_generation_policy",
        "tin_npi_connector_lookup",
        "tin_npi_connector_evidence",
        "tin_npi_connector_current",
    ):
        assert f'CREATE TABLE "tin_connector"."{table_name}"' in normalized


def test_connector_migration_is_token_only_and_snapshot_neutral(monkeypatch):
    _, _, normalized = _upgrade_statements(monkeypatch)

    for forbidden in (
        "snapshot_key",
        "tin_key",
        "tin_value",
        "business_name",
        "raw_tin",
        "address_site_key",
        "premise_key",
        "PARTITION BY",
        "ATTACH PARTITION",
        "DETACH PARTITION",
    ):
        assert forbidden not in normalized
    assert "token_only_v1" in normalized
    assert "same_organization_identifier" in normalized
    assert "healthporta.tin-npi.site-by-current-entity-address-unified.v1" in (
        normalized
    )
    assert "tin_npi_connector_reverse_lookup" not in normalized
    assert "tin_npi_connector_site" not in normalized


def test_policy_registries_authenticate_descriptors_and_manifest_keys(
    monkeypatch,
):
    _, _, normalized = _upgrade_statements(monkeypatch)

    assert "PTG2V4TINPOLICY" in normalized
    assert "ein_ascii_digits_or_2_7_hyphen_v1" in normalized
    assert "hmac_sha256_ptg_tin_v1" in normalized
    assert "tin_hmac_sha256_full_32_bytes_authoritative" in normalized
    assert (
        "token_policy_descriptor_sha256 = "
        '"tin_connector"."tin_npi_connector_token_policy_descriptor_sha256"'
        "(token_policy_id)"
    ) in normalized
    assert "healthporta.tin-npi.fhir-identifier-policy.v2" in normalized
    assert "healthporta.tin-npi.fhir-identifier-rule.v1" in normalized
    assert (
        'CREATE FUNCTION "tin_connector".'
        '"tin_npi_connector_identifier_rule_sha256"(candidate_rule jsonb)'
    ) in normalized
    assert "key_count <> 2" in normalized
    assert "key_count <> 10" in normalized
    for identifier_rule_key in (
        "endpoint_id",
        "ein_systems",
        "ein_type_codings",
        "excluded_identifier_uses",
        "identifier_rule_sha256",
        "npi_systems",
        "npi_type_codings",
        "period_policy_id",
        "rule_id",
        "source_id",
    ):
        assert f"'{identifier_rule_key}'" in normalized
    assert (
        "FOREIGN KEY ( identifier_policy_id, identifier_policy_sha256 ) "
        'REFERENCES "tin_connector"."tin_npi_connector_identifier_policy"'
    ) in normalized
    assert "key_count <> 15" in normalized
    for allowed_top_level_key in (
        "fhir_datasets",
        "input_relations",
        "lookup_contract_id",
        "lookup_schema_version",
        "source_scope_contract_id",
        "token_policies",
        "token_policy_scope_contract_id",
        "token_policy_ids",
    ):
        assert f"'{allowed_top_level_key}'" in normalized


def test_fhir_source_vector_is_current_published_only(monkeypatch):
    _, _, normalized = _upgrade_statements(monkeypatch)

    assert "item ->> 'status' <> 'published'" in normalized
    assert "item -> 'is_current' <> 'true'::jsonb" in normalized
    assert "item -> 'promote_on_cutover' <> 'false'::jsonb" in normalized
    assert (
        "jsonb_typeof( item -> 'expected_incumbent_dataset_id' ) <> 'null'"
    ) in normalized
    assert (
        "jsonb_typeof( item -> 'recorded_expected_resources' ) <> 'array'"
    ) in normalized
    assert "COUNT( DISTINCT value ->> 'source_id' )" in normalized
    assert (
        "COUNT( DISTINCT dataset - 'source_id' - 'identifier_rule_id' "
        "- 'identifier_rule_sha256' ) <> 1"
    ) in normalized
    assert "item ->> 'identifier_rule_id'" in normalized
    assert "item ->> 'identifier_rule_sha256'" in normalized
    assert "all-current-published-organization-sources.v1" in normalized
    assert "(item -> 'selected_resources') @> '[\"Organization\"]'::jsonb" in (
        normalized
    )
    assert "organization_resource_count" in normalized
    assert "organization_resource_sha256" in normalized
    assert "source_summary_sha256" in normalized


def test_generation_digests_are_recomputed_from_canonical_content(monkeypatch):
    _, _, normalized = _upgrade_statements(monkeypatch)

    assert "UNIQUE (source_vector_id)" in normalized
    assert "healthporta.tin-npi.source-vector.v1" in normalized
    assert "healthporta.tin-npi.source-ordinal-map.v1" in normalized
    assert "healthporta.tin-npi.lookup-row.v3" in normalized
    assert "healthporta.tin-npi.lookup-bucket.v1" in normalized
    assert "healthporta.tin-npi.lookup-set.v4" in normalized
    assert "healthporta.tin-npi.generation.v3" in normalized
    assert "healthporta.tin-npi.fhir-organization-scan-proof.v2" in normalized
    assert "healthporta.tin-npi.fhir-evidence.v2" in normalized
    assert "healthporta.tin-npi.fhir-evidence-set.v1" in normalized
    assert (
        'calculated_lookup_digest := "tin_connector".'
        '"tin_npi_connector_lookup_set_sha256"(NEW.generation_key)'
    ) in normalized
    assert "calculated_lookup_digest <> NEW.lookup_digest" in normalized
    assert "identifier_rule_difference_count <> 0" in normalized
    assert "invalid_evidence_count <> 0" in normalized
    assert "invalid_evidence_record_count <> 0" in normalized
    assert "distinct_policy_count <> NEW.token_policy_count" in normalized
    assert "distinct_npi_set_count <> 1" in normalized
    assert "distinct_payload_count <> 1" in normalized
    assert (
        "observed_matched_record_count <> NEW.matched_organization_count" in normalized
    )
    assert "evidence_projection_difference_count <> 0" in normalized
    assert "evidence_scan_digest_difference_count <> 0" in normalized
    assert "observed_audit_evidence_count <> NEW.evidence_count" in normalized
    assert (
        "dataset_proof ->> 'matched_evidence_sha256', 'hex' ) <> "
        '"tin_connector"."tin_npi_connector_evidence_set_sha256"'
        "( NEW.generation_key, source_scope.source_ordinal )"
    ) in normalized
    assert "source_membership_difference_count <> 0" in normalized
    assert "invalid_policy_count <> 0" in normalized
    assert "unused_policy_count <> 0" in normalized
    assert "source_policy_evidence_difference_count <> 0" in normalized


def test_lookup_is_compact_nonpartitioned_and_preserves_prefix_collisions(
    monkeypatch,
):
    _, _, normalized = _upgrade_statements(monkeypatch)

    assert "generation_key bigint GENERATED ALWAYS AS IDENTITY" in normalized
    assert (
        "PRIMARY KEY ( generation_key, token_policy_id, tin_id_128, "
        "tin_hmac_sha256 )"
    ) in normalized
    assert (
        "FOREIGN KEY (generation_key, token_policy_id) REFERENCES "
        '"tin_connector"."tin_npi_connector_generation_policy" '
        "( generation_key, token_policy_id ) ON DELETE CASCADE"
    ) in normalized
    assert "npis bigint[] NOT NULL" in normalized
    assert "source_bitmap bytea NOT NULL" in normalized
    assert "npi_source_bitmap_matrix bytea NOT NULL" in normalized
    assert "source_evidence_counts bigint[] NOT NULL" in normalized
    assert "tin_npi_connector_valid_source_evidence" in normalized
    assert "tin_id_128 = substring(tin_hmac_sha256 FROM 1 FOR 16)" in normalized
    assert "octet_length(tin_id_128) = 16" in normalized
    assert "octet_length(tin_hmac_sha256) = 32" in normalized
    assert (
        "octet_length(candidate_npi_source_bitmap_matrix) "
        "<> npi_count * bitmap_width"
    ) in normalized
    assert "source_evidence_count < npi_support_count" in normalized
    assert (
        "octet_length(npi_source_bitmap_matrix) "
        "<> cardinality(npis) * ((NEW.source_count + 7) / 8)"
    ) in normalized
    assert "PARTITION BY" not in normalized
    assert "USING gin" not in normalized.lower()


def test_evidence_rows_are_token_only_rule_bound_and_content_authenticated(
    monkeypatch,
):
    _, _, normalized = _upgrade_statements(monkeypatch)

    assert ('CREATE TABLE "tin_connector"."tin_npi_connector_evidence"') in normalized
    for column in (
        "evidence_id bytea NOT NULL",
        "source_record_hmac_sha256 bytea NOT NULL",
        "source_record_identity_sha256 bytea NOT NULL",
        "source_record_payload_sha256 bytea NOT NULL",
        "identifier_policy_sha256 bytea NOT NULL",
        "identifier_rule_id varchar(128) NOT NULL",
        "identifier_rule_sha256 bytea NOT NULL",
    ):
        assert column in normalized
    assert (
        'CREATE FUNCTION "tin_connector".' '"tin_npi_connector_evidence_id_sha256"'
    ) in normalized
    assert (
        'CREATE FUNCTION "tin_connector".' '"tin_npi_connector_evidence_set_sha256"'
    ) in normalized
    assert (
        "evidence_id = " '"tin_connector"."tin_npi_connector_evidence_id_sha256"'
    ) in normalized
    assert "ON DELETE RESTRICT" in normalized
    assert "Immutable token-only Organization evidence retained for audit" in (
        normalized
    )


def test_bulk_load_is_fenced_once_per_statement_and_seal_waits(monkeypatch):
    _, statements, normalized = _upgrade_statements(monkeypatch)

    insert_triggers = [
        " ".join(statement.split())
        for statement in statements
        if "_insert_guard" in statement
        and "inserted_rows" in statement
        and "CREATE TRIGGER" in statement
    ]
    assert len(insert_triggers) == 3
    assert all("AFTER INSERT" in statement for statement in insert_triggers)
    assert all(
        "REFERENCING NEW TABLE AS inserted_rows FOR EACH STATEMENT" in statement
        for statement in insert_triggers
    )
    assert "SELECT DISTINCT generation_key FROM inserted_rows" in normalized
    assert "FOR SHARE" in normalized
    assert "healthporta.tin_npi_build_token" in normalized
    assert "build_lease_expires_at" in normalized
    assert "tin_npi_connector_build_token_or_lease_invalid" in normalized
    assert "NEW.evidence_as_of::timestamptz > NEW.created_at" in normalized


def test_pointer_cutover_is_monotonic_cas_with_source_relation_fence(
    monkeypatch,
):
    _, _, normalized = _upgrade_statements(monkeypatch)

    assert (
        "pointer_key, pointer_version, generation_key, published_at "
        ") VALUES (1, 0, NULL, NULL)"
    ) in normalized
    assert "NEW.pointer_version <> OLD.pointer_version + 1" in normalized
    assert "tin_npi_connector_pointer_cas_conflict" in normalized
    assert ("generation_key IS NOT DISTINCT FROM expected_generation_key") in normalized
    assert "expected_source_vector_id" in normalized
    assert "assert_tin_npi_connector_source_fence" in normalized
    assert (
        normalized.count(
            'PERFORM "tin_connector"."assert_tin_npi_connector_source_fence"'
            "(target_generation_key)"
        )
        == 1
    )
    assert normalized.index(
        'PERFORM "tin_connector"."assert_tin_npi_connector_source_fence"'
    ) < normalized.index(
        'CREATE FUNCTION "tin_connector"."rollback_tin_npi_connector_generation"'
    )
    assert "provider_directory_api_endpoint" in normalized
    assert "provider_directory_source" in normalized
    assert "provider_directory_endpoint_dataset" in normalized
    endpoint_lock = (
        'LOCK TABLE "tin_connector"."provider_directory_api_endpoint" '
        "IN EXCLUSIVE MODE"
    )
    source_lock = (
        'LOCK TABLE "tin_connector"."provider_directory_source" ' "IN EXCLUSIVE MODE"
    )
    dataset_lock = (
        'LOCK TABLE "tin_connector"."provider_directory_endpoint_dataset" '
        "IN EXCLUSIVE MODE"
    )
    relation_lock = "LOCK TABLE %I.%I IN ACCESS SHARE MODE"
    assert endpoint_lock in normalized
    assert source_lock in normalized
    assert dataset_lock in normalized
    assert normalized.index(endpoint_lock) < normalized.index(source_lock)
    assert normalized.index(source_lock) < normalized.index(dataset_lock)
    assert normalized.index(dataset_lock) < normalized.index(relation_lock)
    assert "tin_npi_connector_fhir_source_scope_changed" in normalized
    assert "tin_npi_connector_fhir_dataset_changed" in normalized
    assert "tin_npi_connector_fhir_current_dataset_changed" in normalized
    assert "source_summary_v1" in normalized
    assert "healthporta.provider-directory.source-summary.v1" in normalized
    assert "organization_resources" in normalized
    assert "ptg2_provider_tax_identity_manifest" in normalized
    assert "all-retained-ptg-tax-policy-descriptors.v1" in normalized
    assert "tin_npi_connector_token_policy_scope_changed" in normalized
    assert (
        'CREATE FUNCTION "tin_connector".'
        '"assert_tin_npi_connector_token_policy_fence"( '
        "target_generation_key bigint, require_exact_scope boolean )"
    ) in normalized
    assert (
        'PERFORM "tin_connector"."assert_tin_npi_connector_token_policy_fence"'
        "( target_generation_key, FALSE )"
    ) in normalized
    assert relation_lock in normalized
    assert "tin_npi_connector_source_relation_changed" in normalized
    assert "healthporta.tin_npi_pointer_generation_key" in normalized
    assert "tin_npi_connector_pointer_action_invalid" in normalized
    assert "REVOKE ALL ON FUNCTION" in normalized
    assert "FROM PUBLIC" in normalized


def test_live_rows_are_immutable_truncate_guarded_and_gc_is_batched(
    monkeypatch,
):
    _, statements, normalized = _upgrade_statements(monkeypatch)

    truncate_triggers = [
        statement
        for statement in statements
        if "_truncate_guard" in statement
        and "guard_tin_npi_connector_truncate" in statement
        and "CREATE TRIGGER" in statement
    ]
    assert len(truncate_triggers) == 7
    assert "tin_npi_connector_truncate_forbidden" in normalized
    assert "tin_npi_connector_child_immutable" in normalized
    assert normalized.count("FOR UPDATE SKIP LOCKED") == 2
    assert normalized.count("LIMIT batch_size") == 2
    assert (
        "IF batch_size IS NULL OR batch_size < 1 OR batch_size > 100000" in normalized
    )
    assert "generation_state IN ('failed', 'retired')" in normalized
    assert "healthporta.tin_npi_gc_generation_key" in normalized
    assert "current_user <> generation_owner" in normalized
    assert "OLD.gc_after > clock_timestamp()" in normalized
    assert "OLD.build_lease_expires_at > clock_timestamp()" in normalized
    assert "target_state NOT IN ('failed', 'retired')" in normalized
    assert "target_state = 'retired'" in normalized
    assert "target_state = 'failed'" in normalized
    assert "tin_npi_connector_generation_not_collectable" in normalized
    evidence_delete = (
        'DELETE FROM "tin_connector"."tin_npi_connector_evidence" ' "WHERE ctid IN"
    )
    lookup_delete = (
        'DELETE FROM "tin_connector"."tin_npi_connector_lookup" ' "WHERE ctid IN"
    )
    assert evidence_delete in normalized
    assert lookup_delete in normalized
    assert normalized.index(evidence_delete) < normalized.index(lookup_delete)
    assert (
        "IF remaining_evidence_rows THEN deleted_lookup_rows := 0; "
        "generation_removed := FALSE"
    ) in normalized


def test_published_dataset_resources_are_database_immutable(monkeypatch):
    _, statements, normalized = _upgrade_statements(monkeypatch)

    resource_triggers = [
        " ".join(statement.split())
        for statement in statements
        if "tin_npi_connector_dataset_resource_" in statement
        and "CREATE TRIGGER" in statement
    ]
    assert len(resource_triggers) == 4
    assert any("AFTER INSERT" in statement for statement in resource_triggers)
    assert any("AFTER UPDATE" in statement for statement in resource_triggers)
    assert any("AFTER DELETE" in statement for statement in resource_triggers)
    assert any("BEFORE TRUNCATE" in statement for statement in resource_triggers)
    assert "REFERENCING NEW TABLE AS new_rows" in normalized
    assert "REFERENCING OLD TABLE AS old_rows" in normalized
    assert "tin_npi_connector_dataset_resource_parent_immutable" in normalized
    assert "FOR SHARE OF dataset" in normalized
    assert "tin_npi_connector_endpoint_dataset_transition_invalid" in normalized
    assert "tin_npi_connector_endpoint_dataset_delete_forbidden" in normalized
    assert "tin_npi_connector_endpoint_dataset_guard_changed" in normalized
    assert normalized.count("ENABLE ALWAYS TRIGGER") == 5
    assert "trigger_row.tgenabled = 'A'" in normalized
    assert "trigger_row.tgtype = expected.trigger_type" in normalized
    assert "trigger_row.tgtype = 31" in normalized
    assert (
        "BEFORE INSERT OR UPDATE OR DELETE ON "
        '"tin_connector"."provider_directory_endpoint_dataset"'
    ) in normalized
    assert "trigger_row.tgattr = ''::int2vector" in normalized
    assert "trigger_row.tgoldtable IS NOT DISTINCT FROM" in normalized
    assert (
        'LOCK TABLE "tin_connector"."provider_directory_dataset_resource" '
        "IN SHARE MODE"
    ) in normalized
    assert "tin_npi_connector_dataset_resource_guard_changed" in normalized
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"guard_tin_npi_connector_dataset_resource" FROM PUBLIC'
    ) in normalized
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"guard_tin_npi_connector_endpoint_dataset" FROM PUBLIC'
    ) in normalized


def test_expired_builds_have_owner_only_abandonment_and_batched_recovery(
    monkeypatch,
):
    _, _, normalized = _upgrade_statements(monkeypatch)

    assert (
        'CREATE FUNCTION "tin_connector".' '"abandon_tin_npi_connector_generation"'
    ) in normalized
    assert "SECURITY DEFINER SET search_path = pg_catalog" in normalized
    assert "healthporta.tin_npi_abandon_generation_key" in normalized
    assert "OLD.build_lease_expires_at <= clock_timestamp()" in normalized
    assert "current_user = generation_owner" in normalized
    assert "target_build_lease_expires_at > clock_timestamp()" in normalized
    assert "tin_npi_connector_generation_not_abandonable" in normalized
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"abandon_tin_npi_connector_generation" FROM PUBLIC'
    ) in normalized
    assert (
        'CREATE FUNCTION "tin_connector".'
        '"retire_tin_npi_connector_generation"( '
        "target_generation_key bigint, retain_until timestamptz )"
    ) in normalized
    assert "healthporta.tin_npi_retire_generation_key" in normalized
    assert "retain_until < clock_timestamp() + interval '24 hours'" in normalized
    assert "tin_npi_connector_generation_not_retirable" in normalized
    assert (
        'REVOKE ALL ON FUNCTION "tin_connector".'
        '"retire_tin_npi_connector_generation" FROM PUBLIC'
    ) in normalized


def test_connector_downgrade_refuses_live_data_then_drops_in_dependency_order(
    monkeypatch,
):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tin_connector")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    assert "tin_npi_connector_downgrade_requires_empty_inactive_foundation" in (
        statements[0]
    )
    assert "tin_npi_connector_token_policy" in statements[0]
    assert "tin_npi_connector_identifier_policy" in statements[0]
    assert statements[1:5] == [
        (
            'DROP TRIGGER IF EXISTS "tin_npi_connector_dataset_resource_'
            'insert_guard" ON "tin_connector".'
            '"provider_directory_dataset_resource";'
        ),
        (
            'DROP TRIGGER IF EXISTS "tin_npi_connector_dataset_resource_'
            'update_guard" ON "tin_connector".'
            '"provider_directory_dataset_resource";'
        ),
        (
            'DROP TRIGGER IF EXISTS "tin_npi_connector_dataset_resource_'
            'delete_guard" ON "tin_connector".'
            '"provider_directory_dataset_resource";'
        ),
        (
            'DROP TRIGGER IF EXISTS "tin_npi_connector_dataset_resource_'
            'truncate_guard" ON "tin_connector".'
            '"provider_directory_dataset_resource";'
        ),
    ]
    assert statements[5] == (
        'DROP TRIGGER IF EXISTS "tin_npi_connector_endpoint_dataset_guard" '
        'ON "tin_connector"."provider_directory_endpoint_dataset";'
    )
    assert statements[6:13] == [
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_current";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_evidence";',
        'DROP TABLE IF EXISTS "tin_connector"."tin_npi_connector_lookup";',
        'DROP TABLE IF EXISTS "tin_connector".'
        '"tin_npi_connector_generation_policy";',
        'DROP TABLE IF EXISTS "tin_connector".' '"tin_npi_connector_generation";',
        'DROP TABLE IF EXISTS "tin_connector".'
        '"tin_npi_connector_identifier_policy";',
        'DROP TABLE IF EXISTS "tin_connector".' '"tin_npi_connector_token_policy";',
    ]
    drop_function_statements = statements[13:]
    assert all(
        statement.startswith('DROP FUNCTION IF EXISTS "tin_connector".')
        for statement in drop_function_statements
    )
    assert drop_function_statements[-1].endswith(
        '"tin_npi_connector_valid_npi"(bigint);'
    )


def test_connector_schema_env_alias_conflict_fails_closed(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(
        RuntimeError,
        match="DB_SCHEMA and HLTHPRT_DB_SCHEMA",
    ):
        migration.upgrade()
