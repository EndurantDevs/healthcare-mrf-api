# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static schema and digest contracts for the connector migration."""

from __future__ import annotations

from tests.tin_npi_connector_migration_support import capture_upgrade


def test_connector_migration_descends_from_current_merged_head(monkeypatch):
    migration, sql_statements, normalized_sql = capture_upgrade(monkeypatch)

    assert migration.revision == "20260729110000_tin_npi_connector"
    assert migration.down_revision == "20260729100000_ptg2_candidate_audit_hold"
    assert len(sql_statements) == 86
    assert normalized_sql.count('CREATE TABLE "tin_connector".') == 7
    table_names = (
        "tin_npi_connector_token_policy",
        "tin_npi_connector_identifier_policy",
        "tin_npi_connector_generation",
        "tin_npi_connector_generation_policy",
        "tin_npi_connector_lookup",
        "tin_npi_connector_evidence",
        "tin_npi_connector_current",
    )
    for table_name in table_names:
        assert f'CREATE TABLE "tin_connector"."{table_name}"' in normalized_sql


def test_connector_migration_is_token_only_and_snapshot_neutral(monkeypatch):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    forbidden_terms = (
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
    )
    for forbidden_term in forbidden_terms:
        assert forbidden_term not in normalized_sql
    assert "token_only_v1" in normalized_sql
    assert "same_organization_identifier" in normalized_sql
    assert "healthporta.tin-npi.site-by-current-entity-address-unified.v1" in (
        normalized_sql
    )
    assert "tin_npi_connector_reverse_lookup" not in normalized_sql
    assert "tin_npi_connector_site" not in normalized_sql


def test_policy_registries_authenticate_descriptors_and_manifest_keys(monkeypatch):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    descriptor_markers = (
        "PTG2V4TINPOLICY",
        "ein_ascii_digits_or_2_7_hyphen_v1",
        "hmac_sha256_ptg_tin_v1",
        "tin_hmac_sha256_full_32_bytes_authoritative",
        "healthporta.tin-npi.fhir-identifier-policy.v2",
        "healthporta.tin-npi.fhir-identifier-rule.v1",
    )
    for descriptor_marker in descriptor_markers:
        assert descriptor_marker in normalized_sql
    assert (
        "token_policy_descriptor_sha256 = "
        '"tin_connector"."tin_npi_connector_token_policy_descriptor_sha256"'
        "(token_policy_id)"
    ) in normalized_sql
    assert (
        'CREATE FUNCTION "tin_connector".'
        '"tin_npi_connector_identifier_rule_sha256"(candidate_rule jsonb)'
    ) in normalized_sql
    assert "key_count <> 2" in normalized_sql
    assert "key_count <> 10" in normalized_sql
    _assert_identifier_rule_keys(normalized_sql)
    assert (
        "FOREIGN KEY ( identifier_policy_id, identifier_policy_sha256 ) "
        'REFERENCES "tin_connector"."tin_npi_connector_identifier_policy"'
    ) in normalized_sql
    assert "key_count <> 15" in normalized_sql
    _assert_source_vector_keys(normalized_sql)


def _assert_identifier_rule_keys(normalized_sql):
    identifier_rule_keys = (
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
    )
    for identifier_rule_key in identifier_rule_keys:
        assert f"'{identifier_rule_key}'" in normalized_sql


def _assert_source_vector_keys(normalized_sql):
    source_vector_keys = (
        "fhir_datasets",
        "input_relations",
        "lookup_contract_id",
        "lookup_schema_version",
        "source_scope_contract_id",
        "token_policies",
        "token_policy_scope_contract_id",
        "token_policy_ids",
    )
    for source_vector_key in source_vector_keys:
        assert f"'{source_vector_key}'" in normalized_sql


def test_fhir_source_vector_is_current_published_only(monkeypatch):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    source_fence_markers = (
        "item ->> 'status' <> 'published'",
        "item -> 'is_current' <> 'true'::jsonb",
        "item -> 'promote_on_cutover' <> 'false'::jsonb",
        "jsonb_typeof( item -> 'expected_incumbent_dataset_id' ) <> 'null'",
        "jsonb_typeof( item -> 'recorded_expected_resources' ) <> 'array'",
        "COUNT( DISTINCT value ->> 'source_id' )",
        "item ->> 'identifier_rule_id'",
        "item ->> 'identifier_rule_sha256'",
        "all-current-published-organization-sources.v1",
        "(item -> 'selected_resources') @> '[\"Organization\"]'::jsonb",
        "organization_resource_count",
        "organization_resource_sha256",
        "source_summary_sha256",
    )
    for source_fence_marker in source_fence_markers:
        assert source_fence_marker in normalized_sql
    assert (
        "COUNT( DISTINCT dataset - 'source_id' - 'identifier_rule_id' "
        "- 'identifier_rule_sha256' ) <> 1"
    ) in normalized_sql


def test_generation_digests_are_recomputed_from_canonical_content(monkeypatch):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    digest_contract_markers = (
        "UNIQUE (source_vector_id)",
        "healthporta.tin-npi.source-vector.v1",
        "healthporta.tin-npi.source-ordinal-map.v1",
        "healthporta.tin-npi.lookup-row.v3",
        "healthporta.tin-npi.lookup-bucket.v1",
        "healthporta.tin-npi.lookup-set.v4",
        "healthporta.tin-npi.generation.v3",
        "healthporta.tin-npi.fhir-organization-scan-proof.v2",
        "healthporta.tin-npi.fhir-evidence.v2",
        "healthporta.tin-npi.fhir-evidence-set.v1",
        "calculated_lookup_digest <> NEW.lookup_digest",
        "identifier_rule_difference_count <> 0",
        "invalid_evidence_count <> 0",
        "invalid_evidence_record_count <> 0",
        "distinct_policy_count <> NEW.token_policy_count",
        "distinct_npi_set_count <> 1",
        "distinct_payload_count <> 1",
        "observed_matched_record_count <> NEW.matched_organization_count",
        "evidence_projection_difference_count <> 0",
        "evidence_scan_digest_difference_count <> 0",
        "observed_audit_evidence_count <> NEW.evidence_count",
        "source_membership_difference_count <> 0",
        "invalid_policy_count <> 0",
        "unused_policy_count <> 0",
        "source_policy_evidence_difference_count <> 0",
    )
    for digest_contract_marker in digest_contract_markers:
        assert digest_contract_marker in normalized_sql
    assert (
        'calculated_lookup_digest := "tin_connector".'
        '"tin_npi_connector_lookup_set_sha256"(NEW.generation_key)'
    ) in normalized_sql
    assert (
        "dataset_proof ->> 'matched_evidence_sha256', 'hex' ) <> "
        '"tin_connector"."tin_npi_connector_evidence_set_sha256"'
        "( NEW.generation_key, source_scope.source_ordinal )"
    ) in normalized_sql


def test_lookup_is_compact_nonpartitioned_and_preserves_prefix_collisions(
    monkeypatch,
):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    lookup_markers = (
        "generation_key bigint GENERATED ALWAYS AS IDENTITY",
        "npis bigint[] NOT NULL",
        "source_bitmap bytea NOT NULL",
        "npi_source_bitmap_matrix bytea NOT NULL",
        "source_evidence_counts bigint[] NOT NULL",
        "tin_npi_connector_valid_source_evidence",
        "tin_id_128 = substring(tin_hmac_sha256 FROM 1 FOR 16)",
        "octet_length(tin_id_128) = 16",
        "octet_length(tin_hmac_sha256) = 32",
        "source_evidence_count < npi_support_count",
    )
    for lookup_marker in lookup_markers:
        assert lookup_marker in normalized_sql
    assert (
        "PRIMARY KEY ( generation_key, token_policy_id, tin_id_128, "
        "tin_hmac_sha256 )"
    ) in normalized_sql
    assert (
        "FOREIGN KEY (generation_key, token_policy_id) REFERENCES "
        '"tin_connector"."tin_npi_connector_generation_policy" '
        "( generation_key, token_policy_id ) ON DELETE CASCADE"
    ) in normalized_sql
    assert (
        "octet_length(candidate_npi_source_bitmap_matrix) "
        "<> npi_count * bitmap_width"
    ) in normalized_sql
    assert (
        "octet_length(npi_source_bitmap_matrix) "
        "<> cardinality(npis) * ((NEW.source_count + 7) / 8)"
    ) in normalized_sql
    assert "PARTITION BY" not in normalized_sql
    assert "USING gin" not in normalized_sql.lower()


def test_evidence_rows_are_token_only_rule_bound_and_content_authenticated(
    monkeypatch,
):
    _, _, normalized_sql = capture_upgrade(monkeypatch)

    assert (
        'CREATE TABLE "tin_connector"."tin_npi_connector_evidence"'
    ) in normalized_sql
    evidence_columns = (
        "evidence_id bytea NOT NULL",
        "source_record_hmac_sha256 bytea NOT NULL",
        "source_record_identity_sha256 bytea NOT NULL",
        "source_record_payload_sha256 bytea NOT NULL",
        "identifier_policy_sha256 bytea NOT NULL",
        "identifier_rule_id varchar(128) NOT NULL",
        "identifier_rule_sha256 bytea NOT NULL",
    )
    for evidence_column in evidence_columns:
        assert evidence_column in normalized_sql
    assert (
        'CREATE FUNCTION "tin_connector"."tin_npi_connector_evidence_id_sha256"'
    ) in normalized_sql
    assert (
        'CREATE FUNCTION "tin_connector"."tin_npi_connector_evidence_set_sha256"'
    ) in normalized_sql
    assert (
        'evidence_id = "tin_connector"."tin_npi_connector_evidence_id_sha256"'
    ) in normalized_sql
    assert "ON DELETE RESTRICT" in normalized_sql
    assert "Immutable token-only Organization evidence retained for audit" in (
        normalized_sql
    )


def test_bulk_load_is_fenced_once_per_statement_and_seal_waits(monkeypatch):
    _, sql_statements, normalized_sql = capture_upgrade(monkeypatch)

    insert_triggers = [
        " ".join(statement.split())
        for statement in sql_statements
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
    assert "SELECT DISTINCT generation_key FROM inserted_rows" in normalized_sql
    assert "FOR SHARE" in normalized_sql
    assert "healthporta.tin_npi_build_token" in normalized_sql
    assert "build_lease_expires_at" in normalized_sql
    assert "tin_npi_connector_build_token_or_lease_invalid" in normalized_sql
    assert "NEW.evidence_as_of::timestamptz > NEW.created_at" in normalized_sql
