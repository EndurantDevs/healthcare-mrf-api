# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add the immutable, publication-disabled public-evidence catalog roots.

Revision ID: 20260808090000_public_evidence_storage_foundation
Revises: 20260807120000_ptg_import_wave_recovery_storage
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808090000_public_evidence_storage_foundation"
down_revision = "20260807120000_ptg_import_wave_recovery_storage"
branch_labels = None
depends_on = None


_SOURCE_POLICIES = {
    "tic": {
        "identity_kind": "immutable_artifact",
        "content_identity_kinds": (
            "logical_json_sha256_v1",
            "raw_container_sha256_v1",
        ),
        "authority": "payer_transparency_in_coverage",
        "trust": "authoritative_tic_rate_group_association",
        "rights": "tic_public_access_processing_retention_reviewed",
        "mode": "declared_complete_artifact",
        "evidence_contract": "tic_artifact_record_attestation_v1",
        "count_unit": "tic_negotiated_rate_record",
        "semantic_limits": (
            "provider_group_membership_not_legal_ownership",
            "tic_rate_not_bound_to_exact_provider_site",
            "tic_provider_rate_association_not_service_capability_or_utilization",
            "tic_shadow_binding_requires_source_coordinate_revalidation",
            "attestation_not_independent_source_closure_proof",
            "contract_digest_not_authenticity_or_source_authority_proof",
            "release_descriptor_not_replacement_deletion_or_current_pointer_authority",
        ),
        "binding_required": True,
    },
    "public_provider_directory_fhir": {
        "identity_kind": "immutable_dataset",
        "content_identity_kinds": (
            "logical_json_sha256_v1",
            "raw_container_sha256_v1",
        ),
        "authority": "public_payer_provider_directory_fhir",
        "trust": "public_provider_directory_source_evidence",
        "rights": "provider_directory_public_access_processing_retention_reviewed",
        "mode": "declared_complete_dataset",
        "evidence_contract": "provider_directory_fhir_resource_attestation_v1",
        "count_unit": "fhir_resource",
        "semantic_limits": (
            "directory_relationship_not_legal_ownership",
            "directory_location_not_exact_rate_site",
            "location_corroboration_requires_exact_npi_active_role_location_plan_network_bridge",
            "attestation_not_independent_source_closure_proof",
            "contract_digest_not_authenticity_or_source_authority_proof",
            "release_descriptor_not_replacement_deletion_or_current_pointer_authority",
        ),
        "binding_required": False,
    },
    "nppes_entity_address": {
        "identity_kind": "immutable_dataset",
        "content_identity_kinds": ("raw_container_sha256_v1",),
        "authority": "cms_nppes_npi_registry",
        "trust": "authoritative_npi_enumeration_and_registry_record_status",
        "rights": "nppes_public_access_processing_retention_reviewed",
        "mode": "declared_complete_dataset",
        "evidence_contract": "nppes_registry_record_attestation_v1",
        "count_unit": "nppes_registry_record",
        "semantic_limits": (
            "non_system_fields_provider_or_authorized_official_reported",
            "nppes_not_payer_confirmed",
            "nppes_has_no_plan_network_binding",
            "nppes_not_tin_address_proof",
            "nppes_not_affiliation_or_ownership_proof",
            "nppes_not_credentialing_proof",
            "nppes_not_current_service_site_proof",
            "nppes_not_universal_ein_npi_crosswalk",
            "registry_address_not_exact_rate_site",
            "attestation_not_independent_source_closure_proof",
            "contract_digest_not_authenticity_or_source_authority_proof",
            "release_descriptor_not_replacement_deletion_or_current_pointer_authority",
        ),
        "binding_required": False,
    },
    "public_hpt": {
        "identity_kind": "immutable_artifact",
        "content_identity_kinds": (
            "logical_json_sha256_v1",
            "raw_container_sha256_v1",
        ),
        "authority": "hospital_published_hpt_machine_readable_artifact",
        "trust": "public_hospital_entity_location_candidate",
        "rights": "hpt_public_access_processing_retention_reviewed",
        "mode": "positive_evidence_only",
        "evidence_contract": "public_hpt_observation_attestation_v1",
        "count_unit": "hpt_candidate_record",
        "semantic_limits": (
            "cms_hpt_rule_schema_is_regulatory_context_not_artifact_authorship",
            "hospital_evidence_not_universal_ein_npi_crosswalk",
            "hospital_location_not_exact_rate_site",
            "attestation_not_independent_source_closure_proof",
            "contract_digest_not_authenticity_or_source_authority_proof",
            "release_descriptor_not_replacement_deletion_or_current_pointer_authority",
        ),
        "binding_required": False,
    },
}

_RELEASE_VALIDATOR_ARGUMENT_TYPES = (
    "text,bytea,text,text,text,text[],text,text,text,bytea,text,text,text,bytea,"
    "bigint,bigint,bytea,text,bytea,text,text,text,bytea,bytea,bytea,"
    "timestamptz,timestamptz,timestamptz,timestamptz,text"
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _qf(schema: str, function: str) -> str:
    return f"{_q(schema)}.{_q(function)}"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _text_array(values: tuple[str, ...]) -> str:
    return "ARRAY[" + ",".join(_literal(value) for value in values) + "]::text[]"


def _source_policy_clause(source_kind: str, policy: dict[str, object]) -> str:
    content_kinds = ",".join(
        _literal(value) for value in policy["content_identity_kinds"]
    )
    binding_clause = (
        "ROW(source_binding_contract_id, source_artifact_source_type, "
        "source_artifact_identity_kind, source_artifact_sha256, "
        "source_binding_sha256, shadow_bundle_binding_sha256) IS NOT NULL "
        "AND source_binding_contract_id = "
        "'ptg2_tax_identity_shadow_source_binding_v1' "
        "AND source_artifact_source_type = 'in_network' "
        "AND source_artifact_identity_kind = artifact_content_identity_kind "
        "AND source_artifact_sha256 = artifact_content_sha256 "
        "AND octet_length(source_binding_sha256) = 32 "
        "AND octet_length(shadow_bundle_binding_sha256) = 32 "
        "AND source_artifact_sha256 <> source_binding_sha256 "
        "AND source_artifact_sha256 <> shadow_bundle_binding_sha256 "
        "AND source_binding_sha256 <> shadow_bundle_binding_sha256"
        if policy["binding_required"]
        else "ROW(source_binding_contract_id, source_artifact_source_type, "
        "source_artifact_identity_kind, source_artifact_sha256, "
        "source_binding_sha256, shadow_bundle_binding_sha256) IS NULL"
    )
    return f"""
        (
            source_kind = {_literal(source_kind)}
            AND artifact_identity_kind = {_literal(policy['identity_kind'])}
            AND artifact_content_identity_kind IN ({content_kinds})
            AND authority_classification = {_literal(policy['authority'])}
            AND trust_classification = {_literal(policy['trust'])}
            AND rights_classification = {_literal(policy['rights'])}
            AND completeness_mode = {_literal(policy['mode'])}
            AND completeness_evidence_contract_id =
                {_literal(policy['evidence_contract'])}
            AND completeness_count_unit = {_literal(policy['count_unit'])}
            AND semantic_limits = {_text_array(policy['semantic_limits'])}
            AND {binding_clause}
        )
    """


def _source_policy_matrix() -> str:
    return " OR ".join(
        _source_policy_clause(source_kind, policy)
        for source_kind, policy in _SOURCE_POLICIES.items()
    )


def _create_source_identity_ref_function(schema: str) -> None:
    function = _qf(schema, "public_evidence_source_identity_ref")
    op.execute(
        f"""
        CREATE FUNCTION {function}(
            candidate_identity_kind text,
            candidate_content_identity_kind text,
            candidate_content_sha256 bytea
        ) RETURNS text
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        SET search_path = pg_catalog
        AS $function$
            WITH encoded_payload AS (
                SELECT convert_to(
                    '{{"content_identity_kind":' ||
                    to_json(candidate_content_identity_kind)::text ||
                    ',"content_sha256":' ||
                    to_json(encode(candidate_content_sha256, 'hex'))::text ||
                    ',"identity_kind":' ||
                    to_json(candidate_identity_kind)::text || '}}',
                    'UTF8'
                ) AS bytes
            ), reference_digest AS (
                SELECT sha256(
                    convert_to(
                        'HEALTHPORTA_PUBLIC_EVIDENCE_REFERENCE_V1',
                        'UTF8'
                    ) || decode('00', 'hex') ||
                    int2send(
                        octet_length(
                            convert_to('immutable_source_identity', 'UTF8')
                        )::smallint
                    ) ||
                    convert_to('immutable_source_identity', 'UTF8') ||
                    int8send(octet_length(bytes)::bigint) || bytes
                ) AS bytes
                FROM encoded_payload
            )
            SELECT 'peid1_' || translate(
                rtrim(encode(bytes, 'base64'), '='),
                '+/',
                '-_'
            )
            FROM reference_digest;
        $function$;
        """
    )


def _create_token_policy_descriptor_function(schema: str) -> None:
    function = _qf(schema, "public_evidence_token_policy_descriptor_sha256")
    op.execute(
        f"""
        CREATE FUNCTION {function}(
            candidate_contract_id text,
            candidate_policy_id text
        ) RETURNS bytea
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        SET search_path = pg_catalog
        AS $function$
            SELECT CASE candidate_contract_id
                WHEN 'ptg_v4_ein_tax_identity_policy_v1' THEN sha256(
                    convert_to('PTG2V4TINPOLICY', 'UTF8') || decode('01', 'hex') ||
                    int4send(octet_length(convert_to(candidate_policy_id, 'UTF8'))) ||
                    convert_to(candidate_policy_id, 'UTF8') ||
                    int4send(33) ||
                    convert_to('ein_ascii_digits_or_2_7_hyphen_v1', 'UTF8') ||
                    int4send(22) ||
                    convert_to('hmac_sha256_ptg_tin_v1', 'UTF8') ||
                    int4send(42) ||
                    convert_to(
                        'tin_id_128=first_16_bytes(tin_hmac_sha256)',
                        'UTF8'
                    ) ||
                    int4send(43) ||
                    convert_to(
                        'tin_hmac_sha256_full_32_bytes_authoritative',
                        'UTF8'
                    )
                )
                WHEN 'healthporta_ein_npi_tax_identity_policy_v1' THEN sha256(
                    convert_to(
                        'HEALTHPORTA_PUBLIC_EIN_NPI_POLICY',
                        'UTF8'
                    ) || decode('01', 'hex') ||
                    int4send(octet_length(convert_to(candidate_policy_id, 'UTF8'))) ||
                    convert_to(candidate_policy_id, 'UTF8') ||
                    int4send(27) ||
                    convert_to('supported_tin_types=ein,npi', 'UTF8') ||
                    int4send(51) ||
                    convert_to(
                        'ein_normalization=ein_ascii_digits_or_2_7_hyphen_v1',
                        'UTF8'
                    ) ||
                    int4send(38) ||
                    convert_to(
                        'npi_normalization=npi_ascii_10_luhn_v1',
                        'UTF8'
                    ) ||
                    int4send(22) ||
                    convert_to('hmac_sha256_ptg_tin_v1', 'UTF8') ||
                    int4send(42) ||
                    convert_to(
                        'tin_id_128=first_16_bytes(tin_hmac_sha256)',
                        'UTF8'
                    ) ||
                    int4send(43) ||
                    convert_to(
                        'tin_hmac_sha256_full_32_bytes_authoritative',
                        'UTF8'
                    )
                )
                ELSE NULL
            END;
        $function$;
        """
    )


def _create_tax_identity_ref_function(schema: str) -> None:
    function = _qf(schema, "public_evidence_tax_identity_ref")
    op.execute(
        f"""
        CREATE FUNCTION {function}(
            candidate_tin_type text,
            candidate_token_policy_contract_id text,
            candidate_token_policy_id text,
            candidate_token_policy_descriptor_sha256 bytea,
            candidate_locator_128 bytea,
            candidate_full_hmac_sha256 bytea,
            candidate_normalization_contract_id text
        ) RETURNS text
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        SET search_path = pg_catalog
        AS $function$
            WITH encoded_payload AS (
                SELECT convert_to(
                    '{{"full_hmac_sha256":' ||
                    to_json(encode(candidate_full_hmac_sha256, 'hex'))::text ||
                    ',"locator_128":' ||
                    to_json(encode(candidate_locator_128, 'hex'))::text ||
                    ',"normalization_contract_id":' ||
                    to_json(candidate_normalization_contract_id)::text ||
                    ',"tin_type":' || to_json(candidate_tin_type)::text ||
                    ',"token_policy_contract_id":' ||
                    to_json(candidate_token_policy_contract_id)::text ||
                    ',"token_policy_descriptor_sha256":' ||
                    to_json(
                        encode(candidate_token_policy_descriptor_sha256, 'hex')
                    )::text ||
                    ',"token_policy_id":' ||
                    to_json(candidate_token_policy_id)::text || '}}',
                    'UTF8'
                ) AS bytes
            ), reference_digest AS (
                SELECT sha256(
                    convert_to(
                        'HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_REFERENCE_V1',
                        'UTF8'
                    ) || decode('00', 'hex') ||
                    int2send(
                        octet_length(convert_to('tax_identity', 'UTF8'))::smallint
                    ) ||
                    convert_to('tax_identity', 'UTF8') ||
                    int8send(octet_length(bytes)::bigint) || bytes
                ) AS bytes
                FROM encoded_payload
            )
            SELECT 'petax1_' || translate(
                rtrim(encode(bytes, 'base64'), '='),
                '+/',
                '-_'
            )
            FROM reference_digest;
        $function$;
        """
    )


def _create_source_release_validator(schema: str) -> None:
    function = _qf(schema, "public_evidence_source_release_valid")
    op.execute(
        f"""
        CREATE FUNCTION {function}(
            candidate_source_release_ref text,
            candidate_contract_sha256 bytea,
            candidate_source_kind text,
            candidate_authority_classification text,
            candidate_trust_classification text,
            candidate_semantic_limits text[],
            candidate_artifact_identity_kind text,
            candidate_artifact_content_identity_kind text,
            candidate_artifact_identity_ref text,
            candidate_artifact_content_sha256 bytea,
            candidate_completeness_mode text,
            candidate_completeness_evidence_contract_id text,
            candidate_completeness_count_unit text,
            candidate_completeness_subject_sha256 bytea,
            candidate_expected_record_count bigint,
            candidate_observed_record_count bigint,
            candidate_evidence_root_sha256 bytea,
            candidate_rights_classification text,
            candidate_rights_proof_sha256 bytea,
            candidate_source_binding_contract_id text,
            candidate_source_artifact_source_type text,
            candidate_source_artifact_identity_kind text,
            candidate_source_artifact_sha256 bytea,
            candidate_source_binding_sha256 bytea,
            candidate_shadow_bundle_binding_sha256 bytea,
            candidate_observed_start_at timestamptz,
            candidate_observed_end_at timestamptz,
            candidate_effective_start_at timestamptz,
            candidate_effective_end_at timestamptz,
            candidate_import_run_ref text
        ) RETURNS boolean
        LANGUAGE plpgsql
        IMMUTABLE
        PARALLEL SAFE
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            artifact_json text;
            attestation_json text;
            binding_json text;
            effective_json text;
            observed_json text;
            semantic_json text;
            import_payload text;
            release_payload text;
            claims_json text;
            lifecycle_json text;
            verification_json text;
            expected_import_run_ref text;
            expected_source_release_ref text;
            expected_contract_sha256 bytea;
            reference_digest bytea;
        BEGIN
            artifact_json := '{{"content_identity_kind":' ||
                to_json(candidate_artifact_content_identity_kind)::text ||
                ',"content_sha256":' ||
                to_json(encode(candidate_artifact_content_sha256, 'hex'))::text ||
                ',"identity_kind":' ||
                to_json(candidate_artifact_identity_kind)::text ||
                ',"identity_ref":' ||
                to_json(candidate_artifact_identity_ref)::text || '}}';
            attestation_json := '{{"count_unit":' ||
                to_json(candidate_completeness_count_unit)::text ||
                ',"evidence_contract_id":' ||
                to_json(candidate_completeness_evidence_contract_id)::text ||
                ',"evidence_root_sha256":' ||
                to_json(encode(candidate_evidence_root_sha256, 'hex'))::text ||
                ',"expected_record_count":' ||
                CASE WHEN candidate_expected_record_count IS NULL
                    THEN 'null'
                    ELSE candidate_expected_record_count::text
                END ||
                ',"mode":' || to_json(candidate_completeness_mode)::text ||
                ',"observed_record_count":' ||
                candidate_observed_record_count::text ||
                ',"subject_sha256":' ||
                to_json(
                    encode(candidate_completeness_subject_sha256, 'hex')
                )::text || '}}';
            binding_json := CASE
                WHEN candidate_source_binding_contract_id IS NULL THEN 'null'
                ELSE '{{"contract_id":' ||
                    to_json(candidate_source_binding_contract_id)::text ||
                    ',"shadow_bundle_binding_sha256":' ||
                    to_json(
                        encode(candidate_shadow_bundle_binding_sha256, 'hex')
                    )::text ||
                    ',"source_artifact_identity_kind":' ||
                    to_json(candidate_source_artifact_identity_kind)::text ||
                    ',"source_artifact_sha256":' ||
                    to_json(
                        encode(candidate_source_artifact_sha256, 'hex')
                    )::text ||
                    ',"source_artifact_source_type":' ||
                    to_json(candidate_source_artifact_source_type)::text ||
                    ',"source_binding_sha256":' ||
                    to_json(encode(candidate_source_binding_sha256, 'hex'))::text ||
                    '}}'
            END;
            effective_json := '{{"end_at":' ||
                CASE WHEN candidate_effective_end_at IS NULL THEN 'null'
                    ELSE to_json(
                        to_char(
                            candidate_effective_end_at AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                        )
                    )::text
                END ||
                ',"start_at":' ||
                to_json(
                    to_char(
                        candidate_effective_start_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                    )
                )::text || '}}';
            observed_json := '{{"end_at":' ||
                to_json(
                    to_char(
                        candidate_observed_end_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                    )
                )::text ||
                ',"start_at":' ||
                to_json(
                    to_char(
                        candidate_observed_start_at AT TIME ZONE 'UTC',
                        'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                    )
                )::text || '}}';
            SELECT COALESCE(
                '[' || string_agg(
                    to_json(semantic_entry.value)::text,
                    ',' ORDER BY semantic_entry.ordinality
                ) || ']',
                '[]'
            ) INTO semantic_json
            FROM unnest(candidate_semantic_limits) WITH ORDINALITY
                AS semantic_entry(value, ordinality);
            claims_json := '{{"exact_rate_site_claimed":' || 'false' ||
                ',"legal_ownership_claimed":' || 'false' ||
                ',"whole_source_complete":' || 'false' || '}}';
            lifecycle_json := '{{"current_pointer_authority":"none",' ||
                '"deletion_enabled":' || 'false' ||
                ',"export_enabled":' || 'false' ||
                ',"publication_enabled":' || 'false' ||
                ',"redistribution_enabled":' || 'false' ||
                ',"replacement_enabled":' || 'false' ||
                ',"retirement_enabled":' || 'false' ||
                ',"serving_authority":"none","state":"verified_disabled",' ||
                '"supersession_enabled":' || 'false' || '}}';
            verification_json := '{{"artifact_bytes_verified":' || 'true' ||
                ',"completeness_attestation_verified":' || 'true' ||
                ',"processing_retention_rights_verified":' || 'true' ||
                ',"public_access_verified":' || 'true' ||
                ',"semantic_limits_verified":' || 'true' || '}}';

            import_payload := '{{"artifact_identity":' || artifact_json ||
                ',"completeness_attestation":' || attestation_json ||
                ',"effective_interval":' || effective_json ||
                ',"observed_interval":' || observed_json ||
                ',"rights_proof_sha256":' ||
                to_json(encode(candidate_rights_proof_sha256, 'hex'))::text ||
                ',"source_binding":' || binding_json ||
                ',"source_kind":' || to_json(candidate_source_kind)::text || '}}';
            reference_digest := sha256(
                convert_to(
                    'HEALTHPORTA_PUBLIC_EVIDENCE_REFERENCE_V1',
                    'UTF8'
                ) || decode('00', 'hex') ||
                int2send(octet_length(convert_to('import_run', 'UTF8'))::smallint) ||
                convert_to('import_run', 'UTF8') ||
                int8send(octet_length(convert_to(import_payload, 'UTF8'))::bigint) ||
                convert_to(import_payload, 'UTF8')
            );
            expected_import_run_ref := 'perun1_' || translate(
                rtrim(encode(reference_digest, 'base64'), '='), '+/', '-_'
            );

            release_payload := '{{"artifact_identity":' || artifact_json ||
                ',"authority_classification":' ||
                to_json(candidate_authority_classification)::text ||
                ',"claims":' || claims_json ||
                ',"completeness_attestation":' || attestation_json ||
                ',"contract":"healthporta.public-evidence-source-release.v1"' ||
                ',"effective_interval":' || effective_json ||
                ',"foundation_scope":"phase_1_public_source_neutral_foundation"' ||
                ',"import_run_ref":' || to_json(candidate_import_run_ref)::text ||
                ',"lifecycle":' || lifecycle_json ||
                ',"observed_interval":' || observed_json ||
                ',"rights":{{"classification":' ||
                to_json(candidate_rights_classification)::text ||
                ',"proof_sha256":' ||
                to_json(encode(candidate_rights_proof_sha256, 'hex'))::text || '}}' ||
                ',"semantic_limits":' || semantic_json ||
                ',"source_binding":' || binding_json ||
                ',"source_kind":' || to_json(candidate_source_kind)::text;
            reference_digest := sha256(
                convert_to(
                    'HEALTHPORTA_PUBLIC_EVIDENCE_REFERENCE_V1',
                    'UTF8'
                ) || decode('00', 'hex') ||
                int2send(
                    octet_length(convert_to('source_release', 'UTF8'))::smallint
                ) ||
                convert_to('source_release', 'UTF8') ||
                int8send(
                    octet_length(convert_to(release_payload ||
                        ',"trust_classification":' ||
                        to_json(candidate_trust_classification)::text ||
                        ',"verification":' || verification_json || '}}',
                        'UTF8'
                    ))::bigint
                ) ||
                convert_to(release_payload ||
                    ',"trust_classification":' ||
                    to_json(candidate_trust_classification)::text ||
                    ',"verification":' || verification_json || '}}',
                    'UTF8'
                )
            );
            expected_source_release_ref := 'perel1_' || translate(
                rtrim(encode(reference_digest, 'base64'), '='), '+/', '-_'
            );

            release_payload := release_payload ||
                ',"source_release_ref":' ||
                to_json(candidate_source_release_ref)::text ||
                ',"trust_classification":' ||
                to_json(candidate_trust_classification)::text ||
                ',"verification":' || verification_json || '}}';
            expected_contract_sha256 := sha256(
                convert_to(
                    'HEALTHPORTA_PUBLIC_EVIDENCE_SOURCE_RELEASE_V1',
                    'UTF8'
                ) || decode('00', 'hex') ||
                int8send(octet_length(convert_to(release_payload, 'UTF8'))::bigint) ||
                convert_to(release_payload, 'UTF8')
            );

            RETURN candidate_import_run_ref = expected_import_run_ref
                AND candidate_source_release_ref = expected_source_release_ref
                AND candidate_contract_sha256 = expected_contract_sha256;
        END;
        $function$;
        """
    )


def _create_guard_function(schema: str) -> None:
    function = _qf(schema, "guard_public_evidence_immutable_catalog")
    op.execute(
        f"""
        CREATE FUNCTION {function}()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        BEGIN
            RAISE EXCEPTION 'public_evidence_catalog_mutation_forbidden'
                USING ERRCODE = '55000';
        END;
        $function$;
        """
    )


def _create_source_identity_table(schema: str) -> None:
    table = _qt(schema, "public_evidence_source_identity")
    ref_function = _qf(schema, "public_evidence_source_identity_ref")
    op.execute(
        f"""
        CREATE TABLE {table} (
            identity_ref varchar(49) NOT NULL,
            identity_kind varchar(24) NOT NULL,
            content_identity_kind varchar(96) NOT NULL,
            content_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('public_evidence_source_identity_pkey')}
                PRIMARY KEY (identity_ref),
            CONSTRAINT {_q('public_evidence_source_identity_content_key')}
                UNIQUE (identity_kind, content_identity_kind, content_sha256),
            CONSTRAINT {_q('public_evidence_source_identity_owner_key')}
                UNIQUE (
                    identity_ref,
                    identity_kind,
                    content_identity_kind,
                    content_sha256
                ),
            CONSTRAINT {_q('public_evidence_source_identity_shape_check')}
                CHECK (
                    identity_ref ~ '^peid1_[A-Za-z0-9_-]{{43}}$'
                    AND identity_kind IN (
                        'immutable_artifact',
                        'immutable_dataset'
                    )
                    AND content_identity_kind ~
                        '^[a-z][a-z0-9_]{{1,94}}_v[1-9][0-9]*$'
                    AND octet_length(content_sha256) = 32
                    AND identity_ref = {ref_function}(
                        identity_kind,
                        content_identity_kind,
                        content_sha256
                    )
                )
        );
        """
    )


def _create_source_release_table(schema: str) -> None:
    table = _qt(schema, "public_evidence_source_release")
    identity = _qt(schema, "public_evidence_source_identity")
    validator = _qf(schema, "public_evidence_source_release_valid")
    policy_matrix = _source_policy_matrix()
    op.execute(
        f"""
        CREATE TABLE {table} (
            source_release_ref varchar(50) NOT NULL,
            contract_sha256 bytea NOT NULL,
            contract varchar(64) NOT NULL,
            foundation_scope varchar(64) NOT NULL,
            source_kind varchar(48) NOT NULL,
            authority_classification varchar(96) NOT NULL,
            trust_classification varchar(96) NOT NULL,
            semantic_limits text[] NOT NULL,
            artifact_identity_ref varchar(49) NOT NULL,
            artifact_identity_kind varchar(24) NOT NULL,
            artifact_content_identity_kind varchar(96) NOT NULL,
            artifact_content_sha256 bytea NOT NULL,
            completeness_mode varchar(32) NOT NULL,
            completeness_evidence_contract_id varchar(96) NOT NULL,
            completeness_count_unit varchar(96) NOT NULL,
            completeness_subject_sha256 bytea NOT NULL,
            expected_record_count bigint,
            observed_record_count bigint NOT NULL,
            evidence_root_sha256 bytea NOT NULL,
            rights_classification varchar(96) NOT NULL,
            rights_proof_sha256 bytea NOT NULL,
            source_binding_contract_id varchar(64),
            source_artifact_source_type varchar(64),
            source_artifact_identity_kind varchar(96),
            source_artifact_sha256 bytea,
            source_binding_sha256 bytea,
            shadow_bundle_binding_sha256 bytea,
            observed_start_at timestamptz NOT NULL,
            observed_end_at timestamptz NOT NULL,
            effective_start_at timestamptz NOT NULL,
            effective_end_at timestamptz,
            import_run_ref varchar(50) NOT NULL,
            lifecycle_state varchar(24) NOT NULL,
            serving_authority varchar(16) NOT NULL,
            current_pointer_authority varchar(16) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('public_evidence_source_release_pkey')}
                PRIMARY KEY (source_release_ref),
            CONSTRAINT {_q('public_evidence_source_release_import_run_key')}
                UNIQUE (import_run_ref),
            CONSTRAINT {_q('public_evidence_source_release_contract_key')}
                UNIQUE (contract_sha256),
            CONSTRAINT {_q('public_evidence_source_release_owner_key')}
                UNIQUE (source_release_ref, contract_sha256),
            CONSTRAINT {_q('public_evidence_source_release_artifact_fkey')}
                FOREIGN KEY (
                    artifact_identity_ref,
                    artifact_identity_kind,
                    artifact_content_identity_kind,
                    artifact_content_sha256
                ) REFERENCES {identity} (
                    identity_ref,
                    identity_kind,
                    content_identity_kind,
                    content_sha256
                ) ON DELETE RESTRICT,
            CONSTRAINT {_q('public_evidence_source_release_fixed_check')}
                CHECK (
                    contract = 'healthporta.public-evidence-source-release.v1'
                    AND foundation_scope =
                        'phase_1_public_source_neutral_foundation'
                    AND lifecycle_state = 'verified_disabled'
                    AND serving_authority = 'none'
                    AND current_pointer_authority = 'none'
                    AND source_release_ref ~
                        '^perel1_[A-Za-z0-9_-]{{43}}$'
                    AND import_run_ref ~ '^perun1_[A-Za-z0-9_-]{{43}}$'
                    AND octet_length(contract_sha256) = 32
                ),
            CONSTRAINT {_q('public_evidence_source_release_digest_check')}
                CHECK (
                    octet_length(artifact_content_sha256) = 32
                    AND octet_length(completeness_subject_sha256) = 32
                    AND completeness_subject_sha256 =
                        artifact_content_sha256
                    AND octet_length(evidence_root_sha256) = 32
                    AND octet_length(rights_proof_sha256) = 32
                ),
            CONSTRAINT {_q('public_evidence_source_release_count_check')}
                CHECK (
                    observed_record_count BETWEEN 0 AND 9007199254740991
                    AND (
                        (
                            completeness_mode = 'positive_evidence_only'
                            AND expected_record_count IS NULL
                        )
                        OR (
                            completeness_mode IN (
                                'declared_complete_artifact',
                                'declared_complete_dataset'
                            )
                            AND expected_record_count IS NOT NULL
                            AND expected_record_count = observed_record_count
                            AND expected_record_count BETWEEN
                                0 AND 9007199254740991
                        )
                    )
                ),
            CONSTRAINT {_q('public_evidence_source_release_interval_check')}
                CHECK (
                    observed_start_at >=
                        TIMESTAMPTZ '0001-01-01 00:00:00+00'
                    AND observed_start_at <
                        TIMESTAMPTZ '10000-01-01 00:00:00+00'
                    AND observed_end_at >=
                        TIMESTAMPTZ '0001-01-01 00:00:00+00'
                    AND observed_end_at <
                        TIMESTAMPTZ '10000-01-01 00:00:00+00'
                    AND effective_start_at >=
                        TIMESTAMPTZ '0001-01-01 00:00:00+00'
                    AND effective_start_at <
                        TIMESTAMPTZ '10000-01-01 00:00:00+00'
                    AND (
                        effective_end_at IS NULL
                        OR effective_end_at >=
                            TIMESTAMPTZ '0001-01-01 00:00:00+00'
                        AND effective_end_at <
                            TIMESTAMPTZ '10000-01-01 00:00:00+00'
                    )
                    AND observed_end_at >= observed_start_at
                    AND (
                        effective_end_at IS NULL
                        OR effective_end_at >= effective_start_at
                    )
                    AND date_trunc('second', observed_start_at) =
                        observed_start_at
                    AND date_trunc('second', observed_end_at) = observed_end_at
                    AND date_trunc('second', effective_start_at) =
                        effective_start_at
                    AND (
                        effective_end_at IS NULL
                        OR date_trunc('second', effective_end_at) =
                            effective_end_at
                    )
                ),
            CONSTRAINT {_q('public_evidence_source_release_policy_check')}
                CHECK ({policy_matrix}),
            CONSTRAINT {_q('public_evidence_source_release_reference_check')}
                CHECK (
                    {validator}(
                        source_release_ref,
                        contract_sha256,
                        source_kind,
                        authority_classification,
                        trust_classification,
                        semantic_limits,
                        artifact_identity_kind,
                        artifact_content_identity_kind,
                        artifact_identity_ref,
                        artifact_content_sha256,
                        completeness_mode,
                        completeness_evidence_contract_id,
                        completeness_count_unit,
                        completeness_subject_sha256,
                        expected_record_count,
                        observed_record_count,
                        evidence_root_sha256,
                        rights_classification,
                        rights_proof_sha256,
                        source_binding_contract_id,
                        source_artifact_source_type,
                        source_artifact_identity_kind,
                        source_artifact_sha256,
                        source_binding_sha256,
                        shadow_bundle_binding_sha256,
                        observed_start_at,
                        observed_end_at,
                        effective_start_at,
                        effective_end_at,
                        import_run_ref
                    ) IS TRUE
                )
        );
        """
    )


def _create_token_policy_table(schema: str) -> None:
    table = _qt(schema, "public_evidence_token_policy")
    descriptor = _qf(schema, "public_evidence_token_policy_descriptor_sha256")
    op.execute(
        f"""
        CREATE TABLE {table} (
            token_policy_contract_id varchar(64) NOT NULL,
            token_policy_id varchar(72) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('public_evidence_token_policy_pkey')}
                PRIMARY KEY (token_policy_contract_id, token_policy_id),
            CONSTRAINT {_q('public_evidence_token_policy_owner_key')}
                UNIQUE (
                    token_policy_contract_id,
                    token_policy_id,
                    token_policy_descriptor_sha256
                ),
            CONSTRAINT {_q('public_evidence_token_policy_shape_check')}
                CHECK (
                    octet_length(token_policy_descriptor_sha256) = 32
                    AND (
                        token_policy_contract_id =
                            'ptg_v4_ein_tax_identity_policy_v1'
                        AND token_policy_id ~
                            '^ptg-tin-hmac-sha256-v1:[a-z0-9]'
                            '[a-z0-9._-]{{0,31}}$'
                        OR token_policy_contract_id =
                            'healthporta_ein_npi_tax_identity_policy_v1'
                        AND token_policy_id ~
                            '^healthporta-tax-identity-hmac-sha256-v1:'
                            '[a-z0-9][a-z0-9._-]{{0,31}}$'
                    )
                    AND token_policy_descriptor_sha256 = {descriptor}(
                        token_policy_contract_id,
                        token_policy_id
                    )
                )
        );
        """
    )


def _create_tax_identity_table(schema: str) -> None:
    table = _qt(schema, "public_evidence_tax_identity")
    token_policy = _qt(schema, "public_evidence_token_policy")
    ref_function = _qf(schema, "public_evidence_tax_identity_ref")
    op.execute(
        f"""
        CREATE TABLE {table} (
            tax_identity_ref varchar(50) NOT NULL,
            tin_type varchar(3) NOT NULL,
            token_policy_contract_id varchar(64) NOT NULL,
            token_policy_id varchar(72) NOT NULL,
            token_policy_descriptor_sha256 bytea NOT NULL,
            locator_128 bytea NOT NULL,
            full_hmac_sha256 bytea NOT NULL,
            normalization_contract_id varchar(64) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('public_evidence_tax_identity_pkey')}
                PRIMARY KEY (tax_identity_ref),
            CONSTRAINT {_q('public_evidence_tax_identity_policy_fkey')}
                FOREIGN KEY (
                    token_policy_contract_id,
                    token_policy_id,
                    token_policy_descriptor_sha256
                ) REFERENCES {token_policy} (
                    token_policy_contract_id,
                    token_policy_id,
                    token_policy_descriptor_sha256
                ) ON DELETE RESTRICT,
            CONSTRAINT {_q('public_evidence_tax_identity_hmac_key')}
                UNIQUE (
                    token_policy_contract_id,
                    token_policy_id,
                    full_hmac_sha256
                ),
            CONSTRAINT {_q('public_evidence_tax_identity_shape_check')}
                CHECK (
                    tax_identity_ref ~ '^petax1_[A-Za-z0-9_-]{{43}}$'
                    AND tin_type IN ('ein', 'npi')
                    AND octet_length(locator_128) = 16
                    AND octet_length(full_hmac_sha256) = 32
                    AND locator_128 = substring(full_hmac_sha256 FROM 1 FOR 16)
                    AND tax_identity_ref = {ref_function}(
                        tin_type,
                        token_policy_contract_id,
                        token_policy_id,
                        token_policy_descriptor_sha256,
                        locator_128,
                        full_hmac_sha256,
                        normalization_contract_id
                    )
                ),
            CONSTRAINT {_q('public_evidence_tax_identity_policy_check')}
                CHECK (
                    token_policy_contract_id =
                        'ptg_v4_ein_tax_identity_policy_v1'
                    AND tin_type = 'ein'
                    AND normalization_contract_id =
                        'ein_ascii_digits_or_2_7_hyphen_v1'
                    OR token_policy_contract_id =
                        'healthporta_ein_npi_tax_identity_policy_v1'
                    AND (
                        tin_type = 'ein'
                        AND normalization_contract_id =
                            'ein_ascii_digits_or_2_7_hyphen_v1'
                        OR tin_type = 'npi'
                        AND normalization_contract_id =
                            'npi_ascii_10_luhn_v1'
                    )
                )
        );
        """
    )
    op.execute(
        f"""
        CREATE INDEX {_q('public_evidence_tax_identity_locator_idx')}
            ON {table} (
                token_policy_contract_id,
                token_policy_id,
                locator_128,
                full_hmac_sha256,
                tin_type,
                tax_identity_ref
            );
        """
    )


def _install_immutable_guards(schema: str) -> None:
    function = _qf(schema, "guard_public_evidence_immutable_catalog")
    for table_name in (
        "public_evidence_source_identity",
        "public_evidence_source_release",
        "public_evidence_token_policy",
        "public_evidence_tax_identity",
    ):
        table = _qt(schema, table_name)
        mutation_trigger = _q(f"{table_name}_mutation_guard")
        truncate_trigger = _q(f"{table_name}_truncate_guard")
        op.execute(
            f"""
            CREATE TRIGGER {mutation_trigger}
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION {function}();
            """
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {mutation_trigger};")
        op.execute(
            f"""
            CREATE TRIGGER {truncate_trigger}
            BEFORE TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION {function}();
            """
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {truncate_trigger};")
        op.execute(f"REVOKE ALL ON TABLE {table} FROM PUBLIC;")


def _revoke_helper_functions(schema: str) -> None:
    function_signatures = (
        ("public_evidence_source_identity_ref", "text,text,bytea"),
        (
            "public_evidence_token_policy_descriptor_sha256",
            "text,text",
        ),
        (
            "public_evidence_tax_identity_ref",
            "text,text,text,bytea,bytea,bytea,text",
        ),
        (
            "public_evidence_source_release_valid",
            _RELEASE_VALIDATOR_ARGUMENT_TYPES,
        ),
        ("guard_public_evidence_immutable_catalog", ""),
    )
    for function_name, argument_types in function_signatures:
        op.execute(
            f"REVOKE ALL ON FUNCTION {_qf(schema, function_name)}"
            f"({argument_types}) FROM PUBLIC;"
        )


def upgrade() -> None:
    """Install four empty immutable roots with no readers or writers."""

    schema = _schema()
    _create_source_identity_ref_function(schema)
    _create_token_policy_descriptor_function(schema)
    _create_tax_identity_ref_function(schema)
    _create_source_release_validator(schema)
    _create_guard_function(schema)
    _create_source_identity_table(schema)
    _create_source_release_table(schema)
    _create_token_policy_table(schema)
    _create_tax_identity_table(schema)
    _install_immutable_guards(schema)
    _revoke_helper_functions(schema)
    op.execute(
        f"COMMENT ON TABLE {_qt(schema, 'public_evidence_source_release')} IS "
        "'Publication-disabled source-evidence catalog; no current pointer';"
    )
    op.execute(
        f"COMMENT ON TABLE {_qt(schema, 'public_evidence_tax_identity')} IS "
        "'Opaque typed HMAC identities only; no raw or masked identifiers';"
    )


def downgrade() -> None:
    """Remove only a still-empty publication-disabled catalog."""

    schema = _schema()
    table_names = (
        "public_evidence_source_identity",
        "public_evidence_source_release",
        "public_evidence_tax_identity",
        "public_evidence_token_policy",
    )
    tables = tuple(_qt(schema, table_name) for table_name in table_names)
    op.execute("LOCK TABLE " + ", ".join(tables) + " IN ACCESS EXCLUSIVE MODE;")
    nonempty_checks = " OR ".join(
        f"EXISTS (SELECT 1 FROM {table} LIMIT 1)" for table in tables
    )
    op.execute(
        f"""
        DO $block$
        BEGIN
            IF {nonempty_checks} THEN
                RAISE EXCEPTION
                    'public_evidence_downgrade_requires_empty_foundation'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """
    )
    for table_name in (
        "public_evidence_tax_identity",
        "public_evidence_token_policy",
        "public_evidence_source_release",
        "public_evidence_source_identity",
    ):
        op.execute(f"DROP TABLE IF EXISTS {_qt(schema, table_name)};")
    for function_name, argument_types in (
        ("guard_public_evidence_immutable_catalog", ""),
        (
            "public_evidence_source_release_valid",
            _RELEASE_VALIDATOR_ARGUMENT_TYPES,
        ),
        (
            "public_evidence_tax_identity_ref",
            "text,text,text,bytea,bytea,bytea,text",
        ),
        (
            "public_evidence_token_policy_descriptor_sha256",
            "text,text",
        ),
        ("public_evidence_source_identity_ref", "text,text,bytea"),
    ):
        op.execute(
            f"DROP FUNCTION IF EXISTS {_qf(schema, function_name)}"
            f"({argument_types});"
        )
