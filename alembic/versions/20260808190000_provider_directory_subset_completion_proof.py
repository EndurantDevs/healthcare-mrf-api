# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Persist reviewed Provider Directory subset-completion proofs.

Revision ID: 20260808190000_provider_directory_subset_completion_proof
Revises: 20260808180000_ptg_import_wave_materialized_preclaim
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import add_column_if_missing


revision = "20260808190000_provider_directory_subset_completion_proof"
down_revision = "20260808180000_ptg_import_wave_materialized_preclaim"
branch_labels = None
depends_on = None


_ENDPOINT_DATASET = "provider_directory_endpoint_dataset"
_DATASET_RESOURCE = "provider_directory_dataset_resource"
_SOURCE = "provider_directory_source"
_ENDPOINT_DATASET_GUARD = "guard_tin_npi_connector_endpoint_dataset"
_DATASET_RESOURCE_GUARD = "guard_tin_npi_connector_dataset_resource"
_SOURCE_GUARD = "guard_provider_directory_subset_published_source"
_SOURCE_GUARD_TRIGGER = "provider_directory_subset_published_source_guard"
_SOURCE_TRUNCATE_GUARD_TRIGGER = (
    "provider_directory_subset_published_source_truncate_guard"
)
_CANONICAL_JSON_FUNCTION = (
    "provider_directory_subset_completion_canonical_json"
)
_CANONICAL_SHA256_FUNCTION = "provider_directory_subset_canonical_sha256"
_PAYLOAD_CANONICAL_JSON_FUNCTION = (
    "provider_directory_subset_payload_canonical_json"
)
_PAYLOAD_SHA256_FUNCTION = "provider_directory_subset_payload_sha256"
_PROOF_PAIR_VALID_FUNCTION = (
    "provider_directory_subset_completion_proof_pair_valid"
)
_PROOF_SHAPE_VALID_FUNCTION = (
    "provider_directory_subset_completion_proof_shape_valid"
)
_REPLAY_SHAPE_VALID_FUNCTION = (
    "provider_directory_subset_replay_evidence_shape_valid"
)
_COVERAGE_SHAPE_VALID_FUNCTION = (
    "provider_directory_subset_coverage_shape_valid"
)
_CONTENT_PROOF_VALID_FUNCTION = (
    "provider_directory_subset_content_proof_valid"
)
_REPLAY_EVIDENCE_KEY = "server_issued_subset_replay_evidence"
_REPLAY_EVIDENCE_SHA256_KEY = (
    "server_issued_subset_replay_evidence_sha256"
)
_SUBSET_COVERAGE_KEY = "server_issued_subset_coverage"
_TWIN_VERIFICATION_KEY = "twin_root_verification_v1"
_TWIN_CAMPAIGN_KEY = "verification_campaign_id"
_TWIN_SCOPE_KEY = "verification_source_scope_hash"
_TWIN_ROLE_KEY = "verification_role"
_TWIN_BASELINE_DATASET_KEY = "verification_baseline_dataset_id"
_BASELINE_GENERATION_INDEX = "pd_endpoint_dataset_subset_baseline_generation_key"
_SUBSET_SOURCE_SCOPE_VERSION = (
    "provider-directory-fhir-server-issued-subset-source-scope-v1"
)
_SUBSET_SOURCE_SCOPE_VERSION_V2 = (
    "provider-directory-fhir-server-issued-subset-source-scope-v2"
)
_REVIEWED_ROOT_POLICY_KEY = "provider_directory_reviewed_root_policy_v1"
_REVIEWED_ROOT_POLICY_VERSION = "provider-directory-reviewed-root-policy-v1"
_ROOT_POLICY_PENDING_SOURCE_STATUS = "pending_reviewed_subset_acquisition"
_ROOT_POLICY_VERIFIED_SOURCE_STATUS = "verified_reviewed_subset_acquisition"
_CONTENT_PROOF_KEY = "provider_directory_content_proof_v1"
_CONTENT_PROOF_CONTRACT = "healthporta.provider-directory.content-proof.v1"
_SUBSET_VERIFIED_SOURCE_STATUS = (
    "verified_two_matching_reviewed_subset_acquisitions"
)
_SUBSET_PENDING_SOURCE_STATUS = (
    "pending_two_matching_reviewed_subset_acquisitions"
)
_SUBSET_SOURCE_SCOPE_METADATA_FIELDS = (
    "provider_directory_supported_resources",
    "provider_directory_fully_enumerable_resources",
    "provider_directory_expected_nonempty_resources",
    "provider_directory_resource_page_count_caps",
    "provider_directory_acquisition_enabled",
    "provider_directory_coverage_mode",
    "provider_directory_manual_only",
    "provider_directory_server_issued_subset_resources",
    "provider_directory_current_version_census_strategy",
    "provider_directory_current_version_census_contract_version",
    "provider_directory_current_version_census_page_count",
    "provider_directory_current_version_census_strategy_version",
    "provider_directory_current_version_census_traversal_version",
    "provider_directory_current_version_census_canonicalization_version",
    "provider_directory_current_version_census_completion_scopes",
    "provider_directory_current_version_census_continuation_strategy",
    "provider_directory_current_version_census_start_urls",
    "provider_directory_verification_campaign_id",
    "provider_directory_configured_endpoint_id",
)
_SUBSET_RESOURCE_TYPES = (
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)
_SUBSET_RESOURCE_FIELDS = (
    "advertised_pre",
    "advertised_post",
    "returned_unique",
    "deficit",
    "geometry_version",
    "page_count",
    "pages",
    "processed_rows",
    "page_entry_counts",
    "continuation_shape_sha256",
    "continuation_shape_chain_sha256",
    "logical_terminal_offset",
    "logical_window_end_offset",
    "terminal_entries",
    "sparse_pages",
    "empty_pages",
    "geometry_sha256",
    "terminal_reason",
    "content_sha256",
    "acquired_content_sha256",
)
_SUBSET_REPLAY_RESOURCE_FIELDS = (
    "pages",
    "continuation_hop_sha256",
    "continuation_hop_chain_sha256",
    "continuation_shape_sha256",
    "continuation_shape_chain_sha256",
)

_LEGACY_ENDPOINT_DATASET_COLUMNS = (
    "dataset_id",
    "endpoint_id",
    "import_run_id",
    "acquisition_root_run_id",
    "previous_dataset_id",
    "dataset_hash",
    "status",
    "is_current",
    "resource_count",
    "created_at",
    "validated_at",
    "published_at",
    "superseded_at",
    "publication_metadata_json",
)
_SUBSET_ENDPOINT_DATASET_COLUMNS = _LEGACY_ENDPOINT_DATASET_COLUMNS + (
    "completion_proof_required_version",
    "completion_proof_json",
    "completion_proof_sha256",
)
_LEGACY_DATASET_RESOURCE_COLUMNS = (
    "dataset_id",
    "resource_type",
    "resource_id",
    "payload_hash",
    "payload_json",
)
_SUBSET_DATASET_RESOURCE_COLUMNS = _LEGACY_DATASET_RESOURCE_COLUMNS + (
    "acquired_resource_sha256",
)

_PARENT_CHECKS = (
    "pd_endpoint_dataset_completion_version_check",
    "pd_endpoint_dataset_completion_pair_check",
    "pd_endpoint_dataset_completion_marker_check",
    "pd_endpoint_dataset_completion_digest_check",
    "pd_endpoint_dataset_completion_shape_check",
    "pd_endpoint_dataset_subset_replay_evidence_check",
)
_CHILD_DIGEST_CHECK = "pd_dataset_resource_acquired_sha256_check"

_TERMINAL_STATUSES_SQL = """
                'validated',
                'published',
                'superseded',
                'verification_baseline',
                'verification_mismatch'
"""


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime or legacy or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _qf(schema: str, relation: str) -> str:
    return f"{_q(schema)}.{_q(relation)}"


def _relation_schema_fence_sql(
    schema: str,
    relation: str,
    expected_columns: tuple[str, ...],
    *,
    compatible_columns: tuple[str, ...] | None = None,
) -> str:
    relation_ref = _qf(schema, relation)
    expected_array = ", ".join(
        _ql(column) for column in sorted(expected_columns)
    )
    compatible_clause = ""
    if compatible_columns is not None:
        compatible_array = ", ".join(
            _ql(column) for column in sorted(compatible_columns)
        )
        compatible_clause = (
            "\n           AND observed_columns IS DISTINCT FROM "
            f"ARRAY[{compatible_array}]::text[]"
        )
    return f"""
    DO $migration$
    DECLARE
        observed_columns text[];
    BEGIN
        SELECT array_agg(attribute.attname ORDER BY attribute.attname)
          INTO observed_columns
          FROM pg_catalog.pg_attribute AS attribute
         WHERE attribute.attrelid = {_ql(relation_ref)}::regclass
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped;
        IF observed_columns IS DISTINCT FROM
                ARRAY[{expected_array}]::text[]{compatible_clause} THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_schema_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _subset_column_shape_fence_sql(schema: str) -> str:
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    resource_ref = _qf(schema, _DATASET_RESOURCE)
    return f"""
    DO $migration$
    DECLARE
        invalid_column_count bigint;
    BEGIN
        SELECT count(*)
          INTO invalid_column_count
          FROM (
                VALUES
                    (
                        {_ql(dataset_ref)}::regclass,
                        'completion_proof_required_version',
                        'integer'
                    ),
                    (
                        {_ql(dataset_ref)}::regclass,
                        'completion_proof_json',
                        'jsonb'
                    ),
                    (
                        {_ql(dataset_ref)}::regclass,
                        'completion_proof_sha256',
                        'character varying(64)'
                    ),
                    (
                        {_ql(resource_ref)}::regclass,
                        'acquired_resource_sha256',
                        'character varying(64)'
                    )
               ) AS expected(relation_oid, column_name, column_type)
          LEFT JOIN pg_catalog.pg_attribute AS attribute
            ON attribute.attrelid = expected.relation_oid
           AND attribute.attname = expected.column_name
           AND attribute.attnum > 0
           AND NOT attribute.attisdropped
          LEFT JOIN pg_catalog.pg_attrdef AS column_default
            ON column_default.adrelid = expected.relation_oid
           AND column_default.adnum = attribute.attnum
         WHERE attribute.attname IS NULL
            OR pg_catalog.format_type(
                    attribute.atttypid,
                    attribute.atttypmod
                ) IS DISTINCT FROM expected.column_type
            OR attribute.attnotnull
            OR column_default.oid IS NOT NULL;
        IF invalid_column_count <> 0 THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_schema_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _guard_trigger_shape_fence_sql(schema: str) -> str:
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    resource_ref = _qf(schema, _DATASET_RESOURCE)
    endpoint_guard = _qf(schema, _ENDPOINT_DATASET_GUARD)
    resource_guard = _qf(schema, _DATASET_RESOURCE_GUARD)
    return f"""
    DO $migration$
    DECLARE
        endpoint_guard_count bigint;
        resource_guard_count bigint;
        guard_function_count bigint;
    BEGIN
        SELECT COUNT(*)
          INTO endpoint_guard_count
          FROM pg_catalog.pg_trigger AS trigger_row
         WHERE trigger_row.tgrelid = {_ql(dataset_ref)}::regclass
           AND trigger_row.tgname =
                   'tin_npi_connector_endpoint_dataset_guard'
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgfoid =
                   {_ql(endpoint_guard + '()')}::regprocedure
           AND trigger_row.tgtype = 31
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NULL
           AND trigger_row.tgnewtable IS NULL;
        IF endpoint_guard_count <> 1 THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_guard_changed'
                USING ERRCODE = '55000';
        END IF;

        SELECT COUNT(*)
          INTO resource_guard_count
          FROM (
                VALUES
                    (
                        'tin_npi_connector_dataset_resource_insert_guard',
                        4::smallint,
                        NULL::name,
                        'new_rows'::name
                    ),
                    (
                        'tin_npi_connector_dataset_resource_update_guard',
                        16::smallint,
                        'old_rows'::name,
                        'new_rows'::name
                    ),
                    (
                        'tin_npi_connector_dataset_resource_delete_guard',
                        8::smallint,
                        'old_rows'::name,
                        NULL::name
                    ),
                    (
                        'tin_npi_connector_dataset_resource_truncate_guard',
                        34::smallint,
                        NULL::name,
                        NULL::name
                    )
               ) AS expected(
                   trigger_name,
                   trigger_type,
                   old_table,
                   new_table
               )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = {_ql(resource_ref)}::regclass
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgfoid =
                   {_ql(resource_guard + '()')}::regprocedure
           AND trigger_row.tgtype = expected.trigger_type
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NOT DISTINCT FROM expected.old_table
           AND trigger_row.tgnewtable IS NOT DISTINCT FROM expected.new_table;
        IF resource_guard_count <> 4 THEN
            RAISE EXCEPTION
                'tin_npi_connector_dataset_resource_guard_changed'
                USING ERRCODE = '55000';
        END IF;

        SELECT COUNT(*)
          INTO guard_function_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
         WHERE function_namespace.nspname = {_ql(schema)}
           AND function_row.proname = ANY(ARRAY[
                {_ql(_ENDPOINT_DATASET_GUARD)},
                {_ql(_DATASET_RESOURCE_GUARD)}
           ]::text[])
           AND function_row.pronargs = 0
           AND function_row.prorettype = 'pg_catalog.trigger'::regtype
           AND function_language.lanname = 'plpgsql'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                   ARRAY['search_path=pg_catalog']::text[]
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.aclexplode(
                       COALESCE(
                           function_row.proacl,
                           pg_catalog.acldefault(
                               'f',
                               function_row.proowner
                           )
                       )
                  ) AS function_acl
                 WHERE function_acl.grantee = 0
                   AND function_acl.privilege_type = 'EXECUTE'
           );
        IF guard_function_count <> 2 THEN
            RAISE EXCEPTION
                'tin_npi_connector_dataset_guard_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _source_guard_shape_fence_sql(
    schema: str,
    *,
    expect_installed: bool,
) -> str:
    source_ref = _qf(schema, _SOURCE)
    guard_ref = _qf(schema, _SOURCE_GUARD)
    expected_trigger_count = 2 if expect_installed else 0
    expected_function_count = 1 if expect_installed else 0
    return f"""
    DO $migration$
    DECLARE
        source_guard_count bigint;
        source_guard_function_count bigint;
    BEGIN
        SELECT COUNT(*)
          INTO source_guard_count
          FROM (VALUES
                (
                    {_ql(_SOURCE_GUARD_TRIGGER)},
                    29::smallint,
                    true,
                    true,
                    true
                ),
                (
                    {_ql(_SOURCE_TRUNCATE_GUARD_TRIGGER)},
                    34::smallint,
                    false,
                    false,
                    false
                )
               ) AS expected(
                    trigger_name,
                    trigger_type,
                    is_constraint,
                    is_deferrable,
                    is_initially_deferred
               )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = {_ql(source_ref)}::regclass
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgfoid =
                   pg_catalog.to_regprocedure({_ql(guard_ref + '()')})
           AND trigger_row.tgtype = expected.trigger_type
           AND (trigger_row.tgconstraint <> 0) = expected.is_constraint
           AND trigger_row.tgdeferrable = expected.is_deferrable
           AND trigger_row.tginitdeferred = expected.is_initially_deferred
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND pg_catalog.octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NULL
           AND trigger_row.tgnewtable IS NULL;
        IF source_guard_count <> {expected_trigger_count} THEN
            RAISE EXCEPTION
                'provider_directory_subset_source_guard_changed'
                USING ERRCODE = '55000';
        END IF;

        SELECT COUNT(*)
          INTO source_guard_function_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
         WHERE function_namespace.nspname = {_ql(schema)}
           AND function_row.proname = {_ql(_SOURCE_GUARD)}
           AND function_row.pronargs = 0
           AND function_row.prorettype = 'pg_catalog.trigger'::regtype
           AND function_language.lanname = 'plpgsql'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                   ARRAY['search_path=pg_catalog']::text[]
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.aclexplode(
                       COALESCE(
                           function_row.proacl,
                           pg_catalog.acldefault('f', function_row.proowner)
                       )
                  ) AS function_acl
                 WHERE function_acl.grantee = 0
                   AND function_acl.privilege_type = 'EXECUTE'
           );
        IF source_guard_function_count <> {expected_function_count} THEN
            RAISE EXCEPTION
                'provider_directory_subset_source_guard_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _canonical_json_runtime_fence_sql() -> str:
    return """
    DO $migration$
    BEGIN
        IF pg_catalog.current_setting('server_encoding') <> 'UTF8'
           OR pg_catalog.to_regprocedure(
                'pg_catalog.sha256(bytea)'
           ) IS NULL THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_digest_runtime_invalid'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _canonical_json_function_sql(schema: str) -> str:
    canonical_ref = _qf(schema, _CANONICAL_JSON_FUNCTION)
    return f"""
    CREATE FUNCTION {canonical_ref}(candidate jsonb)
    RETURNS text
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        candidate_type text;
        candidate_text text;
        canonical_value text;
    BEGIN
        candidate_type := pg_catalog.jsonb_typeof(candidate);
        CASE candidate_type
            WHEN 'null' THEN
                RETURN 'null';
            WHEN 'boolean' THEN
                RETURN candidate::text;
            WHEN 'number' THEN
                candidate_text := candidate::text;
                IF candidate_text !~ '^-?(0|[1-9][0-9]*)$' THEN
                    RAISE EXCEPTION
                        'provider_directory_subset_completion_number_invalid'
                        USING ERRCODE = '22023';
                END IF;
                RETURN candidate_text;
            WHEN 'string' THEN
                candidate_text := candidate #>> '{{}}';
                IF pg_catalog.octet_length(candidate_text) <>
                        pg_catalog.char_length(candidate_text) THEN
                    RAISE EXCEPTION
                        'provider_directory_subset_completion_string_non_ascii'
                        USING ERRCODE = '22023';
                END IF;
                RETURN pg_catalog.replace(
                    pg_catalog.to_json(candidate_text)::text,
                    pg_catalog.chr(127),
                    '\\u007f'
                );
            WHEN 'array' THEN
                SELECT '[' || COALESCE(
                           pg_catalog.string_agg(
                               {canonical_ref}(element.value),
                               ',' ORDER BY element.ordinal
                           ),
                           ''
                       ) || ']'
                  INTO canonical_value
                  FROM pg_catalog.jsonb_array_elements(candidate)
                       WITH ORDINALITY AS element(value, ordinal);
                RETURN canonical_value;
            WHEN 'object' THEN
                IF EXISTS (
                    SELECT 1
                      FROM pg_catalog.jsonb_each(candidate) AS item(key, value)
                     WHERE pg_catalog.octet_length(item.key) <>
                           pg_catalog.char_length(item.key)
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_subset_completion_key_non_ascii'
                        USING ERRCODE = '22023';
                END IF;
                SELECT '{{' || COALESCE(
                           pg_catalog.string_agg(
                               pg_catalog.replace(
                                   pg_catalog.to_json(item.key)::text,
                                   pg_catalog.chr(127),
                                   '\\u007f'
                               )
                               || ':'
                               || {canonical_ref}(item.value),
                               ',' ORDER BY
                                   item.key COLLATE pg_catalog."C"
                           ),
                           ''
                       ) || '}}'
                  INTO canonical_value
                  FROM pg_catalog.jsonb_each(candidate) AS item(key, value);
                RETURN canonical_value;
            ELSE
                RAISE EXCEPTION
                    'provider_directory_subset_completion_json_type_invalid'
                    USING ERRCODE = '22023';
        END CASE;
    END;
    $function$;
    """


def _canonical_sha256_function_sql(schema: str) -> str:
    canonical_ref = _qf(schema, _CANONICAL_JSON_FUNCTION)
    sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    return f"""
    CREATE FUNCTION {sha256_ref}(candidate jsonb)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    {canonical_ref}(candidate),
                    'UTF8'
                )
            ),
            'hex'
        );
    $function$;
    """


def _payload_canonical_json_function_sql(schema: str) -> str:
    canonical_ref = _qf(schema, _PAYLOAD_CANONICAL_JSON_FUNCTION)
    return f"""
    CREATE FUNCTION {canonical_ref}(candidate jsonb)
    RETURNS text
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        candidate_type text;
        candidate_text text;
        canonical_value text;
    BEGIN
        candidate_type := pg_catalog.jsonb_typeof(candidate);
        CASE candidate_type
            WHEN 'null' THEN
                RETURN 'null';
            WHEN 'boolean' THEN
                RETURN candidate::text;
            WHEN 'number' THEN
                candidate_text := pg_catalog.trim_scale(
                    candidate::text::numeric
                )::text;
                RETURN CASE
                    WHEN candidate_text IN ('-0', '0') THEN '0'
                    ELSE candidate_text
                END;
            WHEN 'string' THEN
                RETURN pg_catalog.to_json(candidate #>> '{{}}')::text;
            WHEN 'array' THEN
                SELECT '[' || COALESCE(
                           pg_catalog.string_agg(
                               {canonical_ref}(element.value),
                               ',' ORDER BY element.ordinal
                           ),
                           ''
                       ) || ']'
                  INTO canonical_value
                  FROM pg_catalog.jsonb_array_elements(candidate)
                       WITH ORDINALITY AS element(value, ordinal);
                RETURN canonical_value;
            WHEN 'object' THEN
                SELECT '{{' || COALESCE(
                           pg_catalog.string_agg(
                               pg_catalog.to_json(item.key)::text
                               || ':'
                               || {canonical_ref}(item.value),
                               ',' ORDER BY
                                   item.key COLLATE pg_catalog."C"
                           ),
                           ''
                       ) || '}}'
                  INTO canonical_value
                  FROM pg_catalog.jsonb_each(candidate) AS item(key, value);
                RETURN canonical_value;
            ELSE
                RAISE EXCEPTION
                    'provider_directory_subset_payload_json_type_invalid'
                    USING ERRCODE = '22023';
        END CASE;
    END;
    $function$;
    """


def _payload_sha256_function_sql(schema: str) -> str:
    canonical_ref = _qf(schema, _PAYLOAD_CANONICAL_JSON_FUNCTION)
    sha256_ref = _qf(schema, _PAYLOAD_SHA256_FUNCTION)
    return f"""
    CREATE FUNCTION {sha256_ref}(candidate jsonb)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    {canonical_ref}(candidate),
                    'UTF8'
                )
            ),
            'hex'
        );
    $function$;
    """


def _proof_pair_valid_function_sql(schema: str) -> str:
    sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    pair_valid_ref = _qf(schema, _PROOF_PAIR_VALID_FUNCTION)
    return f"""
    CREATE FUNCTION {pair_valid_ref}(
        candidate jsonb,
        candidate_sha256 text
    ) RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
        SELECT candidate_sha256 ~ '^[0-9a-f]{{64}}$'
           AND candidate_sha256 = {sha256_ref}(candidate);
    $function$;
    """


def _proof_shape_valid_function_sql(
    schema: str,
    *,
    replace_existing: bool = False,
    reviewed_subset_profile_aware: bool = False,
    reviewed_subset_terminal_window_profile_aware: bool = False,
) -> str:
    proof_valid_ref = _qf(schema, _PROOF_SHAPE_VALID_FUNCTION)
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    resource_types_sql = ", ".join(_ql(value) for value in _SUBSET_RESOURCE_TYPES)
    resource_fields_sql = ", ".join(_ql(value) for value in _SUBSET_RESOURCE_FIELDS)
    numeric_fields_sql = ", ".join(
        _ql(value)
        for value in (
            "advertised_pre",
            "advertised_post",
            "returned_unique",
            "deficit",
            "geometry_version",
            "page_count",
            "pages",
            "processed_rows",
            "logical_terminal_offset",
            "logical_window_end_offset",
            "terminal_entries",
            "sparse_pages",
            "empty_pages",
        )
    )
    create_function = (
        "CREATE OR REPLACE FUNCTION"
        if replace_existing
        else "CREATE FUNCTION"
    )
    exact_profile_sql = """
        IF candidate ->> 'strategy_version' =
                'provider-directory-fhir-server-issued-traversal-subset-v3'
           AND candidate -> 'completion_scopes' =
                '["advertised-count-stability",'
                '"source-issued-continuation",'
                '"returned-resource-content"]'::jsonb THEN
            max_advertised_count_decrease := 0;
    """
    bounded_profile_sql = """
        ELSIF candidate ->> 'strategy_version' =
                'provider-directory-fhir-server-issued-traversal-subset-v4'
           AND candidate -> 'completion_scopes' =
                '["advertised-count-monotone-decrease-at-most-one",'
                '"source-issued-continuation",'
                '"returned-resource-content"]'::jsonb THEN
            max_advertised_count_decrease := 1;
    """
    terminal_window_profile_sql = """
        ELSIF candidate ->> 'strategy_version' =
                'provider-directory-fhir-server-issued-traversal-subset-v5'
           AND candidate -> 'completion_scopes' =
                '["advertised-count-monotone-decrease-bounded-by-one-percent-and-twenty-pages",'
                '"terminal-logical-window-covers-advertised-pre",'
                '"source-issued-continuation",'
                '"returned-resource-content"]'::jsonb THEN
            max_advertised_count_decrease := 0;
            terminal_count_window_required := TRUE;
    """
    profile_sql = (
        exact_profile_sql
        + (bounded_profile_sql if reviewed_subset_profile_aware else "")
        + (
            terminal_window_profile_sql
            if reviewed_subset_terminal_window_profile_aware
            else ""
        )
        + """
        ELSE
            RETURN FALSE;
        END IF;
        """
        if (
            reviewed_subset_profile_aware
            or reviewed_subset_terminal_window_profile_aware
        )
        else "max_advertised_count_decrease := 0;"
    )
    terminal_window_declaration_sql = (
        "terminal_count_window_required boolean := FALSE;"
        if reviewed_subset_terminal_window_profile_aware
        else ""
    )
    terminal_window_limit_sql = (
        """
            IF terminal_count_window_required THEN
                max_advertised_count_decrease := LEAST(
                    page_count * 20,
                    pg_catalog.ceil(advertised_pre / 100::numeric)
                );
            END IF;
        """
        if reviewed_subset_terminal_window_profile_aware
        else ""
    )
    terminal_window_geometry_sql = (
        """
               OR (
                    terminal_count_window_required
                    AND (
                        (resource_value ->> 'logical_terminal_offset')::numeric
                            > advertised_pre
                        OR advertised_pre >
                            (resource_value ->>
                                'logical_window_end_offset')::numeric
                    )
               )
        """
        if reviewed_subset_terminal_window_profile_aware
        else ""
    )
    return f"""
    {create_function} {proof_valid_ref}(
        candidate jsonb,
        expected_dataset_hash text,
        expected_resource_count bigint
    ) RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        dataset_value jsonb;
        map_name text;
        map_value jsonb;
        resource_type text;
        resource_value jsonb;
        field_name text;
        entry_value jsonb;
        shape_value jsonb;
        geometry_value jsonb;
        page_count numeric;
        pages numeric;
        advertised_pre numeric;
        advertised_post numeric;
        returned_unique numeric;
        deficit numeric;
        processed_rows numeric;
        sparse_pages numeric;
        empty_pages numeric;
        terminal_entries numeric;
        page_row_sum numeric;
        sparse_page_count numeric;
        empty_page_count numeric;
        dataset_resource_count numeric;
        dataset_count_sum numeric := 0;
        max_advertised_count_decrease numeric;
        {terminal_window_declaration_sql}
        cutoff_value text;
    BEGIN
        {profile_sql}
        IF pg_catalog.jsonb_typeof(candidate) IS DISTINCT FROM 'object'
           OR pg_catalog.jsonb_typeof(candidate -> 'page_count')
                IS DISTINCT FROM 'number'
           OR candidate ->> 'page_count' !~ '^([1-9][0-9]{{0,2}}|1000)$'
           OR pg_catalog.jsonb_typeof(candidate -> 'resources')
                IS DISTINCT FROM 'object'
           OR NOT (candidate -> 'resources' ?&
                   ARRAY[{resource_types_sql}]::text[])
           OR (candidate -> 'resources') -
                   ARRAY[{resource_types_sql}]::text[] <> '{{}}'::jsonb
           OR pg_catalog.jsonb_typeof(candidate -> 'dataset')
                IS DISTINCT FROM 'object' THEN
            RETURN FALSE;
        END IF;
        cutoff_value := candidate ->> 'cutoff';
        IF pg_catalog.jsonb_typeof(candidate -> 'cutoff')
                IS DISTINCT FROM 'string'
           OR cutoff_value !~
                '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:'
                '[0-9]{{2}}:[0-9]{{2}}\\.[0-9]{{6}}Z$'
           OR pg_catalog.to_char(
                cutoff_value::timestamptz AT TIME ZONE 'UTC',
                'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
              ) IS DISTINCT FROM cutoff_value THEN
            RETURN FALSE;
        END IF;
        page_count := (candidate ->> 'page_count')::numeric;
        dataset_value := candidate -> 'dataset';
        IF NOT (dataset_value ?& ARRAY[
                'hash', 'count', 'resource_hashes', 'resource_counts',
                'acquired_resource_hashes'
           ]::text[])
           OR dataset_value - ARRAY[
                'hash', 'count', 'resource_hashes', 'resource_counts',
                'acquired_resource_hashes'
           ]::text[] <> '{{}}'::jsonb
           OR pg_catalog.jsonb_typeof(dataset_value -> 'hash')
                IS DISTINCT FROM 'string'
           OR dataset_value ->> 'hash' !~ '^[0-9a-f]{{64}}$'
           OR dataset_value ->> 'hash' IS DISTINCT FROM expected_dataset_hash
           OR pg_catalog.jsonb_typeof(dataset_value -> 'count')
                IS DISTINCT FROM 'number'
           OR dataset_value ->> 'count' !~ '^(0|[1-9][0-9]*)$'
           OR (dataset_value ->> 'count')::numeric
                IS DISTINCT FROM expected_resource_count::numeric THEN
            RETURN FALSE;
        END IF;
        FOREACH map_name IN ARRAY ARRAY[
            'resource_hashes', 'resource_counts',
            'acquired_resource_hashes'
        ]::text[] LOOP
            map_value := dataset_value -> map_name;
            IF pg_catalog.jsonb_typeof(map_value) IS DISTINCT FROM 'object'
               OR NOT (map_value ?& ARRAY[{resource_types_sql}]::text[])
               OR map_value - ARRAY[{resource_types_sql}]::text[] <>
                    '{{}}'::jsonb THEN
                RETURN FALSE;
            END IF;
        END LOOP;
        FOREACH resource_type IN ARRAY
                ARRAY[{resource_types_sql}]::text[] LOOP
            resource_value := candidate -> 'resources' -> resource_type;
            IF pg_catalog.jsonb_typeof(resource_value)
                    IS DISTINCT FROM 'object'
               OR NOT (resource_value ?&
                       ARRAY[{resource_fields_sql}]::text[])
               OR resource_value - ARRAY[{resource_fields_sql}]::text[] <>
                    '{{}}'::jsonb THEN
                RETURN FALSE;
            END IF;
            FOREACH field_name IN ARRAY
                    ARRAY[{numeric_fields_sql}]::text[] LOOP
                IF pg_catalog.jsonb_typeof(resource_value -> field_name)
                        IS DISTINCT FROM 'number'
                   OR resource_value ->> field_name !~
                        '^(0|[1-9][0-9]*)$' THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            advertised_pre :=
                (resource_value ->> 'advertised_pre')::numeric;
            advertised_post :=
                (resource_value ->> 'advertised_post')::numeric;
            returned_unique :=
                (resource_value ->> 'returned_unique')::numeric;
            deficit := (resource_value ->> 'deficit')::numeric;
            pages := (resource_value ->> 'pages')::numeric;
            processed_rows :=
                (resource_value ->> 'processed_rows')::numeric;
            sparse_pages :=
                (resource_value ->> 'sparse_pages')::numeric;
            empty_pages :=
                (resource_value ->> 'empty_pages')::numeric;
            terminal_entries :=
                (resource_value ->> 'terminal_entries')::numeric;
            {terminal_window_limit_sql}
            IF advertised_post > advertised_pre
               OR advertised_pre - advertised_post >
                    max_advertised_count_decrease
               OR returned_unique > advertised_post
               OR deficit IS DISTINCT FROM advertised_pre - returned_unique
               OR (resource_value ->> 'geometry_version')::numeric <> 2
               OR (resource_value ->> 'page_count')::numeric <>
                    page_count
               OR pages <= 0
               OR processed_rows IS DISTINCT FROM returned_unique
               OR (resource_value ->> 'logical_terminal_offset')::numeric <>
                    (pages - 1) * page_count
               OR (resource_value ->> 'logical_window_end_offset')::numeric <>
                    pages * page_count
               {terminal_window_geometry_sql}
               OR terminal_entries > page_count
               OR returned_unique > pages * page_count
               OR sparse_pages > pages
               OR empty_pages > sparse_pages
               OR resource_value ->> 'terminal_reason' IS DISTINCT FROM
                    'source_no_next' THEN
                RETURN FALSE;
            END IF;
            FOREACH field_name IN ARRAY ARRAY[
                'geometry_sha256', 'content_sha256',
                'acquired_content_sha256',
                'continuation_shape_chain_sha256'
            ]::text[] LOOP
                IF pg_catalog.jsonb_typeof(resource_value -> field_name)
                        IS DISTINCT FROM 'string'
                   OR resource_value ->> field_name !~
                        '^[0-9a-f]{{64}}$' THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            IF pg_catalog.jsonb_typeof(
                    resource_value -> 'page_entry_counts'
               ) IS DISTINCT FROM 'array'
               OR pg_catalog.jsonb_array_length(
                    resource_value -> 'page_entry_counts'
               )::numeric <> pages
               OR pg_catalog.jsonb_typeof(
                    resource_value -> 'continuation_shape_sha256'
               ) IS DISTINCT FROM 'array'
               OR pg_catalog.jsonb_array_length(
                    resource_value -> 'continuation_shape_sha256'
               )::numeric <> pages - 1 THEN
                RETURN FALSE;
            END IF;
            page_row_sum := 0;
            sparse_page_count := 0;
            empty_page_count := 0;
            FOR entry_value IN SELECT value FROM
                    pg_catalog.jsonb_array_elements(
                        resource_value -> 'page_entry_counts'
                    ) LOOP
                IF pg_catalog.jsonb_typeof(entry_value)
                        IS DISTINCT FROM 'number'
                   OR entry_value #>> '{{}}' !~ '^(0|[1-9][0-9]*)$'
                   OR (entry_value #>> '{{}}')::numeric > page_count THEN
                    RETURN FALSE;
                END IF;
                page_row_sum := page_row_sum +
                    (entry_value #>> '{{}}')::numeric;
                IF (entry_value #>> '{{}}')::numeric < page_count THEN
                    sparse_page_count := sparse_page_count + 1;
                END IF;
                IF (entry_value #>> '{{}}')::numeric = 0 THEN
                    empty_page_count := empty_page_count + 1;
                END IF;
            END LOOP;
            IF page_row_sum IS DISTINCT FROM processed_rows
               OR sparse_page_count IS DISTINCT FROM sparse_pages
               OR empty_page_count IS DISTINCT FROM empty_pages
               OR (resource_value -> 'page_entry_counts' ->> -1)::numeric
                    IS DISTINCT FROM terminal_entries THEN
                RETURN FALSE;
            END IF;
            FOR shape_value IN SELECT value FROM
                    pg_catalog.jsonb_array_elements(
                        resource_value -> 'continuation_shape_sha256'
                    ) LOOP
                IF pg_catalog.jsonb_typeof(shape_value)
                        IS DISTINCT FROM 'string'
                   OR shape_value #>> '{{}}' !~ '^[0-9a-f]{{64}}$' THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            IF {canonical_sha256_ref}(
                    resource_value -> 'continuation_shape_sha256'
               ) IS DISTINCT FROM resource_value ->>
                    'continuation_shape_chain_sha256' THEN
                RETURN FALSE;
            END IF;
            geometry_value := pg_catalog.jsonb_build_object(
                'empty_pages', empty_pages,
                'logical_window_end_offset',
                    (resource_value ->> 'logical_window_end_offset')::numeric,
                'page_count', page_count,
                'pages_processed', pages,
                'processed_rows', processed_rows,
                'page_entry_counts',
                    resource_value -> 'page_entry_counts',
                'sparse_pages', sparse_pages,
                'terminal_page_entries', terminal_entries,
                'terminal_page_start_offset',
                    (resource_value ->> 'logical_terminal_offset')::numeric,
                'version', 2
            );
            IF {canonical_sha256_ref}(geometry_value) IS DISTINCT FROM
                    resource_value ->> 'geometry_sha256' THEN
                RETURN FALSE;
            END IF;
            IF pg_catalog.jsonb_typeof(
                    dataset_value -> 'resource_counts' -> resource_type
               ) IS DISTINCT FROM 'number'
               OR dataset_value -> 'resource_counts' ->> resource_type !~
                    '^(0|[1-9][0-9]*)$' THEN
                RETURN FALSE;
            END IF;
            dataset_resource_count := (
                dataset_value -> 'resource_counts' ->> resource_type
            )::numeric;
            IF dataset_resource_count IS DISTINCT FROM returned_unique
               OR pg_catalog.jsonb_typeof(
                    dataset_value -> 'resource_hashes' -> resource_type
               ) IS DISTINCT FROM 'string'
               OR dataset_value -> 'resource_hashes' ->> resource_type !~
                    '^[0-9a-f]{{64}}$'
               OR dataset_value -> 'resource_hashes' ->> resource_type
                    IS DISTINCT FROM resource_value ->> 'content_sha256'
               OR pg_catalog.jsonb_typeof(
                    dataset_value -> 'acquired_resource_hashes'
                        -> resource_type
               ) IS DISTINCT FROM 'string'
               OR dataset_value -> 'acquired_resource_hashes'
                    ->> resource_type !~ '^[0-9a-f]{{64}}$'
               OR dataset_value -> 'acquired_resource_hashes'
                    ->> resource_type IS DISTINCT FROM
                        resource_value ->> 'acquired_content_sha256' THEN
                RETURN FALSE;
            END IF;
            dataset_count_sum := dataset_count_sum + dataset_resource_count;
        END LOOP;
        RETURN dataset_count_sum IS NOT DISTINCT FROM
            expected_resource_count::numeric;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
    $function$;
    """


def _replay_shape_valid_function_sql(schema: str) -> str:
    replay_valid_ref = _qf(schema, _REPLAY_SHAPE_VALID_FUNCTION)
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    resource_types_sql = ", ".join(_ql(value) for value in _SUBSET_RESOURCE_TYPES)
    replay_fields_sql = ", ".join(
        _ql(value) for value in _SUBSET_REPLAY_RESOURCE_FIELDS
    )
    return f"""
    CREATE FUNCTION {replay_valid_ref}(
        replay_candidate jsonb,
        replay_sha256 text,
        completion_candidate jsonb,
        completion_sha256 text
    ) RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        resource_type text;
        replay_resource jsonb;
        completion_resource jsonb;
        digest_value jsonb;
        pages numeric;
    BEGIN
        IF pg_catalog.jsonb_typeof(replay_candidate)
                IS DISTINCT FROM 'object'
           OR NOT (replay_candidate ?& ARRAY[
                'version', 'completion_proof_sha256', 'resources'
           ]::text[])
           OR replay_candidate - ARRAY[
                'version', 'completion_proof_sha256', 'resources'
           ]::text[] <> '{{}}'::jsonb
           OR replay_candidate ->> 'version' IS DISTINCT FROM
                'provider-directory-fhir-server-issued-replay-evidence-v1'
           OR replay_candidate ->> 'completion_proof_sha256'
                IS DISTINCT FROM completion_sha256
           OR replay_sha256 !~ '^[0-9a-f]{{64}}$'
           OR completion_sha256 !~ '^[0-9a-f]{{64}}$'
           OR {canonical_sha256_ref}(replay_candidate)
                IS DISTINCT FROM replay_sha256
           OR pg_catalog.jsonb_typeof(replay_candidate -> 'resources')
                IS DISTINCT FROM 'object'
           OR NOT (replay_candidate -> 'resources' ?&
                   ARRAY[{resource_types_sql}]::text[])
           OR (replay_candidate -> 'resources') -
                   ARRAY[{resource_types_sql}]::text[] <> '{{}}'::jsonb
           OR pg_catalog.jsonb_typeof(completion_candidate -> 'resources')
                IS DISTINCT FROM 'object' THEN
            RETURN FALSE;
        END IF;
        FOREACH resource_type IN ARRAY
                ARRAY[{resource_types_sql}]::text[] LOOP
            replay_resource :=
                replay_candidate -> 'resources' -> resource_type;
            completion_resource :=
                completion_candidate -> 'resources' -> resource_type;
            IF pg_catalog.jsonb_typeof(replay_resource)
                    IS DISTINCT FROM 'object'
               OR NOT (replay_resource ?& ARRAY[{replay_fields_sql}]::text[])
               OR replay_resource - ARRAY[{replay_fields_sql}]::text[] <>
                    '{{}}'::jsonb
               OR pg_catalog.jsonb_typeof(replay_resource -> 'pages')
                    IS DISTINCT FROM 'number'
               OR replay_resource ->> 'pages' !~ '^[1-9][0-9]*$'
               OR pg_catalog.jsonb_typeof(completion_resource -> 'pages')
                    IS DISTINCT FROM 'number'
               OR replay_resource -> 'pages' IS DISTINCT FROM
                    completion_resource -> 'pages' THEN
                RETURN FALSE;
            END IF;
            pages := (replay_resource ->> 'pages')::numeric;
            IF pg_catalog.jsonb_typeof(
                    replay_resource -> 'continuation_hop_sha256'
               ) IS DISTINCT FROM 'array'
               OR pg_catalog.jsonb_array_length(
                    replay_resource -> 'continuation_hop_sha256'
               )::numeric <> pages - 1
               OR pg_catalog.jsonb_typeof(
                    replay_resource -> 'continuation_shape_sha256'
               ) IS DISTINCT FROM 'array'
               OR replay_resource -> 'continuation_shape_sha256'
                    IS DISTINCT FROM completion_resource
                        -> 'continuation_shape_sha256' THEN
                RETURN FALSE;
            END IF;
            FOREACH digest_value IN ARRAY ARRAY[
                replay_resource -> 'continuation_hop_chain_sha256',
                replay_resource -> 'continuation_shape_chain_sha256'
            ] LOOP
                IF pg_catalog.jsonb_typeof(digest_value)
                        IS DISTINCT FROM 'string'
                   OR digest_value #>> '{{}}' !~ '^[0-9a-f]{{64}}$' THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            FOR digest_value IN SELECT value FROM
                    pg_catalog.jsonb_array_elements(
                        replay_resource -> 'continuation_hop_sha256'
                    ) LOOP
                IF pg_catalog.jsonb_typeof(digest_value)
                        IS DISTINCT FROM 'string'
                   OR digest_value #>> '{{}}' !~ '^[0-9a-f]{{64}}$' THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            IF {canonical_sha256_ref}(
                    replay_resource -> 'continuation_hop_sha256'
               ) IS DISTINCT FROM replay_resource ->>
                    'continuation_hop_chain_sha256'
               OR {canonical_sha256_ref}(
                    replay_resource -> 'continuation_shape_sha256'
               ) IS DISTINCT FROM replay_resource ->>
                    'continuation_shape_chain_sha256' THEN
                RETURN FALSE;
            END IF;
        END LOOP;
        RETURN TRUE;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
    $function$;
    """


def _coverage_shape_valid_function_sql(
    schema: str,
    *,
    replace_existing: bool = False,
    reviewed_root_policy_aware: bool = False,
) -> str:
    coverage_valid_ref = _qf(schema, _COVERAGE_SHAPE_VALID_FUNCTION)
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    resource_types_sql = ", ".join(_ql(value) for value in _SUBSET_RESOURCE_TYPES)
    create_function = (
        "CREATE OR REPLACE FUNCTION"
        if replace_existing
        else "CREATE FUNCTION"
    )
    expected_resource_twin_state = (
        "(CASE WHEN expected_twin_state = 'not_required' "
        "THEN 'not_required' ELSE 'pending_matching_reviewed_root' END)"
        if reviewed_root_policy_aware
        else "'pending_matching_reviewed_root'"
    )
    return f"""
    {create_function} {coverage_valid_ref}(
        coverage_candidate jsonb,
        completion_candidate jsonb,
        completion_sha256 text,
        expected_twin_state text
    ) RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        resource_type text;
        resource_coverage jsonb;
        resource_proof jsonb;
        geometry_value jsonb;
        continuation_value jsonb;
        count_name text;
        aggregate_value numeric;
        unresolved_counts jsonb;
    BEGIN
        IF pg_catalog.jsonb_typeof(coverage_candidate)
                IS DISTINCT FROM 'object'
           OR NOT (coverage_candidate ?& ARRAY[
                'cutoff', 'scope', 'advertised_pre', 'advertised_post',
                'returned_unique', 'deficit', 'resources',
                'traversal_complete', 'twin_state', 'proof_sha256',
                'unresolved_reference_count', 'unresolved_reference_counts',
                'missing_target_semantics', 'absence_semantics',
                'publication_state_at_completion'
           ]::text[])
           OR coverage_candidate - ARRAY[
                'cutoff', 'scope', 'advertised_pre', 'advertised_post',
                'returned_unique', 'deficit', 'resources',
                'traversal_complete', 'twin_state', 'proof_sha256',
                'unresolved_reference_count', 'unresolved_reference_counts',
                'missing_target_semantics', 'absence_semantics',
                'publication_state_at_completion'
           ]::text[] <> '{{}}'::jsonb
           OR coverage_candidate ->> 'cutoff' IS DISTINCT FROM
                completion_candidate ->> 'cutoff'
           OR coverage_candidate ->> 'scope' IS DISTINCT FROM
                'server_issued_traversal_subset'
           OR coverage_candidate -> 'traversal_complete'
                IS DISTINCT FROM 'true'::jsonb
           OR coverage_candidate ->> 'twin_state'
                IS DISTINCT FROM expected_twin_state
           OR coverage_candidate ->> 'proof_sha256'
                IS DISTINCT FROM completion_sha256
           OR coverage_candidate ->> 'missing_target_semantics'
                IS DISTINCT FROM 'preserved_not_synthesized'
           OR coverage_candidate ->> 'absence_semantics'
                IS DISTINCT FROM 'unknown_under_subset'
           OR coverage_candidate ->> 'publication_state_at_completion'
                IS DISTINCT FROM 'not_published'
           OR pg_catalog.jsonb_typeof(coverage_candidate -> 'resources')
                IS DISTINCT FROM 'object'
           OR NOT (coverage_candidate -> 'resources' ?&
                   ARRAY[{resource_types_sql}]::text[])
           OR (coverage_candidate -> 'resources') -
                   ARRAY[{resource_types_sql}]::text[] <> '{{}}'::jsonb THEN
            RETURN FALSE;
        END IF;
        FOREACH count_name IN ARRAY ARRAY[
            'advertised_pre', 'advertised_post', 'returned_unique', 'deficit'
        ]::text[] LOOP
            IF pg_catalog.jsonb_typeof(coverage_candidate -> count_name)
                    IS DISTINCT FROM 'number'
               OR coverage_candidate ->> count_name !~
                    '^(0|[1-9][0-9]*)$' THEN
                RETURN FALSE;
            END IF;
            SELECT pg_catalog.sum((resource.value ->> count_name)::numeric)
              INTO aggregate_value
              FROM pg_catalog.jsonb_each(
                    completion_candidate -> 'resources'
              ) AS resource(key, value);
            IF (coverage_candidate ->> count_name)::numeric
                    IS DISTINCT FROM aggregate_value THEN
                RETURN FALSE;
            END IF;
        END LOOP;
        FOREACH resource_type IN ARRAY
                ARRAY[{resource_types_sql}]::text[] LOOP
            resource_coverage :=
                coverage_candidate -> 'resources' -> resource_type;
            resource_proof :=
                completion_candidate -> 'resources' -> resource_type;
            IF pg_catalog.jsonb_typeof(resource_coverage)
                    IS DISTINCT FROM 'object'
               OR NOT (resource_coverage ?& ARRAY[
                    'cutoff', 'scope', 'advertised_pre', 'advertised_post',
                    'returned_unique', 'deficit', 'geometry', 'continuation',
                    'twin_state', 'proof_state',
                    'unresolved_reference_count', 'absence_semantics'
               ]::text[])
               OR resource_coverage - ARRAY[
                    'cutoff', 'scope', 'advertised_pre', 'advertised_post',
                    'returned_unique', 'deficit', 'geometry', 'continuation',
                    'twin_state', 'proof_state',
                    'unresolved_reference_count', 'absence_semantics'
               ]::text[] <> '{{}}'::jsonb
               OR resource_coverage ->> 'cutoff' IS DISTINCT FROM
                    completion_candidate ->> 'cutoff'
               OR resource_coverage ->> 'scope' IS DISTINCT FROM
                    'server_issued_traversal_subset'
               OR resource_coverage ->> 'twin_state' IS DISTINCT FROM
                    {expected_resource_twin_state}
               OR resource_coverage ->> 'proof_state' IS DISTINCT FROM
                    'resource_terminal_verified'
               OR resource_coverage -> 'unresolved_reference_count'
                    IS DISTINCT FROM 'null'::jsonb
               OR resource_coverage ->> 'absence_semantics'
                    IS DISTINCT FROM 'unknown_under_subset' THEN
                RETURN FALSE;
            END IF;
            FOREACH count_name IN ARRAY ARRAY[
                'advertised_pre', 'advertised_post',
                'returned_unique', 'deficit'
            ]::text[] LOOP
                IF resource_coverage -> count_name IS DISTINCT FROM
                        resource_proof -> count_name THEN
                    RETURN FALSE;
                END IF;
            END LOOP;
            geometry_value := resource_coverage -> 'geometry';
            IF pg_catalog.jsonb_typeof(geometry_value)
                    IS DISTINCT FROM 'object'
               OR NOT (geometry_value ?& ARRAY[
                    'pages', 'logical_terminal_offset', 'sparse_pages',
                    'empty_pages', 'page_entry_counts_sha256',
                    'geometry_sha256'
               ]::text[])
               OR geometry_value - ARRAY[
                    'pages', 'logical_terminal_offset', 'sparse_pages',
                    'empty_pages', 'page_entry_counts_sha256',
                    'geometry_sha256'
               ]::text[] <> '{{}}'::jsonb
               OR geometry_value -> 'pages' IS DISTINCT FROM
                    resource_proof -> 'pages'
               OR geometry_value -> 'logical_terminal_offset'
                    IS DISTINCT FROM
                        resource_proof -> 'logical_terminal_offset'
               OR geometry_value -> 'sparse_pages' IS DISTINCT FROM
                    resource_proof -> 'sparse_pages'
               OR geometry_value -> 'empty_pages' IS DISTINCT FROM
                    resource_proof -> 'empty_pages'
               OR geometry_value ->> 'page_entry_counts_sha256'
                    IS DISTINCT FROM {canonical_sha256_ref}(
                        resource_proof -> 'page_entry_counts'
                    )
               OR geometry_value ->> 'geometry_sha256'
                    IS DISTINCT FROM resource_proof ->> 'geometry_sha256'
                    THEN
                RETURN FALSE;
            END IF;
            continuation_value := resource_coverage -> 'continuation';
            IF pg_catalog.jsonb_typeof(continuation_value)
                    IS DISTINCT FROM 'object'
               OR NOT (continuation_value ?&
                       ARRAY['validated_hops', 'chain_sha256']::text[])
               OR continuation_value -
                       ARRAY['validated_hops', 'chain_sha256']::text[] <>
                    '{{}}'::jsonb
               OR continuation_value -> 'validated_hops' IS DISTINCT FROM
                    pg_catalog.to_jsonb(
                        (resource_proof ->> 'pages')::numeric - 1
                    )
               OR continuation_value ->> 'chain_sha256' IS DISTINCT FROM
                    resource_proof ->> 'continuation_shape_chain_sha256'
                    THEN
                RETURN FALSE;
            END IF;
        END LOOP;
        unresolved_counts :=
            coverage_candidate -> 'unresolved_reference_counts';
        IF (coverage_candidate -> 'unresolved_reference_count' = 'null'::jsonb)
                IS DISTINCT FROM (unresolved_counts = 'null'::jsonb) THEN
            RETURN FALSE;
        END IF;
        IF unresolved_counts <> 'null'::jsonb THEN
            IF pg_catalog.jsonb_typeof(unresolved_counts)
                    IS DISTINCT FROM 'object'
               OR NOT (unresolved_counts ?& ARRAY[
                    'dataset_network_plan',
                    'dataset_affiliation_organization'
               ]::text[])
               OR unresolved_counts - ARRAY[
                    'dataset_network_plan',
                    'dataset_affiliation_organization'
               ]::text[] <> '{{}}'::jsonb
               OR EXISTS (
                    SELECT 1
                      FROM pg_catalog.jsonb_each(unresolved_counts)
                           AS unresolved(key, value)
                     WHERE pg_catalog.jsonb_typeof(unresolved.value)
                                IS DISTINCT FROM 'number'
                        OR unresolved.value #>> '{{}}' !~
                                '^(0|[1-9][0-9]*)$'
               )
               OR (coverage_candidate ->>
                        'unresolved_reference_count')::numeric
                    IS DISTINCT FROM (
                        SELECT pg_catalog.sum(
                                   (unresolved.value #>> '{{}}')::numeric
                               )
                          FROM pg_catalog.jsonb_each(unresolved_counts)
                               AS unresolved(key, value)
                    ) THEN
                RETURN FALSE;
            END IF;
        END IF;
        RETURN TRUE;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
    $function$;
    """


def _content_proof_valid_function_sql(
    schema: str,
    *,
    replace_existing: bool = False,
) -> str:
    """Validate the complete stored-content proof used by one-root policy."""

    function_ref = _qf(schema, _CONTENT_PROOF_VALID_FUNCTION)
    canonical_json_ref = _qf(schema, _CANONICAL_JSON_FUNCTION)
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    create_function = (
        "CREATE OR REPLACE FUNCTION"
        if replace_existing
        else "CREATE FUNCTION"
    )
    proof_fields = (
        "contract_id", "complete", "dataset_id", "endpoint_id",
        "acquisition_root_run_id", "source_ids", "selected_resources",
        "dataset_hash", "resource_count", "resource_hashes",
        "resource_counts", "source_metrics", "npi_set_sha256",
        "shard_count", "shard_set_sha256", "shards", "proof_sha256",
    )
    descriptor_fields = (
        "shard_id", "dataset_id", "endpoint_id",
        "acquisition_root_run_id", "source_ids", "resource_count",
        "resource_counts", "first_identity", "last_identity",
        "input_sha256", "artifact_sha256", "artifact_byte_count",
    )
    proof_fields_sql = ", ".join(_ql(value) for value in proof_fields)
    descriptor_fields_sql = ", ".join(
        _ql(value) for value in descriptor_fields
    )
    metric_fields_sql = ", ".join(
        _ql(value)
        for value in (
            "address_records", "addressed_locations",
            "distinct_npis", "geocoded_locations",
        )
    )
    return f"""
    {create_function} {function_ref}(
        candidate jsonb,
        expected_dataset_id text,
        expected_endpoint_id text,
        expected_root_run_id text,
        expected_source_ids jsonb,
        expected_selected_resources jsonb,
        expected_dataset_hash text,
        expected_resource_count bigint,
        expected_resource_hashes jsonb,
        expected_resource_counts jsonb
    ) RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    STRICT
    PARALLEL SAFE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        descriptor jsonb;
        descriptor_index integer;
        previous_shard_id text := NULL;
        current_shard_id text;
        descriptor_count numeric;
        descriptor_count_sum numeric := 0;
        expected_shard_id text;
        computed_shard_set_sha256 text;
        resource_count_sum numeric;
        metric_value jsonb;
    BEGIN
        IF pg_catalog.jsonb_typeof(candidate) IS DISTINCT FROM 'object'
           OR NOT (candidate ?& ARRAY[{proof_fields_sql}]::text[])
           OR candidate - ARRAY[{proof_fields_sql}]::text[] <> '{{}}'::jsonb
           OR candidate ->> 'contract_id' IS DISTINCT FROM
                {_ql(_CONTENT_PROOF_CONTRACT)}
           OR candidate -> 'complete' IS DISTINCT FROM 'true'::jsonb
           OR candidate ->> 'dataset_id' IS DISTINCT FROM expected_dataset_id
           OR candidate ->> 'endpoint_id' IS DISTINCT FROM expected_endpoint_id
           OR candidate ->> 'acquisition_root_run_id' IS DISTINCT FROM
                expected_root_run_id
           OR candidate -> 'source_ids' IS DISTINCT FROM expected_source_ids
           OR candidate -> 'selected_resources' IS DISTINCT FROM
                expected_selected_resources
           OR candidate ->> 'dataset_hash' IS DISTINCT FROM
                expected_dataset_hash
           OR candidate -> 'resource_count' IS DISTINCT FROM
                pg_catalog.to_jsonb(expected_resource_count)
           OR candidate -> 'resource_hashes' IS DISTINCT FROM
                expected_resource_hashes
           OR candidate -> 'resource_counts' IS DISTINCT FROM
                expected_resource_counts
           OR candidate ->> 'npi_set_sha256' !~ '^[0-9a-f]{{64}}$'
           OR candidate ->> 'shard_set_sha256' !~ '^[0-9a-f]{{64}}$'
           OR candidate ->> 'proof_sha256' !~ '^[0-9a-f]{{64}}$'
           OR {canonical_sha256_ref}(candidate - 'proof_sha256')
                IS DISTINCT FROM candidate ->> 'proof_sha256'
           OR pg_catalog.jsonb_typeof(candidate -> 'resource_counts')
                IS DISTINCT FROM 'object'
           OR pg_catalog.jsonb_typeof(candidate -> 'resource_hashes')
                IS DISTINCT FROM 'object'
           OR (candidate -> 'resource_counts')
                - ARRAY(
                    SELECT selected.value
                      FROM pg_catalog.jsonb_array_elements_text(
                           expected_selected_resources
                      ) AS selected(value)
                  ) <> '{{}}'::jsonb
           OR (candidate -> 'resource_hashes')
                - ARRAY(
                    SELECT selected.value
                      FROM pg_catalog.jsonb_array_elements_text(
                           expected_selected_resources
                      ) AS selected(value)
                  ) <> '{{}}'::jsonb
           OR EXISTS (
                SELECT 1
                  FROM pg_catalog.jsonb_array_elements_text(
                       expected_selected_resources
                  ) AS selected(value)
                 WHERE NOT (candidate -> 'resource_counts' ? selected.value)
                    OR NOT (candidate -> 'resource_hashes' ? selected.value)
           ) THEN
            RETURN FALSE;
        END IF;

        SELECT pg_catalog.sum((count_value #>> '{{}}')::numeric)
          INTO resource_count_sum
          FROM pg_catalog.jsonb_each(
               candidate -> 'resource_counts'
          ) AS resource_count(resource_type, count_value)
         WHERE pg_catalog.jsonb_typeof(count_value) = 'number'
           AND count_value #>> '{{}}' ~ '^(0|[1-9][0-9]*)$';
        IF resource_count_sum IS DISTINCT FROM expected_resource_count
           OR EXISTS (
                SELECT 1
                  FROM pg_catalog.jsonb_each(
                       candidate -> 'resource_counts'
                  ) AS resource_count(resource_type, count_value)
                 WHERE pg_catalog.jsonb_typeof(count_value) <> 'number'
                    OR count_value #>> '{{}}' !~ '^(0|[1-9][0-9]*)$'
                    OR candidate -> 'resource_hashes' ->> resource_type
                        !~ '^[0-9a-f]{{64}}$'
           ) THEN
            RETURN FALSE;
        END IF;

        IF pg_catalog.jsonb_typeof(candidate -> 'source_metrics')
                IS DISTINCT FROM 'object'
           OR NOT (candidate -> 'source_metrics'
                   ?& ARRAY[{metric_fields_sql}]::text[])
           OR (candidate -> 'source_metrics')
                   - ARRAY[{metric_fields_sql}]::text[] <> '{{}}'::jsonb THEN
            RETURN FALSE;
        END IF;
        FOR metric_value IN
            SELECT metric.value
              FROM pg_catalog.jsonb_each(candidate -> 'source_metrics') AS metric
        LOOP
            IF pg_catalog.jsonb_typeof(metric_value) <> 'number'
               OR metric_value #>> '{{}}' !~ '^(0|[1-9][0-9]*)$' THEN
                RETURN FALSE;
            END IF;
        END LOOP;

        IF pg_catalog.jsonb_typeof(candidate -> 'shard_count') <> 'number'
           OR candidate ->> 'shard_count' !~ '^[1-9][0-9]*$'
           OR pg_catalog.jsonb_typeof(candidate -> 'shards') <> 'array'
           OR pg_catalog.jsonb_array_length(candidate -> 'shards') < 1
           OR pg_catalog.jsonb_array_length(candidate -> 'shards')
                IS DISTINCT FROM (candidate ->> 'shard_count')::integer THEN
            RETURN FALSE;
        END IF;

        FOR descriptor, descriptor_index IN
            SELECT shard.value, shard.ordinality::integer
              FROM pg_catalog.jsonb_array_elements(candidate -> 'shards')
                   WITH ORDINALITY AS shard(value, ordinality)
             ORDER BY shard.ordinality
        LOOP
            IF pg_catalog.jsonb_typeof(descriptor) <> 'object'
               OR NOT (descriptor ?& ARRAY[{descriptor_fields_sql}]::text[])
               OR descriptor - ARRAY[{descriptor_fields_sql}]::text[]
                    <> '{{}}'::jsonb
               OR descriptor ->> 'dataset_id' IS DISTINCT FROM
                    expected_dataset_id
               OR descriptor ->> 'endpoint_id' IS DISTINCT FROM
                    expected_endpoint_id
               OR descriptor ->> 'acquisition_root_run_id' IS DISTINCT FROM
                    expected_root_run_id
               OR descriptor -> 'source_ids' IS DISTINCT FROM
                    expected_source_ids
               OR descriptor ->> 'input_sha256' !~ '^[0-9a-f]{{64}}$'
               OR descriptor ->> 'artifact_sha256' !~ '^[0-9a-f]{{64}}$'
               OR descriptor ->> 'shard_id' !~ '^[0-9a-f]{{64}}$'
               OR pg_catalog.jsonb_typeof(descriptor -> 'resource_count')
                    <> 'number'
               OR descriptor ->> 'resource_count' !~ '^[1-9][0-9]*$'
               OR pg_catalog.jsonb_typeof(descriptor -> 'artifact_byte_count')
                    <> 'number'
               OR descriptor ->> 'artifact_byte_count' !~ '^[1-9][0-9]*$'
               OR pg_catalog.jsonb_typeof(descriptor -> 'resource_counts')
                    <> 'object'
               OR descriptor -> 'resource_counts' = '{{}}'::jsonb
               OR (descriptor -> 'resource_counts')
                    - ARRAY(
                        SELECT selected.value
                          FROM pg_catalog.jsonb_array_elements_text(
                               expected_selected_resources
                          ) AS selected(value)
                      ) <> '{{}}'::jsonb
               OR pg_catalog.jsonb_typeof(descriptor -> 'first_identity')
                    <> 'array'
               OR pg_catalog.jsonb_array_length(
                    descriptor -> 'first_identity'
                  ) <> 3
               OR pg_catalog.jsonb_typeof(descriptor -> 'last_identity')
                    <> 'array'
               OR pg_catalog.jsonb_array_length(
                    descriptor -> 'last_identity'
                  ) <> 3
               OR EXISTS (
                    SELECT 1
                      FROM pg_catalog.jsonb_array_elements(
                           (descriptor -> 'first_identity') ||
                           (descriptor -> 'last_identity')
                      ) AS identity_part(value)
                     WHERE pg_catalog.jsonb_typeof(identity_part.value)
                            <> 'string'
                        OR identity_part.value #>> '{{}}' = ''
               )
               OR descriptor -> 'first_identity' ->> 2
                    !~ '^[0-9a-f]{{64}}$'
               OR descriptor -> 'last_identity' ->> 2
                    !~ '^[0-9a-f]{{64}}$'
               OR pg_catalog.jsonb_build_array(
                    descriptor -> 'first_identity' -> 0,
                    descriptor -> 'first_identity' -> 1
                  ) > pg_catalog.jsonb_build_array(
                    descriptor -> 'last_identity' -> 0,
                    descriptor -> 'last_identity' -> 1
                  ) THEN
                RETURN FALSE;
            END IF;
            SELECT pg_catalog.sum((count_value #>> '{{}}')::numeric)
              INTO descriptor_count
              FROM pg_catalog.jsonb_each(
                   descriptor -> 'resource_counts'
              ) AS resource_count(resource_type, count_value)
             WHERE pg_catalog.jsonb_typeof(count_value) = 'number'
               AND count_value #>> '{{}}' ~ '^[1-9][0-9]*$';
            IF descriptor_count IS DISTINCT FROM
                    (descriptor ->> 'resource_count')::numeric
               OR EXISTS (
                    SELECT 1
                      FROM pg_catalog.jsonb_each(
                           descriptor -> 'resource_counts'
                      ) AS resource_count(resource_type, count_value)
                     WHERE pg_catalog.jsonb_typeof(count_value) <> 'number'
                        OR count_value #>> '{{}}' !~ '^[1-9][0-9]*$'
               ) THEN
                RETURN FALSE;
            END IF;
            expected_shard_id := {canonical_sha256_ref}(
                pg_catalog.jsonb_build_array(
                    expected_dataset_id,
                    expected_endpoint_id,
                    expected_root_run_id,
                    expected_source_ids,
                    descriptor ->> 'input_sha256'
                )
            );
            current_shard_id := descriptor ->> 'shard_id';
            IF current_shard_id IS DISTINCT FROM expected_shard_id
               OR (previous_shard_id IS NOT NULL
                   AND current_shard_id <= previous_shard_id) THEN
                RETURN FALSE;
            END IF;
            previous_shard_id := current_shard_id;
            descriptor_count_sum := descriptor_count_sum + descriptor_count;
        END LOOP;

        SELECT pg_catalog.encode(
                   pg_catalog.sha256(
                       pg_catalog.convert_to(
                           pg_catalog.string_agg(
                               {canonical_json_ref}(shard.value),
                               E'\\n' ORDER BY shard.ordinality
                           ),
                           'UTF8'
                       )
                   ),
                   'hex'
               )
          INTO computed_shard_set_sha256
          FROM pg_catalog.jsonb_array_elements(candidate -> 'shards')
               WITH ORDINALITY AS shard(value, ordinality);
        IF descriptor_count_sum IS DISTINCT FROM expected_resource_count
           OR EXISTS (
                SELECT 1
                  FROM pg_catalog.jsonb_each_text(
                       expected_resource_counts
                  ) AS expected_count(resource_type, count_text)
                 WHERE (
                    SELECT COALESCE(
                               pg_catalog.sum(
                                   (shard.value -> 'resource_counts'
                                       ->> expected_count.resource_type)::numeric
                               ),
                               0::numeric
                           )
                      FROM pg_catalog.jsonb_array_elements(
                           candidate -> 'shards'
                      ) AS shard(value)
                     WHERE shard.value -> 'resource_counts'
                            ? expected_count.resource_type
                 ) IS DISTINCT FROM expected_count.count_text::numeric
           )
           OR computed_shard_set_sha256 IS DISTINCT FROM
                candidate ->> 'shard_set_sha256' THEN
            RETURN FALSE;
        END IF;
        RETURN TRUE;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
    $function$;
    """


def _proof_function_shape_fence_sql(schema: str) -> str:
    canonical_ref = _qf(schema, _CANONICAL_JSON_FUNCTION)
    sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    payload_canonical_ref = _qf(schema, _PAYLOAD_CANONICAL_JSON_FUNCTION)
    payload_sha256_ref = _qf(schema, _PAYLOAD_SHA256_FUNCTION)
    pair_valid_ref = _qf(schema, _PROOF_PAIR_VALID_FUNCTION)
    proof_shape_valid_ref = _qf(schema, _PROOF_SHAPE_VALID_FUNCTION)
    replay_shape_valid_ref = _qf(schema, _REPLAY_SHAPE_VALID_FUNCTION)
    coverage_shape_valid_ref = _qf(schema, _COVERAGE_SHAPE_VALID_FUNCTION)
    return f"""
    DO $migration$
    DECLARE
        valid_function_count bigint;
    BEGIN
        SELECT COUNT(*)
          INTO valid_function_count
          FROM (
                VALUES
                    (
                        {_ql(canonical_ref + '(jsonb)')}::regprocedure,
                        'text'::regtype,
                        'plpgsql'::name
                    ),
                    (
                        {_ql(sha256_ref + '(jsonb)')}::regprocedure,
                        'text'::regtype,
                        'sql'::name
                    ),
                    (
                        {_ql(payload_canonical_ref + '(jsonb)')}::regprocedure,
                        'text'::regtype,
                        'plpgsql'::name
                    ),
                    (
                        {_ql(payload_sha256_ref + '(jsonb)')}::regprocedure,
                        'text'::regtype,
                        'sql'::name
                    ),
                    (
                        {_ql(pair_valid_ref + '(jsonb,text)')}::regprocedure,
                        'boolean'::regtype,
                        'sql'::name
                    ),
                    (
                        {_ql(proof_shape_valid_ref + '(jsonb,text,bigint)')}::regprocedure,
                        'boolean'::regtype,
                        'plpgsql'::name
                    ),
                    (
                        {_ql(replay_shape_valid_ref + '(jsonb,text,jsonb,text)')}::regprocedure,
                        'boolean'::regtype,
                        'plpgsql'::name
                    ),
                    (
                        {_ql(coverage_shape_valid_ref + '(jsonb,jsonb,text,text)')}::regprocedure,
                        'boolean'::regtype,
                        'plpgsql'::name
                    )
               ) AS expected(function_oid, return_type, language_name)
          JOIN pg_catalog.pg_proc AS function_row
            ON function_row.oid = expected.function_oid
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
         WHERE function_row.prorettype = expected.return_type
           AND function_language.lanname = expected.language_name
           AND function_row.provolatile = 'i'
           AND function_row.proisstrict IS TRUE
           AND function_row.proparallel = 's'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                   ARRAY['search_path=pg_catalog']::text[]
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.aclexplode(
                       COALESCE(
                           function_row.proacl,
                           pg_catalog.acldefault(
                               'f',
                               function_row.proowner
                           )
                       )
                  ) AS function_acl
                 WHERE function_acl.grantee = 0
                   AND function_acl.privilege_type = 'EXECUTE'
           );
        IF valid_function_count <> 8 THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _create_proof_functions(schema: str) -> None:
    op.execute(_canonical_json_runtime_fence_sql())
    op.execute(_canonical_json_function_sql(schema))
    op.execute(_canonical_sha256_function_sql(schema))
    op.execute(_payload_canonical_json_function_sql(schema))
    op.execute(_payload_sha256_function_sql(schema))
    op.execute(_proof_pair_valid_function_sql(schema))
    op.execute(_proof_shape_valid_function_sql(schema))
    op.execute(_replay_shape_valid_function_sql(schema))
    op.execute(_coverage_shape_valid_function_sql(schema))
    canonical_ref = _qf(schema, _CANONICAL_JSON_FUNCTION)
    sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    payload_canonical_ref = _qf(schema, _PAYLOAD_CANONICAL_JSON_FUNCTION)
    payload_sha256_ref = _qf(schema, _PAYLOAD_SHA256_FUNCTION)
    pair_valid_ref = _qf(schema, _PROOF_PAIR_VALID_FUNCTION)
    proof_shape_valid_ref = _qf(schema, _PROOF_SHAPE_VALID_FUNCTION)
    replay_shape_valid_ref = _qf(schema, _REPLAY_SHAPE_VALID_FUNCTION)
    coverage_shape_valid_ref = _qf(schema, _COVERAGE_SHAPE_VALID_FUNCTION)
    op.execute(
        f"REVOKE ALL ON FUNCTION {canonical_ref}(jsonb) FROM PUBLIC;"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {sha256_ref}(jsonb) FROM PUBLIC;")
    op.execute(
        f"REVOKE ALL ON FUNCTION {payload_canonical_ref}(jsonb) FROM PUBLIC;"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {payload_sha256_ref}(jsonb) FROM PUBLIC;"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {pair_valid_ref}(jsonb, text) FROM PUBLIC;"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        f"{proof_shape_valid_ref}(jsonb, text, bigint) FROM PUBLIC;"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        f"{replay_shape_valid_ref}(jsonb, text, jsonb, text) FROM PUBLIC;"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        f"{coverage_shape_valid_ref}(jsonb, jsonb, text, text) FROM PUBLIC;"
    )
    op.execute(_proof_function_shape_fence_sql(schema))


def _drop_proof_functions(schema: str) -> None:
    canonical_ref = _qf(schema, _CANONICAL_JSON_FUNCTION)
    sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    payload_canonical_ref = _qf(schema, _PAYLOAD_CANONICAL_JSON_FUNCTION)
    payload_sha256_ref = _qf(schema, _PAYLOAD_SHA256_FUNCTION)
    pair_valid_ref = _qf(schema, _PROOF_PAIR_VALID_FUNCTION)
    proof_shape_valid_ref = _qf(schema, _PROOF_SHAPE_VALID_FUNCTION)
    replay_shape_valid_ref = _qf(schema, _REPLAY_SHAPE_VALID_FUNCTION)
    coverage_shape_valid_ref = _qf(schema, _COVERAGE_SHAPE_VALID_FUNCTION)
    op.execute(
        f"DROP FUNCTION {coverage_shape_valid_ref}(jsonb, jsonb, text, text);"
    )
    op.execute(
        f"DROP FUNCTION {replay_shape_valid_ref}(jsonb, text, jsonb, text);"
    )
    op.execute(
        f"DROP FUNCTION {proof_shape_valid_ref}(jsonb, text, bigint);"
    )
    op.execute(f"DROP FUNCTION {pair_valid_ref}(jsonb, text);")
    op.execute(f"DROP FUNCTION {payload_sha256_ref}(jsonb);")
    op.execute(f"DROP FUNCTION {payload_canonical_ref}(jsonb);")
    op.execute(f"DROP FUNCTION {sha256_ref}(jsonb);")
    op.execute(f"DROP FUNCTION {canonical_ref}(jsonb);")


_SUBSET_IMMUTABLE_COMPARISON_SQL = """
                ROW(
                    NEW.dataset_id,
                    NEW.endpoint_id,
                    NEW.import_run_id,
                    NEW.acquisition_root_run_id,
                    NEW.previous_dataset_id,
                    NEW.dataset_hash,
                    NEW.resource_count,
                    NEW.created_at,
                    NEW.validated_at,
                    NEW.completion_proof_required_version,
                    NEW.completion_proof_json,
                    NEW.completion_proof_sha256
                ) IS DISTINCT FROM ROW(
                    OLD.dataset_id,
                    OLD.endpoint_id,
                    OLD.import_run_id,
                    OLD.acquisition_root_run_id,
                    OLD.previous_dataset_id,
                    OLD.dataset_hash,
                    OLD.resource_count,
                    OLD.created_at,
                    OLD.validated_at,
                    OLD.completion_proof_required_version,
                    OLD.completion_proof_json,
                    OLD.completion_proof_sha256
                )
"""


_LEGACY_IMMUTABLE_COMPARISON_SQL = """
                ROW(
                    NEW.dataset_id,
                    NEW.endpoint_id,
                    NEW.import_run_id,
                    NEW.acquisition_root_run_id,
                    NEW.previous_dataset_id,
                    NEW.dataset_hash,
                    NEW.resource_count,
                    NEW.created_at,
                    NEW.validated_at
                ) IS DISTINCT FROM ROW(
                    OLD.dataset_id,
                    OLD.endpoint_id,
                    OLD.import_run_id,
                    OLD.acquisition_root_run_id,
                    OLD.previous_dataset_id,
                    OLD.dataset_hash,
                    OLD.resource_count,
                    OLD.created_at,
                    OLD.validated_at
                )
"""


def _endpoint_dataset_lifecycle_sql(
    immutable_comparison_sql: str,
    *,
    subset_metadata_is_immutable: bool,
) -> str:
    subset_metadata_check = ""
    if subset_metadata_is_immutable:
        subset_metadata_check = """
        OR (
            OLD.completion_proof_required_version = 3
            AND NEW.publication_metadata_json::jsonb IS DISTINCT FROM
                    OLD.publication_metadata_json::jsonb
        )
        """
    return f"""
        (
            {immutable_comparison_sql}
        ) {subset_metadata_check} OR (
            OLD.status = 'validated'
            AND NOT (
                (
                    NEW.status = 'validated'
                    AND NEW.is_current IS NOT DISTINCT FROM OLD.is_current
                    AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                    AND NEW.superseded_at IS NOT DISTINCT FROM OLD.superseded_at
                )
                OR (
                    NEW.status = 'published'
                    AND OLD.is_current IS FALSE
                    AND OLD.validated_at IS NOT NULL
                    AND OLD.published_at IS NULL
                    AND OLD.superseded_at IS NULL
                    AND NEW.is_current IS TRUE
                    AND NEW.published_at IS NOT DISTINCT FROM
                        transaction_timestamp()
                    AND NEW.superseded_at IS NULL
                )
            )
        ) OR (
            OLD.status = 'published'
            AND NOT (
                (
                    NEW.status = 'published'
                    AND NEW.is_current IS NOT DISTINCT FROM OLD.is_current
                    AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                    AND NEW.superseded_at IS NOT DISTINCT FROM OLD.superseded_at
                )
                OR (
                    NEW.status = 'superseded'
                    AND OLD.is_current IS TRUE
                    AND OLD.published_at IS NOT NULL
                    AND OLD.superseded_at IS NULL
                    AND NEW.is_current IS FALSE
                    AND NEW.published_at IS NOT DISTINCT FROM OLD.published_at
                    AND NEW.superseded_at IS NOT NULL
                    AND NEW.superseded_at >= NEW.published_at
                    AND NEW.superseded_at IS NOT DISTINCT FROM
                        transaction_timestamp()
                )
            )
        ) OR (
            OLD.status = 'superseded'
            AND (
                NEW.status <> 'superseded'
                OR NEW.is_current IS NOT DISTINCT FROM TRUE
                OR NEW.is_current IS DISTINCT FROM OLD.is_current
                OR NEW.published_at IS DISTINCT FROM OLD.published_at
                OR NEW.superseded_at IS DISTINCT FROM OLD.superseded_at
            )
        )
    """


def _reviewed_root_policy_sql(metadata: str, required_root_count: int) -> str:
    """Return exact closed root-policy JSON equality."""

    return f"""
        {metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)} =
            pg_catalog.jsonb_build_object(
                'policy_version', {_ql(_REVIEWED_ROOT_POLICY_VERSION)},
                'required_root_count', {required_root_count}
            )
    """


def _subset_matched_twin_sql(
    schema: str,
    *,
    reviewed_root_policy_aware: bool = False,
) -> str:
    """Require one exact sealed v3 baseline behind validated publication."""

    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    metadata = "NEW.publication_metadata_json::jsonb"
    verification = f"({metadata} -> {_ql(_TWIN_VERIFICATION_KEY)})"
    proof = f"({verification} -> 'proof')"
    baseline_metadata = "baseline.publication_metadata_json::jsonb"
    baseline_verification = (
        f"({baseline_metadata} -> {_ql(_TWIN_VERIFICATION_KEY)})"
    )
    baseline_proof = f"({baseline_verification} -> 'proof')"
    binding_sql = _subset_twin_proof_binding_sql(
        "NEW",
        metadata,
        proof,
    )
    candidate_policy_sql = "true"
    baseline_policy_sql = "true"
    if reviewed_root_policy_aware:
        candidate_policy_sql = f"""
            (
                NOT ({metadata} ? {_ql(_REVIEWED_ROOT_POLICY_KEY)})
                OR ({_reviewed_root_policy_sql(metadata, 2)})
            )
        """
        baseline_policy_sql = f"""
            (
                (
                    NOT ({metadata} ? {_ql(_REVIEWED_ROOT_POLICY_KEY)})
                    AND NOT (
                        {baseline_metadata} ? {_ql(_REVIEWED_ROOT_POLICY_KEY)}
                    )
                ) OR (
                    ({_reviewed_root_policy_sql(metadata, 2)})
                    AND {baseline_metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)}
                        = {metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)}
                )
            )
        """
    return f"""
        pg_catalog.jsonb_typeof({verification}) = 'object'
        AND {verification} ?& ARRAY[
            'role', 'admission_role', 'result', 'proof',
            'baseline_dataset_id', 'baseline_acquisition_root_run_id',
            'mismatch_fields'
        ]::text[]
        AND {verification} - ARRAY[
            'role', 'admission_role', 'result', 'proof',
            'baseline_dataset_id', 'baseline_acquisition_root_run_id',
            'mismatch_fields'
        ]::text[] = '{{}}'::jsonb
        AND {metadata} -> 'requires_twin_root_verification' = 'true'::jsonb
        AND {metadata} ->> 'acquisition_root_run_id' =
            NEW.acquisition_root_run_id
        AND NULLIF({metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)}, '') =
            NEW.completion_proof_json ->> 'campaign_id'
        AND {metadata} ->> {_ql(_TWIN_ROLE_KEY)} =
            'verification_candidate'
        AND NULLIF({metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)}, '')
            IS NOT NULL
        AND {verification} ->> 'role' = 'verification_candidate'
        AND {verification} ->> 'admission_role' = 'verification_candidate'
        AND {verification} ->> 'result' = 'matched'
        AND {verification} -> 'mismatch_fields' = '[]'::jsonb
        AND {verification} ->> 'baseline_dataset_id' =
            {metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)}
        AND ({candidate_policy_sql})
        AND {proof} ->> 'endpoint_id' = NEW.endpoint_id
        AND {proof} ->> 'acquisition_root_run_id' =
            NEW.acquisition_root_run_id
        AND {proof} ->> {_ql(_TWIN_CAMPAIGN_KEY)} =
            {metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)}
        AND {proof} ->> {_ql(_TWIN_SCOPE_KEY)} =
            {metadata} ->> {_ql(_TWIN_SCOPE_KEY)}
        AND {proof} -> 'source_ids' = {metadata} -> 'source_ids'
        AND {proof} -> 'selected_resources' =
            {metadata} -> 'selected_resources'
        AND {proof} -> 'expected_resources' =
            {metadata} -> 'expected_resources'
        AND {proof} ->> 'dataset_hash' = NEW.dataset_hash
        AND {proof} ->> 'resource_count' = NEW.resource_count::text
        AND {proof} -> 'completion_proof' = NEW.completion_proof_json
        AND {proof} ->> 'completion_proof_sha256' =
            NEW.completion_proof_sha256
        AND {proof} -> 'resource_hashes' =
            NEW.completion_proof_json -> 'dataset' -> 'resource_hashes'
        AND {proof} -> 'resource_counts' =
            NEW.completion_proof_json -> 'dataset' -> 'resource_counts'
        AND ({binding_sql})
        AND (
            (
                NEW.status = 'validated'
                AND NEW.is_current IS FALSE
                AND NEW.validated_at IS NOT NULL
                AND NEW.published_at IS NULL
                AND NEW.superseded_at IS NULL
            ) OR (
                NEW.status = 'published'
                AND NEW.is_current IS TRUE
                AND NEW.validated_at IS NOT NULL
                AND NEW.published_at IS NOT NULL
                AND NEW.superseded_at IS NULL
            )
        )
        AND EXISTS (
            SELECT 1
              FROM {dataset_ref} AS baseline
             WHERE baseline.dataset_id =
                    {metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)}
               AND baseline.endpoint_id = NEW.endpoint_id
               AND baseline.status = 'verification_baseline'
               AND baseline.is_current IS FALSE
               AND baseline.completion_proof_required_version = 3
               AND baseline.acquisition_root_run_id IS DISTINCT FROM
                    NEW.acquisition_root_run_id
               AND baseline.completion_proof_json = NEW.completion_proof_json
               AND baseline.completion_proof_sha256 =
                    NEW.completion_proof_sha256
               AND baseline.dataset_hash = NEW.dataset_hash
               AND baseline.resource_count = NEW.resource_count
               AND ({baseline_policy_sql})
               AND pg_catalog.jsonb_typeof({baseline_verification}) = 'object'
               AND {baseline_metadata} -> 'requires_twin_root_verification' =
                    'true'::jsonb
               AND {baseline_metadata} ->> {_ql(_TWIN_ROLE_KEY)} =
                    'baseline_candidate'
               AND {baseline_metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)}
                    IS NULL
               AND {baseline_verification} ->> 'role' = 'baseline'
               AND {baseline_verification} ->> 'admission_role' =
                    'baseline_candidate'
               AND {baseline_verification} ->> 'result' =
                    'baseline_recorded'
               AND {baseline_proof} ->> 'endpoint_id' = baseline.endpoint_id
               AND {baseline_proof} ->> 'acquisition_root_run_id' =
                    baseline.acquisition_root_run_id
               AND {verification} ->> 'baseline_acquisition_root_run_id' =
                    baseline.acquisition_root_run_id
               AND {baseline_proof} ->> {_ql(_TWIN_CAMPAIGN_KEY)} =
                    {baseline_metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)}
               AND {baseline_proof} ->> {_ql(_TWIN_SCOPE_KEY)} =
                    {baseline_metadata} ->> {_ql(_TWIN_SCOPE_KEY)}
               AND {baseline_proof} -> 'source_ids' =
                    {baseline_metadata} -> 'source_ids'
               AND {baseline_proof} -> 'selected_resources' =
                    {baseline_metadata} -> 'selected_resources'
               AND {baseline_proof} -> 'expected_resources' =
                    {baseline_metadata} -> 'expected_resources'
               AND (({proof}) - 'acquisition_root_run_id') =
                    (({baseline_proof}) - 'acquisition_root_run_id')
        )
        AND (
            SELECT pg_catalog.count(*)
              FROM {dataset_ref} AS generation_baseline
             WHERE generation_baseline.endpoint_id = NEW.endpoint_id
               AND generation_baseline.status = 'verification_baseline'
               AND generation_baseline.completion_proof_required_version = 3
               AND generation_baseline.publication_metadata_json::jsonb
                    ->> {_ql(_TWIN_CAMPAIGN_KEY)} =
                    {metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)}
               AND generation_baseline.publication_metadata_json::jsonb
                    ->> {_ql(_TWIN_SCOPE_KEY)} =
                    {metadata} ->> {_ql(_TWIN_SCOPE_KEY)}
        ) = 1
    """


def _subset_single_root_sql(
    schema: str,
    *,
    dataset_alias: str = "NEW",
) -> str:
    """Bind a policy-one terminal row to its sealed direct content proof."""

    metadata = f"{dataset_alias}.publication_metadata_json::jsonb"
    content_proof = f"({metadata} -> {_ql(_CONTENT_PROOF_KEY)})"
    content_proof_valid_ref = _qf(schema, _CONTENT_PROOF_VALID_FUNCTION)
    return f"""
        ({_reviewed_root_policy_sql(metadata, 1)})
        AND {metadata} -> 'requires_twin_root_verification' = 'false'::jsonb
        AND NOT ({metadata} ? {_ql(_TWIN_ROLE_KEY)})
        AND NOT ({metadata} ? {_ql(_TWIN_BASELINE_DATASET_KEY)})
        AND NOT ({metadata} ? {_ql(_TWIN_VERIFICATION_KEY)})
        AND {content_proof_valid_ref}(
            {content_proof},
            {dataset_alias}.dataset_id,
            {dataset_alias}.endpoint_id,
            {dataset_alias}.acquisition_root_run_id,
            {metadata} -> 'source_ids',
            {metadata} -> 'selected_resources',
            {dataset_alias}.dataset_hash,
            {dataset_alias}.resource_count,
            {dataset_alias}.completion_proof_json
                -> 'dataset' -> 'resource_hashes',
            {dataset_alias}.completion_proof_json
                -> 'dataset' -> 'resource_counts'
        ) IS TRUE
    """


def _subset_twin_proof_binding_sql(
    dataset: str,
    metadata: str,
    proof: str,
) -> str:
    """Bind one embedded twin proof to its marker-3 parent row."""

    resources_json = _ql(
        "[" + ",".join(
            '"' + resource_type + '"'
            for resource_type in _SUBSET_RESOURCE_TYPES
        ) + "]"
    )
    proof_fields_sql = ", ".join(
        _ql(field_name)
        for field_name in (
            "endpoint_id",
            "acquisition_root_run_id",
            "source_ids",
            "selected_resources",
            "expected_resources",
            _TWIN_CAMPAIGN_KEY,
            _TWIN_SCOPE_KEY,
            "dataset_hash",
            "resource_count",
            "resource_hashes",
            "resource_counts",
            "completion_proof",
            "completion_proof_sha256",
        )
    )
    return f"""
        pg_catalog.jsonb_typeof(({proof})) = 'object'
        AND ({proof}) ?& ARRAY[{proof_fields_sql}]::text[]
        AND ({proof}) - ARRAY[{proof_fields_sql}]::text[] = '{{}}'::jsonb
        AND {metadata} ->> 'acquisition_root_run_id' =
            {dataset}.acquisition_root_run_id
        AND NULLIF({metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)}, '') =
            {dataset}.completion_proof_json ->> 'campaign_id'
        AND {proof} ->> 'endpoint_id' = {dataset}.endpoint_id
        AND {proof} ->> 'acquisition_root_run_id' =
            {dataset}.acquisition_root_run_id
        AND {proof} -> {_ql(_TWIN_CAMPAIGN_KEY)} =
            {metadata} -> {_ql(_TWIN_CAMPAIGN_KEY)}
        AND {proof} -> {_ql(_TWIN_SCOPE_KEY)} =
            {metadata} -> {_ql(_TWIN_SCOPE_KEY)}
        AND {metadata} ->> {_ql(_TWIN_SCOPE_KEY)} ~ '^[0-9a-f]{{64}}$'
        AND pg_catalog.jsonb_typeof({metadata} -> 'source_ids') = 'array'
        AND pg_catalog.jsonb_array_length({metadata} -> 'source_ids') = 1
        AND {proof} -> 'source_ids' = {metadata} -> 'source_ids'
        AND {metadata} -> 'selected_resources' = {resources_json}::jsonb
        AND {metadata} -> 'expected_resources' = {resources_json}::jsonb
        AND {proof} -> 'selected_resources' =
            {metadata} -> 'selected_resources'
        AND {proof} -> 'expected_resources' =
            {metadata} -> 'expected_resources'
        AND {proof} ->> 'dataset_hash' = {dataset}.dataset_hash
        AND {proof} -> 'resource_count' =
            pg_catalog.to_jsonb({dataset}.resource_count)
        AND {proof} -> 'resource_hashes' =
            {dataset}.completion_proof_json -> 'dataset' -> 'resource_hashes'
        AND {proof} -> 'resource_counts' =
            {dataset}.completion_proof_json -> 'dataset' -> 'resource_counts'
        AND {proof} -> 'completion_proof' =
            {dataset}.completion_proof_json
        AND {proof} ->> 'completion_proof_sha256' =
            {dataset}.completion_proof_sha256
    """


def _subset_baseline_twin_sql() -> str:
    metadata = "NEW.publication_metadata_json::jsonb"
    verification = f"({metadata} -> {_ql(_TWIN_VERIFICATION_KEY)})"
    proof = f"({verification} -> 'proof')"
    binding_sql = _subset_twin_proof_binding_sql(
        "NEW", metadata, proof
    )
    return f"""
        {metadata} -> 'requires_twin_root_verification' = 'true'::jsonb
        AND NEW.is_current IS FALSE
        AND NEW.validated_at IS NULL
        AND NEW.published_at IS NULL
        AND NEW.superseded_at IS NULL
        AND {metadata} ->> {_ql(_TWIN_ROLE_KEY)} = 'baseline_candidate'
        AND {metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)} IS NULL
        AND pg_catalog.jsonb_typeof({verification}) = 'object'
        AND {verification} ?& ARRAY[
            'role', 'admission_role', 'result', 'proof'
        ]::text[]
        AND {verification} - ARRAY[
            'role', 'admission_role', 'result', 'proof'
        ]::text[] = '{{}}'::jsonb
        AND {verification} ->> 'role' = 'baseline'
        AND {verification} ->> 'admission_role' = 'baseline_candidate'
        AND {verification} ->> 'result' = 'baseline_recorded'
        AND ({binding_sql})
    """


def _subset_mismatch_vector_sql(
    baseline_proof: str,
    candidate_proof: str,
) -> str:
    field_names = (
        "endpoint_id",
        "source_ids",
        "selected_resources",
        "expected_resources",
        _TWIN_CAMPAIGN_KEY,
        _TWIN_SCOPE_KEY,
        "dataset_hash",
        "resource_count",
        "resource_hashes",
        "resource_counts",
        "completion_proof",
        "completion_proof_sha256",
    )
    rows_sql = ",\n".join(
        "                ("
        + str(ordinal)
        + ", "
        + _ql(field_name)
        + f", {baseline_proof} -> {_ql(field_name)}, "
        + f"{candidate_proof} -> {_ql(field_name)})"
        for ordinal, field_name in enumerate(field_names, start=1)
    )
    return f"""
        SELECT COALESCE(
                   pg_catalog.jsonb_agg(
                       mismatch.field_name ORDER BY mismatch.ordinal
                   ),
                   '[]'::jsonb
               )
          FROM (VALUES
{rows_sql}
               ) AS mismatch(
                    ordinal, field_name, baseline_value, candidate_value
               )
         WHERE mismatch.baseline_value IS DISTINCT FROM
               mismatch.candidate_value
    """


def _subset_mismatch_twin_sql(schema: str) -> str:
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    metadata = "NEW.publication_metadata_json::jsonb"
    verification = f"({metadata} -> {_ql(_TWIN_VERIFICATION_KEY)})"
    proof = f"({verification} -> 'proof')"
    baseline_metadata = "baseline.publication_metadata_json::jsonb"
    baseline_verification = (
        f"({baseline_metadata} -> {_ql(_TWIN_VERIFICATION_KEY)})"
    )
    baseline_proof = f"({baseline_verification} -> 'proof')"
    binding_sql = _subset_twin_proof_binding_sql(
        "NEW", metadata, proof
    )
    mismatch_vector_sql = _subset_mismatch_vector_sql(
        baseline_proof, proof
    )
    return f"""
        {metadata} -> 'requires_twin_root_verification' = 'true'::jsonb
        AND NEW.is_current IS FALSE
        AND NEW.validated_at IS NULL
        AND NEW.published_at IS NULL
        AND NEW.superseded_at IS NULL
        AND {metadata} ->> {_ql(_TWIN_ROLE_KEY)} =
            'verification_candidate'
        AND NULLIF(
            {metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)}, ''
        ) IS NOT NULL
        AND pg_catalog.jsonb_typeof({verification}) = 'object'
        AND {verification} ?& ARRAY[
            'role', 'admission_role', 'result', 'proof',
            'baseline_dataset_id', 'baseline_acquisition_root_run_id',
            'mismatch_fields'
        ]::text[]
        AND {verification} - ARRAY[
            'role', 'admission_role', 'result', 'proof',
            'baseline_dataset_id', 'baseline_acquisition_root_run_id',
            'mismatch_fields'
        ]::text[] = '{{}}'::jsonb
        AND {verification} ->> 'role' = 'verification_candidate'
        AND {verification} ->> 'admission_role' =
            'verification_candidate'
        AND {verification} ->> 'result' = 'mismatch'
        AND {verification} ->> 'baseline_dataset_id' =
            {metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)}
        AND pg_catalog.jsonb_typeof(
            {verification} -> 'mismatch_fields'
        ) = 'array'
        AND pg_catalog.jsonb_array_length(
            {verification} -> 'mismatch_fields'
        ) > 0
        AND ({binding_sql})
        AND EXISTS (
            SELECT 1
              FROM {dataset_ref} AS baseline
             WHERE baseline.dataset_id =
                    {metadata} ->> {_ql(_TWIN_BASELINE_DATASET_KEY)}
               AND baseline.endpoint_id = NEW.endpoint_id
               AND baseline.status = 'verification_baseline'
               AND baseline.is_current IS FALSE
               AND baseline.completion_proof_required_version = 3
               AND baseline.acquisition_root_run_id IS DISTINCT FROM
                    NEW.acquisition_root_run_id
               AND {verification} ->> 'baseline_acquisition_root_run_id' =
                    baseline.acquisition_root_run_id
               AND {baseline_metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)} =
                    {metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)}
               AND {baseline_metadata} ->> {_ql(_TWIN_SCOPE_KEY)} =
                    {metadata} ->> {_ql(_TWIN_SCOPE_KEY)}
               AND {baseline_proof} -> 'source_ids' =
                    {proof} -> 'source_ids'
               AND {baseline_proof} -> 'selected_resources' =
                    {proof} -> 'selected_resources'
               AND {baseline_proof} -> 'expected_resources' =
                    {proof} -> 'expected_resources'
               AND {baseline_proof} -> {_ql(_TWIN_CAMPAIGN_KEY)} =
                    {proof} -> {_ql(_TWIN_CAMPAIGN_KEY)}
               AND {baseline_proof} -> {_ql(_TWIN_SCOPE_KEY)} =
                    {proof} -> {_ql(_TWIN_SCOPE_KEY)}
               AND ({mismatch_vector_sql}) =
                    {verification} -> 'mismatch_fields'
        )
        AND (
            SELECT pg_catalog.count(*)
              FROM {dataset_ref} AS generation_baseline
             WHERE generation_baseline.endpoint_id = NEW.endpoint_id
               AND generation_baseline.status = 'verification_baseline'
               AND generation_baseline.completion_proof_required_version = 3
               AND generation_baseline.publication_metadata_json::jsonb
                    ->> {_ql(_TWIN_CAMPAIGN_KEY)} =
                    {metadata} ->> {_ql(_TWIN_CAMPAIGN_KEY)}
               AND generation_baseline.publication_metadata_json::jsonb
                    ->> {_ql(_TWIN_SCOPE_KEY)} =
                    {metadata} ->> {_ql(_TWIN_SCOPE_KEY)}
        ) = 1
    """


def _subset_dataset_content_sql(schema: str) -> str:
    """Bind the terminal proof to ordered projected and raw child rows."""

    resource_ref = _qf(schema, _DATASET_RESOURCE)
    canonical_json_ref = _qf(schema, _CANONICAL_JSON_FUNCTION)
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    payload_sha256_ref = _qf(schema, _PAYLOAD_SHA256_FUNCTION)
    resource_types_sql = ", ".join(_ql(value) for value in _SUBSET_RESOURCE_TYPES)
    dataset_resources = f"""
        SELECT resource_type, resource_id, payload_hash, payload_json,
               acquired_resource_sha256
          FROM {resource_ref}
         WHERE dataset_id = NEW.dataset_id
           AND resource_type NOT LIKE 'LU:%:pass:%'
    """
    return f"""
        NOT EXISTS (
            SELECT 1
              FROM ({dataset_resources}) AS child
             WHERE child.resource_type <>
                   ALL(ARRAY[{resource_types_sql}]::text[])
                OR child.payload_hash IS DISTINCT FROM
                   {payload_sha256_ref}(
                       child.payload_json::jsonb
                       - 'resource_url'
                       - 'fhir_self_url'
                       - 'fhir_fetch_url'
                       - 'fhir_fetch_mode'
                   )
        )
        AND (SELECT pg_catalog.count(*)
               FROM ({dataset_resources}) AS child) = NEW.resource_count
        AND (
            SELECT pg_catalog.encode(
                       pg_catalog.sha256(
                           pg_catalog.convert_to(
                               COALESCE(
                                   pg_catalog.string_agg(
                                       {canonical_json_ref}(
                                           pg_catalog.jsonb_build_array(
                                               child.resource_type,
                                               child.resource_id,
                                               child.payload_hash
                                           )
                                       ),
                                       E'\\n' ORDER BY
                                           child.resource_type,
                                           child.resource_id
                                   ),
                                   ''
                               ),
                               'UTF8'
                           )
                       ),
                       'hex'
                   )
              FROM ({dataset_resources}) AS child
        ) = NEW.dataset_hash
        AND NOT EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_each(
                    NEW.completion_proof_json -> 'dataset'
                        -> 'resource_counts'
              ) AS expected(resource_type, resource_count)
             WHERE (expected.resource_count #>> '{{}}')::bigint <>
                    (
                        SELECT pg_catalog.count(*)
                          FROM ({dataset_resources}) AS child
                         WHERE child.resource_type = expected.resource_type
                    )
                OR NEW.completion_proof_json -> 'dataset'
                       -> 'resource_hashes' ->> expected.resource_type <>
                    (
                        SELECT pg_catalog.encode(
                                   pg_catalog.sha256(
                                       pg_catalog.convert_to(
                                           COALESCE(
                                               pg_catalog.string_agg(
                                                   {canonical_json_ref}(
                                                       pg_catalog.jsonb_build_array(
                                                           child.resource_type,
                                                           child.resource_id,
                                                           child.payload_hash
                                                       )
                                                   ),
                                                   E'\\n' ORDER BY
                                                       child.resource_id
                                               ),
                                               ''
                                           ),
                                           'UTF8'
                                       )
                                   ),
                                   'hex'
                               )
                          FROM ({dataset_resources}) AS child
                         WHERE child.resource_type = expected.resource_type
                    )
                OR NEW.completion_proof_json -> 'dataset'
                       -> 'acquired_resource_hashes'
                       ->> expected.resource_type <>
                    (
                        SELECT {canonical_sha256_ref}(
                                   COALESCE(
                                       pg_catalog.jsonb_agg(
                                           pg_catalog.jsonb_build_object(
                                               'resource_id', child.resource_id,
                                               'sha256',
                                                   child.acquired_resource_sha256
                                           ) ORDER BY child.resource_id
                                       ),
                                       '[]'::jsonb
                                   )
                               )
                          FROM ({dataset_resources}) AS child
                         WHERE child.resource_type = expected.resource_type
                    )
        )
    """


def _create_subset_baseline_generation_index(schema: str) -> None:
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    op.execute(
        f"""
        CREATE UNIQUE INDEX {_q(_BASELINE_GENERATION_INDEX)}
            ON {dataset_ref} (
                endpoint_id,
                ((publication_metadata_json::jsonb
                    ->> {_ql(_TWIN_CAMPAIGN_KEY)})),
                ((publication_metadata_json::jsonb
                    ->> {_ql(_TWIN_SCOPE_KEY)}))
            )
         WHERE completion_proof_required_version = 3
           AND status = 'verification_baseline';
        """
    )


def _drop_subset_baseline_generation_index(schema: str) -> None:
    op.execute(
        "DROP INDEX " + _qf(schema, _BASELINE_GENERATION_INDEX) + ";"
    )


def _subset_source_metadata_identity_sql(
    source_metadata: str,
    *,
    include_reviewed_root_policy: bool = False,
) -> str:
    metadata_fields = _SUBSET_SOURCE_SCOPE_METADATA_FIELDS
    if include_reviewed_root_policy:
        metadata_fields += (_REVIEWED_ROOT_POLICY_KEY,)
    entries = ",\n".join(
        "                pg_catalog.jsonb_build_array("
        + _ql(field_name)
        + f", {source_metadata} ? {_ql(field_name)}, "
        + f"{source_metadata} -> {_ql(field_name)})"
        for field_name in metadata_fields
    )
    return "pg_catalog.jsonb_build_array(\n" + entries + "\n            )"


def _subset_source_scope_payload_sql(
    source_alias: str,
    source_metadata: str,
    dataset_metadata: str,
    dataset_alias: str,
    *,
    use_configured_endpoint_identity: bool = False,
    include_reviewed_root_policy: bool = False,
) -> str:
    metadata_identity = _subset_source_metadata_identity_sql(
        source_metadata,
        include_reviewed_root_policy=include_reviewed_root_policy,
    )
    identity_version = (
        _SUBSET_SOURCE_SCOPE_VERSION_V2
        if include_reviewed_root_policy
        else _SUBSET_SOURCE_SCOPE_VERSION
    )
    endpoint_identity = (
        f"{source_metadata} ->> "
        "'provider_directory_configured_endpoint_id'"
        if use_configured_endpoint_identity
        else f"{source_alias}.endpoint_id"
    )
    return f"""
        pg_catalog.jsonb_build_object(
            'identity_version', {_ql(identity_version)},
            'source_ids', {dataset_metadata} -> 'source_ids',
            'cutoff', {dataset_alias}.completion_proof_json -> 'cutoff',
            'source', pg_catalog.jsonb_build_object(
                'source_id', {source_alias}.source_id,
                'endpoint_id', {endpoint_identity},
                'canonical_api_base', {source_alias}.canonical_api_base,
                'requires_registration', {source_alias}.requires_registration,
                'requires_api_key', {source_alias}.requires_api_key,
                'auth_type', {source_alias}.auth_type
            ),
            'metadata_identity', {metadata_identity}
        )
    """


def _subset_source_resource_set_sql(
    source_metadata: str,
    field_name: str,
) -> str:
    resource_types_sql = ", ".join(
        _ql(resource_type) for resource_type in _SUBSET_RESOURCE_TYPES
    )
    resource_value = f"({source_metadata} -> {_ql(field_name)})"
    return f"""
        pg_catalog.jsonb_typeof({resource_value}) = 'array'
        AND pg_catalog.jsonb_array_length({resource_value}) = 7
        AND NOT EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements_text({resource_value})
                   AS configured(resource_type)
             WHERE configured.resource_type <>
                   ALL(ARRAY[{resource_types_sql}]::text[])
        )
        AND (
            SELECT pg_catalog.count(DISTINCT configured.resource_type)
              FROM pg_catalog.jsonb_array_elements_text({resource_value})
                   AS configured(resource_type)
        ) = 7
    """


def _subset_source_fixed_identity_sql(
    source_metadata: str,
    dataset_alias: str,
    *,
    reviewed_subset_profile_aware: bool = False,
    reviewed_subset_terminal_window_profile_aware: bool = False,
) -> str:
    resource_types_sql = ", ".join(
        _ql(resource_type) for resource_type in _SUBSET_RESOURCE_TYPES
    )
    supported_sql = _subset_source_resource_set_sql(
        source_metadata, "provider_directory_supported_resources"
    )
    expected_nonempty_sql = _subset_source_resource_set_sql(
        source_metadata, "provider_directory_expected_nonempty_resources"
    )
    subset_sql = _subset_source_resource_set_sql(
        source_metadata, "provider_directory_server_issued_subset_resources"
    )
    start_urls = (
        f"({source_metadata} -> "
        "'provider_directory_current_version_census_start_urls')"
    )
    page_caps = (
        f"({source_metadata} -> "
        "'provider_directory_resource_page_count_caps')"
    )
    terminal_window_profile_sql = (
        f"""
            OR (
                {dataset_alias}.completion_proof_json ->> 'strategy_version' =
                    'provider-directory-fhir-server-issued-traversal-subset-v5'
                AND {dataset_alias}.completion_proof_json
                    -> 'completion_scopes' =
                    '["advertised-count-monotone-decrease-bounded-by-one-percent-and-twenty-pages",'
                    '"terminal-logical-window-covers-advertised-pre",'
                    '"source-issued-continuation",'
                    '"returned-resource-content"]'::jsonb
            )
        """
        if reviewed_subset_terminal_window_profile_aware
        else ""
    )
    profile_sql = (
        f"""
        {source_metadata}
            ->> 'provider_directory_current_version_census_strategy_version' =
            {dataset_alias}.completion_proof_json ->> 'strategy_version'
        AND {source_metadata}
            -> 'provider_directory_current_version_census_completion_scopes' =
            {dataset_alias}.completion_proof_json -> 'completion_scopes'
        AND (
            (
                {dataset_alias}.completion_proof_json ->> 'strategy_version' =
                    'provider-directory-fhir-server-issued-traversal-subset-v3'
                AND {dataset_alias}.completion_proof_json
                    -> 'completion_scopes' =
                    '["advertised-count-stability",'
                    '"source-issued-continuation",'
                    '"returned-resource-content"]'::jsonb
            ) OR (
                {dataset_alias}.completion_proof_json ->> 'strategy_version' =
                    'provider-directory-fhir-server-issued-traversal-subset-v4'
                AND {dataset_alias}.completion_proof_json
                    -> 'completion_scopes' =
                    '["advertised-count-monotone-decrease-at-most-one",'
                    '"source-issued-continuation",'
                    '"returned-resource-content"]'::jsonb
            )
            {terminal_window_profile_sql}
        )
        """
        if (
            reviewed_subset_profile_aware
            or reviewed_subset_terminal_window_profile_aware
        )
        else f"""
        {source_metadata}
            ->> 'provider_directory_current_version_census_strategy_version' =
            'provider-directory-fhir-server-issued-traversal-subset-v3'
        AND {source_metadata}
            -> 'provider_directory_current_version_census_completion_scopes' =
            '["advertised-count-stability","source-issued-continuation",'
            '"returned-resource-content"]'::jsonb
        """
    )
    return f"""
        {source_metadata} -> 'provider_directory_manual_only' = 'true'::jsonb
        AND {source_metadata} -> 'provider_directory_acquisition_enabled' =
            'true'::jsonb
        AND {source_metadata} -> 'provider_directory_fully_enumerable_resources'
            = '[]'::jsonb
        AND {source_metadata} ->> 'provider_directory_coverage_mode' =
            'server-issued-traversal-subset'
        AND ({supported_sql})
        AND ({expected_nonempty_sql})
        AND ({subset_sql})
        AND {source_metadata}
            -> 'provider_directory_current_version_census_contract_version'
            = '3'::jsonb
        AND {source_metadata}
            ->> 'provider_directory_current_version_census_strategy' =
            'server-issued-traversal-subset'
        AND ({profile_sql})
        AND {source_metadata}
            ->> 'provider_directory_current_version_census_traversal_version' =
            'provider-directory-fhir-smile-logical-offset-v3'
        AND {source_metadata}
            ->> 'provider_directory_current_version_census_canonicalization_version' =
            'provider-directory-fhir-returned-resource-json-v2'
        AND {source_metadata}
            ->> 'provider_directory_current_version_census_continuation_strategy' =
            'smile-opaque-logical-offset-v3'
        AND {source_metadata}
            -> 'provider_directory_current_version_census_page_count' =
            {dataset_alias}.completion_proof_json -> 'page_count'
        AND {source_metadata}
            ->> 'provider_directory_verification_campaign_id' =
            {dataset_alias}.completion_proof_json ->> 'campaign_id'
        AND pg_catalog.jsonb_typeof({start_urls}) = 'object'
        AND {start_urls} ?& ARRAY[{resource_types_sql}]::text[]
        AND {start_urls} - ARRAY[{resource_types_sql}]::text[] = '{{}}'::jsonb
        AND NOT EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_each({start_urls}) AS start_url(type, value)
             WHERE pg_catalog.jsonb_typeof(start_url.value) <> 'string'
                OR NULLIF(start_url.value #>> '{{}}', '') IS NULL
        )
        AND pg_catalog.jsonb_typeof({page_caps}) = 'object'
        AND {page_caps} ?& ARRAY[{resource_types_sql}]::text[]
        AND {page_caps} - ARRAY[{resource_types_sql}]::text[] = '{{}}'::jsonb
        AND NOT EXISTS (
            SELECT 1
             FROM pg_catalog.jsonb_each({page_caps}) AS page_cap(type, value)
             WHERE page_cap.value IS DISTINCT FROM
                   {dataset_alias}.completion_proof_json -> 'page_count'
        )
    """


def _subset_source_sql(
    schema: str,
    *,
    require_verified: bool,
    dataset_alias: str = "NEW",
    use_configured_endpoint_identity: bool = False,
    require_physical_match: bool = True,
    reviewed_root_policy_aware: bool = False,
    reviewed_subset_profile_aware: bool = False,
    reviewed_subset_terminal_window_profile_aware: bool = False,
) -> str:
    """Bind a terminal row to its current reviewed manual source."""

    source_ref = _qf(schema, _SOURCE)
    metadata = f"{dataset_alias}.publication_metadata_json::jsonb"
    source_metadata = "current_source.metadata_json::jsonb"
    fixed_identity_sql = _subset_source_fixed_identity_sql(
        source_metadata,
        dataset_alias,
        reviewed_subset_profile_aware=reviewed_subset_profile_aware,
        reviewed_subset_terminal_window_profile_aware=(
            reviewed_subset_terminal_window_profile_aware
        ),
    )
    scope_payload_sql = _subset_source_scope_payload_sql(
        "current_source",
        source_metadata,
        metadata,
        dataset_alias,
        use_configured_endpoint_identity=use_configured_endpoint_identity,
    )
    policy_scope_payload_sql = _subset_source_scope_payload_sql(
        "current_source",
        source_metadata,
        metadata,
        dataset_alias,
        use_configured_endpoint_identity=use_configured_endpoint_identity,
        include_reviewed_root_policy=True,
    )
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    if require_verified:
        source_status_sql = f"= {_ql(_SUBSET_VERIFIED_SOURCE_STATUS)}"
        policy_source_status_sql = (
            f"= {_ql(_ROOT_POLICY_VERIFIED_SOURCE_STATUS)}"
        )
    else:
        source_status_sql = (
            "IN ("
            + _ql(_SUBSET_PENDING_SOURCE_STATUS)
            + ", "
            + _ql(_SUBSET_VERIFIED_SOURCE_STATUS)
            + ")"
        )
        policy_source_status_sql = (
            "IN ("
            + _ql(_ROOT_POLICY_PENDING_SOURCE_STATUS)
            + ", "
            + _ql(_ROOT_POLICY_VERIFIED_SOURCE_STATUS)
            + ")"
        )
    legacy_source_identity_sql = f"""
        NOT ({metadata} ? {_ql(_REVIEWED_ROOT_POLICY_KEY)})
        AND NOT ({source_metadata} ? {_ql(_REVIEWED_ROOT_POLICY_KEY)})
        AND {source_metadata}
             ->> 'provider_directory_candidate_status'
             {source_status_sql}
        AND {canonical_sha256_ref}({scope_payload_sql}) =
             {metadata} ->> {_ql(_TWIN_SCOPE_KEY)}
    """
    policy_source_identity_sql = f"""
        pg_catalog.jsonb_typeof(
            {metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)}
        ) = 'object'
        AND {metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)} =
            pg_catalog.jsonb_build_object(
                'policy_version', {_ql(_REVIEWED_ROOT_POLICY_VERSION)},
                'required_root_count',
                    {metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)}
                        -> 'required_root_count'
            )
        AND {metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)}
                -> 'required_root_count' IN ('1'::jsonb, '2'::jsonb)
        AND {source_metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)} =
            {metadata} -> {_ql(_REVIEWED_ROOT_POLICY_KEY)}
        AND {source_metadata}
             ->> 'provider_directory_candidate_status'
             {policy_source_status_sql}
        AND {canonical_sha256_ref}({policy_scope_payload_sql}) =
             {metadata} ->> {_ql(_TWIN_SCOPE_KEY)}
    """
    source_identity_sql = (
        f"(({legacy_source_identity_sql}) OR ({policy_source_identity_sql}))"
        if reviewed_root_policy_aware
        else f"({source_metadata} ->> "
             "'provider_directory_candidate_status' "
             f"{source_status_sql} AND "
             f"{canonical_sha256_ref}({scope_payload_sql}) = "
             f"{metadata} ->> {_ql(_TWIN_SCOPE_KEY)})"
    )
    if require_physical_match:
        physical_endpoint_sql = (
            f"current_source.endpoint_id = {dataset_alias}.endpoint_id"
        )
    else:
        physical_endpoint_sql = (
            "NULLIF(current_source.endpoint_id, '') IS NOT NULL"
        )
    return f"""
        pg_catalog.jsonb_typeof({metadata} -> 'source_ids') = 'array'
        AND pg_catalog.jsonb_array_length({metadata} -> 'source_ids') = 1
        AND EXISTS (
            SELECT 1
              FROM {source_ref} AS current_source
             WHERE current_source.source_id =
                    {metadata} -> 'source_ids' ->> 0
               AND {physical_endpoint_sql}
               AND current_source.canonical_api_base IS NOT NULL
               AND current_source.requires_registration IS FALSE
               AND current_source.requires_api_key IS FALSE
               AND current_source.auth_type = 'none'
               AND pg_catalog.jsonb_typeof({source_metadata}) = 'object'
               AND {source_metadata}
                    ->> 'provider_directory_configured_endpoint_id' =
                    {dataset_alias}.endpoint_id
               AND ({fixed_identity_sql})
               AND ({source_identity_sql})
        )
        AND NOT EXISTS (
            SELECT 1
              FROM {source_ref} AS endpoint_source
             WHERE (
                    endpoint_source.endpoint_id = {dataset_alias}.endpoint_id
                    OR endpoint_source.metadata_json::jsonb
                         ->> 'provider_directory_configured_endpoint_id' =
                         {dataset_alias}.endpoint_id
               )
               AND endpoint_source.source_id IS DISTINCT FROM
                    ({metadata} -> 'source_ids' ->> 0)
        )
    """


def _subset_published_source_guard_sql(
    schema: str,
    *,
    use_configured_endpoint_identity: bool = False,
    replace_existing: bool = False,
    reviewed_root_policy_aware: bool = False,
    reviewed_subset_profile_aware: bool = False,
    reviewed_subset_terminal_window_profile_aware: bool = False,
) -> str:
    """Reject source mutations that invalidate published subset evidence."""

    guard_ref = _qf(schema, _SOURCE_GUARD)
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    source_valid_sql = _subset_source_sql(
        schema,
        require_verified=True,
        dataset_alias="published_dataset",
        use_configured_endpoint_identity=use_configured_endpoint_identity,
        require_physical_match=True,
        reviewed_root_policy_aware=reviewed_root_policy_aware,
        reviewed_subset_profile_aware=reviewed_subset_profile_aware,
        reviewed_subset_terminal_window_profile_aware=(
            reviewed_subset_terminal_window_profile_aware
        ),
    )
    create_function = (
        "CREATE OR REPLACE FUNCTION"
        if replace_existing
        else "CREATE FUNCTION"
    )
    return f"""
    {create_function} {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        affected_source_ids text[] := ARRAY[]::text[];
        affected_endpoint_ids text[] := ARRAY[]::text[];
    BEGIN
        IF pg_catalog.current_setting('transaction_isolation') <>
                'read committed' THEN
            RAISE EXCEPTION
                'provider_directory_subset_source_isolation_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
                SELECT 1
                  FROM {dataset_ref} AS published_dataset
                 WHERE published_dataset.completion_proof_required_version = 3
                   AND (
                        published_dataset.status = 'published'
                        OR published_dataset.is_current IS TRUE
                   )
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_published_source_mutation_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_OP <> 'INSERT' THEN
            affected_source_ids := pg_catalog.array_append(
                affected_source_ids, OLD.source_id::text
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids, OLD.endpoint_id::text
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids,
                NULLIF(
                    OLD.metadata_json::jsonb
                        ->> 'provider_directory_configured_endpoint_id',
                    ''
                )
            );
        END IF;
        IF TG_OP <> 'DELETE' THEN
            affected_source_ids := pg_catalog.array_append(
                affected_source_ids, NEW.source_id::text
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids, NEW.endpoint_id::text
            );
            affected_endpoint_ids := pg_catalog.array_append(
                affected_endpoint_ids,
                NULLIF(
                    NEW.metadata_json::jsonb
                        ->> 'provider_directory_configured_endpoint_id',
                    ''
                )
            );
        END IF;
        affected_source_ids := pg_catalog.array_remove(
            affected_source_ids, NULL
        );
        affected_endpoint_ids := pg_catalog.array_remove(
            affected_endpoint_ids, NULL
        );

        IF EXISTS (
            SELECT 1
              FROM {dataset_ref} AS published_dataset
             WHERE published_dataset.completion_proof_required_version = 3
               AND (
                    published_dataset.status = 'published'
                    OR published_dataset.is_current IS TRUE
               )
               AND (
                    published_dataset.endpoint_id =
                        ANY(affected_endpoint_ids)
                    OR published_dataset.publication_metadata_json::jsonb
                         -> 'source_ids' ?| affected_source_ids
               )
               AND ({source_valid_sql}) IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_published_source_mutation_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _create_subset_published_source_guard(schema: str) -> None:
    source_ref = _qf(schema, _SOURCE)
    guard_ref = _qf(schema, _SOURCE_GUARD)
    op.execute(_subset_published_source_guard_sql(schema))
    op.execute(
        f"""
        CREATE CONSTRAINT TRIGGER {_q(_SOURCE_GUARD_TRIGGER)}
        AFTER INSERT OR UPDATE OR DELETE ON {source_ref}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION {guard_ref}();
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q(_SOURCE_TRUNCATE_GUARD_TRIGGER)}
        BEFORE TRUNCATE ON {source_ref}
        FOR EACH STATEMENT EXECUTE FUNCTION {guard_ref}();
        """
    )
    op.execute(
        f"""
        ALTER TABLE {source_ref}
            ENABLE ALWAYS TRIGGER {_q(_SOURCE_GUARD_TRIGGER)};
        """
    )
    op.execute(
        f"""
        ALTER TABLE {source_ref}
            ENABLE ALWAYS TRIGGER {_q(_SOURCE_TRUNCATE_GUARD_TRIGGER)};
        """
    )
    op.execute(
        f"""
        REVOKE ALL ON FUNCTION {guard_ref}() FROM PUBLIC;
        """
    )


def _drop_subset_published_source_guard(schema: str) -> None:
    source_ref = _qf(schema, _SOURCE)
    guard_ref = _qf(schema, _SOURCE_GUARD)
    op.execute(
        f"DROP TRIGGER {_q(_SOURCE_GUARD_TRIGGER)} ON {source_ref};"
    )
    op.execute(
        f"DROP TRIGGER {_q(_SOURCE_TRUNCATE_GUARD_TRIGGER)} ON {source_ref};"
    )
    op.execute(f"DROP FUNCTION {guard_ref}();")


def _subset_endpoint_dataset_guard_sql(
    schema: str,
    *,
    use_configured_endpoint_identity: bool = False,
    reviewed_root_policy_aware: bool = False,
    reviewed_subset_profile_aware: bool = False,
    reviewed_subset_terminal_window_profile_aware: bool = False,
) -> str:
    guard_ref = _qf(schema, _ENDPOINT_DATASET_GUARD)
    source_ref = _qf(schema, _SOURCE)
    pair_valid_ref = _qf(schema, _PROOF_PAIR_VALID_FUNCTION)
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    proof_shape_valid_ref = _qf(schema, _PROOF_SHAPE_VALID_FUNCTION)
    replay_shape_valid_ref = _qf(schema, _REPLAY_SHAPE_VALID_FUNCTION)
    coverage_shape_valid_ref = _qf(schema, _COVERAGE_SHAPE_VALID_FUNCTION)
    lifecycle_sql = _endpoint_dataset_lifecycle_sql(
        _SUBSET_IMMUTABLE_COMPARISON_SQL,
        subset_metadata_is_immutable=True,
    )
    matched_twin_sql = _subset_matched_twin_sql(
        schema,
        reviewed_root_policy_aware=reviewed_root_policy_aware,
    )
    single_root_sql = _subset_single_root_sql(schema)
    baseline_twin_sql = _subset_baseline_twin_sql()
    mismatch_twin_sql = _subset_mismatch_twin_sql(schema)
    terminal_source_sql = _subset_source_sql(
        schema,
        require_verified=False,
        use_configured_endpoint_identity=use_configured_endpoint_identity,
        require_physical_match=not use_configured_endpoint_identity,
        reviewed_root_policy_aware=reviewed_root_policy_aware,
        reviewed_subset_profile_aware=reviewed_subset_profile_aware,
        reviewed_subset_terminal_window_profile_aware=(
            reviewed_subset_terminal_window_profile_aware
        ),
    )
    published_source_sql = _subset_source_sql(
        schema,
        require_verified=True,
        use_configured_endpoint_identity=use_configured_endpoint_identity,
        require_physical_match=True,
        reviewed_root_policy_aware=reviewed_root_policy_aware,
        reviewed_subset_profile_aware=reviewed_subset_profile_aware,
        reviewed_subset_terminal_window_profile_aware=(
            reviewed_subset_terminal_window_profile_aware
        ),
    )
    metadata = "NEW.publication_metadata_json::jsonb"
    expected_coverage_state_sql = (
        f"CASE WHEN ({_reviewed_root_policy_sql(metadata, 1)}) "
        "THEN 'not_required' ELSE "
        f"{metadata} -> {_ql(_TWIN_VERIFICATION_KEY)} ->> 'result' END"
        if reviewed_root_policy_aware
        else f"{metadata} -> {_ql(_TWIN_VERIFICATION_KEY)} ->> 'result'"
    )
    twin_terminal_policy_sql = (
        f"""
        (
            NOT ({metadata} ? {_ql(_REVIEWED_ROOT_POLICY_KEY)})
            OR ({_reviewed_root_policy_sql(metadata, 2)})
        )
        """
        if reviewed_root_policy_aware
        else "true"
    )
    validated_proof_sql = (
        f"(({matched_twin_sql}) OR ({single_root_sql}))"
        if reviewed_root_policy_aware
        else f"({matched_twin_sql})"
    )
    dataset_content_sql = _subset_dataset_content_sql(schema)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            IF NEW.status IN ('validated', 'published', 'superseded')
               OR NEW.completion_proof_required_version NOT IN (3)
               OR NEW.completion_proof_json IS NOT NULL
               OR NEW.completion_proof_sha256 IS NOT NULL
               OR (
                    NEW.completion_proof_required_version = 3
                    AND NEW.status IN (
                        'verification_baseline',
                        'verification_mismatch'
                    )
               ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_insert_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;

        IF TG_OP = 'DELETE' THEN
            IF OLD.status IN ('validated', 'published', 'superseded')
               OR (
                    OLD.completion_proof_required_version = 3
                    AND OLD.status IN ({_TERMINAL_STATUSES_SQL})
               ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_delete_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN OLD;
        END IF;

        IF NEW.completion_proof_required_version IS DISTINCT FROM
                OLD.completion_proof_required_version THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_marker_immutable'
                USING ERRCODE = '55000';
        END IF;
        IF (NEW.completion_proof_json IS NULL) <>
                (NEW.completion_proof_sha256 IS NULL) THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_pair_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF NEW.completion_proof_json IS NOT NULL AND
                {pair_valid_ref}(
                    NEW.completion_proof_json,
                    NEW.completion_proof_sha256
                ) IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_digest_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF OLD.completion_proof_json IS NOT NULL
           OR OLD.completion_proof_sha256 IS NOT NULL THEN
            IF NEW.completion_proof_json IS DISTINCT FROM
                    OLD.completion_proof_json
               OR NEW.completion_proof_sha256 IS DISTINCT FROM
                    OLD.completion_proof_sha256 THEN
                RAISE EXCEPTION
                    'provider_directory_subset_completion_proof_immutable'
                    USING ERRCODE = '55000';
            END IF;
        ELSIF NEW.completion_proof_json IS NOT NULL
              OR NEW.completion_proof_sha256 IS NOT NULL THEN
            IF OLD.completion_proof_required_version IS DISTINCT FROM 3
               OR OLD.status IN ({_TERMINAL_STATUSES_SQL})
               OR NEW.status NOT IN ({_TERMINAL_STATUSES_SQL}) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_completion_proof_invalid'
                    USING ERRCODE = '55000';
            END IF;
        END IF;

        IF NEW.completion_proof_required_version = 3 THEN
            IF NEW.status IN ({_TERMINAL_STATUSES_SQL}) AND (
                NEW.completion_proof_json IS NULL
                OR NEW.completion_proof_sha256 IS NULL
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_completion_proof_required'
                    USING ERRCODE = '55000';
            ELSIF NEW.status NOT IN ({_TERMINAL_STATUSES_SQL}) AND (
                NEW.completion_proof_json IS NOT NULL
                OR NEW.completion_proof_sha256 IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_completion_proof_invalid'
                    USING ERRCODE = '55000';
            END IF;
        ELSIF NEW.completion_proof_json IS NOT NULL
              OR NEW.completion_proof_sha256 IS NOT NULL THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_proof_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND NEW.status IN ({_TERMINAL_STATUSES_SQL})
           AND {proof_shape_valid_ref}(
                NEW.completion_proof_json,
                NEW.dataset_hash,
                NEW.resource_count
           ) IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider_directory_subset_completion_proof_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND NEW.status IN ({_TERMINAL_STATUSES_SQL})
           AND ({dataset_content_sql}) IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider_directory_subset_dataset_content_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND NEW.status IN ({_TERMINAL_STATUSES_SQL})
           AND (
                pg_catalog.jsonb_typeof(
                    NEW.publication_metadata_json::jsonb
                        -> {_ql(_REPLAY_EVIDENCE_KEY)}
                ) IS DISTINCT FROM 'object'
                OR pg_catalog.jsonb_typeof(
                    NEW.publication_metadata_json::jsonb
                        -> {_ql(_REPLAY_EVIDENCE_SHA256_KEY)}
                ) IS DISTINCT FROM 'string'
                OR (
                    NEW.publication_metadata_json::jsonb
                        ->> {_ql(_REPLAY_EVIDENCE_SHA256_KEY)}
                    = {canonical_sha256_ref}(
                        NEW.publication_metadata_json::jsonb
                            -> {_ql(_REPLAY_EVIDENCE_KEY)}
                    )
                ) IS DISTINCT FROM TRUE
                OR {replay_shape_valid_ref}(
                    NEW.publication_metadata_json::jsonb
                        -> {_ql(_REPLAY_EVIDENCE_KEY)},
                    NEW.publication_metadata_json::jsonb
                        ->> {_ql(_REPLAY_EVIDENCE_SHA256_KEY)},
                    NEW.completion_proof_json,
                    NEW.completion_proof_sha256
                ) IS DISTINCT FROM TRUE
                OR {coverage_shape_valid_ref}(
                    NEW.publication_metadata_json::jsonb
                        -> {_ql(_SUBSET_COVERAGE_KEY)},
                    NEW.completion_proof_json,
                    NEW.completion_proof_sha256,
                    {expected_coverage_state_sql}
                ) IS DISTINCT FROM TRUE
           ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_replay_evidence_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND NEW.status = 'verification_baseline'
           AND (
                ({baseline_twin_sql}) IS DISTINCT FROM TRUE
                OR ({twin_terminal_policy_sql}) IS DISTINCT FROM TRUE
           ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_baseline_twin_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND NEW.status = 'verification_mismatch'
           AND (
                ({mismatch_twin_sql}) IS DISTINCT FROM TRUE
                OR ({twin_terminal_policy_sql}) IS DISTINCT FROM TRUE
           ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_mismatch_twin_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND NEW.status IN ('validated', 'published')
           AND ({validated_proof_sql}) IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider_directory_subset_matched_twin_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND OLD.status NOT IN ({_TERMINAL_STATUSES_SQL})
           AND NEW.status IN (
                'verification_baseline',
                'verification_mismatch',
                'validated'
           )
           AND ({terminal_source_sql}) IS DISTINCT FROM TRUE THEN
            RAISE EXCEPTION
                'provider_directory_subset_terminal_source_invalid'
                USING ERRCODE = '55000';
        END IF;

        IF NEW.completion_proof_required_version = 3
           AND OLD.status = 'validated'
           AND NEW.status = 'published' THEN
            IF pg_catalog.current_setting('transaction_isolation') <>
                    'read committed' THEN
                RAISE EXCEPTION
                    'provider_directory_subset_source_isolation_invalid'
                    USING ERRCODE = '55000';
            END IF;
            LOCK TABLE {source_ref} IN SHARE MODE;
            IF ({published_source_sql}) IS DISTINCT FROM TRUE THEN
                RAISE EXCEPTION
                    'provider_directory_subset_published_source_invalid'
                    USING ERRCODE = '55000';
            END IF;
        END IF;

        IF OLD.status NOT IN ({_TERMINAL_STATUSES_SQL}) THEN
            IF NEW.status IN ('published', 'superseded') THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF OLD.completion_proof_required_version IS DISTINCT FROM 3
           AND OLD.status IN (
                'verification_baseline',
                'verification_mismatch'
           ) THEN
            IF NEW.status IN ('published', 'superseded') THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF OLD.completion_proof_required_version = 3
           AND OLD.status IN (
                'verification_baseline',
                'verification_mismatch'
           ) THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF {lifecycle_sql} THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _legacy_endpoint_dataset_guard_sql(schema: str) -> str:
    guard_ref = _qf(schema, _ENDPOINT_DATASET_GUARD)
    lifecycle_sql = _endpoint_dataset_lifecycle_sql(
        _LEGACY_IMMUTABLE_COMPARISON_SQL,
        subset_metadata_is_immutable=False,
    )
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            IF NEW.status IN (
                'validated',
                'published',
                'superseded'
            ) THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_insert_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF OLD.status NOT IN ('validated', 'published', 'superseded') THEN
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            IF NEW.status IN ('published', 'superseded') THEN
                RAISE EXCEPTION
                    'tin_npi_connector_endpoint_dataset_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_delete_forbidden'
                USING ERRCODE = '55000';
        END IF;
        IF {lifecycle_sql} THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_transition_invalid'
                USING ERRCODE = '55000';
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _dataset_resource_guard_sql(
    schema: str,
    *,
    subset_aware: bool,
) -> str:
    guard_ref = _qf(schema, _DATASET_RESOURCE_GUARD)
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    insert_predicate = """
                dataset.status IN ('validated', 'published', 'superseded')
    """
    update_predicate = insert_predicate
    delete_predicate = """
                dataset.status IN ('validated', 'published')
    """
    insert_digest_check = ""
    update_digest_check = ""
    digest_rejection = ""
    digest_declaration = ""
    if subset_aware:
        subset_predicate = f"""
                OR (
                    dataset.completion_proof_required_version = 3
                    AND dataset.status IN ({_TERMINAL_STATUSES_SQL})
                )
        """
        insert_predicate += subset_predicate
        update_predicate += subset_predicate
        delete_predicate += subset_predicate
        insert_digest_check = f"""
            SELECT COUNT(*)
              INTO invalid_digest_parent_count
              FROM new_rows AS resource
              JOIN {dataset_ref} AS dataset
                ON dataset.dataset_id = resource.dataset_id
             WHERE ((
                       dataset.completion_proof_required_version = 3
                    ) IS TRUE) IS DISTINCT FROM (
                       resource.acquired_resource_sha256 IS NOT NULL
                    );
        """
        update_digest_check = insert_digest_check
        digest_declaration = "invalid_digest_parent_count bigint;"
        digest_rejection = """
        IF invalid_digest_parent_count <> 0 THEN
            RAISE EXCEPTION
                'provider_directory_subset_acquired_digest_marker_invalid'
                USING ERRCODE = '55000';
        END IF;
        """
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        immutable_parent_count bigint;
        {digest_declaration}
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION
                'tin_npi_connector_dataset_resource_truncate_forbidden'
                USING ERRCODE = '55000';
        ELSIF TG_OP = 'INSERT' THEN
            PERFORM dataset.dataset_id
              FROM {dataset_ref} AS dataset
              JOIN (
                    SELECT DISTINCT dataset_id
                      FROM new_rows
                   ) AS affected
                ON affected.dataset_id = dataset.dataset_id
             ORDER BY dataset.dataset_id
               FOR SHARE OF dataset;
            SELECT COUNT(*)
              INTO immutable_parent_count
              FROM (
                    SELECT DISTINCT dataset_id
                      FROM new_rows
                   ) AS affected
              JOIN {dataset_ref} AS dataset
                ON dataset.dataset_id = affected.dataset_id
             WHERE {insert_predicate};
            {insert_digest_check}
        ELSIF TG_OP = 'DELETE' THEN
            PERFORM dataset.dataset_id
              FROM {dataset_ref} AS dataset
              JOIN (
                    SELECT DISTINCT dataset_id
                      FROM old_rows
                   ) AS affected
                ON affected.dataset_id = dataset.dataset_id
             ORDER BY dataset.dataset_id
               FOR SHARE OF dataset;
            SELECT COUNT(*)
              INTO immutable_parent_count
              FROM (
                    SELECT DISTINCT dataset_id
                      FROM old_rows
                   ) AS affected
              JOIN {dataset_ref} AS dataset
                ON dataset.dataset_id = affected.dataset_id
             WHERE {delete_predicate};
        ELSIF TG_OP = 'UPDATE' THEN
            PERFORM dataset.dataset_id
              FROM {dataset_ref} AS dataset
              JOIN (
                    SELECT dataset_id FROM old_rows
                    UNION
                    SELECT dataset_id FROM new_rows
                   ) AS affected
                ON affected.dataset_id = dataset.dataset_id
             ORDER BY dataset.dataset_id
               FOR SHARE OF dataset;
            SELECT COUNT(*)
              INTO immutable_parent_count
              FROM (
                    SELECT dataset_id FROM old_rows
                    UNION
                    SELECT dataset_id FROM new_rows
                   ) AS affected
              JOIN {dataset_ref} AS dataset
                ON dataset.dataset_id = affected.dataset_id
             WHERE {update_predicate};
            {update_digest_check}
        ELSE
            RAISE EXCEPTION
                'tin_npi_connector_dataset_resource_action_invalid'
                USING ERRCODE = '55000';
        END IF;
        IF immutable_parent_count <> 0 THEN
            RAISE EXCEPTION
                'tin_npi_connector_dataset_resource_parent_immutable'
                USING ERRCODE = '55000';
        END IF;
        {digest_rejection}
        RETURN NULL;
    END;
    $function$;
    """


def _subset_proof_shape_check(
    schema: str,
    *,
    reviewed_subset_profile_aware: bool = False,
    reviewed_subset_terminal_window_profile_aware: bool = False,
) -> str:
    proof_shape_valid_ref = _qf(schema, _PROOF_SHAPE_VALID_FUNCTION)
    terminal_window_profile_sql = (
        """
        OR (
            completion_proof_json ->> 'strategy_version' =
                'provider-directory-fhir-server-issued-traversal-subset-v5'
            AND completion_proof_json -> 'completion_scopes' =
                '["advertised-count-monotone-decrease-bounded-by-one-percent-and-twenty-pages",'
                '"terminal-logical-window-covers-advertised-pre",'
                '"source-issued-continuation",'
                '"returned-resource-content"]'::jsonb
        )
        """
        if reviewed_subset_terminal_window_profile_aware
        else ""
    )
    profile_sql = (
        f"""
        (
            completion_proof_json ->> 'strategy_version' =
                'provider-directory-fhir-server-issued-traversal-subset-v3'
            AND completion_proof_json -> 'completion_scopes' =
                '["advertised-count-stability",'
                '"source-issued-continuation",'
                '"returned-resource-content"]'::jsonb
        ) OR (
            completion_proof_json ->> 'strategy_version' =
                'provider-directory-fhir-server-issued-traversal-subset-v4'
            AND completion_proof_json -> 'completion_scopes' =
                '["advertised-count-monotone-decrease-at-most-one",'
                '"source-issued-continuation",'
                '"returned-resource-content"]'::jsonb
        )
        {terminal_window_profile_sql}
        """
        if (
            reviewed_subset_profile_aware
            or reviewed_subset_terminal_window_profile_aware
        )
        else """
        completion_proof_json ->> 'strategy_version' =
            'provider-directory-fhir-server-issued-traversal-subset-v3'
        AND completion_proof_json -> 'completion_scopes' =
            '["advertised-count-stability",'
            '"source-issued-continuation",'
            '"returned-resource-content"]'::jsonb
        """
    )
    return """
    completion_proof_json IS NULL OR (
        pg_catalog.jsonb_typeof(completion_proof_json) = 'object'
        AND completion_proof_json ?& ARRAY[
            'proof_version',
            'contract_version',
            'semantics',
            'strategy_version',
            'traversal_version',
            'canonicalization_version',
            'completion_scopes',
            'campaign_id',
            'cutoff',
            'page_count',
            'resources',
            'dataset'
        ]::text[]
        AND completion_proof_json - ARRAY[
            'proof_version',
            'contract_version',
            'semantics',
            'strategy_version',
            'traversal_version',
            'canonicalization_version',
            'completion_scopes',
            'campaign_id',
            'cutoff',
            'page_count',
            'resources',
            'dataset'
        ]::text[] = '{}'::jsonb
        AND completion_proof_json ->> 'proof_version' =
            'provider-directory-fhir-server-issued-subset-completion-v1'
        AND completion_proof_json -> 'contract_version' = '3'::jsonb
        AND completion_proof_json ->> 'semantics' =
            'server-issued-traversal-subset'
        AND ({profile_sql})
        AND completion_proof_json ->> 'traversal_version' =
            'provider-directory-fhir-smile-logical-offset-v3'
        AND completion_proof_json ->> 'canonicalization_version' =
            'provider-directory-fhir-returned-resource-json-v2'
        AND pg_catalog.jsonb_typeof(
            completion_proof_json -> 'campaign_id'
        ) = 'string'
        AND completion_proof_json ->> 'campaign_id' <> ''
        AND pg_catalog.jsonb_typeof(completion_proof_json -> 'cutoff') =
            'string'
        AND completion_proof_json ->> 'cutoff' ~
            '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:'
            '[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z$'
        AND pg_catalog.jsonb_typeof(
            completion_proof_json -> 'page_count'
        ) = 'number'
        AND completion_proof_json ->> 'page_count' ~
            '^([1-9][0-9]{0,2}|1000)$'
        AND pg_catalog.jsonb_typeof(
            completion_proof_json -> 'resources'
        ) = 'object'
        AND completion_proof_json -> 'resources' ?& ARRAY[
            'HealthcareService',
            'InsurancePlan',
            'Location',
            'Organization',
            'OrganizationAffiliation',
            'Practitioner',
            'PractitionerRole'
        ]::text[]
        AND (completion_proof_json -> 'resources') - ARRAY[
            'HealthcareService',
            'InsurancePlan',
            'Location',
            'Organization',
            'OrganizationAffiliation',
            'Practitioner',
            'PractitionerRole'
        ]::text[] = '{}'::jsonb
        AND pg_catalog.jsonb_typeof(completion_proof_json -> 'dataset') =
            'object'
        AND completion_proof_json -> 'dataset' ?& ARRAY[
            'hash',
            'count',
            'resource_hashes',
            'resource_counts',
            'acquired_resource_hashes'
        ]::text[]
        AND (completion_proof_json -> 'dataset') - ARRAY[
            'hash',
            'count',
            'resource_hashes',
            'resource_counts',
            'acquired_resource_hashes'
        ]::text[] = '{}'::jsonb
        AND dataset_hash IS NOT NULL
        AND completion_proof_json -> 'dataset' ->> 'hash' = dataset_hash
        AND completion_proof_json -> 'dataset' -> 'count' =
            pg_catalog.to_jsonb(resource_count)
        AND {proof_shape_valid_ref}(
            completion_proof_json,
            dataset_hash,
            resource_count
        ) IS TRUE
    )
    """.replace("{profile_sql}", profile_sql).replace(
        "{proof_shape_valid_ref}", proof_shape_valid_ref
    )


def _completion_digest_check(schema: str) -> str:
    pair_valid_ref = _qf(schema, _PROOF_PAIR_VALID_FUNCTION)
    return f"""
    completion_proof_sha256 IS NULL OR
        {pair_valid_ref}(
            completion_proof_json,
            completion_proof_sha256
        ) IS TRUE
    """


def _subset_replay_evidence_check(
    schema: str,
    *,
    reviewed_root_policy_aware: bool = False,
) -> str:
    canonical_sha256_ref = _qf(schema, _CANONICAL_SHA256_FUNCTION)
    replay_shape_valid_ref = _qf(schema, _REPLAY_SHAPE_VALID_FUNCTION)
    coverage_shape_valid_ref = _qf(schema, _COVERAGE_SHAPE_VALID_FUNCTION)
    expected_twin_state_sql = (
        f"CASE WHEN ({_reviewed_root_policy_sql('publication_metadata_json::jsonb', 1)}) "
        "THEN 'not_required' ELSE publication_metadata_json::jsonb "
        f"-> {_ql(_TWIN_VERIFICATION_KEY)} ->> 'result' END"
        if reviewed_root_policy_aware
        else (
            "publication_metadata_json::jsonb "
            f"-> {_ql(_TWIN_VERIFICATION_KEY)} ->> 'result'"
        )
    )
    return f"""
    completion_proof_required_version IS DISTINCT FROM 3
    OR status NOT IN ({_TERMINAL_STATUSES_SQL})
    OR (
        pg_catalog.jsonb_typeof(
            publication_metadata_json::jsonb
                -> {_ql(_REPLAY_EVIDENCE_KEY)}
        ) IS NOT DISTINCT FROM 'object'
        AND pg_catalog.jsonb_typeof(
            publication_metadata_json::jsonb
                -> {_ql(_REPLAY_EVIDENCE_SHA256_KEY)}
        ) IS NOT DISTINCT FROM 'string'
        AND (
            publication_metadata_json::jsonb
                ->> {_ql(_REPLAY_EVIDENCE_SHA256_KEY)}
            = {canonical_sha256_ref}(
                publication_metadata_json::jsonb
                    -> {_ql(_REPLAY_EVIDENCE_KEY)}
            )
        ) IS TRUE
        AND {replay_shape_valid_ref}(
            publication_metadata_json::jsonb
                -> {_ql(_REPLAY_EVIDENCE_KEY)},
            publication_metadata_json::jsonb
                ->> {_ql(_REPLAY_EVIDENCE_SHA256_KEY)},
            completion_proof_json,
            completion_proof_sha256
        ) IS TRUE
        AND {coverage_shape_valid_ref}(
            publication_metadata_json::jsonb
                -> {_ql(_SUBSET_COVERAGE_KEY)},
            completion_proof_json,
            completion_proof_sha256,
            {expected_twin_state_sql}
        ) IS TRUE
    )
    """


def _revoke_guard_execute(schema: str) -> None:
    endpoint_guard = _qf(schema, _ENDPOINT_DATASET_GUARD)
    resource_guard = _qf(schema, _DATASET_RESOURCE_GUARD)
    op.execute(f"REVOKE ALL ON FUNCTION {endpoint_guard}() FROM PUBLIC;")
    op.execute(f"REVOKE ALL ON FUNCTION {resource_guard}() FROM PUBLIC;")


def _lock_guarded_relations(schema: str) -> None:
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    resource_ref = _qf(schema, _DATASET_RESOURCE)
    source_ref = _qf(schema, _SOURCE)
    op.execute(
        f"LOCK TABLE {dataset_ref}, {resource_ref}, {source_ref} "
        "IN ACCESS EXCLUSIVE MODE;"
    )


def upgrade() -> None:
    schema = _schema()
    _lock_guarded_relations(schema)
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _ENDPOINT_DATASET,
            _LEGACY_ENDPOINT_DATASET_COLUMNS,
            compatible_columns=_SUBSET_ENDPOINT_DATASET_COLUMNS,
        )
    )
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _DATASET_RESOURCE,
            _LEGACY_DATASET_RESOURCE_COLUMNS,
            compatible_columns=_SUBSET_DATASET_RESOURCE_COLUMNS,
        )
    )
    op.execute(_guard_trigger_shape_fence_sql(schema))
    op.execute(
        _source_guard_shape_fence_sql(
            schema,
            expect_installed=False,
        )
    )

    add_column_if_missing(
        op,
        _ENDPOINT_DATASET,
        sa.Column(
            "completion_proof_required_version",
            sa.Integer(),
            nullable=True,
        ),
        schema=schema,
    )
    add_column_if_missing(
        op,
        _ENDPOINT_DATASET,
        sa.Column(
            "completion_proof_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        schema=schema,
    )
    add_column_if_missing(
        op,
        _ENDPOINT_DATASET,
        sa.Column("completion_proof_sha256", sa.String(64), nullable=True),
        schema=schema,
    )
    add_column_if_missing(
        op,
        _DATASET_RESOURCE,
        sa.Column("acquired_resource_sha256", sa.String(64), nullable=True),
        schema=schema,
    )
    op.execute(_subset_column_shape_fence_sql(schema))
    _create_subset_baseline_generation_index(schema)
    _create_proof_functions(schema)

    op.create_check_constraint(
        _PARENT_CHECKS[0],
        _ENDPOINT_DATASET,
        "completion_proof_required_version IS NULL "
        "OR completion_proof_required_version = 3",
        schema=schema,
    )
    op.create_check_constraint(
        _PARENT_CHECKS[1],
        _ENDPOINT_DATASET,
        "(completion_proof_json IS NULL) = "
        "(completion_proof_sha256 IS NULL)",
        schema=schema,
    )
    op.create_check_constraint(
        _PARENT_CHECKS[2],
        _ENDPOINT_DATASET,
        "completion_proof_required_version IS NOT NULL "
        "OR (completion_proof_json IS NULL "
        "AND completion_proof_sha256 IS NULL)",
        schema=schema,
    )
    op.create_check_constraint(
        _PARENT_CHECKS[3],
        _ENDPOINT_DATASET,
        _completion_digest_check(schema),
        schema=schema,
    )
    op.create_check_constraint(
        _PARENT_CHECKS[4],
        _ENDPOINT_DATASET,
        _subset_proof_shape_check(schema),
        schema=schema,
    )
    op.create_check_constraint(
        _PARENT_CHECKS[5],
        _ENDPOINT_DATASET,
        _subset_replay_evidence_check(schema),
        schema=schema,
    )
    op.create_check_constraint(
        _CHILD_DIGEST_CHECK,
        _DATASET_RESOURCE,
        "acquired_resource_sha256 IS NULL OR "
        "acquired_resource_sha256 ~ '^[0-9a-f]{64}$'",
        schema=schema,
    )

    op.execute(_subset_endpoint_dataset_guard_sql(schema))
    op.execute(
        _dataset_resource_guard_sql(
            schema,
            subset_aware=True,
        )
    )
    _revoke_guard_execute(schema)
    _create_subset_published_source_guard(schema)
    op.execute(_guard_trigger_shape_fence_sql(schema))
    op.execute(
        _source_guard_shape_fence_sql(
            schema,
            expect_installed=True,
        )
    )
    op.execute(_proof_function_shape_fence_sql(schema))
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _ENDPOINT_DATASET,
            _SUBSET_ENDPOINT_DATASET_COLUMNS,
        )
    )
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _DATASET_RESOURCE,
            _SUBSET_DATASET_RESOURCE_COLUMNS,
        )
    )


def downgrade() -> None:
    schema = _schema()
    dataset_ref = _qf(schema, _ENDPOINT_DATASET)
    resource_ref = _qf(schema, _DATASET_RESOURCE)
    _lock_guarded_relations(schema)
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _ENDPOINT_DATASET,
            _SUBSET_ENDPOINT_DATASET_COLUMNS,
        )
    )
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _DATASET_RESOURCE,
            _SUBSET_DATASET_RESOURCE_COLUMNS,
        )
    )
    op.execute(_guard_trigger_shape_fence_sql(schema))
    op.execute(
        _source_guard_shape_fence_sql(
            schema,
            expect_installed=True,
        )
    )
    op.execute(_proof_function_shape_fence_sql(schema))
    op.execute(
        f"""
        DO $migration$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM {dataset_ref}
                 WHERE completion_proof_required_version IS NOT NULL
                    OR completion_proof_json IS NOT NULL
                    OR completion_proof_sha256 IS NOT NULL
            ) OR EXISTS (
                SELECT 1
                  FROM {resource_ref}
                 WHERE acquired_resource_sha256 IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_completion_downgrade_blocked'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $migration$;
        """
    )

    op.execute(_legacy_endpoint_dataset_guard_sql(schema))
    op.execute(
        _dataset_resource_guard_sql(
            schema,
            subset_aware=False,
        )
    )
    _revoke_guard_execute(schema)
    _drop_subset_published_source_guard(schema)

    op.drop_constraint(
        _CHILD_DIGEST_CHECK,
        _DATASET_RESOURCE,
        type_="check",
        schema=schema,
    )
    _drop_subset_baseline_generation_index(schema)
    for constraint_name in reversed(_PARENT_CHECKS):
        op.drop_constraint(
            constraint_name,
            _ENDPOINT_DATASET,
            type_="check",
            schema=schema,
        )
    op.drop_column(
        _DATASET_RESOURCE,
        "acquired_resource_sha256",
        schema=schema,
    )
    op.drop_column(
        _ENDPOINT_DATASET,
        "completion_proof_sha256",
        schema=schema,
    )
    op.drop_column(
        _ENDPOINT_DATASET,
        "completion_proof_json",
        schema=schema,
    )
    op.drop_column(
        _ENDPOINT_DATASET,
        "completion_proof_required_version",
        schema=schema,
    )
    _drop_proof_functions(schema)

    op.execute(_guard_trigger_shape_fence_sql(schema))
    op.execute(
        _source_guard_shape_fence_sql(
            schema,
            expect_installed=False,
        )
    )
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _ENDPOINT_DATASET,
            _LEGACY_ENDPOINT_DATASET_COLUMNS,
        )
    )
    op.execute(
        _relation_schema_fence_sql(
            schema,
            _DATASET_RESOURCE,
            _LEGACY_DATASET_RESOURCE_COLUMNS,
        )
    )
