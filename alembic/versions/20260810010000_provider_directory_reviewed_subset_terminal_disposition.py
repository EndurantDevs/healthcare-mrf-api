# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seal one exact reviewed-subset mixed terminal disposition.

Revision ID: 20260810010000_provider_directory_reviewed_subset_terminal_disposition
Revises: 20260810000000_provider_directory_reviewed_subset_bounded_drift
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = (
    "20260810010000_provider_directory_reviewed_subset_terminal_disposition"
)
down_revision = (
    "20260810000000_provider_directory_reviewed_subset_bounded_drift"
)
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = (
    "20260810000000_provider_directory_reviewed_subset_bounded_drift.py"
)
_MARKER = "provider_directory_reviewed_subset_terminal_disposition_v1"
_LEGACY_MARKER = "provider_directory_reviewed_subset_abandonment_v1"
_CONTRACT = (
    "healthporta.provider-directory.reviewed-subset-terminal-disposition.v1"
)
_REASON = "bounded_advertised_count_drift_with_retained_progress"
_STATUS = "acquisition_abandoned"
_PRIOR_STATUS = "failed"
_VALID = "provider_directory_subset_terminal_disposition_valid"
_DATASET_CONSTRAINT = (
    "pd_subset_terminal_disposition_dataset_consistency_guard"
)
_CHECKPOINT_CONSTRAINT = (
    "provider_directory_subset_terminal_disposition_checkpoint_guard"
)
_STABLE_COMPLETE = "stable_complete"
_COUNT_DRIFT = "terminal_count_drift"
_RETRYABLE_HTTP_500 = "retryable_http_500"
_FETCH_MODE = "server_issued_traversal_subset"
_BLOCKED_CENSUS_DRIFT = (
    "provider_directory_current_version_census_completeness_blocked:"
    "census_drift"
)
_RETRYABLE_HTTP_500_ERROR = (
    "provider_directory_current_version_census_completeness_retryable:"
    "http_500"
)
_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v3"
)
_SEMANTICS = "server-issued-traversal-subset"
_TRAVERSAL_VERSION = "provider-directory-fhir-smile-logical-offset-v3"
_CANONICALIZATION_VERSION = (
    "provider-directory-fhir-returned-resource-json-v2"
)
_COMPLETION_SCOPES_JSON = (
    '["advertised-count-stability","source-issued-continuation",'
    '"returned-resource-content"]'
)
_SOURCE_PROFILE_RESOURCE_TYPES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
)
_DIAGNOSTIC_INTEGER_FIELDS = (
    "rows_written",
    "source_fetch_elapsed_ms",
    "page_prefetch_started",
    "page_prefetch_consumed",
    "page_prefetch_discarded",
    "pagination_cooldown_retries",
)
_DIAGNOSTIC_NUMBER_FIELDS = (
    "stream_write_elapsed_seconds",
    "checkpoint_persist_elapsed_seconds",
    "page_prefetch_wait_seconds",
    "pagination_cooldown_wait_seconds",
)
_DIAGNOSTIC_BOOLEAN_FIELDS = (
    "page_prefetch_eligible",
    "pagination_cooldown_recovered",
    "pagination_cooldown_exhausted",
    "pagination_cooldown_deadline_blocked",
)
_MARKER_FIELDS = (
    "contract_version",
    "reason_code",
    "source_scope_sha256",
    "resource_types",
    "resource_dispositions",
    "checkpoint_count",
    "checkpoint_pages_processed",
    "diagnostic_pages_processed",
    "terminal_page_delta",
    "checkpoint_rows_processed",
    "resource_count",
    "proof_shard_count",
    "proof_row_count",
    "source_diagnostics_sha256",
    "source_import_sha256",
    "candidate_metadata_sha256",
)
_RESOURCE_DISPOSITION_FIELDS = (
    "disposition",
    "checkpoint_state",
    "checkpoint_pages",
    "diagnostic_pages",
    "page_delta",
    "retained_rows",
    "advertised_pre",
    "advertised_post",
    "returned_unique",
    "deficit",
    "diagnostic_sha256",
    "checkpoint_proof_sha256",
    "start_url_sha256",
    "recent_cursor_hashes_sha256",
)
_COMPLETION_PROOF_FIELDS = (
    "acquisition_root_run_id",
    "terminal_run_id",
    "source_ids",
    "selected_resources",
    "resource_diagnostics",
    "verification_campaign_id",
    "verification_source_scope_hash",
)
_SOURCE_IMPORT_FIELDS = ("run_id", "observed_at", "resources")
_DIAGNOSTIC_FIELDS = (
    "absence_semantics",
    "bounded",
    "caresource_opaque_cursor_completeness",
    "checkpoint_persist_elapsed_seconds",
    "collection_complete",
    "complete",
    "current_version_census_completeness",
    "deadline_reached",
    "error",
    "fetch_mode",
    "hard_page_limit_reached",
    "last_updated_completeness",
    "next_url_remaining",
    "page_limit_reached",
    "page_prefetch_consumed",
    "page_prefetch_discarded",
    "page_prefetch_eligible",
    "page_prefetch_started",
    "page_prefetch_wait_seconds",
    "pages_fetched",
    "pagination_cooldown_deadline_blocked",
    "pagination_cooldown_exhausted",
    "pagination_cooldown_recovered",
    "pagination_cooldown_retries",
    "pagination_cooldown_wait_seconds",
    "plan_graph_complete",
    "retry_not_before",
    "row_limit_reached",
    "rows_fetched",
    "rows_written",
    "server_issued_subset_completeness",
    "server_issued_subset_coverage",
    "source_continuation_exhausted",
    "source_fetch",
    "source_fetch_elapsed_ms",
    "stream_write_elapsed_seconds",
    "traversal_complete",
)
_ACTIVE_PROOF_FIELDS = (
    "campaign_id",
    "canonicalization_version",
    "completion_scopes",
    "continuation_hop_sha256",
    "continuation_shape_sha256",
    "contract_identity",
    "contract_version",
    "cutoff",
    "page_count",
    "page_entry_counts",
    "page_geometry",
    "pre_count",
    "resource_type",
    "semantics",
    "strategy_version",
    "traversal_version",
    "verified",
)
_COMPLETED_PROOF_FIELDS = (
    "advertised_post",
    "advertised_pre",
    "deficit",
    "post_count",
    "processed_rows",
    "returned_unique",
    "terminal_page_geometry",
    "terminal_reason",
    "unique_candidate_rows",
    "unreturned_count",
)
_STABLE_COMPLETE_PROOF_FIELDS = tuple(
    sorted((*_ACTIVE_PROOF_FIELDS, *_COMPLETED_PROOF_FIELDS))
)
_COUNT_DRIFT_PROOF_FIELDS = tuple(
    sorted((*_STABLE_COMPLETE_PROOF_FIELDS, "failure"))
)


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_reviewed_subset_terminal_disposition_predecessor",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(
            "provider directory bounded-drift revision is unavailable"
        )
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _subset() -> ModuleType:
    return _predecessor()._subset()


def _abandonment() -> ModuleType:
    return _predecessor()._abandonment()


def _resource_array_sql() -> str:
    subset = _subset()
    return (
        "ARRAY["
        + ", ".join(
            subset._ql(resource_type)
            for resource_type in subset._SUBSET_RESOURCE_TYPES
        )
        + "]::text[]"
    )


def _source_profile_resource_array_sql() -> str:
    subset = _subset()
    return (
        "ARRAY["
        + ", ".join(
            subset._ql(resource_type)
            for resource_type in _SOURCE_PROFILE_RESOURCE_TYPES
        )
        + "]::text[]"
    )


def _json_fields_sql(fields: tuple[str, ...]) -> str:
    subset = _subset()
    return ", ".join(subset._ql(field_name) for field_name in fields)


def _lock_relations(schema: str) -> None:
    subset = _subset()
    abandonment = _abandonment()
    relation_names = (
        subset._ENDPOINT_DATASET,
        subset._DATASET_RESOURCE,
        subset._SOURCE,
        abandonment._PROOF_SHARD,
        abandonment._CHECKPOINT,
        abandonment._BULK_CHECKPOINT,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(
            subset._qf(schema, relation_name)
            for relation_name in relation_names
        )
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _base_shape_fences(schema: str) -> None:
    predecessor = _predecessor()
    abandonment = _abandonment()
    subset = _subset()
    activation = predecessor._activation()
    for fence_sql in abandonment._shape_fence_sqls(schema):
        op.execute(fence_sql)
    op.execute(abandonment._preflight_sql(schema, expect_installed=True))
    op.execute(subset._proof_function_shape_fence_sql(schema))
    op.execute(
        activation._activation_shape_fence_sql(
            schema,
            expect_installed=True,
        )
    )
    op.execute(
        predecessor._proof_check_shape_fence_sql(
            schema,
            bounded_profile_installed=True,
        )
    )
    op.execute(
        predecessor._function_profile_fence_sql(
            schema,
            bounded_profile_installed=True,
        )
    )


def _number_sql(value_sql: str, *, nullable: bool = False) -> str:
    null_branch = " OR {value} = 'null'::jsonb" if nullable else ""
    null_branch = null_branch.format(value=value_sql)
    return f"""
        (
            (
                pg_catalog.jsonb_typeof({value_sql}) = 'number'
                AND {value_sql} #>> '{{}}' ~ '^(0|[1-9][0-9]*)$'
            ){null_branch}
        )
    """


def _nonnegative_json_number_sql(value_sql: str) -> str:
    return f"""
        pg_catalog.jsonb_typeof({value_sql}) = 'number'
        AND ({value_sql} #>> '{{}}')::numeric >= 0
    """


def _sha_scalar_invalid_sql(value_sql: str) -> str:
    return f"""
        pg_catalog.jsonb_typeof({value_sql}) IS DISTINCT FROM 'string'
        OR {value_sql} #>> '{{}}' !~ '^[0-9a-f]{{64}}$'
    """


def _sha_vector_invalid_sql(
    vector_sql: str,
    expected_count_sql: str,
) -> str:
    return f"""
        pg_catalog.jsonb_typeof({vector_sql}) IS DISTINCT FROM 'array'
        OR pg_catalog.jsonb_array_length({vector_sql})
             IS DISTINCT FROM ({expected_count_sql})
        OR EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(
                   CASE
                       WHEN pg_catalog.jsonb_typeof({vector_sql}) = 'array'
                       THEN {vector_sql}
                       ELSE '[]'::jsonb
                   END
              ) AS digest(value)
             WHERE pg_catalog.jsonb_typeof(digest.value) <> 'string'
                OR digest.value #>> '{{}}' !~ '^[0-9a-f]{{64}}$'
        )
    """


def _sha_array_invalid_sql(vector_sql: str) -> str:
    """Reject non-array or non-SHA256 checkpoint hash history."""

    return f"""
        pg_catalog.jsonb_typeof({vector_sql}) IS DISTINCT FROM 'array'
        OR EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(
                   CASE
                       WHEN pg_catalog.jsonb_typeof({vector_sql}) = 'array'
                       THEN {vector_sql}
                       ELSE '[]'::jsonb
                   END
              ) AS digest(value)
             WHERE pg_catalog.jsonb_typeof(digest.value) <> 'string'
                OR digest.value #>> '{{}}' !~ '^[0-9a-f]{{64}}$'
        )
    """


def _recent_history_invalid_sql() -> str:
    """Return exact retained cursor-history lifecycle checks."""

    recent = "checkpoint_row.recent_cursor_hashes::jsonb"
    history_length = """
        checkpoint_row.pages_processed
        - CASE WHEN diagnostic -> 'complete' = 'true'::jsonb THEN 1 ELSE 0 END
    """
    return f"""
        ({history_length}) < 0
        OR pg_catalog.jsonb_array_length({recent}) IS DISTINCT FROM
             LEAST(({history_length}), 64::bigint)
        OR (
            SELECT pg_catalog.count(*)
              FROM pg_catalog.jsonb_array_elements({recent}) AS cursor(value)
        ) IS DISTINCT FROM (
            SELECT pg_catalog.count(DISTINCT cursor.value #>> '{{}}')
              FROM pg_catalog.jsonb_array_elements({recent}) AS cursor(value)
        )
        OR (
            ({history_length}) BETWEEN 1 AND 64
            AND {recent} ->> 0 IS DISTINCT FROM checkpoint_row.start_url_hash
        )
    """


def _expected_coverage_sql(evidence_sha256_ref: str) -> str:
    """Rebuild the exact identifier-free coverage projection."""

    return f"""
        pg_catalog.jsonb_build_object(
            'cutoff', proof -> 'cutoff',
            'scope', 'server_issued_traversal_subset',
            'advertised_pre', proof -> 'advertised_pre',
            'advertised_post', proof -> 'advertised_post',
            'returned_unique', proof -> 'returned_unique',
            'deficit', proof -> 'deficit',
            'geometry', CASE
                WHEN pg_catalog.jsonb_typeof(terminal_geometry) = 'object'
                THEN pg_catalog.jsonb_build_object(
                    'pages', terminal_geometry -> 'pages_processed',
                    'logical_terminal_offset',
                        terminal_geometry -> 'terminal_page_start_offset',
                    'sparse_pages', terminal_geometry -> 'sparse_pages',
                    'empty_pages', terminal_geometry -> 'empty_pages',
                    'page_entry_counts_sha256', {evidence_sha256_ref}(
                        proof -> 'page_entry_counts'
                    ),
                    'geometry_sha256', {evidence_sha256_ref}(
                        terminal_geometry || pg_catalog.jsonb_build_object(
                            'page_entry_counts', proof -> 'page_entry_counts'
                        )
                    )
                )
                ELSE 'null'::jsonb
            END,
            'continuation', pg_catalog.jsonb_build_object(
                'validated_hops', pg_catalog.jsonb_array_length(
                    proof -> 'continuation_shape_sha256'
                ),
                'chain_sha256', {evidence_sha256_ref}(
                    proof -> 'continuation_shape_sha256'
                )
            ),
            'twin_state', 'pending_matching_reviewed_root',
            'proof_state', CASE
                WHEN proof -> 'verified' = 'true'::jsonb
                THEN 'resource_terminal_verified'
                ELSE 'not_verified'
            END,
            'unresolved_reference_count', 'null'::jsonb,
            'absence_semantics', 'unknown_under_subset'
        )
    """


def _text_sha256_sql(value_sql: str) -> str:
    return f"""
        pg_catalog.encode(
            pg_catalog.sha256(pg_catalog.convert_to({value_sql}, 'UTF8')),
            'hex'
        )
    """


def _source_contract_identity_sql(source_alias: str, proof_sql: str) -> str:
    """Render the full admitted source contract identity payload."""

    subset = _subset()
    source_metadata = f"{source_alias}.metadata_json::jsonb"
    start_hash_fields = ", ".join(
        subset._ql(resource_type)
        + ", "
        + _text_sha256_sql(
            source_metadata
            + " #>> "
            + subset._ql(
                "{provider_directory_current_version_census_start_urls,"
                + resource_type
                + "}"
            )
        )
        for resource_type in _SOURCE_PROFILE_RESOURCE_TYPES
    )
    source_resources = _source_profile_resource_array_sql()
    return f"""
        pg_catalog.jsonb_build_object(
            'contract_version', {proof_sql} -> 'contract_version',
            'semantics', {proof_sql} -> 'semantics',
            'source_id', {source_alias}.source_id,
            'strategy', {proof_sql} -> 'strategy_version',
            'cutoff', {proof_sql} -> 'cutoff',
            'resources', pg_catalog.to_jsonb({source_resources}),
            'expected_nonempty_resources',
                pg_catalog.to_jsonb({source_resources}),
            'continuation_strategy', {source_metadata} ->
                'provider_directory_current_version_census_continuation_strategy',
            'reviewed_start_url_sha256_by_resource',
                pg_catalog.jsonb_build_object({start_hash_fields}),
            'page_count', {proof_sql} -> 'page_count',
            'traversal_version', {proof_sql} -> 'traversal_version',
            'canonicalization_version',
                {proof_sql} -> 'canonicalization_version',
            'completion_scopes', {proof_sql} -> 'completion_scopes',
            'campaign_id', {proof_sql} -> 'campaign_id'
        )
    """


def _expected_start_url_sha256_sql(
    source_alias: str,
    proof_sql: str,
    resource_type_sql: str,
) -> str:
    """Hash the exact reviewed request URL for a canonical UTC cutoff."""

    reviewed_url = f"""
        {source_alias}.canonical_api_base || '/' || {resource_type_sql}
        || '?_lastUpdated=lt'
        || pg_catalog.replace({proof_sql} ->> 'cutoff', ':', '%3A')
        || '&_count=' || ({proof_sql} ->> 'page_count')
    """
    return _text_sha256_sql(reviewed_url)


def _valid_function_sql(schema: str) -> str:
    """Return the closed, exception-safe retained-evidence validator."""

    subset = _subset()
    abandonment = _abandonment()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    resource_ref = subset._qf(schema, subset._DATASET_RESOURCE)
    proof_ref = subset._qf(schema, abandonment._PROOF_SHARD)
    checkpoint_ref = subset._qf(schema, abandonment._CHECKPOINT)
    bulk_ref = subset._qf(schema, abandonment._BULK_CHECKPOINT)
    valid_ref = subset._qf(schema, _VALID)
    evidence_sha256_ref = subset._qf(
        schema,
        subset._PAYLOAD_SHA256_FUNCTION,
    )
    marker_fields = _json_fields_sql(_MARKER_FIELDS)
    disposition_fields = _json_fields_sql(_RESOURCE_DISPOSITION_FIELDS)
    completion_fields = _json_fields_sql(_COMPLETION_PROOF_FIELDS)
    diagnostic_fields = _json_fields_sql(_DIAGNOSTIC_FIELDS)
    active_proof_fields = _json_fields_sql(_ACTIVE_PROOF_FIELDS)
    stable_proof_fields = _json_fields_sql(_STABLE_COMPLETE_PROOF_FIELDS)
    drift_proof_fields = _json_fields_sql(_COUNT_DRIFT_PROOF_FIELDS)
    resources = _resource_array_sql()
    diagnostic_integers_valid = " AND ".join(
        "(" + _number_sql("diagnostic -> " + subset._ql(field_name)) + ")"
        for field_name in _DIAGNOSTIC_INTEGER_FIELDS
    )
    diagnostic_numbers_valid = " AND ".join(
        "("
        + _nonnegative_json_number_sql(
            "diagnostic -> " + subset._ql(field_name)
        )
        + ")"
        for field_name in _DIAGNOSTIC_NUMBER_FIELDS
    )
    diagnostic_booleans_valid = " AND ".join(
        "pg_catalog.jsonb_typeof(diagnostic -> "
        + subset._ql(field_name)
        + ") = 'boolean'"
        for field_name in _DIAGNOSTIC_BOOLEAN_FIELDS
    )
    expected_coverage = _expected_coverage_sql(evidence_sha256_ref)
    recent_history_invalid = _recent_history_invalid_sql()
    reviewed_policy = subset._reviewed_root_policy_sql(
        "candidate_metadata",
        1,
    )
    page_entries_invalid = """
        pg_catalog.jsonb_typeof(proof -> 'page_entry_counts')
            IS DISTINCT FROM 'array'
        OR EXISTS (
            SELECT 1
              FROM pg_catalog.jsonb_array_elements(
                   CASE
                       WHEN pg_catalog.jsonb_typeof(
                                proof -> 'page_entry_counts'
                            ) = 'array'
                       THEN proof -> 'page_entry_counts'
                       ELSE '[]'::jsonb
                   END
              ) AS page_entry(value)
             WHERE pg_catalog.jsonb_typeof(page_entry.value) <> 'number'
                OR page_entry.value #>> '{}' !~ '^(0|[1-9][0-9]*)$'
                OR (page_entry.value #>> '{}')::numeric > page_count
        )
    """
    return f"""
    CREATE FUNCTION {valid_ref}(candidate_dataset_id text)
    RETURNS boolean
    LANGUAGE plpgsql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        candidate_row record;
        checkpoint_row record;
        candidate_metadata jsonb;
        diagnostics jsonb;
        completion_copy jsonb;
        marker jsonb;
        disposition jsonb;
        diagnostic jsonb;
        proof jsonb;
        page_geometry jsonb;
        terminal_geometry jsonb;
        current_resource_type text;
        checkpoint_canonical_api_base text;
        expected_resources jsonb := pg_catalog.to_jsonb({resources});
        checkpoint_count bigint;
        relation_resource_count bigint;
        resource_type_count bigint;
        proof_shard_count bigint;
        proof_row_count numeric;
        invalid_proof_count bigint;
        proof_resource_count numeric;
        disposition_count_complete integer := 0;
        disposition_count_drift integer := 0;
        disposition_count_retryable integer := 0;
        checkpoint_pages numeric := 0;
        diagnostic_pages numeric := 0;
        checkpoint_rows numeric := 0;
        terminal_page_delta numeric := 0;
        page_count numeric;
        entry_count_sum numeric;
        checkpoint_entry_sum numeric;
        sparse_page_count bigint;
        empty_page_count bigint;
        terminal_pages numeric;
        terminal_entries numeric;
        geometry_pages bigint;
        geometry_rows bigint;
        completed_count_fields_present boolean;
        shared_proof_identity jsonb;
    BEGIN
        SELECT dataset.* INTO STRICT candidate_row
          FROM {dataset_ref} AS dataset
         WHERE dataset.dataset_id = candidate_dataset_id;
        candidate_metadata := candidate_row.publication_metadata_json::jsonb;
        marker := candidate_metadata -> '{_MARKER}';

        IF candidate_row.status <> '{_STATUS}'
           OR candidate_row.is_current IS NOT FALSE
           OR candidate_row.completion_proof_required_version IS DISTINCT FROM 3
           OR candidate_row.completion_proof_json IS NOT NULL
           OR candidate_row.completion_proof_sha256 IS NOT NULL
           OR candidate_row.validated_at IS NOT NULL
           OR candidate_row.published_at IS NOT NULL
           OR candidate_row.superseded_at IS NOT NULL
           OR candidate_metadata ? '{_LEGACY_MARKER}'
           OR pg_catalog.jsonb_typeof(marker) IS DISTINCT FROM 'object'
           OR NOT (marker ?& ARRAY[{marker_fields}]::text[])
           OR marker - ARRAY[{marker_fields}]::text[] <> '{{}}'::jsonb
           OR marker ->> 'contract_version' <> '{_CONTRACT}'
           OR marker ->> 'reason_code' <> '{_REASON}'
           OR ({_sha_scalar_invalid_sql("marker -> 'source_scope_sha256'")})
           OR marker -> 'resource_types' IS DISTINCT FROM expected_resources
           OR pg_catalog.jsonb_typeof(marker -> 'resource_dispositions')
                IS DISTINCT FROM 'object'
           OR (marker -> 'resource_dispositions') - {resources} <> '{{}}'::jsonb
           OR EXISTS (
                SELECT 1
                  FROM pg_catalog.unnest({resources}) AS expected(resource_type)
                 WHERE NOT (
                       marker -> 'resource_dispositions' ? expected.resource_type
                 )
           )
           OR NOT ({_number_sql("marker -> 'checkpoint_count'")})
           OR NOT ({_number_sql("marker -> 'checkpoint_pages_processed'")})
           OR NOT ({_number_sql("marker -> 'diagnostic_pages_processed'")})
           OR NOT ({_number_sql("marker -> 'terminal_page_delta'")})
           OR NOT ({_number_sql("marker -> 'checkpoint_rows_processed'")})
           OR NOT ({_number_sql("marker -> 'resource_count'")})
           OR NOT ({_number_sql("marker -> 'proof_shard_count'")})
           OR NOT ({_number_sql("marker -> 'proof_row_count'")})
           OR ({_sha_scalar_invalid_sql("marker -> 'source_diagnostics_sha256'")})
           OR ({_sha_scalar_invalid_sql("marker -> 'source_import_sha256'")})
           OR ({_sha_scalar_invalid_sql("marker -> 'candidate_metadata_sha256'")})
           OR marker ->> 'candidate_metadata_sha256' IS DISTINCT FROM
                {evidence_sha256_ref}(candidate_metadata - '{_MARKER}')
           OR candidate_metadata -> 'source_ids' IS DISTINCT FROM
                pg_catalog.jsonb_build_array(
                    candidate_metadata #>> '{{source_ids,0}}'
                )
           OR NULLIF(candidate_metadata #>> '{{source_ids,0}}', '') IS NULL
           OR candidate_metadata -> 'selected_resources'
                IS DISTINCT FROM expected_resources
           OR candidate_metadata -> 'expected_resources'
                IS DISTINCT FROM expected_resources
           OR candidate_metadata -> 'requires_twin_root_verification'
                IS DISTINCT FROM 'false'::jsonb
           OR candidate_metadata -> 'completion_proof_required_version'
                IS DISTINCT FROM '3'::jsonb
           OR candidate_metadata ->> 'resource_hash_contract'
                <> 'transport_neutral_v2'
           OR candidate_metadata -> 'reused_from_checkpoint'
                IS DISTINCT FROM 'true'::jsonb
           OR pg_catalog.jsonb_typeof(
                candidate_metadata -> 'acquisition_root_run_id'
              ) IS DISTINCT FROM 'string'
           OR candidate_metadata ->> 'acquisition_root_run_id'
                IS DISTINCT FROM candidate_row.acquisition_root_run_id
           OR ({reviewed_policy}) IS DISTINCT FROM TRUE
           OR ({_sha_scalar_invalid_sql(
                "candidate_metadata -> 'verification_source_scope_hash'"
              )})
           OR candidate_metadata ->> 'verification_source_scope_hash'
                = marker ->> 'source_scope_sha256'
           OR pg_catalog.jsonb_typeof(
                candidate_metadata -> 'verification_campaign_id'
              ) IS DISTINCT FROM 'string'
           OR NULLIF(
                candidate_metadata ->> 'verification_campaign_id',
                ''
              ) IS NULL THEN
            RETURN FALSE;
        END IF;

        diagnostics := candidate_metadata -> 'resource_diagnostics';
        completion_copy := candidate_metadata -> 'completion_proof_v1';
        IF pg_catalog.jsonb_typeof(diagnostics) IS DISTINCT FROM 'object'
           OR diagnostics - {resources} <> '{{}}'::jsonb
           OR NOT (diagnostics ?& {resources})
           OR pg_catalog.jsonb_typeof(completion_copy) IS DISTINCT FROM 'object'
           OR NOT (completion_copy ?& ARRAY[{completion_fields}]::text[])
           OR completion_copy - ARRAY[{completion_fields}]::text[]
                <> '{{}}'::jsonb
           OR diagnostics IS DISTINCT FROM
                completion_copy -> 'resource_diagnostics'
           OR pg_catalog.jsonb_typeof(
                completion_copy -> 'acquisition_root_run_id'
              ) IS DISTINCT FROM 'string'
           OR completion_copy ->> 'acquisition_root_run_id'
                IS DISTINCT FROM candidate_row.acquisition_root_run_id
           OR pg_catalog.jsonb_typeof(completion_copy -> 'terminal_run_id')
                IS DISTINCT FROM 'string'
           OR completion_copy ->> 'terminal_run_id'
                IS DISTINCT FROM candidate_row.import_run_id
           OR completion_copy -> 'source_ids'
                IS DISTINCT FROM candidate_metadata -> 'source_ids'
           OR completion_copy -> 'selected_resources'
                IS DISTINCT FROM expected_resources
           OR completion_copy -> 'verification_campaign_id'
                IS DISTINCT FROM candidate_metadata -> 'verification_campaign_id'
           OR completion_copy -> 'verification_source_scope_hash'
                IS DISTINCT FROM
                   candidate_metadata -> 'verification_source_scope_hash'
           OR marker ->> 'source_diagnostics_sha256' IS DISTINCT FROM
                {evidence_sha256_ref}(diagnostics) THEN
            RETURN FALSE;
        END IF;

        SELECT pg_catalog.count(*) INTO checkpoint_count
          FROM {checkpoint_ref} AS checkpoint
         WHERE checkpoint.dataset_id = candidate_row.dataset_id;
        IF checkpoint_count <> pg_catalog.array_length({resources}, 1)
           OR checkpoint_count IS DISTINCT FROM
                (marker ->> 'checkpoint_count')::numeric THEN
            RETURN FALSE;
        END IF;

        FOREACH current_resource_type IN ARRAY {resources} LOOP
            IF (
                SELECT pg_catalog.count(*)
                  FROM {checkpoint_ref} AS checkpoint
                 WHERE checkpoint.dataset_id = candidate_row.dataset_id
                   AND checkpoint.resource_type = current_resource_type
            ) <> 1 THEN
                RETURN FALSE;
            END IF;
            SELECT checkpoint.* INTO STRICT checkpoint_row
              FROM {checkpoint_ref} AS checkpoint
             WHERE checkpoint.dataset_id = candidate_row.dataset_id
               AND checkpoint.resource_type = current_resource_type;
            IF checkpoint_canonical_api_base IS NULL THEN
                checkpoint_canonical_api_base :=
                    checkpoint_row.canonical_api_base;
            END IF;
            disposition := marker -> 'resource_dispositions'
                -> current_resource_type;
            diagnostic := diagnostics -> current_resource_type;
            proof := checkpoint_row.completeness_json::jsonb;
            page_geometry := proof -> 'page_geometry';
            terminal_geometry := proof -> 'terminal_page_geometry';
            geometry_pages := checkpoint_row.pages_processed;
            geometry_rows := checkpoint_row.rows_processed;
            IF disposition ->> 'disposition' = '{_STABLE_COMPLETE}' THEN
                geometry_pages := geometry_pages - 1;
                geometry_rows := geometry_rows
                    - (terminal_geometry ->> 'terminal_page_entries')::bigint;
            END IF;

            IF pg_catalog.jsonb_typeof(disposition) IS DISTINCT FROM 'object'
               OR NOT (disposition ?& ARRAY[{disposition_fields}]::text[])
               OR disposition - ARRAY[{disposition_fields}]::text[]
                    <> '{{}}'::jsonb
               OR ({_sha_scalar_invalid_sql(
                    "disposition -> 'diagnostic_sha256'"
                  )})
               OR ({_sha_scalar_invalid_sql(
                    "disposition -> 'checkpoint_proof_sha256'"
                  )})
               OR ({_sha_scalar_invalid_sql(
                    "disposition -> 'start_url_sha256'"
                  )})
               OR ({_sha_scalar_invalid_sql(
                    "disposition -> 'recent_cursor_hashes_sha256'"
                  )})
               OR disposition ->> 'diagnostic_sha256' IS DISTINCT FROM
                    {evidence_sha256_ref}(diagnostic)
               OR disposition ->> 'checkpoint_proof_sha256' IS DISTINCT FROM
                    {evidence_sha256_ref}(proof)
               OR disposition ->> 'start_url_sha256' IS DISTINCT FROM
                    checkpoint_row.start_url_hash
               OR disposition ->> 'recent_cursor_hashes_sha256'
                    IS DISTINCT FROM {evidence_sha256_ref}(
                        checkpoint_row.recent_cursor_hashes::jsonb
                    )
               OR NOT ({_number_sql("disposition -> 'checkpoint_pages'")})
               OR NOT ({_number_sql("disposition -> 'diagnostic_pages'")})
               OR NOT ({_number_sql("disposition -> 'page_delta'")})
               OR NOT ({_number_sql("disposition -> 'retained_rows'")})
               OR NOT ({_number_sql("disposition -> 'advertised_pre'", nullable=True)})
               OR NOT ({_number_sql("disposition -> 'advertised_post'", nullable=True)})
               OR NOT ({_number_sql("disposition -> 'returned_unique'", nullable=True)})
               OR NOT ({_number_sql("disposition -> 'deficit'", nullable=True)})
               OR NULLIF(checkpoint_row.canonical_api_base, '') IS NULL
               OR checkpoint_row.canonical_api_base IS DISTINCT FROM
                    checkpoint_canonical_api_base
               OR checkpoint_row.source_scope_hash IS DISTINCT FROM
                    marker ->> 'source_scope_sha256'
               OR checkpoint_row.source_ids::jsonb IS DISTINCT FROM
                    candidate_metadata -> 'source_ids'
               OR checkpoint_row.dataset_id IS DISTINCT FROM
                    candidate_row.dataset_id
               OR checkpoint_row.acquisition_root_run_id IS DISTINCT FROM
                    candidate_row.acquisition_root_run_id
               OR checkpoint_row.owner_run_id IS DISTINCT FROM
                    candidate_row.import_run_id
               OR checkpoint_row.state <> '{_STATUS}'
               OR checkpoint_row.completed_at IS NULL
               OR checkpoint_row.pages_processed < 0
               OR checkpoint_row.rows_processed < 0
               OR checkpoint_row.start_url_hash !~ '^[0-9a-f]{{64}}$'
               OR ({_sha_array_invalid_sql(
                    'checkpoint_row.recent_cursor_hashes::jsonb'
               )})
               OR ({recent_history_invalid})
               OR disposition -> 'checkpoint_pages' IS DISTINCT FROM
                    pg_catalog.to_jsonb(checkpoint_row.pages_processed)
               OR disposition -> 'retained_rows' IS DISTINCT FROM
                    pg_catalog.to_jsonb(checkpoint_row.rows_processed)
               OR pg_catalog.jsonb_typeof(diagnostic) IS DISTINCT FROM 'object'
               OR NOT (diagnostic ?& ARRAY[{diagnostic_fields}]::text[])
               OR diagnostic - ARRAY[{diagnostic_fields}]::text[]
                    <> '{{}}'::jsonb
               OR diagnostic ->> 'fetch_mode' <> '{_FETCH_MODE}'
               OR diagnostic -> 'bounded' IS DISTINCT FROM 'false'::jsonb
               OR diagnostic -> 'collection_complete'
                    IS DISTINCT FROM 'false'::jsonb
               OR diagnostic -> 'row_limit_reached'
                    IS DISTINCT FROM 'false'::jsonb
               OR diagnostic -> 'page_limit_reached'
                    IS DISTINCT FROM 'false'::jsonb
               OR diagnostic -> 'hard_page_limit_reached'
                    IS DISTINCT FROM 'false'::jsonb
               OR diagnostic -> 'deadline_reached'
                    IS DISTINCT FROM 'false'::jsonb
               OR diagnostic -> 'plan_graph_complete'
                    IS DISTINCT FROM 'false'::jsonb
               OR diagnostic -> 'source_fetch'
                    IS DISTINCT FROM 'null'::jsonb
               OR diagnostic -> 'last_updated_completeness'
                    IS DISTINCT FROM 'null'::jsonb
               OR diagnostic -> 'caresource_opaque_cursor_completeness'
                    IS DISTINCT FROM 'null'::jsonb
               OR diagnostic -> 'current_version_census_completeness'
                    IS DISTINCT FROM 'null'::jsonb
               OR diagnostic ->> 'absence_semantics'
                    <> 'unknown_under_subset'
               OR NOT ({_number_sql("diagnostic -> 'pages_fetched'")})
               OR NOT ({_number_sql("diagnostic -> 'rows_fetched'")})
               OR NOT ({diagnostic_integers_valid})
               OR NOT ({diagnostic_numbers_valid})
               OR NOT ({diagnostic_booleans_valid})
               OR NOT (
                    diagnostic -> 'retry_not_before' = 'null'::jsonb
                    OR (
                        pg_catalog.jsonb_typeof(
                            diagnostic -> 'retry_not_before'
                        ) = 'string'
                        AND NULLIF(
                            diagnostic ->> 'retry_not_before', ''
                        ) IS NOT NULL
                    )
               )
               OR diagnostic -> 'rows_fetched' IS DISTINCT FROM
                    pg_catalog.to_jsonb(checkpoint_row.rows_processed)
               OR diagnostic -> 'server_issued_subset_completeness'
                    IS DISTINCT FROM proof - 'continuation_hop_sha256'
               OR proof -> 'contract_version' IS DISTINCT FROM '3'::jsonb
               OR proof ->> 'strategy_version' <> '{_STRATEGY_VERSION}'
               OR proof ->> 'semantics' <> '{_SEMANTICS}'
               OR proof ->> 'traversal_version' <> '{_TRAVERSAL_VERSION}'
               OR proof ->> 'canonicalization_version'
                    <> '{_CANONICALIZATION_VERSION}'
               OR proof -> 'completion_scopes'
                    IS DISTINCT FROM '{_COMPLETION_SCOPES_JSON}'::jsonb
               OR proof ->> 'resource_type'
                    IS DISTINCT FROM current_resource_type
               OR pg_catalog.jsonb_typeof(proof -> 'cutoff')
                    IS DISTINCT FROM 'string'
               OR NULLIF(proof ->> 'cutoff', '') IS NULL
               OR proof ->> 'cutoff' !~
                    '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T'
                    '[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}\.[0-9]{{6}}Z$'
               OR ({_sha_scalar_invalid_sql("proof -> 'contract_identity'")})
               OR pg_catalog.jsonb_typeof(proof -> 'campaign_id')
                    IS DISTINCT FROM 'string'
               OR NULLIF(proof ->> 'campaign_id', '') IS NULL
               OR proof ->> 'campaign_id' IS DISTINCT FROM
                    candidate_metadata ->> 'verification_campaign_id'
               OR NOT ({_number_sql("proof -> 'page_count'")})
               OR (proof ->> 'page_count')::numeric NOT BETWEEN 1 AND 1000
               OR NOT ({_number_sql("proof -> 'pre_count'")})
               OR pg_catalog.jsonb_typeof(page_geometry)
                    IS DISTINCT FROM 'object'
               OR NOT (page_geometry ?& ARRAY[
                    'version', 'page_count', 'checkpointed_pages',
                    'checkpointed_rows', 'logical_next_offset',
                    'sparse_pages', 'empty_pages'
                  ]::text[])
               OR page_geometry - ARRAY[
                    'version', 'page_count', 'checkpointed_pages',
                    'checkpointed_rows', 'logical_next_offset',
                    'sparse_pages', 'empty_pages'
                  ]::text[] <> '{{}}'::jsonb
               OR EXISTS (
                    SELECT 1
                      FROM pg_catalog.jsonb_each(page_geometry)
                           AS geometry(field_name, field_value)
                     WHERE NOT ({_number_sql('geometry.field_value')})
               )
               OR page_geometry -> 'version' <> '2'::jsonb
               OR page_geometry -> 'page_count'
                    IS DISTINCT FROM proof -> 'page_count'
               OR page_geometry -> 'checkpointed_pages'
                    IS DISTINCT FROM
                       pg_catalog.to_jsonb(geometry_pages)
               OR page_geometry -> 'checkpointed_rows'
                    IS DISTINCT FROM
                       pg_catalog.to_jsonb(geometry_rows)
               OR (page_geometry ->> 'logical_next_offset')::numeric
                    IS DISTINCT FROM
                       geometry_pages::numeric
                       * (proof ->> 'page_count')::numeric
               OR (page_geometry ->> 'sparse_pages')::numeric
                    > geometry_pages
               OR (page_geometry ->> 'empty_pages')::numeric
                    > (page_geometry ->> 'sparse_pages')::numeric
               OR {page_entries_invalid}
               OR {_sha_vector_invalid_sql("proof -> 'continuation_hop_sha256'", 'geometry_pages')}
               OR {_sha_vector_invalid_sql("proof -> 'continuation_shape_sha256'", 'geometry_pages')} THEN
                RETURN FALSE;
            END IF;

            IF NOT (
                CASE
                    WHEN pg_catalog.pg_input_is_valid(
                         proof ->> 'cutoff', 'pg_catalog.timestamptz'
                    )
                    THEN pg_catalog.to_char(
                         (proof ->> 'cutoff')::pg_catalog.timestamptz
                             AT TIME ZONE 'UTC',
                         'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
                    ) = proof ->> 'cutoff'
                    ELSE FALSE
                END
            ) THEN
                RETURN FALSE;
            END IF;

            IF shared_proof_identity IS NULL THEN
                shared_proof_identity := pg_catalog.jsonb_build_array(
                    proof -> 'cutoff', proof -> 'contract_identity',
                    proof -> 'page_count', proof -> 'campaign_id'
                );
            ELSIF shared_proof_identity IS DISTINCT FROM
                    pg_catalog.jsonb_build_array(
                        proof -> 'cutoff', proof -> 'contract_identity',
                        proof -> 'page_count', proof -> 'campaign_id'
                    ) THEN
                RETURN FALSE;
            END IF;
            IF diagnostic -> 'server_issued_subset_coverage'
                    IS DISTINCT FROM ({expected_coverage}) THEN
                RETURN FALSE;
            END IF;

            page_count := (proof ->> 'page_count')::numeric;
            SELECT COALESCE(pg_catalog.sum((entry.value #>> '{{}}')::numeric), 0),
                   pg_catalog.count(*) FILTER (
                       WHERE (entry.value #>> '{{}}')::numeric < page_count
                   ),
                   pg_catalog.count(*) FILTER (
                       WHERE (entry.value #>> '{{}}')::numeric = 0
                   )
              INTO checkpoint_entry_sum, sparse_page_count, empty_page_count
              FROM (
                   SELECT entry.value, entry.ordinal
                     FROM pg_catalog.jsonb_array_elements(
                          proof -> 'page_entry_counts'
                     ) WITH ORDINALITY AS entry(value, ordinal)
                    WHERE entry.ordinal <= geometry_pages
              ) AS entry;
            IF checkpoint_entry_sum
                    IS DISTINCT FROM geometry_rows::numeric
               OR sparse_page_count IS DISTINCT FROM
                    (page_geometry ->> 'sparse_pages')::bigint
               OR empty_page_count IS DISTINCT FROM
                    (page_geometry ->> 'empty_pages')::bigint THEN
                RETURN FALSE;
            END IF;

            completed_count_fields_present :=
                proof ?& ARRAY[
                    'post_count', 'processed_rows',
                    'unique_candidate_rows', 'advertised_pre',
                    'advertised_post', 'returned_unique', 'deficit',
                    'terminal_reason', 'terminal_page_geometry'
                ]::text[];
            IF disposition ->> 'disposition' = '{_STABLE_COMPLETE}' THEN
                disposition_count_complete := disposition_count_complete + 1;
                IF current_resource_type <> ALL(
                       ARRAY['Organization', 'Practitioner']::text[]
                   )
                   OR NOT (proof ?& ARRAY[{stable_proof_fields}]::text[])
                   OR proof - ARRAY[{stable_proof_fields}]::text[]
                        <> '{{}}'::jsonb
                   OR checkpoint_row.rows_processed <= 0
                   OR disposition ->> 'checkpoint_state' <> 'complete'
                   OR disposition -> 'page_delta' <> '0'::jsonb
                   OR diagnostic -> 'complete' IS DISTINCT FROM 'true'::jsonb
                   OR diagnostic -> 'error' IS DISTINCT FROM 'null'::jsonb
                   OR diagnostic -> 'traversal_complete'
                        IS DISTINCT FROM 'true'::jsonb
                   OR diagnostic -> 'source_continuation_exhausted'
                        IS DISTINCT FROM 'true'::jsonb
                   OR diagnostic -> 'next_url_remaining'
                        IS DISTINCT FROM 'false'::jsonb
                   OR checkpoint_row.next_url IS NOT NULL
                   OR proof -> 'verified' IS DISTINCT FROM 'true'::jsonb
                   OR proof ? 'failure'
                   OR NOT completed_count_fields_present
                   OR proof -> 'pre_count' IS DISTINCT FROM proof -> 'post_count'
                   OR diagnostic -> 'pages_fetched' IS DISTINCT FROM
                        pg_catalog.to_jsonb(checkpoint_row.pages_processed)
                   OR disposition -> 'checkpoint_state' <> '"complete"'::jsonb
                   OR disposition -> 'diagnostic_pages' IS DISTINCT FROM
                        diagnostic -> 'pages_fetched' THEN
                    RETURN FALSE;
                END IF;
            ELSIF disposition ->> 'disposition' = '{_COUNT_DRIFT}' THEN
                disposition_count_drift := disposition_count_drift + 1;
                IF current_resource_type <> 'Location'
                   OR NOT (proof ?& ARRAY[{drift_proof_fields}]::text[])
                   OR proof - ARRAY[{drift_proof_fields}]::text[]
                        <> '{{}}'::jsonb
                   OR checkpoint_row.rows_processed <= 0
                   OR disposition ->> 'checkpoint_state' <> 'active'
                   OR disposition -> 'page_delta' <> '1'::jsonb
                   OR diagnostic -> 'complete' IS DISTINCT FROM 'false'::jsonb
                   OR diagnostic ->> 'error' <> '{_BLOCKED_CENSUS_DRIFT}'
                   OR diagnostic -> 'traversal_complete'
                        IS DISTINCT FROM 'false'::jsonb
                   OR diagnostic -> 'source_continuation_exhausted'
                        IS DISTINCT FROM 'false'::jsonb
                   OR diagnostic -> 'next_url_remaining'
                        IS DISTINCT FROM 'false'::jsonb
                   OR NULLIF(checkpoint_row.next_url, '') IS NULL
                   OR proof -> 'verified' IS DISTINCT FROM 'false'::jsonb
                   OR (proof ->> 'pre_count')::numeric <= 0
                   OR checkpoint_row.rows_processed >
                        (proof ->> 'pre_count')::numeric
                   OR proof ->> 'failure' <> 'census_drift'
                   OR NOT completed_count_fields_present
                   OR (proof ->> 'pre_count')::numeric
                        - (proof ->> 'post_count')::numeric <> 1
                   OR diagnostic -> 'pages_fetched' IS DISTINCT FROM
                        pg_catalog.to_jsonb(checkpoint_row.pages_processed + 1)
                   OR disposition -> 'diagnostic_pages' IS DISTINCT FROM
                        diagnostic -> 'pages_fetched' THEN
                    RETURN FALSE;
                END IF;
            ELSIF disposition ->> 'disposition' = '{_RETRYABLE_HTTP_500}' THEN
                disposition_count_retryable := disposition_count_retryable + 1;
                IF current_resource_type <> ALL(ARRAY[
                       'HealthcareService', 'InsurancePlan',
                       'OrganizationAffiliation', 'PractitionerRole'
                   ]::text[])
                   OR NOT (proof ?& ARRAY[{active_proof_fields}]::text[])
                   OR proof - ARRAY[{active_proof_fields}]::text[]
                        <> '{{}}'::jsonb
                   OR disposition ->> 'checkpoint_state' <> 'active'
                   OR disposition -> 'page_delta' <> '0'::jsonb
                   OR diagnostic -> 'complete' IS DISTINCT FROM 'false'::jsonb
                   OR diagnostic ->> 'error' <> '{_RETRYABLE_HTTP_500_ERROR}'
                   OR diagnostic -> 'traversal_complete'
                        IS DISTINCT FROM 'false'::jsonb
                   OR diagnostic -> 'source_continuation_exhausted'
                        IS DISTINCT FROM 'false'::jsonb
                   OR diagnostic -> 'next_url_remaining'
                        IS DISTINCT FROM 'true'::jsonb
                   OR NULLIF(checkpoint_row.next_url, '') IS NULL
                   OR proof -> 'verified' IS DISTINCT FROM 'false'::jsonb
                   OR (proof ->> 'pre_count')::numeric <= 0
                   OR checkpoint_row.rows_processed >
                        (proof ->> 'pre_count')::numeric
                   OR proof ?| ARRAY[
                        'post_count', 'processed_rows',
                        'unique_candidate_rows', 'failure',
                        'terminal_page_geometry', 'advertised_pre',
                        'advertised_post', 'returned_unique', 'deficit',
                        'terminal_reason'
                      ]::text[]
                   OR disposition -> 'advertised_pre' IS DISTINCT FROM
                        proof -> 'pre_count'
                   OR disposition -> 'advertised_post' <> 'null'::jsonb
                   OR disposition -> 'returned_unique' <> 'null'::jsonb
                   OR disposition -> 'deficit' <> 'null'::jsonb
                   OR diagnostic -> 'pages_fetched' IS DISTINCT FROM
                        pg_catalog.to_jsonb(checkpoint_row.pages_processed)
                   OR disposition -> 'diagnostic_pages' IS DISTINCT FROM
                        diagnostic -> 'pages_fetched' THEN
                    RETURN FALSE;
                END IF;
            ELSE
                RETURN FALSE;
            END IF;

            IF disposition ->> 'disposition' <> '{_RETRYABLE_HTTP_500}' THEN
                IF NOT ({_number_sql("proof -> 'post_count'")})
                   OR NOT ({_number_sql("proof -> 'processed_rows'")})
                   OR NOT ({_number_sql("proof -> 'unique_candidate_rows'")})
                   OR NOT ({_number_sql("proof -> 'advertised_pre'")})
                   OR NOT ({_number_sql("proof -> 'advertised_post'")})
                   OR NOT ({_number_sql("proof -> 'returned_unique'")})
                   OR NOT ({_number_sql("proof -> 'deficit'")})
                   OR NOT ({_number_sql("proof -> 'unreturned_count'")})
                   OR (proof ->> 'processed_rows')::numeric
                        <> (proof ->> 'unique_candidate_rows')::numeric
                   OR (proof ->> 'unique_candidate_rows')::numeric
                        > (proof ->> 'post_count')::numeric
                   OR proof -> 'advertised_pre'
                        IS DISTINCT FROM proof -> 'pre_count'
                   OR proof -> 'advertised_post'
                        IS DISTINCT FROM proof -> 'post_count'
                   OR proof -> 'returned_unique'
                        IS DISTINCT FROM proof -> 'unique_candidate_rows'
                   OR (proof ->> 'deficit')::numeric
                        <> (proof ->> 'pre_count')::numeric
                           - (proof ->> 'unique_candidate_rows')::numeric
                   OR proof -> 'unreturned_count'
                        IS DISTINCT FROM proof -> 'deficit'
                   OR proof ->> 'terminal_reason' <> 'source_no_next'
                   OR disposition -> 'advertised_pre'
                        IS DISTINCT FROM proof -> 'pre_count'
                   OR disposition -> 'advertised_post'
                        IS DISTINCT FROM proof -> 'post_count'
                   OR disposition -> 'returned_unique'
                        IS DISTINCT FROM proof -> 'unique_candidate_rows'
                   OR disposition -> 'deficit'
                        IS DISTINCT FROM proof -> 'deficit'
                   OR disposition -> 'returned_unique'
                        IS DISTINCT FROM
                           pg_catalog.to_jsonb(checkpoint_row.rows_processed)
                   OR pg_catalog.jsonb_typeof(terminal_geometry)
                        IS DISTINCT FROM 'object'
                   OR NOT ({_number_sql("terminal_geometry -> 'pages_processed'")})
                   OR NOT ({_number_sql("terminal_geometry -> 'terminal_page_entries'")})
                   OR terminal_geometry -> 'pages_processed' IS DISTINCT FROM
                        pg_catalog.to_jsonb(
                            checkpoint_row.pages_processed
                            + CASE
                                  WHEN disposition ->> 'disposition' =
                                       '{_COUNT_DRIFT}'
                                  THEN 1 ELSE 0
                              END
                        ) THEN
                    RETURN FALSE;
                END IF;
                terminal_pages :=
                    (terminal_geometry ->> 'pages_processed')::numeric;
                terminal_entries :=
                    (terminal_geometry ->> 'terminal_page_entries')::numeric;
                SELECT COALESCE(pg_catalog.sum(
                           (entry.value #>> '{{}}')::numeric
                       ), 0)
                  INTO entry_count_sum
                  FROM pg_catalog.jsonb_array_elements(
                       proof -> 'page_entry_counts'
                  ) AS entry(value);
                IF pg_catalog.jsonb_array_length(
                       proof -> 'page_entry_counts'
                   ) IS DISTINCT FROM terminal_pages::integer
                   OR entry_count_sum IS DISTINCT FROM
                        (proof ->> 'processed_rows')::numeric
                   OR proof -> 'page_entry_counts'
                          -> (terminal_pages::integer - 1)
                        IS DISTINCT FROM pg_catalog.to_jsonb(terminal_entries)
                   OR terminal_geometry IS DISTINCT FROM
                        pg_catalog.jsonb_build_object(
                            'version', 2,
                            'page_count', proof -> 'page_count',
                            'pages_processed', terminal_pages,
                            'processed_rows', proof -> 'processed_rows',
                            'terminal_page_start_offset',
                                (terminal_pages - 1) * page_count,
                            'logical_window_end_offset',
                                terminal_pages * page_count,
                            'terminal_page_entries', terminal_entries,
                            'sparse_pages',
                                (page_geometry ->> 'sparse_pages')::numeric
                                + CASE
                                      WHEN terminal_entries < page_count
                                      THEN 1 ELSE 0
                                  END,
                            'empty_pages',
                                (page_geometry ->> 'empty_pages')::numeric
                                + CASE
                                      WHEN terminal_entries = 0
                                      THEN 1 ELSE 0
                                  END
                        )
                   OR {_sha_vector_invalid_sql("proof -> 'continuation_hop_sha256'", 'terminal_pages - 1')}
                   OR {_sha_vector_invalid_sql("proof -> 'continuation_shape_sha256'", 'terminal_pages - 1')} THEN
                    RETURN FALSE;
                END IF;
            END IF;

            SELECT pg_catalog.count(*) INTO resource_type_count
              FROM {resource_ref} AS resource
             WHERE resource.dataset_id = candidate_row.dataset_id
               AND resource.resource_type = current_resource_type
               AND resource.resource_type NOT LIKE 'LU:%:pass:%';
            IF resource_type_count IS DISTINCT FROM
                   checkpoint_row.rows_processed THEN
                RETURN FALSE;
            END IF;
            SELECT COALESCE(pg_catalog.sum(
                       CASE
                           WHEN proof_count.resource_count_text
                                  ~ '^(0|[1-9][0-9]*)$'
                           THEN proof_count.resource_count_text::numeric
                           ELSE NULL
                       END
                   ), 0)
              INTO proof_resource_count
              FROM {proof_ref} AS shard
              CROSS JOIN LATERAL pg_catalog.jsonb_each_text(
                   shard.resource_counts_json
              ) AS proof_count(resource_type, resource_count_text)
             WHERE shard.dataset_id = candidate_row.dataset_id
               AND proof_count.resource_type = current_resource_type;
            IF proof_resource_count IS DISTINCT FROM
                   checkpoint_row.rows_processed::numeric THEN
                RETURN FALSE;
            END IF;

            checkpoint_pages := checkpoint_pages
                + checkpoint_row.pages_processed;
            diagnostic_pages := diagnostic_pages
                + (diagnostic ->> 'pages_fetched')::numeric;
            checkpoint_rows := checkpoint_rows
                + checkpoint_row.rows_processed;
            terminal_page_delta := terminal_page_delta
                + (disposition ->> 'page_delta')::numeric;
        END LOOP;

        SELECT pg_catalog.count(*) INTO relation_resource_count
          FROM {resource_ref} AS resource
         WHERE resource.dataset_id = candidate_row.dataset_id
           AND resource.resource_type NOT LIKE 'LU:%:pass:%';
        SELECT pg_catalog.count(*),
               COALESCE(pg_catalog.sum(shard.resource_count), 0),
               pg_catalog.count(*) FILTER (
                   WHERE shard.acquisition_root_run_id IS DISTINCT FROM
                             candidate_row.acquisition_root_run_id
                      OR shard.endpoint_id IS DISTINCT FROM
                             candidate_row.endpoint_id
                      OR shard.source_ids_json IS DISTINCT FROM
                             candidate_metadata -> 'source_ids'
                      OR pg_catalog.jsonb_typeof(shard.resource_counts_json)
                             IS DISTINCT FROM 'object'
                      OR EXISTS (
                          SELECT 1
                            FROM pg_catalog.jsonb_each(
                                 CASE
                                     WHEN pg_catalog.jsonb_typeof(
                                              shard.resource_counts_json
                                          ) = 'object'
                                     THEN shard.resource_counts_json
                                     ELSE '{{}}'::jsonb
                                 END
                            ) AS proof_count(resource_type, count_json)
                           WHERE NOT (expected_resources ? proof_count.resource_type)
                              OR pg_catalog.jsonb_typeof(count_json) <> 'number'
                              OR count_json #>> '{{}}'
                                   !~ '^(0|[1-9][0-9]*)$'
                      )
                      OR (
                          SELECT COALESCE(pg_catalog.sum(
                              CASE
                                  WHEN pg_catalog.jsonb_typeof(count_json) = 'number'
                                   AND count_json #>> '{{}}'
                                          ~ '^(0|[1-9][0-9]*)$'
                                  THEN (count_json #>> '{{}}')::numeric
                                  ELSE NULL
                              END
                          ), 0)
                            FROM pg_catalog.jsonb_each(
                                 CASE
                                     WHEN pg_catalog.jsonb_typeof(
                                              shard.resource_counts_json
                                          ) = 'object'
                                     THEN shard.resource_counts_json
                                     ELSE '{{}}'::jsonb
                                 END
                            ) AS proof_count(resource_type, count_json)
                      ) IS DISTINCT FROM shard.resource_count::numeric
               )
          INTO proof_shard_count, proof_row_count, invalid_proof_count
          FROM {proof_ref} AS shard
         WHERE shard.dataset_id = candidate_row.dataset_id;

        IF disposition_count_complete <> 2
           OR disposition_count_drift <> 1
           OR disposition_count_retryable <> 4
           OR terminal_page_delta <> 1
           OR checkpoint_pages IS DISTINCT FROM
                (marker ->> 'checkpoint_pages_processed')::numeric
           OR diagnostic_pages IS DISTINCT FROM
                (marker ->> 'diagnostic_pages_processed')::numeric
           OR terminal_page_delta IS DISTINCT FROM
                (marker ->> 'terminal_page_delta')::numeric
           OR checkpoint_rows IS DISTINCT FROM
                (marker ->> 'checkpoint_rows_processed')::numeric
           OR relation_resource_count IS DISTINCT FROM candidate_row.resource_count
           OR relation_resource_count IS DISTINCT FROM
                (marker ->> 'resource_count')::numeric
           OR checkpoint_rows IS DISTINCT FROM relation_resource_count::numeric
           OR proof_shard_count IS DISTINCT FROM
                (marker ->> 'proof_shard_count')::numeric
           OR proof_shard_count <= 0
           OR proof_row_count IS DISTINCT FROM
                (marker ->> 'proof_row_count')::numeric
           OR proof_row_count IS DISTINCT FROM relation_resource_count::numeric
           OR invalid_proof_count <> 0
           OR EXISTS (
                SELECT 1
                  FROM {bulk_ref} AS bulk_checkpoint
                 WHERE bulk_checkpoint.dataset_id = candidate_row.dataset_id
                    OR bulk_checkpoint.acquisition_root_run_id =
                         candidate_row.acquisition_root_run_id
           ) THEN
            RETURN FALSE;
        END IF;
        RETURN TRUE;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
    $function$;
    """


def _replace_once(source: str, needle: str, replacement: str) -> str:
    if source.count(needle) != 1:
        raise RuntimeError(
            "provider directory abandonment guard renderer changed"
        )
    return source.replace(needle, replacement, 1)


def _dataset_guard_sql(schema: str) -> str:
    """Widen only the existing v1 guard's dispatch, preserving its OID."""

    subset = _subset()
    abandonment = _abandonment()
    source_ref = subset._qf(schema, subset._SOURCE)
    checkpoint_ref = subset._qf(schema, abandonment._CHECKPOINT)
    new_valid_ref = subset._qf(schema, _VALID)
    evidence_sha256_ref = subset._qf(
        schema,
        subset._PAYLOAD_SHA256_FUNCTION,
    )
    canonical_sha256_ref = subset._qf(
        schema,
        subset._CANONICAL_SHA256_FUNCTION,
    )
    source_metadata = "source.metadata_json::jsonb"
    source_fixed_identity = subset._subset_source_fixed_identity_sql(
        source_metadata,
        "terminal_profile",
        reviewed_subset_profile_aware=True,
    )
    source_scope_payload = subset._subset_source_scope_payload_sql(
        "source",
        source_metadata,
        "terminal_profile.publication_metadata_json",
        "terminal_profile",
        use_configured_endpoint_identity=True,
        include_reviewed_root_policy=True,
    )
    source_profile_resources = _source_profile_resource_array_sql()
    terminal_contract_identity = _source_contract_identity_sql(
        "source",
        "terminal_profile.completion_proof_json",
    )
    expected_start_url_sha256 = _expected_start_url_sha256_sql(
        "source",
        "terminal_profile.completion_proof_json",
        "checkpoint.resource_type",
    )
    source_start_urls_are_exact = " AND ".join(
        "source.metadata_json::jsonb #>> "
        + subset._ql(
            "{provider_directory_current_version_census_start_urls,"
            + resource_type
            + "}"
        )
        + " = source.canonical_api_base || '/' || "
        + subset._ql(resource_type)
        for resource_type in _SOURCE_PROFILE_RESOURCE_TYPES
    )
    completion_fields = _json_fields_sql(_COMPLETION_PROOF_FIELDS)
    source_import_fields = _json_fields_sql(_SOURCE_IMPORT_FIELDS)
    original = abandonment._dataset_guard_sql(
        schema,
        reviewed_root_policy_aware=True,
    )
    after_needle = f"""        IF TG_WHEN = 'AFTER' THEN
            IF NEW.status = '{_STATUS}'
               AND {subset._qf(schema, abandonment._VALID)}(NEW.dataset_id) IS DISTINCT FROM TRUE THEN"""
    after_replacement = f"""        IF TG_WHEN = 'AFTER' THEN
            IF NEW.status = '{_STATUS}'
               AND NEW.publication_metadata_json::jsonb ? '{_MARKER}' THEN
                IF NEW.publication_metadata_json::jsonb ? '{_LEGACY_MARKER}'
                   OR {new_valid_ref}(NEW.dataset_id) IS DISTINCT FROM TRUE THEN
                    RAISE EXCEPTION
                        'provider_directory_subset_terminal_disposition_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NULL;
            END IF;
            IF NEW.status = '{_STATUS}'
               AND {subset._qf(schema, abandonment._VALID)}(NEW.dataset_id) IS DISTINCT FROM TRUE THEN"""
    transition_needle = f"""        IF NEW.status = '{_STATUS}'
           OR NEW.publication_metadata_json::jsonb ? '{_LEGACY_MARKER}' THEN"""
    transition_replacement = f"""        IF NEW.publication_metadata_json::jsonb ? '{_MARKER}' THEN
            IF OLD.status <> '{_PRIOR_STATUS}'
               OR NEW.status <> '{_STATUS}'
               OR OLD.publication_metadata_json::jsonb ?| ARRAY[
                    '{_LEGACY_MARKER}', '{_MARKER}'
                  ]::text[]
               OR NEW.publication_metadata_json::jsonb ? '{_LEGACY_MARKER}'
               OR pg_catalog.jsonb_typeof(
                    NEW.publication_metadata_json::jsonb -> '{_MARKER}'
                  ) IS DISTINCT FROM 'object'
               OR NEW.is_current IS NOT FALSE
               OR NEW.completion_proof_required_version IS DISTINCT FROM 3
               OR NEW.completion_proof_json IS NOT NULL
               OR NEW.completion_proof_sha256 IS NOT NULL
               OR NEW.validated_at IS NOT NULL
               OR NEW.published_at IS NOT NULL
               OR NEW.superseded_at IS NOT NULL
               OR NEW.resource_count < 0
               OR NEW.publication_metadata_json::jsonb
                      ->> 'acquisition_root_run_id'
                    IS DISTINCT FROM NEW.acquisition_root_run_id
               OR NEW.publication_metadata_json::jsonb
                      ->> 'resource_hash_contract'
                    <> 'transport_neutral_v2'
               OR NEW.publication_metadata_json::jsonb
                      -> 'reused_from_checkpoint'
                    IS DISTINCT FROM 'true'::jsonb
               OR pg_catalog.jsonb_typeof(
                    NEW.publication_metadata_json::jsonb
                        -> 'completion_proof_v1'
                  ) IS DISTINCT FROM 'object'
               OR NOT (
                    (NEW.publication_metadata_json::jsonb
                        -> 'completion_proof_v1')
                    ?& ARRAY[{completion_fields}]::text[]
                  )
               OR (NEW.publication_metadata_json::jsonb
                      -> 'completion_proof_v1')
                      - ARRAY[{completion_fields}]::text[]
                    <> '{{}}'::jsonb
               OR (
                    SELECT pg_catalog.count(*)
                      FROM {source_ref} AS source_alias
                     WHERE source_alias.source_id =
                               NEW.publication_metadata_json::jsonb
                                   #>> '{{source_ids,0}}'
                        OR source_alias.endpoint_id = NEW.endpoint_id
                        OR source_alias.metadata_json::jsonb
                               ->> 'provider_directory_configured_endpoint_id'
                               = NEW.endpoint_id
                  ) <> 1
               OR NOT EXISTS (
                    SELECT 1
                      FROM {source_ref} AS source
                      CROSS JOIN LATERAL (
                           SELECT NEW.publication_metadata_json::jsonb
                                      AS publication_metadata_json,
                                  NEW.publication_metadata_json::jsonb #> ARRAY[
                                      'resource_diagnostics',
                                      'HealthcareService',
                                      'server_issued_subset_completeness'
                                  ]::text[] AS completion_proof_json
                      ) AS terminal_profile
                     WHERE source.source_id =
                               NEW.publication_metadata_json::jsonb
                                   #>> '{{source_ids,0}}'
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_configured_endpoint_id'
                              = NEW.endpoint_id
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_candidate_status'
                              = 'pending_reviewed_subset_acquisition'
                       AND NOT (source.metadata_json::jsonb ?| ARRAY[
                            'provider_directory_reviewed_subset_activation_v1',
                            'provider_directory_reviewed_subset_activation_v2'
                       ]::text[])
                       AND ({subset._reviewed_root_policy_sql(
                            'source.metadata_json::jsonb',
                            1,
                       )})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_reviewed_root_policy_v1'
                           = NEW.publication_metadata_json::jsonb
                              -> 'provider_directory_reviewed_root_policy_v1'
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_verification_campaign_id'
                           = NEW.publication_metadata_json::jsonb
                              ->> 'verification_campaign_id'
                       AND ({subset._reviewed_root_policy_sql(
                            'NEW.publication_metadata_json::jsonb',
                            1,
                       )})
                       AND source.requires_registration IS FALSE
                       AND source.requires_api_key IS FALSE
                       AND source.auth_type = 'none'
                       AND NULLIF(source.endpoint_id, '') IS NOT NULL
                       AND ({source_fixed_identity})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_supported_resources'
                           = pg_catalog.to_jsonb({source_profile_resources})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_expected_nonempty_resources'
                           = pg_catalog.to_jsonb({source_profile_resources})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_server_issued_subset_resources'
                           = pg_catalog.to_jsonb({source_profile_resources})
                       AND ({source_start_urls_are_exact})
                       AND {canonical_sha256_ref}(
                              {terminal_contract_identity}
                           ) = terminal_profile.completion_proof_json
                              ->> 'contract_identity'
                       AND {canonical_sha256_ref}({source_scope_payload}) =
                           NEW.publication_metadata_json::jsonb
                              #>> '{{{_MARKER},source_scope_sha256}}'
                       AND source.metadata_json::jsonb
                              #>> '{{last_resource_import,run_id}}'
                              = NEW.import_run_id
                       AND pg_catalog.jsonb_typeof(
                              source.metadata_json::jsonb
                                  -> 'last_resource_import'
                           ) = 'object'
                       AND (source.metadata_json::jsonb
                              -> 'last_resource_import')
                              ?& ARRAY[{source_import_fields}]::text[]
                       AND (source.metadata_json::jsonb
                              -> 'last_resource_import')
                              - ARRAY[{source_import_fields}]::text[]
                           = '{{}}'::jsonb
                       AND pg_catalog.jsonb_typeof(
                              source.metadata_json::jsonb
                                  #> '{{last_resource_import,run_id}}'
                           ) = 'string'
                       AND pg_catalog.jsonb_typeof(
                              source.metadata_json::jsonb
                                  #> '{{last_resource_import,observed_at}}'
                           ) = 'string'
                       AND source.metadata_json::jsonb
                              #>> '{{last_resource_import,observed_at}}'
                           ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T'
                             '[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z$'
                       AND CASE
                               WHEN pg_catalog.pg_input_is_valid(
                                    source.metadata_json::jsonb
                                        #>> '{{last_resource_import,observed_at}}',
                                    'pg_catalog.timestamptz'
                               )
                               THEN pg_catalog.to_char(
                                    (
                                        source.metadata_json::jsonb
                                            #>> '{{last_resource_import,observed_at}}'
                                    )::pg_catalog.timestamptz
                                        AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                               ) = source.metadata_json::jsonb
                                      #>> '{{last_resource_import,observed_at}}'
                               ELSE FALSE
                           END
                       AND pg_catalog.jsonb_typeof(
                              source.metadata_json::jsonb
                                  #> '{{last_resource_import,resources}}'
                           ) = 'object'
                       AND source.metadata_json::jsonb
                              #> '{{last_resource_import,resources}}'
                           = NEW.publication_metadata_json::jsonb
                              -> 'resource_diagnostics'
                       AND source.metadata_json::jsonb
                              #> '{{last_resource_import,resources}}'
                           = NEW.publication_metadata_json::jsonb
                              #> '{{completion_proof_v1,resource_diagnostics}}'
                       AND NULLIF(source.canonical_api_base, '') IS NOT NULL
                       AND NOT EXISTS (
                            SELECT 1
                              FROM {checkpoint_ref} AS checkpoint
                             WHERE checkpoint.dataset_id = NEW.dataset_id
                               AND checkpoint.canonical_api_base
                                    IS DISTINCT FROM
                                      source.canonical_api_base
                       )
                       AND NOT EXISTS (
                            SELECT 1
                              FROM {checkpoint_ref} AS checkpoint
                             WHERE checkpoint.dataset_id = NEW.dataset_id
                               AND checkpoint.start_url_hash IS DISTINCT FROM
                                   ({expected_start_url_sha256})
                       )
                       AND NEW.publication_metadata_json::jsonb
                              #>> '{{{_MARKER},source_import_sha256}}'
                           = {evidence_sha256_ref}(
                               source.metadata_json::jsonb
                                   -> 'last_resource_import'
                             )
                       AND NEW.publication_metadata_json::jsonb
                              #>> '{{{_MARKER},source_diagnostics_sha256}}'
                           = {evidence_sha256_ref}(
                               source.metadata_json::jsonb
                                   #> '{{last_resource_import,resources}}'
                             )
               )
               OR COALESCE(
                    NEW.publication_metadata_json::jsonb,
                    '{{}}'::jsonb
                  ) - '{_MARKER}'
                  IS DISTINCT FROM COALESCE(
                    OLD.publication_metadata_json::jsonb,
                    '{{}}'::jsonb
                  )
               OR to_jsonb(NEW) - ARRAY[
                    'status', 'resource_count', 'publication_metadata_json'
                  ]::text[]
                  IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
                    'status', 'resource_count', 'publication_metadata_json'
                  ]::text[] THEN
                RAISE EXCEPTION
                    'provider_directory_subset_terminal_disposition_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF NEW.status = '{_STATUS}'
           OR NEW.publication_metadata_json::jsonb ? '{_LEGACY_MARKER}' THEN"""
    return _replace_once(
        _replace_once(original, after_needle, after_replacement),
        transition_needle,
        transition_replacement,
    )


def _checkpoint_guard_sql(schema: str) -> str:
    """Dispatch deferred checkpoint validation without replacing triggers."""

    subset = _subset()
    abandonment = _abandonment()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    valid_ref = subset._qf(schema, _VALID)
    original = abandonment._checkpoint_guard_sql(schema)
    declaration_needle = """        parent_status text;
        target_dataset_id text;"""
    declaration_replacement = """        parent_status text;
        parent_has_terminal_disposition boolean;
        target_dataset_id text;"""
    select_needle = f"""            SELECT dataset.status INTO parent_status
              FROM {dataset_ref} AS dataset
             WHERE dataset.dataset_id = target_dataset_id;
            IF parent_status = '{_STATUS}'"""
    select_replacement = f"""            SELECT dataset.status,
                   dataset.publication_metadata_json::jsonb ? '{_MARKER}'
              INTO parent_status, parent_has_terminal_disposition
              FROM {dataset_ref} AS dataset
             WHERE dataset.dataset_id = target_dataset_id;
            IF parent_has_terminal_disposition IS TRUE THEN
                IF TG_OP = 'DELETE'
                   OR NEW.state <> '{_STATUS}'
                   OR {valid_ref}(target_dataset_id) IS DISTINCT FROM TRUE THEN
                    RAISE EXCEPTION
                        'provider_directory_subset_terminal_disposition_checkpoint_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NULL;
            END IF;
            IF parent_status = '{_STATUS}'"""
    return _replace_once(
        _replace_once(
            original,
            declaration_needle,
            declaration_replacement,
        ),
        select_needle,
        select_replacement,
    )


def _new_object_shape_fence_sql(
    schema: str,
    *,
    expect_installed: bool,
) -> str:
    subset = _subset()
    abandonment = _abandonment()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    checkpoint_ref = subset._qf(schema, abandonment._CHECKPOINT)
    valid_ref = subset._qf(schema, _VALID)
    dataset_guard_ref = subset._qf(schema, abandonment._DATASET_GUARD)
    checkpoint_guard_ref = subset._qf(schema, abandonment._CHECKPOINT_GUARD)
    expected_function_count = 1 if expect_installed else 0
    expected_trigger_count = 2 if expect_installed else 0
    return f"""
    DO $migration$
    DECLARE
        function_count bigint;
        trigger_count bigint;
    BEGIN
        SELECT pg_catalog.count(*) INTO function_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS language_row
            ON language_row.oid = function_row.prolang
         WHERE function_row.oid = pg_catalog.to_regprocedure(
                   {subset._ql(valid_ref + '(text)')}
               )
           AND namespace_row.nspname = {subset._ql(schema)}
           AND function_row.pronargs = 1
           AND function_row.prorettype = 'pg_catalog.bool'::regtype
           AND language_row.lanname = 'plpgsql'
           AND function_row.provolatile = 's'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                ARRAY['search_path=pg_catalog']::text[]
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.aclexplode(COALESCE(
                       function_row.proacl,
                       pg_catalog.acldefault('f', function_row.proowner)
                  )) AS function_acl
                 WHERE function_acl.grantee = 0
                   AND function_acl.privilege_type = 'EXECUTE'
           );
        SELECT pg_catalog.count(*) INTO trigger_count
          FROM (VALUES
               (
                   {subset._ql(dataset_ref)}::regclass,
                   {subset._ql(_DATASET_CONSTRAINT)},
                   17,
                   pg_catalog.to_regprocedure(
                       {subset._ql(dataset_guard_ref + '()')}
                   )
               ),
               (
                   {subset._ql(checkpoint_ref)}::regclass,
                   {subset._ql(_CHECKPOINT_CONSTRAINT)},
                   29,
                   pg_catalog.to_regprocedure(
                       {subset._ql(checkpoint_guard_ref + '()')}
                   )
               )
          ) AS expected(
               relation_oid, trigger_name, trigger_type, function_oid
          )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = expected.relation_oid
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgfoid = expected.function_oid
           AND trigger_row.tgtype = expected.trigger_type
           AND trigger_row.tgconstraint <> 0
           AND trigger_row.tgdeferrable IS TRUE
           AND trigger_row.tginitdeferred IS TRUE
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND pg_catalog.octet_length(trigger_row.tgargs) = 0;
        IF function_count <> {expected_function_count}
           OR trigger_count <> {expected_trigger_count} THEN
            RAISE EXCEPTION
                'provider_directory_subset_terminal_disposition_shape_changed'
                USING ERRCODE = '55000';
        END IF;
        IF NOT {str(expect_installed).lower()} AND EXISTS (
            SELECT 1
              FROM {dataset_ref} AS dataset
             WHERE dataset.publication_metadata_json::jsonb ? '{_MARKER}'
        ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_terminal_disposition_adoption_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _create_constraints(schema: str) -> None:
    subset = _subset()
    abandonment = _abandonment()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    checkpoint_ref = subset._qf(schema, abandonment._CHECKPOINT)
    dataset_guard_ref = subset._qf(schema, abandonment._DATASET_GUARD)
    checkpoint_guard_ref = subset._qf(schema, abandonment._CHECKPOINT_GUARD)
    op.execute(
        f"CREATE CONSTRAINT TRIGGER {subset._q(_DATASET_CONSTRAINT)} "
        f"AFTER UPDATE ON {dataset_ref} DEFERRABLE INITIALLY DEFERRED "
        f"FOR EACH ROW EXECUTE FUNCTION {dataset_guard_ref}();"
    )
    op.execute(
        f"CREATE CONSTRAINT TRIGGER {subset._q(_CHECKPOINT_CONSTRAINT)} "
        f"AFTER INSERT OR UPDATE OR DELETE ON {checkpoint_ref} "
        f"DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        f"EXECUTE FUNCTION {checkpoint_guard_ref}();"
    )
    op.execute(
        f"ALTER TABLE {dataset_ref} ENABLE ALWAYS TRIGGER "
        f"{subset._q(_DATASET_CONSTRAINT)};"
    )
    op.execute(
        f"ALTER TABLE {checkpoint_ref} ENABLE ALWAYS TRIGGER "
        f"{subset._q(_CHECKPOINT_CONSTRAINT)};"
    )


def _downgrade_evidence_fence_sql(schema: str) -> str:
    subset = _subset()
    abandonment = _abandonment()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    checkpoint_ref = subset._qf(schema, abandonment._CHECKPOINT)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM {dataset_ref} AS dataset
             WHERE dataset.publication_metadata_json::jsonb ? '{_MARKER}'
        ) OR EXISTS (
            SELECT 1
              FROM {checkpoint_ref} AS checkpoint
              JOIN {dataset_ref} AS dataset
                ON dataset.dataset_id = checkpoint.dataset_id
             WHERE dataset.publication_metadata_json::jsonb ? '{_MARKER}'
                OR (
                    checkpoint.state = '{_STATUS}'
                    AND dataset.status = '{_STATUS}'
                    AND NOT (
                        dataset.publication_metadata_json::jsonb
                            ? '{_LEGACY_MARKER}'
                    )
                )
        ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_terminal_disposition_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _drop_new_objects(schema: str) -> None:
    subset = _subset()
    abandonment = _abandonment()
    op.execute(
        f"DROP TRIGGER {subset._q(_CHECKPOINT_CONSTRAINT)} ON "
        f"{subset._qf(schema, abandonment._CHECKPOINT)};"
    )
    op.execute(
        f"DROP TRIGGER {subset._q(_DATASET_CONSTRAINT)} ON "
        f"{subset._qf(schema, subset._ENDPOINT_DATASET)};"
    )
    op.execute(f"DROP FUNCTION {subset._qf(schema, _VALID)}(text);")


def _revoke_valid_execute(schema: str) -> None:
    subset = _subset()
    op.execute(
        f"REVOKE ALL ON FUNCTION {subset._qf(schema, _VALID)}(text) "
        "FROM PUBLIC;"
    )


def _restore_abandonment_guards(schema: str) -> None:
    abandonment = _abandonment()
    op.execute(
        abandonment._dataset_guard_sql(
            schema,
            reviewed_root_policy_aware=True,
        )
    )
    op.execute(abandonment._checkpoint_guard_sql(schema))


def upgrade() -> None:
    subset = _subset()
    schema = subset._schema()
    _lock_relations(schema)
    _base_shape_fences(schema)
    op.execute(_new_object_shape_fence_sql(schema, expect_installed=False))
    op.execute(_valid_function_sql(schema))
    _revoke_valid_execute(schema)
    op.execute(_dataset_guard_sql(schema))
    op.execute(_checkpoint_guard_sql(schema))
    _create_constraints(schema)
    op.execute(_new_object_shape_fence_sql(schema, expect_installed=True))
    _base_shape_fences(schema)


def downgrade() -> None:
    subset = _subset()
    schema = subset._schema()
    _lock_relations(schema)
    _base_shape_fences(schema)
    op.execute(_new_object_shape_fence_sql(schema, expect_installed=True))
    op.execute(_downgrade_evidence_fence_sql(schema))
    _drop_new_objects(schema)
    _restore_abandonment_guards(schema)
    op.execute(_new_object_shape_fence_sql(schema, expect_installed=False))
    _base_shape_fences(schema)
