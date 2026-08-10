# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit one exact reviewed direct-v4 terminal disposition.

Revision ID: 20260810110000_provider_directory_reviewed_subset_direct_v4_disposition
Revises: 20260810100000_provider_directory_terminal_root_retirement_resource_count_repair
"""

from __future__ import annotations

from functools import lru_cache
import hashlib
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = (
    "20260810110000_provider_directory_reviewed_subset_direct_v4_disposition"
)
down_revision = (
    "20260810100000_provider_directory_terminal_root_retirement_resource_count_repair"
)
branch_labels = None
depends_on = None


_SCOPE_BINDING_FILE = (
    "20260810020000_provider_directory_terminal_scope_binding.py"
)
_MARKER = "provider_directory_reviewed_subset_terminal_disposition_v1"
_LEGACY_MARKER = "provider_directory_reviewed_subset_abandonment_v1"
_CONTRACT = (
    "healthporta.provider-directory.reviewed-subset-terminal-disposition.v2"
)
_REASON = "reviewed_current_version_census_drift"
_STATUS = "acquisition_abandoned"
_PRIOR_STATUS = "failed"
_VALID = "provider_directory_subset_terminal_disposition_valid"
_DIRECT_VALID = "provider_directory_subset_terminal_disposition_v4_valid"
_DATASET_CONSTRAINT = "pd_subset_terminal_disposition_dataset_consistency_guard"
_CHECKPOINT_CONSTRAINT = (
    "provider_directory_subset_terminal_disposition_checkpoint_guard"
)
_IDENTITY_SNAPSHOT = "provider_directory_terminal_v4_identity_snapshot"
_CAMPAIGN = "provider-directory-reviewed-subset-2026-08-10-v4"
_MARKER_SHA256 = (
    "e6f19eb70f8b5a84c76e61c19c379541bb6865b7de3114de01dd2a32181cb299"
)
_RESOURCE_TYPES = (
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
)
_DRIFT_RESOURCE_TYPES = (
    "HealthcareService",
    "OrganizationAffiliation",
    "PractitionerRole",
)
_SOURCE_RESOURCE_TYPES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
)


@lru_cache(maxsize=1)
def _scope_binding() -> ModuleType:
    path = Path(__file__).with_name(_SCOPE_BINDING_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_terminal_v4_scope_binding",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory scope-binding revision unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _terminal() -> ModuleType:
    return _scope_binding()._predecessor()


def _subset() -> ModuleType:
    return _terminal()._subset()


def _abandonment() -> ModuleType:
    return _terminal()._abandonment()


def _qf(schema: str, name: str) -> str:
    return _subset()._qf(schema, name)


def _ql(value: str) -> str:
    return _subset()._ql(value)


def _replace_exact(
    source: str,
    needle: str,
    replacement: str,
    *,
    expected_count: int = 1,
) -> str:
    if source.count(needle) != expected_count:
        raise RuntimeError("provider directory terminal renderer changed")
    return source.replace(needle, replacement)


def _resource_array(values: tuple[str, ...] = _RESOURCE_TYPES) -> str:
    return "ARRAY[" + ", ".join(_ql(value) for value in values) + "]::text[]"


def _resource_json(values: tuple[str, ...] = _RESOURCE_TYPES) -> str:
    return "[" + ",".join('"' + value + '"' for value in values) + "]"


def _direct_valid_sql(schema: str) -> str:
    """Validate the immutable v2 marker against retained durable rows."""

    subset = _subset()
    abandonment = _abandonment()
    dataset_ref = _qf(schema, subset._ENDPOINT_DATASET)
    resource_ref = _qf(schema, subset._DATASET_RESOURCE)
    proof_ref = _qf(schema, abandonment._PROOF_SHARD)
    checkpoint_ref = _qf(schema, abandonment._CHECKPOINT)
    bulk_ref = _qf(schema, abandonment._BULK_CHECKPOINT)
    payload_sha_ref = _qf(schema, subset._PAYLOAD_SHA256_FUNCTION)
    helper_ref = _qf(schema, _DIRECT_VALID)
    resources = _resource_array()
    drift_resources = _resource_array(_DRIFT_RESOURCE_TYPES)
    return f"""
    CREATE FUNCTION {helper_ref}(candidate_dataset_id text)
    RETURNS boolean
    LANGUAGE plpgsql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        candidate record;
        checkpoint_row record;
        candidate_metadata jsonb;
        completion_copy jsonb;
        diagnostics jsonb;
        disposition jsonb;
        marker jsonb;
        current_resource_type text;
        checkpoint_count bigint;
        relation_resource_count numeric;
        resource_type_count bigint;
        proof_shard_count bigint;
        proof_row_count numeric;
        invalid_proof_count bigint;
        proof_resource_count numeric;
        checkpoint_page_total numeric := 0;
        diagnostic_page_total numeric := 0;
        checkpoint_row_total numeric := 0;
    BEGIN
        IF NULLIF(candidate_dataset_id, '') IS NULL THEN
            RETURN FALSE;
        END IF;
        SELECT dataset.* INTO STRICT candidate
          FROM {dataset_ref} AS dataset
         WHERE dataset.dataset_id = candidate_dataset_id;
        candidate_metadata := candidate.publication_metadata_json::jsonb;
        marker := candidate_metadata -> '{_MARKER}';

        IF pg_catalog.jsonb_typeof(marker) IS DISTINCT FROM 'object'
           OR {payload_sha_ref}(marker) <> '{_MARKER_SHA256}' THEN
            RETURN FALSE;
        END IF;
        diagnostics := candidate_metadata -> 'resource_diagnostics';
        completion_copy := candidate_metadata -> 'completion_proof_v1';
        IF candidate.status <> '{_STATUS}'
           OR candidate.is_current IS NOT FALSE
           OR candidate.previous_dataset_id IS NOT NULL
           OR candidate.dataset_hash IS NOT NULL
           OR candidate.validated_at IS NOT NULL
           OR candidate.published_at IS NOT NULL
           OR candidate.superseded_at IS NOT NULL
           OR candidate.completion_proof_required_version IS DISTINCT FROM 3
           OR candidate.completion_proof_json IS NOT NULL
           OR candidate.completion_proof_sha256 IS NOT NULL
           OR NULLIF(candidate.import_run_id, '') IS NULL
           OR candidate.import_run_id IS DISTINCT FROM
                candidate.acquisition_root_run_id
           OR candidate_metadata ? '{_LEGACY_MARKER}'
           OR marker ->> 'contract_version' <> '{_CONTRACT}'
           OR marker ->> 'reason_code' <> '{_REASON}'
           OR marker -> 'resource_types' IS DISTINCT FROM
                '{_resource_json()}'::jsonb
           OR marker ->> 'candidate_metadata_sha256' IS DISTINCT FROM
                {payload_sha_ref}(candidate_metadata - '{_MARKER}')
           OR marker ->> 'source_diagnostics_sha256' IS DISTINCT FROM
                {payload_sha_ref}(diagnostics)
           OR candidate.resource_count IS DISTINCT FROM
                (marker ->> 'resource_count')::bigint
           OR candidate_metadata -> 'source_ids' IS NULL
           OR pg_catalog.jsonb_array_length(
                candidate_metadata -> 'source_ids'
              ) <> 1
           OR candidate_metadata -> 'selected_resources'
                IS DISTINCT FROM '{_resource_json()}'::jsonb
           OR candidate_metadata -> 'expected_resources'
                IS DISTINCT FROM '{_resource_json()}'::jsonb
           OR candidate_metadata -> 'reused_from_checkpoint'
                IS DISTINCT FROM 'false'::jsonb
           OR candidate_metadata ->> 'verification_campaign_id'
                <> '{_CAMPAIGN}'
           OR candidate_metadata ->> 'acquisition_root_run_id'
                IS DISTINCT FROM candidate.acquisition_root_run_id
           OR pg_catalog.jsonb_typeof(diagnostics) IS DISTINCT FROM 'object'
           OR diagnostics - {resources} <> '{{}}'::jsonb
           OR NOT (diagnostics ?& {resources})
           OR pg_catalog.jsonb_typeof(completion_copy) IS DISTINCT FROM 'object'
           OR completion_copy -> 'resource_diagnostics'
                IS DISTINCT FROM diagnostics
           OR completion_copy ->> 'verification_campaign_id'
                <> '{_CAMPAIGN}'
           OR completion_copy ->> 'acquisition_root_run_id'
                IS DISTINCT FROM candidate.acquisition_root_run_id
           OR completion_copy ->> 'terminal_run_id'
                IS DISTINCT FROM candidate.import_run_id
           OR completion_copy -> 'source_ids'
                IS DISTINCT FROM candidate_metadata -> 'source_ids'
           OR completion_copy -> 'selected_resources'
                IS DISTINCT FROM '{_resource_json()}'::jsonb
           OR marker -> 'direct_lineage' IS DISTINCT FROM
                pg_catalog.jsonb_build_object(
                    'checkpoint_retry_count', 0,
                    'competing_candidate_count', 0,
                    'current_dataset_count', 0,
                    'import_run_row_count', 0,
                    'owner_equals_root', true,
                    'previous_dataset_present', false,
                    'previous_reference_count', 0
                ) THEN
            RETURN FALSE;
        END IF;

        SELECT pg_catalog.count(*),
               pg_catalog.count(DISTINCT stored_checkpoint.resource_type)
          INTO checkpoint_count, resource_type_count
          FROM {checkpoint_ref} AS stored_checkpoint
         WHERE stored_checkpoint.dataset_id = candidate.dataset_id;
        IF checkpoint_count <> 7 OR resource_type_count <> 7 OR EXISTS (
            SELECT 1
              FROM {checkpoint_ref} AS stored_checkpoint
             WHERE stored_checkpoint.dataset_id = candidate.dataset_id
               AND stored_checkpoint.resource_type <> ALL({resources})
        ) THEN
            RETURN FALSE;
        END IF;

        SELECT pg_catalog.count(*),
               COALESCE(pg_catalog.sum(shard.resource_count), 0),
               pg_catalog.count(*) FILTER (
                   WHERE shard.endpoint_id IS DISTINCT FROM candidate.endpoint_id
                      OR shard.acquisition_root_run_id IS DISTINCT FROM
                           candidate.acquisition_root_run_id
                      OR shard.source_ids_json::jsonb IS DISTINCT FROM
                           candidate_metadata -> 'source_ids'
                      OR shard.resource_count <= 0
                      OR (
                           SELECT pg_catalog.count(*) <> 1
                             FROM pg_catalog.jsonb_object_keys(
                                  CASE
                                      WHEN pg_catalog.jsonb_typeof(
                                               shard.resource_counts_json
                                           ) = 'object'
                                      THEN shard.resource_counts_json
                                      ELSE '{{}}'::jsonb
                                  END
                             ) AS resource_key
                         )
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
                             ) AS resource_count(key, value)
                            WHERE resource_count.key <> ALL({resources})
                               OR pg_catalog.jsonb_typeof(
                                    resource_count.value
                                  ) <> 'number'
                               OR resource_count.value #>> '{{}}'
                                    !~ '^[1-9][0-9]*$'
                               OR CASE
                                      WHEN pg_catalog.jsonb_typeof(
                                               resource_count.value
                                           ) = 'number'
                                       AND resource_count.value #>> '{{}}'
                                               ~ '^[1-9][0-9]*$'
                                      THEN (resource_count.value #>> '{{}}')::numeric
                                           IS DISTINCT FROM
                                             shard.resource_count::numeric
                                      ELSE true
                                  END
                      )
               )
          INTO proof_shard_count, proof_row_count, invalid_proof_count
          FROM {proof_ref} AS shard
         WHERE shard.dataset_id = candidate.dataset_id;
        IF invalid_proof_count <> 0 THEN
            RETURN FALSE;
        END IF;

        FOREACH current_resource_type IN ARRAY {resources} LOOP
            SELECT stored_checkpoint.* INTO STRICT checkpoint_row
              FROM {checkpoint_ref} AS stored_checkpoint
             WHERE stored_checkpoint.dataset_id = candidate.dataset_id
               AND stored_checkpoint.resource_type = current_resource_type;
            disposition := marker -> 'resource_dispositions'
                                  -> current_resource_type;
            IF checkpoint_row.state <> '{_STATUS}'
               OR checkpoint_row.completed_at IS NULL
               OR checkpoint_row.acquisition_root_run_id IS DISTINCT FROM
                    candidate.acquisition_root_run_id
               OR checkpoint_row.owner_run_id IS DISTINCT FROM
                    candidate.import_run_id
               OR checkpoint_row.retry_of_run_id IS NOT NULL
               OR checkpoint_row.source_scope_hash IS DISTINCT FROM
                    marker ->> 'source_scope_sha256'
               OR checkpoint_row.source_ids::jsonb IS DISTINCT FROM
                    candidate_metadata -> 'source_ids'
               OR checkpoint_row.pages_processed IS DISTINCT FROM
                    (disposition ->> 'checkpoint_pages')::bigint
               OR checkpoint_row.rows_processed IS DISTINCT FROM
                    (disposition ->> 'retained_rows')::bigint
               OR checkpoint_row.start_url_hash IS DISTINCT FROM
                    disposition ->> 'start_url_sha256'
               OR {payload_sha_ref}(checkpoint_row.recent_cursor_hashes::jsonb)
                    IS DISTINCT FROM
                      disposition ->> 'recent_cursor_hashes_sha256'
               OR {payload_sha_ref}(checkpoint_row.completeness_json::jsonb)
                    IS DISTINCT FROM
                      disposition ->> 'checkpoint_proof_sha256'
               OR {payload_sha_ref}(diagnostics -> current_resource_type)
                    IS DISTINCT FROM disposition ->> 'diagnostic_sha256'
               OR (
                    current_resource_type = ANY({drift_resources})
                    AND (
                        disposition ->> 'disposition'
                            <> 'terminal_census_drift'
                        OR disposition ->> 'checkpoint_state' <> 'active'
                        OR disposition -> 'page_delta' <> '1'::jsonb
                    )
               )
               OR (
                    current_resource_type <> ALL({drift_resources})
                    AND (
                        disposition ->> 'disposition' <> 'verified_complete'
                        OR disposition ->> 'checkpoint_state' <> 'complete'
                        OR disposition -> 'page_delta' <> '0'::jsonb
                    )
               ) THEN
                RETURN FALSE;
            END IF;
            SELECT pg_catalog.count(*) INTO STRICT relation_resource_count
              FROM {resource_ref} AS resource
             WHERE resource.dataset_id = candidate.dataset_id
               AND resource.resource_type = current_resource_type;
            SELECT COALESCE(
                       pg_catalog.sum((resource_count.value #>> '{{}}')::numeric),
                       0
                   )
              INTO proof_resource_count
              FROM {proof_ref} AS shard
              CROSS JOIN LATERAL pg_catalog.jsonb_each(
                   shard.resource_counts_json
              ) AS resource_count(key, value)
             WHERE shard.dataset_id = candidate.dataset_id
               AND resource_count.key = current_resource_type;
            IF relation_resource_count IS DISTINCT FROM
                    (disposition ->> 'retained_rows')::numeric
               OR proof_resource_count IS DISTINCT FROM relation_resource_count
               OR (
                    current_resource_type = ANY({drift_resources})
                    AND (disposition ->> 'returned_unique')::numeric
                          - relation_resource_count
                        IS DISTINCT FROM
                          (disposition ->> 'terminal_page_entry_count')::numeric
               ) THEN
                RETURN FALSE;
            END IF;
            checkpoint_page_total := checkpoint_page_total
                + checkpoint_row.pages_processed;
            diagnostic_page_total := diagnostic_page_total
                + (disposition ->> 'diagnostic_pages')::numeric;
            checkpoint_row_total := checkpoint_row_total
                + checkpoint_row.rows_processed;
        END LOOP;

        SELECT pg_catalog.count(*) INTO resource_type_count
          FROM (
               SELECT resource.resource_type
                 FROM {resource_ref} AS resource
                WHERE resource.dataset_id = candidate.dataset_id
                  AND resource.resource_type NOT LIKE 'LU:%:pass:%'
                GROUP BY resource.resource_type
          ) AS observed_resource_type;
        IF resource_type_count <> 7
           OR proof_shard_count IS DISTINCT FROM
                (marker ->> 'proof_shard_count')::bigint
           OR proof_row_count IS DISTINCT FROM
                (marker ->> 'proof_row_count')::numeric
           OR checkpoint_page_total IS DISTINCT FROM
                (marker ->> 'checkpoint_pages_processed')::numeric
           OR diagnostic_page_total IS DISTINCT FROM
                (marker ->> 'diagnostic_pages_processed')::numeric
           OR diagnostic_page_total - checkpoint_page_total IS DISTINCT FROM
                (marker ->> 'terminal_page_delta')::numeric
           OR checkpoint_row_total IS DISTINCT FROM
                (marker ->> 'checkpoint_rows_processed')::numeric
           OR checkpoint_row_total IS DISTINCT FROM candidate.resource_count
           OR EXISTS (
                SELECT 1 FROM {bulk_ref} AS bulk
                 WHERE bulk.dataset_id = candidate.dataset_id
                    OR bulk.acquisition_root_run_id =
                         candidate.acquisition_root_run_id
           ) THEN
            RETURN FALSE;
        END IF;
        RETURN TRUE;
    EXCEPTION WHEN OTHERS THEN
        RETURN FALSE;
    END;
    $function$;
    """


def _shared_valid_sql(schema: str) -> str:
    """Dispatch v2 markers to one private helper and preserve v1 inline."""

    terminal = _terminal()
    helper_ref = _qf(schema, _DIRECT_VALID)
    needle = (
        f"        marker := candidate_metadata -> '{terminal._MARKER}';"
    )
    replacement = needle + f"""
        IF marker ->> 'contract_version' = '{_CONTRACT}' THEN
            RETURN {helper_ref}(candidate_dataset_id);
        END IF;"""
    return _replace_exact(
        _scope_binding()._valid_function_sql(schema),
        needle,
        replacement,
    )


def _direct_transition_sql(schema: str) -> str:
    subset = _subset()
    abandonment = _abandonment()
    dataset_ref = _qf(schema, subset._ENDPOINT_DATASET)
    source_ref = _qf(schema, subset._SOURCE)
    checkpoint_ref = _qf(schema, abandonment._CHECKPOINT)
    bulk_ref = _qf(schema, abandonment._BULK_CHECKPOINT)
    import_run_ref = _qf(schema, "import_run")
    payload_sha_ref = _qf(schema, subset._PAYLOAD_SHA256_FUNCTION)
    canonical_sha_ref = _qf(schema, subset._CANONICAL_SHA256_FUNCTION)
    source_resources = _resource_array(_SOURCE_RESOURCE_TYPES)
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
    contract_identity = _terminal()._source_contract_identity_sql(
        "source",
        "terminal_profile.completion_proof_json",
    )
    expected_start_hash = _terminal()._expected_start_url_sha256_sql(
        "source",
        "terminal_profile.completion_proof_json",
        "checkpoint.resource_type",
    )
    page_caps = "pg_catalog.jsonb_build_object(" + ", ".join(
        _ql(resource_type) + ", 250"
        for resource_type in _RESOURCE_TYPES
    ) + ")"
    start_urls_exact = " AND ".join(
        "source.metadata_json::jsonb #>> "
        + _ql(
            "{provider_directory_current_version_census_start_urls,"
            + resource_type
            + "}"
        )
        + " = source.canonical_api_base || '/' || "
        + _ql(resource_type)
        for resource_type in _SOURCE_RESOURCE_TYPES
    )
    return f"""
        IF NEW.publication_metadata_json::jsonb #>> ARRAY[
             '{_MARKER}', 'contract_version'
           ]::text[] = '{_CONTRACT}' THEN
            IF OLD.status <> '{_PRIOR_STATUS}'
               OR NEW.status <> '{_STATUS}'
               OR OLD.publication_metadata_json::jsonb ?| ARRAY[
                    '{_LEGACY_MARKER}', '{_MARKER}'
                  ]::text[]
               OR NEW.publication_metadata_json::jsonb ? '{_LEGACY_MARKER}'
               OR {payload_sha_ref}(
                    NEW.publication_metadata_json::jsonb -> '{_MARKER}'
                  ) <> '{_MARKER_SHA256}'
               OR NEW.publication_metadata_json::jsonb
                      #>> '{{{_MARKER},candidate_metadata_sha256}}'
                    IS DISTINCT FROM {payload_sha_ref}(
                        OLD.publication_metadata_json::jsonb
                    )
               OR NEW.resource_count IS DISTINCT FROM (
                    NEW.publication_metadata_json::jsonb
                        #>> '{{{_MARKER},resource_count}}'
                  )::bigint
               OR NEW.resource_count IS DISTINCT FROM OLD.resource_count
               OR COALESCE(NEW.publication_metadata_json::jsonb, '{{}}'::jsonb)
                    - '{_MARKER}'
                  IS DISTINCT FROM COALESCE(
                    OLD.publication_metadata_json::jsonb,
                    '{{}}'::jsonb
                  )
               OR to_jsonb(NEW) - ARRAY[
                    'status', 'publication_metadata_json'
                  ]::text[]
                  IS DISTINCT FROM to_jsonb(OLD) - ARRAY[
                    'status', 'publication_metadata_json'
                  ]::text[]
               OR NEW.is_current IS NOT FALSE
               OR NEW.previous_dataset_id IS NOT NULL
               OR NEW.dataset_hash IS NOT NULL
               OR NEW.validated_at IS NOT NULL
               OR NEW.published_at IS NOT NULL
               OR NEW.superseded_at IS NOT NULL
               OR NEW.import_run_id IS DISTINCT FROM
                    NEW.acquisition_root_run_id
               OR NEW.publication_metadata_json::jsonb
                      -> 'reused_from_checkpoint' IS DISTINCT FROM 'false'::jsonb
               OR NEW.publication_metadata_json::jsonb
                      ->> 'verification_campaign_id' <> '{_CAMPAIGN}'
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
                    SELECT 1 FROM {source_ref} AS source
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
                       AND NULLIF(source.endpoint_id, '') IS NOT NULL
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
                       AND source.requires_registration IS FALSE
                       AND source.requires_api_key IS FALSE
                       AND source.auth_type = 'none'
                       AND ({subset._reviewed_root_policy_sql(source_metadata, 1)})
                       AND ({subset._reviewed_root_policy_sql(
                            'NEW.publication_metadata_json::jsonb', 1
                       )})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_reviewed_root_policy_v1'
                           = NEW.publication_metadata_json::jsonb
                              -> 'provider_directory_reviewed_root_policy_v1'
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_verification_campaign_id'
                           = NEW.publication_metadata_json::jsonb
                              ->> 'verification_campaign_id'
                       AND ({source_fixed_identity})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_supported_resources'
                           = pg_catalog.to_jsonb({source_resources})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_expected_nonempty_resources'
                           = pg_catalog.to_jsonb({source_resources})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_server_issued_subset_resources'
                           = pg_catalog.to_jsonb({source_resources})
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_fully_enumerable_resources'
                           = '[]'::jsonb
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_resource_page_count_caps'
                           = {page_caps}
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_acquisition_enabled'
                           = 'true'::jsonb
                       AND source.metadata_json::jsonb
                              -> 'provider_directory_manual_only'
                           = 'true'::jsonb
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_coverage_mode'
                           = 'server-issued-traversal-subset'
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_current_version_census_continuation_strategy'
                           = 'smile-opaque-logical-offset-v3'
                       AND ({start_urls_exact})
                       AND {canonical_sha_ref}({contract_identity}) =
                           terminal_profile.completion_proof_json
                              ->> 'contract_identity'
                       AND {canonical_sha_ref}({source_scope_payload}) =
                           NEW.publication_metadata_json::jsonb
                              ->> 'verification_source_scope_hash'
                       AND pg_catalog.jsonb_typeof(
                              source.metadata_json::jsonb
                                  -> 'last_resource_import'
                           ) = 'object'
                       AND (source.metadata_json::jsonb
                              -> 'last_resource_import')
                              ?& ARRAY['run_id', 'observed_at', 'resources']::text[]
                       AND (source.metadata_json::jsonb
                              -> 'last_resource_import')
                              - ARRAY['run_id', 'observed_at', 'resources']::text[]
                           = '{{}}'::jsonb
                       AND source.metadata_json::jsonb
                              #>> '{{last_resource_import,run_id}}'
                           = NEW.import_run_id
                       AND source.metadata_json::jsonb
                              #>> '{{last_resource_import,observed_at}}'
                           ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T'
                             '([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z$'
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
                                    )::pg_catalog.timestamptz AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                               ) = source.metadata_json::jsonb
                                      #>> '{{last_resource_import,observed_at}}'
                               ELSE FALSE
                           END
                       AND NEW.publication_metadata_json::jsonb
                              #>> '{{{_MARKER},source_import_sha256}}'
                           = {payload_sha_ref}(
                               source.metadata_json::jsonb
                                   -> 'last_resource_import'
                             )
                       AND NEW.publication_metadata_json::jsonb
                              #>> '{{{_MARKER},source_diagnostics_sha256}}'
                           = {payload_sha_ref}(
                               source.metadata_json::jsonb
                                   #> '{{last_resource_import,resources}}'
                             )
                       AND source.metadata_json::jsonb
                              #> '{{last_resource_import,resources}}'
                           = NEW.publication_metadata_json::jsonb
                              -> 'resource_diagnostics'
                       AND source.metadata_json::jsonb
                              #> '{{last_resource_import,resources}}'
                           = NEW.publication_metadata_json::jsonb
                              #> '{{completion_proof_v1,resource_diagnostics}}'
                       AND NOT EXISTS (
                            SELECT 1 FROM {checkpoint_ref} AS checkpoint
                             WHERE checkpoint.dataset_id = NEW.dataset_id
                               AND checkpoint.canonical_api_base
                                    IS DISTINCT FROM source.canonical_api_base
                       )
                       AND NOT EXISTS (
                            SELECT 1 FROM {checkpoint_ref} AS checkpoint
                             WHERE checkpoint.dataset_id = NEW.dataset_id
                               AND checkpoint.start_url_hash IS DISTINCT FROM
                                    ({expected_start_hash})
                       )
               )
               OR EXISTS (
                    SELECT 1 FROM {import_run_ref} AS import_row
                     WHERE import_row.run_id IN (
                               NEW.acquisition_root_run_id, NEW.import_run_id
                           )
                        OR import_row.retry_of_run_id IN (
                               NEW.acquisition_root_run_id, NEW.import_run_id
                           )
               )
               OR EXISTS (
                    SELECT 1 FROM {dataset_ref} AS other
                     WHERE other.dataset_id <> NEW.dataset_id
                       AND other.endpoint_id = NEW.endpoint_id
                       AND (
                            other.is_current IS TRUE
                            OR (
                                other.status IN ('acquiring', 'failed')
                                AND other.publication_metadata_json::jsonb
                                      ->> 'verification_campaign_id'
                                    = '{_CAMPAIGN}'
                            )
                       )
               )
               OR EXISTS (
                    SELECT 1 FROM {dataset_ref} AS child
                     WHERE child.previous_dataset_id = NEW.dataset_id
               )
               OR EXISTS (
                    SELECT 1 FROM {bulk_ref} AS bulk
                     WHERE bulk.dataset_id = NEW.dataset_id
                        OR bulk.acquisition_root_run_id =
                             NEW.acquisition_root_run_id
               )
               OR EXISTS (
                    SELECT 1 FROM {checkpoint_ref} AS checkpoint
                     WHERE checkpoint.dataset_id = NEW.dataset_id
                       AND checkpoint.retry_of_run_id IS NOT NULL
               ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_terminal_v4_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
    """


def _dataset_guard_sql(schema: str) -> str:
    """Add one exact v2 transition before the unchanged v1 branch."""

    terminal = _terminal()
    original = _scope_binding()._dataset_guard_sql(schema)
    needle = f"""        IF NEW.publication_metadata_json::jsonb ? '{terminal._MARKER}' THEN"""
    replacement = _direct_transition_sql(schema) + needle
    return _replace_exact(original, needle, replacement)


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
        "import_run",
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(_qf(schema, relation_name) for relation_name in relation_names)
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _normalized_body_sha256(function_sql: str) -> str:
    delimiter = "AS $function$"
    if function_sql.count(delimiter) != 1 or function_sql.count("$function$;") != 1:
        raise RuntimeError("provider directory function body renderer changed")
    body = function_sql.split(delimiter, 1)[1].rsplit("$function$;", 1)[0]
    normalized = " ".join(body.split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _helper_acl_sql(schema: str) -> str:
    return f"""
    DO $migration$
    DECLARE
        owner_name text;
        role_name text;
    BEGIN
        SELECT owner_role.rolname INTO STRICT owner_name
          FROM pg_catalog.pg_proc AS shared
          JOIN pg_catalog.pg_roles AS owner_role
            ON owner_role.oid = shared.proowner
         WHERE shared.oid = pg_catalog.to_regprocedure(
                   {_ql(_qf(schema, _VALID) + '(text)')}
               );
        EXECUTE pg_catalog.format(
            'ALTER FUNCTION %I.%I(text) OWNER TO %I',
            {_ql(schema)}, {_ql(_DIRECT_VALID)}, owner_name
        );
        EXECUTE pg_catalog.format(
            'REVOKE ALL ON FUNCTION %I.%I(text) FROM PUBLIC',
            {_ql(schema)}, {_ql(_DIRECT_VALID)}
        );
        FOR role_name IN
            SELECT grantee.rolname
              FROM pg_catalog.pg_proc AS helper
              CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
                   helper.proacl,
                   pg_catalog.acldefault('f', helper.proowner)
              )) AS function_acl
              JOIN pg_catalog.pg_roles AS grantee
                ON grantee.oid = function_acl.grantee
             WHERE helper.oid = pg_catalog.to_regprocedure(
                       {_ql(_qf(schema, _DIRECT_VALID) + '(text)')}
                   )
               AND function_acl.privilege_type = 'EXECUTE'
               AND function_acl.grantee <> helper.proowner
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE EXECUTE ON FUNCTION %I.%I(text) FROM %I',
                {_ql(schema)}, {_ql(_DIRECT_VALID)}, role_name
            );
        END LOOP;
    END;
    $migration$;
    """


def _body_shape_fence_sql(schema: str, *, installed: bool) -> str:
    abandonment = _abandonment()
    expected_sql_by_signature = {
        _qf(schema, _VALID) + "(text)": (
            _shared_valid_sql(schema)
            if installed
            else _scope_binding()._valid_function_sql(schema)
        ),
        _qf(schema, abandonment._DATASET_GUARD) + "()": (
            _dataset_guard_sql(schema)
            if installed
            else _scope_binding()._dataset_guard_sql(schema)
        ),
        _qf(schema, abandonment._CHECKPOINT_GUARD) + "()": (
            _terminal()._checkpoint_guard_sql(schema)
        ),
    }
    values = ", ".join(
        "(" + _ql(signature) + ", " + _ql(_normalized_body_sha256(sql)) + ")"
        for signature, sql in expected_sql_by_signature.items()
    )
    helper_signature = _qf(schema, _DIRECT_VALID) + "(text)"
    helper_count = 1 if installed else 0
    helper_body = _normalized_body_sha256(_direct_valid_sql(schema))
    return f"""
    DO $migration$
    DECLARE
        matched_count bigint;
        raw_helper_count bigint;
        helper_shape_count bigint;
    BEGIN
        SELECT pg_catalog.count(*) INTO matched_count
          FROM (VALUES {values}) AS expected(signature, body_sha256)
          JOIN pg_catalog.pg_proc AS function_row
            ON function_row.oid = pg_catalog.to_regprocedure(expected.signature)
         WHERE pg_catalog.encode(
                   pg_catalog.sha256(pg_catalog.convert_to(
                     pg_catalog.btrim(pg_catalog.regexp_replace(
                       function_row.prosrc, '[[:space:]]+', ' ', 'g'
                     )), 'UTF8'
                   )), 'hex'
               ) = expected.body_sha256;
        SELECT pg_catalog.count(*) INTO raw_helper_count
         WHERE pg_catalog.to_regprocedure({_ql(helper_signature)}) IS NOT NULL;
        SELECT pg_catalog.count(*) INTO helper_shape_count
          FROM pg_catalog.pg_proc AS helper
          JOIN pg_catalog.pg_proc AS shared
            ON shared.oid = pg_catalog.to_regprocedure(
                 {_ql(_qf(schema, _VALID) + '(text)')}
               )
          JOIN pg_catalog.pg_language AS language_row
            ON language_row.oid = helper.prolang
         WHERE helper.oid = pg_catalog.to_regprocedure(
                   {_ql(helper_signature)}
               )
           AND helper.prorettype = 'pg_catalog.bool'::regtype
           AND helper.prokind = 'f'
           AND language_row.lanname = 'plpgsql'
           AND helper.provolatile = 's'
           AND helper.proisstrict IS FALSE
           AND helper.proparallel = 'u'
           AND helper.prosecdef IS TRUE
           AND helper.proowner = shared.proowner
           AND helper.proconfig IS NOT DISTINCT FROM
                ARRAY['search_path=pg_catalog']::text[]
           AND pg_catalog.encode(
                 pg_catalog.sha256(pg_catalog.convert_to(
                   pg_catalog.btrim(pg_catalog.regexp_replace(
                     helper.prosrc, '[[:space:]]+', ' ', 'g'
                   )), 'UTF8'
                 )), 'hex'
               ) = '{helper_body}'
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.aclexplode(COALESCE(
                     helper.proacl,
                     pg_catalog.acldefault('f', helper.proowner)
                )) AS helper_acl
                 WHERE helper_acl.privilege_type = 'EXECUTE'
                   AND helper_acl.grantee <> helper.proowner
           );
        IF matched_count <> 3
           OR raw_helper_count <> {helper_count}
           OR helper_shape_count <> {helper_count} THEN
            RAISE EXCEPTION
                'provider_directory_subset_terminal_v4_shape_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _identity_snapshot_sql(schema: str) -> str:
    abandonment = _abandonment()
    signatures = (
        _qf(schema, _VALID) + "(text)",
        _qf(schema, abandonment._DATASET_GUARD) + "()",
        _qf(schema, abandonment._CHECKPOINT_GUARD) + "()",
    )
    values = ", ".join("(" + _ql(value) + ")" for value in signatures)
    return f"""
    CREATE TEMP TABLE {_IDENTITY_SNAPSHOT} ON COMMIT DROP AS
    SELECT 'function'::text AS object_kind,
           expected.signature AS object_name,
           function_row.oid AS object_oid,
           function_row.proowner AS owner_oid,
           function_row.proacl AS function_acl,
           NULL::oid AS linked_oid
      FROM (VALUES {values}) AS expected(signature)
      JOIN pg_catalog.pg_proc AS function_row
        ON function_row.oid = pg_catalog.to_regprocedure(expected.signature)
    UNION ALL
    SELECT 'trigger', expected.relation_name || ':' || expected.trigger_name,
           trigger_row.oid, NULL::oid, NULL::aclitem[], trigger_row.tgfoid
      FROM (VALUES
           (
               {_ql(_qf(schema, _subset()._ENDPOINT_DATASET))},
               {_ql(_DATASET_CONSTRAINT)}
           ),
           (
               {_ql(_qf(schema, _abandonment()._CHECKPOINT))},
               {_ql(_CHECKPOINT_CONSTRAINT)}
           )
      ) AS expected(relation_name, trigger_name)
      JOIN pg_catalog.pg_trigger AS trigger_row
        ON trigger_row.tgrelid = expected.relation_name::regclass
       AND trigger_row.tgname = expected.trigger_name;
    """


def _identity_continuity_sql(schema: str) -> str:
    abandonment = _abandonment()
    signatures = (
        _qf(schema, _VALID) + "(text)",
        _qf(schema, abandonment._DATASET_GUARD) + "()",
        _qf(schema, abandonment._CHECKPOINT_GUARD) + "()",
    )
    values = ", ".join("(" + _ql(value) + ")" for value in signatures)
    return f"""
    DO $migration$
    BEGIN
        IF (SELECT pg_catalog.count(*) FROM {_IDENTITY_SNAPSHOT}) <> 5
           OR EXISTS (
                WITH current_identity AS (
                    SELECT 'function'::text AS object_kind,
                           expected.signature AS object_name,
                           function_row.oid AS object_oid,
                           function_row.proowner AS owner_oid,
                           function_row.proacl AS function_acl,
                           NULL::oid AS linked_oid
                      FROM (VALUES {values}) AS expected(signature)
                      JOIN pg_catalog.pg_proc AS function_row
                        ON function_row.oid = pg_catalog.to_regprocedure(
                             expected.signature
                           )
                    UNION ALL
                    SELECT 'trigger',
                           expected.relation_name || ':' || expected.trigger_name,
                           trigger_row.oid, NULL::oid, NULL::aclitem[],
                           trigger_row.tgfoid
                      FROM (VALUES
                           (
                               {_ql(_qf(schema, _subset()._ENDPOINT_DATASET))},
                               {_ql(_DATASET_CONSTRAINT)}
                           ),
                           (
                               {_ql(_qf(schema, abandonment._CHECKPOINT))},
                               {_ql(_CHECKPOINT_CONSTRAINT)}
                           )
                      ) AS expected(relation_name, trigger_name)
                      JOIN pg_catalog.pg_trigger AS trigger_row
                        ON trigger_row.tgrelid = expected.relation_name::regclass
                       AND trigger_row.tgname = expected.trigger_name
                )
                SELECT 1 FROM {_IDENTITY_SNAPSHOT} AS original
                FULL JOIN current_identity AS current
                  USING (object_kind, object_name)
                 WHERE original.object_oid IS DISTINCT FROM current.object_oid
                    OR original.owner_oid IS DISTINCT FROM current.owner_oid
                    OR original.function_acl IS DISTINCT FROM current.function_acl
                    OR original.linked_oid IS DISTINCT FROM current.linked_oid
           ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_terminal_v4_identity_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _v2_evidence_fence_sql(schema: str, *, present: bool) -> str:
    dataset_ref = _qf(schema, _subset()._ENDPOINT_DATASET)
    predicate = f"""dataset.publication_metadata_json::jsonb #>> ARRAY[
                         '{_MARKER}', 'contract_version'
                     ]::text[] = '{_CONTRACT}'"""
    if present:
        return f"""
        DO $migration$
        BEGIN
            IF EXISTS (SELECT 1 FROM {dataset_ref} AS dataset WHERE {predicate}) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_terminal_v4_downgrade_blocked'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $migration$;
        """
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (SELECT 1 FROM {dataset_ref} AS dataset WHERE {predicate}) THEN
            RAISE EXCEPTION
                'provider_directory_subset_terminal_v4_adoption_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _predecessor_shape_fences(schema: str) -> None:
    terminal = _terminal()
    original_op = terminal.op
    try:
        terminal.op = op
        terminal._base_shape_fences(schema)
        op.execute(
            terminal._new_object_shape_fence_sql(
                schema,
                expect_installed=True,
            )
        )
    finally:
        terminal.op = original_op


def _drop_identity_snapshot() -> None:
    op.execute(f"DROP TABLE {_IDENTITY_SNAPSHOT};")


def upgrade() -> None:
    schema = _subset()._schema()
    _lock_relations(schema)
    _predecessor_shape_fences(schema)
    op.execute(_body_shape_fence_sql(schema, installed=False))
    op.execute(_v2_evidence_fence_sql(schema, present=False))
    op.execute(_identity_snapshot_sql(schema))
    op.execute(_direct_valid_sql(schema))
    op.execute(_helper_acl_sql(schema))
    op.execute(_shared_valid_sql(schema))
    op.execute(_dataset_guard_sql(schema))
    op.execute(_body_shape_fence_sql(schema, installed=True))
    op.execute(_identity_continuity_sql(schema))
    _drop_identity_snapshot()
    _predecessor_shape_fences(schema)


def downgrade() -> None:
    schema = _subset()._schema()
    _lock_relations(schema)
    op.execute(_body_shape_fence_sql(schema, installed=True))
    op.execute(_v2_evidence_fence_sql(schema, present=True))
    op.execute(_identity_snapshot_sql(schema))
    op.execute(_scope_binding()._valid_function_sql(schema))
    op.execute(_scope_binding()._dataset_guard_sql(schema))
    op.execute(f"DROP FUNCTION {_qf(schema, _DIRECT_VALID)}(text);")
    op.execute(_body_shape_fence_sql(schema, installed=False))
    op.execute(_identity_continuity_sql(schema))
    _drop_identity_snapshot()
    _predecessor_shape_fences(schema)
