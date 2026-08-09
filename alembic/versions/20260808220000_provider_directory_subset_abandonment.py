# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seal abandoned Provider Directory reviewed subset acquisitions.

Revision ID: 20260808220000_provider_directory_subset_abandonment
Revises: 20260808210000_provider_directory_subset_payload_guard_repair
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op

revision = "20260808220000_provider_directory_subset_abandonment"
down_revision = "20260808210000_provider_directory_subset_payload_guard_repair"
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = "20260808210000_provider_directory_subset_payload_guard_repair.py"
_DATASET = "provider_directory_endpoint_dataset"
_RESOURCE = "provider_directory_dataset_resource"
_SOURCE = "provider_directory_source"
_PROOF_SHARD = "provider_directory_dataset_proof_shard"
_CHECKPOINT = "provider_directory_pagination_checkpoint"
_BULK_CHECKPOINT = "provider_directory_bulk_acquisition_checkpoint"
_STATUS = "acquisition_abandoned"
_MARKER = "provider_directory_reviewed_subset_abandonment_v1"
_CONTRACT = "healthporta.provider-directory.reviewed-subset-abandonment.v1"
_VALID = "provider_directory_subset_abandonment_valid"
_DATASET_GUARD = "guard_provider_directory_subset_abandonment_dataset"
_CHILD_GUARD = "guard_provider_directory_subset_abandonment_child"
_CHECKPOINT_GUARD = "guard_provider_directory_subset_abandonment_checkpoint"
_DATASET_ROW_TRIGGER = "pd_subset_abandonment_dataset_guard"
_DATASET_CONSTRAINT = "pd_subset_abandonment_dataset_consistency_guard"
_DATASET_TRUNCATE = "pd_subset_abandonment_dataset_truncate_guard"
_RESOURCE_INSERT = "pd_subset_abandonment_resource_insert_guard"
_RESOURCE_UPDATE = "pd_subset_abandonment_resource_update_guard"
_RESOURCE_DELETE = "pd_subset_abandonment_resource_delete_guard"
_PROOF_INSERT = "pd_subset_abandonment_proof_insert_guard"
_PROOF_UPDATE = "pd_subset_abandonment_proof_update_guard"
_PROOF_DELETE = "pd_subset_abandonment_proof_delete_guard"
_PROOF_TRUNCATE = "pd_subset_abandonment_proof_truncate_guard"
_BULK_INSERT = "pd_subset_abandonment_bulk_insert_guard"
_BULK_UPDATE = "pd_subset_abandonment_bulk_update_guard"
_BULK_DELETE = "pd_subset_abandonment_bulk_delete_guard"
_BULK_TRUNCATE = "pd_subset_abandonment_bulk_truncate_guard"
_CHECKPOINT_ROW_TRIGGER = "pd_subset_abandonment_checkpoint_guard"
_CHECKPOINT_CONSTRAINT = "provider_directory_subset_abandonment_checkpoint_guard"
_CHECKPOINT_TRUNCATE = "pd_subset_abandonment_checkpoint_truncate_guard"
_PROOF_COLUMNS = (
    "dataset_id",
    "shard_id",
    "endpoint_id",
    "acquisition_root_run_id",
    "source_ids_json",
    "resource_count",
    "resource_counts_json",
    "first_identity_json",
    "last_identity_json",
    "input_sha256",
    "artifact_sha256",
    "artifact_byte_count",
    "payload_bytes",
    "created_at",
)
_CHECKPOINT_COLUMNS = (
    "canonical_api_base",
    "resource_type",
    "source_scope_hash",
    "dataset_id",
    "source_ids",
    "acquisition_root_run_id",
    "owner_run_id",
    "retry_of_run_id",
    "start_url_hash",
    "next_url",
    "state",
    "pages_processed",
    "rows_processed",
    "recent_cursor_hashes",
    "completeness_json",
    "created_at",
    "updated_at",
    "completed_at",
)
_BULK_CHECKPOINT_COLUMNS = (
    "checkpoint_id",
    "canonical_api_base",
    "resource_type",
    "source_scope_hash",
    "strategy_version",
    "acquisition_root_run_id",
    "owner_run_id",
    "retry_of_run_id",
    "endpoint_id",
    "dataset_id",
    "start_url_hash",
    "status_url_ciphertext",
    "status_url_hash",
    "manifest_hash",
    "manifest_ciphertext",
    "manifest_json",
    "state",
    "lease_expires_at",
    "rows_written",
    "error",
    "created_at",
    "accepted_at",
    "last_polled_at",
    "next_poll_at",
    "manifest_received_at",
    "completed_at",
    "failed_at",
    "updated_at",
)


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_subset_payload_guard_repair_predecessor",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory guard repair revision is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _subset() -> ModuleType:
    return _predecessor()._predecessor()._predecessor()


def _shape_fence_sqls(schema: str) -> tuple[str, ...]:
    subset = _subset()
    return (
        *_predecessor()._shape_fence_sqls(schema),
        subset._relation_schema_fence_sql(
            schema,
            _PROOF_SHARD,
            _PROOF_COLUMNS,
        ),
        subset._relation_schema_fence_sql(
            schema,
            _CHECKPOINT,
            _CHECKPOINT_COLUMNS,
        ),
        subset._relation_schema_fence_sql(
            schema,
            _BULK_CHECKPOINT,
            _BULK_CHECKPOINT_COLUMNS,
        ),
    )


def _marker_integer_sql(field_name: str) -> str:
    """Return an exception-safe nonnegative JSON integer expression."""

    return f"""
        CASE
            WHEN pg_catalog.jsonb_typeof(candidate.marker -> '{field_name}')
                    = 'number'
             AND candidate.marker ->> '{field_name}' ~ '^(0|[1-9][0-9]*)$'
            THEN (candidate.marker ->> '{field_name}')::numeric
            ELSE NULL
        END
    """


def _valid_function_sql(schema: str) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, _DATASET)
    resource_ref = subset._qf(schema, _RESOURCE)
    proof_ref = subset._qf(schema, _PROOF_SHARD)
    bulk_ref = subset._qf(schema, _BULK_CHECKPOINT)
    checkpoint_ref = subset._qf(schema, _CHECKPOINT)
    valid_ref = subset._qf(schema, _VALID)
    reviewed_resource_array = (
        "ARRAY["
        + ", ".join(
            subset._ql(resource_type) for resource_type in subset._SUBSET_RESOURCE_TYPES
        )
        + "]::text[]"
    )
    return f"""
    CREATE OR REPLACE FUNCTION {valid_ref}(candidate_dataset_id text)
    RETURNS boolean
    LANGUAGE sql
    STABLE
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    WITH candidate AS (
        SELECT dataset.*,
               dataset.publication_metadata_json::jsonb
                   -> '{_MARKER}' AS marker
          FROM {dataset_ref} AS dataset
         WHERE dataset.dataset_id = candidate_dataset_id
    ), checkpoint_summary AS (
        SELECT checkpoint.dataset_id,
               count(*) AS checkpoint_count,
               sum(checkpoint.pages_processed) AS pages_processed,
               sum(checkpoint.rows_processed) AS rows_processed,
               jsonb_agg(checkpoint.resource_type ORDER BY checkpoint.resource_type)
                   AS resource_types,
               jsonb_object_agg(
                   checkpoint.resource_type,
                   'http_410'::text
                   ORDER BY checkpoint.resource_type
               ) AS terminal_error_codes,
               min(checkpoint.source_scope_hash) AS source_scope_hash,
               count(DISTINCT checkpoint.source_scope_hash) AS scope_count,
               count(*) FILTER (
                   WHERE checkpoint.state <> '{_STATUS}'
                      OR checkpoint.completed_at IS NULL
               ) AS invalid_state_count
          FROM {checkpoint_ref} AS checkpoint
         WHERE checkpoint.dataset_id = candidate_dataset_id
         GROUP BY checkpoint.dataset_id
    ), resource_summary AS (
        SELECT candidate.dataset_id,
               count(resource.dataset_id) AS resource_count
          FROM candidate
          LEFT JOIN {resource_ref} AS resource
            ON resource.dataset_id = candidate.dataset_id
           AND resource.resource_type NOT LIKE 'LU:%:pass:%'
         GROUP BY candidate.dataset_id
    ), proof_summary AS (
        SELECT candidate.dataset_id,
               count(shard.dataset_id) AS shard_count,
               sum(shard.resource_count) AS proof_row_count,
               count(*) FILTER (
                   WHERE shard.dataset_id IS NOT NULL
                     AND (
                          shard.acquisition_root_run_id IS DISTINCT FROM
                              candidate.acquisition_root_run_id
                          OR shard.endpoint_id IS DISTINCT FROM candidate.endpoint_id
                          OR shard.source_ids_json IS DISTINCT FROM
                              candidate.publication_metadata_json::jsonb
                                  -> 'source_ids'
                     )
               ) AS invalid_lineage_count,
               count(*) FILTER (
                   WHERE shard.dataset_id IS NOT NULL
                     AND (
                          pg_catalog.jsonb_typeof(
                              shard.resource_counts_json
                          ) IS DISTINCT FROM 'object'
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
                               WHERE NOT (
                                         candidate.publication_metadata_json::jsonb
                                             -> 'selected_resources'
                                      ) ? proof_count.resource_type
                                  OR pg_catalog.jsonb_typeof(
                                         proof_count.count_json
                                     ) IS DISTINCT FROM 'number'
                                  OR proof_count.count_json #>> '{{}}'
                                         !~ '^(0|[1-9][0-9]*)$'
                          )
                          OR (
                              SELECT COALESCE(sum(
                                  CASE
                                      WHEN pg_catalog.jsonb_typeof(
                                               proof_count.count_json
                                           ) = 'number'
                                       AND proof_count.count_json #>> '{{}}'
                                               ~ '^(0|[1-9][0-9]*)$'
                                      THEN (proof_count.count_json #>> '{{}}')::numeric
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
               ) AS invalid_resource_map_count
          FROM candidate
          LEFT JOIN {proof_ref} AS shard
            ON candidate.dataset_id = shard.dataset_id
         GROUP BY candidate.dataset_id
    )
    SELECT COALESCE(bool_and(
        candidate.status = '{_STATUS}'
        AND candidate.is_current IS FALSE
        AND candidate.completion_proof_required_version = 3
        AND candidate.completion_proof_json IS NULL
        AND candidate.completion_proof_sha256 IS NULL
        AND candidate.validated_at IS NULL
        AND candidate.published_at IS NULL
        AND candidate.superseded_at IS NULL
        AND jsonb_typeof(candidate.marker) = 'object'
        AND candidate.marker ?& ARRAY[
            'contract_version', 'reason_code', 'source_scope_sha256',
            'resource_types', 'terminal_error_codes', 'checkpoint_count',
            'pages_processed', 'rows_processed', 'resource_count',
            'proof_shard_count', 'proof_row_count'
        ]::text[]
        AND candidate.marker - ARRAY[
            'contract_version', 'reason_code', 'source_scope_sha256',
            'resource_types', 'terminal_error_codes', 'checkpoint_count',
            'pages_processed', 'rows_processed', 'resource_count',
            'proof_shard_count', 'proof_row_count'
        ]::text[] = '{{}}'::jsonb
        AND candidate.marker ->> 'contract_version' = '{_CONTRACT}'
        AND candidate.marker ->> 'reason_code' = 'expired_server_cursor'
        AND candidate.marker ->> 'source_scope_sha256'
                ~ '^[0-9a-f]{{64}}$'
        AND candidate.marker -> 'resource_types'
                = checkpoint_summary.resource_types
        AND candidate.marker -> 'resource_types'
                = pg_catalog.to_jsonb({reviewed_resource_array})
        AND candidate.marker -> 'terminal_error_codes'
                = checkpoint_summary.terminal_error_codes
        AND candidate.publication_metadata_json::jsonb -> 'selected_resources'
                = checkpoint_summary.resource_types
        AND candidate.publication_metadata_json::jsonb
                ->> 'verification_source_scope_hash'
                ~ '^[0-9a-f]{{64}}$'
        AND NULLIF(
                candidate.publication_metadata_json::jsonb
                    ->> 'verification_campaign_id',
                ''
            ) IS NOT NULL
        AND jsonb_typeof(
                candidate.publication_metadata_json::jsonb -> 'source_ids'
            ) = 'array'
        AND jsonb_array_length(
                candidate.publication_metadata_json::jsonb -> 'source_ids'
            ) = 1
        AND checkpoint_summary.source_scope_hash
                = candidate.marker ->> 'source_scope_sha256'
        AND candidate.publication_metadata_json::jsonb
                ->> 'verification_source_scope_hash'
                IS DISTINCT FROM checkpoint_summary.source_scope_hash
        AND checkpoint_summary.scope_count = 1
        AND checkpoint_summary.invalid_state_count = 0
        AND checkpoint_summary.checkpoint_count
                = ({_marker_integer_sql('checkpoint_count')})
        AND checkpoint_summary.pages_processed
                = ({_marker_integer_sql('pages_processed')})
        AND checkpoint_summary.rows_processed
                = ({_marker_integer_sql('rows_processed')})
        AND resource_summary.resource_count = candidate.resource_count
        AND resource_summary.resource_count
                = ({_marker_integer_sql('resource_count')})
        AND checkpoint_summary.rows_processed = resource_summary.resource_count
        AND proof_summary.shard_count
                = ({_marker_integer_sql('proof_shard_count')})
        AND COALESCE(proof_summary.proof_row_count, 0)
                = ({_marker_integer_sql('proof_row_count')})
        AND COALESCE(proof_summary.proof_row_count, 0)
                = resource_summary.resource_count
        AND proof_summary.invalid_lineage_count = 0
        AND proof_summary.invalid_resource_map_count = 0
        AND NOT EXISTS (
            SELECT 1
              FROM {checkpoint_ref} AS checkpoint
             WHERE checkpoint.dataset_id = candidate.dataset_id
               AND (
                    checkpoint.acquisition_root_run_id IS DISTINCT FROM
                        candidate.acquisition_root_run_id
                    OR checkpoint.dataset_id IS DISTINCT FROM candidate.dataset_id
                    OR checkpoint.source_ids::jsonb IS DISTINCT FROM
                        candidate.publication_metadata_json::jsonb -> 'source_ids'
                    OR checkpoint.source_scope_hash IS DISTINCT FROM
                        candidate.marker ->> 'source_scope_sha256'
                    OR checkpoint.owner_run_id IS DISTINCT FROM
                        candidate.import_run_id
                    OR checkpoint.pages_processed < 0
                    OR checkpoint.rows_processed < 0
                    OR (
                        SELECT count(*)
                          FROM {resource_ref} AS resource
                         WHERE resource.dataset_id = candidate.dataset_id
                           AND resource.resource_type = checkpoint.resource_type
                    ) IS DISTINCT FROM checkpoint.rows_processed
                    OR (
                        SELECT COALESCE(sum(
                            CASE
                                WHEN proof_count.resource_count_text
                                         ~ '^(0|[1-9][0-9]*)$'
                                THEN proof_count.resource_count_text::numeric
                                ELSE NULL
                            END
                        ), 0)
                          FROM {proof_ref} AS shard
                          CROSS JOIN LATERAL jsonb_each_text(
                              shard.resource_counts_json
                          ) AS proof_count(resource_type, resource_count_text)
                         WHERE shard.dataset_id = candidate.dataset_id
                           AND proof_count.resource_type = checkpoint.resource_type
                    ) IS DISTINCT FROM checkpoint.rows_processed::numeric
               )
        )
        AND NOT EXISTS (
            SELECT 1
              FROM {bulk_ref} AS bulk_checkpoint
             WHERE bulk_checkpoint.dataset_id = candidate.dataset_id
                OR bulk_checkpoint.acquisition_root_run_id
                    = candidate.acquisition_root_run_id
        )
    ), false)
      FROM candidate
      JOIN checkpoint_summary USING (dataset_id)
      JOIN resource_summary USING (dataset_id)
      JOIN proof_summary USING (dataset_id);
    $function$;
    """


def _dataset_guard_sql(schema: str) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, _DATASET)
    source_ref = subset._qf(schema, _SOURCE)
    checkpoint_ref = subset._qf(schema, _CHECKPOINT)
    valid_ref = subset._qf(schema, _VALID)
    guard_ref = subset._qf(schema, _DATASET_GUARD)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
                SELECT 1 FROM {dataset_ref} AS dataset
                 WHERE dataset.status = '{_STATUS}'
                    OR dataset.publication_metadata_json::jsonb ? '{_MARKER}'
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_WHEN = 'AFTER' THEN
            IF NEW.status = '{_STATUS}'
               AND {valid_ref}(NEW.dataset_id) IS DISTINCT FROM TRUE THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_OP = 'INSERT' THEN
            IF NEW.status = '{_STATUS}'
               OR NEW.publication_metadata_json::jsonb ? '{_MARKER}' THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_insert_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
            IF OLD.status = '{_STATUS}'
               OR OLD.publication_metadata_json::jsonb ? '{_MARKER}' THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_delete_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN OLD;
        END IF;
        IF OLD.status = '{_STATUS}'
           OR OLD.publication_metadata_json::jsonb ? '{_MARKER}' THEN
            IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF NEW.status = '{_STATUS}'
           OR NEW.publication_metadata_json::jsonb ? '{_MARKER}' THEN
            IF OLD.status NOT IN ('acquiring', 'incomplete', 'failed')
               OR NEW.status <> '{_STATUS}'
               OR NEW.is_current IS NOT FALSE
               OR NEW.completion_proof_required_version IS DISTINCT FROM 3
               OR NEW.completion_proof_json IS NOT NULL
               OR NEW.completion_proof_sha256 IS NOT NULL
               OR NEW.resource_count < 0
               OR (
                    SELECT count(*)
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
                     WHERE source.source_id =
                               NEW.publication_metadata_json::jsonb
                                   #>> '{{source_ids,0}}'
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_configured_endpoint_id'
                              = NEW.endpoint_id
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_candidate_status'
                              = 'pending_two_matching_reviewed_subset_acquisitions'
                       AND source.metadata_json::jsonb
                              ->> 'provider_directory_verification_campaign_id'
                              = NEW.publication_metadata_json::jsonb
                                  ->> 'verification_campaign_id'
                       AND NOT source.metadata_json::jsonb ?
                              'provider_directory_reviewed_subset_activation_v1'
                       AND source.metadata_json::jsonb
                              #>> '{{last_resource_import,run_id}}'
                              = NEW.import_run_id
                       AND pg_catalog.jsonb_typeof(
                              source.metadata_json::jsonb
                                  #> '{{last_resource_import,resources}}'
                           ) = 'object'
                       AND (
                            SELECT COALESCE(
                                pg_catalog.jsonb_agg(
                                    resource_type ORDER BY resource_type
                                ),
                                '[]'::jsonb
                            )
                              FROM pg_catalog.jsonb_object_keys(
                                   source.metadata_json::jsonb
                                       #> '{{last_resource_import,resources}}'
                              ) AS resource_key(resource_type)
                           ) = NEW.publication_metadata_json::jsonb
                                   -> 'selected_resources'
                       AND NOT EXISTS (
                            SELECT 1
                              FROM pg_catalog.jsonb_each(
                                   source.metadata_json::jsonb
                                       #> '{{last_resource_import,resources}}'
                              ) AS diagnostic(resource_type, diagnostic_json)
                             WHERE pg_catalog.jsonb_typeof(diagnostic_json)
                                       <> 'object'
                                OR diagnostic_json ->> 'fetch_mode'
                                       <> 'server_issued_traversal_subset'
                                OR diagnostic_json -> 'complete'
                                       IS DISTINCT FROM 'false'::jsonb
                                OR diagnostic_json -> 'bounded'
                                       IS DISTINCT FROM 'false'::jsonb
                                OR NOT (
                                    diagnostic_json ->> 'error' = 'http_410'
                                    OR diagnostic_json ->> 'error'
                                           = 'provider_directory_current_version_census_completeness_blocked:http_410'
                                )
                       )
                       AND NOT EXISTS (
                            SELECT 1
                              FROM {checkpoint_ref} AS checkpoint
                             WHERE checkpoint.dataset_id = NEW.dataset_id
                               AND checkpoint.canonical_api_base
                                      IS DISTINCT FROM
                                      source.canonical_api_base
                       )
               )
               OR jsonb_typeof(
                    NEW.publication_metadata_json::jsonb -> '{_MARKER}'
                  ) IS DISTINCT FROM 'object'
               OR COALESCE(NEW.publication_metadata_json::jsonb, '{{}}'::jsonb)
                      - '{_MARKER}'
                  IS DISTINCT FROM
                  COALESCE(OLD.publication_metadata_json::jsonb, '{{}}'::jsonb)
               OR to_jsonb(NEW) - ARRAY[
                    'status', 'resource_count', 'publication_metadata_json'
                  ]::text[]
                  IS DISTINCT FROM
                  to_jsonb(OLD) - ARRAY[
                    'status', 'resource_count', 'publication_metadata_json'
                  ]::text[] THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _child_guard_sql(schema: str) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, _DATASET)
    guard_ref = subset._qf(schema, _CHILD_GUARD)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        affected_dataset_ids text[];
        immutable_parent_count bigint;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
                SELECT 1 FROM {dataset_ref} AS dataset
                 WHERE dataset.status = '{_STATUS}'
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_child_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        ELSIF TG_TABLE_NAME = '{_BULK_CHECKPOINT}' AND TG_OP = 'INSERT' THEN
            SELECT pg_catalog.array_agg(
                       dataset.dataset_id ORDER BY dataset.dataset_id
                   )
              INTO affected_dataset_ids
              FROM {dataset_ref} AS dataset
             WHERE dataset.dataset_id IN (
                       SELECT dataset_id FROM new_rows
                   )
                OR dataset.acquisition_root_run_id IN (
                       SELECT acquisition_root_run_id FROM new_rows
                   );
        ELSIF TG_TABLE_NAME = '{_BULK_CHECKPOINT}' AND TG_OP = 'DELETE' THEN
            SELECT pg_catalog.array_agg(
                       dataset.dataset_id ORDER BY dataset.dataset_id
                   )
              INTO affected_dataset_ids
              FROM {dataset_ref} AS dataset
             WHERE dataset.dataset_id IN (
                       SELECT dataset_id FROM old_rows
                   )
                OR dataset.acquisition_root_run_id IN (
                       SELECT acquisition_root_run_id FROM old_rows
                   );
        ELSIF TG_TABLE_NAME = '{_BULK_CHECKPOINT}' AND TG_OP = 'UPDATE' THEN
            SELECT pg_catalog.array_agg(
                       dataset.dataset_id ORDER BY dataset.dataset_id
                   )
              INTO affected_dataset_ids
              FROM {dataset_ref} AS dataset
             WHERE dataset.dataset_id IN (
                       SELECT dataset_id FROM old_rows
                       UNION SELECT dataset_id FROM new_rows
                   )
                OR dataset.acquisition_root_run_id IN (
                       SELECT acquisition_root_run_id FROM old_rows
                       UNION SELECT acquisition_root_run_id FROM new_rows
                   );
        ELSIF TG_OP = 'INSERT' THEN
            SELECT pg_catalog.array_agg(dataset_id ORDER BY dataset_id)
              INTO affected_dataset_ids
              FROM (SELECT DISTINCT dataset_id FROM new_rows) AS affected;
        ELSIF TG_OP = 'DELETE' THEN
            SELECT pg_catalog.array_agg(dataset_id ORDER BY dataset_id)
              INTO affected_dataset_ids
              FROM (SELECT DISTINCT dataset_id FROM old_rows) AS affected;
        ELSIF TG_OP = 'UPDATE' THEN
            SELECT pg_catalog.array_agg(dataset_id ORDER BY dataset_id)
              INTO affected_dataset_ids
              FROM (
                    SELECT dataset_id FROM old_rows
                    UNION SELECT dataset_id FROM new_rows
                   ) AS affected;
        ELSE
            RAISE EXCEPTION
                'provider_directory_subset_abandonment_child_action_invalid'
                USING ERRCODE = '55000';
        END IF;
        PERFORM dataset.dataset_id
          FROM {dataset_ref} AS dataset
         WHERE dataset.dataset_id = ANY(
                   COALESCE(affected_dataset_ids, ARRAY[]::text[])
               )
         ORDER BY dataset.dataset_id
         FOR SHARE OF dataset;
        SELECT count(*) INTO immutable_parent_count
          FROM {dataset_ref} AS dataset
         WHERE dataset.dataset_id = ANY(
                   COALESCE(affected_dataset_ids, ARRAY[]::text[])
               )
           AND dataset.status = '{_STATUS}';
        IF immutable_parent_count <> 0 THEN
            RAISE EXCEPTION
                'provider_directory_subset_abandonment_child_immutable'
                USING ERRCODE = '55000';
        END IF;
        RETURN NULL;
    END;
    $function$;
    """


def _checkpoint_guard_sql(schema: str) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, _DATASET)
    checkpoint_ref = subset._qf(schema, _CHECKPOINT)
    valid_ref = subset._qf(schema, _VALID)
    guard_ref = subset._qf(schema, _CHECKPOINT_GUARD)
    return f"""
    CREATE OR REPLACE FUNCTION {guard_ref}()
    RETURNS trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog
    AS $function$
    DECLARE
        parent_endpoint_id text;
        parent_status text;
        target_dataset_id text;
    BEGIN
        IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
                SELECT 1 FROM {checkpoint_ref} AS checkpoint
                 WHERE checkpoint.state = '{_STATUS}'
            ) OR EXISTS (
                SELECT 1 FROM {dataset_ref} AS dataset
                 WHERE dataset.status = '{_STATUS}'
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_checkpoint_truncate_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END IF;
        IF TG_WHEN = 'AFTER' THEN
            target_dataset_id = CASE
                WHEN TG_OP = 'DELETE' THEN OLD.dataset_id ELSE NEW.dataset_id
            END;
            SELECT dataset.status INTO parent_status
              FROM {dataset_ref} AS dataset
             WHERE dataset.dataset_id = target_dataset_id;
            IF parent_status = '{_STATUS}'
               OR (TG_OP <> 'DELETE' AND NEW.state = '{_STATUS}')
               OR (TG_OP = 'DELETE' AND OLD.state = '{_STATUS}') THEN
                IF TG_OP = 'DELETE'
                   OR NEW.state <> '{_STATUS}'
                   OR {valid_ref}(target_dataset_id) IS DISTINCT FROM TRUE THEN
                    RAISE EXCEPTION
                        'provider_directory_subset_abandonment_checkpoint_invalid'
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            RETURN NULL;
        END IF;
        IF TG_OP = 'INSERT' THEN
            IF NEW.state = '{_STATUS}' THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_checkpoint_insert_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
            IF OLD.state = '{_STATUS}' THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_checkpoint_delete_forbidden'
                    USING ERRCODE = '55000';
            END IF;
            RETURN OLD;
        END IF;
        IF OLD.state = '{_STATUS}' THEN
            IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_checkpoint_immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END IF;
        IF NEW.state = '{_STATUS}' THEN
            SELECT dataset.endpoint_id INTO parent_endpoint_id
              FROM {dataset_ref} AS dataset
             WHERE dataset.dataset_id = NEW.dataset_id;
            IF pg_catalog.pg_try_advisory_xact_lock(
                   pg_catalog.hashtextextended(
                       'provider-directory-pagination:'
                           || NEW.canonical_api_base,
                       0
                   )
               ) IS NOT TRUE
               OR parent_endpoint_id IS NULL
               OR pg_catalog.pg_try_advisory_xact_lock(
                   pg_catalog.hashtextextended(parent_endpoint_id, 0)
               ) IS NOT TRUE
               OR OLD.state NOT IN ('active', 'complete')
               OR NEW.updated_at IS DISTINCT FROM transaction_timestamp()
               OR NEW.completed_at IS DISTINCT FROM
                    COALESCE(OLD.completed_at, transaction_timestamp())
               OR to_jsonb(NEW) - ARRAY[
                    'state', 'updated_at', 'completed_at'
                  ]::text[]
                  IS DISTINCT FROM
                  to_jsonb(OLD) - ARRAY[
                    'state', 'updated_at', 'completed_at'
                  ]::text[] THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_checkpoint_transition_invalid'
                    USING ERRCODE = '55000';
            END IF;
        END IF;
        RETURN NEW;
    END;
    $function$;
    """


def _create_triggers(schema: str) -> None:
    subset = _subset()
    dataset_ref = subset._qf(schema, _DATASET)
    resource_ref = subset._qf(schema, _RESOURCE)
    proof_ref = subset._qf(schema, _PROOF_SHARD)
    bulk_ref = subset._qf(schema, _BULK_CHECKPOINT)
    checkpoint_ref = subset._qf(schema, _CHECKPOINT)
    dataset_guard = subset._qf(schema, _DATASET_GUARD)
    child_guard = subset._qf(schema, _CHILD_GUARD)
    checkpoint_guard = subset._qf(schema, _CHECKPOINT_GUARD)
    statements = (
        f"CREATE TRIGGER {subset._q(_DATASET_ROW_TRIGGER)} BEFORE INSERT OR UPDATE OR DELETE ON {dataset_ref} FOR EACH ROW EXECUTE FUNCTION {dataset_guard}();",
        f"CREATE CONSTRAINT TRIGGER {subset._q(_DATASET_CONSTRAINT)} AFTER UPDATE ON {dataset_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {dataset_guard}();",
        f"CREATE TRIGGER {subset._q(_DATASET_TRUNCATE)} BEFORE TRUNCATE ON {dataset_ref} FOR EACH STATEMENT EXECUTE FUNCTION {dataset_guard}();",
        f"CREATE TRIGGER {subset._q(_RESOURCE_INSERT)} AFTER INSERT ON {resource_ref} REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_RESOURCE_UPDATE)} AFTER UPDATE ON {resource_ref} REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_RESOURCE_DELETE)} AFTER DELETE ON {resource_ref} REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_PROOF_INSERT)} AFTER INSERT ON {proof_ref} REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_PROOF_UPDATE)} AFTER UPDATE ON {proof_ref} REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_PROOF_DELETE)} AFTER DELETE ON {proof_ref} REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_PROOF_TRUNCATE)} BEFORE TRUNCATE ON {proof_ref} FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_BULK_INSERT)} AFTER INSERT ON {bulk_ref} REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_BULK_UPDATE)} AFTER UPDATE ON {bulk_ref} REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_BULK_DELETE)} AFTER DELETE ON {bulk_ref} REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_BULK_TRUNCATE)} BEFORE TRUNCATE ON {bulk_ref} FOR EACH STATEMENT EXECUTE FUNCTION {child_guard}();",
        f"CREATE TRIGGER {subset._q(_CHECKPOINT_ROW_TRIGGER)} BEFORE INSERT OR UPDATE OR DELETE ON {checkpoint_ref} FOR EACH ROW EXECUTE FUNCTION {checkpoint_guard}();",
        f"CREATE CONSTRAINT TRIGGER {subset._q(_CHECKPOINT_CONSTRAINT)} AFTER INSERT OR UPDATE OR DELETE ON {checkpoint_ref} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {checkpoint_guard}();",
        f"CREATE TRIGGER {subset._q(_CHECKPOINT_TRUNCATE)} BEFORE TRUNCATE ON {checkpoint_ref} FOR EACH STATEMENT EXECUTE FUNCTION {checkpoint_guard}();",
    )
    for statement in statements:
        op.execute(statement)
    for relation_ref, trigger_names in (
        (dataset_ref, (_DATASET_ROW_TRIGGER, _DATASET_CONSTRAINT, _DATASET_TRUNCATE)),
        (resource_ref, (_RESOURCE_INSERT, _RESOURCE_UPDATE, _RESOURCE_DELETE)),
        (proof_ref, (_PROOF_INSERT, _PROOF_UPDATE, _PROOF_DELETE, _PROOF_TRUNCATE)),
        (bulk_ref, (_BULK_INSERT, _BULK_UPDATE, _BULK_DELETE, _BULK_TRUNCATE)),
        (
            checkpoint_ref,
            (_CHECKPOINT_ROW_TRIGGER, _CHECKPOINT_CONSTRAINT, _CHECKPOINT_TRUNCATE),
        ),
    ):
        for trigger_name in trigger_names:
            op.execute(
                f"ALTER TABLE {relation_ref} ENABLE ALWAYS TRIGGER {subset._q(trigger_name)};"
            )


def _preflight_sql(schema: str, *, expect_installed: bool) -> str:
    subset = _subset()
    expected_function_count = 4 if expect_installed else 0
    expected_trigger_count = 17 if expect_installed else 0
    dataset_ref = subset._qf(schema, _DATASET)
    resource_ref = subset._qf(schema, _RESOURCE)
    proof_ref = subset._qf(schema, _PROOF_SHARD)
    bulk_ref = subset._qf(schema, _BULK_CHECKPOINT)
    checkpoint_ref = subset._qf(schema, _CHECKPOINT)
    valid_ref = subset._qf(schema, _VALID)
    dataset_guard_ref = subset._qf(schema, _DATASET_GUARD)
    child_guard_ref = subset._qf(schema, _CHILD_GUARD)
    checkpoint_guard_ref = subset._qf(schema, _CHECKPOINT_GUARD)
    function_specs = (
        (valid_ref + "(text)", 1, "pg_catalog.bool", "sql", "s"),
        (dataset_guard_ref + "()", 0, "pg_catalog.trigger", "plpgsql", "v"),
        (child_guard_ref + "()", 0, "pg_catalog.trigger", "plpgsql", "v"),
        (checkpoint_guard_ref + "()", 0, "pg_catalog.trigger", "plpgsql", "v"),
    )
    function_values = ",\n".join(
        "("
        + ", ".join(
            (
                f"pg_catalog.to_regprocedure({subset._ql(signature)})",
                str(argument_count),
                f"{subset._ql(return_type)}::regtype",
                subset._ql(language_name),
                subset._ql(volatility),
            )
        )
        + ")"
        for signature, argument_count, return_type, language_name, volatility in function_specs
    )
    trigger_specs = (
        (
            dataset_ref,
            _DATASET_ROW_TRIGGER,
            31,
            dataset_guard_ref,
            False,
            False,
            False,
            None,
            None,
        ),
        (
            dataset_ref,
            _DATASET_CONSTRAINT,
            17,
            dataset_guard_ref,
            True,
            True,
            True,
            None,
            None,
        ),
        (
            dataset_ref,
            _DATASET_TRUNCATE,
            34,
            dataset_guard_ref,
            False,
            False,
            False,
            None,
            None,
        ),
        (
            resource_ref,
            _RESOURCE_INSERT,
            4,
            child_guard_ref,
            False,
            False,
            False,
            None,
            "new_rows",
        ),
        (
            resource_ref,
            _RESOURCE_UPDATE,
            16,
            child_guard_ref,
            False,
            False,
            False,
            "old_rows",
            "new_rows",
        ),
        (
            resource_ref,
            _RESOURCE_DELETE,
            8,
            child_guard_ref,
            False,
            False,
            False,
            "old_rows",
            None,
        ),
        (
            proof_ref,
            _PROOF_INSERT,
            4,
            child_guard_ref,
            False,
            False,
            False,
            None,
            "new_rows",
        ),
        (
            proof_ref,
            _PROOF_UPDATE,
            16,
            child_guard_ref,
            False,
            False,
            False,
            "old_rows",
            "new_rows",
        ),
        (
            proof_ref,
            _PROOF_DELETE,
            8,
            child_guard_ref,
            False,
            False,
            False,
            "old_rows",
            None,
        ),
        (
            proof_ref,
            _PROOF_TRUNCATE,
            34,
            child_guard_ref,
            False,
            False,
            False,
            None,
            None,
        ),
        (
            bulk_ref,
            _BULK_INSERT,
            4,
            child_guard_ref,
            False,
            False,
            False,
            None,
            "new_rows",
        ),
        (
            bulk_ref,
            _BULK_UPDATE,
            16,
            child_guard_ref,
            False,
            False,
            False,
            "old_rows",
            "new_rows",
        ),
        (
            bulk_ref,
            _BULK_DELETE,
            8,
            child_guard_ref,
            False,
            False,
            False,
            "old_rows",
            None,
        ),
        (
            bulk_ref,
            _BULK_TRUNCATE,
            34,
            child_guard_ref,
            False,
            False,
            False,
            None,
            None,
        ),
        (
            checkpoint_ref,
            _CHECKPOINT_ROW_TRIGGER,
            31,
            checkpoint_guard_ref,
            False,
            False,
            False,
            None,
            None,
        ),
        (
            checkpoint_ref,
            _CHECKPOINT_CONSTRAINT,
            29,
            checkpoint_guard_ref,
            True,
            True,
            True,
            None,
            None,
        ),
        (
            checkpoint_ref,
            _CHECKPOINT_TRUNCATE,
            34,
            checkpoint_guard_ref,
            False,
            False,
            False,
            None,
            None,
        ),
    )
    trigger_values = ",\n".join(
        "("
        + ", ".join(
            (
                f"{subset._ql(relation_ref)}::regclass",
                subset._ql(trigger_name),
                str(trigger_type),
                f"pg_catalog.to_regprocedure({subset._ql(function_ref + '()')})",
                str(is_constraint).lower(),
                str(is_deferrable).lower(),
                str(is_initially_deferred).lower(),
                "NULL" if old_table is None else subset._ql(old_table),
                "NULL" if new_table is None else subset._ql(new_table),
            )
        )
        + ")"
        for (
            relation_ref,
            trigger_name,
            trigger_type,
            function_ref,
            is_constraint,
            is_deferrable,
            is_initially_deferred,
            old_table,
            new_table,
        ) in trigger_specs
    )
    return f"""
    DO $migration$
    DECLARE
        observed_function_count bigint;
        observed_trigger_count bigint;
    BEGIN
        SELECT count(*) INTO observed_function_count
          FROM (VALUES {function_values}) AS expected(
               function_oid,
               argument_count,
               return_type,
               language_name,
               volatility
          )
          JOIN pg_catalog.pg_proc AS function_row
            ON function_row.oid = expected.function_oid
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS language_row
            ON language_row.oid = function_row.prolang
         WHERE namespace_row.nspname = {subset._ql(schema)}
           AND function_row.pronargs = expected.argument_count
           AND function_row.prorettype = expected.return_type
           AND language_row.lanname = expected.language_name
           AND function_row.provolatile = expected.volatility
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
        SELECT count(*) INTO observed_trigger_count
          FROM (VALUES {trigger_values}) AS expected(
               relation_oid,
               trigger_name,
               trigger_type,
               function_oid,
               is_constraint,
               is_deferrable,
               is_initially_deferred,
               old_table,
               new_table
          )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = expected.relation_oid
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgtype = expected.trigger_type
           AND trigger_row.tgfoid = expected.function_oid
           AND (trigger_row.tgconstraint <> 0) = expected.is_constraint
           AND trigger_row.tgdeferrable = expected.is_deferrable
           AND trigger_row.tginitdeferred = expected.is_initially_deferred
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND pg_catalog.octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NOT DISTINCT FROM expected.old_table
           AND trigger_row.tgnewtable IS NOT DISTINCT FROM expected.new_table;
        IF observed_function_count <> {expected_function_count}
           OR observed_trigger_count <> {expected_trigger_count} THEN
            RAISE EXCEPTION
                'provider_directory_subset_abandonment_shape_changed'
                USING ERRCODE = '55000';
        END IF;
        IF NOT {str(expect_installed).lower()} AND (
            EXISTS (
                SELECT 1 FROM {dataset_ref} AS dataset
                 WHERE dataset.status = '{_STATUS}'
                    OR dataset.publication_metadata_json::jsonb ? '{_MARKER}'
            ) OR EXISTS (
                SELECT 1 FROM {checkpoint_ref} AS checkpoint
                 WHERE checkpoint.state = '{_STATUS}'
            )
        ) THEN
            RAISE EXCEPTION
                'provider_directory_subset_abandonment_adoption_forbidden'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _revoke_execute(schema: str) -> None:
    subset = _subset()
    signatures = (
        subset._qf(schema, _VALID) + "(text)",
        subset._qf(schema, _DATASET_GUARD) + "()",
        subset._qf(schema, _CHILD_GUARD) + "()",
        subset._qf(schema, _CHECKPOINT_GUARD) + "()",
    )
    for signature in signatures:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC;")


def _drop_objects(schema: str) -> None:
    subset = _subset()
    relation_by_trigger = {
        _DATASET_ROW_TRIGGER: _DATASET,
        _DATASET_CONSTRAINT: _DATASET,
        _DATASET_TRUNCATE: _DATASET,
        _RESOURCE_INSERT: _RESOURCE,
        _RESOURCE_UPDATE: _RESOURCE,
        _RESOURCE_DELETE: _RESOURCE,
        _PROOF_INSERT: _PROOF_SHARD,
        _PROOF_UPDATE: _PROOF_SHARD,
        _PROOF_DELETE: _PROOF_SHARD,
        _PROOF_TRUNCATE: _PROOF_SHARD,
        _BULK_INSERT: _BULK_CHECKPOINT,
        _BULK_UPDATE: _BULK_CHECKPOINT,
        _BULK_DELETE: _BULK_CHECKPOINT,
        _BULK_TRUNCATE: _BULK_CHECKPOINT,
        _CHECKPOINT_ROW_TRIGGER: _CHECKPOINT,
        _CHECKPOINT_CONSTRAINT: _CHECKPOINT,
        _CHECKPOINT_TRUNCATE: _CHECKPOINT,
    }
    for trigger_name, relation_name in reversed(tuple(relation_by_trigger.items())):
        op.execute(
            f"DROP TRIGGER {subset._q(trigger_name)} ON {subset._qf(schema, relation_name)};"
        )
    op.execute(f"DROP FUNCTION {subset._qf(schema, _CHECKPOINT_GUARD)}();")
    op.execute(f"DROP FUNCTION {subset._qf(schema, _CHILD_GUARD)}();")
    op.execute(f"DROP FUNCTION {subset._qf(schema, _DATASET_GUARD)}();")
    op.execute(f"DROP FUNCTION {subset._qf(schema, _VALID)}(text);")


def upgrade() -> None:
    subset = _subset()
    schema = subset._schema()
    guarded_relations = (
        _DATASET,
        _RESOURCE,
        _SOURCE,
        _PROOF_SHARD,
        _CHECKPOINT,
        _BULK_CHECKPOINT,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(subset._qf(schema, name) for name in guarded_relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    for fence_sql in _shape_fence_sqls(schema):
        op.execute(fence_sql)
    op.execute(_preflight_sql(schema, expect_installed=False))
    op.execute(_valid_function_sql(schema))
    op.execute(_dataset_guard_sql(schema))
    op.execute(_child_guard_sql(schema))
    op.execute(_checkpoint_guard_sql(schema))
    _revoke_execute(schema)
    _create_triggers(schema)
    op.execute(_preflight_sql(schema, expect_installed=True))
    for fence_sql in _shape_fence_sqls(schema):
        op.execute(fence_sql)


def downgrade() -> None:
    subset = _subset()
    schema = subset._schema()
    guarded_relations = (
        _DATASET,
        _RESOURCE,
        _SOURCE,
        _PROOF_SHARD,
        _CHECKPOINT,
        _BULK_CHECKPOINT,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(subset._qf(schema, name) for name in guarded_relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    for fence_sql in _shape_fence_sqls(schema):
        op.execute(fence_sql)
    op.execute(_preflight_sql(schema, expect_installed=True))
    dataset_ref = subset._qf(schema, _DATASET)
    checkpoint_ref = subset._qf(schema, _CHECKPOINT)
    op.execute(f"""
        DO $migration$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {dataset_ref} AS dataset
                 WHERE dataset.status = '{_STATUS}'
                    OR dataset.publication_metadata_json::jsonb ? '{_MARKER}'
            ) OR EXISTS (
                SELECT 1 FROM {checkpoint_ref} AS checkpoint
                 WHERE checkpoint.state = '{_STATUS}'
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_subset_abandonment_downgrade_blocked'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $migration$;
        """)
    _drop_objects(schema)
    op.execute(_preflight_sql(schema, expect_installed=False))
    for fence_sql in _shape_fence_sqls(schema):
        op.execute(fence_sql)
