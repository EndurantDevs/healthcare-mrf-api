# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Static PostgreSQL contracts for retained UHC source-local publication."""

PUBLISH_VALIDATED_UHC_DATASET_SQL = """
UPDATE __ENDPOINT_DATASET_REF__
   SET status = :published_status,
       is_current = true,
       published_at = now(),
       superseded_at = NULL
 WHERE dataset_id = :dataset_id
   AND endpoint_id = :endpoint_id
   AND COALESCE(acquisition_root_run_id, import_run_id)
       IS NOT DISTINCT FROM :evidence_run_id
   AND previous_dataset_id IS NOT DISTINCT FROM :incumbent_dataset_id
   AND dataset_hash IS NOT DISTINCT FROM :dataset_hash
   AND status = :validated_status
   AND is_current = false
   AND validated_at IS NOT NULL
   AND superseded_at IS NULL
   AND (
        NOT (
            COALESCE(publication_metadata_json::jsonb, '{}'::jsonb)
                ? :uhc_publication_key
            OR COALESCE(
                publication_metadata_json::jsonb -> 'source_ids',
                '[]'::jsonb
            ) @> jsonb_build_array(CAST(:uhc_source_id AS text))
        )
        OR (
            publication_metadata_json::jsonb
                -> :uhc_publication_key ->> 'contract_id'
                = :uhc_publication_contract_id
            AND publication_metadata_json::jsonb
                -> :uhc_publication_key -> 'complete'
                = 'true'::jsonb
            AND publication_metadata_json::jsonb
                -> :uhc_publication_key ->> 'source_id'
                = :uhc_source_id
            AND publication_metadata_json::jsonb
                -> :uhc_publication_key ->> 'dataset_id'
                = dataset_id
            AND publication_metadata_json::jsonb
                -> :uhc_publication_key ->> 'acquisition_root_run_id'
                = acquisition_root_run_id
            AND publication_metadata_json::jsonb
                -> :uhc_publication_key ->> 'semantic_contract_id'
                = :uhc_semantic_contract_id
            AND publication_metadata_json::jsonb -> 'source_ids'
                = jsonb_build_array(CAST(:uhc_source_id AS text))
            AND publication_metadata_json::jsonb -> 'selected_resources'
                = CAST(:uhc_selected_resources AS jsonb)
            AND publication_metadata_json::jsonb
                -> :uhc_summary_input_key -> 'complete'
                = 'true'::jsonb
            AND publication_metadata_json::jsonb
                -> :uhc_summary_input_key ->> 'contract_id'
                = :uhc_summary_input_contract_id
            AND publication_metadata_json::jsonb
                -> :uhc_summary_input_key ->> 'source_id'
                = :uhc_source_id
            AND publication_metadata_json::jsonb
                -> :uhc_summary_input_key ->> 'semantic_contract_id'
                = :uhc_semantic_contract_id
            AND publication_metadata_json::jsonb
                -> :uhc_summary_input_key ->> 'input_sha256'
                = publication_metadata_json::jsonb
                    -> :uhc_publication_key ->> 'summary_input_sha256'
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'contract_id'
                = :uhc_content_proof_contract_id
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key -> 'complete'
                = 'true'::jsonb
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'source_id'
                = :uhc_source_id
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'dataset_id'
                = dataset_id
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'endpoint_id'
                = endpoint_id
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'acquisition_root_run_id'
                = acquisition_root_run_id
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'dataset_hash'
                = dataset_hash
            AND CAST(
                publication_metadata_json::jsonb
                    -> :uhc_content_proof_key ->> 'resource_count'
                AS bigint
            ) = resource_count
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'proof_sha256'
                ~ '^[0-9a-f]{64}$'
            AND publication_metadata_json::jsonb
                -> :uhc_content_proof_key ->> 'materialization_sha256'
                ~ '^[0-9a-f]{64}$'
            AND CAST(
                publication_metadata_json::jsonb
                    -> :uhc_content_proof_key ->> 'shard_count'
                AS integer
            ) > 0
            AND publication_metadata_json::jsonb
                -> :outcome_key -> 'complete'
                = 'true'::jsonb
            AND CAST(
                publication_metadata_json::jsonb
                    -> :outcome_key ->> 'version'
                AS integer
            ) = 1
            AND publication_metadata_json::jsonb
                -> :outcome_key ->> 'dataset_id'
                = dataset_id
            AND publication_metadata_json::jsonb
                -> :outcome_key ->> 'endpoint_id'
                = endpoint_id
            AND publication_metadata_json::jsonb
                -> :outcome_key ->> 'acquisition_root_run_id'
                = acquisition_root_run_id
            AND publication_metadata_json::jsonb
                -> :outcome_key ->> 'dataset_hash'
                = dataset_hash
            AND CAST(
                publication_metadata_json::jsonb
                    -> :outcome_key ->> 'resource_count'
                AS bigint
            ) = resource_count
            AND publication_metadata_json::jsonb
                -> :source_summary_key ->> 'dataset_id'
                = dataset_id
            AND publication_metadata_json::jsonb
                -> :source_summary_key ->> 'endpoint_id'
                = endpoint_id
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'complete'
                = 'true'::jsonb
            AND publication_metadata_json::jsonb
                -> :source_summary_key ->> 'contract_id'
                = :source_summary_contract_id
            AND CAST(
                publication_metadata_json::jsonb
                    -> :source_summary_key ->> 'contract_version'
                AS integer
            ) = :source_summary_contract_version
            AND publication_metadata_json::jsonb
                -> :source_summary_key ->> 'acquisition_root_run_id'
                = acquisition_root_run_id
            AND publication_metadata_json::jsonb
                -> :source_summary_key ->> 'dataset_hash'
                = dataset_hash
            AND publication_metadata_json::jsonb
                -> :source_summary_key ->> 'semantic_contract_id'
                = :uhc_semantic_contract_id
            AND CAST(
                publication_metadata_json::jsonb
                    -> :source_summary_key ->> 'total_resources'
                AS bigint
            ) = resource_count
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'resource_counts'
                = publication_metadata_json::jsonb
                    -> :outcome_key -> 'resource_counts'
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'resource_counts'
                = publication_metadata_json::jsonb
                    -> :uhc_content_proof_key -> 'resource_counts'
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'resource_hashes'
                = publication_metadata_json::jsonb
                    -> :uhc_content_proof_key -> 'resource_hashes'
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'source_ids'
                = publication_metadata_json::jsonb
                    -> :outcome_key -> 'source_ids'
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'source_ids'
                = publication_metadata_json::jsonb -> 'source_ids'
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'selected_resources'
                = publication_metadata_json::jsonb
                    -> :outcome_key -> 'selected_resources'
            AND publication_metadata_json::jsonb
                -> :source_summary_key -> 'selected_resources'
                = publication_metadata_json::jsonb -> 'selected_resources'
        )
   );
"""

__all__ = ["PUBLISH_VALIDATED_UHC_DATASET_SQL"]
