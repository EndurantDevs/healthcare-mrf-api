-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- A processing epoch authenticates the writer; it never replaces provenance.
EXISTS (
    SELECT 1 FROM __CONTROL__.custom_import_build_continuation continuation
    JOIN __CONTROL__.custom_import_snapshot_family snapshot
        ON snapshot.family_id=continuation.snapshot_family_id
    WHERE continuation.continuation_id=l.continuation_id
        AND current_setting('custom_import.continuation_id',true)=l.continuation_id::text
        AND current_setting('custom_import.continuation_nonce_sha256',true)=encode(l.token_sha256,'hex')
        AND l.continuation_started_at IS NOT NULL AND isfinite(l.continuation_started_at)
        AND l.continuation_started_at>=continuation.created_at
        AND l.continuation_started_at<continuation.launch_expires_at
        AND l.continuation_started_at<=clock_timestamp()
        AND l.expires_at IS NOT NULL AND isfinite(l.expires_at) AND l.expires_at>clock_timestamp()
        AND e.state='running' AND l.fence=b.producing_fence
        AND continuation.contract='custom-import/build-continuation/v1'
        AND continuation.processing_deadline_removed
        AND continuation.nonce_sha256=l.token_sha256
        AND b.phase IN ('graph','output','verifying','verified')
        AND b.source_frozen_at IS NOT NULL AND b.plan_complete_at IS NOT NULL
        AND ROW(continuation.build_id,continuation.dataset_id,continuation.definition_revision_id,
            continuation.schema_revision_id,continuation.execution_id,continuation.capture_bundle_id,
            continuation.producer_fence,continuation.producer_token_sha256,
            continuation.request_identity_sha256,continuation.base_generation_id,
            continuation.base_pointer_version,continuation.original_deadline_at)
            IS NOT DISTINCT FROM ROW(b.build_id,b.dataset_id,b.definition_revision_id,
                b.schema_revision_id,b.execution_id,b.capture_bundle_id,b.producing_fence,
                b.producing_token_sha256,b.request_identity_sha256,b.base_generation_id,
                b.base_pointer_version,b.build_deadline_at)
        AND ROW(e.execution_id,e.dataset_id,e.definition_revision_id,e.schema_revision_id,
            e.capture_bundle_id,e.request_identity_sha256)
            IS NOT DISTINCT FROM ROW(b.execution_id,b.dataset_id,b.definition_revision_id,
                b.schema_revision_id,b.capture_bundle_id,b.request_identity_sha256)
        AND ROW(snapshot.dataset_id,snapshot.definition_revision_id,snapshot.schema_revision_id,
            snapshot.execution_id,snapshot.capture_bundle_id,snapshot.producing_fence,
            snapshot.producing_token_sha256)
            IS NOT DISTINCT FROM ROW(b.dataset_id,b.definition_revision_id,b.schema_revision_id,
                b.execution_id,b.capture_bundle_id,b.producing_fence,b.producing_token_sha256)
        AND (snapshot.generation_id IS NULL OR snapshot.generation_id=b.generation_id)
)
