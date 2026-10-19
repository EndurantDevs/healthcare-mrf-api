-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
CREATE TABLE __CONTROL__.custom_import_build_continuation (
    continuation_id BIGSERIAL PRIMARY KEY,
    contract VARCHAR(48) NOT NULL,
    authorization_id VARCHAR(64) NOT NULL,
    dataset_id BIGINT NOT NULL,
    definition_revision_id BIGINT NOT NULL,
    schema_revision_id BIGINT NOT NULL,
    execution_id BIGINT NOT NULL REFERENCES __CONTROL__.custom_import_execution(execution_id) ON DELETE RESTRICT,
    build_id BIGINT NOT NULL REFERENCES __CONTROL__.custom_import_build_attempt(build_id) ON DELETE RESTRICT,
    snapshot_family_id BIGINT NOT NULL REFERENCES __CONTROL__.custom_import_snapshot_family(family_id) ON DELETE RESTRICT,
    capture_bundle_id BIGINT NOT NULL,
    producer_fence BIGINT NOT NULL,
    producer_token_sha256 BYTEA NOT NULL,
    request_identity_sha256 BYTEA NOT NULL,
    expected_authority_id BIGINT REFERENCES __CONTROL__.custom_import_build_continuation(continuation_id) ON DELETE RESTRICT,
    base_generation_id BIGINT,
    base_pointer_version BIGINT NOT NULL,
    worker_identity_sha256 BYTEA NOT NULL,
    predecessor_identity_sha256 BYTEA NOT NULL,
    nonce_sha256 BYTEA NOT NULL,
    processing_deadline_removed BOOLEAN NOT NULL,
    original_deadline_at TIMESTAMPTZ NOT NULL,
    launch_expires_at TIMESTAMPTZ NOT NULL,
    canonical_authorization TEXT NOT NULL,
    authorization_sha256 BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT transaction_timestamp() NOT NULL,
    CONSTRAINT custom_import_continuation_authorization_key UNIQUE(authorization_id),
    CONSTRAINT custom_import_continuation_digest_key UNIQUE(authorization_sha256),
    CONSTRAINT custom_import_continuation_nonce_key UNIQUE(nonce_sha256),
    CONSTRAINT custom_import_continuation_shape CHECK (
        contract='custom-import/build-continuation/v1' AND authorization_id ~ '^[0-9a-f]{64}$'
        AND producer_fence>0 AND processing_deadline_removed
        AND ((base_generation_id IS NULL AND base_pointer_version=0)
            OR (base_generation_id>0 AND base_pointer_version>0))
        AND (expected_authority_id IS NULL OR expected_authority_id<>continuation_id)
        AND octet_length(canonical_authorization) BETWEEN 1 AND 16384
        AND octet_length(producer_token_sha256)=32 AND octet_length(request_identity_sha256)=32
        AND octet_length(worker_identity_sha256)=32 AND octet_length(predecessor_identity_sha256)=32
        AND octet_length(nonce_sha256)=32 AND octet_length(authorization_sha256)=32)
);
-- statement boundary --
CREATE INDEX custom_import_continuation_build_idx
    ON __CONTROL__.custom_import_build_continuation(build_id,continuation_id);
