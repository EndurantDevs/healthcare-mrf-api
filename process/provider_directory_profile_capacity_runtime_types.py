# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Runtime capacity configuration types and deployment contract constants."""

from __future__ import annotations

from dataclasses import dataclass

from process import provider_directory_profile_capacity as capacity
from process.provider_directory_profile_capacity_trust import (
    CAPACITY_TRUST_CONTRACT_ID,
)

CAPACITY_LIMITS_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-limits.v2"
)
CAPACITY_LIMITS_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_CAPACITY_LIMITS_JSON"
)
CAPACITY_TRUST_ENV = (
    "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_CAPACITY_TRUST_JSON"
)

_RELATION_NAMES = (
    "artifact_scope",
    "evidence_stage",
    "affected_npi_stage",
    "profile_stage",
    "evidence_target",
    "profile_target",
)
_RELATION_FIELDS = frozenset(
    {
        "relation_name",
        "max_scratch_bytes",
        "max_target_growth_bytes",
        "max_deleted_logical_bytes",
        "max_temp_bytes",
        "max_wal_bytes",
    }
)
_LIMIT_FIELDS = frozenset(
    {
        "contract_id",
        "artifact_scope_batch_size",
        "pool_reserve_connections",
        "work_mem_bytes",
        "maintenance_work_mem_bytes",
        "temp_file_limit_bytes",
        "max_build_seconds",
        "statement_timeout_ms",
        "lock_timeout_ms",
        "minimum_remaining_bytes",
        "max_artifact_scope_rows",
        "max_evidence_rows",
        "max_affected_npis",
        "max_profile_rows",
        "relation_byte_caps",
    }
)
_POSITIVE_LIMIT_FIELDS = _LIMIT_FIELDS - {
    "contract_id",
    "relation_byte_caps",
}
_TRUST_FIELDS = frozenset(
    {
        "contract_id",
        "signature_algorithm",
        "environment_id",
        "attestor_id",
        "active_key_id",
        "keys",
        "database_system_identifier",
        "database_oid",
        "database_name",
        "tablespaces",
        "volumes",
    }
)
_TRUST_TABLESPACE_FIELDS = frozenset(
    {"tablespace_name", "tablespace_oid", "usage", "volume_digest"}
)
_TRUST_VOLUME_FIELDS = frozenset({"volume_class", "volume_digest"})
_TRUST_KEY_FIELDS = frozenset(
    {
        "key_id",
        "public_key_hex",
        "attestor_release_digest",
        "status",
        "retired_at",
        "verify_until",
    }
)
_MAX_SIGNED_BIGINT = (1 << 63) - 1


class ProviderDirectoryProfileCapacityConfigurationError(ValueError):
    """Report invalid or missing deployment capacity limits."""


@dataclass(frozen=True)
class ProviderDirectoryProfileCapacityLimits:
    """Deployment hard ceilings that still require a matching signed lease."""

    artifact_scope_batch_size: int
    pool_reserve_connections: int
    work_mem_bytes: int
    maintenance_work_mem_bytes: int
    temp_file_limit_bytes: int
    max_build_seconds: int
    statement_timeout_ms: int
    lock_timeout_ms: int
    minimum_remaining_bytes: int
    max_artifact_scope_rows: int
    max_evidence_rows: int
    max_affected_npis: int
    max_profile_rows: int
    relation_byte_caps: tuple[
        capacity.ProviderDirectoryProfileRelationByteCaps,
        ...,
    ]


@dataclass(frozen=True)
class ProviderDirectoryProfileCapacityGeometryInputs:
    """Build and PostgreSQL identity observed before creating scratch data."""

    selection_proof_id: str
    profile_input_digest: str
    profile_schema_version: int
    profile_strategy_version: str
    executable_plan_hash: str
    profile_as_of: str
    current_source_vector_hash: str
    desired_source_vector_hash: str
    current_context_vector_hash: str
    desired_context_vector_hash: str
    sql_contract_digest: str
    database_system_identifier: str
    database_oid: int
    database_name: str
    tablespace_oid: int
    tablespace_name: str
    evidence_target_oid: int
    profile_target_oid: int
    postgres_server_version_num: int
    postgres_block_size_bytes: int
    postgres_wal_block_size_bytes: int
    postgres_wal_segment_size_bytes: int
    postgres_full_page_writes: bool
    postgres_wal_compression: str
    postgres_wal_level: str
    postgres_wal_log_hints: bool
    postgres_data_checksums: bool
    postgres_default_toast_compression: str
    postgres_checkpoint_timeout_seconds: int
    postgres_max_wal_size_bytes: int
    evidence_target_storage_fingerprint: str
    profile_target_storage_fingerprint: str
    build_checkpoint_oid: int
    serving_generation_oid: int
    delta_receipt_oid: int
    import_run_oid: int
    capacity_consumption_oid: int
    build_checkpoint_storage_fingerprint: str
    serving_generation_storage_fingerprint: str
    delta_receipt_storage_fingerprint: str
    import_run_storage_fingerprint: str
    capacity_consumption_storage_fingerprint: str
    control_wal_plan_input_hash: str
    control_wal_upper_bound_bytes: int
    control_metadata_data_upper_bound_bytes: int
    artifact_scope_wave_count: int
    evidence_wave_count: int
    compact_wave_count: int
    artifact_scope_worker_count: int
    evidence_worker_count: int
    compact_worker_count: int
    database_pool_size: int
    artifact_scope_projected_rows: int
    artifact_scope_projected_logical_bytes: int
    artifact_scope_projection_hash: str
