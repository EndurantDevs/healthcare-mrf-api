# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable types and constants for Provider Directory Profile capacity."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

CAPACITY_GEOMETRY_CONTRACT_ID = "healthporta.provider-directory-profile-capacity-geometry.v5"
PROFILE_MATERIALIZATION_MODE = "source_delta"
PROFILE_STRATEGY_VERSION = (
    "source-fact-role32-org32-member32-dataset-pract-auth-npi5m-v5"
)

_GEOMETRY_HASH_DOMAIN = "provider_directory_profile_capacity_geometry.v5"
_CONTROL_WAL_PLAN_INPUT_HASH_DOMAIN = (
    "provider_directory_profile_control_wal_plan_input.v3"
)
_CONTROL_WAL_HASH_DOMAIN = (
    "provider_directory_profile_control_wal_projection.v4"
)
PHYSICAL_PROJECTION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-delta-physical-projection.v1"
)
CUTOVER_FORECAST_CONTRACT_ID = (
    "healthporta.provider-directory-profile-cutover-forecast.v1"
)
CUTOVER_ACTUAL_CONTRACT_ID = (
    "healthporta.provider-directory-profile-cutover-actual.v1"
)
CONTROL_WAL_PROJECTION_CONTRACT_ID = (
    "healthporta.provider-directory-profile-control-wal-projection.v4"
)
ARTIFACT_SCOPE_RECOVERY_CONTRACT_ID = (
    "provider-directory-artifact-scope-recovery-v1"
)
POSTGRES_SUPPORTED_MAJOR = 18
POSTGRES_BLOCK_SIZE_BYTES = 8192
POSTGRES_TOAST_MAX_CHUNK_SIZE_BYTES = 1996
POSTGRES_MAXALIGN_BYTES = 8
POSTGRES_BTREE_VERSION = 4
METADATA_WAL_UPPER_BOUND_BYTES = 64 * 1024 * 1024
METADATA_DATA_UPPER_BOUND_BYTES = 32 * 1024 * 1024
METADATA_PAYLOAD_UPPER_BOUND_BYTES = 64 * 1024
CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT = 64 * 1024 * 1024
CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT = 32 * 1024 * 1024
CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT = 32 * 1024 * 1024
CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE = 64 * 1024
CONTROL_WAL_ARTIFACT_SCOPE_NAMES = (
    "source",
    "InsurancePlan",
    "Practitioner",
    "Organization",
    "Location",
    "PractitionerRole",
    "HealthcareService",
    "OrganizationAffiliation",
    "Endpoint",
)
CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT = len(
    CONTROL_WAL_ARTIFACT_SCOPE_NAMES
)
CONTROL_WAL_ARTIFACT_PK_STATEMENTS_PER_TABLE = 2
CONTROL_WAL_ARTIFACT_BUCKET_INDEX_STATEMENT_COUNT = 2
CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT = (
    CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT
    * (1 + CONTROL_WAL_ARTIFACT_PK_STATEMENTS_PER_TABLE)
    + CONTROL_WAL_ARTIFACT_BUCKET_INDEX_STATEMENT_COUNT
)
CONTROL_WAL_PROFILE_STAGE_CREATE_STATEMENT_COUNT = 3
CONTROL_WAL_EVIDENCE_STAGE_INDEX_STATEMENT_COUNT = 4
CONTROL_WAL_COMPACT_STAGE_INDEX_STATEMENT_COUNT = 1
CONTROL_WAL_PROFILE_STAGE_INDEX_STATEMENT_COUNT = (
    CONTROL_WAL_EVIDENCE_STAGE_INDEX_STATEMENT_COUNT
    + CONTROL_WAL_COMPACT_STAGE_INDEX_STATEMENT_COUNT
)
CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT = 3
CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT = (
    CONTROL_WAL_PROFILE_STAGE_CREATE_STATEMENT_COUNT
    + CONTROL_WAL_PROFILE_STAGE_INDEX_STATEMENT_COUNT
)
CONTROL_WAL_PROFILE_STAGE_ANALYZE_STATEMENT_COUNT = 3
CONTROL_WAL_PROFILE_STAGE_DROP_STATEMENT_COUNT = 3
CONTROL_WAL_CHECKPOINT_PHASE_STATE_UPDATE_COUNT = 2
CONTROL_WAL_IMPORT_RUN_PHASE_START_UPDATE_COUNT = 2
CONTROL_WAL_AFFECTED_NPI_DELTA_STATEMENT_COUNT = 1
CONTROL_WAL_FINAL_CUTOVER_CHECKPOINT_UPDATE_COUNT = 1
CONTROL_WAL_FAILURE_CHECKPOINT_UPDATE_COUNT = 1
CONTROL_WAL_CHECKPOINT_RETIRE_UPDATE_COUNT = 1
CONTROL_WAL_CHECKPOINT_REINITIALIZE_DELETE_COUNT = 1
CONTROL_WAL_CHECKPOINT_INITIAL_INSERT_COUNT = 1
_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SYSTEM_IDENTIFIER_PATTERN = re.compile(r"^[1-9][0-9]{0,19}$")
_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_MAX_SIGNED_BIGINT = (1 << 63) - 1
_MAX_UNSIGNED_BIGINT = (1 << 64) - 1
_MAX_OID = (1 << 32) - 1
_MAX_POOL_SIZE = 256
_MAX_WORKERS = 2
PROFILE_DEDICATED_ADVISORY_LOCK_CONNECTIONS = 3
PROFILE_CONTROL_CONNECTION_RESERVE = 1
PROFILE_MINIMUM_POOL_RESERVE = (
    PROFILE_DEDICATED_ADVISORY_LOCK_CONNECTIONS
    + PROFILE_CONTROL_CONNECTION_RESERVE
)

_RELATION_NAMES = (
    "artifact_scope",
    "evidence_stage",
    "affected_npi_stage",
    "profile_stage",
    "evidence_target",
    "profile_target",
)
_SCRATCH_RELATION_NAMES = frozenset(_RELATION_NAMES[:4])
_TARGET_RELATION_NAMES = frozenset(_RELATION_NAMES[4:])
_CONTROL_WAL_OPERATION_ORDER = (
    ("pre_cutover", "admission_row_lock"),
    ("pre_cutover", "capacity_consumption_insert"),
    ("pre_cutover", "artifact_scope_recovery_drop"),
    ("pre_cutover", "artifact_scope_layout"),
    ("pre_cutover", "artifact_scope_payload"),
    ("pre_cutover", "artifact_scope_analyze"),
    ("pre_cutover", "profile_stage_reinitialize"),
    ("pre_cutover", "profile_stage_initialize"),
    ("pre_cutover", "evidence_progress_start"),
    ("pre_cutover", "evidence_payload"),
    ("pre_cutover", "evidence_checkpoint_advance"),
    ("pre_cutover", "evidence_import_run_progress"),
    ("pre_cutover", "evidence_stage_analyze"),
    ("pre_cutover", "evidence_checkpoint_complete"),
    ("pre_cutover", "affected_npi_payload"),
    ("pre_cutover", "affected_npi_analyze"),
    ("pre_cutover", "profile_progress_start"),
    ("pre_cutover", "profile_payload"),
    ("pre_cutover", "profile_checkpoint_advance"),
    ("pre_cutover", "profile_import_run_progress"),
    ("pre_cutover", "profile_stage_analyze"),
    ("pre_cutover", "profile_checkpoint_ready"),
    ("pre_cutover", "cutover_row_lock"),
    ("post_cutover", "profile_checkpoint_retire"),
    ("post_cutover", "profile_stage_drop"),
    ("post_cutover", "artifact_scope_drop"),
    ("failure_reserve", "profile_checkpoint_failure_reserve"),
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
_GEOMETRY_FIELDS = frozenset(
    """
    contract_id selection_proof_id profile_input_digest materialization_mode
    profile_schema_version profile_strategy_version executable_plan_hash
    profile_as_of current_source_vector_hash desired_source_vector_hash
    current_context_vector_hash desired_context_vector_hash sql_contract_digest
    database_system_identifier database_oid database_name tablespace_oid
    tablespace_name evidence_target_oid profile_target_oid evidence_wave_count
    postgres_server_version_num postgres_block_size_bytes
    postgres_toast_max_chunk_size_bytes postgres_maxalign_bytes
    postgres_btree_version
    postgres_wal_block_size_bytes postgres_wal_segment_size_bytes
    postgres_full_page_writes postgres_wal_compression postgres_wal_level
    postgres_wal_log_hints postgres_data_checksums
    postgres_default_toast_compression postgres_checkpoint_timeout_seconds
    postgres_max_wal_size_bytes evidence_target_storage_fingerprint
    profile_target_storage_fingerprint build_checkpoint_oid
    serving_generation_oid delta_receipt_oid import_run_oid
    capacity_consumption_oid
    build_checkpoint_storage_fingerprint serving_generation_storage_fingerprint
    delta_receipt_storage_fingerprint import_run_storage_fingerprint
    capacity_consumption_storage_fingerprint
    control_wal_plan_input_hash control_wal_upper_bound_bytes
    control_metadata_data_upper_bound_bytes
    physical_projection_contract_id
    metadata_data_upper_bound_bytes metadata_wal_upper_bound_bytes
    compact_wave_count artifact_scope_wave_count evidence_worker_count
    compact_worker_count artifact_scope_worker_count artifact_scope_batch_size
    artifact_scope_projection_hash artifact_scope_projected_logical_bytes
    database_pool_size pool_reserve_connections
    max_parallel_workers_per_gather max_parallel_maintenance_workers
    work_mem_bytes maintenance_work_mem_bytes temp_file_limit_bytes
    max_build_seconds statement_timeout_ms lock_timeout_ms minimum_remaining_bytes
    max_artifact_scope_rows max_evidence_rows max_affected_npis
    max_profile_rows relation_byte_caps
    """.split()
)
_HASH_FIELDS = frozenset(
    {
        "selection_proof_id",
        "profile_input_digest",
        "executable_plan_hash",
        "current_source_vector_hash",
        "desired_source_vector_hash",
        "current_context_vector_hash",
        "desired_context_vector_hash",
        "sql_contract_digest",
        "evidence_target_storage_fingerprint",
        "profile_target_storage_fingerprint",
        "build_checkpoint_storage_fingerprint",
        "serving_generation_storage_fingerprint",
        "delta_receipt_storage_fingerprint",
        "import_run_storage_fingerprint",
        "capacity_consumption_storage_fingerprint",
        "control_wal_plan_input_hash",
        "artifact_scope_projection_hash",
    }
)
_POSITIVE_ROW_CAP_FIELDS = frozenset(
    {
        "max_evidence_rows",
        "max_affected_npis",
        "max_profile_rows",
    }
)

class ProviderDirectoryProfileCapacityError(ValueError):
    """Report malformed or unsafe executable Profile geometry."""

@dataclass(frozen=True)
class ProviderDirectoryProfileRelationByteCaps:
    """Hard byte ceilings for one executable-plan relation class."""
    relation_name: str
    max_scratch_bytes: int
    max_target_growth_bytes: int
    max_deleted_logical_bytes: int
    max_temp_bytes: int
    max_wal_bytes: int


@dataclass(frozen=True)
class ProviderDirectoryProfileTargetDeltaInput:
    """Exact staged and retained measurements available before target DML."""

    relation_name: str
    inserted_rows: int
    inserted_toast_chunks: int
    deleted_rows: int
    deleted_logical_bytes: int
    deleted_toast_chunks: int
    main_index_pages: tuple[int, ...]
    toast_index_pages: tuple[int, ...]


@dataclass(frozen=True)
class ProviderDirectoryProfileTargetDeltaProjection:
    """Conservative signed upper bounds for one target relation."""

    relation_name: str
    target_growth_bytes: int
    deleted_logical_bytes: int
    wal_bytes: int


@dataclass(frozen=True)
class ProviderDirectoryProfileDeltaProjection:
    """Preventive data and WAL bounds for the complete atomic delta."""

    targets: tuple[ProviderDirectoryProfileTargetDeltaProjection, ...]
    target_data_bytes: int
    wal_bytes: int


@dataclass(frozen=True)
class ProviderDirectoryProfileScratchInput:
    """Exact logical output available before one bounded scratch write."""

    relation_name: str
    inserted_rows: int
    inserted_logical_bytes: int
    toastable_column_count: int
    main_index_pages: tuple[int, ...]
    toast_index_pages: tuple[int, ...]


@dataclass(frozen=True)
class ProviderDirectoryProfileScratchProjection:
    """Conservative physical and WAL bounds for one scratch write."""

    relation_name: str
    inserted_rows: int
    inserted_logical_bytes: int
    inserted_toast_chunks_upper: int
    growth_bytes: int
    wal_bytes: int


@dataclass(frozen=True)
class ProviderDirectoryProfileMetadataMutationInput:
    """One exact final metadata row mutation in the cutover transaction."""

    relation_name: str
    operation: str
    payload_upper_bytes: int
    deleted_toast_chunks: int
    main_index_pages: tuple[int, ...]
    toast_index_pages: tuple[int, ...]


@dataclass(frozen=True)
class ProviderDirectoryProfileMetadataProjection:
    """Protocol-derived final metadata and commit capacity forecast."""

    data_bytes: int
    wal_bytes: int
    commit_envelope_bytes: int


@dataclass(frozen=True)
class ProfileControlArtifactBatchCount:
    """Exact batch count for one ordered artifact-scope relation."""

    artifact_name: str
    batch_count: int


@dataclass(frozen=True)
class ProfileControlWalPlanInput:
    """Exact successful-build coordinates and control-row layouts."""

    artifact_batch_counts: tuple[
        ProfileControlArtifactBatchCount,
        ...,
    ]
    artifact_scope_recovery_contract_id: str
    evidence_batch_count: int
    compact_batch_count: int
    affected_source_count: int
    admission_row_lock_count: int
    cutover_row_lock_count: int
    build_checkpoint_insert: ProviderDirectoryProfileMetadataMutationInput
    build_checkpoint_update: ProviderDirectoryProfileMetadataMutationInput
    import_run_update: ProviderDirectoryProfileMetadataMutationInput
    capacity_consumption_insert: (
        ProviderDirectoryProfileMetadataMutationInput
    )


@dataclass(frozen=True)
class ProviderDirectoryProfileControlWalOperation:
    """One ordered, uniformly repeatable control-WAL reservation unit."""

    phase: str
    operation_name: str
    operation_count: int
    metadata_mutation_count: int
    fixed_statement_count: int
    commit_count: int
    metadata_data_bytes: int
    metadata_wal_bytes: int
    fixed_statement_wal_bytes: int
    commit_envelope_bytes: int
    metadata_data_bytes_per_operation: int
    wal_bytes_per_operation: int
    wal_bytes: int


@dataclass(frozen=True)
class ProviderDirectoryProfileControlWalProjection:
    """Immutable pre/post/failure control-WAL ledger for one build."""

    contract_id: str
    capacity_geometry_hash: str
    final_cutover_contract_id: str
    plan_input: ProfileControlWalPlanInput
    operations: tuple[ProviderDirectoryProfileControlWalOperation, ...]
    pre_cutover_wal_bytes: int
    post_cutover_wal_bytes: int
    failure_reserve_wal_bytes: int
    total_control_metadata_data_bytes: int
    total_control_wal_bytes: int


@dataclass(frozen=True)
class ProviderDirectoryProfileCapacityGeometry:
    """Proof-bound limits and execution identity for one delta build."""
    contract_id: str
    selection_proof_id: str
    profile_input_digest: str
    materialization_mode: str
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
    postgres_toast_max_chunk_size_bytes: int
    postgres_maxalign_bytes: int
    postgres_btree_version: int
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
    physical_projection_contract_id: str
    metadata_data_upper_bound_bytes: int
    metadata_wal_upper_bound_bytes: int
    evidence_wave_count: int
    compact_wave_count: int
    artifact_scope_wave_count: int
    evidence_worker_count: int
    compact_worker_count: int
    artifact_scope_worker_count: int
    artifact_scope_batch_size: int
    artifact_scope_projection_hash: str
    artifact_scope_projected_logical_bytes: int
    database_pool_size: int
    pool_reserve_connections: int
    max_parallel_workers_per_gather: int
    max_parallel_maintenance_workers: int
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
    relation_byte_caps: tuple[ProviderDirectoryProfileRelationByteCaps, ...]
    @property
    def maximum_worker_count(self) -> int:
        """Return maximum concurrent Python workers in any frozen wave."""
        return max(
            self.artifact_scope_worker_count,
            self.evidence_worker_count, self.compact_worker_count,
        )
    @property
    def reservation_bytes_by_storage_class(self) -> dict[str, int]:
        """Return exact data, temp, and WAL upper bounds for signed leases."""
        relation_caps = self.relation_byte_caps
        return {
            "data": (
                sum(
                    relation.max_scratch_bytes
                    + relation.max_target_growth_bytes
                    for relation in relation_caps
                )
                + self.metadata_data_upper_bound_bytes
                + self.control_metadata_data_upper_bound_bytes
            ),
            "temp": (
                max(1, self.maximum_worker_count)
                * self.temp_file_limit_bytes
            ),
            "wal": (
                sum(relation.max_wal_bytes for relation in relation_caps)
                + self.metadata_wal_upper_bound_bytes
                + self.control_wal_upper_bound_bytes
            ),
        }
