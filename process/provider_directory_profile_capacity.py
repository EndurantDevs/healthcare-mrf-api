# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compatibility facade for Provider Directory Profile capacity contracts."""

from __future__ import annotations

from process.provider_directory_profile_capacity_types import *
from process.provider_directory_profile_capacity_types import (
    _CONTROL_WAL_HASH_DOMAIN,
    _CONTROL_WAL_OPERATION_ORDER,
    _CONTROL_WAL_PLAN_INPUT_HASH_DOMAIN,
    _DATE_PATTERN,
    _GEOMETRY_FIELDS,
    _GEOMETRY_HASH_DOMAIN,
    _HASH_FIELDS,
    _HASH_PATTERN,
    _MAX_OID,
    _MAX_POOL_SIZE,
    _MAX_SIGNED_BIGINT,
    _MAX_UNSIGNED_BIGINT,
    _MAX_WORKERS,
    _POSITIVE_ROW_CAP_FIELDS,
    _RELATION_FIELDS,
    _RELATION_NAMES,
    _SCRATCH_RELATION_NAMES,
    _SYSTEM_IDENTIFIER_PATTERN,
    _TARGET_RELATION_NAMES,
)
from process.provider_directory_profile_capacity_geometry import (
    _assert_execution_limits,
    _assert_relation_cap_shape,
    _assert_wave_geometry,
    _bounded_integer,
    _database_system_identifier,
    _error,
    _exact_fields,
    _exact_hash,
    _exact_text,
    _nonnegative_bigint,
    _positive_bigint,
    _profile_as_of,
    _validated_execution_geometry,
    _validated_relation_cap_sequence,
    _validated_scalar_geometry,
    _validated_single_relation_cap,
    canonical_capacity_geometry_json,
    capacity_geometry_hash,
    capacity_geometry_payload,
    revalidate_capacity_geometry,
    validated_capacity_geometry,
)
from process.provider_directory_profile_capacity_target import (
    _btree_insert_growth_pages,
    _btree_wal_page_touches,
    _ceil_log2,
    _checked_add,
    _target_delta_projection,
    project_profile_delta_capacity,
)
from process.provider_directory_profile_capacity_physical import (
    _metadata_mutation_projection,
    project_profile_delta_metadata_capacity,
    project_profile_scratch_capacity,
)
from process.provider_directory_profile_capacity_control_budget import (
    _commit_control_operation,
    _control_metadata_mutation_bounds,
    _control_metadata_projection_per_operation,
    _control_operation_totals,
    _control_wal_nonnegative_integer,
    _control_wal_operation,
    _control_wal_product,
    _empty_control_wal_operation,
    _final_control_index_page_bounds,
    _fixed_control_operation,
    _metadata_control_operation,
    _row_lock_control_operation,
    _validate_control_artifact_batches,
    _validate_control_wal_metadata_input,
    _validate_control_wal_plan_input,
)
from process.provider_directory_profile_capacity_control_identity import (
    _control_metadata_payload,
    canonical_control_wal_plan_json,
    profile_control_wal_plan_input_hash,
    profile_control_wal_plan_input_payload,
)
from process.provider_directory_profile_capacity_control_operations import (
    _affected_control_operations,
    _artifact_control_operations,
    _control_wal_operation_ledger,
    _control_wal_phase_total,
    _evidence_control_operations,
    _profile_control_operations,
    _stage_control_operations,
    _terminal_control_operations,
)
from process.provider_directory_profile_capacity_control_projection import (
    _assert_control_wal_projection_shape,
    _validate_control_operation_shape,
    canonical_profile_control_wal_projection_json,
    profile_control_wal_projection_hash,
    profile_control_wal_projection_payload,
    project_profile_control_wal_capacity,
    remaining_profile_control_wal_bytes,
    revalidate_profile_control_wal_projection,
)
from process.provider_directory_profile_capacity_cutover_contract import (
    _assert_cutover_layout,
    _cutover_nonnegative_integer,
    _cutover_target_projection,
)
from process.provider_directory_profile_capacity_cutover import (
    validate_profile_delta_cutover_evidence,
)

canonical_profile_control_wal_plan_input_json = canonical_control_wal_plan_json
