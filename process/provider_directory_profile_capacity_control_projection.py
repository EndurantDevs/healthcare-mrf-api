# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Project, serialize, and revalidate Provider Directory control-WAL capacity."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any, Mapping

from process.provider_directory_profile_capacity_control_budget import (
    _control_metadata_mutation_bounds,
    _control_wal_nonnegative_integer,
    _validate_control_wal_plan_input,
)
from process.provider_directory_profile_capacity_control_identity import (
    profile_control_wal_plan_input_hash,
    profile_control_wal_plan_input_payload,
)
from process.provider_directory_profile_capacity_control_operations import (
    _control_wal_operation_ledger,
    _control_wal_phase_total,
)
from process.provider_directory_profile_capacity_geometry import (
    _error,
    capacity_geometry_hash,
    revalidate_capacity_geometry,
)
from process.provider_directory_profile_capacity_target import _checked_add
from process.provider_directory_profile_capacity_types import (
    CONTROL_WAL_PROJECTION_CONTRACT_ID,
    CUTOVER_FORECAST_CONTRACT_ID,
    ProfileControlWalPlanInput,
    ProviderDirectoryProfileCapacityGeometry,
    ProviderDirectoryProfileControlWalOperation,
    ProviderDirectoryProfileControlWalProjection,
    _CONTROL_WAL_HASH_DOMAIN,
    _CONTROL_WAL_OPERATION_ORDER,
    _HASH_PATTERN,
)

def project_profile_control_wal_capacity(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    plan_input: ProfileControlWalPlanInput,
) -> ProviderDirectoryProfileControlWalProjection:
    """Project all non-cutover control WAL before the first scratch DML.

    Payload heap/index WAL stays in its physical projection; this ledger
    reserves commits, control rows, and fixed catalog statements. The final
    atomic cutover remains owned by ``project_profile_delta_metadata_capacity``.
    """

    verified_geometry = revalidate_capacity_geometry(geometry)
    _validate_control_wal_plan_input(plan_input)
    if (
        profile_control_wal_plan_input_hash(plan_input)
        != verified_geometry.control_wal_plan_input_hash
    ):
        raise _error("control_wal_plan_input_hash_mismatch")
    metadata_mutation_bounds = _control_metadata_mutation_bounds(
        verified_geometry,
        plan_input,
    )
    operations = _control_wal_operation_ledger(
        verified_geometry,
        plan_input,
        metadata_mutation_bounds,
    )
    operation_order_pairs = tuple(
        (operation.phase, operation.operation_name)
        for operation in operations
    )
    if operation_order_pairs != _CONTROL_WAL_OPERATION_ORDER:
        raise _error("control_wal_projection_operation_order_invalid")
    phase_totals = tuple(
        _control_wal_phase_total(operations, phase)
        for phase in ("pre_cutover", "post_cutover", "failure_reserve")
    )
    return ProviderDirectoryProfileControlWalProjection(
        contract_id=CONTROL_WAL_PROJECTION_CONTRACT_ID,
        capacity_geometry_hash=capacity_geometry_hash(verified_geometry),
        final_cutover_contract_id=CUTOVER_FORECAST_CONTRACT_ID,
        plan_input=plan_input,
        operations=operations,
        pre_cutover_wal_bytes=phase_totals[0],
        post_cutover_wal_bytes=phase_totals[1],
        failure_reserve_wal_bytes=phase_totals[2],
        total_control_metadata_data_bytes=_checked_add(
            *(operation.metadata_data_bytes for operation in operations)
        ),
        total_control_wal_bytes=_checked_add(*phase_totals),
    )


def _validate_control_operation_shape(
    control_operation: ProviderDirectoryProfileControlWalOperation,
) -> None:
    for field_name in (
        "operation_count",
        "metadata_mutation_count",
        "fixed_statement_count",
        "commit_count",
        "metadata_data_bytes",
        "metadata_wal_bytes",
        "fixed_statement_wal_bytes",
        "commit_envelope_bytes",
        "metadata_data_bytes_per_operation",
        "wal_bytes_per_operation",
        "wal_bytes",
    ):
        _control_wal_nonnegative_integer(
            getattr(control_operation, field_name),
            field_name,
        )
    if (
        control_operation.metadata_mutation_count
        != (
            control_operation.operation_count
            if (
                control_operation.metadata_data_bytes_per_operation
                or control_operation.metadata_wal_bytes
            )
            else 0
        )
        or control_operation.metadata_data_bytes
        != control_operation.operation_count
        * control_operation.metadata_data_bytes_per_operation
        or control_operation.wal_bytes
        != control_operation.operation_count
        * control_operation.wal_bytes_per_operation
        or control_operation.wal_bytes
        != control_operation.metadata_wal_bytes
        + control_operation.fixed_statement_wal_bytes
        + control_operation.commit_envelope_bytes
    ):
        raise _error("control_wal_projection_invalid")


def _assert_control_wal_projection_shape(
    projection: ProviderDirectoryProfileControlWalProjection,
) -> None:
    observed_operation_order = (
        tuple(
            (
                control_operation.phase,
                control_operation.operation_name,
            )
            for control_operation in projection.operations
            if isinstance(
                control_operation,
                ProviderDirectoryProfileControlWalOperation,
            )
        )
        if isinstance(projection, ProviderDirectoryProfileControlWalProjection)
        and isinstance(projection.operations, tuple)
        else ()
    )
    if (
        not isinstance(
            projection,
            ProviderDirectoryProfileControlWalProjection,
        )
        or projection.contract_id != CONTROL_WAL_PROJECTION_CONTRACT_ID
        or not isinstance(projection.capacity_geometry_hash, str)
        or not _HASH_PATTERN.fullmatch(
            projection.capacity_geometry_hash
        )
        or projection.final_cutover_contract_id
        != CUTOVER_FORECAST_CONTRACT_ID
        or not isinstance(projection.operations, tuple)
        or observed_operation_order != _CONTROL_WAL_OPERATION_ORDER
        or len(projection.operations) != len(_CONTROL_WAL_OPERATION_ORDER)
    ):
        raise _error("control_wal_projection_invalid")
    _validate_control_wal_plan_input(projection.plan_input)
    for control_operation in projection.operations:
        _validate_control_operation_shape(control_operation)
    phase_totals = tuple(
        _control_wal_phase_total(projection.operations, phase)
        for phase in ("pre_cutover", "post_cutover", "failure_reserve")
    )
    if (
        projection.pre_cutover_wal_bytes != phase_totals[0]
        or projection.post_cutover_wal_bytes != phase_totals[1]
        or projection.failure_reserve_wal_bytes != phase_totals[2]
        or projection.total_control_metadata_data_bytes
        != _checked_add(
            *(
                operation.metadata_data_bytes
                for operation in projection.operations
            )
        )
        or projection.total_control_wal_bytes
        != _checked_add(*phase_totals)
    ):
        raise _error("control_wal_projection_invalid")


def revalidate_profile_control_wal_projection(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    projection: ProviderDirectoryProfileControlWalProjection,
) -> ProviderDirectoryProfileControlWalProjection:
    """Recompute every formula before trusting a retained control ledger."""

    _assert_control_wal_projection_shape(projection)
    verified_geometry = revalidate_capacity_geometry(geometry)
    if (
        projection.total_control_metadata_data_bytes
        != verified_geometry.control_metadata_data_upper_bound_bytes
    ):
        raise _error(
            "control_metadata_data_projection_geometry_bound_mismatch"
        )
    if (
        projection.total_control_wal_bytes
        != verified_geometry.control_wal_upper_bound_bytes
    ):
        raise _error("control_wal_projection_geometry_bound_mismatch")
    recomputed = project_profile_control_wal_capacity(
        verified_geometry,
        projection.plan_input,
    )
    if recomputed != projection:
        raise _error("control_wal_projection_formula_changed")
    return projection


def profile_control_wal_projection_payload(
    projection: ProviderDirectoryProfileControlWalProjection,
) -> dict[str, Any]:
    """Return exact JSON-compatible evidence for one control-WAL plan."""

    _assert_control_wal_projection_shape(projection)
    return {
        "contract_id": projection.contract_id,
        "capacity_geometry_hash": projection.capacity_geometry_hash,
        "final_cutover_contract_id": projection.final_cutover_contract_id,
        "plan_input": profile_control_wal_plan_input_payload(
            projection.plan_input
        ),
        "operations": [
            dataclasses.asdict(control_operation)
            for control_operation in projection.operations
        ],
        "pre_cutover_wal_bytes": projection.pre_cutover_wal_bytes,
        "post_cutover_wal_bytes": projection.post_cutover_wal_bytes,
        "failure_reserve_wal_bytes": (
            projection.failure_reserve_wal_bytes
        ),
        "total_control_metadata_data_bytes": (
            projection.total_control_metadata_data_bytes
        ),
        "total_control_wal_bytes": projection.total_control_wal_bytes,
    }


def canonical_profile_control_wal_projection_json(
    projection: ProviderDirectoryProfileControlWalProjection,
) -> str:
    """Return canonical durable JSON for the ordered control-WAL ledger."""

    return json.dumps(
        profile_control_wal_projection_payload(projection),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def profile_control_wal_projection_hash(
    projection: ProviderDirectoryProfileControlWalProjection,
) -> str:
    """Return the deterministic identity of one control-WAL projection."""

    canonical_projection = (
        canonical_profile_control_wal_projection_json(projection)
    )
    hash_input = f"{_CONTROL_WAL_HASH_DOMAIN}:{canonical_projection}"
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


def remaining_profile_control_wal_bytes(
    projection: ProviderDirectoryProfileControlWalProjection,
    completed_operation_counts: Mapping[str, int] | None = None,
    *,
    failure_reserve_released: bool = False,
) -> int:
    """Return exact unconsumed control WAL from committed operation counts.

    The failure reserve remains held until a terminal success explicitly
    releases it. On a failure path, recording its one completed mutation also
    consumes the same reserve.
    """

    _assert_control_wal_projection_shape(projection)
    if (
        completed_operation_counts is not None
        and not isinstance(completed_operation_counts, Mapping)
    ):
        raise _error("control_wal_projection_completed_operation_invalid")
    if not isinstance(failure_reserve_released, bool):
        raise _error("control_wal_projection_failure_release_invalid")
    completed_counts_by_name = dict(completed_operation_counts or {})
    operations_by_name = {
        control_operation.operation_name: control_operation
        for control_operation in projection.operations
    }
    if set(completed_counts_by_name) - set(operations_by_name):
        raise _error("control_wal_projection_completed_operation_unknown")
    remaining = 0
    for operation_name, control_operation in operations_by_name.items():
        completed_count = _control_wal_nonnegative_integer(
            completed_counts_by_name.get(operation_name, 0),
            "completed_operation_count",
        )
        if (
            control_operation.phase == "failure_reserve"
            and failure_reserve_released
        ):
            completed_count = control_operation.operation_count
        if completed_count > control_operation.operation_count:
            raise _error(
                "control_wal_projection_completed_operation_exceeded:"
                + operation_name
            )
        remaining = _checked_add(
            remaining,
            (control_operation.operation_count - completed_count)
            * control_operation.wal_bytes_per_operation,
        )
    return remaining
