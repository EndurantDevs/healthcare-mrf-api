# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Project, serialize, and revalidate Provider Directory control-WAL capacity."""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import json
from typing import Any, Mapping
from process import provider_directory_profile_initial_contract as initial

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
    BOUNDED_CUTOVER_FORECAST_CONTRACT_ID,
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
    expected_order = _CONTROL_WAL_OPERATION_ORDER + ((("pre_cutover", "initial_cutover"),)
                                                    if isinstance(geometry, initial.InitialCapacityGeometry) else ())
    if operation_order_pairs != expected_order:
        raise _error("control_wal_projection_operation_order_invalid")
    phase_totals = tuple(
        _control_wal_phase_total(operations, phase)
        for phase in ("pre_cutover", "post_cutover", "failure_reserve")
    )
    return ProviderDirectoryProfileControlWalProjection(
        contract_id=(initial.CONTROL_WAL_CONTRACT if isinstance(geometry, initial.InitialCapacityGeometry)
                     else CONTROL_WAL_PROJECTION_CONTRACT_ID),
        capacity_geometry_hash=capacity_geometry_hash(verified_geometry),
        final_cutover_contract_id=verified_geometry.cutover_forecast_contract_id,
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


def _assert_control_wal_projection_totals(projection):
    """Recompute metadata and WAL sums from the validated operations."""
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


def _assert_control_wal_projection_shape(
    projection: ProviderDirectoryProfileControlWalProjection,
) -> None:
    """Validate the closed operation order before trusting any ledger totals."""
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
    expected_order = _CONTROL_WAL_OPERATION_ORDER + ((("pre_cutover", "initial_cutover"),)
                                                    if getattr(projection, "contract_id", None) == initial.CONTROL_WAL_CONTRACT else ())
    if (
        not isinstance(
            projection,
            ProviderDirectoryProfileControlWalProjection,
        )
        or projection.contract_id not in {CONTROL_WAL_PROJECTION_CONTRACT_ID, initial.CONTROL_WAL_CONTRACT}
        or not isinstance(projection.capacity_geometry_hash, str)
        or not _HASH_PATTERN.fullmatch(
            projection.capacity_geometry_hash
        )
        or projection.final_cutover_contract_id
        not in {CUTOVER_FORECAST_CONTRACT_ID, BOUNDED_CUTOVER_FORECAST_CONTRACT_ID, initial.FORECAST_CONTRACT}
        or not isinstance(projection.operations, tuple)
        or observed_operation_order != expected_order
        or len(projection.operations) != len(expected_order)
    ):
        raise _error("control_wal_projection_invalid")
    _validate_control_wal_plan_input(projection.plan_input)
    for control_operation in projection.operations:
        _validate_control_operation_shape(control_operation)
    _assert_control_wal_projection_totals(projection)


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


def _pending_reservation_wal(fhir, admission, window, relation_counts, control_next):
    tracker = admission.wal_tracker
    if not admission.geometry.bounded_admission:
        return 0
    if window is not None and window[1] is not None:
        relation_name = window[1]
        cap = fhir._provider_directory_profile_capacity_relation_cap(admission, relation_name)
        if (relation_counts.get(relation_name, 0)
            + tracker.pending_control_wal_bytes.get(window[0], 0)
            + control_next > cap.max_wal_bytes):
            raise RuntimeError("provider_directory_profile_capacity_window_wal_projected")
    return (sum(tracker.pending_control_wal_bytes.values())
            + sum(tracker.pending_relation_wal_bytes.values())
            + tracker.pending_metadata_wal_bytes)


def _publish_wal_reservation(fhir, admission, window, control_next,
                             relation_increments, relation_counts, metadata_wal_bytes):
    tracker = admission.wal_tracker
    if admission.geometry.bounded_admission:
        for name, increment in relation_increments.items():
            tracker.pending_relation_wal_bytes[name] = (
                tracker.pending_relation_wal_bytes.get(name, 0) + increment
            )
        if control_next:
            owner = window[0] if window is not None else fhir.asyncio.current_task()
            tracker.pending_control_wal_bytes[owner] = (
                tracker.pending_control_wal_bytes.get(owner, 0) + control_next
            )
        tracker.pending_metadata_wal_bytes += metadata_wal_bytes
    else:
        tracker.accounted_relation_wal_bytes = relation_counts


async def reserve_wal_budget(fhir, admission, *, control_operation_counts=None,
                            relation_wal_bytes=None, metadata_wal_bytes=0):
    """Validate a complete preventive candidate before publishing its charges."""
    control_increments, relation_increments = fhir._profile_wal_reservation_inputs(
        control_operation_counts, relation_wal_bytes, metadata_wal_bytes,
    )
    tracker = admission.wal_tracker
    async with tracker.lock:
        window = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
        if admission.geometry.bounded_admission:
            await fhir._profile_capacity_remaining_ms(admission)
            if relation_increments and (
                tracker.unresolved_window or window is None
                or set(relation_increments) != {window[1]}
            ):
                raise RuntimeError("provider_directory_profile_capacity_relation_window_required")
        projection = fhir.profile_capacity.revalidate_profile_control_wal_projection(
            admission.geometry, admission.control_wal_projection,
        )
        control_counts, control_next, control_remaining = fhir._profile_control_wal_candidate(
            tracker, projection, control_increments,
        )
        relation_counts, relation_next, relation_remaining = fhir._profile_relation_wal_candidate(
            admission, relation_increments,
        )
        metadata_candidate, metadata_remaining = fhir._profile_metadata_wal_candidate(
            admission, metadata_wal_bytes,
        )
        pending_wal_bytes = _pending_reservation_wal(
            fhir, admission, window, relation_counts, control_next,
        )
        await fhir._validate_profile_total_wal_budget(
            admission, pending_wal_bytes + control_next + relation_next + metadata_wal_bytes,
            control_remaining + relation_remaining + metadata_remaining,
        )
        tracker.accounted_control_operation_counts = control_counts
        _publish_wal_reservation(fhir, admission, window, control_next, relation_increments,
                                 relation_counts, metadata_wal_bytes)
        tracker.accounted_metadata_wal_bytes = metadata_candidate


async def _claim_mutation_window(fhir, tracker, owner, relation_name, relation_refs):
    caller = fhir.asyncio.current_task()
    async with tracker.lock:
        preceding_control = tracker.pending_control_wal_bytes.pop(caller, 0)
        if preceding_control:
            tracker.pending_control_wal_bytes[owner] = preceding_control
        if relation_name is not None:
            tracker.relation_refs_by_class.setdefault(relation_name, set()).update(relation_refs)


async def _validate_window_growth(fhir, admission, relation_name, data_before, actual_wal):
    tracker = admission.wal_tracker
    data_after = await fhir._provider_directory_profile_capacity_relation_bytes(
        tracker.relation_refs_by_class[relation_name],
    )
    growth_pending = tracker.pending_growth_bytes.get(relation_name, 0)
    cap = fhir._provider_directory_profile_capacity_relation_cap(admission, relation_name)
    baseline = tracker.target_bytes_before.get(relation_name, 0)
    maximum_data = cap.max_scratch_bytes + cap.max_target_growth_bytes
    settled_wal = tracker.accounted_relation_wal_bytes.get(relation_name, 0) + actual_wal
    if (data_after > data_before + growth_pending
        or max(data_after - baseline, 0) > maximum_data
        or settled_wal > cap.max_wal_bytes):
        raise RuntimeError("provider_directory_profile_capacity_window_data_or_wal_overrun")
    return settled_wal


async def _publish_window_settlement(fhir, admission, owner, relation_name,
                                    control_pending, relation_pending, actual_wal, settled_wal):
    """Validate candidate settlement before releasing any uncertain exposure."""
    tracker = admission.wal_tracker
    async with tracker.lock:
        _, _, remaining_relation = fhir._profile_relation_wal_candidate(admission, {})
        if relation_name is not None:
            remaining_relation += relation_pending - actual_wal
        pending_after = (
            sum(tracker.pending_control_wal_bytes.values()) - control_pending
            + sum(tracker.pending_relation_wal_bytes.values()) - relation_pending
            + tracker.pending_metadata_wal_bytes
        )
        remaining_after = (
            remaining_relation
            + fhir.profile_capacity.remaining_profile_control_wal_bytes(
                admission.control_wal_projection, tracker.accounted_control_operation_counts,
            )
            + admission.geometry.metadata_wal_upper_bound_bytes
            - tracker.accounted_metadata_wal_bytes
        )
        await fhir._validate_profile_total_wal_budget(admission, pending_after, remaining_after)
        tracker.pending_control_wal_bytes.pop(owner, None)
        if relation_name is not None:
            tracker.accounted_relation_wal_bytes[relation_name] = settled_wal
            tracker.pending_relation_wal_bytes.pop(relation_name, None)
            tracker.pending_growth_bytes.pop(relation_name, None)


async def _settle_mutation_window(fhir, admission, owner, relation_name, wal_before, data_before):
    await fhir._profile_capacity_remaining_ms(admission)
    wal_after = await fhir._provider_directory_profile_current_wal_bytes(admission)
    actual_wal = wal_after - wal_before
    tracker = admission.wal_tracker
    control_pending = tracker.pending_control_wal_bytes.get(owner, 0)
    relation_pending = tracker.pending_relation_wal_bytes.get(relation_name, 0)
    if not 0 <= actual_wal <= relation_pending + control_pending:
        raise RuntimeError("provider_directory_profile_capacity_window_wal_overrun")
    settled_wal = (await _validate_window_growth(fhir, admission, relation_name, data_before, actual_wal)
                   if relation_name is not None else None)
    await _publish_window_settlement(fhir, admission, owner, relation_name, control_pending,
                                     relation_pending, actual_wal, settled_wal)


@contextlib.asynccontextmanager
async def mutation_window(fhir, relation_name, relation_refs=()):
    """Settle a quiescent successful interval and poison uncertain failures."""
    admission = fhir._provider_directory_profile_capacity_admission()
    if admission is None or not admission.geometry.bounded_admission:
        yield
        return
    current = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
    if current is not None:
        if relation_name is not None and relation_name != current[1]:
            raise RuntimeError("provider_directory_profile_capacity_nested_window_invalid")
        yield
        return
    tracker = admission.wal_tracker
    # ponytail: serialize mutation windows to attribute each global LSN interval;
    # concurrent settlement needs an independently proven attribution method.
    async with tracker.mutation_lock:
        if relation_name is not None and tracker.unresolved_window:
            raise RuntimeError("provider_directory_profile_capacity_window_unresolved")
        owner = object()
        await _claim_mutation_window(fhir, tracker, owner, relation_name, relation_refs)
        token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((owner, relation_name))
        try:
            await fhir._profile_capacity_remaining_ms(admission)
            wal_before = await fhir._provider_directory_profile_current_wal_bytes(admission)
            data_before = (await fhir._provider_directory_profile_capacity_relation_bytes(
                tracker.relation_refs_by_class[relation_name],
            ) if relation_name is not None else 0)
            yield
            # Callers exit only after every worker commit/abort path returns.
            # Rollback alone never reaches this settlement path.
            await _settle_mutation_window(fhir, admission, owner, relation_name, wal_before, data_before)
        except BaseException:
            tracker.unresolved_window = True
            raise
        finally:
            fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


async def _reserve_scratch_projection(fhir, admission, relation_name, relation_ref, projection):
    relation_bytes = await fhir._provider_directory_profile_capacity_relation_bytes((relation_ref,))
    relation_cap = fhir._provider_directory_profile_capacity_relation_cap(admission, relation_name)
    if admission.geometry.bounded_admission:
        await fhir._reserve_profile_capacity_growth(admission, relation_name, projection.growth_bytes)
    elif relation_bytes + projection.growth_bytes > relation_cap.max_scratch_bytes:
        raise RuntimeError(
            "provider_directory_profile_capacity_scratch_growth_projected:"
            f"{relation_name}:observed={relation_bytes}:"
            f"projected={projection.growth_bytes}:"
            f"maximum={relation_cap.max_scratch_bytes}"
        )
    await fhir._reserve_provider_directory_profile_wal_budget(
        admission, relation_wal_bytes={relation_name: projection.wal_bytes},
    )


async def project_scratch_window(fhir, relation_name, relation_ref, relation_oid, *,
                                 inserted_rows, inserted_logical_bytes, expected_persistence):
    """Observe the scratch layout and reserve data and WAL before mutation."""
    admission = fhir._provider_directory_profile_capacity_admission()
    if admission is None:
        raise RuntimeError("provider_directory_profile_capacity_admission_missing")
    layout = await fhir._provider_directory_profile_relation_storage_fingerprint(
        relation_oid, expected_persistence=expected_persistence,
    )
    if layout.relation_oid != relation_oid:
        raise fhir.ProviderDirectoryArtifactBuildStale(
            "provider_directory_profile_capacity_scratch_oid_changed",
        )
    projection = fhir.profile_capacity.project_profile_scratch_capacity(
        admission.geometry,
        fhir.profile_capacity.ProviderDirectoryProfileScratchInput(
            relation_name=relation_name, inserted_rows=inserted_rows,
            inserted_logical_bytes=inserted_logical_bytes,
            toastable_column_count=len(layout.toastable_columns),
            main_index_pages=layout.main_index_pages, toast_index_pages=layout.toast_index_pages,
        ),
    )
    await _reserve_scratch_projection(fhir, admission, relation_name, relation_ref, projection)
    return projection


async def assert_wal_budget(fhir, admission):
    """Require observed WAL plus every unspent bound to fit the lease."""

    tracker = admission.wal_tracker
    async with tracker.lock:
        remaining_control_wal_bytes = (
            fhir.profile_capacity.remaining_profile_control_wal_bytes(
                admission.control_wal_projection,
                tracker.accounted_control_operation_counts,
            )
        )
        remaining_relation_wal_bytes = sum(
            relation_cap.max_wal_bytes
            - tracker.accounted_relation_wal_bytes.get(
                relation_cap.relation_name,
                0,
            )
            - (tracker.pending_relation_wal_bytes.get(relation_cap.relation_name, 0)
               if admission.geometry.bounded_admission else 0)
            for relation_cap in admission.geometry.relation_byte_caps
        )
        remaining_metadata_wal_bytes = (
            admission.geometry.metadata_wal_upper_bound_bytes
            - tracker.accounted_metadata_wal_bytes
        )
        observed_wal_bytes = (
            await fhir._provider_directory_profile_current_wal_bytes(admission)
        )
        maximum_wal_bytes = (
            admission.geometry.reservation_bytes_by_storage_class["wal"]
        )
        if (
            observed_wal_bytes
            + (sum(tracker.pending_control_wal_bytes.values())
               + sum(tracker.pending_relation_wal_bytes.values())
               + tracker.pending_metadata_wal_bytes
               if admission.geometry.bounded_admission else 0)
            + remaining_control_wal_bytes
            + remaining_relation_wal_bytes
            + remaining_metadata_wal_bytes
            > maximum_wal_bytes
        ):
            raise RuntimeError(
                "provider_directory_profile_capacity_total_wal_exceeded"
            )


async def relation_bytes(fhir, relation_refs):
    """Measure distinct relation extents and reject missing physical targets."""
    normalized_refs = sorted(set(relation_refs))
    if not normalized_refs:
        return 0
    size_row = await fhir.db.first(
        """
        WITH selected(relation_ref) AS (
            SELECT unnest(CAST(:relation_refs AS text[]))
        ), measured AS (
            SELECT relation_ref,
                   to_regclass(relation_ref) AS relation_oid
              FROM selected
        )
        SELECT bool_and(relation_oid IS NOT NULL) AS all_present,
               COALESCE(
                   SUM(pg_total_relation_size(relation_oid)),
                   0
               )::bigint AS total_bytes
          FROM measured;
        """,
        relation_refs=normalized_refs,
    )
    if size_row is None:
        raise RuntimeError(
            "provider_directory_profile_capacity_relation_size_missing"
        )
    size_map = fhir._pagination_checkpoint_row_mapping(size_row)
    if size_map.get("all_present") is not True:
        raise fhir.ProviderDirectoryArtifactBuildStale(
            "provider_directory_profile_capacity_relation_missing"
        )
    total_bytes = int(size_map["total_bytes"])
    if total_bytes < 0:
        raise RuntimeError(
            "provider_directory_profile_capacity_relation_size_invalid"
        )
    return total_bytes


async def reserve_growth(fhir, admission, relation_name, growth_bytes):
    """Include every relation and pending write in one signed data class."""
    tracker = admission.wal_tracker
    if not isinstance(growth_bytes, int) or isinstance(growth_bytes, bool) or growth_bytes < 0:
        raise RuntimeError("provider_directory_profile_capacity_growth_reservation_invalid")
    window = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
    if window is None or window[1] != relation_name or tracker.unresolved_window:
        raise RuntimeError("provider_directory_profile_capacity_relation_window_required")
    observed = await fhir._provider_directory_profile_capacity_relation_bytes(
        tracker.relation_refs_by_class[relation_name]
    )
    cap = fhir._provider_directory_profile_capacity_relation_cap(admission, relation_name)
    pending = tracker.pending_growth_bytes.get(relation_name, 0) + growth_bytes
    baseline = tracker.target_bytes_before.get(relation_name, 0)
    if growth_bytes < 0 or max(observed - baseline, 0) + pending > (
        cap.max_scratch_bytes + cap.max_target_growth_bytes
    ):
        raise RuntimeError("provider_directory_profile_capacity_window_growth_projected")
    tracker.pending_growth_bytes[relation_name] = pending


async def assert_relation_guards(fhir, relation_map, triggers, relation_oid, attributes, indexes, constraints, trigger_expectations):
    """Validate the exact installed guard catalog before fingerprinting a layout."""
    expected_user_trigger_count, expected_immutable_trigger_error, expected_single_use_receipt = trigger_expectations
    dependencies = None
    if (
        expected_user_trigger_count == 0
        and relation_map.get("relation_name") == "provider_directory_profile_serving_generation"
        and int(relation_map.get("user_trigger_count") or 0) in {2, 3}
    ):
        from process.provider_directory_cms_receipt_guard import assert_profile_receipt_guards

        await assert_profile_receipt_guards(fhir.db, relation_map, triggers)
        if int(relation_map.get("user_trigger_count") or 0) == 3:
            from process.provider_directory_profile_initial_guards import initial_guard_dependencies

            dependencies = await initial_guard_dependencies(fhir.db, relation_map["schema_name"])
    elif relation_map.get("relation_name") == fhir.profile_initial_contract.RECEIPT_TABLE:
        from process.provider_directory_profile_initial_guards import assert_initial_receipt_catalog, assert_initial_receipt_guards

        await assert_initial_receipt_guards(fhir.db, relation_map, triggers)
        assert_initial_receipt_catalog(relation_oid, attributes, indexes, constraints)
        from process.provider_directory_profile_initial_guards import initial_guard_dependencies

        dependencies = await initial_guard_dependencies(fhir.db, relation_map["schema_name"])
    elif relation_map.get("relation_name") == fhir.ImportRun.__tablename__ and triggers:
        from process.provider_directory_import_run_guards import assert_import_run_guards

        dependencies = await assert_import_run_guards(fhir.db, relation_map, triggers)
    else:
        fhir._assert_profile_capacity_trigger_shape(
            triggers,
            expected_user_trigger_count,
            expected_immutable_trigger_error,
            expected_single_use_receipt,
        )
    return dependencies
