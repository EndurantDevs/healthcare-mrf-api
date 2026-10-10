# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issue signed monitored budgets for one exact CMS serving preparation.

Reviewed ceilings reserve storage; they do not predict the complete native heap,
index or WAL footprint. Execution checks actual owned storage before growth and
logging. The unchanged, separately signed Profile admission keeps its own geometry.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

from process import provider_directory_cms_capacity_contract as contract
from process import provider_directory_cms_serving_receipt as receipts
from process import provider_directory_profile_capacity_runtime as capacity_runtime
from process.provider_directory_cms_address import cms_address_preparation
from process.provider_directory_cms_desired_fence import prepare_desired_fence
from process.provider_directory_cms_native_inputs import (
    assert_native_address_input_fence,
    capture_native_address_input_fence,
    register_native_address_inputs,
)
from process.provider_directory_cms_nonprofile_capacity import (
    _assert_runtime_storage,
    _database_observation,
    _validate_plan,
)
from process.provider_directory_cms_preparation import NonprofileAdmissionPlan, desired_fence_hash
from process.provider_directory_profile_capacity_preflight_contract import (
    CAPACITY_QUIESCENCE_CONTRACT_ID,
    CAPACITY_QUIESCENCE_DIGEST_DOMAIN,
    assert_preflight_expiry,
    preflight_domain_sha256,
    profile_execution_identity_payload,
    utc_second_text,
)
from process.provider_directory_profile_runtime_observation import (
    assert_capacity_lease_matches_runtime_observation,
    observe_profile_runtime,
)
from process.provider_directory_profile_temp_limit import apply_temp_file_limit


@dataclass(frozen=True)
class _PreflightInputs:
    """Keep full-read evidence until fresh bounded checks admit its receipt."""

    plan: NonprofileAdmissionPlan
    fence: Any
    artifact_projection: Any
    native_dependencies: dict
    native_input_fence: dict
    runtime: dict
    database_binding: dict
    serving: Any


def _error(reason):
    return RuntimeError("provider_directory_cms_capacity_" + reason)


async def _runtime_settings(session, limits, database):
    """Bound every preflight backend; these reads never inherit unbounded temp."""
    await apply_temp_file_limit(database, limits["temp_file_limit_bytes_per_backend"])
    await session.execute(text("SET LOCAL max_parallel_workers_per_gather=0"))
    await session.execute(text("SET LOCAL max_parallel_maintenance_workers=0"))
    await session.execute(text("SET LOCAL lock_timeout='5s'"))


async def _verified_pair(fhir, request):
    """Require the original current Profile signature before any registration."""
    now = await fhir._profile_capacity_preflight_clock()
    profile_lease = contract.verified_cms_paired_profile_lease(
        request, trust=capacity_runtime.configured_capacity_lease_trust(), now=now
    )
    seconds = request.cms_nonprofile_admission["limits"]["required_build_seconds"]
    if (profile_lease.max_build_deadline - now).total_seconds() < seconds:
        raise _error("paired_profile_deadline_too_short")
    return profile_lease


async def _register_inputs(fhir):
    """Commit additive revision guards before the read-only preparation snapshot."""
    async with fhir.db.transaction() as session:
        await session.execute(text("SET LOCAL lock_timeout='5s'"))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        await register_native_address_inputs(session, fhir._schema())


def _monitored_plan(request, profile_lease, fence, projection, address, artifact_targets, resource_types):
    """Reserve reviewed ceilings, binding exact inputs without claiming predicted growth."""
    limits = request.cms_nonprofile_admission["limits"]
    if limits["max_wal_bytes"] < 2:
        raise _error("phase_wal_budget_too_small")
    preparation_wal_budget = limits["max_wal_bytes"] // 2
    plan = NonprofileAdmissionPlan(
        selection_proof_id=request.execution.attestation.proof_id,
        desired_profile_as_of=request.execution.attestation.desired_profile_as_of,
        desired_fence_hash=desired_fence_hash(fence),
        artifact_scope_projection_hash=projection.projection_hash,
        publish_targets=tuple(sorted(artifact_targets)),
        resource_types=tuple(sorted(resource_types)),
        batch_size=limits["batch_size"],
        worker_count=limits["worker_count"],
        reservation_bytes=tuple((name, limits["max_" + name + "_bytes"]) for name in ("data", "temp", "wal")),
        minimum_remaining_bytes=limits["minimum_remaining_bytes"],
        required_build_seconds=limits["required_build_seconds"],
        native_address_targets=address.native_targets,
        native_address_input_hash=address.input_hash,
        temp_file_limit_bytes_per_backend=limits["temp_file_limit_bytes_per_backend"],
        # The legacy logging name covers admission, scratch and logging as one phase.
        # Both fields are monitored ceilings, not theoretical upper bounds.
        logging_wal_upper_bound_bytes=preparation_wal_budget,
        cutover_wal_upper_bound_bytes=limits["max_wal_bytes"] - preparation_wal_budget,
        paired_profile_lease_digest=profile_lease.lease_digest,
    )
    _validate_plan(plan)
    contract.assert_cms_geometry_matches_request(plan, request)
    return plan


def _database_binding(observed):
    return {
        **{name: observed[name] for name in ("database_system_identifier", "database_oid", "database_name")},
        "tablespace_oid": observed["data_tablespace_oid"],
        "tablespace_name": observed["data_tablespace_name"],
    }


async def _paired_preflight(fhir, profile_lease, *, lock=False):
    """Exclude only a still-open durable receipt matching the entire verified pair."""
    row = await fhir.db.first(
        f"SELECT * FROM {fhir._profile_capacity_preflight_receipt_ref(fhir._schema())} "
        "WHERE receipt_sha256=:receipt_sha256" + (" FOR UPDATE" if lock else ""),
        receipt_sha256=profile_lease.nonce,
    )
    if row is None:
        raise _error("paired_preflight_missing")
    row = fhir._pagination_checkpoint_row_mapping(row)
    receipt = fhir._profile_capacity_preflight_stored_receipt(row, profile_lease)
    if receipt != profile_lease.signing_preflight_guard["healthcare_receipt"]:
        raise _error("paired_preflight_changed")
    fhir._assert_profile_capacity_receipt_open(row, profile_lease, await fhir._profile_capacity_preflight_clock())
    return receipt


async def _read_inputs(fhir, request, profile_lease):
    """Compute the full selected projection and native pins in one repeatable snapshot."""
    execution, schema = request.execution, fhir._schema()
    limits = request.cms_nonprofile_admission["limits"]
    artifact_targets = set(fhir.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS) - {"profile", "corroboration"}
    resource_types = fhir._provider_directory_artifact_resource_types(artifact_targets, publish_corroboration=False)
    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await _runtime_settings(session, limits, fhir.db)
        await fhir.assert_profile_selection_current_in_transaction(
            execution.attestation, fhir._provider_directory_profile_selection_catalog()
        )
        await _paired_preflight(fhir, profile_lease)
        fence = await prepare_desired_fence(fhir, execution, run_id=None, metrics={}, publish_targets=artifact_targets)
        projection = await fhir._provider_directory_artifact_scope_exact_projection(
            schema, fence, resource_types, source_fence=fence, batch_size=limits["batch_size"]
        )
        fhir._assert_provider_directory_artifact_scope_exact_capacity(projection)
        dependencies = await receipts.capture_native_dependencies(session, schema)
        native_input_fence = await capture_native_address_input_fence(session, schema)
        # The factory is not invoked here. Its run id is deliberately absent from its input hash.
        address = cms_address_preparation(
            fhir,
            execution,
            fence,
            dependencies,
            native_input_fence=native_input_fence,
            run_id="run_" + uuid.uuid4().hex,
            worker_count=limits["worker_count"],
            temp_file_limit_bytes_per_backend=limits["temp_file_limit_bytes_per_backend"],
        )
        if "registry_source_retention" in request.cms_nonprofile_admission:
            address = address.with_registry_source_retention(
                request.cms_nonprofile_admission["registry_source_retention"]
            )
        plan = _monitored_plan(request, profile_lease, fence, projection, address, artifact_targets, resource_types)
        observed = await _database_observation(fhir, plan)
        _assert_runtime_storage(plan, profile_lease, observed)
        runtime = await observe_profile_runtime(fhir.db)
        assert_capacity_lease_matches_runtime_observation(profile_lease, runtime)
        serving = await fhir._profile_capacity_preflight_serving(schema)
        return _PreflightInputs(
            plan, fence, projection, dependencies, native_input_fence, runtime, _database_binding(observed), serving
        )


def _projection_fields(request, inputs):
    projection = inputs.artifact_projection
    return {
        "request_contract_id": request.request_payload["contract_id"],
        "request_sha256": request.request_sha256,
        "profile_execution_identity": profile_execution_identity_payload(request),
        "capacity_limits": request.limits_payload,
        "capacity_limits_sha256": request.limits_sha256,
        "capacity_geometry_hash": inputs.plan.capacity_geometry_hash,
        "capacity_geometry": inputs.plan.payload,
        "required_reservation_bytes_by_storage_class": dict(inputs.plan.reservation_bytes),
        "artifact_scope_projection": {
            "projected_rows": projection.projected_rows,
            "projected_logical_bytes": projection.projected_logical_bytes,
            "projection_hash": projection.projection_hash,
        },
        "runtime_observation": inputs.runtime,
        "serving_generation_preflight": inputs.serving.payload,
        "serving_generation_preflight_sha256": inputs.serving.payload_sha256,
        "database_binding": inputs.database_binding,
    }


async def _quiescence(fhir, request, profile_lease, observed_at):
    """Retain all existing owner boundaries while allowing the exact pending Profile pair."""
    query = fhir._profile_capacity_quiescence_sql(fhir._schema()).replace(
        "AND request_sha256 <> :request_sha256",
        "AND request_sha256 <> :request_sha256 AND request_sha256 <> :paired_request_sha256",
    )
    quiescence_row = await fhir.db.first(
        query,
        active_statuses=list(fhir._PROFILE_ACTIVE_RUN_STATUSES),
        profile_params=json.dumps(
            {
                "provider_directory_profile_contract_id": fhir.PROFILE_EXECUTION_CONTRACT_ID,
                "publish_artifacts_only": True,
                "publish_artifacts_targets": ["profile"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        current_run_id=None,
        observed_at=observed_at,
        request_sha256=request.request_sha256,
        paired_request_sha256=profile_lease.signing_preflight_guard["healthcare_receipt"]["request_sha256"],
    )
    if quiescence_row is None:
        raise _error("quiescence_missing")
    counts_by_boundary = dict(fhir._pagination_checkpoint_row_mapping(quiescence_row))
    if set(counts_by_boundary) != {
        "active_profile_run_count",
        "claimed_profile_checkpoint_count",
        "unexpired_capacity_consumption_count",
        "outstanding_preflight_receipt_count",
    } or any(type(count) is not int or count != 0 for count in counts_by_boundary.values()):
        raise _error("not_quiescent")
    payload_by_field = {
        "contract_id": CAPACITY_QUIESCENCE_CONTRACT_ID,
        **counts_by_boundary,
        "active_profile_run_statuses": list(fhir._PROFILE_ACTIVE_RUN_STATUSES),
        "claimed_checkpoint_states": ["building_evidence", "evidence_complete", "building_profile", "ready"],
        "capacity_consumption_boundary": "unexpired",
        "preflight_receipt_boundary": "unconsumed_and_unexpired",
    }
    return payload_by_field, preflight_domain_sha256(CAPACITY_QUIESCENCE_DIGEST_DOMAIN, payload_by_field)


async def _assert_current_inputs(fhir, request, inputs, profile_lease, session):
    """Recheck bounded identity after expensive reads and hold it through receipt issuance."""
    await fhir.assert_profile_selection_current_in_transaction(
        request.execution.attestation, fhir._provider_directory_profile_selection_catalog()
    )
    await fhir._lock_and_verify_artifact_dataset_fence(inputs.fence)
    await assert_native_address_input_fence(session, fhir._schema(), inputs.native_input_fence)
    await receipts.assert_native_dependencies(session, fhir._schema(), inputs.native_dependencies)
    observed = await _database_observation(fhir, inputs.plan)
    _assert_runtime_storage(inputs.plan, profile_lease, observed)
    runtime = await observe_profile_runtime(fhir.db)
    serving = await fhir._profile_capacity_preflight_serving(fhir._schema())
    if (
        runtime != inputs.runtime
        or _database_binding(observed) != inputs.database_binding
        or serving.payload != inputs.serving.payload
        or serving.payload_sha256 != inputs.serving.payload_sha256
    ):
        raise _error("preflight_inputs_changed")


async def _issue_receipt(fhir, request, inputs, profile_lease):
    """Persist a CMS-purpose receipt through the existing serialized single-use ledger."""
    schema = fhir._schema()
    async with fhir.db.transaction() as session:
        # Fresh statement snapshots see writers committed before the NOWAIT locks.
        # Existing admission/table locks serialize every ledger read and write.
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        await _runtime_settings(session, request.cms_nonprofile_admission["limits"], fhir.db)
        await _assert_current_inputs(fhir, request, inputs, profile_lease, session)
        await fhir._lock_profile_capacity_preflight_state(schema)
        if (await _verified_pair(fhir, request)).lease_digest != profile_lease.lease_digest:
            raise _error("paired_profile_changed")
        await _paired_preflight(fhir, profile_lease, lock=True)
        existing = await fhir._profile_capacity_preflight_existing_receipt(schema, request)
        observed_at = await fhir._profile_capacity_preflight_clock()
        quiescence, quiescence_hash = await _quiescence(fhir, request, profile_lease, observed_at)
        issued_at = fhir._profile_capacity_receipt_issued_at(existing, observed_at)
        assert_preflight_expiry(request, issued_at=issued_at)
        if request.expires_at > profile_lease.expires_at:
            raise _error("paired_profile_expiry_too_short")
        receipt_by_field = {
            "contract_id": contract.CMS_PREFLIGHT_CONTRACT,
            **_projection_fields(request, inputs),
            "request_nonce": request.request_nonce,
            "control_plane_receipt_sha256": request.control_plane_receipt_sha256,
            "issued_at": utc_second_text(issued_at),
            "expires_at": utc_second_text(request.expires_at),
            "quiescence": quiescence,
            "quiescence_sha256": quiescence_hash,
            "preflight_receipt_storage": await fhir._profile_capacity_preflight_receipt_layout(schema),
        }
        receipt_by_field["receipt_sha256"] = preflight_domain_sha256(contract.CMS_PREFLIGHT_CONTRACT, receipt_by_field)
        fhir._checked_serialized_metadata_payload_bytes(receipt_by_field, fixed_row_overhead=4_096)
        return await fhir._persist_or_replay_capacity_receipt(
            schema, request, receipt_by_field, existing, issued_at=issued_at, observed_at=observed_at
        )


async def _produce(fhir, request, *, issue):
    """Keep registration, read-only observation and bounded issuance in separate transactions."""
    if fhir.db._transaction_binding() is not None or fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get():
        raise _error("own_transaction_required")
    profile_lease = await _verified_pair(fhir, request)
    token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(request.execution)
    from process import provider_directory_profile_initial as initial
    from process.provider_directory_profile_initial_contract import initial_profile_for_request

    initial_token = initial.REQUESTED.set(initial_profile_for_request(request))
    try:
        await _register_inputs(fhir)
        inputs = await _read_inputs(fhir, request, profile_lease)
        if issue:
            return await _issue_receipt(fhir, request, inputs, profile_lease)
        result_by_field = {"contract_id": contract.CMS_PROJECTION_CONTRACT, **_projection_fields(request, inputs)}
        result_by_field["authority_projection_sha256"] = preflight_domain_sha256(
            contract.CMS_PROJECTION_CONTRACT, result_by_field
        )
        return result_by_field
    finally:
        initial.REQUESTED.reset(initial_token)
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)


async def capacity_authority_projection(fhir, request):
    """Project monitored budgets and exact inputs without creating a signing receipt."""
    return await _produce(fhir, request, issue=False)


async def capacity_preflight(fhir, request):
    """Issue the independently signed CMS receipt while preserving its Profile pair."""
    return await _produce(fhir, request, issue=True)
