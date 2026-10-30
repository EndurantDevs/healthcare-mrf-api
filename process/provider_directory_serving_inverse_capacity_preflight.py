# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issue standalone inverse receipts through the existing native preflight ledger."""

import asyncio
from contextlib import asynccontextmanager

from sqlalchemy import text

from process import provider_directory_serving_inverse_capacity_contract as contract
from process import provider_directory_serving_inverse_capacity_inputs as inputs
from process import provider_directory_serving_inverse_capacity_signing as signing
from process.provider_directory_cms_nonprofile_capacity import _read_database_observation
from process.provider_directory_output_snapshot_catalog import decoded
from process.provider_directory_profile_runtime_observation import observe_profile_runtime


async def configure_preparation(fhir, limits):
    """Bound the actual original backend; preparation keeps its separate existing wall."""
    settings_by_field = dict(
        max_parallel_workers_per_gather="0",
        max_parallel_maintenance_workers="0",
        temp_file_limit=str(limits["temp_file_limit_bytes_per_backend"] // 1024) + "kB",
        lock_timeout=str(limits["lock_timeout_ms"]) + "ms",
        statement_timeout=str(limits["max_prepare_seconds"] * 1000) + "ms",
    )
    for name, value in settings_by_field.items():
        await fhir.db.scalar("SELECT set_config(:name,:value,true)", name=name, value=value)


@asynccontextmanager
async def _owner(fhir, request, *, readonly):
    contract.require(fhir.db._transaction_binding() is None, "preflight_owner")
    async with asyncio.timeout(request.limits_payload["max_prepare_seconds"]), fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" + (" READ ONLY" if readonly else "")))
        await configure_preparation(fhir, request.limits_payload)
        yield session


def geometry(request, capture, observation, native):
    """Sign existing measured aggregate ceilings, never a B-tree forecast for other methods."""
    limits = request.limits_payload
    value_by_field = dict(
        contract_id=contract.GEOMETRY,
        admission_purpose=contract.PURPOSE,
        input_sha256=capture["input_sha256"],
        scope=capture,
        limits_sha256=request.limits_sha256,
        reservation_bytes=[[kind, limits["max_" + kind + "_bytes"]] for kind in ("data", "temp", "wal")],
        minimum_remaining_bytes=limits["minimum_remaining_bytes"],
        required_build_seconds=limits["max_prepare_seconds"],
        temp_file_limit_bytes_per_backend=limits["temp_file_limit_bytes_per_backend"],
        preparation_wal_upper_bound_bytes=limits["max_preparation_wal_bytes"],
        cutover_wal_upper_bound_bytes=limits["max_cutover_wal_bytes"],
        commit_wal_upper_bound_bytes=limits["max_commit_wal_bytes"],
        postgres_block_size_bytes=native["block_size"],
        wal_segment_size_bytes=native["wal_segment_size"],
        data_tablespace_oid=observation["data_tablespace_oid"],
        data_tablespace_name=observation["data_tablespace_name"],
        temp_tablespace_oid=observation["temp_tablespace_oid"],
        temp_tablespace_name=observation["temp_tablespace_name"],
        worker_count=1,
    )
    return contract.validated_inverse_geometry(value_by_field, limits=limits, execution=request.recovery_execution)


async def _native_sizes(fhir):
    row = await fhir.db.first(
        "SELECT current_setting('block_size')::bigint AS block_size,pg_size_bytes(current_setting('wal_segment_size'))::bigint AS wal_segment_size,current_setting('wal_block_size')::bigint AS wal_block_size"
    )
    contract.require(row is not None, "native_sizes")
    return dict(fhir._pagination_checkpoint_row_mapping(row))


async def _quiescence(fhir, request, *, run_id=None):
    now = await fhir._profile_capacity_preflight_clock()
    value, _digest = await fhir._profile_capacity_quiescence(
        fhir._schema(), observed_at=now, request_sha256=request.request_sha256, current_run_id=run_id
    )
    inverse_count = await fhir.db.scalar(
        f"SELECT count(*) FROM {fhir._unscoped_qt(fhir._schema(), fhir.ImportRun.__tablename__)} WHERE importer='provider-directory-fhir' AND status=ANY(CAST(:statuses AS text[])) AND params::jsonb->>'provider_directory_serving_operation'='recover' AND (CAST(:run AS text) IS NULL OR run_id<>:run)",
        statuses=list(fhir._PROFILE_ACTIVE_RUN_STATUSES),
        run=run_id,
    )
    contract.require(type(inverse_count) is int and inverse_count == 0, "active_inverse")
    return {**value, "contract_id": contract.HEALTHCARE_QUIESCENCE, "active_inverse_run_count": inverse_count}


async def _capture_geometry(fhir, request, existing=None):
    captured = await inputs.capture_inverse_inputs(fhir, request.recovery_execution)
    scope = captured.payload
    if existing is not None:
        previous = decoded(existing["receipt_json"])["capacity_geometry"]["scope"]
        contract.require(inputs.has_same_captured_inputs(previous, scope), "preflight_replay_changed")
        scope = previous
    observation = await _read_database_observation(fhir)
    await inputs.assert_scope_tablespace(fhir, scope, observation["data_tablespace_oid"])
    native = await _native_sizes(fhir)
    plan = geometry(request, scope, observation, native)
    from process.provider_directory_serving_inverse_commit import assert_commit_reserve

    await assert_commit_reserve(fhir, plan, native)
    return plan, await observe_profile_runtime(fhir.db)


async def _inverse_capacity_authority_projection(fhir, request):
    """Observe actual inverse authority inputs without inserting or consuming a receipt."""
    contract.require(
        type(request) is contract.InverseCapacityPreflightRequest and request.request_nonce is None,
        "projection_request",
    )
    async with _owner(fhir, request, readonly=True):
        plan, runtime = await _capture_geometry(fhir, request)
        value_by_field = dict(
            contract_id=contract.PROJECTION_RECEIPT,
            request_contract_id=contract.PROJECTION_REQUEST,
            request_sha256=request.request_sha256,
            recovery_execution_identity=request.recovery_execution,
            operation_sha256=contract.operation_sha256(request.recovery_execution),
            capacity_limits=request.limits_payload,
            capacity_limits_sha256=request.limits_sha256,
            capacity_geometry_hash=signing.geometry_sha256(plan),
            capacity_geometry=plan,
            required_reservation_bytes_by_storage_class=dict(plan["reservation_bytes"]),
            runtime_observation=runtime,
        )
        value_by_field["authority_projection_sha256"] = contract.digest(contract.PROJECTION_RECEIPT, value_by_field)
        return value_by_field


async def _inverse_capacity_preflight(fhir, request):
    """Serialize and exactly replay the same existing single-use preflight ledger row."""
    contract.require(
        type(request) is contract.InverseCapacityPreflightRequest and request.request_nonce is not None,
        "preflight_request",
    )
    async with _owner(fhir, request, readonly=False):
        await fhir._lock_profile_capacity_preflight_state(fhir._schema())
        existing = await fhir._profile_capacity_preflight_existing_receipt(fhir._schema(), request)
        plan, runtime = await _capture_geometry(fhir, request, existing)
        quiescence = await _quiescence(fhir, request)
        now = await fhir._profile_capacity_preflight_clock()
        contract.require(now < request.expires_at, "preflight_expired")
        issued = fhir._profile_capacity_receipt_issued_at(existing, now)
        receipt_by_field = dict(
            contract_id=contract.HEALTHCARE_RECEIPT,
            request_contract_id=contract.HEALTHCARE_REQUEST,
            request_sha256=request.request_sha256,
            request_nonce=request.request_nonce,
            control_plane_receipt_sha256=request.control_plane_receipt_sha256,
            issued_at=fhir.utc_second_text(issued),
            expires_at=fhir.utc_second_text(request.expires_at),
            recovery_execution_identity=request.recovery_execution,
            capacity_limits=request.limits_payload,
            capacity_limits_sha256=request.limits_sha256,
            capacity_geometry_hash=signing.geometry_sha256(plan),
            capacity_geometry=plan,
            required_reservation_bytes_by_storage_class=dict(plan["reservation_bytes"]),
            runtime_observation=runtime,
            quiescence=quiescence,
            quiescence_sha256=contract.digest(contract.HEALTHCARE_QUIESCENCE, quiescence),
            preflight_receipt_storage=await fhir._profile_capacity_preflight_receipt_layout(fhir._schema()),
        )
        receipt_by_field["receipt_sha256"] = contract.digest(contract.HEALTHCARE_RECEIPT, receipt_by_field)
        fhir._checked_serialized_metadata_payload_bytes(receipt_by_field, fixed_row_overhead=4096)
        return await fhir._persist_or_replay_capacity_receipt(
            fhir._schema(), request, receipt_by_field, existing, issued_at=issued, observed_at=now
        )


def receipt_row_values(request, receipt, *, issued_at):
    """Reuse existing provenance columns without claiming a new Profile publication."""
    contract.require(
        type(request) is contract.InverseCapacityPreflightRequest
        and receipt["contract_id"] == contract.HEALTHCARE_RECEIPT,
        "receipt_row",
    )
    plan = contract.validated_inverse_geometry(
        receipt["capacity_geometry"], limits=request.limits_payload, execution=request.recovery_execution
    )
    provenance = plan["scope"]["profile_provenance"]
    return dict(
        receipt_sha256=receipt["receipt_sha256"],
        request_nonce=request.request_nonce,
        request_sha256=request.request_sha256,
        control_plane_receipt_sha256=request.control_plane_receipt_sha256,
        contract_id=contract.HEALTHCARE_RECEIPT,
        request_contract_id=contract.HEALTHCARE_REQUEST,
        limits_contract_id=contract.LIMITS,
        selection_proof_id=provenance["selection_proof_id"],
        profile_input_digest=provenance["profile_input_digest"],
        control_generation=provenance["control_generation"],
        profile_schema_version=provenance["profile_schema_version"],
        profile_strategy_version=provenance["profile_strategy_version"],
        materialization_mode="serving_inverse",
        limits_sha256=request.limits_sha256,
        capacity_geometry_hash=receipt["capacity_geometry_hash"],
        serving_preflight_sha256=plan["scope"]["input_sha256"],
        quiescence_sha256=receipt["quiescence_sha256"],
        receipt_json=contract.canonical(receipt),
        issued_at=issued_at,
        expires_at=request.expires_at,
        created_at=issued_at,
    )


def read_inverse_receipt_row(row, lease):
    """Authenticate stored single-use bytes/provenance against the verified immutable lease."""
    receipt = decoded(row.get("receipt_json"))
    expected = lease.signing_preflight_guard["healthcare_receipt"]
    contract.require(type(receipt) is dict and receipt == expected, "stored_receipt")
    contract.require(receipt["receipt_sha256"] == lease.nonce == row.get("receipt_sha256"), "stored_nonce")
    contract.require(
        receipt["receipt_sha256"]
        == contract.digest(
            contract.HEALTHCARE_RECEIPT, {key: value for key, value in receipt.items() if key != "receipt_sha256"}
        ),
        "stored_hash",
    )
    request = contract.validated_inverse_preflight_request(lease.signing_preflight_guard["healthcare_request"])
    values = receipt_row_values(request, receipt, issued_at=contract.utc_second(receipt["issued_at"]))
    contract.require(
        all(row.get(key) == value for key, value in values.items() if key != "receipt_json"), "stored_provenance"
    )
    return receipt


async def inverse_capacity_authority_projection(fhir, request):
    """Keep native input drift on the existing authenticated API's stale response."""
    try:
        return await _inverse_capacity_authority_projection(fhir, request)
    except (ValueError, TimeoutError) as error:
        raise fhir.ProviderDirectoryArtifactBuildStale(
            "provider_directory_serving_inverse_projection_changed"
        ) from error


async def inverse_capacity_preflight(fhir, request):
    """Use the same stale response for refused bounded inverse preparation."""
    try:
        return await _inverse_capacity_preflight(fhir, request)
    except (ValueError, TimeoutError) as error:
        raise fhir.ProviderDirectoryArtifactBuildStale(
            "provider_directory_serving_inverse_preflight_changed"
        ) from error
