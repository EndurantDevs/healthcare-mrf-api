# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Initial Profile replacement inside the existing admission and bundle owner."""

from __future__ import annotations

import asyncio
import contextvars
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

from process import provider_directory_profile_initial_contract as contract

REQUESTED = contextvars.ContextVar("profile_initial_publication_requested", default=False)


def requested(fhir):
    """Require explicit initial mode in this operation or its signed execution."""
    return REQUESTED.get() or contract.execution_initial_requested(
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
    )


def targets(identity):
    """Expose the initial physical snapshot or the existing serving predecessor."""
    return identity.initial_targets if identity.initial_targets is not None else identity.serving_state


async def capture_targets(fhir, schema):
    """Observe existing physical targets and authentic history without creating serving state."""
    await _assert_no_publication(fhir, schema)
    evidence_ref = fhir._unscoped_qt(schema, fhir.profile_artifact.PROFILE_EVIDENCE_TABLE)
    profile_ref = fhir._unscoped_qt(schema, fhir.profile_artifact.PROFILE_TABLE)
    profile_oid, evidence_oid = await fhir._locked_profile_adoption_target_oids(schema, profile_ref, evidence_ref)
    layouts = [
        await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        for oid in (evidence_oid, profile_oid)
    ]
    history_by_field_name, evidence_rows, profile_rows = await _capture_legacy_history(
        fhir, schema, profile_ref, evidence_ref
    )
    target_payload = contract.validated_target_state(
        {
            "contract_id": contract.TARGET_CONTRACT,
            "resolution": "legacy_as_of_unknown" if history_by_field_name else "empty",
            "serving_singleton_absent": True,
            "initial_commit_receipt_absent": True,
            "evidence_target_oid": evidence_oid,
            "profile_target_oid": profile_oid,
            "evidence_target_storage_fingerprint": layouts[0].exact_fingerprint,
            "profile_target_storage_fingerprint": layouts[1].exact_fingerprint,
            "evidence_target_bytes": int(
                await fhir.db.scalar("SELECT pg_total_relation_size(CAST(:oid AS oid))", oid=evidence_oid)
            ),
            "profile_target_bytes": int(
                await fhir.db.scalar("SELECT pg_total_relation_size(CAST(:oid AS oid))", oid=profile_oid)
            ),
            "evidence_rows": evidence_rows,
            "profile_rows": profile_rows,
            "historical_publication": history_by_field_name,
        }
    )
    return contract.InitialTargets(
        evidence_target_oid=evidence_oid, profile_target_oid=profile_oid, payload=target_payload
    )


async def _assert_no_publication(fhir, schema):
    """Check bounded singleton and receipt absence without scanning incumbent facts."""
    if await fhir._provider_directory_profile_serving_state(schema) is not None:
        raise RuntimeError("provider_directory_profile_initial_serving_exists")
    common_ref = fhir._unscoped_qt(schema, "provider_directory_cms_serving_receipt")
    if await fhir.db.scalar("SELECT to_regclass(:ref) IS NOT NULL", ref=common_ref) and await fhir.db.scalar(
        f"SELECT EXISTS (SELECT 1 FROM {common_ref})"
    ):
        raise RuntimeError("provider_directory_profile_initial_common_history_exists")
    receipt_ref = fhir._unscoped_qt(schema, contract.RECEIPT_TABLE)
    if await fhir.db.scalar(f"SELECT EXISTS (SELECT 1 FROM {receipt_ref})"):
        raise RuntimeError("provider_directory_profile_initial_receipt_exists")


async def admission_identity(fhir, fence, initial_targets):
    """Build identity without adopting a serving generation for the initial targets."""
    identity = await fhir._profile_build_identity_inputs(
        fhir._schema(), fence, has_existing_artifacts=False, allow_serving_generation_adoption=False
    )
    return bind_identity(fhir, identity, initial_targets)


def bind_identity(fhir, identity, initial_targets):
    """Bind the absent predecessor snapshot into the immutable build identity."""
    return replace(
        identity,
        initial_targets=initial_targets,
        resume_lineage_hash=fhir._identity_hash(
            {
                "contract": "provider-directory-profile-initial-resume.v1",
                "lineage": identity.resume_lineage_hash,
                "initial_target_state_sha256": contract.target_state_sha256(initial_targets.payload),
            }
        ),
    )


async def receipt_layout(fhir, schema):
    """Inspect the logged initial receipt relation and its exact immutable guards."""
    oid = await fhir.db.scalar("SELECT CAST(:ref AS regclass)::oid::bigint", ref=f"{schema}.{contract.RECEIPT_TABLE}")
    return await fhir._provider_directory_profile_relation_storage_fingerprint(
        oid,
        expected_persistence="p",
        expected_user_trigger_count=3,
        expected_immutable_trigger_error="provider_directory_profile_initial_receipt_immutable",
    )


def geometry_inputs(fhir, identity, ordinary, receipt_storage):
    """Bind initial receipt identity only when all of its storage uses the funded tablespace."""
    from dataclasses import asdict

    if receipt_storage.effective_tablespace_oids != (ordinary.tablespace_oid,):
        raise RuntimeError("provider_directory_profile_initial_receipt_tablespace_unsupported")
    return contract.InitialCapacityGeometryInputs(
        **asdict(ordinary),
        initial_target_state_sha256=contract.target_state_sha256(identity.initial_targets.payload),
        initial_receipt_oid=receipt_storage.relation_oid,
        initial_receipt_storage_fingerprint=receipt_storage.exact_fingerprint,
    )


def build_from_stages(fhir, stages, profile_delta=None):
    """Carry exactly the two admitted replacement stages through existing bundle owners."""
    initial_stages = [stage for stage in stages if getattr(stage, "profile_initial_build", None) is not None]
    if not initial_stages:
        if any(
            stage.target_relation in {fhir.profile_artifact.PROFILE_EVIDENCE_TABLE, fhir.profile_artifact.PROFILE_TABLE}
            for stage in stages
        ):
            raise RuntimeError("provider_directory_profile_initial_descriptor_required")
        return None
    build = initial_stages[0].profile_initial_build
    expected_by_target = {
        fhir.profile_artifact.PROFILE_EVIDENCE_TABLE: build.evidence_stage,
        fhir.profile_artifact.PROFILE_TABLE: build.profile_stage,
    }
    if (
        profile_delta is not None
        or len(initial_stages) != 2
        or any(stage.profile_initial_build != build or stage.schema != build.schema for stage in initial_stages)
        or {stage.target_relation: stage.stage_table for stage in initial_stages} != expected_by_target
        or any(stage.target_relation in expected_by_target and stage not in initial_stages for stage in stages)
        or build.materialization_mode != "full_swap"
    ):
        raise RuntimeError("provider_directory_profile_initial_stage_pair_invalid")
    return build


async def lock_metadata(fhir, stages, profile_delta=None):
    """Fence admitted initial metadata before target and source locks."""
    build = build_from_stages(fhir, stages, profile_delta)
    if build is None:
        return
    admission = fhir._provider_directory_profile_capacity_admission()
    if (
        admission is None
        or not isinstance(admission.geometry, contract.InitialCapacityGeometry)
        or admission.admitted_identity is None
        or admission.admitted_identity.initial_targets is None
        or admission.run_id != build.owner_run_id
        or admission.build_id != fhir._provider_directory_profile_build_id(build)
    ):
        raise RuntimeError("provider_directory_profile_initial_admission_required")
    await fhir._profile_capacity_remaining_ms(admission)
    # Same metadata ordering as fresh admission, before aliases, target and source locks.
    await fhir._lock_profile_capacity_preflight_state(build.schema)
    await fhir.db.status(
        f"LOCK TABLE {fhir._unscoped_qt(build.schema, contract.RECEIPT_TABLE)} IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )


def _fence_hash(fhir, fence):
    from dataclasses import asdict

    if fence is None:
        raise RuntimeError("provider_directory_profile_initial_fence_missing")
    return fhir._identity_hash(asdict(fence))


async def _metadata_projection(fhir, admission, receipt_storage, pending_items):
    capacity = fhir.profile_capacity
    serving = await fhir._provider_directory_profile_relation_storage_fingerprint(
        admission.geometry.serving_generation_oid, expected_persistence="p"
    )
    mutations = tuple(
        capacity.ProviderDirectoryProfileMetadataMutationInput(
            relation_name=name,
            operation="insert",
            payload_upper_bytes=capacity.METADATA_PAYLOAD_UPPER_BOUND_BYTES,
            deleted_toast_chunks=0,
            main_index_pages=layout.main_index_pages,
            toast_index_pages=layout.toast_index_pages,
        )
        for name, layout in (("serving_generation", serving), ("initial_receipt", receipt_storage))
    )
    return capacity.project_profile_delta_metadata_capacity(
        admission.geometry, mutations, pending_commit_items=pending_items
    )


async def preparation_timeout_seconds(fhir, stages, timeout_seconds):
    """Let fresh initial census use its finite signed build time before live cutover."""
    if build_from_stages(fhir, stages) is None:
        return timeout_seconds
    return (await fhir._profile_capacity_remaining_ms(fhir._provider_directory_profile_capacity_admission())) / 1000


async def _freeze_targets(fhir, build, admission):
    """Restore signed preparation settings and block incumbent writers before full checks."""
    await fhir._profile_capacity_remaining_ms(admission)
    statement_timeout_ms = (await fhir._profile_capacity_observed_settings()).get("statement_timeout_ms")
    if type(statement_timeout_ms) is not int or statement_timeout_ms <= 0:
        raise RuntimeError("provider_directory_profile_initial_statement_timeout_invalid")
    await fhir._apply_provider_directory_profile_capacity_settings(admission)
    pair = sorted((fhir.profile_artifact.PROFILE_EVIDENCE_TABLE, fhir.profile_artifact.PROFILE_TABLE))
    await fhir.db.status(
        "LOCK TABLE " + ", ".join(fhir._unscoped_qt(build.schema, name) for name in pair) + " IN SHARE MODE NOWAIT"
    )
    identity = admission.admitted_identity
    if await capture_targets(fhir, build.schema) != identity.initial_targets:
        raise RuntimeError("provider_directory_profile_initial_targets_changed")
    await fhir._admission_database_guard(identity, admission.database_identity)
    return statement_timeout_ms


async def prepare_cutover(fhir, stages, fence):
    """Census fresh stages and freeze incumbent facts while keeping readers available."""
    build = build_from_stages(fhir, stages)
    if build is None:
        return None
    admission = fhir._provider_directory_profile_capacity_admission()
    statement_timeout_ms = await _freeze_targets(fhir, build, admission)
    layout = await receipt_layout(fhir, build.schema)
    if (
        layout.relation_oid != admission.geometry.initial_receipt_oid
        or layout.exact_fingerprint != admission.geometry.initial_receipt_storage_fingerprint
        or layout.effective_tablespace_oids != (admission.geometry.tablespace_oid,)
    ):
        raise RuntimeError("provider_directory_profile_initial_receipt_storage_changed")
    initial_stages = [stage for stage in stages if stage.profile_initial_build is not None]
    by_target = {stage.target_relation: stage for stage in initial_stages}
    await fhir._assert_provider_directory_profile_checkpoint_ready(
        build,
        by_target[fhir.profile_artifact.PROFILE_EVIDENCE_TABLE].build_fence,
        by_target[fhir.profile_artifact.PROFILE_TABLE].build_fence,
    )
    await fhir._assert_provider_directory_profile_capacity_consumption(admission, build)
    oid_by_target, counts = await _inspect_cutover_stages(fhir, build, admission, initial_stages)
    if admission.wal_tracker.unresolved_window:
        raise RuntimeError("provider_directory_profile_capacity_window_unresolved")
    projection = await _metadata_projection(fhir, admission, layout, len(stages) + len(fence.datasets) + 2)
    forecast_by_field = {
        "contract_id": contract.FORECAST_CONTRACT,
        "build_id": admission.build_id,
        "capacity_geometry_hash": build.capacity_geometry_hash,
        "initial_target_state_sha256": admission.geometry.initial_target_state_sha256,
        "dataset_fence_sha256": _fence_hash(fhir, fence),
        "new_target_oids": oid_by_target,
        "metadata_data_bytes": projection.data_bytes,
        "metadata_wal_bytes": projection.wal_bytes,
        "commit_envelope_bytes": projection.commit_envelope_bytes,
        "ddl_statement_upper_bound": contract.INITIAL_CUTOVER_STATEMENTS,
    }
    return {
        "build": build,
        "admission": admission,
        "counts": counts,
        "oids": oid_by_target,
        "projection": projection,
        "forecast": forecast_by_field,
        "forecast_hash": fhir._identity_hash(forecast_by_field),
        "statement_timeout_ms": statement_timeout_ms,
        "wal_start": await fhir.db.scalar("SELECT pg_current_wal_insert_lsn()::text"),
    }


async def arm_cutover(fhir, cutover, timeout, fence):
    """Restore the original live ceiling within the remaining signed preparation wall."""
    if cutover is None:
        return
    admission = cutover["admission"]
    remaining_ms = await fhir._profile_capacity_remaining_ms(admission)
    statement_timeout_ms = min(
        cutover["statement_timeout_ms"],
        admission.geometry.statement_timeout_ms,
        remaining_ms - fhir.PROVIDER_DIRECTORY_PROFILE_DEADLINE_COMMIT_RESERVE_MS,
    )
    if timeout is not None:
        deadline = asyncio.get_running_loop().time() + fhir._provider_directory_artifact_transaction_timeout_seconds(
            fence
        )
        timeout.reschedule(min(timeout.when(), deadline))
    await fhir.db.status(f"SET LOCAL statement_timeout='{statement_timeout_ms}ms'")


async def begin_cutover(fhir, cutover, stages):
    """Recheck locked identities and start WAL after owner callbacks, without another census."""
    if cutover is None:
        return
    build, admission = cutover["build"], cutover["admission"]
    await fhir._profile_capacity_remaining_ms(admission)
    await _assert_no_publication(fhir, build.schema)
    for role, name in (
        ("evidence", fhir.profile_artifact.PROFILE_EVIDENCE_TABLE),
        ("profile", fhir.profile_artifact.PROFILE_TABLE),
    ):
        oid = await fhir._provider_directory_relation_oid(build.schema, name)
        expected = admission.admitted_identity.initial_targets.payload
        if oid != expected[role + "_target_oid"]:
            raise RuntimeError("provider_directory_profile_initial_targets_changed")
        layout = await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        if layout.exact_fingerprint != expected[role + "_target_storage_fingerprint"]:
            raise RuntimeError("provider_directory_profile_initial_targets_changed")
    initial_stage_by_target = {
        stage.target_relation: stage for stage in stages if stage.profile_initial_build is not None
    }
    await fhir._assert_provider_directory_profile_checkpoint_ready(
        build,
        initial_stage_by_target[fhir.profile_artifact.PROFILE_EVIDENCE_TABLE].build_fence,
        initial_stage_by_target[fhir.profile_artifact.PROFILE_TABLE].build_fence,
    )
    for name, stage in initial_stage_by_target.items():
        if await fhir._provider_directory_relation_oid(stage.schema, stage.stage_table) != cutover["oids"][name]:
            raise RuntimeError("provider_directory_profile_initial_stage_changed")
    await fhir._assert_provider_directory_profile_capacity_consumption(admission, build)
    await fhir._admission_database_guard(admission.admitted_identity, admission.database_identity)
    cutover["wal_start"] = await fhir.db.scalar("SELECT pg_current_wal_insert_lsn()::text")


@asynccontextmanager
async def swap_window(fhir, cutover):
    """Reserve the finite initial cutover attempt inside the existing mutation window."""
    if cutover is None:
        yield
        return
    admission = cutover["admission"]
    async with fhir._profile_capacity_mutation_window(None):
        await fhir._reserve_provider_directory_profile_wal_budget(
            admission, control_operation_counts={"initial_cutover": 1}
        )
        yield
        await fhir._profile_capacity_remaining_ms(admission)


def _serving_values(fhir, cutover):
    build, admission = cutover["build"], cutover["admission"]
    execution = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get()
    return {
        "status": "published",
        "operation": "publish",
        "control_generation": execution.generation,
        "generation_id": build.generation_id,
        "selection_proof_id": build.selection_proof_id,
        "authority_revision": build.authority_revision,
        "profile_schema_version": execution.attestation.profile_schema_version,
        "profile_strategy_version": execution.attestation.profile_strategy_version,
        "source_vector_hash": build.desired_source_vector_hash,
        "source_context_vector_hash": build.desired_source_context_vector_hash,
        "executable_plan_hash": admission.geometry.executable_plan_hash,
        "profile_as_of": build.profile_as_of,
        "evidence_target_oid": cutover["oids"][fhir.profile_artifact.PROFILE_EVIDENCE_TABLE],
        "profile_target_oid": cutover["oids"][fhir.profile_artifact.PROFILE_TABLE],
        "evidence_rows": int(cutover["counts"]["evidence_rows"]),
        "profile_rows": int(cutover["counts"]["profile_rows"]),
    }


async def finish_cutover(fhir, cutover, fence):
    """First singleton, immutable result and checkpoint retirement share the swap transaction."""
    build, admission, projection = cutover["build"], cutover["admission"], cutover["projection"]
    serving = _serving_values(fhir, cutover)
    source_vector, context_vector, receipt_by_field = await _initial_commit_payload(
        fhir, cutover, build, admission, serving, fence
    )
    fhir._checked_serialized_metadata_payload_bytes(receipt_by_field, fixed_row_overhead=4096)
    serving_by_column = {
        **serving,
        "source_vector_json": contract.canonical_json(source_vector),
        "source_context_vector_json": contract.canonical_json(context_vector),
        "capacity_geometry_hash": build.capacity_geometry_hash,
        "capacity_geometry_json": build.capacity_geometry_json,
        "cutover_forecast_hash": cutover["forecast_hash"],
    }
    columns = tuple(serving_by_column)
    expressions = [f"CAST(:{name} AS jsonb)" if name.endswith("_json") else f":{name}" for name in columns]
    async with metadata_window(fhir, admission, projection):
        inserted = await fhir.db.status(
            f"""INSERT INTO {fhir._provider_directory_profile_serving_generation_ref(build.schema)}
            (singleton_key,capacity_geometry_status,{",".join(columns)},published_at,updated_at)
            VALUES ('global','verified',{",".join(expressions)},now(),now())""",
            **serving_by_column,
        )
        if fhir._coerce_rowcount(inserted) != 1:
            raise RuntimeError("provider_directory_profile_initial_serving_insert_missing")
        await fhir.db.status(
            f"""INSERT INTO {fhir._unscoped_qt(build.schema, contract.RECEIPT_TABLE)}
            (build_id,attestation_id,generation_id,run_id,contract_id,payload,payload_sha256,committed_at)
            SELECT :build,:attestation,:generation,:run,:contract,payload,
                   encode(sha256(convert_to(payload::text,'UTF8')),'hex'),now()
            FROM (SELECT CAST(:payload AS jsonb) AS payload) row""",
            build=admission.build_id,
            attestation=admission.lease.attestation_id,
            generation=build.generation_id,
            run=admission.run_id,
            contract=contract.COMMIT_CONTRACT,
            payload=contract.canonical_json(receipt_by_field),
        )
    async with fhir._profile_capacity_mutation_window(None):
        await fhir._delete_provider_directory_profile_build_checkpoint(build.schema, admission.build_id)
    await fhir._validate_profile_delta_total_wal(admission, SimpleNamespace(metadata_projection=projection))


@asynccontextmanager
async def metadata_window(fhir, admission, projection):
    """Keep final metadata charged until its actual WAL and deadline have passed."""
    tracker = admission.wal_tracker
    try:
        async with tracker.mutation_lock:
            await fhir._profile_capacity_remaining_ms(admission)
            await fhir._reserve_provider_directory_profile_wal_budget(
                admission, metadata_wal_bytes=projection.wal_bytes + projection.commit_envelope_bytes
            )
            before = await fhir._provider_directory_profile_current_wal_bytes(admission)
            yield
            after = await fhir._provider_directory_profile_current_wal_bytes(admission)
            if not 0 <= after - before <= projection.wal_bytes:
                raise RuntimeError("provider_directory_profile_capacity_metadata_wal_exceeded")
            async with tracker.lock:
                remaining = tracker.pending_metadata_wal_bytes - projection.wal_bytes
                if remaining < projection.commit_envelope_bytes:
                    raise RuntimeError("provider_directory_profile_capacity_metadata_reservation_missing")
                candidate = replace(
                    admission, wal_tracker=replace(tracker, pending_metadata_wal_bytes=remaining, lock=asyncio.Lock())
                )
                await fhir._validate_profile_delta_total_wal(candidate, SimpleNamespace(metadata_projection=projection))
                await fhir._profile_capacity_remaining_ms(admission)
                tracker.pending_metadata_wal_bytes = remaining
    except BaseException:
        tracker.unresolved_window = True
        raise


def _assert_receipt_payload(fhir, receipt_by_field, receipt_payload, geometry):
    """Validate the closed receipt fields and their reserved metadata bounds."""
    serving_by_field, forecast, actual = (
        receipt_payload.get("serving", {}),
        receipt_payload.get("forecast", {}),
        receipt_payload.get("actual", {}),
    )
    from process.provider_directory_profile_initial_guards import _PROFILE_FIELDS

    if (
        set(receipt_payload) != _initial_receipt_fields()
        or set(serving_by_field) != set(_PROFILE_FIELDS)
        or any(
            receipt_payload.get(name) != receipt_by_field.get(name) for name in ("build_id", "run_id", "attestation_id")
        )
        or serving_by_field.get("generation_id") != receipt_by_field["generation_id"]
        or receipt_payload["capacity_geometry_hash"] != fhir.profile_capacity.capacity_geometry_hash(geometry)
        or receipt_payload["initial_target_state_sha256"] != geometry.initial_target_state_sha256
        or receipt_payload["executable_plan_hash"] != geometry.executable_plan_hash
        or receipt_payload["common_receipt_required"]
        is not any(pair.get("source_id") == "cms-npd" for pair in receipt_payload["source_vector"])
        or forecast.get("contract_id") != contract.FORECAST_CONTRACT
        or forecast.get("build_id") != receipt_by_field["build_id"]
        or set(forecast) != _initial_forecast_fields()
        or forecast.get("capacity_geometry_hash") != receipt_payload["capacity_geometry_hash"]
        or forecast.get("initial_target_state_sha256") != geometry.initial_target_state_sha256
        or forecast.get("dataset_fence_sha256") != receipt_payload["dataset_fence_sha256"]
        or forecast.get("ddl_statement_upper_bound") != contract.INITIAL_CUTOVER_STATEMENTS
        or forecast.get("new_target_oids")
        != {
            fhir.profile_artifact.PROFILE_EVIDENCE_TABLE: serving_by_field["evidence_target_oid"],
            fhir.profile_artifact.PROFILE_TABLE: serving_by_field["profile_target_oid"],
        }
        or set(actual)
        != {
            "contract_id",
            "forecast_hash",
            "wal_start_lsn",
            "wal_observed_lsn",
            "evidence_target_storage_fingerprint",
            "profile_target_storage_fingerprint",
        }
        or actual.get("contract_id") != contract.ACTUAL_CONTRACT
        or actual.get("forecast_hash") != fhir._identity_hash(forecast)
    ):
        raise RuntimeError("provider_directory_profile_initial_receipt_binding_changed")
    if (
        any(
            type(forecast[name]) is not int or forecast[name] < 0
            for name in ("metadata_data_bytes", "metadata_wal_bytes", "commit_envelope_bytes")
        )
        or forecast["metadata_data_bytes"] > geometry.metadata_data_upper_bound_bytes
        or forecast["metadata_wal_bytes"] + forecast["commit_envelope_bytes"] > geometry.metadata_wal_upper_bound_bytes
    ):
        raise RuntimeError("provider_directory_profile_initial_receipt_bounds_invalid")


async def _committed_receipt(fhir, schema, build_id):
    """Read a retired initial build only with its immutable storage and provenance intact."""
    receipt_row = await fhir.db.first(
        f"SELECT *,payload_sha256=encode(sha256(convert_to(payload::text,'UTF8')),'hex') AS valid_hash "
        f"FROM {fhir._unscoped_qt(schema, contract.RECEIPT_TABLE)} WHERE build_id=:build",
        build=build_id,
    )
    if receipt_row is None:
        return None
    receipt_by_field = dict(fhir._pagination_checkpoint_row_mapping(receipt_row))
    receipt_payload = receipt_by_field["payload"]
    if isinstance(receipt_payload, str):
        receipt_payload = json.loads(receipt_payload)
    if receipt_by_field["valid_hash"] is not True or receipt_payload.get("contract_id") != contract.COMMIT_CONTRACT:
        raise RuntimeError("provider_directory_profile_initial_receipt_invalid")
    receipt_by_field["payload"] = receipt_payload
    layout = await receipt_layout(fhir, schema)
    geometry = fhir.profile_capacity.validated_capacity_geometry(receipt_payload.get("capacity_geometry"))
    if (
        not isinstance(geometry, contract.InitialCapacityGeometry)
        or layout.relation_oid != geometry.initial_receipt_oid
        or layout.exact_fingerprint != geometry.initial_receipt_storage_fingerprint
        or layout.effective_tablespace_oids != (geometry.tablespace_oid,)
    ):
        raise RuntimeError("provider_directory_profile_initial_receipt_storage_changed")
    _assert_receipt_payload(fhir, receipt_by_field, receipt_payload, geometry)
    if await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {fhir._provider_directory_profile_checkpoint_ref(schema)} WHERE build_id=:build)",
        build=build_id,
    ):
        raise RuntimeError("provider_directory_profile_initial_checkpoint_not_retired")
    if receipt_payload["common_receipt_required"] and not await fhir.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {fhir._unscoped_qt(schema, 'provider_directory_cms_serving_receipt')} "
        "WHERE publication_xid=:xid AND profile_generation_id=:generation)",
        xid=receipt_by_field["publication_xid"],
        generation=receipt_by_field["generation_id"],
    ):
        raise RuntimeError("provider_directory_profile_initial_common_receipt_missing")
    return receipt_by_field


async def _assert_current_receipt(fhir, schema, receipt):
    payload = receipt["payload"]
    serving = await fhir._provider_directory_profile_serving_state(schema)
    if (
        serving is None
        or {key: getattr(serving, key) for key in payload["serving"]} != payload["serving"]
        or serving.capacity_geometry_hash != payload["capacity_geometry_hash"]
        or json.loads(serving.capacity_geometry_json) != payload["capacity_geometry"]
        or fhir._provider_directory_profile_source_vector_json(serving.source_vector) != payload["source_vector"]
        or fhir._provider_directory_profile_source_context_vector_json(serving.source_context_vector)
        != payload["source_context_vector"]
    ):
        raise RuntimeError("provider_directory_profile_initial_receipt_not_current")
    for prefix in ("evidence", "profile"):
        target = (
            fhir.profile_artifact.PROFILE_EVIDENCE_TABLE
            if prefix == "evidence"
            else fhir.profile_artifact.PROFILE_TABLE
        )
        oid = await fhir._provider_directory_relation_oid(schema, target)
        layout = await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        if (
            oid != payload["serving"][prefix + "_target_oid"]
            or layout.exact_fingerprint != payload["actual"][prefix + "_target_storage_fingerprint"]
        ):
            raise RuntimeError("provider_directory_profile_initial_receipt_targets_changed")
    return serving


async def is_committed(fhir, stages):
    """Confirm the current committed result against the original consumed signed lease."""
    build = build_from_stages(fhir, stages)
    if build is None:
        return True
    try:
        receipt = await _committed_receipt(fhir, build.schema, fhir._provider_directory_profile_build_id(build))
        if (
            receipt is None
            or receipt["generation_id"] != build.generation_id
            or receipt["run_id"] != build.owner_run_id
        ):
            return False
        await _assert_current_receipt(fhir, build.schema, receipt)
        admission = fhir._provider_directory_profile_capacity_admission()
        if admission is None or receipt["attestation_id"] != admission.lease.attestation_id:
            return False
        consumed = await fhir._replay_bound_consumption(
            fhir._unscoped_qt(build.schema, fhir.ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__),
            admission.run_id,
            admission.build_id,
        )
        consumption_values = receipt["payload"]["serving"]
        replay_by_field = {
            **consumption_values,
            "build_id": receipt["build_id"],
            "to_source_vector_hash": consumption_values["source_vector_hash"],
            "to_source_context_vector_hash": consumption_values["source_context_vector_hash"],
        }
        lease = fhir._verified_provider_directory_profile_replay_lease(consumed, replay_by_field, admission.geometry)
        fhir._assert_replay_timeline(consumed, receipt, lease)
        return receipt["payload"]["lease_digest"] == lease.lease_digest and receipt["payload"][
            "capacity_geometry_hash"
        ] == fhir.profile_capacity.capacity_geometry_hash(admission.geometry)
    except Exception:
        return False


async def _assert_replay_sources(fhir, schema, receipt, execution, fence, is_current):
    """Bind the historical selection to its common receipt or current dataset fence."""
    receipt_payload, serving_by_field = receipt["payload"], receipt["payload"]["serving"]
    source_pairs = tuple((pair["source_id"], pair["dataset_id"]) for pair in execution.attestation.pairs)
    if fhir._provider_directory_profile_source_vector_json(source_pairs) != receipt_payload["source_vector"]:
        raise RuntimeError("provider_directory_profile_initial_replay_sources_changed")
    if fence is None:
        if receipt_payload["common_receipt_required"] is not True:
            raise RuntimeError("provider_directory_profile_initial_fence_missing")
        from process import provider_directory_cms_replay as cms_replay

        await cms_replay._registered_execution(fhir, execution)
        common_receipt = await cms_replay._historical_common(
            fhir, schema, receipt["generation_id"], publication_xid=receipt["publication_xid"]
        )
        if common_receipt is None:
            raise RuntimeError("provider_directory_profile_initial_common_receipt_missing")
        cms_replay._assert_common_execution(
            fhir,
            execution,
            {
                **serving_by_field,
                "to_source_vector_hash": serving_by_field["source_vector_hash"],
                "to_source_context_vector_hash": serving_by_field["source_context_vector_hash"],
            },
            common_receipt,
        )
    if is_current and fence is not None:
        _source_vector, context_vector = await fhir._provider_directory_profile_replay_source_context(execution)
        if (
            fhir._provider_directory_profile_source_context_vector_json(context_vector)
            != receipt_payload["source_context_vector"]
        ):
            raise RuntimeError("provider_directory_profile_initial_replay_sources_changed")
    return source_pairs


async def _replay_authority(fhir, schema, consumption_ref, run_id, execution, receipt):
    """Verify the original owner and consumed signed lease before reading its result."""
    receipt_payload, serving_by_field = receipt["payload"], receipt["payload"]["serving"]
    owner = receipt["run_id"]
    current = await fhir._replay_current_consumption(consumption_ref, run_id)
    if owner != run_id and current is not None:
        raise RuntimeError("provider_directory_profile_replay_current_consumption_conflict")
    consumption = await fhir._replay_bound_consumption(consumption_ref, owner, receipt["build_id"])
    await fhir._assert_replay_owner(schema, owner, run_id)
    geometry = fhir.profile_capacity.validated_capacity_geometry(receipt_payload["capacity_geometry"])
    if not isinstance(geometry, contract.InitialCapacityGeometry):
        raise RuntimeError("provider_directory_profile_initial_replay_geometry_invalid")
    lease_by_field = {
        **serving_by_field,
        "build_id": receipt["build_id"],
        "to_source_vector_hash": serving_by_field["source_vector_hash"],
        "to_source_context_vector_hash": serving_by_field["source_context_vector_hash"],
        "committed_at": receipt["committed_at"],
    }
    lease = fhir._verified_provider_directory_profile_replay_lease(consumption, lease_by_field, geometry)
    signed_receipt = lease.signing_preflight_guard["healthcare_receipt"]
    expected_generation = (
        "pdprofile_"
        + fhir.hashlib.sha256(f"{receipt['build_id']}:{serving_by_field['profile_as_of']}".encode()).hexdigest()[:32]
    )
    if (
        receipt_payload["capacity_geometry_hash"] != fhir.profile_capacity.capacity_geometry_hash(geometry)
        or signed_receipt["capacity_geometry"] != receipt_payload["capacity_geometry"]
        or receipt_payload["initial_target_state_sha256"] != geometry.initial_target_state_sha256
        or receipt_payload["attestation_id"] != lease.attestation_id
        or receipt_payload["lease_digest"] != lease.lease_digest
        or receipt_payload["preflight_receipt_sha256"] != lease.nonce
        or serving_by_field["generation_id"] != expected_generation
        or geometry.selection_proof_id != execution.attestation.proof_id
        or geometry.profile_input_digest != execution.attestation.profile_input_digest
        or geometry.profile_schema_version != serving_by_field["profile_schema_version"]
        or geometry.profile_strategy_version != serving_by_field["profile_strategy_version"]
        or geometry.profile_as_of != serving_by_field["profile_as_of"]
        or geometry.sql_contract_digest != fhir._provider_directory_profile_sql_contract_digest()
        or geometry.executable_plan_hash != serving_by_field["executable_plan_hash"]
        or geometry.desired_source_vector_hash != serving_by_field["source_vector_hash"]
        or geometry.desired_context_vector_hash != serving_by_field["source_context_vector_hash"]
    ):
        raise RuntimeError("provider_directory_profile_initial_replay_binding_changed")
    return owner, consumption, geometry, lease_by_field, lease


async def committed_replay(fhir, schema, consumption_ref, run_id, execution, fence):
    """Read a consumed initial result without issuing authority or rewriting old history."""
    await fhir._replay_control_run(schema, run_id)
    receipt_rows = await fhir.db.all(
        f"SELECT build_id FROM {fhir._unscoped_qt(schema, contract.RECEIPT_TABLE)} "
        "WHERE payload->'serving'->>'selection_proof_id'=:proof "
        "OR payload->'serving'->>'control_generation'=:generation",
        proof=execution.attestation.proof_id,
        generation=str(execution.generation),
    )
    if not receipt_rows:
        return None
    if len(receipt_rows) != 1:
        raise RuntimeError("provider_directory_profile_initial_replay_ambiguous")
    receipt = await _committed_receipt(
        fhir, schema, fhir._pagination_checkpoint_row_mapping(receipt_rows[0])["build_id"]
    )
    receipt_payload, serving_by_field = receipt["payload"], receipt["payload"]["serving"]
    current_serving, is_current = await _replay_selection_state(fhir, schema, serving_by_field, execution, receipt)
    source_pairs = await _assert_replay_sources(fhir, schema, receipt, execution, fence, is_current)
    owner, consumption, geometry, lease_by_field, lease = await _replay_authority(
        fhir, schema, consumption_ref, run_id, execution, receipt
    )
    serving = await _assert_current_receipt(fhir, schema, receipt) if is_current else current_serving
    database = await fhir._provider_directory_profile_capacity_database_identity(schema, serving)
    # Initial admission bound the old physical targets. Their replacements are bound by
    # the immutable transaction receipt; every unchanged database/metadata field still matches.
    _assert_replay_database(fhir, database, geometry, lease, consumption, lease_by_field)
    if (
        is_current
        and fence is not None
        and (
            receipt_payload["dataset_fence_sha256"] != _fence_hash(fhir, fence)
            or not await fhir._is_provider_directory_dataset_cutover_committed(fence)
        )
    ):
        raise RuntimeError("provider_directory_profile_initial_replay_dataset_changed")
    replay_by_field = {
        "profile_rows": serving_by_field["profile_rows"],
        "evidence_rows": serving_by_field["evidence_rows"],
        "selected_evidence_rows": serving_by_field["evidence_rows"],
        "generation_id": serving_by_field["generation_id"],
        "profile_as_of": serving_by_field["profile_as_of"],
        "dataset_ids": sorted({dataset for _source, dataset in source_pairs}),
        "incremental": False,
        "capacity": fhir._provider_directory_profile_capacity_metric_map(geometry, lease),
        "committed_replay": {
            "run_id": owner,
            "build_id": receipt["build_id"],
            "committed_at": receipt["committed_at"].isoformat(),
            "current": is_current,
            "status": "current" if is_current else "superseded",
            "current_generation_id": current_serving.generation_id,
        },
    }
    if run_id != owner:
        replay_by_field["committed_replay"]["replayed_by_run_id"] = run_id
    return replay_by_field


async def resolve_completion(fhir, stages, fence, promotion_identities):
    """Drain-safe bounded readback after the owning transaction has returned or aborted."""
    async with asyncio.timeout(10):
        async with fhir.db.transaction():
            await fhir.db.status("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            await fhir.db.status("SET LOCAL statement_timeout='8s'")
            await fhir.db.status("SET LOCAL lock_timeout='100ms'")
            return (
                await is_committed(fhir, stages)
                and await fhir._is_provider_directory_artifact_promotion_committed(promotion_identities, fence)
                and (fence is None or await fhir._is_provider_directory_dataset_cutover_committed(fence))
            )


async def _capture_legacy_history(fhir, schema, profile_ref, evidence_ref):
    """Validate the newest terminal history against the physical incumbent targets."""
    history_row = await fhir.db.first(
        f"""SELECT run_id, finished_at, metrics::jsonb -> :key AS result
        FROM {fhir._unscoped_qt(schema, fhir.ImportRun.__tablename__)}
        WHERE importer='provider-directory-fhir' AND status='succeeded' AND metrics::jsonb -> :key IS NOT NULL
        ORDER BY finished_at DESC NULLS LAST, run_id DESC LIMIT 1""",
        key=fhir.PROFILE_SELECTION_RESULT_METRIC,
    )
    history_by_field_name, evidence_rows, profile_rows = None, 0, 0
    if history_row is not None:
        history_by_field = fhir._pagination_checkpoint_row_mapping(history_row)
        legacy_result = history_by_field["result"]
        if isinstance(legacy_result, str):
            legacy_result = json.loads(legacy_result)
        if history_by_field.get("finished_at") is None:
            raise RuntimeError("provider_directory_profile_initial_history_unfinished")
        legacy_result = contract.validated_legacy_result(legacy_result)
        profile_rows, evidence_rows = fhir._profile_adoption_attested_row_counts(legacy_result)
        await fhir._profile_adoption_targets(
            schema,
            generation_id=legacy_result["profile_generation_id"],
            profile_rows=profile_rows,
            evidence_rows=evidence_rows,
        )
        if await fhir.db.scalar(
            f"SELECT EXISTS (SELECT 1 FROM {profile_ref} WHERE generation_id <> :generation)",
            generation=legacy_result["profile_generation_id"],
        ):
            raise RuntimeError("provider_directory_profile_initial_mixed_generation")
        history_by_field_name = {
            "run_id": history_by_field["run_id"],
            "result_sha256": contract.result_sha256(legacy_result),
            "result": legacy_result,
            "profile_as_of": None,
            "temporal_metadata": "not_recorded_by_producer",
        }
    elif await fhir.db.scalar(f"SELECT EXISTS (SELECT 1 FROM {profile_ref}) OR EXISTS (SELECT 1 FROM {evidence_ref})"):
        raise RuntimeError("provider_directory_profile_initial_unattested_targets")
    return history_by_field_name, evidence_rows, profile_rows


async def _inspect_cutover_stages(fhir, build, admission, initial_stages):
    """Verify replacement identities and bounded row counts before metadata projection."""
    oid_by_target = {}
    for stage in initial_stages:
        if await fhir._provider_directory_relation_oid(stage.schema, stage.target_relation + "_old") is not None:
            raise RuntimeError("provider_directory_profile_initial_old_target_conflict")
        oid = await fhir._provider_directory_relation_oid(stage.schema, stage.stage_table)
        await fhir._provider_directory_profile_relation_storage_fingerprint(oid, expected_persistence="p")
        oid_by_target[stage.target_relation] = oid
    counts = await fhir._provider_directory_profile_stage_metrics(
        build.schema, build.evidence_stage, build.profile_stage, list(build.source_ids)
    )
    for name, table, observed, maximum in (
        ("evidence_stage", build.evidence_stage, int(counts["evidence_rows"]), admission.geometry.max_evidence_rows),
        ("profile_stage", build.profile_stage, int(counts["profile_rows"]), admission.geometry.max_profile_rows),
    ):
        await fhir._assert_provider_directory_profile_capacity_scratch(
            name, (fhir._unscoped_qt(build.schema, table),), observed_rows=observed, maximum_rows=maximum
        )
    return oid_by_target, counts


async def _initial_commit_payload(fhir, cutover, build, admission, serving, fence):
    """Capture the exact replacement layouts and signed initial commit payload."""
    source_vector = fhir._provider_directory_profile_source_vector_json(build.desired_source_vector)
    context_vector = fhir._provider_directory_profile_source_context_vector_json(build.desired_source_context_vector)
    target_layouts = [
        await fhir._provider_directory_profile_relation_storage_fingerprint(serving[name], expected_persistence="p")
        for name in ("evidence_target_oid", "profile_target_oid")
    ]
    actual_by_field = {
        "contract_id": contract.ACTUAL_CONTRACT,
        "forecast_hash": cutover["forecast_hash"],
        "wal_start_lsn": cutover["wal_start"],
        "wal_observed_lsn": await fhir.db.scalar("SELECT pg_current_wal_insert_lsn()::text"),
        "evidence_target_storage_fingerprint": target_layouts[0].exact_fingerprint,
        "profile_target_storage_fingerprint": target_layouts[1].exact_fingerprint,
    }
    receipt_by_field = {
        "contract_id": contract.COMMIT_CONTRACT,
        "build_id": admission.build_id,
        "run_id": admission.run_id,
        "attestation_id": admission.lease.attestation_id,
        "lease_digest": admission.lease.lease_digest,
        "preflight_receipt_sha256": admission.lease.nonce,
        "initial_target_state_sha256": admission.geometry.initial_target_state_sha256,
        "capacity_geometry_hash": build.capacity_geometry_hash,
        "capacity_geometry": json.loads(build.capacity_geometry_json),
        "executable_plan_hash": admission.geometry.executable_plan_hash,
        "serving": serving,
        "forecast": cutover["forecast"],
        "actual": actual_by_field,
        "source_vector": source_vector,
        "source_context_vector": context_vector,
        "dataset_fence_sha256": _fence_hash(fhir, fence),
        "common_receipt_required": any(source_id == "cms-npd" for source_id, _dataset in build.desired_source_vector),
    }
    return source_vector, context_vector, receipt_by_field


def _initial_receipt_fields():
    """Return the closed immutable initial commit payload field set."""
    return {
        "contract_id",
        "build_id",
        "run_id",
        "attestation_id",
        "lease_digest",
        "preflight_receipt_sha256",
        "initial_target_state_sha256",
        "capacity_geometry_hash",
        "capacity_geometry",
        "executable_plan_hash",
        "serving",
        "forecast",
        "actual",
        "source_vector",
        "source_context_vector",
        "dataset_fence_sha256",
        "common_receipt_required",
    }


def _initial_forecast_fields():
    """Return the closed initial cutover forecast field set."""
    return {
        "contract_id",
        "build_id",
        "capacity_geometry_hash",
        "initial_target_state_sha256",
        "dataset_fence_sha256",
        "new_target_oids",
        "metadata_data_bytes",
        "metadata_wal_bytes",
        "commit_envelope_bytes",
        "ddl_statement_upper_bound",
    }


def _assert_replay_database(fhir, database, geometry, lease, consumption, lease_by_field):
    """Check unchanged database identity and the original consumed authority timeline."""
    for name in (
        "database_system_identifier",
        "database_oid",
        "database_name",
        "tablespace_oid",
        "tablespace_name",
        "postgres_server_version_num",
        "postgres_block_size_bytes",
        "postgres_wal_block_size_bytes",
        "postgres_wal_segment_size_bytes",
        "postgres_full_page_writes",
        "postgres_wal_compression",
        "postgres_wal_level",
        "postgres_wal_log_hints",
        "postgres_data_checksums",
        "postgres_default_toast_compression",
        "postgres_checkpoint_timeout_seconds",
        "postgres_max_wal_size_bytes",
        "build_checkpoint_oid",
        "serving_generation_oid",
        "delta_receipt_oid",
        "import_run_oid",
        "capacity_consumption_oid",
        "build_checkpoint_storage_fingerprint",
        "serving_generation_storage_fingerprint",
        "delta_receipt_storage_fingerprint",
        "import_run_storage_fingerprint",
        "capacity_consumption_storage_fingerprint",
    ):
        if getattr(database, name) != getattr(geometry, name):
            raise RuntimeError("provider_directory_profile_initial_replay_database_changed")
    fhir._assert_provider_directory_profile_capacity_tablespaces(lease, database)
    fhir._assert_replay_timeline(consumption, lease_by_field, lease)


async def _replay_selection_state(fhir, schema, serving_by_field, execution, receipt):
    """Match the original selection before resolving its current or superseded serving state."""
    expected_by_field = {
        "selection_proof_id": execution.attestation.proof_id,
        "control_generation": execution.generation,
        "authority_revision": execution.attestation.authority_revision,
        "profile_schema_version": execution.attestation.profile_schema_version,
        "profile_strategy_version": execution.attestation.profile_strategy_version,
        "operation": execution.attestation.operation,
        "profile_as_of": execution.attestation.desired_profile_as_of,
    }
    if {name: serving_by_field.get(name) for name in expected_by_field} != expected_by_field:
        raise RuntimeError("provider_directory_profile_initial_replay_selection_changed")
    current_serving = await fhir._provider_directory_profile_serving_state(schema)
    if current_serving is None:
        raise RuntimeError("provider_directory_profile_initial_replay_serving_missing")
    is_current = current_serving.generation_id == receipt["generation_id"]
    return current_serving, is_current
