# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare the existing shared serving builders from their verified ordinary pair."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from functools import partial

from process import provider_directory_cms_preflight as preflight
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_common_serving_postimages as postimages
from process import provider_directory_ordinary_row_retention as retention
from process.provider_directory_cms_storage_continuation import configured_storage_continuation
from process.provider_directory_ordinary_address import prepare_ordinary_address
from process.provider_directory_ordinary_address_admission import produce_ordinary_address_admission
from process.provider_directory_ordinary_serving_preparation import prepare_ordinary_serving_artifacts
from process.provider_directory_profile_capacity_attestation import verify_database_capacity_lease
from process.provider_directory_profile_capacity_runtime import configured_capacity_lease_trust
from process.provider_directory_serving_capacity_request import (
    validated_serving_execution_capacity,
    validated_serving_preflight_request,
)
from process.provider_directory_serving_capacity_signing import (
    assert_serving_geometry_matches_request,
    validated_serving_capacity_geometry,
)


async def _verified_request(fhir, execution):
    """Verify both signatures before the request can drive native input reads."""
    envelope = validated_serving_execution_capacity(
        execution.serving_nonprofile_capacity_attestation,
        profile_envelope=execution.capacity_attestation,
        attestation=execution.attestation,
        generation=execution.generation,
    )
    guard = envelope["lease"]["signing_preflight_guard"]
    request = validated_serving_preflight_request(guard["healthcare_request"])
    plan = validated_serving_capacity_geometry(guard["healthcare_receipt"]["capacity_geometry"])
    assert_serving_geometry_matches_request(plan, request)
    trust = configured_capacity_lease_trust()
    verify_database_capacity_lease(
        envelope,
        trust=trust,
        now=await fhir._profile_capacity_preflight_clock(),
        expected_capacity_geometry_hash=plan.capacity_geometry_hash,
        expected_database_system_identifier=trust.database_system_identifier,
        expected_database_oid=trust.database_oid,
        expected_database_name=trust.database_name,
    )
    profile_lease = await preflight._verified_pair(fhir, request)
    if profile_lease.lease_digest != plan.paired_profile_lease_digest:
        raise RuntimeError("provider_directory_ordinary_paired_lease_changed")
    if (
        request.execution.attestation.payload != execution.attestation.payload
        or request.execution.generation != execution.generation
    ):
        raise RuntimeError("provider_directory_ordinary_execution_changed")
    # Keep the actual runtime object and its signed initial mode on the original context.
    return replace(request, execution=execution), plan, profile_lease


async def _retain_with_postimages(prepared, expected_by_family):
    retained = await retention.prepare_retained_generation(prepared)
    async with preparation.nonprofile_sql_transaction(prepared.fhir, prepared.nonprofile_admission):
        queries = postimages.expected_postimage_queries(prepared, retained)
        session = prepared.fhir.db._transaction_binding().session
        expected_by_family.update(await postimages.capture_postimages(session, queries))
    return retained


async def _discard_unpublished(prepared):
    retained = prepared.retained_generation
    if retained is None or retained.cutover_actor is not None:
        return
    capacity_token = prepared.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
    admission_token = preparation._ACTIVE.set(prepared.nonprofile_admission)
    try:
        await retention.discard_unpublished_retention(prepared)
    finally:
        preparation._ACTIVE.reset(admission_token)
        prepared.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(capacity_token)


async def _current_inputs(fhir, *, execution, run_id, control_run_id, metrics):
    """Verify the current pair and capture once for either reuse or preparation."""
    if fhir.db._transaction_binding() is not None:
        raise RuntimeError("provider_directory_ordinary_preparation_requires_own_transaction")
    run_id = fhir._validated_admission_run_id(run_id, control_run_id, execution)
    request, plan, profile_lease = await _verified_request(fhir, execution)
    snapshot = await preflight._read_inputs(fhir, request, profile_lease)
    if snapshot.plan.payload != plan.payload:
        raise RuntimeError("provider_directory_ordinary_capture_geometry_changed")
    admission = await produce_ordinary_address_admission(
        fhir,
        execution,
        snapshot.fence,
        run_id=run_id,
        signed_plan=plan,
        captured_inputs=snapshot.captured_inputs,
        fresh_storage_envelope=configured_storage_continuation(),
    )
    prepared = preparation.PreparedServingArtifacts(
        fhir, snapshot.fence, execution, admission, None, None, metrics, {}, None
    )
    return prepared, snapshot


@asynccontextmanager
async def _prepare_current(prepared, *, run_id, control_run_id):
    """Prepare only after the caller has chosen the conservative build path."""
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    run_id = fhir._validated_admission_run_id(run_id, control_run_id, prepared.execution)
    expected_by_family = {}
    try:
        async with (
            asyncio.timeout(await preparation.remaining_build_seconds(fhir, admission)),
            prepare_ordinary_serving_artifacts(
                prepared,
                run_id=run_id,
                control_run_id=control_run_id,
                address_preparation=partial(prepare_ordinary_address, run_id=run_id),
                retention_preparation=partial(_retain_with_postimages, expected_by_family=expected_by_family),
            ),
        ):
            yield prepared, expected_by_family
    finally:
        await _discard_unpublished(prepared)


@asynccontextmanager
async def prepare_current_ordinary_serving(fhir, *, execution, run_id, control_run_id, metrics):
    """Keep all candidates and recovery hashes alive for one caller-owned cut."""
    prepared, _snapshot = await _current_inputs(
        fhir, execution=execution, run_id=run_id, control_run_id=control_run_id, metrics=metrics
    )
    async with _prepare_current(prepared, run_id=run_id, control_run_id=control_run_id) as result:
        yield result


async def publish_current_ordinary_serving(fhir, *, execution, run_id, control_run_id, metrics):
    """Use the same worker for the paired common cut and its verified final result."""
    from process.provider_directory_ordinary_serving_publication import commit_ordinary_serving
    from process.provider_directory_ordinary_serving_reuse import reuse_current_ordinary_serving

    validated_run = fhir._validated_admission_run_id(run_id, control_run_id, execution)
    async with fhir.suppress_control_run_heartbeat_persistence(validated_run):
        prepared, snapshot = await _current_inputs(
            fhir, execution=execution, run_id=validated_run, control_run_id=control_run_id, metrics=metrics
        )
        async with asyncio.timeout(await preparation.remaining_build_seconds(fhir, prepared.nonprofile_admission)):
            reused = await reuse_current_ordinary_serving(
                prepared, snapshot, run_id=validated_run, control_run_id=control_run_id
            )
        if reused is not None:
            return await fhir._record_artifact_promotion_metrics(prepared.fence, reused, 0)
        async with _prepare_current(prepared, run_id=validated_run, control_run_id=control_run_id) as (
            prepared,
            expected_postimages,
        ):
            result = await commit_ordinary_serving(prepared, expected_postimages, run_id=validated_run)
            return await fhir._record_artifact_promotion_metrics(prepared.fence, result, len(prepared.stages))
