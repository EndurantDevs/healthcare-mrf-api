# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connect desired CMS preparation to the existing atomic serving owner."""

import asyncio
from types import SimpleNamespace

from sqlalchemy import text

from process import provider_directory_cms_serving_receipt as receipts
from process.provider_directory_cms_address import cms_address_preparation
from process.provider_directory_cms_capacity_contract import (
    assert_cms_geometry_matches_request,
    validated_cms_capacity_geometry,
    validated_cms_execution_capacity,
    verified_cms_paired_profile_lease,
)
from process.provider_directory_cms_desired_fence import prepare_desired_fence
from process.provider_directory_cms_native_inputs import capture_native_address_input_fence
from process.provider_directory_cms_nonprofile_capacity import produce_nonprofile_admission
from process.provider_directory_cms_preparation import (
    nonprofile_sql_transaction,
    prepare_serving_artifacts,
    remaining_build_seconds,
)
from process.provider_directory_cms_publication import commit_prepared_serving_generation
from process.provider_directory_cms_storage_continuation import configured_storage_continuation
from process.provider_directory_profile_capacity_attestation import (
    CAPACITY_LEASE_DIGEST_DOMAIN,
    _domain_hash,
    verify_database_capacity_lease,
)
from process.provider_directory_profile_capacity_preflight_contract import validated_capacity_preflight_request
from process.provider_directory_profile_capacity_runtime import configured_capacity_lease_trust
from process.provider_directory_profile_temp_limit import apply_temp_file_limit


async def _current_predecessor(fhir, session):
    """Existing history without a matching current receipt is drift, not bootstrap."""
    predecessor = await receipts.read_current_receipt(session, fhir._schema())
    if predecessor is None and await session.scalar(
        text(f"SELECT EXISTS (SELECT 1 FROM {fhir._qt(fhir._schema(), 'provider_directory_cms_serving_receipt')})")
    ):
        raise RuntimeError("cms_serving_current_receipt_unavailable")
    return predecessor


async def _candidate_proof(fhir, session, execution):
    """Read one sealed identity without taking row locks in the build snapshot."""
    desired = execution.attestation.desired_cms_dataset
    proof = (
        (
            await session.execute(
                text(
                    f"SELECT dataset_id,endpoint_id,dataset_hash,release_id,proof_version "
                    f"FROM {fhir._qt(fhir._schema(), 'provider_directory_cms_candidate_coverage')} "
                    "WHERE dataset_id=:dataset_id AND endpoint_id=:endpoint_id "
                    "AND dataset_hash=:dataset_hash AND proof_version=2"
                ),
                desired,
            )
        )
        .mappings()
        .one_or_none()
    )
    if proof is None:
        raise RuntimeError("cms_npd_candidate_coverage_unavailable")
    return dict(proof)


def _signed_plan(execution):
    """Retain the exact paired envelopes and parse their closed signed geometry."""
    envelope = validated_cms_execution_capacity(
        execution.cms_nonprofile_capacity_attestation,
        profile_envelope=execution.capacity_attestation,
        attestation=execution.attestation,
        generation=execution.generation,
    )
    guard = envelope["lease"]["signing_preflight_guard"]
    plan = validated_cms_capacity_geometry(guard["healthcare_receipt"]["capacity_geometry"])
    assert_cms_geometry_matches_request(plan, validated_capacity_preflight_request(guard["healthcare_request"]))
    return envelope, plan


async def _verified_capture_limits(fhir, execution, plan):
    """Verify both original signatures before using their bounds for retained-input reads."""
    envelope, signed_plan = _signed_plan(execution)
    if signed_plan != plan:
        raise RuntimeError("cms_serving_capture_geometry_changed")
    trust = configured_capacity_lease_trust()
    now = await fhir._profile_capacity_preflight_clock()
    lease = verify_database_capacity_lease(
        envelope,
        trust=trust,
        now=now,
        expected_capacity_geometry_hash=plan.capacity_geometry_hash,
        expected_database_system_identifier=trust.database_system_identifier,
        expected_database_oid=trust.database_oid,
        expected_database_name=trust.database_name,
    )
    request = validated_capacity_preflight_request(lease.signing_preflight_guard["healthcare_request"])
    profile = verified_cms_paired_profile_lease(request, trust=trust, now=now)
    if profile.lease_digest != plan.paired_profile_lease_digest:
        raise RuntimeError("cms_serving_capture_paired_lease_changed")
    return SimpleNamespace(lease=lease, paired_profile_lease=profile, plan=plan)


async def _capture_publish_inputs(fhir, execution, run_id, metrics, plan):
    """Validate full retained contents and pin revisions within both verified build deadlines."""
    limits = await _verified_capture_limits(fhir, execution, plan)
    targets = set(fhir.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS) - {"profile", "corroboration"}
    async with asyncio.timeout(await remaining_build_seconds(fhir, limits)):
        async with fhir.db.transaction() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await apply_temp_file_limit(fhir.db, plan.temp_file_limit_bytes_per_backend)
            async with nonprofile_sql_transaction(fhir, limits):
                fence = await prepare_desired_fence(
                    fhir, execution, run_id=run_id, metrics=metrics, publish_targets=targets
                )
                predecessor = await _current_predecessor(fhir, session)
                dependencies = await receipts.capture_native_dependencies(session, fhir._schema())
                native_fence = await capture_native_address_input_fence(session, fhir._schema())
                proof = await _candidate_proof(fhir, session, execution)
    return SimpleNamespace(
        fence=fence,
        predecessor=predecessor,
        dependencies=dependencies,
        native_fence=native_fence,
        proof=proof,
        capacity_lease=limits.lease,
    )


def _captured_capacity_request(snapshot, envelope):
    """Use only the exact envelope already verified before native input capture."""
    lease = getattr(snapshot, "capacity_lease", None)
    if (
        lease is None
        or lease.lease_digest != _domain_hash(CAPACITY_LEASE_DIGEST_DOMAIN, envelope)
        or lease.signing_preflight_guard != envelope["lease"]["signing_preflight_guard"]
    ):
        raise RuntimeError("cms_serving_capture_signed_request_changed")
    return validated_capacity_preflight_request(lease.signing_preflight_guard["healthcare_request"])


async def _authenticated_source_job(invocation, execution, request, snapshot):
    """Require the owner capability before any signed retention workload grows."""
    from process.provider_directory_cms_source_runtime import CMSRegistrySourceInvocation

    if invocation is None:
        if "registry_source_retention" in request.cms_nonprofile_admission:
            raise RuntimeError("cms_registry_source_runtime_required")
        return None
    if type(invocation) is not CMSRegistrySourceInvocation:
        raise RuntimeError("cms_registry_source_runtime_invalid")
    return await invocation.bind_verified_job(execution, request, snapshot.capacity_lease)


async def _publish_cms(fhir, execution, run_id, control_run_id, metrics, *, registry_source_invocation=None):
    """Build all desired serving families before their single bounded publication."""
    envelope, plan = _signed_plan(execution)
    authority = configured_storage_continuation()
    snapshot = await _capture_publish_inputs(fhir, execution, run_id, metrics, plan)
    request = _captured_capacity_request(snapshot, envelope)
    source_job = await _authenticated_source_job(registry_source_invocation, execution, request, snapshot)
    factory = cms_address_preparation(
        fhir,
        execution,
        snapshot.fence,
        snapshot.dependencies,
        native_input_fence=snapshot.native_fence,
        run_id=run_id,
        worker_count=plan.worker_count,
        temp_file_limit_bytes_per_backend=plan.temp_file_limit_bytes_per_backend,
    )
    if "registry_source_retention" in request.cms_nonprofile_admission:
        factory = factory.with_registry_source_retention(request.cms_nonprofile_admission["registry_source_retention"])
    if factory.input_hash != plan.native_address_input_hash:
        raise RuntimeError("cms_address_admitted_inputs_changed")
    admission = await produce_nonprofile_admission(
        fhir,
        execution,
        snapshot.fence,
        run_id=run_id,
        assigned_envelope=envelope,
        profile_envelope=execution.capacity_attestation,
        signed_plan=plan,
        fresh_storage_envelope=authority,
    )
    if source_job is not None:
        admission.registry_source_job = source_job
    async with prepare_serving_artifacts(
        fhir,
        execution,
        snapshot.fence,
        run_id=run_id,
        control_run_id=control_run_id,
        metrics=metrics,
        nonprofile_admission=admission,
        address_preparation=factory,
    ) as prepared:
        publication_by_field = await commit_prepared_serving_generation(
            fhir,
            execution,
            prepared,
            address=prepared.address,
            candidate_proof=snapshot.proof,
            native_dependencies=snapshot.dependencies,
            predecessor=snapshot.predecessor,
        )
        return await fhir._record_artifact_promotion_metrics(snapshot.fence, publication_by_field, len(prepared.stages))


async def _purge_common_profile(fhir, execution, run_id, control_run_id, metrics):
    """Purge only Profile while retaining the independently accepted CMS/native source."""
    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        predecessor = await _current_predecessor(fhir, session)
        if predecessor is None:
            return None
        dependencies = await receipts.capture_native_dependencies(session, fhir._schema())
    fence = fhir.ProviderDirectoryArtifactDatasetFence(())
    proof_by_field = {
        field: predecessor["payload"]["cms"][field]
        for field in ("dataset_id", "endpoint_id", "dataset_hash", "release_id", "proof_version")
    }
    async with prepare_serving_artifacts(
        fhir, execution, fence, run_id=run_id, control_run_id=control_run_id, metrics=metrics
    ) as prepared:
        result = await commit_prepared_serving_generation(
            fhir,
            execution,
            prepared,
            address=None,
            candidate_proof=proof_by_field,
            native_dependencies=dependencies,
            predecessor=predecessor,
        )
        return await fhir._record_artifact_promotion_metrics(fence, result, len(prepared.stages))


async def _ordinary_profile(fhir, execution, run_id, control_run_id, metrics):
    """Keep the established pre-CMS Profile build and committed replay behavior."""
    source_ids = [pair["source_id"] for pair in execution.attestation.pairs]
    fence = await fhir._attested_profile_publication_fence(
        run_id=run_id, metrics=metrics, execution=execution, source_ids=source_ids
    )
    fhir._assert_profile_selection_matches_artifact_fence(execution, fence)
    replayed = await fhir._provider_directory_profile_committed_run_replay(
        run_id=run_id, control_run_id=control_run_id, execution=execution, fence=fence
    )
    if replayed is not None:
        return await fhir._provider_directory_profile_replay_publish_metrics(metrics, fence, replayed)
    return await fhir._publish_attested_provider_directory_profile_build(
        run_id=run_id,
        control_run_id=control_run_id,
        metrics=metrics,
        execution=execution,
        fence=fence,
        source_ids=source_ids,
    )


async def _assert_retained_legacy_cms(fhir, execution):
    """Permit an ordinary Profile refresh only for the exactly proved incumbent CMS source."""
    selected = next((pair for pair in execution.attestation.pairs if pair["source_id"] == "cms-npd"), None)
    if selected is None:
        return
    async with fhir.db.transaction() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        predecessor = await _current_predecessor(fhir, session)
    fields = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "acquisition_root_run_id")
    if predecessor is None or any(selected[field] != predecessor["payload"]["cms"][field] for field in fields):
        raise RuntimeError("cms_serving_desired_selection_required")


async def publish_current_attested_profile(
    fhir, *, run_id, control_run_id, metrics, execution, registry_source_invocation=None
):
    """Route only current selections; historical replay remains in the outer owner."""
    await fhir.assert_registered_profile_selection_current(
        execution.attestation, fhir._provider_directory_profile_selection_catalog()
    )
    token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
    try:
        if execution.attestation.desired_cms_dataset is not None or execution.attestation.operation == "purge":
            if fhir.db._transaction_binding() is not None:
                raise RuntimeError("cms_serving_preparation_requires_own_transaction")
            validated_run = fhir._validated_admission_run_id(run_id, control_run_id, execution)
            async with fhir.suppress_control_run_heartbeat_persistence(validated_run):
                if execution.attestation.operation == "publish":
                    publication_metrics = await _publish_cms(
                        fhir,
                        execution,
                        validated_run,
                        control_run_id,
                        metrics,
                        registry_source_invocation=registry_source_invocation,
                    )
                else:
                    publication_metrics = await _purge_common_profile(
                        fhir, execution, validated_run, control_run_id, metrics
                    )
        else:
            publication_metrics = None
            await _assert_retained_legacy_cms(fhir, execution)
        if publication_metrics is None:
            publication_metrics = await _ordinary_profile(fhir, execution, run_id, control_run_id, metrics)
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)
    fhir._attach_profile_selection_result(execution, publication_metrics)
    return publication_metrics
