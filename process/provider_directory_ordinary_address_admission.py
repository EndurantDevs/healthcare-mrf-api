# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind ordinary Address admission to the coordinator's original server capture."""

from dataclasses import dataclass

from process import provider_directory_serving_capacity_contract as contract
from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan


@dataclass(frozen=True)
class _FinishedProfileCutover:
    """Retain one completed Profile interval within its existing native common owner."""

    admission: object
    profile: object
    owner: object
    actor_json: str


def require_capture(fhir, plan, capture):
    """Worker JSON and detached captures never establish native input authority."""
    from process.provider_directory_artifact_bundle_preparation import CapturedServingInputs

    contract.require(type(plan) is OrdinaryServingAdmissionPlan, "plan_type")
    contract.require(type(capture) is CapturedServingInputs and capture.database is fhir.db, "capture_owner")
    contract.require(
        plan.payload["ordinary_scope"] == contract.validate_scope(capture.scope_payload), "capture_changed"
    )
    return capture


async def recheck_capture(fhir, execution, plan, capture):
    """Recheck current semantic inputs on the existing owner's actual database."""
    captured = require_capture(fhir, plan, capture)
    contract.require_plan_selection(plan.payload, execution.attestation.payload, captured.scope_payload)
    await captured.recheck(fhir)
    require_capture(fhir, plan, capture)


async def produce_ordinary_address_admission(
    fhir,
    execution,
    fence,
    *,
    run_id,
    signed_plan,
    captured_inputs,
    fresh_storage_envelope,
):
    """Use the actual execution's assigned pair on the existing runtime producer."""
    from process.provider_directory_cms_nonprofile_capacity import _produce_admission

    require_capture(fhir, signed_plan, captured_inputs)
    return await _produce_admission(
        fhir,
        execution,
        fence,
        run_id=run_id,
        envelopes=(execution.serving_nonprofile_capacity_attestation, execution.capacity_attestation),
        signed_plan=signed_plan,
        captured_inputs=captured_inputs,
        fresh_storage_envelope=fresh_storage_envelope,
    )


async def _cutover_owner(fhir, admission):
    """Use the already-issued terminal fence and existing measured transaction owner."""
    from process import provider_directory_generic_source_artifacts as artifacts
    from process import provider_directory_owned_wal_transaction as owned
    from process import provider_directory_serving_recovery_receipts as receipts

    capture, _factory, session = artifacts._ordinary_terminal_owner(fhir, admission)
    contract.require(artifacts.is_fence_held(fhir, capture._fence, None), "finished_profile_fence")
    producer = admission.check_cutover.__self__
    owner = owned.current_owned_wal_transaction(fhir.db)
    contract.require(
        type(owner) is owned.OwnedWalTransaction
        and owner.session is session
        and owner.commit_state == "not_attempted"
        and not owner.cleanup_complete
        and producer._cutover_binding is fhir.db._transaction_binding()
        and producer._cutover_binding is not None
        and type(producer.cutover_wal_start_bytes) is int
        and producer.cutover_wal_start_bytes >= 0
        and fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get() is None,
        "finished_profile_owner",
    )
    actual_session, driver, actor = await receipts.ordinary(fhir.db)
    contract.require(
        actual_session is session and driver is owner.retained_driver and actor["backend"] == owner.retained_pid,
        "finished_profile_owner",
    )
    return producer, owner, receipts.encoded(actor)


async def finish_profile_cutover(prepared):
    """Seal Profile only after its original final WAL and commit-envelope checks."""
    from process.provider_directory_cms_preparation import PreparedServingArtifacts

    contract.require(type(prepared) is PreparedServingArtifacts, "finished_profile_preparation")
    fhir, admission = prepared.fhir, prepared.nonprofile_admission
    producer, owner, actor_json = await _cutover_owner(fhir, admission)
    profile = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get()
    contract.require(
        producer._ordinary_finished_profile is None
        and profile is not None
        and profile is producer.resumed_profile
        and prepared.execution is producer.execution
        and prepared.fence is producer.fence
        and profile.wal_tracker.unresolved_window is False,
        "finished_profile_admission",
    )
    await producer._assert_active_profile(profile)
    await fhir._assert_provider_directory_profile_wal_budget(profile)
    producer._ordinary_finished_profile = _FinishedProfileCutover(admission, profile, owner, actor_json)


async def finished_profile_for_cutover(producer, request):
    """Keep the exact paired admission after its meter closes, only on this ordinary cut."""
    from process.provider_directory_cms_preparation import _ACTIVE

    marker = producer._ordinary_finished_profile
    contract.require(
        type(producer.plan) is OrdinaryServingAdmissionPlan
        and type(marker) is _FinishedProfileCutover
        and producer.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is None
        and _ACTIVE.get() is marker.admission
        and producer.resumed_profile is marker.profile
        and request.phase == "cutover"
        and request.plan is producer.plan
        and request.lease is producer.lease,
        "finished_profile_admission",
    )
    current, owner, actor_json = await _cutover_owner(producer.fhir, marker.admission)
    contract.require(
        current is producer and owner is marker.owner and actor_json == marker.actor_json,
        "finished_profile_owner",
    )
    return marker.profile
