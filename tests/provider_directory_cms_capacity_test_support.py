# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real signed Profile pairs and closed CMS receipts using the existing synthetic key."""

from copy import deepcopy
from types import SimpleNamespace

from process import provider_directory_cms_capacity_contract as contract
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_snapshot as snapshot
from process.provider_directory_cms_preparation import NonprofileAdmissionPlan, desired_fence_hash
from tests import provider_directory_profile_capacity_signing_guard_test_support as support
from tests.test_provider_directory_profile_capacity_attestation import (
    VALIDATION_TIME,
    _golden_body,
    _signed_envelope,
    _verify,
)
from tests.test_provider_directory_profile_selection_desired import _desired, _selection_rows


def cms_execution(*, day="2026-07-30"):
    """Use the actual desired-selection computation with the golden runtime's node identity."""
    desired = _desired(day=day)
    catalog, sources, current, candidate = _selection_rows(desired)
    computed = snapshot._computed_desired_selection_from_rows(
        catalog,
        node_id="dev-node",
        source_rows=sources,
        dataset_rows=current,
        desired_dataset_row=candidate,
        desired_selection=desired,
    )
    identity_by_field = {**computed.identity_payload, "authority_revision": 7}
    attestation = selection.validated_profile_selection_attestation(
        {**identity_by_field, "proof_id": selection._proof_id(identity_by_field)}
    )
    return selection.ProviderDirectoryProfileExecution(attestation, 11, capacity_attestation={})


def _profile_guard(execution):
    """Assemble the unchanged Profile chain around one real desired execution."""
    golden = _golden_body()
    timing = support._GuardTiming(
        *(
            preflight._utc_timestamp(golden[name])
            for name in (
                "observed_at",
                "issued_at",
                "expires_at",
                "max_build_deadline",
            )
        ),
        "22" * 32,
    )
    task_by_field = support._execution_by_field(execution)
    limits_by_field = support._limits_payload()
    storage_by_field = support._storage_observation(timing)
    control_request = support._control_plane_request(task_by_field, limits_by_field, storage_by_field, timing)
    execution_request = support._execution_request(task_by_field, limits_by_field, timing)
    identity_by_field = preflight.profile_execution_identity_payload(execution_request)
    followup_by_field = support._held_followup(execution, timing)
    control_receipt = support._control_plane_receipt(
        control_request,
        execution_request,
        identity_by_field,
        storage_by_field,
        followup_by_field,
        timing,
    )
    healthcare_request = support._healthcare_request(task_by_field, limits_by_field, control_receipt, timing)
    validated = support._validated_request(healthcare_request)
    healthcare_receipt = support._healthcare_receipt(
        validated,
        control_receipt,
        identity_by_field,
        limits_by_field,
        "55" * 32,
        timing,
    )
    return support._guard_payload(
        control_request,
        control_receipt,
        healthcare_request,
        validated,
        healthcare_receipt,
        followup_by_field,
    )


def _receipt_digest(receipt):
    """Use each explicit receipt contract as its existing digest domain."""
    receipt["receipt_sha256"] = preflight.preflight_domain_sha256(
        receipt["contract_id"],
        {name: value for name, value in receipt.items() if name != "receipt_sha256"},
    )


def rehash_guard(guard):
    """Repair hash links after a semantic mutation so rejection cannot rely on stale hashes."""
    control = guard["control_plane_receipt"]
    control["request_sha256"] = preflight.preflight_domain_sha256(
        support.guard_contract.CONTROL_PLANE_REQUEST_DIGEST_DOMAIN,
        guard["control_plane_request"],
    )
    _receipt_digest(control)
    request = guard["healthcare_request"]
    request["signing_guard"]["control_plane_receipt_sha256"] = control["receipt_sha256"]
    healthcare = guard["healthcare_receipt"]
    healthcare["request_sha256"] = preflight.preflight_domain_sha256(request["contract_id"], request)
    healthcare["control_plane_receipt_sha256"] = control["receipt_sha256"]
    _receipt_digest(healthcare)
    guard.update(
        control_plane_request_sha256=control["request_sha256"],
        control_plane_receipt_sha256=control["receipt_sha256"],
        healthcare_request_sha256=healthcare["request_sha256"],
        healthcare_receipt_sha256=healthcare["receipt_sha256"],
    )


def sign_guard(guard, *, cms=False):
    """Sign all changed content with the golden Ed25519 key, preserving runtime witness fields."""

    def mutate(body):
        receipt = guard["healthcare_receipt"]
        body.update(
            signing_preflight_guard=deepcopy(guard),
            nonce=receipt["receipt_sha256"],
            capacity_geometry_hash=receipt["capacity_geometry_hash"],
            signing_preflight_guard_sha256=preflight.preflight_domain_sha256(
                support.guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
                guard,
            ),
        )
        if cms:
            body["reservation_id"] = "synthetic-nonprofile"
            for volume in body["volumes"]:
                volume["reserved_bytes"] = receipt["required_reservation_bytes_by_storage_class"][
                    volume["volume_class"]
                ]

    return _signed_envelope(body_mutator=mutate)


def paired_profile_envelope(execution=None):
    """Return a current signed Profile-purpose lease for the exact desired CMS execution."""
    return sign_guard(_profile_guard(execution or cms_execution()))


def cms_plan():
    """Bind every nonprofile plan field to the real paired proof, date, and lease digest."""
    execution = cms_execution()
    profile = _verify(paired_profile_envelope(execution))
    fence = SimpleNamespace(
        datasets=tuple(
            SimpleNamespace(
                **pair, evidence_run_id=pair["acquisition_root_run_id"], artifact_resources=("Practitioner",)
            )
            for pair in execution.attestation.pairs
        )
    )
    plan = NonprofileAdmissionPlan(
        execution.attestation.proof_id,
        execution.attestation.desired_profile_as_of,
        desired_fence_hash(fence),
        "cd" * 32,
        ("address_overlay",),
        ("Practitioner",),
        1000,
        2,
        (("data", 100_000), ("temp", 100_000), ("wal", 100_000)),
        1000,
        60,
        ("entity_address_unified",),
        "ef" * 32,
        1024,
        50_000,
        1000,
        profile.lease_digest,
    )
    return plan, fence, profile


def cms_limits(plan):
    """Transport closed ceilings without passing an asserted engine plan or input hash."""
    return {
        "contract_id": contract.CMS_LIMITS_CONTRACT,
        **{
            name: getattr(plan, name)
            for name in (
                "batch_size",
                "worker_count",
                "temp_file_limit_bytes_per_backend",
                "minimum_remaining_bytes",
                "required_build_seconds",
            )
        },
        **{"max_" + storage + "_bytes": count for storage, count in plan.reservation_bytes},
    }


def cms_guard(plan=None, *, execution=None, paired_envelope=None):
    """Build explicit CMS v4/Control v2 receipts over a complete nonprofile plan."""
    execution = execution or cms_execution()
    plan = plan or cms_plan()[0]
    pair = paired_envelope or paired_profile_envelope(execution)
    guard = _profile_guard(execution)
    admission_by_field = {
        "contract_id": contract.CMS_ADMISSION_REQUEST_CONTRACT,
        "admission_purpose": "cms_nonprofile",
        "paired_profile_lease": pair,
        "limits": cms_limits(plan),
    }
    guard["control_plane_request"].update(
        contract_id=contract.CMS_CONTROL_REQUEST_CONTRACT,
        **{contract.CMS_ADMISSION_FIELD: deepcopy(admission_by_field)},
    )
    guard["control_plane_receipt"].update(
        contract_id=contract.CMS_CONTROL_CONTRACT,
        request_contract_id=contract.CMS_CONTROL_REQUEST_CONTRACT,
    )
    guard["healthcare_request"].update(
        contract_id=contract.CMS_PREFLIGHT_REQUEST_CONTRACT,
        **{contract.CMS_ADMISSION_FIELD: deepcopy(admission_by_field)},
    )
    body = pair["lease"]
    data_tablespace = next(entry for entry in body["tablespaces"] if entry["usage"] == "data")
    guard["healthcare_receipt"].update(
        contract_id=contract.CMS_PREFLIGHT_CONTRACT,
        request_contract_id=contract.CMS_PREFLIGHT_REQUEST_CONTRACT,
        capacity_geometry_hash=plan.capacity_geometry_hash,
        capacity_geometry=plan.payload,
        required_reservation_bytes_by_storage_class=dict(plan.reservation_bytes),
        artifact_scope_projection={"projection_hash": plan.artifact_scope_projection_hash},
        database_binding={
            **{
                name: body[name]
                for name in (
                    "database_system_identifier",
                    "database_oid",
                    "database_name",
                )
            },
            **{name: data_tablespace[name] for name in ("tablespace_oid", "tablespace_name")},
        },
    )
    rehash_guard(guard)
    return guard


def signed_cms_plan(plan):
    """Return actual CMS signature verification, never a Profile-only guard stand-in."""
    return _verify(sign_guard(cms_guard(plan), cms=True), expected_capacity_geometry_hash=plan.capacity_geometry_hash)


def cms_request(*, projection=False):
    """Return the explicit preflight or receipt-free authority-projection request."""
    request = deepcopy(cms_guard()["healthcare_request"])
    if projection:
        request["contract_id"] = contract.CMS_PROJECTION_REQUEST_CONTRACT
        request.pop("signing_guard")
    return request
