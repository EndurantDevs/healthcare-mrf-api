# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic signed ordinary plans through the existing Profile and lease codecs."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_serving_capacity_contract as ordinary
from process.provider_directory_cms_preparation import NonprofileAdmissionPlan, desired_fence_hash
from process.provider_directory_serving_capacity_plan import ordinary_plan
from tests.provider_directory_cms_capacity_test_support import rehash_guard
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_serving_capacity_request_test_support import ordinary_execution, ordinary_request
from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope, _verify


def verify_serving_envelope(envelope):
    """Use the unchanged verifier and the synthetic fixture's exact assigned trust."""
    body = envelope["lease"]
    return _verify(
        envelope,
        trust=capacity_trust_from_envelope(envelope),
        expected_capacity_geometry_hash=body["capacity_geometry_hash"],
        **{"expected_" + name: body[name] for name in ("database_system_identifier", "database_oid", "database_name")},
    )


def sign_serving_guard(guard, paired_envelope, *, body_mutator=None):
    """Issue a distinct synthetic authority; no existing signed body is reused."""
    from tests import provider_directory_profile_capacity_signing_guard_test_support as support

    def mutate(body):
        body.clear()
        body.update(deepcopy(paired_envelope["lease"]))
        body.pop("attestation_id")
        receipt = guard["healthcare_receipt"]
        body.update(
            reservation_id="synthetic-ordinary-serving",
            signing_preflight_guard=deepcopy(guard),
            signing_preflight_guard_sha256=preflight.preflight_domain_sha256(
                support.guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN, guard
            ),
            nonce=receipt["receipt_sha256"],
            capacity_geometry_hash=receipt["capacity_geometry_hash"],
        )
        for volume in body["volumes"]:
            volume["reserved_bytes"] = receipt["required_reservation_bytes_by_storage_class"][volume["volume_class"]]
        if body_mutator is not None:
            body_mutator(body)

    return _signed_envelope(body_mutator=mutate)


def serving_case(*, initial_mode=False):
    """Prepare a real signed Profile pair and a finite ordinary plan, without native claims."""
    raw_request = ordinary_request(initial_mode=initial_mode)
    request = preflight.validated_capacity_preflight_request(raw_request)
    pair = raw_request[ordinary.ADMISSION_FIELD]["paired_profile_lease"]
    profile = verify_serving_envelope(pair)
    execution = replace(ordinary_execution(), capacity_attestation=pair)
    fence = SimpleNamespace(
        datasets=tuple(
            SimpleNamespace(
                **source_pair,
                evidence_run_id=source_pair["acquisition_root_run_id"],
                artifact_resources=("Practitioner",),
                semantic_projection_as_of=None,
            )
            for source_pair in execution.attestation.pairs
        )
    )
    limits = request.serving_nonprofile_admission["limits"]
    base = NonprofileAdmissionPlan(
        execution.attestation.proof_id,
        execution.attestation.desired_profile_as_of,
        desired_fence_hash(fence),
        "cd" * 32,
        ("address_overlay",),
        ("Practitioner",),
        limits["batch_size"],
        limits["worker_count"],
        tuple((name, limits["max_" + name + "_bytes"]) for name in ("data", "temp", "wal")),
        limits["minimum_remaining_bytes"],
        limits["required_build_seconds"],
        ordinary.NATIVE_ADDRESS_TARGETS,
        "ef" * 32,
        limits["temp_file_limit_bytes_per_backend"],
        limits["max_wal_bytes"] // 2,
        limits["max_wal_bytes"] - limits["max_wal_bytes"] // 2,
        profile.lease_digest,
    )
    plan = ordinary_plan(
        base,
        request.serving_nonprofile_admission["ordinary_scope"],
        paired_profile_wal=ordinary.paired_profile_wal_bytes(profile.signing_preflight_guard["healthcare_receipt"]),
    )
    guard = _serving_guard(request, plan, pair)
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    return SimpleNamespace(
        request=request,
        execution=execution,
        fence=fence,
        pair=pair,
        profile=profile,
        plan=plan,
        guard=guard,
        envelope=sign_serving_guard(guard, pair),
    )


def _serving_guard(request, plan, pair):
    guard = deepcopy(pair["lease"]["signing_preflight_guard"])
    for name in ("control_plane_request", "control_plane_receipt", "healthcare_request", "healthcare_receipt"):
        guard[name].pop("profile_materialization", None)
    guard["control_plane_request"].update(
        contract_id=ordinary.CONTROL_REQUEST,
        **{ordinary.ADMISSION_FIELD: deepcopy(request.serving_nonprofile_admission)},
    )
    guard["control_plane_receipt"].update(
        contract_id=ordinary.CONTROL_RECEIPT,
        request_contract_id=ordinary.CONTROL_REQUEST,
    )
    guard["healthcare_request"] = deepcopy(request.request_payload)
    body = pair["lease"]
    data_tablespace = next(entry for entry in body["tablespaces"] if entry["usage"] == "data")
    guard["healthcare_receipt"].update(
        contract_id=ordinary.PREFLIGHT_RECEIPT,
        request_contract_id=ordinary.PREFLIGHT_REQUEST,
        capacity_geometry=plan.payload,
        capacity_geometry_hash=plan.capacity_geometry_hash,
        required_reservation_bytes_by_storage_class=dict(plan.reservation_bytes),
        artifact_scope_projection={
            "projected_rows": 1,
            "projected_logical_bytes": 128,
            "projection_hash": plan.artifact_scope_projection_hash,
        },
        database_binding={
            **{name: body[name] for name in ("database_system_identifier", "database_oid", "database_name")},
            **{name: data_tablespace[name] for name in ("tablespace_oid", "tablespace_name")},
        },
    )
    nonce = "39" * 32
    guard["control_plane_request"]["signing_intent"]["request_nonce"] = nonce
    guard["control_plane_receipt"]["request_nonce"] = nonce
    guard["healthcare_request"]["signing_guard"]["request_nonce"] = nonce
    guard["healthcare_receipt"]["request_nonce"] = nonce
    rehash_guard(guard)
    return guard
