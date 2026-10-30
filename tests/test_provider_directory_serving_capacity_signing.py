# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real signed ordinary receipt binding and old-purpose parity without native admission."""

import importlib
import json
from copy import deepcopy
from dataclasses import replace

import pytest

from process import provider_directory_cms_capacity_contract as cms
from process import provider_directory_cms_nonprofile_capacity as shared
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_serving_capacity_contract as ordinary
from process import provider_directory_serving_capacity_signing as signing
from process.provider_directory_profile_capacity_attestation import assert_database_capacity_lease_reservation
from tests.provider_directory_cms_capacity_test_support import cms_plan, rehash_guard, signed_cms_plan
from tests.provider_directory_serving_capacity_signing_test_support import (
    serving_case,
    sign_serving_guard,
    verify_serving_envelope,
)
from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope, _verify

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _resign(case):
    rehash_guard(case.guard)
    return sign_serving_guard(case.guard, case.pair)


@pytest.mark.parametrize("initial_mode", [False, True])
def test_signed_ordinary_plan_reuses_verifier_and_shared_producer_checks(initial_mode):
    case = serving_case(initial_mode=initial_mode)
    lease = verify_serving_envelope(case.envelope)
    assert signing.validated_serving_capacity_geometry(case.plan.payload) == case.plan
    signing.assert_serving_geometry_matches_request(case.plan, case.request)
    shared._assert_signed_plan(fhir, case.execution, case.plan, lease)
    shared._assert_paired_reservation(lease, case.profile, case.plan.minimum_remaining_bytes)
    assert_database_capacity_lease_reservation(
        lease,
        required_bytes_by_storage_class=dict(case.plan.reservation_bytes),
        minimum_remaining_bytes=case.plan.minimum_remaining_bytes,
        required_build_seconds=case.plan.required_build_seconds,
    )
    assert lease.reservation_id != case.profile.reservation_id
    receipt = lease.signing_preflight_guard["healthcare_receipt"]
    assert receipt["contract_id"] == ordinary.PREFLIGHT_RECEIPT
    assert receipt["profile_execution_identity"]["materialization_mode"] == (
        "full_swap" if initial_mode else "source_delta"
    )
    row = cms.capacity_preflight_receipt_row_values(
        case.request, receipt, issued_at=preflight._utc_timestamp(receipt["issued_at"])
    )
    assert cms.read_capacity_preflight_receipt(row, lease) == receipt
    assert fhir._profile_capacity_preflight_stored_receipt(row, lease) == receipt
    assert json.loads(row["receipt_json"]) == receipt


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract_id", cms.CMS_GEOMETRY_CONTRACT),
        ("admission_purpose", "cms_nonprofile"),
        ("publish_targets", ["profile"]),
        ("native_address_targets", ["another_target"]),
        ("resource_types", ["Practitioner", "Practitioner"]),
        ("desired_profile_as_of", "2026-7-30"),
        ("worker_count", True),
        ("reservation_bytes", [["data", 10], ["temp", 10], ["wal", 10]]),
    ],
)
def test_ordinary_geometry_is_a_closed_native_plan(field, value):
    raw = serving_case().plan.payload
    raw[field] = value
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError):
        signing.validated_serving_capacity_geometry(raw)


@pytest.mark.parametrize("change", ["data_double", "wal_double", "temp", "phase", "pair_digest", "proof", "scope"])
def test_signed_geometry_cannot_recount_retention_or_replace_scope(change):
    case = serving_case()
    plan = case.plan
    scope = case.request.serving_nonprofile_admission["ordinary_scope"]
    reservation_by_class = dict(plan.reservation_bytes)
    if change == "data_double":
        reservation_by_class["data"] += scope["max_retention_data_bytes"]
    elif change == "wal_double":
        reservation_by_class["wal"] += scope["max_retention_wal_bytes"] + scope["max_commit_wal_bytes"]
    elif change == "temp":
        reservation_by_class["temp"] += 1024
    changed_scope_by_field = {**scope, "predecessor_sha256": "99" * 32}
    changed_scope_by_field["input_sha256"] = ordinary.input_sha256(
        {name: changed_scope_by_field[name] for name in ordinary.INPUT_FIELDS}
    )
    replacements_by_case = {
        "phase": {
            "logging_wal_upper_bound_bytes": plan.logging_wal_upper_bound_bytes + 1,
            "cutover_wal_upper_bound_bytes": plan.cutover_wal_upper_bound_bytes - 1,
        },
        "pair_digest": {"paired_profile_lease_digest": "99" * 32},
        "proof": {"selection_proof_id": "99" * 32},
        "scope": {"ordinary_scope_json": ordinary.canonical(changed_scope_by_field)},
    }
    plan = replace(plan, reservation_bytes=tuple(reservation_by_class.items()), **replacements_by_case.get(change, {}))
    case.guard["healthcare_receipt"].update(
        capacity_geometry=plan.payload,
        capacity_geometry_hash=plan.capacity_geometry_hash,
        required_reservation_bytes_by_storage_class=dict(plan.reservation_bytes),
    )
    with pytest.raises(ValueError, match="signing_preflight_guard"):
        verify_serving_envelope(_resign(case))


@pytest.mark.parametrize("change", ["reservation", "projection", "control_scope", "serving", "database", "purpose"])
def test_resigned_receipt_rejects_changed_bindings(change):
    case = serving_case()
    receipt = case.guard["healthcare_receipt"]
    changes_by_name = {
        "reservation": (
            receipt["required_reservation_bytes_by_storage_class"],
            "data",
            receipt["required_reservation_bytes_by_storage_class"]["data"] + 1,
        ),
        "projection": (receipt["artifact_scope_projection"], "projection_hash", "99" * 32),
        "control_scope": (
            case.guard["control_plane_request"][ordinary.ADMISSION_FIELD]["ordinary_scope"],
            "max_commit_wal_bytes",
            999,
        ),
        "serving": (receipt["serving_generation_preflight"], "resolution", "foreign"),
        "database": (receipt["database_binding"], "database_oid", receipt["database_binding"]["database_oid"] + 1),
        "purpose": (receipt, "contract_id", cms.CMS_PREFLIGHT_CONTRACT),
    }
    section, field, value = changes_by_name[change]
    section[field] = value
    if change == "serving":
        receipt["serving_generation_preflight_sha256"] = preflight.preflight_domain_sha256(
            preflight.CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN, receipt["serving_generation_preflight"]
        )
    with pytest.raises(ValueError, match="signing_preflight_guard"):
        verify_serving_envelope(_resign(case))


def test_resigned_pair_substitution_reaches_exact_digest_refusal():
    case = serving_case()
    original_body = deepcopy(case.pair["lease"])
    original_body.pop("attestation_id")
    original_body["reservation_id"] = "synthetic-alternate-profile"
    alternate_pair = _signed_envelope(body_mutator=lambda body: body.update(original_body))
    assert verify_serving_envelope(alternate_pair).lease_digest != case.profile.lease_digest
    for name in ("control_plane_request", "healthcare_request"):
        case.guard[name][ordinary.ADMISSION_FIELD]["paired_profile_lease"] = deepcopy(alternate_pair)
    with pytest.raises(ValueError, match="signing_preflight_guard") as refusal:
        verify_serving_envelope(_resign(case))
    assert "serving_geometry_invalid" in str(refusal.value.__cause__)


def test_profile_reservation_cannot_be_reused_for_ordinary_lease():
    case = serving_case()
    envelope = sign_serving_guard(
        case.guard, case.pair, body_mutator=lambda body: body.update(reservation_id=case.profile.reservation_id)
    )
    lease = verify_serving_envelope(envelope)
    with pytest.raises(RuntimeError, match="profile_reservation_reused"):
        shared._assert_paired_reservation(lease, case.profile, case.plan.minimum_remaining_bytes)


@pytest.mark.parametrize("purpose", ["profile", "cms"])
def test_other_verified_purpose_cannot_satisfy_the_ordinary_plan(purpose):
    case = serving_case()
    foreign = case.profile if purpose == "profile" else signed_cms_plan(cms_plan()[0])
    with pytest.raises(RuntimeError, match="signed_plan_changed"):
        shared._assert_signed_plan(fhir, case.execution, case.plan, foreign)
    case.guard["healthcare_receipt"]["capacity_geometry"] = foreign.signing_preflight_guard["healthcare_receipt"][
        "capacity_geometry"
    ]
    with pytest.raises(ValueError, match="signing_preflight_guard"):
        verify_serving_envelope(_resign(case))


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract_id", cms.CMS_PREFLIGHT_CONTRACT),
        ("request_contract_id", preflight.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID),
        ("selection_proof_id", "99" * 32),
        ("capacity_geometry_hash", "99" * 32),
        ("materialization_mode", "full_swap"),
        ("control_generation", 999),
    ],
)
def test_stored_v6_receipt_keeps_exact_metadata_binding(field, value):
    case = serving_case()
    lease = verify_serving_envelope(case.envelope)
    receipt = lease.signing_preflight_guard["healthcare_receipt"]
    row = cms.capacity_preflight_receipt_row_values(
        case.request, receipt, issued_at=preflight._utc_timestamp(receipt["issued_at"])
    )
    row[field] = value
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError, match="stored_receipt_metadata_changed"
    ):
        cms.read_capacity_preflight_receipt(row, lease)


def test_unsigned_outer_relabel_and_bad_signature_are_rejected():
    case = serving_case()
    envelope = deepcopy(case.envelope)
    envelope["lease"]["signing_preflight_guard"]["healthcare_receipt"]["capacity_geometry"]["ordinary_scope"][
        "input_sha256"
    ] = "99" * 32
    with pytest.raises(ValueError):
        verify_serving_envelope(envelope)
    envelope = deepcopy(case.envelope)
    envelope["signature"] = ("A" if envelope["signature"][0] != "A" else "B") + envelope["signature"][1:]
    with pytest.raises(ValueError, match="invalid_signature"):
        verify_serving_envelope(envelope)


def test_old_profile_golden_and_cms_receipt_bytes_remain_unchanged():
    golden = _verify()
    plan = cms_plan()[0]
    lease = signed_cms_plan(plan)
    assert (
        golden.signing_preflight_guard["healthcare_receipt"]["contract_id"] == preflight.CAPACITY_PREFLIGHT_CONTRACT_ID
    )
    receipt = lease.signing_preflight_guard["healthcare_receipt"]
    request = preflight.validated_capacity_preflight_request(lease.signing_preflight_guard["healthcare_request"])
    row = cms.capacity_preflight_receipt_row_values(
        request, receipt, issued_at=preflight._utc_timestamp(receipt["issued_at"])
    )
    assert cms.read_capacity_preflight_receipt(row, lease) == receipt
    assert receipt["capacity_geometry"] == plan.payload
