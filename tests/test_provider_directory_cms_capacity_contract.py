# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit CMS capacity lanes and complete signature-bound engine geometry."""

import datetime
from copy import deepcopy
from dataclasses import fields, replace

import pytest

from process import provider_directory_cms_capacity_contract as contract
from process import provider_directory_profile_capacity_attestation as attestation
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_selection_contract as selection
from process.provider_directory_cms_preparation import NonprofileAdmissionPlan
from tests.provider_directory_cms_capacity_test_support import (
    cms_execution,
    cms_guard,
    cms_plan,
    cms_request,
    paired_profile_envelope,
    rehash_guard,
    sign_guard,
)
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _golden_body, _trust, _verify


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    """Validate every task against the same synthetic runtime identity as the signed pair."""
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


@pytest.mark.parametrize("projection", [False, True])
def test_explicit_cms_request_preserves_exact_desired_execution(projection):
    raw = cms_request(projection=projection)
    validate = (
        preflight.validated_capacity_authority_projection_request
        if projection
        else preflight.validated_capacity_preflight_request
    )
    request = validate(raw)
    assert request.request_payload == raw
    assert request.request_sha256 == preflight.preflight_domain_sha256(raw["contract_id"], raw)
    assert request.cms_nonprofile_admission == raw[contract.CMS_ADMISSION_FIELD]
    assert request.execution == cms_execution()
    assert request.execution.attestation.desired_profile_as_of == "2026-07-30"
    assert request.execution.attestation.desired_cms_dataset["is_current"] is False
    pair = contract.verified_cms_paired_profile_lease(request, trust=_trust(), now=VALIDATION_TIME)
    assert pair == _verify(raw[contract.CMS_ADMISSION_FIELD]["paired_profile_lease"])


def test_real_cms_signature_binds_complete_plan_and_database():
    plan, _fence, pair = cms_plan()
    guard = cms_guard(plan)
    lease = _verify(sign_guard(guard, cms=True), expected_capacity_geometry_hash=plan.capacity_geometry_hash)
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    assert contract.validated_cms_capacity_geometry(guard["healthcare_receipt"]["capacity_geometry"]) == plan
    contract.assert_cms_geometry_matches_request(plan, request)
    assert plan.paired_profile_lease_digest == pair.lease_digest
    assert lease.signing_preflight_guard["control_plane_receipt"]["contract_id"] == contract.CMS_CONTROL_CONTRACT
    assert lease.signing_preflight_guard["healthcare_receipt"]["contract_id"] == contract.CMS_PREFLIGHT_CONTRACT
    assert lease.reservation_bytes_by_storage_class == dict(plan.reservation_bytes)
    binding = guard["healthcare_receipt"]["database_binding"]
    assert contract.validated_cms_database_binding(binding) == binding
    assert set(binding) == {
        "database_system_identifier",
        "database_oid",
        "database_name",
        "tablespace_oid",
        "tablespace_name",
    }


def _cms_task():
    """Use one actual signed pair and the exact desired execution carried by its guard."""
    guard = cms_guard()
    task_by_field = deepcopy(guard["healthcare_request"]["profile_execution"])
    task_by_field["provider_directory_profile_capacity_attestation"] = deepcopy(
        guard["healthcare_request"][contract.CMS_ADMISSION_FIELD]["paired_profile_lease"]
    )
    task_by_field[selection.CMS_CAPACITY_EXECUTION_PARAM] = sign_guard(guard, cms=True)
    return task_by_field


def test_cms_task_retains_both_exact_leases_without_changing_profile_execution():
    task_by_field = _cms_task()
    execution = selection.validated_profile_execution(task_by_field)
    assert execution.attestation == cms_execution().attestation
    assert execution.generation == cms_execution().generation
    assert execution.capacity_attestation == task_by_field["provider_directory_profile_capacity_attestation"]
    assert execution.cms_nonprofile_capacity_attestation == task_by_field[selection.CMS_CAPACITY_EXECUTION_PARAM]
    task_by_field[selection.CMS_CAPACITY_EXECUTION_PARAM]["lease"]["reservation_id"] = "changed-reservation"
    assert execution.cms_nonprofile_capacity_attestation["lease"]["reservation_id"] != "changed-reservation"


@pytest.mark.parametrize(
    "change", ["profile-purpose", "changed-pair", "changed-generation", "changed-execution", "empty"]
)
def test_cms_task_rejects_wrong_purpose_pair_or_execution(change):
    task_by_field = _cms_task()
    if change == "profile-purpose":
        task_by_field[selection.CMS_CAPACITY_EXECUTION_PARAM] = task_by_field[
            "provider_directory_profile_capacity_attestation"
        ]
    elif change == "changed-pair":
        task_by_field["provider_directory_profile_capacity_attestation"]["signature"] = "A" * 86
    elif change == "changed-generation":
        task_by_field["provider_directory_profile_generation"] += 1
    elif change == "changed-execution":
        task_by_field["provider_directory_profile_selection_attestation"] = cms_execution(
            day="2026-07-31"
        ).attestation.payload
    else:
        task_by_field[selection.CMS_CAPACITY_EXECUTION_PARAM] = None
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError, match="CMS capacity attestation is invalid"):
        selection.validated_profile_execution(task_by_field)


def test_legacy_profile_task_rejects_cms_capacity_field():
    from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution

    task_by_field = _cms_task()
    task_by_field["provider_directory_profile_selection_attestation"] = (
        synthetic_profile_execution().attestation.payload
    )
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError, match="CMS capacity attestation is invalid"):
        selection.validated_profile_execution(task_by_field)
    task_by_field.pop(selection.CMS_CAPACITY_EXECUTION_PARAM)
    execution = selection.validated_profile_execution(task_by_field)
    assert execution.cms_nonprofile_capacity_attestation is None


@pytest.mark.parametrize("projection", [False, True])
@pytest.mark.parametrize(
    "missing", ["profile_execution", "cms_nonprofile_admission", "provider_directory_profile_capacity_limits"]
)
def test_cms_request_requires_each_authority_input(projection, missing):
    request = cms_request(projection=projection)
    request.pop(missing)
    validate = (
        preflight.validated_capacity_authority_projection_request
        if projection
        else preflight.validated_capacity_preflight_request
    )
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="fields_invalid"):
        validate(request)


@pytest.mark.parametrize(
    "location,field",
    [
        ("request", "signed_plan"),
        ("request", "capacity_geometry"),
        ("admission", "native_address_input_hash"),
        ("admission", "capacity_geometry_hash"),
        ("limits", "artifact_scope_projection_hash"),
    ],
)
def test_callers_cannot_supply_engine_plan_or_projection(location, field):
    request = cms_request()
    admission = request[contract.CMS_ADMISSION_FIELD]
    targets_by_name = {"request": request, "admission": admission, "limits": admission["limits"]}
    targets_by_name[location][field] = "a" * 64
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="fields_invalid"):
        preflight.validated_capacity_preflight_request(request)


@pytest.mark.parametrize("missing", ["contract_id", "admission_purpose", "paired_profile_lease", "limits"])
def test_cms_admission_requires_its_closed_fields(missing):
    admission = cms_request()[contract.CMS_ADMISSION_FIELD]
    admission.pop(missing)
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="fields_invalid"):
        contract.validated_cms_capacity_admission(admission)


@pytest.mark.parametrize("missing", sorted(contract.CMS_LIMIT_FIELDS))
def test_cms_limits_require_all_reviewed_ceilings(missing):
    admission = cms_request()[contract.CMS_ADMISSION_FIELD]
    admission["limits"].pop(missing)
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="fields_invalid"):
        contract.validated_cms_capacity_admission(admission)


@pytest.mark.parametrize(
    "field,value",
    [
        ("worker_count", True),
        ("max_data_bytes", 0),
        ("temp_file_limit_bytes_per_backend", 1),
        ("max_temp_bytes", 1024),
        ("required_build_seconds", -1),
    ],
)
def test_cms_limits_reject_unbounded_execution(field, value):
    admission = cms_request()[contract.CMS_ADMISSION_FIELD]
    admission["limits"][field] = value
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError):
        contract.validated_cms_capacity_admission(admission)


@pytest.mark.parametrize(
    "change", ["wrong-purpose", "recursive-pair", "missing-pair-signature", "profile-only-execution"]
)
def test_other_purposes_cannot_enter_cms_lane(change):
    request = cms_request()
    admission = request[contract.CMS_ADMISSION_FIELD]
    if change == "wrong-purpose":
        admission["admission_purpose"] = "profile"
    elif change == "recursive-pair":
        admission["paired_profile_lease"] = sign_guard(cms_guard(), cms=True)
    elif change == "missing-pair-signature":
        admission["paired_profile_lease"].pop("signature")
    else:
        request["profile_execution"] = _golden_body()["signing_preflight_guard"]["healthcare_request"][
            "profile_execution"
        ]
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError):
        preflight.validated_capacity_preflight_request(request)


def test_signature_shape_does_not_replace_pair_authentication():
    request = cms_request()
    envelope = request[contract.CMS_ADMISSION_FIELD]["paired_profile_lease"]
    envelope["signature"] = ("A" if envelope["signature"][0] != "A" else "B") + envelope["signature"][1:]
    parsed = preflight.validated_capacity_preflight_request(request)
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="invalid_signature"):
        contract.verified_cms_paired_profile_lease(parsed, trust=_trust(), now=VALIDATION_TIME)


def test_cms_outer_signature_is_verified():
    guard = cms_guard()
    envelope = sign_guard(guard, cms=True)
    envelope["signature"] = ("A" if envelope["signature"][0] != "A" else "B") + envelope["signature"][1:]
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="invalid_signature"):
        _verify(envelope, expected_capacity_geometry_hash=guard["healthcare_receipt"]["capacity_geometry_hash"])


@pytest.mark.parametrize(
    "override",
    [
        {"public_key": b"\0" * 32},
        {"key_id": "other-key"},
        {"environment_id": "other-environment"},
        {"attestor_id": "other-authority"},
        {"attestor_release_digest": "99" * 32},
        {"database_oid": 16402},
    ],
)
def test_pair_requires_configured_trust_pins(override):
    request = preflight.validated_capacity_preflight_request(cms_request())
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError):
        contract.verified_cms_paired_profile_lease(request, trust=_trust(**override), now=VALIDATION_TIME)


def test_pair_requires_current_trust_and_exact_execution():
    request = preflight.validated_capacity_preflight_request(cms_request())
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="trust_required"):
        contract.verified_cms_paired_profile_lease(request, trust=None, now=VALIDATION_TIME)
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError):
        contract.verified_cms_paired_profile_lease(
            request, trust=_trust(), now=VALIDATION_TIME + datetime.timedelta(hours=2)
        )
    changed = cms_request()
    changed[contract.CMS_ADMISSION_FIELD]["paired_profile_lease"] = paired_profile_envelope(
        cms_execution(day="2026-07-31")
    )
    request = preflight.validated_capacity_preflight_request(changed)
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="execution_changed"):
        contract.verified_cms_paired_profile_lease(request, trust=_trust(), now=VALIDATION_TIME)


def test_relabelled_pair_rejects_before_nested_decoder(monkeypatch):
    request = cms_request()
    body = request[contract.CMS_ADMISSION_FIELD]["paired_profile_lease"]["lease"]
    nested = body["signing_preflight_guard"]["healthcare_request"]
    nested["contract_id"] = contract.CMS_PREFLIGHT_REQUEST_CONTRACT
    nested[contract.CMS_ADMISSION_FIELD] = deepcopy(request[contract.CMS_ADMISSION_FIELD])

    def unexpected_decoder(_body):
        """Fail if a relabelled nested pair reaches recursive decoding."""
        raise AssertionError("nested decoder must not run")

    monkeypatch.setattr(attestation, "_parse_capacity_lease_fields", unexpected_decoder)
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError, match="paired_profile_purpose_invalid"
    ):
        preflight.validated_capacity_preflight_request(request)


def _cms_receipt_row():
    """Use the real signed chain and production row mapper for persistence checks."""
    guard = cms_guard()
    receipt = guard["healthcare_receipt"]
    lease = _verify(sign_guard(guard, cms=True), expected_capacity_geometry_hash=receipt["capacity_geometry_hash"])
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    issued_at = preflight._utc_timestamp(receipt["issued_at"])
    row = contract.capacity_preflight_receipt_row_values(request, receipt, issued_at=issued_at)
    return request, receipt, lease, row


def test_cms_receipt_roundtrips_matching_versions_and_all_metadata():
    _request, receipt, lease, row = _cms_receipt_row()
    assert row["contract_id"] == contract.CMS_PREFLIGHT_CONTRACT
    assert row["request_contract_id"] == contract.CMS_PREFLIGHT_REQUEST_CONTRACT
    assert contract.read_capacity_preflight_receipt(row, lease) == receipt


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract_id", preflight.CAPACITY_PREFLIGHT_CONTRACT_ID),
        ("request_sha256", "00" * 32),
        ("control_generation", 12),
        ("selection_proof_id", "00" * 32),
    ],
)
def test_cms_receipt_rejects_altered_ledger_metadata(field, value):
    _request, _receipt, lease, row = _cms_receipt_row()
    row[field] = value
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError, match="stored_receipt_metadata_changed"
    ):
        contract.read_capacity_preflight_receipt(row, lease)


@pytest.mark.parametrize("field", ["contract_id", "request_contract_id", "request_sha256"])
def test_receipt_mapper_rejects_crossed_versions_or_foreign_request(field):
    request, receipt, _lease, row = _cms_receipt_row()
    receipt[field] = "foreign-contract" if field != "request_sha256" else "00" * 32
    with pytest.raises(
        preflight.ProviderDirectoryProfileCapacityPreflightError, match="receipt_version_or_request_changed"
    ):
        contract.capacity_preflight_receipt_row_values(request, receipt, issued_at=row["issued_at"])


@pytest.mark.asyncio
@pytest.mark.parametrize("projection", [False, True])
async def test_cms_request_dispatches_to_separate_producer(monkeypatch, projection):
    import importlib
    from unittest.mock import AsyncMock, Mock

    from process import provider_directory_cms_preflight as cms_preflight

    fhir = importlib.import_module("process.provider_directory_fhir")
    database_work = Mock(side_effect=AssertionError("database work must not start"))
    monkeypatch.setattr(fhir.db, "transaction", database_work)
    validate = (
        preflight.validated_capacity_authority_projection_request
        if projection
        else preflight.validated_capacity_preflight_request
    )
    produce = (
        fhir.provider_directory_profile_capacity_authority_projection
        if projection
        else fhir.provider_directory_profile_capacity_preflight
    )
    runner = AsyncMock(return_value={"cms": True})
    monkeypatch.setattr(cms_preflight, "capacity_authority_projection" if projection else "capacity_preflight", runner)
    request = validate(cms_request(projection=projection))
    assert await produce(request) == {"cms": True}
    runner.assert_awaited_once_with(fhir, request)
    database_work.assert_not_called()


@pytest.mark.parametrize("missing", [field.name for field in fields(NonprofileAdmissionPlan)])
def test_engine_geometry_requires_every_plan_field(missing):
    geometry = cms_plan()[0].payload
    geometry.pop(missing)
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="geometry_fields_invalid"):
        contract.validated_cms_capacity_geometry(geometry)


@pytest.mark.parametrize(
    "field,value",
    [
        ("worker_count", True),
        ("temp_file_limit_bytes_per_backend", 1),
        ("desired_profile_as_of", "2026-7-30"),
        ("native_address_targets", []),
        ("paired_profile_lease_digest", ""),
        ("reservation_bytes", [["data", 1000]]),
    ],
)
def test_engine_geometry_rejects_unbounded_or_noncanonical_values(field, value):
    geometry = cms_plan()[0].payload
    geometry[field] = value
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError):
        contract.validated_cms_capacity_geometry(geometry)


@pytest.mark.parametrize(
    "field,value",
    [
        ("selection_proof_id", "99" * 32),
        ("desired_profile_as_of", "2026-07-31"),
        ("paired_profile_lease_digest", "99" * 32),
        ("batch_size", 2000),
        ("reservation_bytes", (("data", 100_001), ("temp", 100_000), ("wal", 100_000))),
    ],
)
def test_plan_cannot_outgrow_or_retarget_request(field, value):
    plan = replace(cms_plan()[0], **{field: value})
    request = preflight.validated_capacity_preflight_request(cms_request())
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="geometry_request_changed"):
        contract.assert_cms_geometry_matches_request(plan, request)


@pytest.mark.parametrize("change", ["geometry", "reservation", "date", "projection", "control-admission"])
def test_resigned_cms_chain_rejects_changed_semantics(change):
    plan = cms_plan()[0]
    guard = cms_guard(plan)
    receipt = guard["healthcare_receipt"]
    if change == "geometry":
        receipt["capacity_geometry"]["native_address_input_hash"] = "99" * 32
    elif change == "reservation":
        receipt["required_reservation_bytes_by_storage_class"]["data"] += 1
    elif change == "date":
        changed = replace(plan, desired_profile_as_of="2026-07-31")
        receipt.update(capacity_geometry=changed.payload, capacity_geometry_hash=changed.capacity_geometry_hash)
    elif change == "projection":
        receipt["artifact_scope_projection"]["projection_hash"] = "99" * 32
    else:
        guard["control_plane_request"][contract.CMS_ADMISSION_FIELD]["limits"]["max_data_bytes"] += 1
    rehash_guard(guard)
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="signing_preflight_guard"):
        _verify(sign_guard(guard, cms=True), expected_capacity_geometry_hash=receipt["capacity_geometry_hash"])


@pytest.mark.parametrize(
    "field,value",
    [
        ("database_system_identifier", "7527713908662902215"),
        ("database_oid", 16402),
        ("database_name", "other_database"),
        ("tablespace_oid", 1664),
        ("tablespace_name", "other_tablespace"),
    ],
)
def test_resigned_cms_database_binding_must_match_outer_lease(field, value):
    guard = cms_guard()
    guard["healthcare_receipt"]["database_binding"][field] = value
    rehash_guard(guard)
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="signing_preflight_guard"):
        _verify(
            sign_guard(guard, cms=True),
            expected_capacity_geometry_hash=guard["healthcare_receipt"]["capacity_geometry_hash"],
        )


@pytest.mark.parametrize(
    "missing", ["database_system_identifier", "database_oid", "database_name", "tablespace_oid", "tablespace_name"]
)
def test_cms_database_binding_requires_all_five_fields(missing):
    guard = cms_guard()
    guard["healthcare_receipt"]["database_binding"].pop(missing)
    rehash_guard(guard)
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="signing_preflight_guard"):
        _verify(
            sign_guard(guard, cms=True),
            expected_capacity_geometry_hash=guard["healthcare_receipt"]["capacity_geometry_hash"],
        )


@pytest.mark.parametrize("projection", [False, True])
def test_profile_request_contract_cannot_transport_cms_admission(projection):
    request = cms_request(projection=projection)
    request["contract_id"] = (
        preflight.CAPACITY_AUTHORITY_PROJECTION_REQUEST_CONTRACT_ID
        if projection
        else preflight.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID
    )
    validate = (
        preflight.validated_capacity_authority_projection_request
        if projection
        else preflight.validated_capacity_preflight_request
    )
    with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="fields_invalid"):
        validate(request)


def test_profile_guard_cannot_smuggle_cms_geometry():
    plan = cms_plan()[0]
    guard = deepcopy(paired_profile_envelope()["lease"]["signing_preflight_guard"])
    guard["healthcare_receipt"].update(
        capacity_geometry=plan.payload,
        capacity_geometry_hash=plan.capacity_geometry_hash,
        required_reservation_bytes_by_storage_class=dict(plan.reservation_bytes),
        artifact_scope_projection={"projection_hash": plan.artifact_scope_projection_hash},
    )
    rehash_guard(guard)
    with pytest.raises(attestation.ProviderDirectoryCapacityLeaseError, match="signing_preflight_guard"):
        _verify(sign_guard(guard), expected_capacity_geometry_hash=plan.capacity_geometry_hash)


def test_existing_profile_golden_contract_remains_unchanged():
    verified = _verify()
    raw = verified.signing_preflight_guard["healthcare_request"]
    request = preflight.validated_capacity_preflight_request(raw)
    assert request.cms_nonprofile_admission is None
    assert request.request_payload["contract_id"] == preflight.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID
    projection_by_field = {name: value for name, value in raw.items() if name != "signing_guard"}
    projection_by_field["contract_id"] = preflight.CAPACITY_AUTHORITY_PROJECTION_REQUEST_CONTRACT_ID
    assert (
        preflight.validated_capacity_authority_projection_request(projection_by_field).cms_nonprofile_admission is None
    )
