# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original reservation identities, conservative lifecycle evidence and conflicts."""

import datetime
import json
from copy import deepcopy

import pytest

from process import provider_directory_capacity_reservation_snapshot as snapshot
from process import provider_directory_cms_capacity_contract as cms_contract
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_failed_cleanup as cleanup
from process.provider_directory_profile_capacity_attestation_contract import CapacityLeaseConsumptionBinding
from process.provider_directory_profile_capacity_consumption import capacity_lease_consumption_values
from tests.provider_directory_cms_capacity_test_support import cms_guard, sign_guard
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _signed_envelope, _verify
from tests.test_provider_directory_profile_failed_cleanup import authorization_fixture

RUN_ID = "run_" + "a" * 32


@pytest.fixture(autouse=True)
def _node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _metadata(*, observed_at=VALIDATION_TIME):
    return {
        "observed_at": observed_at,
        "database_snapshot": "100:100:",
        "database_system_identifier": "7527713908662902214",
        "database_oid": 16401,
        "database_name": "healthporta_test",
    }


def _consumption(envelope=None, *, run_id=RUN_ID):
    envelope = envelope or _signed_envelope()
    verified = _verify(envelope, expected_capacity_geometry_hash=envelope["lease"]["capacity_geometry_hash"])
    binding = CapacityLeaseConsumptionBinding(
        run_id, "pdpb_" + "b" * 32, "ab" * 32, "cd" * 32, "ef" * 32, "ba" * 32, "2026-07-30"
    )
    values = capacity_lease_consumption_values(verified, binding, accepted_at=VALIDATION_TIME)
    values["admission_purpose"] = (
        "cms_nonprofile"
        if cms_contract.CMS_ADMISSION_FIELD in verified.signing_preflight_guard["healthcare_request"]
        else "profile"
    )
    return values


def _preflight(envelope):
    guard = envelope["lease"]["signing_preflight_guard"]
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    receipt = guard["healthcare_receipt"]
    return {
        **cms_contract.capacity_preflight_receipt_row_values(
            request, receipt, issued_at=preflight._utc_timestamp(receipt["issued_at"])
        ),
        "consumed_at": None,
        "consumed_run_id": None,
        "consumed_attestation_id": None,
    }


def _run(envelope, *, status="running", run_id=RUN_ID):
    execution = envelope["lease"]["signing_preflight_guard"]["healthcare_request"]["profile_execution"]
    params_by_field = {**deepcopy(execution), snapshot.PROFILE_CAPACITY_PARAM: envelope}
    if cms_contract.CMS_ADMISSION_FIELD in envelope["lease"]["signing_preflight_guard"]["healthcare_request"]:
        params_by_field[snapshot.PROFILE_CAPACITY_PARAM] = envelope["lease"]["signing_preflight_guard"][
            "healthcare_request"
        ][cms_contract.CMS_ADMISSION_FIELD]["paired_profile_lease"]
        params_by_field[snapshot.CMS_CAPACITY_EXECUTION_PARAM] = envelope
    return {
        "run_id": run_id,
        "node_id": execution["provider_directory_profile_selection_attestation"]["node_id"],
        "importer": "provider-directory-fhir",
        "status": status,
        "params": params_by_field,
    }


def _project(*, consumptions=(), runs=(), preflights=(), stages=(), cleanup_claims=(), observed_at=VALIDATION_TIME):
    return snapshot.reservation_projection(
        _metadata(observed_at=observed_at), list(consumptions), list(runs), list(preflights), list(stages),
        list(cleanup_claims),
    )


def _cleanup_claim():
    envelope, _trust, _key = authorization_fixture(VALIDATION_TIME)
    body = envelope["authorization"]
    return {
        **{name: body[name] for name in ("operation_id", "reservation_id", "nonce")},
        **{name: body["checkpoint"][name] for name in ("build_id", "owner_run_id")},
        "checkpoint_preimage_sha256": body["checkpoint"]["preimage_sha256"],
        "authorization_sha256": cleanup.digest(envelope),
        "authorization_json": cleanup.canonical(body),
        "signature": envelope["signature"],
        "claimed_at": cleanup.timestamp(body["issued_at"]),
        "expires_at": cleanup.timestamp(body["expires_at"]),
        "max_operation_deadline": cleanup.timestamp(body["max_operation_deadline"]),
    }, envelope


@pytest.mark.parametrize("offset_microseconds,expected_count", [(-1, 1), (0, 0), (1, 0)])
def test_cleanup_snapshot_excludes_only_signed_expiry(offset_microseconds, expected_count):
    claim, envelope = _cleanup_claim()
    before = deepcopy(claim)
    observed_at = claim["expires_at"] + datetime.timedelta(microseconds=offset_microseconds)
    assert claim["max_operation_deadline"] < observed_at
    result = _project(cleanup_claims=[claim], observed_at=observed_at)
    assert len(result["failed_profile_cleanup_claims"]) == expected_count
    if expected_count:
        assert result["failed_profile_cleanup_claims"][0]["envelope"] == envelope
    assert claim == before
    assert result["release_proof_available"] is False


@pytest.mark.parametrize("field", ["expires_at", "authorization_sha256"])
def test_cleanup_snapshot_refuses_corrupt_expired_claim(field):
    claim, _envelope = _cleanup_claim()
    observed_at = claim["expires_at"] + datetime.timedelta(seconds=1)
    claim[field] = observed_at if field == "expires_at" else "0" * 64
    with pytest.raises(RuntimeError, match="claim_corrupt"):
        _project(cleanup_claims=[claim], observed_at=observed_at)


def test_consumption_and_pending_run_share_one_original_reservation(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    envelope = _signed_envelope()
    consumption = _consumption(envelope)
    result = _project(consumptions=[consumption, consumption], runs=[_run(envelope)], preflights=[_preflight(envelope)])
    assert len(result["reservations"]) == 1
    reservation = result["reservations"][0]
    assert reservation["envelope"] == envelope
    assert reservation["lease_digest"] == consumption["lease_digest"]
    assert reservation["database_binding"] == {name: consumption[name] for name in snapshot._DATABASE_FIELDS}
    assert reservation["durable_preflight_present"] is True
    assert [item["kind"] for item in reservation["observations"]] == ["consumption", "import_run"]
    assert result["capacity_complete"] is False
    assert result["signature_verification_required"] is True
    assert "reserved_bytes_by_volume" not in result
    assert result["preflight_receipts"][0]["original_envelope_missing"] is False


def test_expired_terminal_consumption_is_history_without_release_claim(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    envelope = _signed_envelope()
    result = _project(
        consumptions=[_consumption(envelope)],
        runs=[_run(envelope, status="failed")],
        observed_at=VALIDATION_TIME + datetime.timedelta(days=2),
    )
    observation = result["reservations"][0]["observations"][0]
    assert observation["admission_unexpired"] is False
    assert observation["build_deadline_passed"] is True
    assert observation["release_status"] == "unproven"
    assert result["owners"][0]["active"] is False
    assert result["release_proof_available"] is False


def test_pending_preflight_preserves_receipt_without_inventing_envelope():
    envelope = _signed_envelope()
    row = _preflight(envelope)
    result = _project(preflights=[row])
    pending = result["preflight_receipts"][0]
    assert result["reservations"] == []
    assert pending["pending"] is True
    assert pending["original_envelope_missing"] is True
    assert pending["reservation_id"] is None
    assert pending["record"]["receipt_json"] == json.loads(row["receipt_json"])


def test_cms_run_retains_two_independent_exact_envelopes_once(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    envelope = sign_guard(cms_guard(), cms=True)
    pair = envelope["lease"]["signing_preflight_guard"]["healthcare_request"][cms_contract.CMS_ADMISSION_FIELD][
        "paired_profile_lease"
    ]
    result = _project(
        consumptions=[_consumption(envelope)],
        runs=[_run(envelope)],
        preflights=[_preflight(envelope), _preflight(pair)],
    )
    assert len(result["reservations"]) == 2
    by_purpose = {item["admission_purpose"]: item for item in result["reservations"]}
    assert by_purpose["cms_nonprofile"]["envelope"] == envelope
    assert by_purpose["profile"]["envelope"] == pair
    assert len(result["owners"][0]["reservation_ids"]) == 2


@pytest.mark.parametrize(
    "column,value",
    [
        ("reservation_id", "changed"),
        ("lease_digest", "00" * 32),
        ("volume_identity_hash", "00" * 32),
        ("database_oid", 1),
        ("admission_purpose", "cms_nonprofile"),
    ],
)
def test_consumption_column_drift_rejected(monkeypatch, column, value):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    row = _consumption()
    row[column] = value
    with pytest.raises(RuntimeError, match="consumption_identity_changed"):
        _project(consumptions=[row])


def test_changed_envelope_under_same_reservation_rejected(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    original = _signed_envelope()
    changed = _signed_envelope(body_mutator=lambda body: body.update(reservation_id="other-reservation"))
    with pytest.raises(RuntimeError, match="reservation_identity_conflict"):
        _project(consumptions=[_consumption(original)], runs=[_run(changed)])


def test_same_lease_cannot_have_multiple_active_run_owners(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    envelope = _signed_envelope()
    with pytest.raises(RuntimeError, match="reservation_owner_conflict"):
        _project(runs=[_run(envelope), _run(envelope, run_id="run_" + "c" * 32)])


@pytest.mark.parametrize("field", ["request_nonce", "request_sha256", "capacity_geometry_hash"])
def test_pending_receipt_metadata_drift_is_rejected(field):
    row = _preflight(_signed_envelope())
    row[field] = "00" * 32
    with pytest.raises(RuntimeError, match="preflight_metadata_changed"):
        _project(preflights=[row])


def test_active_owner_without_envelope_remains_an_explicit_gap():
    owner_by_field = {"run_id": RUN_ID, "node_id": "dev-node", "status": "queued", "params": {}}
    result = _project(runs=[owner_by_field])
    assert result["owners"][0]["capacity_envelope_missing"] is True
    assert result["capacity_complete"] is False


def test_stage_presence_does_not_become_cleanup_proof():
    stage_by_field = {
        "expected_oid": 42,
        "current_name_oid": None,
        "expected_oid_current_name": "published_profile",
        "state": "ready",
    }
    result = _project(stages=[stage_by_field])
    assert result["profile_stage_observations"] == [stage_by_field]
    assert result["release_proof_available"] is False


def _legacy_consumption(version):
    """Retain opaque synthetic historical bytes; their old signature is not reverified."""
    row = _consumption()
    body = json.loads(row["canonical_lease_json"])
    body.pop("signing_preflight_guard")
    body.pop("signing_preflight_guard_sha256")
    body["contract_id"] = f"provider-directory-database-capacity-lease-v{version}"
    body["reservation_id"] = f"retained-reservation-{version}"
    body["attestation_id"] = str(version) * 64
    row.update(
        contract_id=body["contract_id"],
        run_id="run_" + str(version) * 32,
        reservation_id=body["reservation_id"],
        attestation_id=body["attestation_id"],
        lease_digest=str(version + 3) * 64,
        canonical_lease_json=json.dumps(body, indent=2),
    )
    return row


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_consumption_retains_exact_opaque_record_and_stored_digests(version):
    row = _legacy_consumption(version)
    original = deepcopy(row)
    result = _project(consumptions=[row, row])
    assert row == original
    assert len(result["reservations"]) == 1
    reservation = result["reservations"][0]
    assert reservation["envelope"] == {"lease": json.loads(row["canonical_lease_json"]), "signature": row["signature"]}
    for name in (
        "reservation_id",
        "attestation_id",
        "lease_digest",
        "tablespace_identity_hash",
        "volume_identity_hash",
    ):
        assert reservation[name] == row[name]
    assert reservation["signature_verification"] == "unresolved_legacy_contract"
    assert reservation["opaque_legacy"] is True
    assert reservation["preflight_receipt_sha256"] is None
    assert reservation["durable_preflight_present"] is False
    assert len(reservation["observations"]) == 1
    assert reservation["observations"][0]["record"] == snapshot._json_values(row)
    assert reservation["observations"][0]["release_status"] == "unproven"
    assert reservation["observations"][0]["admission_unexpired"] is None
    assert reservation["observations"][0]["recorded_expiry_in_future"] is True
    assert result["capacity_complete"] is False and result["signature_verification_required"] is True


def test_legacy_versions_coexist_with_current_lease_without_nonce_inference():
    envelope = _signed_envelope()
    result = _project(
        consumptions=[_legacy_consumption(1), _legacy_consumption(2), _consumption(envelope)],
        preflights=[_preflight(envelope)],
    )
    assert len(result["reservations"]) == 3
    assert sum(item["opaque_legacy"] for item in result["reservations"]) == 2
    # Both synthetic legacy nonces equal this receipt's hash. Only v3 supplies its binding.
    assert result["preflight_receipts"][0]["reservation_id"] == envelope["lease"]["reservation_id"]


@pytest.mark.parametrize("status", ["running", "succeeded"])
def test_legacy_consumption_owner_is_retained_with_unresolved_execution(status):
    row = _legacy_consumption(2)
    envelope_by_field = {"lease": json.loads(row["canonical_lease_json"]), "signature": row["signature"]}
    owner_by_field = {
        "run_id": row["run_id"],
        "node_id": "historical-node",
        "status": status,
        "params": {snapshot.PROFILE_CAPACITY_PARAM: envelope_by_field},
    }
    result = _project(consumptions=[row], runs=[owner_by_field])
    reservation = result["reservations"][0]
    assert result["owners"][0]["reservation_ids"] == [row["reservation_id"]]
    assert reservation["observations"][1]["execution_verification"] == "unresolved_legacy_contract"
    assert result["capacity_complete"] is False
    assert result["release_proof_available"] is False


def test_changed_legacy_owner_envelope_is_rejected():
    row = _legacy_consumption(1)
    changed_envelope_by_field = {"lease": json.loads(row["canonical_lease_json"]), "signature": "changed"}
    owner_by_field = {
        "run_id": row["run_id"],
        "node_id": "historical-node",
        "status": "succeeded",
        "params": {snapshot.PROFILE_CAPACITY_PARAM: changed_envelope_by_field},
    }
    with pytest.raises(RuntimeError, match="legacy_run_envelope_changed"):
        _project(consumptions=[row], runs=[owner_by_field])


@pytest.mark.parametrize("field", ["lease_digest", "canonical_lease_json"])
def test_legacy_reservation_conflict_still_rejected(field):
    row = _legacy_consumption(1)
    changed_record_by_field = {**row, field: "f" * 64 if field == "lease_digest" else '{"changed":true}'}
    with pytest.raises(RuntimeError, match="reservation_identity_conflict"):
        _project(consumptions=[row, changed_record_by_field])
