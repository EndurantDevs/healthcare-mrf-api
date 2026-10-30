# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary observations preserve both original reservations and their exact run owner."""

from copy import deepcopy

import pytest

from process import provider_directory_capacity_reservation_snapshot as snapshot
from process import provider_directory_serving_capacity_contract as serving
from process.provider_directory_profile_capacity_attestation_contract import CapacityLeaseConsumptionBinding
from process.provider_directory_profile_capacity_consumption import capacity_lease_consumption_values
from tests.provider_directory_serving_capacity_signing_test_support import serving_case, verify_serving_envelope
from tests.test_provider_directory_capacity_reservation_snapshot import RUN_ID, _preflight, _project
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _run(case):
    return {
        "run_id": RUN_ID,
        "node_id": case.execution.attestation.node_id,
        "importer": "provider-directory-fhir",
        "status": "running",
        "params": {
            **case.guard["healthcare_request"]["profile_execution"],
            snapshot.PROFILE_CAPACITY_PARAM: case.pair,
            serving.EXECUTION_FIELD: case.envelope,
        },
    }


def _consumption(case):
    lease = verify_serving_envelope(case.envelope)
    binding = CapacityLeaseConsumptionBinding(
        RUN_ID, "pdpb_" + "b" * 32, "ab" * 32, "cd" * 32, "ef" * 32, "ba" * 32, "2026-07-30"
    )
    return {
        **capacity_lease_consumption_values(lease, binding, accepted_at=VALIDATION_TIME),
        "admission_purpose": serving.PURPOSE,
    }


@pytest.mark.parametrize("initial_mode", [False, True])
def test_observation_deduplicates_full_original_pair_and_preserves_owner(initial_mode):
    case = serving_case(initial_mode=initial_mode)
    result = _project(
        consumptions=[_consumption(case)],
        runs=[_run(case)],
        preflights=[_preflight(case.envelope), _preflight(case.pair)],
    )
    assert len(result["reservations"]) == 2
    by_purpose = {item["admission_purpose"]: item for item in result["reservations"]}
    assert by_purpose[serving.PURPOSE]["envelope"] == case.envelope
    assert by_purpose["profile"]["envelope"] == case.pair
    assert all(item["durable_preflight_present"] for item in by_purpose.values())
    assert result["owners"][0]["reservation_ids"] == sorted(
        [case.envelope["lease"]["reservation_id"], case.pair["lease"]["reservation_id"]]
    )
    assert {"kind": "paired_profile", "serving_reservation_id": case.envelope["lease"]["reservation_id"]} in by_purpose[
        "profile"
    ]["observations"]
    assert result["capacity_complete"] is False
    assert result["control_authority_required"] is result["signature_verification_required"] is True
    assert result["release_proof_available"] is False


@pytest.mark.parametrize("change", ["profile_field", "cms_field", "both", "missing_pair", "generation"])
def test_observation_refuses_purpose_pair_or_owner_substitution(change):
    case = serving_case()
    run = _run(case)
    if change == "profile_field":
        run["params"][snapshot.PROFILE_CAPACITY_PARAM] = case.envelope
    elif change == "cms_field":
        run["params"][snapshot.CMS_CAPACITY_EXECUTION_PARAM] = run["params"].pop(serving.EXECUTION_FIELD)
    elif change == "both":
        run["params"][snapshot.CMS_CAPACITY_EXECUTION_PARAM] = case.envelope
    elif change == "missing_pair":
        run["params"].pop(snapshot.PROFILE_CAPACITY_PARAM)
    else:
        run["params"]["provider_directory_profile_generation"] += 1
    with pytest.raises(RuntimeError):
        _project(runs=[run])


def test_pending_ordinary_receipt_never_invents_original_authority():
    case = serving_case()
    row = _preflight(case.envelope)
    before = deepcopy(row)
    result = _project(preflights=[row])
    assert row == before
    assert result["reservations"] == result["owners"] == []
    assert result["preflight_receipts"][0]["original_envelope_missing"] is True
