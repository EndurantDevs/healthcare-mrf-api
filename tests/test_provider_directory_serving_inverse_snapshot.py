# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standalone inverse snapshots retain original bytes, exact owners and conservative claims."""

from copy import deepcopy

import pytest

from process import provider_directory_capacity_reservation_snapshot as snapshot
from process import provider_directory_serving_inverse_capacity as capacity
from process import provider_directory_serving_inverse_capacity_contract as inverse
from process import provider_directory_serving_inverse_capacity_preflight as preflight
from process.provider_directory_profile_capacity_consumption import capacity_lease_consumption_values
from tests.test_provider_directory_capacity_reservation_snapshot import _project
from tests.test_provider_directory_serving_inverse_capacity_admission import RUN, task
from tests.test_provider_directory_serving_inverse_capacity_signing import verify


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def inverse_run(params=None, *, status="running"):
    params = task() if params is None else params
    execution = capacity.task_execution(params).recovery_execution
    return dict(
        run_id=RUN, node_id=execution["node_id"], importer="provider-directory-fhir", status=status, params=params
    )


def inverse_consumption(params):
    lease = verify(params[inverse.EXECUTION_FIELD])
    geometry = lease.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"]
    return {
        "admission_purpose": inverse.PURPOSE,
        **capacity_lease_consumption_values(lease, capacity._binding(RUN, geometry), accepted_at=lease.issued_at),
    }


def inverse_preflight(params):
    request = capacity.task_execution(params)
    receipt = params[inverse.EXECUTION_FIELD]["lease"]["signing_preflight_guard"]["healthcare_receipt"]
    return {
        **preflight.receipt_row_values(request, receipt, issued_at=inverse.utc_second(receipt["issued_at"])),
        "consumed_at": None,
        "consumed_run_id": None,
        "consumed_attestation_id": None,
    }


@pytest.mark.parametrize("has_consumption,status", [(False, "queued"), (True, "running"), (True, "succeeded")])
def test_inverse_snapshot_preserves_one_original_lease_without_profile_pair(has_consumption, status):
    params = task()
    before = deepcopy(params)
    consumed = inverse_consumption(params) if has_consumption else None
    projection = _project(
        consumptions=[consumed, consumed] if has_consumption else [],
        runs=[inverse_run(params, status=status)],
        preflights=[inverse_preflight(params)],
    )
    assert params == before and len(projection["reservations"]) == 1
    observed = projection["reservations"][0]
    assert observed["admission_purpose"] == inverse.PURPOSE
    assert observed["envelope"] == params[inverse.EXECUTION_FIELD]
    assert observed["durable_preflight_present"] is True and observed["opaque_legacy"] is False
    assert observed["signature_verification"] == "required_by_authority"
    assert not any(entry["kind"] == "paired_profile" for entry in observed["observations"])
    assert projection["owners"][0]["reservation_ids"] == [observed["reservation_id"]]
    assert projection["owners"][0]["capacity_envelope_missing"] is False
    assert projection["owners"][0]["active"] is (status != "succeeded")
    assert projection["capacity_complete"] is projection["release_proof_available"] is False
    assert projection["control_authority_required"] is projection["signature_verification_required"] is True


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("provider_directory_serving_operation", "publish"),
        ("provider_directory_serving_generation", "00000000-0000-4000-8000-000000000099"),
        (inverse.EVENT_FIELD, "f" * 64),
        ("source_ids", ["different-source"]),
        ("run_id", "run_" + "b" * 32),
        (snapshot.PROFILE_CAPACITY_PARAM, {}),
        (snapshot.CMS_CAPACITY_EXECUTION_PARAM, {}),
        (snapshot.serving_contract.EXECUTION_FIELD, {}),
        ("import_resources", True),
    ],
)
def test_inverse_snapshot_refuses_changed_operation_or_mixed_authority(field, replacement):
    run_by_field = inverse_run()
    run_by_field["params"][field] = replacement
    with pytest.raises((RuntimeError, ValueError)):
        _project(runs=[run_by_field])


@pytest.mark.parametrize("field,replacement", [("node_id", "other-node"), ("importer", "other"), ("run_id", "bad")])
def test_inverse_snapshot_refuses_a_different_actual_run_owner(field, replacement):
    run_by_field = inverse_run()
    run_by_field[field] = replacement
    with pytest.raises((RuntimeError, ValueError)):
        _project(runs=[run_by_field])


@pytest.mark.parametrize("missing", [inverse.EVENT_FIELD, inverse.EXECUTION_FIELD])
def test_partial_inverse_owner_cannot_disappear_from_accounting(missing):
    run_by_field = inverse_run()
    run_by_field["params"].pop(missing)
    with pytest.raises((RuntimeError, ValueError)):
        _project(runs=[run_by_field])


@pytest.mark.parametrize(
    "field", ["executable_plan_hash", "selection_proof_id", "source_vector_hash", "source_context_vector_hash"]
)
def test_inverse_consumption_keeps_exact_operation_and_historical_provenance(field):
    params = task()
    consumed = inverse_consumption(params)
    consumed[field] = "0" * 64
    with pytest.raises(RuntimeError, match="consumption_identity_changed"):
        _project(consumptions=[consumed])


def test_inverse_snapshot_keeps_the_existing_consumption_owner_conflict_guard():
    params = task()
    run_by_field = inverse_run(params)
    run_by_field["run_id"] = run_by_field["params"]["run_id"] = "run_" + "b" * 32
    with pytest.raises(RuntimeError, match="reservation_owner_conflict"):
        _project(consumptions=[inverse_consumption(params)], runs=[run_by_field])


@pytest.mark.parametrize(
    "field", ["materialization_mode", "limits_contract_id", "control_generation", "serving_preflight_sha256"]
)
def test_inverse_preflight_refuses_relabelled_native_metadata(field):
    params = task()
    stored_by_field = inverse_preflight(params)
    stored_by_field[field] = 99 if field == "control_generation" else "profile"
    with pytest.raises(RuntimeError, match="preflight_inverse_contract_changed"):
        _project(preflights=[stored_by_field])


def test_pending_inverse_receipt_without_original_envelope_is_still_unresolved():
    stored_by_field = inverse_preflight(task())
    before = deepcopy(stored_by_field)
    projection = _project(preflights=[stored_by_field])
    assert stored_by_field == before and projection["reservations"] == projection["owners"] == []
    assert projection["preflight_receipts"][0]["original_envelope_missing"] is True
    assert projection["preflight_receipts"][0]["reservation_id"] is None
    assert projection["release_proof_available"] is False
