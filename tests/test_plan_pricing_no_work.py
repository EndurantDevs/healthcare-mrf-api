# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""No-work witnesses require closed lineage and a definitive producer reply."""

from copy import deepcopy
from datetime import datetime

import pytest

from db.models import ImportRun
from process.plan_pricing_admission import PricingAdmission
from process.plan_pricing_control import DELIVERY_FIELD, pricing_control_digest, projection_admission_key
from process.plan_pricing_no_work import (
    RETRY_INDEX,
    RETRY_PREDICATE,
    _expected_runs,
    _require_terminal_run,
    native_run_coordinates,
)


def _terminal():
    request_by_field = {
        "binding_manifest_digest": "a" * 64,
        "projection_contract": "supported",
        "publication_identity": "b" * 64,
        "publication_attempt_identity": "c" * 64,
        "bindings": [{"synthetic": "private-request-content"}],
    }
    dispatch_by_field = {"key": projection_admission_key(request_by_field), "retry_of": None, "run_id": "exact"}
    record_by_field = {
        "publication_identity": request_by_field["publication_identity"],
        "attempt_identity": request_by_field["publication_attempt_identity"],
        "request_digest": pricing_control_digest(request_by_field),
        "admission_key": dispatch_by_field["key"],
        "dispatches": [dispatch_by_field],
        "engine_run_id": "exact",
        "node_id": "synthetic-node",
        "status": "failed",
        "preparation_status": "admitting",
    }
    run_by_field = {
        "run_id": "exact",
        "engine": "healthcare-mrf-api",
        "node_id": record_by_field["node_id"],
        "importer": "plan-pricing-projection",
        "idempotency_key": dispatch_by_field["key"],
        "retry_of_run_id": None,
        "triggered_by": "release",
        "params": {
            name: request_by_field[name] for name in ("bindings", "binding_manifest_digest", "projection_contract")
        },
        "status": "failed",
        "finished_at": datetime(2026, 10, 15),
        "progress": {"attempt_id": "exact:" + "d" * 32, "attempt_started_at": "2026-10-15T13:00:00+00:00"},
        "metrics": {DELIVERY_FIELD: {"run_id": "exact", "state": "settled", "outcome": "queued"}},
        "row_digest": "e" * 64,
    }
    return PricingAdmission(record_by_field, request_by_field, dispatch_by_field, None, {}), run_by_field


@pytest.mark.parametrize("state", (None, "pending", "unknown"))
def test_terminal_database_state_never_settles_uncertain_producer(state):
    admission, run_by_field = _terminal()
    if state is None:
        run_by_field["metrics"].clear()
    else:
        run_by_field["metrics"][DELIVERY_FIELD]["state"] = state
    with pytest.raises(ValueError, match="producer_not_settled"):
        _require_terminal_run(run_by_field, admission.dispatch, admission)


@pytest.mark.parametrize("damage", ("params", "node_id", "retry_of_run_id", "finished_at", "status"))
def test_terminal_witness_requires_whole_native_request_and_terminal_state(damage):
    admission, run_by_field = _terminal()
    changes_by_field = {"params": {"changed": True}, "retry_of_run_id": "foreign-parent"}
    run_by_field[damage] = changes_by_field.get(damage)
    with pytest.raises(ValueError):
        _require_terminal_run(run_by_field, admission.dispatch, admission)


def test_terminal_journal_with_unacknowledged_dispatch_refuses():
    admission, run_by_field = _terminal()
    _require_terminal_run(run_by_field, admission.dispatch, admission)
    assert _expected_runs(admission) == {"exact": admission.dispatch}
    admission.record_by_field["dispatches"] = [{**admission.dispatch, "run_id": None}]
    admission.record_by_field["engine_run_id"] = None
    with pytest.raises(ValueError):
        _expected_runs(admission)


def test_wire_coordinates_exclude_request_and_raw_metrics():
    admission, run_by_field = _terminal()
    run_by_field = deepcopy(run_by_field)
    run_by_field["metrics"]["private"] = "never-returned"
    coordinates = native_run_coordinates(run_by_field)
    assert coordinates["row_digest"] == run_by_field["row_digest"]
    assert "params" not in coordinates and "private" not in str(coordinates)
    assert "private-request-content" not in str(coordinates)
    _require_terminal_run(run_by_field, admission.dispatch, admission)


def test_runtime_model_keeps_nonunique_complete_retry_census_index():
    matching_specs = [spec for spec in ImportRun.__my_additional_indexes__ if spec["name"] == RETRY_INDEX]
    assert len(matching_specs) == 1
    assert matching_specs[0]["index_elements"] == ("retry_of_run_id", "run_id")
    assert matching_specs[0]["where"] == RETRY_PREDICATE
    assert not matching_specs[0].get("unique", False)
