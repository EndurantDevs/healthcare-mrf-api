# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared missing-record acceptance boundaries, independent of importer type."""

import copy

import pytest

from process.import_completion_policy import (
    IMPORT_COMPLETION_POLICY_ID,
    completion_receipt,
    import_completion_decision,
    validate_completion_receipt,
    validate_completion_request,
)


@pytest.mark.parametrize(
    ("expected", "retained", "exhausted", "disposition"),
    [
        (200, 199, True, "accepted_partial"),
        (199, 198, True, "rejected"),
        (200, 198, True, "rejected"),
        (200, 199, False, "retry"),
        (200, 200, False, "complete"),
        (0, 0, False, "complete"),
        (10**30, 995 * 10**27, True, "accepted_partial"),
        (10**30, 995 * 10**27 - 1, True, "rejected"),
    ],
)
def test_verified_record_coverage(expected, retained, exhausted, disposition):
    decision = import_completion_decision(expected_count=expected, retained_count=retained, retries_exhausted=exhausted)
    assert decision["disposition"] == disposition
    assert decision["expected_count"] == expected
    assert decision["retained_count"] == retained
    assert decision["missing_count"] == expected - retained
    assert decision["max_missing_basis_points"] == 50


@pytest.mark.parametrize(
    ("retained", "exhausted", "operator_acceptance", "disposition"),
    [
        (199, True, True, "accepted_partial"),
        (199, True, False, "rejected"),
        (199, False, True, "accepted_partial"),
        (0, True, True, "rejected"),
    ],
)
def test_unknown_coverage_stays_unknown(retained, exhausted, operator_acceptance, disposition):
    decision = import_completion_decision(
        expected_count=None,
        retained_count=retained,
        retries_exhausted=exhausted,
        accept_unknown_coverage=operator_acceptance,
    )
    assert decision["disposition"] == disposition
    assert decision["expected_count"] is decision["missing_count"] is None
    if disposition == "accepted_partial":
        assert decision["acceptance_basis"] == "operator_accepted_unknown"
        assert decision["retries_exhausted"] is exhausted


@pytest.mark.parametrize(
    "changes",
    [
        {"expected_count": True},
        {"expected_count": -1},
        {"expected_count": "200"},
        {"expected_count": 198},
        {"retained_count": True},
        {"retained_count": -1},
        {"retained_count": 200.0},
        {"retries_exhausted": 1},
        {"accept_unknown_coverage": "yes"},
    ],
)
def test_malformed_or_inconsistent_evidence_refused(changes):
    evidence_by_field = {"expected_count": 200, "retained_count": 199, "retries_exhausted": True, **changes}
    with pytest.raises(ValueError, match="^import_completion_evidence_invalid$"):
        import_completion_decision(**evidence_by_field)


def test_operator_acceptance_does_not_relax_known_limit():
    decision = import_completion_decision(
        expected_count=200, retained_count=198, retries_exhausted=True, accept_unknown_coverage=True
    )
    assert decision["disposition"] == "rejected"


def _request():
    return {
        "policy_id": IMPORT_COMPLETION_POLICY_ID,
        "candidate_id": "example-candidate",
        "acquisition_root_run_id": "example-acquisition",
        "retry_of_run_id": "example-failed-attempt",
        "retained_counts": {"Organization": 199, "Location": 2},
        "expected_counts": {"Organization": 200, "Location": 2},
        "accept_unknown_coverage": False,
    }


def _receipt(request=None, **changes):
    request = _request() if request is None else request
    arguments_by_name = {
        "retained_counts": request["retained_counts"],
        "content_hash": "a" * 64,
        "incomplete_partitions": ["Organization"],
        "retry_evidence": {"Organization": {"retries": 2, "exhausted": True, "operator_stopped": False}},
        **changes,
    }
    return completion_receipt(request, **arguments_by_name)


def test_receipt_rechecks_closed_counts_and_retains_unknown_absence():
    receipt = _receipt()
    validate_completion_receipt(receipt, retained_counts=_request()["retained_counts"], content_hash="a" * 64)
    assert receipt["decisions"]["Organization"]["missing_count"] == 1
    assert receipt["absence_semantics"] == "unknown_for_unavailable_input"
    with pytest.raises(ValueError, match="candidate_changed"):
        _receipt(retained_counts={"Organization": 198, "Location": 3})
    with pytest.raises(ValueError, match="receipt_changed"):
        validate_completion_receipt(receipt, retained_counts=_request()["retained_counts"], content_hash="b" * 64)


@pytest.mark.parametrize("field", ["candidate_id", "policy_id", "retained_counts", "accept_unknown_coverage"])
def test_request_requires_exact_candidate_contract(field):
    request = _request()
    del request[field]
    with pytest.raises(ValueError, match="request_invalid"):
        validate_completion_request(request)


@pytest.mark.parametrize(
    "change",
    [
        {"incomplete_partitions": ["Organization", "Organization"]},
        {"incomplete_partitions": ["Unknown"]},
        {"retry_evidence": {}},
        {"content_hash": "not-a-hash"},
    ],
)
def test_receipt_refuses_unsettled_or_different_scope(change):
    with pytest.raises(ValueError):
        _receipt(**change)


def test_operator_stop_does_not_relax_automatic_record_limit_or_skip_retries():
    evidence_by_field = {"Organization": {"retries": 2, "exhausted": False, "operator_stopped": True}}
    with pytest.raises(ValueError, match="not_accepted"):
        _receipt(retry_evidence=evidence_by_field)
    request = _request()
    request["expected_counts"]["Organization"] = None
    request["accept_unknown_coverage"] = True
    receipt = _receipt(request, retry_evidence=evidence_by_field)
    assert receipt["decisions"]["Organization"]["missing_count"] is None
    evidence_by_field["Organization"]["retries"] = 1
    with pytest.raises(ValueError, match="not_accepted"):
        _receipt(request, retry_evidence=evidence_by_field)


def test_receipt_cannot_be_edited_to_accept_a_different_decision():
    receipt = copy.deepcopy(_receipt())
    receipt["decisions"]["Organization"]["missing_count"] = 0
    with pytest.raises(ValueError, match="receipt_changed"):
        validate_completion_receipt(receipt, retained_counts=_request()["retained_counts"], content_hash="a" * 64)


def test_expected_total_mismatch_cannot_hide_in_completed_partition():
    request = _request()
    request["expected_counts"]["Location"] = 3
    with pytest.raises(ValueError, match="expected_count_changed"):
        _receipt(request)
