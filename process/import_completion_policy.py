# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared acceptance decision for retained imports with unavailable input."""

from __future__ import annotations

from typing import Any

IMPORT_COMPLETION_POLICY_ID = "healthporta.import-completion.v1"
MAX_MISSING_BASIS_POINTS = 50
IMPORT_COMPLETION_PARAM = "import_completion"


def import_completion_decision(
    *,
    expected_count: int | None,
    retained_count: int,
    retries_exhausted: bool,
    accept_unknown_coverage: bool = False,
) -> dict[str, Any]:
    """Apply the shared 0.5% limit without guessing an unknown population.

    Counts cover distinct records in one exact candidate scope. The caller
    supplies a trustworthy expected count and durable retry exhaustion for
    automatic acceptance, binds the result to its candidate, and preserves validation and writer closure.
    Explicit unknown-coverage acceptance must come from an authorized operator.
    This decision never converts a validation or authorization failure.
    """
    if (
        type(retained_count) is not int
        or retained_count < 0
        or (expected_count is not None and (type(expected_count) is not int or expected_count < retained_count))
        or type(retries_exhausted) is not bool
        or type(accept_unknown_coverage) is not bool
    ):
        raise ValueError("import_completion_evidence_invalid")

    missing_count = None if expected_count is None else expected_count - retained_count
    basis = None
    if missing_count == 0:
        disposition = "complete"
        basis = "expected_count"
    elif expected_count is None and accept_unknown_coverage and retained_count > 0:
        disposition = "accepted_partial"
        basis = "operator_accepted_unknown"
    elif not retries_exhausted:
        disposition = "retry"
    elif expected_count is not None:
        disposition = (
            "accepted_partial" if missing_count * 10000 <= expected_count * MAX_MISSING_BASIS_POINTS else "rejected"
        )
        basis = "expected_count"
    else:
        disposition = "rejected"

    return {
        "policy_id": IMPORT_COMPLETION_POLICY_ID,
        "disposition": disposition,
        "expected_count": expected_count,
        "retained_count": retained_count,
        "missing_count": missing_count,
        "max_missing_basis_points": MAX_MISSING_BASIS_POINTS,
        "retries_exhausted": retries_exhausted,
        "acceptance_basis": basis,
    }


def validate_completion_request(request: Any) -> dict[str, Any]:
    """Validate an explicit, candidate-bound request at an authorized boundary."""
    fields = {
        "policy_id",
        "candidate_id",
        "acquisition_root_run_id",
        "retry_of_run_id",
        "retained_counts",
        "expected_counts",
        "accept_unknown_coverage",
    }
    if (
        type(request) is not dict
        or request.keys() != fields
        or request.get("policy_id") != IMPORT_COMPLETION_POLICY_ID
        or any(
            type(request.get(key)) is not str
            or not request[key]
            or request[key] != request[key].strip()
            or len(request[key]) > 128
            for key in ("candidate_id", "acquisition_root_run_id", "retry_of_run_id")
        )
        or type(request.get("accept_unknown_coverage")) is not bool
        or type(request.get("retained_counts")) is not dict
        or type(request.get("expected_counts")) is not dict
        or not request["retained_counts"]
        or request["retained_counts"].keys() != request["expected_counts"].keys()
        or len(request["retained_counts"]) > 128
    ):
        raise ValueError("import_completion_request_invalid")
    for partition, count in request["retained_counts"].items():
        if type(partition) is not str or not partition or len(partition) > 128:
            raise ValueError("import_completion_request_invalid")
        import_completion_decision(
            expected_count=request["expected_counts"][partition],
            retained_count=count,
            retries_exhausted=False,
            accept_unknown_coverage=request["accept_unknown_coverage"],
        )
    return request


def completion_receipt(
    request: dict[str, Any],
    *,
    retained_counts: dict[str, int],
    content_hash: str,
    incomplete_partitions: list[str],
    retry_evidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Recheck settled acquisition evidence against the closed candidate's counts."""
    validate_completion_request(request)
    if (
        retained_counts != request["retained_counts"]
        or type(content_hash) is not str
        or len(content_hash) != 64
        or set(content_hash) - set("0123456789abcdef")
        or type(incomplete_partitions) is not list
        or not incomplete_partitions
        or len(set(incomplete_partitions)) != len(incomplete_partitions)
        or not set(incomplete_partitions) <= retained_counts.keys()
        or type(retry_evidence) is not dict
        or retry_evidence.keys() != set(incomplete_partitions)
    ):
        raise ValueError("import_completion_candidate_changed")
    if any(
        request["expected_counts"][partition] not in (None, count)
        for partition, count in retained_counts.items()
        if partition not in incomplete_partitions
    ):
        raise ValueError("import_completion_expected_count_changed")
    decision_by_partition = {
        partition: _partition_decision(request, partition, retained_counts[partition], retry_evidence[partition])
        for partition in incomplete_partitions
    }
    return {
        "request": request,
        "content_hash": content_hash,
        "incomplete_partitions": sorted(incomplete_partitions),
        "retry_evidence": retry_evidence,
        "decisions": decision_by_partition,
        "absence_semantics": "unknown_for_unavailable_input",
    }


def _partition_decision(request, partition, retained_count, evidence):
    if (
        type(evidence) is not dict
        or evidence.keys() != {"retries", "exhausted", "operator_stopped"}
        or type(evidence["retries"]) is not int
        or evidence["retries"] < 1
        or type(evidence["exhausted"]) is not bool
        or type(evidence["operator_stopped"]) is not bool
    ):
        raise ValueError("import_completion_retry_evidence_invalid")
    is_exhausted = evidence["exhausted"]
    if not is_exhausted and not (
        request["expected_counts"][partition] is None
        and request["accept_unknown_coverage"]
        and evidence["retries"] >= 2
    ):
        raise ValueError("import_completion_not_accepted")
    decision = import_completion_decision(
        expected_count=request["expected_counts"][partition],
        retained_count=retained_count,
        retries_exhausted=is_exhausted,
        accept_unknown_coverage=request["accept_unknown_coverage"],
    )
    if decision["disposition"] not in {"complete", "accepted_partial"}:
        raise ValueError("import_completion_not_accepted")
    return decision


def validate_completion_receipt(receipt: Any, *, retained_counts: dict[str, int], content_hash: str) -> None:
    """Recompute a stored receipt when consuming a closed candidate or handoff."""
    if type(receipt) is not dict:
        raise ValueError("import_completion_receipt_invalid")
    try:
        expected = completion_receipt(
            receipt["request"],
            retained_counts=retained_counts,
            content_hash=content_hash,
            incomplete_partitions=receipt["incomplete_partitions"],
            retry_evidence=receipt["retry_evidence"],
        )
    except (KeyError, TypeError) as error:
        raise ValueError("import_completion_receipt_invalid") from error
    if receipt != expected:
        raise ValueError("import_completion_receipt_changed")
