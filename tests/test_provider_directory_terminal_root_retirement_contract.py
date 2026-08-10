# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed unit contract for terminal Provider Directory root retirement."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib
import json

import pytest

from process import provider_directory_terminal_root_retirement_contract as contract


SHA = "a" * 64


def _relation_evidence() -> dict[str, dict[str, object]]:
    return {
        relation_name: {"row_count": 0, "row_sha256": SHA}
        for relation_name in contract.REQUIRED_CHILD_RELATIONS
    }


def _evidence() -> dict[str, object]:
    return {
        "actual_resource_count": 7,
        "child_relations": _relation_evidence(),
        "lineage_finished_at": "2026-08-10T00:00:00+00:00",
        "lineage_sha256": SHA,
        "parent_identity_sha256": SHA,
        "parent_resource_count": 7,
        "predecessor_identity_sha256": SHA,
        "prior_status": "acquiring",
        "proof_row_count": 9,
        "proof_shard_count": 2,
        "resource_counts": {"PractitionerRole": 4, "Organization": 3},
        "source_identity_sha256": SHA,
        "target_identity_sha256": SHA,
        "terminal_run_count": 3,
    }


def _request(**overrides: object) -> contract.TerminalRootRetirementRequest:
    request_by_field: dict[str, object] = {
        "source_id": "source-synthetic",
        "endpoint_id": "endpoint-synthetic",
        "dataset_id": "dataset-candidate",
        "acquisition_root_run_id": "run-root",
        "owner_run_id": "run-owner",
        "expected_current_dataset_id": "dataset-current",
    }
    request_by_field.update(overrides)
    return contract.TerminalRootRetirementRequest(**request_by_field)


def test_request_requires_exact_distinct_selectors_and_token() -> None:
    request = _request(expected_evidence_sha256=SHA)

    assert request.expected_evidence_sha256 == SHA
    assert request.minimum_terminal_age_seconds == 900
    with pytest.raises(contract.TerminalRootRetirementError, match="request_invalid"):
        _request(expected_current_dataset_id="dataset-candidate")
    with pytest.raises(contract.TerminalRootRetirementError, match="request_invalid"):
        _request(expected_evidence_sha256="not-a-digest")
    with pytest.raises(contract.TerminalRootRetirementError, match="request_invalid"):
        _request(minimum_terminal_age_seconds=True)


def test_evidence_keeps_retained_and_proof_counts_independent() -> None:
    evidence_by_field = _evidence()

    validated = contract.validated_retirement_evidence(evidence_by_field)

    assert validated["actual_resource_count"] == 7
    assert validated["parent_resource_count"] == 7
    assert validated["proof_row_count"] == 9
    assert validated["proof_shard_count"] == 2
    assert list(validated["resource_counts"]) == ["Organization", "PractitionerRole"]


@pytest.mark.parametrize(
    ("mutation",),
    [
        (lambda value: value["child_relations"].pop(next(iter(value["child_relations"]))),),
        (lambda value: value["child_relations"].update({"unexpected_relation": {"row_count": 0, "row_sha256": SHA}}),),
        (lambda value: value.update({"prior_status": "incomplete"}),),
        (lambda value: value.update({"terminal_run_count": True}),),
        (lambda value: value.update({"lineage_finished_at": "2026-08-10"}),),
    ],
)
def test_evidence_rejects_open_or_ambiguous_shapes(mutation) -> None:
    evidence_by_field = _evidence()
    mutation(evidence_by_field)

    with pytest.raises(contract.TerminalRootRetirementError, match="evidence_invalid"):
        contract.validated_retirement_evidence(evidence_by_field)


def test_marker_and_result_are_identifier_free_and_deterministic() -> None:
    marker_by_field = contract.retirement_marker(
        _evidence(),
        minimum_terminal_age_seconds=900,
        retired_at=datetime(2026, 8, 10, tzinfo=timezone.utc).isoformat(),
    )
    marker_sha256 = contract.canonical_json_sha256(marker_by_field)
    result = contract.TerminalRootRetirementResult(
        retired=True,
        marker_sha256=marker_sha256,
    )

    assert marker_by_field["contract_version"] == contract.RETIREMENT_CONTRACT_VERSION
    assert json.loads(contract.retirement_result_json(result)) == {
        "already_applied": False,
        "marker_sha256": marker_sha256,
        "retired": True,
        "status": "ok",
    }


def test_operator_gate_and_schema_are_fail_closed(monkeypatch) -> None:
    monkeypatch.delenv(contract.RETIREMENT_ENABLED_ENV, raising=False)
    with pytest.raises(contract.TerminalRootRetirementError, match="disabled"):
        contract.require_terminal_root_retirement_gate()

    monkeypatch.setenv(contract.RETIREMENT_ENABLED_ENV, "true")
    contract.require_terminal_root_retirement_gate()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "invalid-name")
    with pytest.raises(contract.TerminalRootRetirementError, match="state_invalid"):
        contract.schema_name()


def test_legacy_hash_contract_accepts_only_absent_or_explicit_v1() -> None:
    assert (
        contract.retirement_resource_hash_contract({})
        == contract.RETIREMENT_RESOURCE_HASH_CONTRACT
    )
    assert (
        contract.retirement_resource_hash_contract(
            {"resource_hash_contract": "transport_bound_v1"}
        )
        == contract.RETIREMENT_RESOURCE_HASH_CONTRACT
    )
    for marker in (None, "transport_neutral_v2", "semantic_content_v4"):
        with pytest.raises(
            contract.TerminalRootRetirementError,
            match="evidence_invalid",
        ):
            contract.retirement_resource_hash_contract(
                {"resource_hash_contract": marker}
            )


def test_retired_status_is_only_added_to_immutable_dataset_states() -> None:
    importer = importlib.import_module("process.provider_directory_fhir")

    assert importer.ENDPOINT_DATASET_ACQUISITION_RETIRED == "acquisition_retired"
    assert (
        importer.ENDPOINT_DATASET_ACQUISITION_RETIRED
        in importer.IMMUTABLE_ENDPOINT_DATASET_STATUSES
    )
    assert not hasattr(importer, "PAGINATION_CHECKPOINT_ACQUISITION_RETIRED")
