# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed boundaries for supplied-row replay and full re-verification."""

from __future__ import annotations

import copy
from dataclasses import replace

import pytest

from public_evidence import source_record_replay_contract as replay_contract
from public_evidence import source_record_replay_primitives as replay_primitives
from process import public_evidence_fhir_organization_replay as replay_executor
from process.tin_npi_connector_evidence import FhirOrganizationEvidenceResult
from public_evidence.source_record_replay_primitives import (
    PublicEvidenceFhirOrganizationReplayError,
)
from tests.public_evidence_adapter_projection_support import multi_member_inventory
from tests.public_evidence_fhir_organization_replay_support import (
    replay_fixture,
    retained_organization_row,
)

_ERROR = "public_evidence_fhir_organization_replay_invalid"


def _request_by_field(fixture) -> dict[str, object]:
    return {
        "release": fixture.release,
        "inventory": fixture.inventory,
        "source_vector": fixture.source_vector,
        "retained_rows": fixture.retained_rows,
        "token_projectors": fixture.token_projectors,
        "record_identity_token_policy_id": fixture.record_policy_id,
    }


def _assert_rejected(fixture, **overrides) -> None:
    request_by_field = _request_by_field(fixture)
    request_by_field.update(overrides)
    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        replay_executor.replay_fhir_organization_retained_rows(**request_by_field)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("token_projectors", ()),
        ("token_projectors", []),
        ("record_identity_token_policy_id", object()),
        ("source_vector", object()),
        ("retained_rows", {}),
        ("retained_rows", 7),
    ),
)
def test_replay_rejects_invalid_request_container_shapes(
    tmp_path,
    field_name: str,
    replacement: object,
) -> None:
    fixture = replay_fixture(tmp_path)

    _assert_rejected(fixture, **{field_name: replacement})


@pytest.mark.parametrize(
    "hostile_row",
    (
        object(),
        {
            "resource_type": "Organization",
            "resource_id": "synthetic-a",
            "payload_hash": "0" * 64,
        },
        {
            "resource_type": "Organization",
            "resource_id": "synthetic-a",
            "payload_hash": "0" * 64,
            "payload_json": None,
        },
    ),
)
def test_replay_rejects_noncanonical_retained_row_shapes(
    tmp_path,
    hostile_row: object,
) -> None:
    fixture = replay_fixture(tmp_path)

    _assert_rejected(
        fixture,
        retained_rows=(hostile_row, fixture.retained_rows[1]),
    )


def test_replay_rejects_nonorganization_row_after_snapshot(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    hostile_row_map = dict(fixture.retained_rows[0])
    hostile_row_map["resource_type"] = "Location"

    _assert_rejected(
        fixture,
        retained_rows=(hostile_row_map, fixture.retained_rows[1]),
    )


def test_replay_keeps_its_single_dataset_guard_independent(
    monkeypatch, tmp_path
) -> None:
    fixture = replay_fixture(tmp_path)
    multiple_dataset_vector = copy.copy(fixture.source_vector)
    object.__setattr__(
        multiple_dataset_vector,
        "fhir_datasets",
        (fixture.dataset, fixture.dataset),
    )
    monkeypatch.setattr(
        replay_executor,
        "validate_connector_source_vector",
        lambda candidate: candidate,
    )

    _assert_rejected(fixture, source_vector=multiple_dataset_vector)


def test_replay_rejects_invalid_record_hmac_from_owned_capability(
    monkeypatch,
    tmp_path,
) -> None:
    retained_rows = tuple(
        retained_organization_row(f"synthetic-unmatched-{ordinal}", matched=False)
        for ordinal in range(2)
    )
    fixture = replay_fixture(tmp_path, retained_rows=retained_rows)
    projector_type = type(fixture.token_projectors[0])
    monkeypatch.setattr(
        projector_type,
        "pseudonymize_source_record",
        lambda self, **coordinates: b"short",
    )

    _assert_rejected(fixture)


def test_replay_rejects_extractor_record_hmac_drift(monkeypatch, tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    original_extract = (
        replay_executor.extract_normalized_organization_evidence_for_policies
    )

    def drifting_extract(*args, **kwargs):
        extracted = original_extract(*args, **kwargs)
        drifted_evidence_rows = tuple(
            replace(evidence, source_record_hmac_sha256=b"\x01" * 32)
            for evidence in extracted.evidence
        )
        return FhirOrganizationEvidenceResult(extracted.state, drifted_evidence_rows)

    monkeypatch.setattr(
        replay_executor,
        "extract_normalized_organization_evidence_for_policies",
        drifting_extract,
    )

    _assert_rejected(fixture)


def test_replay_rejects_non_single_scan_proof(monkeypatch, tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    monkeypatch.setattr(
        replay_executor,
        "scan_proofs_and_evidence",
        lambda scan_records, *, source_vector: ((), ()),
    )

    _assert_rejected(fixture)


def test_result_builder_and_shape_checks_fail_closed(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = replay_executor.replay_fhir_organization_retained_rows(
        **_request_by_field(fixture)
    )
    proof = replay_contract._VerifiedReplayProof(
        replay_result.source_vector_sha256,
        replay_result.dataset_fence_sha256,
        replay_result.token_policy_id,
        replay_result.token_policy_descriptor_sha256,
        replay_result.source_record_vector_sha256,
        replay_result.scan_proof_sha256,
    )

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        replay_contract._verified_replay_proof(object())
    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        replay_contract._build_fhir_organization_replay_result(
            release=fixture.release,
            inventory=fixture.inventory,
            proof=proof,
            execution_seal=object(),
        )
    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        replay_executor.verify_fhir_organization_replay_result(
            object(),
            **_request_by_field(fixture),
        )


def test_result_shape_normalizes_unexpected_field_errors(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = replay_executor.replay_fhir_organization_retained_rows(
        **_request_by_field(fixture)
    )
    hostile_result = replay_result._replace(replay_ref=object())

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        replay_executor.verify_fhir_organization_replay_result(
            hostile_result,
            **_request_by_field(fixture),
        )


def test_replay_rejects_a_valid_inventory_from_another_source_scope(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    wrong_release, _source_records, wrong_inventory, _witnesses = (
        multi_member_inventory("tic", member_count=2)
    )

    _assert_rejected(
        fixture,
        release=wrong_release,
        inventory=wrong_inventory,
    )


def test_json_snapshot_boundaries_are_closed() -> None:
    assert replay_executor._detached_json_value([None, True, 7, 1.5, "text"]) == [
        None,
        True,
        7,
        1.5,
        "text",
    ]
    for hostile_json in ({1: "value"}, object(), float("nan"), float("inf")):
        with pytest.raises(
            PublicEvidenceFhirOrganizationReplayError,
            match=f"^{_ERROR}$",
        ):
            replay_executor._detached_json_value(hostile_json)
    nested_json_list: object = None
    for _depth in range(replay_executor._MAX_JSON_DEPTH + 2):
        nested_json_list = [nested_json_list]
    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        replay_executor._detached_json_value(nested_json_list)


@pytest.mark.parametrize(
    ("function_name", "arguments"),
    (
        ("canonical_replay_sha256", ("", {})),
        ("canonical_replay_sha256", ("valid", object())),
        ("derived_replay_ref", ("é", {})),
        ("strict_replay_sha256", ("A" * 64,)),
        ("strict_replay_token_policy_id", ("wrong-policy",)),
        ("canonical_replay_binding_sha256", ("binding", ())),
        ("canonical_source_record_vector_sha256", ((),)),
        ("canonical_source_record_vector_sha256", (("per1_b", "per1_a"),)),
    ),
)
def test_pure_replay_primitives_reject_noncanonical_inputs(
    function_name: str,
    arguments: tuple[object, ...],
) -> None:
    function = getattr(replay_primitives, function_name)

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        function(*arguments)
