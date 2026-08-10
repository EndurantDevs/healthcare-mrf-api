# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Candidate identity and projection-date coverage for semantic proof v3."""

from __future__ import annotations

import datetime as dt
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    persisted_resource_hash_contract,
)
from tests.test_provider_directory_semantic_proof_v3 import (
    PROJECTION_AS_OF,
    _dataset_row,
    _observation,
)


importer = importlib.import_module("process.provider_directory_fhir")
PROOF_RESOURCE_SCOPE = ["Practitioner"]

def test_contract_reader_keeps_markerless_v1_and_explicit_v2():
    assert persisted_resource_hash_contract(None) == (
        LEGACY_RESOURCE_HASH_CONTRACT
    )
    assert persisted_resource_hash_contract(
        {"resource_hash_contract": TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT}
    ) == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    assert persisted_resource_hash_contract(
        {"resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT}
    ) == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT


def _semantic_candidate_selection():
    return importer.EndpointDatasetCandidateSelection(
        dataset_id="dataset-1",
        acquisition_root_run_id="root-1",
        previous_dataset_id=None,
        reused_from_checkpoint=False,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )


def _semantic_candidate_state(
    projection_as_of,
) -> dict[str, dict[str, object]]:
    return {
        "publication_metadata_json": {
            "resource_hash_contract": (
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            "semantic_projection_as_of": projection_as_of,
            "selected_resources": ["Practitioner"],
            "proof_resource_scope": PROOF_RESOURCE_SCOPE,
        }
    }


def test_v3_projection_date_is_created_once_and_reused(monkeypatch):
    """Create one root date and reject every noncanonical replay value."""

    selection = _semantic_candidate_selection()
    monkeypatch.setattr(
        importer,
        "_now",
        lambda: dt.datetime(2026, 8, 9, tzinfo=dt.UTC),
    )
    fresh = importer._selection_with_semantic_projection_as_of(
        selection,
        {},
        ("Practitioner",),
    )
    assert fresh.semantic_projection_as_of == PROJECTION_AS_OF
    assert fresh.proof_resource_scope == ("Practitioner",)

    monkeypatch.setattr(
        importer,
        "_now",
        lambda: dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
    )
    resumed = importer._selection_with_semantic_projection_as_of(
        selection,
        _semantic_candidate_state(PROJECTION_AS_OF),
        ("Practitioner",),
    )
    assert resumed.semantic_projection_as_of == PROJECTION_AS_OF

    for invalid_value in (None, " 2026-08-09", "2026-8-9", dt.date(2026, 8, 9)):
        with pytest.raises(
            RuntimeError,
            match="semantic_projection_as_of",
        ):
            importer._selection_with_semantic_projection_as_of(
                selection,
                _semantic_candidate_state(invalid_value),
                ("Practitioner",),
            )


def test_twin_successor_inherits_and_fences_projection_date(monkeypatch):
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-successor",
        acquisition_root_run_id="root-successor",
        source_ids=("source-1",),
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
        import_run_id="root-successor",
        previous_dataset_id=None,
        requires_twin_root_verification=True,
        verification_campaign_id="campaign-1",
        verification_source_scope_hash="scope-1",
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-10",
        proof_resource_scope=("Practitioner",),
    )
    baseline_by_field = {
        "dataset_id": "dataset-baseline",
        "publication_metadata_json": {
            "resource_hash_contract": (
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            "semantic_projection_as_of": PROJECTION_AS_OF,
            "selected_resources": ["Practitioner"],
            "proof_resource_scope": PROOF_RESOURCE_SCOPE,
        },
    }
    monkeypatch.setattr(
        importer,
        "_compatible_twin_root_baseline",
        lambda _candidate, _state: baseline_by_field,
    )

    admitted = importer._candidate_with_locked_twin_root_admission(
        candidate,
        baseline_by_field,
    )
    assert admitted.semantic_projection_as_of == PROJECTION_AS_OF

    resumed = importer.replace(
        candidate,
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
        verification_baseline_dataset_id="dataset-baseline",
    )
    with pytest.raises(RuntimeError, match="baseline_incompatible"):
        importer._candidate_with_locked_twin_root_admission(
            resumed,
            baseline_by_field,
        )


def _fresh_twin_successor(selected_resources):
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-successor",
        acquisition_root_run_id="root-successor",
        source_ids=("source-1",),
        selected_resources=selected_resources,
        expected_resources=selected_resources,
        import_run_id="root-successor",
        previous_dataset_id=None,
        requires_twin_root_verification=True,
        verification_campaign_id="campaign-1",
        verification_source_scope_hash="scope-1",
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-10",
        proof_resource_scope=importer._provider_directory_proof_resource_scope(
            selected_resources
        ),
    )


def _baseline_identity(
    selected_resources,
    baseline_contract,
    baseline_projection_date,
    baseline_scope,
):
    metadata_by_field = {
        "source_ids": ["source-1"],
        "selected_resources": list(selected_resources),
        "expected_resources": list(selected_resources),
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: "campaign-1",
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: "scope-1",
    }
    proof_by_field = {
        "endpoint_id": "endpoint-1",
        **metadata_by_field,
    }
    if baseline_contract != LEGACY_RESOURCE_HASH_CONTRACT:
        metadata_by_field["resource_hash_contract"] = baseline_contract
    if baseline_projection_date is not None:
        metadata_by_field["semantic_projection_as_of"] = (
            baseline_projection_date
        )
        proof_by_field["semantic_projection_as_of"] = (
            baseline_projection_date
        )
    if baseline_scope is not None:
        metadata_by_field["proof_resource_scope"] = list(baseline_scope)
        proof_by_field["proof_resource_scope"] = list(baseline_scope)
    return {
        "dataset_id": "dataset-baseline",
        "acquisition_root_run_id": "root-baseline",
        "status": importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        "verification_baseline_count": 1,
        "completion_proof_required_version": None,
        "publication_metadata_json": metadata_by_field,
    }, proof_by_field


@pytest.mark.parametrize(
    ("baseline_contract", "baseline_projection_date", "baseline_scope"),
    (
        (LEGACY_RESOURCE_HASH_CONTRACT, None, None),
        (TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT, None, None),
        (
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            PROJECTION_AS_OF,
            ("PractitionerRole",),
        ),
    ),
)
def test_fresh_twin_successor_inherits_persisted_baseline_contract(
    monkeypatch,
    baseline_contract,
    baseline_projection_date,
    baseline_scope,
):
    """Inherit the baseline contract without consulting today's defaults."""

    selected_resources = ("PractitionerRole",)
    candidate = _fresh_twin_successor(selected_resources)
    baseline_by_field, baseline_proof_by_field = _baseline_identity(
        selected_resources,
        baseline_contract,
        baseline_projection_date,
        baseline_scope,
    )
    monkeypatch.setattr(
        importer,
        "_twin_root_baseline_proof",
        lambda _dataset_map: baseline_proof_by_field,
    )

    admitted = importer._candidate_with_locked_twin_root_admission(
        candidate,
        baseline_by_field,
    )

    assert admitted.resource_hash_contract == baseline_contract
    assert admitted.semantic_projection_as_of == baseline_projection_date
    assert admitted.proof_resource_scope == baseline_scope


@pytest.mark.asyncio
async def test_finalization_lock_rejects_projection_date_tamper():
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="root-1",
        source_ids=("source-1",),
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
        import_run_id="root-1",
        previous_dataset_id=None,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
        proof_resource_scope=("Practitioner",),
    )
    connection = SimpleNamespace(
        first=AsyncMock(
            side_effect=[
                {"endpoint_id": "endpoint-1"},
                {
                    "dataset_id": "dataset-1",
                    "acquisition_root_run_id": "root-1",
                    "is_current": False,
                    "status": importer.ENDPOINT_DATASET_ACQUIRING,
                    "previous_dataset_id": None,
                    "completion_proof_required_version": None,
                    "completion_proof_json": None,
                    "completion_proof_sha256": None,
                    "publication_metadata_json": {
                        "resource_hash_contract": (
                            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                        ),
                        "semantic_projection_as_of": "2026-08-10",
                        "selected_resources": ["Practitioner"],
                        "proof_resource_scope": PROOF_RESOURCE_SCOPE,
                    },
                },
            ]
        )
    )

    with pytest.raises(
        RuntimeError,
        match="candidate_stale",
    ):
        await importer._lock_endpoint_dataset_for_validation(
            connection,
            candidate,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "projection_fields",
    [
        {"age_years": 40, "age_as_of": None},
        {"age_years": 40, "age_as_of": "2026-08-10"},
        {
            "years_of_practice": 10,
            "years_of_practice_as_of": PROJECTION_AS_OF,
            "years_of_practice_basis": None,
            "years_of_practice_start_date": "2016-08-09",
        },
    ],
)
async def test_v3_accumulator_rejects_partial_or_wrong_date_projection(
    projection_fields,
):
    observation = _observation()
    observation.update(projection_fields)
    incoming_row = _dataset_row(observation)
    connection = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "dataset_id": "dataset-1",
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
                "is_current": False,
                "publication_metadata_json": {
                    "selected_resources": ["Practitioner"],
                    "resource_hash_contract": (
                        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                    ),
                    "semantic_projection_as_of": PROJECTION_AS_OF,
                    "proof_resource_scope": PROOF_RESOURCE_SCOPE,
                },
            }
        ),
        all=AsyncMock(return_value=[]),
        scalar=AsyncMock(return_value=None),
    )

    with pytest.raises(ValueError, match="semantic_.*projection"):
        await importer._accumulated_endpoint_dataset_rows(
            connection,
            [incoming_row],
            dataset_id="dataset-1",
            resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            semantic_projection_as_of=PROJECTION_AS_OF,
        )
    connection.all.assert_not_awaited()
    connection.scalar.assert_awaited_once()


@pytest.mark.asyncio
async def test_accumulator_rejects_resource_outside_parent_scope(
):
    resource_hash_contract = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    incoming_row = _dataset_row(_observation(), resource_hash_contract)
    semantic_projection_as_of = (
        PROJECTION_AS_OF
        if resource_hash_contract == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        else None
    )
    connection = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "dataset_id": "dataset-1",
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
                "is_current": False,
                "publication_metadata_json": {
                    "selected_resources": ["Organization"],
                    "resource_hash_contract": resource_hash_contract,
                    "proof_resource_scope": ["Endpoint", "Organization"],
                    **(
                        {
                            "semantic_projection_as_of": (
                                semantic_projection_as_of
                            )
                        }
                        if semantic_projection_as_of is not None
                        else {}
                    ),
                },
            }
        ),
        all=AsyncMock(return_value=[]),
        scalar=AsyncMock(return_value=None),
    )

    with pytest.raises(RuntimeError, match="resource_scope_changed"):
        await importer._accumulated_endpoint_dataset_rows(
            connection,
            [incoming_row],
            dataset_id="dataset-1",
            resource_hash_contract=(
                resource_hash_contract
            ),
            semantic_projection_as_of=semantic_projection_as_of,
        )
    connection.all.assert_not_awaited()
    connection.scalar.assert_not_awaited()
