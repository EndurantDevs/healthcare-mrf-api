# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import dataclasses
import hashlib
import importlib
import json
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _candidate(
    *,
    dataset_id: str = "dataset_candidate",
    root_run_id: str = "root_candidate",
    requires_verification: bool = True,
    verification_role: str | None = None,
) -> importer.EndpointDatasetCandidate:
    resources = ("Organization", "Practitioner")
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id=dataset_id,
        acquisition_root_run_id=root_run_id,
        source_ids=("source_a", "source_b"),
        selected_resources=resources,
        expected_resources=resources,
        import_run_id=root_run_id,
        previous_dataset_id="dataset_current",
        requires_twin_root_verification=requires_verification,
        verification_campaign_id=(
            "reviewed-candidates-v1" if requires_verification else None
        ),
        verification_source_scope_hash=(
            "scope-v1" if requires_verification else None
        ),
        verification_role=verification_role,
        verification_baseline_dataset_id=(
            "dataset_baseline"
            if verification_role
            == importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
            else None
        ),
    )


def _diagnostics() -> dict[str, dict[str, object]]:
    return {
        resource_type: {
            "complete": True,
            "bounded": False,
            "error": None,
            "next_url_remaining": False,
        }
        for resource_type in ("Organization", "Practitioner")
    }


def _content_proof() -> importer.EndpointDatasetContentProof:
    return importer.EndpointDatasetContentProof(
        dataset_hash="a" * 64,
        resource_count=2,
        resource_hashes={"Organization": "b" * 64, "Practitioner": "c" * 64},
        resource_counts={"Organization": 1, "Practitioner": 1},
    )


def _baseline_map(
    candidate: importer.EndpointDatasetCandidate,
    proof: importer.EndpointDatasetContentProof | None = None,
) -> dict[str, object]:
    content_proof = proof or _content_proof()
    verification_proof = importer._twin_root_content_proof(candidate, content_proof)
    verification_proof["acquisition_root_run_id"] = "root_baseline"
    return {
        "dataset_id": "dataset_baseline",
        "endpoint_id": candidate.endpoint_id,
        "acquisition_root_run_id": "root_baseline",
        "status": importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        "dataset_hash": content_proof.dataset_hash,
        "resource_count": content_proof.resource_count,
        "verification_baseline_count": 1,
        "publication_metadata_json": {
            "source_ids": list(candidate.source_ids),
            "selected_resources": list(candidate.selected_resources),
            "expected_resources": list(candidate.expected_resources),
            "requires_twin_root_verification": True,
            importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: (
                candidate.verification_campaign_id
            ),
            importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
                candidate.verification_source_scope_hash
            ),
            importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
                importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
            ),
            importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: None,
            importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
                "role": "baseline",
                "admission_role": importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
                "result": "baseline_recorded",
                "proof": verification_proof,
            }
        },
    }


def test_current_root_cannot_be_repaired_as_an_acquisition_candidate():
    with pytest.raises(RuntimeError, match="root_already_finalized"):
        importer._should_repair_empty_endpoint_dataset_orphan(
            {
                "endpoint_id": "endpoint_1",
                "acquisition_root_run_id": "root_candidate",
                "is_current": True,
                "status": importer.ENDPOINT_DATASET_PUBLISHED,
            },
            "endpoint_1",
            "root_candidate",
            None,
            None,
            None,
        )


@pytest.mark.asyncio
async def test_pending_candidate_requires_a_root_run(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_select_endpoint_dataset_candidate",
        AsyncMock(
            return_value=importer.EndpointDatasetCandidateSelection(
                dataset_id="dataset_candidate",
                acquisition_root_run_id=None,
                previous_dataset_id=None,
                reused_from_checkpoint=False,
            )
        ),
    )
    ensure_candidate = AsyncMock()
    monkeypatch.setattr(importer, "_ensure_endpoint_dataset_candidate", ensure_candidate)
    with pytest.raises(RuntimeError, match="verification_root_required"):
        await importer._prepare_endpoint_dataset_candidate(
            [
                {
                    "source_id": "source_a",
                    "endpoint_id": "endpoint_1",
                    "metadata_json": json.dumps(
                        {
                            "provider_directory_candidate_status": (
                                importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING
                            ),
                            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY: (
                                "reviewed-candidates-v1"
                            ),
                        }
                    ),
                    "api_base": importer.MICHIGAN_PROVIDER_DIRECTORY_BASE,
                }
            ],
            ["Practitioner"],
            run_id=None,
            retry_of_run_id=None,
            pagination_root_run_id=None,
            checkpoint_context=None,
        )
    ensure_candidate.assert_not_awaited()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row["publication_metadata_json"].clear(),
        lambda row: row["publication_metadata_json"][
            importer.TWIN_ROOT_VERIFICATION_METADATA_KEY
        ].update(role="candidate"),
        lambda row: row["publication_metadata_json"][
            importer.TWIN_ROOT_VERIFICATION_METADATA_KEY
        ]["proof"].update(resource_hashes=[]),
        lambda row: row.update(dataset_hash="d" * 64),
        lambda row: row.update(resource_count=999),
    ],
)
def test_baseline_proof_rejects_invalid_or_inconsistent_evidence(mutation):
    baseline = _baseline_map(_candidate())
    mutation(baseline)
    with pytest.raises(RuntimeError, match="verification_baseline_invalid"):
        importer._twin_root_baseline_proof(baseline)


def test_compatible_baseline_is_exact_and_from_a_distinct_root():
    candidate = _candidate()
    baseline = _baseline_map(candidate)
    assert importer._compatible_twin_root_baseline(candidate, {}) is None
    assert importer._compatible_twin_root_baseline(candidate, baseline) == baseline

    for field_name, value in (
        ("status", importer.ENDPOINT_DATASET_VALIDATED),
        ("verification_baseline_count", 2),
        ("acquisition_root_run_id", candidate.acquisition_root_run_id),
    ):
        incompatible_baseline_map = dict(baseline, **{field_name: value})
        with pytest.raises(RuntimeError, match="active_conflict"):
            importer._compatible_twin_root_baseline(
                candidate, incompatible_baseline_map
            )

    wrong_identity = _baseline_map(candidate)
    wrong_identity["publication_metadata_json"][
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY
    ]["proof"]["source_ids"] = ["other_source"]
    with pytest.raises(RuntimeError, match="verification_baseline_invalid"):
        importer._compatible_twin_root_baseline(candidate, wrong_identity)


@pytest.mark.asyncio
async def test_candidate_admission_allows_only_one_compatible_baseline(monkeypatch):
    candidate = _candidate()
    baseline = _baseline_map(candidate)
    locked_state = AsyncMock(return_value={})
    monkeypatch.setattr(importer, "_locked_endpoint_verification_state", locked_state)
    await importer._assert_no_conflicting_endpoint_candidate(None, candidate)

    locked_state.return_value = baseline
    admitted = await importer._assert_no_conflicting_endpoint_candidate(
        None, candidate
    )
    assert admitted.verification_role == (
        importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
    )
    assert admitted.verification_baseline_dataset_id == "dataset_baseline"

    established = dataclasses.replace(candidate, requires_twin_root_verification=False)
    locked_state.return_value = {}
    await importer._assert_no_conflicting_endpoint_candidate(None, established)

    locked_state.return_value = dict(
        baseline,
        status=importer.ENDPOINT_DATASET_VALIDATED,
    )
    with pytest.raises(RuntimeError, match="active_conflict"):
        await importer._assert_no_conflicting_endpoint_candidate(None, candidate)
    with pytest.raises(RuntimeError, match="active_conflict"):
        await importer._assert_no_conflicting_endpoint_candidate(None, established)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_status",
    [
        importer.ENDPOINT_DATASET_FAILED,
        importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
        importer.ENDPOINT_DATASET_VALIDATED,
        importer.ENDPOINT_DATASET_PUBLISHED,
        importer.ENDPOINT_DATASET_SUPERSEDED,
    ],
)
async def test_terminal_twin_successor_blocks_a_distinct_third_root(
    monkeypatch,
    terminal_status,
):
    terminal_state_by_field = dict(
        _baseline_map(_candidate()),
        dataset_id="dataset_terminal_successor",
        acquisition_root_run_id="root_terminal_successor",
        status=terminal_status,
        publication_metadata_json={"requires_twin_root_verification": True},
    )
    monkeypatch.setattr(
        importer,
        "_locked_endpoint_verification_state",
        AsyncMock(return_value=terminal_state_by_field),
    )
    with pytest.raises(RuntimeError, match="active_conflict"):
        await importer._assert_no_conflicting_endpoint_candidate(
            None, _candidate()
        )


class _VerificationStateConnection:
    def __init__(self):
        self.query_params = None

    async def first(self, _sql, **params):
        self.query_params = params
        return None


@pytest.mark.asyncio
async def test_terminal_successor_lookup_is_enabled_only_for_twin_candidates():
    connection = _VerificationStateConnection()
    candidate = _candidate()
    assert await importer._locked_endpoint_verification_state(
        connection, candidate
    ) == {}
    assert connection.query_params["include_twin_states"] is True
    assert connection.query_params["terminal_successor_statuses"] == [
        importer.ENDPOINT_DATASET_FAILED,
        importer.ENDPOINT_DATASET_PUBLISHED,
        importer.ENDPOINT_DATASET_SUPERSEDED,
    ]

    established = dataclasses.replace(
        candidate, requires_twin_root_verification=False
    )
    await importer._locked_endpoint_verification_state(connection, established)
    assert connection.query_params["include_twin_states"] is False


class _ContentConnection:
    def __init__(self, rows):
        self.rows = rows

    async def all(self, _sql, **params):
        after = (
            params.get("after_resource_type", ""),
            params.get("after_resource_id", ""),
        )
        return sorted(
            (
                row
                for row in self.rows
                if (row["resource_type"], row["resource_id"]) > after
            ),
            key=lambda row: (row["resource_type"], row["resource_id"]),
        )[: params["batch_size"]]


@pytest.mark.asyncio
async def test_content_proof_has_ordered_overall_and_per_resource_hashes(monkeypatch):
    resource_rows = [
        {"resource_type": "Practitioner", "resource_id": "p2", "payload_hash": "h2"},
        {"resource_type": "Organization", "resource_id": "o1", "payload_hash": "h1"},
        {"resource_type": "Practitioner", "resource_id": "p1", "payload_hash": "h3"},
    ]
    monkeypatch.setattr(importer, "ENDPOINT_DATASET_HASH_BATCH_SIZE", 2)
    proof = await importer._endpoint_dataset_content_proof(
        _ContentConnection(resource_rows),
        "dataset_candidate",
        ("HealthcareService", "Organization", "Practitioner"),
    )
    ordered_identities = sorted(
        (
            resource_row["resource_type"],
            resource_row["resource_id"],
            resource_row["payload_hash"],
        )
        for resource_row in resource_rows
    )
    overall = "\n".join(
        importer._stable_identity_json(identity) for identity in ordered_identities
    )
    practitioner = "\n".join(
        importer._stable_identity_json(identity)
        for identity in ordered_identities
        if identity[0] == "Practitioner"
    )
    assert proof.dataset_hash == hashlib.sha256(overall.encode()).hexdigest()
    assert proof.resource_count == 3
    assert proof.resource_counts == {
        "HealthcareService": 0,
        "Organization": 1,
        "Practitioner": 2,
    }
    assert proof.resource_hashes["Practitioner"] == hashlib.sha256(
        practitioner.encode()
    ).hexdigest()
    assert proof.resource_hashes["HealthcareService"] == hashlib.sha256().hexdigest()


def test_twin_root_decision_is_baseline_then_match_or_terminal_mismatch():
    candidate = _candidate()
    proof = _content_proof()
    status, metadata, mismatches = importer._twin_root_verification_decision(
        candidate, proof, None
    )
    assert status == importer.ENDPOINT_DATASET_VERIFICATION_BASELINE
    assert mismatches == []
    assert metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["role"] == "baseline"

    baseline = _baseline_map(candidate, proof)
    status, metadata, mismatches = importer._twin_root_verification_decision(
        candidate, proof, baseline
    )
    assert status == importer.ENDPOINT_DATASET_VALIDATED
    assert mismatches == []
    assert metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["result"] == "matched"

    changed = dataclasses.replace(proof, dataset_hash="f" * 64)
    status, metadata, mismatches = importer._twin_root_verification_decision(
        candidate, changed, baseline
    )
    assert status == importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH
    assert "dataset_hash" in mismatches
    assert metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["result"] == "mismatch"

    established = dataclasses.replace(candidate, requires_twin_root_verification=False)
    assert importer._twin_root_verification_decision(established, proof, None) == (
        importer.ENDPOINT_DATASET_VALIDATED,
        {},
        [],
    )


def test_verification_statuses_fit_existing_schema_and_stay_out_of_artifacts():
    status_length = importer.ProviderDirectoryEndpointDataset.__table__.c.status.type.length
    assert status_length == 32
    assert all(
        len(status) <= status_length
        for status in (
            importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
            importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
        )
    )
    artifact_sql = importer._artifact_dataset_options_cte("dataset_table")
    assert importer.ENDPOINT_DATASET_VERIFICATION_BASELINE not in artifact_sql
    assert importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH not in artifact_sql
    assert ":validated_status" in artifact_sql
    reviewed_artifact_sql = importer._artifact_dataset_options_cte(
        "dataset_table", "source_table"
    )
    assert importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING not in reviewed_artifact_sql
    assert importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED in reviewed_artifact_sql
    assert importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY in reviewed_artifact_sql
    assert importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY in reviewed_artifact_sql
    assert "baseline_recorded" in reviewed_artifact_sql
    assert "validated_candidate_count" in reviewed_artifact_sql
