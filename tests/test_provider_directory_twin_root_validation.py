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
    dataset_id: str,
    root_run_id: str,
    *,
    verification_role: str = importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
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
        requires_twin_root_verification=True,
        verification_campaign_id="reviewed-candidates-v1",
        verification_source_scope_hash="scope-v1",
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


def _resource_rows() -> list[dict[str, str]]:
    return [
        {"resource_type": "Organization", "resource_id": "o1", "payload_hash": "h1"},
        {"resource_type": "Practitioner", "resource_id": "p1", "payload_hash": "h2"},
    ]


def _content_proof(
    resource_rows: list[dict[str, str]],
) -> importer.EndpointDatasetContentProof:
    identities = sorted(
        (
            resource_entry["resource_type"],
            resource_entry["resource_id"],
            resource_entry["payload_hash"],
        )
        for resource_entry in resource_rows
    )
    overall_digest = hashlib.sha256(
        "\n".join(
            importer._stable_identity_json(identity) for identity in identities
        ).encode()
    ).hexdigest()
    hashes_by_resource = {}
    counts_by_resource = {}
    for resource_type in ("Organization", "Practitioner"):
        resource_identities = [
            identity for identity in identities if identity[0] == resource_type
        ]
        hashes_by_resource[resource_type] = hashlib.sha256(
            "\n".join(
                importer._stable_identity_json(identity)
                for identity in resource_identities
            ).encode()
        ).hexdigest()
        counts_by_resource[resource_type] = len(resource_identities)
    return importer.EndpointDatasetContentProof(
        dataset_hash=overall_digest,
        resource_count=len(identities),
        resource_hashes=hashes_by_resource,
        resource_counts=counts_by_resource,
    )


def _baseline_dataset_map(
    candidate: importer.EndpointDatasetCandidate,
    resource_rows: list[dict[str, str]],
) -> dict[str, object]:
    content_proof = _content_proof(resource_rows)
    proof = importer._twin_root_content_proof(candidate, content_proof)
    proof["acquisition_root_run_id"] = "root_baseline"
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
                "proof": proof,
            }
        },
    }


class _ValidationHarness:
    def __init__(self, candidate, resource_rows, baseline_dataset_map=None):
        self.candidate = candidate
        self.resource_rows = resource_rows
        self.baseline_dataset_map = baseline_dataset_map
        self.saved_params = None
        self.is_committed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, _exc, _traceback):
        self.is_committed = exc_type is None

    async def first(self, sql, **_params):
        if "verification_baseline_count" in sql:
            return self.baseline_dataset_map
        if "WITH immutable_resources AS" in sql:
            return {
                "distinct_npis": 0,
                "address_records": 0,
                "addressed_locations": 0,
                "geocoded_locations": 0,
            }
        if "provider_directory_api_endpoint" in sql:
            return {"endpoint_id": self.candidate.endpoint_id}
        if "SELECT dataset_id, acquisition_root_run_id" in sql:
            return {
                "dataset_id": self.candidate.dataset_id,
                "acquisition_root_run_id": self.candidate.acquisition_root_run_id,
                "is_current": False,
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
                "previous_dataset_id": self.candidate.previous_dataset_id,
            }
        return {"dataset_id": self.candidate.previous_dataset_id}

    async def all(self, sql, **params):
        if "provider_directory_bulk_acquisition_checkpoint" in sql:
            return []
        after = (
            params.get("after_resource_type", ""),
            params.get("after_resource_id", ""),
        )
        return [
            resource_entry
            for resource_entry in self.resource_rows
            if (
                resource_entry["resource_type"],
                resource_entry["resource_id"],
            )
            > after
        ][: params["batch_size"]]

    async def status(self, sql, **params):
        if str(sql).startswith("SET TRANSACTION"):
            return 1
        self.saved_params = params
        return 1


async def _validate(
    monkeypatch,
    candidate: importer.EndpointDatasetCandidate,
    resource_rows: list[dict[str, str]],
    baseline_dataset_map: dict[str, object] | None = None,
):
    harness = _ValidationHarness(candidate, resource_rows, baseline_dataset_map)
    relations = AsyncMock(
        return_value={
            importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
                "complete": True,
                "edge_count": 0,
            },
            importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
                "complete": True,
                "edge_count": 0,
            },
        }
    )
    monkeypatch.setattr(importer.db, "acquire", lambda: harness)
    monkeypatch.setattr(
        importer, "_build_endpoint_dataset_serving_relations", relations
    )
    summary = await importer._validate_endpoint_dataset_candidate(
        candidate, _diagnostics()
    )
    return summary, harness, relations


@pytest.mark.asyncio
async def test_first_root_is_hash_only_non_publishable_baseline(monkeypatch):
    candidate = _candidate("dataset_first", "root_first")
    summary, harness, relations = await _validate(
        monkeypatch, candidate, _resource_rows()
    )
    assert harness.is_committed is True
    assert summary["status"] == importer.ENDPOINT_DATASET_VERIFICATION_BASELINE
    assert summary["validated"] is False
    metadata = json.loads(harness.saved_params["publication_metadata_json"])
    verification = metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]
    assert verification["role"] == "baseline"
    assert verification["proof"]["resource_counts"] == {
        "Organization": 1,
        "Practitioner": 1,
    }
    relations.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_second_root_is_the_only_validated_candidate(monkeypatch):
    candidate = _candidate(
        "dataset_second",
        "root_second",
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
    )
    resource_rows = _resource_rows()
    baseline_dataset_map = _baseline_dataset_map(candidate, resource_rows)
    summary, harness, relations = await _validate(
        monkeypatch, candidate, resource_rows, baseline_dataset_map
    )
    assert summary["status"] == importer.ENDPOINT_DATASET_VALIDATED
    assert summary["validated"] is True
    metadata = json.loads(harness.saved_params["publication_metadata_json"])
    verification = metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]
    assert verification["result"] == "matched"
    assert verification["baseline_dataset_id"] == "dataset_baseline"
    assert summary["baseline_payload_rows_retired"] == 1
    assert verification["baseline_payload_retirement"] == {
        "baseline_dataset_id": "dataset_baseline",
        "deleted_resource_rows": 1,
    }
    relations.assert_awaited_once_with(harness, candidate)


@pytest.mark.asyncio
async def test_second_root_mismatch_is_terminal_without_serving_build(monkeypatch):
    candidate = _candidate(
        "dataset_mismatch",
        "root_mismatch",
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
    )
    baseline_rows = _resource_rows()
    baseline_dataset_map = _baseline_dataset_map(candidate, baseline_rows)
    changed_rows = [dict(baseline_rows[0], payload_hash="changed"), baseline_rows[1]]
    summary, harness, relations = await _validate(
        monkeypatch, candidate, changed_rows, baseline_dataset_map
    )
    assert summary["status"] == importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH
    assert summary["validated"] is False
    assert "dataset_hash" in summary["mismatch_fields"]
    metadata = json.loads(harness.saved_params["publication_metadata_json"])
    verification = metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]
    assert verification["proof"]["resource_hashes"]
    assert "baseline_payload_retirement" not in verification
    assert "baseline_payload_rows_retired" not in summary
    relations.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_mismatch_clears_checkpoints_then_fails_run(monkeypatch):
    candidate = _candidate("dataset_mismatch", "root_mismatch")
    finalize = AsyncMock(
        return_value={
            "dataset_id": candidate.dataset_id,
            "status": importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
        }
    )
    clear = AsyncMock()
    monkeypatch.setattr(importer, "_finalize_endpoint_dataset_candidate", finalize)
    monkeypatch.setattr(
        importer,
        "_clear_finalized_endpoint_dataset_pagination_checkpoints",
        clear,
    )
    with pytest.raises(RuntimeError, match="verification_mismatch"):
        await importer._finalize_candidate_and_clear_checkpoints(
            candidate, _diagnostics()
        )
    clear.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
    ],
)
async def test_terminal_verification_states_are_checkpoint_finalized(
    monkeypatch, status
):
    candidate = dataclasses.replace(
        _candidate("dataset_candidate", "root_candidate"),
        checkpoint_context=importer.PaginationCheckpointContext(
            canonical_api_base="https://example.test/fhir",
            source_scope_hash="scope",
            source_ids=("source_a", "source_b"),
            owner_run_id="root_candidate",
            acquisition_root_run_id="root_candidate",
        ),
    )
    clear = AsyncMock()
    monkeypatch.setattr(importer, "_clear_finalized_dataset_checkpoints", clear)
    await importer._clear_finalized_endpoint_dataset_pagination_checkpoints(
        candidate,
        {"dataset_id": candidate.dataset_id, "status": status},
    )
    clear.assert_awaited_once_with(candidate)
