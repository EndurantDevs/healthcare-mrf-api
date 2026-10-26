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
        resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
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
            importer.RESOURCE_HASH_CONTRACT_METADATA_KEY: candidate.resource_hash_contract,
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


def test_failed_candidate_requires_fresh_root_after_checkpoint_generation_change():
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=importer.SAN_MATEO_COUNTY_PROVIDER_DIRECTORY_BASE,
        source_scope_hash="page-index-scope",
        source_ids=("san-mateo",),
        owner_run_id="retry_2",
        retry_of_run_id="root_candidate",
        acquisition_root_run_id="root_candidate",
    )
    with pytest.raises(RuntimeError, match="uncheckpointed_candidate_exists"):
        importer._should_repair_empty_endpoint_dataset_orphan(
            {
                "endpoint_id": "endpoint_1",
                "acquisition_root_run_id": "root_candidate",
                "is_current": False,
                "status": importer.ENDPOINT_DATASET_FAILED,
            },
            "endpoint_1",
            "root_candidate",
            None,
            "root_candidate",
            checkpoint_context,
        )


@pytest.mark.asyncio
async def test_validated_dataset_store_fails_when_candidate_update_is_lost():
    connection = AsyncMock()
    connection.status.return_value = "UPDATE 0"
    connection.all.return_value = [
        {"source_id": "source_a"},
        {"source_id": "source_b"},
    ]
    candidate = _candidate()

    with pytest.raises(RuntimeError, match="validation_lost"):
        await importer._store_validated_endpoint_dataset(
            connection,
            candidate,
            candidate.previous_dataset_id,
            "d" * 64,
            2,
            {"verification": "matched"},
            status=importer.ENDPOINT_DATASET_VALIDATED,
        )

    assert connection.status.await_args.kwargs["marks_validated"] is True
    assert "FOR NO KEY UPDATE OF source" in connection.all.await_args.args[0]


@pytest.mark.asyncio
async def test_verification_baseline_store_does_not_mark_dataset_validated():
    connection = AsyncMock()
    connection.status.return_value = "UPDATE 1"
    candidate = _candidate()

    await importer._store_validated_endpoint_dataset(
        connection,
        candidate,
        candidate.previous_dataset_id,
        "d" * 64,
        2,
        {"verification": "baseline"},
        status=importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
    )

    assert connection.status.await_args.kwargs["status"] == (
        importer.ENDPOINT_DATASET_VERIFICATION_BASELINE
    )
    assert connection.status.await_args.kwargs["marks_validated"] is False


@pytest.mark.asyncio
async def test_legacy_pending_candidate_cannot_start_acquisition(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_state",
        AsyncMock(return_value={}),
    )
    ensure_candidate = AsyncMock()
    monkeypatch.setattr(importer, "_ensure_endpoint_dataset_candidate", ensure_candidate)
    with pytest.raises(RuntimeError, match="single_root_required"):
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


def _private_header(**changes):
    """Use an existing complete synthetic content proof for private admission parity."""
    from tests.test_provider_directory_dataset_selection_bounded_db import _large_metadata_by_field

    return dict(
        dataset_id="dataset_shared", endpoint_id="endpoint_shared", import_run_id="root-shared",
        acquisition_root_run_id="root-shared", status=importer.ENDPOINT_DATASET_VALIDATED,
        dataset_hash="e" * 64, resource_count=1, verification_baseline_count=0,
        publication_metadata_json=_large_metadata_by_field(1), completion_proof_required_version=None,
        completion_proof_json=None, completion_proof_sha256=None, has_admission_receipt=False,
        retired_candidate=False,
    ) | changes


@pytest.mark.parametrize("change", [
    {}, {"has_admission_receipt": True, "publication_metadata_json": object()},
    {"publication_metadata_json": None}, {"publication_metadata_json": []}, {"publication_metadata_json": {}},
])
def test_private_admission_keeps_valid_or_unproven_contenders(change):
    """Sealed, unavailable and valid legacy evidence continue to block acquisition."""
    assert importer._is_private_candidate_blocking(_private_header(**change)) is True


@pytest.mark.parametrize("field,replacement", [
    ("dataset_id", "another_dataset"), ("endpoint_id", "another_endpoint"),
    ("acquisition_root_run_id", "another_run"), ("dataset_hash", "f" * 64), ("resource_count", 2),
])
def test_private_admission_binds_complete_header_identity(field, replacement):
    """A complete proof for a different header is provably invalid."""
    assert importer._is_private_candidate_blocking(_private_header(**{field: replacement})) is False


def test_private_admission_preserves_root_fallback_and_shard_bound():
    """Legacy root fallback and oversized proof treatment match the existing predicate."""
    assert importer._is_private_candidate_blocking(_private_header(acquisition_root_run_id=None)) is True
    state_by_field = _private_header()
    proof = state_by_field["publication_metadata_json"][importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY]
    proof["shards"] = [{}] * (importer.ADMISSION_LEGACY_SHARD_MAX_COUNT + 1)
    assert importer._is_private_candidate_blocking(state_by_field) is True
    assert importer._is_private_candidate_blocking(_private_header(retired_candidate=True)) is False


@pytest.mark.parametrize("field,replacement", [
    ("proof_sha256", "0" * 64), ("contract_id", "unknown"), ("shards", []),
    ("resource_hash_contract", "unknown"), ("semantic_projection_as_of", "2020-01-01"),
])
def test_private_admission_reuses_full_proof_validation(field, replacement):
    """Malformed stored evidence cannot become a valid blocker by retaining its header."""
    state_by_field = _private_header()
    proof = state_by_field["publication_metadata_json"][importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY]
    proof[field] = replacement
    assert importer._is_private_candidate_blocking(state_by_field) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("next_state", [None, "validated", "acquiring", "incomplete"])
async def test_private_admission_keeps_locked_order_and_resumes_after_invalid(next_state):
    """Skip one invalid contender while retaining active and validated conflicts."""
    from types import SimpleNamespace

    invalid = _private_header(dataset_hash="f" * 64)
    contender = _private_header(dataset_id="next_dataset", status=next_state, has_admission_receipt=True)
    observations = []

    async def first(query, **parameters):
        """Record immutable query arguments before the rejection list advances."""
        observations.append((query, list(parameters["rejected_dataset_ids"])))
        return invalid if len(observations) == 1 else contender if next_state else None

    selected = await importer._locked_private_verification_state(
        SimpleNamespace(first=first), '"private_source"."provider_directory_endpoint_dataset"',
        {"include_twin_states": False},
    )
    assert [rejected for _, rejected in observations] == [[], ["dataset_shared"]]
    assert all("LIMIT 1 FOR UPDATE" in query and "dataset.dataset_id <> :dataset_id" in query
               for query, _ in observations)
    if next_state:
        assert selected["dataset_id"] == "next_dataset" and selected["status"] == next_state
        assert selected["publication_metadata_json"] is None
    else:
        assert selected == {}


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
