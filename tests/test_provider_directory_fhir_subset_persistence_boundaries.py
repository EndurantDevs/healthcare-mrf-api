# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed persistence boundaries for reviewed Provider Directory subsets."""

from __future__ import annotations

import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_proof_store import (
    ProviderDirectoryProofStoreError,
)
from tests.provider_directory_fhir_subset_completion_support import (
    build_persisted_subset_inputs,
    importer,
)


_PROOF_ERROR = "provider_directory_endpoint_dataset_verification_proof_invalid"


def _persisted_inputs():
    return build_persisted_subset_inputs()


def test_parent_completion_pair_accepts_exact_pair_and_legacy_absence():
    _, _, content, dataset, _, _, _ = _persisted_inputs()

    observed = importer._validated_parent_subset_completion_pair(dataset)

    assert observed == (
        content.completion_proof,
        content.completion_proof_sha256,
    )
    assert importer._validated_parent_subset_completion_pair({}) is None


def test_parent_completion_pair_rejects_orphan_marker_and_invalid_pair():
    _, _, _, dataset, _, _, _ = _persisted_inputs()
    invalid_datasets = (
        {**dataset, "completion_proof_required_version": None},
        {**dataset, "completion_proof_required_version": 2},
        {**dataset, "completion_proof_json": {}},
    )

    for invalid_dataset in invalid_datasets:
        with pytest.raises(RuntimeError, match=_PROOF_ERROR):
            importer._validated_parent_subset_completion_pair(invalid_dataset)


def test_persisted_completion_pair_accepts_exact_and_empty_legacy_shapes():
    _, _, content, dataset, metadata, embedded, _ = _persisted_inputs()

    observed = importer._validated_persisted_subset_completion_pair(
        dataset,
        embedded,
        metadata,
    )

    assert observed == (
        content.completion_proof,
        content.completion_proof_sha256,
    )
    assert importer._validated_persisted_subset_completion_pair(
        {},
        {},
        {},
    ) is None


def test_persisted_completion_pair_rejects_orphan_and_drifted_embeds():
    _, _, _, dataset, metadata, embedded, _ = _persisted_inputs()
    orphan_proof_by_field = {
        "completion_proof": embedded["completion_proof"]
    }
    drifted_proof_by_field = {
        **embedded,
        "completion_proof_sha256": "f" * 64,
    }

    with pytest.raises(RuntimeError, match=_PROOF_ERROR):
        importer._validated_persisted_subset_completion_pair(
            {},
            orphan_proof_by_field,
            {},
        )
    with pytest.raises(RuntimeError, match=_PROOF_ERROR):
        importer._validated_persisted_subset_completion_pair(
            dataset,
            drifted_proof_by_field,
            metadata,
        )


def test_finalized_completion_pair_revalidates_stored_content(monkeypatch):
    _, _, _, dataset, metadata, _, stored_summary = _persisted_inputs()
    validator = Mock(return_value=stored_summary)
    monkeypatch.setattr(
        importer,
        "validate_stored_dataset_proof_metadata",
        validator,
    )

    importer._validate_finalized_subset_completion_pair(dataset, metadata)

    assert validator.call_count == 1


def test_finalized_completion_pair_rejects_missing_identity(monkeypatch):
    _, _, _, dataset, metadata, _, stored_summary = _persisted_inputs()
    monkeypatch.setattr(
        importer,
        "validate_stored_dataset_proof_metadata",
        lambda *_args, **_kwargs: stored_summary,
    )

    with pytest.raises(RuntimeError, match=_PROOF_ERROR):
        importer._validate_finalized_subset_completion_pair(
            {**dataset, "dataset_id": None},
            metadata,
        )


def test_finalized_completion_pair_closes_validator_and_summary_drift(
    monkeypatch,
):
    _, _, _, dataset, metadata, _, stored_summary = _persisted_inputs()

    def reject(*_args, **_kwargs):
        raise ProviderDirectoryProofStoreError("synthetic invalid proof")

    monkeypatch.setattr(
        importer,
        "validate_stored_dataset_proof_metadata",
        reject,
    )
    with pytest.raises(RuntimeError, match=_PROOF_ERROR):
        importer._validate_finalized_subset_completion_pair(dataset, metadata)

    monkeypatch.setattr(
        importer,
        "validate_stored_dataset_proof_metadata",
        lambda *_args, **_kwargs: {**stored_summary, "dataset_hash": "f" * 64},
    )
    with pytest.raises(RuntimeError, match=_PROOF_ERROR):
        importer._validate_finalized_subset_completion_pair(dataset, metadata)


def test_coverage_validators_reject_root_totals_and_geometry_drift():
    _, _, content, _, metadata, _, _ = _persisted_inputs()
    proof = content.completion_proof
    sha256 = content.completion_proof_sha256
    assert proof is not None and sha256 is not None
    coverage = metadata[importer.SERVER_ISSUED_SUBSET_COVERAGE_KEY]
    resources = proof["resources"]

    invalid_root = copy.deepcopy(metadata)
    invalid_root[importer.SERVER_ISSUED_SUBSET_COVERAGE_KEY]["scope"] = "wrong"
    with pytest.raises(ValueError, match="subset_coverage_invalid"):
        importer._validated_subset_coverage_root(invalid_root, proof, sha256)

    invalid_totals_by_field = {**coverage, "advertised_pre": -1}
    with pytest.raises(ValueError, match="subset_coverage_invalid"):
        importer._validate_subset_coverage_totals(
            invalid_totals_by_field,
            resources,
        )

    resource_type = next(iter(resources))
    invalid_geometry_by_field = {"pages": -1}
    with pytest.raises(ValueError, match="subset_coverage_invalid"):
        importer._validate_subset_resource_geometry(
            invalid_geometry_by_field,
            resources[resource_type],
        )


def test_coverage_validators_reject_continuation_and_unresolved_drift():
    _, _, content, _, metadata, _, _ = _persisted_inputs()
    proof = content.completion_proof
    assert proof is not None
    resource_type = next(iter(proof["resources"]))

    with pytest.raises(ValueError, match="subset_coverage_invalid"):
        importer._validate_subset_resource_continuation(
            {"validated_hops": -1, "chain_sha256": "f" * 64},
            proof["resources"][resource_type],
        )

    coverage = metadata[importer.SERVER_ISSUED_SUBSET_COVERAGE_KEY]
    one_sided_coverage_by_field = {
        **coverage,
        "unresolved_reference_count": 1,
    }
    with pytest.raises(ValueError, match="subset_coverage_invalid"):
        importer._validate_subset_unresolved_coverage(
            one_sided_coverage_by_field
        )
    malformed_coverage_by_field = {
        **coverage,
        "unresolved_reference_count": 1,
        "unresolved_reference_counts": {"wrong": 1},
    }
    with pytest.raises(ValueError, match="subset_coverage_invalid"):
        importer._validate_subset_unresolved_coverage(
            malformed_coverage_by_field
        )


def _acquired_rows(candidate):
    return [
        {
            "resource_type": resource_type,
            "resource_id": f"{resource_type.lower()}-1",
            "acquired_resource_sha256": "d" * 64,
        }
        for resource_type in candidate.selected_resources
    ]


@pytest.mark.asyncio
async def test_acquired_resource_hashes_bind_every_selected_family():
    candidate, _, content, _, _, _, _ = _persisted_inputs()
    connection = SimpleNamespace(all=AsyncMock(return_value=_acquired_rows(candidate)))

    observed = await importer._subset_acquired_resource_hashes(
        connection,
        candidate,
        content.resource_counts,
    )

    assert observed == {
        resource_type: importer.subset_canonical_sha256(
            [
                {
                    "resource_id": f"{resource_type.lower()}-1",
                    "sha256": "d" * 64,
                }
            ]
        )
        for resource_type in sorted(candidate.selected_resources)
    }


@pytest.mark.asyncio
async def test_acquired_resource_hashes_reject_row_and_count_drift():
    candidate, _, content, _, _, _, _ = _persisted_inputs()
    rows = _acquired_rows(candidate)
    invalid_rows = [
        [{**rows[0], "resource_type": "Unknown"}, *rows[1:]],
        [{**rows[0], "resource_id": None}, *rows[1:]],
        [{**rows[0], "acquired_resource_sha256": "bad"}, *rows[1:]],
    ]

    for invalid in invalid_rows:
        connection = SimpleNamespace(all=AsyncMock(return_value=invalid))
        with pytest.raises(RuntimeError, match="acquired_content_invalid"):
            await importer._subset_acquired_resource_hashes(
                connection,
                candidate,
                content.resource_counts,
            )

    connection = SimpleNamespace(all=AsyncMock(return_value=rows[:-1]))
    with pytest.raises(RuntimeError, match="acquired_content_count_mismatch"):
        await importer._subset_acquired_resource_hashes(
            connection,
            candidate,
            content.resource_counts,
        )


@pytest.mark.asyncio
async def test_candidate_content_proof_attaches_acquired_hashes(monkeypatch):
    candidate, _, content, _, _, _, _ = _persisted_inputs()
    stored = SimpleNamespace(
        dataset_hash=content.dataset_hash,
        resource_count=content.resource_count,
        resource_hashes=content.resource_hashes,
        resource_counts=content.resource_counts,
        source_metrics={},
        metadata={},
    )
    acquired = dict.fromkeys(candidate.selected_resources, "d" * 64)
    monkeypatch.setattr(
        importer,
        "build_stored_dataset_proof",
        AsyncMock(return_value=stored),
    )
    monkeypatch.setattr(
        importer,
        "_subset_acquired_resource_hashes",
        AsyncMock(return_value=acquired),
    )

    observed = await importer._candidate_endpoint_dataset_content_proof(
        SimpleNamespace(),
        candidate,
    )

    assert observed.acquired_resource_hashes == acquired


def test_persisted_replay_is_reattached_only_from_valid_resource_maps():
    _, diagnostics, _, _, metadata, _, _ = _persisted_inputs()
    diagnostic_without_replay_by_type = {
        resource_type: {
            field_name: field_value
            for field_name, field_value in diagnostic.items()
            if field_name
            != importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY
        }
        for resource_type, diagnostic in diagnostics.items()
    }

    assert importer._diagnostics_with_persisted_subset_replay(
        diagnostic_without_replay_by_type,
        None,
    ) == diagnostic_without_replay_by_type
    assert importer._diagnostics_with_persisted_subset_replay(
        diagnostic_without_replay_by_type,
        {},
    ) == diagnostic_without_replay_by_type
    observed = importer._diagnostics_with_persisted_subset_replay(
        diagnostic_without_replay_by_type,
        metadata,
    )
    assert all(
        importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY in diagnostic
        for diagnostic in observed.values()
    )
    malformed = copy.deepcopy(metadata)
    replay_resources = malformed[
        importer.SERVER_ISSUED_SUBSET_REPLAY_EVIDENCE_KEY
    ]["resources"]
    malformed_resource_type = next(iter(replay_resources))
    replay_resources[malformed_resource_type] = None
    malformed_observed = importer._diagnostics_with_persisted_subset_replay(
        diagnostic_without_replay_by_type,
        malformed,
    )
    assert (
        importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY
        not in malformed_observed[malformed_resource_type]
    )


def test_subset_coverage_helpers_reject_missing_or_invalid_evidence():
    candidate, _, content, _, _, _, _ = _persisted_inputs()

    with pytest.raises(RuntimeError, match="subset_coverage_invalid"):
        importer._subset_resource_coverage_map({})
    with pytest.raises(RuntimeError, match="subset_coverage_invalid"):
        importer._subset_unresolved_relation_counts(
            {
                importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
                    "unresolved_reference_count": -1
                },
                importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
                    "unresolved_reference_count": 0
                },
            }
        )
    with pytest.raises(RuntimeError, match="subset_coverage_invalid"):
        importer._subset_dataset_coverage_metadata(
            candidate,
            {},
            replace(
                content,
                completion_proof=None,
                completion_proof_sha256=None,
            ),
            {},
            {},
        )
