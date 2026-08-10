# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lifecycle identity boundaries for semantic-v4 Organization roots."""

from __future__ import annotations

import datetime as dt
import importlib


importer = importlib.import_module("process.provider_directory_fhir")

CONTRACT = importer.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
PROJECTION_AS_OF = "2026-08-10"
PROOF_SCOPE = importer._provider_directory_proof_resource_scope(
    ("Organization",)
)


def _candidate(
    resource_hash_contract: str = CONTRACT,
) -> importer.EndpointDatasetCandidate:
    """Return one exact twin-root candidate under the requested contract."""

    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-synthetic",
        dataset_id="dataset-synthetic",
        acquisition_root_run_id="root-synthetic",
        source_ids=("source-synthetic",),
        selected_resources=("Organization",),
        expected_resources=("Organization",),
        import_run_id="root-synthetic",
        previous_dataset_id=None,
        requires_twin_root_verification=True,
        verification_campaign_id="campaign-synthetic",
        verification_source_scope_hash="scope-synthetic",
        verification_role=importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
        resource_hash_contract=resource_hash_contract,
        semantic_projection_as_of=PROJECTION_AS_OF,
        proof_resource_scope=PROOF_SCOPE,
    )


def _content_proof() -> importer.EndpointDatasetContentProof:
    """Return one stable Organization content identity."""

    return importer.EndpointDatasetContentProof(
        dataset_hash="a" * 64,
        resource_count=1,
        resource_hashes={"Organization": "b" * 64},
        resource_counts={"Organization": 1},
    )


def _embedded_proof(resource_hash_contract: str = CONTRACT) -> dict[str, object]:
    """Build one contract-shaped embedded twin proof."""

    return importer._twin_root_content_proof(
        _candidate(resource_hash_contract),
        _content_proof(),
    )


def _stored_proof(resource_hash_contract: str = CONTRACT) -> dict[str, object]:
    """Return the sealed fields compared during finalized replay."""

    content_proof = _content_proof()
    return {
        importer.RESOURCE_HASH_CONTRACT_METADATA_KEY: resource_hash_contract,
        "dataset_hash": content_proof.dataset_hash,
        "resource_count": content_proof.resource_count,
        "resource_hashes": content_proof.resource_hashes,
        "resource_counts": content_proof.resource_counts,
    }


def test_v4_fresh_and_resumed_roots_keep_hash_identity(monkeypatch) -> None:
    """Create one v4 root identity and recover it without recomputation."""

    selection = importer.EndpointDatasetCandidateSelection(
        dataset_id="dataset-synthetic",
        acquisition_root_run_id="root-synthetic",
        previous_dataset_id=None,
        reused_from_checkpoint=False,
        resource_hash_contract=CONTRACT,
    )
    monkeypatch.setattr(
        importer,
        "_now",
        lambda: dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
    )
    fresh = importer._selection_with_semantic_projection_as_of(
        selection,
        {},
        ("Organization",),
    )
    persisted_state_by_field = {
        "publication_metadata_json": {
            importer.RESOURCE_HASH_CONTRACT_METADATA_KEY: CONTRACT,
            importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY: PROJECTION_AS_OF,
            "selected_resources": ["Organization"],
            importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY: list(
                PROOF_SCOPE
            ),
        }
    }
    resumed = importer._selection_with_semantic_projection_as_of(
        selection,
        persisted_state_by_field,
        ("Organization",),
    )
    assert fresh.semantic_projection_as_of == PROJECTION_AS_OF
    assert fresh.proof_resource_scope == PROOF_SCOPE
    assert resumed == fresh


def test_v4_twin_contract_shape() -> None:
    """Make v4 self-describing without changing historical v3 proof shape."""

    v4_proof = _embedded_proof()
    assert v4_proof[importer.RESOURCE_HASH_CONTRACT_METADATA_KEY] == CONTRACT
    drifted_proof_by_field = dict(v4_proof)
    drifted_proof_by_field[
        importer.RESOURCE_HASH_CONTRACT_METADATA_KEY
    ] = importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    assert importer._twin_root_mismatch_fields(
        v4_proof,
        drifted_proof_by_field,
    ) == [importer.RESOURCE_HASH_CONTRACT_METADATA_KEY]

    v3_proof = _embedded_proof(
        importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    assert importer.RESOURCE_HASH_CONTRACT_METADATA_KEY not in v3_proof


def test_finalized_twin_proof_requires_exact_v4_contract() -> None:
    """Reject missing or cross-version markers beside a sealed v4 proof."""

    embedded_proof_by_field = _embedded_proof()
    metadata_by_field = {
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "proof": embedded_proof_by_field
        }
    }
    assert importer._is_finalized_semantic_twin_proof_exact(
        metadata_by_field,
        _stored_proof(),
    )
    for invalid_contract in (
        None,
        importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    ):
        invalid_proof_by_field = dict(embedded_proof_by_field)
        if invalid_contract is None:
            invalid_proof_by_field.pop(
                importer.RESOURCE_HASH_CONTRACT_METADATA_KEY
            )
        else:
            invalid_proof_by_field[
                importer.RESOURCE_HASH_CONTRACT_METADATA_KEY
            ] = invalid_contract
        assert not importer._is_finalized_semantic_twin_proof_exact(
            {
                importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
                    "proof": invalid_proof_by_field
                }
            },
            _stored_proof(),
        )


def test_v3_finalized_twin_proof_rejects_new_contract_field() -> None:
    """Keep historical v3 embedded proofs readable and shape-exact."""

    v3_contract = importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    v3_proof_by_field = _embedded_proof(v3_contract)
    metadata_by_field = {
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "proof": v3_proof_by_field
        }
    }
    assert importer._is_finalized_semantic_twin_proof_exact(
        metadata_by_field,
        _stored_proof(v3_contract),
    )
    v3_proof_by_field[importer.RESOURCE_HASH_CONTRACT_METADATA_KEY] = CONTRACT
    assert not importer._is_finalized_semantic_twin_proof_exact(
        metadata_by_field,
        _stored_proof(v3_contract),
    )


def test_artifact_sql_binds_v4_twin_contract() -> None:
    """Require parent and baseline SQL to retain the nested v4 marker."""

    matched_sql = " ".join(
        importer._artifact_matched_proof_sql(
            "candidate_metadata",
            "candidate_verification",
            "candidate_proof",
        ).split()
    )
    equality_sql = " ".join(
        importer._artifact_twin_proof_equality_sql(
            "candidate_proof",
            "baseline_proof",
        ).split()
    )
    contract_key = importer.RESOURCE_HASH_CONTRACT_METADATA_KEY
    assert f"candidate_proof ->> '{contract_key}'" in matched_sql
    assert CONTRACT in matched_sql
    assert (
        f"(candidate_proof -> '{contract_key}') IS NOT DISTINCT FROM "
        f"(baseline_proof -> '{contract_key}')"
    ) in equality_sql
