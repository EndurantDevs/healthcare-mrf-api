# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import dataclasses
import importlib
from unittest.mock import AsyncMock

import pytest

from tests import test_provider_directory_artifact_verification_contract as artifact
from tests import test_provider_directory_twin_root_verification as verification


importer = importlib.import_module("process.provider_directory_fhir")


def _candidate(**changes):
    return dataclasses.replace(verification._candidate(), **changes)


def _orphan_metadata(candidate) -> dict[str, object]:
    return {
        "acquisition_root_run_id": candidate.acquisition_root_run_id,
        "selected_resources": list(candidate.selected_resources),
        "source_ids": list(candidate.source_ids),
        "requires_twin_root_verification": (
            candidate.requires_twin_root_verification
        ),
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: (
            candidate.verification_campaign_id
        ),
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
            candidate.verification_source_scope_hash
        ),
        importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: candidate.verification_role,
        importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: (
            candidate.verification_baseline_dataset_id
        ),
    }


def test_empty_orphan_identity_accepts_exact_and_rejects_drift():
    candidate = _candidate(
        repair_empty_orphan=True,
        verification_role=importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
    )
    existing_candidate_map = {
        "endpoint_id": candidate.endpoint_id,
        "status": importer.ENDPOINT_DATASET_ACQUIRING,
        "publication_metadata_json": _orphan_metadata(candidate),
    }
    importer._assert_empty_orphan_candidate_identity(
        existing_candidate_map,
        candidate,
    )

    mismatched_candidate_map = copy.deepcopy(existing_candidate_map)
    mismatched_candidate_map["publication_metadata_json"]["source_ids"] = [
        "other_source"
    ]
    with pytest.raises(RuntimeError, match="orphan_identity_mismatch"):
        importer._assert_empty_orphan_candidate_identity(
            mismatched_candidate_map,
            candidate,
        )


def test_locked_twin_admission_rejects_each_invalid_identity_shape():
    with pytest.raises(RuntimeError, match="verification_identity_required"):
        importer._candidate_with_locked_twin_root_admission(
            _candidate(
                verification_campaign_id=None,
                verification_source_scope_hash=None,
            ),
            {},
        )
    with pytest.raises(RuntimeError, match="verification_admission_mismatch"):
        importer._candidate_with_locked_twin_root_admission(
            _candidate(
                verification_role=importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
                verification_baseline_dataset_id="unexpected_baseline",
            ),
            {},
        )
    with pytest.raises(RuntimeError, match="verification_role_invalid"):
        importer._candidate_with_locked_twin_root_admission(
            _candidate(verification_role="invalid_role"),
            {},
        )
    with pytest.raises(RuntimeError, match="verification_admission_mismatch"):
        importer._candidate_with_locked_twin_root_admission(
            _candidate(
                verification_role=(
                    importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
                ),
                verification_baseline_dataset_id="missing_baseline",
            ),
            {},
        )


def test_persisted_verification_fields_reject_invalid_profiles():
    with pytest.raises(RuntimeError, match="verification_requirement_missing"):
        importer._persisted_endpoint_dataset_verification_fields(
            {},
            requires_twin_root_verification=True,
            verification_campaign_id="campaign-v1",
            verification_source_scope_hash="scope-v1",
        )
    with pytest.raises(RuntimeError, match="verification_profile_mismatch"):
        importer._persisted_endpoint_dataset_verification_fields(
            {"requires_twin_root_verification": False},
            requires_twin_root_verification=True,
            verification_campaign_id=None,
            verification_source_scope_hash=None,
        )
    assert importer._persisted_endpoint_dataset_verification_fields(
        {"requires_twin_root_verification": False},
        requires_twin_root_verification=False,
        verification_campaign_id=None,
        verification_source_scope_hash=None,
    ) == {
        "requires_twin_root_verification": False,
        "verification_campaign_id": None,
        "verification_source_scope_hash": None,
    }


def test_persisted_verification_admission_rejects_invalid_roles():
    with pytest.raises(RuntimeError, match="verification_admission_invalid"):
        importer._persisted_verification_admission_fields(
            {
                importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
                    importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
                ),
                importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: (
                    "unexpected_baseline"
                ),
            }
        )
    with pytest.raises(RuntimeError, match="verification_role_invalid"):
        importer._persisted_verification_admission_fields(
            {importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: "invalid_role"}
        )


def test_terminal_selection_rejects_current_or_missing_metadata():
    terminal_row_map = {
        "status": importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        "endpoint_id": "endpoint_1",
        "acquisition_root_run_id": "root_candidate",
        "is_current": True,
    }
    selection_params_by_name = {
        "requires_twin_root_verification": True,
        "verification_campaign_id": "reviewed-candidates-v1",
        "verification_source_scope_hash": "scope-v1",
    }
    with pytest.raises(RuntimeError, match="finalized_state_invalid"):
        importer._verification_terminal_endpoint_dataset_selection(
            terminal_row_map,
            "dataset_candidate",
            "endpoint_1",
            "root_candidate",
            **selection_params_by_name,
        )
    terminal_row_map["is_current"] = False
    terminal_row_map["publication_metadata_json"] = None
    with pytest.raises(RuntimeError, match="verification_metadata_invalid"):
        importer._verification_terminal_endpoint_dataset_selection(
            terminal_row_map,
            "dataset_candidate",
            "endpoint_1",
            "root_candidate",
            **selection_params_by_name,
        )


def test_twin_scope_rejects_missing_and_mismatched_checkpoint_identity():
    with pytest.raises(RuntimeError, match="verification_scope_required"):
        importer._twin_root_scope_hash(
            [
                {
                    "source_id": "source_a",
                    "api_base": "https://uncheckpointed.example.test/fhir",
                }
            ],
            "campaign-v1",
            None,
        )
    with pytest.raises(RuntimeError, match="verification_scope_required"):
        importer._twin_root_common_acquisition_contract([])

    source_record = artifact._source_record()
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=source_record["canonical_api_base"],
        source_scope_hash="wrong-scope",
        source_ids=("source_a",),
        owner_run_id="root_candidate",
        acquisition_root_run_id="root_candidate",
    )
    with pytest.raises(RuntimeError, match="verification_scope_mismatch"):
        importer._twin_root_scope_hash(
            [source_record],
            "campaign-v1",
            checkpoint_context,
        )


def test_artifact_contract_rejects_profile_and_scope_drift():
    source_record = artifact._source_record()
    publication_metadata_by_field = {"source_ids": ["source_a"]}
    with pytest.raises(RuntimeError, match="resource_profile_changed"):
        importer._artifact_promotion_source_contract(
            source_record,
            publication_metadata_by_field,
            ("Practitioner",),
            dataset_id="dataset_candidate",
            verification_campaign_id="campaign-v1",
            verification_source_scope_hash="wrong-scope",
        )
    with pytest.raises(RuntimeError, match="source_scope_changed"):
        importer._artifact_promotion_source_contract(
            source_record,
            publication_metadata_by_field,
            ("Organization",),
            dataset_id="dataset_candidate",
            verification_campaign_id="campaign-v1",
            verification_source_scope_hash="wrong-scope",
        )


def test_artifact_alias_and_fence_guards_reject_drift():
    dataset = artifact._selected_dataset()
    with pytest.raises(RuntimeError, match="source_contract_invalid"):
        importer._artifact_promotion_alias_from_row(
            dataset,
            {"source_id": None, "endpoint_id": None, "source_record_json": {}},
        )

    changed_source = copy.deepcopy(artifact._source_record())
    changed_source["canonical_api_base"] = (
        "https://replacement.example.test/fhir"
    )
    with pytest.raises(RuntimeError, match="source_scope_changed"):
        importer._artifact_promotion_alias_from_row(
            dataset,
            artifact._locked_alias_row(changed_source),
        )

    conflicting_alias = dataclasses.replace(dataset, dataset_id="dataset_other")
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (dataset,),
        promotion_aliases=(conflicting_alias,),
    )
    with pytest.raises(RuntimeError, match="promotion_alias_ambiguous"):
        fence.locked_alias_datasets


@pytest.mark.asyncio
async def test_reviewed_alias_expansion_requires_every_proven_source(monkeypatch):
    dataset = artifact._selected_dataset(("source_a", "source_b"))
    fence = importer.ProviderDirectoryArtifactDatasetFence(
        (dataset,),
        should_select_validated_candidates=True,
    )
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            return_value=[
                artifact._locked_alias_row(artifact._source_record("source_a"))
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="promotion_source_alias_missing"):
        await importer._expand_reviewed_artifact_promotion_aliases(fence)
