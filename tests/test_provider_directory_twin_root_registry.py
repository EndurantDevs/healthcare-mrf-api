# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")
CAMPAIGN_ID = "reviewed-candidates-v1"
_UNSET = object()


def _source_row(
    status: object = _UNSET,
    campaign_id: object = _UNSET,
) -> dict[str, str]:
    metadata: dict[str, object] = {}
    if status is not _UNSET:
        metadata["provider_directory_candidate_status"] = status
    if campaign_id is not _UNSET:
        metadata[
            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
        ] = campaign_id
    return {"metadata_json": json.dumps(metadata)}


def _verification_profile(
    is_required: bool = True,
) -> importer.EndpointDatasetVerificationProfile:
    return importer.EndpointDatasetVerificationProfile(
        is_twin_root_required=is_required,
        campaign_id=CAMPAIGN_ID if is_required else None,
        source_scope_hash="scope-v1" if is_required else None,
    )


def _resumable_dataset(metadata: dict[str, object]) -> dict[str, object]:
    return {
        "dataset_id": "dataset_candidate",
        "endpoint_id": "endpoint_1",
        "acquisition_root_run_id": "root_candidate",
        "previous_dataset_id": "dataset_current",
        "status": importer.ENDPOINT_DATASET_ACQUIRING,
        "is_current": False,
        "publication_metadata_json": metadata,
    }


async def _select_resumable_dataset(
    dataset: dict[str, object],
    profile: importer.EndpointDatasetVerificationProfile,
):
    return await importer._select_resumable_endpoint_dataset_candidate(
        dataset,
        "dataset_candidate",
        "endpoint_1",
        ("Organization", "Practitioner"),
        "root_candidate",
        None,
        None,
        profile,
    )


def _disable_checkpoint_lookup(monkeypatch) -> None:
    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        AsyncMock(return_value=None),
    )


def test_source_registry_status_controls_twin_root_requirement():
    pending_row = _source_row(
        importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING,
        CAMPAIGN_ID,
    )
    assert importer._needs_source_group_twin_root_verification(
        [pending_row, pending_row]
    )
    assert not importer._needs_source_group_twin_root_verification(
        [_source_row()]
    )
    verified_row = _source_row(
        importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        CAMPAIGN_ID,
    )
    assert importer._source_group_twin_root_verification_profile(
        [verified_row]
    ) == (False, CAMPAIGN_ID)


@pytest.mark.parametrize(
    ("source_rows", "error_pattern"),
    [
        (
            [
                _source_row(importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING, CAMPAIGN_ID),
                _source_row(),
            ],
            "verification_profile_ambiguous",
        ),
        (
            [_source_row(importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING)],
            "verification_campaign_required",
        ),
        ([_source_row("pendng", CAMPAIGN_ID)], "verification_status_invalid"),
        (
            [
                _source_row(importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING, CAMPAIGN_ID),
                _source_row(
                    importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING,
                    "reviewed-candidates-v2",
                ),
            ],
            "verification_profile_ambiguous",
        ),
        ([_source_row(campaign_id=CAMPAIGN_ID)], "verification_campaign_unexpected"),
    ],
)
def test_source_registry_rejects_invalid_verification_profiles(
    source_rows,
    error_pattern,
):
    with pytest.raises(RuntimeError, match=error_pattern):
        importer._needs_source_group_twin_root_verification(source_rows)


@pytest.mark.asyncio
async def test_retry_preserves_persisted_verification_requirement(monkeypatch):
    _disable_checkpoint_lookup(monkeypatch)
    metadata = {
        "requires_twin_root_verification": True,
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: CAMPAIGN_ID,
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: "scope-v1",
        importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
            importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
        ),
        importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: None,
    }
    selection = await _select_resumable_dataset(
        _resumable_dataset(metadata),
        _verification_profile(),
    )
    assert selection.repair_empty_orphan is True
    assert selection.requires_twin_root_verification is True
    assert selection.verification_role == (
        importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
    )


@pytest.mark.asyncio
async def test_retry_rejects_invalid_persisted_verification_requirement(
    monkeypatch,
):
    _disable_checkpoint_lookup(monkeypatch)
    dataset = _resumable_dataset(
        {"requires_twin_root_verification": "yes"}
    )
    with pytest.raises(RuntimeError, match="verification_requirement_invalid"):
        await _select_resumable_dataset(dataset, _verification_profile())


@pytest.mark.asyncio
async def test_established_retry_preserves_absent_verification_requirement(
    monkeypatch,
):
    _disable_checkpoint_lookup(monkeypatch)
    selection = await _select_resumable_dataset(
        _resumable_dataset({}),
        _verification_profile(False),
    )
    assert selection.requires_twin_root_verification is False
