# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import importlib
import json
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def test_reviewed_sources_use_distinct_nonempty_campaigns():
    campaign_ids = [
        importer.MICHIGAN_VERIFICATION_CAMPAIGN_ID,
        importer.SCAN_VERIFICATION_CAMPAIGN_ID,
        *importer.REVIEWED_PROVIDER_DIRECTORY_CAMPAIGN_BY_SEED_ID.values(),
    ]

    assert all(campaign_ids)
    assert len(campaign_ids) == len(set(campaign_ids))
    enabled_rows = [
        row
        for row in importer._reviewed_provider_directory_candidate_seed_rows()
        if row["metadata_json"].get(
            "provider_directory_acquisition_enabled"
        )
    ]
    assert {
        row["id"]: row["metadata_json"][
            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
        ]
        for row in enabled_rows
    } == importer.REVIEWED_PROVIDER_DIRECTORY_CAMPAIGN_BY_SEED_ID


def test_established_checkpoint_candidate_keeps_legacy_profile_compatibility():
    source_by_field = {
        "source_id": "source_a",
        "endpoint_id": "endpoint_1",
        "api_base": importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE,
        "metadata_json": {
            "provider_directory_supported_resources": ["Organization"],
            "provider_directory_fully_enumerable_resources": [
                "Organization"
            ],
        },
    }
    assert (
        importer._is_artifact_source_verification_profile_present(
            source_by_field
        )
        is False
    )
    assert importer._pagination_checkpoint_scope_identity(
        source_by_field,
        ["source_a"],
    ) is not None
    dataset = importer._provider_directory_artifact_dataset_from_row(
        {
            "source_id": "source_a",
            "endpoint_id": "endpoint_1",
            "source_record_json": source_by_field,
            "dataset_id": "dataset_candidate",
            "evidence_run_id": "root_candidate",
            "selected_resources": ["Organization"],
            "recorded_expected_resources": None,
            "status": importer.ENDPOINT_DATASET_VALIDATED,
            "is_current": False,
            "current_dataset_count": 1,
            "current_dataset_id": "dataset_current",
            "validated_candidate_count": 1,
            "previous_dataset_id": "dataset_current",
            "promote_on_cutover": True,
            "dataset_hash": "a" * 64,
            "resource_count": 1,
            "validated_at": "2026-07-20T00:00:00+00:00",
            "publication_metadata_json": {
                "selected_resources": ["Organization"]
            },
        }
    )

    assert dataset is not None
    assert dataset.verification_campaign_id is None
    assert dataset.verification_source_scope_hash is None
    assert dataset.verification_source_ids == ("source_a",)
    assert dataset.recorded_expected_resources is None


def test_established_aetna_checkpoint_scope_hash_is_byte_stable(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)
    override_token = importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.set(
        None
    )
    try:
        source_record = importer._source_row_from_seed(
            importer._aetna_provider_directory_data_seed_rows()[0]
        )
        scope_identity = importer._pagination_checkpoint_scope_identity(
            source_record,
            [source_record["source_id"]],
        )
    finally:
        importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.reset(
            override_token
        )

    assert source_record["source_id"] == "pdfhir_d68a896335981928bdbbb80e"
    assert scope_identity == (
        importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "102e21d81674eaaf2661fb6b6f77adcf4b29976cd297090eebbaadf653c643f0",
    )


def _source_record(
    source_id: str = "source_a",
    source_status: str = importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
) -> dict[str, object]:
    return {
        "source_id": source_id,
        "endpoint_id": "serving_endpoint_old",
        "api_base": importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": importer.IOWA_MEDICAID_PROVIDER_DIRECTORY_BASE,
        "metadata_json": {
            "provider_directory_candidate_status": (
                source_status
            ),
            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY: (
                "campaign-v1"
            ),
            importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY: (
                "candidate_endpoint"
            ),
            "provider_directory_supported_resources": ["Organization"],
            "provider_directory_fully_enumerable_resources": [
                "Organization"
            ],
            "provider_directory_acquisition_enabled": True,
            "provider_directory_coverage_mode": "full",
        },
    }


def _reviewed_scope_hash(source_record: dict[str, object]) -> str:
    scope_hash = importer._twin_root_scope_hash(
        [source_record],
        "campaign-v1",
        None,
    )
    assert scope_hash is not None
    return scope_hash


@pytest.mark.parametrize(
    "metadata_updates",
    [
        pytest.param(
            {"provider_directory_resource_page_count_caps": {"Organization": 17}},
            id="page-count-caps",
        ),
        pytest.param(
            {"provider_directory_acquisition_enabled": False},
            id="acquisition-enabled",
        ),
        pytest.param(
            {"provider_directory_coverage_mode": "targeted"},
            id="coverage-mode",
        ),
        pytest.param(
            {
                "provider_directory_supported_resources": [
                    "Organization",
                    "Practitioner",
                ]
            },
            id="supported-resources",
        ),
        pytest.param(
            {
                "provider_directory_fully_enumerable_resources": [
                    "Organization",
                    "Practitioner",
                ]
            },
            id="fully-enumerable-resources",
        ),
        pytest.param(
            {
                "provider_directory_supported_resources": ["Practitioner"],
                "provider_directory_fully_enumerable_resources": [
                    "Practitioner"
                ],
            },
            id="expected-resources",
        ),
        pytest.param(
            {
                importer.LAST_UPDATED_PARTITION_METADATA_KEY: {
                    "enabled": True,
                    "resources": {
                        "Organization": {
                            "start": "1900-01-01T00:00:00Z",
                            "end": "resolved_now",
                            "ceiling": 1000,
                            "minimum_width_seconds": 1,
                            "page_count": 50,
                            "volatile_metadata_paths": [],
                        }
                    },
                }
            },
            id="last-updated-partition",
        ),
    ],
)
def test_reviewed_twin_scope_binds_operational_metadata(
    metadata_updates,
):
    source_record = _source_record()
    baseline_hash = _reviewed_scope_hash(source_record)
    changed_source_record = copy.deepcopy(source_record)
    changed_source_record["metadata_json"].update(metadata_updates)

    assert _reviewed_scope_hash(changed_source_record) != baseline_hash


def test_reviewed_twin_scope_binds_resource_endpoint_config():
    source_record = _source_record()
    changed_source_record = copy.deepcopy(source_record)
    changed_source_record["endpoint_organization"] = (
        "https://replacement.example.test/fhir/Organization"
    )

    assert _reviewed_scope_hash(changed_source_record) != _reviewed_scope_hash(
        source_record
    )


def test_reviewed_twin_scope_rejects_alias_contract_drift():
    source_a = _source_record("source_a")
    source_b = _source_record("source_b")
    source_b["metadata_json"]["provider_directory_coverage_mode"] = (
        "targeted"
    )

    with pytest.raises(RuntimeError, match="verification_scope_ambiguous"):
        importer._twin_root_scope_hash(
            [source_a, source_b],
            "campaign-v1",
            None,
        )


def test_reviewed_twin_scope_binds_credential_descriptor(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)
    override_token = importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.set(
        None
    )
    source_record = _source_record()
    try:
        monkeypatch.delenv(
            importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
            raising=False,
        )
        baseline_hash = _reviewed_scope_hash(source_record)
        monkeypatch.setenv(
            importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
            json.dumps(
                {
                    "sources": {
                        "source_a": {"headers": {"X-Provider-Key": "secret"}}
                    }
                }
            ),
        )
        credentialed_hash = _reviewed_scope_hash(source_record)
    finally:
        importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.reset(
            override_token
        )

    assert credentialed_hash != baseline_hash


def _selected_dataset(
    source_ids: tuple[str, ...] = ("source_a",),
    *,
    source_status: str = importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
) -> importer.ProviderDirectoryArtifactDataset:
    source_record = _source_record(source_status=source_status)
    scope_identity = importer._pagination_checkpoint_scope_identity(
        source_record,
        list(source_ids),
    )
    assert scope_identity is not None
    verification_scope_hash = importer._twin_root_verification_scope_hash(
        [source_record],
        list(source_ids),
        scope_identity[1],
    )
    dataset = importer._provider_directory_artifact_dataset_from_row(
        {
            "source_id": "source_a",
            "endpoint_id": "candidate_endpoint",
            "serving_endpoint_id": "serving_endpoint_old",
            "source_record_json": source_record,
            "dataset_id": "dataset_candidate",
            "evidence_run_id": "root_candidate",
            "selected_resources": ["Organization"],
            "recorded_expected_resources": ["Organization"],
            "status": importer.ENDPOINT_DATASET_VALIDATED,
            "is_current": False,
            "current_dataset_count": 1,
            "current_dataset_id": "dataset_current",
            "validated_candidate_count": 1,
            "previous_dataset_id": "dataset_current",
            "promote_on_cutover": True,
            "dataset_hash": "a" * 64,
            "resource_count": 1,
            "validated_at": "2026-07-20T00:00:00+00:00",
            "publication_metadata_json": {
                "source_ids": list(source_ids),
                "selected_resources": ["Organization"],
                "expected_resources": ["Organization"],
                importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: "campaign-v1",
                importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
                    verification_scope_hash
                ),
            },
        }
    )
    assert dataset is not None
    assert dataset.source_verification_contract
    assert dataset.source_verification_contract_hash
    return dataset


def test_promotion_contract_rejects_pending_source_profile():
    with pytest.raises(RuntimeError, match="candidate_status_not_verified"):
        _selected_dataset(
            source_status=importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING
        )


def _locked_alias_row(source_record: dict[str, object]) -> dict[str, object]:
    return {
        "source_id": source_record["source_id"],
        "endpoint_id": source_record["endpoint_id"],
        "source_record_json": source_record,
    }


def test_cutover_source_contract_accepts_exact_locked_source():
    dataset = _selected_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))

    importer._assert_locked_artifact_fence_aliases(
        fence,
        [_locked_alias_row(_source_record())],
    )


@pytest.mark.parametrize(
    "metadata_key,changed_value",
    [
        (
            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY,
            "campaign-v2",
        ),
        (
            importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY,
            "replacement_endpoint",
        ),
        (
            "provider_directory_candidate_status",
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING,
        ),
        (
            importer.LAST_UPDATED_PARTITION_METADATA_KEY,
            {"enabled": True, "resources": {}},
        ),
    ],
)
def test_cutover_source_contract_rejects_profile_drift(
    metadata_key,
    changed_value,
):
    dataset = _selected_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    changed_source = copy.deepcopy(_source_record())
    changed_source["metadata_json"][metadata_key] = changed_value

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_verification_contract_changed",
    ):
        importer._assert_locked_artifact_fence_aliases(
            fence,
            [_locked_alias_row(changed_source)],
        )


def test_cutover_source_contract_rejects_acquisition_config_drift():
    dataset = _selected_dataset()
    fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    changed_source = copy.deepcopy(_source_record())
    changed_source["canonical_api_base"] = (
        "https://replacement.example.test/fhir"
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_verification_contract_changed",
    ):
        importer._assert_locked_artifact_fence_aliases(
            fence,
            [_locked_alias_row(changed_source)],
        )


@pytest.mark.asyncio
async def test_reviewed_promotion_expands_and_repoints_full_proven_alias_set(
    monkeypatch,
):
    promotion_dataset = _selected_dataset(("source_a", "source_b"))
    initial_fence = importer.ProviderDirectoryArtifactDatasetFence(
        (promotion_dataset,),
        should_select_validated_candidates=True,
    )
    source_records = [_source_record("source_a"), _source_record("source_b")]
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            return_value=[
                _locked_alias_row(source_record)
                for source_record in source_records
            ]
        ),
    )

    expanded_fence = await importer._expand_reviewed_artifact_promotion_aliases(
        initial_fence
    )

    assert expanded_fence.source_ids == ["source_a"]
    assert expanded_fence.locked_source_ids == ["source_a", "source_b"]
    assert expanded_fence.published_source_endpoint_tuples == (
        ("source_a", "candidate_endpoint"),
        ("source_b", "candidate_endpoint"),
    )
    update_status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", update_status)
    await importer._cutover_provider_directory_artifact_sources(
        expanded_fence
    )
    assert [
        call.kwargs["source_id"] for call in update_status.await_args_list
    ] == ["source_a", "source_b"]

    changed_source_b = copy.deepcopy(source_records[1])
    changed_source_b["metadata_json"][
        importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
    ] = "campaign-v2"
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_verification_contract_changed",
    ):
        importer._assert_locked_artifact_fence_aliases(
            expanded_fence,
            [
                _locked_alias_row(source_records[0]),
                _locked_alias_row(changed_source_b),
            ],
        )
