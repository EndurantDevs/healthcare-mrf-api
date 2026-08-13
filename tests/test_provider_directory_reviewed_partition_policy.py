# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")

EXPECTED_ROLE_PARTITION = {
    "PractitionerRole": {
        "start": "1900-01-01T00:00:00Z",
        "end": "2026-08-17T00:00:00Z",
        "ceiling": 3000,
        "minimum_width_seconds": 1,
        "boundary_precision_seconds": 1,
        "page_count": 1000,
        "maximum_pages_per_window": 4,
        "volatile_metadata_paths": [],
    }
}
EXPECTED_CAMPAIGN = (
    "provider-directory-reviewed-practitioner-role-partition-2026-08-17-v3"
)


def _partitioned_reviewed_seed_row():
    matching_rows = [
        row
        for row in importer._reviewed_provider_directory_candidate_seed_rows()
        if importer.LAST_UPDATED_PARTITION_METADATA_KEY in row["metadata_json"]
    ]
    assert len(matching_rows) == 1
    return matching_rows[0]


def _partitioned_source_record():
    return importer._source_row_from_seed(_partitioned_reviewed_seed_row())


def _without_partition_policy(source_record):
    legacy_record = copy.deepcopy(source_record)
    legacy_record["metadata_json"].pop(importer.LAST_UPDATED_PARTITION_METADATA_KEY)
    return legacy_record


def test_reviewed_role_partition_policy_is_fixed_and_ordinary():
    seed_row = _partitioned_reviewed_seed_row()
    metadata = seed_row["metadata_json"]

    assert metadata[importer.LAST_UPDATED_PARTITION_METADATA_KEY] == {
        "enabled": True,
        "resources": EXPECTED_ROLE_PARTITION,
    }
    source_record = _partitioned_source_record()
    assert "provider_directory_candidate_status" not in metadata
    assert (
        importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
        not in metadata
    )
    assert importer.REVIEWED_ROOT_POLICY_METADATA_KEY not in metadata
    assert importer.REVIEWED_SUBSET_ACTIVATION_METADATA_KEY not in metadata
    assert importer.REVIEWED_SUBSET_ACTIVATION_METADATA_KEY_V2 not in metadata
    assert importer._reviewed_source_profile_key(source_record) == (
        None,
        None,
        None,
        False,
    )


def test_reviewed_role_partition_policy_is_resource_isolated():
    source_record = _partitioned_source_record()
    role_config, role_error = importer._last_updated_partition_config(
        source_record,
        "PractitionerRole",
    )
    other_config, other_error = importer._last_updated_partition_config(
        source_record,
        "Practitioner",
    )

    assert role_error is None
    assert role_config is not None
    assert role_config.ceiling == 3000
    assert role_config.page_count == 1000
    assert role_config.maximum_pages_per_window == 4
    assert role_config.identity()["maximum_pages_per_window"] == 4
    assert other_config is None
    assert other_error == "resource_not_opted_in"


def test_reviewed_role_partition_policy_fences_prior_root_scope():
    current_source = _partitioned_source_record()
    legacy_source = _without_partition_policy(current_source)
    source_ids = [current_source["source_id"]]
    legacy_checkpoint = importer._pagination_checkpoint_context(
        legacy_source,
        source_ids,
        run_id="root_legacy",
        retry_of_run_id=None,
    )

    current_scope = importer._pagination_checkpoint_scope_identity(
        current_source,
        source_ids,
    )
    legacy_scope = importer._pagination_checkpoint_scope_identity(
        legacy_source,
        source_ids,
    )
    current_twin_scope = importer._twin_root_scope_hash(
        [current_source], EXPECTED_CAMPAIGN, None
    )
    legacy_twin_scope = importer._twin_root_scope_hash(
        [legacy_source], EXPECTED_CAMPAIGN, None
    )

    assert legacy_checkpoint is not None
    assert current_scope != legacy_scope
    assert current_twin_scope != legacy_twin_scope
    with pytest.raises(
        RuntimeError,
        match="provider_directory_endpoint_dataset_verification_scope_mismatch",
    ):
        importer._twin_root_scope_hash(
            [current_source], EXPECTED_CAMPAIGN, legacy_checkpoint
        )


def test_reviewed_role_partition_policy_fences_prior_campaign_generation():
    current_source = _partitioned_source_record()
    prior_source = copy.deepcopy(current_source)
    prior_metadata = prior_source["metadata_json"]
    prior_metadata[importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY] = (
        "provider-directory-reviewed-practitioner-role-partition-2026-08-11-v2"
    )
    prior_role = prior_metadata[importer.LAST_UPDATED_PARTITION_METADATA_KEY][
        "resources"
    ]["PractitionerRole"]
    prior_role["end"] = "2026-08-11T00:00:00Z"
    prior_role["maximum_pages_per_window"] = 3
    source_ids = [current_source["source_id"]]
    prior_checkpoint = importer._pagination_checkpoint_context(
        prior_source,
        source_ids,
        run_id="root_prior_generation",
        retry_of_run_id=None,
    )

    assert importer._pagination_checkpoint_scope_identity(
        current_source,
        source_ids,
    ) != importer._pagination_checkpoint_scope_identity(prior_source, source_ids)
    assert importer._twin_root_scope_hash(
        [current_source], EXPECTED_CAMPAIGN, None
    ) != importer._twin_root_scope_hash(
        [prior_source],
        prior_metadata[importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY],
        None,
    )
    with pytest.raises(
        RuntimeError,
        match="provider_directory_endpoint_dataset_verification_scope_mismatch",
    ):
        importer._twin_root_scope_hash(
            [current_source], EXPECTED_CAMPAIGN, prior_checkpoint
        )


def test_partition_page_limit_uses_only_bound_metadata(
    monkeypatch,
):
    monkeypatch.setattr(importer, "_max_page_count", lambda: 10_000)
    reviewed_source = _partitioned_source_record()
    assert (
        importer._last_updated_window_hard_page_limit(
            reviewed_source,
            "PractitionerRole",
            3000,
        )
        == 4
    )
    assert (
        importer._last_updated_window_hard_page_limit(
            reviewed_source,
            "PractitionerRole",
            2,
        )
        == 2
    )
    assert (
        importer._last_updated_window_hard_page_limit(
            reviewed_source,
            "Practitioner",
            3000,
        )
        == 3000
    )
    assert (
        importer._last_updated_window_hard_page_limit(
            {
                "canonical_api_base": reviewed_source["canonical_api_base"],
                "metadata_json": {},
            },
            "PractitionerRole",
            3000,
        )
        == 3000
    )


@pytest.mark.parametrize("maximum_pages", (True, 0, 3001))
def test_partition_page_limit_rejects_invalid_metadata(maximum_pages):
    source_record = copy.deepcopy(_partitioned_source_record())
    source_record["metadata_json"][importer.LAST_UPDATED_PARTITION_METADATA_KEY][
        "resources"
    ]["PractitionerRole"]["maximum_pages_per_window"] = maximum_pages

    partition_config, error = importer._last_updated_partition_config(
        source_record,
        "PractitionerRole",
    )

    assert partition_config is None
    assert error == "invalid_config:maximum_pages_per_window_invalid"


@pytest.mark.asyncio
async def test_partition_dispatch_applies_reviewed_resource_timeout(monkeypatch):
    expected_result = object()
    partition_fetch = AsyncMock(return_value=expected_result)
    monkeypatch.setattr(
        importer,
        "_fetch_last_updated_partition_resource_rows",
        partition_fetch,
    )

    actual_result = await importer._fetch_resource_rows(
        _partitioned_source_record(),
        "PractitionerRole",
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=15,
        run_id="run-reviewed-partition",
    )

    assert actual_result is expected_result
    fetch_options = partition_fetch.await_args.args[4]
    assert fetch_options.timeout == 300


def _attached_partition_source_record():
    source_record = _partitioned_source_record()
    importer._attach_provider_directory_endpoint_ids([source_record])
    return source_record


@pytest.mark.asyncio
async def test_persisted_partition_generation_matches_exact_upsert(monkeypatch):
    expected_source = _attached_partition_source_record()
    persisted_source = copy.deepcopy(expected_source)
    query = AsyncMock(return_value=[SimpleNamespace(_mapping=persisted_source)])
    monkeypatch.setattr(importer.db, "all", query)

    await importer._assert_persisted_reviewed_partition_generations([expected_source])

    query.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drift_field", "drift_value"),
    (
        (
            "provider_directory_candidate_status",
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        ),
        ("provider_directory_verification_campaign_id", "older-campaign"),
        ("provider_directory_reviewed_subset_activation_v2", {}),
        ("endpoint_id", "other-endpoint"),
    ),
)
async def test_persisted_partition_generation_rejects_drift(
    monkeypatch,
    drift_field,
    drift_value,
):
    expected_source = _attached_partition_source_record()
    persisted_source = copy.deepcopy(expected_source)
    if drift_field == "endpoint_id":
        persisted_source[drift_field] = drift_value
    else:
        persisted_source["metadata_json"][drift_field] = drift_value
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[persisted_source]),
    )

    with pytest.raises(
        RuntimeError,
        match="reviewed_partition_generation_persistence_mismatch",
    ):
        await importer._assert_persisted_reviewed_partition_generations(
            [expected_source]
        )


@pytest.mark.asyncio
async def test_partition_generation_rejects_protected_expected_metadata(
    monkeypatch,
):
    expected_source = _attached_partition_source_record()
    expected_source["metadata_json"][importer.REVIEWED_ROOT_POLICY_METADATA_KEY] = {}
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[copy.deepcopy(expected_source)]),
    )

    with pytest.raises(
        RuntimeError,
        match="reviewed_partition_generation_persistence_mismatch",
    ):
        await importer._assert_persisted_reviewed_partition_generations(
            [expected_source]
        )


@pytest.mark.asyncio
async def test_partition_generation_readback_rejects_other_campaigns(
    monkeypatch,
):
    source_record = _attached_partition_source_record()
    source_record["metadata_json"][
        importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
    ] = "other-campaign"
    query = AsyncMock(return_value=[copy.deepcopy(source_record)])
    monkeypatch.setattr(importer.db, "all", query)

    with pytest.raises(
        RuntimeError,
        match="reviewed_partition_generation_persistence_mismatch",
    ):
        await importer._assert_persisted_reviewed_partition_generations(
            [source_record]
        )
