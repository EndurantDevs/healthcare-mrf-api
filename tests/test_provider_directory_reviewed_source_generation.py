# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary proof for reviewed source-generation persistence."""

from __future__ import annotations

import copy
import datetime
import importlib
import json
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_fhir_manual_catalog as manual_catalog
from process.provider_directory_fhir_census_binding import (
    bind_current_version_census_contract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    current_version_census_request,
)
from process.provider_directory_fhir_root_policy import (
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)


importer = importlib.import_module("process.provider_directory_fhir")
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _manual_entry() -> dict:
    manifest = json.loads(
        manual_catalog.DEFAULT_MANUAL_SOURCE_MANIFEST.read_text(
            encoding="utf-8"
        )
    )
    entries = [
        entry
        for entry in manifest["entries"]
        if entry.get("classification")
        == manual_catalog.MANUAL_ACQUISITION_CLASSIFICATION
    ]
    assert len(entries) == 1
    return entries[0]


def _reviewed_subset_task(entry: dict) -> dict:
    return {
        "provider_directory_acquisition_strategy": (
            "server-issued-traversal-subset"
        ),
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": list(entry["source_ids"]),
        "resources": list(entry["resources"]),
        "import_resources": True,
        "run_id": "synthetic-reviewed-source-root",
        "full_refresh": True,
        "resource_limit": 0,
        "page_limit": 0,
        "page_count": entry["manual_current_version_census"]["page_count"],
        "stream_batch_size": 5000,
        "source_concurrency": 1,
        "resource_scan_concurrency": 1,
        "linked_resource_limit": 0,
        "linked_resource_deadline_seconds": 0,
        "resource_deadline_seconds": 0,
        "probe": True,
        "bulk_export": False,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "defer_typed_materialization": True,
        "open_only": True,
        "include_auth_required": False,
    }


def _bound_reviewed_subset_source() -> dict:
    entry = _manual_entry()
    seed_row = manual_catalog.reviewed_manual_census_seed_rows(
        entry["source_ids"][0],
    )[0]
    source_record = importer._source_row_from_seed(seed_row)
    request = current_version_census_request(
        _reviewed_subset_task(entry),
        allowed_resources=importer.DEFAULT_RESOURCES,
        now=datetime.datetime(2026, 8, 2, tzinfo=datetime.UTC),
    )
    assert request is not None
    bind_current_version_census_contract(request, [source_record])
    importer._attach_provider_directory_endpoint_ids([source_record])
    return source_record


def _clear_environment(monkeypatch) -> None:
    for environment_name in (
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV,
        "HLTHPRT_PROVIDER_DIRECTORY_RETEST_RESULTS_URL",
    ):
        monkeypatch.delenv(environment_name, raising=False)


@pytest.mark.asyncio
async def test_reviewed_generation_readback_accepts_exact_contract(monkeypatch):
    expected_source = _bound_reviewed_subset_source()
    persisted_source = copy.deepcopy(expected_source)
    persisted_source.pop(CURRENT_VERSION_CENSUS_CONTRACT_FIELD)
    persisted_source["endpoint_id"] = "incumbent-serving-endpoint"
    readback = AsyncMock(return_value=[persisted_source])
    monkeypatch.setattr(importer.db, "all", readback)

    await importer._assert_persisted_reviewed_subset_generations(
        [expected_source]
    )

    readback.assert_awaited_once()


def test_reviewed_generation_accepts_pending_twin_contract_without_policy():
    expected_source = _bound_reviewed_subset_source()
    expected_metadata = expected_source["metadata_json"]
    expected_metadata.pop(REVIEWED_ROOT_POLICY_METADATA_KEY)
    expected_metadata["provider_directory_candidate_status"] = (
        importer.PROVIDER_DIRECTORY_SUBSET_TWIN_ROOT_PENDING
    )
    persisted_source = copy.deepcopy(expected_source)

    assert importer._is_reviewed_subset_generation_exact(
        expected_source,
        persisted_source,
    )


@pytest.mark.parametrize(
    ("field_name", "drift_value"),
    (
        (
            importer.CURRENT_VERSION_CENSUS_STRATEGY_VERSION_FIELD,
            "provider-directory-fhir-server-issued-traversal-subset-v4",
        ),
        (
            importer.CURRENT_VERSION_CENSUS_COMPLETION_SCOPES_FIELD,
            ["source-issued-continuation"],
        ),
        (
            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY,
            "synthetic-older-campaign",
        ),
        (
            importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY,
            "configured-endpoint-drift",
        ),
        (
            "provider_directory_candidate_status",
            importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED,
        ),
        (
            REVIEWED_ROOT_POLICY_METADATA_KEY,
            {
                "policy_version": "provider-directory-reviewed-root-policy-v1",
                "required_root_count": 2,
            },
        ),
        (importer.REVIEWED_SUBSET_ACTIVATION_METADATA_KEY_V2, {}),
    ),
)
def test_reviewed_generation_readback_rejects_contract_drift(
    field_name,
    drift_value,
):
    expected_source = _bound_reviewed_subset_source()
    persisted_source = copy.deepcopy(expected_source)
    persisted_source["metadata_json"][field_name] = drift_value

    assert not importer._is_reviewed_subset_generation_exact(
        expected_source,
        persisted_source,
    )


@pytest.mark.asyncio
async def test_generation_rejection_precedes_source_probe(monkeypatch):
    entry = _manual_entry()
    _clear_environment(monkeypatch)
    for function_name in (
        "ensure_database",
        "_ensure_provider_directory_tables",
        "_raise_if_resource_import_cancelled",
        "_clear_resource_rows_seen",
        "_mark_provider_directory_progress",
    ):
        monkeypatch.setattr(importer, function_name, AsyncMock())
    source_upsert = AsyncMock(
        side_effect=RuntimeError(
            "provider_directory_reviewed_subset_generation_persistence_mismatch"
        )
    )
    source_probe = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_upsert_provider_directory_source_rows",
        source_upsert,
    )
    monkeypatch.setattr(importer, "_run_source_probe_batch", source_probe)

    with pytest.raises(
        RuntimeError,
        match="reviewed_subset_generation_persistence_mismatch",
    ):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            _reviewed_subset_task(entry),
        )

    source_probe.assert_not_awaited()
    assert source_upsert.await_args.kwargs == {
        "require_reviewed_subset_generation": True
    }
