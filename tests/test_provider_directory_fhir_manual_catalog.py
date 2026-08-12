# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Manifest-backed admission tests for manual current-version census sources."""

from __future__ import annotations

import copy
import datetime
import hashlib
import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_fhir_manual_catalog as manual_catalog
from process.provider_directory_fhir_census_binding import (
    bind_current_version_census_contract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD,
    CURRENT_VERSION_CENSUS_START_URLS_FIELD,
    current_version_census_request,
)
from process.provider_directory_fhir_root_policy import (
    POLICY_PENDING_STATUS,
    REVIEWED_ROOT_POLICY_METADATA_KEY,
)


importer = importlib.import_module("process.provider_directory_fhir")
MANIFEST_PATH = manual_catalog.DEFAULT_MANUAL_SOURCE_MANIFEST
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _manifest_document() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _manual_entry(manifest: dict | None = None) -> dict:
    document = manifest or _manifest_document()
    entries = [
        entry
        for entry in document["entries"]
        if entry.get("classification")
        == manual_catalog.MANUAL_ACQUISITION_CLASSIFICATION
    ]
    assert len(entries) == 1
    return entries[0]


def _write_manifest(tmp_path: Path, manifest: dict) -> Path:
    manifest_path = tmp_path / "provider-directory-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True),
        encoding="utf-8",
    )
    return manifest_path


def _load_mutated_manifest(tmp_path: Path, mutate) -> None:
    manifest = copy.deepcopy(_manifest_document())
    mutate(_manual_entry(manifest))
    entry = _manual_entry(manifest)
    manual_catalog.reviewed_manual_census_seed_rows(
        entry["source_ids"][0],
        manifest_path=_write_manifest(tmp_path, manifest),
    )


def _reviewed_subset_task(entry: dict, **overrides) -> dict:
    task_by_field = {
        "provider_directory_acquisition_strategy": (
            "server-issued-traversal-subset"
        ),
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": list(entry["source_ids"]),
        "resources": list(entry["resources"]),
        "import_resources": True,
        "run_id": "manual-source-census-root",
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
    task_by_field.update(overrides)
    return task_by_field


def _clear_catalog_and_credential_environment(monkeypatch) -> None:
    for environment_name in (
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV,
        "HLTHPRT_PROVIDER_DIRECTORY_RETEST_RESULTS_URL",
    ):
        monkeypatch.delenv(environment_name, raising=False)


def test_reviewed_manifest_seed_binds_exact_identity_without_cutoff():
    entry = _manual_entry()
    source_id = entry["source_ids"][0]
    seed_rows = (
        manual_catalog.reviewed_manual_census_seed_rows(
            source_id
        )
    )

    assert len(seed_rows) == 1
    source_record = importer._source_row_from_seed(seed_rows[0])
    assert source_record["source_id"] == source_id
    assert source_record["requires_registration"] is False
    assert source_record["auth_type"] == "none"
    assert source_record["last_validated_status"] is None
    assert source_record["data_quality_flag"] is None
    metadata = source_record["metadata_json"]
    assert metadata["provider_directory_manual_only"] is True
    assert metadata["provider_directory_acquisition_enabled"] is True
    campaign_field = manual_catalog.MANUAL_SOURCE_VERIFICATION_CAMPAIGN_FIELD
    assert metadata[campaign_field] == entry["manual_current_version_census"][
        "verification_campaign_id"
    ]
    assert metadata["provider_directory_supported_resources"] == entry["resources"]
    assert metadata["provider_directory_fully_enumerable_resources"] == []
    assert metadata["provider_directory_server_issued_subset_resources"] == (
        entry["resources"]
    )
    assert metadata["provider_directory_resource_page_count_caps"] == {
        resource_type: entry["manual_current_version_census"]["page_count"]
        for resource_type in entry["resources"]
    }
    assert "provider_directory_census_cutoff" not in metadata
    assert all(
        "_lastUpdated" not in start_url
        for start_url in metadata[CURRENT_VERSION_CENSUS_START_URLS_FIELD].values()
    )
    request = current_version_census_request(
        _reviewed_subset_task(entry),
        allowed_resources=importer.DEFAULT_RESOURCES,
        now=datetime.datetime(2026, 8, 2, tzinfo=datetime.UTC),
    )
    assert request is not None
    contract = bind_current_version_census_contract(request, [source_record])
    assert contract.resources == tuple(entry["resources"])
    assert contract.expected_nonempty_resources == tuple(
        entry["manual_current_version_census"]["expected_nonempty_resources"]
    )
    assert metadata[CURRENT_VERSION_CENSUS_METADATA_STRATEGY_FIELD] == (
        request.strategy.value
    )
    assert metadata[CURRENT_VERSION_CENSUS_CONTINUATION_STRATEGY_FIELD] == (
        entry["manual_current_version_census"]["continuation_strategy"]
    )
    assert metadata[CURRENT_VERSION_CENSUS_START_URLS_FIELD] == entry[
        "manual_current_version_census"
    ]["start_urls"]


def test_reviewed_seed_persists_single_root_policy():
    entry = _manual_entry()
    seed_row = manual_catalog.reviewed_manual_census_seed_rows(
        entry["source_ids"][0],
    )[0]

    metadata = seed_row["metadata_json"]
    assert metadata["provider_directory_candidate_status"] == (
        POLICY_PENDING_STATUS
    )
    assert metadata[REVIEWED_ROOT_POLICY_METADATA_KEY] == {
        "policy_version": "provider-directory-reviewed-root-policy-v1",
        "required_root_count": 1,
    }


def test_reviewed_seed_policy_cannot_be_overridden():
    entry = _manual_entry()

    with pytest.raises(TypeError, match="root_policy"):
        manual_catalog.reviewed_manual_census_seed_rows(
            entry["source_ids"][0],
            root_policy=object(),
        )


def test_reviewed_manual_source_resolves_without_runtime_selector():
    entry = _manual_entry()

    assert manual_catalog.reviewed_manual_census_source_id() == (
        entry["source_ids"][0]
    )


@pytest.mark.parametrize("manual_only", (False, None))
def test_reviewed_subset_manual_only_is_identity_bound(manual_only):
    entry = _manual_entry()
    seed_row = manual_catalog.reviewed_manual_census_seed_rows(
        entry["source_ids"][0]
    )[0]
    source_record = importer._source_row_from_seed(seed_row)
    original_contract = importer._twin_root_source_acquisition_contract(
        source_record
    )
    mutated = copy.deepcopy(source_record)
    metadata = mutated["metadata_json"]
    if manual_only is None:
        metadata.pop("provider_directory_manual_only")
    else:
        metadata["provider_directory_manual_only"] = manual_only

    assert importer._is_reviewed_subset_source_metadata(metadata) is False
    assert (
        importer._twin_root_source_acquisition_contract(mutated)
        != original_contract
    )
    with pytest.raises(
        RuntimeError,
        match="artifact_subset_contract_invalid",
    ):
        importer._artifact_source_with_subset_contract(mutated, CUTOFF)

    identity_sql = importer._artifact_subset_source_identity_sql(
        "source.metadata_json::jsonb"
    )
    assert "provider_directory_manual_only" in identity_sql
    assert "= 'true'::jsonb" in identity_sql


def test_legacy_source_contract_hashes_remain_byte_exact():
    legacy_source_record_by_field = {
        "source_id": "legacy-source",
        "api_base": "https://legacy.example.test/fhir",
        "canonical_api_base": "https://legacy.example.test/fhir",
        "metadata_json": {
            "provider_directory_supported_resources": ["Organization"],
            "provider_directory_fully_enumerable_resources": ["Organization"],
            "provider_directory_expected_nonempty_resources": ["Organization"],
            "provider_directory_resource_page_count_caps": {"Organization": 100},
            "provider_directory_page_count_caps": {"Organization": 100},
            "provider_directory_resource_page_count_cap": 100,
            "provider_directory_acquisition_enabled": True,
            "provider_directory_coverage_mode": "exhaustive",
        },
    }
    twin_contract = importer._twin_root_source_acquisition_contract(
        legacy_source_record_by_field
    )
    artifact_contract = importer._artifact_source_verification_contract(
        legacy_source_record_by_field,
        verification_campaign_id=None,
        verification_source_scope_hash=None,
        source_ids=("legacy-source",),
        completion_proof_cutoff=None,
    )

    def digest(contract):
        identity = importer._stable_identity_json(contract).encode("utf-8")
        return hashlib.sha256(identity).hexdigest()

    assert digest(twin_contract) == (
        "122e90674eb2537f48013790fc1f7f4a324de6c8eeeaf3d54b1f3ab7f46eec35"
    )
    assert digest(artifact_contract) == (
        "268326d29dd8c07f7cbb0b757f258401bea0da796b3ab28c463f044ea2629481"
    )


def test_reviewed_manual_source_is_protected_from_catalog_cleanup():
    entry = _manual_entry()

    protected_source_ids = importer._configured_catalog_protected_source_ids({})
    assert entry["source_ids"][0] in protected_source_ids


def test_manual_manifest_rejects_missing_start_url(tmp_path):
    def mutate(entry):
        start_urls = entry["manual_current_version_census"]["start_urls"]
        start_urls.pop(entry["resources"][0])

    with pytest.raises(RuntimeError, match="start_urls_invalid"):
        _load_mutated_manifest(tmp_path, mutate)


def test_manual_manifest_rejects_empty_expected_nonempty_profile(tmp_path):
    def mutate(entry):
        entry["manual_current_version_census"][
            "expected_nonempty_resources"
        ] = []

    with pytest.raises(RuntimeError, match="expected_nonempty_resources_invalid"):
        _load_mutated_manifest(tmp_path, mutate)


@pytest.mark.parametrize("invalid_page_count", (True, 0, 1001))
def test_manual_manifest_rejects_invalid_page_count(
    tmp_path,
    invalid_page_count,
):
    def mutate(entry):
        entry["manual_current_version_census"]["page_count"] = (
            invalid_page_count
        )

    with pytest.raises(RuntimeError, match="page_count_invalid"):
        _load_mutated_manifest(tmp_path, mutate)


def test_manual_manifest_rejects_persisted_cutoff(tmp_path):
    def mutate(entry):
        entry["manual_current_version_census"][
            "provider_directory_census_cutoff"
        ] = CUTOFF

    with pytest.raises(RuntimeError, match="persisted_cutoff_forbidden"):
        _load_mutated_manifest(tmp_path, mutate)


def test_manual_manifest_rejects_persisted_last_updated_filter(tmp_path):
    def mutate(entry):
        resource_type = entry["resources"][0]
        entry["manual_current_version_census"]["start_urls"][resource_type] += (
            "?_lastUpdated=lt" + CUTOFF
        )

    with pytest.raises(RuntimeError, match="start_urls_invalid"):
        _load_mutated_manifest(tmp_path, mutate)


def test_manual_manifest_rejects_source_identity_drift(tmp_path):
    def mutate(entry):
        entry["manual_current_version_census"]["plan_name"] += " drift"

    with pytest.raises(RuntimeError, match="source_identity_drift"):
        _load_mutated_manifest(tmp_path, mutate)

    manifest = copy.deepcopy(_manifest_document())
    mutate(_manual_entry(manifest))
    with pytest.raises(RuntimeError, match="source_identity_drift"):
        manual_catalog.reviewed_manual_census_source_id(
            manifest_path=_write_manifest(tmp_path, manifest)
        )


class _CatalogResolved(Exception):
    pass


@pytest.mark.asyncio
async def test_subset_process_uses_only_the_reviewed_local_catalog(monkeypatch):
    entry = _manual_entry()
    _clear_catalog_and_credential_environment(monkeypatch)
    monkeypatch.setattr(importer, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_tables",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_raise_if_resource_import_cancelled",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_clear_resource_rows_seen",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_mark_provider_directory_progress",
        AsyncMock(),
    )
    resolver_mock_by_name = {
        resolver_name: Mock(
            side_effect=AssertionError(
                    f"{resolver_name} must not resolve a reviewed subset source"
            )
        )
        for resolver_name in (
            "_resolve_seed_db",
            "_resolve_retest_results",
            "_seed_rows_from_supplemental_catalogs",
            "_add_uhc_official_file_source",
        )
    }
    for resolver_name, resolver_mock in resolver_mock_by_name.items():
        monkeypatch.setattr(importer, resolver_name, resolver_mock)
    source_upsert = AsyncMock(side_effect=_CatalogResolved)
    monkeypatch.setattr(
        importer,
        "_upsert_provider_directory_source_rows",
        source_upsert,
    )

    with pytest.raises(_CatalogResolved):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            _reviewed_subset_task(entry),
        )

    for resolver_mock in resolver_mock_by_name.values():
        resolver_mock.assert_not_called()
    source_records = source_upsert.await_args.args[0]
    assert len(source_records) == 1
    assert source_records[0]["source_id"] == entry["source_ids"][0]
    assert CURRENT_VERSION_CENSUS_CONTRACT_FIELD in source_records[0]


@pytest.mark.asyncio
async def test_subset_process_rejects_arbitrary_seed_database_before_io(
    monkeypatch,
    tmp_path,
):
    entry = _manual_entry()
    _clear_catalog_and_credential_environment(monkeypatch)
    database_setup = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", database_setup)

    with pytest.raises(
        ValueError,
        match="runtime_invalid:local_supplemental_catalog_inputs",
    ):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            _reviewed_subset_task(
                entry,
                seed_db_path=str(tmp_path / "alternate.sqlite"),
            ),
        )

    database_setup.assert_not_awaited()


@pytest.mark.asyncio
async def test_subset_runtime_preflight_precedes_catalog_resolution(
    monkeypatch,
):
    entry = _manual_entry()
    _clear_catalog_and_credential_environment(monkeypatch)
    catalog_loader = Mock()
    database_setup = AsyncMock()
    monkeypatch.setattr(
        importer,
        "reviewed_manual_census_seed_rows",
        catalog_loader,
    )
    monkeypatch.setattr(importer, "ensure_database", database_setup)

    with pytest.raises(ValueError, match="census_runtime_invalid"):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            _reviewed_subset_task(entry, full_refresh=False),
        )

    catalog_loader.assert_not_called()
    database_setup.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_manual_source_rejects_before_database_or_cancellation(
    monkeypatch,
):
    entry = _manual_entry()
    _clear_catalog_and_credential_environment(monkeypatch)
    database_setup = AsyncMock()
    cancellation_check = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", database_setup)
    monkeypatch.setattr(
        importer,
        "_raise_if_resource_import_cancelled",
        cancellation_check,
    )

    with pytest.raises(RuntimeError, match="source_resolution_ambiguous"):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            _reviewed_subset_task(
                entry,
                source_ids=["synthetic-unknown-source"],
            ),
        )

    cancellation_check.assert_not_awaited()
    database_setup.assert_not_awaited()
