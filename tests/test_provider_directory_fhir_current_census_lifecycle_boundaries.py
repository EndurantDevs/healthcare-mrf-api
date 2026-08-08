# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lifecycle failure boundaries for the current-version census executor."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_FETCH_MODE,
)


importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"
RESOURCE_TYPE = "Organization"


def _contract() -> CurrentVersionCensusContract:
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=(RESOURCE_TYPE,),
        expected_nonempty_resources=(RESOURCE_TYPE,),
        start_urls=((RESOURCE_TYPE, f"{BASE}/{RESOURCE_TYPE}?active=true"),),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _source_record() -> dict[str, object]:
    return {
        "source_id": "synthetic-source",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            "provider_directory_supported_resources": [RESOURCE_TYPE],
            "provider_directory_fully_enumerable_resources": [RESOURCE_TYPE],
        },
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: _contract(),
    }


def _checkpoint_context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="run-1",
        retry_of_run_id=None,
        acquisition_root_run_id="run-1",
        dataset_id="dataset-1",
    )


def _count_bundle(total: object) -> dict[str, object]:
    return {"resourceType": "Bundle", "type": "searchset", "total": total}


def _resource_bundle(*, malformed_link: bool = False) -> dict[str, object]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": "not-a-list" if malformed_link else [],
        "entry": [
            {
                "fullUrl": f"{BASE}/{RESOURCE_TYPE}/org-1",
                "resource": {"resourceType": RESOURCE_TYPE, "id": "org-1"},
            }
        ],
    }


def _pristine_resume() -> importer.PaginationResumeState:
    return importer.PaginationResumeState(
        next_url=_contract().start_url(RESOURCE_TYPE, 250),
        pages_processed=0,
        rows_processed=0,
        recent_url_hashes=(),
    )


async def _acquire(
    monkeypatch: pytest.MonkeyPatch,
    fetch_results: tuple[object, ...],
    *,
    parse_row: bool = False,
) -> tuple[importer.ResourceFetchResult, SimpleNamespace]:
    operation_spies = SimpleNamespace(
        fetch=AsyncMock(side_effect=fetch_results),
        proof=AsyncMock(),
        checkpoint=AsyncMock(),
        rows=AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(return_value=_pristine_resume()),
    )
    monkeypatch.setattr(importer, "_fetch_source_json", operation_spies.fetch)
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        operation_spies.proof,
    )
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint",
        operation_spies.checkpoint,
    )
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        AsyncMock(return_value=1),
    )
    if parse_row:
        monkeypatch.setattr(
            importer,
            "parse_fhir_resource",
            Mock(
                return_value=(
                    importer.ProviderDirectoryOrganization,
                    {"source_id": "synthetic-source", "resource_id": "org-1"},
                )
            ),
        )
    resource_fetch_result = await importer._fetch_resource_rows(
        _source_record(),
        RESOURCE_TYPE,
        per_resource_limit=0,
        page_limit=0,
        page_count=250,
        timeout=3,
        run_id="run-1",
        row_batch_handler=operation_spies.rows,
        row_batch_size=100,
        retain_rows=False,
        pagination_checkpoint=_checkpoint_context(),
    )
    assert resource_fetch_result is not None
    return resource_fetch_result, operation_spies


def _exact_task() -> dict[str, object]:
    return {
        "provider_directory_acquisition_strategy": (
            "cutoff-bounded-current-version-census"
        ),
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": ["synthetic-source"],
        "resources": [RESOURCE_TYPE],
        "import_resources": True,
        "run_id": "run-1",
        "full_refresh": True,
        "resource_limit": 0,
        "page_limit": 0,
        "stream_batch_size": 100,
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


@pytest.mark.asyncio
async def test_malformed_pre_count_stops_before_page_transport(monkeypatch):
    result, spies = await _acquire(
        monkeypatch,
        ((200, _count_bundle(True), None, 1),),
    )
    assert "pre_census_" in result.error
    assert "count_total_invalid" in result.error
    assert result.fetch_mode == CURRENT_VERSION_CENSUS_FETCH_MODE
    spies.rows.assert_not_awaited()
    spies.checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_transient_post_count_retains_failed_proof(monkeypatch):
    result, spies = await _acquire(
        monkeypatch,
        (
            (200, _count_bundle(1), None, 1),
            (200, _resource_bundle(), None, 1),
            (503, {}, None, 1),
        ),
        parse_row=True,
    )
    assert "post_census_http_503" in result.error
    assert result.next_url_remaining is True
    assert result.fetch_diagnostic["verified"] is False
    assert spies.proof.await_count == 2
    assert spies.proof.await_args.args[2]["processed_rows"] == 1


@pytest.mark.asyncio
async def test_invalid_exact_page_shape_stops_before_row_parsing(monkeypatch):
    invalid_page = _resource_bundle()
    invalid_page["type"] = "collection"
    result, spies = await _acquire(
        monkeypatch,
        (
            (200, _count_bundle(1), None, 1),
            (200, invalid_page, None, 1),
        ),
    )
    assert "searchset_required" in result.error
    spies.rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_exact_next_link_stops_before_post_count(monkeypatch):
    result, spies = await _acquire(
        monkeypatch,
        (
            (200, _count_bundle(1), None, 1),
            (200, _resource_bundle(malformed_link=True), None, 1),
        ),
        parse_row=True,
    )
    assert "next_link_invalid" in result.error
    assert spies.fetch.await_count == 2
    spies.rows.assert_awaited_once()


@pytest.mark.asyncio
async def test_exact_environment_catalog_is_rejected_before_database(
    monkeypatch,
):
    for environment_name in (
        importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV,
        importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV,
    ):
        monkeypatch.delenv(environment_name, raising=False)
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_RETEST_RESULTS_URL",
        "https://catalog.example.test/results.json",
    )
    ensure_database = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", ensure_database)
    with pytest.raises(ValueError, match="runtime_invalid:remote_catalog_inputs"):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            _exact_task(),
        )
    ensure_database.assert_not_awaited()
