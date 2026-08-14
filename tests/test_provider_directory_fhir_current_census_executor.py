# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Integrated current-version census execution against the shared importer."""

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
    current_version_census_completed_proof,
    current_version_census_initial_proof,
)


importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example.test/fhir"
CUTOFF = "2026-08-01T12:00:00.000000Z"


def _contract(*, resources=("Organization",)):
    return CurrentVersionCensusContract(
        source_id="synthetic-source",
        cutoff=CUTOFF,
        resources=resources,
        expected_nonempty_resources=resources,
        start_urls=tuple(
            (
                resource_type,
                f"{BASE}/{resource_type}?active=true",
            )
            for resource_type in resources
        ),
        continuation_strategy=CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
    )


def _source_record(*, resources=("Organization",)):
    return {
        "source_id": "synthetic-source",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            "provider_directory_supported_resources": list(resources),
            "provider_directory_fully_enumerable_resources": list(resources),
        },
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: _contract(resources=resources),
    }


def _checkpoint_context():
    return importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="run-1",
        retry_of_run_id=None,
        acquisition_root_run_id="run-1",
        dataset_id="dataset-1",
    )


def _organization_bundle(*, next_url=None):
    links = [] if next_url is None else [{"relation": "next", "url": next_url}]
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": links,
        "entry": [
            {
                "fullUrl": f"{BASE}/Organization/org-1",
                "resource": {"resourceType": "Organization", "id": "org-1"},
            }
        ],
    }


def _complete_diagnostic(source_record, resource_type, count):
    contract = source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD]
    initial_proof_by_field = current_version_census_initial_proof(
        contract,
        resource_type,
        count,
        expected_page_count=250,
    )
    return {
        "fetch_mode": CURRENT_VERSION_CENSUS_FETCH_MODE,
        "current_version_census_completeness": (
            current_version_census_completed_proof(
                initial_proof_by_field,
                post_count=count,
                processed_rows=count,
                unique_candidate_rows=count,
                pages_processed=1,
                expected_page_count=250,
                terminal_page_entry_count=count,
            )
        ),
    }


def test_bound_manual_source_is_admitted_without_weakening_generic_gate():
    bound_source = _source_record()
    generic_manual_source_by_field = {
        **bound_source,
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: None,
    }

    assert importer._source_rows_allowed_for_probe([bound_source]) == [bound_source]
    assert importer._source_rows_allowed_for_probe(
        [generic_manual_source_by_field]
    ) == []

    selected_sources, metrics_by_name = importer._select_resource_import_sources(
        [bound_source],
        valid_source_ids={"synthetic-source"},
        open_only=True,
        include_auth_required=False,
        requested_resource_types=["Organization"],
    )
    assert selected_sources == [bound_source]
    assert metrics_by_name["source_import_skipped_manual_only"] == 0


def test_start_url_and_checkpoint_scope_bind_cutoff_and_contract():
    source_record = _source_record()
    start_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    first_scope = importer._pagination_checkpoint_scope_identity(
        source_record,
        ["synthetic-source"],
    )
    source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD] = _contract(
        resources=("Organization",)
    )
    source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD] = (
        source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD].__class__(
            **{
                **source_record[CURRENT_VERSION_CENSUS_CONTRACT_FIELD].__dict__,
                "cutoff": "2026-08-01T13:00:00.000000Z",
            }
        )
    )
    second_scope = importer._pagination_checkpoint_scope_identity(
        source_record,
        ["synthetic-source"],
    )

    assert "active=true" in start_url
    assert "_lastUpdated=lt2026-08-01T12%3A00%3A00.000000Z" in start_url
    assert "_count=250" in start_url
    assert first_scope is not None and second_scope is not None
    assert first_scope[1] != second_scope[1]


def test_strict_next_url_keeps_raw_cursor_and_canonicalizes_replay_identity():
    source_record = _source_record()
    current_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    assert current_url is not None
    raw_next_url = (
        f"{BASE}?_getpages=opaque&_getpagesoffset=250&_count=250&_pretty=true"
    )

    assert importer._resolved_fhir_next_url(
        source_record,
        current_url,
        raw_next_url,
        resource_type="Organization",
        page_entry_count=250,
        pre_total=500,
    ) == raw_next_url
    assert importer._pagination_url_identity(raw_next_url) == (
        importer._pagination_url_identity(
            f"{BASE}?_pretty=true&_count=250&_getpagesoffset=250&_getpages=opaque"
        )
    )


def test_multiple_next_links_and_wrong_resource_type_fail_closed():
    multiple_links = _organization_bundle(next_url="first")
    multiple_links["link"].append({"relation": "next", "url": "second"})
    with pytest.raises(ValueError, match="next_link_invalid"):
        importer._current_version_census_next_link(multiple_links)

    for malformed_links in ({"next": "not-a-list"}, ["not-an-object"]):
        malformed_bundle = _organization_bundle()
        malformed_bundle["link"] = malformed_links
        with pytest.raises(ValueError, match="next_link_invalid"):
            importer._current_version_census_next_link(malformed_bundle)

    wrong_resource_bundle = _organization_bundle()
    wrong_resource_bundle["entry"][0]["resource"]["resourceType"] = "Location"
    assert importer._current_version_census_bundle_error(
        wrong_resource_bundle,
        "Organization",
    ) == "provider_directory_current_version_census_resource_type_mismatch"


@pytest.mark.asyncio
async def test_census_source_fetch_uses_pinned_no_redirect_transport(monkeypatch):
    source_record = _source_record()
    request_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    assert request_url is not None
    source_session = SimpleNamespace(closed=False)
    anonymous_fetch = AsyncMock(return_value=(200, {}, None, 1))
    monkeypatch.setattr(
        importer,
        "_fetch_json_with_source_session",
        anonymous_fetch,
    )
    session_token = importer._SOURCE_HTTP_SESSION.set(source_session)
    try:
        await importer._fetch_source_json_once(
            source_record,
            request_url,
            timeout=5,
        )
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)

    anonymous_fetch.assert_awaited_once_with(
        source_session,
        request_url,
        timeout=5,
        allow_redirects=False,
        preserve_url_bytes=True,
    )


@pytest.mark.asyncio
async def test_direct_retry_enables_terminal_census_checkpoint_restart(monkeypatch):
    context = importer.PaginationCheckpointContext(
        canonical_api_base=BASE,
        source_scope_hash="a" * 64,
        source_ids=("synthetic-source",),
        owner_run_id="retry-run",
        acquisition_root_run_id="root-run",
        retry_of_run_id="failed-run",
        dataset_id="dataset-1",
    )
    load_checkpoint = AsyncMock(
        side_effect=RuntimeError("stop-after-checkpoint-options")
    )
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        load_checkpoint,
    )

    with pytest.raises(RuntimeError, match="stop-after-checkpoint-options"):
        await importer._fetch_resource_rows(
            _source_record(),
            "Organization",
            per_resource_limit=0,
            page_limit=0,
            page_count=250,
            timeout=3,
            run_id="retry-run",
            row_batch_handler=AsyncMock(return_value=0),
            row_batch_size=100,
            retain_rows=False,
            pagination_checkpoint=context,
        )

    assert load_checkpoint.await_args.kwargs == {
        "allow_terminal_census_restart": True
    }


@pytest.mark.asyncio
async def test_command_census_preflight_runs_before_startup(monkeypatch):
    event_names = []

    async def record_startup(_runtime_context):
        event_names.append("startup")

    async def record_process(_runtime_context, task_by_field):
        event_names.append("process")
        assert task_by_field["provider_directory_census_cutoff"] == CUTOFF
        return {"ok": True}

    async def record_shutdown(_runtime_context):
        event_names.append("shutdown")

    monkeypatch.setattr(importer, "startup", record_startup)
    monkeypatch.setattr(importer, "process_data", record_process)
    monkeypatch.setattr(importer, "shutdown", record_shutdown)
    command_by_field = {
        "provider_directory_acquisition_strategy": (
            "cutoff-bounded-current-version-census"
        ),
        "provider_directory_census_cutoff": CUTOFF,
        "source_ids": ["synthetic-source"],
        "resources": "Organization",
        "import_resources": True,
    }

    assert await importer.run_provider_directory_fhir_command(
        **command_by_field
    ) == {"ok": True}
    assert event_names == ["process", "shutdown"]

    event_names.clear()
    with pytest.raises(ValueError, match="cutoff_timezone_required"):
        await importer.run_provider_directory_fhir_command(
            **{
                **command_by_field,
                "provider_directory_census_cutoff": "2026-08-01T12:00:00",
            }
        )
    assert event_names == []


def _exact_census_fetch_mock(event_names):
    """Return a three-step fetch mock that records the post-count boundary."""

    fetch_responses = iter(
        (
            (
                200,
                {"resourceType": "Bundle", "type": "searchset", "total": 1},
                None,
                1,
            ),
            (200, _organization_bundle(), None, 2),
            (
                200,
                {"resourceType": "Bundle", "type": "searchset", "total": 1},
                None,
                1,
            ),
        )
    )

    async def fetch_page(*_args, **_kwargs):
        response = next(fetch_responses)
        if len(fetch_mock.await_args_list) == 3:
            event_names.append("post-count")
        return response

    fetch_mock = AsyncMock(side_effect=fetch_page)
    return fetch_mock


def _install_exact_census_fetch_harness(monkeypatch, start_url):
    """Install one successful terminal exact-census lifecycle harness."""

    event_names = []
    fetch_mock = _exact_census_fetch_mock(event_names)

    async def save_completeness(*_args, **_kwargs):
        event_names.append("proof")

    async def save_checkpoint(*_args, **kwargs):
        event_names.append("terminal" if kwargs["next_url"] is None else "cursor")

    async def persist_rows(_model, resource_rows):
        event_names.append("rows")
        assert [resource_row["resource_id"] for resource_row in resource_rows] == [
            "org-1"
        ]
        return 1

    monkeypatch.setattr(importer, "_fetch_source_json", fetch_mock)
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(
            return_value=importer.PaginationResumeState(
                next_url=start_url,
                pages_processed=0,
                rows_processed=0,
                recent_url_hashes=(),
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint_completeness",
        save_completeness,
    )
    monkeypatch.setattr(importer, "_save_pagination_checkpoint", save_checkpoint)
    monkeypatch.setattr(
        importer,
        "_caresource_unique_candidate_count",
        AsyncMock(return_value=1),
    )
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
    return event_names, fetch_mock, persist_rows


@pytest.mark.asyncio
async def test_exact_census_persists_page_before_verified_terminal_checkpoint(
    monkeypatch,
):
    """Persist rows and exact proof before one verified terminal checkpoint."""

    source_record = _source_record()
    checkpoint_context = _checkpoint_context()
    start_url = importer._resource_start_url(
        source_record,
        "Organization",
        page_count=250,
    )
    assert start_url is not None
    event_names, fetch_mock, persist_rows = _install_exact_census_fetch_harness(
        monkeypatch,
        start_url,
    )

    fetch_result = await importer._fetch_resource_rows(
        source_record,
        "Organization",
        per_resource_limit=0,
        page_limit=0,
        page_count=250,
        timeout=5,
        run_id="run-1",
        row_batch_handler=persist_rows,
        row_batch_size=100,
        retain_rows=False,
        pagination_checkpoint=checkpoint_context,
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.fetch_mode == CURRENT_VERSION_CENSUS_FETCH_MODE
    assert fetch_result.fetch_diagnostic["verified"] is True
    assert fetch_result.fetch_diagnostic["pre_count"] == 1
    assert fetch_result.fetch_diagnostic["post_count"] == 1
    assert event_names == ["proof", "post-count", "rows", "terminal"]
    requested_urls = [call.args[1] for call in fetch_mock.await_args_list]
    assert "_summary=count" in requested_urls[0]
    assert requested_urls[1] == start_url
    assert requested_urls[2] == requested_urls[0]


def test_cross_resource_proof_rejects_missing_or_zero_expected_collection():
    source_record = _source_record(resources=("Organization", "Location"))
    complete_diagnostic_by_field = _complete_diagnostic(
        source_record,
        "Organization",
        1,
    )
    with pytest.raises(RuntimeError, match="diagnostics_incomplete"):
        importer._validate_current_version_census_diagnostics(
            [source_record],
            {"Organization": complete_diagnostic_by_field},
        )

    with pytest.raises(ValueError, match="expected_nonempty_zero"):
        importer._validate_current_version_census_diagnostics(
            [source_record],
            {
                "Organization": complete_diagnostic_by_field,
                "Location": _complete_diagnostic(
                    source_record,
                    "Location",
                    0,
                ),
            },
        )
