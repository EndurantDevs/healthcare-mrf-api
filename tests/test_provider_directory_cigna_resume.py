# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded re-enumeration of unusable collection continuations."""

import importlib
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from tests.test_process_provider_directory_fhir_unit import (
    _RestPageHarness,
    unit_checkpoint_catalog,
    unit_source_factory,
)

directory = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def resumed_cursor():
    """Represent retained work without any deployed endpoint identity."""
    return directory.PaginationResumeState(
        f"{directory.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_getpages=unusable",
        10,
        1000,
        (),
        resumed=True,
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"has_restart_attempted": True},
        {"resume_state": None},
        {"status_code": 503},
        {"fetch_error": None},
        {"fetch_error": "source_auth_missing"},
        {"request_url": f"{directory.CIGNA_PROVIDER_DIRECTORY_BASE}/Practitioner?_count=100"},
        {"request_url": "https://directory.example.test/fhir/Practitioner?_getpages=unusable"},
        {"is_current_version_census": True},
        {"allow_retained_pagination_reset": False},
    ],
)
def test_outcome_restart_keeps_existing_replay_and_source_fences(resumed_cursor, overrides):
    options_by_name = {
        "status_code": 200,
        "fetch_error": directory.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR,
        "request_url": resumed_cursor.next_url,
        "resume_state": resumed_cursor,
        "has_restart_attempted": False,
    }
    assert directory._should_restart_expired_pagination_checkpoint(**options_by_name)
    assert not directory._should_restart_expired_pagination_checkpoint(**(options_by_name | overrides))
    assert not directory._should_restart_expired_pagination_checkpoint(
        **(options_by_name | {"resume_state": replace(resumed_cursor, resumed=False)})
    )


def _outcome_response(scan):
    """Exercise the normal collection-error classification before replay."""
    payload = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [
            {"resource": {"resourceType": "OperationOutcome", "issue": [{"severity": "fatal", "code": "processing"}]}}
        ],
    }
    return directory._source_fetch_result_with_payload_error(scan.source, scan.urls[-1], (200, payload, None, 5))


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_source_factory", "unit_checkpoint_catalog")
@pytest.mark.parametrize("recovers", [False, True])
async def test_unusable_resumed_cursor_reenumerates_once_and_still_requires_completion(monkeypatch, recovers):
    scan = _RestPageHarness(monkeypatch, prefetch=False)
    scan.resume_state = directory.PaginationResumeState(scan.urls[-1], 10, 1000, (), resumed=True)
    successful_page = scan.responses[scan.urls[-1]]
    failed_page = _outcome_response(scan)
    attempts = []

    async def fetch_page(page_index):
        if page_index == 1:
            attempts.append(page_index)
            scan.responses[scan.urls[-1]] = successful_page if recovers and len(attempts) > 1 else failed_page

    scan.fetch_hook = fetch_page
    reset = AsyncMock()
    monkeypatch.setattr(directory, "_reset_pagination_checkpoint", reset)

    result = await scan.run()

    reset.assert_awaited_once()
    assert reset.await_args.args[:3] == (scan.checkpoint_context, "Practitioner", scan.urls[0])
    assert scan.requests == [scan.urls[-1], scan.urls[0], scan.urls[-1]]
    assert result.complete is recovers
    assert result.rows_written == (2 if recovers else 1)
    assert result.next_url_remaining is not recovers
    assert result.error == (None if recovers else directory.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR)


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_source_factory")
@pytest.mark.parametrize("prefetch", [False, True])
@pytest.mark.parametrize("after_page", [False, True])
@pytest.mark.parametrize("status", [200, 400, 404, 410, 503])
async def test_retained_prefix_is_preserved_when_cursor_replay_is_disabled(
    monkeypatch, request, prefetch, after_page, status
):
    if after_page:
        request.getfixturevalue("unit_checkpoint_catalog")
    scan = _RestPageHarness(monkeypatch, pages=3, prefetch=prefetch)
    scan.checkpoint_context = replace(scan.checkpoint_context, allow_retained_pagination_reset=False)
    scan.resume_state = directory.PaginationResumeState(scan.urls[1], 10, 1000, (), resumed=True)
    failed_index = 2 if after_page else 1
    failure = _outcome_response(scan) if status == 200 else (status, None, None, 5)
    scan.responses[scan.urls[failed_index]] = failure
    reset = AsyncMock()
    monkeypatch.setattr(directory, "_reset_pagination_checkpoint", reset)

    result = await scan.run()

    reset.assert_not_awaited()
    assert scan.requests == scan.urls[1 : failed_index + 1]
    assert not result.complete and result.next_url_remaining
    assert result.error == (directory.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR if status == 200 else f"http_{status}")
    assert result.rows_fetched == 1000 + int(after_page)
    assert result.pages_fetched == 10 + int(after_page)
    assert len(scan.write_attempts) == len(scan.checkpoints) == int(after_page)
    if after_page:
        assert scan.checkpoints[-1]["next_url"] == scan.urls[failed_index]
        assert scan.checkpoints[-1]["rows_processed"] == 1001
    assert result.page_prefetch_consumed == int(prefetch and after_page)


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_source_factory", "unit_checkpoint_catalog")
@pytest.mark.parametrize("prefetch", [False, True])
async def test_disabling_replay_keeps_successful_retained_resume(monkeypatch, prefetch):
    scan = _RestPageHarness(monkeypatch, pages=3, prefetch=prefetch)
    scan.checkpoint_context = replace(scan.checkpoint_context, allow_retained_pagination_reset=False)
    scan.resume_state = directory.PaginationResumeState(scan.urls[1], 10, 1000, (), resumed=True)
    reset = AsyncMock()
    monkeypatch.setattr(directory, "_reset_pagination_checkpoint", reset)

    result = await scan.run()

    assert result.complete and result.rows_fetched == 1002
    assert scan.requests == scan.urls[1:]
    assert scan.checkpoints[-1]["next_url"] is None
    reset.assert_not_awaited()


@pytest.mark.parametrize("value", [None, 0, 1, "false", "true"])
def test_retained_reset_control_is_strict_before_execution(value):
    with pytest.raises(ValueError, match="allow_retained_pagination_reset_invalid"):
        directory.current_version_census_request(
            {"allow_retained_pagination_reset": value}, allowed_resources=directory.DEFAULT_RESOURCES
        )
    with pytest.raises(ValueError, match="allow_retained_pagination_reset_invalid"):
        directory.PaginationCheckpointContext(
            "https://directory.example.test/fhir",
            "scope",
            ("source",),
            "run",
            "run",
            allow_retained_pagination_reset=value,
        )


@pytest.mark.asyncio
async def test_resource_group_carries_no_reset_control_into_checkpoint_context(monkeypatch):
    scan = _RestPageHarness(monkeypatch, prefetch=False)
    prepare = AsyncMock(return_value=None)
    monkeypatch.setattr(directory, "_prepare_endpoint_dataset_candidate", prepare)
    records, candidate = await directory._prepare_resource_import_source_group(
        [scan.source],
        ["Practitioner"],
        run_id="retry",
        retry_of_run_id="parent",
        pagination_root_run_id="root",
        is_checkpointing_enabled=True,
        allow_retained_pagination_reset=False,
    )
    assert candidate is None
    assert records[0]["_pagination_checkpoint_context"].allow_retained_pagination_reset is False
    assert prepare.await_args.kwargs["checkpoint_context"].allow_retained_pagination_reset is False


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_source_factory")
async def test_initial_collection_outcome_never_resets_retained_storage(monkeypatch):
    scan = _RestPageHarness(monkeypatch, prefetch=False)
    scan.responses[scan.urls[0]] = _outcome_response(scan)
    reset = AsyncMock()
    monkeypatch.setattr(directory, "_reset_pagination_checkpoint", reset)

    result = await scan.run()

    assert not result.complete
    assert result.error == directory.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR
    assert scan.requests == [scan.urls[0]]
    reset.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.usefixtures("unit_source_factory")
@pytest.mark.parametrize("prefetch", [False, True])
@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("entry_fields", [{}, {"entry": []}])
async def test_empty_continuation_cannot_complete_retained_acquisition(
    monkeypatch, request, prefetch, resumed, entry_fields
):
    """An empty lost-cursor response must not certify the retained prefix."""
    if not resumed:
        request.getfixturevalue("unit_checkpoint_catalog")
    scan = _RestPageHarness(monkeypatch, prefetch=prefetch)
    if resumed:
        scan.resume_state = directory.PaginationResumeState(scan.urls[-1], 10, 1000, (), resumed=True)
    scan.responses[scan.urls[-1]] = directory._source_fetch_result_with_payload_error(
        scan.source,
        scan.urls[-1],
        (200, {"resourceType": "Bundle", "type": "searchset", "total": 0, **entry_fields}, None, 5),
    )
    reset = AsyncMock()
    monkeypatch.setattr(directory, "_reset_pagination_checkpoint", reset)

    acquisition_result = await scan.run()

    assert not acquisition_result.complete
    assert acquisition_result.error == "provider_directory_ambiguous_empty_continuation"
    assert acquisition_result.next_url_remaining
    assert all(checkpoint["next_url"] is not None for checkpoint in scan.checkpoints)
    if not resumed:
        assert scan.checkpoints
        assert scan.checkpoints[-1]["next_url"] == scan.urls[-1]
        assert acquisition_result.rows_written == 1
    assert scan.requests == ([scan.urls[-1]] if resumed else scan.urls)
    assert acquisition_result.rows_fetched == (1000 if resumed else 1)
    reset.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_continuation_does_not_retry_or_change_page_size(monkeypatch):
    """Leave unusable continuation recovery to an explicit preserved-data decision."""
    source_by_field = {"canonical_api_base": directory.CIGNA_PROVIDER_DIRECTORY_BASE}
    url = f"{directory.CIGNA_PROVIDER_DIRECTORY_BASE}/Location?_count=100&_getpages=unusable"
    fetch_once = AsyncMock(return_value=(200, {"resourceType": "Bundle", "type": "searchset", "total": 0}, None, 5))
    monkeypatch.setattr(directory, "_fetch_source_json_once", fetch_once)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FETCH_ATTEMPTS", "3")

    fetch_result, smaller_page, retries = await directory._fetch_source_json_candidate(
        source_by_field, url, timeout=3, is_last_candidate=False
    )

    assert fetch_result[2] == directory.CIGNA_AMBIGUOUS_EMPTY_CONTINUATION_ERROR
    assert not smaller_page and retries == 0
    fetch_once.assert_awaited_once_with(source_by_field, url, timeout=3)


@pytest.mark.parametrize(
    ("base", "query", "fields"),
    [
        (directory.CIGNA_PROVIDER_DIRECTORY_BASE, "_count=100", {"total": 0}),
        ("https://directory.example.test/fhir", "_getpages=next", {"total": 0}),
        (directory.CIGNA_PROVIDER_DIRECTORY_BASE, "_getpages=next", {}),
        (directory.CIGNA_PROVIDER_DIRECTORY_BASE, "_getpages=next", {"total": 2}),
        (
            directory.CIGNA_PROVIDER_DIRECTORY_BASE,
            "_getpages=next",
            {"total": 1, "entry": [{"resource": {"resourceType": "Endpoint", "id": "endpoint-1"}}]},
        ),
    ],
)
def test_empty_continuation_guard_preserves_other_collection_responses(base, query, fields):
    """Keep initial emptiness and unambiguous existing terminal behavior."""
    source_by_field = {"canonical_api_base": base}
    fetch_result = (200, {"resourceType": "Bundle", "type": "searchset", **fields}, None, 5)

    assert (
        directory._source_fetch_result_with_payload_error(source_by_field, f"{base}/Endpoint?{query}", fetch_result)
        == fetch_result
    )
