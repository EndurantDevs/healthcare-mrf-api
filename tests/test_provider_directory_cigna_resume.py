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
