# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock, call

import pytest


importer = importlib.import_module("process.provider_directory_fhir")

COUNT_URL = "https://example.test/fhir/PractitionerRole?_summary=count&_total=accurate"
SOURCE_RECORD = {
    "source_id": "source-synthetic",
    "canonical_api_base": "https://example.test/fhir",
}


def _count_bundle(total: object) -> dict[str, object]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
    }


@pytest.mark.asyncio
async def test_count_envelope_retry_preserves_url_and_accepts_exact_bundle(
    monkeypatch,
):
    fetch = AsyncMock(
        side_effect=(
            (200, {"resourceType": "Bundle", "type": "collection"}, None, 1),
            (200, None, None, 1),
            (200, _count_bundle(17), None, 1),
        )
    )
    sleep = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    count_fetch = await importer._fetch_last_updated_partition_count(
        SOURCE_RECORD,
        COUNT_URL,
        timeout=300,
    )

    assert count_fetch.observation == importer.CountObservation.exact(17)
    assert count_fetch.pages_fetched == 3
    assert count_fetch.error is None
    assert fetch.await_args_list == [
        call(SOURCE_RECORD, COUNT_URL, timeout=300),
        call(SOURCE_RECORD, COUNT_URL, timeout=300),
        call(SOURCE_RECORD, COUNT_URL, timeout=300),
    ]
    assert sleep.await_args_list == [call(0.25), call(0.5)]


@pytest.mark.asyncio
async def test_count_envelope_retry_exhausts_without_fallback(monkeypatch):
    fetch = AsyncMock(return_value=(200, None, None, 1))
    sleep = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    count_fetch = await importer._fetch_last_updated_partition_count(
        SOURCE_RECORD,
        COUNT_URL,
        timeout=300,
    )

    assert count_fetch.observation is not None
    assert count_fetch.observation.kind.value == "unknown"
    assert count_fetch.pages_fetched == 3
    assert count_fetch.error == "non_searchset_count_bundle_retry_exhausted"
    assert fetch.await_count == 3
    assert sleep.await_args_list == [call(0.25), call(0.5)]


@pytest.mark.asyncio
async def test_count_envelope_retry_preserves_later_transport_failure(monkeypatch):
    fetch = AsyncMock(
        side_effect=(
            (200, None, None, 1),
            (503, {}, None, 1),
        )
    )
    sleep = AsyncMock()
    retry_not_before = "2026-08-11T00:00:00Z"
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    monkeypatch.setattr(
        importer,
        "_last_updated_partition_retry_not_before",
        lambda *_args, **_kwargs: retry_not_before,
    )

    count_fetch = await importer._fetch_last_updated_partition_count(
        SOURCE_RECORD,
        COUNT_URL,
        timeout=300,
    )

    assert count_fetch.observation is None
    assert count_fetch.pages_fetched == 2
    assert count_fetch.error == "http_503"
    assert count_fetch.transient is True
    assert count_fetch.retry_not_before == retry_not_before
    assert fetch.await_args_list == [
        call(SOURCE_RECORD, COUNT_URL, timeout=300),
        call(SOURCE_RECORD, COUNT_URL, timeout=300),
    ]
    sleep.assert_awaited_once_with(0.25)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_error"),
    (
        (_count_bundle(True), "count_total_not_exact"),
        (
            {**_count_bundle(1), "entry": [{"resource": {"id": "synthetic"}}]},
            "count_bundle_contains_entries",
        ),
        (
            {
                **_count_bundle(1),
                "link": [{"relation": "next", "url": "https://example.test/next"}],
            },
            "count_bundle_has_next_link",
        ),
    ),
)
async def test_count_envelope_retry_does_not_retry_semantic_rejections(
    monkeypatch,
    payload,
    expected_error,
):
    fetch = AsyncMock(return_value=(200, payload, None, 1))
    sleep = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    count_fetch = await importer._fetch_last_updated_partition_count(
        SOURCE_RECORD,
        COUNT_URL,
        timeout=300,
    )

    assert count_fetch.error == expected_error
    assert count_fetch.pages_fetched == 1
    fetch.assert_awaited_once_with(SOURCE_RECORD, COUNT_URL, timeout=300)
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_count_envelope_retry_does_not_retry_operation_outcome(monkeypatch):
    fetch = AsyncMock(
        return_value=(
            200,
            {"resourceType": "OperationOutcome"},
            importer.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR,
            1,
        )
    )
    sleep = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    count_fetch = await importer._fetch_last_updated_partition_count(
        SOURCE_RECORD,
        COUNT_URL,
        timeout=300,
    )

    assert count_fetch.observation == importer.CountObservation.error(
        importer.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR
    )
    assert count_fetch.pages_fetched == 1
    assert count_fetch.error == importer.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR
    fetch.assert_awaited_once_with(SOURCE_RECORD, COUNT_URL, timeout=300)
    sleep.assert_not_awaited()
