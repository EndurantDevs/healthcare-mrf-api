# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Clock and durable-defer boundaries for reviewed traversal retries."""

from __future__ import annotations

import asyncio
import datetime
import email.utils
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
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


def _request_url() -> str:
    return _contract().start_url(RESOURCE_TYPE, 250)


def _fake_clock(monkeypatch):
    clock = SimpleNamespace(value=0.0)
    monkeypatch.setattr(importer.time, "monotonic", lambda: clock.value)
    return clock


def _advancing_sleep(monkeypatch, clock):
    def advance_clock(seconds):
        clock.value += seconds

    sleep = AsyncMock(side_effect=advance_clock)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    return sleep


@pytest.mark.parametrize(
    ("retry_after", "retry_index", "waited_seconds", "expected"),
    [
        (None, 0, 0.0, 1.0),
        ("invalid", 0, 0.0, 1.0),
        ("0", 0, 0.0, 1.0),
        ("2", 0, 0.0, 2.0),
        ("3", 0, 0.0, None),
        ("4", 0, 0.0, None),
        (None, 1, 1.0, 1.0),
        ("2", 1, 1.0, None),
    ],
)
def test_retry_after_delta_and_wait_budget_boundaries(
    retry_after,
    retry_index,
    waited_seconds,
    expected,
):
    response_by_field = (
        {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}
        if retry_after is not None
        else None
    )
    assert (
        importer._current_version_census_retry_delay_seconds(
            response_by_field,
            retry_index=retry_index,
            retry_waited_seconds=waited_seconds,
        )
        == expected
    )


def test_retry_after_http_date_obeys_schedule_and_budget():
    current_time = datetime.datetime(2026, 8, 10, 12, 0, tzinfo=datetime.UTC)
    allowed_retry_after = email.utils.format_datetime(
        current_time + datetime.timedelta(seconds=2),
        usegmt=True,
    )
    refused_retry_after = email.utils.format_datetime(
        current_time + datetime.timedelta(seconds=4),
        usegmt=True,
    )

    assert (
        importer._current_version_census_retry_delay_seconds(
            {importer.SOURCE_RETRY_AFTER_FIELD: allowed_retry_after},
            retry_index=0,
            retry_waited_seconds=0.0,
            now_utc=current_time,
        )
        == 2.0
    )
    assert (
        importer._current_version_census_retry_delay_seconds(
            {importer.SOURCE_RETRY_AFTER_FIELD: refused_retry_after},
            retry_index=0,
            retry_waited_seconds=0.0,
            now_utc=current_time,
        )
        is None
    )
    assert (
        importer._transient_source_retry_not_before(
            429,
            {importer.SOURCE_RETRY_AFTER_FIELD: refused_retry_after},
            None,
            retry_count=0,
            max_delay_seconds=None,
            now_utc=current_time,
        )
        == "2026-08-10T12:00:04Z"
    )


@pytest.mark.asyncio
async def test_request_duration_does_not_change_retry_wait_targets(monkeypatch):
    source_record = _source_record()
    clock = _fake_clock(monkeypatch)
    sleep = _advancing_sleep(monkeypatch, clock)
    responses = [
        (503, {}, None, 100),
        (503, {}, None, 100),
        (200, {"resourceType": "Bundle", "type": "searchset"}, None, 100),
    ]

    def complete_attempt(*_args, **_kwargs):
        clock.value += 0.1
        return responses.pop(0)

    attempt = AsyncMock(side_effect=complete_attempt)
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)

    fetch_result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=3,
    )

    assert fetch_result[0] == 200
    assert fetch_result[3] == 300
    assert retry_count == 2
    assert attempt.await_count == 3
    assert sleep.await_args_list == [call(1.0), call(1.0)]


@pytest.mark.asyncio
async def test_slow_timeout_does_not_consume_retry_wait_budget(monkeypatch):
    source_record = _source_record()
    clock = _fake_clock(monkeypatch)
    sleep = _advancing_sleep(monkeypatch, clock)
    responses = [
        (None, None, "SocketTimeoutError", 60_000),
        (None, None, "SocketTimeoutError", 60_000),
        (200, {"resourceType": "Bundle", "type": "searchset"}, None, 100),
    ]

    def complete_attempt(*_args, **_kwargs):
        clock.value += 60.0
        return responses.pop(0)

    attempt = AsyncMock(side_effect=complete_attempt)
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)

    fetch_result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=60,
    )

    assert fetch_result[0] == 200
    assert fetch_result[3] == 120_100
    assert retry_count == 2
    assert attempt.await_count == 3
    assert sleep.await_args_list == [call(1.0), call(1.0)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second_wake_at",
    [
        pytest.param(2.999, id="before-old-deadline"),
        pytest.param(3.0, id="at-old-deadline"),
        pytest.param(3.1, id="after-old-deadline"),
    ],
)
async def test_wall_clock_oversleep_does_not_consume_wait_budget(
    monkeypatch,
    second_wake_at,
):
    source_record = _source_record()
    clock = _fake_clock(monkeypatch)
    wake_times = iter((1.0, second_wake_at))
    sleep = AsyncMock(
        side_effect=lambda _seconds: setattr(
            clock,
            "value",
            next(wake_times),
        )
    )
    attempt = AsyncMock(
        side_effect=[
            (503, {}, None, 7),
            (503, {}, None, 11),
            (200, {"resourceType": "Bundle", "type": "searchset"}, None, 13),
        ]
    )
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)

    fetch_result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=3,
    )

    assert attempt.await_count == 3
    assert retry_count == 2
    assert sleep.await_args_list == [call(1.0), call(1.0)]
    assert fetch_result[0] == 200
    assert importer.SOURCE_FETCH_DIAGNOSTIC_FIELD not in source_record


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_after_kind", ["delta", "date"])
async def test_over_budget_retry_after_stops_before_an_early_request(
    monkeypatch,
    retry_after_kind,
):
    source_record = _source_record()
    _fake_clock(monkeypatch)
    retry_after = "1200"
    if retry_after_kind == "date":
        retry_after = email.utils.format_datetime(
            datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=1200),
            usegmt=True,
        )
    response_by_field = {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}
    attempt = AsyncMock(return_value=(503, response_by_field, None, 7))
    sleep = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    fetch_result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=3,
    )

    assert fetch_result == (503, response_by_field, None, 7)
    assert retry_count == 0
    attempt.assert_awaited_once()
    sleep.assert_not_awaited()


def test_long_retry_after_preserves_full_and_generic_durable_boundaries():
    current_time = datetime.datetime(2026, 8, 10, 12, 0, tzinfo=datetime.UTC)
    response_by_field = {importer.SOURCE_RETRY_AFTER_FIELD: "1200"}

    assert (
        importer._transient_source_retry_not_before(
            429,
            response_by_field,
            None,
            retry_count=0,
            max_delay_seconds=None,
            now_utc=current_time,
        )
        == "2026-08-10T12:20:00Z"
    )
    assert (
        importer._transient_source_retry_not_before(
            429,
            response_by_field,
            None,
            retry_count=0,
            now_utc=current_time,
        )
        == "2026-08-10T12:10:00Z"
    )


@pytest.mark.parametrize(
    "retry_after",
    [
        "1e300",
        "9" * 309,
        email.utils.format_datetime(
            datetime.datetime.max.replace(tzinfo=datetime.UTC, microsecond=0),
            usegmt=True,
        ),
    ],
)
def test_extreme_retry_after_uses_far_future_durable_sentinel(retry_after):
    assert (
        importer._transient_source_retry_not_before(
            503,
            {importer.SOURCE_RETRY_AFTER_FIELD: retry_after},
            None,
            retry_count=0,
            max_delay_seconds=None,
            now_utc=datetime.datetime(2026, 8, 10, 12, 0, tzinfo=datetime.UTC),
        )
        == "9999-12-31T23:59:59Z"
    )


@pytest.mark.parametrize("retry_after", ["Infinity", "NaN"])
def test_nonfinite_non_delta_retry_after_is_ignored(retry_after):
    assert importer._source_retry_after_seconds(
        {importer.SOURCE_RETRY_AFTER_FIELD: retry_after},
        max_delay_seconds=None,
    ) is None


@pytest.mark.asyncio
async def test_current_count_failure_preserves_the_full_durable_defer(monkeypatch):
    source_record = _source_record()
    response_by_field = {importer.SOURCE_RETRY_AFTER_FIELD: "1200"}
    fetch = AsyncMock(return_value=(429, response_by_field, None, 7))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    started_at = datetime.datetime.now(datetime.UTC)

    count_fetch = await importer._fetch_current_version_census_count(
        source_record,
        _request_url(),
        timeout=3,
    )

    retry_at = datetime.datetime.fromisoformat(
        count_fetch.retry_not_before.replace("Z", "+00:00")
    )
    assert count_fetch.count is None
    assert count_fetch.transient is True
    assert count_fetch.error == "http_429"
    assert started_at + datetime.timedelta(seconds=1199) <= retry_at
    fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_current_count_transient_fallback_is_one_second(monkeypatch):
    source_record = _source_record()
    fetch = AsyncMock(return_value=(503, {}, None, 7))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    started_at = datetime.datetime.now(datetime.UTC)

    count_fetch = await importer._fetch_current_version_census_count(
        source_record,
        _request_url(),
        timeout=3,
    )

    retry_at = datetime.datetime.fromisoformat(
        count_fetch.retry_not_before.replace("Z", "+00:00")
    )
    assert count_fetch.count is None
    assert count_fetch.transient is True
    assert started_at <= retry_at
    assert retry_at <= started_at + datetime.timedelta(seconds=2)
    fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancellation_during_retry_wait_propagates(monkeypatch):
    source_record = _source_record()
    _fake_clock(monkeypatch)
    attempt = AsyncMock(return_value=(503, {}, None, 7))
    recorder = Mock()
    sleep = AsyncMock(side_effect=asyncio.CancelledError)
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)
    monkeypatch.setattr(importer, "_record_terminal_source_fetch_diagnostic", recorder)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        await importer._fetch_current_version_census_json(
            source_record,
            _request_url(),
            timeout=3,
        )

    attempt.assert_awaited_once()
    sleep.assert_awaited_once_with(1.0)
    recorder.assert_not_called()
