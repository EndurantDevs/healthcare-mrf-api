# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed retry boundaries for reviewed current-version traversal requests."""

from __future__ import annotations

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


def _bundle() -> dict[str, object]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [],
    }


def _retry_clock(monkeypatch):
    clock = SimpleNamespace(value=0.0)
    sleep = AsyncMock(
        side_effect=lambda seconds: setattr(
            clock,
            "value",
            clock.value + seconds,
        )
    )
    monkeypatch.setattr(importer.time, "monotonic", lambda: clock.value)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    return clock, sleep


@pytest.mark.asyncio
async def test_one_attempt_primitive_performs_one_transport_call(monkeypatch):
    source_record = _source_record()
    transport = AsyncMock(return_value=(503, {"diagnostics": "private"}, None, 7))
    monkeypatch.setattr(importer, "_fetch_source_json_once", transport)

    result = await importer._fetch_current_version_census_json_once(
        source_record,
        _request_url(),
        timeout=11,
    )

    assert result == (503, {"diagnostics": "private"}, None, 7)
    transport.assert_awaited_once_with(source_record, _request_url(), timeout=11)
    assert importer.SOURCE_FETCH_DIAGNOSTIC_FIELD not in source_record


@pytest.mark.asyncio
async def test_503_recovery_reuses_exact_no_redirect_url_and_clears_diagnostic(
    monkeypatch,
):
    source_record = _source_record()
    source_record[importer.SOURCE_FETCH_DIAGNOSTIC_FIELD] = {"stale": True}
    request_url = f"{_request_url()}&_getpages=opaque%2Bprivate%3D"
    session = SimpleNamespace(closed=False)
    transport = AsyncMock(
        side_effect=[
            (503, {"diagnostics": "private"}, None, 7),
            (200, _bundle(), None, 11),
        ]
    )
    _clock, sleep = _retry_clock(monkeypatch)
    monkeypatch.setattr(importer, "_fetch_json_with_source_session", transport)
    session_token = importer._SOURCE_HTTP_SESSION.set(session)
    try:
        fetch_result = await importer._fetch_source_json(
            source_record,
            request_url,
            timeout=13,
        )
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)

    assert fetch_result == (200, _bundle(), None, 18)
    assert transport.await_args_list == [
        call(
            session,
            request_url,
            timeout=13,
            allow_redirects=False,
            preserve_url_bytes=True,
        ),
        call(
            session,
            request_url,
            timeout=13,
            allow_redirects=False,
            preserve_url_bytes=True,
        ),
    ]
    sleep.assert_awaited_once_with(300.0)
    assert importer.SOURCE_FETCH_DIAGNOSTIC_FIELD not in source_record


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
async def test_retryable_http_exhaustion_is_fixed_and_redacted(
    monkeypatch,
    status_code,
):
    source_record = _source_record()
    request_url = f"{_request_url()}&_getpages=private-cursor"
    response_by_field = {"diagnostics": "private-response"}
    attempt = AsyncMock(return_value=(status_code, response_by_field, None, 7))
    _clock, sleep = _retry_clock(monkeypatch)
    recorder = Mock(wraps=importer._record_terminal_source_fetch_diagnostic)
    generic_attempts = Mock(side_effect=AssertionError("generic policy used"))
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_FETCH_ATTEMPTS", "99")
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)
    monkeypatch.setattr(importer, "_source_fetch_retry_attempts", generic_attempts)
    monkeypatch.setattr(importer, "_record_terminal_source_fetch_diagnostic", recorder)

    fetch_result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        request_url,
        timeout=3,
    )

    assert fetch_result == (status_code, response_by_field, None, 21)
    assert retry_count == 2
    assert attempt.await_count == 3
    assert all(
        attempt_call.args[1] == request_url for attempt_call in attempt.await_args_list
    )
    assert sleep.await_args_list == [call(300.0), call(599.0)]
    generic_attempts.assert_not_called()
    recorder.assert_called_once()
    diagnostic = source_record[importer.SOURCE_FETCH_DIAGNOSTIC_FIELD]
    assert diagnostic["url_hash"] == importer._pagination_url_hash(request_url)
    assert diagnostic["retry_count"] == 2
    assert diagnostic["elapsed_ms"] == 21
    assert "private" not in str(diagnostic)


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [423, 429])
async def test_locked_and_rate_limited_responses_defer_without_inline_retry(
    monkeypatch,
    status_code,
):
    source_record = _source_record()
    response_by_field = {importer.SOURCE_RETRY_AFTER_FIELD: "600"}
    attempt = AsyncMock(return_value=(status_code, response_by_field, None, 7))
    _clock, sleep = _retry_clock(monkeypatch)
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)

    fetch_result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=3,
    )

    assert fetch_result == (status_code, response_by_field, None, 7)
    assert retry_count == 0
    attempt.assert_awaited_once()
    sleep.assert_not_awaited()
    assert source_record[importer.SOURCE_FETCH_DIAGNOSTIC_FIELD]["retry_count"] == 0


@pytest.mark.asyncio
async def test_transport_error_exhaustion_records_aggregate_diagnostic(monkeypatch):
    source_record = _source_record()
    attempt = AsyncMock(return_value=(None, None, "TimeoutError", 5))
    _clock, sleep = _retry_clock(monkeypatch)
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)

    result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=3,
    )

    assert result == (None, None, "TimeoutError", 15)
    assert retry_count == 2
    assert attempt.await_count == 3
    assert sleep.await_args_list == [call(300.0), call(599.0)]
    assert (
        source_record[importer.SOURCE_FETCH_DIAGNOSTIC_FIELD]["response_class"]
        == "transient_transport_error"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 410])
async def test_client_and_gone_responses_are_not_retried(monkeypatch, status_code):
    source_record = _source_record()
    attempt = AsyncMock(return_value=(status_code, {}, None, 4))
    _clock, sleep = _retry_clock(monkeypatch)
    monkeypatch.setattr(importer, "_fetch_current_version_census_json_once", attempt)

    result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=3,
    )

    assert result == (status_code, {}, None, 4)
    assert retry_count == 0
    attempt.assert_awaited_once()
    sleep.assert_not_awaited()
    assert importer.SOURCE_FETCH_DIAGNOSTIC_FIELD not in source_record


@pytest.mark.asyncio
async def test_semantic_http_200_failure_is_not_retried(monkeypatch):
    source_record = _source_record()
    transport = AsyncMock(
        return_value=(
            200,
            {"resourceType": "OperationOutcome", "diagnostics": "private"},
            None,
            6,
        )
    )
    _clock, sleep = _retry_clock(monkeypatch)
    monkeypatch.setattr(importer, "_fetch_source_json_once", transport)

    result, retry_count = await importer._fetch_current_version_census_json(
        source_record,
        _request_url(),
        timeout=3,
    )

    assert result[0] == 200
    assert result[2] == importer.RESOURCE_SEARCH_OPERATION_OUTCOME_ERROR
    assert retry_count == 0
    transport.assert_awaited_once()
    sleep.assert_not_awaited()
    assert importer.SOURCE_FETCH_DIAGNOSTIC_FIELD not in source_record


@pytest.mark.asyncio
async def test_both_current_contract_dispatch_branches_use_the_wrapper(monkeypatch):
    source_record = _source_record()
    payload = _bundle()
    wrapper = AsyncMock(return_value=((200, payload, None, 9), 2))
    monkeypatch.setattr(importer, "_fetch_current_version_census_json", wrapper)

    assert await importer._fetch_source_json(
        source_record,
        _request_url(),
        timeout=3,
    ) == (200, payload, None, 9)
    assert await importer._fetch_source_json_candidate(
        source_record,
        _request_url(),
        timeout=3,
        is_last_candidate=False,
    ) == ((200, payload, None, 9), False, 2)
    assert wrapper.await_count == 2
