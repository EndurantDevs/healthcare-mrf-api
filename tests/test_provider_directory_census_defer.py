# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable census deferral must avoid sleeping through resource deadlines or replaying terminal pages."""

import asyncio
import copy
import dataclasses
import datetime
import hashlib
import ssl
from unittest.mock import AsyncMock

import pytest

from tests.test_process_provider_directory_fhir_unit import (
    _caresource_checkpoint_context,
    _install_caresource_scan_callbacks,
    _last_updated_partition_test_window,
    importer,
    unit_checkpoint_catalog,
    unit_source_factory,
)
from tests.test_provider_directory_capped_partition import capped_source
from tests.test_provider_directory_leaf_census import census_clock, retry_source


@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.parametrize("mode", ["leaf", "opaque"])
@pytest.mark.parametrize("fetch_name", ["_fetch_last_updated_partition_count", "_fetch_caresource_census_count"])
@pytest.mark.parametrize("status", [429, 503])
@pytest.mark.parametrize("deadline_seconds", [None, 30])
@pytest.mark.asyncio
async def test_real_common_count_loop_defers_without_sleeping(
    monkeypatch, census_clock, retry_after, mode, fetch_name, status, deadline_seconds
):
    source_by_field = retry_source(mode)
    fetch = AsyncMock(return_value=(status, {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}, None, 1))
    sleep = AsyncMock(side_effect=AssertionError("count retry must be durably deferred"))
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    count_result = await getattr(importer, fetch_name)(
        source_by_field,
        source_by_field["api_base"] + "/Practitioner?_summary=count",
        timeout=90,
        deadline_at=importer.time.monotonic() + deadline_seconds if deadline_seconds is not None else None,
    )
    assert count_result.transient and count_result.retry_not_before == "2025-01-01T01:00:00Z"
    assert count_result.error == f"http_{status}"
    fetch.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.fixture
def queued_deadline(monkeypatch):
    started, cancelled = asyncio.Event(), asyncio.Event()
    timeouts = []

    async def queued_pacing(_source, *, allow_census_defer=False):
        assert allow_census_defer
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    async def expire_remaining_budget(awaitable, timeout):
        timeouts.append(timeout)
        task = asyncio.create_task(awaitable)
        await started.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise TimeoutError

    unpaced = AsyncMock(side_effect=AssertionError("queued request must not reach the origin"))
    monkeypatch.setattr(importer, "_pace_source_request", queued_pacing)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", unpaced)
    monkeypatch.setattr(importer.asyncio, "wait_for", expire_remaining_budget)
    return unpaced, timeouts, cancelled


@pytest.mark.parametrize("fetch_name", ["_fetch_last_updated_partition_count", "_fetch_caresource_census_count"])
@pytest.mark.parametrize("mode", ["opaque", "capped"])
@pytest.mark.asyncio
async def test_logical_count_deadline_cancels_queued_pacing(census_clock, queued_deadline, fetch_name, mode):
    unpaced, timeouts, cancelled = queued_deadline
    source_by_field = capped_source() if mode == "capped" else retry_source(mode)
    source_by_field["_provider_directory_request_pacing_state"] = {
        "retry_after_monotonic": importer.time.monotonic() + 3600
    }
    count_result = await getattr(importer, fetch_name)(
        source_by_field,
        importer._last_updated_partition_url(
            source_by_field["api_base"] + "/Practitioner",
            _last_updated_partition_test_window(),
            page_count=1,
            count_only=True,
        ),
        timeout=90,
        deadline_at=importer.time.monotonic() + 30,
    )
    assert count_result.transient and "resource deadline" in count_result.error
    assert count_result.retry_not_before == ("2025-01-01T00:05:00Z" if mode == "capped" else "2025-01-01T01:00:00Z")
    if fetch_name == "_fetch_last_updated_partition_count":
        assert count_result.observation is None
    assert cancelled.is_set() and len(timeouts) == 1 and 0 < timeouts[0] <= 30
    unpaced.assert_not_awaited()


async def fetch_test_window(source_by_field, *, deadline_seconds=30):
    window = _last_updated_partition_test_window()
    request_url = importer._last_updated_partition_url(
        source_by_field["api_base"] + "/Practitioner", window, page_count=1, count_only=False
    )
    return await importer._fetch_last_updated_partition_window(
        source_by_field,
        "Practitioner",
        request_url,
        window,
        timeout=90,
        cancel_ctx=None,
        cancel_task=None,
        deadline_at=importer.time.monotonic() + deadline_seconds if deadline_seconds is not None else None,
    )


@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.parametrize("mode", ["leaf", "opaque"])
@pytest.mark.asyncio
async def test_actual_window_loop_preserves_long_server_delay(monkeypatch, census_clock, retry_after, mode):
    fetch = AsyncMock(return_value=(503, {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}, None, 1))
    sleep = AsyncMock(side_effect=AssertionError("window retry must be durably deferred"))
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    window_result = await fetch_test_window(retry_source(mode))
    assert not window_result.complete and window_result.transient and window_result.error == "http_503"
    assert window_result.retry_not_before == "2025-01-01T01:00:00Z"
    fetch.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_window_logical_deadline_cancels_queued_pacing(census_clock, queued_deadline):
    unpaced, timeouts, cancelled = queued_deadline
    source_by_field = retry_source("leaf")
    source_by_field["_provider_directory_request_pacing_state"] = {
        "retry_after_monotonic": importer.time.monotonic() + 3600
    }
    window_result = await fetch_test_window(source_by_field)
    assert not window_result.complete and window_result.transient and "resource deadline" in window_result.error
    assert window_result.retry_not_before == "2025-01-01T01:00:00Z" and not window_result.resources
    assert cancelled.is_set() and len(timeouts) == 1 and 0 < timeouts[0] <= 30
    unpaced.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_capped_origin_timeout_retains_legacy_split_contract(monkeypatch):
    fetch = AsyncMock(return_value=(None, None, "TimeoutError: origin read timed out", 1))
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    source_by_field = capped_source()
    request_url = importer._last_updated_partition_url(
        source_by_field["api_base"] + "/Practitioner",
        _last_updated_partition_test_window(),
        page_count=1,
        count_only=True,
    )
    count_result = await importer._fetch_last_updated_partition_count(source_by_field, request_url, timeout=90)
    assert count_result.observation == importer.CountObservation.split("timeout")
    assert not count_result.transient and fetch.await_count == importer._source_fetch_retry_attempts()


@pytest.mark.asyncio
async def test_opaque_resource_deadline_includes_its_initial_census(monkeypatch, census_clock):
    base = "https://directory.example.test/fhir"
    source_by_field = {
        "source_id": "synthetic-source",
        "api_base": base,
        "metadata_json": {"provider_directory_opaque_cursor_census": True},
    }
    fetch = AsyncMock(return_value=(503, {importer.SOURCE_RETRY_AFTER_FIELD: "3600"}, None, 1))
    wait_for = AsyncMock(wraps=importer.asyncio.wait_for)
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch)
    monkeypatch.setattr(importer.asyncio, "wait_for", wait_for)
    monkeypatch.setattr(
        importer,
        "_load_or_initialize_pagination_checkpoint",
        AsyncMock(return_value=importer.PaginationResumeState(base + "/PractitionerRole", 0, 0, ())),
    )
    fetch_result = await importer._fetch_resource_rows(
        source_by_field,
        "PractitionerRole",
        per_resource_limit=0,
        page_limit=0,
        timeout=90,
        deadline_seconds=30,
        page_count=100,
        run_id="run_test",
        pagination_checkpoint=_caresource_checkpoint_context(),
        row_batch_handler=AsyncMock(),
        retain_rows=False,
    )
    assert not fetch_result.complete and fetch_result.retry_not_before == "2025-01-01T01:00:00Z"
    fetch.assert_awaited_once()
    wait_for.assert_awaited_once()
    assert 0 < wait_for.await_args.kwargs["timeout"] <= 30


def terminal_resume():
    request_url = "https://directory.example.test/fhir?_getpages=opaque%2f+value&_getpagesoffset=1"
    proof_by_field = importer.fhir_response.opaque_terminal_proof(
        importer,
        {"strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION, "pre_count": 2},
        request_url,
        2,
        2,
    )
    return importer.PaginationResumeState(request_url, 2, 2, (), completeness=proof_by_field)


@pytest.mark.parametrize("corruption", ["hash", "rows", "pages", "next", "extra", "candidate"])
@pytest.mark.asyncio
async def test_terminal_receipt_refuses_changed_witness_or_candidate(monkeypatch, corruption):
    resume = terminal_resume()
    receipt_by_field = resume.completeness["terminal_page"]
    if corruption == "hash":
        receipt_by_field["request_sha256"] = "0" * 64
    elif corruption in {"rows", "pages"}:
        receipt_by_field[f"{corruption}_processed"] = True
    elif corruption == "next":
        receipt_by_field["has_next"] = True
    elif corruption == "extra":
        receipt_by_field["unknown"] = 1
    monkeypatch.setattr(
        importer, "_caresource_unique_candidate_count", AsyncMock(return_value=1 if corruption == "candidate" else 2)
    )
    fetch = AsyncMock(side_effect=AssertionError("invalid terminal receipt must not fetch"))
    monkeypatch.setattr(importer, "_fetch_source_json_once", fetch)
    census_result = await importer._prepare_caresource_pre_census(
        retry_source("opaque"),
        "PractitionerRole",
        importer.ProviderDirectoryPractitionerRole,
        _caresource_checkpoint_context(),
        resume,
        "https://directory.example.test/fhir/PractitionerRole",
        timeout=3,
    )
    assert (
        isinstance(census_result, importer.ResourceFetchResult)
        and "terminal_" in census_result.error
        and not census_result.next_url_remaining
    )
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_count_defer_resumes_without_replaying_committed_terminal_rows(
    monkeypatch, unit_source_factory, unit_checkpoint_catalog, census_clock
):
    base = "https://directory.example.test/fhir"
    start_url = base + "/PractitionerRole?_format=json&_count=100"
    terminal_url = base + "?_getpages=opaque%2f+value&_getpagesoffset=1&_count=100"
    source_by_field = {
        "source_id": "synthetic-source",
        "api_base": base,
        "endpoint_practitioner_role": start_url,
        "metadata_json": {"provider_directory_opaque_cursor_census": True},
    }
    requests, checkpoints, proofs, _handler = _install_caresource_scan_callbacks(monkeypatch, start_url, terminal_url)
    original_fetch = importer._fetch_source_json
    committed_ids = []

    async def fetch(source_record, request_url, *, timeout):
        if "_summary=count" in request_url and len(requests) == 3:
            requests.append(request_url)
            return 503, {importer.SOURCE_RETRY_AFTER_FIELD: "3600"}, None, 1
        return await original_fetch(source_record, request_url, timeout=timeout)

    async def write(_model, rows):
        committed_ids.extend(row["resource_id"] for row in rows)
        return len(rows)

    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    context = dataclasses.replace(_caresource_checkpoint_context(), canonical_api_base=base)
    fetch_options_by_field = dict(
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run_retry",
        row_batch_handler=write,
        row_batch_size=1000,
        retain_rows=False,
        pagination_checkpoint=context,
    )
    first = await importer._fetch_resource_rows(source_by_field, "PractitionerRole", **fetch_options_by_field)
    assert not first.complete and first.retry_not_before == "2025-01-01T01:00:00Z"
    assert checkpoints[-1]["next_url"] == terminal_url and checkpoints[-1]["rows_processed"] == 2
    assert proofs[-1]["terminal_page"]["request_sha256"] == hashlib.sha256(terminal_url.encode()).hexdigest()
    resume = importer.PaginationResumeState(terminal_url, 2, 2, (), completeness=copy.deepcopy(proofs[-1]))
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=resume))
    census_clock["now"] += datetime.timedelta(days=3)
    second = await importer._fetch_resource_rows(source_by_field, "PractitionerRole", **fetch_options_by_field)
    assert second.complete and second.rows_fetched == 2 and second.rows_written == 0 and second.pages_fetched == 2
    assert committed_ids == ["role-1", "role-2"] and requests.count(terminal_url) == 1
    assert requests[-1] == importer._caresource_census_url(start_url) and checkpoints[-1]["next_url"] is None


@pytest.mark.parametrize("mode", ["leaf", "opaque"])
@pytest.mark.asyncio
async def test_explicit_census_forces_verified_bounded_session(monkeypatch, mode):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    source_by_field = retry_source(mode)
    async with importer.source_selection.verified_session_scope(importer, [source_by_field], request_concurrency=8):
        session = importer._SOURCE_HTTP_SESSION.get()
        assert session.connector.limit == session.connector.limit_per_host == 8
        assert session.connector._ssl.check_hostname and session.connector._ssl.verify_mode == ssl.CERT_REQUIRED
    assert session.closed and importer._SOURCE_HTTP_SESSION.get() is None
    assert not importer.source_selection.requires_verified_session(importer, retry_source("legacy"))


@pytest.mark.parametrize("mode", ["leaf", "opaque"])
@pytest.mark.parametrize(
    "suffix",
    ["?_getpages=opaque%2f+value", "/?_getpages=opaque%252f+value", "/Practitioner?_cursor=opaque%2f+value"],
)
def test_census_cursor_scope_preserves_issued_query_bytes(mode, suffix):
    base = "https://directory.example.test/fhir/provider/R4"
    source_by_field = retry_source(mode)
    source_by_field.update(api_base=base, canonical_api_base=base)
    source_by_field["metadata_json"]["provider_directory_supported_resources"] = ["Practitioner"]
    issued_url = base + suffix
    assert importer._resolved_fhir_next_url(source_by_field, base + "/Practitioner", issued_url) == issued_url


@pytest.mark.parametrize("mode", ["leaf", "opaque"])
@pytest.mark.parametrize(
    "next_url",
    [
        "https://directory.example.test/fhir/patient/R4/Patient?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4/Patient?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4/Claim?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4/Practitioner/nested?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4/%2e%2e/Patient?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4/%252e%252e/Patient?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4/Practitioner%2fPatient?_cursor=opaque",
        "https://foreign.example.test/fhir/provider/R4/Practitioner?_cursor=opaque",
        "http://directory.example.test/fhir/provider/R4/Practitioner?_cursor=opaque",
        "https://directory.example.test:444/fhir/provider/R4/Practitioner?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4/Location?_cursor=opaque",
        "https://directory.example.test/fhir/provider/R4?_type=Patient",
    ],
)
def test_census_cursor_refuses_other_api_and_resource_paths(mode, next_url):
    base = "https://directory.example.test/fhir/provider/R4"
    source_by_field = retry_source(mode)
    source_by_field.update(api_base=base, canonical_api_base=base)
    source_by_field["metadata_json"].update(
        provider_directory_supported_resources=["Practitioner"],
        provider_directory_pagination_allowed_hosts=["foreign.example.test"],
    )
    with pytest.raises(ValueError, match="untrusted_pagination_link"):
        importer._resolved_fhir_next_url(source_by_field, base + "/Practitioner", next_url)


def test_legacy_cursor_host_policy_is_unchanged():
    base = "https://directory.example.test/fhir/provider/R4"
    source_by_field = {"api_base": base}
    next_url = "https://directory.example.test/another-public-directory/Practitioner?_cursor=opaque"
    assert importer._resolved_fhir_next_url(source_by_field, base + "/Practitioner", next_url) == next_url


@pytest.mark.parametrize("mode", ["leaf", "opaque", "legacy", "current-version"])
def test_retained_origin_cooldown_only_extends_explicit_census(monkeypatch, census_clock, mode):
    source_by_field = retry_source("opaque" if mode == "current-version" else mode)
    source_by_field["_provider_directory_request_pacing_state"] = {
        "retry_after_monotonic": importer.time.monotonic() + 3600
    }
    monkeypatch.setattr(
        importer, "current_version_census_contract", lambda _source: object() if mode == "current-version" else None
    )
    boundary = importer.fhir_response.retained_census_retry_boundary(importer, source_by_field, "2025-01-01T00:01:00Z")
    assert boundary == ("2025-01-01T01:00:00Z" if mode in {"leaf", "opaque"} else "2025-01-01T00:01:00Z")


@pytest.fixture
def queued_census_sibling(monkeypatch):
    async def run_queued_request(source_by_field, retry_after, queued_fetch):
        started, release, queued = asyncio.Event(), asyncio.Event(), asyncio.Event()
        source_by_field["_provider_directory_request_pacing_state"] = {}

        async def origin(*_args, **_kwargs):
            started.set()
            await release.wait()
            return 503, {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}, None, 1

        unpaced = AsyncMock(side_effect=origin)
        monkeypatch.setattr(importer, "_fetch_source_json_unpaced", unpaced)
        sleep = AsyncMock(side_effect=AssertionError("shared census cooldown must not sleep"))
        monkeypatch.setattr(importer.asyncio, "sleep", sleep)
        sibling = asyncio.create_task(
            importer._fetch_source_json(source_by_field, source_by_field["api_base"] + "/Location", timeout=90)
        )
        tasks = [sibling]
        try:
            await asyncio.wait_for(started.wait(), 2)
            condition = source_by_field["_provider_directory_request_pacing_state"]["request_slots"].condition
            original_wait = condition.wait

            async def wait_for_slot():
                queued.set()
                return await original_wait()

            monkeypatch.setattr(condition, "wait", wait_for_slot)
            tasks.append(asyncio.create_task(queued_fetch()))
            await asyncio.wait_for(queued.wait(), 2)
            release.set()
            sibling_result, queued_result = await asyncio.gather(*tasks)
            assert sibling_result[:3] == (503, {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}, None)
            unpaced.assert_awaited_once()
            sleep.assert_not_awaited()
            return queued_result
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return run_queued_request


@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.parametrize("mode", ["leaf", "opaque"])
@pytest.mark.parametrize(
    "fetch_name", ["_fetch_last_updated_partition_count", "_fetch_caresource_census_count", "window"]
)
@pytest.mark.asyncio
async def test_unbounded_queued_census_defers_without_origin_send(
    census_clock, queued_census_sibling, retry_after, mode, fetch_name
):
    source_by_field = retry_source(mode)

    async def queued_fetch():
        if fetch_name == "window":
            return await fetch_test_window(source_by_field, deadline_seconds=None)
        return await getattr(importer, fetch_name)(
            source_by_field, source_by_field["api_base"] + "/Practitioner?_summary=count", timeout=90
        )

    queued_result = await queued_census_sibling(source_by_field, retry_after, queued_fetch)
    assert queued_result.transient and queued_result.retry_not_before == "2025-01-01T01:00:00Z"
    assert queued_result.error == importer.source_selection.CENSUS_REQUEST_DEFERRED_ERROR
    if fetch_name != "_fetch_caresource_census_count":
        assert queued_result.pages_fetched == 0


@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.asyncio
async def test_opaque_queued_page_retains_cursor_and_shared_boundary(
    monkeypatch, census_clock, queued_census_sibling, retry_after
):
    source_by_field = {
        "source_id": "synthetic-source",
        "api_base": "https://directory.example.test/fhir",
        "metadata_json": {"provider_directory_opaque_cursor_census": True},
    }
    issued_url = source_by_field["api_base"] + "?_getpages=opaque%2f+value&_getpagesoffset=2"
    resume = importer.PaginationResumeState(
        issued_url,
        2,
        2,
        (),
        completeness={"strategy_version": importer.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION, "pre_count": 3},
    )
    monkeypatch.setattr(importer, "_load_or_initialize_pagination_checkpoint", AsyncMock(return_value=resume))
    write = AsyncMock(side_effect=AssertionError("queued request cannot write provider rows"))

    async def queued_fetch():
        return await importer._fetch_resource_rows(
            source_by_field,
            "Practitioner",
            per_resource_limit=0,
            page_limit=0,
            deadline_seconds=0,
            page_count=1,
            timeout=90,
            run_id="run_test",
            row_batch_handler=write,
            retain_rows=False,
            pagination_checkpoint=dataclasses.replace(
                _caresource_checkpoint_context(), canonical_api_base=source_by_field["api_base"]
            ),
        )

    queued_result = await queued_census_sibling(source_by_field, retry_after, queued_fetch)
    assert queued_result.error == importer.source_selection.CENSUS_REQUEST_DEFERRED_ERROR
    assert not queued_result.complete and queued_result.next_url_remaining
    assert queued_result.retry_not_before == "2025-01-01T01:00:00Z"
    assert queued_result.pages_fetched == 2 and queued_result.rows_fetched == 2
    assert queued_result.rows_written == 0 and queued_result.source_fetch_elapsed_ms == 0
    assert resume.next_url == issued_url
    diagnostic = importer._terminal_source_fetch_diagnostic(source_by_field, issued_url)
    assert diagnostic["status"] is None and diagnostic["retry_count"] == 0 and diagnostic["elapsed_ms"] == 0
    write.assert_not_awaited()


@pytest.mark.parametrize("mode", ["leaf", "opaque"])
@pytest.mark.asyncio
async def test_census_already_pacing_observes_new_shared_cooldown(monkeypatch, census_clock, mode):
    source_by_field = retry_source(mode)
    source_by_field["metadata_json"]["provider_directory_request_interval_seconds"] = 1
    pacing_by_field = {"_provider_directory_last_request_monotonic": 100.0}
    source_by_field["_provider_directory_request_pacing_state"] = pacing_by_field
    clock_by_field = {"monotonic": 100.0}
    monkeypatch.setattr(importer.time, "monotonic", lambda: clock_by_field["monotonic"])

    async def interval_sleep(seconds):
        assert seconds == 1
        clock_by_field["monotonic"] += seconds
        pacing_by_field["retry_after_monotonic"] = clock_by_field["monotonic"] + 3600

    sleep = AsyncMock(side_effect=interval_sleep)
    unpaced = AsyncMock(side_effect=AssertionError("pacing cooldown must prevent the origin send"))
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", unpaced)
    count_result = await importer._fetch_last_updated_partition_count(
        source_by_field, source_by_field["api_base"] + "/Practitioner?_summary=count", timeout=90
    )
    assert count_result.transient and count_result.retry_not_before == "2025-01-01T01:00:00Z"
    assert count_result.pages_fetched == 0
    sleep.assert_awaited_once_with(1.0)
    unpaced.assert_not_awaited()


@pytest.mark.parametrize("status", [423, 429])
@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.asyncio
async def test_opaque_cooldown_does_not_sleep_census_server_boundary(monkeypatch, census_clock, status, retry_after):
    source_by_field = retry_source("opaque")
    prior_result = status, {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}, None, 1
    sleep = AsyncMock(side_effect=AssertionError("opaque retry must retain the durable boundary"))
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    cooldown_result = await importer._retry_rest_pagination_after_cooldown(
        source_by_field,
        source_by_field["api_base"] + "?_getpages=opaque%2f+value",
        prior_result,
        timeout=90,
        deadline_at=None,
    )
    assert cooldown_result.fetch_result == prior_result and cooldown_result.retries == 0
    assert not cooldown_result.recovered and cooldown_result.wait_seconds == 0
    sleep.assert_not_awaited()
