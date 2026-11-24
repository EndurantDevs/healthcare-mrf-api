# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only a sustained Optum outage should pause a shared resource scan wave."""

import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_fhir_response as response
from process import provider_directory_optum_outage as outage
from tests.test_provider_directory_prefetch_controls import source_request_clock

importer = importlib.import_module("process.provider_directory_fhir")
BASE = importer.UHC_PROVIDER_DIRECTORY_BASE
PROBE = f"{BASE}/InsurancePlan?_count=1"
CURSOR = f"{BASE}?_getpages=opaque%2Btoken&_getpagesoffset=100"
ERROR = (500, {"resourceType": "OperationOutcome"}, None, 1)
PAGE = (200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 1)
GOOD_PROBE = (
    200,
    {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": {"resourceType": "InsurancePlan", "id": "example"}}],
    },
    None,
    1,
)


@pytest.fixture
def shared_source(monkeypatch):
    monkeypatch.setattr(importer, "_fetch_source_json", importer._fetch_source_json_once)
    return {"api_base": BASE, "_provider_directory_request_pacing_state": {}}


def search_request(source_record, index, *, resource_type="Organization", current_url=None):
    parameter = "family" if resource_type == "Practitioner" else "name"
    start = f"{BASE}/{resource_type}?_count=100&{parameter}=A{index}"
    return SimpleNamespace(
        source_record=source_record,
        resource_type=resource_type,
        partition_start_url=start,
        current_url=current_url or start,
        is_residual_only=False,
        fetch_options=SimpleNamespace(timeout=14, deadline_at=None, cancel_ctx=None, cancel_task=None),
    )


async def fetch_partition(request):
    cursor_key = f"{request.resource_type}:{request.partition_start_url}"
    scope = importer.source_proxy.PARTITION_CURSOR_KEY
    token = scope.set(cursor_key)
    try:
        return await response.fetch_partition_response(importer, request)
    finally:
        scope.reset(token)


async def seed_failed_partitions(shared_source):
    for index in range(outage.OUTAGE_SAMPLE_SIZE - 1):
        assert (await fetch_partition(search_request(shared_source, index)))[0] in outage.HTTP_OUTAGE_STATUSES


@pytest.mark.parametrize(
    "successful_probe",
    [GOOD_PROBE, (200, {"resourceType": "Bundle", "type": "searchset", "total": 0}, None, 1), PAGE],
    ids=["one-match", "omitted-entry", "empty-entry"],
)
async def test_outage_holds_other_family_for_single_exact_probe(
    monkeypatch, shared_source, source_request_clock, successful_probe
):
    probe_started = asyncio.Event()
    finish_probe = asyncio.Event()
    sibling_started = asyncio.Event()
    requests = []
    sibling = search_request(dict(shared_source), 100, resource_type="Practitioner")
    active = search_request(dict(shared_source), 19, current_url=CURSOR)

    async def send(_source_record, url, *, timeout):
        requests.append((url, timeout, importer.source_proxy.PARTITION_CURSOR_KEY.get()))
        if url == PROBE:
            probe_started.set()
            await finish_probe.wait()
            return successful_probe
        if url == sibling.current_url:
            sibling_started.set()
        return PAGE if finish_probe.is_set() else ERROR

    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", send)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        await seed_failed_partitions(shared_source)
        tasks = [asyncio.create_task(fetch_partition(active))]
        try:
            await asyncio.wait_for(probe_started.wait(), 1)
            tasks.append(asyncio.create_task(fetch_partition(sibling)))
            await asyncio.sleep(0)
            assert not sibling_started.is_set()
            assert response.request_concurrency_snapshot(shared_source)["outage_active"] == 1
            finish_probe.set()
            assert await asyncio.wait_for(asyncio.gather(*tasks), 1) == [PAGE, PAGE]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    assert [sent for sent in requests if sent[0] == PROBE] == [(PROBE, 10, "optum-outage-probe")]
    cursor_sends = [sent for sent in requests if sent[0] == CURSOR]
    assert len(cursor_sends) == 2 and cursor_sends[0] == cursor_sends[1]
    assert source_request_clock.delays.count(outage.PROBE_DELAYS[0]) == 1
    snapshot = response.request_concurrency_snapshot(shared_source)
    assert snapshot["outage_active"] == 0 and snapshot["outage_pauses"] == snapshot["outage_probes"] == 1
    assert snapshot["http_500_responses"] == outage.OUTAGE_SAMPLE_SIZE


@pytest.mark.parametrize("bundle_fields", [{"entry": None}, {"entry": [], "total": 1}, {"total": False}])
def test_malformed_empty_probe_cannot_reopen_scan(bundle_fields):
    probe = (200, {**PAGE[1], **bundle_fields}, None, 1)
    assert not outage._is_healthy_search(probe)


async def test_failed_probes_back_off_until_valid_insurance_plan(monkeypatch, shared_source, source_request_clock):
    network_failures = [ERROR, (502, {}, None, 1), (503, {}, None, 1), (504, {}, None, 1)]
    throttled = (429, {importer.SOURCE_RETRY_AFTER_FIELD: "30"}, None, 1)
    metadata = (200, {"resourceType": "CapabilityStatement"}, None, 1)
    probe_replies = iter((ERROR, throttled, metadata, GOOD_PROBE))
    backend_ready = asyncio.Event()
    probe_times = []

    async def send(_source_record, url, *, timeout):
        if url == PROBE:
            probe_times.append(source_request_clock.now)
            reply = next(probe_replies)
            if reply == GOOD_PROBE:
                backend_ready.set()
            return reply
        if backend_ready.is_set():
            return PAGE
        return network_failures[int(url.rsplit("=A", 1)[1]) % len(network_failures)]

    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", send)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        await seed_failed_partitions(shared_source)
        assert await fetch_partition(search_request(shared_source, 19, resource_type="Practitioner")) == PAGE

    assert probe_times[1] - probe_times[0] == 300
    assert probe_times[2] - probe_times[1] == 900
    assert probe_times[3] - probe_times[2] == 900
    snapshot = response.request_concurrency_snapshot(shared_source)
    assert snapshot["outage_probes"] == 4 and snapshot["throttled_responses"] == 1


@pytest.mark.parametrize("distinct_partitions,failures", [(1, 20), (20, 17)])
async def test_local_or_sparse_500s_do_not_pause(monkeypatch, shared_source, distinct_partitions, failures):
    send = AsyncMock(side_effect=[ERROR] * failures + [PAGE] * (20 - failures))
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", send)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        for index in range(20):
            await fetch_partition(search_request(shared_source, index % distinct_partitions))
    assert send.await_count == 20
    snapshot = response.request_concurrency_snapshot(shared_source)
    assert snapshot["http_500_responses"] == failures and "outage_pauses" not in snapshot
    assert snapshot["limit"] == snapshot["capacity"] == 32


async def test_outage_wait_respects_resource_deadline(monkeypatch, shared_source):
    send = AsyncMock(return_value=ERROR)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", send)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        await seed_failed_partitions(shared_source)
        last = search_request(shared_source, 19)
        last.fetch_options.deadline_at = importer.time.monotonic() + 0.04
        assert await asyncio.wait_for(fetch_partition(last), 1) == (None, None, "deadline_reached", 0)
    assert send.await_count == 20


async def test_outage_wait_polls_import_cancellation(monkeypatch, shared_source, source_request_clock):
    send = AsyncMock(return_value=ERROR)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", send)
    cancelled = AsyncMock(side_effect=asyncio.CancelledError)
    monkeypatch.setattr(importer, "_raise_if_resource_import_cancelled", cancelled)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        await seed_failed_partitions(shared_source)
        last = search_request(shared_source, 19)
        last.fetch_options.cancel_ctx = {}
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(fetch_partition(last), 1)
    assert send.await_count == 20 and source_request_clock.delays == []
    cancelled.assert_awaited_once_with(last.fetch_options.cancel_ctx, None)


@pytest.mark.parametrize("probe_failure", [asyncio.CancelledError, RuntimeError])
async def test_failed_probe_releases_ownership_after_backoff(
    monkeypatch, shared_source, source_request_clock, probe_failure
):
    probe_times = []

    async def send(_source_record, url, *, timeout):
        if url == PROBE:
            probe_times.append(source_request_clock.now)
            if len(probe_times) == 1:
                raise probe_failure
            return GOOD_PROBE
        return PAGE if len(probe_times) == 2 else ERROR

    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", send)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        await seed_failed_partitions(shared_source)
        with pytest.raises(probe_failure):
            await asyncio.wait_for(fetch_partition(search_request(shared_source, 19)), 1)
        sibling = search_request(dict(shared_source), 100, resource_type="Practitioner")
        assert await asyncio.wait_for(fetch_partition(sibling), 1) == PAGE

    assert probe_times[1] - probe_times[0] == 300
    snapshot = response.request_concurrency_snapshot(shared_source)
    assert snapshot["outage_active"] == 0 and snapshot["outage_probes"] == 2


async def test_known_apostrophe_500s_do_not_open_outage(monkeypatch, shared_source):
    send = AsyncMock(return_value=ERROR)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", send)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        for index in range(20):
            request = search_request(shared_source, index)
            request.partition_start_url += "%27T"
            request.current_url = request.partition_start_url
            assert await fetch_partition(request) == ERROR
    assert send.await_count == 20
    assert "outage_pauses" not in response.request_concurrency_snapshot(shared_source)
