# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent bulk work shares bounded transfer and writer capacity."""

import asyncio
import dataclasses
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from process import provider_directory_bulk_parallel as parallel
from tests.test_process_provider_directory_fhir_unit import (
    _parallel_resource_source,
    _patch_resource_scan_storage,
    _resource_concurrency_options,
    _unsafe_resource_scan_options,
)
from tests.test_provider_directory_fhir_bulk_atomicity import _identity

directory = importlib.import_module("process.provider_directory_fhir")


def _source():
    source_by_field = _parallel_resource_source()
    source_by_field["metadata_json"] = {
        "provider_directory_bulk_export_eligible": True,
        "provider_directory_bulk_export_auto_enabled": True,
        "provider_directory_bulk_export_range_resume": True,
        "provider_directory_bulk_export_output_concurrency": 8,
        "provider_directory_request_interval_seconds": 0,
    }
    source_by_field["_provider_directory_request_pacing_state"] = {}
    return source_by_field


@pytest.mark.parametrize(("pool_capacity", "expected"), [(2, 1), (3, 2), (5, 4), (9, 8), (16, 8)])
def test_export_wave_keeps_one_ordinary_connection_reserved(monkeypatch, pool_capacity, expected):
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", str(pool_capacity))
    _patch_resource_scan_storage(monkeypatch, isolated=True)
    options = _resource_concurrency_options(
        requested_concurrency=8, resource_types=tuple(directory.RESOURCE_MODELS_BY_TYPE)[:8]
    )
    source_by_field = _source()
    assert directory._effective_resource_scan_concurrency(source_by_field, options) == expected
    assert "PractitionerRole" in directory.resource_scan_types(
        directory, source_by_field, options.resource_types, options
    )
    for overrides in _unsafe_resource_scan_options():
        assert (
            directory._effective_resource_scan_concurrency(source_by_field, dataclasses.replace(options, **overrides))
            == 1
        )
    source_by_field["metadata_json"]["provider_directory_bulk_export_range_resume"] = False
    assert directory._effective_resource_scan_concurrency(source_by_field, options) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_resources_and_files_share_eight_real_http_streams(cancel):
    source_by_field = _source()
    parallel.prepare_scan(directory, source_by_field, 4)
    reached_limit, release = asyncio.Event(), asyncio.Event()
    counts_by_state = {"active": 0, "maximum": 0, "requests": 0}

    async def serve(request):
        counts_by_state["active"] += 1
        counts_by_state["requests"] += 1
        counts_by_state["maximum"] = max(counts_by_state["maximum"], counts_by_state["active"])
        if counts_by_state["active"] == 8:
            reached_limit.set()
        try:
            await release.wait()
            return web.Response(text=request.path)
        finally:
            counts_by_state["active"] -= 1

    app = web.Application()
    app.router.add_get("/{resource}/{file}", serve)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:

        async def read(resource, output):
            async with parallel.request(
                directory, dict(source_by_field), session, server.make_url(f"/{resource}/{output}")
            ) as response:
                return await response.text()

        tasks = [asyncio.create_task(read(resource, output)) for resource in range(3) for output in range(5)]
        try:
            await asyncio.wait_for(reached_limit.wait(), 3)
            await asyncio.sleep(0.02)
            assert counts_by_state["requests"] == 8
            if cancel:
                for task in tasks:
                    task.cancel()
            release.set()
            responses = await asyncio.gather(*tasks, return_exceptions=cancel)
            assert len(responses) == 15
            if cancel:
                assert all(isinstance(response, asyncio.CancelledError) for response in responses)
            else:
                assert len(set(responses)) == 15
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert counts_by_state["maximum"] == 8
    assert parallel.state(directory, source_by_field)["requests"].active == 0


@pytest.mark.asyncio
async def test_provider_limit_reduces_shared_budget_and_honors_retry_after():
    source_by_field = _source()
    parallel.prepare_scan(directory, source_by_field, 4)
    calls = []

    async def serve(_request):
        calls.append(directory.time.monotonic())
        return web.Response(status=429 if len(calls) == 1 else 200, headers={"Retry-After": "0.04"})

    app = web.Application()
    app.router.add_get("/output", serve)
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        async with parallel.request(directory, source_by_field, session, server.make_url("/output")) as response:
            assert response.status == 200
    assert len(calls) == 2 and calls[1] - calls[0] >= 0.035
    shared = parallel.state(directory, source_by_field)
    assert shared["requests"].limit == 4 and shared["resources"].limit == 2
    assert shared is parallel.state(directory, dict(source_by_field))
    assert parallel.state(directory, _source())["requests"].limit == 8


def test_limit_responses_share_one_reduction_and_extend_the_active_window(monkeypatch):
    source_by_field = _source()
    parallel.prepare_scan(directory, source_by_field, 8)
    shared = parallel.state(directory, source_by_field)
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(directory, "time", SimpleNamespace(monotonic=lambda: clock.now))

    for now, delay, deadline in ((100, "10", 110), (101, "2", 110), (102, "20", 122), (103, "0", 122)):
        clock.now = now
        parallel._throttle(directory, dict(source_by_field), shared, SimpleNamespace(headers={"Retry-After": delay}))
        assert shared["requests"].limit == shared["resources"].limit == 4
        assert source_by_field["_provider_directory_request_pacing_state"]["retry_after_monotonic"] == deadline


@pytest.mark.parametrize("next_response_at", [110.0, 111.0])
def test_expired_retry_after_window_allows_one_further_reduction(monkeypatch, next_response_at):
    source_by_field = _source()
    parallel.prepare_scan(directory, source_by_field, 8)
    shared = parallel.state(directory, source_by_field)
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(directory, "time", SimpleNamespace(monotonic=lambda: clock.now))
    response = SimpleNamespace(headers={"Retry-After": "10"})
    parallel._throttle(directory, source_by_field, shared, response)
    assert shared["requests"].limit == shared["resources"].limit == 4

    clock.now = next_response_at
    for _ in range(3):
        parallel._throttle(directory, dict(source_by_field), shared, response)
        assert shared["requests"].limit == shared["resources"].limit == 2
        assert (
            source_by_field["_provider_directory_request_pacing_state"]["retry_after_monotonic"]
            == next_response_at + 10
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "export_start", "expected_calls"), [(429, True, 3), (503, True, 1), (504, False, 3)]
)
async def test_rejections_are_bounded_and_ambiguous_starts_are_not_retried(status, export_start, expected_calls):
    source_by_field = _source()
    calls = []

    class Session:
        @asynccontextmanager
        async def get(self, url, **options):
            calls.append((url, options))
            yield SimpleNamespace(status=status, headers={"Retry-After": "0"})

    async with parallel.request(
        directory, source_by_field, Session(), "https://example.test/fhir/$export", export_start=export_start
    ) as response:
        assert response.status == status
    assert len(calls) == expected_calls
    assert parallel.state(directory, source_by_field)["requests"].active == 0
    assert parallel.state(directory, source_by_field)["requests"].limit == max(1, 8 // 2**expected_calls)


@pytest.mark.asyncio
async def test_source_batches_are_serial_without_serializing_transfers(monkeypatch):
    source_by_field = _source()
    parallel.prepare_scan(directory, source_by_field, 4)
    monkeypatch.setattr(directory, "is_source_storage_required", lambda: True)
    started, release = asyncio.Event(), asyncio.Event()
    counts_by_state = {"transfers": 0, "writes": 0, "maximum_writes": 0}

    async def output():
        counts_by_state["transfers"] += 1
        if counts_by_state["transfers"] == 8:
            started.set()
        await release.wait()
        async with parallel.write_slot(directory, dict(source_by_field)):
            counts_by_state["writes"] += 1
            counts_by_state["maximum_writes"] = max(counts_by_state["maximum_writes"], counts_by_state["writes"])
            await asyncio.sleep(0)
            counts_by_state["writes"] -= 1

    tasks = [asyncio.create_task(output()) for _ in range(8)]
    try:
        await asyncio.wait_for(started.wait(), 1)
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 1)
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert counts_by_state["transfers"] == 8 and counts_by_state["maximum_writes"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503, 504])
async def test_limited_export_never_falls_back_to_an_uncheckpointed_rest_scan(monkeypatch, status):
    monkeypatch.setattr(directory, "_bulk_http_get_json", AsyncMock(return_value=(status, {}, None, None)))
    release = AsyncMock()
    unknown = AsyncMock(return_value=({}, None, "bulk_export_acceptance_outcome_unknown"))
    monkeypatch.setattr(directory, "_release_bulk_export_reservation", release)
    monkeypatch.setattr(directory, "_fail_unknown_bulk_export_acceptance", unknown)
    checkpoint, _manifest, error = await directory._start_checkpointed_bulk_export(
        object(), _source(), _identity(), timeout=3, ownership_probe=AsyncMock()
    )
    assert checkpoint == {} and error
    if status == 429:
        release.assert_awaited_once()
        unknown.assert_not_awaited()
    else:
        unknown.assert_awaited_once()
        release.assert_not_awaited()


@pytest.mark.asyncio
async def test_timeout_preserves_primary_error_and_releases_transfer_slot():
    source_by_field = _source()

    class Session:
        def get(self, *_args, **_options):
            raise TimeoutError("primary transport failure")

    with pytest.raises(TimeoutError, match="primary transport failure"):
        async with parallel.request(directory, source_by_field, Session(), "https://files.example.org/output"):
            pytest.fail("timed out transport returned a response")
    assert parallel.state(directory, source_by_field)["requests"].active == 0
    assert parallel.state(directory, source_by_field)["requests"].limit == 4
