# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Adaptive downloads retain cursor tasks while reducing endpoint pressure."""

import asyncio
import importlib
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_fhir_response import request_concurrency_snapshot
from tests.test_process_provider_directory_fhir_unit import (
    _checkpoint_complete_fetch_result,
    _patch_parallel_resource_flow,
    _source_http_test_session,
)
from tests.test_provider_directory_prefetch_controls import source_request_clock

importer = importlib.import_module("process.provider_directory_fhir")
adapters = importlib.import_module("process.provider_directory_generic_source_adapters")
BASE = "https://directory.example.test/fhir"
SUCCESS = (200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 0)


def source(base=BASE):
    """Use the shared state normally inherited by resource-owned source views."""
    return {"api_base": base, "_provider_directory_request_pacing_state": {}}


async def finish(tasks):
    """Always join cancelled cursor tasks before closing their transport."""
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


class BlockedRequests:
    """Expose actual send admission without relying on timing tolerances."""

    def __init__(self, count):
        self.started = [asyncio.Event() for _ in range(count)]
        self.release = [asyncio.Event() for _ in range(count)]
        self.results = {}
        self.admitted = asyncio.Queue()

    async def fetch(self, _source, url, *, timeout):
        index = int(url.rsplit("/", 1)[1])
        self.started[index].set()
        self.admitted.put_nowait(index)
        await self.release[index].wait()
        return self.results.get(index, SUCCESS)

    async def wait(self, indices):
        await asyncio.wait_for(asyncio.gather(*(self.started[index].wait() for index in indices)), 2)

    async def admissions(self, count):
        return await asyncio.wait_for(asyncio.gather(*(self.admitted.get() for _ in range(count))), 2)


def start_fetch(source_record, index, base=BASE):
    """Start a real shared response-gate call on one synthetic resource path."""
    return asyncio.create_task(importer._fetch_source_json_once(source_record, f"{base}/{index}", timeout=1))


@pytest.mark.parametrize(("base", "allowance"), [(BASE, 1), (importer.UHC_PROVIDER_DIRECTORY_BASE, 4)])
async def test_concurrent_load_failures_share_one_reduction_only_for_optum(monkeypatch, base, allowance):
    """An old wave cannot successively halve Optum's allowance while its cursors drain."""
    network = BlockedRequests(12)
    responses = ((503, {}, None, 0), (504, {}, None, 0), (None, {}, "TimeoutError", 0))
    for index in range(8):
        network.results[index] = responses[index % len(responses)]
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    shared_source = source(base)
    assert request_concurrency_snapshot(shared_source) is None
    async with importer._source_http_session_scope(force=True, request_concurrency=8):
        tasks = [start_fetch(dict(shared_source), index, base) for index in range(12)]
        try:
            await network.wait(range(8))
            for index in range(8):
                network.release[index].set()
                assert await tasks[index] == network.results[index]
            await network.wait(range(8, 8 + allowance))
            assert sum(event.is_set() for event in network.started[8:]) == allowance
            snapshot = request_concurrency_snapshot(shared_source)
            if base == BASE:
                assert snapshot is None
            else:
                assert snapshot["limit"] == snapshot["active"] == allowance
                assert snapshot["reductions"] == 1
                assert snapshot["recoveries"] == snapshot["waiters"] == 0
                network.results[8] = (None, {}, "request timed out", 0)
                network.release[8].set()
                assert await tasks[8] == network.results[8]
                snapshot = request_concurrency_snapshot(shared_source)
                assert snapshot["limit"] == 4
                assert snapshot["transient_failures"] == 9
        finally:
            await finish(tasks)


@pytest.mark.parametrize("failure_indices", [(0, 31, 63), range(0, 160, 13)])
async def test_optum_sparse_transient_responses_keep_full_admission(monkeypatch, failure_indices):
    """Intermittent gateway and timeout errors leave the configured allowance available."""
    base = importer.UHC_PROVIDER_DIRECTORY_BASE
    failures = ((502, {}, None, 0), (504, {}, None, 0), (None, {}, "request timed out", 0))
    failure_by_index = {index: failures[position % len(failures)] for position, index in enumerate(failure_indices)}

    async def fetch(_source, url, *, timeout):
        return failure_by_index.get(int(url.rsplit("/", 1)[1]), SUCCESS)

    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", fetch)
    shared_source = source(base)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        for index in range(max(failure_indices) + 2):
            response = await importer._fetch_source_json_once(shared_source, f"{base}/{index}", timeout=1)
            assert response == failure_by_index.get(index, SUCCESS)
    snapshot = request_concurrency_snapshot(shared_source)
    assert snapshot["limit"] == snapshot["capacity"] == 32
    assert snapshot["reductions"] == snapshot["throttled_responses"] == 0
    assert snapshot["transient_failures"] == len(failure_by_index)
    assert snapshot["transient_http_502"] == len(failure_indices[::3])
    assert snapshot["transient_http_504"] == len(failure_indices[1::3])
    assert snapshot["transient_timeout"] == len(failure_indices[2::3])


@pytest.mark.parametrize("failure_indices", [(0, 2, 4, 11), (3, 7, 11)])
async def test_optum_reduces_for_a_high_transient_rate_after_minimum_sample(monkeypatch, failure_indices):
    base = importer.UHC_PROVIDER_DIRECTORY_BASE
    shared_source = source(base)

    async def fetch(_source, url, *, timeout):
        return (503, {}, None, 0) if int(url.rsplit("/", 1)[1]) in failure_indices else SUCCESS

    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", fetch)
    async with importer._source_http_session_scope(force=True, request_concurrency=32):
        for index in range(12):
            await importer._fetch_source_json_once(shared_source, f"{base}/{index}", timeout=1)
            assert request_concurrency_snapshot(shared_source)["limit"] == (32 if index < 11 else 16)
    snapshot = request_concurrency_snapshot(shared_source)
    assert snapshot["reductions"] == 1
    assert snapshot["transient_http_503"] == len(failure_indices)


async def test_optum_recovers_only_after_healthy_time_at_capacity_and_pending_demand(monkeypatch, source_request_clock):
    """Require both sustained utilization and healthy finishes in the current allowance."""
    base = importer.UHC_PROVIDER_DIRECTORY_BASE
    shared_source = source(base)
    network = BlockedRequests(44)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    async with importer._source_http_session_scope(force=True, request_concurrency=2):
        tasks = [start_fetch(dict(shared_source), index, base) for index in range(44)]

        async def complete(index, seconds=0.0, result=SUCCESS):
            source_request_clock.now += seconds
            network.results[index] = result
            network.release[index].set()
            assert await tasks[index] == result
            return request_concurrency_snapshot(shared_source)["limit"]

        async def complete_next(seconds=0.0, result=SUCCESS):
            (index,) = await network.admissions(1)
            return await complete(index, seconds, result)

        try:
            assert await network.admissions(2) == [0, 1]
            assert request_concurrency_snapshot(shared_source) == {
                "capacity": 2,
                "limit": 2,
                "active": 2,
                "waiters": 42,
                "started": 2,
                "completed": 0,
                "request_time_ms": 0,
                "reductions": 0,
                "recoveries": 0,
                "throttled_responses": 0,
                "transient_failures": 0,
            }
            assert await complete(0, 1.0, (429, {}, None, 0)) == 1
            assert await complete(1, 40.0) == 1
            for _ in range(8):
                assert await complete_next(1.0) == 1
            assert await complete_next(40.0, (200, {"resourceType": "OperationOutcome"}, None, 0)) == 1
            assert await complete_next(1.0) == 2
            for _ in range(12):
                assert await complete_next(5.0) == 2
        finally:
            await finish(tasks)
    snapshot = request_concurrency_snapshot(shared_source)
    assert snapshot["limit"] == snapshot["capacity"] == 2
    assert snapshot["active"] == snapshot["waiters"] == 0
    assert snapshot["started"] == snapshot["completed"]
    assert snapshot["reductions"] == snapshot["recoveries"] == 1
    assert snapshot["throttled_responses"] == 1
    assert snapshot["request_time_ms"] >= 90_000


async def test_idle_time_and_uncontended_requests_cannot_raise_optum_allowance(monkeypatch, source_request_clock):
    """Slow individual requests do not by themselves establish demand for another slot."""
    base = importer.UHC_PROVIDER_DIRECTORY_BASE
    shared_source = source(base)
    network = BlockedRequests(12)
    network.results[0] = (429, {}, None, 0)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    async with importer._source_http_session_scope(force=True, request_concurrency=2):
        for index in range(12):
            task = start_fetch(shared_source, index, base)
            try:
                await network.wait([index])
                source_request_clock.now += 40
                network.release[index].set()
                assert await task == network.results.get(index, SUCCESS)
                source_request_clock.now += 60
            finally:
                await finish([task])
    snapshot = request_concurrency_snapshot(shared_source)
    assert snapshot["limit"] == 1 and snapshot["recoveries"] == 0


async def test_optum_delayed_throttle_cuts_a_recovered_limit(monkeypatch, source_request_clock):
    """A pending throttle from a lower allowance still applies after recovery."""
    base = importer.UHC_PROVIDER_DIRECTORY_BASE
    shared_source = source(base)
    network = BlockedRequests(16)
    throttle = (429, {}, None, 0)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    async with importer._source_http_session_scope(force=True, request_concurrency=4):
        tasks = [start_fetch(dict(shared_source), index, base) for index in range(16)]
        try:
            initial_indices = await network.admissions(4)
            network.results[initial_indices[0]] = throttle
            for index in initial_indices:
                network.release[index].set()
                await tasks[index]
            slow, next_index = await network.admissions(2)
            for completed in range(8):
                source_request_clock.now += 4
                network.release[next_index].set()
                assert await tasks[next_index] == SUCCESS
                if completed < 7:
                    (next_index,) = await network.admissions(1)
            assert request_concurrency_snapshot(shared_source)["limit"] == 4
            network.results[slow] = throttle
            network.release[slow].set()
            assert await tasks[slow] == throttle
            snapshot = request_concurrency_snapshot(shared_source)
            assert snapshot["limit"] == snapshot["reductions"] == snapshot["throttled_responses"] == 2
        finally:
            await finish(tasks)


async def test_optum_stale_throttle_preserves_current_failure_window(monkeypatch):
    """A throttle from a higher allowance cannot erase a new failure cluster."""
    base = importer.UHC_PROVIDER_DIRECTORY_BASE
    shared_source = source(base)
    network = BlockedRequests(16)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    async with importer._source_http_session_scope(force=True, request_concurrency=8):
        tasks = [start_fetch(dict(shared_source), index, base) for index in range(16)]

        async def complete(index, response=SUCCESS):
            network.results[index] = response
            network.release[index].set()
            assert await tasks[index] == response
            return request_concurrency_snapshot(shared_source)["limit"]

        try:
            await network.wait(range(8))
            for index in range(3):
                assert await complete(index, (503, {}, None, 0)) == 8
                await network.wait([index + 8])
            assert await complete(3, (503, {}, None, 0)) == 4
            for index in (4, 6, 7, 8, 9, 10):
                assert await complete(index) == 4
            for index in (11, 12, 13):
                await network.wait([index])
                assert await complete(index, (504, {}, None, 0)) == 4
            await network.wait([14])
            assert await complete(5, (429, {}, None, 0)) == 4
            assert await complete(14, (None, {}, "ServerDisconnectedError", 0)) == 2
            snapshot = request_concurrency_snapshot(shared_source)
            assert snapshot["transient_disconnect"] == snapshot["throttled_responses"] == 1
        finally:
            await finish(tasks)


async def test_eight_inflight_requests_drain_before_reduced_allowance_admits_a_sibling(monkeypatch):
    """A throttle must not cancel cursors or let queued sends spend the old limit."""
    network = BlockedRequests(9)
    network.results[0] = (503, {}, None, 0)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    shared_source = source()
    async with importer._source_http_session_scope(force=True, request_concurrency=8):
        tasks = [
            asyncio.create_task(importer._fetch_source_json_once(dict(shared_source), f"{BASE}/{index}", timeout=1))
            for index in range(8)
        ]
        try:
            await network.wait(range(8))
            tasks.append(asyncio.create_task(importer._fetch_source_json_once(shared_source, f"{BASE}/8", timeout=1)))
            await asyncio.sleep(0)
            assert not network.started[8].is_set()
            for index in range(4):
                network.release[index].set()
                await tasks[index]
                await asyncio.sleep(0)
                assert not network.started[8].is_set()
            network.release[4].set()
            await tasks[4]
            await network.wait([8])
            assert all(not task.cancelled() for task in tasks)
        finally:
            await finish(tasks)


@pytest.mark.parametrize(
    ("status", "error", "expected"),
    [
        (429, None, 2),
        (503, None, 2),
        (504, None, 2),
        (None, "TimeoutError", 2),
        (None, "request timed out", 2),
        (200, None, 4),
        (400, None, 4),
        (401, None, 4),
        (403, None, 4),
        (500, None, 4),
        (None, "certificate verification failed", 4),
    ],
)
async def test_only_load_or_timeout_failures_reduce_following_request_admission(monkeypatch, status, error, expected):
    """Authentication and data errors keep their original failure classification."""
    network = BlockedRequests(5)
    network.results[0] = (status, {}, error, 0)
    network.release[0].set()
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    shared_source = source()
    async with importer._source_http_session_scope(force=True, request_concurrency=4):
        assert await importer._fetch_source_json_once(shared_source, f"{BASE}/0", timeout=1) == network.results[0]
        tasks = [
            asyncio.create_task(importer._fetch_source_json_once(dict(shared_source), f"{BASE}/{index}", timeout=1))
            for index in range(1, 5)
        ]
        try:
            await network.wait(range(1, expected + 1))
            await asyncio.sleep(0)
            assert sum(event.is_set() for event in network.started[1:]) == expected
        finally:
            await finish(tasks)


async def test_repeated_throttles_keep_at_least_one_request_slot(monkeypatch):
    """Success can continue at the floor after more throttles than reductions."""
    network = BlockedRequests(7)
    for index in range(5):
        network.results[index] = (429, {}, None, 0)
        network.release[index].set()
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    shared_source = source()
    async with importer._source_http_session_scope(force=True, request_concurrency=8):
        for index in range(5):
            await importer._fetch_source_json_once(shared_source, f"{BASE}/{index}", timeout=1)
        tasks = [
            asyncio.create_task(importer._fetch_source_json_once(shared_source, f"{BASE}/{index}", timeout=1))
            for index in (5, 6)
        ]
        try:
            await network.wait([5])
            await asyncio.sleep(0)
            assert not network.started[6].is_set()
            network.release[5].set()
            assert await tasks[0] == SUCCESS
            await network.wait([6])
        finally:
            await finish(tasks)


@pytest.mark.parametrize("base", [BASE, importer.UHC_PROVIDER_DIRECTORY_BASE])
async def test_cancelled_waiter_and_active_request_return_their_allowances(monkeypatch, base):
    """Both cancellation positions must permit a subsequent request to finish."""
    network = BlockedRequests(3)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    shared_source = source(base)
    async with importer._source_http_session_scope(force=True, request_concurrency=1):
        tasks = [start_fetch(shared_source, 0, base)]
        try:
            await network.wait([0])
            tasks.append(start_fetch(shared_source, 1, base))
            await asyncio.sleep(0)
            tasks[1].cancel()
            tasks[0].cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            assert all(task.cancelled() for task in tasks)
            tasks.append(start_fetch(shared_source, 2, base))
            await network.wait([2])
            network.release[2].set()
            assert await tasks[2] == SUCCESS
            assert not network.started[1].is_set()
        finally:
            await finish(tasks)


async def test_host_reduction_is_shared_by_aliases_without_reducing_an_independent_host(monkeypatch):
    """Origin sharing prevents alias bypass while unrelated endpoints keep working."""
    network = BlockedRequests(8)
    network.results[0] = (429, {}, None, 0)
    network.release[0].set()
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    alias_source = source(BASE + "/alias")
    independent_base = "https://independent.example.test/fhir"
    async with importer._source_http_session_scope(force=True, request_concurrency=4, source_concurrency=2):
        await importer._fetch_source_json_once(source(), f"{BASE}/0", timeout=1)
        tasks = [
            asyncio.create_task(importer._fetch_source_json_once(alias_source, f"{BASE}/alias/{index}", timeout=1))
            for index in range(1, 4)
        ] + [
            asyncio.create_task(
                importer._fetch_source_json_once(source(independent_base), f"{independent_base}/{index}", timeout=1)
            )
            for index in range(4, 8)
        ]
        try:
            await network.wait([1, 2, 4, 5, 6, 7])
            await asyncio.sleep(0)
            assert not network.started[3].is_set()
        finally:
            await finish(tasks)


async def test_distinct_origin_allowances_still_share_the_total_transport_budget(monkeypatch):
    """Creating another origin cannot expand the run's configured HTTP capacity."""
    network = BlockedRequests(8)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    async with importer._source_http_session_scope(force=True, request_concurrency=4):
        tasks = [
            asyncio.create_task(
                importer._fetch_source_json_once(
                    source(f"https://endpoint-{index}.example.test/fhir"),
                    f"https://endpoint-{index}.example.test/fhir/{index}",
                    timeout=1,
                )
            )
            for index in range(8)
        ]
        try:
            await network.wait(range(4))
            await asyncio.sleep(0)
            assert sum(event.is_set() for event in network.started) == 4
            tasks[4].cancel()
            await asyncio.gather(tasks[4], return_exceptions=True)
            network.release[0].set()
            await tasks[0]
            await network.wait([5])
        finally:
            await finish(tasks)


@pytest.mark.parametrize("base", [BASE, importer.UHC_PROVIDER_DIRECTORY_BASE])
async def test_already_reserved_requests_recheck_reduced_allowance_after_pacing(monkeypatch, base):
    """Reservations waiting in pacing must not all send after a sibling halves the limit."""
    network = BlockedRequests(8)
    network.results[0] = (429, {}, None, 0)
    paced, release_pacing = asyncio.Queue(), asyncio.Event()

    async def pace(source_record, *, allow_census_defer=False):
        if source_record.get("_queued_index") is not None:
            paced.put_nowait(source_record["_queued_index"])
            await release_pacing.wait()

    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    monkeypatch.setattr(importer, "_pace_source_request", pace)
    shared_source = source(base)
    async with importer._source_http_session_scope(force=True, request_concurrency=8):
        tasks = [start_fetch(shared_source, 0, base)]
        try:
            assert await network.admissions(1) == [0]
            tasks.extend(start_fetch({**shared_source, "_queued_index": index}, index, base) for index in range(1, 8))
            await asyncio.wait_for(asyncio.gather(*(paced.get() for _ in range(7))), 2)
            network.release[0].set()
            await tasks[0]
            release_pacing.set()
            await network.admissions(4)
            await asyncio.sleep(0)
            assert sum(event.is_set() for event in network.started[1:]) == 4
        finally:
            release_pacing.set()
            await finish(tasks)


async def test_already_reserved_requests_recheck_reduced_allowance_after_total_budget_wait(monkeypatch):
    """A freed total budget cannot revive an origin's superseded larger allowance."""
    network = BlockedRequests(15)
    network.results[7] = (429, {}, None, 0)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    independent_base = "https://independent.example.test/fhir"
    shared_source = source()
    async with importer._source_http_session_scope(force=True, request_concurrency=8):
        tasks = [start_fetch(source(independent_base), index, independent_base) for index in range(7)]
        try:
            await network.admissions(7)
            tasks.append(start_fetch(shared_source, 7))
            assert await network.admissions(1) == [7]
            tasks.extend(start_fetch(shared_source, index) for index in range(8, 15))
            await asyncio.sleep(0)
            network.release[7].set()
            await tasks[7]
            for index in range(7):
                network.release[index].set()
            await asyncio.gather(*tasks[:7])
            await network.admissions(4)
            await asyncio.sleep(0)
            assert sum(event.is_set() for event in network.started[8:]) == 4
        finally:
            await finish(tasks)


@pytest.mark.parametrize("status", [429, 503, 504])
@pytest.mark.parametrize("base", [BASE, importer.UHC_PROVIDER_DIRECTORY_BASE])
async def test_limited_response_delays_sibling_by_retry_after(monkeypatch, source_request_clock, status, base):
    """All concurrency-limiting HTTP statuses preserve the server's cooldown deadline."""
    network = BlockedRequests(2)
    network.results[0] = (status, {importer.SOURCE_RETRY_AFTER_FIELD: "2"}, None, 0)
    for release in network.release:
        release.set()
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", network.fetch)
    shared_source = source(base)
    async with importer._source_http_session_scope(force=True, request_concurrency=4):
        await importer._fetch_source_json_once(shared_source, f"{base}/0", timeout=1)
        assert source_request_clock.now == 10.0
        assert await importer._fetch_source_json_once(dict(shared_source), f"{base}/1", timeout=1) == SUCCESS
    assert source_request_clock.now == 12.0
    assert source_request_clock.delays == [2.0]


@pytest.mark.parametrize("status", [429, 503, 504])
async def test_optum_oversized_retry_after_caps_immediate_retry(monkeypatch, source_request_clock, status):
    base = importer.UHC_PROVIDER_DIRECTORY_BASE
    response = (status, {importer.SOURCE_RETRY_AFTER_FIELD: "9" * 400}, None, 0)
    fetch = AsyncMock(side_effect=[response, SUCCESS])
    advance_clock = asyncio.sleep

    async def bounded_sleep(delay):
        assert 0 <= delay <= 600
        await advance_clock(delay)

    monkeypatch.setattr(asyncio, "sleep", bounded_sleep)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", fetch)
    async with importer._source_http_session_scope(force=True, request_concurrency=4):
        assert await importer._fetch_source_json(source(base), f"{base}/PractitionerRole", timeout=1) == SUCCESS
    assert fetch.await_count == 2
    assert source_request_clock.delays == [600.0]


class SourceWaveHarness:
    """Separate resource-owned views from one bounded storage writer budget."""

    def __init__(self):
        self.resources = tuple(importer.DEFAULT_RESOURCES)
        self.foundations = set(self.resources) - {"PractitionerRole"}
        self.started, self.completed, self.source_views = set(), set(), []
        self.downloads_ready, self.writers_busy, self.release_writes = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.writer_slots = asyncio.Semaphore(2)
        self.active_writers = self.maximum_writers = 0

    async def persist(self, _model, rows, **_options):
        """Queue resource writes within an independent two-writer allowance."""
        async with self.writer_slots:
            self.active_writers += 1
            self.maximum_writers = max(self.maximum_writers, self.active_writers)
            if self.active_writers == 2:
                self.writers_busy.set()
            try:
                await self.release_writes.wait()
                return len(rows)
            finally:
                self.active_writers -= 1

    async def fetch(self, source_view, resource, **options):
        """Keep each foundation's private cursor view until its batch commits."""
        self.source_views.append(source_view)
        source_view["_test_cursor"] = resource
        if resource == "PractitionerRole":
            assert self.completed == self.foundations
        else:
            self.started.add(resource)
            if self.started == self.foundations:
                self.downloads_ready.set()
            await self.downloads_ready.wait()
        assert source_view["_test_cursor"] == resource
        model = importer.RESOURCE_MODELS_BY_TYPE[resource]
        count = await options["row_batch_handler"](model, [{"source_id": "source_a", "resource_id": resource}])
        self.completed.add(resource)
        return _checkpoint_complete_fetch_result(model, count)


async def test_source_wave_opens_seven_cursor_views_before_two_bounded_writers_finish(monkeypatch):
    """Download overlap does not require increasing the storage write allowance."""
    wave = SourceWaveHarness()
    source_record = _patch_parallel_resource_flow(monkeypatch, wave.fetch, wave.persist)
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(adapters, "resource_scan_estimates", AsyncMock(return_value={}))
    with _source_http_test_session():
        task = asyncio.create_task(
            importer._import_resources(
                [source_record],
                resources=list(wave.resources),
                per_resource_limit=0,
                page_limit=0,
                page_count=100,
                timeout=3,
                run_id="run_1",
                stream_batch_size=100,
                source_concurrency=1,
                resource_scan_concurrency=8,
                is_pagination_checkpointing_enabled=True,
                defer_typed_materialization=True,
            )
        )
        try:
            await asyncio.wait_for(wave.downloads_ready.wait(), 2)
            await asyncio.wait_for(wave.writers_busy.wait(), 2)
            assert len(wave.source_views) == 7 and len({id(view) for view in wave.source_views}) == 7
            assert wave.maximum_writers == 2 and not wave.completed
            wave.release_writes.set()
            await asyncio.wait_for(task, 3)
        finally:
            wave.release_writes.set()
            await finish([task])
    assert wave.completed == set(wave.resources)
    assert wave.maximum_writers == 2
