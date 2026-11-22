# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent chain successes survive a sibling HTTP failure."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_network_first_intake as intake


def test_settle_keeps_successful_pages_and_raises_failure():
    async def succeed(value):
        await asyncio.sleep(0)
        return value

    async def fail():
        raise ValueError("upstream failed")

    commit = AsyncMock()
    with pytest.raises(ValueError, match="upstream failed"):
        asyncio.run(intake._settle([succeed("first"), fail(), succeed("third")], commit))
    assert [call.args for call in commit.await_args_list] == [(0, "first"), (2, "third")]


def test_settle_runs_independent_requests_concurrently():
    counts_by_state = {"active": 0, "peak": 0}

    async def request():
        counts_by_state["active"] += 1
        counts_by_state["peak"] = max(counts_by_state["peak"], counts_by_state["active"])
        await asyncio.sleep(0)
        counts_by_state["active"] -= 1
        return {}

    commit = AsyncMock()
    asyncio.run(intake._settle([request() for _ in range(intake.CONCURRENCY)], commit))
    assert counts_by_state["peak"] == 8 and commit.await_count == 8


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_during_commit", [False, True])
async def test_completed_page_commits_before_slow_sibling_and_cancellation_joins_it(cancel_during_commit):
    committed = asyncio.Event()
    sibling_closed = asyncio.Event()
    never = asyncio.Event()
    saved_pages = []

    async def slow():
        try:
            await never.wait()
        finally:
            await asyncio.sleep(0)
            sibling_closed.set()

    async def ready():
        return "ready"

    async def commit(index, page):
        saved_pages.append((index, page))
        committed.set()
        if cancel_during_commit:
            await never.wait()

    settlement = asyncio.create_task(intake._settle([slow(), ready()], commit))
    try:
        await asyncio.wait_for(committed.wait(), 1)
        assert saved_pages == [(1, "ready")]
        assert not settlement.done()
    finally:
        settlement.cancel()
        with pytest.raises(asyncio.CancelledError):
            await settlement
    assert sibling_closed.is_set()


@pytest.mark.asyncio
async def test_commit_failure_still_drains_successful_siblings():
    saved_pages = []

    async def commit(index, page):
        if index == 0:
            raise ValueError("persistence failed")
        saved_pages.append((index, page))

    with pytest.raises(ValueError, match="persistence failed"):
        await intake._settle([asyncio.sleep(0, "first"), asyncio.sleep(0, "second")], commit)
    assert saved_pages == [(1, "second")]


def test_disabled_source_does_not_select_special_acquisition():
    directory = SimpleNamespace(_source_metadata=lambda row: row.get("metadata_json", {}))
    assert intake.is_source(directory, {"source_id": "ordinary"}) is False
    with pytest.raises(ValueError, match="source_mismatch"):
        intake.is_source(
            directory, {"source_id": "ordinary", "metadata_json": {"provider_directory_network_snapshot": {}}}
        )


@pytest.mark.parametrize(
    "requested,source_cap,stream_cap,expected", [(1, 8, 8, 1), (8, 3, 8, 3), (8, 8, 2, 2), (20, 20, 20, 8)]
)
def test_requested_source_and_shared_stream_caps(requested, source_cap, stream_cap, expected):
    directory = SimpleNamespace(
        _validated_resource_scan_concurrency=int,
        _source_resource_scan_concurrency=lambda source: source_cap,
        _SOURCE_HTTP_SESSION=SimpleNamespace(
            get=lambda: SimpleNamespace(connector=SimpleNamespace(limit=stream_cap, limit_per_host=stream_cap))
        ),
    )
    assert intake.concurrency_limit(directory, {}, requested) == expected


@pytest.mark.asyncio
async def test_peak_observes_admitted_sends_and_survives_a_failed_request():
    from process import provider_directory_source_selection as selection

    counts_by_state = {"active": 0, "peak": 0}
    source_by_field = {"_http_concurrency_observation": counts_by_state}
    both_sending = asyncio.Event()
    release = asyncio.Event()

    async def fetch(_session, url, **_options):
        if counts_by_state["active"] == 2:
            both_sending.set()
        await release.wait()
        if url == "failed":
            raise TimeoutError
        return "ok"

    directory = SimpleNamespace(
        _SOURCE_HTTP_PINNED_ANONYMOUS_SESSION=SimpleNamespace(get=object),
        _fetch_json_with_source_session=fetch,
    )
    tasks = [
        asyncio.create_task(selection.fetch_same_collection(directory, source_by_field, url, timeout=1))
        for url in ("first", "failed")
    ]
    try:
        await asyncio.wait_for(both_sending.wait(), 1)
        assert counts_by_state == {"active": 2, "peak": 2}
    finally:
        release.set()
        fetch_results = await asyncio.gather(*tasks, return_exceptions=True)
    assert fetch_results[0] == "ok" and isinstance(fetch_results[1], TimeoutError)
    assert await selection.fetch_same_collection(directory, source_by_field, "last", timeout=1) == "ok"
    assert counts_by_state == {"active": 0, "peak": 2}
