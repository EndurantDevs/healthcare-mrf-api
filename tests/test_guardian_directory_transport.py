# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check source-wide pacing and the disposition of concurrent finder calls."""

import asyncio
import sqlite3
from contextlib import asynccontextmanager, closing
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import guardian_directory_capture as guardian
from process.ext.source_http_proxy import SourceProxyUnavailable
from process.guardian_directory_store import GuardianCapture
from process.vsp_directory_contract import canonical_json


@pytest.fixture
def open_pool(tmp_path, monkeypatch):
    """Run the ordinary transport with an isolated store and an observable HTTP endpoint."""
    plan = SimpleNamespace(root=tmp_path, max_concurrency=2, requests_per_second=20)
    binding = Mock()
    binding.cooldown_snapshot.return_value = {"cooldowns": []}
    selection = Mock(
        side_effect=[
            SimpleNamespace(proxy_id=route_id, request_options={"proxy": "https://proxy.test"})
            for route_id in ("route-a", "route-b")
        ]
    )
    monkeypatch.setattr(guardian, "_selection", selection)
    monkeypatch.setattr(guardian, "_json_body", AsyncMock(return_value=b'{"ok":true}'))
    pause = AsyncMock()

    @asynccontextmanager
    async def open_transport(response):
        @asynccontextmanager
        async def create_http(**options):
            assert options["connector"].limit == 2
            await options["connector"].close()
            yield SimpleNamespace(request=response)

        monkeypatch.setattr(guardian.aiohttp, "ClientSession", create_http)
        with closing(sqlite3.connect(":memory:")) as db:
            capture = GuardianCapture(db, "example", 10, fresh=True)
            async with guardian._requester(plan, capture, binding, AsyncMock(), pause, AsyncMock()) as request:
                yield request, capture, pause, selection

    return open_transport


async def test_pooled_requests_share_origin_starts_and_exact_bodies(open_pool):
    """Two connections overlap, remain paced, and issue a shared request once."""
    pair_started, release = asyncio.Event(), asyncio.Event()
    starts = []

    @asynccontextmanager
    async def response(method, url, **options):
        starts.append(asyncio.get_running_loop().time())
        if len(starts) == 2:
            pair_started.set()
        await release.wait()
        yield SimpleNamespace(status=200)

    async with open_pool(response) as (request, capture, _, selection):
        calls = [asyncio.create_task(request("search", {"q": term})) for term in ("a", "a", "b")]
        try:
            await asyncio.wait_for(pair_started.wait(), 5)
        finally:
            release.set()
        assert all(reply == {"ok": True} for reply in await asyncio.gather(*calls))
        assert request.stats() == {
            "requested_concurrency": 2,
            "worker_peak_in_flight": 2,
            "worker_joined_requests": 1,
            "worker_distinct_proxy_routes": 2,
        }
        assert starts[1] - starts[0] >= 0.04
        assert await request("search", {"q": "a"}) == await request("search", {"q": "c"})
        assert capture.attempt_count() == 3
        assert selection.call_count == 2 and all(call.args[2] is None for call in selection.call_args_list)


async def test_destination_hold_stops_queued_request_and_keeps_admitted_response(open_pool):
    """A rate limit on one connection fences the pool while its peer retains a success."""
    admitted, limited = asyncio.Event(), asyncio.Event()
    issued_terms = []

    @asynccontextmanager
    async def response(method, url, **options):
        query = options["json"]["q"]
        issued_terms.append(query)
        if query == "admitted":
            admitted.set()
            await asyncio.wait_for(limited.wait(), 5)
            yield SimpleNamespace(status=200)
        else:
            await asyncio.wait_for(admitted.wait(), 5)
            try:
                yield SimpleNamespace(status=429, headers={"Retry-After": "30"})
            finally:
                limited.set()

    with pytest.raises(SourceProxyUnavailable):
        async with open_pool(response) as (request, capture, pause, _):
            outcomes = await asyncio.gather(
                *(request("search", {"q": term}) for term in ("admitted", "limited", "queued")), return_exceptions=True
            )
            assert issued_terms == ["admitted", "limited"]
            assert outcomes[0] == {"ok": True} and all(
                isinstance(error, SourceProxyUnavailable) for error in outcomes[1:]
            )
            assert capture.lookup("search", canonical_json({"q": "admitted"}))
            assert capture.attempt_count() == 2
            raise outcomes[1]
    pause.assert_awaited_once()
