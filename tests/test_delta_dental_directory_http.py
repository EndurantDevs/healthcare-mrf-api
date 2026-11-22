# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Qualify direct finder requests and edition replay with synthetic offices."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import delta_dental_directory_http as transport
from process.delta_dental_directory_capture import FinderCapturePlan, FinderSearch
from process.delta_dental_directory_json import SOURCE_ENDPOINT, finder_payload, parse_finder_json_page
from process.delta_dental_directory_observations import export_retained_capture
from process.delta_dental_directory_worker import _run_http_capture
from process.ext.source_http_proxy import SourceProxyError, SourceProxyUnavailable

PPO = FinderSearch("Delta Dental PPO", "00000", 30)
PATIENT_DIRECT = FinderSearch("Delta Dental Patient Direct", "00000", 30)


def _response(count, label):
    rows = [
        {
            "provider": {"firstName": "Example", "lastName": "Dentist"},
            "providerNPI": "1234567893",
            "providerServiceOfficeId": index + 1,
            "serviceOfficeNPI": "1234567893",
            "serviceOffice": {
                "businessName": "Example Care",
                "serviceOfficeNumber": index + 1,
                "location": {
                    "streetAddress": "1 Example Street",
                    "streetAddressLine2": "Suite A",
                    "city": "Example",
                    "stateType": "MI",
                    "zipCode": "00000",
                },
            },
            "products": [{"productType": label}],
        }
        for index in range(count)
    ]
    return json.dumps({"providerResults": rows}).encode()


class _Response:
    def __init__(self, status, body, headers=None):
        self.status = status
        self.headers = headers or {}
        self.content_type = "application/json"
        self.content_length = len(body)
        self.content = self
        self.body = body

    async def iter_chunked(self, _size):
        yield self.body


@pytest.fixture
def scoped_transport(monkeypatch):
    requests = []
    route = SimpleNamespace(proxy_id="synthetic", request_options={"proxy": "https://proxy.example"})
    binding = Mock()
    binding.cooldown_snapshot.return_value = {"cooldowns": []}
    monkeypatch.setattr(transport, "_selection", Mock(return_value=route))

    @asynccontextmanager
    async def post(url, **options):
        requests.append((url, options))
        yield dispatch.response(options["json"])

    @asynccontextmanager
    async def client(**options):
        assert options["trust_env"] is False
        yield SimpleNamespace(post=post)

    dispatch = SimpleNamespace(
        requests=requests,
        response=lambda payload: _Response(200, _response(1, "Delta Dental PPO")),
        binding=binding,
        pause=AsyncMock(),
    )
    monkeypatch.setattr(transport.aiohttp, "ClientSession", client)
    return dispatch


@pytest.mark.asyncio
async def test_one_post_per_exact_search_and_zero_requests_on_warm_replay(tmp_path, scoped_transport):
    dispatch = scoped_transport
    response_by_network = {
        "PPO": _response(100, PPO.selector),
        "PatientDirect": _response(1, PATIENT_DIRECT.selector),
    }
    dispatch.response = lambda payload: _Response(200, response_by_network[payload["productType"]])
    cold = await _run_http_capture(
        dispatch.binding,
        FinderCapturePlan(tmp_path, "test", "test", (PPO, PATIENT_DIRECT, PPO), plan_id="first"),
        AsyncMock(),
        dispatch.pause,
    )
    warm = await _run_http_capture(
        dispatch.binding,
        FinderCapturePlan(tmp_path, "test", "test", (PATIENT_DIRECT, PPO), plan_id="second"),
        AsyncMock(),
        dispatch.pause,
    )
    assert cold["transport"]["origin_requests"] == 2
    assert warm["transport"]["origin_requests"] == 0
    assert cold["gaps"] == warm["gaps"] == {"result_cap_reached": 1}
    assert (
        cold["membership_observations"]
        == warm["membership_observations"]
        == {
            PPO.selector: 100,
            PATIENT_DIRECT.selector: 1,
        }
    )
    assert cold["directory_complete"] is warm["directory_complete"] is False
    for (url, options), search in zip(dispatch.requests, (PPO, PATIENT_DIRECT), strict=True):
        assert url == SOURCE_ENDPOINT
        assert options["json"] == finder_payload(search)
        assert options["json"]["address"] == "00000"
        assert options["json"]["websiteMc"] == "DDPA"
        assert options["json"]["maximumNumberOfRecordsToReturn"] == 100
        assert options["allow_redirects"] is False
        assert options["proxy"] == "https://proxy.example"
    output = tmp_path / "private.ndjson"
    assert export_retained_capture(tmp_path / "finder-test.sqlite3", output)["card_observations"] == 101
    first = json.loads(output.read_text().splitlines()[0])
    assert first["provider_office_id"] == "1"
    assert first["practitioner_npi"] == first["office_npi"] == "1234567893"
    assert first["address"] == "1 Example Street, Suite A, Example, MI 00000"


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "expected"), [(403, SourceProxyError), (429, SourceProxyUnavailable)])
async def test_denial_or_limit_stops_before_another_request(tmp_path, scoped_transport, status, expected):
    dispatch = scoped_transport
    dispatch.response = lambda _: _Response(status, b"", headers={"Retry-After": "120"})
    plan = FinderCapturePlan(tmp_path, "held", "held", (PPO, PATIENT_DIRECT))
    with pytest.raises(expected):
        await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)
    dispatch.pause.assert_awaited_once()
    dispatch.binding.defer.assert_called_once()
    assert len(dispatch.requests) == 1


@pytest.mark.asyncio
async def test_rejected_and_empty_windows_remain_incomplete(tmp_path, scoped_transport):
    dispatch = scoped_transport
    response_by_network = {"PPO": _Response(422, b""), "PatientDirect": _Response(200, _response(0, ""))}
    dispatch.response = lambda payload: response_by_network[payload["productType"]]
    plan = FinderCapturePlan(tmp_path, "test", "test", (PPO, PATIENT_DIRECT))
    summary = await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)
    assert summary["gaps"] == {"finder_search_rejected": 1, "empty_window_unverified": 1}
    assert summary["office_observations"] == 0


def test_office_products_never_expand_unknown_or_composite_labels():
    raw = json.loads(_response(1, "Delta Dental PPO Plus Premier"))
    row = raw["providerResults"][0]
    row["providerNPI"] = "0000000000"
    row["products"].append({"productType": "Regional Commercial Advantage"})
    page = parse_finder_json_page(json.dumps(raw).encode(), PPO, 1)
    (card,) = page.cards
    assert card.network_labels == ("Delta Dental PPO Plus Premier", "Regional Commercial Advantage")
    assert not card.memberships
    assert card.practitioner_npi is None
    assert page.gaps == {"provider_npi_unavailable"}


@pytest.mark.asyncio
async def test_an_interrupted_legacy_window_keeps_its_page_without_egress(tmp_path, scoped_transport):
    import sqlite3

    from process.delta_dental_directory_capture import iter_finder_windows
    from process.delta_dental_directory_observations import parse_rendered_finder_page

    plan = FinderCapturePlan(tmp_path, "test", "test", (PPO,), plan_id="html")
    html = (
        b"""
        <div aria-live="polite">51 dentist results found within 30 miles for Delta Dental PPO</div>
    """
        + 50
        * b"""
        <article>
          <a href="#" aria-label="1 Example St, Example, AA 00000, get directions for Example Dentist">Address</a>
          <div aria-label="Example Dentist participates in"><button disabled>Delta Dental PPO</button></div>
        </article>
    """
    )

    async def legacy_page(_search, number, _previous_page):
        assert number == 1
        return html

    windows = iter_finder_windows(
        plan, fetch_page=legacy_page, parse_page=parse_rendered_finder_page, cancel_check=AsyncMock()
    )
    with pytest.raises(AssertionError):
        async for _ in windows:
            pytest.fail("legacy search stopped before its next page")
    with sqlite3.connect(tmp_path / "finder-test.sqlite3") as database:
        (saved,) = database.execute("SELECT body FROM finder_page").fetchone()
        assert saved == html
    inherited = await _run_http_capture(
        scoped_transport.binding,
        FinderCapturePlan(tmp_path, "test", "test", (PPO,), plan_id="direct"),
        AsyncMock(),
        scoped_transport.pause,
    )
    assert not scoped_transport.requests
    assert inherited["office_observations"] == 50
    assert inherited["gaps"]["legacy_window_remaining_page_unavailable"] == 1


def test_selects_only_an_authenticated_https_proxy_for_the_exact_path(monkeypatch):
    from process.ext.source_http_proxy import POLICY_ENV, POOL_ENV, SourceProxyConfig

    rule_by_field = {"source_id": "finder", "use_proxy": True, "endpoint_scope": SOURCE_ENDPOINT}
    monkeypatch.setenv(POLICY_ENV, json.dumps({"version": 1, "revision": "policy", "sources": [rule_by_field]}))
    for scheme in ("http", "https"):
        proxy_routes = [{"id": "synthetic", "url": f"{scheme}://user:example@proxy.example.test"}]
        monkeypatch.setenv(POOL_ENV, json.dumps({"version": 1, "revision": "pool", "proxies": proxy_routes}))
        binding = SourceProxyConfig.from_environment().bind("finder", SOURCE_ENDPOINT)
        if scheme == "http":
            with pytest.raises(SourceProxyError, match="proxy_required"):
                transport._selection(binding, SOURCE_ENDPOINT)
            continue
        selected = transport._selection(binding, SOURCE_ENDPOINT)
        assert selected.request_options["ssl"].check_hostname
        assert selected.request_options["proxy_auth"]
        with pytest.raises(SourceProxyError, match="source_proxy_target_invalid"):
            transport._selection(binding, "https://external.example/")
        binding.defer(selected.proxy_id, None)
        with pytest.raises(SourceProxyUnavailable):
            transport._selection(binding, SOURCE_ENDPOINT)


@pytest.mark.asyncio
async def test_redirect_response_does_not_trigger_origin_fallback(tmp_path, scoped_transport):
    dispatch = scoped_transport
    dispatch.response = lambda _: _Response(302, b"", headers={"Location": "https://external.example/"})
    with pytest.raises(SourceProxyError, match="response_status_invalid"):
        await _run_http_capture(
            dispatch.binding, FinderCapturePlan(tmp_path, "test", "test", (PPO,)), AsyncMock(), dispatch.pause
        )
    assert len(dispatch.requests) == 1
    assert dispatch.requests[0][0] == SOURCE_ENDPOINT
    assert dispatch.requests[0][1]["allow_redirects"] is False


@pytest.mark.parametrize("body", [b"{", b"[]", b'{"providerResults":null}', _response(101, "Delta Dental PPO")])
def test_invalid_response_is_distinct_from_an_empty_success(body):
    with pytest.raises(ValueError, match="finder_response"):
        parse_finder_json_page(body, PPO, 1)


def test_partial_envelope_keeps_only_defensible_membership_rows():
    body = json.loads(_response(3, PPO.selector))
    first, second, third = body["providerResults"]
    first["products"] = [None, {"productType": PPO.selector}, {"productType": PPO.selector}]
    first["serviceOffice"]["location"]["zipCode"] = ""
    second["products"] = None
    del second["serviceOffice"]["location"]
    del third["provider"]["firstName"]
    body["providerResults"].append(None)
    page = parse_finder_json_page(json.dumps(body).encode(), PPO, 1)
    assert page.displayed_total == 4
    assert len(page.cards) == 2
    assert page.cards[0].network_labels == (PPO.selector,)
    assert page.cards[1].network_labels == ()
    assert all(not card.office_memberships for card in page.cards)
    assert page.gaps == {"invalid_product_observation", "invalid_provider_office_observation"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scenario", "message"),
    [
        ("wrong-content-type", "response_content_invalid"),
        ("long-response", "response_too_large"),
        ("long-chunked-response", "response_too_large"),
        ("timeout", "request_failed"),
    ],
)
async def test_unusable_response_stops_after_the_one_attempt(
    tmp_path, scoped_transport, monkeypatch, scenario, message
):
    dispatch = scoped_transport
    reply = _Response(200, b"synthetic response")
    match scenario:
        case "wrong-content-type":
            reply.content_type = "text/html"
        case "long-response":
            monkeypatch.setattr(transport, "MAX_PAGE_BYTES", 7)
        case "long-chunked-response":
            monkeypatch.setattr(transport, "MAX_PAGE_BYTES", 7)
            reply.content_length = None
    if scenario == "timeout":
        dispatch.response = Mock(side_effect=TimeoutError)
    else:
        dispatch.response = Mock(return_value=reply)
    with pytest.raises(SourceProxyError, match=message):
        await _run_http_capture(
            dispatch.binding, FinderCapturePlan(tmp_path, "test", "test", (PPO,)), AsyncMock(), dispatch.pause
        )
    assert len(dispatch.requests) == 1


@pytest.mark.asyncio
async def test_preflight_refuses_exhausted_budget_route_drift_and_cancellation(scoped_transport, monkeypatch):
    from process.control_cancel import ImportCancelledError
    from process.delta_dental_directory_capture import FinderPaginationUnresolved

    dispatch = scoped_transport
    cancel_check = AsyncMock()
    counts_by_action = {"origin_requests": transport.DEFAULT_REQUEST_BUDGET}
    async with transport.open_finder_http(
        dispatch.binding, cancel_check=cancel_check, pause=dispatch.pause, stats=counts_by_action
    ) as fetch_page:
        with pytest.raises(FinderPaginationUnresolved, match="legacy_window"):
            await fetch_page(PPO, 2, None)
        with pytest.raises(SourceProxyError, match="request_budget_exhausted"):
            await fetch_page(PPO, 1, None)
        monkeypatch.setattr(transport, "_selection", Mock(return_value=SimpleNamespace(proxy_id="other")))
        with pytest.raises(SourceProxyError, match="proxy_route_changed"):
            await fetch_page(PPO, 1, None)
        cancel_check.side_effect = ImportCancelledError("cancelled")
        with pytest.raises(ImportCancelledError):
            await fetch_page(PPO, 1, None)
    assert not dispatch.requests


def test_retry_after_accepts_a_full_date_and_invalid_values_remain_held():
    from datetime import UTC, datetime, timedelta
    from email.utils import format_datetime

    now = datetime.now(UTC).replace(microsecond=0)
    deadline = now + timedelta(minutes=15)
    assert transport._retry_boundary(format_datetime(deadline)) == deadline.isoformat()
    assert datetime.fromisoformat(transport._retry_boundary(None)) >= now + timedelta(seconds=60)
    assert transport._retry_boundary("unsupported") is None
    assert transport._retry_boundary("Wed, 06 Jan 2099 01:02:03") is None
    with pytest.raises(SourceProxyError, match="proxy_required"):
        transport._selection(None, SOURCE_ENDPOINT)


def _install_parallel_transport(monkeypatch, dispatch, response):
    import asyncio

    def select(_binding, _url, cursor="finder-http", exclude_ids=frozenset()):
        assert cursor not in exclude_ids
        return SimpleNamespace(proxy_id=cursor, request_options={"proxy": f"https://{cursor}.example.test"})

    @asynccontextmanager
    async def post(_url, **options):
        request = (asyncio.get_running_loop().time(), options["json"]["address"], options["proxy"])
        dispatch.requests.append(request)
        yield await response(request[1])

    @asynccontextmanager
    async def client(**_options):
        yield SimpleNamespace(post=post)

    monkeypatch.setattr(transport, "_selection", select)
    monkeypatch.setattr(transport.aiohttp, "ClientSession", client)


def _synthetic_searches(count):
    return tuple(FinderSearch(PPO.selector, f"{index:05}", 30) for index in range(count))


@pytest.mark.asyncio
async def test_eight_sticky_workers_share_one_pacer_and_exact_cache(tmp_path, scoped_transport, monkeypatch):
    import asyncio

    dispatch = scoped_transport
    all_active = asyncio.Event()
    counts = SimpleNamespace(active=0, peak=0)

    async def response(address):
        counts.active += 1
        counts.peak = max(counts.peak, counts.active)
        if counts.peak == 8:
            all_active.set()
        try:
            await asyncio.wait_for(all_active.wait(), timeout=3)
            return _Response(200, _response(100 if address == "00000" else 1, PPO.selector))
        finally:
            counts.active -= 1

    _install_parallel_transport(monkeypatch, dispatch, response)
    searches = _synthetic_searches(9)
    plan = FinderCapturePlan(tmp_path, "test", "test", (*searches, searches[0]), concurrency=8, requests_per_second=50)
    summary = await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)
    assert counts.peak == 8
    assert len(dispatch.requests) == summary["search_windows"] == 9
    assert len({proxy for _, _, proxy in dispatch.requests}) == 8
    starts = [started for started, _, _ in dispatch.requests]
    assert all(second - first >= 0.02 for first, second in zip(starts, starts[1:]))
    assert summary["gaps"] == {"result_cap_reached": 1}
    assert summary["directory_complete"] is False

    warm = FinderCapturePlan(tmp_path, "test", "test", searches, plan_id="warm", concurrency=8, requests_per_second=50)
    replay = await _run_http_capture(dispatch.binding, warm, AsyncMock(), dispatch.pause)
    assert len(dispatch.requests) == replay["pages_reused"] == 9
    assert replay["transport"]["origin_requests"] == 0


@pytest.mark.asyncio
async def test_racing_holds_stop_admission_and_drain_successes(tmp_path, scoped_transport, monkeypatch):
    import asyncio
    import sqlite3

    from process import delta_dental_directory_worker as worker

    dispatch = scoped_transport
    all_active = asyncio.Event()
    finite_retained = asyncio.Event()
    indefinite_retained = asyncio.Event()
    real_persist = worker._persist_destination_hold

    def observe_hold(root, until):
        merged = real_persist(root, until)
        (finite_retained if until is not None else indefinite_retained).set()
        return merged

    async def response(address):
        if len(dispatch.requests) == 8:
            all_active.set()
        await asyncio.wait_for(all_active.wait(), timeout=3)
        if address == "00000":
            return _Response(429, b"", {"Retry-After": "120"})
        if address == "00001":
            await finite_retained.wait()
            return _Response(403, b"")
        await indefinite_retained.wait()
        if address == "00002":
            return _Response(503, b"", {"Retry-After": "240"})
        return _Response(200, _response(1, PPO.selector))

    monkeypatch.setattr(worker, "_persist_destination_hold", observe_hold)
    _install_parallel_transport(monkeypatch, dispatch, response)
    searches = _synthetic_searches(12)
    plan = FinderCapturePlan(tmp_path, "held", "held", searches, concurrency=8, requests_per_second=100)
    with pytest.raises(SourceProxyError, match="access_denied"):
        await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)
    assert len(dispatch.requests) == 8
    assert dispatch.pause.await_args.args[0] is None
    assert json.loads((tmp_path / "destination-hold.json").read_text()) == {"retry_not_before": None}
    with sqlite3.connect(tmp_path / "finder-held.sqlite3") as database:
        assert database.execute("SELECT COUNT(*) FROM finder_page").fetchone() == (5,)
        assert database.execute("SELECT zip_code FROM finder_attempt ORDER BY zip_code").fetchall() == [
            ("00001",),
            ("00002",),
        ]


@pytest.mark.asyncio
async def test_resume_retries_only_definite_rate_rejection(tmp_path, scoped_transport, monkeypatch):
    from dataclasses import replace

    dispatch = scoped_transport
    plan = FinderCapturePlan(tmp_path, "resume", "resume", (PPO, PATIENT_DIRECT))

    monkeypatch.setattr(transport, "_retry_boundary", lambda _: "2000-01-01T00:00:00+00:00")
    dispatch.response = lambda _: _Response(429, b"")
    with pytest.raises(SourceProxyUnavailable):
        await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)

    dispatch.response = Mock(side_effect=[TimeoutError, _Response(200, _response(1, PATIENT_DIRECT.selector))])
    with pytest.raises(SourceProxyError, match="request_failed"):
        await _run_http_capture(dispatch.binding, replace(plan, resume=True), AsyncMock(), dispatch.pause)
    summary = await _run_http_capture(dispatch.binding, replace(plan, resume=True), AsyncMock(), dispatch.pause)
    assert [options["json"]["productType"] for _, options in dispatch.requests] == ["PPO", "PPO", "PatientDirect"]
    assert summary["gaps"] == {"finder_request_outcome_uncertain": 1}
    assert summary["pages_retained"] == 1


@pytest.mark.asyncio
async def test_rate_budget_spans_editions(tmp_path, scoped_transport, monkeypatch):
    dispatch = scoped_transport

    async def response(_address):
        return _Response(200, _response(1, PPO.selector))

    _install_parallel_transport(monkeypatch, dispatch, response)
    for edition in ("first", "second"):
        plan = FinderCapturePlan(tmp_path, edition, edition, (PPO,), requests_per_second=10)
        await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)
    assert dispatch.requests[1][0] - dispatch.requests[0][0] >= 0.09


@pytest.mark.asyncio
async def test_parallel_egress_refuses_duplicate_configured_routes(tmp_path, scoped_transport):
    dispatch = scoped_transport
    plan = FinderCapturePlan(tmp_path, "test", "test", (PPO,), concurrency=8, requests_per_second=1)
    with pytest.raises(SourceProxyError, match="distinct_proxy_routes_required"):
        await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)
    assert not dispatch.requests


@pytest.mark.asyncio
async def test_parallel_budget_drains_already_admitted_successes(tmp_path, scoped_transport, monkeypatch):
    import asyncio
    import sqlite3

    dispatch = scoped_transport
    both_admitted = asyncio.Event()

    async def response(_address):
        if len(dispatch.requests) == 2:
            both_admitted.set()
        await asyncio.wait_for(both_admitted.wait(), timeout=3)
        return _Response(200, _response(1, PPO.selector))

    _install_parallel_transport(monkeypatch, dispatch, response)
    plan = FinderCapturePlan(
        tmp_path,
        "bounded",
        "bounded",
        _synthetic_searches(8),
        concurrency=8,
        requests_per_second=100,
        max_origin_requests=2,
    )
    with pytest.raises(SourceProxyError, match="request_budget_exhausted"):
        await _run_http_capture(dispatch.binding, plan, AsyncMock(), dispatch.pause)
    assert len(dispatch.requests) == 2
    with sqlite3.connect(tmp_path / "finder-bounded.sqlite3") as database:
        assert database.execute("SELECT COUNT(*) FROM finder_page").fetchone() == (2,)
        assert database.execute("SELECT COUNT(*) FROM finder_attempt").fetchone() == (0,)


@pytest.mark.asyncio
async def test_progress_reports_only_aggregate_counts_at_intervals(tmp_path, scoped_transport, monkeypatch):
    from unittest.mock import call

    from process import delta_dental_directory_worker as worker

    dispatch = scoped_transport
    ticks = iter((0, 300, 300, 301, 600, 600))
    monkeypatch.setattr(worker, "monotonic", lambda: next(ticks))
    plan = FinderCapturePlan(tmp_path, "test", "test", _synthetic_searches(3), requests_per_second=100)
    directory = SimpleNamespace(_mark_provider_directory_progress=AsyncMock())
    pause, report = worker._observation_reporters(directory, "synthetic", None, 3)
    await _run_http_capture(dispatch.binding, plan, AsyncMock(), pause, report=report)

    def aggregate_call(done):
        return call(
            "synthetic",
            phase="delta dental finder capture",
            done=done,
            total=3,
            message="capturing finder observations",
            metrics={
                "delta_dental_directory_observation": {
                    "search_windows": done,
                    "pages_retained": done,
                    "pages_reused": 0,
                }
            },
        )

    assert directory._mark_provider_directory_progress.await_args_list == [aggregate_call(1), aggregate_call(3)]


def test_pins_eight_distinct_credential_routes_in_a_real_binding(monkeypatch):
    from process.ext.source_http_proxy import POLICY_ENV, POOL_ENV, SourceProxyConfig

    rule_by_field = {"source_id": "finder", "use_proxy": True, "endpoint_scope": SOURCE_ENDPOINT}
    monkeypatch.setenv(POLICY_ENV, json.dumps({"version": 1, "revision": "policy", "sources": [rule_by_field]}))
    routes = [
        {"id": f"example-{index}", "url": f"https://sticky-{index}:example@proxy.example.test"} for index in range(8)
    ]
    pool_by_field = {"version": 1, "revision": "pool", "proxies": routes}
    monkeypatch.setenv(POOL_ENV, json.dumps(pool_by_field))
    binding = SourceProxyConfig.from_environment().bind("finder", SOURCE_ENDPOINT)
    pinned = transport._pin_worker_routes(binding, 8)
    assert {route.proxy_id for route in pinned} == {route["id"] for route in routes}
    for index, route in enumerate(pinned):
        assert transport._validate_worker_route(binding, pinned, index) is route

    routes[1]["url"] = routes[0]["url"]
    monkeypatch.setenv(POOL_ENV, json.dumps(pool_by_field))
    aliased = SourceProxyConfig.from_environment().bind("finder", SOURCE_ENDPOINT)
    with pytest.raises(SourceProxyError, match="distinct_proxy_routes_required"):
        transport._pin_worker_routes(aliased, 8)
