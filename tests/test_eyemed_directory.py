# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""EyeMed location-search capture, recovery, and publication."""

import asyncio
import gzip
import json
import sqlite3
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from importlib import import_module
from time import monotonic
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import provider_directory_sources
from process import eyemed_directory_capture as eyemed
from process import eyemed_directory_integration as adapter
from process import provider_directory_source_proxy as proxy
from process import provider_directory_source_selection as selection

directory = import_module("process.provider_directory_fhir")


def catalog(*codes):
    return {
        "messageCode": "NTW001",
        "nextParams": None,
        "networks": [{"networkId": code, "networkName": "Network " + code} for code in codes],
    }


def site(npi=None):
    return {
        "locationId": "office-1",
        "businessName": "Example Clinic",
        "address": {"city": "Example"},
        "doctors": [
            {
                "providerId": "doctor-1",
                "firstName": "Alex",
                "lastName": "Example",
                "npiLicenseNumber": npi,
                "acceptingNewPatients": "No",
            }
        ],
    }


def search(code, *sites):
    return {
        "messageCode": "PRV001",
        "networkId": code,
        "networkName": "Network " + code,
        "lastRefreshedDate": "11/01/2025",
        "defaultInd": False,
        "zipClassSortedLocations": list(sites),
        "retailLocations": [],
        "independentLocations": [],
        "preferredLocations": [],
    }


def encode(obj):
    return json.dumps(obj).encode()


def plan(tmp_path):
    return eyemed.SearchPlan(tmp_path, ("00001", "00002"), 1, 20)


@pytest.mark.asyncio
async def test_atomic_windows_resume_and_preserve_network_memberships(tmp_path):
    path = tmp_path / "capture.sqlite3"
    first_requests = AsyncMock(
        side_effect=[
            encode(catalog("1", "2")),
            encode(search("1", site("1234567893"))),
            TimeoutError(),
        ]
    )
    with sqlite3.connect(path) as db:
        first_capture = eyemed.EyeMedCapture(db, plan(tmp_path), False)
        with pytest.raises(TimeoutError):
            await first_capture.collect(first_requests, AsyncMock(), AsyncMock())
    empty_by_field = {"messageCode": "PRV002", "message": "", "nextParams": "opaque"}
    second_requests = AsyncMock(
        side_effect=[
            encode(search("2", site())),
            encode(search("1", site("1245319599"))),
            encode(empty_by_field),
        ]
    )
    with sqlite3.connect(path) as db:
        capture = eyemed.EyeMedCapture(db, plan(tmp_path), True)
        proof = await capture.collect(second_requests, AsyncMock(), AsyncMock())
        requests = [call.args for call in second_requests.await_args_list]
        assert [(kind, body["address"]["zip"], body["networkId"]) for kind, body in requests] == [
            ("search", "00001", "2"),
            ("search", "00002", "1"),
            ("search", "00002", "2"),
        ]
        assert proof["site_memberships"] == proof["clinician_site_memberships"] == {"1": 1, "2": 1}
        assert proof["publisher_prv002_by_network"] == {"2": 1}
        assert proof["ambiguous_practitioner_npis"] == 1
        assert proof["publisher_refresh_dates"] == [
            {"network_id": "1", "last_refreshed_date": "11/01/2025", "windows": 2},
            {"network_id": "2", "last_refreshed_date": "11/01/2025", "windows": 1},
        ]
        assert proof["resource_counts"] == {
            "Organization": 3,
            "OrganizationAffiliation": 2,
            "Practitioner": 1,
            "PractitionerRole": 2,
            "Location": 1,
        }
        resources = list(capture.iter_resources())
        practitioner = next(
            resource_row for resource_row in resources if resource_row["resourceType"] == "Practitioner"
        )
        assert practitioner["identifier"] == [{"system": "urn:eyemed:provider-id", "value": "doctor-1"}]
        assert {
            resource_row["extension"][0]["valueReference"]["reference"]
            for resource_row in resources
            if resource_row["resourceType"] == "PractitionerRole"
        } == {
            "Organization/eyemed-network-1",
            "Organization/eyemed-network-2",
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"networkId": "2"},
        {"defaultInd": True},
        {"networkName": "renamed"},
        {"messageCode": "unknown"},
        {"zipClassSortedLocations": None},
        {"messageCode": "PRV002", "zipClassSortedLocations": [site()]},
    ],
)
async def test_rejects_unrecognized_or_defaulted_search_without_egress_on_retry(tmp_path, overrides):
    import_result = search("1", site()) | overrides
    request = AsyncMock(side_effect=[encode(catalog("1")), encode(import_result)])
    with sqlite3.connect(":memory:") as db:
        capture = eyemed.EyeMedCapture(db, plan(tmp_path), False)
        for _ in range(2):
            with pytest.raises(ValueError):
                await capture.collect(request, AsyncMock(), AsyncMock())
        assert request.await_count == 2
        assert db.execute("SELECT done FROM reply WHERE network='1'").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM resource WHERE family='Location'").fetchone() == (0,)


def named_empty_search():
    return search("1") | {"networkId": None, "networkSetId": None, "defaultedNetworkId": None, "message": ""}


@pytest.mark.asyncio
async def test_resume_accepts_retained_named_empty_reply_once(tmp_path):
    path = tmp_path / "capture.sqlite3"
    scope = eyemed.SearchPlan(tmp_path, ("00001",), 1, 2)
    raw = encode(named_empty_search())
    with sqlite3.connect(path) as db:
        capture = eyemed.EyeMedCapture(db, scope, False)

        async def initial_request(path, _payload):
            capture.reserve()
            return encode(catalog("1")) if path == "networks" else raw

        await capture.reply("", "", initial_request)
        await capture.reply("1", "00001", initial_request)
        assert db.execute("SELECT done FROM reply WHERE network='1'").fetchone() == (0,)

    with sqlite3.connect(path) as db:
        capture = eyemed.EyeMedCapture(db, scope, True)
        capture.accept_search = Mock(wraps=capture.accept_search)
        request = AsyncMock(side_effect=AssertionError("unexpected request"))
        for _ in range(2):
            proof = await capture.collect(request, AsyncMock(), AsyncMock())
            assert proof["windows_by_network"] == proof["publisher_prv001_null_network_empty_by_network"] == {"1": 1}
            assert proof["publisher_prv002_by_network"] == proof["rejected_zips_by_network"] == {}
            assert proof["site_memberships"] == proof["clinician_site_memberships"] == {}
            assert proof["publisher_refresh_dates"] == [
                {"network_id": "1", "last_refreshed_date": "11/01/2025", "windows": 1}
            ]
            assert proof["origin_requests"] == 2
        capture.accept_search.assert_called_once_with(raw, "1", "00001", "Network 1")
        request.assert_not_awaited()
        assert db.execute("SELECT done FROM reply WHERE network='1'").fetchone() == (4,)


@pytest.mark.parametrize(
    "overrides",
    [
        *({field: [site()]} for field in eyemed.LOCATION_ARRAYS),
        {"networkId": "2"},
        {"networkSetId": "2"},
        {"defaultedNetworkId": "2"},
        {"networkName": "Network 2"},
        {"networkName": " Network 1 "},
        {"defaultInd": True},
        {"defaultInd": 0},
        {"message": "other"},
        {"messageCode": "unknown"},
    ],
)
def test_unbound_search_rejects_rows_and_conflicting_metadata(overrides):
    with pytest.raises(ValueError):
        list(eyemed._locations(named_empty_search() | overrides, "1", "Network 1"))


@pytest.mark.parametrize(
    "field", [*eyemed.LOCATION_ARRAYS, "networkId", "networkSetId", "defaultedNetworkId", "message", "defaultInd"]
)
def test_unbound_search_requires_explicit_empty_metadata(field):
    payload = named_empty_search()
    del payload[field]
    with pytest.raises(ValueError):
        list(eyemed._locations(payload, "1", "Network 1"))


@pytest.mark.asyncio
async def test_selected_source_import_requires_proxy_and_writes_fhir_memberships(tmp_path, monkeypatch):
    source_by_field = {
        **adapter.source_row(directory._now()),
        "_endpoint_dataset_id": "test-dataset",
        "_resource_hash_contract": "test",
        "_pagination_checkpoint_context": SimpleNamespace(
            acquisition_root_run_id="test-root", dataset_id="test-dataset", retry_of_run_id=None
        ),
    }
    assert selection.search_directory_adapter(source_by_field) is adapter
    assert selection.requested_directory_adapter([eyemed.SOURCE_ID], None, None) is adapter
    assert directory._endpoint_dataset_expected_resources([source_by_field]) == eyemed.RESOURCE_TYPES
    monkeypatch.setattr(proxy, "current_run", lambda: SimpleNamespace(bindings={}, credentials={}))
    import_options_by_name = {
        "run_id": "test-run",
        "persist": AsyncMock(),
        "cancel_check": AsyncMock(),
        "progress_callback": AsyncMock(),
    }
    with pytest.raises(ValueError, match="authenticated_proxy_required"):
        await adapter.import_group(directory, source_by_field, **import_options_by_name)
    monkeypatch.setattr(eyemed, "candidate_write_scope", nullcontext)
    monkeypatch.setattr(adapter, "load_search_plan", lambda: plan(tmp_path))
    scope = SimpleNamespace(bindings={eyemed.SOURCE_ID: object()}, credentials={})
    monkeypatch.setattr(proxy, "current_run", lambda: scope)
    monkeypatch.setattr(eyemed, "_selection", lambda *_a, **_k: SimpleNamespace(proxy_id="route"))
    monkeypatch.setattr(eyemed.FinderAdmission, "admit", AsyncMock())
    origin = AsyncMock(side_effect=[encode(catalog("1")), encode(search("1", site())), encode(search("1", site()))])
    monkeypatch.setattr(eyemed, "_request", origin)
    copied_rows = []

    async def persist(model, rows):
        copied_rows.extend((model.__name__, resource_row) for resource_row in rows)
        return len(rows)

    import_options_by_name["persist"] = persist
    import_result = await adapter.import_group(directory, source_by_field, **import_options_by_name)
    assert import_result[0] == [eyemed.SOURCE_ID] and sum(import_result[2].values()) == len(copied_rows) == 6
    assert [
        resource_row["network_refs"]
        for _, resource_row in copied_rows
        if "network_refs" in resource_row and resource_row["network_refs"]
    ] == [
        ["Organization/eyemed-network-1"],
        ["Organization/eyemed-network-1"],
    ]


def test_catalog_requires_admitted_route(monkeypatch):
    for enabled in (False, True):
        monkeypatch.setattr(
            provider_directory_sources,
            "has_fresh_source_route",
            lambda source, endpoint: enabled and (source, endpoint) == (eyemed.SOURCE_ID, eyemed.ENDPOINT_SCOPE),
        )
        rows = provider_directory_sources.provider_directory_source_catalog()["items"]
        catalog_rows = [resource_row for resource_row in rows if resource_row["entry_id"] == eyemed.ENTRY_ID]
        assert len(catalog_rows) == int(enabled)
        if enabled:
            assert catalog_rows[0]["resource_profile"] == "M5" and catalog_rows[0]["resources"] == list(
                eyemed.RESOURCE_TYPES
            )


@pytest.mark.asyncio
async def test_rejected_zip_still_queries_every_network(tmp_path):
    rejection_by_field = {"messageCode": "PRV007", "message": "", "nextParams": "opaque"}
    origin = AsyncMock(
        side_effect=[
            encode(catalog("1", "2")),
            encode(rejection_by_field),
            encode(search("2", site())),
            encode(search("1")),
            encode(search("2")),
        ]
    )
    with sqlite3.connect(":memory:") as db:
        capture = eyemed.EyeMedCapture(db, plan(tmp_path), False)
        proof = await capture.collect(origin, AsyncMock(), AsyncMock())
        assert proof["rejected_zips_by_network"] == {"1": 1}
        assert proof["windows_by_network"] == {"1": 2, "2": 2}
        assert proof["site_memberships"] == {"2": 1}
        assert proof["publisher_prv002_by_network"] == {}
        assert await capture.collect(origin, AsyncMock(), AsyncMock()) == proof
        assert origin.await_count == 5


class PublisherReply:
    def __init__(self, *, status, body=b"", encoding="identity"):
        self.status = status
        self.headers = {"Retry-After": "120", "Content-Encoding": encoding}
        self.content_length = len(body)
        self.content_type = "application/json"
        self.body = body
        self.content = self

    async def iter_chunked(self, size):
        for offset in range(0, len(self.body), size):
            yield self.body[offset : offset + size]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 429, 503])
async def test_denial_and_quota_hold_pin_route(status):
    origin = Mock(return_value=PublisherReply(status=status))
    route = SimpleNamespace(proxy_id="sticky", request_options={})
    admission = Mock()
    binding = Mock()
    with pytest.raises(eyemed.SourceProxyUnavailable) as stopped:
        await eyemed._request(SimpleNamespace(request=origin), route, admission, binding, "https://example.org", None)
    until = stopped.value.retry_not_before
    assert bool(until) == (status != 403)
    binding.defer.assert_called_once_with("sticky", until)
    admission.record_hold.assert_called_once_with(until, status != 403)
    assert origin.call_args.kwargs["allow_redirects"] is False


@pytest.mark.asyncio
async def test_gzip_enforces_decoded_budget(monkeypatch):
    route = SimpleNamespace(proxy_id="sticky", request_options={})
    raw = b"0" * 100
    origin = Mock(return_value=PublisherReply(status=200, encoding="gzip", body=gzip.compress(raw)))
    assert (
        await eyemed._request(SimpleNamespace(request=origin), route, Mock(), Mock(), "https://example.org", {}) == raw
    )
    assert origin.call_args.kwargs["headers"]["Accept-Encoding"] == "gzip"
    monkeypatch.setattr(eyemed, "_MAX_REPLY", 30)
    with pytest.raises(ValueError, match="decoded_limit_exceeded"):
        await eyemed._request(SimpleNamespace(request=origin), route, Mock(), Mock(), "https://example.org", {})


@pytest.mark.asyncio
async def test_run_budget_and_scope_survive_reopen(tmp_path):
    path = tmp_path / "capture.sqlite3"
    scope = eyemed.SearchPlan(tmp_path, ("00001",), 1, 2)
    with sqlite3.connect(path) as db:
        first = eyemed.EyeMedCapture(db, scope, False)
        first.reserve()
    with sqlite3.connect(path) as db:
        reopened = eyemed.EyeMedCapture(db, scope, True)
        reopened.reserve()
        with pytest.raises(eyemed.SourceProxyError, match="budget_exhausted"):
            reopened.reserve()
        with pytest.raises(ValueError, match="scope_changed"):
            eyemed.EyeMedCapture(db, plan(tmp_path), True)


def concurrent_plan(tmp_path):
    return eyemed.SearchPlan(tmp_path, ("00001", "00002"), 10, 20, 4)


def random_route(_binding, _url, cursor, exclude_ids, **_options):
    assert cursor is None
    return SimpleNamespace(proxy_id=next(name for name in ("a", "b", "c", "d") if name not in exclude_ids))


async def capture_with_transport(plan, binding, *, retained=False):
    async with eyemed.acquire_directory(
        plan,
        "root",
        binding,
        progress=AsyncMock(),
        pause=AsyncMock(),
        cancel_check=AsyncMock(),
        require_retained=retained,
    ) as (_capture, proof):
        return proof


@pytest.mark.asyncio
async def test_parallel_windows_share_rate_and_retry_route(tmp_path, monkeypatch):
    calls = []
    four_started = asyncio.Event()

    async def origin(_http, route, _admission, _binding, _url, payload):
        if payload is None:
            return encode(catalog("1", "2", "3", "4"))
        key = (payload["address"]["zip"], payload["networkId"])
        calls.append((key, route.proxy_id, monotonic()))
        is_first = len(calls) == 1
        if len(calls) == 4:
            four_started.set()
        await asyncio.wait_for(four_started.wait(), 10)
        if is_first:
            raise ConnectionError
        return encode(search(key[1], site()))

    monkeypatch.setattr(eyemed, "_selection", random_route)
    monkeypatch.setattr(eyemed, "_request", origin)
    proof = await capture_with_transport(concurrent_plan(tmp_path), Mock())
    assert proof["origin_requests"] == 10 and len(calls) == 9
    assert proof["peak_in_flight_this_worker"] == proof["distinct_routes_this_worker"] == 4
    assert len({key for key, _, _ in calls}) == 8
    assert all(now[2] - prev[2] >= 0.085 for prev, now in zip(calls, calls[1:]))
    assert [route for key, route, _ in calls if key == calls[0][0]] == [calls[0][1]] * 2
    assert proof["windows_by_network"] == dict.fromkeys("1234", 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["cancel", "hold", "deny"])
async def test_drained_replies_survive_global_stop(tmp_path, monkeypatch, stop):
    calls = []
    started = asyncio.Event()
    release = asyncio.Event()
    boundary = (datetime.now(UTC) + timedelta(seconds=120)).isoformat()
    later_boundary = (datetime.now(UTC) + timedelta(seconds=300)).isoformat()

    async def origin(_http, route, admission, binding, _url, payload):
        if payload is None:
            return encode(catalog("1", "2", "3", "4"))
        code = payload["networkId"]
        calls.append(code)
        if len(calls) == 4:
            started.set()
        await release.wait()
        if stop != "cancel" and code in {"1", "2"}:
            until = boundary if code == "1" else later_boundary if stop == "hold" else None
            admission.record_hold(until, until is not None)
            binding.defer(route.proxy_id, until)
            raise eyemed.SourceProxyUnavailable(until)
        await asyncio.sleep(0.01)
        return encode(search(code, site()))

    monkeypatch.setattr(eyemed, "_selection", random_route)
    monkeypatch.setattr(eyemed, "_request", origin)
    task = asyncio.create_task(capture_with_transport(concurrent_plan(tmp_path), Mock()))
    await asyncio.wait_for(started.wait(), 10)
    if stop == "cancel":
        task.cancel()
        await asyncio.sleep(0)
    release.set()
    with pytest.raises(asyncio.CancelledError if stop == "cancel" else eyemed.SourceProxyError):
        await task
    assert sorted(calls) == list("1234")
    with sqlite3.connect(tmp_path / "root" / "capture.sqlite3") as db:
        completed_ids = {code for (code,) in db.execute("SELECT network FROM reply WHERE done=1")}
        assert completed_ids == (set("1234") if stop == "cancel" else set("34"))
        assert db.execute("SELECT attempts FROM run").fetchone()[0] == 5
    if stop != "cancel":
        with pytest.raises(eyemed.SourceProxyUnavailable) as first_retry:
            await capture_with_transport(concurrent_plan(tmp_path), Mock(), retained=True)
        assert first_retry.value.retry_not_before == (later_boundary if stop == "hold" else None)
    else:
        assert (await capture_with_transport(concurrent_plan(tmp_path), Mock(), retained=True))["origin_requests"] == 9
    assert len(calls) == (8 if stop == "cancel" else 4)


def test_legacy_single_lane_plan_identity(tmp_path):
    scope = plan(tmp_path)
    assert json.loads(scope.identity()) == [eyemed.ADAPTER_ID, list(scope.zip_codes), 1, 20]
    assert json.loads(concurrent_plan(tmp_path).identity())[-1] == 4


@pytest.mark.asyncio
async def test_fewer_distinct_routes_reduce_parallelism(tmp_path, monkeypatch):
    calls = []
    overlap = asyncio.Event()

    def limited_selection(_binding, _url, cursor, exclude_ids, **_options):
        assert cursor is None
        available = {"a", "b"} - exclude_ids
        if not available:
            raise eyemed.SourceProxyUnavailable(None)
        return SimpleNamespace(proxy_id=min(available))

    async def origin(_http, _route, _admission, _binding, _url, payload):
        if payload is None:
            return encode(catalog("1", "2", "3", "4"))
        calls.append((payload["address"]["zip"], payload["networkId"]))
        if len(calls) == 2:
            overlap.set()
        await asyncio.wait_for(overlap.wait(), 10)
        return encode(search(payload["networkId"], site()))

    monkeypatch.setattr(eyemed, "_selection", limited_selection)
    monkeypatch.setattr(eyemed, "_request", origin)
    proof = await capture_with_transport(concurrent_plan(tmp_path), Mock())
    assert proof["peak_in_flight_this_worker"] == proof["distinct_routes_this_worker"] == 2
    assert len(set(calls)) == len(calls) == 8


def test_search_name_matches_catalog_whitespace():
    network = eyemed._networks(encode(catalog("1") | {"networks": [{"networkId": "1", "networkName": "Network  1"}]}))[
        "1"
    ]
    payload = search("1", site()) | {"networkName": " Network  1 "}
    assert list(eyemed._locations(payload, "1", network["name"])) == [site()]
