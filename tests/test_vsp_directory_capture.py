# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic acquisition counts prove reuse without making source requests."""

import hashlib
from collections import Counter
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from process import vsp_directory_capture as acquisition
from process.vsp_directory_contract import (
    API_BASE,
    ENDPOINT_SCOPE,
    NETWORKS,
    SEARCH_URL,
    SOURCE_ID,
    VSPDirectoryError,
    canonical_json,
    request_key,
)
from process.vsp_directory_geometry import GeoTile
from process.vsp_directory_store import VSPCaptureStore

TILE = GeoTile(0, 10, 0, 10)


def office(identifier):
    return {
        "pid": identifier,
        "name": "Synthetic Office",
        "addr": {
            "street1": "12 Example Road",
            "city": "Example City",
            "stateCode": "NY",
            "zipCode": "10001",
            "latitude": 5.0,
            "longitude": 5.0,
        },
        "locationDrProfileLink": {
            "href": API_BASE + f"provider-web/publicdoctorprofile?officepublicid={identifier}&asofdate=2035-01-01"
        },
        "providerSummaries": [{"pid": 2, "npi": "1234567893", "first": "Alex", "last": "Example"}],
    }


class Store:
    """Only successful exact queries survive an acquisition restart."""

    def __init__(self):
        self.cache, self.receipts, self.checkpoints = {}, [], {}

    def lookup(self, method, url, payload=None):
        return deepcopy(self.cache.get(request_key(method, url, payload)))

    def retain(self, raw, metadata):
        record = (acquisition.strict_json(raw), deepcopy(metadata))
        self.receipts.append(record)
        self.cache.setdefault(metadata["request_key"], record)

    def checkpoint(self, name, value):
        self.checkpoints[name] = deepcopy(value)

    def load_checkpoint(self, name):
        return deepcopy(self.checkpoints.get(name))


class Source:
    """Three nested networks, with one shared doctor at distinct offices."""

    def __init__(self, *, saturated=False, changed=False, fail_profile=None):
        identifiers = range(101, 151) if saturated else range(101, 104)
        self.offices = {str(identifier): office(identifier) for identifier in identifiers}
        self.network_offices = {"Signature": set(self.offices), "Choice": {"101", "102"}, "Advantage": {"101"}}
        self.counts = {name: len(ids) for name, ids in self.network_offices.items()}
        if saturated:
            self.counts["Signature"] += 1
        self.changed, self.fail_profile = changed, fail_profile
        self.observations = Counter()

    def response(self, method, url, payload):
        if method == "POST":
            assert url == SEARCH_URL
            name = payload["networkNames"][0]
            if payload["limit"] == "1":
                self.observations[name] += 1
                changed = self.changed and name == "Choice" and self.observations[name] > 1
                return {"totalCounts": self.counts[name] + int(changed), "locations": []}
            assert payload == TILE.request(name)
            return {
                "totalCounts": self.counts[name],
                "locations": [self.offices[oid] for oid in sorted(self.network_offices[name])],
            }
        assert method == "GET" and url.startswith(API_BASE + "provider-web/publicdoctorprofile?")
        oid = parse_qs(urlsplit(url).query)["officepublicid"][0]
        if oid == self.fail_profile:
            self.fail_profile = None
            raise VSPDirectoryError("synthetic_partial_failure")
        codes = [code for code, name in NETWORKS.items() if oid in self.network_offices[name]]
        return [{"doctorPublicId": 2, "networkIds": codes, "providerNetworks": [{"networkId": code} for code in codes]}]


class HTTP:
    def __init__(self, source):
        self.source, self.requests, self.calls = source, 0, []

    async def request(self, method, url, payload=None):
        self.requests += 1
        self.calls.append((method, url, deepcopy(payload)))
        value = deepcopy(self.source.response(method, url, payload))
        raw = canonical_json(value).encode()
        return (
            value,
            raw,
            {
                "method": method,
                "url": url,
                "payload": payload,
                "request_key": request_key(method, url, payload),
                "observed_at": f"2035-01-01T00:00:{self.requests:02}+00:00",
                "sha256": hashlib.sha256(raw).hexdigest(),
                "byte_count": len(raw),
                "proxy_id": "route-example",
            },
        )


async def noop(*_args):
    return None


@pytest.fixture
def small_domain(monkeypatch):
    monkeypatch.setattr(acquisition, "world_tiles", lambda: (TILE,))


@pytest.fixture
def writer(monkeypatch):
    completed_captures = []

    async def write(capture, start, end):
        result_by_field = {"start": start, "end": end, "memberships": deepcopy(capture.memberships)}
        completed_captures.append(result_by_field)
        return result_by_field

    monkeypatch.setattr(acquisition, "write_normalized_capture", write)
    return completed_captures


def capture(source, store):
    transport = HTTP(source)
    return acquisition.VSPCapture(store, transport, noop, noop), transport


@pytest.mark.asyncio
async def test_largest_network_reuses_shared_profiles_and_skips_subset_discovery(small_domain, writer):
    source, store = Source(), Store()
    run, transport = capture(source, store)
    result = await run.run()
    tile_requests = [payload for method, _, payload in transport.calls if method == "POST" and payload["limit"] != "1"]
    assert tile_requests == [TILE.request("Signature")]
    assert transport.requests == 10  # Three opening counts, one tile, three profiles, three closing counts.
    assert Counter(method for method, _, _ in transport.calls) == {"POST": 7, "GET": 3}
    assert {name: len(ids) for name, ids in result["memberships"].items()} == source.counts
    assert source.observations == {name: 2 for name in NETWORKS.values()}
    assert len(store.receipts) == 10 and len(store.cache) == 7 and writer == [result]


@pytest.mark.asyncio
async def test_global_census_closure_skips_remaining_world_tiles(monkeypatch, writer):
    monkeypatch.setattr(acquisition, "world_tiles", lambda: (TILE, GeoTile(20, 30, 20, 30)))
    run, transport = capture(Source(), Store())
    await run.run()
    assert transport.requests == 10 and len(run.partitions) == 1
    assert run.partitions[0]["closed_by_global_census"] is True


@pytest.mark.asyncio
async def test_restart_replays_exact_successes_and_fetches_only_missing_profiles(small_domain, writer):
    source, store = Source(fail_profile="102"), Store()
    first, attempted = capture(source, store)
    with pytest.raises(VSPDirectoryError, match="^synthetic_partial_failure$"):
        await first.run()
    assert attempted.requests == 6 and len(store.receipts) == 5 and writer == []
    resumed, remaining = capture(source, store)
    await resumed.run()
    assert remaining.requests == 5 and resumed.cache_hits == 5
    assert Counter(method for method, _, _ in remaining.calls) == {"POST": 3, "GET": 2}
    assert all(payload["limit"] == "1" for method, _, payload in remaining.calls if method == "POST")
    assert len(store.receipts) == 10 and len(writer) == 1


@pytest.mark.asyncio
async def test_changed_fresh_closing_census_cannot_publish(small_domain, writer):
    run, transport = capture(Source(changed=True), Store())
    with pytest.raises(VSPDirectoryError, match="^vsp_census_changed_during_capture$"):
        await run.run()
    assert transport.requests == 10 and writer == []


@pytest.mark.asyncio
async def test_unknown_office_in_saturated_leaf_remains_incomplete(small_domain, writer):
    run, transport = capture(Source(saturated=True), Store())
    with pytest.raises(VSPDirectoryError, match="^vsp_office_census_unresolved$"):
        await run.run()
    assert len(run.offices) == 50 and len(run.unresolved) == 1 and writer == []
    assert run.unresolved[0]["count"] == 51
    assert transport.requests <= 54  # No subdivision can separate these identical coordinates.


@pytest.mark.asyncio
async def test_real_writer_round_trip_preserves_network_and_resource_counts(small_domain, tmp_path):
    store = VSPCaptureStore(tmp_path, "synthetic-capture")
    run, transport = capture(Source(), store)
    completed = await run.run()
    resources = list(completed.iter_resources())
    assert transport.requests == 10
    assert Counter(item["resourceType"] for item in resources) == {
        "Organization": 3,
        "Location": 3,
        "Practitioner": 1,
        "PractitionerRole": 3,
    }
    assert completed.resource_counts == {
        "InsurancePlan": 0,
        "OrganizationAffiliation": 0,
        "Organization": 3,
        "Location": 3,
        "Practitioner": 1,
        "PractitionerRole": 3,
    }
    assert completed.proof["publisher_locations"] == {"Signature": 3, "Choice": 2, "Advantage": 1}
    assert completed.proof["network_npis"] == {name: 1 for name in NETWORKS.values()}
    assert completed.proof["retained_response_count"] == completed.proof["http_attempts"] == 10
    assert store.load_checkpoint("complete") == completed.manifest


@pytest.mark.asyncio
async def test_probe_and_capture_share_retained_response_and_respect_origin_pause(small_domain, tmp_path, monkeypatch):
    offline_http = HTTP(Source())
    sessions = []

    @asynccontextmanager
    async def offline_session(*, auto_decompress, trust_env, cookie_jar):
        assert not auto_decompress and not trust_env and isinstance(cookie_jar, acquisition.aiohttp.DummyCookieJar)
        sessions.append(True)
        yield None

    async def exchange(_http, method, url, body, selection):
        assert selection is None
        return await offline_http.request(method, url, body), None

    wait = AsyncMock()
    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", offline_session)
    monkeypatch.setattr(acquisition.VSPHTTP, "_exchange", exchange)
    monkeypatch.setattr(acquisition.VSPHTTP, "wait", wait)
    monkeypatch.setattr(acquisition, "configured_root", lambda: tmp_path)
    arguments_by_name = {
        "source_record": {"source_id": SOURCE_ID, "canonical_api_base": ENDPOINT_SCOPE},
        "root_run_id": "probe-and-capture",
        "proxy_binding": None,
    }
    probe = await acquisition.probe_vsp_directory(**arguments_by_name)
    store = VSPCaptureStore(tmp_path, arguments_by_name["root_run_id"])
    (first_receipt,) = store.requests()
    assert probe == {
        "complete": False,
        "network": "Signature",
        "total_count": 3,
        "capture_sha256": first_receipt["sha256"],
    }
    assert store.load_checkpoint("complete") is None
    await store.pause((datetime.now(UTC) + timedelta(minutes=20)).isoformat(), None)
    wait.reset_mock()
    completed = await acquisition.capture_vsp_directory(**arguments_by_name, progress_callback=noop, cancel_check=noop)
    assert 1190 < wait.await_args_list[0].args[0] <= 1200
    assert len(sessions) == 2
    assert len(list(completed.iter_resources())) == 10
    assert completed.proof["http_attempts"] == offline_http.requests == 10
    assert offline_http.source.observations == dict.fromkeys(NETWORKS.values(), 2)


@pytest.mark.asyncio
async def test_saturated_tile_splits_until_source_offices_account_for_the_census(small_domain):
    first_offices = [office(i) for i in range(101, 151)]
    for site in first_offices:
        site["addr"].update(latitude=7.0, longitude=7.0)
    first_offices[-1]["addr"]["latitude"] = 8.0
    missing = office(151)
    missing["addr"].update(latitude=2.0, longitude=7.0)
    source = Source()
    run, transport = capture(source, Store())
    children = TILE.split()
    queries = [TILE, *children[:2]]
    results = [
        {"totalCounts": 51, "locations": first_offices},
        {"totalCounts": 0, "locations": []},
        {"totalCounts": 1, "locations": [missing]},
    ]

    def response(method, url, payload):
        tile = queries.pop(0)
        assert method == "POST" and url == SEARCH_URL and payload == tile.request("Signature")
        return results.pop(0)

    source.response = response
    await run.discover("Signature", 51)
    assert transport.requests == 3 and not results and not queries
    assert [part["count"] for part in run.partitions] == [0, 1]
    assert run.partitions[-1]["closed_by_global_census"] and len(run.memberships["Signature"]) == 51
