# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic acquisition counts prove reuse without making source requests."""

import hashlib
from collections import Counter
from copy import deepcopy
from urllib.parse import parse_qs, urlsplit

import pytest

from process import vsp_directory_capture as acquisition
from process.vsp_directory_contract import (
    API_BASE,
    NETWORKS,
    SEARCH_URL,
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
