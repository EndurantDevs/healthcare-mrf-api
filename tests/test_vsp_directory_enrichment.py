# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Missing doctor joins use retained identities before exact-office repair."""

import asyncio
import hashlib
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from process import vsp_directory_capture as acquisition
from process.vsp_directory_capture import VSPCapture, _bound_http
from process.vsp_directory_contract import (
    API_BASE,
    ENDPOINT_SCOPE,
    SEARCH_URL,
    SOURCE_ID,
    VSPDirectoryError,
    canonical_json,
    request_key,
)
from process.vsp_directory_enrichment import _add_resource, enrich_capture, write_normalized_capture
from process.vsp_directory_store import VSPCaptureStore


def office(identifier, *summaries):
    return {
        "pid": identifier,
        "addr": {
            "street1": "12 Example Road",
            "city": "Example City",
            "stateCode": "NY",
            "zipCode": "10001",
            "latitude": 40.0,
            "longitude": -100.0,
        },
        "locationDrProfileLink": {
            "href": API_BASE + f"provider-web/publicdoctorprofile?officepublicid={identifier}&asofdate=2035-01-01"
        },
        "providerSummaries": list(summaries),
    }


FIRST = {"pid": 2, "npi": "1234567893"}
SECOND = {"pid": 3, "npi": "1999999992"}


class HTTP:
    def __init__(self, *, repair, missing_doctor_codes=None):
        self.repair, self.requests, self.calls = repair, 0, []
        self.missing_doctor_codes = missing_doctor_codes

    async def request(self, method, url, request_body=None):
        self.requests += 1
        self.calls.append(url)
        assert method == "GET"
        if url.startswith(SEARCH_URL):
            response_records = office(52, FIRST, SECOND) if self.repair else office(52, FIRST)
        else:
            oid = parse_qs(urlsplit(url).query)["officepublicid"][0]
            doctors = (2, 3) if oid == "52" else (3,)
            response_records = [
                {
                    "doctorPublicId": did,
                    "networkIds": self.missing_doctor_codes
                    if did == 3 and self.missing_doctor_codes is not None
                    else ["1000", "2000", "4000"],
                }
                for did in doctors
            ]
        raw = canonical_json(response_records).encode()
        return (
            deepcopy(response_records),
            raw,
            {
                "method": method,
                "url": url,
                "payload": request_body,
                "request_key": request_key(method, url, request_body),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "byte_count": len(raw),
                "observed_at": datetime.now(UTC).isoformat(),
            },
        )


async def noop(*_args):
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("shared_identity", [False, True])
async def test_identity_cache_precedes_detail_lookup_and_restart_never_refetches(tmp_path, shared_identity):
    store = VSPCaptureStore(tmp_path, "synthetic-repair")
    first_http = HTTP(repair=True)
    run = VSPCapture(store, first_http, noop, noop)
    run.remember(office(52, FIRST), "Signature", "a" * 64)
    if shared_identity:
        run.remember(office(53, SECOND), "Signature", "b" * 64)
    await enrich_capture(run)
    assert {row["npi"] for row in run.offices["52"]["providerSummaries"]} == {FIRST["npi"], SECOND["npi"]}
    assert first_http.requests == 2
    assert sum(url.startswith(SEARCH_URL) for url in first_http.calls) == int(not shared_identity)
    assert len(run.identity_reuses) == int(shared_identity)
    resumed_http = HTTP(repair=False)
    resumed = VSPCapture(store, resumed_http, noop, noop)
    resumed.remember(office(52, FIRST), "Signature", "a" * 64)
    if shared_identity:
        resumed.remember(office(53, SECOND), "Signature", "b" * 64)
    await enrich_capture(resumed)
    assert resumed_http.requests == 0 and resumed.cache_hits == 2
    assert resumed.memberships == run.memberships


@pytest.mark.asyncio
async def test_exact_office_without_missing_doctor_refuses_completion(tmp_path):
    run = VSPCapture(
        VSPCaptureStore(tmp_path, "synthetic-unresolved"), HTTP(repair=False, missing_doctor_codes=["2000"]), noop, noop
    )
    run.remember(office(52, FIRST), "Signature", "a" * 64)
    with pytest.raises(VSPDirectoryError, match="^vsp_doctor_identity_unresolved$"):
        await enrich_capture(run)
    assert run.http.requests == 2 and run.store.load_checkpoint("complete") is None


@pytest.mark.asyncio
async def test_repair_uses_missing_doctors_network_instead_of_office_discovery_scope(tmp_path):
    run = VSPCapture(
        VSPCaptureStore(tmp_path, "synthetic-mixed"), HTTP(repair=True, missing_doctor_codes=["2000"]), noop, noop
    )
    run.remember(office(52, FIRST), "Signature", "a" * 64)
    await enrich_capture(run)
    assert run.http.calls[-1] == SEARCH_URL + "/52?networkNames=Choice" and run.http.requests == 2


@pytest.mark.asyncio
async def test_unselected_doctor_does_not_trigger_an_identity_request_or_a_membership(tmp_path):
    run = VSPCapture(
        VSPCaptureStore(tmp_path, "outside-network"), HTTP(repair=False, missing_doctor_codes=["other"]), noop, noop
    )
    run.remember(office(52, FIRST), "Signature", "a" * 64)
    await enrich_capture(run)
    counts = dict.fromkeys(run.memberships, 1)
    completed = await write_normalized_capture(run, counts, counts)
    assert len(run.http.calls) == 1 and not run.http.calls[0].startswith(SEARCH_URL)
    assert completed.proof["outside_profile_count"] == completed.proof["other_network_id_count"] == 1
    assert completed.resource_counts["PractitionerRole"] == completed.resource_counts["Practitioner"] == 1


def test_repeated_location_identity_must_keep_the_same_resource():
    resources_by_type = {"Location": {}}
    site_by_field = {"resourceType": "Location", "id": "site_by_field", "name": "Example Office"}
    size = _add_resource(resources_by_type, site_by_field, 0)
    assert _add_resource(resources_by_type, dict(site_by_field), size) == size
    with pytest.raises(VSPDirectoryError, match="^vsp_normalized_identity_conflict$"):
        _add_resource(resources_by_type, {**site_by_field, "name": "Different Office"}, size)
    assert list(resources_by_type["Location"].values()) == [(canonical_json(site_by_field) + "\n").encode()]


@pytest.mark.asyncio
async def test_later_partial_office_keeps_proven_identity_and_reuses_profile(tmp_path):
    run = VSPCapture(
        VSPCaptureStore(tmp_path, "partial-office"), HTTP(repair=False, missing_doctor_codes=["other"]), noop, noop
    )
    run.remember(office(52, FIRST), "Signature", "a" * 64)
    await enrich_capture(run)
    run.remember(office(52, {"pid": 2, "npi": None}), "Choice", "b" * 64)
    await enrich_capture(run)
    assert run.http.requests == 1 and run.offices["52"]["providerSummaries"] == [FIRST]
    assert run.identities["2"]["request_key"] == "a" * 64
    assert run.memberships == {name: {"52"} for name in ("Signature", "Choice", "Advantage")}


@pytest.mark.asyncio
async def test_unconfirmed_office_network_stops_before_fetching_other_profiles(tmp_path, monkeypatch):
    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-unconfirmed"), HTTP(repair=False), noop, noop)
    run.remember(office(52, FIRST), "Signature", "a" * 64)
    run.remember(office(53, SECOND), "Signature", "b" * 64)
    fetch = AsyncMock(return_value=([], {}))
    monkeypatch.setattr(run, "fetch", fetch)
    with pytest.raises(VSPDirectoryError, match="^vsp_profile_membership_census_unresolved$"):
        await enrich_capture(run)
    assert fetch.await_count == 1 and run.http.requests == 0


async def _completed_fixture(tmp_path):
    store = VSPCaptureStore(tmp_path, "synthetic-completed")
    run = VSPCapture(store, HTTP(repair=True), noop, noop)
    run.remember(office(52, FIRST), "Signature", "a" * 64)
    await enrich_capture(run)
    census_by_network = {"Signature": 1, "Choice": 1, "Advantage": 1}
    completed = await write_normalized_capture(run, census_by_network, census_by_network)
    store.checkpoint(
        "binding",
        {
            "policy_revision": "policy-example",
            "pool_revision": "pool-example",
            "attempts": run.http.requests,
            "cooldowns": {"version": 1, "cooldowns": []},
        },
    )
    return store, completed


@pytest.mark.asyncio
async def test_completed_summary_counts_are_covered_by_the_capture_digest(tmp_path):
    _, completed = await _completed_fixture(tmp_path)
    assert len(list(completed.iter_resources())) == 8
    completed.manifest["proof"]["network_npis"]["Choice"] += 1
    with pytest.raises(VSPDirectoryError, match="^vsp_capture_manifest_changed$"):
        list(completed.iter_resources())


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "root", "pool"])
async def test_completed_replay_checks_custody_without_reopening_http(tmp_path, monkeypatch, change):
    store, completed = await _completed_fixture(tmp_path)
    root_run_id = "synthetic-completed"
    if change == "root":
        root_run_id = "different-root"
        copied = VSPCaptureStore(tmp_path, root_run_id)
        copied.checkpoint("complete", completed.manifest)
        copied.checkpoint("binding", store.load_checkpoint("binding"))
    binding = SimpleNamespace(
        policy_revision="policy-example",
        pool_revision="changed-pool" if change == "pool" else "pool-example",
        select=lambda *_args: None,
        restore_cooldowns=lambda _snapshot: None,
    )
    monkeypatch.setattr(acquisition, "configured_root", lambda: tmp_path)

    def forbidden_session(**_kwargs):
        pytest.fail("closed replay must not open an HTTP session")

    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", forbidden_session)
    request = acquisition.capture_vsp_directory(
        source_record={"source_id": SOURCE_ID, "canonical_api_base": ENDPOINT_SCOPE},
        root_run_id=root_run_id,
        proxy_binding=binding,
        progress_callback=noop,
        cancel_check=noop,
    )
    if change:
        reason = "vsp_capture_binding_changed" if change == "pool" else "vsp_capture_identity_changed"
        with pytest.raises(VSPDirectoryError, match="^" + reason + "$"):
            await request
    else:
        replayed = await request
        assert list(replayed.iter_resources()) == list(completed.iter_resources())


@pytest.mark.asyncio
async def test_campaign_budget_and_cooldowns_survive_restart(tmp_path):
    store = VSPCaptureStore(tmp_path, "synthetic-binding")
    restored_snapshots = []
    binding = SimpleNamespace(
        policy_revision="policy-example",
        pool_revision="pool-example",
        select=lambda *_args: None,
        restore_cooldowns=restored_snapshots.append,
    )
    client = _bound_http(store, None, binding, noop)
    client.requests = client.max_requests
    cooldown_by_field = {"version": 1, "cooldowns": []}
    await client.pause(datetime.now(UTC).isoformat(), cooldown_by_field)
    resumed = _bound_http(store, None, binding, noop)
    assert resumed.requests == resumed.max_requests and restored_snapshots == [cooldown_by_field]
    with pytest.raises(VSPDirectoryError, match="^vsp_request_budget_exhausted$"):
        await resumed.request("POST", SEARCH_URL, {})
    binding.pool_revision = "changed-pool"
    with pytest.raises(VSPDirectoryError, match="^vsp_capture_binding_changed$"):
        _bound_http(store, None, binding, noop)


@pytest.mark.asyncio
async def test_direct_transport_budget_survives_restart_and_cannot_switch_modes(tmp_path):
    store = VSPCaptureStore(tmp_path, "synthetic-direct")
    direct = _bound_http(store, None, None, noop)
    direct.requests = 7
    await direct.pause(datetime.now(UTC).isoformat(), None)
    assert _bound_http(store, None, None, noop).requests == 7
    binding = SimpleNamespace(
        policy_revision="policy-example", pool_revision="pool-example", select=lambda *_args: None
    )
    with pytest.raises(VSPDirectoryError, match="^vsp_capture_binding_changed$"):
        _bound_http(store, None, binding, noop)


@pytest.mark.asyncio
async def test_long_origin_wait_can_be_cancelled_without_waiting_out_the_cooldown(tmp_path):
    sleeps = []

    async def cancel_check():
        if sleeps:
            raise asyncio.CancelledError

    async def sleep(seconds):
        sleeps.append(seconds)

    binding = SimpleNamespace(
        policy_revision="policy-example", pool_revision="pool-example", select=lambda *_args: None
    )
    client = _bound_http(VSPCaptureStore(tmp_path, "synthetic-cancel"), None, binding, cancel_check)
    client.sleep = sleep
    with pytest.raises(asyncio.CancelledError):
        await client.wait(3600)
    assert sleeps == [15] and client.requests == 0
