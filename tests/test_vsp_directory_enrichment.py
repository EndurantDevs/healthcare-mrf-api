# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Missing doctor joins use retained identities before exact-office repair."""

import asyncio
import hashlib
import json
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
from process.vsp_directory_normalization import VSPDirectoryNormalizationError
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
    run.remember_search(office(52, FIRST), "Signature", "a" * 64)
    if shared_identity:
        run.remember_search(office(53, SECOND), "Signature", "b" * 64)
    await enrich_capture(run)
    assert {row["npi"] for row in run.offices["52"]["providerSummaries"]} == {FIRST["npi"], SECOND["npi"]}
    assert first_http.requests == 2
    assert sum(url.startswith(SEARCH_URL) for url in first_http.calls) == int(not shared_identity)
    assert len(run.identity_reuses) == int(shared_identity)
    resumed_http = HTTP(repair=False)
    resumed = VSPCapture(store, resumed_http, noop, noop)
    resumed.remember_search(office(52, FIRST), "Signature", "a" * 64)
    if shared_identity:
        resumed.remember_search(office(53, SECOND), "Signature", "b" * 64)
    await enrich_capture(resumed)
    assert resumed_http.requests == 0 and resumed.cache_hits == 2
    assert resumed.memberships == run.memberships


@pytest.mark.asyncio
async def test_exact_office_without_summary_preserves_canonical_identity_and_replay(tmp_path):
    store = VSPCaptureStore(tmp_path, "synthetic-profile-identity")
    for expected_requests in (2, 0):
        run = VSPCapture(store, HTTP(repair=False, missing_doctor_codes=["2000"]), noop, noop)
        run.remember_search(office(52, FIRST), "Signature", "a" * 64)
        await enrich_capture(run)
        assert run.http.requests == expected_requests
        if expected_requests:
            assert store.load_checkpoint("complete") is None
            continue
        counts = dict.fromkeys(run.memberships, 1)
        completed = await write_normalized_capture(run, counts, counts)
        assert completed.resource_counts["Practitioner"] == completed.resource_counts["PractitionerRole"] == 2
        assert completed.proof["profiles_without_npi"] == 1
        resources = list(completed.iter_resources())
        doctor = next(row for row in resources if row["id"] == "vsp-doctor-3")
        assert doctor["identifier"] == [{"system": "urn:vsp:doctor-id", "value": "3"}]
        role = next(row for row in resources if row["id"] == "vsp-role-3-52")
        assert role["practitioner"]["reference"] == "Practitioner/vsp-doctor-3"
        assert role["extension"][0]["valueReference"]["reference"] == "Organization/vsp-network-2000"
        assert run.offices["52"]["providerSummaries"] == [FIRST]


@pytest.mark.asyncio
@pytest.mark.parametrize("later_repair", [False, True])
async def test_empty_identity_lookup_preserves_profile_and_tries_remaining_networks(tmp_path, later_repair):
    class EmptyOfficeHTTP(HTTP):
        async def request(self, method, url, request_body=None):
            response_body, raw, metadata = await super().request(method, url, request_body)
            if url.startswith(SEARCH_URL) and not (later_repair and url.endswith("=Choice")):
                empty_office_by_field = {"pid": 0, "distance": 0.0}
                raw = canonical_json(empty_office_by_field).encode()
                metadata = {**metadata, "sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)}
                return empty_office_by_field, raw, metadata
            return response_body, raw, metadata

    store = VSPCaptureStore(tmp_path, "synthetic-empty-office")
    expected_requests = 3 if later_repair else 4
    for request_count in (expected_requests, 0):
        run = VSPCapture(store, EmptyOfficeHTTP(repair=True), noop, noop)
        run.remember_search(office(52, FIRST), "Signature", "a" * 64)
        await enrich_capture(run)
        assert run.http.requests == request_count
        if request_count:
            continue
        completed = await write_normalized_capture(
            run, dict.fromkeys(run.memberships, 1), dict.fromkeys(run.memberships, 1)
        )
        assert completed.resource_counts["Practitioner"] == completed.resource_counts["PractitionerRole"] == 2
        assert completed.proof["profiles_without_npi"] == int(not later_repair)
        assert completed.proof["network_npis"] == dict.fromkeys(run.memberships, 2 if later_repair else 1)
        assert run.search_memberships == {"52": {"Signature": {"2"}}}
        assert run.site_memberships == {"Signature": {"52"}, "Choice": set(), "Advantage": set()}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {"pid": 53, "distance": 0},
        {"pid": 0, "distance": 0, "providerSummaries": []},
        {"pid": 0, "distance": 1},
        {"pid": False, "distance": 0},
        {"pid": 0.0, "distance": 0},
        {"pid": "0", "distance": 0},
        {"pid": 0, "distance": False},
    ],
)
async def test_nonempty_or_malformed_identity_lookup_still_fails(tmp_path, response):
    class MismatchedHTTP(HTTP):
        async def request(self, method, url, request_body=None):
            response_body, raw, metadata = await super().request(method, url, request_body)
            if url.startswith(SEARCH_URL):
                response_body = response
                raw = canonical_json(response_body).encode()
                metadata = {**metadata, "sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)}
            return response_body, raw, metadata

    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-mismatched-office"), MismatchedHTTP(repair=True), noop, noop)
    run.remember_search(office(52, FIRST), "Signature", "a" * 64)
    with pytest.raises(VSPDirectoryError, match="vsp_office_response_mismatch|vsp_public_id_invalid"):
        await enrich_capture(run)
    assert run.store.load_checkpoint("complete") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("conflicting_later_profile", [False, True])
@pytest.mark.parametrize("null_npi_summary", [False, True])
async def test_profile_npi_reuses_identity_cache_without_extra_summary_requests(
    tmp_path, conflicting_later_profile, null_npi_summary
):
    class ProfileNPIHTTP(HTTP):
        async def request(self, method, url, request_body=None):
            response_records, raw, metadata = await super().request(method, url, request_body)
            assert not url.startswith(SEARCH_URL)
            for detail in response_records:
                if detail["doctorPublicId"] == 3:
                    detail["npi"] = "1999999992" if "officepublicid=52" in url else "1234567893"
            raw = canonical_json(response_records).encode()
            return (
                response_records,
                raw,
                {**metadata, "sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)},
            )

    store = VSPCaptureStore(tmp_path, "synthetic-profile-npi")
    run = VSPCapture(store, ProfileNPIHTTP(repair=False), noop, noop)
    summary_records = [FIRST]
    if null_npi_summary:
        summary_records.append({"pid": 3, "npi": None, "first": "Example", "middle": "M", "last": "Clinician"})
    run.remember_search(office(52, *summary_records), "Signature", "a" * 64)
    if conflicting_later_profile:
        run.remember_search(office(53), "Signature", "b" * 64)
        with pytest.raises(VSPDirectoryError, match="^vsp_doctor_identity_changed$"):
            await enrich_capture(run)
        return
    await enrich_capture(run)
    assert run.http.requests == 1 and run.identities["3"]["summary"]["npi"] == "1999999992"
    if null_npi_summary:
        assert run.identities["3"]["summary"] == {**summary_records[1], "npi": "1999999992"}
    assert run.search_memberships == {"52": {"Signature": {"2", "3"} if null_npi_summary else {"2"}}}
    resumed = VSPCapture(store, ProfileNPIHTTP(repair=False), noop, noop)
    resumed.remember_search(office(52, *summary_records), "Signature", "a" * 64)
    await enrich_capture(resumed)
    assert resumed.http.requests == 0
    completed = await write_normalized_capture(
        resumed, dict.fromkeys(resumed.memberships, 1), dict.fromkeys(resumed.memberships, 1)
    )
    assert completed.proof["profiles_without_npi"] == 0
    assert completed.proof["network_npis"] == dict.fromkeys(resumed.memberships, 2)
    if null_npi_summary:
        doctor = next(resource for resource in completed.iter_resources() if resource["id"] == "vsp-npi-1999999992")
        assert doctor["name"] == [{"family": "Clinician", "given": ["Example", "M"]}]


@pytest.mark.asyncio
async def test_profile_npi_does_not_overwrite_nonnull_invalid_summary(tmp_path):
    class InvalidSummaryHTTP(HTTP):
        async def request(self, method, url, request_body=None):
            response_records, _, metadata = await super().request(method, url, request_body)
            if url.startswith(SEARCH_URL):
                response_records = office(52, FIRST, {"pid": 3, "npi": "1234567890"})
            else:
                response_records[1]["npi"] = "1999999992"
            raw = canonical_json(response_records).encode()
            return (
                response_records,
                raw,
                {**metadata, "sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)},
            )

    run = VSPCapture(
        VSPCaptureStore(tmp_path, "synthetic-profile-npi-conflict"), InvalidSummaryHTTP(repair=False), noop, noop
    )
    run.remember_search(office(52, FIRST, {"pid": 3, "npi": "1234567890"}), "Signature", "a" * 64)
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_conflicting_npi$"):
        await enrich_capture(run)
    assert run.offices["52"]["providerSummaries"][1]["npi"] == "1234567890"
    assert "3" not in run.identities and run.store.load_checkpoint("complete") is None


@pytest.mark.asyncio
async def test_invalid_publisher_npi_retains_source_identity_after_bounded_repair_and_replay(tmp_path):
    class InvalidNPIHTTP(HTTP):
        async def request(self, method, url, request_body=None):
            value, _, metadata = await super().request(method, url, request_body)
            if url.startswith(SEARCH_URL):
                value["providerSummaries"][1]["npi"] = "1234567890"
            raw = canonical_json(value).encode()
            return value, raw, {**metadata, "sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)}

    store = VSPCaptureStore(tmp_path, "synthetic-invalid-npi")
    memberships = dict.fromkeys(("Signature", "Choice", "Advantage"), 1)
    for expected_requests in (4, 0):
        run = VSPCapture(store, InvalidNPIHTTP(repair=True), noop, noop)
        run.remember_search(office(52, FIRST, {"pid": 3, "npi": "1234567890"}), "Signature", "a" * 64)
        await enrich_capture(run)
        assert run.http.requests == expected_requests
        if expected_requests:
            assert store.load_checkpoint("complete") is None
            continue
        completed = await write_normalized_capture(run, memberships, memberships)
        assert completed.resource_counts["Practitioner"] == completed.resource_counts["PractitionerRole"] == 2
        assert completed.proof["network_npis"] == dict.fromkeys(memberships, 1)
        assert completed.proof["profiles_without_npi"] == 1
        resources = list(completed.iter_resources())
        practitioner = next(row for row in resources if row["id"] == "vsp-doctor-3")
        assert practitioner["identifier"] == [{"system": "urn:vsp:doctor-id", "value": "3"}]
        assert run.offices["52"]["providerSummaries"][1]["npi"] == "1234567890"


@pytest.mark.asyncio
async def test_repair_uses_missing_doctors_network_instead_of_office_discovery_scope(tmp_path):
    run = VSPCapture(
        VSPCaptureStore(tmp_path, "synthetic-mixed"), HTTP(repair=True, missing_doctor_codes=["2000"]), noop, noop
    )
    run.remember_search(office(52, FIRST), "Signature", "a" * 64)
    await enrich_capture(run)
    assert run.http.calls[-1] == SEARCH_URL + "/52?networkNames=Choice" and run.http.requests == 2


@pytest.mark.asyncio
async def test_later_office_repair_replaces_earlier_source_identity_before_final_normalization(tmp_path):
    class LaterIdentityHTTP(HTTP):
        async def request(self, method, url, request_body=None):
            value, _, metadata = await super().request(method, url, request_body)
            if url.startswith(SEARCH_URL):
                oid = int(urlsplit(url).path.rsplit("/", 1)[1])
                value = office(oid, FIRST, {"pid": 3, "npi": "1234567890"}) if oid == 52 else office(oid, SECOND)
            raw = canonical_json(value).encode()
            return value, raw, {**metadata, "sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)}

    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-late-npi"), LaterIdentityHTTP(repair=True), noop, noop)
    run.remember_search(office(52, FIRST, {"pid": 3, "npi": "1234567890"}), "Signature", "a" * 64)
    run.remember_search(office(53, {"pid": 3, "npi": "1234567890"}), "Signature", "b" * 64)
    await enrich_capture(run)
    assert run.offices["52"]["providerSummaries"][1]["npi"] == "1234567890"
    counts = dict.fromkeys(run.memberships, 2)
    completed = await write_normalized_capture(run, counts, counts)
    assert run.http.requests == 6
    assert completed.proof["profiles_without_npi"] == 0
    assert completed.proof["network_npis"] == dict.fromkeys(counts, 2)
    assert completed.resource_counts["Practitioner"] == 2 and completed.resource_counts["PractitionerRole"] == 3
    assert not any(row["id"].startswith("vsp-doctor-") for row in completed.iter_resources())


@pytest.mark.asyncio
async def test_unselected_doctor_does_not_trigger_an_identity_request_or_a_membership(tmp_path):
    run = VSPCapture(
        VSPCaptureStore(tmp_path, "outside-network"), HTTP(repair=False, missing_doctor_codes=["other"]), noop, noop
    )
    run.remember_search(office(52, FIRST), "Signature", "a" * 64)
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
    run.remember_search(office(52, FIRST), "Signature", "a" * 64)
    await enrich_capture(run)
    run.remember_search(office(52, {"pid": 2, "npi": None}), "Choice", "b" * 64)
    await enrich_capture(run)
    assert run.http.requests == 1 and run.offices["52"]["providerSummaries"] == [FIRST]
    assert run.identities["2"]["request_key"] == "a" * 64
    assert run.memberships == {name: {"52"} for name in ("Signature", "Choice", "Advantage")}


@pytest.mark.asyncio
@pytest.mark.parametrize("npi", [None, " 1234567893 ", 1234567893])
async def test_unconfirmed_office_network_stops_before_fetching_other_profiles(tmp_path, monkeypatch, npi):
    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-unconfirmed"), HTTP(repair=False), noop, noop)
    run.remember_search(office(52, {"pid": 2, "npi": npi}), "Signature", "a" * 64)
    run.remember_search(office(53, SECOND), "Signature", "b" * 64)
    fetch = AsyncMock(return_value=([], {}))
    monkeypatch.setattr(run, "fetch", fetch)
    with pytest.raises(VSPDirectoryError, match="^vsp_profile_membership_census_unresolved$"):
        await enrich_capture(run)
    assert fetch.await_count == 1 and run.http.requests == 0


async def _completed_fixture(tmp_path):
    store = VSPCaptureStore(tmp_path, "synthetic-completed")
    run = VSPCapture(store, HTTP(repair=True), noop, noop)
    run.remember_search(office(52, FIRST), "Signature", "a" * 64)
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


class RetailHTTP(HTTP):
    network_ids = ("VWRK ", "VWRKQ")
    positive_networks = frozenset({"Signature", "Choice", "Advantage"})
    location_type = "Affiliate"
    clinician = False

    async def request(self, method, url, request_body=None):
        from tests.test_vsp_directory_normalization import retail_office_and_profile

        site, display = retail_office_and_profile()
        site["locationType"] = self.location_type
        display["networkIds"] = list(self.network_ids)
        display["providerNetworks"] = [{"networkId": code} for code in self.network_ids]
        site["locationDrProfileLink"] = office(52)["locationDrProfileLink"]
        if method == "GET":
            assert not url.startswith(SEARCH_URL), "A retail display must not trigger doctor NPI repair"
            _, _, metadata = await super().request(method, url, request_body)
            profiles = [display]
            if self.clinician:
                profiles.append(
                    {
                        "doctorPublicId": 2,
                        "networkIds": ["2000"],
                        "firstName": "Alex",
                        "lastName": "Example",
                        "doctorDegreeCode": "OD",
                        "serviceTypeCode": "DL",
                    }
                )
        else:
            assert method == "POST"
            self.requests += 1
            self.calls.append(url)
            site.pop("providerSummaries")
            if self.clinician:
                site["providerSummaries"] = [FIRST]
            locations = [site] if request_body["networkNames"][0] in self.positive_networks else []
            search_by_field = {"totalCounts": len(locations), "locations": locations}
            metadata = {
                "method": method,
                "url": url,
                "payload": request_body,
                "request_key": request_key(method, url, request_body),
                "observed_at": datetime.now(UTC).isoformat(),
            }
        response_body = profiles if method == "GET" else search_by_field
        raw = canonical_json(response_body).encode()
        return response_body, raw, {**metadata, "sha256": hashlib.sha256(raw).hexdigest(), "byte_count": len(raw)}


def _company_capture_records(secondary_service="DL"):
    from tests.test_vsp_directory_normalization import company_office_fixture, profile

    offices_by_id, profiles_by_id = {}, {}
    for oid, service in ((52, "EC"), (53, secondary_service)):
        site, display, summary_dict = company_office_fixture(oid, service)
        site.update(parentCompanyCd="EXAMPLE", name="Current Optical Brand")
        display.update(firstName="Earlier", lastName="Company")
        summary_dict.update(first="Earlier", last="Company")
        if service == "FS":
            site["name"] = "Independent Optical"
        site["locationDrProfileLink"] = office(oid)["locationDrProfileLink"]
        profiles_by_id[str(oid)] = [display]
        if service == "EC":
            site["providerSummaries"].append(FIRST)
            profiles_by_id[str(oid)].append({**profile(2, "2000"), "doctorDegreeCode": "OD", "serviceTypeCode": "EC"})
        offices_by_id[str(oid)] = site
    return offices_by_id, profiles_by_id


class CompanyHTTP:
    def __init__(self, secondary_service="DL"):
        self.requests = 0
        self.offices_by_id, self.profiles_by_id = _company_capture_records(secondary_service)

    async def request(self, method, url, request_body=None):
        self.requests += 1
        if method == "GET":
            assert not url.startswith(SEARCH_URL), "A company identity must not trigger clinical NPI repair"
            oid = parse_qs(urlsplit(url).query)["officepublicid"][0]
            response_body = self.profiles_by_id[oid]
        else:
            assert method == "POST"
            locations = list(self.offices_by_id.values()) if request_body["networkNames"] == ["Signature"] else []
            search_by_field = {"totalCounts": len(locations), "locations": locations}
            response_body = search_by_field
        raw = canonical_json(response_body).encode()
        return (
            response_body,
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


@pytest.mark.asyncio
@pytest.mark.parametrize("secondary_service", ["DL", "FS"])
async def test_company_npi_replays_to_both_sites_without_granting_clinical_membership(tmp_path, secondary_service):
    from process.vsp_directory_enrichment import _normalize_offices

    store = VSPCaptureStore(tmp_path, "synthetic-company-sites")
    for expected_requests in (5 if secondary_service == "DL" else 4, 0):
        run = VSPCapture(store, CompanyHTTP(secondary_service), noop, noop)
        offices_by_id, _ = _company_capture_records(secondary_service)
        for site in offices_by_id.values():
            run.remember_search(site, "Signature", "a" * 64)
        await enrich_capture(run)
        assert run.http.requests == expected_requests
        assert run.identities["9"]["resource_type"] == "Organization"
        resources, npis, accounting, _ = await _normalize_offices(run)
        assert set(resources["Practitioner"]) == {"vsp-npi-1234567893"}
        assert set(resources["PractitionerRole"]) == {"vsp-role-2-52-1234567893"}
        assert npis == {"Signature": set(), "Choice": {"Practitioner/vsp-npi-1234567893"}, "Advantage": set()}
        assert accounting["retail_locations"] == 2 and accounting["profiles_without_npi"] == 0
        for oid in ("52", "53"):
            organization = json.loads(resources["Organization"]["vsp-site-org-" + oid])
            assert organization["identifier"][-1] == {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1999999992"}
            affiliation = json.loads(resources["OrganizationAffiliation"]["vsp-site-network-" + oid])
            assert affiliation["network"] == [{"reference": "Organization/vsp-network-1000"}]


@pytest.mark.asyncio
async def test_distinct_company_identities_preserve_site_accounting_and_replay(tmp_path):
    from process.vsp_directory_enrichment import _normalize_offices
    from tests.test_vsp_directory_normalization import multi_company_office_fixture

    store = VSPCaptureStore(tmp_path, "synthetic-multiple-companies")
    for expected_requests in (3, 0):
        site, profiles = multi_company_office_fixture()
        site["locationDrProfileLink"] = office(52)["locationDrProfileLink"]
        http = CompanyHTTP()
        http.offices_by_id, http.profiles_by_id = {"52": site}, {"52": profiles}
        run = VSPCapture(store, http, noop, noop)
        run.remember_search(site, "Signature", "a" * 64)
        await enrich_capture(run)
        resources, npis, accounting, _ = await _normalize_offices(run)
        assert http.requests == expected_requests
        assert accounting["retail_locations"] == 1 and accounting["retail_company_organizations"] == 2
        assert len(resources["Organization"]) == 6 and len(resources["OrganizationAffiliation"]) == 3
        assert npis == {"Signature": set(), "Choice": {"Practitioner/vsp-npi-1234567893"}, "Advantage": set()}
        assert run.identities["9"]["resource_type"] == run.identities["10"]["resource_type"] == "Organization"


@pytest.mark.parametrize("damage", ["clinical", "name", "npi"])
def test_company_cache_rejects_kind_name_or_npi_changes(tmp_path, damage):
    from tests.test_vsp_directory_normalization import company_office_fixture

    site, _, summary = company_office_fixture()
    site["locationDrProfileLink"] = office(52)["locationDrProfileLink"]
    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-company-conflict"), CompanyHTTP(), noop, noop)
    run.remember(site, "a" * 64)
    incoming_summary_dict = dict(summary)
    if damage == "clinical":
        incoming_summary_dict.update(degree="OD", first="Alex", last="Example")
    if damage == "name":
        incoming_summary_dict["last"] = "Vision 43"
    if damage == "npi":
        incoming_summary_dict["npi"] = "1234567893"
    with pytest.raises(VSPDirectoryError, match="^vsp_doctor_identity_changed$"):
        run.remember({**site, "providerSummaries": [incoming_summary_dict]}, "b" * 64)


@pytest.mark.asyncio
async def test_late_company_identity_cannot_repair_an_earlier_clinician(tmp_path):
    from process.vsp_directory_enrichment import _normalize_offices
    from tests.test_vsp_directory_normalization import company_office_fixture, profile

    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-late-company"), CompanyHTTP(), noop, noop)
    run.remember_search(office(52, {"pid": 9, "first": "Alex", "last": "Example", "npi": None}), "Signature", "a" * 64)
    run.profiles["52"] = [{**profile(9, "1000"), "doctorDegreeCode": "OD", "serviceTypeCode": "FS"}]
    resources, _, _, _ = await _normalize_offices(run)
    assert "vsp-doctor-9" in resources["Practitioner"]
    site, display, _ = company_office_fixture(53)
    site["locationDrProfileLink"] = office(53)["locationDrProfileLink"]
    run.remember_search(site, "Signature", "b" * 64)
    run.profiles["53"] = [display]
    with pytest.raises(VSPDirectoryError, match="^vsp_profile_membership_census_unresolved$"):
        await _normalize_offices(run)
    assert run.http.requests == 0


@pytest.mark.asyncio
async def test_retail_capture_accounts_for_locations_and_replays_without_origin(tmp_path):
    from tests.test_vsp_directory_normalization import retail_office_and_profile

    site, _ = retail_office_and_profile()
    site["locationDrProfileLink"] = office(52)["locationDrProfileLink"]
    store = VSPCaptureStore(tmp_path, "synthetic-retail-site")
    for expected in (4, 0):
        run = VSPCapture(store, RetailHTTP(repair=False), noop, noop)
        run.remember_search(site, "Signature", "a" * 64)
        await enrich_capture(run)
        assert run.http.requests == expected and not run.search_memberships.get("52")
        if expected:
            continue
        counts = dict.fromkeys(run.memberships, 1)
        completed = await write_normalized_capture(run, counts, counts)
        assert completed.resource_counts["Location"] == completed.resource_counts["OrganizationAffiliation"] == 1
        assert completed.resource_counts["Organization"] == 4
        assert completed.resource_counts["Practitioner"] == completed.resource_counts["PractitionerRole"] == 0
        assert completed.proof["retail_locations"] == 1 and completed.proof["profiles_without_npi"] == 0
        assert completed.proof["network_npis"] == dict.fromkeys(counts, 0)


@pytest.mark.asyncio
async def test_retail_display_with_global_npi_identity_remains_unresolved(tmp_path):
    from tests.test_vsp_directory_normalization import retail_office_and_profile

    site, _ = retail_office_and_profile()
    site["locationDrProfileLink"] = office(52)["locationDrProfileLink"]
    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-retail-conflict"), RetailHTTP(repair=False), noop, noop)
    run.remember_search(site, "Signature", "a" * 64)
    run.remember(office(53, {"pid": 9, "npi": FIRST["npi"]}), "b" * 64)
    with pytest.raises(VSPDirectoryError, match="^vsp_profile_membership_census_unresolved$"):
        await enrich_capture(run)


@pytest.mark.asyncio
@pytest.mark.parametrize("location_type", ["Affiliate", "VSP"])
async def test_retail_canonical_profile_codes_do_not_grant_site_membership(tmp_path, location_type):
    from process.vsp_directory_enrichment import _normalize_offices
    from tests.test_vsp_directory_normalization import retail_office_and_profile

    site, _ = retail_office_and_profile()
    site["locationType"] = location_type
    site["locationDrProfileLink"] = office(52)["locationDrProfileLink"]
    http = RetailHTTP(repair=False)
    http.location_type = location_type
    http.network_ids, http.positive_networks = ("2000",), {"Signature"}
    run = VSPCapture(VSPCaptureStore(tmp_path, "synthetic-retail-canonical"), http, noop, noop)
    run.remember_search(site, "Signature", "a" * 64)
    await enrich_capture(run)
    assert run.memberships == {"Signature": {"52"}, "Choice": set(), "Advantage": set()}
    resources, npis, accounting, other_codes = await _normalize_offices(run)
    assert all(not values for values in npis.values())
    assert json.loads(resources["OrganizationAffiliation"]["vsp-site-network-52"])["network"] == [
        {"reference": "Organization/vsp-network-1000"}
    ]
    assert accounting["retail_locations"] == 1 and not other_codes
    assert not resources["Practitioner"] and not resources["PractitionerRole"]
    run.remember(office(53, {"pid": 9, "npi": FIRST["npi"]}), "b" * 64)
    with pytest.raises(VSPDirectoryError, match="^vsp_profile_membership_census_unresolved$"):
        await _normalize_offices(run)


@pytest.mark.asyncio
async def test_mixed_office_keeps_clinical_codes_out_of_retail_networks(tmp_path):
    from process.vsp_directory_enrichment import _normalize_offices
    from tests.test_vsp_directory_normalization import retail_office_and_profile

    site, _ = retail_office_and_profile()
    site.update(
        locationType="VSP", providerSummaries=[FIRST], locationDrProfileLink=office(52)["locationDrProfileLink"]
    )
    http = RetailHTTP(repair=False)
    http.location_type, http.clinician = "VSP", True
    http.network_ids, http.positive_networks = ("4000",), {"Signature"}
    store = VSPCaptureStore(tmp_path, "synthetic-mixed-retail")
    for expected in (3, 0):
        run = VSPCapture(store, http, noop, noop)
        run.remember_search(site, "Signature", "a" * 64)
        await enrich_capture(run)
        assert http.requests == expected
        assert run.site_memberships == {"Signature": {"52"}, "Choice": set(), "Advantage": set()}
        assert run.memberships == {"Signature": {"52"}, "Choice": {"52"}, "Advantage": set()}
        rows, npis, accounting, _ = await _normalize_offices(run)
        assert len(rows["Location"]) == len(rows["Practitioner"]) == len(rows["PractitionerRole"]) == 1
        assert json.loads(rows["OrganizationAffiliation"]["vsp-site-network-52"])["network"] == [
            {"reference": "Organization/vsp-network-1000"}
        ]
        assert npis == {"Signature": set(), "Choice": {"Practitioner/vsp-npi-1234567893"}, "Advantage": set()}
        assert accounting["retail_locations"] == 1
        http = RetailHTTP(repair=False)
