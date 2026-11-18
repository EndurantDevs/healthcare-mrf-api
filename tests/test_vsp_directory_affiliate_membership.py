# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Affiliate memberships require retained office-scoped search doctor evidence."""

from copy import deepcopy
from urllib.parse import parse_qs, urlsplit

import pytest

from process import vsp_directory_capture as acquisition
from process.vsp_directory_contract import NETWORKS, PROFILE_MEMBERSHIP_UNRESOLVED, SEARCH_URL, VSPDirectoryError
from process.vsp_directory_enrichment import enrich_capture, write_normalized_capture
from process.vsp_directory_geometry import office_search_request
from process.vsp_directory_normalization import VSPDirectoryNormalizationError
from process.vsp_directory_store import VSPCaptureStore
from tests.test_vsp_directory_capture import HTTP, TILE, noop
from tests.test_vsp_directory_enrichment import FIRST, SECOND, office
from tests.test_vsp_directory_normalization import profile, retail_office_and_profile


def affiliate(identifier, *summaries):
    office_by_field = {**office(identifier, *summaries), "locationType": "Affiliate"}
    office_by_field["addr"]["latitude"] += identifier / 1000
    return office_by_field


class AffiliateSource:
    def __init__(self, *, scopes=None, summaries=None, offices=(52,)):
        self.offices = {oid: affiliate(oid) for oid in offices}
        self.scopes = scopes if scopes is not None else {(52, name): (2, 3) for name in NETWORKS.values()}
        self.summaries = summaries or {2: FIRST, 3: SECOND}
        self.profiles = {
            oid: [
                {
                    **profile(did, "BRANCH ", "OPTION"),
                    "providerNetworks": [{"networkId": "BRANCH"}, {"networkId": "OPTION"}],
                }
                for did in (2, 3, 4)
            ]
            for oid in offices
        }
        self.capped = set()
        self.capped_present = False
        self.repair = None
        self.census_extra = 0
        self.location_type = "Affiliate"

    def response(self, method, url, payload):
        if method == "GET":
            if url.startswith(SEARCH_URL):
                assert self.repair is not None
                return self.repair
            oid = int(parse_qs(urlsplit(url).query)["officepublicid"][0])
            return self.profiles[oid]
        assert method == "POST" and url == SEARCH_URL and len(payload["networkNames"]) == 1
        network = payload["networkNames"][0]
        if payload["limit"] == "1":
            count = sum(bool(self.scopes.get((oid, network))) for oid in self.offices)
            return {"totalCounts": count + (self.census_extra if network == "Signature" else 0)}
        is_targeted = payload["radius"] == "0.05"
        selected_offices = [
            oid
            for oid, row in self.offices.items()
            if not is_targeted or str(row["addr"]["latitude"]) == payload["latitude"]
        ]
        rows = [
            affiliate(oid, *(self.summaries[did] for did in self.scopes[oid, network]))
            for oid in selected_offices
            if (oid, network) in self.scopes
        ]
        for row in rows:
            row["locationType"] = self.location_type
        if is_targeted and (selected_offices[0], network) in self.capped:
            rows = (rows[:1] if self.capped_present else []) + [office(100 + index) for index in range(50)]
            return {"totalCounts": 51, "locations": rows[:50]}
        return {"totalCounts": len(rows), "locations": rows}


def capture(tmp_path, source):
    store = VSPCaptureStore(tmp_path, "synthetic-affiliate")
    return acquisition.VSPCapture(store, HTTP(source), noop, noop)


def roles(resources):
    return {
        row["id"]: {item["valueReference"]["reference"].rsplit("-", 1)[1] for item in row["extension"]}
        for row in resources
        if row["resourceType"] == "PractitionerRole"
    }


@pytest.fixture(autouse=True)
def one_world_tile(monkeypatch):
    monkeypatch.setattr(acquisition, "world_tiles", lambda: (TILE,))


@pytest.mark.asyncio
@pytest.mark.parametrize("summary_only", [False, True])
async def test_unlocated_affiliate_uses_exact_network_scope_and_keeps_its_address(tmp_path, summary_only):
    publisher = AffiliateSource()
    source_office = affiliate(52, FIRST, SECOND)
    source_office["addr"].update(latitude=0.0, longitude=0.0)
    company_summaries = []
    if summary_only:
        source_office.update(name="Example Optical", serviceTypeCd="EC", parentCompanyCd="EXAMPLE", droStoreNumber="1")
        company_summaries.append({"pid": 9, "degree": "NA", "first": "Example", "last": "Optical", "npi": "1000000004"})
        source_office["providerSummaries"].extend(company_summaries)
        publisher.profiles[52] = []
    advantage_office_by_field = {**source_office, "providerSummaries": [FIRST, *company_summaries]}
    original_response = publisher.response

    def response(method, url, payload):
        assert method == "GET"
        if url == SEARCH_URL + "/52?networkNames=Advantage":
            return advantage_office_by_field
        if url == SEARCH_URL + "/52?networkNames=Choice":
            return {"pid": 0, "distance": 0}
        return original_response(method, url, payload)

    publisher.response = response
    run = capture(tmp_path, publisher)
    run.remember_search(source_office, "Signature", "a" * 64)
    await enrich_capture(run)
    assert run.http.requests == 3
    company_ids = {"9"} if summary_only else set()
    assert run.search_memberships["52"] == {
        "Signature": {"2", "3"} | company_ids,
        "Advantage": {"2"} | company_ids,
        "Choice": set(),
    }
    counts_by_network = {name: len(ids) for name, ids in run.memberships.items()}
    completed = await write_normalized_capture(run, counts_by_network, counts_by_network)
    resources = list(completed.iter_resources())
    locations = [resource for resource in resources if resource["resourceType"] == "Location"]
    (location,) = locations
    assert "position" not in location
    assert location["address"]["postalCode"] == source_office["addr"]["zipCode"]
    assert sorted(roles(resources).values(), key=len) == [{"1000"}, {"1000", "4000"}]
    if summary_only:
        site = next(resource for resource in resources if resource["id"] == "vsp-site-org-52")
        assert {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000004"} in site["identifier"]
        assert all(resource["id"] != "vsp-doctor-9" for resource in resources)


@pytest.mark.asyncio
async def test_unlocated_store_lookup_can_omit_doctor_summaries(tmp_path):
    source = AffiliateSource()
    site, company = retail_office_and_profile()
    site["addr"].update(latitude=0.0, longitude=0.0)
    site["locationDrProfileLink"] = source.offices[52]["locationDrProfileLink"]
    source.profiles[52] = [company]
    source.repair = {key: value for key, value in site.items() if key != "providerSummaries"}
    run = capture(tmp_path, source)
    run.remember_search(site, "Signature", "a" * 64)
    await enrich_capture(run)
    assert run.http.requests == 4
    assert run.site_memberships == {name: {"52"} for name in NETWORKS.values()}
    assert run.offices["52"]["providerSummaries"] == []
    saved, _ = run.store.lookup("GET", SEARCH_URL + "/52?networkNames=Advantage")
    assert "providerSummaries" not in saved


@pytest.mark.asyncio
async def test_affiliate_capture_uses_only_search_doctors_and_retained_targeted_scopes(tmp_path):
    source = AffiliateSource()
    original_profiles = deepcopy(source.profiles)
    run = capture(tmp_path, source)
    await run.discover("Signature", 1)
    await enrich_capture(run)
    assert run.http.requests == 4
    assert run.search_memberships == {"52": {name: {"2", "3"} for name in NETWORKS.values()}}
    assert run.profiles["52"] == original_profiles[52] == source.profiles[52]
    assert "search:52" in run._sizes
    census_by_network = dict.fromkeys(NETWORKS.values(), 1)
    completed = await write_normalized_capture(run, census_by_network, census_by_network)
    assert list(roles(completed.iter_resources()).values()) == [{"1000", "2000", "4000"}] * 2
    assert completed.proof["outside_profile_count"] == 1
    assert completed.manifest["other_network_ids"] == {"BRANCH": 1, "OPTION": 1}
    resumed = capture(tmp_path, source)
    await resumed.discover("Signature", 1)
    await enrich_capture(resumed)
    assert resumed.http.requests == 0 and resumed.cache_hits == 4
    assert resumed.search_memberships == run.search_memberships
    # The production transport restores the retained campaign's request count.
    resumed.http.requests = run.http.requests
    replay = await write_normalized_capture(resumed, census_by_network, census_by_network)
    assert list(replay.iter_resources()) == list(completed.iter_resources())


@pytest.mark.asyncio
async def test_affiliate_scopes_close_all_networks_without_extra_world_searches(tmp_path):
    run = capture(tmp_path, AffiliateSource())
    completed = await run.run()
    spatial_queries = [body for method, _, body in run.http.calls if method == "POST" and body["limit"] == "50"]
    assert len(spatial_queries) == 3
    assert {body["networkNames"][0] for body in spatial_queries} == set(NETWORKS.values())
    assert spatial_queries[0]["radius"] != "0.05"
    assert [body["radius"] for body in spatial_queries[1:]] == ["0.05", "0.05"]
    assert completed.proof["publisher_locations"] == dict.fromkeys(NETWORKS.values(), 1)
    assert completed.proof["network_npis"] == dict.fromkeys(NETWORKS.values(), 2)


@pytest.mark.asyncio
async def test_other_network_enrichment_preserves_geographic_census_addition(tmp_path):
    source = AffiliateSource(
        scopes={(52, "Signature"): (2,), (52, "Advantage"): (2,), (53, "Choice"): (2,), (53, "Signature"): (3,)},
        offices=(52, 53),
    )
    original_response = source.response

    def response(method, url, payload):
        if method == "POST" and payload["networkNames"] == ["Signature"] and payload["radius"] != "0.05":
            if payload["limit"] == "1":
                return {"totalCounts": 1}
            return {"totalCounts": 1, "locations": [affiliate(52, FIRST)]}
        return original_response(method, url, payload)

    source.response = response
    run = capture(tmp_path, source)
    completed = await run.run()
    assert completed.proof["publisher_locations"] == {"Signature": 2, "Choice": 1, "Advantage": 1}
    assert completed.proof["geographically_verified_additions"] == {"Signature": 1, "Choice": 0, "Advantage": 0}
    assert run.http.requests == 14


@pytest.mark.asyncio
async def test_early_affiliate_scope_preserves_only_the_extra_geographic_site(tmp_path):
    source = AffiliateSource(
        scopes={
            (52, "Signature"): (2,),
            (53, "Signature"): (2,),
            (52, "Choice"): (2,),
            (53, "Choice"): (3,),
            (52, "Advantage"): (2,),
        },
        offices=(52, 53),
    )
    original_response = source.response

    def response(method, url, payload):
        if method == "POST" and payload["networkNames"] == ["Choice"] and payload["radius"] != "0.05":
            if payload["limit"] == "1":
                return {"totalCounts": 1}
            return {"totalCounts": 1, "locations": [affiliate(52, FIRST)]}
        return original_response(method, url, payload)

    source.response = response
    completed = await capture(tmp_path, source).run()
    assert completed.proof["publisher_locations"] == {"Signature": 2, "Choice": 2, "Advantage": 1}
    assert completed.proof["geographically_verified_additions"] == {"Signature": 0, "Choice": 1, "Advantage": 0}


@pytest.mark.asyncio
async def test_each_network_uses_its_incoming_summaries_before_office_merge(tmp_path):
    source = AffiliateSource(scopes={(52, "Signature"): (2, 3), (52, "Choice"): (3,)})
    run = capture(tmp_path, source)
    await run.discover("Signature", 1)
    await enrich_capture(run)
    assert run.search_memberships == {"52": {"Signature": {"2", "3"}, "Choice": {"3"}, "Advantage": set()}}
    census_by_network = {"Signature": 1, "Choice": 1, "Advantage": 0}
    completed = await write_normalized_capture(run, census_by_network, census_by_network)
    assert roles(completed.iter_resources()) == {
        "vsp-role-2-52-1234567893": {"1000"},
        "vsp-role-3-52-1999999992": {"1000", "2000"},
    }


@pytest.mark.asyncio
async def test_get_identity_repair_never_expands_affiliate_membership(tmp_path):
    source = AffiliateSource(scopes={(52, "Signature"): (2,)}, summaries={2: {"pid": 2, "npi": None}})
    unscoped_summary_by_field = {"pid": 5, "npi": "1000000004"}
    source.repair = affiliate(52, FIRST, SECOND, unscoped_summary_by_field)
    run = capture(tmp_path, source)
    await run.discover("Signature", 1)
    await enrich_capture(run)
    assert run.search_memberships == {"52": {"Signature": {"2"}, "Choice": set(), "Advantage": set()}}
    assert {row["pid"] for row in run.offices["52"]["providerSummaries"]} == {2, 3}
    saved, _ = run.store.lookup("GET", SEARCH_URL + "/52?networkNames=Signature")
    assert saved["providerSummaries"][-1] == unscoped_summary_by_field
    assert run.http.calls[-1][0:2] == ("GET", SEARCH_URL + "/52?networkNames=Signature")
    census_by_network = {"Signature": 1, "Choice": 0, "Advantage": 0}
    completed = await write_normalized_capture(run, census_by_network, census_by_network)
    assert list(roles(completed.iter_resources()).values()) == [{"1000"}]
    assert completed.proof["outside_profile_count"] == 2
    requests = run.http.requests
    await enrich_capture(run)
    assert run.http.requests == requests


@pytest.mark.asyncio
async def test_affiliate_without_search_doctor_ids_remains_unresolved(tmp_path):
    source = AffiliateSource(scopes={(52, "Signature"): ()})
    run = capture(tmp_path, source)
    await run.discover("Signature", 1)
    with pytest.raises(VSPDirectoryError, match="^" + PROFILE_MEMBERSHIP_UNRESOLVED + "$"):
        await enrich_capture(run)
    assert run.store.load_checkpoint("complete") is None
    assert not any(run.search_memberships["52"].values())


@pytest.mark.asyncio
async def test_shared_doctor_identity_never_crosses_office_network_scopes(tmp_path):
    source = AffiliateSource(scopes={(52, "Signature"): (2,), (53, "Choice"): (2,)}, offices=(52, 53))
    run = capture(tmp_path, source)
    run.remember_search(affiliate(52, FIRST), "Signature", "a" * 64)
    run.remember_search(affiliate(53, {"pid": 2, "npi": None}), "Choice", "b" * 64)
    await enrich_capture(run)
    assert run.memberships == {"Signature": {"52"}, "Choice": {"53"}, "Advantage": set()}
    assert len(run.identity_reuses) == 1
    assert all(method != "GET" or not url.startswith(SEARCH_URL) for method, url, _ in run.http.calls)
    census_by_network = {"Signature": 1, "Choice": 1, "Advantage": 0}
    completed = await write_normalized_capture(run, census_by_network, census_by_network)
    assert roles(completed.iter_resources()) == {
        "vsp-role-2-52-1234567893": {"1000"},
        "vsp-role-2-53-1234567893": {"2000"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("target_present", [False, True])
async def test_capped_targeted_search_proves_presence_only(tmp_path, target_present):
    source = AffiliateSource()
    source.capped.add((52, "Advantage"))
    source.capped_present = target_present
    run = capture(tmp_path, source)
    await run.discover("Signature", 1)
    if target_present:
        await enrich_capture(run)
        assert run.memberships["Advantage"] == {"52"}
    else:
        with pytest.raises(VSPDirectoryError, match="^" + PROFILE_MEMBERSHIP_UNRESOLVED + "$"):
            await enrich_capture(run)
        assert "Advantage" not in run.search_memberships["52"]
        assert run.store.load_checkpoint("complete") is None
    assert set(run.offices) == {"52"}


@pytest.mark.asyncio
@pytest.mark.parametrize("difference", [1, 10, 11])
async def test_census_shortfall_requires_complete_sweep_and_stays_bounded(tmp_path, difference):
    source = AffiliateSource()
    source.census_extra = difference
    run = capture(tmp_path, source)
    if difference == 11:
        with pytest.raises(VSPDirectoryError, match="^vsp_office_census_unresolved$"):
            await run.run()
        assert run.store.load_checkpoint("complete") is None
    else:
        completed = await run.run()
        assert completed.proof["closed_geographic_census_shortfalls"] == {"Signature": difference}
        assert completed.proof["publisher_locations"] == dict.fromkeys(NETWORKS.values(), 1)
    assert run.memberships == {name: {"52"} for name in NETWORKS.values()}
    assert run.unresolved == []


@pytest.mark.asyncio
async def test_search_doctor_without_office_profile_still_refuses_output(tmp_path):
    source = AffiliateSource()
    source.profiles[52] = source.profiles[52][:1]
    source.summaries[3] = {"pid": 3}
    run = capture(tmp_path, source)
    await run.discover("Signature", 1)
    with pytest.raises(VSPDirectoryNormalizationError, match="^vsp_directory_unresolved_doctor$"):
        await enrich_capture(run)


@pytest.mark.asyncio
async def test_summary_only_vsp_doctors_replay_scopes_without_origin(tmp_path):
    source = AffiliateSource(scopes={(52, "Signature"): (2, 3), (52, "Choice"): (2,), (52, "Advantage"): (2,)})
    source.location_type = "VSP"
    source.profiles[52] = []
    for expected in (4, 0):
        run = capture(tmp_path, source)
        await run.discover("Signature", 1)
        await enrich_capture(run)
        assert run.http.requests == expected
        if expected:
            continue
        census_by_network = {name: len(ids) for name, ids in run.memberships.items()}
        completed = await write_normalized_capture(run, census_by_network, census_by_network)
        assert completed.resource_counts["Practitioner"] == completed.resource_counts["PractitionerRole"] == 2
        assert roles(list(completed.iter_resources())) == {
            "vsp-role-2-52-1234567893": {"1000", "2000", "4000"},
            "vsp-role-3-52-1999999992": {"1000"},
        }


@pytest.mark.asyncio
async def test_missing_profile_record_uses_verified_summaries_and_replays(tmp_path):
    from tests.test_vsp_directory_http_store import MISSING_PROFILE

    source = AffiliateSource()
    source.location_type = "VSP"
    source.profiles[52] = MISSING_PROFILE

    class MissingProfileHTTP(HTTP):
        async def request(self, method, url, body=None):
            value, raw, metadata = await super().request(method, url, body)
            if method == "GET":
                metadata["http_status"] = 404
            return value, raw, metadata

    store = VSPCaptureStore(tmp_path, "synthetic-missing-profile-office")
    for expected in (4, 0):
        run = acquisition.VSPCapture(store, MissingProfileHTTP(source), noop, noop)
        await run.discover("Signature", 1)
        await enrich_capture(run)
        assert run.http.requests == expected and run.profiles == {"52": []}
        if not expected:
            counts_by_network = {name: len(ids) for name, ids in run.memberships.items()}
            completed = await write_normalized_capture(run, counts_by_network, counts_by_network)
            assert completed.resource_counts["Practitioner"] == completed.resource_counts["PractitionerRole"] == 2


def test_search_memberships_share_the_existing_source_index_budget(tmp_path):
    run = capture(tmp_path, AffiliateSource())
    run._total_bytes = 512 * 1024 * 1024
    with pytest.raises(VSPDirectoryError, match="^vsp_source_index_byte_budget_exhausted$"):
        run.remember_search_scope("52", "Signature", {"2"})


def test_targeted_search_preserves_source_position_at_coordinate_boundaries():
    row = affiliate(52)
    row["addr"].update(latitude=90.0, longitude=-180.0)
    payload = office_search_request(row, "Choice")
    assert (payload["latitude"], payload["longitude"], payload["radius"]) == ("90.0", "-180.0", "0.05")
    assert payload["networkNames"] == ["Choice"] and payload["locationType"] == ["VSP", "Affiliate"]
