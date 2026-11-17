# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Repeated office observations keep the first linked profile across a restart."""

from copy import deepcopy

import pytest

from process import vsp_directory_capture as acquisition
from process.vsp_directory_contract import ENDPOINT_SCOPE, SEARCH_URL, SOURCE_ID, VSPDirectoryError
from process.vsp_directory_geometry import GeoTile, census_request
from process.vsp_directory_store import VSPCaptureStore
from tests.test_vsp_directory_capture import HTTP, TILE, Source, Store, noop


@pytest.fixture
def mixed_date_capture(monkeypatch, tmp_path):
    """Provide overlapping office dates and one interrupted profile request."""
    publisher = Source(fail_profile="102")
    second_tile = GeoTile(0, 10, 5, 15)
    first_office = publisher.offices["101"]
    first_office["providerSummaries"] = []
    first_url = first_office["locationDrProfileLink"]["href"]
    later_office_by_field = {
        **first_office,
        "name": "Later Synthetic Office",
        "locationDrProfileLink": {"href": first_url.replace("2035-01-01", "2035-01-02")},
    }
    repair_url = SEARCH_URL + "/101?networkNames=Advantage"
    for office_id in ("102", "103"):
        publisher.offices[office_id]["addr"]["longitude"] = 14.0
    original_response = publisher.response

    def response(method, url, request_by_field):
        if method == "POST" and request_by_field == TILE.request("Signature"):
            return {"totalCounts": 1, "locations": [first_office]}
        if method == "POST" and request_by_field == second_tile.request("Signature"):
            return {
                "totalCounts": 3,
                "locations": [later_office_by_field, *[publisher.offices[key] for key in ("102", "103")]],
            }
        if url == repair_url:
            return {
                **later_office_by_field,
                "locationDrProfileLink": {"href": first_url.replace("2035-01-01", "2035-01-03")},
                "providerSummaries": [{"pid": 3, "npi": "1999999992"}],
            }
        reply = original_response(method, url, request_by_field)
        if url == first_url:
            reply[0]["doctorPublicId"] = 3
        return reply

    publisher.response = response
    http = HTTP(publisher)

    async def exchange(_http, method, url, body, _selection):
        return await http.request(method, url, body), None

    monkeypatch.setattr(acquisition, "world_tiles", lambda: (TILE, second_tile))
    monkeypatch.setattr(acquisition, "configured_root", lambda: tmp_path)
    monkeypatch.setattr(acquisition.VSPHTTP, "_exchange", exchange)
    monkeypatch.setattr(acquisition.VSPHTTP, "wait", noop)
    capture_by_argument = {
        "source_record": {"source_id": SOURCE_ID, "canonical_api_base": ENDPOINT_SCOPE},
        "root_run_id": "mixed-date-office",
        "proxy_binding": None,
        "progress_callback": noop,
        "cancel_check": noop,
    }
    return capture_by_argument, publisher, http, first_url, repair_url


@pytest.mark.asyncio
async def test_mixed_date_office_uses_first_profile_through_repair_and_restart(
    monkeypatch, tmp_path, mixed_date_capture
):
    capture_by_argument, publisher, http, first_url, repair_url = mixed_date_capture
    with pytest.raises(VSPDirectoryError, match="^synthetic_partial_failure$"):
        await acquisition.capture_vsp_directory(**capture_by_argument)
    store = VSPCaptureStore(tmp_path, capture_by_argument["root_run_id"], require_retained=True)
    receipts = list(store.requests())
    assert len(receipts) == 7 and store.load_checkpoint("complete") is None
    assert {receipt["url"] for receipt in receipts if receipt["method"] == "GET"} == {first_url, repair_url}
    boundary = len(http.calls)

    completed = await acquisition.capture_vsp_directory(**capture_by_argument, require_retained=True)
    assert http.calls[boundary:] == [
        *(("GET", publisher.offices[office_id]["locationDrProfileLink"]["href"], None) for office_id in ("102", "103")),
        *(("POST", SEARCH_URL, census_request(name)) for name in sorted(publisher.counts)),
    ]
    resources = list(completed.iter_resources())
    locations_by_id = {resource["id"]: resource for resource in resources if resource["resourceType"] == "Location"}
    assert locations_by_id["vsp-office-101"]["name"] == publisher.offices["101"]["name"]
    assert any(resource["id"] == "vsp-role-3-101-1999999992" for resource in resources)
    assert completed.proof["publisher_locations"] == publisher.counts
    assert completed.proof["network_npis"] == {"Signature": 2, "Choice": 2, "Advantage": 1}
    assert completed.proof["retained_response_count"] == 12

    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", lambda **_: pytest.fail("completed replay opened HTTP"))
    reopened = await acquisition.capture_vsp_directory(**capture_by_argument, require_retained=True)
    assert list(reopened.iter_resources()) == resources and reopened.proof == completed.proof


@pytest.mark.parametrize(
    ("defect", "code"),
    [
        ("office", "vsp_profile_office_mismatch"),
        ("date", "vsp_profile_url_invalid"),
        ("position", "vsp_office_position_changed"),
        ("npi", "vsp_doctor_identity_changed"),
    ],
)
def test_later_dated_office_still_validates_links_location_and_npi(defect, code):
    first = Source().offices["101"]
    run = acquisition.VSPCapture(Store(), None, noop, noop)
    run.remember_search(first, "Signature", "a" * 64)
    later = deepcopy(first)
    later["locationDrProfileLink"]["href"] = later["locationDrProfileLink"]["href"].replace("2035-01-01", "2035-01-02")
    if defect == "office":
        later["locationDrProfileLink"]["href"] = later["locationDrProfileLink"]["href"].replace(
            "officepublicid=101", "officepublicid=102"
        )
    elif defect == "date":
        later["locationDrProfileLink"]["href"] = later["locationDrProfileLink"]["href"].replace(
            "2035-01-02", "2035-99-99"
        )
    elif defect == "position":
        later["addr"]["latitude"] = 6.0
    else:
        later["providerSummaries"][0]["npi"] = "1999999992"
    with pytest.raises(VSPDirectoryError, match="^" + code + "$"):
        run.remember_search(later, "Choice", "b" * 64)
