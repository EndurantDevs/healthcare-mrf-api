# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact structured publisher groups, with no rendered-link or campus inference."""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import hospital_hpt_locator as locator
from process.hospital_price_pipeline import refreshed_locator_candidates
from tests.hospital_price_control_support import acquisition_module


PAGE = locator.HCA_STRUCTURED_LOCATOR_URL
SELECTOR = "https://files.example/mental-health.json"
SIGNED = SELECTOR + "?si=policy&spr=https&sv=2026-02-06&sr=c&sig=a%2Bb%2fc%2Fd%3D"


def _facilities():
    return [
        {"locations": [{"locationName": "Acute Hospital", "isParentFacility": True},
                       {"locationName": "Acute Annex", "isParentFacility": False}],
         "mrfPriceTransparencyDownloadURL": "https://files.example/acute.json?sig=acute"},
        {"locations": [{"locationName": "Mental Health Hospital", "isParentFacility": True}],
         "mrfPriceTransparencyDownloadURL": SIGNED},
    ]


def _component(facilities):
    return {"componentName": "PricingTransparencyBlockPOC", "fields": {"facilities": facilities}}


def _document(component):
    return {"props": {"pageProps": {"layoutData": {"sitecore": {"route": {
        "placeholders": {"body": [{"componentName": "Layout", "placeholders": {
            "column": [component], "unrelated": [{"componentName": "Navigation"}]
        }}]}
    }}}}}}


def _page(document=None):
    document = _document(_component(_facilities())) if document is None else document
    return ('<!doctype html><html><body><br/><a href="https://files.example/wrong.json">'
            'Mental Health Hospital</a><script>ignored()</script>'
            '<script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(document) + '</script></body></html>').encode()


def _parse(payload):
    return locator.parse_hospital_locator_source(payload, cms_hpt_url=PAGE)


def test_structured_groups_preserve_literal_pairs():
    expected_pairs = [(location["locationName"], facility["mrfPriceTransparencyDownloadURL"])
                for facility in _facilities() for location in facility["locations"]]
    assert [(record.location_name, record.mrf_url) for record in _parse(_page())] == expected_pairs
    escaped = _page().replace(b"https://", b"https:\\/\\/")
    assert _parse(escaped) == _parse(_page())


@pytest.mark.parametrize("requested_page", (None, PAGE + "?view=all", "https://other.example/prices"))
def test_structured_dispatch_requires_exact_page(requested_page):
    with pytest.raises(locator.HospitalHptLocatorError):
        locator.parse_hospital_locator_source(_page(), cms_hpt_url=requested_page)
    text = b"location-name: Hospital\nmrf-url: https://files.example/a.json?token=a:b\n"
    assert locator.parse_hospital_locator_source(text, cms_hpt_url=requested_page) == (
        locator.HospitalHptLocatorRecord("Hospital", "https://files.example/a.json?token=a:b"),
    )


@pytest.mark.parametrize("payload", (
    b"<html><body>Access denied</body></html>",
    _page().replace(b'type="application/json"', b'type="text/plain"'),
    _page().replace(b'id="__NEXT_DATA__"', b'id="__NEXT_DATA__" id="other"'),
    _page().replace(b'type="application/json"', b'type="application/json" type'),
    _page() + _page(),
    _page().replace(b'</script></body>', b''),
    b'<script id="__NEXT_DATA__" type="application/json"/>',
    _page().replace(b'</script></body>', b' trailing</script></body>'),
    _page().replace(b'"props":', b'"props": {}, "props":'),
    _page().replace(b'"Mental Health Hospital"', b'"Mental\tHealth Hospital"'),
    _page().replace(b'"Mental Health Hospital"', b'NaN'),
    _page({}), _page([]),
))
def test_structured_script_rejects_invalid_envelopes(payload):
    with pytest.raises(locator.HospitalHptLocatorError):
        _parse(payload)


@pytest.mark.parametrize("component", (
    None, {}, {"componentName": "Other"},
    {"componentName": "Other", "placeholders": []},
    {"componentName": "Other", "placeholders": {"body": {}}},
    {"componentName": "PricingTransparencyBlockPOC", "fields": None},
    _component(None), _component({}), _component([]),
    {**_component(_facilities()), "placeholders": {"nested": [_component(_facilities())]}},
))
def test_structured_component_requires_unique_shape(component):
    with pytest.raises(locator.HospitalHptLocatorError):
        _parse(_page(_document(component)))


@pytest.mark.parametrize("route", (None, {}, {"placeholders": {}, "componentName": "PricingTransparencyBlockPOC"}))
def test_structured_route_requires_placeholder_tree(route):
    document = _document(_component(_facilities()))
    document["props"]["pageProps"]["layoutData"]["sitecore"]["route"] = route
    with pytest.raises(locator.HospitalHptLocatorError):
        _parse(_page(document))


@pytest.mark.parametrize("field,value", (
    ("locations", None), ("locations", {}), ("locations", []), ("locations", [None]),
    ("mrfPriceTransparencyDownloadURL", None), ("mrfPriceTransparencyDownloadURL", 3),
    ("mrfPriceTransparencyDownloadURL", ""), ("mrfPriceTransparencyDownloadURL", " /relative"),
    ("mrfPriceTransparencyDownloadURL", "https://user:pass@files.example/a.json"),
    ("mrfPriceTransparencyDownloadURL", "https://files.example:bad/a.json"),
    ("mrfPriceTransparencyDownloadURL", "https://files.example/a.json#fragment"),
    ("mrfPriceTransparencyDownloadURL", "https://files.example/a.json?facility=other"),
    ("mrfPriceTransparencyDownloadURL", "https://files.example/a.json?sig=a\x00b"),
))
def test_structured_facility_rejects_invalid_fields(field, value):
    facilities = _facilities()
    facilities[1][field] = value
    with pytest.raises(locator.HospitalHptLocatorError):
        _parse(_page(_document(_component(facilities))))


@pytest.mark.parametrize("field,value", (
    ("locationName", None), ("locationName", 1), ("locationName", ""),
    ("locationName", " Hospital "), ("locationName", "Hospital\nAnnex"),
    ("locationName", "&#32;"), ("locationName", "Hospital\x00"), ("locationName", "Hospital\ud800"),
    ("isParentFacility", "true"), ("isParentFacility", 1), ("isParentFacility", None),
))
def test_structured_location_rejects_invalid_fields(field, value):
    facilities = _facilities()
    facilities[1]["locations"][0][field] = value
    with pytest.raises(locator.HospitalHptLocatorError):
        _parse(_page(_document(_component(facilities))))


@pytest.mark.parametrize("duplicate", (
    {"locations": [{"locationName": "Other Campus", "isParentFacility": True}],
     "mrfPriceTransparencyDownloadURL": SIGNED},
    {"locations": [{"locationName": "Other Campus", "isParentFacility": True}],
     "mrfPriceTransparencyDownloadURL": SELECTOR + "?sig=different"},
    {"locations": [{"locationName": "MENTAL HEALTH HOSPITAL", "isParentFacility": True}],
     "mrfPriceTransparencyDownloadURL": "https://files.example/different.json"},
    {"locations": [{"locationName": "Other Campus", "isParentFacility": True}]},
    {"locations": [{"locationName": "Other Campus"}],
     "mrfPriceTransparencyDownloadURL": "https://files.example/different.json"},
    {"locations": [{"locationName": "Other Campus", "isParentFacility": True}],
     "mrfPriceTransparencyDownloadURL": "https://files.example/different.json", "unpairedDownload": "other"},
))
def test_structured_groups_reject_ambiguous_pairs(duplicate):
    facilities = [*_facilities(), duplicate]
    with pytest.raises(locator.HospitalHptLocatorError):
        _parse(_page(_document(_component(facilities))))


@pytest.mark.parametrize("payload", (None, b'\xff', b' ' * 1_000_001))
def test_structured_payload_keeps_original_bounds(payload):
    with pytest.raises(locator.HospitalHptLocatorError):
        _parse(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "page", "selector", "body"))
async def test_structured_fetch_preserves_raw_observation(tmp_path, monkeypatch, failure):
    acquisition = acquisition_module()
    page_url = PAGE if failure != "page" else "https://other.example/prices"
    payload = _page() if failure != "body" else b'<html>Access denied</html>'
    raw_path = tmp_path / "page.html"
    raw_path.write_bytes(payload)
    raw = SimpleNamespace(raw_path=raw_path, byte_count=len(payload),
                          raw_sha256=hashlib.sha256(payload).hexdigest(), head=None)
    download = AsyncMock(return_value=raw)
    observation = AsyncMock()
    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    monkeypatch.setattr(acquisition, "_record_locator_observation", observation)
    hospital_by_field = {"hospital_id": "a", "name": "Catalog Mental Facility", "cms_hpt_url": page_url,
                "locator_mrf_url": SELECTOR if failure != "selector" else "https://files.example/absent.json"}
    result = await acquisition.fetch_locator((page_url, (hospital_by_field,)), object())
    candidate, = acquisition.candidates_from_locators((result,))
    assert download.await_count == 1
    assert download.call_args.args[0] == page_url
    assert download.call_args.kwargs["max_bytes"] == 1_000_000
    assert raw_path.read_bytes() == payload
    assert observation.call_args.args[4] is raw
    assert result.fetch_failed is False
    assert candidate.observation_id == result.observation_id
    assert candidate.locator_name == (hospital_by_field["name"] if failure else None)
    assert candidate.initial_error_code == (
        "hospitalhptlocator" if failure in {"page", "body"} else "locator_unmatched" if failure else None
    )
    assert (result.records is None) == (failure in {"page", "body"})
    assert candidate.source_url == (page_url if failure else SIGNED)


@pytest.mark.asyncio
async def test_structured_refresh_uses_normal_page_dispatch(tmp_path, monkeypatch):
    acquisition = acquisition_module()
    raw_path = tmp_path / "page.html"
    raw_path.write_bytes(_page())
    download = AsyncMock(return_value=SimpleNamespace(raw_path=raw_path, head=None))
    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    monkeypatch.setattr(acquisition, "_record_locator_observation", AsyncMock())
    attempt = SimpleNamespace(hospital_id="a", hospital_name="Catalog Mental Facility", locator_name=None)
    operations = SimpleNamespace(fetch_locator=acquisition.fetch_locator,
                                 candidates_from_locators=acquisition.candidates_from_locators)
    refreshed = await refreshed_locator_candidates(
        PAGE, (attempt,), object(), SELECTOR, {SELECTOR + "?sig=expired"}, operations,
    )
    assert set(refreshed) == {"a"}
    assert refreshed["a"].source_url == SIGNED
    assert refreshed["a"].locator_name is None
    assert download.call_args.args[0] == PAGE
    assert await refreshed_locator_candidates(PAGE, (attempt,), object(), SELECTOR, {SIGNED}, operations) == {}
