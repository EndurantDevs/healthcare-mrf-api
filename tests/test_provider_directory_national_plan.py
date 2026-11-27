# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check national ZIP coverage, retained scopes and publisher-specific request budgets."""

import hashlib
import json

import pytest

from process.provider_directory_national_plan import delta_plan, main, nomi_plan
from process.provider_directory_zip_inventory import national_zip_codes
from process.vsp_directory_geometry import distance_miles, world_tiles


def test_published_zip_inventory_keeps_leading_zeroes_and_territories():
    codes = national_zip_codes()
    assert len(codes) == len(set(codes)) == 41515
    assert {"00501", "00926", "00802", "96913", "96950", "96799", "99747"} <= set(codes)
    assert hashlib.sha256(("\n".join(codes) + "\n").encode()).hexdigest() == (
        "e230cd5c7298738c28575b975d8c61a78529cc7d2318162608ba67ca11c1d3e2"
    )


def test_nomi_national_supplement_preserves_windows_and_request_limits():
    payload = {
        "group_id": "fixture",
        "searches": [{"zip_code": "99747", "radius_miles": 25}, {"zip_code": "00926", "radius_miles": 250}],
        "page_size": 100,
        "requests_per_second": 0.5,
        "max_origin_requests": 12,
    }
    plan = nomi_plan(payload)
    assert plan["searches"] == payload["searches"] + [
        {"zip_code": "99747", "radius_miles": 50},
        {"zip_code": "96913", "radius_miles": 250},
        {"zip_code": "96799", "radius_miles": 250},
    ]
    assert nomi_plan(plan) == plan
    with pytest.raises(ValueError, match="authorized_manifest_invalid"):
        nomi_plan({**payload, "max_origin_requests": 4})


def test_delta_expands_only_a_new_edition_and_respects_its_explicit_budget(monkeypatch):
    monkeypatch.setattr(
        "process.provider_directory_national_plan.national_zip_codes", lambda: ("00501", "00926", "99747")
    )
    payload = {
        "authorization_scope": "fixture",
        "data_edition": "old",
        "zip_inventory": ["99747", "01234"],
        "radius_miles": 30,
        "selector_scope": "all-only",
        "max_origin_requests": 2,
    }
    plan = delta_plan(payload, "new", 4)
    assert plan == {
        **payload,
        "zip_inventory": ["99747", "01234", "00501", "00926"],
        "data_edition": "new",
        "max_origin_requests": 4,
    }
    assert payload["zip_inventory"] == ["99747", "01234"]
    with pytest.raises(ValueError, match="new Delta directory edition"):
        delta_plan(payload, "old", 4)
    with pytest.raises(ValueError, match="authorized_manifest_invalid"):
        delta_plan(payload, "new", 3)


def test_cli_writes_new_plan_without_overwriting_retained_input(tmp_path):
    previous = tmp_path / "previous.json"
    previous.write_text(
        json.dumps(
            {
                "group_id": "example",
                "searches": [{"zip_code": "01234", "radius_miles": 250}],
                "requests_per_second": 1,
                "page_size": 500,
                "max_origin_requests": 20,
            }
        )
    )
    original = previous.read_bytes()
    with pytest.raises(FileExistsError):
        main(["nomi", str(previous), str(previous)])
    assert previous.read_bytes() == original
    destination = tmp_path / "next.json"
    main(["nomi", str(previous), str(destination)])
    assert len(json.loads(destination.read_text())["searches"]) == 5
    assert not destination.stat().st_mode & 0o077


def test_global_vsp_tiles_include_the_remote_search_centers():
    for lat, lon in ((70.1188, -143.6703), (18.3469, -66.0543), (13.4783, 144.8152), (-14.2198, -170.3701)):
        tile = next(tile for tile in world_tiles() if tile.has_point(lat, lon))
        assert distance_miles(*tile.center, lat, lon) <= float(tile.request("Signature")["radius"])
