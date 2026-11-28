# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic, offline coverage for scoped directory acquisition and restart."""

import asyncio
import gzip
import json
import sqlite3
from collections import Counter
from contextlib import closing
from unittest.mock import AsyncMock, Mock

import pytest
from shapely.geometry import MultiPolygon, box, mapping

from process import guardian_directory_capture as acquisition
from process import guardian_directory_geography as geography
from process import guardian_directory_sweep as sweep
from process.guardian_directory_store import GuardianCapture
from process.vsp_directory_contract import canonical_json
from process.vsp_directory_geometry import GeoTile

SELECTORS = (("PLAN-A", ""), ("PLAN-A", "CLIENT-X"), ("PLAN-B", ""))


@pytest.fixture
def capture():
    with closing(sqlite3.connect(":memory:")) as db:
        yield GuardianCapture(db, "edition", 100, fresh=True)


def _card(number="001", latitude=40.0, longitude=-80.0):
    return {
        "denMastId": number,
        "practiceId": "009",
        "planType": "PLAN-A",
        "cardType": "P",
        "latitude": latitude,
        "longitude": longitude,
        "distance": 0.0,
    }


def _plan(tmp_path, rows, **options):
    from process.guardian_directory_capture import _MANIFEST

    manifest = tmp_path / _MANIFEST.name
    selectors = [{"planType": plan, **({"clientType": client} if client else {})} for plan, client in SELECTORS]
    manifest.write_text(
        canonical_json(
            {"regions": rows, "selectors": selectors, "requests_per_second": 1, "max_origin_requests": 1000, **options}
        )
    )
    return sweep.load_plan(manifest, tmp_path)


@pytest.fixture
def plan_regions(tmp_path, monkeypatch):
    """Provide independent synthetic boundaries and postal records for the normal plan loader."""
    regions, features = [], []
    postal_rows = ["PHYSICAL STATE,PHYSICAL ZIP"]
    for index, state in enumerate(sorted(sweep.JURISDICTIONS)):
        zip_code = f"{index:05d}"
        postal_rows.append(f"{state},{zip_code}")
        bounds = [[0, 2, 0, 2]]
        if state == "AK":
            bounds.extend([[0, 1, 179, 180], [0, 1, -180, -179]])
        polygons = [box(west, south, east, north) for south, north, west, east in bounds]
        features.append({"properties": {"STUSPS": state}, "geometry": mapping(MultiPolygon(polygons))})
        regions.extend({"state": state, "zip_code": zip_code, "bounds": rectangle} for rectangle in bounds)
    reference = tmp_path / "census-state-boundaries-2025.geojson.gz"
    reference.write_bytes(gzip.compress(canonical_json({"features": features}).encode()))
    (tmp_path / "zip_codes_list.csv").write_text("\n".join(postal_rows))
    monkeypatch.setattr(geography, "_REFERENCE", tmp_path)
    return regions


def test_plan_requires_complete_geography(tmp_path, plan_regions):
    rows = plan_regions
    assert len(_plan(tmp_path, rows).regions) == len(rows)
    assert _plan(tmp_path, rows).max_concurrency == 1
    assert _plan(tmp_path, rows, max_concurrency=8).max_concurrency == 8
    for invalid_limit in (True, 0, 9, 1.5):
        with pytest.raises(ValueError, match="concurrency_invalid"):
            _plan(tmp_path, rows, max_concurrency=invalid_limit)
    for invalid in (rows[:-1], [*rows, rows[0]], [*rows[1:], {**rows[0], "zip_code": "123"}]):
        with pytest.raises(ValueError):
            _plan(tmp_path, invalid)


def test_geography_requires_the_interior_of_each_state(tmp_path, plan_regions):
    """A frame containing every corner still leaves the state's interior uncovered."""
    state = "PA"
    row = next(region for region in plan_regions if region["state"] == state)
    other_regions = [region for region in plan_regions if region["state"] != state]
    strips = [[0, 2, 0, 0.5], [0, 2, 1.5, 2], [0, 0.5, 0.5, 1.5], [1.5, 2, 0.5, 1.5]]
    with pytest.raises(ValueError, match="jurisdiction_coverage_incomplete"):
        _plan(tmp_path, other_regions + [{**row, "bounds": strip} for strip in strips])


@pytest.mark.parametrize("edge", [-180, 179])
def test_geography_requires_both_sides_of_date_line(tmp_path, plan_regions, edge):
    rows = [r for r in plan_regions if r["state"] != "AK" or r["bounds"][2] != edge]
    with pytest.raises(ValueError, match="jurisdiction_coverage_incomplete"):
        _plan(tmp_path, rows)


async def test_collapsed_plan_rejected_before_transport(tmp_path, monkeypatch, plan_regions):
    _plan(tmp_path, plan_regions)
    manifest = tmp_path / acquisition._MANIFEST.name
    document = json.loads(manifest.read_bytes())
    document["regions"] = list({row["state"]: {**row, "bounds": [0, 0.01, 0, 0.01]} for row in plan_regions}.values())
    manifest.write_text(canonical_json(document))
    monkeypatch.setattr(acquisition, "_MANIFEST", manifest)
    monkeypatch.setattr(acquisition, "_artifact_root", lambda: tmp_path)
    transport = Mock()
    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", transport)
    with pytest.raises(ValueError, match="jurisdiction_coverage_incomplete"):
        async with acquisition.acquire_directory(
            acquisition.load_search_plan(),
            "run_test",
            object(),
            cancel_check=AsyncMock(),
            progress=AsyncMock(),
            pause=AsyncMock(),
        ):
            pytest.fail("Incomplete geographic coverage must stop before capture")
    transport.assert_not_called()


def test_geography_rejects_wrong_postal_state(tmp_path, plan_regions):
    other_zip = next(region["zip_code"] for region in plan_regions if region["state"] == "HI")
    rows = [{**r, "zip_code": other_zip} if r["state"] == "PA" else r for r in plan_regions]
    with pytest.raises(ValueError, match="postal_state_invalid"):
        _plan(tmp_path, rows)


def test_missing_reference_jurisdiction_refuses_plan(tmp_path, plan_regions):
    reference = tmp_path / "census-state-boundaries-2025.geojson.gz"
    document = json.loads(gzip.decompress(reference.read_bytes()))
    document["features"].pop()
    reference.write_bytes(gzip.compress(canonical_json(document).encode()))
    with pytest.raises(ValueError, match="reference_incomplete"):
        _plan(tmp_path, plan_regions)


def test_bundled_geography_includes_islands_and_postal_coverage():
    boundaries, locations = geography._read_reference(geography._REFERENCE)
    assert set(boundaries) == sweep.JURISDICTIONS
    assert {state for state, _ in locations} >= sweep.JURISDICTIONS
    assert boundaries["AK"].intersects(box(-180, 50, -178, 56))
    assert boundaries["AK"].intersects(box(170, 50, 180, 56))
    assert boundaries["HI"].intersects(box(-180, 25, -170, 29))


def test_configured_search_selectors(tmp_path, plan_regions):
    """Selector changes create a distinct edition and keep its request budget valid."""
    rows = plan_regions
    original = _plan(tmp_path, rows)
    assert original.selectors == SELECTORS
    changed = _plan(tmp_path, rows, selectors=[{"planType": "PLAN-A", "clientType": "CLIENT-Y"}])
    assert changed.selectors == (("PLAN-A", "CLIENT-Y"),) and changed.identity != original.identity
    with pytest.raises(ValueError, match="budget_invalid"):
        _plan(tmp_path, rows, max_origin_requests=len(rows))
    manifest = tmp_path / acquisition._MANIFEST.name
    manifest.write_text(canonical_json({"regions": rows, "requests_per_second": 1, "max_origin_requests": 1000}))
    with pytest.raises(ValueError, match="manifest_invalid"):
        sweep.load_plan(manifest, tmp_path)


@pytest.mark.parametrize(
    "entries",
    [
        [],
        None,
        {},
        [{}],
        [{"clientType": "CLIENT-X"}],
        [{"planType": ""}],
        [{"planType": 2}],
        [{"planType": "PLAN-A", "clientType": None}],
        [{"planType": "PLAN-A", "clientType": ""}],
        [{"planType": " PLAN-A"}],
        [{"planType": "A" * 101}],
        [{"planType": "PLAN-A", "unknown": "X"}],
        [{"planType": "PLAN-A", "clientType": "X"}, {"clientType": "X", "planType": "PLAN-A"}],
    ],
)
def test_invalid_search_selectors(entries):
    with pytest.raises(ValueError, match="guardian_selector"):
        sweep._search_selectors(entries)


def test_retained_reservations_prevent_duplicate_attempts(capture):
    capture.reserve("search", "a")
    with pytest.raises(ValueError, match="outcome_unresolved"):
        capture.lookup("search", "a")
    capture.retain("search", "a", b"{}")
    assert capture.lookup("search", "a") == b"{}"
    capture.reserve("search", "b")
    capture.release_limited("search", "b")
    assert capture.lookup("search", "b") is None
    capture.reserve("search", "b")
    with pytest.raises(ValueError, match="outcome_unresolved"):
        capture.summary(())
    capture.retain("search", "b", b"{}")
    assert capture.summary(())["origin_requests"] == 3
    with pytest.raises(ValueError, match="scope_changed"):
        GuardianCapture(capture.db, "different", 100, fresh=False)
    capture.budget = 3
    with pytest.raises(ValueError, match="budget_exhausted"):
        capture.reserve("search", "c")
    assert capture.lookup("search", "c") is None


def test_safe_circle_and_distinct_observation_coverage(capture):
    center = (40.0, -80.0)
    capture.observe(
        "plan",
        [
            ("a", *center, None, None),
            ("b", *center, None, None),
            ("outer", 41.0, -80.0, None, None),
            ("c", *center, None, None),
        ],
    )
    capture.observe("other", [("unrelated", *center, None, None)])
    capture.observe("plan", [("c", 40.01, -80.0, None, None)])
    assert capture.is_count_reconciled("plan", center, 2, {"a"}, 2)
    assert not capture.is_count_reconciled("plan", center, 2, {"a"}, 1)
    assert not capture.is_count_reconciled("plan", center, 2, {"a"}, 3)
    capture.add_circle("plan", center, 9)
    assert capture.covers_tile("plan", center, 8)
    assert not capture.covers_tile("other", center, 1)
    assert not capture.covers_tile("plan", (40.2, -80.0), 1)


async def test_all_selectors_share_identical_detail(tmp_path, capture, monkeypatch):
    tile = GeoTile(39.999, 40.001, -80.001, -79.999)
    region = sweep.SearchRegion("PA", "00001", tile)
    plan = sweep.SearchPlan(tmp_path, (region,), 1.0, 100, "edition", SELECTORS)
    selectors = []
    detail_calls = []
    card = _card()
    detail_payload = sweep._detail_key(card)

    async def request(kind, payload):
        if kind == "search":
            params = payload["searchParams"]
            selectors.append((params["planType"], params.get("clientType", "")))
            assert params["miles"] == "2" and payload["filters"] == {}
            return {"providers": [card], "totalResults": 1, "redirect": False}
        assert payload == detail_payload
        detail_calls.append(payload)
        key = canonical_json(payload)
        capture.reserve(kind, key)
        capture.retain(kind, key, b"{}")
        return card

    resource_by_field = {"resourceType": "Organization", "id": "network"}
    monkeypatch.setattr(sweep, "normalize_guardian_detail", lambda detail: (resource_by_field,))
    proof = await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert proof["coverage_scope"] == "guardian_first_party_configured_jurisdiction_bounds"
    assert selectors == list(plan.selectors) and proof["search_selector_count"] == len(plan.selectors)
    assert detail_calls == [detail_payload]
    assert list(capture.iter_resources()) == [resource_by_field]
    await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert len(detail_calls) == 1 and len(selectors) == len(plan.selectors)


def test_redirect_and_truncated_counts_fail_closed():
    handoff_map = {"redirect": True, "providers": None, "totalResults": 0, "redirectURL": "https://partner.test/"}
    assert sweep._search_result(handoff_map) == "https://partner.test/"
    for overrides in [
        {"providers": [_card()]},
        {"totalResults": 1},
        {"redirectURL": "http://partner.test"},
        {"redirectURL": "https://user:pass@partner.test"},
        {"redirectURL": ""},
    ]:
        with pytest.raises(ValueError, match="redirect_invalid"):
            sweep._search_result({**handoff_map, **overrides})
    for record in [
        {"redirect": False, "providers": None, "totalResults": 1},
        {"redirect": 0, "providers": [], "totalResults": 0},
        {"redirect": False, "providers": [], "totalResults": True},
    ]:
        with pytest.raises(ValueError):
            sweep._search_result(record)
    assert sweep._check_coordinates(_card(latitude=41.0), (40.0, -80.0), 2) == (None, None)


async def test_capped_search_retains_unlocated_card_without_using_it_in_other_windows(capture, monkeypatch):
    center = (40.0, -80.0)
    sites = [
        {**_card(str(index), *sweep._offset(center, east, north)), "distance": 1.0}
        for index, (east, north) in enumerate(((1, 0), (0, 1), (-1, 0)))
    ]
    suspect_card_map = {**_card("different", *sweep._offset(center, 0, 1.5)), "distance": 1.0}
    cards = [*sites, suspect_card_map]
    tile = GeoTile(39.999, 40.001, -80.001, -79.999)
    region = sweep.SearchRegion("PA", "00001", tile)
    selector = ("PLAN-A", "")
    scope = canonical_json([region.state, region.zip_code, *selector])
    monkeypatch.setattr(sweep, "_CARD_LIMIT", len(cards))
    page_map = {"redirect": False, "providers": cards, "totalResults": len(cards) + 1}
    await sweep._sweep(capture, region, selector, tile, AsyncMock(return_value=page_map), {"capped": 0})
    assert capture.has_gaps() and not capture.covers_tile(scope, center, 0.1)
    keys = {canonical_json(sweep._detail_key(card)) for card in cards}
    assert len(keys) == 4 and capture.is_count_reconciled(scope, center, 10, keys, 4)
    assert capture.is_count_reconciled(scope, center, 10, set(), 3)
    assert not capture.is_count_reconciled(scope, center, 10, set(), 4)
    assert len(capture.detail_batch("", 10)) == 4
    updated_card_map = {**suspect_card_map, "distance": 1.5}
    assert not sweep._observe(capture, scope, [updated_card_map], center, 10)[1]
    assert sweep._observe(capture, scope, cards, center, 10) == (Counter(keys), False)
    assert len(capture.detail_batch("", 10)) == 4
    assert capture.is_count_reconciled(scope, center, 10, set(), 3)

    capture.add_circle("another", center, 2)
    sweep._observe(capture, "another", [updated_card_map], center, 10)
    assert sweep._observe(capture, "another", cards, center, 10) == (Counter(keys), False)
    assert capture.coverage_invalidated and not capture.covers_tile("another", center, 0.1)


@pytest.mark.parametrize("distance", [None, "unknown", -1, 500, 10**400])
async def test_unlocated_site_is_exact_only_and_next_window_runs(capture, distance):
    tile = GeoTile(39.999, 40.001, -80.001, -79.999)
    region = sweep.SearchRegion("PA", "00001", tile)
    selector = ("PLAN-A", "")
    cards = [{**_card(str(number)), "distance": distance} for number in range(5)]
    page_map = {"providers": cards, "totalResults": len(cards), "redirect": False}
    request = AsyncMock(return_value=page_map)
    for window in (tile, tile.split()[0]):
        await sweep._sweep(capture, region, selector, window, request, {})
    assert request.await_count == 2 and not capture.has_gaps()
    assert len(capture.detail_batch("", 10)) == len(cards)
    assert not capture.db.execute("SELECT 1 FROM circle").fetchone()
    assert not capture.db.execute("SELECT 1 FROM observation WHERE lat IS NOT NULL").fetchone()


async def test_capped_unlocated_site_requires_direct_union(capture, monkeypatch):
    tile = GeoTile(39.999, 40.001, -80.001, -79.999)
    region = sweep.SearchRegion("PA", "00001", tile)
    selector = ("PLAN-A", "")
    scope = canonical_json([region.state, region.zip_code, *selector])
    monkeypatch.setattr(sweep, "_CARD_LIMIT", 2)
    cards = [{**_card(str(number)), "latitude": None} for number in range(3)]
    page_map = {"providers": cards[:2], "totalResults": 3, "redirect": False}
    await sweep._sweep(capture, region, selector, tile, AsyncMock(return_value=page_map), {"capped": 0})
    window, count, keys, span, reason = next(capture.gap_batches(scope))
    target = sweep.DenseWindow(window, region, selector, count, keys, span, reason)
    sweep._observe(capture, scope, [_card("2")], tile.center, 2)
    assert reason == "direct_only_result_cap" and not target.is_closed(capture)
    capture.add_circle(scope, tile.center, 2)
    assert not target.is_closed(capture)
    assert not sweep._is_gap_closed(capture, window, count, keys | Counter(["extra", "another"]), reason)
    capture.db.execute("DELETE FROM circle")
    direct_page_map = {"providers": cards[2:], "totalResults": 1, "redirect": False}
    await target.probe(capture, AsyncMock(return_value=direct_page_map), tile.center, {"specialties": ["Specialty"]})
    assert target.is_closed(capture) and not capture.has_gaps()
    assert len(capture.detail_batch("", 10)) == 3
    assert not capture.covers_tile(scope, tile.center, 0.1)
    capture.record_gap(window, reason, count, keys | Counter(["extra", "another"]), span)
    capture.record_gap(window, "unresolved_result_cap", count, keys, span)
    window, count, retained_keys, _, reason = next(capture.gap_batches(scope))
    assert reason == "direct_only_result_cap" and len(retained_keys) > count
    assert not sweep._is_gap_closed(capture, window, count, retained_keys, reason)


def test_legacy_geographic_shortcuts_rebuild_from_retained_responses(capture):
    capture.reserve("search", "request")
    capture.retain("search", "request", b"{}")
    capture.add_circle("scope", (40, -80), 2)
    capture.record_gap(["scope", [0, 1, 0, 1]], "unresolved_result_cap", 2, {"one"}, 0)
    capture.db.execute("PRAGMA user_version=0")

    def interrupt_version(action, name, value, *unused):
        if action == sqlite3.SQLITE_PRAGMA and name == "user_version" and value is not None:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    capture.db.set_authorizer(interrupt_version)
    with pytest.raises(sqlite3.DatabaseError):
        GuardianCapture(capture.db, "edition", 100, fresh=False)
    capture.db.set_authorizer(None)
    assert capture.covers_tile("scope", (40, -80), 0.1) and capture.has_gaps()
    restored = GuardianCapture(capture.db, "edition", 100, fresh=False)
    assert restored.attempt_count() == 1 and restored.lookup("search", "request") == b"{}"
    assert not restored.covers_tile("scope", (40, -80), 0.1) and restored.has_gaps()


@pytest.mark.parametrize("interrupted", [False, True])
async def test_revoked_positions_revisit_previously_skipped_tiles(capture, monkeypatch, interrupted):
    first = sweep.SearchRegion("PA", "00001", GeoTile(39.999, 40.001, -80.001, -79.999))
    second = sweep.SearchRegion("PA", "00001", GeoTile(40.999, 41.001, -80.001, -79.999))
    selector = ("PLAN-A", "")
    scope = canonical_json([first.state, first.zip_code, *selector])
    plan = sweep.SearchPlan(None, (first, second), 1.0, 100, "edition", (selector,))
    sweep._observe(capture, scope, [_card()], first.tile.center, 2)
    capture.add_circle(scope, first.tile.center, 2)
    origin_centers = []
    monkeypatch.setattr(sweep, "normalize_guardian_detail", lambda detail: ())

    async def request(kind, query):
        key = canonical_json(query)
        if retained := capture.lookup(kind, key):
            return json.loads(retained)
        if kind == "detail":
            capture.reserve(kind, key)
            capture.retain(kind, key, canonical_json(query).encode())
            return query
        lat = query["searchParams"]["latitude"]
        origin_centers.append(lat)
        card = _card() if lat == 40 else {**_card(), "distance": None}
        page_map = {"providers": [card], "totalResults": 1, "redirect": False}
        capture.reserve(kind, key)
        capture.retain(kind, key, canonical_json(page_map).encode())
        return page_map

    async def stop_after_conflict(_counts):
        if interrupted and capture.coverage_invalidated:
            raise RuntimeError("interrupted")

    if interrupted:
        with pytest.raises(RuntimeError, match="interrupted"):
            await sweep.capture_directory(capture, plan, request, AsyncMock(), stop_after_conflict)
        capture = GuardianCapture(capture.db, "edition", 100, fresh=False)
        assert not capture.covers_tile(scope, first.tile.center, 0.1)
    await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert origin_centers == [41, 40] and capture.attempt_count() == 3
    assert not capture.is_count_reconciled(scope, first.tile.center, 2, set(), 1)


async def test_unresolved_cap_records_gap(capture, monkeypatch):
    monkeypatch.setattr(sweep, "normalize_guardian_detail", lambda detail: ())
    capture.budget = 1000
    tile = GeoTile(39.9999, 40.0001, -80.0001, -79.9999)
    cards = [_card(str(n)) for n in range(150)]

    async def fetch(kind, payload):
        if kind == "detail":
            key = canonical_json(payload)
            capture.reserve(kind, key)
            capture.retain(kind, key, canonical_json(payload).encode())
            return payload
        if kind == "specialty":
            return [{"specialtyId": "1", "specialtyName": "Specialty"}]
        params = payload["searchParams"]
        distance = sweep.distance_miles(params["latitude"], params["longitude"], *tile.center)
        return {
            "redirect": False,
            "providers": [{**card, "distance": round(distance, 1)} for card in cards],
            "totalResults": 160,
        }

    request = AsyncMock(side_effect=fetch)
    plan = sweep.SearchPlan(None, (sweep.SearchRegion("PA", "00001", tile),), 1.0, 100, "edition", SELECTORS)
    with pytest.raises(ValueError, match="coverage_incomplete"):
        await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert request.await_count == len(plan.selectors) * (1 + 4 + 18 + 1 + 1) + len(cards)
    assert capture.db.execute("SELECT reason,count(*) FROM gap GROUP BY reason").fetchall() == [
        ("unresolved_result_cap", len(plan.selectors))
    ]
    assert capture.projected_count() == len(cards)


async def test_destination_denials_and_retry_after(capture):
    binding, route, admission = Mock(), Mock(), Mock()
    for status in (403, 429, 503):
        key = str(status)
        capture.reserve("search", key)
        response = Mock(status=status, headers={"Retry-After": "2"})
        with pytest.raises(acquisition.SourceProxyUnavailable):
            await acquisition._read_response(response, binding, route, admission, capture, "search", key)
        if status == 429:
            assert capture.lookup("search", key) is None
        else:
            with pytest.raises(ValueError, match="outcome_unresolved"):
                capture.lookup("search", key)
    assert binding.defer.call_count == 3
    assert admission.record_hold.call_args_list[0].args == (None, False)


async def test_capped_specialty_union(capture):
    tile = GeoTile(39.999, 40.001, -80.001, -79.999)
    region = sweep.SearchRegion("PA", "00001", tile)
    scope = canonical_json(["PA", "00001", "PLAN-A", ""])
    window_parts = [scope, tile.bounds]
    cards = [_card(str(i)) for i in range(151)]
    keys, _ = sweep._observe(capture, scope, cards[:150], tile.center, 2)
    capture.record_gap(window_parts, "unresolved_result_cap", 151, keys, 0.0)

    async def fetch(kind, payload):
        if kind == "specialty":
            return [{"specialtyId": "1", "specialtyName": "Specialty"}]
        assert payload == sweep.search_payload(region, tile.center, 2, ("PLAN-A", ""), {"specialties": ["Specialty"]})
        return {"redirect": False, "providers": cards[1:], "totalResults": 150}

    request = AsyncMock(side_effect=fetch)
    target = sweep.DenseWindow(window_parts, region, ("PLAN-A", ""), 151, keys, 0.0)
    await target.probe(capture, request, tile.center, {"specialties": ["Specialty"]})
    assert target.is_closed(capture) and not capture.has_gaps() and request.await_count == 1


async def test_facility_npi_count(capture, monkeypatch):
    identity_by_field = {**sweep._detail_key(_card()), "cardType": "F"}
    missing_by_field = {**identity_by_field, "practiceId": "002"}
    capture.enqueue_details([identity_by_field, missing_by_field])

    async def request(kind, payload):
        text = canonical_json(payload)
        capture.reserve(kind, text)
        capture.retain(kind, text, b"{}")
        return {
            **payload,
            "denMastId": "-1",
            "npi": "123456" if payload == identity_by_field else "No Information Available",
        }

    monkeypatch.setattr(sweep, "normalize_guardian_detail", lambda detail: ())
    await sweep._capture_details(capture, request, AsyncMock(), AsyncMock())
    assert capture.summary(())["facility_details_with_unattributed_npi"] == 1
    with pytest.raises(ValueError, match="identity_changed"):
        sweep._validated_detail(identity_by_field, {**identity_by_field, "practiceId": "incorrect"})


async def test_partner_handoff_completes_first_party_window(capture):
    region = sweep.SearchRegion("MD", "00002", GeoTile(39, 39.01, -76, -75.99))
    plan = sweep.SearchPlan(None, (region,), 1.0, 100, "edition", SELECTORS)
    request = AsyncMock(
        return_value={"redirect": True, "providers": None, "totalResults": 0, "redirectURL": "https://partner.test/"}
    )
    await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    proof = capture.summary(acquisition.RESOURCE_TYPES)
    assert proof["delegated_windows"] == len(plan.selectors) and proof["coverage_gaps"] == 0
    assert proof["delegated_destinations"] == {"https://partner.test/": len(plan.selectors)}
    assert proof["partner_membership_complete"] is False and not any(proof["resource_counts"].values())
    assert all(call.args[0] == "search" for call in request.await_args_list)


@pytest.fixture
def replay_transport(tmp_path, monkeypatch):
    """Connect a one-region edition to an observable proxy session."""
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    region = sweep.SearchRegion("PA", "00001", GeoTile(39.999, 40.001, -80.001, -79.999))
    plan = sweep.SearchPlan(tmp_path, (region,), 1, 100, "bound-edition", SELECTORS)
    monkeypatch.setattr(acquisition, "_artifact_root", lambda: tmp_path)
    monkeypatch.setattr(sweep, "load_plan", lambda path, root: plan)
    transport_calls = []
    binding = Mock()

    @asynccontextmanager
    async def response(method, url, **options):
        assert method == "POST" and url == acquisition.ENDPOINT_SCOPE + "/providersearch"
        assert options["proxy"] == "https://proxy.test" and options["allow_redirects"] is False
        assert options["headers"]["ga-request-id"]
        transport_calls.append(options["json"])
        yield SimpleNamespace(status=200)

    @asynccontextmanager
    async def session(**options):
        assert options["trust_env"] is False
        await options["connector"].close()
        yield SimpleNamespace(request=response)

    async def reserve(self, charge):
        charge()

    monkeypatch.setattr(acquisition.aiohttp, "ClientSession", session)
    monkeypatch.setattr(acquisition.FinderAdmission, "admit", reserve)
    route = SimpleNamespace(proxy_id="route-test", request_options={"proxy": "https://proxy.test"})
    selections = Mock(return_value=route)
    monkeypatch.setattr(acquisition, "_selection", selections)
    monkeypatch.setattr(
        acquisition, "_json_body", AsyncMock(return_value=b'{"redirect":false,"totalResults":0,"providers":[]}')
    )
    return binding, transport_calls, selections


async def test_normal_entrypoint_replays_edition(replay_transport):
    """Only a fresh edition issues searches when the worker opens the same root twice."""
    binding, transport_calls, selections = replay_transport
    for resumed in (False, True):
        async with acquisition.acquire_directory(
            acquisition.load_search_plan(),
            "run_01",
            binding,
            cancel_check=AsyncMock(),
            progress=AsyncMock(),
            pause=AsyncMock(),
            require_retained=resumed,
        ) as (capture, proof):
            assert not list(capture.iter_resources())
            assert proof["origin_requests_by_kind"] == {"search": len(SELECTORS)} and proof["coverage_gaps"] == 0
    assert len(transport_calls) == len(SELECTORS)
    assert all(
        call.args[:3] == (binding, acquisition.ENDPOINT_SCOPE + "/providersearch", acquisition.SOURCE_ID)
        for call in selections.call_args_list
    )
    with pytest.raises(ValueError, match="retained_capture_required"):
        async with acquisition.acquire_directory(
            acquisition.load_search_plan(),
            "missing",
            binding,
            cancel_check=AsyncMock(),
            progress=AsyncMock(),
            pause=AsyncMock(),
            require_retained=True,
        ):
            pytest.fail("missing retained edition should stop before transport")


@pytest.mark.parametrize("conflicting", [False, True])
async def test_full_response_counts_occurrences_and_queues_distinct_details(capture, conflicting):
    region = sweep.SearchRegion("PA", "00001", GeoTile(39.999, 40.001, -80.001, -79.999))
    card = _card()
    other_card_map = {**card, "ddsPracticeName": "Example Office", "phone": "555-0100"}
    if conflicting:
        other_card_map["latitude"], other_card_map["longitude"] = sweep._offset(region.tile.center, 0.5, 0)
        other_card_map["distance"] = 0.5
    request = AsyncMock(
        return_value={"redirect": False, "totalResults": 3, "providers": [card, other_card_map, _card("002")]}
    )
    await sweep._sweep(capture, region, ("PLAN-A", ""), region.tile, request, {})
    assert not capture.has_gaps() and not capture.db.execute("SELECT 1 FROM circle").fetchone()
    assert len(capture.detail_batch("", 5)) == 2
    assert capture.db.execute("SELECT count(*) FROM observation WHERE lat IS NULL").fetchone()[0] == int(conflicting)


async def test_duplicate_leaf_reconciles_only_simultaneous_same_query_occurrences(capture, monkeypatch):
    region = sweep.SearchRegion("PA", "00001", GeoTile(39.999, 40.001, -80.001, -79.999))
    selector = ("PLAN-A", "CLIENT-X")
    scope = canonical_json([region.state, region.zip_code, *selector])
    monkeypatch.setattr(sweep, "_CARD_LIMIT", 3)
    cards = [_card("A"), _card("A"), _card("B")]
    page_map = {"providers": cards, "redirect": False, "totalResults": 5}
    await sweep._sweep(capture, region, selector, region.tile, AsyncMock(return_value=page_map), {"capped": 0})
    window, count, keys, span, reason = next(capture.gap_batches(scope))
    target = sweep.DenseWindow(window, region, selector, count, keys, span, reason)
    subset_map = {"providers": [_card("B"), _card("C")], "totalResults": 2, "redirect": False}
    capture.observe("other selector", [(canonical_json(sweep._detail_key(_card("D"))), *target.center, None, None)])
    await target.probe(capture, AsyncMock(return_value=subset_map), sweep._offset(target.center, 1, 0), {})
    assert target.keys.total() == 3 and not target.is_closed(capture)
    for label in ("First", "Second"):
        subset_map["providers"][1]["phone"] = label
        await target.probe(capture, AsyncMock(return_value=subset_map), target.center, {"specialties": [label]})
    assert target.keys.total() == 4 and len(target.keys) == 3 and not target.is_closed(capture)
    restored = next(capture.gap_batches(scope))
    assert restored[2] == target.keys
    extra_map = {"providers": [_card("C"), _card("C")], "totalResults": 2, "redirect": False}
    await target.probe(capture, AsyncMock(return_value=extra_map), target.center, {"specialties": ["Third"]})
    assert target.is_closed(capture) and not capture.has_gaps()
    assert not capture.covers_tile(scope, target.center, 0.1)
    assert not sweep._is_gap_closed(capture, window, count - 1, target.keys, reason)


async def test_short_page_keeps_gap_and_independent_known_details(capture, monkeypatch):
    first = sweep.SearchRegion("PA", "00001", GeoTile(39.999, 40.001, -80.001, -79.999))
    second = sweep.SearchRegion("PA", "00001", GeoTile(40.999, 41.001, -80.001, -79.999))
    plan = sweep.SearchPlan(None, (first, second), 1, 100, "edition", (("PLAN-A", ""),))
    capture.enqueue_details([sweep._detail_key(_card("earlier"))])
    actions = []
    monkeypatch.setattr(sweep, "normalize_guardian_detail", lambda detail: ())

    async def request(kind, fields):
        actions.append(kind)
        if kind == "detail":
            key = canonical_json(fields)
            capture.reserve(kind, key)
            capture.retain(kind, key, canonical_json(fields).encode())
            return fields
        latitude = fields["searchParams"]["latitude"]
        return {
            "providers": [_card(str(latitude), latitude)],
            "totalResults": 2 if latitude == 40 else 1,
            "redirect": False,
        }

    with pytest.raises(ValueError, match="coverage_incomplete"):
        await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert actions == ["detail", "search", "detail", "search", "detail"]
    assert capture.projected_count() == 3
    assert capture.db.execute("SELECT reason FROM gap").fetchall() == [("search_cardinality_mismatch",)]


async def test_interleaves_bounded_detail_batches_and_drains_lower_keys(capture, monkeypatch):
    regions = tuple(sweep.SearchRegion("PA", f"{i:05d}", GeoTile(39.999, 40.001, -80.001, -79.999)) for i in range(3))
    plan = sweep.SearchPlan(None, regions, 1, 1000, "edition", (("PLAN-A", ""),), max_concurrency=2)
    capture.budget = 1000
    capture.enqueue_details([sweep._detail_key(_card(f"prior-{n:03d}")) for n in range(220)])
    monkeypatch.setattr(sweep, "normalize_guardian_detail", lambda detail: ())
    projected_counts = []

    async def request(kind, fields):
        if kind == "detail":
            key = canonical_json(fields)
            capture.reserve(kind, key)
            capture.retain(kind, key, canonical_json(fields).encode())
            return fields
        projected_counts.append(capture.projected_count())
        return {"providers": [_card("earlier")], "totalResults": 1, "redirect": False}

    await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert projected_counts == [100, 200, 220]
    assert capture.attempt_count() == capture.projected_count() == 221


@pytest.mark.parametrize("invalid_fields", [{"latitude": "unknown", "longitude": -80}, {"specialties": []}])
async def test_matching_unprojectable_detail_is_retained_while_others_finish(capture, invalid_fields):
    region = sweep.SearchRegion("PA", "00001", GeoTile(39.999, 40.001, -80.001, -79.999))
    plan = sweep.SearchPlan(None, (region,), 1, 100, "edition", (("PLAN-A", ""),))
    keys = [sweep._detail_key(_card(number)) for number in ("A", "B")]
    capture.enqueue_details(keys)
    sent_keys = []

    async def request(kind, fields):
        if kind == "search":
            return {"providers": [], "totalResults": 0, "redirect": False}
        key = canonical_json(fields)
        if prior := capture.lookup(kind, key):
            return json.loads(prior)
        sent_keys.append(key)
        detail_map = {
            **fields,
            "address1": "1 Example St",
            "city": "Example",
            "state": "PA",
            "zip": "00001",
            "networks": "Example Network",
            "latitude": 40,
            "longitude": -80,
        }
        if fields == keys[0]:
            detail_map.update(invalid_fields)
        capture.reserve(kind, key)
        capture.retain(kind, key, canonical_json(detail_map).encode())
        return detail_map

    for _ in range(2):
        with pytest.raises(ValueError, match="detail_coverage_incomplete"):
            await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert len(sent_keys) == len(set(sent_keys)) == 2 and capture.projected_count() == 1
    assert capture.db.execute("SELECT status FROM request WHERE payload=?", (canonical_json(keys[0]),)).fetchone() == (
        "received",
    )
    with pytest.raises(ValueError, match="detail_coverage_incomplete"):
        capture.summary(())


@pytest.mark.parametrize("override", [{"practiceId": "different"}, {"networks": []}, {"providers": [{}]}])
async def test_identity_or_shape_error_stops_detail_work(capture, override):
    fields = sweep._detail_key(_card())
    detail_map = {
        **fields,
        "address1": "1 Example St",
        "city": "Example",
        "state": "PA",
        "zip": "00001",
        "networks": "Example Network",
        **override,
    }
    capture.enqueue_details([fields, sweep._detail_key(_card("other"))])
    request = AsyncMock(return_value=detail_map)
    with pytest.raises(ValueError, match="identity_changed|detail_text_invalid|nested_provider_shape_invalid"):
        await sweep._capture_details(capture, request, AsyncMock(), AsyncMock())
    assert request.await_count == 1 and capture.projected_count() == 0


async def test_untrusted_filtered_response_retains_keys_without_parent_credit(capture):
    region = sweep.SearchRegion("PA", "00001", GeoTile(39.999, 40.001, -80.001, -79.999))
    selector = ("PLAN-A", "")
    scope = canonical_json([region.state, region.zip_code, *selector])
    keys, _ = sweep._observe(capture, scope, [_card("A")], region.tile.center, 2)
    target = sweep.DenseWindow([scope, region.tile.bounds], region, selector, 2, keys, 0.1)
    page_map = {
        "providers": [{**_card("B"), "ddsPracticeName": "Different Office"}],
        "totalResults": 1,
        "redirect": False,
    }
    await target.probe(
        capture, AsyncMock(return_value=page_map), target.center, {}, qualifier=("officeName", "Example")
    )
    assert target.keys.total() == 1 and len(capture.detail_batch("", 5)) == 2
    assert not target.is_closed(capture)


async def test_boundary_name_requires_original_scope(capture):
    center = (40.0, -80.0)
    tile = GeoTile(39.999, 40.001, -80.001, -79.999)
    region = sweep.SearchRegion("PA", "00001", tile)
    selector = ("PLAN-A", "synthetic")
    scope = canonical_json([region.state, region.zip_code, *selector])
    window_parts = [scope, tile.bounds]
    keys, _ = sweep._observe(capture, scope, [_card()], center, 2)
    lat, lon = sweep._offset(center, 1.95, 0)
    row_map = {**_card("002", lat, lon), "ddsPracticeName": "Example Office", "ddsLastName": "Example"}
    sweep._observe(capture, scope, [row_map], (lat, lon), 2)
    target = sweep.DenseWindow(window_parts, region, selector, 2, keys, 0.1)
    assert not target.is_closed(capture)
    assert capture.boundary_names("another scope", center, 2, keys, 2) == ()
    name = capture.boundary_names(scope, center, 2, keys, 2)[0]
    assert name == ("officeName", "Example Office")

    async def fetch(kind, payload):
        expected = sweep.search_payload(region, center, 2, selector, {})
        expected["searchParams"]["officeName"] = name[1]
        assert payload == expected
        return {"providers": [{**row_map, "distance": 1.95}], "redirect": False, "totalResults": 1}

    await target.probe(capture, fetch, center, {}, qualifier=name)
    assert target.is_closed(capture)


async def test_parallel_failure_drains_started_requests():
    both_started = asyncio.Event()
    release = asyncio.Event()
    origin_items = []
    completed_items = []
    failed = RuntimeError("synthetic failure")

    async def request(kind, item):
        origin_items.append(item)
        if item == "first":
            await both_started.wait()
            raise failed
        both_started.set()
        await release.wait()
        completed_items.append(item)

    async def operate(item, guarded_request):
        await guarded_request("search", item)
        await guarded_request("detail", item)

    task = asyncio.create_task(sweep._run_limited(["first", "second", "third"], 2, request, operate))
    await both_started.wait()
    await asyncio.sleep(0)
    release.set()
    with pytest.raises(RuntimeError) as result:
        await task
    assert result.value is failed and origin_items == ["first", "second"] and completed_items == ["second"]


async def test_concurrent_scopes_and_details(tmp_path, capture, monkeypatch):
    tile = GeoTile(39.999, 40.001, -80.001, -79.999)
    plan = sweep.SearchPlan(tmp_path, (sweep.SearchRegion("PA", "00001", tile),), 1, 100, "edition", SELECTORS, 2)
    counts = Counter()
    peak = Counter()
    calls = Counter()
    cards = [_card(str(i)) for i in range(3)]

    async def request(kind, fields):
        counts[kind] += 1
        calls[kind] += 1
        peak[kind] = max(peak[kind], counts[kind])
        await asyncio.sleep(0)
        counts[kind] -= 1
        if kind == "search":
            return {"totalResults": 3, "redirect": False, "providers": cards}
        key = canonical_json(fields)
        capture.reserve(kind, key)
        capture.retain(kind, key, b"{}")
        return fields

    monkeypatch.setattr(sweep, "normalize_guardian_detail", lambda detail: ())
    proof = await sweep.capture_directory(capture, plan, request, AsyncMock(), AsyncMock())
    assert peak == {"search": 2, "detail": 2}
    assert calls == {"search": len(plan.selectors), "detail": 3}
    assert proof["requested_concurrency"] == proof["search_worker_limit"] == proof["detail_worker_limit"] == 2
