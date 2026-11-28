# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reconcile the public finder's geographic search windows before publishing an edition."""

import math
import re
from dataclasses import dataclass
from itertools import batched, islice
from pathlib import Path
from urllib.parse import urlsplit

from process.delta_dental_directory_capture import _capture_parallel
from process.guardian_directory_geography import JURISDICTIONS, validate_region_coverage
from process.guardian_directory_normalization import normalize_guardian_detail
from process.vsp_directory_contract import canonical_json
from process.vsp_directory_geometry import GeoTile, distance_miles
from process.vsp_directory_http import strict_json

RADII = (2, 5, 10, 20, 50, 100, 150, 200)
_CARD_LIMIT = 150


@dataclass(frozen=True)
class SearchRegion:
    state: str
    zip_code: str
    tile: GeoTile


@dataclass(frozen=True)
class SearchPlan:
    root: Path
    regions: tuple[SearchRegion, ...]
    requests_per_second: float
    max_origin_requests: int
    identity: str
    selectors: tuple[tuple[str, str], ...]
    max_concurrency: int = 1


def _region(record):
    if not isinstance(record, dict) or record.keys() != {"state", "zip_code", "bounds"}:
        raise ValueError("guardian_region_invalid")
    state, location, bounds = record["state"], record["zip_code"], record["bounds"]
    if not isinstance(state, str) or state not in JURISDICTIONS:
        raise ValueError("guardian_region_invalid")
    if not isinstance(location, str) or re.fullmatch("[0-9]{5}", location) is None:
        raise ValueError("guardian_region_invalid")
    if type(bounds) is not list or len(bounds) != 4:
        raise ValueError("guardian_region_invalid")
    return SearchRegion(state, location, GeoTile(*bounds))


def _search_selectors(entries):
    """Read the exact plan and optional client identifiers for this edition."""
    if not isinstance(entries, list) or not entries:
        raise ValueError("guardian_selectors_invalid")
    selectors = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.keys() - {"clientType"} != {"planType"}:
            raise ValueError("guardian_selector_invalid")
        if any(type(code) is not str or not re.fullmatch(r"[A-Za-z0-9._-]{1,100}", code) for code in entry.values()):
            raise ValueError("guardian_selector_invalid")
        selectors.append((entry["planType"], entry.get("clientType", "")))
    if len(set(selectors)) != len(selectors):
        raise ValueError("guardian_selectors_duplicate")
    return tuple(selectors)


def load_plan(path, root):
    """Validate the bounded worker manifest and every required jurisdiction."""
    with path.open("rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("guardian_manifest_too_large")
    plan = strict_json(raw)
    if type(plan) is not dict or plan.keys() - {"max_concurrency"} != {
        "regions",
        "selectors",
        "requests_per_second",
        "max_origin_requests",
    }:
        raise ValueError("guardian_manifest_invalid")
    concurrency = plan.get("max_concurrency", 1)
    if type(concurrency) is not int or not 1 <= concurrency <= 8:
        raise ValueError("guardian_request_concurrency_invalid")
    rate, budget, region_entries = plan["requests_per_second"], plan["max_origin_requests"], plan["regions"]
    if type(rate) not in (int, float) or not math.isfinite(rate) or rate <= 0:
        raise ValueError("guardian_request_rate_invalid")
    if not isinstance(region_entries, list) or not region_entries:
        raise ValueError("guardian_regions_missing")
    regions = tuple(_region(entry) for entry in region_entries)
    if len(set(regions)) != len(regions) or {region.state for region in regions} != JURISDICTIONS:
        raise ValueError("guardian_jurisdiction_coverage_incomplete")
    selectors = _search_selectors(plan["selectors"])
    if type(budget) is not int or budget < len(regions) * len(selectors):
        raise ValueError("guardian_request_budget_invalid")
    validate_region_coverage(regions)
    return SearchPlan(root, regions, rate, budget, canonical_json(plan), selectors, concurrency)


def _extent(tile):
    lat, lon = tile.center
    return max(
        distance_miles(lat, lon, corner_lat, corner_lon)
        for corner_lat in (tile.south, tile.north)
        for corner_lon in (tile.west, tile.east)
    )


def search_payload(region, center, radius, selector, filters):
    """Build the publisher search form for one verified selector and center."""
    plan_type, client = selector
    latitude, longitude = center
    return {
        "searchParams": {
            "planType": plan_type,
            "location": region.zip_code,
            "state": region.state,
            "miles": str(radius),
            "latitude": latitude,
            "longitude": longitude,
            **({"clientType": client} if client else {}),
        },
        "filters": filters,
        "memberType": "",
        "previouslySearched": True,
        "previouslyReturnedAtLeastOneResult": False,
        "previouslyReturnedNoResults": False,
        "sortField": "Distance",
        "langauge": "en",
    }


def _detail_key(card):
    if type(card) is not dict or card.get("cardType") not in {"F", "P"}:
        raise ValueError("guardian_search_card_invalid")
    identity_by_field = {}
    for field in ("denMastId", "cardType", "planType", "practiceId"):
        value = card.get(field)
        if type(value) not in (int, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,100}", str(value)):
            raise ValueError("guardian_search_card_identity_invalid")
        identity_by_field[field] = value
    return identity_by_field


def _check_coordinates(card, center, radius):
    latitude = card.get("latitude")
    longitude = card.get("longitude")
    reported = card.get("distance")
    if not all(type(value) in (float, int) for value in (latitude, longitude, reported)):
        return None, None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180 and reported >= 0):
        return None, None
    distance = distance_miles(*center, latitude, longitude)
    tolerance = 0.1 + distance * 0.01
    if distance <= radius * 1.01 + 0.1 and distance - tolerance <= reported <= distance + tolerance:
        return latitude, longitude
    return None, None


def _delegated_destination(result):
    destination = result.get("redirectURL")
    if (
        type(destination) is not str
        or len(destination) > 8192
        or any(char.isspace() or ord(char) < 32 or char == "\\" for char in destination)
    ):
        raise ValueError("guardian_redirect_invalid")
    parsed = urlsplit(destination)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or type(result.get("totalResults")) is not int
        or result["totalResults"] != 0
        or "providers" not in result
        or result["providers"] not in (None, [])
    ):
        raise ValueError("guardian_redirect_invalid")
    return destination


def _search_result(result):
    if type(result) is not dict or type(result.get("redirect")) is not bool:
        raise ValueError("guardian_search_response_invalid")
    if result["redirect"]:
        return _delegated_destination(result)
    count, cards = result.get("totalResults"), result.get("providers")
    if (
        result.get("redirectURL")
        or type(count) is not int
        or count < 0
        or type(cards) is not list
        or len(cards) != min(count, _CARD_LIMIT)
    ):
        raise ValueError("guardian_search_count_mismatch")
    return count, cards


def _validated_detail(key, detail):
    if type(detail) is not dict:
        raise ValueError("guardian_detail_invalid")
    if any(detail.get(k) != key[k] for k in ("cardType", "practiceId", "planType")):
        raise ValueError("guardian_detail_identity_changed")
    # A facility detail identifies its practice and may replace the search's clinician key with a sentinel.
    if detail.get("denMastId") != key["denMastId"] and not (
        key["cardType"] == "F" and detail.get("denMastId") in ("0", "-1")
    ):
        raise ValueError("guardian_detail_identity_changed")
    return normalize_guardian_detail(detail)


async def _run_limited(items, limit, request, operation):
    """Drain outstanding requests and stop issuing new work after the first failure."""
    failures = []

    async def guarded_request(kind, fields):
        """Stop queued origin work once any lane has failed."""
        if failures:
            raise failures[0]
        return await request(kind, fields)

    async def run_item(item, _worker_id):
        """Retain the first failure while already admitted work drains."""
        if failures:
            raise failures[0]
        try:
            await operation(item, guarded_request)
        except Exception as error:
            if not failures:
                failures.append(error)
            raise failures[0]

    async for _ in _capture_parallel(items, limit, run_item):
        continue


async def _capture_details(capture, request, cancel_check, progress, limit=1):
    async def project_detail(row, guarded_request):
        """Commit the normalized detail under its unique publisher request key."""
        await cancel_check()
        key = row[0]
        fields = strict_json(key)
        detail = await guarded_request("detail", fields)
        resources = _validated_detail(fields, detail)
        capture.project(
            key,
            resources,
            unattributed_facility_npi=fields["cardType"] == "F"
            and str(detail.get("npi") or "").strip().casefold() not in {"", "no information available"},
        )

    after = ""
    completed = capture.projected_count()
    while batch := capture.detail_batch(after, 100):
        await _run_limited(batch, limit, request, project_detail)
        after = batch[-1][0]
        completed += len(batch)
        await progress({"guardian_details_completed": completed})


def _name(card, field):
    text = card.get(field)
    return text if type(text) is str and 3 <= len(text) <= 200 and text.isprintable() else None


def _observe(capture, scope, cards, center, radius):
    """Retain direct identities even when the page cannot support geographic extrapolation."""
    observations = []
    detail_keys = []
    for card in cards:
        key = _detail_key(card)
        latitude, longitude = _check_coordinates(card, center, radius)
        detail_keys.append(key)
        observations.append(
            (
                canonical_json(key),
                latitude,
                longitude,
                _name(card, "ddsPracticeName"),
                _name(card, "ddsLastName"),
            )
        )
    distinct_keys = {row[0] for row in observations}
    if len(distinct_keys) != len(detail_keys):
        raise ValueError("guardian_search_duplicate_card")
    capture.enqueue_details(detail_keys)
    capture.observe(scope, observations)
    return distinct_keys, capture.has_locations_for_all_keys(scope, distinct_keys)


def _observed_span(cards, radius):
    """Use available display distances only for prioritizing other probes."""
    distances = (card.get("distance") for card in cards)
    valid_distances = (d for d in distances if type(d) in (int, float) and 0 <= d <= radius)
    return max(valid_distances, default=radius)


async def _sweep(capture, region, selector, tile, request, counts_by_outcome):
    scope = canonical_json([region.state, region.zip_code, *selector])
    extent = _extent(tile)
    if capture.covers_tile(scope, tile.center, extent):
        counts_by_outcome["covered_by_prior_circle"] += 1
        return
    radius = next((miles for miles in RADII if miles >= extent * 1.01 + 0.1), None)
    if radius is None:
        for child in tile.split():
            await _sweep(capture, region, selector, child, request, counts_by_outcome)
        return
    window_parts = [scope, tile.bounds]
    query = search_payload(region, tile.center, radius, selector, {})
    search_page = _search_result(await request("search", query))
    if isinstance(search_page, str):
        capture.record_delegation(query, window_parts, search_page)
        return
    count, cards = search_page
    keys, located = _observe(capture, scope, cards, tile.center, radius)
    if len(cards) == count:
        if located:
            capture.add_circle(scope, tile.center, (radius - 0.1) / 1.01)
        return
    counts_by_outcome["capped"] += 1
    if located and capture.is_count_reconciled(scope, tile.center, radius, keys, count):
        capture.add_circle(scope, tile.center, (radius - 0.1) / 1.01)
        counts_by_outcome["reconciled_to_total"] += 1
        return
    if radius == RADII[0] or tile.depth == 24:
        reason = "unresolved_result_cap" if located else "direct_only_result_cap"
        capture.record_gap(window_parts, reason, count, keys, _observed_span(cards, radius))
        return
    for child in tile.split():
        await _sweep(capture, region, selector, child, request, counts_by_outcome)
        if (
            located
            and capture.has_locations_for_all_keys(scope, keys)
            and capture.is_count_reconciled(scope, tile.center, radius, keys, count)
        ):
            capture.add_circle(scope, tile.center, (radius - 0.1) / 1.01)
            counts_by_outcome["reconciled_to_total"] += 1
            break


async def capture_directory(capture, plan, request, cancel_check, progress):
    """Acquire every selected scope and expose details only after checking outstanding gaps."""
    counts_by_outcome = {"capped": 0, "covered_by_prior_circle": 0, "reconciled_to_total": 0}

    async def sweep_selector(selector, guarded_request):
        """Keep all dependent tiles for this region and selector in order."""
        await cancel_check()
        await _sweep(capture, region, selector, region.tile, guarded_request, counts_by_outcome)

    while True:
        capture.coverage_invalidated = False
        for region in plan.regions:
            await _run_limited(plan.selectors, plan.max_concurrency, request, sweep_selector)
            await progress(
                {"guardian_search": dict(counts_by_outcome), "jurisdiction": region.state, **capture.progress()}
            )
        await _reconcile_gaps(capture, request, cancel_check, progress, counts_by_outcome, plan.max_concurrency)
        if not capture.coverage_invalidated:
            break
    if capture.has_gaps():
        raise ValueError("guardian_directory_coverage_incomplete")
    await _capture_details(capture, request, cancel_check, progress, plan.max_concurrency)
    return {
        "coverage_scope": "guardian_first_party_configured_jurisdiction_bounds",
        "jurisdictions": sorted(JURISDICTIONS),
        "region_count": len(plan.regions),
        "search_selector_count": len(plan.selectors),
        "coverage": counts_by_outcome,
        "requested_concurrency": plan.max_concurrency,
        "search_worker_limit": min(plan.max_concurrency, len(plan.selectors)),
        "detail_worker_limit": plan.max_concurrency,
    }


def _specialty_names(options):
    if type(options) is not list or not 0 < len(options) <= 128:
        raise ValueError("guardian_specialty_response_invalid")
    names = []
    for option in options:
        if type(option) is not dict or not isinstance(option.get("specialtyName"), str):
            raise ValueError("guardian_specialty_response_invalid")
        name = option["specialtyName"].strip()
        if not name or len(name) > 160 or name in names:
            raise ValueError("guardian_specialty_response_invalid")
        names.append(name)
    return names


def _neighbor_centers(center):
    """Sample two rings of overlapping minimum-radius searches without assuming nearest-first truncation."""
    for distance, count in ((1.5, 8), (0.75, 4)):
        for step in range(count):
            bearing = math.tau * step / count
            yield _offset(center, distance * math.sin(bearing), distance * math.cos(bearing))


def _is_gap_closed(capture, window_parts, count, keys, reason):
    scope, bounds = window_parts
    tile = GeoTile(*bounds)
    if len(keys) > count:
        return False
    if reason == "direct_only_result_cap" or not capture.has_locations_for_all_keys(scope, keys):
        if len(keys) == count:
            capture.clear_gap(window_parts)
            return True
        return False
    if capture.is_count_reconciled(scope, tile.center, RADII[0], keys, count):
        capture.add_circle(scope, tile.center, (RADII[0] - 0.1) / 1.01)
    if capture.covers_tile(scope, tile.center, _extent(tile)):
        capture.clear_gap(window_parts)
        return True
    return False


def _offset(center, east, north):
    lat, lon = map(math.radians, center)
    delta = math.hypot(east, north) / 3958.7613
    angle = math.atan2(east, north)
    target = math.asin(math.sin(lat) * math.cos(delta) + math.cos(lat) * math.sin(delta) * math.cos(angle))
    longitude = lon + math.atan2(
        math.sin(angle) * math.sin(delta) * math.cos(lat), math.cos(delta) - math.sin(lat) * math.sin(target)
    )
    return round(math.degrees(target), 7), round((math.degrees(longitude) + 180) % 360 - 180, 7)


def _next_center(center, spans):
    """Prioritize locations far from previously observed cards. Distances here never certify completeness."""
    candidates = (
        _offset(center, east / 10, north / 10)
        for east in range(-20, 21, 2)
        for north in range(-20, 21, 2)
        if math.hypot(east, north) <= 20
    )
    return max(
        (min(distance_miles(*point, *seen) - width for seen, width in spans.items()), point)
        for point in candidates
        if point not in spans
    )[1]


@dataclass
class DenseWindow:
    cells: list
    region: SearchRegion
    selector: tuple
    total: int
    keys: set
    parent_span: float
    reason: str = "unresolved_result_cap"

    @property
    def center(self):
        """Return the original publisher census center."""
        return self.region.tile.center

    async def probe(self, capture, request, center, filters, *, qualifier=None):
        """Admit concrete returned card identities and preserve the original geographic scope."""
        query = search_payload(self.region, center, RADII[0], self.selector, filters)
        if qualifier:
            field, name = qualifier
            query["searchParams"][field] = name
        page = _search_result(await request("search", query))
        if isinstance(page, str):
            capture.record_delegation(query, self.cells, page)
            return 0.0
        reported, cards = page
        if qualifier:
            field, name = qualifier
            source_name = "ddsPracticeName" if field == "officeName" else "ddsLastName"
            if any(name.casefold() not in str(card.get(source_name) or "").casefold() for card in cards):
                raise ValueError("guardian_search_name_filter_mismatch")
        scope = self.cells[0]
        extra, located = _observe(capture, scope, cards, center, RADII[0])
        if center == self.center and (filters or qualifier):
            self.keys.update(extra)
            capture.record_gap(self.cells, self.reason, self.total, self.keys, self.parent_span)
        elif not (filters or qualifier) and located and len(cards) == reported:
            capture.add_circle(scope, center, (RADII[0] - 0.1) / 1.01)
        return RADII[0] if len(cards) == reported else _observed_span(cards, RADII[0])

    def is_closed(self, capture):
        """Require a previously complete circle or the parent's exact distinct card total."""
        return _is_gap_closed(capture, self.cells, self.total, self.keys, self.reason)


async def _can_reconcile_gap(capture, window_parts, count, keys, request, parent_span, reason):
    scope, bounds = window_parts
    state, location, plan_type, client = strict_json(scope)
    region = SearchRegion(state, location, GeoTile(*bounds))
    target = DenseWindow(window_parts, region, (plan_type, client), count, keys, parent_span, reason)
    span_by_center = {target.center: parent_span}
    for point in islice(_neighbor_centers(target.center), 0, 8, 2):
        span_by_center[point] = await target.probe(capture, request, point, {})
        if target.is_closed(capture):
            return True
    tried_names = set()
    for _ in range(18):
        qualifiers = capture.boundary_names(scope, target.center, RADII[0], keys, count)
        qualifier = next((item for item in qualifiers if item not in tried_names), None)
        if qualifier:
            tried_names.add(qualifier)
            await target.probe(capture, request, target.center, {}, qualifier=qualifier)
        else:
            point = _next_center(target.center, span_by_center)
            span_by_center[point] = await target.probe(capture, request, point, {})
        if target.is_closed(capture):
            return True
    options = await request("specialty", {"planType": "DHMO" if plan_type == "MDC" else "PPO"})
    for name in _specialty_names(options):
        await target.probe(capture, request, target.center, {"specialties": [name]})
        if target.is_closed(capture):
            return True
    return False


async def _reconcile_gaps(capture, request, cancel_check, progress, counts_by_outcome, limit=1):
    """Use neighboring retained evidence first, then refine only independent outstanding scopes."""

    async def reconcile_scope(scope, guarded_request):
        """Finish neighboring cells in one scope before starting competing refinements."""
        for window_parts, count, keys, parent_span, reason in capture.gap_batches(scope):
            await cancel_check()
            is_resolved = _is_gap_closed(capture, window_parts, count, keys, reason)
            if not is_resolved and refine:
                is_resolved = await _can_reconcile_gap(
                    capture, window_parts, count, keys, guarded_request, parent_span, reason
                )
            if is_resolved:
                counts_by_outcome["reconciled_to_total"] += 1

    for refine in (False, True, False):
        for group in batched(capture.gap_scopes(), 32):
            await _run_limited(group, limit, request, reconcile_scope)
        await progress({"guardian_search": dict(counts_by_outcome), **capture.progress()})
