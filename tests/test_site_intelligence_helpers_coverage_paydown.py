# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""DB-free edge contracts for site-intelligence calculation helpers."""

import math
from types import SimpleNamespace

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import site_intelligence


@pytest.fixture(autouse=True)
def _reset_site_intelligence_caches():
    site_intelligence._TABLE_EXISTS_CACHE.clear()
    site_intelligence._ZCTA_OVERLAP_AVAILABLE_CACHE = None
    yield
    site_intelligence._TABLE_EXISTS_CACHE.clear()
    site_intelligence._ZCTA_OVERLAP_AVAILABLE_CACHE = None


class _Result:
    def __init__(self, rows=(), *, scalar=None):
        self._rows = list(rows)
        self._scalar = scalar

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return self._scalar


class _Session:
    def __init__(self, *results, failure=None, rollback_failure=None):
        self.results = list(results)
        self.failure = failure
        self.rollback_failure = rollback_failure
        self.rollback_count = 0

    def get_bind(self):
        return object()

    async def execute(self, _statement, _params=None):
        if self.failure is not None:
            raise self.failure
        if not self.results:
            raise AssertionError("unexpected site-intelligence query")
        return self.results.pop(0)

    async def rollback(self):
        self.rollback_count += 1
        if self.rollback_failure is not None:
            raise self.rollback_failure


def test_session_numeric_validation_and_scoring_helpers(monkeypatch):
    with pytest.raises(RuntimeError, match="session not available"):
        site_intelligence._get_session(SimpleNamespace(ctx=SimpleNamespace()))
    session = object()
    assert site_intelligence._get_session(
        SimpleNamespace(ctx=SimpleNamespace(sa_session=session))
    ) is session

    assert site_intelligence._safe_int(None, 7) == 7
    assert site_intelligence._safe_int("12") == 12
    assert site_intelligence._safe_int("bad", 4) == 4
    assert site_intelligence._safe_float(None, 1.5) == 1.5
    assert site_intelligence._safe_float("2.5") == 2.5
    assert site_intelligence._safe_float("bad", 3.5) == 3.5

    for empty in (None, "", "null"):
        assert site_intelligence._parse_radius_miles(empty) is None
    assert site_intelligence._parse_radius_miles(" 4.5 ") == 4.5
    with pytest.raises(InvalidUsage, match="must be numeric"):
        site_intelligence._parse_radius_miles("far")
    with pytest.raises(InvalidUsage, match="greater than 0"):
        site_intelligence._parse_radius_miles("0")
    with pytest.raises(InvalidUsage, match="<= 50"):
        site_intelligence._parse_radius_miles("51")

    for empty in (None, "", "null"):
        assert site_intelligence._parse_target_scripts_per_day(empty) == 100
    assert site_intelligence._parse_target_scripts_per_day("42.8") == 42
    with pytest.raises(InvalidUsage, match="must be numeric"):
        site_intelligence._parse_target_scripts_per_day("many")
    for invalid in ("9", "301"):
        with pytest.raises(InvalidUsage, match="between 10 and 300"):
            site_intelligence._parse_target_scripts_per_day(invalid)

    assert site_intelligence._score_band(70) == "Strong: High Viability"
    assert site_intelligence._score_band(50).startswith("Possible")
    assert site_intelligence._score_band(49).startswith("Weak")
    assert site_intelligence._effective_score_band("Recommend", False).startswith("Strong")
    assert site_intelligence._effective_score_band("Conditional", True).endswith("scripts/day")
    assert site_intelligence._effective_score_band("Conditional", False).startswith("Possible")
    assert site_intelligence._effective_score_band("Not Recommended", True).startswith("Weak")

    assert site_intelligence._base_recommendation_for_score(70)[0] == "Recommend"
    assert site_intelligence._base_recommendation_for_score(50)[0] == "Conditional"
    assert site_intelligence._base_recommendation_for_score(10)[0] == "Not Recommended"
    assert site_intelligence._confidence_percent(True, True, True, True, True, True) == 95
    assert site_intelligence._confidence_percent(False, False, False, False, False, False) == 55

    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    assert site_intelligence._address_serving_source() == "entity_address_unified"
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", " NPI_ADDRESS ")
    assert site_intelligence._address_serving_source() == "npi_address"


def test_volume_chronic_distance_and_drive_radius_helpers(monkeypatch):
    close = site_intelligence._estimate_expected_volume(50_000, 10_000, 10, 1, 5.0, 1.0, 1.0)
    medium = site_intelligence._estimate_expected_volume(50_000, 10_000, 10, 1, 5.0, 4.0, 4.0)
    far = site_intelligence._estimate_expected_volume(0, 0, 0, 100, 0.0, 8.0, 8.0)
    absent = site_intelligence._estimate_expected_volume(1_000_000, 1_000_000, 1000, 0, 100.0, None, None)
    assert close["daily"] > medium["daily"]
    assert far["daily"] == 10
    assert absent["daily"] == 300
    assert close["weekly"] == close["daily"] * 7
    assert close["annual"] == close["daily"] * 365

    assert site_intelligence._is_chronic_measure("diabetes", None) is True
    assert site_intelligence._is_chronic_measure("OTHER", "Adult asthma prevalence") is True
    assert site_intelligence._is_chronic_measure(None, "unrelated") is False

    assert site_intelligence._haversine_miles(41.0, -87.0, 41.0, -87.0) == 0
    assert site_intelligence._haversine_miles(41.0, -87.0, 42.0, -87.0) > 60

    monkeypatch.setenv("HLTHPRT_SITE_INTEL_DRIVE_SPEED_MPH", "30")
    monkeypatch.setenv("HLTHPRT_SITE_INTEL_DRIVE_CIRCUITY_FACTOR", "0")
    fallback_radius = site_intelligence._drive_minutes_to_radius_miles(60)
    assert fallback_radius == pytest.approx(30 / site_intelligence.DEFAULT_DRIVE_CIRCUITY_FACTOR)
    monkeypatch.setenv("HLTHPRT_SITE_INTEL_DRIVE_CIRCUITY_FACTOR", "1.5")
    assert site_intelligence._drive_minutes_to_radius_miles(30) == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_table_cache_and_zcta_availability_boundaries(monkeypatch):
    site_intelligence._TABLE_EXISTS_CACHE.clear()
    exists_session = _Session(_Result(scalar="mrf.geo_zip_lookup"))
    assert await site_intelligence._table_exists(
        exists_session, site_intelligence.GeoZipLookup
    ) is True

    calls = []

    async def is_table_present(_session, model):
        calls.append(model)
        return True

    monkeypatch.setattr(site_intelligence, "_table_exists", is_table_present)
    site_intelligence._TABLE_EXISTS_CACHE.clear()
    assert await site_intelligence._table_exists_cached(object(), site_intelligence.GeoZipLookup)
    assert await site_intelligence._table_exists_cached(object(), site_intelligence.GeoZipLookup)
    assert calls == [site_intelligence.GeoZipLookup]
    table = site_intelligence.GeoZipLookup.__table__
    cache_key = f"{table.schema or 'mrf'}.{table.name}"
    site_intelligence._TABLE_EXISTS_CACHE[cache_key] = (
        site_intelligence.time.monotonic()
        - site_intelligence.TABLE_EXISTS_CACHE_TTL_SECONDS
        - 1,
        True,
    )
    assert await site_intelligence._table_exists_cached(object(), site_intelligence.GeoZipLookup)
    assert calls == [site_intelligence.GeoZipLookup, site_intelligence.GeoZipLookup]

    site_intelligence._ZCTA_OVERLAP_AVAILABLE_CACHE = None
    no_bind = SimpleNamespace(execute=lambda *_args: None)
    assert await site_intelligence._zcta_overlap_available(no_bind) is False
    assert await site_intelligence._zcta_overlap_available(no_bind) is False

    site_intelligence._ZCTA_OVERLAP_AVAILABLE_CACHE = None
    available_row = SimpleNamespace(has_zcta5=True, has_geom=True, has_postgis=True)
    assert await site_intelligence._zcta_overlap_available(
        _Session(_Result([available_row]))
    ) is True

    site_intelligence._ZCTA_OVERLAP_AVAILABLE_CACHE = None
    incomplete_row = SimpleNamespace(has_zcta5=True, has_geom=False, has_postgis=True)
    assert await site_intelligence._zcta_overlap_available(
        _Session(_Result([incomplete_row]))
    ) is False

    site_intelligence._ZCTA_OVERLAP_AVAILABLE_CACHE = None
    failed = _Session(failure=RuntimeError("probe failed"))
    assert await site_intelligence._zcta_overlap_available(failed) is False
    assert failed.rollback_count == 1

    site_intelligence._ZCTA_OVERLAP_AVAILABLE_CACHE = None
    rollback_failed = _Session(
        failure=RuntimeError("probe failed"),
        rollback_failure=RuntimeError("rollback failed"),
    )
    assert await site_intelligence._zcta_overlap_available(rollback_failed) is False


@pytest.mark.asyncio
async def test_radius_zip_weights_success_fallback_and_validation(monkeypatch):
    assert await site_intelligence._radius_zip_weights(object(), 41.0, -87.0, 0) == (
        {},
        "none",
    )

    async def is_overlap_unavailable(_session):
        return False

    monkeypatch.setattr(site_intelligence, "_zcta_overlap_available", is_overlap_unavailable)
    assert await site_intelligence._radius_zip_weights(object(), 41.0, -87.0, 3) == (
        {},
        "zip_centroid",
    )

    async def is_overlap_available(_session):
        return True

    monkeypatch.setattr(site_intelligence, "_zcta_overlap_available", is_overlap_available)
    overlap_rows = [
        SimpleNamespace(zip_code="60654", overlap_weight=0.25),
        SimpleNamespace(zip_code="60654-extra", overlap_weight=0.75),
        SimpleNamespace(zip_code="6061", overlap_weight=0.5),
        SimpleNamespace(zip_code="", overlap_weight=0.5),
        SimpleNamespace(zip_code="60655", overlap_weight=0),
        SimpleNamespace(zip_code="60656", overlap_weight=2.0),
    ]
    weights, method = await site_intelligence._radius_zip_weights(
        _Session(_Result(overlap_rows)), 41.0, -87.0, 3
    )
    assert method == "zcta_polygon_overlap"
    assert weights == {"60654": 0.75, "60656": 1.0}

    failed = _Session(failure=RuntimeError("spatial query failed"))
    assert await site_intelligence._radius_zip_weights(failed, 41.0, -87.0, 3) == (
        {},
        "zip_centroid",
    )
    assert failed.rollback_count == 1

    rollback_failed = _Session(
        failure=RuntimeError("spatial query failed"),
        rollback_failure=RuntimeError("rollback failed"),
    )
    assert await site_intelligence._radius_zip_weights(
        rollback_failed, 41.0, -87.0, 3
    ) == ({}, "zip_centroid")


@pytest.mark.asyncio
async def test_nearest_anchor_and_radius_lists_ignore_unusable_rows():
    assert await site_intelligence._nearest_anchor(
        object(), 41.0, -87.0, "Hospital", 10, facility_table_available=False
    ) is None
    assert await site_intelligence._nearest_anchor(
        _Session(_Result()), 41.0, -87.0, "Hospital", 10, facility_table_available=True
    ) is None

    anchor_candidates = [
        SimpleNamespace(name="bad", latitude="bad", longitude=-87.0),
        SimpleNamespace(name="far", latitude=42.0, longitude=-87.0),
        SimpleNamespace(name=None, latitude=41.01, longitude=-87.0),
        SimpleNamespace(name="nearest", latitude=41.001, longitude=-87.0),
    ]
    nearest = await site_intelligence._nearest_anchor(
        _Session(_Result(anchor_candidates)), 41.0, -87.0, "Hospital", 100, facility_table_available=True
    )
    assert nearest["name"] == "nearest"
    assert nearest["miles"] < 1

    assert await site_intelligence._anchors_within_radius(
        object(), 41.0, -87.0, "Hospital", 5, facility_table_available=False
    ) == []
    assert await site_intelligence._anchors_within_radius(
        _Session(_Result()), 41.0, -87.0, "Hospital", 5, facility_table_available=True
    ) == []

    listed = await site_intelligence._anchors_within_radius(
        _Session(_Result(anchor_candidates)),
        41.0,
        -87.0,
        "Hospital",
        5,
        facility_table_available=True,
        limit=1,
    )
    assert listed == [
        {
            "name": "nearest",
            "lat": 41.001,
            "lng": -87.0,
            "miles": listed[0]["miles"],
        }
    ]
    assert listed[0]["miles"] < 1


@pytest.mark.asyncio
async def test_anchor_candidate_loading_groups_valid_coordinates():
    assert await site_intelligence._load_anchor_candidates(
        object(), 41.0, -87.0, 20, facility_table_available=False
    ) == {"Hospital": [], "FQHC": []}

    rows = [
        SimpleNamespace(facility_type="Hospital", name="General", latitude=41.01, longitude=-87.0),
        SimpleNamespace(facility_type="FQHC", name=None, latitude=41.02, longitude=-87.0),
        SimpleNamespace(facility_type="Hospital", name="Bad", latitude=None, longitude=-87.0),
    ]
    grouped = await site_intelligence._load_anchor_candidates(
        _Session(_Result(rows)), 41.0, -87.0, 20, facility_table_available=True
    )
    assert grouped["Hospital"][0]["name"] == "General"
    assert grouped["FQHC"][0]["name"] == ""
    assert math.isfinite(grouped["Hospital"][0]["miles"])
