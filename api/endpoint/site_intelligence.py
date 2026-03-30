# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import math
import os
import time
from collections import defaultdict

from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import and_, func, select, text

from db.models import (DoctorClinicianAddress, FacilityAnchor, GeoZipLookup,
                       LODESWorkplaceAggregate, MedicareEnrollmentStats,
                       NPIAddress, NPIDataTaxonomy, PartDPharmacyActivity,
                       PharmacyEconomicsSummary, PricingPlacesZcta)

blueprint = Blueprint("site_intelligence", url_prefix="/site-intelligence", version=1)

EARTH_RADIUS_MILES = 3958.8
MILES_TO_METERS = 1609.344
ZIP_MILES_PER_DEGREE = 69.0
DEFAULT_DRIVE_SPEED_MPH = 25.0
DEFAULT_DRIVE_CIRCUITY_FACTOR = 1.35
TRADE_AREA_MINUTES = (5, 10, 15)
CHRONIC_MEASURE_IDS = {
    "CASTHMA",
    "CHD",
    "COPD",
    "DIABETES",
    "STROKE",
    "BPHIGH",
    "KIDNEY",
}
CHRONIC_MEASURE_NAME_HINTS = (
    "asthma",
    "heart disease",
    "copd",
    "diabetes",
    "stroke",
    "high blood pressure",
    "kidney disease",
)
NP_PA_TYPE_HINTS = ("nurse practitioner", "physician assistant", "physician asst")
ANCHOR_COUNT_RADIUS_MILES = 3.0
MAX_ANCHOR_LIST_ITEMS = 25
TABLE_EXISTS_CACHE_TTL_SECONDS = 300.0
_TABLE_EXISTS_CACHE: dict[str, tuple[float, bool]] = {}
_ZCTA_OVERLAP_AVAILABLE_CACHE: tuple[float, bool] | None = None


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


async def _table_exists(session, model) -> bool:
    table = model.__table__
    schema = table.schema or "mrf"
    qualified = f"{schema}.{table.name}"
    result = await session.execute(select(func.to_regclass(qualified)))
    return bool(result.scalar())


async def _table_exists_cached(session, model) -> bool:
    table = model.__table__
    schema = table.schema or "mrf"
    cache_key = f"{schema}.{table.name}"
    now = time.monotonic()
    cached = _TABLE_EXISTS_CACHE.get(cache_key)
    if cached and (now - cached[0]) < TABLE_EXISTS_CACHE_TTL_SECONDS:
        return cached[1]
    exists = await _table_exists(session, model)
    _TABLE_EXISTS_CACHE[cache_key] = (now, exists)
    return exists


async def _zcta_overlap_available(session) -> bool:
    global _ZCTA_OVERLAP_AVAILABLE_CACHE
    now = time.monotonic()
    if _ZCTA_OVERLAP_AVAILABLE_CACHE and (
        now - _ZCTA_OVERLAP_AVAILABLE_CACHE[0]
    ) < TABLE_EXISTS_CACHE_TTL_SECONDS:
        return _ZCTA_OVERLAP_AVAILABLE_CACHE[1]
    # Test/session doubles don't support this optional path; keep deterministic behavior.
    if not hasattr(session, "get_bind"):
        _ZCTA_OVERLAP_AVAILABLE_CACHE = (now, False)
        return False
    try:
        probe = await session.execute(
            text(
                """
                SELECT
                    to_regclass('tiger.zcta5') IS NOT NULL AS has_zcta5,
                    to_regclass('tiger.zcta5') IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema='tiger'
                          AND table_name='zcta5'
                          AND column_name='the_geom'
                    ) AS has_geom,
                    to_regclass('public.spatial_ref_sys') IS NOT NULL AS has_postgis
                """
            )
        )
        row = probe.first()
        available = bool(
            row and bool(row.has_zcta5) and bool(row.has_geom) and bool(row.has_postgis)
        )
    except Exception:
        if hasattr(session, "rollback"):
            try:
                await session.rollback()
            except Exception:
                pass
        available = False
    _ZCTA_OVERLAP_AVAILABLE_CACHE = (now, available)
    return available


async def _radius_zip_weights(
    session,
    lat: float,
    lng: float,
    radius_miles: float,
) -> tuple[dict[str, float], str]:
    if radius_miles <= 0:
        return {}, "none"
    if not await _zcta_overlap_available(session):
        return {}, "zip_centroid"

    radius_meters = radius_miles * MILES_TO_METERS
    sql = text(
        """
        WITH circle AS (
          SELECT ST_Buffer(
            ST_SetSRID(ST_Point(:lng, :lat), 4326)::geography,
            :radius_meters
          ) AS geom
        )
        SELECT
          z.zcta5ce::text AS zip_code,
          GREATEST(
            0.0,
            LEAST(
              1.0,
              ST_Area(
                ST_Intersection(z.the_geom::geography, c.geom)
              ) / NULLIF(ST_Area(z.the_geom::geography), 0)
            )
          ) AS overlap_weight
        FROM tiger.zcta5 AS z
        CROSS JOIN circle AS c
        WHERE ST_Intersects(z.the_geom::geography, c.geom)
        """
    )
    try:
        rows = (await session.execute(sql, {"lat": lat, "lng": lng, "radius_meters": radius_meters})).all()
    except Exception:
        if hasattr(session, "rollback"):
            try:
                await session.rollback()
            except Exception:
                pass
        return {}, "zip_centroid"

    weights: dict[str, float] = {}
    for row in rows:
        zip_code = str(row.zip_code or "").strip()
        weight = _safe_float(row.overlap_weight, default=0.0) or 0.0
        if not zip_code or weight <= 0:
            continue
        # Keep only 5-digit ZIP for consistent joins.
        zip_code = zip_code[:5]
        if len(zip_code) != 5:
            continue
        weights[zip_code] = max(weights.get(zip_code, 0.0), min(weight, 1.0))

    return weights, ("zcta_polygon_overlap" if weights else "zip_centroid")


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return EARTH_RADIUS_MILES * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _drive_minutes_to_radius_miles(minutes: int) -> float:
    speed = float(os.getenv("HLTHPRT_SITE_INTEL_DRIVE_SPEED_MPH", str(DEFAULT_DRIVE_SPEED_MPH)))
    circuity = float(
        os.getenv("HLTHPRT_SITE_INTEL_DRIVE_CIRCUITY_FACTOR", str(DEFAULT_DRIVE_CIRCUITY_FACTOR))
    )
    circuity = circuity if circuity > 0 else DEFAULT_DRIVE_CIRCUITY_FACTOR
    return (speed * (minutes / 60.0)) / circuity


def _safe_int(value, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_radius_miles(value: str | None) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        parsed = float(str(value).strip())
    except ValueError as exc:
        raise InvalidUsage("radius_miles must be numeric") from exc
    if parsed <= 0:
        raise InvalidUsage("radius_miles must be greater than 0")
    if parsed > 50:
        raise InvalidUsage("radius_miles must be <= 50")
    return parsed


def _confidence_percent(
    has_trade_area: bool,
    has_demand: bool,
    has_chronic: bool,
    has_supply: bool,
    has_economics: bool,
    has_anchors: bool,
) -> int:
    confidence = 0.55
    confidence += 0.10 if has_trade_area else 0.0
    confidence += 0.10 if has_demand else 0.0
    confidence += 0.07 if has_chronic else 0.0
    confidence += 0.08 if has_supply else 0.0
    confidence += 0.06 if has_economics else 0.0
    confidence += 0.04 if has_anchors else 0.0
    confidence = max(0.45, min(0.95, confidence))
    return int(round(confidence * 100))


def _score_band(score: float) -> str:
    if score >= 70:
        return "Strong: High Viability"
    if score >= 50:
        return "Possible: Viable depending on competition and payer mix"
    return "Weak: High Risk"


def _effective_score_band(final_decision: str, target_adjusted: bool) -> str:
    if final_decision == "Recommend":
        return "Strong: High Viability"
    if final_decision == "Conditional":
        if target_adjusted:
            return "Possible: Target gap to configured scripts/day"
        return "Possible: Viable depending on competition and payer mix"
    return "Weak: High Risk"


def _parse_target_scripts_per_day(value: str | None) -> int:
    if value in (None, "", "null"):
        return 100
    try:
        parsed = int(float(str(value).strip()))
    except ValueError as exc:
        raise InvalidUsage("target_scripts_per_day must be numeric") from exc
    if parsed < 10 or parsed > 300:
        raise InvalidUsage("target_scripts_per_day must be between 10 and 300")
    return parsed


def _base_recommendation_for_score(score: float) -> tuple[str, str]:
    if score >= 70:
        return (
            "Recommend",
            "Strong pharmacy viability signal based on current demand/supply/economics profile.",
        )
    if score >= 50:
        return (
            "Conditional",
            "Viability is moderate; proceed with caution and confirm local competition, lease, and payer assumptions.",
        )
    return (
        "Not Recommended",
        "Current signals indicate elevated commercial risk for this location.",
    )


def _estimate_expected_volume(
    seniors: int,
    workers: int,
    np_pa_count: int,
    active_pharmacy_count: int,
    avg_margin: float,
    nearest_hospital_miles: float | None,
    nearest_fqhc_miles: float | None,
) -> dict[str, int]:
    daily = (
        seniors * 0.00028
        + workers * 0.00003
        + np_pa_count * 0.01
        - active_pharmacy_count * 0.35
        + avg_margin * 0.85
    )
    if nearest_hospital_miles is not None and nearest_hospital_miles > 0:
        if nearest_hospital_miles <= 2:
            daily += 4
        elif nearest_hospital_miles <= 5:
            daily += 2
    if nearest_fqhc_miles is not None and nearest_fqhc_miles > 0:
        if nearest_fqhc_miles <= 2:
            daily += 2
        elif nearest_fqhc_miles <= 5:
            daily += 1
    daily = max(10, min(300, daily))
    daily_rounded = int(round(daily))
    return {
        "daily": daily_rounded,
        "weekly": daily_rounded * 7,
        "monthly": int(round(daily_rounded * 30.4)),
        "annual": daily_rounded * 365,
    }


def _is_chronic_measure(measure_id: str | None, measure_name: str | None) -> bool:
    if measure_id and measure_id.upper() in CHRONIC_MEASURE_IDS:
        return True
    text = (measure_name or "").strip().lower()
    return any(hint in text for hint in CHRONIC_MEASURE_NAME_HINTS)


async def _nearest_anchor(
    session,
    lat: float,
    lng: float,
    facility_type: str,
    search_miles: float,
    *,
    facility_table_available: bool,
) -> dict | None:
    if not facility_table_available:
        return None
    lat_delta = search_miles / ZIP_MILES_PER_DEGREE
    cos_lat = max(0.15, abs(math.cos(math.radians(lat))))
    lng_delta = search_miles / (ZIP_MILES_PER_DEGREE * cos_lat)

    stmt = (
        select(FacilityAnchor.name, FacilityAnchor.latitude, FacilityAnchor.longitude)
        .where(
            and_(
                FacilityAnchor.facility_type == facility_type,
                FacilityAnchor.latitude.isnot(None),
                FacilityAnchor.longitude.isnot(None),
                FacilityAnchor.latitude.between(lat - lat_delta, lat + lat_delta),
                FacilityAnchor.longitude.between(lng - lng_delta, lng + lng_delta),
            )
        )
    )
    rows = (await session.execute(stmt)).all()
    if not rows:
        return None

    nearest = None
    for row in rows:
        row_lat = _safe_float(row.latitude)
        row_lng = _safe_float(row.longitude)
        if row_lat is None or row_lng is None:
            continue
        miles = _haversine_miles(lat, lng, row_lat, row_lng)
        if nearest is None or miles < nearest["miles"]:
            nearest = {
                "name": str(row.name or facility_type),
                "lat": row_lat,
                "lng": row_lng,
                "miles": miles,
            }
    return nearest


async def _anchors_within_radius(
    session,
    lat: float,
    lng: float,
    facility_type: str,
    search_miles: float,
    *,
    facility_table_available: bool,
    limit: int = MAX_ANCHOR_LIST_ITEMS,
) -> list[dict]:
    if not facility_table_available:
        return []
    lat_delta = search_miles / ZIP_MILES_PER_DEGREE
    cos_lat = max(0.15, abs(math.cos(math.radians(lat))))
    lng_delta = search_miles / (ZIP_MILES_PER_DEGREE * cos_lat)

    stmt = (
        select(FacilityAnchor.name, FacilityAnchor.latitude, FacilityAnchor.longitude)
        .where(
            and_(
                FacilityAnchor.facility_type == facility_type,
                FacilityAnchor.latitude.isnot(None),
                FacilityAnchor.longitude.isnot(None),
                FacilityAnchor.latitude.between(lat - lat_delta, lat + lat_delta),
                FacilityAnchor.longitude.between(lng - lng_delta, lng + lng_delta),
            )
        )
    )
    rows = (await session.execute(stmt)).all()
    if not rows:
        return []

    items: list[dict] = []
    for row in rows:
        row_lat = _safe_float(row.latitude)
        row_lng = _safe_float(row.longitude)
        if row_lat is None or row_lng is None:
            continue
        miles = _haversine_miles(lat, lng, row_lat, row_lng)
        if miles > search_miles:
            continue
        items.append(
            {
                "name": str(row.name or facility_type),
                "lat": row_lat,
                "lng": row_lng,
                "miles": round(float(miles), 2),
            }
        )
    items.sort(key=lambda item: float(item.get("miles", 999999)))
    return items[: max(1, int(limit))]


async def _load_anchor_candidates(
    session,
    lat: float,
    lng: float,
    search_miles: float,
    *,
    facility_table_available: bool,
) -> dict[str, list[dict]]:
    if not facility_table_available:
        return {"Hospital": [], "FQHC": []}
    lat_delta = search_miles / ZIP_MILES_PER_DEGREE
    cos_lat = max(0.15, abs(math.cos(math.radians(lat))))
    lng_delta = search_miles / (ZIP_MILES_PER_DEGREE * cos_lat)

    stmt = (
        select(
            FacilityAnchor.facility_type,
            FacilityAnchor.name,
            FacilityAnchor.latitude,
            FacilityAnchor.longitude,
        )
        .where(
            and_(
                FacilityAnchor.facility_type.in_(("Hospital", "FQHC")),
                FacilityAnchor.latitude.isnot(None),
                FacilityAnchor.longitude.isnot(None),
                FacilityAnchor.latitude.between(lat - lat_delta, lat + lat_delta),
                FacilityAnchor.longitude.between(lng - lng_delta, lng + lng_delta),
            )
        )
    )
    rows = (await session.execute(stmt)).all()
    by_type: dict[str, list[dict]] = {"Hospital": [], "FQHC": []}
    for row in rows:
        row_lat = _safe_float(row.latitude)
        row_lng = _safe_float(row.longitude)
        if row_lat is None or row_lng is None:
            continue
        miles = _haversine_miles(lat, lng, row_lat, row_lng)
        by_type.setdefault(str(row.facility_type or ""), []).append(
            {
                "name": str(row.name or ""),
                "lat": row_lat,
                "lng": row_lng,
                "miles": miles,
            }
        )
    return by_type


@blueprint.get("/score")
async def get_site_score(request):
    lat_str = request.args.get("lat")
    lng_str = request.args.get("lng")

    if not lat_str or not lng_str:
        raise InvalidUsage("lat and lng parameters are required")

    try:
        lat = float(lat_str)
        lng = float(lng_str)
    except ValueError as exc:
        raise InvalidUsage("lat and lng must be numeric") from exc

    if not -90.0 <= lat <= 90.0:
        raise InvalidUsage("lat must be between -90 and 90")
    if not -180.0 <= lng <= 180.0:
        raise InvalidUsage("lng must be between -180 and 180")
    pharmacy_radius_miles = _parse_radius_miles(request.args.get("radius_miles"))
    target_scripts_per_day = _parse_target_scripts_per_day(
        request.args.get("target_scripts_per_day")
    )

    session = _get_session(request)
    table_exists = {
        "geo_zip": await _table_exists_cached(session, GeoZipLookup),
        "medicare": await _table_exists_cached(session, MedicareEnrollmentStats),
        "lodes": await _table_exists_cached(session, LODESWorkplaceAggregate),
        "places": await _table_exists_cached(session, PricingPlacesZcta),
        "doctors": await _table_exists_cached(session, DoctorClinicianAddress),
        "npi_address": await _table_exists_cached(session, NPIAddress),
        "npi_taxonomy": await _table_exists_cached(session, NPIDataTaxonomy),
        "partd_pharmacy": await _table_exists_cached(session, PartDPharmacyActivity),
        "facility_anchor": await _table_exists_cached(session, FacilityAnchor),
        "economics": await _table_exists_cached(session, PharmacyEconomicsSummary),
    }
    if not table_exists["geo_zip"]:
        confidence = _confidence_percent(
            has_trade_area=False,
            has_demand=False,
            has_chronic=False,
            has_supply=False,
            has_economics=False,
            has_anchors=False,
        )
        return response.json(
            {
                "score_band": "Weak: Very Low Viability",
                "score_band_effective": "Weak: High Risk",
                "confidence": f"{confidence}%",
                "drivers": {
                    "positive": [],
                    "negative": ["ZIP reference dataset is unavailable"],
                },
                "demand_metrics": {
                    "total_seniors": "0",
                    "daytime_workers": "0",
                    "chronic_disease_rate": "N/A",
                },
                "supply_metrics": {
                    "np_pa_count": 0,
                    "provider_count": 0,
                    "active_pharmacy_count": 0,
                    "nearest_hospital_miles": "N/A",
                    "nearest_fqhc_miles": "N/A",
                    "hospital_count_3mi": 0,
                    "fqhc_count_3mi": 0,
                    "hospitals_3mi": [],
                    "fqhcs_3mi": [],
                },
                "economic_metrics": {
                    "average_gross_profit_spread": "$0.00",
                    "top_dispensed_generic": "N/A",
                    "top_dispensed_margin": "$0.00",
                },
                "expected_volume": {
                    "daily": 10,
                    "weekly": 70,
                    "monthly": 304,
                    "annual": 3650,
                },
                "score_components": {
                    "demand": 0.0,
                    "prescriber": 0.0,
                    "competition": 0.0,
                    "economics": 0.0,
                    "anchors": 0.0,
                    "total": 0.0,
                },
                "target_assessment": {
                    "target_daily": target_scripts_per_day,
                    "target_met": False,
                    "gap_daily": max(0, target_scripts_per_day - 10),
                    "coverage_ratio": round(10 / target_scripts_per_day, 4),
                },
                "recommendation": {
                    "base_decision": "Not Recommended",
                    "final_decision": "Not Recommended",
                    "rationale": "Current signals indicate elevated commercial risk for this location.",
                    "target_adjusted": False,
                },
                "trade_areas": {},
                "methodology": {
                    "trade_area_minutes": list(TRADE_AREA_MINUTES),
                    "datasets_available": table_exists,
                },
            }
        )

    radius_by_minutes = {minutes: _drive_minutes_to_radius_miles(minutes) for minutes in TRADE_AREA_MINUTES}
    max_radius = max(radius_by_minutes[max(TRADE_AREA_MINUTES)], pharmacy_radius_miles or 0.0)

    lat_delta = max_radius / ZIP_MILES_PER_DEGREE
    cos_lat = max(0.15, abs(math.cos(math.radians(lat))))
    lng_delta = max_radius / (ZIP_MILES_PER_DEGREE * cos_lat)

    zip_stmt = (
        select(GeoZipLookup.zip_code, GeoZipLookup.state, GeoZipLookup.latitude, GeoZipLookup.longitude)
        .where(
            and_(
                GeoZipLookup.latitude.between(lat - lat_delta, lat + lat_delta),
                GeoZipLookup.longitude.between(lng - lng_delta, lng + lng_delta),
            )
        )
    )
    zip_rows = (await session.execute(zip_stmt)).all()

    trade_area_zips: dict[int, set[str]] = {minutes: set() for minutes in TRADE_AREA_MINUTES}
    nearest_state = None
    nearest_zip_distance = None
    zip_distance_map: dict[str, float] = {}

    for row in zip_rows:
        zip_code = str(row.zip_code or "").strip()
        row_lat = _safe_float(row.latitude)
        row_lng = _safe_float(row.longitude)
        if not zip_code or row_lat is None or row_lng is None:
            continue
        distance = _haversine_miles(lat, lng, row_lat, row_lng)
        if distance > max_radius:
            continue
        zip_distance_map[zip_code] = distance
        if nearest_zip_distance is None or distance < nearest_zip_distance:
            nearest_zip_distance = distance
            nearest_state = row.state
        for minutes, radius_miles in radius_by_minutes.items():
            if distance <= radius_miles:
                trade_area_zips[minutes].add(zip_code)

    all_zip_codes = sorted(zip_distance_map.keys())
    if not all_zip_codes:
        confidence = _confidence_percent(
            has_trade_area=False,
            has_demand=False,
            has_chronic=False,
            has_supply=False,
            has_economics=False,
            has_anchors=False,
        )
        return response.json(
            {
                "score_band": "Weak: Very Low Viability",
                "score_band_effective": "Weak: High Risk",
                "confidence": f"{confidence}%",
                "drivers": {"positive": [], "negative": ["No ZIP centroids found in a 15-minute trade area"]},
                "demand_metrics": {
                    "total_seniors": "0",
                    "daytime_workers": "0",
                    "chronic_disease_rate": "N/A",
                },
                "supply_metrics": {
                    "np_pa_count": 0,
                    "provider_count": 0,
                    "active_pharmacy_count": 0,
                    "nearest_hospital_miles": "N/A",
                    "nearest_fqhc_miles": "N/A",
                    "hospital_count_3mi": 0,
                    "fqhc_count_3mi": 0,
                    "hospitals_3mi": [],
                    "fqhcs_3mi": [],
                },
                "economic_metrics": {
                    "average_gross_profit_spread": "$0.00",
                    "top_dispensed_generic": "N/A",
                    "top_dispensed_margin": "$0.00",
                },
                "expected_volume": {
                    "daily": 10,
                    "weekly": 70,
                    "monthly": 304,
                    "annual": 3650,
                },
                "score_components": {
                    "demand": 0.0,
                    "prescriber": 0.0,
                    "competition": 0.0,
                    "economics": 0.0,
                    "anchors": 0.0,
                    "total": 0.0,
                },
                "target_assessment": {
                    "target_daily": target_scripts_per_day,
                    "target_met": False,
                    "gap_daily": max(0, target_scripts_per_day - 10),
                    "coverage_ratio": round(10 / target_scripts_per_day, 4),
                },
                "recommendation": {
                    "base_decision": "Not Recommended",
                    "final_decision": "Not Recommended",
                    "rationale": "Current signals indicate elevated commercial risk for this location.",
                    "target_adjusted": False,
                },
                "trade_areas": {},
            }
        )

    medicare_rows = []
    if table_exists["medicare"]:
        medicare_rows = (
            await session.execute(
                select(
                    MedicareEnrollmentStats.zcta_code,
                    MedicareEnrollmentStats.year,
                    MedicareEnrollmentStats.total_beneficiaries,
                    MedicareEnrollmentStats.part_d_beneficiaries,
                ).where(MedicareEnrollmentStats.zcta_code.in_(all_zip_codes))
            )
        ).all()
    medicare_by_zip: dict[str, dict] = {}
    for row in medicare_rows:
        zip_code = str(row.zcta_code)
        year = _safe_int(getattr(row, "year", 0))
        current = medicare_by_zip.get(zip_code)
        if not current or year > current["year"]:
            medicare_by_zip[zip_code] = {
                "year": year,
                "total_beneficiaries": _safe_int(row.total_beneficiaries),
                "part_d_beneficiaries": _safe_int(row.part_d_beneficiaries),
            }

    lodes_rows = []
    if table_exists["lodes"]:
        lodes_rows = (
            await session.execute(
                select(
                    LODESWorkplaceAggregate.zcta_code,
                    LODESWorkplaceAggregate.year,
                    LODESWorkplaceAggregate.total_workers,
                ).where(
                    LODESWorkplaceAggregate.zcta_code.in_(all_zip_codes)
                )
            )
        ).all()
    workers_by_zip: dict[str, dict] = {}
    for row in lodes_rows:
        zip_code = str(row.zcta_code)
        year = _safe_int(getattr(row, "year", 0))
        current = workers_by_zip.get(zip_code)
        if not current or year > current["year"]:
            workers_by_zip[zip_code] = {
                "year": year,
                "total_workers": _safe_int(row.total_workers),
            }

    places_rows = []
    if table_exists["places"]:
        places_rows = (
            await session.execute(
                select(
                    PricingPlacesZcta.zcta,
                    PricingPlacesZcta.year,
                    PricingPlacesZcta.measure_id,
                    PricingPlacesZcta.measure_name,
                    PricingPlacesZcta.data_value,
                ).where(
                    and_(
                        PricingPlacesZcta.zcta.in_(all_zip_codes),
                        PricingPlacesZcta.measure_id.in_(tuple(CHRONIC_MEASURE_IDS)),
                        PricingPlacesZcta.data_value.isnot(None),
                    )
                )
            )
        ).all()
    latest_places_by_key: dict[tuple[str, str], tuple[int, float]] = {}
    chronic_measure_name_by_key: dict[str, str] = {}
    for row in places_rows:
        zip_code = str(row.zcta)
        measure_id = str(row.measure_id or "").strip()
        if not _is_chronic_measure(measure_id, row.measure_name):
            continue
        year = _safe_int(row.year)
        value = _safe_float(row.data_value)
        if value is None:
            continue
        measure_key = measure_id or str(row.measure_name or "unknown")
        key = (zip_code, measure_key)
        current = latest_places_by_key.get(key)
        if not current or year > current[0]:
            latest_places_by_key[key] = (year, value)
        if measure_key not in chronic_measure_name_by_key:
            chronic_measure_name_by_key[measure_key] = str(row.measure_name or measure_key)
    chronic_by_zip: dict[str, list[float]] = defaultdict(list)
    for (zip_code, _), (_year, value) in latest_places_by_key.items():
        chronic_by_zip[zip_code].append(value)

    all_providers_by_zip: dict[str, set[int]] = defaultdict(set)
    np_pa_by_zip: dict[str, set[int]] = defaultdict(set)

    provider_zip_rows = []
    if table_exists["doctors"]:
        provider_zip_rows = (
            await session.execute(
                select(
                    DoctorClinicianAddress.zip_code,
                    DoctorClinicianAddress.npi,
                    DoctorClinicianAddress.provider_type,
                ).where(DoctorClinicianAddress.zip_code.in_(all_zip_codes))
            )
        ).all()
    for row in provider_zip_rows:
        zip_code = str(row.zip_code or "")
        npi = _safe_int(row.npi, default=0)
        if not zip_code or npi <= 0:
            continue
        all_providers_by_zip[zip_code].add(npi)
        provider_type = str(row.provider_type or "").lower()
        if any(hint in provider_type for hint in NP_PA_TYPE_HINTS):
            np_pa_by_zip[zip_code].add(npi)

    active_pharmacy_rows = []
    partd_pharmacy_rows = []
    if table_exists["partd_pharmacy"]:
        partd_pharmacy_rows = (
            await session.execute(
                select(
                    PartDPharmacyActivity.zip_code,
                    PartDPharmacyActivity.npi,
                    PartDPharmacyActivity.medicare_active,
                ).where(PartDPharmacyActivity.zip_code.in_(all_zip_codes))
            )
        ).all()
    for row in partd_pharmacy_rows:
        if bool(row.medicare_active):
            active_pharmacy_rows.append(row)
    active_pharmacies_by_zip: dict[str, set[int]] = defaultdict(set)
    partd_pharmacies_by_zip: dict[str, set[int]] = defaultdict(set)
    for row in active_pharmacy_rows:
        zip_code = str(row.zip_code or "")
        npi = _safe_int(row.npi, default=0)
        if zip_code and npi > 0:
            active_pharmacies_by_zip[zip_code].add(npi)
    for row in partd_pharmacy_rows:
        zip_code = str(row.zip_code or "")
        npi = _safe_int(row.npi, default=0)
        if zip_code and npi > 0:
            partd_pharmacies_by_zip[zip_code].add(npi)

    pharmacy_rows = []
    if table_exists["npi_address"] and table_exists["npi_taxonomy"]:
        pharmacy_rows = (
            await session.execute(
                select(NPIAddress.npi, NPIAddress.lat, NPIAddress.long)
                .join(NPIDataTaxonomy, NPIDataTaxonomy.npi == NPIAddress.npi)
                .where(
                    and_(
                        NPIAddress.type == "primary",
                        NPIAddress.lat.isnot(None),
                        NPIAddress.long.isnot(None),
                        NPIAddress.lat.between(lat - lat_delta, lat + lat_delta),
                        NPIAddress.long.between(lng - lng_delta, lng + lng_delta),
                        NPIDataTaxonomy.healthcare_provider_taxonomy_code.like("3336%"),
                    )
                )
            )
        ).all()
    pharmacy_distance_map: dict[int, float] = {}
    for row in pharmacy_rows:
        npi = _safe_int(row.npi, default=0)
        row_lat = _safe_float(row.lat)
        row_lng = _safe_float(row.long)
        if npi <= 0 or row_lat is None or row_lng is None:
            continue
        distance = _haversine_miles(lat, lng, row_lat, row_lng)
        if distance > max_radius:
            continue
        current = pharmacy_distance_map.get(npi)
        if current is None or distance < current:
            pharmacy_distance_map[npi] = distance

    pharmacy_radius_value = pharmacy_radius_miles or radius_by_minutes[15]
    radius_zip_weights, selected_radius_method = await _radius_zip_weights(
        session,
        lat,
        lng,
        pharmacy_radius_value,
    )
    # Fallback to centroid inclusion when polygon overlap is unavailable.
    if not radius_zip_weights:
        radius_zip_weights = {
            z: 1.0 for z, d in zip_distance_map.items() if d <= pharmacy_radius_value
        }
        if not radius_zip_weights and zip_distance_map:
            nearest_zip = min(zip_distance_map.items(), key=lambda item: item[1])[0]
            radius_zip_weights = {nearest_zip: 1.0}
            selected_radius_method = "address_zip_fallback"
    radius_zip_codes = list(radius_zip_weights.keys())

    seniors_radius = int(
        round(
            sum(
                medicare_by_zip.get(z, {}).get("total_beneficiaries", 0) * radius_zip_weights.get(z, 0.0)
                for z in radius_zip_codes
            )
        )
    )
    workers_radius = int(
        round(
            sum(
                workers_by_zip.get(z, {}).get("total_workers", 0) * radius_zip_weights.get(z, 0.0)
                for z in radius_zip_codes
            )
        )
    )

    chronic_weighted_sum = 0.0
    chronic_weighted_den = 0.0
    for z in radius_zip_codes:
        values = chronic_by_zip.get(z, [])
        if not values:
            continue
        weight = radius_zip_weights.get(z, 0.0)
        if weight <= 0:
            continue
        zip_avg = sum(values) / len(values)
        chronic_weighted_sum += zip_avg * weight
        chronic_weighted_den += weight
    chronic_avg_radius = (
        (chronic_weighted_sum / chronic_weighted_den) if chronic_weighted_den > 0 else None
    )

    chronic_measure_values_radius: dict[str, tuple[float, float, int]] = defaultdict(
        lambda: (0.0, 0.0, 0)
    )
    for (zip_code, measure_key), (_year, value) in latest_places_by_key.items():
        weight = radius_zip_weights.get(zip_code, 0.0)
        if weight <= 0:
            continue
        sum_v, sum_w, samples = chronic_measure_values_radius[measure_key]
        chronic_measure_values_radius[measure_key] = (
            sum_v + (value * weight),
            sum_w + weight,
            samples + 1,
        )
    chronic_breakdown_radius = []
    for measure_key, (sum_v, sum_w, samples) in sorted(chronic_measure_values_radius.items()):
        if sum_w <= 0:
            continue
        chronic_breakdown_radius.append(
            {
                "measure_id": measure_key,
                "measure_name": chronic_measure_name_by_key.get(measure_key, measure_key),
                "avg_rate": round(sum_v / sum_w, 2),
                "zip_samples": samples,
            }
        )

    # Weighted ZIP approximation for selected-radius provider and pharmacy availability.
    provider_count_radius = int(
        round(
            sum(len(all_providers_by_zip.get(z, set())) * radius_zip_weights.get(z, 0.0) for z in radius_zip_codes)
        )
    )
    np_pa_count_radius = int(
        round(sum(len(np_pa_by_zip.get(z, set())) * radius_zip_weights.get(z, 0.0) for z in radius_zip_codes))
    )
    if pharmacy_distance_map:
        pharmacy_count_radius = sum(
            1 for distance in pharmacy_distance_map.values() if distance <= pharmacy_radius_value
        )
    else:
        pharmacy_count_radius = int(
            round(
                sum(
                    len(partd_pharmacies_by_zip.get(z, set())) * radius_zip_weights.get(z, 0.0)
                    for z in radius_zip_codes
                )
            )
        )
    active_pharmacy_count_radius = int(
        round(
            sum(
                len(active_pharmacies_by_zip.get(z, set())) * radius_zip_weights.get(z, 0.0)
                for z in radius_zip_codes
            )
        )
    )

    anchor_candidates = await _load_anchor_candidates(
        session,
        lat,
        lng,
        search_miles=40.0,
        facility_table_available=table_exists["facility_anchor"],
    )

    def _nearest_from_candidates(items: list[dict]) -> dict | None:
        if not items:
            return None
        nearest = min(items, key=lambda item: float(item.get("miles", 999999)))
        return {
            "name": str(nearest.get("name") or ""),
            "lat": nearest.get("lat"),
            "lng": nearest.get("lng"),
            "miles": float(nearest.get("miles") or 0.0),
        }

    def _within_radius_from_candidates(items: list[dict], search_miles: float) -> list[dict]:
        selected = [
            {
                "name": str(item.get("name") or ""),
                "lat": item.get("lat"),
                "lng": item.get("lng"),
                "miles": round(float(item.get("miles") or 0.0), 2),
            }
            for item in items
            if float(item.get("miles") or 0.0) <= search_miles
        ]
        selected.sort(key=lambda item: float(item.get("miles", 999999)))
        return selected[:MAX_ANCHOR_LIST_ITEMS]

    nearest_hospital = _nearest_from_candidates(anchor_candidates.get("Hospital", []))
    nearest_fqhc = _nearest_from_candidates(anchor_candidates.get("FQHC", []))
    hospitals_3mi = _within_radius_from_candidates(
        anchor_candidates.get("Hospital", []), ANCHOR_COUNT_RADIUS_MILES
    )
    fqhcs_3mi = _within_radius_from_candidates(
        anchor_candidates.get("FQHC", []), ANCHOR_COUNT_RADIUS_MILES
    )

    economics_rows = []
    if nearest_state and table_exists["economics"]:
        economics_rows = (
            await session.execute(
                select(
                    PharmacyEconomicsSummary.drug_name,
                    PharmacyEconomicsSummary.sdud_volume,
                    PharmacyEconomicsSummary.estimated_gross_margin,
                )
                .where(PharmacyEconomicsSummary.state == nearest_state)
                .order_by(PharmacyEconomicsSummary.sdud_volume.desc())
                .limit(20)
            )
        ).all()

    weighted_margin_numerator = 0.0
    weighted_margin_denominator = 0
    top_generic = "N/A"
    top_margin = 0.0
    if economics_rows:
        first = economics_rows[0]
        top_generic = str(first.drug_name or "N/A")
        top_margin = _safe_float(first.estimated_gross_margin, 0.0) or 0.0
        for row in economics_rows:
            volume = _safe_int(row.sdud_volume)
            margin = _safe_float(row.estimated_gross_margin)
            if volume > 0 and margin is not None:
                weighted_margin_numerator += margin * volume
                weighted_margin_denominator += volume
    avg_margin = (
        (weighted_margin_numerator / weighted_margin_denominator)
        if weighted_margin_denominator > 0
        else 0.0
    )

    trade_area_payload = {}
    for minutes in TRADE_AREA_MINUTES:
        zips = sorted(trade_area_zips[minutes])
        seniors = sum(medicare_by_zip.get(z, {}).get("total_beneficiaries", 0) for z in zips)
        workers = sum(workers_by_zip.get(z, {}).get("total_workers", 0) for z in zips)
        provider_count = len(set().union(*(all_providers_by_zip.get(z, set()) for z in zips))) if zips else 0
        np_pa_count = len(set().union(*(np_pa_by_zip.get(z, set()) for z in zips))) if zips else 0
        active_pharmacy_count = (
            len(set().union(*(active_pharmacies_by_zip.get(z, set()) for z in zips))) if zips else 0
        )
        chronic_values = [v for z in zips for v in chronic_by_zip.get(z, [])]
        chronic_avg = (sum(chronic_values) / len(chronic_values)) if chronic_values else None

        trade_area_payload[str(minutes)] = {
            "radius_miles": round(radius_by_minutes[minutes], 2),
            "zip_count": len(zips),
            "metrics": {
                "total_seniors": seniors,
                "daytime_workers": workers,
                "provider_count": provider_count,
                "np_pa_count": np_pa_count,
                "active_pharmacy_count": active_pharmacy_count,
                "avg_chronic_disease_rate": round(chronic_avg, 2) if chronic_avg is not None else None,
            },
        }

    zips_15 = sorted(trade_area_zips[15])
    seniors_15 = sum(medicare_by_zip.get(z, {}).get("total_beneficiaries", 0) for z in zips_15)
    workers_15 = sum(workers_by_zip.get(z, {}).get("total_workers", 0) for z in zips_15)
    provider_count_15 = (
        len(set().union(*(all_providers_by_zip.get(z, set()) for z in zips_15))) if zips_15 else 0
    )
    np_pa_count_15 = len(set().union(*(np_pa_by_zip.get(z, set()) for z in zips_15))) if zips_15 else 0
    if pharmacy_distance_map:
        pharmacy_count_15 = sum(
            1 for distance in pharmacy_distance_map.values() if distance <= radius_by_minutes[15]
        )
    else:
        pharmacy_count_15 = (
            len(set().union(*(partd_pharmacies_by_zip.get(z, set()) for z in zips_15))) if zips_15 else 0
        )
    active_pharmacy_count_15 = (
        len(set().union(*(active_pharmacies_by_zip.get(z, set()) for z in zips_15))) if zips_15 else 0
    )
    chronic_values_15 = [v for z in zips_15 for v in chronic_by_zip.get(z, [])]
    chronic_avg_15 = (sum(chronic_values_15) / len(chronic_values_15)) if chronic_values_15 else None
    chronic_measure_values_15: dict[str, list[float]] = defaultdict(list)
    for (zip_code, measure_key), (_year, value) in latest_places_by_key.items():
        if zip_code in zips_15:
            chronic_measure_values_15[measure_key].append(value)
    chronic_breakdown_15 = []
    for measure_key, values in sorted(chronic_measure_values_15.items()):
        if not values:
            continue
        chronic_breakdown_15.append(
            {
                "measure_id": measure_key,
                "measure_name": chronic_measure_name_by_key.get(measure_key, measure_key),
                "avg_rate": round(sum(values) / len(values), 2),
                "zip_samples": len(values),
            }
        )

    demand_score = 0.0
    if seniors_radius >= 15000:
        demand_score += 18
    elif seniors_radius >= 7000:
        demand_score += 12
    elif seniors_radius <= 1500:
        demand_score -= 8

    if workers_radius >= 30000:
        demand_score += 12
    elif workers_radius >= 12000:
        demand_score += 8
    elif workers_radius <= 4000:
        demand_score -= 4

    prescriber_score = 0.0
    if np_pa_count_15 >= 80:
        prescriber_score += 25
    elif np_pa_count_15 >= 35:
        prescriber_score += 18
    elif np_pa_count_15 >= 15:
        prescriber_score += 10
    else:
        prescriber_score -= 10

    competition_score = 0.0
    if active_pharmacy_count_15 <= 4:
        competition_score += 18
    elif active_pharmacy_count_15 <= 10:
        competition_score += 10
    elif active_pharmacy_count_15 <= 20:
        competition_score += 2
    else:
        competition_score -= 12

    economics_score = 0.0
    if avg_margin >= 14.0:
        economics_score += 15
    elif avg_margin >= 9.0:
        economics_score += 10
    elif avg_margin >= 5.0:
        economics_score += 4
    else:
        economics_score -= 4

    anchors_score = 0.0
    nearest_hospital_miles = _safe_float(nearest_hospital.get("miles")) if nearest_hospital else None
    nearest_fqhc_miles = _safe_float(nearest_fqhc.get("miles")) if nearest_fqhc else None

    if nearest_hospital_miles is not None and nearest_hospital_miles <= 3.0:
        anchors_score += 6
    elif nearest_hospital_miles is not None and nearest_hospital_miles <= 7.0:
        anchors_score += 3

    if nearest_fqhc_miles is not None and nearest_fqhc_miles <= 3.0:
        anchors_score += 4
    elif nearest_fqhc_miles is not None and nearest_fqhc_miles <= 7.0:
        anchors_score += 2

    score = demand_score + prescriber_score + competition_score + economics_score + anchors_score
    score = max(0.0, min(100.0, score))
    expected_volume = _estimate_expected_volume(
        seniors=seniors_radius,
        workers=workers_radius,
        np_pa_count=np_pa_count_radius,
        active_pharmacy_count=active_pharmacy_count_radius,
        avg_margin=avg_margin,
        nearest_hospital_miles=nearest_hospital_miles,
        nearest_fqhc_miles=nearest_fqhc_miles,
    )
    target_met = expected_volume["daily"] >= target_scripts_per_day
    target_gap = max(0, target_scripts_per_day - expected_volume["daily"])
    target_ratio = (
        round(expected_volume["daily"] / target_scripts_per_day, 4)
        if target_scripts_per_day > 0
        else 0.0
    )
    base_decision, base_rationale = _base_recommendation_for_score(score)
    final_decision = base_decision
    target_adjusted = False
    if not target_met and base_decision == "Recommend":
        final_decision = "Conditional"
        target_adjusted = True
    recommendation_rationale = base_rationale
    if not target_met:
        recommendation_rationale = (
            f"{base_rationale} Expected daily scripts ({expected_volume['daily']}) are below "
            f"the configured target ({target_scripts_per_day})."
        )

    positive_drivers = []
    negative_drivers = []
    if seniors_radius >= 7000:
        positive_drivers.append("High Medicare-eligible population in selected local radius")
    elif seniors_radius <= 1500:
        negative_drivers.append("Low Medicare-eligible population in selected local radius")
    if workers_radius >= 12000:
        positive_drivers.append("Strong daytime worker demand")
    if np_pa_count_15 >= 35:
        positive_drivers.append("Strong NP/PA prescriber density")
    elif np_pa_count_15 < 15:
        negative_drivers.append("Limited NP/PA prescriber density")
    if active_pharmacy_count_15 >= 20:
        negative_drivers.append("High active-pharmacy competition nearby")
    elif active_pharmacy_count_15 <= 4:
        positive_drivers.append("Limited active-pharmacy competition")
    if avg_margin >= 9.0:
        positive_drivers.append("Favorable state-level Medicaid gross margin profile")
    elif avg_margin < 5.0:
        negative_drivers.append("Weak state-level Medicaid gross margin profile")
    if nearest_hospital_miles is not None and nearest_hospital_miles <= 3.0:
        positive_drivers.append(f"Hospital anchor within {nearest_hospital_miles:.1f} miles")
    if nearest_fqhc_miles is not None and nearest_fqhc_miles <= 3.0:
        positive_drivers.append(f"FQHC anchor within {nearest_fqhc_miles:.1f} miles")

    confidence = _confidence_percent(
        has_trade_area=True,
        has_demand=(seniors_radius > 0 or workers_radius > 0),
        has_chronic=(chronic_avg_radius is not None),
        has_supply=(provider_count_15 > 0),
        has_economics=(weighted_margin_denominator > 0),
        has_anchors=(nearest_hospital_miles is not None or nearest_fqhc_miles is not None),
    )

    base_score_band = _score_band(score)
    effective_score_band = _effective_score_band(final_decision, target_adjusted)

    return response.json(
        {
            "score_value": round(float(score), 1),
            "score_band": base_score_band,
            "score_band_effective": effective_score_band,
            "confidence": f"{confidence}%",
            "drivers": {
                "positive": positive_drivers,
                "negative": negative_drivers,
            },
            "demand_metrics": {
                "total_seniors": f"{seniors_radius:,}",
                "daytime_workers": f"{workers_radius:,}",
                "chronic_disease_rate": (
                    f"{chronic_avg_radius:.2f}% avg"
                    if chronic_avg_radius is not None
                    else "N/A"
                ),
                "chronic_measure_breakdown": chronic_breakdown_radius,
                "baseline_15_total_seniors": f"{seniors_15:,}",
                "baseline_15_daytime_workers": f"{workers_15:,}",
                "baseline_15_chronic_disease_rate": (
                    f"{chronic_avg_15:.2f}% avg"
                    if chronic_avg_15 is not None
                    else "N/A"
                ),
                "radius_miles": round(pharmacy_radius_value, 1),
            },
            "supply_metrics": {
                "np_pa_count": np_pa_count_15,
                "np_pa_count_radius": np_pa_count_radius,
                "provider_count": provider_count_15,
                "provider_count_radius": provider_count_radius,
                "pharmacy_count": pharmacy_count_15,
                "active_pharmacy_count": active_pharmacy_count_15,
                "pharmacy_count_radius": pharmacy_count_radius,
                "active_pharmacy_count_radius": active_pharmacy_count_radius,
                "active_pharmacy_radius_miles": round(pharmacy_radius_value, 1),
                "nearest_hospital_miles": (
                    f"{nearest_hospital_miles:.1f}" if nearest_hospital_miles is not None else "N/A"
                ),
                "nearest_fqhc_miles": (
                    f"{nearest_fqhc_miles:.1f}" if nearest_fqhc_miles is not None else "N/A"
                ),
                "nearest_hospital": (
                    {
                        "name": nearest_hospital.get("name"),
                        "lat": nearest_hospital.get("lat"),
                        "lng": nearest_hospital.get("lng"),
                        "miles": round(float(nearest_hospital_miles), 2),
                    }
                    if nearest_hospital and nearest_hospital_miles is not None
                    else None
                ),
                "nearest_fqhc": (
                    {
                        "name": nearest_fqhc.get("name"),
                        "lat": nearest_fqhc.get("lat"),
                        "lng": nearest_fqhc.get("lng"),
                        "miles": round(float(nearest_fqhc_miles), 2),
                    }
                    if nearest_fqhc and nearest_fqhc_miles is not None
                    else None
                ),
                "hospital_count_3mi": len(hospitals_3mi),
                "fqhc_count_3mi": len(fqhcs_3mi),
                "hospitals_3mi": hospitals_3mi,
                "fqhcs_3mi": fqhcs_3mi,
            },
            "economic_metrics": {
                "average_gross_profit_spread": f"${avg_margin:.2f}",
                "top_dispensed_generic": top_generic,
                "top_dispensed_margin": f"${top_margin:.2f}",
                "top_generic_breakdown": [
                    {
                        "drug_name": str(row.drug_name or "N/A").strip(),
                        "estimated_margin": round(_safe_float(row.estimated_gross_margin, 0.0) or 0.0, 2),
                        "sdud_volume": _safe_int(row.sdud_volume),
                    }
                    for row in economics_rows[:10]
                ],
            },
            "expected_volume": expected_volume,
            "score_components": {
                "demand": round(float(demand_score), 1),
                "prescriber": round(float(prescriber_score), 1),
                "competition": round(float(competition_score), 1),
                "economics": round(float(economics_score), 1),
                "anchors": round(float(anchors_score), 1),
                "total": round(float(score), 1),
            },
            "target_assessment": {
                "target_daily": target_scripts_per_day,
                "target_met": target_met,
                "gap_daily": target_gap,
                "coverage_ratio": target_ratio,
            },
            "recommendation": {
                "base_decision": base_decision,
                "final_decision": final_decision,
                "rationale": recommendation_rationale,
                "target_adjusted": target_adjusted,
            },
            "trade_areas": trade_area_payload,
            "methodology": {
                "trade_area_minutes": list(TRADE_AREA_MINUTES),
                "drive_speed_mph": float(
                    os.getenv("HLTHPRT_SITE_INTEL_DRIVE_SPEED_MPH", str(DEFAULT_DRIVE_SPEED_MPH))
                ),
                "drive_circuity_factor": float(
                    os.getenv(
                        "HLTHPRT_SITE_INTEL_DRIVE_CIRCUITY_FACTOR",
                        str(DEFAULT_DRIVE_CIRCUITY_FACTOR),
                    )
                ),
                "state_used_for_economics": nearest_state,
                "active_pharmacy_radius_miles": round(pharmacy_radius_value, 1),
                "demand_scope_radius_miles": round(pharmacy_radius_value, 1),
                "demand_scope": {
                    "zcta_polygon_overlap": "selected_radius_zcta_polygon_overlap",
                    "address_zip_fallback": "selected_radius_address_zip_fallback",
                }.get(selected_radius_method, "selected_radius_zip_centroid"),
                "datasets_available": table_exists,
            },
        }
    )
