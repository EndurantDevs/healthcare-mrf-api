# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
from typing import Any

import sanic.exceptions
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import (Column, Float, Integer, MetaData, String, Table, and_, case, cast,
                        func, or_, select, text)

from api.endpoint.pagination import parse_pagination
from db.models import (CodeCatalog, CodeCrosswalk, PricingProcedure,
                       PricingProcedureGeoBenchmark,
                       DoctorClinicianAddress, EntityAddressUnified,
                       GeoZipLookup, NPIAddress, NPIData, NPIDataTaxonomy,
                       NUCCTaxonomy, ProviderEnrichmentSummary,
                       PricingPrescription, PricingProvider,
                       PricingProviderPrescription,
                       PricingProviderProcedure,
                       PricingProviderProcedureCostProfile,
                       PricingProviderProcedureLocation,
                       PricingProcedurePeerStats)

blueprint = Blueprint("pricing", url_prefix="/pricing", version=1)


provider_table = PricingProvider.__table__
procedure_table = PricingProcedure.__table__
procedure_geo_benchmark_table = PricingProcedureGeoBenchmark.__table__
provider_procedure_table = PricingProviderProcedure.__table__
location_table = PricingProviderProcedureLocation.__table__
provider_procedure_cost_profile_table = PricingProviderProcedureCostProfile.__table__
procedure_peer_stats_table = PricingProcedurePeerStats.__table__
prescription_table = PricingPrescription.__table__
provider_prescription_table = PricingProviderPrescription.__table__
code_catalog_table = CodeCatalog.__table__
code_crosswalk_table = CodeCrosswalk.__table__
geo_zip_table = GeoZipLookup.__table__
provider_enrichment_summary_table = ProviderEnrichmentSummary.__table__
npi_data_table = NPIData.__table__

QUALITY_SCORE_TABLE_NAME = "pricing_provider_quality_score"
QUALITY_DOMAIN_TABLE_NAME = "pricing_provider_quality_domain"
QUALITY_FEATURE_TABLE_NAME = "pricing_provider_quality_feature"
QUALITY_PEER_TARGET_TABLE_NAME = "pricing_provider_quality_peer_target"
QUALITY_QPP_TABLE_NAME = "pricing_qpp_provider"
QUALITY_SVI_TABLE_NAME = "pricing_svi_zcta"


MAX_LIMIT = 200
INTERNAL_CODE_SYSTEM = "HP_PROCEDURE_CODE"
INTERNAL_RX_CODE_SYSTEM = "HP_RX_CODE"
PROCEDURE_OVERRIDE_CODE_SYSTEMS = (INTERNAL_CODE_SYSTEM, "CPT", "HCPCS")
RX_EXTERNAL_CODE_PRIORITY = ("NDC", "RXNORM")
MAX_CODE_EXPANSION_HOPS = max(int(os.getenv("HLTHPRT_MAX_CODE_EXPANSION_HOPS", "4")), 1)
INT_PATTERN = re.compile(r"^-?\d+$")
FIVE_DIGIT_CODE_PATTERN = re.compile(r"^\d{5}$")
PROCEDURE_MATCH_THRESHOLD_DEFAULT = 0.30
SCORE_VARIANTS_SCOPE_PROVIDER = "provider"
SCORE_VARIANTS_SCOPE_VALUES = (SCORE_VARIANTS_SCOPE_PROVIDER,)
PROVIDER_QUALITY_MODEL_VERSION = str(os.getenv("HLTHPRT_PROVIDER_QUALITY_MODEL_VERSION", "v2")).strip() or "v2"
PROVIDER_QUALITY_SHRINKAGE_ALPHA = max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_SHRINKAGE_ALPHA", "100.0")), 1.0)
PROVIDER_QUALITY_DEFAULT_SVI = min(
    max(float(os.getenv("HLTHPRT_PROVIDER_QUALITY_DEFAULT_SVI", "0.5")), 0.0),
    1.0,
)


def _env_flag(*names: str, default: bool = False) -> bool:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        text_value = str(raw).strip()
        if not text_value:
            continue
        return text_value.lower() in {"1", "true", "yes", "on"}
    return default


_US_STATE_NAME_TO_CODE = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
    "PUERTO RICO": "PR",
    "GUAM": "GU",
    "AMERICAN SAMOA": "AS",
    "NORTHERN MARIANA ISLANDS": "MP",
    "COMMONWEALTH OF THE NORTHERN MARIANA ISLANDS": "MP",
    "US VIRGIN ISLANDS": "VI",
    "U.S. VIRGIN ISLANDS": "VI",
    "VIRGIN ISLANDS": "VI",
}


def _state_code_sql(expr: str) -> str:
    normalized = f"UPPER(NULLIF(BTRIM(COALESCE({expr}, '')), ''))"
    mapping_cases = "\n".join(
        f"            WHEN {normalized} = '{name}' THEN '{code}'"
        for name, code in _US_STATE_NAME_TO_CODE.items()
    )
    return f"""
        CASE
            WHEN {normalized} IS NULL THEN NULL
            WHEN LENGTH({normalized}) = 2 THEN {normalized}
{mapping_cases}
            ELSE NULL
        END
    """


ENABLE_PRICING_SCHEMA_CACHE = _env_flag(
    "HLTHPRT_ENABLE_PRICING_SCHEMA_CACHE",
    "HLTHPRT_ENABLE_SCHEMA_CACHE",
)
_PRICING_SCHEMA_CACHE_TTL_SECONDS = 300.0
_PRICING_TABLE_EXISTS_CACHE: dict[str, tuple[float, bool]] = {}
_PRICING_TABLE_COLUMNS_CACHE: dict[str, tuple[float, tuple[str, ...]]] = {}


def _parse_pricing_default_year() -> int | None:
    raw = str(os.getenv("HLTHPRT_PRICING_DEFAULT_YEAR", "")).strip()
    if raw == "":
        return None
    try:
        year = int(raw)
    except ValueError as exc:
        raise RuntimeError("Invalid HLTHPRT_PRICING_DEFAULT_YEAR value") from exc
    if year < 2013:
        raise RuntimeError("HLTHPRT_PRICING_DEFAULT_YEAR must be >= 2013")
    return year


PRICING_DEFAULT_YEAR = _parse_pricing_default_year()
PRICING_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
quality_score_table = Table(
    QUALITY_SCORE_TABLE_NAME,
    MetaData(),
    Column("npi", Integer),
    Column("year", Integer),
    Column("benchmark_mode", String),
    Column("tier", String),
    Column("score_0_100", Float),
    schema=PRICING_SCHEMA,
)
QUALITY_BENCHMARK_MODE_ORDER = ("zip", "state", "national")
PROCEDURE_COST_PROFILE_MIN_CLAIMS = max(int(os.getenv("HLTHPRT_COST_LEVEL_CONFIDENCE_LOW_LT", "11")), 1)
PROCEDURE_COST_PROFILE_MEDIUM_CLAIMS = max(
    int(os.getenv("HLTHPRT_COST_LEVEL_CONFIDENCE_MEDIUM_LT", "51")),
    PROCEDURE_COST_PROFILE_MIN_CLAIMS + 1,
)
PROCEDURE_ZIP_FALLBACK_STEPS_MILES = (10.0, 20.0, 30.0)
PROCEDURE_ZIP_MAX_RADIUS_MILES = max(
    float(os.getenv("HLTHPRT_PRICING_PROCEDURE_ZIP_MAX_RADIUS_MILES", "50")),
    max(PROCEDURE_ZIP_FALLBACK_STEPS_MILES),
)
PROCEDURE_COST_LEVEL_ZIP_RADIUS_DEFAULT_MILES = min(
    max(float(os.getenv("HLTHPRT_PRICING_PROCEDURE_COST_ZIP_RADIUS_DEFAULT_MILES", "30")), 0.0),
    PROCEDURE_ZIP_MAX_RADIUS_MILES,
)
PROCEDURE_SEARCH_ZIP_RADIUS_DEFAULT_MILES = min(
    max(float(os.getenv("HLTHPRT_PRICING_PROCEDURE_SEARCH_ZIP_RADIUS_DEFAULT_MILES", "0")), 0.0),
    PROCEDURE_ZIP_MAX_RADIUS_MILES,
)
PROCEDURE_COST_COHORT_STRATEGY_PRECOMPUTED = "precomputed"
PROCEDURE_COST_COHORT_STRATEGY_NEAR_DYNAMIC = "near_dynamic"
PROCEDURE_COST_COHORT_STRATEGY_VALUES = (
    PROCEDURE_COST_COHORT_STRATEGY_PRECOMPUTED,
    PROCEDURE_COST_COHORT_STRATEGY_NEAR_DYNAMIC,
)
PROCEDURE_COST_DYNAMIC_MIN_PEER_CLAIMS = max(
    int(os.getenv("HLTHPRT_COST_LEVEL_MIN_PEER_CLAIMS", str(PROCEDURE_COST_PROFILE_MIN_CLAIMS))),
    1,
)
PROCEDURE_COST_DYNAMIC_MIN_PEER_PROVIDERS = max(
    int(os.getenv("HLTHPRT_COST_LEVEL_MIN_PEER_PROVIDERS", "10")),
    2,
)
PROCEDURE_COST_DYNAMIC_IQR_FACTOR = max(
    float(os.getenv("HLTHPRT_COST_LEVEL_OUTLIER_IQR_FACTOR", "1.5")),
    0.0,
)


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _row_to_dict(row):
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    return dict(row)


def _coalesce_value(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _normalize_service_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Keep legacy fields for compatibility, but expose physician-service-native aliases.
    """
    if "procedure_code" not in payload:
        return payload
    include_legacy = bool(payload.pop("__include_legacy_fields__", False))
    procedure_code = payload.get("procedure_code")
    service_name = _coalesce_value(
        payload.get("service_description"),
        payload.get("generic_name"),
        payload.get("reported_code"),
        payload.get("brand_name"),
    )
    reported_code = _coalesce_value(payload.get("reported_code"), payload.get("brand_name"))
    reported_code_system = None
    if reported_code:
        code_text = str(reported_code).strip().upper()
        if code_text:
            reported_code_system = "CPT" if FIVE_DIGIT_CODE_PATTERN.fullmatch(code_text) else "HCPCS"
    payload["service_code_system"] = INTERNAL_CODE_SYSTEM
    payload["service_code"] = str(procedure_code) if procedure_code is not None else None
    payload["service_name"] = service_name
    payload["service_description"] = service_name
    payload["reported_code"] = str(reported_code).strip().upper() if reported_code else None
    payload["reported_code_system"] = reported_code_system
    payload["total_services"] = _coalesce_value(payload.get("total_services"), payload.get("total_claims"))
    payload["total_beneficiary_day_services"] = _coalesce_value(
        payload.get("total_beneficiary_day_services"),
        payload.get("total_30day_fills"),
    )
    payload["total_beneficiaries"] = _coalesce_value(payload.get("total_beneficiaries"), payload.get("total_benes"))
    payload["total_submitted_charges"] = _coalesce_value(payload.get("total_submitted_charges"), payload.get("total_day_supply"))
    payload["total_allowed_amount"] = _coalesce_value(payload.get("total_allowed_amount"), payload.get("total_drug_cost"))
    payload["ge65_total_services"] = _coalesce_value(payload.get("ge65_total_services"), payload.get("ge65_total_claims"))
    payload["ge65_total_beneficiaries"] = _coalesce_value(payload.get("ge65_total_beneficiaries"), payload.get("ge65_total_benes"))
    payload["ge65_total_allowed_amount"] = _coalesce_value(
        payload.get("ge65_total_allowed_amount"),
        payload.get("ge65_total_drug_cost"),
    )
    payload["legacy_field_aliases"] = {
        "procedure_code": "service_code",
        "service_description": "service_name",
        "reported_code": "reported_code",
        "total_claims": "total_services",
        "total_30day_fills": "total_beneficiary_day_services",
        "total_day_supply": "total_submitted_charges",
        "total_benes": "total_beneficiaries",
        "total_drug_cost": "total_allowed_amount",
    }
    if include_legacy:
        payload["generic_name"] = payload.get("service_description")
        payload["brand_name"] = payload.get("reported_code")
        payload["total_claims"] = payload.get("total_services")
        payload["total_30day_fills"] = payload.get("total_beneficiary_day_services")
        payload["total_day_supply"] = payload.get("total_submitted_charges")
        payload["total_benes"] = payload.get("total_beneficiaries")
        payload["total_drug_cost"] = payload.get("total_allowed_amount")
        payload["ge65_total_claims"] = payload.get("ge65_total_services")
        payload["ge65_total_benes"] = payload.get("ge65_total_beneficiaries")
        payload["ge65_total_drug_cost"] = payload.get("ge65_total_allowed_amount")
    if not include_legacy:
        for key in (
            "generic_name",
            "brand_name",
            "total_claims",
            "total_30day_fills",
            "total_day_supply",
            "total_drug_cost",
            "total_benes",
            "ge65_total_claims",
            "ge65_total_benes",
            "ge65_total_drug_cost",
            "legacy_field_aliases",
        ):
            payload.pop(key, None)
    return payload


def _normalize_provider_payload(payload: dict[str, Any], include_legacy: bool) -> dict[str, Any]:
    payload["total_services"] = _coalesce_value(payload.get("total_services"), payload.get("total_claims"))
    payload["total_reported_service_codes"] = _coalesce_value(
        payload.get("total_reported_service_codes"),
        payload.get("total_distinct_hcpcs_codes"),
        payload.get("total_30day_fills"),
    )
    payload["total_submitted_charges"] = _coalesce_value(payload.get("total_submitted_charges"), payload.get("total_day_supply"))
    payload["total_beneficiaries"] = _coalesce_value(payload.get("total_beneficiaries"), payload.get("total_benes"))
    payload["total_allowed_amount"] = _coalesce_value(payload.get("total_allowed_amount"), payload.get("total_drug_cost"))
    payload["legacy_field_aliases"] = {
        "total_claims": "total_services",
        "total_30day_fills": "total_reported_service_codes",
        "total_day_supply": "total_submitted_charges",
        "total_benes": "total_beneficiaries",
        "total_drug_cost": "total_allowed_amount",
    }
    if include_legacy:
        payload["total_claims"] = payload.get("total_services")
        payload["total_30day_fills"] = payload.get("total_reported_service_codes")
        payload["total_day_supply"] = payload.get("total_submitted_charges")
        payload["total_benes"] = payload.get("total_beneficiaries")
        payload["total_drug_cost"] = payload.get("total_allowed_amount")
    if not include_legacy:
        for key in (
            "total_distinct_hcpcs_codes",
            "total_claims",
            "total_30day_fills",
            "total_day_supply",
            "total_benes",
            "total_drug_cost",
            "legacy_field_aliases",
        ):
            payload.pop(key, None)
    return payload


def _normalize_provider_service_aggregate(payload: dict[str, Any], include_legacy: bool) -> dict[str, Any]:
    payload["total_services"] = _coalesce_value(payload.get("total_services"), payload.get("total_claims"))
    payload["total_beneficiaries"] = _coalesce_value(payload.get("total_beneficiaries"), payload.get("total_benes"))
    payload["total_submitted_charges"] = _coalesce_value(payload.get("total_submitted_charges"), payload.get("total_day_supply"))
    payload["total_allowed_amount"] = _coalesce_value(payload.get("total_allowed_amount"), payload.get("total_drug_cost"))
    total_services = _as_float(payload.get("total_services"))
    total_submitted_charges = _as_float(payload.get("total_submitted_charges"))
    total_allowed_amount = _as_float(payload.get("total_allowed_amount"))

    cost_index = _as_float(payload.get("cost_index"))
    if cost_index is None and total_services and total_services > 0 and total_allowed_amount is not None:
        cost_index = total_allowed_amount / total_services
    payload["cost_index"] = cost_index

    avg_submitted_charge = _as_float(payload.get("avg_submitted_charge"))
    if avg_submitted_charge is None and total_services and total_services > 0 and total_submitted_charges is not None:
        avg_submitted_charge = total_submitted_charges / total_services
    payload["avg_submitted_charge"] = avg_submitted_charge

    avg_allowed_amount = _as_float(payload.get("avg_allowed_amount"))
    if avg_allowed_amount is None:
        avg_allowed_amount = cost_index
    payload["avg_allowed_amount"] = avg_allowed_amount

    payload["legacy_field_aliases"] = {
        "total_claims": "total_services",
        "total_day_supply": "total_submitted_charges",
        "total_benes": "total_beneficiaries",
        "total_drug_cost": "total_allowed_amount",
        "charge_per_service_avg": "avg_submitted_charge",
        "medicare_avg_submitted_charge_per_service": "avg_submitted_charge",
        "medicare_avg_allowed_amount_per_service": "avg_allowed_amount",
        "medicare_average_price_per_service": "avg_allowed_amount",
        "average_price": "avg_allowed_amount",
    }
    if include_legacy:
        payload["total_claims"] = payload.get("total_services")
        payload["total_day_supply"] = payload.get("total_submitted_charges")
        payload["total_benes"] = payload.get("total_beneficiaries")
        payload["total_drug_cost"] = payload.get("total_allowed_amount")
        payload["charge_per_service_avg"] = payload.get("avg_submitted_charge")
        payload["medicare_avg_submitted_charge_per_service"] = payload.get("avg_submitted_charge")
        payload["medicare_avg_allowed_amount_per_service"] = payload.get("avg_allowed_amount")
        payload["medicare_average_price_per_service"] = payload.get("avg_allowed_amount")
        payload["average_price"] = payload.get("avg_allowed_amount")
    if not include_legacy:
        for key in (
            "total_claims",
            "total_day_supply",
            "total_benes",
            "total_drug_cost",
            "charge_per_service_avg",
            "medicare_avg_submitted_charge_per_service",
            "medicare_avg_allowed_amount_per_service",
            "medicare_average_price_per_service",
            "average_price",
            "legacy_field_aliases",
        ):
            payload.pop(key, None)
    return payload


def _normalize_prescription_payload(payload: dict[str, Any]) -> dict[str, Any]:
    payload["prescription_code_system"] = payload.get("rx_code_system")
    payload["prescription_code"] = payload.get("rx_code")
    payload["prescription_name"] = payload.get("rx_name") or payload.get("generic_name") or payload.get("brand_name")
    payload["total_prescriptions"] = payload.get("total_claims")
    payload["total_beneficiaries"] = payload.get("total_benes")
    payload["total_allowed_amount"] = payload.get("total_drug_cost")
    payload["ge65_total_prescriptions"] = payload.get("ge65_total_claims")
    payload["ge65_total_beneficiaries"] = payload.get("ge65_total_benes")
    payload["ge65_total_allowed_amount"] = payload.get("ge65_total_drug_cost")
    return payload


def _normalize_prescription_provider_aggregate(payload: dict[str, Any]) -> dict[str, Any]:
    payload["total_prescriptions"] = payload.get("total_claims")
    payload["total_beneficiaries"] = payload.get("total_benes")
    payload["total_allowed_amount"] = payload.get("total_drug_cost")
    return payload


def _parse_int(raw: Any, param: str, minimum: int | None = None) -> int | None:
    if raw in (None, "", "null"):
        return None
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise InvalidUsage(f"Parameter '{param}' must be an integer") from exc
    if minimum is not None and value < minimum:
        raise InvalidUsage(f"Parameter '{param}' must be >= {minimum}")
    return value


def _parse_float(raw: Any, param: str, minimum: float | None = None) -> float | None:
    if raw in (None, "", "null"):
        return None
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise InvalidUsage(f"Parameter '{param}' must be numeric") from exc
    if minimum is not None and value < minimum:
        raise InvalidUsage(f"Parameter '{param}' must be >= {minimum}")
    return value


def _normalize_zip5(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) < 5:
        return None
    return digits[:5]


def _parse_zip_radius_miles(raw: Any, *, param: str, default: float) -> float:
    if raw in (None, "", "null"):
        value = default
    else:
        try:
            value = float(str(raw).strip())
        except (TypeError, ValueError) as exc:
            raise InvalidUsage(f"Parameter '{param}' must be numeric") from exc
    if value < 0:
        raise InvalidUsage(f"Parameter '{param}' must be >= 0")
    return min(value, PROCEDURE_ZIP_MAX_RADIUS_MILES)


def _distance_miles_expression(anchor_lat: float, anchor_long: float):
    return (
        69.0
        * func.sqrt(
            func.pow(geo_zip_table.c.latitude - anchor_lat, 2)
            + func.pow(
                (geo_zip_table.c.longitude - anchor_long)
                * func.cos(func.radians((geo_zip_table.c.latitude + anchor_lat) / 2.0)),
                2,
            )
        )
    )


def _distance_bucket(distance_miles: float | None) -> str | None:
    if distance_miles is None:
        return None
    if distance_miles <= 0.001:
        return "zip_exact"
    for step in PROCEDURE_ZIP_FALLBACK_STEPS_MILES:
        if distance_miles <= step + 1e-6:
            return f"within_{int(step)}mi"
    return f"within_{int(PROCEDURE_ZIP_MAX_RADIUS_MILES)}mi_plus"


async def _lookup_zip_context(session, zip5: str | None) -> dict[str, Any] | None:
    normalized_zip = _normalize_zip5(zip5)
    if not normalized_zip:
        return None
    lookup_result = await session.execute(
        select(
            geo_zip_table.c.zip_code.label("zip5"),
            geo_zip_table.c.state.label("state"),
            geo_zip_table.c.city_lower.label("city_lower"),
            geo_zip_table.c.latitude.label("latitude"),
            geo_zip_table.c.longitude.label("longitude"),
        )
        .where(geo_zip_table.c.zip_code == normalized_zip)
        .limit(1)
    )
    row = lookup_result.first()
    if row is None:
        return None
    payload = _row_to_dict(row)
    payload["zip5"] = _normalize_zip5(payload.get("zip5")) or normalized_zip
    payload["state"] = str(payload.get("state") or "").strip().upper() or None
    payload["city_lower"] = str(payload.get("city_lower") or "").strip().lower() or None
    payload["latitude"] = _as_float(payload.get("latitude"))
    payload["longitude"] = _as_float(payload.get("longitude"))
    return payload


async def _zip_radius_rows(
    session,
    *,
    zip5: str | None,
    radius_miles: float,
    state_hint: str | None = None,
    anchor_context: dict[str, Any] | None = None,
    limit: int = 512,
) -> list[dict[str, Any]]:
    normalized_zip = _normalize_zip5(zip5)
    if not normalized_zip:
        return []

    anchor = anchor_context
    if anchor is not None:
        anchor_zip = _normalize_zip5(anchor.get("zip5"))
        if anchor_zip != normalized_zip:
            anchor = None
    if anchor is None:
        anchor = await _lookup_zip_context(session, normalized_zip)
    state_hint_normalized = str(state_hint or "").strip().upper() or None
    if anchor is None:
        return [
            {
                "zip5": normalized_zip,
                "state": state_hint_normalized,
                "city_lower": None,
                "distance_miles": 0.0,
                "is_anchor": True,
            }
        ]

    anchor_lat = _as_float(anchor.get("latitude"))
    anchor_long = _as_float(anchor.get("longitude"))
    anchor_state = str(anchor.get("state") or "").strip().upper() or None
    state_filter = state_hint_normalized or anchor_state
    if anchor_lat is None or anchor_long is None:
        return [
            {
                "zip5": normalized_zip,
                "state": state_filter,
                "city_lower": anchor.get("city_lower"),
                "distance_miles": 0.0,
                "is_anchor": True,
            }
        ]

    distance_expr = _distance_miles_expression(anchor_lat, anchor_long).label("distance_miles")
    filters = [
        geo_zip_table.c.latitude.isnot(None),
        geo_zip_table.c.longitude.isnot(None),
        distance_expr <= max(radius_miles, 0.0),
    ]
    if state_filter:
        filters.append(geo_zip_table.c.state == state_filter)

    result = await session.execute(
        select(
            geo_zip_table.c.zip_code.label("zip5"),
            geo_zip_table.c.state.label("state"),
            geo_zip_table.c.city_lower.label("city_lower"),
            distance_expr,
        )
        .where(and_(*filters))
        .order_by(distance_expr.asc(), geo_zip_table.c.zip_code.asc())
        .limit(limit)
    )

    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for row in result:
        payload = _row_to_dict(row)
        candidate_zip = _normalize_zip5(payload.get("zip5"))
        if candidate_zip is None or candidate_zip in seen:
            continue
        seen.add(candidate_zip)
        distance_value = _as_float(payload.get("distance_miles"))
        rows.append(
            {
                "zip5": candidate_zip,
                "state": str(payload.get("state") or "").strip().upper() or None,
                "city_lower": str(payload.get("city_lower") or "").strip().lower() or None,
                "distance_miles": distance_value,
                "is_anchor": candidate_zip == normalized_zip,
            }
        )

    if normalized_zip not in seen:
        rows.insert(
            0,
            {
                "zip5": normalized_zip,
                "state": state_filter,
                "city_lower": anchor.get("city_lower"),
                "distance_miles": 0.0,
                "is_anchor": True,
            },
        )
    return rows


def _zip_ring_candidates(
    zip_rows: list[dict[str, Any]],
    *,
    anchor_zip5: str | None,
    radius_miles: float,
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    if not zip_rows:
        return [], {}

    normalized_anchor = _normalize_zip5(anchor_zip5)
    ordered_rows = sorted(
        zip_rows,
        key=lambda item: (_as_float(item.get("distance_miles")) or 0.0, str(item.get("zip5") or "")),
    )

    steps = [step for step in PROCEDURE_ZIP_FALLBACK_STEPS_MILES if step <= radius_miles + 1e-6]
    if not steps and radius_miles > 0:
        steps = [radius_miles]
    elif steps and steps[-1] < radius_miles:
        steps.append(radius_miles)

    ordered_zips: list[str] = []
    metadata: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()

    if normalized_anchor:
        seen.add(normalized_anchor)
        ordered_zips.append(normalized_anchor)
        metadata[normalized_anchor] = {
            "distance_miles": 0.0,
            "distance_bucket": "zip_exact",
            "selected_radius_miles": 0.0,
            "is_anchor": True,
        }

    for step in steps:
        for row in ordered_rows:
            candidate_zip = _normalize_zip5(row.get("zip5"))
            if candidate_zip is None or candidate_zip in seen:
                continue
            distance_value = _as_float(row.get("distance_miles"))
            if distance_value is None or distance_value > step + 1e-6:
                continue
            seen.add(candidate_zip)
            ordered_zips.append(candidate_zip)
            metadata[candidate_zip] = {
                "distance_miles": distance_value,
                "distance_bucket": _distance_bucket(distance_value),
                "selected_radius_miles": float(step),
                "is_anchor": bool(row.get("is_anchor")),
            }

    return ordered_zips, metadata


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_bool(raw: Any, param: str, default: bool = False) -> bool:
    if raw in (None, ""):
        return default
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise InvalidUsage(f"Parameter '{param}' must be boolean")


def _normalize_code_system(raw: Any) -> str:
    text = str(raw or INTERNAL_CODE_SYSTEM).strip().upper()
    return text or INTERNAL_CODE_SYSTEM


def _parse_procedure_override_code_system(raw: Any, param_name: str = "procedure_code_system") -> str:
    system = _normalize_code_system(raw or INTERNAL_CODE_SYSTEM)
    if system not in PROCEDURE_OVERRIDE_CODE_SYSTEMS:
        allowed = ", ".join(PROCEDURE_OVERRIDE_CODE_SYSTEMS)
        raise InvalidUsage(f"Parameter '{param_name}' must be one of: {allowed}")
    return system


def _normalize_code(raw: Any, param: str = "code") -> str:
    text = str(raw or "").strip().upper()
    if not text:
        raise InvalidUsage(f"Parameter '{param}' is required")
    return text


def _normalize_query_text(raw: Any, param: str = "q", min_len: int = 2) -> str:
    text = str(raw or "").strip().lower()
    if len(text) < min_len:
        raise InvalidUsage(f"Parameter '{param}' must be at least {min_len} characters")
    return text


def _cost_level_from_percentile(percentile: float) -> str:
    if percentile <= 0.20:
        return "$"
    if percentile <= 0.40:
        return "$$"
    if percentile <= 0.60:
        return "$$$"
    if percentile <= 0.80:
        return "$$$$"
    return "$$$$$"


def _cost_level_from_thresholds(
    value: float | None,
    p20: float | None,
    p40: float | None,
    p60: float | None,
    p80: float | None,
) -> str | None:
    if value is None:
        return None
    if p20 is not None and value <= float(p20):
        return "$"
    if p40 is not None and value <= float(p40):
        return "$$"
    if p60 is not None and value <= float(p60):
        return "$$$"
    if p80 is not None and value <= float(p80):
        return "$$$$"
    return "$$$$$"


def _confidence_label_from_claim_count(claim_count: float | None) -> str:
    count = float(claim_count or 0.0)
    if count < PROCEDURE_COST_PROFILE_MIN_CLAIMS:
        return "low"
    if count < PROCEDURE_COST_PROFILE_MEDIUM_CLAIMS:
        return "medium"
    return "high"


def _percentile_cont(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    p = min(max(float(percentile), 0.0), 1.0)
    rank = (len(values) - 1) * p
    lower_idx = int(math.floor(rank))
    upper_idx = int(math.ceil(rank))
    if lower_idx == upper_idx:
        return float(values[lower_idx])
    fraction = rank - lower_idx
    return float(values[lower_idx] + (values[upper_idx] - values[lower_idx]) * fraction)


def _trim_charge_rows_log_iqr(
    rows: list[dict[str, Any]],
    *,
    iqr_factor: float,
) -> list[dict[str, Any]]:
    if len(rows) < 4:
        return rows
    charge_logs = sorted(
        math.log(max(_as_float(row.get("avg_submitted_charge")) or 0.0, 1e-9))
        for row in rows
        if (_as_float(row.get("avg_submitted_charge")) or 0.0) > 0
    )
    if len(charge_logs) < 4:
        return rows
    q1 = _percentile_cont(charge_logs, 0.25)
    q3 = _percentile_cont(charge_logs, 0.75)
    if q1 is None or q3 is None or q3 <= q1:
        return rows
    spread = q3 - q1
    lower_bound = q1 - iqr_factor * spread
    upper_bound = q3 + iqr_factor * spread

    trimmed: list[dict[str, Any]] = []
    for row in rows:
        charge = _as_float(row.get("avg_submitted_charge"))
        if charge is None or charge <= 0:
            continue
        charge_ln = math.log(max(charge, 1e-9))
        if lower_bound <= charge_ln <= upper_bound:
            trimmed.append(row)
    return trimmed or rows


async def _build_dynamic_zip_peer_stats(
    session,
    *,
    year: int,
    procedure_code: int,
    setting_key: str,
    specialty_candidates: list[str],
    zip_candidates: list[str],
    min_claims: int,
    min_providers: int,
    iqr_factor: float,
) -> tuple[dict[str, Any], str] | None:
    if not zip_candidates:
        return None

    for specialty_candidate in specialty_candidates:
        filters = [
            provider_procedure_cost_profile_table.c.year == year,
            provider_procedure_cost_profile_table.c.procedure_code == procedure_code,
            provider_procedure_cost_profile_table.c.setting_key == setting_key,
            provider_procedure_cost_profile_table.c.geography_scope == "zip5",
            provider_procedure_cost_profile_table.c.geography_value.in_(zip_candidates),
            provider_procedure_cost_profile_table.c.claim_count >= min_claims,
            provider_procedure_cost_profile_table.c.avg_submitted_charge > 0,
        ]
        if specialty_candidate != "__all__":
            filters.append(provider_procedure_cost_profile_table.c.specialty_key == specialty_candidate)

        candidate_result = await session.execute(
            select(
                provider_procedure_cost_profile_table.c.npi,
                provider_procedure_cost_profile_table.c.claim_count,
                provider_procedure_cost_profile_table.c.avg_submitted_charge,
                provider_procedure_cost_profile_table.c.geography_value,
            ).where(and_(*filters))
        )
        candidate_rows = [_row_to_dict(row) for row in candidate_result]
        if len(candidate_rows) < min_providers:
            continue

        trimmed_rows = _trim_charge_rows_log_iqr(candidate_rows, iqr_factor=iqr_factor)
        if len(trimmed_rows) < min_providers:
            continue

        charges = sorted(
            float(_as_float(row.get("avg_submitted_charge")) or 0.0)
            for row in trimmed_rows
            if (_as_float(row.get("avg_submitted_charge")) or 0.0) > 0
        )
        if len(charges) < min_providers:
            continue
        claim_counts = [float(_as_float(row.get("claim_count")) or 0.0) for row in trimmed_rows]

        peer_payload = {
            "provider_count": len(trimmed_rows),
            "min_claim_count": min(claim_counts) if claim_counts else None,
            "max_claim_count": max(claim_counts) if claim_counts else None,
            "p10": _percentile_cont(charges, 0.10),
            "p20": _percentile_cont(charges, 0.20),
            "p40": _percentile_cont(charges, 0.40),
            "p50": _percentile_cont(charges, 0.50),
            "p60": _percentile_cont(charges, 0.60),
            "p80": _percentile_cont(charges, 0.80),
            "p90": _percentile_cont(charges, 0.90),
            "specialty_key": specialty_candidate,
        }
        return peer_payload, specialty_candidate
    return None


def _parse_setting_key(raw: Any) -> str:
    value = str(raw or "all").strip().lower()
    return value or "all"


def _parse_procedure_cost_cohort_strategy(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if not value:
        return PROCEDURE_COST_COHORT_STRATEGY_PRECOMPUTED
    if value in {"default", "static"}:
        return PROCEDURE_COST_COHORT_STRATEGY_PRECOMPUTED
    if value not in PROCEDURE_COST_COHORT_STRATEGY_VALUES:
        allowed = ", ".join(PROCEDURE_COST_COHORT_STRATEGY_VALUES)
        raise InvalidUsage(f"Parameter 'cohort_strategy' must be one of: {allowed}")
    return value


def _parse_specialty_key(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    return text or None


def _cohort_type_from_scope(scope: str | None) -> str:
    scope_key = str(scope or "").strip().lower()
    if scope_key == "zip5":
        return "zip"
    if scope_key in {"zip_radius", "state_city", "state", "national"}:
        return scope_key
    return scope_key or "unknown"


def _specialty_scope_label(specialty_key: str | None) -> str:
    key = str(specialty_key or "").strip().lower()
    if key == "__all__":
        return "all_specialties"
    return "specialty_specific"


def _parse_score_variants_scope(raw: Any, param_name: str = "variants_scope") -> str | None:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    if text not in SCORE_VARIANTS_SCOPE_VALUES:
        allowed = ", ".join(SCORE_VARIANTS_SCOPE_VALUES)
        raise InvalidUsage(f"Parameter '{param_name}' must be one of: {allowed}")
    return text


def _validate_removed_variants_param(raw: Any, param_name: str = "variants") -> None:
    text = str(raw or "").strip().lower()
    if not text:
        return
    raise InvalidUsage(
        f"Parameter '{param_name}' is no longer supported; use 'variants_scope=provider'."
    )


def _parse_probability_clamped(raw: Any, param: str, default: float) -> float:
    if raw in (None, "", "null"):
        value = default
    else:
        try:
            value = float(str(raw).strip())
        except (TypeError, ValueError) as exc:
            raise InvalidUsage(f"Parameter '{param}' must be numeric") from exc
    return min(1.0, max(0.0, value))


def _extract_query_values(args, key: str) -> list[Any]:
    if hasattr(args, "getlist"):
        values = list(args.getlist(key))
        if values:
            return values
    raw = args.get(key)
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        return list(raw)
    return [raw]


def _parse_code_list_query_param(args, key: str) -> list[str]:
    values = _extract_query_values(args, key)
    parsed: list[str] = []
    for raw_value in values:
        for token in str(raw_value or "").split(","):
            normalized = str(token).strip().upper()
            if normalized:
                parsed.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for token in parsed:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def _geography_candidates(
    *,
    state_raw: Any,
    city_raw: Any,
    zip5_raw: Any,
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    state = str(state_raw or "").strip().upper()
    city = str(city_raw or "").strip().lower()
    zip5 = _normalize_zip5(zip5_raw) or ""

    if zip5:
        candidates.append(("zip5", zip5))
    if state and city:
        candidates.append(("state_city", f"{state}|{city}"))
    if state:
        candidates.append(("state", state))
    candidates.append(("national", "US"))

    # Keep order and de-duplicate.
    seen: set[tuple[str, str]] = set()
    ordered: list[tuple[str, str]] = []
    for item in candidates:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


async def _enrich_provider_service_cost_indices(
    session,
    items: list[dict[str, Any]],
    *,
    year: int,
    internal_codes: list[int],
    fallback_state: str | None = None,
    fallback_city: str | None = None,
    fallback_zip5: str | None = None,
) -> None:
    if not items or not internal_codes:
        return
    if not await _table_exists(session, provider_procedure_cost_profile_table.name):
        return
    if not await _table_exists(session, procedure_peer_stats_table.name):
        return

    npi_candidates: dict[int, list[tuple[str, str]]] = {}
    geography_values_by_scope: dict[str, set[str]] = {"national": {"US"}}
    for item in items:
        npi_raw = item.get("npi")
        if npi_raw is None:
            continue
        try:
            npi = int(str(npi_raw).strip())
        except (TypeError, ValueError):
            continue
        if npi <= 0:
            continue

        state = str(item.get("state") or fallback_state or "").strip().upper()
        city = str(item.get("city") or fallback_city or "").strip().lower()
        zip5 = str(item.get("zip5") or fallback_zip5 or "").strip()
        geography_candidates = _geography_candidates(state_raw=state, city_raw=city, zip5_raw=zip5)
        if not geography_candidates:
            continue

        npi_candidates[npi] = geography_candidates
        for scope, value in geography_candidates:
            geography_values_by_scope.setdefault(scope, set()).add(value)

    if not npi_candidates:
        return

    geography_clauses = []
    for scope, values in geography_values_by_scope.items():
        sanitized_values = sorted({str(value).strip() for value in values if str(value).strip()})
        if not sanitized_values:
            continue
        geography_clauses.append(
            and_(
                provider_procedure_cost_profile_table.c.geography_scope == scope,
                provider_procedure_cost_profile_table.c.geography_value.in_(sanitized_values),
            )
        )
    if not geography_clauses:
        return

    profile_result = await session.execute(
        select(provider_procedure_cost_profile_table).where(
            and_(
                provider_procedure_cost_profile_table.c.year == year,
                provider_procedure_cost_profile_table.c.npi.in_(sorted(npi_candidates.keys())),
                provider_procedure_cost_profile_table.c.procedure_code.in_(internal_codes),
                provider_procedure_cost_profile_table.c.setting_key == "all",
                or_(*geography_clauses),
            )
        )
    )
    profile_rows = [_row_to_dict(row) for row in profile_result]
    if not profile_rows:
        return

    profiles_by_npi_and_geo: dict[tuple[int, str, str], list[dict[str, Any]]] = {}
    for row in profile_rows:
        npi_raw = row.get("npi")
        if npi_raw is None:
            continue
        try:
            npi = int(npi_raw)
        except (TypeError, ValueError):
            continue
        geography_scope = str(row.get("geography_scope") or "").strip()
        geography_value = str(row.get("geography_value") or "").strip()
        if not geography_scope or not geography_value:
            continue
        profiles_by_npi_and_geo.setdefault((npi, geography_scope, geography_value), []).append(row)

    selected_profiles: dict[int, dict[str, Any]] = {}
    for npi, candidates in npi_candidates.items():
        selected: dict[str, Any] | None = None
        for geography_scope, geography_value in candidates:
            candidates_for_geo = profiles_by_npi_and_geo.get((npi, geography_scope, geography_value), [])
            if not candidates_for_geo:
                continue
            selected = max(
                candidates_for_geo,
                key=lambda payload: (
                    _as_float(payload.get("claim_count")) or 0.0,
                    _as_float(payload.get("avg_submitted_charge")) or 0.0,
                ),
            )
            break
        if selected is not None:
            selected_profiles[npi] = selected

    if not selected_profiles:
        return

    peer_key_candidates: set[tuple[int, int, str, str, str, str]] = set()
    for profile in selected_profiles.values():
        procedure_code_raw = profile.get("procedure_code")
        year_raw = profile.get("year")
        geography_scope = str(profile.get("geography_scope") or "").strip()
        geography_value = str(profile.get("geography_value") or "").strip()
        specialty_key = str(profile.get("specialty_key") or "").strip().lower()
        setting_key = str(profile.get("setting_key") or "all").strip().lower() or "all"
        if not geography_scope or not geography_value:
            continue
        try:
            procedure_code = int(procedure_code_raw)
            profile_year = int(year_raw)
        except (TypeError, ValueError):
            continue
        specialty_candidates = [specialty_key] if specialty_key else []
        specialty_candidates.append("__all__")
        for peer_specialty in specialty_candidates:
            peer_key_candidates.add(
                (
                    procedure_code,
                    profile_year,
                    geography_scope,
                    geography_value,
                    peer_specialty,
                    setting_key,
                )
            )

    if not peer_key_candidates:
        return

    peer_clauses = [
        and_(
            procedure_peer_stats_table.c.procedure_code == procedure_code,
            procedure_peer_stats_table.c.year == profile_year,
            procedure_peer_stats_table.c.geography_scope == geography_scope,
            procedure_peer_stats_table.c.geography_value == geography_value,
            procedure_peer_stats_table.c.specialty_key == peer_specialty,
            procedure_peer_stats_table.c.setting_key == setting_key,
        )
        for (
            procedure_code,
            profile_year,
            geography_scope,
            geography_value,
            peer_specialty,
            setting_key,
        ) in peer_key_candidates
    ]
    peer_result = await session.execute(select(procedure_peer_stats_table).where(or_(*peer_clauses)))
    peer_rows = [_row_to_dict(row) for row in peer_result]
    if not peer_rows:
        return

    peers_by_key: dict[tuple[int, int, str, str, str, str], dict[str, Any]] = {}
    for row in peer_rows:
        try:
            key = (
                int(row.get("procedure_code")),
                int(row.get("year")),
                str(row.get("geography_scope") or "").strip(),
                str(row.get("geography_value") or "").strip(),
                str(row.get("specialty_key") or "").strip().lower(),
                str(row.get("setting_key") or "").strip().lower(),
            )
        except (TypeError, ValueError):
            continue
        peers_by_key[key] = row

    for item in items:
        npi_raw = item.get("npi")
        if npi_raw is None:
            continue
        try:
            npi = int(str(npi_raw).strip())
        except (TypeError, ValueError):
            continue

        profile = selected_profiles.get(npi)
        if profile is None:
            continue

        try:
            procedure_code = int(profile.get("procedure_code"))
            profile_year = int(profile.get("year"))
        except (TypeError, ValueError):
            continue

        geography_scope = str(profile.get("geography_scope") or "").strip()
        geography_value = str(profile.get("geography_value") or "").strip()
        specialty_key = str(profile.get("specialty_key") or "").strip().lower()
        setting_key = str(profile.get("setting_key") or "all").strip().lower() or "all"

        peer_row = peers_by_key.get(
            (
                procedure_code,
                profile_year,
                geography_scope,
                geography_value,
                specialty_key,
                setting_key,
            )
        )
        if peer_row is None:
            peer_row = peers_by_key.get(
                (
                    procedure_code,
                    profile_year,
                    geography_scope,
                    geography_value,
                    "__all__",
                    setting_key,
                )
            )
        if peer_row is None:
            continue

        estimated_cost_level = _cost_level_from_thresholds(
            _as_float(profile.get("avg_submitted_charge")),
            _as_float(peer_row.get("p20")),
            _as_float(peer_row.get("p40")),
            _as_float(peer_row.get("p60")),
            _as_float(peer_row.get("p80")),
        )
        if estimated_cost_level is None:
            continue
        item["cost_index"] = estimated_cost_level
        item["estimated_cost_level"] = estimated_cost_level


async def _default_year(session, table) -> int:
    result = await session.execute(select(func.max(table.c.year)))
    year = result.scalar()
    if year is None:
        raise sanic.exceptions.NotFound("No pricing data available")
    return int(year)


async def _resolve_year(session, table, requested_year: int | None) -> tuple[int, str]:
    if requested_year is not None:
        return requested_year, "request"
    if PRICING_DEFAULT_YEAR is not None:
        return PRICING_DEFAULT_YEAR, "env"
    return await _default_year(session, table), "data_max"


async def _table_exists(session, table_name: str) -> bool:
    qualified_name = table_name if "." in table_name else f"{PRICING_SCHEMA}.{table_name}"
    if ENABLE_PRICING_SCHEMA_CACHE:
        cached = _PRICING_TABLE_EXISTS_CACHE.get(qualified_name)
        if cached is not None:
            cached_at, cached_value = cached
            if (time.monotonic() - cached_at) <= _PRICING_SCHEMA_CACHE_TTL_SECONDS:
                return bool(cached_value)
            _PRICING_TABLE_EXISTS_CACHE.pop(qualified_name, None)
    result = await session.execute(text("SELECT to_regclass(:name)"), {"name": qualified_name})
    exists = bool(result.scalar())
    if ENABLE_PRICING_SCHEMA_CACHE:
        _PRICING_TABLE_EXISTS_CACHE[qualified_name] = (time.monotonic(), exists)
    return exists


async def _table_columns(session, table_name: str) -> set[str]:
    qualified_name = table_name if "." in table_name else f"{PRICING_SCHEMA}.{table_name}"
    schema_name, simple_name = qualified_name.split(".", 1)
    if ENABLE_PRICING_SCHEMA_CACHE:
        cached = _PRICING_TABLE_COLUMNS_CACHE.get(qualified_name)
        if cached is not None:
            cached_at, cached_columns = cached
            if (time.monotonic() - cached_at) <= _PRICING_SCHEMA_CACHE_TTL_SECONDS:
                return set(cached_columns)
            _PRICING_TABLE_COLUMNS_CACHE.pop(qualified_name, None)
    result = await session.execute(
        text(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = :schema_name
               AND table_name = :table_name
            """
        ),
        {"schema_name": schema_name, "table_name": simple_name},
    )
    columns = tuple(str(row[0]).strip() for row in result.fetchall() if row and row[0])
    if ENABLE_PRICING_SCHEMA_CACHE:
        _PRICING_TABLE_COLUMNS_CACHE[qualified_name] = (time.monotonic(), columns)
    return set(columns)


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text_value = str(value).strip().lower()
    if text_value in {"1", "true", "yes", "y", "on"}:
        return True
    if text_value in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _ci_payload(low: Any, high: Any) -> dict[str, float | None]:
    return {
        "low": _as_float(low),
        "high": _as_float(high),
    }


async def _resolve_quality_year(session, requested_year: int | None) -> tuple[int, str]:
    if requested_year is not None:
        return requested_year, "request"
    if PRICING_DEFAULT_YEAR is not None:
        return PRICING_DEFAULT_YEAR, "env"
    result = await session.execute(
        text(f"SELECT MAX(year) FROM {PRICING_SCHEMA}.{QUALITY_SCORE_TABLE_NAME}")
    )
    year = result.scalar()
    if year is None:
        raise sanic.exceptions.NotFound("No provider quality score data available")
    return int(year), "data_max"


def _parse_benchmark_mode(raw_value: Any, param_name: str = "benchmark_mode") -> str | None:
    if raw_value is None:
        return None
    value = str(raw_value).strip().lower()
    if not value:
        return None
    if value not in QUALITY_BENCHMARK_MODE_ORDER:
        allowed = ", ".join(QUALITY_BENCHMARK_MODE_ORDER)
        raise InvalidUsage(f"Parameter '{param_name}' must be one of: {allowed}")
    return value


async def _resolve_quality_benchmark_mode(
    session,
    year: int,
    requested_mode: str | None,
) -> tuple[str, str]:
    if requested_mode in QUALITY_BENCHMARK_MODE_ORDER:
        return requested_mode, "request"
    result = await session.execute(
        text(
            f"""
            SELECT benchmark_mode
            FROM {PRICING_SCHEMA}.{QUALITY_SCORE_TABLE_NAME}
            WHERE year = :year
            GROUP BY benchmark_mode
            ORDER BY CASE benchmark_mode
                WHEN 'zip' THEN 0
                WHEN 'state' THEN 1
                WHEN 'national' THEN 2
                ELSE 3
            END ASC
            LIMIT 1
            """
        ),
        {"year": year},
    )
    mode = str(result.scalar() or "").strip().lower()
    if mode in QUALITY_BENCHMARK_MODE_ORDER:
        return mode, "data_priority"
    return "national", "default"


def _empty_domain_payload() -> dict[str, Any]:
    return {
        "risk_ratio_point": None,
        "score_0_100": None,
        "evidence_n": None,
        "ci_75": {"low": None, "high": None},
        "ci_90": {"low": None, "high": None},
    }


def _empty_domains_payload() -> dict[str, dict[str, Any]]:
    return {
        "appropriateness": _empty_domain_payload(),
        "effectiveness": _empty_domain_payload(),
        "cost": _empty_domain_payload(),
    }


def _build_quality_mode_payload(
    score_data: dict[str, Any],
    domains_payload: dict[str, Any],
    cohort_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_profile = _build_quality_evidence_profile(score_data, cohort_context)
    payload = {
        "model_version": str(score_data.get("model_version") or "v2"),
        "benchmark_mode": score_data.get("benchmark_mode"),
        "tier": score_data.get("tier"),
        "borderline_status": _as_bool(score_data.get("borderline_status")),
        "score_0_100": _as_float(score_data.get("score_0_100")),
        "estimated_cost_level": score_data.get("estimated_cost_level"),
        "score_method": score_data.get("score_method"),
        "confidence_0_100": _as_float(score_data.get("confidence_0_100")),
        "confidence_band": score_data.get("confidence_band"),
        "cost_source": score_data.get("cost_source"),
        "data_coverage_0_100": _as_float(score_data.get("data_coverage_0_100")),
        "provider_class": _normalize_provider_class(score_data.get("provider_class")),
        "evidence_profile": evidence_profile,
        "unavailable_reasons": list(score_data.get("unavailable_reasons") or []),
        "overall": {
            "risk_ratio_point": _as_float(score_data.get("risk_ratio_point")),
            "ci_75": _ci_payload(score_data.get("ci75_low"), score_data.get("ci75_high")),
            "ci_90": _ci_payload(score_data.get("ci90_low"), score_data.get("ci90_high")),
        },
        "domains": domains_payload,
        "curation_checks": {
            "low_score_threshold_failed": bool(score_data.get("low_score_threshold_failed")),
            "low_confidence_threshold_failed": bool(score_data.get("low_confidence_threshold_failed")),
            "high_score_threshold_passed": bool(score_data.get("high_score_threshold_passed")),
            "high_confidence_threshold_passed": bool(score_data.get("high_confidence_threshold_passed")),
        },
    }
    if cohort_context is not None:
        payload["cohort_context"] = cohort_context
    return payload


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _lowercase_keys(payload: dict[str, Any]) -> dict[str, Any]:
    return {str(key).lower(): value for key, value in payload.items()}


def _pick_first_from_lowered(payload_lower: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload_lower.get(key.lower())
        if value is not None:
            return value
    return None


def _normalize_cohort_level(raw: Any) -> str | None:
    text = str(raw or "").strip().upper()
    if not text:
        return None
    if text in {"L0", "L1", "L2", "L3"}:
        return text
    if text in {"0", "1", "2", "3"}:
        return f"L{text}"
    return None


def _cohort_context_from_score_row(
    score_data: dict[str, Any],
    *,
    computed_live: bool,
    procedure_match_threshold: float | None = None,
) -> dict[str, Any] | None:
    context_raw = score_data.get("cohort_context")
    context: dict[str, Any]
    if isinstance(context_raw, dict):
        context = dict(context_raw)
    else:
        selected_scope = _normalize_peer_geography_scope(
            score_data.get("cohort_geography_scope")
            or score_data.get("cohort_geo_scope")
        )
        selected_value = (
            score_data.get("cohort_geography_value")
            or score_data.get("cohort_geo_value")
        )
        context = {
            "selected_geography": score_data.get("selected_geography")
            or _selected_geography_label(selected_scope, selected_value),
            "selected_cohort_level": score_data.get("selected_cohort_level")
            or score_data.get("cohort_level")
            or score_data.get("cohort_tier"),
            "peer_count": score_data.get("peer_count")
            or score_data.get("cohort_peer_n"),
            "specialty_key": score_data.get("specialty_key")
            or score_data.get("cohort_specialty_key")
            or score_data.get("cohort_specialty"),
            "taxonomy_code": score_data.get("taxonomy_code")
            or score_data.get("cohort_taxonomy_code")
            or score_data.get("cohort_taxonomy"),
            "procedure_bucket": score_data.get("procedure_bucket")
            or score_data.get("cohort_procedure_bucket"),
            "procedure_match_threshold": score_data.get("procedure_match_threshold"),
        }
        if not any(value is not None for value in context.values()):
            return None

    threshold_value = _as_float(context.get("procedure_match_threshold"))
    if threshold_value is None and procedure_match_threshold is not None:
        threshold_value = procedure_match_threshold
    if threshold_value is not None:
        context["procedure_match_threshold"] = min(1.0, max(0.0, float(threshold_value)))

    context["selected_geography"] = context.get("selected_geography")
    context["selected_cohort_level"] = _normalize_cohort_level(context.get("selected_cohort_level"))
    context["peer_count"] = _as_int(context.get("peer_count"))
    context["specialty_key"] = _parse_specialty_key(context.get("specialty_key"))
    taxonomy_code = context.get("taxonomy_code")
    context["taxonomy_code"] = str(taxonomy_code).strip().upper() if taxonomy_code not in (None, "") else None
    procedure_bucket = context.get("procedure_bucket")
    context["procedure_bucket"] = str(procedure_bucket).strip() if procedure_bucket not in (None, "") else None
    context["computed_live"] = bool(computed_live)
    return context


def _normalize_provider_class(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    return text or None


def _selected_geography_label(scope: str | None, value: Any) -> str | None:
    normalized_scope = _normalize_peer_geography_scope(scope)
    value_text = str(value or "").strip()
    if normalized_scope == "national":
        return "national"
    if normalized_scope in {"zip", "state"} and value_text:
        return f"{normalized_scope}:{value_text}"
    return value_text or None


def _build_quality_evidence_profile(
    score_data: dict[str, Any],
    cohort_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = cohort_context if isinstance(cohort_context, dict) else None
    location_source = str(score_data.get("location_source") or "").strip() or None
    cohort_level = _normalize_cohort_level(
        _coalesce_value(
            context.get("selected_cohort_level") if context else None,
            score_data.get("cohort_level"),
            score_data.get("cohort_tier"),
        )
    )
    cohort_size = _as_int(
        _coalesce_value(
            context.get("peer_count") if context else None,
            score_data.get("cohort_peer_n"),
            score_data.get("peer_count"),
        )
    )
    return {
        "has_claims": bool(_as_bool(score_data.get("has_claims"))),
        "has_qpp": bool(_as_bool(score_data.get("has_qpp"))),
        "has_rx": bool(_as_bool(score_data.get("has_rx"))),
        "has_enrollment": bool(_as_bool(score_data.get("has_enrollment"))),
        "has_medicare_claims": bool(_as_bool(score_data.get("has_medicare_claims"))),
        "location_source": location_source,
        "cohort_level": cohort_level,
        "cohort_size": cohort_size,
    }


def _tier_from_quality_summary(
    *,
    risk_ratio_point: float | None,
    ci75_high: float | None,
    ci90_low: float | None,
    confidence_0_100: float | None = None,
) -> str | None:
    rr_value = _as_float(risk_ratio_point)
    ci75_high_value = _as_float(ci75_high)
    ci90_low_value = _as_float(ci90_low)
    confidence_value = _as_float(confidence_0_100) or 0.0
    if rr_value is None:
        return None
    if confidence_value >= 55.0:
        if rr_value >= 1.12 and (ci90_low_value or 0.0) >= 1.08:
            return "low"
        if rr_value <= 0.88 and ci75_high_value is not None and ci75_high_value < 1.0:
            return "high"
    return "acceptable"


def _estimated_data_coverage_score(profile: dict[str, Any]) -> float:
    score = 10.0
    if profile.get("taxonomy_code"):
        score += 15.0
    if profile.get("specialty_key"):
        score += 10.0
    if profile.get("zip5"):
        score += 10.0
    if profile.get("state_key"):
        score += 5.0
    if _normalize_provider_class(profile.get("provider_class")) not in {None, "unknown"}:
        score += 10.0
    if profile.get("has_enrollment"):
        score += 5.0
    if profile.get("has_medicare_claims"):
        score += 5.0
    if str(profile.get("location_source") or "").strip() not in {"", "unknown"}:
        score += 5.0
    return round(min(score, 60.0), 2)


def _estimated_confidence_score(profile: dict[str, Any], peer_count: int) -> float:
    confidence = 15.0
    if profile.get("taxonomy_code"):
        confidence += 12.0
    if profile.get("specialty_key"):
        confidence += 6.0
    if profile.get("zip5"):
        confidence += 8.0
    elif profile.get("state_key"):
        confidence += 5.0
    if _normalize_provider_class(profile.get("provider_class")) not in {None, "unknown"}:
        confidence += 4.0
    if str(profile.get("location_source") or "").strip() not in {"", "unknown"}:
        confidence += 4.0
    if peer_count >= 100:
        confidence += 5.0
    elif peer_count >= 30:
        confidence += 3.0
    return round(min(confidence, 54.0), 2)


def _build_provider_quality_response_payload(
    *,
    provider_npi: int,
    year_used: int,
    year_source: str,
    selected_payload: dict[str, Any],
    selected_mode: str,
    scores_by_benchmark_mode: dict[str, dict[str, Any] | None],
    benchmark_mode: str | None,
    restrict_available_to_selected: bool = False,
    variants_scope: str | None = None,
    variants_by_benchmark_mode: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    available_modes = [
        mode
        for mode in QUALITY_BENCHMARK_MODE_ORDER
        if scores_by_benchmark_mode.get(mode) is not None
    ]
    response_payload = {
        "npi": provider_npi,
        "year_used": year_used,
        "year_source": year_source,
        "model_version": selected_payload.get("model_version"),
        "benchmark_mode": selected_mode,
        "tier": selected_payload.get("tier"),
        "borderline_status": selected_payload.get("borderline_status"),
        "score_0_100": selected_payload.get("score_0_100"),
        "estimated_cost_level": selected_payload.get("estimated_cost_level"),
        "score_method": selected_payload.get("score_method"),
        "confidence_0_100": selected_payload.get("confidence_0_100"),
        "confidence_band": selected_payload.get("confidence_band"),
        "cost_source": selected_payload.get("cost_source"),
        "data_coverage_0_100": selected_payload.get("data_coverage_0_100"),
        "provider_class": selected_payload.get("provider_class"),
        "evidence_profile": selected_payload.get("evidence_profile"),
        "unavailable_reasons": selected_payload.get("unavailable_reasons"),
        "overall": selected_payload.get("overall"),
        "domains": selected_payload.get("domains"),
        "curation_checks": selected_payload.get("curation_checks"),
        "available_benchmark_modes": (
            available_modes
            if (
                benchmark_mode is None
                or selected_payload.get("score_method") == "unavailable"
                or not restrict_available_to_selected
            )
            else [selected_mode]
        ),
        "scores_by_benchmark_mode": scores_by_benchmark_mode,
    }
    if selected_payload.get("cohort_context") is not None:
        response_payload["cohort_context"] = selected_payload.get("cohort_context")
    if variants_scope == SCORE_VARIANTS_SCOPE_PROVIDER:
        response_payload["variants_scope"] = variants_scope
        response_payload["variants_by_benchmark_mode"] = variants_by_benchmark_mode or {
            mode: []
            for mode in QUALITY_BENCHMARK_MODE_ORDER
        }
    return response_payload


def _parse_token_list(value: Any) -> list[str]:
    if value is None:
        return []

    token_source: list[Any] = []
    if isinstance(value, (list, tuple, set)):
        token_source = list(value)
    elif isinstance(value, str):
        raw_text = value.strip()
        if not raw_text:
            return []
        parsed_json: Any = None
        if raw_text.startswith("[") and raw_text.endswith("]"):
            try:
                parsed_json = json.loads(raw_text)
            except json.JSONDecodeError:
                parsed_json = None
        if isinstance(parsed_json, list):
            token_source = list(parsed_json)
        else:
            token_source = re.split(r"[,\s|;]+", raw_text)
    else:
        token_source = [value]

    normalized_tokens: list[str] = []
    for item in token_source:
        token = str(item or "").strip().upper()
        if not token:
            continue
        if INT_PATTERN.fullmatch(token):
            normalized_tokens.append(str(int(token)))
        else:
            normalized_tokens.append(token)
    return normalized_tokens


def _normalize_peer_geography_scope(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    if text in {"zip", "zip5", "zip_code", "zipcode", "postal", "postal_code", "zip_ring"}:
        return "zip"
    if text in {"state", "state_key", "region", "province"}:
        return "state"
    if text in {"national", "country", "us", "usa", "all"}:
        return "national"
    return text or None


def _extract_peer_target_geography(payload_lower: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    selected_geography = _pick_first_from_lowered(payload_lower, "selected_geography")
    selected_geography_text = str(selected_geography or "").strip()
    if selected_geography_text:
        if ":" in selected_geography_text:
            prefix, suffix = selected_geography_text.split(":", 1)
            scope = _normalize_peer_geography_scope(prefix)
            value = str(suffix or "").strip() or None
            if scope == "national":
                return "national", "US", "national"
            if scope is not None:
                return scope, value, selected_geography_text
        selected_scope = _normalize_peer_geography_scope(selected_geography_text)
        if selected_scope == "national":
            return "national", "US", "national"

    scope = _normalize_peer_geography_scope(
        _pick_first_from_lowered(
            payload_lower,
            "geography_scope",
            "geography_level",
            "geography_type",
            "geo_scope",
            "scope",
        )
    )
    value_raw = _pick_first_from_lowered(
        payload_lower,
        "geography_value",
        "geography_key",
        "geo_value",
        "zip5",
        "zipcode",
        "zip",
        "state_key",
        "state",
    )
    value_text = str(value_raw or "").strip() or None

    if scope is None and value_text is not None:
        value_upper = value_text.upper()
        if value_upper in {"US", "USA", "NATIONAL", "ALL", "*"}:
            scope = "national"
        elif len(value_upper) == 2 and value_upper.isalpha():
            scope = "state"
        elif len(value_upper) >= 5 and value_upper[:5].isdigit():
            scope = "zip"

    if scope == "national":
        return "national", "US", "national"
    if scope is None:
        return None, value_text, selected_geography_text or None
    return scope, value_text, selected_geography_text or (f"{scope}:{value_text}" if value_text else None)


def _geography_priority_for_benchmark_mode(
    benchmark_mode: str,
    *,
    state_key: str | None,
    zip5: str | None,
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    if benchmark_mode == "zip" and zip5:
        candidates.append(("zip", zip5))
    if benchmark_mode in {"zip", "state"} and state_key:
        candidates.append(("state", state_key))
    candidates.append(("national", "US"))

    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for scope, value in candidates:
        item = (scope, str(value).strip())
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _row_matches_geography(
    row_scope: str | None,
    row_value: str | None,
    target_scope: str,
    target_value: str,
) -> bool:
    row_scope = _normalize_peer_geography_scope(row_scope)
    row_value_text = str(row_value or "").strip().upper()
    target_value_text = str(target_value or "").strip().upper()

    if target_scope == "national":
        if row_scope in {None, "national"}:
            return True
        return row_value_text in {"US", "USA", "NATIONAL", "ALL", "*"}

    if row_scope != target_scope:
        return False
    if not target_value_text:
        return True

    if target_scope == "zip":
        return row_value_text[:5] == target_value_text[:5]
    if target_scope == "state":
        return (
            row_value_text == target_value_text
            or row_value_text.startswith(f"{target_value_text}|")
            or row_value_text.startswith(f"{target_value_text}:")
        )
    return row_value_text == target_value_text


def _value_matches_or_generic(row_value: Any, requested_value: str | None, *, upper: bool) -> bool:
    if requested_value is None:
        return True
    text = str(row_value or "").strip()
    if not text:
        return True
    normalized = text.upper() if upper else text.lower()
    generic_values = {"__all__", "all", "any", "*", "n/a", "na", "none", "null"}
    if normalized.lower() in generic_values:
        return True
    request_normalized = requested_value.upper() if upper else requested_value.lower()
    return normalized == request_normalized


def _procedure_match_ratio(payload_lower: dict[str, Any], requested_procedure_codes: set[str]) -> tuple[float, str | None]:
    bucket_raw = _pick_first_from_lowered(
        payload_lower,
        "procedure_bucket",
        "procedure_codes",
        "procedure_code_list",
        "procedure_codes_csv",
        "procedure_code_set",
    )
    bucket_tokens = set(_parse_token_list(bucket_raw))
    bucket_display = str(bucket_raw).strip() if bucket_raw not in (None, "") else None
    if not requested_procedure_codes:
        return 1.0, bucket_display
    if not bucket_tokens:
        return 1.0, bucket_display
    overlap = requested_procedure_codes.intersection(bucket_tokens)
    ratio = len(overlap) / max(len(requested_procedure_codes), 1)
    return ratio, bucket_display


def _peer_count_from_row(payload_lower: dict[str, Any]) -> int:
    peer_count = _pick_first_from_lowered(payload_lower, "peer_count", "peer_n", "peer_size", "n_peers")
    return _as_int(peer_count) or 0


def _cohort_level_rank(cohort_level: str | None) -> int:
    normalized = _normalize_cohort_level(cohort_level) or "L3"
    return int(normalized[-1])


def _peer_target_sort_key(item: dict[str, Any]) -> tuple[int, int, float, float]:
    return (
        int(item["geography_rank"]),
        int(item["cohort_rank"]),
        -float(item["peer_count"]),
        -float(item["procedure_match_ratio"]),
    )


def _get_cached_lowercase_payload(row: dict[str, Any]) -> dict[str, Any]:
    cached = row.get("__payload_lower")
    if isinstance(cached, dict):
        return cached
    source = row
    nested_row_data = row.get("row_data")
    if isinstance(nested_row_data, dict):
        source = nested_row_data
    payload_lower = _lowercase_keys(source)
    row["__payload_lower"] = payload_lower
    return payload_lower


def _collect_peer_target_candidates(
    rows: list[dict[str, Any]],
    *,
    benchmark_mode: str,
    state_key: str | None,
    zip5: str | None,
    specialty_key: str | None,
    taxonomy_code: str | None,
    requested_procedure_codes: set[str],
) -> list[dict[str, Any]]:
    geography_priority = _geography_priority_for_benchmark_mode(
        benchmark_mode,
        state_key=state_key,
        zip5=zip5,
    )
    candidates: list[dict[str, Any]] = []

    for row in rows:
        payload_lower = _get_cached_lowercase_payload(row)
        row_mode = str(_pick_first_from_lowered(payload_lower, "benchmark_mode", "mode") or "").strip().lower()
        if row_mode and row_mode in QUALITY_BENCHMARK_MODE_ORDER and row_mode != benchmark_mode:
            continue

        row_scope, row_value, row_geography_label = _extract_peer_target_geography(payload_lower)
        geography_rank = None
        matched_geography_scope = None
        matched_geography_value = None
        for index, (target_scope, target_value) in enumerate(geography_priority):
            if _row_matches_geography(row_scope, row_value, target_scope, target_value):
                geography_rank = index
                matched_geography_scope = target_scope
                matched_geography_value = target_value
                break
        if geography_rank is None:
            continue

        cohort_level = _normalize_cohort_level(
            _pick_first_from_lowered(payload_lower, "cohort_level", "cohort_tier", "cohort", "level")
        ) or "L3"
        specialty_match = _value_matches_or_generic(
            _pick_first_from_lowered(payload_lower, "specialty_key", "specialty"),
            specialty_key,
            upper=False,
        )
        taxonomy_match = _value_matches_or_generic(
            _pick_first_from_lowered(payload_lower, "taxonomy_code", "taxonomy"),
            taxonomy_code,
            upper=True,
        )
        procedure_match_ratio, procedure_bucket = _procedure_match_ratio(payload_lower, requested_procedure_codes)

        candidates.append(
            {
                "row": row,
                "payload_lower": payload_lower,
                "geography_rank": geography_rank,
                "cohort_level": cohort_level,
                "cohort_rank": _cohort_level_rank(cohort_level),
                "specialty_match": specialty_match,
                "taxonomy_match": taxonomy_match,
                "procedure_match_ratio": procedure_match_ratio,
                "strict_match": bool(specialty_match and taxonomy_match),
                "procedure_bucket": procedure_bucket,
                "peer_count": _peer_count_from_row(payload_lower),
                "selected_geography": row_geography_label
                or (
                    "national"
                    if matched_geography_scope == "national"
                    else f"{matched_geography_scope}:{matched_geography_value}"
                ),
            }
        )

    return sorted(candidates, key=_peer_target_sort_key)


def _variant_scope_inputs_from_mode_payload(
    mode_payload: dict[str, Any] | None,
    *,
    fallback_specialty_key: str | None,
    fallback_taxonomy_code: str | None,
    fallback_procedure_codes: set[str],
) -> tuple[str | None, str | None, set[str]]:
    if not isinstance(mode_payload, dict):
        return fallback_specialty_key, fallback_taxonomy_code, set(fallback_procedure_codes)
    context = mode_payload.get("cohort_context")
    if not isinstance(context, dict):
        return fallback_specialty_key, fallback_taxonomy_code, set(fallback_procedure_codes)

    specialty_key = _parse_specialty_key(context.get("specialty_key")) or fallback_specialty_key
    taxonomy_raw = context.get("taxonomy_code")
    taxonomy_code = (
        str(taxonomy_raw).strip().upper()
        if taxonomy_raw not in (None, "")
        else fallback_taxonomy_code
    )
    procedure_bucket = context.get("procedure_bucket")
    procedure_codes = set(_parse_token_list(procedure_bucket))
    if not procedure_codes:
        procedure_codes = set(fallback_procedure_codes)
    return specialty_key, taxonomy_code, procedure_codes


def _collect_provider_scope_variant_candidates(
    candidates: list[dict[str, Any]],
    *,
    provider_specialty_key: str | None,
    provider_taxonomy_code: str | None,
    provider_procedure_codes: set[str],
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for candidate in candidates:
        payload_lower = candidate.get("payload_lower")
        if not isinstance(payload_lower, dict):
            continue

        row_specialty = _parse_specialty_key(
            _pick_first_from_lowered(payload_lower, "specialty_key", "specialty")
        )
        row_taxonomy_raw = _pick_first_from_lowered(payload_lower, "taxonomy_code", "taxonomy")
        row_taxonomy = str(row_taxonomy_raw).strip().upper() if row_taxonomy_raw not in (None, "") else None

        if provider_specialty_key and row_specialty and row_specialty != provider_specialty_key:
            continue
        if provider_taxonomy_code and row_taxonomy != provider_taxonomy_code:
            continue
        if provider_procedure_codes and (_as_float(candidate.get("procedure_match_ratio")) or 0.0) <= 0.0:
            continue
        filtered.append(candidate)

    if filtered:
        return filtered
    if candidates:
        return [candidates[0]]
    return []


def _select_peer_target_candidate(
    rows: list[dict[str, Any]],
    *,
    benchmark_mode: str,
    state_key: str | None,
    zip5: str | None,
    specialty_key: str | None,
    taxonomy_code: str | None,
    requested_procedure_codes: set[str],
    procedure_match_threshold: float,
) -> dict[str, Any] | None:
    candidates = _collect_peer_target_candidates(
        rows,
        benchmark_mode=benchmark_mode,
        state_key=state_key,
        zip5=zip5,
        specialty_key=specialty_key,
        taxonomy_code=taxonomy_code,
        requested_procedure_codes=requested_procedure_codes,
    )
    if not candidates:
        return None

    strict_candidates = [
        item
        for item in candidates
        if item["specialty_match"]
        and item["taxonomy_match"]
        and float(item["procedure_match_ratio"]) >= procedure_match_threshold
    ]
    if strict_candidates:
        return strict_candidates[0]

    return candidates[0]


def _extract_peer_target_values(payload_lower: dict[str, Any]) -> dict[str, float | None]:
    return {
        "target_appropriateness": _as_float(
            _pick_first_from_lowered(
                payload_lower,
                "target_appropriateness",
                "appropriateness_target",
                "target_utilization",
                "target_utilization_adjusted",
            )
        ),
        "target_rx_appropriateness": _as_float(
            _pick_first_from_lowered(
                payload_lower,
                "target_rx_appropriateness",
                "rx_appropriateness_target",
                "target_rx_claim_rate",
                "target_drug_appropriateness",
            )
        ),
        "target_effectiveness": _as_float(
            _pick_first_from_lowered(
                payload_lower,
                "target_effectiveness",
                "effectiveness_target",
                "target_qpp_quality",
                "target_quality_score",
            )
        ),
        "target_qpp_cost": _as_float(
            _pick_first_from_lowered(
                payload_lower,
                "target_qpp_cost",
                "qpp_cost_target",
                "target_cost_score",
            )
        ),
        "target_cost": _as_float(
            _pick_first_from_lowered(
                payload_lower,
                "target_cost",
                "cost_target",
                "target_allowed_amount",
                "target_cost_adjusted",
            )
        ),
    }


def _geometric_mean(values: list[float | None]) -> float:
    if not values:
        return 1.0
    safe_values = [max(_as_float(value) or 1.0, 0.0001) for value in values]
    return math.exp(sum(math.log(value) for value in safe_values) / len(safe_values))


def _score_from_risk_ratio(risk_ratio: float | None) -> float:
    rr = _as_float(risk_ratio) or 1.0
    return round(min(100.0, max(0.0, 50.0 - 125.0 * (rr - 1.0))), 2)


def _estimated_cost_level_from_risk_ratio(rr_cost: float | None) -> str:
    value = _as_float(rr_cost) or 1.0
    if value <= 0.80:
        return "$"
    if value <= 0.90:
        return "$$"
    if value <= 1.10:
        return "$$$"
    if value <= 1.25:
        return "$$$$"
    return "$$$$$"


def _compute_live_measure(
    *,
    observed: float | None,
    target: float | None,
    evidence_n: float,
    reverse_ratio: bool,
) -> dict[str, float]:
    observed_value = _as_float(observed)
    target_value = _as_float(target)
    evidence = max(_as_float(evidence_n) or 0.0, 1.0)
    if reverse_ratio:
        if observed_value is None or observed_value <= 0 or target_value is None or target_value <= 0:
            rr_raw = 1.0
        else:
            rr_raw = target_value / observed_value
    else:
        if observed_value is None or observed_value <= 0 or target_value is None or target_value <= 0:
            rr_raw = 1.0
        else:
            rr_raw = observed_value / target_value

    shrink_weight = min(1.0, max(0.0, evidence / (evidence + PROVIDER_QUALITY_SHRINKAGE_ALPHA)))
    stderr = max(0.0001, math.sqrt(1.0 / (evidence + 1.0)))
    risk_ratio = 1.0 + (rr_raw - 1.0) * shrink_weight
    return {
        "risk_ratio": risk_ratio,
        "ci75_low": max(0.01, risk_ratio - 1.15 * stderr),
        "ci75_high": risk_ratio + 1.15 * stderr,
        "ci90_low": max(0.01, risk_ratio - 1.64 * stderr),
        "ci90_high": risk_ratio + 1.64 * stderr,
        "evidence_n": evidence,
    }


def _aggregate_domain(measures: list[dict[str, float]]) -> dict[str, Any]:
    if not measures:
        return _empty_domain_payload()
    risk_ratio = _geometric_mean([measure.get("risk_ratio") for measure in measures])
    ci75_low = _geometric_mean([measure.get("ci75_low") for measure in measures])
    ci75_high = _geometric_mean([measure.get("ci75_high") for measure in measures])
    ci90_low = _geometric_mean([measure.get("ci90_low") for measure in measures])
    ci90_high = _geometric_mean([measure.get("ci90_high") for measure in measures])
    evidence_n = float(sum(_as_float(measure.get("evidence_n")) or 0.0 for measure in measures))
    return {
        "risk_ratio_point": risk_ratio,
        "score_0_100": _score_from_risk_ratio(risk_ratio),
        "evidence_n": evidence_n,
        "ci_75": {"low": ci75_low, "high": ci75_high},
        "ci_90": {"low": ci90_low, "high": ci90_high},
    }


async def _load_provider_quality_profile(
    session,
    *,
    npi: int,
    year: int | None,
) -> dict[str, Any] | None:
    provider_year_filter = "AND p.year = :year" if year is not None else ""
    claims_state_expr = _state_code_sql("p.state")
    doctor_state_expr = _state_code_sql("d.state")
    unified_state_expr = _state_code_sql("e.state_name")
    npi_state_expr = _state_code_sql("a.state_name")
    taxonomy_table_exists = await _table_exists(session, NPIDataTaxonomy.__tablename__)
    nucc_table_exists = await _table_exists(session, NUCCTaxonomy.__tablename__)
    provider_enrichment_exists = await _table_exists(session, ProviderEnrichmentSummary.__tablename__)
    doctor_clinician_exists = await _table_exists(session, DoctorClinicianAddress.__tablename__)
    unified_address_exists = await _table_exists(session, EntityAddressUnified.__tablename__)
    npi_address_exists = await _table_exists(session, NPIAddress.__tablename__)
    unified_address_columns = (
        await _table_columns(session, EntityAddressUnified.__tablename__)
        if unified_address_exists
        else set()
    )
    nucc_columns = (
        await _table_columns(session, NUCCTaxonomy.__tablename__)
        if nucc_table_exists
        else set()
    )
    unified_confirmed_order_sql = (
        "CASE WHEN COALESCE(e.multi_source_confirmed, FALSE) THEN 0 ELSE 1 END,"
        if "multi_source_confirmed" in unified_address_columns
        else ""
    )
    unified_source_count_order_sql = (
        "COALESCE(e.source_count, 0) DESC,"
        if "source_count" in unified_address_columns
        else ""
    )
    unified_checksum_order_sql = "e.checksum" if "checksum" in unified_address_columns else "COALESCE(e.entity_id, 0)"
    provider_enrichment_cte = (
        f"""
            provider_enrichment_choice AS (
                SELECT
                    pe.npi::bigint AS npi,
                    COALESCE(pe.has_any_enrollment, FALSE)::boolean AS has_any_enrollment,
                    COALESCE(pe.has_hospital_enrollment, FALSE)::boolean AS has_hospital_enrollment,
                    COALESCE(pe.has_hha_enrollment, FALSE)::boolean AS has_hha_enrollment,
                    COALESCE(pe.has_hospice_enrollment, FALSE)::boolean AS has_hospice_enrollment,
                    COALESCE(pe.has_fqhc_enrollment, FALSE)::boolean AS has_fqhc_enrollment,
                    COALESCE(pe.has_rhc_enrollment, FALSE)::boolean AS has_rhc_enrollment,
                    COALESCE(pe.has_snf_enrollment, FALSE)::boolean AS has_snf_enrollment,
                    COALESCE(pe.has_medicare_claims, FALSE)::boolean AS has_medicare_claims
                FROM {PRICING_SCHEMA}.{ProviderEnrichmentSummary.__tablename__} pe
                WHERE pe.npi = :npi
                LIMIT 1
            ),
        """
        if provider_enrichment_exists
        else """
            provider_enrichment_choice AS (
                SELECT
                    NULL::bigint AS npi,
                    FALSE::boolean AS has_any_enrollment,
                    FALSE::boolean AS has_hospital_enrollment,
                    FALSE::boolean AS has_hha_enrollment,
                    FALSE::boolean AS has_hospice_enrollment,
                    FALSE::boolean AS has_fqhc_enrollment,
                    FALSE::boolean AS has_rhc_enrollment,
                    FALSE::boolean AS has_snf_enrollment,
                    FALSE::boolean AS has_medicare_claims
                WHERE FALSE
            ),
        """
    )
    taxonomy_cte = (
        f"""
            taxonomy_choice AS (
                SELECT
                    UPPER(NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), ''))::varchar AS taxonomy_code
                FROM {PRICING_SCHEMA}.{NPIDataTaxonomy.__tablename__} t
                WHERE t.npi = :npi
                  AND NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '') IS NOT NULL
                ORDER BY
                    CASE
                        WHEN UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y' THEN 0
                        ELSE 1
                    END,
                    t.checksum
                LIMIT 1
            ),
        """
        if taxonomy_table_exists
        else """
            taxonomy_choice AS (
                SELECT NULL::varchar AS taxonomy_code
                WHERE FALSE
            ),
        """
    )
    doctor_address_cte = (
        f"""
            doctor_address_choice AS (
                SELECT
                    ({doctor_state_expr})::varchar AS state_key,
                    NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5
                FROM {PRICING_SCHEMA}.{DoctorClinicianAddress.__tablename__} d
                WHERE d.npi = :npi
                  AND (
                        NULLIF(BTRIM(COALESCE(d.state, '')), '') IS NOT NULL
                     OR NULLIF(BTRIM(COALESCE(d.zip_code, '')), '') IS NOT NULL
                  )
                ORDER BY
                    CASE
                        WHEN NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), '') IS NOT NULL
                         AND NULLIF(BTRIM(COALESCE(d.state, '')), '') IS NOT NULL
                        THEN 0
                        ELSE 1
                    END,
                    d.address_checksum
                LIMIT 1
            ),
        """
        if doctor_clinician_exists
        else """
            doctor_address_choice AS (
                SELECT NULL::varchar AS state_key, NULL::varchar AS zip5
                WHERE FALSE
            ),
        """
    )
    unified_address_cte = (
        f"""
            unified_address_choice AS (
                SELECT
                    ({unified_state_expr})::varchar AS state_key,
                    NULLIF(LEFT(REGEXP_REPLACE(COALESCE(e.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5
                FROM {PRICING_SCHEMA}.{EntityAddressUnified.__tablename__} e
                WHERE COALESCE(e.npi, e.inferred_npi) = :npi
                  AND e.type IN ('practice', 'primary', 'secondary', 'site')
                  AND (
                        NULLIF(BTRIM(COALESCE(e.state_name, '')), '') IS NOT NULL
                     OR NULLIF(BTRIM(COALESCE(e.postal_code, '')), '') IS NOT NULL
                  )
                ORDER BY
                    {unified_confirmed_order_sql}
                    {unified_source_count_order_sql}
                    CASE e.type
                        WHEN 'practice' THEN 0
                        WHEN 'primary' THEN 1
                        WHEN 'secondary' THEN 2
                        ELSE 3
                    END,
                    {unified_checksum_order_sql}
                LIMIT 1
            ),
        """
        if unified_address_exists
        else """
            unified_address_choice AS (
                SELECT NULL::varchar AS state_key, NULL::varchar AS zip5
                WHERE FALSE
            ),
        """
    )
    npi_address_cte = (
        f"""
            npi_address_choice AS (
                SELECT
                    ({npi_state_expr})::varchar AS state_key,
                    NULLIF(LEFT(REGEXP_REPLACE(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5
                FROM {PRICING_SCHEMA}.{NPIAddress.__tablename__} a
                WHERE a.npi = :npi
                  AND a.type IN ('practice', 'primary', 'secondary')
                  AND (
                        NULLIF(BTRIM(COALESCE(a.state_name, '')), '') IS NOT NULL
                     OR NULLIF(BTRIM(COALESCE(a.postal_code, '')), '') IS NOT NULL
                  )
                ORDER BY
                    CASE a.type
                        WHEN 'practice' THEN 0
                        WHEN 'primary' THEN 1
                        WHEN 'secondary' THEN 2
                        ELSE 3
                    END,
                    a.checksum
                LIMIT 1
            ),
        """
        if npi_address_exists
        else """
            npi_address_choice AS (
                SELECT NULL::varchar AS state_key, NULL::varchar AS zip5
                WHERE FALSE
            ),
        """
    )
    nucc_cte = (
        f"""
            taxonomy_classification_choice AS (
                SELECT
                    LOWER(NULLIF(BTRIM(COALESCE(nt.classification, '')), ''))::varchar AS taxonomy_classification
                FROM {PRICING_SCHEMA}.{NUCCTaxonomy.__tablename__} nt
                JOIN taxonomy_choice tc
                  ON UPPER(BTRIM(COALESCE(nt.code, ''))) = tc.taxonomy_code
                LIMIT 1
            ),
        """
        if nucc_table_exists and "classification" in nucc_columns
        else """
            taxonomy_classification_choice AS (
                SELECT NULL::varchar AS taxonomy_classification
                WHERE FALSE
            ),
        """
    )
    result = await session.execute(
        text(
            f"""
            WITH provider_choice AS (
                SELECT
                    LOWER(NULLIF(BTRIM(COALESCE(p.provider_type, '')), ''))::varchar AS specialty_key,
                    ({claims_state_expr})::varchar AS claims_state,
                    NULLIF(BTRIM(COALESCE(p.zip5, '')), '')::varchar AS claims_zip5
                FROM {PRICING_SCHEMA}.{PricingProvider.__tablename__} p
                WHERE p.npi = :npi
                  {provider_year_filter}
                ORDER BY p.year DESC
                LIMIT 1
            ),
            {taxonomy_cte}
            {provider_enrichment_cte}
            {nucc_cte}
            {doctor_address_cte}
            {unified_address_cte}
            {npi_address_cte}
            SELECT
                nd.npi,
                pc.specialty_key,
                tc.taxonomy_code,
                tcc.taxonomy_classification,
                COALESCE(da.zip5, ua.zip5, na.zip5, pc.claims_zip5)::varchar AS zip5,
                COALESCE(da.state_key, ua.state_key, na.state_key, pc.claims_state)::varchar AS state_key,
                CASE
                    WHEN COALESCE(nd.entity_type_code, 0) = 1 THEN 'clinician'
                    WHEN COALESCE(pe.has_hospital_enrollment, FALSE)
                      OR COALESCE(pe.has_hha_enrollment, FALSE)
                      OR COALESCE(pe.has_hospice_enrollment, FALSE)
                      OR COALESCE(pe.has_fqhc_enrollment, FALSE)
                      OR COALESCE(pe.has_rhc_enrollment, FALSE)
                      OR COALESCE(pe.has_snf_enrollment, FALSE)
                    THEN 'facility'
                    WHEN COALESCE(nd.entity_type_code, 0) = 2 THEN 'organization'
                    ELSE 'unknown'
                END::varchar AS provider_class,
                CASE
                    WHEN da.zip5 IS NOT NULL OR da.state_key IS NOT NULL THEN 'doctor_clinician_address'
                    WHEN ua.zip5 IS NOT NULL OR ua.state_key IS NOT NULL THEN 'entity_address_unified'
                    WHEN na.zip5 IS NOT NULL OR na.state_key IS NOT NULL THEN 'npi_address'
                    WHEN pc.claims_zip5 IS NOT NULL OR pc.claims_state IS NOT NULL THEN 'claims_pricing'
                    ELSE 'unknown'
                END::varchar AS location_source,
                COALESCE(pe.has_any_enrollment, FALSE)::boolean AS has_enrollment,
                COALESCE(pe.has_medicare_claims, FALSE)::boolean AS has_medicare_claims
            FROM {PRICING_SCHEMA}.{NPIData.__tablename__} nd
            LEFT JOIN provider_choice pc ON TRUE
            LEFT JOIN taxonomy_choice tc ON TRUE
            LEFT JOIN taxonomy_classification_choice tcc ON TRUE
            LEFT JOIN provider_enrichment_choice pe ON TRUE
            LEFT JOIN doctor_address_choice da ON TRUE
            LEFT JOIN unified_address_choice ua ON TRUE
            LEFT JOIN npi_address_choice na ON TRUE
            WHERE nd.npi = :npi
            LIMIT 1
            """
        ),
        {"npi": npi, "year": year},
    )
    row = result.first()
    if row is None:
        return None
    row_data = _row_to_dict(row)
    specialty_key = _parse_specialty_key(row_data.get("specialty_key"))
    taxonomy_code = str(row_data.get("taxonomy_code") or "").strip().upper() or None
    return {
        "npi": npi,
        "specialty_key": specialty_key,
        "taxonomy_code": taxonomy_code,
        "taxonomy_classification": str(row_data.get("taxonomy_classification") or "").strip().lower() or None,
        "zip5": str(row_data.get("zip5") or "").strip()[:5] or None,
        "state_key": str(row_data.get("state_key") or "").strip().upper() or None,
        "provider_class": _normalize_provider_class(row_data.get("provider_class")) or "unknown",
        "location_source": str(row_data.get("location_source") or "").strip() or "unknown",
        "has_enrollment": bool(_as_bool(row_data.get("has_enrollment"))),
        "has_medicare_claims": bool(_as_bool(row_data.get("has_medicare_claims"))),
    }


def _provider_quality_unavailable_reasons(
    profile: dict[str, Any] | None,
    *,
    benchmark_mode: str | None = None,
) -> list[str]:
    if profile is None:
        return ["provider_profile_not_found"]

    reasons: list[str] = []
    if not profile.get("specialty_key") and not profile.get("taxonomy_code"):
        reasons.append("missing_specialty_or_taxonomy")
    if not profile.get("state_key") and not profile.get("zip5"):
        reasons.append("missing_geography")
    if benchmark_mode == "zip" and not profile.get("zip5"):
        reasons.append("missing_zip5_for_zip_benchmark")
    if benchmark_mode == "state" and not profile.get("state_key"):
        reasons.append("missing_state_for_state_benchmark")
    return reasons


def _estimated_benchmark_modes_for_profile(
    profile: dict[str, Any],
    *,
    benchmark_mode: str | None = None,
) -> list[str]:
    if benchmark_mode is not None:
        if benchmark_mode == "zip":
            return ["zip"] if profile.get("zip5") else []
        if benchmark_mode == "state":
            return ["state"] if profile.get("state_key") else []
        return ["national"]

    modes: list[str] = []
    if profile.get("zip5"):
        modes.append("zip")
    if profile.get("state_key"):
        modes.append("state")
    modes.append("national")
    return modes


async def _load_estimated_quality_modes(
    session,
    *,
    profile: dict[str, Any],
    year: int,
    benchmark_mode: str | None = None,
    taxonomy_code_override: str | None = None,
    specialty_key_override: str | None = None,
) -> dict[str, dict[str, Any] | None]:
    scores_by_benchmark_mode: dict[str, dict[str, Any] | None] = {
        mode: None
        for mode in QUALITY_BENCHMARK_MODE_ORDER
    }

    candidate_modes = _estimated_benchmark_modes_for_profile(profile, benchmark_mode=benchmark_mode)
    if not candidate_modes:
        return scores_by_benchmark_mode

    taxonomy_code = str(taxonomy_code_override or profile.get("taxonomy_code") or "").strip().upper() or None
    specialty_key = _parse_specialty_key(specialty_key_override or profile.get("specialty_key"))
    provider_class = _normalize_provider_class(profile.get("provider_class"))

    for mode in candidate_modes:
        score_filters = [
            "s.year = :year",
            "s.benchmark_mode = :benchmark_mode",
            "COALESCE(LOWER(BTRIM(COALESCE(s.score_method, 'direct'))), 'direct') <> 'unavailable'",
        ]
        feature_filters: list[str] = []
        params: dict[str, Any] = {"year": year, "benchmark_mode": mode}
        if provider_class not in {None, "unknown"}:
            feature_filters.append("COALESCE(LOWER(BTRIM(COALESCE(f.provider_class, ''))), 'unknown') = :provider_class")
            params["provider_class"] = provider_class
        if taxonomy_code:
            feature_filters.append("UPPER(BTRIM(COALESCE(f.taxonomy_code, ''))) = :taxonomy_code")
            params["taxonomy_code"] = taxonomy_code
        elif specialty_key:
            feature_filters.append("LOWER(BTRIM(COALESCE(f.specialty_key, ''))) = :specialty_key")
            params["specialty_key"] = specialty_key
        else:
            continue
        if mode == "zip":
            feature_filters.append("f.zip5 = :zip5")
            params["zip5"] = profile.get("zip5")
        elif mode == "state":
            feature_filters.append("UPPER(BTRIM(COALESCE(f.state, ''))) = :state_key")
            params["state_key"] = profile.get("state_key")

        where_sql = " AND ".join(score_filters + feature_filters)
        score_result = await session.execute(
            text(
                f"""
                SELECT
                    COUNT(*)::int AS peer_count,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY s.risk_ratio_point) AS risk_ratio_point,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY s.ci75_low) AS ci75_low,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY s.ci75_high) AS ci75_high,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY s.ci90_low) AS ci90_low,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY s.ci90_high) AS ci90_high,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY s.score_0_100) AS score_0_100
                FROM {PRICING_SCHEMA}.{QUALITY_SCORE_TABLE_NAME} s
                JOIN {PRICING_SCHEMA}.{QUALITY_FEATURE_TABLE_NAME} f
                  ON f.npi = s.npi
                 AND f.year = s.year
                WHERE {where_sql}
                """
            ),
            params,
        )
        score_row = score_result.first()
        if score_row is None:
            continue
        score_data_raw = _row_to_dict(score_row)
        peer_count = _as_int(score_data_raw.get("peer_count")) or 0
        if peer_count <= 0:
            continue

        domains_payload = _empty_domains_payload()
        domain_result = await session.execute(
            text(
                f"""
                SELECT
                    d.domain,
                    COUNT(*)::int AS peer_count,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY d.risk_ratio) AS risk_ratio,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY d.score_0_100) AS score_0_100,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY d.ci75_low) AS ci75_low,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY d.ci75_high) AS ci75_high,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY d.ci90_low) AS ci90_low,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY d.ci90_high) AS ci90_high
                FROM {PRICING_SCHEMA}.{QUALITY_DOMAIN_TABLE_NAME} d
                JOIN {PRICING_SCHEMA}.{QUALITY_FEATURE_TABLE_NAME} f
                  ON f.npi = d.npi
                 AND f.year = d.year
                WHERE d.year = :year
                  AND d.benchmark_mode = :benchmark_mode
                  AND {' AND '.join(feature_filters)}
                GROUP BY d.domain
                """
            ),
            params,
        )
        for domain_row in domain_result:
            domain_data = _row_to_dict(domain_row)
            domain_name = str(domain_data.get("domain") or "").strip().lower()
            if domain_name not in domains_payload:
                continue
            domains_payload[domain_name] = {
                "risk_ratio_point": _as_float(domain_data.get("risk_ratio")),
                "score_0_100": _as_float(domain_data.get("score_0_100")),
                "evidence_n": float(_as_int(domain_data.get("peer_count")) or peer_count),
                "ci_75": _ci_payload(domain_data.get("ci75_low"), domain_data.get("ci75_high")),
                "ci_90": _ci_payload(domain_data.get("ci90_low"), domain_data.get("ci90_high")),
            }

        rr_cost = _as_float(domains_payload["cost"].get("risk_ratio_point"))
        confidence_0_100 = _estimated_confidence_score(profile, peer_count)
        score_row_payload = {
            "model_version": PROVIDER_QUALITY_MODEL_VERSION,
            "benchmark_mode": mode,
            "tier": _tier_from_quality_summary(
                risk_ratio_point=_as_float(score_data_raw.get("risk_ratio_point")),
                ci75_high=_as_float(score_data_raw.get("ci75_high")),
                ci90_low=_as_float(score_data_raw.get("ci90_low")),
                confidence_0_100=confidence_0_100,
            ),
            "borderline_status": False,
            "score_0_100": _as_float(score_data_raw.get("score_0_100"))
            or _score_from_risk_ratio(_as_float(score_data_raw.get("risk_ratio_point"))),
            "estimated_cost_level": _estimated_cost_level_from_risk_ratio(rr_cost),
            "score_method": "estimated",
            "confidence_0_100": confidence_0_100,
            "confidence_band": "low",
            "cost_source": "peer_estimated",
            "data_coverage_0_100": _estimated_data_coverage_score(profile),
            "provider_class": profile.get("provider_class"),
            "location_source": profile.get("location_source"),
            "has_claims": False,
            "has_qpp": False,
            "has_rx": False,
            "has_enrollment": profile.get("has_enrollment"),
            "has_medicare_claims": profile.get("has_medicare_claims"),
            "risk_ratio_point": _as_float(score_data_raw.get("risk_ratio_point")),
            "ci75_low": _as_float(score_data_raw.get("ci75_low")),
            "ci75_high": _as_float(score_data_raw.get("ci75_high")),
            "ci90_low": _as_float(score_data_raw.get("ci90_low")),
            "ci90_high": _as_float(score_data_raw.get("ci90_high")),
            "low_score_threshold_failed": (_as_float(score_data_raw.get("risk_ratio_point")) or 0.0) >= 1.12,
            "low_confidence_threshold_failed": (_as_float(score_data_raw.get("ci90_low")) or 0.0) >= 1.08,
            "high_score_threshold_passed": (_as_float(score_data_raw.get("risk_ratio_point")) or 1.0) <= 0.88,
            "high_confidence_threshold_passed": (
                _as_float(score_data_raw.get("ci75_high")) is not None
                and (_as_float(score_data_raw.get("ci75_high")) or 0.0) < 1.0
            ),
        }
        cohort_context = {
            "selected_geography": _selected_geography_label(
                "national" if mode == "national" else mode,
                "US" if mode == "national" else (profile.get("zip5") if mode == "zip" else profile.get("state_key")),
            ),
            "selected_cohort_level": None,
            "peer_count": peer_count,
            "specialty_key": specialty_key,
            "taxonomy_code": taxonomy_code,
            "procedure_bucket": None,
            "computed_live": False,
            "procedure_match_threshold": None,
        }
        scores_by_benchmark_mode[mode] = _build_quality_mode_payload(
            score_row_payload,
            domains_payload,
            cohort_context=cohort_context,
        )

    return scores_by_benchmark_mode


async def _resolve_procedure_override_codes(
    session,
    procedure_codes: list[str],
    procedure_code_system: str,
) -> set[str]:
    if not procedure_codes:
        return set()

    normalized_codes = [str(code or "").strip().upper() for code in procedure_codes if str(code or "").strip()]
    if not normalized_codes:
        return set()

    if procedure_code_system == INTERNAL_CODE_SYSTEM:
        internal_codes = {str(int(code)) for code in normalized_codes if INT_PATTERN.fullmatch(code)}
        if internal_codes:
            return internal_codes

    resolved_codes: set[str] = set()
    for code in normalized_codes:
        try:
            code_context = await _resolve_code_context(session, procedure_code_system, code, expand_codes=False)
        except Exception:  # noqa: BLE001
            continue
        for internal_code in code_context.get("internal_codes", []):
            resolved_codes.add(str(internal_code))
    if resolved_codes:
        return resolved_codes
    return set(normalized_codes)


async def _load_provider_quality_observed(session, *, npi: int, year: int) -> dict[str, Any] | None:
    provider_result = await session.execute(
        select(provider_table).where(and_(provider_table.c.npi == npi, provider_table.c.year == year))
    )
    provider_row = provider_result.first()
    if provider_row is None:
        return None
    provider_payload = _row_to_dict(provider_row)

    total_services = _as_float(provider_payload.get("total_services")) or 0.0
    total_beneficiaries = _as_float(provider_payload.get("total_beneficiaries")) or 0.0
    total_allowed_amount = _as_float(provider_payload.get("total_allowed_amount")) or 0.0
    state_key = str(provider_payload.get("state") or "").strip().upper() or None
    zip5 = str(provider_payload.get("zip5") or "").strip() or None

    qpp_quality_score = None
    qpp_cost_score = None
    try:
        qpp_result = await session.execute(
            text(
                f"""
                SELECT quality_score, cost_score
                FROM {PRICING_SCHEMA}.{QUALITY_QPP_TABLE_NAME}
                WHERE npi = :npi AND year = :year
                LIMIT 1
                """
            ),
            {"npi": npi, "year": year},
        )
        qpp_row = qpp_result.first()
        if qpp_row is not None:
            qpp_payload = _row_to_dict(qpp_row)
            qpp_quality_score = _as_float(qpp_payload.get("quality_score"))
            qpp_cost_score = _as_float(qpp_payload.get("cost_score"))
    except Exception:  # noqa: BLE001
        qpp_quality_score = None
        qpp_cost_score = None

    total_rx_claims = 0.0
    total_rx_beneficiaries = 0.0
    try:
        rx_result = await session.execute(
            select(
                func.coalesce(func.sum(provider_prescription_table.c.total_claims), 0.0).label("total_rx_claims"),
                func.coalesce(func.sum(provider_prescription_table.c.total_benes), 0.0).label("total_rx_beneficiaries"),
            ).where(
                and_(
                    provider_prescription_table.c.npi == npi,
                    provider_prescription_table.c.year == year,
                )
            )
        )
        rx_row = rx_result.first()
        if rx_row is not None:
            rx_payload = _row_to_dict(rx_row)
            total_rx_claims = _as_float(rx_payload.get("total_rx_claims")) or 0.0
            total_rx_beneficiaries = _as_float(rx_payload.get("total_rx_beneficiaries")) or 0.0
    except Exception:  # noqa: BLE001
        total_rx_claims = 0.0
        total_rx_beneficiaries = 0.0

    svi_overall = PROVIDER_QUALITY_DEFAULT_SVI
    if zip5:
        try:
            svi_result = await session.execute(
                text(
                    f"""
                    SELECT svi_overall
                    FROM {PRICING_SCHEMA}.{QUALITY_SVI_TABLE_NAME}
                    WHERE zcta = :zcta AND year = :year
                    LIMIT 1
                    """
                ),
                {"zcta": zip5, "year": year},
            )
            svi_value = _as_float(svi_result.scalar())
            if svi_value is not None:
                svi_overall = min(1.0, max(0.0, svi_value))
        except Exception:  # noqa: BLE001
            svi_overall = PROVIDER_QUALITY_DEFAULT_SVI

    utilization_rate = None
    if total_beneficiaries > 0:
        utilization_rate = total_services / total_beneficiaries
    svi_adjustment = 1.0 + 0.2 * (svi_overall - 0.5)
    if svi_adjustment == 0:
        svi_adjustment = 1.0
    utilization_adjusted = (utilization_rate / svi_adjustment) if utilization_rate is not None else None
    cost_adjusted = total_allowed_amount / svi_adjustment
    rx_claim_rate = (total_rx_claims / total_rx_beneficiaries) if total_rx_beneficiaries > 0 else None

    return {
        "npi": npi,
        "year": year,
        "state_key": state_key,
        "zip5": zip5,
        "total_services": total_services,
        "total_beneficiaries": total_beneficiaries,
        "total_allowed_amount": total_allowed_amount,
        "total_rx_claims": total_rx_claims,
        "total_rx_beneficiaries": total_rx_beneficiaries,
        "utilization_adjusted": utilization_adjusted,
        "cost_adjusted": cost_adjusted,
        "qpp_quality_score": qpp_quality_score,
        "qpp_cost_score": qpp_cost_score,
        "rx_claim_rate": rx_claim_rate,
    }


async def _load_quality_peer_targets(
    session,
    *,
    year: int,
    benchmark_modes: list[str] | tuple[str, ...] | None = None,
    state_key: str | None = None,
    zip5: str | None = None,
) -> list[dict[str, Any]]:
    normalized_modes = [
        mode
        for mode in (benchmark_modes or QUALITY_BENCHMARK_MODE_ORDER)
        if mode in QUALITY_BENCHMARK_MODE_ORDER
    ]
    if not normalized_modes:
        normalized_modes = list(QUALITY_BENCHMARK_MODE_ORDER)

    state_value = str(state_key or "").strip().upper() or None
    zip_value = str(zip5 or "").strip()[:5] or None
    state_enabled = state_value is not None and any(mode in {"zip", "state"} for mode in normalized_modes)
    zip_enabled = zip_value is not None and any(mode == "zip" for mode in normalized_modes)

    mode_placeholders = ", ".join(f":mode_{idx}" for idx, _ in enumerate(normalized_modes))
    params: dict[str, Any] = {"year": year}
    for idx, mode in enumerate(normalized_modes):
        params[f"mode_{idx}"] = mode

    params.update(
        {
            "state_key": state_value,
            "zip5": zip_value,
            "state_enabled": state_enabled,
            "zip_enabled": zip_enabled,
        }
    )

    select_sql = f"""
        SELECT
            year,
            benchmark_mode,
            geography_scope,
            geography_value,
            cohort_level,
            specialty_key,
            taxonomy_code,
            procedure_bucket,
            peer_n,
            target_appropriateness,
            target_cost,
            target_effectiveness,
            target_qpp_cost,
            target_rx_appropriateness
        FROM {PRICING_SCHEMA}.{QUALITY_PEER_TARGET_TABLE_NAME}
        WHERE year = :year
          AND benchmark_mode IN ({mode_placeholders})
    """

    # Fast path: run index-friendly scope-specific probes.
    query_parts = [select_sql + "\n          AND geography_scope = 'national'"]
    if state_enabled:
        query_parts.append(
            select_sql + "\n          AND geography_scope = 'state'\n          AND geography_value = :state_key"
        )
    if zip_enabled:
        query_parts.append(
            select_sql + "\n          AND geography_scope = 'zip'\n          AND geography_value = :zip5"
        )
    fast_query = "\n        UNION ALL\n".join(query_parts)

    geography_clause = """
          AND (
                geography_scope IS NULL
                OR geography_scope = 'national'
                OR (
                    :state_enabled
                    AND geography_scope = 'state'
                    AND geography_value = :state_key
                )
                OR (
                    :zip_enabled
                    AND geography_scope = 'zip'
                    AND geography_value = :zip5
                )
          )
    """

    try:
        result = await session.execute(text(fast_query), params)
        rows = [_row_to_dict(row) for row in result]
        if rows:
            return rows
        # Safety fallback for unexpected geography-key formats.
        fallback_result = await session.execute(text(select_sql + geography_clause), params)
        fallback_rows = [_row_to_dict(row) for row in fallback_result]
        if fallback_rows:
            return fallback_rows
        fallback_result = await session.execute(text(select_sql), params)
        return [_row_to_dict(row) for row in fallback_result]
    except Exception:  # noqa: BLE001
        return []


def _build_live_mode_payload_for_candidate(
    *,
    benchmark_mode: str,
    observed_data: dict[str, Any],
    specialty_key: str | None,
    taxonomy_code: str | None,
    procedure_match_threshold: float,
    selected_candidate: dict[str, Any] | None,
) -> dict[str, Any]:
    target_values: dict[str, float | None] = {
        "target_appropriateness": None,
        "target_rx_appropriateness": None,
        "target_effectiveness": None,
        "target_qpp_cost": None,
        "target_cost": None,
    }
    selected_specialty_key = specialty_key
    selected_taxonomy_code = taxonomy_code
    selected_procedure_bucket = None
    selected_peer_count = None
    selected_geography = None
    selected_cohort_level = None
    if selected_candidate is not None:
        payload_lower = selected_candidate["payload_lower"]
        target_values = _extract_peer_target_values(payload_lower)
        selected_specialty_raw = _pick_first_from_lowered(payload_lower, "specialty_key", "specialty")
        selected_taxonomy_raw = _pick_first_from_lowered(payload_lower, "taxonomy_code", "taxonomy")
        selected_specialty_key = _parse_specialty_key(selected_specialty_raw) or specialty_key
        if selected_taxonomy_raw not in (None, ""):
            selected_taxonomy_code = str(selected_taxonomy_raw).strip().upper()
        selected_procedure_bucket = selected_candidate.get("procedure_bucket")
        selected_peer_count = _as_int(selected_candidate.get("peer_count"))
        selected_geography = selected_candidate.get("selected_geography")
        selected_cohort_level = selected_candidate.get("cohort_level")

    evidence_n_claims = max(
        _as_float(observed_data.get("total_services")) or 0.0,
        _as_float(observed_data.get("total_beneficiaries")) or 0.0,
        1.0,
    )
    evidence_n_rx = max(
        _as_float(observed_data.get("total_rx_claims")) or 0.0,
        _as_float(observed_data.get("total_rx_beneficiaries")) or 0.0,
        1.0,
    )

    measures_by_domain = {
        "appropriateness": [
            _compute_live_measure(
                observed=_as_float(observed_data.get("utilization_adjusted")),
                target=target_values.get("target_appropriateness"),
                evidence_n=evidence_n_claims,
                reverse_ratio=False,
            ),
            _compute_live_measure(
                observed=_as_float(observed_data.get("rx_claim_rate")),
                target=target_values.get("target_rx_appropriateness"),
                evidence_n=evidence_n_rx,
                reverse_ratio=False,
            ),
        ],
        "effectiveness": [
            _compute_live_measure(
                observed=_as_float(observed_data.get("qpp_quality_score")),
                target=target_values.get("target_effectiveness"),
                evidence_n=evidence_n_claims,
                reverse_ratio=True,
            ),
        ],
        "cost": [
            _compute_live_measure(
                observed=_as_float(observed_data.get("qpp_cost_score")),
                target=target_values.get("target_qpp_cost"),
                evidence_n=evidence_n_claims,
                reverse_ratio=True,
            ),
            _compute_live_measure(
                observed=_as_float(observed_data.get("cost_adjusted")),
                target=target_values.get("target_cost"),
                evidence_n=evidence_n_claims,
                reverse_ratio=False,
            ),
        ],
    }

    domains_payload = _empty_domains_payload()
    for domain, measures in measures_by_domain.items():
        domains_payload[domain] = _aggregate_domain(measures)

    rr_appropr = _as_float(domains_payload["appropriateness"]["risk_ratio_point"]) or 1.0
    rr_effect = _as_float(domains_payload["effectiveness"]["risk_ratio_point"]) or 1.0
    rr_cost = _as_float(domains_payload["cost"]["risk_ratio_point"]) or 1.0

    ci75_appropr_low = _as_float(domains_payload["appropriateness"]["ci_75"]["low"]) or 1.0
    ci75_effect_low = _as_float(domains_payload["effectiveness"]["ci_75"]["low"]) or 1.0
    ci75_cost_low = _as_float(domains_payload["cost"]["ci_75"]["low"]) or 1.0
    ci75_appropr_high = _as_float(domains_payload["appropriateness"]["ci_75"]["high"]) or 1.0
    ci75_effect_high = _as_float(domains_payload["effectiveness"]["ci_75"]["high"]) or 1.0
    ci75_cost_high = _as_float(domains_payload["cost"]["ci_75"]["high"]) or 1.0
    ci90_appropr_low = _as_float(domains_payload["appropriateness"]["ci_90"]["low"]) or 1.0
    ci90_effect_low = _as_float(domains_payload["effectiveness"]["ci_90"]["low"]) or 1.0
    ci90_cost_low = _as_float(domains_payload["cost"]["ci_90"]["low"]) or 1.0
    ci90_appropr_high = _as_float(domains_payload["appropriateness"]["ci_90"]["high"]) or 1.0
    ci90_effect_high = _as_float(domains_payload["effectiveness"]["ci_90"]["high"]) or 1.0
    ci90_cost_high = _as_float(domains_payload["cost"]["ci_90"]["high"]) or 1.0

    rr_clinical = math.sqrt(max(rr_appropr, 0.0001) * max(rr_effect, 0.0001))
    rr_overall = math.exp(
        0.5 * math.log(max(rr_clinical, 0.0001))
        + 0.5 * math.log(max(rr_cost, 0.0001))
    )
    ci75_low = math.exp(
        0.5 * math.log(max(math.sqrt(max(ci75_appropr_low, 0.0001) * max(ci75_effect_low, 0.0001)), 0.0001))
        + 0.5 * math.log(max(ci75_cost_low, 0.0001))
    )
    ci75_high = math.exp(
        0.5 * math.log(max(math.sqrt(max(ci75_appropr_high, 0.0001) * max(ci75_effect_high, 0.0001)), 0.0001))
        + 0.5 * math.log(max(ci75_cost_high, 0.0001))
    )
    ci90_low = math.exp(
        0.5 * math.log(max(math.sqrt(max(ci90_appropr_low, 0.0001) * max(ci90_effect_low, 0.0001)), 0.0001))
        + 0.5 * math.log(max(ci90_cost_low, 0.0001))
    )
    ci90_high = math.exp(
        0.5 * math.log(max(math.sqrt(max(ci90_appropr_high, 0.0001) * max(ci90_effect_high, 0.0001)), 0.0001))
        + 0.5 * math.log(max(ci90_cost_high, 0.0001))
    )

    low_score_check = rr_overall >= 1.12
    low_confidence_check = ci90_low >= 1.08
    high_score_check = rr_overall <= 0.88
    high_confidence_check = ci75_high < 1.0
    if low_score_check and low_confidence_check:
        tier = "low"
    elif high_score_check and high_confidence_check:
        tier = "high"
    else:
        tier = "acceptable"
    borderline_status = (int(low_score_check) + int(low_confidence_check)) == 1
    has_claims = (
        (_as_float(observed_data.get("total_services")) or 0.0) > 0
        or (_as_float(observed_data.get("total_beneficiaries")) or 0.0) > 0
    )
    has_qpp = (
        _as_float(observed_data.get("qpp_quality_score")) is not None
        or _as_float(observed_data.get("qpp_cost_score")) is not None
    )
    has_rx = (_as_float(observed_data.get("total_rx_claims")) or 0.0) > 0
    data_coverage_0_100 = min(
        100.0,
        (
            (40.0 if has_claims else 0.0)
            + (25.0 if has_qpp else 0.0)
            + (15.0 if has_rx else 0.0)
            + (10.0 if selected_peer_count and selected_peer_count >= 30 else 0.0)
            + (10.0 if observed_data.get("zip5") or observed_data.get("state_key") else 0.0)
        ),
    )
    confidence_0_100 = min(
        100.0,
        (
            (35.0 if has_claims else 0.0)
            + (20.0 if has_qpp else 0.0)
            + (10.0 if has_rx else 0.0)
            + (10.0 if selected_peer_count and selected_peer_count >= 100 else 5.0 if selected_peer_count and selected_peer_count >= 30 else 0.0)
            + (5.0 if observed_data.get("zip5") else 0.0)
            + (5.0 if observed_data.get("state_key") else 0.0)
        ),
    )
    if has_claims and has_qpp:
        score_method = "direct"
    elif has_claims or has_qpp or has_rx:
        score_method = "mixed"
    else:
        score_method = "unavailable"
    if confidence_0_100 >= 80.0:
        confidence_band = "high"
    elif confidence_0_100 >= 55.0:
        confidence_band = "medium"
    elif confidence_0_100 > 0.0:
        confidence_band = "low"
    else:
        confidence_band = "none"

    score_data = {
        "model_version": PROVIDER_QUALITY_MODEL_VERSION,
        "benchmark_mode": benchmark_mode,
        "tier": tier,
        "borderline_status": borderline_status,
        "score_0_100": _score_from_risk_ratio(rr_overall),
        "estimated_cost_level": _estimated_cost_level_from_risk_ratio(rr_cost),
        "risk_ratio_point": rr_overall,
        "ci75_low": ci75_low,
        "ci75_high": ci75_high,
        "ci90_low": ci90_low,
        "ci90_high": ci90_high,
        "low_score_threshold_failed": low_score_check,
        "low_confidence_threshold_failed": low_confidence_check,
        "high_score_threshold_passed": high_score_check,
        "high_confidence_threshold_passed": high_confidence_check,
        "score_method": score_method,
        "cost_source": "direct" if has_claims else "peer_estimated",
        "confidence_0_100": confidence_0_100,
        "confidence_band": confidence_band,
        "data_coverage_0_100": data_coverage_0_100,
        "provider_class": observed_data.get("provider_class"),
        "location_source": observed_data.get("location_source"),
        "has_claims": has_claims,
        "has_qpp": has_qpp,
        "has_rx": has_rx,
        "has_enrollment": observed_data.get("has_enrollment"),
        "has_medicare_claims": observed_data.get("has_medicare_claims"),
    }
    cohort_context = {
        "selected_geography": selected_geography,
        "selected_cohort_level": selected_cohort_level,
        "peer_count": selected_peer_count,
        "specialty_key": selected_specialty_key,
        "taxonomy_code": selected_taxonomy_code,
        "procedure_bucket": selected_procedure_bucket,
        "computed_live": True,
        "procedure_match_threshold": procedure_match_threshold,
    }
    return _build_quality_mode_payload(score_data, domains_payload, cohort_context=cohort_context)


def _build_live_mode_payload(
    *,
    benchmark_mode: str,
    observed_data: dict[str, Any],
    peer_target_rows: list[dict[str, Any]],
    specialty_key: str | None,
    taxonomy_code: str | None,
    requested_procedure_codes: set[str],
    procedure_match_threshold: float,
) -> dict[str, Any]:
    selected_candidate = _select_peer_target_candidate(
        peer_target_rows,
        benchmark_mode=benchmark_mode,
        state_key=observed_data.get("state_key"),
        zip5=observed_data.get("zip5"),
        specialty_key=specialty_key,
        taxonomy_code=taxonomy_code,
        requested_procedure_codes=requested_procedure_codes,
        procedure_match_threshold=procedure_match_threshold,
    )
    return _build_live_mode_payload_for_candidate(
        benchmark_mode=benchmark_mode,
        observed_data=observed_data,
        specialty_key=specialty_key,
        taxonomy_code=taxonomy_code,
        procedure_match_threshold=procedure_match_threshold,
        selected_candidate=selected_candidate,
    )


def _build_live_variant_payload(
    *,
    benchmark_mode: str,
    observed_data: dict[str, Any],
    specialty_key: str | None,
    taxonomy_code: str | None,
    procedure_match_threshold: float,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    mode_payload = _build_live_mode_payload_for_candidate(
        benchmark_mode=benchmark_mode,
        observed_data=observed_data,
        specialty_key=specialty_key,
        taxonomy_code=taxonomy_code,
        procedure_match_threshold=procedure_match_threshold,
        selected_candidate=candidate,
    )
    cohort_context = mode_payload.get("cohort_context") or {}
    return {
        "benchmark_mode": benchmark_mode,
        "tier": mode_payload.get("tier"),
        "score_0_100": mode_payload.get("score_0_100"),
        "overall": mode_payload.get("overall"),
        "domains": mode_payload.get("domains"),
        "curation_checks": mode_payload.get("curation_checks"),
        "cohort_context": cohort_context,
        "strict_match": bool(candidate.get("specialty_match") and candidate.get("taxonomy_match")),
        "procedure_match_ratio": _as_float(candidate.get("procedure_match_ratio")),
    }


def _live_mode_payload_is_available(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    context = payload.get("cohort_context")
    if not isinstance(context, dict):
        return False
    if context.get("selected_geography") not in (None, ""):
        return True
    if context.get("selected_cohort_level") not in (None, ""):
        return True
    if _as_int(context.get("peer_count")) is not None:
        return True
    return False


def _normalize_order(raw_order: Any):
    order = str(raw_order or "desc").strip().lower()
    if order not in {"asc", "desc"}:
        raise InvalidUsage("Parameter 'order' must be either 'asc' or 'desc'")
    return order


def _apply_ordering(query, order_by: str, order: str, field_map: dict[str, Any]):
    column = field_map.get(order_by)
    if column is None:
        allowed = ", ".join(sorted(field_map.keys()))
        raise InvalidUsage(f"Unsupported order_by '{order_by}'. Allowed: {allowed}")
    if order == "asc":
        return query.order_by(column.asc())
    return query.order_by(column.desc())


async def _query_crosswalk_edges(session, pairs: set[tuple[str, str]]) -> list[dict[str, Any]]:
    if not pairs:
        return []
    clauses = []
    for system, code in pairs:
        clauses.append(
            and_(
                func.upper(code_crosswalk_table.c.from_system) == system,
                func.upper(code_crosswalk_table.c.from_code) == code,
            )
        )
        clauses.append(
            and_(
                func.upper(code_crosswalk_table.c.to_system) == system,
                func.upper(code_crosswalk_table.c.to_code) == code,
            )
        )
    result = await session.execute(select(code_crosswalk_table).where(or_(*clauses)))
    return [_row_to_dict(row) for row in result]


async def _query_catalog_neighbors(session, pairs: set[tuple[str, str]]) -> set[tuple[str, str]]:
    if not pairs:
        return set()

    named_pairs = set()
    for system, code in pairs:
        lookup = await session.execute(
            select(code_catalog_table.c.display_name)
            .where(
                and_(
                    func.upper(code_catalog_table.c.code_system) == system,
                    func.upper(code_catalog_table.c.code) == code,
                )
            )
            .limit(1)
        )
        display_name = lookup.scalar()
        if not display_name:
            continue
        named_pairs.add((system, code))
        neighbors = await session.execute(
            select(code_catalog_table.c.code_system, code_catalog_table.c.code).where(
                func.lower(code_catalog_table.c.display_name) == str(display_name).strip().lower()
            )
        )
        for row in neighbors:
            pair = (str(row[0]).upper(), str(row[1]).upper())
            named_pairs.add(pair)
    return named_pairs


async def _resolve_code_context(
    session,
    code_system_raw: Any,
    code_raw: Any,
    expand_codes: bool = False,
) -> dict[str, Any]:
    code_system = _normalize_code_system(code_system_raw)
    code = _normalize_code(code_raw, "code")

    # Common fast path: internal numeric code with no expansion requested.
    if code_system == INTERNAL_CODE_SYSTEM and INT_PATTERN.fullmatch(code) and not expand_codes:
        internal_code = int(code)
        return {
            "input_code": {"code_system": code_system, "code": code},
            "resolved_codes": [{"code_system": code_system, "code": code}],
            "internal_codes": [internal_code],
            "matched_via": [],
            "expanded": False,
        }

    visited: set[tuple[str, str]] = {(code_system, code)}
    frontier: set[tuple[str, str]] = {(code_system, code)}
    matched_via: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str, str]] = set()

    hops = MAX_CODE_EXPANSION_HOPS if expand_codes else 1
    for _ in range(hops):
        edges = await _query_crosswalk_edges(session, frontier)
        if not edges:
            break
        next_frontier: set[tuple[str, str]] = set()
        for edge in edges:
            from_pair = (
                str(edge.get("from_system", "")).upper(),
                str(edge.get("from_code", "")).upper(),
            )
            to_pair = (
                str(edge.get("to_system", "")).upper(),
                str(edge.get("to_code", "")).upper(),
            )
            edge_key = (*from_pair, *to_pair)
            if edge_key not in seen_edges:
                seen_edges.add(edge_key)
                matched_via.append(
                    {
                        "from_system": from_pair[0],
                        "from_code": from_pair[1],
                        "to_system": to_pair[0],
                        "to_code": to_pair[1],
                        "match_type": edge.get("match_type"),
                        "confidence": edge.get("confidence"),
                        "source": edge.get("source"),
                    }
                )
            for pair in (from_pair, to_pair):
                if pair not in visited:
                    visited.add(pair)
                    next_frontier.add(pair)
        frontier = next_frontier
        if not frontier:
            break

    # Optional expansion by display_name peers from code catalog.
    if expand_codes:
        named_pairs = await _query_catalog_neighbors(session, visited)
        visited.update(named_pairs)

    internal_codes = sorted(
        {
            int(pair[1])
            for pair in visited
            if pair[0] == INTERNAL_CODE_SYSTEM and INT_PATTERN.fullmatch(pair[1])
        }
    )

    resolved_codes = [
        {"code_system": pair[0], "code": pair[1]}
        for pair in sorted(visited, key=lambda item: (item[0], item[1]))
    ]

    return {
        "input_code": {"code_system": code_system, "code": code},
        "resolved_codes": resolved_codes,
        "internal_codes": internal_codes,
        "matched_via": matched_via,
        "expanded": bool(expand_codes),
    }


async def _resolve_internal_codes_for_request(
    session,
    code_value: Any,
    args,
    default_system: str = INTERNAL_CODE_SYSTEM,
) -> tuple[list[int], dict[str, Any]]:
    code_system = _normalize_code_system(args.get("code_system") or default_system)
    expand_codes = _parse_bool(args.get("expand_codes"), "expand_codes", default=False)
    code_context = await _resolve_code_context(session, code_system, code_value, expand_codes=expand_codes)
    internal_codes = code_context["internal_codes"]
    if not internal_codes:
        raise sanic.exceptions.NotFound("No mapped internal procedure code found")
    return internal_codes, code_context


async def _resolve_internal_rx_codes_for_request(
    session,
    rx_code_value: Any,
    args,
    default_system: str = INTERNAL_RX_CODE_SYSTEM,
) -> tuple[list[str], dict[str, Any]]:
    code_system = _normalize_code_system(args.get("rx_code_system") or args.get("code_system") or default_system)
    code = _normalize_code(rx_code_value, "rx_code")
    expand_codes = _parse_bool(args.get("expand_codes"), "expand_codes", default=False)

    if code_system == INTERNAL_RX_CODE_SYSTEM:
        return [code], {
            "input_code": {"code_system": code_system, "code": code},
            "resolved_codes": [{"code_system": INTERNAL_RX_CODE_SYSTEM, "code": code}],
            "matched_via": [],
            "expanded": bool(expand_codes),
        }

    clauses = [
        and_(
            func.upper(code_crosswalk_table.c.from_system) == code_system,
            func.upper(code_crosswalk_table.c.from_code) == code,
            func.upper(code_crosswalk_table.c.to_system) == INTERNAL_RX_CODE_SYSTEM,
        )
    ]
    if expand_codes:
        clauses.append(
            and_(
                func.upper(code_crosswalk_table.c.to_system) == code_system,
                func.upper(code_crosswalk_table.c.to_code) == code,
                func.upper(code_crosswalk_table.c.from_system) == INTERNAL_RX_CODE_SYSTEM,
            )
        )

    result = await session.execute(select(code_crosswalk_table).where(or_(*clauses)))
    rows = [_row_to_dict(row) for row in result]
    resolved_codes: list[str] = []
    matched_via: list[dict[str, Any]] = []

    for row in rows:
        from_system = str(row.get("from_system") or "").upper()
        to_system = str(row.get("to_system") or "").upper()
        from_code = str(row.get("from_code") or "").upper()
        to_code = str(row.get("to_code") or "").upper()
        if to_system == INTERNAL_RX_CODE_SYSTEM:
            resolved_codes.append(to_code)
            matched_via.append(
                {
                    "from_system": from_system,
                    "from_code": from_code,
                    "to_system": to_system,
                    "to_code": to_code,
                    "match_type": row.get("match_type"),
                    "confidence": row.get("confidence"),
                    "source": row.get("source"),
                }
            )
        elif expand_codes and from_system == INTERNAL_RX_CODE_SYSTEM:
            resolved_codes.append(from_code)
            matched_via.append(
                {
                    "from_system": from_system,
                    "from_code": from_code,
                    "to_system": to_system,
                    "to_code": to_code,
                    "match_type": row.get("match_type"),
                    "confidence": row.get("confidence"),
                    "source": row.get("source"),
                }
            )

    unique_codes = sorted({code_item for code_item in resolved_codes if code_item})
    if not unique_codes:
        raise sanic.exceptions.NotFound("No mapped internal prescription code found")

    return unique_codes, {
        "input_code": {"code_system": code_system, "code": code},
        "resolved_codes": [{"code_system": INTERNAL_RX_CODE_SYSTEM, "code": item} for item in unique_codes],
        "matched_via": matched_via,
        "expanded": bool(expand_codes),
    }


async def _resolve_external_rx_codes_for_internal(
    session,
    internal_codes: list[str],
) -> dict[str, dict[str, list[str]]]:
    normalized_codes = sorted({str(code or "").strip().upper() for code in internal_codes if str(code or "").strip()})
    if not normalized_codes:
        return {}

    external_systems = tuple(RX_EXTERNAL_CODE_PRIORITY)
    query = select(code_crosswalk_table).where(
        or_(
            and_(
                func.upper(code_crosswalk_table.c.from_system) == INTERNAL_RX_CODE_SYSTEM,
                func.upper(code_crosswalk_table.c.from_code).in_(normalized_codes),
                func.upper(code_crosswalk_table.c.to_system).in_(external_systems),
            ),
            and_(
                func.upper(code_crosswalk_table.c.to_system) == INTERNAL_RX_CODE_SYSTEM,
                func.upper(code_crosswalk_table.c.to_code).in_(normalized_codes),
                func.upper(code_crosswalk_table.c.from_system).in_(external_systems),
            ),
        )
    )
    result = await session.execute(query)
    rows = [_row_to_dict(row) for row in result]

    mapping: dict[str, dict[str, list[str]]] = {}
    for row in rows:
        from_system = str(row.get("from_system") or "").strip().upper()
        from_code = str(row.get("from_code") or "").strip().upper()
        to_system = str(row.get("to_system") or "").strip().upper()
        to_code = str(row.get("to_code") or "").strip().upper()

        internal_code: str | None = None
        external_system: str | None = None
        external_code: str | None = None

        if from_system == INTERNAL_RX_CODE_SYSTEM and to_system in external_systems:
            internal_code = from_code
            external_system = to_system
            external_code = to_code
        elif to_system == INTERNAL_RX_CODE_SYSTEM and from_system in external_systems:
            internal_code = to_code
            external_system = from_system
            external_code = from_code

        if not internal_code or not external_system or not external_code:
            continue

        target = mapping.setdefault(internal_code, {system: [] for system in external_systems})
        if external_code not in target[external_system]:
            target[external_system].append(external_code)
    return mapping


def _select_preferred_external_rx_code(
    fallback_system: Any,
    fallback_code: Any,
    *,
    ndc_codes: list[str] | None = None,
    rxnorm_codes: list[str] | None = None,
) -> tuple[str | None, str | None]:
    for code in ndc_codes or []:
        value = str(code or "").strip().upper()
        if value:
            return "NDC", value
    for code in rxnorm_codes or []:
        value = str(code or "").strip().upper()
        if value:
            return "RXNORM", value

    fallback_system_text = str(fallback_system or "").strip().upper() or None
    fallback_code_text = str(fallback_code or "").strip().upper() or None
    return fallback_system_text, fallback_code_text


def _apply_prescription_code_preferences(
    items: list[dict[str, Any]],
    external_codes_by_internal: dict[str, dict[str, list[str]]] | None = None,
) -> None:
    external_codes_by_internal = external_codes_by_internal or {}
    for item in items:
        internal_code = str(item.get("rx_code") or item.get("prescription_code") or "").strip().upper()
        mapped = external_codes_by_internal.get(internal_code, {})
        ndc_codes = mapped.get("NDC") if isinstance(mapped, dict) else None
        rxnorm_codes = mapped.get("RXNORM") if isinstance(mapped, dict) else None

        if ndc_codes:
            item["ndc_code"] = ndc_codes[0]
        if rxnorm_codes:
            item["rxnorm_id"] = rxnorm_codes[0]

        preferred_system, preferred_code = _select_preferred_external_rx_code(
            item.get("prescription_code_system"),
            item.get("prescription_code"),
            ndc_codes=ndc_codes,
            rxnorm_codes=rxnorm_codes,
        )
        item["preferred_prescription_code_system"] = preferred_system
        item["preferred_prescription_code"] = preferred_code


@blueprint.get("/statistics", name="pricing.statistics")
async def pricing_statistics(request):
    session = _get_session(request)

    medicare_individuals_stmt = (
        select(func.count())
        .select_from(
            provider_enrichment_summary_table.join(
                npi_data_table, npi_data_table.c.npi == provider_enrichment_summary_table.c.npi
            )
        )
        .where(
            provider_enrichment_summary_table.c.has_medicare_claims.is_(True),
            npi_data_table.c.entity_type_code == 1,
        )
    )
    providers_with_procedures_stmt = select(func.count(func.distinct(provider_procedure_table.c.npi)))
    procedure_codes_stmt = select(func.count(func.distinct(provider_procedure_table.c.procedure_code)))
    procedure_zip_codes_stmt = select(func.count(func.distinct(location_table.c.zip5))).where(
        location_table.c.zip5.is_not(None),
        func.length(func.trim(location_table.c.zip5)) > 0,
    )

    medicare_individuals_result, providers_with_procedures_result, procedure_codes_result, procedure_zip_codes_result = await asyncio.gather(
        session.execute(medicare_individuals_stmt),
        session.execute(providers_with_procedures_stmt),
        session.execute(procedure_codes_stmt),
        session.execute(procedure_zip_codes_stmt),
    )

    medicare_individuals = medicare_individuals_result.scalar()
    providers_with_procedures = providers_with_procedures_result.scalar()
    procedure_codes = procedure_codes_result.scalar()
    procedure_zip_codes = procedure_zip_codes_result.scalar()

    return response.json({
        "medicare_individual_providers": int(medicare_individuals or 0),
        "providers_with_procedure_history": int(providers_with_procedures or 0),
        "procedure_codes_tracked": int(procedure_codes or 0),
        "procedure_zip_codes": int(procedure_zip_codes or 0),
    })


@blueprint.get("/providers", name="pricing.providers.list")
@blueprint.get("/physicians", name="pricing.physicians.list")
async def list_pricing_providers(request):
    session = _get_session(request)
    args = request.args

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    npi = _parse_int(args.get("npi"), "npi", minimum=1)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    min_claims = _parse_float(args.get("min_claims"), "min_claims", minimum=0)
    min_total_cost = _parse_float(args.get("min_total_cost"), "min_total_cost", minimum=0)
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)
    benchmark_mode = _parse_benchmark_mode(args.get("benchmark_mode"), "benchmark_mode")
    q = str(args.get("q", "")).strip()
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    specialty = str(args.get("specialty", "")).strip().lower()

    year, year_source = await _resolve_year(session, provider_table, year)

    filters = [provider_table.c.year == year]
    if npi is not None:
        filters.append(provider_table.c.npi == npi)
    if state:
        filters.append(func.upper(provider_table.c.state) == state)
    if city:
        filters.append(func.lower(provider_table.c.city).like(f"%{city}%"))
    if specialty:
        filters.append(func.lower(provider_table.c.provider_type).like(f"%{specialty}%"))
    if q:
        q_like = f"%{q.lower()}%"
        filters.append(
            or_(
                func.lower(provider_table.c.provider_name).like(q_like),
                func.lower(provider_table.c.provider_type).like(q_like),
                cast(provider_table.c.npi, String).like(f"%{q}%"),
            )
        )
    if min_claims is not None:
        filters.append(provider_table.c.total_services >= min_claims)
    if min_total_cost is not None:
        filters.append(provider_table.c.total_allowed_amount >= min_total_cost)

    where_clause = and_(*filters)
    count_result = await session.execute(select(func.count()).select_from(provider_table).where(where_clause))
    total = int(count_result.scalar() or 0)

    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_allowed_amount")
    benchmark_mode_used = benchmark_mode
    benchmark_mode_source = "request" if benchmark_mode else None
    if order_by == "tier_relevance" and await _table_exists(session, QUALITY_SCORE_TABLE_NAME):
        benchmark_mode_used, benchmark_mode_source = await _resolve_quality_benchmark_mode(
            session,
            year,
            benchmark_mode,
        )
        provider_with_scores = provider_table.outerjoin(
            quality_score_table,
            and_(
                quality_score_table.c.npi == provider_table.c.npi,
                quality_score_table.c.year == provider_table.c.year,
                quality_score_table.c.benchmark_mode == benchmark_mode_used,
            ),
        )
        tier_rank = case(
            (quality_score_table.c.tier == "high", 0),
            (quality_score_table.c.tier == "acceptable", 1),
            (quality_score_table.c.tier == "low", 2),
            else_=3,
        )
        query = (
            select(provider_table)
            .select_from(provider_with_scores)
            .where(where_clause)
            .order_by(
                tier_rank.asc(),
                quality_score_table.c.score_0_100.desc(),
                provider_table.c.total_allowed_amount.desc(),
            )
        )
    else:
        query = select(provider_table).where(where_clause)
        query = _apply_ordering(
            query,
            order_by,
            order,
            {
                "npi": provider_table.c.npi,
                "provider_name": provider_table.c.provider_name,
                "total_services": provider_table.c.total_services,
                "total_allowed_amount": provider_table.c.total_allowed_amount,
                "total_beneficiaries": provider_table.c.total_beneficiaries,
                "total_claims": provider_table.c.total_services,
                "total_30day_fills": provider_table.c.total_distinct_hcpcs_codes,
                "total_day_supply": provider_table.c.total_submitted_charges,
                "total_drug_cost": provider_table.c.total_allowed_amount,
                "total_benes": provider_table.c.total_beneficiaries,
            },
        )
    query = query.limit(pagination.limit).offset(pagination.offset)

    result = await session.execute(query)
    items = [_normalize_provider_payload(_row_to_dict(row), include_legacy=include_legacy_fields) for row in result]

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "benchmark_mode": benchmark_mode_used,
                "benchmark_mode_source": benchmark_mode_source,
                "npi": npi,
                "state": state or None,
                "city": city or None,
                "specialty": specialty or None,
                "q": q or None,
                "min_claims": min_claims,
                "min_total_cost": min_total_cost,
                "include_legacy_fields": include_legacy_fields,
                "order_by": order_by,
                "order": order,
            },
        }
    )


@blueprint.get("/providers/<npi>", name="pricing.providers.get")
@blueprint.get("/physicians/<npi>", name="pricing.physicians.get")
async def get_pricing_provider(request, npi: str):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")

    year, year_source = await _resolve_year(session, provider_table, year)
    provider_result = await session.execute(
        select(provider_table).where(
            and_(provider_table.c.npi == provider_npi, provider_table.c.year == year)
        )
    )
    provider_row = provider_result.first()
    if provider_row is None:
        raise sanic.exceptions.NotFound("Provider not found")

    service_count_result = await session.execute(
        select(func.count()).select_from(provider_procedure_table).where(
            and_(provider_procedure_table.c.npi == provider_npi, provider_procedure_table.c.year == year)
        )
    )
    location_count_result = await session.execute(
        select(func.count(func.distinct(location_table.c.location_key))).select_from(location_table).where(
            and_(location_table.c.npi == provider_npi, location_table.c.year == year)
        )
    )
    payload = _normalize_provider_payload(_row_to_dict(provider_row), include_legacy=include_legacy_fields)
    payload["year_used"] = year
    payload["year_source"] = year_source
    payload["summary"] = {
        "service_count": int(service_count_result.scalar() or 0),
        "location_count": int(location_count_result.scalar() or 0),
    }
    return response.json(payload)


@blueprint.get("/providers/<npi>/score", name="pricing.providers.score")
@blueprint.get("/physicians/<npi>/score", name="pricing.physicians.score")
async def get_pricing_provider_score(request, npi: str):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    benchmark_mode = _parse_benchmark_mode(args.get("benchmark_mode"), "benchmark_mode")
    specialty_key = _parse_specialty_key(args.get("specialty"))
    taxonomy_code_raw = str(args.get("taxonomy_code", "")).strip()
    taxonomy_code = taxonomy_code_raw.upper() if taxonomy_code_raw else None
    procedure_codes_raw = args.get("procedure_codes")
    procedure_codes = _parse_code_list_query_param(args, "procedure_codes")
    if not procedure_codes and procedure_codes_raw not in (None, ""):
        procedure_codes = _parse_code_list_query_param({"procedure_codes": procedure_codes_raw}, "procedure_codes")
    procedure_code_system = _parse_procedure_override_code_system(
        args.get("procedure_code_system"),
        "procedure_code_system",
    )
    procedure_match_threshold = _parse_probability_clamped(
        args.get("procedure_match_threshold"),
        "procedure_match_threshold",
        PROCEDURE_MATCH_THRESHOLD_DEFAULT,
    )
    _validate_removed_variants_param(args.get("variants"), "variants")
    variants_scope = _parse_score_variants_scope(args.get("variants_scope"), "variants_scope")
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")

    async def _fallback_response(
        *,
        year_used: int,
        year_source_value: str,
    ):
        profile = await _load_provider_quality_profile(session, npi=provider_npi, year=year_used)
        reasons = _provider_quality_unavailable_reasons(profile, benchmark_mode=benchmark_mode)
        scores_by_benchmark_mode: dict[str, dict[str, Any] | None] = {
            mode: None
            for mode in QUALITY_BENCHMARK_MODE_ORDER
        }
        if not reasons and profile is not None:
            try:
                scores_by_benchmark_mode = await _load_estimated_quality_modes(
                    session,
                    profile=profile,
                    year=year_used,
                    benchmark_mode=benchmark_mode,
                    taxonomy_code_override=taxonomy_code,
                    specialty_key_override=specialty_key,
                )
            except Exception:  # noqa: BLE001
                scores_by_benchmark_mode = {
                    mode: None
                    for mode in QUALITY_BENCHMARK_MODE_ORDER
                }
            available_modes = [
                mode
                for mode in QUALITY_BENCHMARK_MODE_ORDER
                if scores_by_benchmark_mode.get(mode) is not None
            ]
            if benchmark_mode is not None:
                selected_payload = scores_by_benchmark_mode.get(benchmark_mode)
                if selected_payload is not None:
                    return response.json(
                        _build_provider_quality_response_payload(
                            provider_npi=provider_npi,
                            year_used=year_used,
                            year_source=year_source_value,
                            selected_payload=selected_payload,
                            selected_mode=benchmark_mode,
                            scores_by_benchmark_mode=scores_by_benchmark_mode,
                            benchmark_mode=benchmark_mode,
                            restrict_available_to_selected=True,
                        )
                    )
                reasons.append(f"benchmark_mode_{benchmark_mode}_estimate_unavailable")
            elif available_modes:
                selected_mode = available_modes[0]
                selected_payload = scores_by_benchmark_mode[selected_mode]
                assert selected_payload is not None
                return response.json(
                    _build_provider_quality_response_payload(
                        provider_npi=provider_npi,
                        year_used=year_used,
                        year_source=year_source_value,
                        selected_payload=selected_payload,
                        selected_mode=selected_mode,
                        scores_by_benchmark_mode=scores_by_benchmark_mode,
                        benchmark_mode=None,
                    )
                )
            else:
                reasons.append("no_matching_peer_cohort")

        selected_mode = benchmark_mode or ("zip" if profile and profile.get("zip5") else "state" if profile and profile.get("state_key") else "national")
        selected_geography = None
        if profile is not None:
            if selected_mode == "zip" and profile.get("zip5"):
                selected_geography = _selected_geography_label("zip", profile.get("zip5"))
            elif selected_mode in {"zip", "state"} and profile.get("state_key"):
                selected_geography = _selected_geography_label("state", profile.get("state_key"))
            elif profile.get("state_key"):
                selected_geography = _selected_geography_label("state", profile.get("state_key"))
            elif profile.get("zip5"):
                selected_geography = _selected_geography_label("zip", profile.get("zip5"))
        selected_payload = _build_quality_mode_payload(
            {
                "model_version": PROVIDER_QUALITY_MODEL_VERSION,
                "benchmark_mode": selected_mode,
                "tier": None,
                "borderline_status": None,
                "score_0_100": None,
                "estimated_cost_level": None,
                "score_method": "unavailable",
                "confidence_0_100": 0.0,
                "confidence_band": "none",
                "cost_source": "unavailable",
                "data_coverage_0_100": _estimated_data_coverage_score(profile or {}) if profile else None,
                "provider_class": (profile or {}).get("provider_class"),
                "location_source": (profile or {}).get("location_source"),
                "has_claims": False,
                "has_qpp": False,
                "has_rx": False,
                "has_enrollment": (profile or {}).get("has_enrollment"),
                "has_medicare_claims": (profile or {}).get("has_medicare_claims"),
                "risk_ratio_point": None,
                "ci75_low": None,
                "ci75_high": None,
                "ci90_low": None,
                "ci90_high": None,
                "low_score_threshold_failed": False,
                "low_confidence_threshold_failed": False,
                "high_score_threshold_passed": False,
                "high_confidence_threshold_passed": False,
                "unavailable_reasons": reasons,
            },
            _empty_domains_payload(),
            cohort_context=(
                {
                    "selected_geography": selected_geography,
                    "selected_cohort_level": None,
                    "peer_count": None,
                    "specialty_key": specialty_key or (profile or {}).get("specialty_key"),
                    "taxonomy_code": taxonomy_code or (profile or {}).get("taxonomy_code"),
                    "procedure_bucket": None,
                    "computed_live": False,
                    "procedure_match_threshold": None,
                }
                if profile is not None
                else None
            ),
        )
        return response.json(
            _build_provider_quality_response_payload(
                provider_npi=provider_npi,
                year_used=year_used,
                year_source=year_source_value,
                selected_payload=selected_payload,
                selected_mode=selected_mode,
                scores_by_benchmark_mode=scores_by_benchmark_mode,
                benchmark_mode=benchmark_mode,
                restrict_available_to_selected=True,
            )
        )

    override_requested = bool(specialty_key or taxonomy_code or procedure_codes or variants_scope is not None)
    if override_requested:
        year, year_source = await _resolve_year(session, provider_table, year)
        observed_data = await _load_provider_quality_observed(session, npi=provider_npi, year=year)
        if observed_data is None:
            return await _fallback_response(year_used=year, year_source_value=year_source)

        requested_procedure_codes = await _resolve_procedure_override_codes(
            session,
            procedure_codes,
            procedure_code_system,
        )
        modes_to_compute = [benchmark_mode] if benchmark_mode is not None else list(QUALITY_BENCHMARK_MODE_ORDER)
        peer_target_rows = await _load_quality_peer_targets(
            session,
            year=year,
            benchmark_modes=modes_to_compute,
            state_key=observed_data.get("state_key"),
            zip5=observed_data.get("zip5"),
        )
        scores_by_benchmark_mode: dict[str, dict[str, Any] | None] = {
            mode: None
            for mode in QUALITY_BENCHMARK_MODE_ORDER
        }
        variants_by_benchmark_mode: dict[str, list[dict[str, Any]]] = {
            mode: []
            for mode in QUALITY_BENCHMARK_MODE_ORDER
        }
        for mode in modes_to_compute:
            mode_payload = _build_live_mode_payload(
                benchmark_mode=mode,
                observed_data=observed_data,
                peer_target_rows=peer_target_rows,
                specialty_key=specialty_key,
                taxonomy_code=taxonomy_code,
                requested_procedure_codes=requested_procedure_codes,
                procedure_match_threshold=procedure_match_threshold,
            )
            if _live_mode_payload_is_available(mode_payload):
                scores_by_benchmark_mode[mode] = mode_payload
            if variants_scope == SCORE_VARIANTS_SCOPE_PROVIDER:
                provider_specialty_key, provider_taxonomy_code, provider_procedure_codes = _variant_scope_inputs_from_mode_payload(
                    mode_payload,
                    fallback_specialty_key=specialty_key,
                    fallback_taxonomy_code=taxonomy_code,
                    fallback_procedure_codes=requested_procedure_codes,
                )
                mode_candidates = _collect_peer_target_candidates(
                    peer_target_rows,
                    benchmark_mode=mode,
                    state_key=observed_data.get("state_key"),
                    zip5=observed_data.get("zip5"),
                    specialty_key=provider_specialty_key,
                    taxonomy_code=provider_taxonomy_code,
                    requested_procedure_codes=provider_procedure_codes,
                )
                mode_candidates = _collect_provider_scope_variant_candidates(
                    mode_candidates,
                    provider_specialty_key=provider_specialty_key,
                    provider_taxonomy_code=provider_taxonomy_code,
                    provider_procedure_codes=provider_procedure_codes,
                )
                variants_by_benchmark_mode[mode] = [
                    _build_live_variant_payload(
                        benchmark_mode=mode,
                        observed_data=observed_data,
                        specialty_key=provider_specialty_key,
                        taxonomy_code=provider_taxonomy_code,
                        procedure_match_threshold=procedure_match_threshold,
                        candidate=candidate,
                    )
                    for candidate in mode_candidates
                ]

        available_modes = [mode for mode in QUALITY_BENCHMARK_MODE_ORDER if scores_by_benchmark_mode.get(mode)]
        if benchmark_mode is not None:
            selected_mode = benchmark_mode
            selected_payload = scores_by_benchmark_mode.get(selected_mode)
            if selected_payload is None:
                return await _fallback_response(year_used=year, year_source_value=year_source)
        else:
            if not available_modes:
                return await _fallback_response(year_used=year, year_source_value=year_source)
            selected_mode = available_modes[0]
            selected_payload = scores_by_benchmark_mode[selected_mode]
        assert selected_payload is not None

        return response.json(
            _build_provider_quality_response_payload(
                provider_npi=provider_npi,
                year_used=year,
                year_source=year_source,
                selected_payload=selected_payload,
                selected_mode=selected_mode,
                scores_by_benchmark_mode=scores_by_benchmark_mode,
                benchmark_mode=benchmark_mode,
                restrict_available_to_selected=benchmark_mode is not None,
                variants_scope=variants_scope,
                variants_by_benchmark_mode=variants_by_benchmark_mode,
            )
        )

    if not await _table_exists(session, QUALITY_SCORE_TABLE_NAME):
        raise sanic.exceptions.NotFound("Provider quality score table not found")
    if not await _table_exists(session, QUALITY_DOMAIN_TABLE_NAME):
        raise sanic.exceptions.NotFound("Provider quality domain table not found")

    year, year_source = await _resolve_quality_year(session, year)

    score_query = text(
        f"""
        SELECT *
        FROM {PRICING_SCHEMA}.{QUALITY_SCORE_TABLE_NAME}
        WHERE npi = :npi AND year = :year
        """
    )
    score_result = await session.execute(score_query, {"npi": provider_npi, "year": year})
    score_rows = [_row_to_dict(row) for row in score_result]
    if not score_rows:
        return await _fallback_response(year_used=year, year_source_value=year_source)

    scores_by_mode_raw: dict[str, dict[str, Any]] = {}
    for row in score_rows:
        mode = str(row.get("benchmark_mode") or "").strip().lower()
        if mode in QUALITY_BENCHMARK_MODE_ORDER and mode not in scores_by_mode_raw:
            scores_by_mode_raw[mode] = row

    if benchmark_mode is not None:
        selected_mode = benchmark_mode
        score_data = scores_by_mode_raw.get(selected_mode)
        if score_data is None:
            raise sanic.exceptions.NotFound(f"Provider quality score not found for benchmark_mode='{selected_mode}'")
    else:
        selected_mode = None
        score_data = None
        for mode in QUALITY_BENCHMARK_MODE_ORDER:
            if mode in scores_by_mode_raw:
                selected_mode = mode
                score_data = scores_by_mode_raw[mode]
                break
        if score_data is None:
            score_data = score_rows[0]
            selected_mode = str(score_data.get("benchmark_mode") or "national")

    selected_cohort_context = _cohort_context_from_score_row(
        score_data,
        computed_live=False,
        procedure_match_threshold=procedure_match_threshold,
    )
    cohort_context_by_mode = {
        mode: _cohort_context_from_score_row(
            mode_score,
            computed_live=False,
            procedure_match_threshold=procedure_match_threshold,
        )
        for mode, mode_score in scores_by_mode_raw.items()
    }

    domains_by_mode: dict[str, dict[str, Any]] = {
        mode: _empty_domains_payload()
        for mode in QUALITY_BENCHMARK_MODE_ORDER
    }
    domain_query = text(
        f"""
        SELECT
            benchmark_mode,
            domain,
            risk_ratio,
            score_0_100,
            ci75_low,
            ci75_high,
            ci90_low,
            ci90_high,
            evidence_n
        FROM {PRICING_SCHEMA}.{QUALITY_DOMAIN_TABLE_NAME}
        WHERE npi = :npi AND year = :year
        """
    )
    domain_result = await session.execute(domain_query, {"npi": provider_npi, "year": year})
    for row in domain_result:
        row_data = _row_to_dict(row)
        mode = str(row_data.get("benchmark_mode") or "").strip().lower()
        if mode not in QUALITY_BENCHMARK_MODE_ORDER:
            continue
        domain_name = str(row_data.get("domain") or "").strip().lower()
        if domain_name not in domains_by_mode[mode]:
            continue
        domains_by_mode[mode][domain_name] = {
            "risk_ratio_point": _as_float(row_data.get("risk_ratio")),
            "score_0_100": _as_float(row_data.get("score_0_100")),
            "evidence_n": _as_float(row_data.get("evidence_n")),
            "ci_75": _ci_payload(row_data.get("ci75_low"), row_data.get("ci75_high")),
            "ci_90": _ci_payload(row_data.get("ci90_low"), row_data.get("ci90_high")),
        }

    selected_payload = _build_quality_mode_payload(
        score_data,
        domains_by_mode.get(selected_mode, _empty_domains_payload()),
        cohort_context=selected_cohort_context,
    )

    scores_by_benchmark_mode: dict[str, dict[str, Any] | None] = {}
    for mode in QUALITY_BENCHMARK_MODE_ORDER:
        mode_score = scores_by_mode_raw.get(mode)
        if mode_score is None:
            scores_by_benchmark_mode[mode] = None
            continue
        scores_by_benchmark_mode[mode] = _build_quality_mode_payload(
            mode_score,
            domains_by_mode.get(mode, _empty_domains_payload()),
            cohort_context=cohort_context_by_mode.get(mode),
        )

    return response.json(
        _build_provider_quality_response_payload(
            provider_npi=provider_npi,
            year_used=year,
            year_source=year_source,
            selected_payload=selected_payload,
            selected_mode=selected_mode,
            scores_by_benchmark_mode=scores_by_benchmark_mode,
            benchmark_mode=benchmark_mode,
        )
    )


@blueprint.get("/providers/<npi>/procedures", name="pricing.providers.procedures.list")
@blueprint.get("/physicians/<npi>/services", name="pricing.physicians.services.list")
async def list_provider_procedures(request, npi: str):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    min_claims = _parse_float(args.get("min_claims"), "min_claims", minimum=0)
    min_total_cost = _parse_float(args.get("min_total_cost"), "min_total_cost", minimum=0)
    q = str(args.get("q", "")).strip().lower()
    service_name = str(args.get("service_name", args.get("generic_name", ""))).strip().lower()
    reported_code = str(args.get("reported_code", args.get("brand_name", ""))).strip().lower()
    code = str(args.get("code", "")).strip()
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)

    year, year_source = await _resolve_year(session, provider_procedure_table, year)
    code_context = None

    filters = [provider_procedure_table.c.npi == provider_npi, provider_procedure_table.c.year == year]
    if service_name:
        filters.append(func.lower(provider_procedure_table.c.service_description).like(f"%{service_name}%"))
    if reported_code:
        filters.append(func.lower(provider_procedure_table.c.reported_code).like(f"%{reported_code}%"))
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(provider_procedure_table.c.service_description).like(q_like),
                func.lower(provider_procedure_table.c.reported_code).like(q_like),
            )
        )
    if code:
        internal_codes, code_context = await _resolve_internal_codes_for_request(
            session,
            code,
            args,
        )
        filters.append(provider_procedure_table.c.procedure_code.in_(internal_codes))
    if min_claims is not None:
        filters.append(provider_procedure_table.c.total_services >= min_claims)
    if min_total_cost is not None:
        filters.append(provider_procedure_table.c.total_allowed_amount >= min_total_cost)

    where_clause = and_(*filters)
    count_result = await session.execute(select(func.count()).select_from(provider_procedure_table).where(where_clause))
    total = int(count_result.scalar() or 0)

    query = select(provider_procedure_table).where(where_clause)
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_allowed_amount")
    query = _apply_ordering(
        query,
        order_by,
        order,
        {
            "procedure_code": provider_procedure_table.c.procedure_code,
            "service_description": provider_procedure_table.c.service_description,
            "reported_code": provider_procedure_table.c.reported_code,
            "total_services": provider_procedure_table.c.total_services,
            "total_allowed_amount": provider_procedure_table.c.total_allowed_amount,
            "total_beneficiaries": provider_procedure_table.c.total_beneficiaries,
            "generic_name": provider_procedure_table.c.service_description,
            "brand_name": provider_procedure_table.c.reported_code,
            "total_claims": provider_procedure_table.c.total_services,
            "total_30day_fills": provider_procedure_table.c.total_beneficiary_day_services,
            "total_day_supply": provider_procedure_table.c.total_submitted_charges,
            "total_drug_cost": provider_procedure_table.c.total_allowed_amount,
            "total_benes": provider_procedure_table.c.total_beneficiaries,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)

    result = await session.execute(query)
    items = [
        _normalize_service_payload({**_row_to_dict(row), "__include_legacy_fields__": include_legacy_fields})
        for row in result
    ]

    query_payload: dict[str, Any] = {
        "npi": provider_npi,
        "year": year,
        "year_used": year,
        "year_source": year_source,
        "service_name": service_name or None,
        "reported_code": reported_code or None,
        "q": q or None,
        "code": code or None,
        "min_claims": min_claims,
        "min_total_cost": min_total_cost,
        "include_legacy_fields": include_legacy_fields,
        "order_by": order_by,
        "order": order,
    }
    if code_context is not None:
        query_payload.update(
            {
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            }
        )

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": query_payload,
        }
    )


async def _provider_procedure_detail(
    request,
    npi: str,
    code_value: str,
    *,
    default_code_system: str = INTERNAL_CODE_SYSTEM,
):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")

    year, year_source = await _resolve_year(session, provider_procedure_table, year)
    internal_codes, code_context = await _resolve_internal_codes_for_request(
        session,
        code_value,
        args,
        default_system=default_code_system,
    )

    query = (
        select(
            provider_procedure_table,
            procedure_table.c.avg_submitted_charge,
            procedure_table.c.avg_allowed_amount,
            procedure_table.c.avg_payment_amount,
            procedure_table.c.avg_standardized_amount,
            procedure_table.c.total_allowed_amount,
            procedure_table.c.total_services.label("market_total_services"),
            procedure_table.c.total_beneficiaries.label("market_total_beneficiaries"),
            procedure_table.c.source_year,
        )
        .select_from(
            provider_procedure_table.outerjoin(
                procedure_table,
                provider_procedure_table.c.procedure_code == procedure_table.c.procedure_code,
            )
        )
        .where(
            and_(
                provider_procedure_table.c.npi == provider_npi,
                provider_procedure_table.c.procedure_code.in_(internal_codes),
                provider_procedure_table.c.year == year,
            )
        )
        .order_by(provider_procedure_table.c.total_services.desc().nullslast())
        .limit(1)
    )

    result = await session.execute(query)
    row = result.first()
    if row is None:
        raise sanic.exceptions.NotFound("Provider procedure not found")

    payload = _normalize_service_payload({**_row_to_dict(row), "__include_legacy_fields__": include_legacy_fields})
    payload["year_used"] = year
    payload["year_source"] = year_source
    payload["input_code"] = code_context["input_code"]
    payload["resolved_codes"] = code_context["resolved_codes"]
    payload["matched_via"] = code_context["matched_via"]
    return response.json(payload)


@blueprint.get("/providers/<npi>/procedures/<procedure_code>", name="pricing.providers.procedures.get")
async def get_provider_procedure(request, npi: str, procedure_code: str):
    return await _provider_procedure_detail(
        request,
        npi,
        procedure_code,
        default_code_system=INTERNAL_CODE_SYSTEM,
    )


@blueprint.get("/physicians/<npi>/services/<code_system>/<code>", name="pricing.physicians.services.get")
async def get_physician_service(request, npi: str, code_system: str, code: str):
    return await _provider_procedure_detail(
        request,
        npi,
        code,
        default_code_system=code_system,
    )


async def _provider_procedure_cost_level(
    request,
    npi: str,
    code_value: str,
    *,
    default_code_system: str = INTERNAL_CODE_SYSTEM,
):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")

    if not await _table_exists(session, provider_procedure_cost_profile_table.name):
        raise sanic.exceptions.NotFound("Cost profile data is not available; run claims-pricing import.")
    if not await _table_exists(session, procedure_peer_stats_table.name):
        raise sanic.exceptions.NotFound("Peer stats data is not available; run claims-pricing import.")

    year = _parse_int(args.get("year"), "year", minimum=2013)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    zip5 = _normalize_zip5(args.get("zip5"))
    zip_radius_miles = _parse_zip_radius_miles(
        args.get("zip_radius_miles"),
        param="zip_radius_miles",
        default=PROCEDURE_COST_LEVEL_ZIP_RADIUS_DEFAULT_MILES,
    )
    cohort_strategy_requested = _parse_procedure_cost_cohort_strategy(args.get("cohort_strategy"))
    specialty_key = _parse_specialty_key(args.get("specialty"))
    setting_key = _parse_setting_key(args.get("setting"))
    zip_context = await _lookup_zip_context(session, zip5)
    if not state and zip_context is not None:
        state = str(zip_context.get("state") or "").strip().upper()
    if not city and zip_context is not None:
        city = str(zip_context.get("city_lower") or "").strip().lower()

    year, year_source = await _resolve_year(session, provider_procedure_cost_profile_table, year)
    internal_codes, code_context = await _resolve_internal_codes_for_request(
        session,
        code_value,
        args,
        default_system=default_code_system,
    )
    profile_order = case(
        (provider_procedure_cost_profile_table.c.geography_scope == "national", 0),
        (provider_procedure_cost_profile_table.c.geography_scope == "state", 1),
        (provider_procedure_cost_profile_table.c.geography_scope == "state_city", 2),
        (provider_procedure_cost_profile_table.c.geography_scope == "zip5", 3),
        else_=4,
    )
    base_profile_filters = [
        provider_procedure_cost_profile_table.c.npi == provider_npi,
        provider_procedure_cost_profile_table.c.year == year,
        provider_procedure_cost_profile_table.c.procedure_code.in_(internal_codes),
        provider_procedure_cost_profile_table.c.setting_key == setting_key,
    ]
    if specialty_key:
        base_profile_filters.append(provider_procedure_cost_profile_table.c.specialty_key == specialty_key)

    base_profile_result = await session.execute(
        select(provider_procedure_cost_profile_table)
        .where(and_(*base_profile_filters))
        .order_by(
            profile_order.asc(),
            provider_procedure_cost_profile_table.c.claim_count.desc().nullslast(),
            provider_procedure_cost_profile_table.c.avg_submitted_charge.desc().nullslast(),
        )
        .limit(1)
    )
    base_profile_row = base_profile_result.first()
    if base_profile_row is None and specialty_key:
        fallback_profile_filters = [
            provider_procedure_cost_profile_table.c.npi == provider_npi,
            provider_procedure_cost_profile_table.c.year == year,
            provider_procedure_cost_profile_table.c.procedure_code.in_(internal_codes),
            provider_procedure_cost_profile_table.c.setting_key == setting_key,
        ]
        fallback_profile_result = await session.execute(
            select(provider_procedure_cost_profile_table)
            .where(and_(*fallback_profile_filters))
            .order_by(
                profile_order.asc(),
                provider_procedure_cost_profile_table.c.claim_count.desc().nullslast(),
                provider_procedure_cost_profile_table.c.avg_submitted_charge.desc().nullslast(),
            )
            .limit(1)
        )
        base_profile_row = fallback_profile_result.first()

    if base_profile_row is None:
        raise sanic.exceptions.NotFound("Provider procedure cost profile not found.")

    selected_profile = _row_to_dict(base_profile_row)
    provider_specialty = str(selected_profile.get("specialty_key") or "").strip().lower()
    procedure_internal_code = int(selected_profile["procedure_code"])

    specialty_candidates: list[str] = []
    if specialty_key:
        specialty_candidates.append(specialty_key)
    elif provider_specialty:
        specialty_candidates.append(provider_specialty)
    specialty_candidates.append("__all__")
    specialty_candidates = list(dict.fromkeys(specialty_candidates))

    zip_candidates: list[str] = []
    zip_candidate_meta: dict[str, dict[str, Any]] = {}
    if zip5:
        zip_rows = await _zip_radius_rows(
            session,
            zip5=zip5,
            radius_miles=zip_radius_miles,
            state_hint=state or None,
            anchor_context=zip_context,
        )
        zip_candidates, zip_candidate_meta = _zip_ring_candidates(
            zip_rows,
            anchor_zip5=zip5,
            radius_miles=zip_radius_miles,
        )
        if not zip_candidates:
            zip_candidates = [zip5]
            zip_candidate_meta[zip5] = {
                "distance_miles": 0.0,
                "distance_bucket": "zip_exact",
                "selected_radius_miles": 0.0,
                "is_anchor": True,
            }

    geography_candidates: list[tuple[str, str]] = []
    for candidate_zip in zip_candidates:
        geography_candidates.append(("zip5", candidate_zip))
    for item in _geography_candidates(state_raw=state, city_raw=city, zip5_raw=None):
        geography_candidates.append(item)

    seen_geographies: set[tuple[str, str]] = set()
    ordered_geographies: list[tuple[str, str]] = []
    for item in geography_candidates:
        normalized_item = (str(item[0]).strip(), str(item[1]).strip())
        if not normalized_item[0] or not normalized_item[1]:
            continue
        if normalized_item in seen_geographies:
            continue
        seen_geographies.add(normalized_item)
        ordered_geographies.append(normalized_item)

    selected_peer: dict[str, Any] | None = None
    selected_scope: str | None = None
    selected_value: str | None = None
    selected_specialty: str | None = None
    cohort_strategy_used = PROCEDURE_COST_COHORT_STRATEGY_PRECOMPUTED

    if cohort_strategy_requested == PROCEDURE_COST_COHORT_STRATEGY_NEAR_DYNAMIC and zip_candidates:
        dynamic_peer_result = await _build_dynamic_zip_peer_stats(
            session,
            year=year,
            procedure_code=procedure_internal_code,
            setting_key=setting_key,
            specialty_candidates=specialty_candidates,
            zip_candidates=zip_candidates,
            min_claims=PROCEDURE_COST_DYNAMIC_MIN_PEER_CLAIMS,
            min_providers=PROCEDURE_COST_DYNAMIC_MIN_PEER_PROVIDERS,
            iqr_factor=PROCEDURE_COST_DYNAMIC_IQR_FACTOR,
        )
        if dynamic_peer_result is not None:
            dynamic_peer_payload, dynamic_specialty = dynamic_peer_result
            selected_peer = dynamic_peer_payload
            selected_scope = "zip_radius"
            selected_value = f"{zip5}|{int(round(zip_radius_miles))}mi"
            selected_specialty = dynamic_specialty
            cohort_strategy_used = PROCEDURE_COST_COHORT_STRATEGY_NEAR_DYNAMIC

    if selected_peer is None:
        geography_clauses = [
            and_(
                procedure_peer_stats_table.c.geography_scope == scope,
                procedure_peer_stats_table.c.geography_value == value,
            )
            for scope, value in ordered_geographies
        ]
        if not geography_clauses:
            geography_clauses = [
                and_(
                    procedure_peer_stats_table.c.geography_scope == "national",
                    procedure_peer_stats_table.c.geography_value == "US",
                )
            ]

        peer_result = await session.execute(
            select(procedure_peer_stats_table).where(
                and_(
                    procedure_peer_stats_table.c.procedure_code == procedure_internal_code,
                    procedure_peer_stats_table.c.year == year,
                    procedure_peer_stats_table.c.setting_key == setting_key,
                    procedure_peer_stats_table.c.specialty_key.in_(specialty_candidates),
                    or_(*geography_clauses),
                )
            )
        )
        peer_rows = [_row_to_dict(row) for row in peer_result]
        peer_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in peer_rows:
            key = (
                str(row.get("geography_scope") or "").strip(),
                str(row.get("geography_value") or "").strip(),
                str(row.get("specialty_key") or "").strip().lower(),
            )
            if not all(key):
                continue
            peer_by_key[key] = row

        for geography_scope, geography_value in ordered_geographies:
            for peer_specialty in specialty_candidates:
                peer_key = (geography_scope, geography_value, peer_specialty)
                peer_payload = peer_by_key.get(peer_key)
                if peer_payload is None:
                    continue
                selected_peer = peer_payload
                selected_scope = geography_scope
                selected_value = geography_value
                selected_specialty = peer_specialty
                break
            if selected_peer is not None:
                break

    if selected_peer is None:
        raise sanic.exceptions.NotFound("Peer group is not available for this provider procedure in the requested region.")

    provider_avg = _as_float(selected_profile.get("avg_submitted_charge"))
    provider_claim_count = _as_float(selected_profile.get("claim_count"))
    provider_total_submitted_charge = _as_float(selected_profile.get("total_submitted_charge"))
    peer_p10 = _as_float(selected_peer.get("p10") if selected_peer else None)
    peer_p20 = _as_float(selected_peer.get("p20") if selected_peer else None)
    peer_p40 = _as_float(selected_peer.get("p40") if selected_peer else None)
    peer_p50 = _as_float(selected_peer.get("p50") if selected_peer else None)
    peer_p60 = _as_float(selected_peer.get("p60") if selected_peer else None)
    peer_p80 = _as_float(selected_peer.get("p80") if selected_peer else None)
    peer_p90 = _as_float(selected_peer.get("p90") if selected_peer else None)
    estimated_cost_level = _cost_level_from_thresholds(
        provider_avg,
        peer_p20,
        peer_p40,
        peer_p60,
        peer_p80,
    )
    procedure_code_text = str(procedure_internal_code)
    procedure_name = None
    reported_code = None
    procedure_detail_result = await session.execute(
        select(
            provider_procedure_table.c.service_description,
            provider_procedure_table.c.reported_code,
        )
        .where(
            and_(
                provider_procedure_table.c.npi == provider_npi,
                provider_procedure_table.c.year == year,
                provider_procedure_table.c.procedure_code == procedure_internal_code,
            )
        )
        .order_by(provider_procedure_table.c.total_services.desc().nullslast())
        .limit(1)
    )
    procedure_detail_row = procedure_detail_result.first()
    if procedure_detail_row is not None:
        procedure_detail = _row_to_dict(procedure_detail_row)
        procedure_name = procedure_detail.get("service_description")
        reported_code_raw = procedure_detail.get("reported_code")
        if reported_code_raw is not None:
            reported_code = str(reported_code_raw).strip().upper() or None

    if not procedure_name:
        catalog_result = await session.execute(
            select(code_catalog_table.c.display_name, code_catalog_table.c.short_description)
            .where(
                and_(
                    func.upper(code_catalog_table.c.code_system) == INTERNAL_CODE_SYSTEM,
                    func.upper(code_catalog_table.c.code) == procedure_code_text,
                )
            )
            .limit(1)
        )
        catalog_row = catalog_result.first()
        if catalog_row is not None:
            catalog_payload = _row_to_dict(catalog_row)
            procedure_name = catalog_payload.get("display_name") or catalog_payload.get("short_description")

    reported_code_system = None
    if reported_code:
        reported_code_system = "CPT" if FIVE_DIGIT_CODE_PATTERN.fullmatch(reported_code) else "HCPCS"

    selected_zip_meta = zip_candidate_meta.get(str(selected_value or "").strip()) if selected_scope == "zip5" else None

    return response.json(
        {
            "npi": provider_npi,
            "year_used": year,
            "year_source": year_source,
            "estimated_cost_level": estimated_cost_level,
            "confidence": _confidence_label_from_claim_count(provider_claim_count),
            "procedure": {
                "code_system": INTERNAL_CODE_SYSTEM,
                "code": procedure_code_text,
                "name": procedure_name,
                "description": procedure_name,
                "reported_code": reported_code,
                "reported_code_system": reported_code_system,
            },
            "provider": {
                "procedure_code": procedure_internal_code,
                "claim_count": provider_claim_count,
                "avg_submitted_charge": provider_avg,
                "total_submitted_charge": provider_total_submitted_charge,
                "specialty_key": selected_profile.get("specialty_key"),
                "setting_key": selected_profile.get("setting_key"),
            },
            "peer_group": {
                "provider_count": int(selected_peer.get("provider_count") or 0),
                "median_avg_submitted_charge": peer_p50,
                "typical_range": {
                    "p10": peer_p10,
                    "p90": peer_p90,
                },
                "estimated_cost_level_thresholds": {
                    "$": {"max_avg_submitted_charge": peer_p20},
                    "$$": {"max_avg_submitted_charge": peer_p40},
                    "$$$": {"max_avg_submitted_charge": peer_p60},
                    "$$$$": {"max_avg_submitted_charge": peer_p80},
                    "$$$$$": {"min_avg_submitted_charge": peer_p80},
                },
                "geography_scope": selected_scope,
                "geography_value": selected_value,
                "specialty_key": selected_specialty,
                "cohort_type": _cohort_type_from_scope(selected_scope),
                "specialty_scope": _specialty_scope_label(selected_specialty),
                "setting_key": setting_key,
                "min_claim_count": _as_float(selected_peer.get("min_claim_count")),
                "max_claim_count": _as_float(selected_peer.get("max_claim_count")),
                "anchor_zip5": zip5,
                "distance_miles": _as_float((selected_zip_meta or {}).get("distance_miles")),
                "distance_bucket": (selected_zip_meta or {}).get("distance_bucket"),
                "selected_radius_miles": _as_float((selected_zip_meta or {}).get("selected_radius_miles")),
                "cohort_strategy_used": cohort_strategy_used,
            },
            "query": {
                "state": state or None,
                "city": city or None,
                "zip5": zip5 or None,
                "zip_radius_miles": zip_radius_miles if zip5 else None,
                "cohort_strategy": cohort_strategy_requested,
                "cohort_strategy_used": cohort_strategy_used,
                "specialty": specialty_key,
                "setting": setting_key,
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            },
        }
    )


@blueprint.get(
    "/providers/<npi>/procedures/<procedure_code>/estimated-cost-level",
    name="pricing.providers.procedures.estimated_cost_level_internal.get",
)
async def get_provider_procedure_estimated_cost_level_internal(request, npi: str, procedure_code: str):
    return await _provider_procedure_cost_level(
        request,
        npi,
        procedure_code,
        default_code_system=INTERNAL_CODE_SYSTEM,
    )


@blueprint.get(
    "/providers/<npi>/procedures/<code_system>/<code>/estimated-cost-level",
    name="pricing.providers.procedures.estimated_cost_level.get",
)
async def get_provider_procedure_estimated_cost_level(request, npi: str, code_system: str, code: str):
    return await _provider_procedure_cost_level(
        request,
        npi,
        code,
        default_code_system=code_system,
    )


@blueprint.get(
    "/physicians/<npi>/services/<code_system>/<code>/estimated-cost-level",
    name="pricing.physicians.services.estimated_cost_level.get",
)
async def get_physician_service_estimated_cost_level(request, npi: str, code_system: str, code: str):
    return await _provider_procedure_cost_level(
        request,
        npi,
        code,
        default_code_system=code_system,
    )


async def _provider_procedure_locations(
    request,
    npi: str,
    code_value: str,
    *,
    default_code_system: str = INTERNAL_CODE_SYSTEM,
):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    zip5 = str(args.get("zip5", "")).strip()
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)

    year, year_source = await _resolve_year(session, location_table, year)
    internal_codes, code_context = await _resolve_internal_codes_for_request(
        session,
        code_value,
        args,
        default_system=default_code_system,
    )

    filters = [
        location_table.c.npi == provider_npi,
        location_table.c.procedure_code.in_(internal_codes),
        location_table.c.year == year,
    ]
    if state:
        filters.append(func.upper(location_table.c.state) == state)
    if city:
        filters.append(func.lower(location_table.c.city).like(f"%{city}%"))
    if zip5:
        filters.append(location_table.c.zip5 == zip5)

    where_clause = and_(*filters)
    count_result = await session.execute(select(func.count()).select_from(location_table).where(where_clause))
    total = int(count_result.scalar() or 0)

    query = select(location_table).where(where_clause)
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "city")
    query = _apply_ordering(
        query,
        order_by,
        order,
        {
            "city": location_table.c.city,
            "state": location_table.c.state,
            "zip5": location_table.c.zip5,
            "npi": location_table.c.npi,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)

    result = await session.execute(query)
    items = [
        _normalize_service_payload({**_row_to_dict(row), "__include_legacy_fields__": include_legacy_fields})
        for row in result
    ]

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "npi": provider_npi,
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "state": state or None,
                "city": city or None,
                "zip5": zip5 or None,
                "include_legacy_fields": include_legacy_fields,
                "order_by": order_by,
                "order": order,
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            },
        }
    )


@blueprint.get("/providers/<npi>/procedures/<procedure_code>/locations", name="pricing.providers.procedures.locations.list")
async def list_provider_procedure_locations(request, npi: str, procedure_code: str):
    return await _provider_procedure_locations(
        request,
        npi,
        procedure_code,
        default_code_system=INTERNAL_CODE_SYSTEM,
    )


@blueprint.get("/physicians/<npi>/services/<code_system>/<code>/locations", name="pricing.physicians.services.locations.list")
async def list_physician_service_locations(request, npi: str, code_system: str, code: str):
    return await _provider_procedure_locations(
        request,
        npi,
        code,
        default_code_system=code_system,
    )


@blueprint.get("/procedures/<code_system>/<code>/providers", name="pricing.procedures.providers.list")
async def list_procedure_providers(request, code_system: str, code: str):
    session = _get_session(request)
    args = request.args

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    min_claims = _parse_float(args.get("min_claims"), "min_claims", minimum=0)
    min_total_cost = _parse_float(args.get("min_total_cost"), "min_total_cost", minimum=0)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    specialty = str(args.get("specialty", "")).strip().lower()
    q = str(args.get("q", "")).strip().lower()
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)

    year, year_source = await _resolve_year(session, provider_procedure_table, year)
    internal_codes, code_context = await _resolve_internal_codes_for_request(
        session,
        code,
        args,
        default_system=code_system,
    )

    filters = [
        provider_procedure_table.c.procedure_code.in_(internal_codes),
        provider_procedure_table.c.year == year,
        provider_table.c.year == year,
        provider_table.c.npi == provider_procedure_table.c.npi,
    ]
    if state:
        filters.append(func.upper(provider_table.c.state) == state)
    if city:
        filters.append(func.lower(provider_table.c.city).like(f"%{city}%"))
    if specialty:
        filters.append(func.lower(provider_table.c.provider_type).like(f"%{specialty}%"))
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(provider_table.c.provider_name).like(q_like),
                func.lower(provider_table.c.provider_type).like(q_like),
                cast(provider_table.c.npi, String).like(f"%{q}%"),
            )
        )
    if min_claims is not None:
        filters.append(provider_procedure_table.c.total_services >= min_claims)
    if min_total_cost is not None:
        filters.append(provider_procedure_table.c.total_allowed_amount >= min_total_cost)
    where_clause = and_(*filters)

    grouped = (
        select(
            provider_procedure_table.c.npi.label("npi"),
            provider_table.c.provider_name.label("provider_name"),
            provider_table.c.provider_type.label("provider_type"),
            provider_table.c.city.label("city"),
            provider_table.c.state.label("state"),
            provider_table.c.zip5.label("zip5"),
            func.sum(provider_procedure_table.c.total_services).label("total_services"),
            func.sum(provider_procedure_table.c.total_submitted_charges).label("total_submitted_charges"),
            func.sum(provider_procedure_table.c.total_allowed_amount).label("total_allowed_amount"),
            func.sum(provider_procedure_table.c.total_beneficiaries).label("total_beneficiaries"),
            func.count().label("matched_rows"),
        )
        .select_from(provider_procedure_table.join(provider_table, provider_table.c.npi == provider_procedure_table.c.npi))
        .where(where_clause)
        .group_by(
            provider_procedure_table.c.npi,
            provider_table.c.provider_name,
            provider_table.c.provider_type,
            provider_table.c.city,
            provider_table.c.state,
            provider_table.c.zip5,
        )
    )
    grouped_subquery = grouped.subquery()
    count_result = await session.execute(select(func.count()).select_from(grouped_subquery))
    total = int(count_result.scalar() or 0)

    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_allowed_amount")
    query = select(grouped_subquery)
    query = _apply_ordering(
        query,
        order_by,
        order,
        {
            "npi": grouped_subquery.c.npi,
            "provider_name": grouped_subquery.c.provider_name,
            "total_services": grouped_subquery.c.total_services,
            "total_submitted_charges": grouped_subquery.c.total_submitted_charges,
            "total_allowed_amount": grouped_subquery.c.total_allowed_amount,
            "total_beneficiaries": grouped_subquery.c.total_beneficiaries,
            "matched_rows": grouped_subquery.c.matched_rows,
            "total_claims": grouped_subquery.c.total_services,
            "total_day_supply": grouped_subquery.c.total_submitted_charges,
            "total_drug_cost": grouped_subquery.c.total_allowed_amount,
            "total_benes": grouped_subquery.c.total_beneficiaries,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)
    result = await session.execute(query)
    items = [_normalize_provider_service_aggregate(_row_to_dict(row), include_legacy=include_legacy_fields) for row in result]

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "state": state or None,
                "city": city or None,
                "specialty": specialty or None,
                "q": q or None,
                "min_claims": min_claims,
                "min_total_cost": min_total_cost,
                "include_legacy_fields": include_legacy_fields,
                "order_by": order_by,
                "order": order,
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            },
        }
    )


@blueprint.get("/procedures/<code_system>/<code>/benchmarks", name="pricing.procedures.benchmarks.get")
async def get_procedure_benchmarks(request, code_system: str, code: str):
    session = _get_session(request)
    args = request.args

    year = _parse_int(args.get("year"), "year", minimum=2013)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)

    year, year_source = await _resolve_year(session, provider_procedure_table, year)
    internal_codes, code_context = await _resolve_internal_codes_for_request(
        session,
        code,
        args,
        default_system=code_system,
    )

    filters = [
        provider_procedure_table.c.procedure_code.in_(internal_codes),
        provider_procedure_table.c.year == year,
        provider_table.c.npi == provider_procedure_table.c.npi,
        provider_table.c.year == provider_procedure_table.c.year,
    ]
    if state:
        filters.append(func.upper(provider_table.c.state) == state)
    if city:
        filters.append(func.lower(provider_table.c.city).like(f"%{city}%"))
    where_clause = and_(*filters)

    aggregate_query = (
        select(
            func.count().label("matched_rows"),
            func.count(func.distinct(provider_procedure_table.c.npi)).label("provider_count"),
            func.sum(provider_procedure_table.c.total_services).label("total_services"),
            func.sum(provider_procedure_table.c.total_submitted_charges).label("total_submitted_charges"),
            func.sum(provider_procedure_table.c.total_allowed_amount).label("total_allowed_amount"),
            func.avg(provider_procedure_table.c.total_allowed_amount).label("avg_total_allowed_amount"),
            func.min(provider_procedure_table.c.total_allowed_amount).label("min_total_allowed_amount"),
            func.max(provider_procedure_table.c.total_allowed_amount).label("max_total_allowed_amount"),
        )
        .select_from(provider_procedure_table.join(provider_table, provider_table.c.npi == provider_procedure_table.c.npi))
        .where(where_clause)
    )
    aggregate_result = await session.execute(aggregate_query)
    aggregate = _row_to_dict(aggregate_result.first() or {})

    provider_costs_subquery = (
        select(
            provider_procedure_table.c.npi.label("npi"),
            func.sum(provider_procedure_table.c.total_allowed_amount).label("provider_total_allowed_amount"),
        )
        .select_from(provider_procedure_table.join(provider_table, provider_table.c.npi == provider_procedure_table.c.npi))
        .where(where_clause)
        .group_by(provider_procedure_table.c.npi)
    ).subquery()

    threshold_query = select(
        func.percentile_cont(0.20).within_group(provider_costs_subquery.c.provider_total_allowed_amount).label("p20"),
        func.percentile_cont(0.40).within_group(provider_costs_subquery.c.provider_total_allowed_amount).label("p40"),
        func.percentile_cont(0.60).within_group(provider_costs_subquery.c.provider_total_allowed_amount).label("p60"),
        func.percentile_cont(0.80).within_group(provider_costs_subquery.c.provider_total_allowed_amount).label("p80"),
    )
    threshold_result = await session.execute(threshold_query)
    thresholds = _row_to_dict(threshold_result.first() or {})

    benchmark_payload: dict[str, Any] = {
        "matched_rows": int(aggregate.get("matched_rows") or 0),
        "provider_count": int(aggregate.get("provider_count") or 0),
        "total_services": float(aggregate.get("total_services") or 0.0),
        "total_submitted_charges": float(aggregate.get("total_submitted_charges") or 0.0),
        "total_allowed_amount": float(aggregate.get("total_allowed_amount") or 0.0),
        "avg_total_allowed_amount": float(aggregate.get("avg_total_allowed_amount") or 0.0),
        "min_total_allowed_amount": float(aggregate.get("min_total_allowed_amount") or 0.0),
        "max_total_allowed_amount": float(aggregate.get("max_total_allowed_amount") or 0.0),
        "estimated_cost_level_thresholds": {
            "$": {"max_total_allowed_amount": thresholds.get("p20")},
            "$$": {"max_total_allowed_amount": thresholds.get("p40")},
            "$$$": {"max_total_allowed_amount": thresholds.get("p60")},
            "$$$$": {"max_total_allowed_amount": thresholds.get("p80")},
            "$$$$$": {"min_total_allowed_amount": thresholds.get("p80")},
        },
    }
    if include_legacy_fields:
        benchmark_payload.update(
            {
                "total_claims": benchmark_payload["total_services"],
                "total_day_supply": benchmark_payload["total_submitted_charges"],
                "total_drug_cost": benchmark_payload["total_allowed_amount"],
                "avg_total_drug_cost": benchmark_payload["avg_total_allowed_amount"],
                "min_total_drug_cost": benchmark_payload["min_total_allowed_amount"],
                "max_total_drug_cost": benchmark_payload["max_total_allowed_amount"],
            }
        )
    return response.json(
        {
            "query": {
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "state": state or None,
                "city": city or None,
                "include_legacy_fields": include_legacy_fields,
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            },
            "benchmark": benchmark_payload,
        }
    )


@blueprint.get("/procedures/<code_system>/<code>/geo-benchmarks", name="pricing.procedures.geo_benchmarks.get")
async def get_procedure_geo_benchmarks(request, code_system: str, code: str):
    session = _get_session(request)
    args = request.args

    year = _parse_int(args.get("year"), "year", minimum=2013)
    state = str(args.get("state", "")).strip().upper()

    if not await _table_exists(session, procedure_geo_benchmark_table.name):
        return response.json(
            {
                "query": {
                    "year": year,
                    "year_used": year,
                    "year_source": "request" if year is not None else "env",
                    "state": state or None,
                    "data_status": "unavailable",
                    "input_code": {"code_system": _normalize_code_system(code_system), "code": _normalize_code(code)},
                    "resolved_codes": [],
                    "matched_via": [],
                },
                "benchmarks": {"national": None, "state": None},
            }
        )

    year, year_source = await _resolve_year(session, procedure_geo_benchmark_table, year)
    internal_codes, code_context = await _resolve_internal_codes_for_request(
        session,
        code,
        args,
        default_system=code_system,
    )

    async def _fetch_scope(scope: str, value: str | None) -> dict[str, Any] | None:
        filters = [
            procedure_geo_benchmark_table.c.year == year,
            procedure_geo_benchmark_table.c.procedure_code.in_(internal_codes),
            procedure_geo_benchmark_table.c.geography_scope == scope,
        ]
        if value is not None:
            filters.append(procedure_geo_benchmark_table.c.geography_value == value)

        total_services_expr = func.sum(procedure_geo_benchmark_table.c.total_services)
        query = (
            select(
                func.count().label("rows"),
                total_services_expr.label("total_services"),
                (
                    func.sum(
                        procedure_geo_benchmark_table.c.avg_submitted_charge
                        * procedure_geo_benchmark_table.c.total_services
                    ) / func.nullif(total_services_expr, 0)
                ).label("avg_submitted_charge"),
                (
                    func.sum(
                        procedure_geo_benchmark_table.c.avg_payment_amount
                        * procedure_geo_benchmark_table.c.total_services
                    ) / func.nullif(total_services_expr, 0)
                ).label("avg_payment_amount"),
                (
                    func.sum(
                        procedure_geo_benchmark_table.c.avg_standardized_amount
                        * procedure_geo_benchmark_table.c.total_services
                    ) / func.nullif(total_services_expr, 0)
                ).label("avg_standardized_amount"),
            )
            .select_from(procedure_geo_benchmark_table)
            .where(and_(*filters))
        )
        result = await session.execute(query)
        row = _row_to_dict(result.first() or {})
        if int(row.get("rows") or 0) <= 0:
            return None
        return {
            "geography_scope": scope,
            "geography_value": value or ("US" if scope == "national" else None),
            "total_services": _as_float(row.get("total_services")),
            "avg_submitted_charge": _as_float(row.get("avg_submitted_charge")),
            "avg_payment_amount": _as_float(row.get("avg_payment_amount")),
            "avg_standardized_amount": _as_float(row.get("avg_standardized_amount")),
        }

    national = await _fetch_scope("national", "US")
    state_benchmark = await _fetch_scope("state", state) if state else None

    return response.json(
        {
            "query": {
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "state": state or None,
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            },
            "benchmarks": {
                "national": national,
                "state": state_benchmark,
            },
        }
    )


@blueprint.get("/procedures/autocomplete", name="pricing.procedures.autocomplete")
@blueprint.get("/services/autocomplete", name="pricing.services.autocomplete")
async def autocomplete_procedures(request):
    session = _get_session(request)
    args = request.args

    pagination = parse_pagination(args, default_limit=20, max_limit=100)
    q = _normalize_query_text(args.get("q"), "q", min_len=2)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    code_system_raw = str(args.get("code_system", "")).strip()
    dedupe_terms = _parse_bool(args.get("dedupe_terms"), "dedupe_terms", default=True)
    max_codes_per_term = _parse_int(args.get("max_codes_per_term"), "max_codes_per_term", minimum=1) or 5
    max_codes_per_term = min(max_codes_per_term, 25)

    if year is not None:
        year_source = "request"
    elif PRICING_DEFAULT_YEAR is not None:
        year = PRICING_DEFAULT_YEAR
        year_source = "env"
    else:
        year_source = "none"

    display_lower = func.lower(func.coalesce(code_catalog_table.c.display_name, ""))
    short_lower = func.lower(func.coalesce(code_catalog_table.c.short_description, ""))
    code_lower = func.lower(func.coalesce(code_catalog_table.c.code, ""))
    q_like = f"%{q}%"
    q_prefix = f"{q}%"

    filters = [
        func.upper(code_catalog_table.c.code_system).in_(("CPT", "HCPCS", INTERNAL_CODE_SYSTEM)),
        func.lower(func.coalesce(code_catalog_table.c.source, "")) == "cms_physician_provider_service",
        or_(
            display_lower.like(q_like),
            short_lower.like(q_like),
            code_lower.like(q_like),
        ),
    ]
    if code_system_raw:
        filters.append(func.upper(code_catalog_table.c.code_system) == _normalize_code_system(code_system_raw))
    if year is not None:
        internal_codes_for_year = select(cast(procedure_table.c.procedure_code, String)).where(
            procedure_table.c.source_year <= year
        )
        external_codes_for_year = select(func.upper(procedure_table.c.reported_code)).where(
            and_(
                procedure_table.c.source_year <= year,
                procedure_table.c.reported_code.isnot(None),
                procedure_table.c.reported_code != "",
            )
        )
        filters.append(
            or_(
                and_(
                    func.upper(code_catalog_table.c.code_system) == INTERNAL_CODE_SYSTEM,
                    code_catalog_table.c.code.in_(internal_codes_for_year),
                ),
                and_(
                    func.upper(code_catalog_table.c.code_system).in_(("CPT", "HCPCS")),
                    func.upper(code_catalog_table.c.code).in_(external_codes_for_year),
                ),
            )
        )

    ranking = case(
        (display_lower.like(q_prefix), 0),
        (short_lower.like(q_prefix), 1),
        (code_lower.like(q_prefix), 2),
        else_=3,
    )
    fetch_limit = min(max((pagination.offset + pagination.limit) * 30, 200), 3000)
    query = (
        select(
            code_catalog_table.c.code_system,
            code_catalog_table.c.code,
            code_catalog_table.c.display_name,
            code_catalog_table.c.short_description,
            code_catalog_table.c.source,
        )
        .where(and_(*filters))
        .order_by(
            ranking.asc(),
            code_catalog_table.c.display_name.asc().nullslast(),
            code_catalog_table.c.code_system.asc(),
            code_catalog_table.c.code.asc(),
        )
        .limit(fetch_limit)
    )
    result = await session.execute(query)
    rows = [_row_to_dict(row) for row in result]

    if not dedupe_terms:
        items = []
        for row in rows:
            display_name = str(row.get("display_name") or row.get("short_description") or row.get("code") or "").strip()
            if not display_name:
                continue
            code_system = str(row.get("code_system") or "").upper()
            code_value = str(row.get("code") or "").upper()
            item = {
                "term": display_name,
                "code_systems": [code_system] if code_system else [],
                "codes": [{"code_system": code_system, "code": code_value}] if code_system and code_value else [],
                "internal_codes": [code_value] if code_system == INTERNAL_CODE_SYSTEM and INT_PATTERN.fullmatch(code_value) else [],
                "sources": [row.get("source")] if row.get("source") else [],
            }
            items.append(item)
        total = len(items)
        page_items = items[pagination.offset: pagination.offset + pagination.limit]
    else:
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            term = str(row.get("display_name") or row.get("short_description") or row.get("code") or "").strip()
            if not term:
                continue
            term_key = term.lower()
            code_system = str(row.get("code_system") or "").upper()
            code_value = str(row.get("code") or "").upper()
            row_rank = 3
            if term_key.startswith(q):
                row_rank = 0
            elif str(row.get("short_description") or "").strip().lower().startswith(q):
                row_rank = 1
            elif code_value.lower().startswith(q):
                row_rank = 2

            current = grouped.get(term_key)
            if current is None:
                current = {
                    "term": term,
                    "code_systems": set(),
                    "codes": [],
                    "internal_codes": set(),
                    "sources": set(),
                    "_seen_codes": set(),
                    "_rank": row_rank,
                }
                grouped[term_key] = current

            current["_rank"] = min(current["_rank"], row_rank)
            if row.get("source"):
                current["sources"].add(str(row["source"]))
            if code_system:
                current["code_systems"].add(code_system)
            if code_system and code_value:
                pair = (code_system, code_value)
                if pair not in current["_seen_codes"] and len(current["codes"]) < max_codes_per_term:
                    current["_seen_codes"].add(pair)
                    current["codes"].append({"code_system": code_system, "code": code_value})
                if code_system == INTERNAL_CODE_SYSTEM and INT_PATTERN.fullmatch(code_value):
                    current["internal_codes"].add(code_value)

        ordered_items = []
        for item in grouped.values():
            code_systems = sorted(item["code_systems"])
            codes = sorted(item["codes"], key=lambda code_item: (code_item["code_system"], code_item["code"]))
            internal_codes = sorted(item["internal_codes"], key=lambda value: int(value))
            sources = sorted(item["sources"])
            ordered_items.append(
                {
                    "term": item["term"],
                    "code_systems": code_systems,
                    "codes": codes,
                    "internal_codes": internal_codes,
                    "sources": sources,
                    "_rank": item["_rank"],
                }
            )

        ordered_items.sort(
            key=lambda item: (
                item["_rank"],
                item["term"].lower(),
            )
        )
        total = len(ordered_items)
        page_items = ordered_items[pagination.offset: pagination.offset + pagination.limit]
        for item in page_items:
            item.pop("_rank", None)

    return response.json(
        {
            "items": page_items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "q": q,
                "year": year,
                "year_source": year_source,
                "code_system": _normalize_code_system(code_system_raw) if code_system_raw else None,
                "dedupe_terms": dedupe_terms,
                "max_codes_per_term": max_codes_per_term,
            },
        }
    )


@blueprint.get("/prescriptions/autocomplete", name="pricing.prescriptions.autocomplete")
@blueprint.get("/drugs/autocomplete", name="pricing.drugs.autocomplete")
async def autocomplete_prescriptions(request):
    session = _get_session(request)
    args = request.args

    pagination = parse_pagination(args, default_limit=20, max_limit=100)
    q = _normalize_query_text(args.get("q"), "q", min_len=2)
    year = _parse_int(args.get("year"), "year", minimum=2013)

    if not await _table_exists(session, provider_prescription_table.name):
        return response.json(
            {
                "items": [],
                "pagination": {
                    "total": 0,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": pagination.page,
                },
                "query": {
                    "q": q,
                    "year": year,
                    "year_source": "request" if year is not None else "env",
                    "data_status": "unavailable",
                },
            }
        )

    year, year_source = await _resolve_year(session, provider_prescription_table, year)
    q_like = f"%{q}%"
    q_prefix = f"{q}%"

    filters = [
        provider_prescription_table.c.year == year,
        provider_prescription_table.c.rx_code_system == INTERNAL_RX_CODE_SYSTEM,
        or_(
            func.lower(func.coalesce(provider_prescription_table.c.rx_name, "")).like(q_like),
            func.lower(func.coalesce(provider_prescription_table.c.generic_name, "")).like(q_like),
            func.lower(func.coalesce(provider_prescription_table.c.brand_name, "")).like(q_like),
            func.lower(func.coalesce(provider_prescription_table.c.rx_code, "")).like(q_like),
        ),
    ]
    grouped_query = (
        select(
            provider_prescription_table.c.rx_code_system.label("rx_code_system"),
            provider_prescription_table.c.rx_code.label("rx_code"),
            func.max(provider_prescription_table.c.rx_name).label("rx_name"),
            func.max(provider_prescription_table.c.generic_name).label("generic_name"),
            func.max(provider_prescription_table.c.brand_name).label("brand_name"),
            func.sum(provider_prescription_table.c.total_claims).label("total_claims"),
            func.sum(provider_prescription_table.c.total_drug_cost).label("total_drug_cost"),
            func.sum(provider_prescription_table.c.total_benes).label("total_benes"),
        )
        .where(and_(*filters))
        .group_by(
            provider_prescription_table.c.rx_code_system,
            provider_prescription_table.c.rx_code,
        )
    )
    grouped_subquery = grouped_query.subquery()
    count_result = await session.execute(select(func.count()).select_from(grouped_subquery))
    total = int(count_result.scalar() or 0)

    ranking = case(
        (func.lower(func.coalesce(grouped_subquery.c.generic_name, "")).like(q_prefix), 0),
        (func.lower(func.coalesce(grouped_subquery.c.brand_name, "")).like(q_prefix), 1),
        (func.lower(func.coalesce(grouped_subquery.c.rx_name, "")).like(q_prefix), 2),
        (func.lower(func.coalesce(grouped_subquery.c.rx_code, "")).like(q_prefix), 3),
        else_=4,
    )
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_claims").strip().lower()
    query = select(grouped_subquery)
    query = _apply_ordering(
        query.order_by(ranking.asc()),
        order_by,
        order,
        {
            "rx_code": grouped_subquery.c.rx_code,
            "rx_name": grouped_subquery.c.rx_name,
            "generic_name": grouped_subquery.c.generic_name,
            "brand_name": grouped_subquery.c.brand_name,
            "total_claims": grouped_subquery.c.total_claims,
            "total_drug_cost": grouped_subquery.c.total_drug_cost,
            "total_benes": grouped_subquery.c.total_benes,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)

    result = await session.execute(query)
    items = []
    for row in result:
        item = _row_to_dict(row)
        generic_name = str(item.get("generic_name") or item.get("rx_name") or item.get("brand_name") or "").strip()
        brand_name = str(item.get("brand_name") or item.get("rx_name") or item.get("generic_name") or "").strip()
        item["generic_name"] = generic_name or None
        item["brand_name"] = brand_name or None
        item["prescription_name"] = str(item.get("rx_name") or generic_name or brand_name or "").strip() or None
        item["display_label"] = (
            f"{generic_name} / {brand_name}" if generic_name and brand_name and generic_name != brand_name else (generic_name or brand_name or None)
        )
        item["prescription_code_system"] = item.get("rx_code_system")
        item["prescription_code"] = item.get("rx_code")
        item["total_prescriptions"] = item.get("total_claims")
        item["total_allowed_amount"] = item.get("total_drug_cost")
        items.append(item)

    if items:
        try:
            external_codes_by_internal = await _resolve_external_rx_codes_for_internal(
                session,
                [str(item.get("rx_code") or "") for item in items],
            )
        except Exception:  # pragma: no cover - defensive fallback for missing/migrating crosswalk table
            external_codes_by_internal = {}
        _apply_prescription_code_preferences(items, external_codes_by_internal)

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "q": q,
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "order_by": order_by,
                "order": order,
            },
        }
    )


@blueprint.get("/providers/by-procedure", name="pricing.providers.by_procedure")
@blueprint.get("/providers/by-service", name="pricing.providers.by_service")
@blueprint.get("/physicians/by-service", name="pricing.physicians.by_service")
async def list_providers_by_procedure(request):
    session = _get_session(request)
    args = request.args

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    min_claims = _parse_float(args.get("min_claims"), "min_claims", minimum=0)
    min_total_cost = _parse_float(args.get("min_total_cost"), "min_total_cost", minimum=0)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    zip5 = _normalize_zip5(args.get("zip5"))
    zip_radius_miles = _parse_zip_radius_miles(
        args.get("zip_radius_miles"),
        param="zip_radius_miles",
        default=PROCEDURE_SEARCH_ZIP_RADIUS_DEFAULT_MILES,
    )
    specialty = str(args.get("specialty", "")).strip().lower()
    q = str(args.get("q", "")).strip().lower()
    code = str(args.get("code", "")).strip()
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_allowed_amount")
    include_legacy_fields = _parse_bool(args.get("include_legacy_fields"), "include_legacy_fields", default=False)
    internal_codes: list[int] = []

    if not q and not code:
        raise InvalidUsage("Provide at least one of 'q' or 'code'")
    if order_by == "cost_index":
        if not code:
            raise InvalidUsage("Parameter 'order_by=cost_index' requires 'code'")
        if not (zip5 or (state and city)):
            raise InvalidUsage("Parameter 'order_by=cost_index' requires either 'zip5' or both 'state' and 'city'")

    year, year_source = await _resolve_year(session, provider_procedure_table, year)
    code_context = None
    zip_distance_map: dict[str, dict[str, Any]] = {}
    zip_filter_values: list[str] = []
    if zip5 and zip_radius_miles > 0:
        zip_rows = await _zip_radius_rows(
            session,
            zip5=zip5,
            radius_miles=zip_radius_miles,
            state_hint=state or None,
        )
        for row in sorted(
            zip_rows,
            key=lambda item: (_as_float(item.get("distance_miles")) or 0.0, str(item.get("zip5") or "")),
        ):
            candidate_zip = _normalize_zip5(row.get("zip5"))
            if candidate_zip is None:
                continue
            if candidate_zip in zip_distance_map:
                continue
            distance_value = _as_float(row.get("distance_miles"))
            zip_distance_map[candidate_zip] = {
                "distance_miles": distance_value,
                "distance_bucket": _distance_bucket(distance_value),
            }
            zip_filter_values.append(candidate_zip)
        if zip5 not in zip_distance_map:
            zip_distance_map[zip5] = {"distance_miles": 0.0, "distance_bucket": "zip_exact"}
            zip_filter_values.insert(0, zip5)

    filters = [
        provider_procedure_table.c.year == year,
        provider_table.c.year == year,
        provider_table.c.npi == provider_procedure_table.c.npi,
    ]
    if state:
        filters.append(func.upper(provider_table.c.state) == state)
    if city and not (zip5 and zip_radius_miles > 0):
        filters.append(func.lower(provider_table.c.city).like(f"%{city}%"))
    if zip_filter_values:
        filters.append(provider_table.c.zip5.in_(zip_filter_values))
    elif zip5:
        filters.append(provider_table.c.zip5 == zip5)
    if specialty:
        filters.append(func.lower(provider_table.c.provider_type).like(f"%{specialty}%"))
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(provider_procedure_table.c.service_description).like(q_like),
                func.lower(provider_procedure_table.c.reported_code).like(q_like),
            )
        )
    if code:
        internal_codes, code_context = await _resolve_internal_codes_for_request(
            session,
            code,
            args,
        )
        filters.append(provider_procedure_table.c.procedure_code.in_(internal_codes))
    if min_claims is not None:
        filters.append(provider_procedure_table.c.total_services >= min_claims)
    if min_total_cost is not None:
        filters.append(provider_procedure_table.c.total_allowed_amount >= min_total_cost)
    where_clause = and_(*filters)

    grouped = (
        select(
            provider_procedure_table.c.npi.label("npi"),
            provider_table.c.provider_name.label("provider_name"),
            provider_table.c.provider_type.label("provider_type"),
            provider_table.c.city.label("city"),
            provider_table.c.state.label("state"),
            provider_table.c.zip5.label("zip5"),
            func.sum(provider_procedure_table.c.total_services).label("total_services"),
            func.sum(provider_procedure_table.c.total_submitted_charges).label("total_submitted_charges"),
            func.sum(provider_procedure_table.c.total_allowed_amount).label("total_allowed_amount"),
            func.sum(provider_procedure_table.c.total_beneficiaries).label("total_beneficiaries"),
            func.count(func.distinct(provider_procedure_table.c.procedure_code)).label("matched_service_codes"),
        )
        .select_from(provider_procedure_table.join(provider_table, provider_table.c.npi == provider_procedure_table.c.npi))
        .where(where_clause)
        .group_by(
            provider_procedure_table.c.npi,
            provider_table.c.provider_name,
            provider_table.c.provider_type,
            provider_table.c.city,
            provider_table.c.state,
            provider_table.c.zip5,
        )
    )
    grouped_subquery = grouped.subquery()
    count_result = await session.execute(select(func.count()).select_from(grouped_subquery))
    total = int(count_result.scalar() or 0)

    cost_index_expr = case(
        (
            grouped_subquery.c.total_services > 0,
            cast(grouped_subquery.c.total_allowed_amount, Float) / grouped_subquery.c.total_services,
        ),
        else_=None,
    ).label("cost_index")

    if order_by == "cost_index":
        query = select(grouped_subquery, cost_index_expr)
    else:
        query = select(grouped_subquery)
    query = _apply_ordering(
        query,
        order_by,
        order,
        {
            "npi": grouped_subquery.c.npi,
            "provider_name": grouped_subquery.c.provider_name,
            "total_services": grouped_subquery.c.total_services,
            "total_submitted_charges": grouped_subquery.c.total_submitted_charges,
            "total_allowed_amount": grouped_subquery.c.total_allowed_amount,
            "total_beneficiaries": grouped_subquery.c.total_beneficiaries,
            "matched_service_codes": grouped_subquery.c.matched_service_codes,
            "cost_index": cost_index_expr,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)
    result = await session.execute(query)
    items = [_normalize_provider_service_aggregate(_row_to_dict(row), include_legacy=include_legacy_fields) for row in result]
    if zip5 and items:
        for item in items:
            item_zip = _normalize_zip5(item.get("zip5"))
            if item_zip is None:
                continue
            distance_payload = zip_distance_map.get(item_zip)
            if distance_payload is None:
                if item_zip != zip5:
                    continue
                distance_payload = {"distance_miles": 0.0, "distance_bucket": "zip_exact"}
            item["distance_miles"] = _as_float(distance_payload.get("distance_miles"))
            item["distance_bucket"] = distance_payload.get("distance_bucket")
            item["anchor_zip5"] = zip5
    if code and internal_codes and items:
        await _enrich_provider_service_cost_indices(
            session,
            items,
            year=year,
            internal_codes=internal_codes,
            fallback_state=state or None,
            fallback_city=city or None,
            fallback_zip5=zip5 or None,
        )

    query_payload: dict[str, Any] = {
        "q": q or None,
        "code": code or None,
        "year": year,
        "year_used": year,
        "year_source": year_source,
        "state": state or None,
        "city": city or None,
        "zip5": zip5 or None,
        "zip_radius_miles": zip_radius_miles if zip5 else None,
        "zip_candidate_count": len(zip_filter_values) if zip_filter_values else (1 if zip5 else None),
        "specialty": specialty or None,
        "min_claims": min_claims,
        "min_total_cost": min_total_cost,
        "include_legacy_fields": include_legacy_fields,
        "order_by": order_by,
        "order": order,
    }
    if code_context is not None:
        query_payload.update(
            {
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            }
        )

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": query_payload,
        }
    )


@blueprint.get("/providers/by-prescription", name="pricing.providers.by_prescription")
@blueprint.get("/providers/by-drug", name="pricing.providers.by_drug")
@blueprint.get("/physicians/by-prescription", name="pricing.physicians.by_prescription")
@blueprint.get("/physicians/by-drug", name="pricing.physicians.by_drug")
async def list_providers_by_prescription(request):
    session = _get_session(request)
    args = request.args

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    min_claims = _parse_float(args.get("min_claims"), "min_claims", minimum=0)
    min_total_cost = _parse_float(args.get("min_total_cost"), "min_total_cost", minimum=0)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    zip5 = str(args.get("zip5", "")).strip()
    specialty = str(args.get("specialty", "")).strip().lower()
    q = str(args.get("q", "")).strip().lower()
    code = str(args.get("code", "")).strip()

    if not q and not code:
        raise InvalidUsage("Provide at least one of 'q' or 'code'")

    if not await _table_exists(session, provider_prescription_table.name):
        return response.json(
            {
                "items": [],
                "pagination": {
                    "total": 0,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": pagination.page,
                },
                "query": {
                    "q": q or None,
                    "code": code or None,
                    "year": year,
                    "year_used": year,
                    "year_source": "request" if year is not None else "env",
                    "data_status": "unavailable",
                },
            }
        )

    year, year_source = await _resolve_year(session, provider_prescription_table, year)
    code_context = None

    filters = [
        provider_prescription_table.c.year == year,
        provider_prescription_table.c.rx_code_system == INTERNAL_RX_CODE_SYSTEM,
    ]
    if state:
        filters.append(func.upper(provider_prescription_table.c.state) == state)
    if city:
        filters.append(func.lower(provider_prescription_table.c.city).like(f"%{city}%"))
    if zip5:
        filters.append(provider_prescription_table.c.zip5 == zip5)
    if specialty:
        filters.append(func.lower(provider_prescription_table.c.provider_type).like(f"%{specialty}%"))
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(provider_prescription_table.c.rx_name).like(q_like),
                func.lower(provider_prescription_table.c.generic_name).like(q_like),
                func.lower(provider_prescription_table.c.brand_name).like(q_like),
            )
        )
    if code:
        internal_rx_codes, code_context = await _resolve_internal_rx_codes_for_request(
            session,
            code,
            args,
            default_system=args.get("rx_code_system") or INTERNAL_RX_CODE_SYSTEM,
        )
        filters.append(provider_prescription_table.c.rx_code.in_(internal_rx_codes))
    if min_claims is not None:
        filters.append(provider_prescription_table.c.total_claims >= min_claims)
    if min_total_cost is not None:
        filters.append(provider_prescription_table.c.total_drug_cost >= min_total_cost)
    where_clause = and_(*filters)

    grouped = (
        select(
            provider_prescription_table.c.npi.label("npi"),
            provider_prescription_table.c.provider_name.label("provider_name"),
            provider_prescription_table.c.provider_type.label("provider_type"),
            provider_prescription_table.c.city.label("city"),
            provider_prescription_table.c.state.label("state"),
            provider_prescription_table.c.zip5.label("zip5"),
            func.sum(provider_prescription_table.c.total_claims).label("total_claims"),
            func.sum(provider_prescription_table.c.total_drug_cost).label("total_drug_cost"),
            func.sum(provider_prescription_table.c.total_benes).label("total_benes"),
            func.count(func.distinct(provider_prescription_table.c.rx_code)).label("matched_prescription_codes"),
        )
        .select_from(provider_prescription_table)
        .where(where_clause)
        .group_by(
            provider_prescription_table.c.npi,
            provider_prescription_table.c.provider_name,
            provider_prescription_table.c.provider_type,
            provider_prescription_table.c.city,
            provider_prescription_table.c.state,
            provider_prescription_table.c.zip5,
        )
    )
    grouped_subquery = grouped.subquery()
    count_result = await session.execute(select(func.count()).select_from(grouped_subquery))
    total = int(count_result.scalar() or 0)

    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_drug_cost")
    query = select(grouped_subquery)
    query = _apply_ordering(
        query,
        order_by,
        order,
        {
            "npi": grouped_subquery.c.npi,
            "provider_name": grouped_subquery.c.provider_name,
            "total_claims": grouped_subquery.c.total_claims,
            "total_drug_cost": grouped_subquery.c.total_drug_cost,
            "total_benes": grouped_subquery.c.total_benes,
            "matched_prescription_codes": grouped_subquery.c.matched_prescription_codes,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)
    result = await session.execute(query)
    items = [_normalize_prescription_provider_aggregate(_row_to_dict(row)) for row in result]

    query_payload: dict[str, Any] = {
        "q": q or None,
        "code": code or None,
        "year": year,
        "year_used": year,
        "year_source": year_source,
        "state": state or None,
        "city": city or None,
        "zip5": zip5 or None,
        "specialty": specialty or None,
        "min_claims": min_claims,
        "min_total_cost": min_total_cost,
        "order_by": order_by,
        "order": order,
    }
    if code_context is not None:
        query_payload.update(
            {
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            }
        )

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": query_payload,
        }
    )


@blueprint.get("/providers/<npi>/prescriptions", name="pricing.providers.prescriptions.list")
@blueprint.get("/physicians/<npi>/prescriptions", name="pricing.physicians.prescriptions.list")
async def list_provider_prescriptions(request, npi: str):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    min_claims = _parse_float(args.get("min_claims"), "min_claims", minimum=0)
    min_total_cost = _parse_float(args.get("min_total_cost"), "min_total_cost", minimum=0)
    q = str(args.get("q", "")).strip().lower()
    generic_name = str(args.get("generic_name", "")).strip().lower()
    brand_name = str(args.get("brand_name", "")).strip().lower()
    rx_name = str(args.get("rx_name", "")).strip().lower()
    code = str(args.get("code", "")).strip()

    if not await _table_exists(session, provider_prescription_table.name):
        return response.json(
            {
                "items": [],
                "pagination": {
                    "total": 0,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": pagination.page,
                },
                "query": {
                    "npi": provider_npi,
                    "year": year,
                    "year_used": year,
                    "year_source": "request" if year is not None else "env",
                    "data_status": "unavailable",
                },
            }
        )

    year, year_source = await _resolve_year(session, provider_prescription_table, year)
    code_context = None

    filters = [provider_prescription_table.c.npi == provider_npi, provider_prescription_table.c.year == year]
    if generic_name:
        filters.append(func.lower(provider_prescription_table.c.generic_name).like(f"%{generic_name}%"))
    if brand_name:
        filters.append(func.lower(provider_prescription_table.c.brand_name).like(f"%{brand_name}%"))
    if rx_name:
        filters.append(func.lower(provider_prescription_table.c.rx_name).like(f"%{rx_name}%"))
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(provider_prescription_table.c.rx_name).like(q_like),
                func.lower(provider_prescription_table.c.generic_name).like(q_like),
                func.lower(provider_prescription_table.c.brand_name).like(q_like),
                func.upper(provider_prescription_table.c.rx_code).like(f"%{q.upper()}%"),
            )
        )
    if code:
        internal_rx_codes, code_context = await _resolve_internal_rx_codes_for_request(
            session,
            code,
            args,
            default_system=args.get("rx_code_system") or INTERNAL_RX_CODE_SYSTEM,
        )
        filters.append(provider_prescription_table.c.rx_code_system == INTERNAL_RX_CODE_SYSTEM)
        filters.append(provider_prescription_table.c.rx_code.in_(internal_rx_codes))
    if min_claims is not None:
        filters.append(provider_prescription_table.c.total_claims >= min_claims)
    if min_total_cost is not None:
        filters.append(provider_prescription_table.c.total_drug_cost >= min_total_cost)

    where_clause = and_(*filters)
    count_result = await session.execute(select(func.count()).select_from(provider_prescription_table).where(where_clause))
    total = int(count_result.scalar() or 0)

    query = select(provider_prescription_table).where(where_clause)
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_drug_cost")
    query = _apply_ordering(
        query,
        order_by,
        order,
        {
            "rx_code": provider_prescription_table.c.rx_code,
            "rx_name": provider_prescription_table.c.rx_name,
            "generic_name": provider_prescription_table.c.generic_name,
            "brand_name": provider_prescription_table.c.brand_name,
            "total_claims": provider_prescription_table.c.total_claims,
            "total_drug_cost": provider_prescription_table.c.total_drug_cost,
            "total_benes": provider_prescription_table.c.total_benes,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)

    result = await session.execute(query)
    items = [_normalize_prescription_payload(_row_to_dict(row)) for row in result]
    if items:
        try:
            external_codes_by_internal = await _resolve_external_rx_codes_for_internal(
                session,
                [str(item.get("rx_code") or "") for item in items],
            )
        except Exception:  # pragma: no cover - defensive fallback for missing/migrating crosswalk table
            external_codes_by_internal = {}
        _apply_prescription_code_preferences(items, external_codes_by_internal)

    query_payload: dict[str, Any] = {
        "npi": provider_npi,
        "year": year,
        "year_used": year,
        "year_source": year_source,
        "rx_name": rx_name or None,
        "generic_name": generic_name or None,
        "brand_name": brand_name or None,
        "q": q or None,
        "code": code or None,
        "min_claims": min_claims,
        "min_total_cost": min_total_cost,
        "order_by": order_by,
        "order": order,
    }
    if code_context is not None:
        query_payload.update(
            {
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            }
        )

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": query_payload,
        }
    )


async def _provider_prescription_detail(
    request,
    npi: str,
    rx_code_system: str,
    rx_code: str,
):
    session = _get_session(request)
    args = request.args

    provider_npi = _parse_int(npi, "npi", minimum=1)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    if provider_npi is None:
        raise InvalidUsage("Path parameter 'npi' must be provided")
    if not await _table_exists(session, provider_prescription_table.name):
        raise sanic.exceptions.NotFound("Prescription data is not available; run drug-claims import.")

    year, year_source = await _resolve_year(session, provider_prescription_table, year)
    internal_rx_codes, code_context = await _resolve_internal_rx_codes_for_request(
        session,
        rx_code,
        dict({"rx_code_system": rx_code_system}, **dict(args)),
        default_system=rx_code_system,
    )

    query = (
        select(provider_prescription_table)
        .where(
            and_(
                provider_prescription_table.c.npi == provider_npi,
                provider_prescription_table.c.rx_code_system == INTERNAL_RX_CODE_SYSTEM,
                provider_prescription_table.c.rx_code.in_(internal_rx_codes),
                provider_prescription_table.c.year == year,
            )
        )
        .order_by(provider_prescription_table.c.total_claims.desc().nullslast())
        .limit(1)
    )

    result = await session.execute(query)
    row = result.first()
    if row is None:
        raise sanic.exceptions.NotFound("Provider prescription not found")

    payload = _normalize_prescription_payload(_row_to_dict(row))
    try:
        external_codes_by_internal = await _resolve_external_rx_codes_for_internal(
            session,
            [str(payload.get("rx_code") or "")],
        )
    except Exception:  # pragma: no cover - defensive fallback for missing/migrating crosswalk table
        external_codes_by_internal = {}
    _apply_prescription_code_preferences([payload], external_codes_by_internal)
    payload["year_used"] = year
    payload["year_source"] = year_source
    payload["input_code"] = code_context["input_code"]
    payload["resolved_codes"] = code_context["resolved_codes"]
    payload["matched_via"] = code_context["matched_via"]
    return response.json(payload)


@blueprint.get(
    "/providers/<npi>/prescriptions/<rx_code_system>/<rx_code>",
    name="pricing.providers.prescriptions.get",
)
async def get_provider_prescription(request, npi: str, rx_code_system: str, rx_code: str):
    return await _provider_prescription_detail(request, npi, rx_code_system, rx_code)


@blueprint.get(
    "/physicians/<npi>/prescriptions/<rx_code_system>/<rx_code>",
    name="pricing.physicians.prescriptions.get",
)
async def get_physician_prescription(request, npi: str, rx_code_system: str, rx_code: str):
    return await _provider_prescription_detail(request, npi, rx_code_system, rx_code)


@blueprint.get(
    "/prescriptions/<rx_code_system>/<rx_code>/providers",
    name="pricing.prescriptions.providers.list",
)
async def list_prescription_providers(request, rx_code_system: str, rx_code: str):
    session = _get_session(request)
    args = request.args

    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)
    year = _parse_int(args.get("year"), "year", minimum=2013)
    min_claims = _parse_float(args.get("min_claims"), "min_claims", minimum=0)
    min_total_cost = _parse_float(args.get("min_total_cost"), "min_total_cost", minimum=0)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()
    specialty = str(args.get("specialty", "")).strip().lower()
    q = str(args.get("q", "")).strip().lower()

    if not await _table_exists(session, provider_prescription_table.name):
        return response.json(
            {
                "items": [],
                "pagination": {
                    "total": 0,
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                    "page": pagination.page,
                },
                "query": {
                    "year": year,
                    "year_used": year,
                    "year_source": "request" if year is not None else "env",
                    "data_status": "unavailable",
                    "input_code": {
                        "code_system": _normalize_code_system(rx_code_system),
                        "code": str(rx_code).strip().upper(),
                    },
                    "resolved_codes": [],
                    "matched_via": [],
                },
            }
        )

    year, year_source = await _resolve_year(session, provider_prescription_table, year)
    internal_rx_codes, code_context = await _resolve_internal_rx_codes_for_request(
        session,
        rx_code,
        dict({"rx_code_system": rx_code_system}, **dict(args)),
        default_system=rx_code_system,
    )

    filters = [
        provider_prescription_table.c.rx_code_system == INTERNAL_RX_CODE_SYSTEM,
        provider_prescription_table.c.rx_code.in_(internal_rx_codes),
        provider_prescription_table.c.year == year,
    ]
    if state:
        filters.append(func.upper(provider_prescription_table.c.state) == state)
    if city:
        filters.append(func.lower(provider_prescription_table.c.city).like(f"%{city}%"))
    if specialty:
        filters.append(func.lower(provider_prescription_table.c.provider_type).like(f"%{specialty}%"))
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(provider_prescription_table.c.provider_name).like(q_like),
                func.lower(provider_prescription_table.c.provider_type).like(q_like),
                cast(provider_prescription_table.c.npi, String).like(f"%{q}%"),
            )
        )
    if min_claims is not None:
        filters.append(provider_prescription_table.c.total_claims >= min_claims)
    if min_total_cost is not None:
        filters.append(provider_prescription_table.c.total_drug_cost >= min_total_cost)

    where_clause = and_(*filters)
    grouped = (
        select(
            provider_prescription_table.c.npi.label("npi"),
            provider_prescription_table.c.provider_name.label("provider_name"),
            provider_prescription_table.c.provider_type.label("provider_type"),
            provider_prescription_table.c.city.label("city"),
            provider_prescription_table.c.state.label("state"),
            provider_prescription_table.c.zip5.label("zip5"),
            func.sum(provider_prescription_table.c.total_claims).label("total_claims"),
            func.sum(provider_prescription_table.c.total_drug_cost).label("total_drug_cost"),
            func.sum(provider_prescription_table.c.total_benes).label("total_benes"),
            func.count().label("matched_rows"),
        )
        .select_from(provider_prescription_table)
        .where(where_clause)
        .group_by(
            provider_prescription_table.c.npi,
            provider_prescription_table.c.provider_name,
            provider_prescription_table.c.provider_type,
            provider_prescription_table.c.city,
            provider_prescription_table.c.state,
            provider_prescription_table.c.zip5,
        )
    )
    grouped_subquery = grouped.subquery()
    count_result = await session.execute(select(func.count()).select_from(grouped_subquery))
    total = int(count_result.scalar() or 0)

    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_drug_cost")
    query = select(grouped_subquery)
    query = _apply_ordering(
        query,
        order_by,
        order,
        {
            "npi": grouped_subquery.c.npi,
            "provider_name": grouped_subquery.c.provider_name,
            "total_claims": grouped_subquery.c.total_claims,
            "total_drug_cost": grouped_subquery.c.total_drug_cost,
            "total_benes": grouped_subquery.c.total_benes,
            "matched_rows": grouped_subquery.c.matched_rows,
        },
    )
    query = query.limit(pagination.limit).offset(pagination.offset)
    result = await session.execute(query)
    items = [_normalize_prescription_provider_aggregate(_row_to_dict(row)) for row in result]

    return response.json(
        {
            "items": items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "state": state or None,
                "city": city or None,
                "specialty": specialty or None,
                "q": q or None,
                "min_claims": min_claims,
                "min_total_cost": min_total_cost,
                "order_by": order_by,
                "order": order,
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            },
        }
    )


@blueprint.get(
    "/prescriptions/<rx_code_system>/<rx_code>/benchmarks",
    name="pricing.prescriptions.benchmarks.get",
)
async def get_prescription_benchmarks(request, rx_code_system: str, rx_code: str):
    session = _get_session(request)
    args = request.args

    year = _parse_int(args.get("year"), "year", minimum=2013)
    state = str(args.get("state", "")).strip().upper()
    city = str(args.get("city", "")).strip().lower()

    if not await _table_exists(session, provider_prescription_table.name):
        return response.json(
            {
                "query": {
                    "year": year,
                    "year_used": year,
                    "year_source": "request" if year is not None else "env",
                    "state": state or None,
                    "city": city or None,
                    "data_status": "unavailable",
                    "input_code": {
                        "code_system": _normalize_code_system(rx_code_system),
                        "code": str(rx_code).strip().upper(),
                    },
                    "resolved_codes": [],
                    "matched_via": [],
                },
                "benchmark": {
                    "matched_rows": 0,
                    "provider_count": 0,
                    "total_prescriptions": 0.0,
                    "total_allowed_amount": 0.0,
                    "avg_total_allowed_amount": 0.0,
                    "min_total_allowed_amount": 0.0,
                    "max_total_allowed_amount": 0.0,
                    "estimated_cost_level_thresholds": {},
                },
            }
        )

    year, year_source = await _resolve_year(session, provider_prescription_table, year)
    internal_rx_codes, code_context = await _resolve_internal_rx_codes_for_request(
        session,
        rx_code,
        dict({"rx_code_system": rx_code_system}, **dict(args)),
        default_system=rx_code_system,
    )

    filters = [
        provider_prescription_table.c.rx_code_system == INTERNAL_RX_CODE_SYSTEM,
        provider_prescription_table.c.rx_code.in_(internal_rx_codes),
        provider_prescription_table.c.year == year,
    ]
    if state:
        filters.append(func.upper(provider_prescription_table.c.state) == state)
    if city:
        filters.append(func.lower(provider_prescription_table.c.city).like(f"%{city}%"))
    where_clause = and_(*filters)

    aggregate_query = (
        select(
            func.count().label("matched_rows"),
            func.count(func.distinct(provider_prescription_table.c.npi)).label("provider_count"),
            func.sum(provider_prescription_table.c.total_claims).label("total_claims"),
            func.sum(provider_prescription_table.c.total_drug_cost).label("total_drug_cost"),
            func.avg(provider_prescription_table.c.total_drug_cost).label("avg_total_drug_cost"),
            func.min(provider_prescription_table.c.total_drug_cost).label("min_total_drug_cost"),
            func.max(provider_prescription_table.c.total_drug_cost).label("max_total_drug_cost"),
        )
        .select_from(provider_prescription_table)
        .where(where_clause)
    )
    aggregate_result = await session.execute(aggregate_query)
    aggregate = _row_to_dict(aggregate_result.first() or {})

    provider_costs_subquery = (
        select(
            provider_prescription_table.c.npi.label("npi"),
            func.sum(provider_prescription_table.c.total_drug_cost).label("provider_total_drug_cost"),
        )
        .select_from(provider_prescription_table)
        .where(where_clause)
        .group_by(provider_prescription_table.c.npi)
    ).subquery()

    threshold_query = select(
        func.percentile_cont(0.20).within_group(provider_costs_subquery.c.provider_total_drug_cost).label("p20"),
        func.percentile_cont(0.40).within_group(provider_costs_subquery.c.provider_total_drug_cost).label("p40"),
        func.percentile_cont(0.60).within_group(provider_costs_subquery.c.provider_total_drug_cost).label("p60"),
        func.percentile_cont(0.80).within_group(provider_costs_subquery.c.provider_total_drug_cost).label("p80"),
    )
    threshold_result = await session.execute(threshold_query)
    thresholds = _row_to_dict(threshold_result.first() or {})

    return response.json(
        {
            "query": {
                "year": year,
                "year_used": year,
                "year_source": year_source,
                "state": state or None,
                "city": city or None,
                "input_code": code_context["input_code"],
                "resolved_codes": code_context["resolved_codes"],
                "matched_via": code_context["matched_via"],
            },
            "benchmark": {
                "matched_rows": int(aggregate.get("matched_rows") or 0),
                "provider_count": int(aggregate.get("provider_count") or 0),
                "total_prescriptions": float(aggregate.get("total_claims") or 0.0),
                "total_allowed_amount": float(aggregate.get("total_drug_cost") or 0.0),
                "avg_total_allowed_amount": float(aggregate.get("avg_total_drug_cost") or 0.0),
                "min_total_allowed_amount": float(aggregate.get("min_total_drug_cost") or 0.0),
                "max_total_allowed_amount": float(aggregate.get("max_total_drug_cost") or 0.0),
                "estimated_cost_level_thresholds": {
                    "$": {"max_total_allowed_amount": thresholds.get("p20")},
                    "$$": {"max_total_allowed_amount": thresholds.get("p40")},
                    "$$$": {"max_total_allowed_amount": thresholds.get("p60")},
                    "$$$$": {"max_total_allowed_amount": thresholds.get("p80")},
                    "$$$$$": {"min_total_allowed_amount": thresholds.get("p80")},
                },
            },
        }
    )
