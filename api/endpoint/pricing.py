# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
import re
from typing import Any

import sanic.exceptions
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import Float, String, and_, case, cast, func, or_, select

from api.endpoint.pagination import parse_pagination
from db.models import (CodeCatalog, CodeCrosswalk, PricingProcedure,
                       PricingProcedureGeoBenchmark,
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


MAX_LIMIT = 200
INTERNAL_CODE_SYSTEM = "HP_PROCEDURE_CODE"
INTERNAL_RX_CODE_SYSTEM = "HP_RX_CODE"
RX_EXTERNAL_CODE_PRIORITY = ("NDC", "RXNORM")
MAX_CODE_EXPANSION_HOPS = max(int(os.getenv("HLTHPRT_MAX_CODE_EXPANSION_HOPS", "4")), 1)
INT_PATTERN = re.compile(r"^-?\d+$")
FIVE_DIGIT_CODE_PATTERN = re.compile(r"^\d{5}$")


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
PROCEDURE_COST_PROFILE_MIN_CLAIMS = max(int(os.getenv("HLTHPRT_COST_LEVEL_CONFIDENCE_LOW_LT", "11")), 1)
PROCEDURE_COST_PROFILE_MEDIUM_CLAIMS = max(
    int(os.getenv("HLTHPRT_COST_LEVEL_CONFIDENCE_MEDIUM_LT", "51")),
    PROCEDURE_COST_PROFILE_MIN_CLAIMS + 1,
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


def _parse_setting_key(raw: Any) -> str:
    value = str(raw or "all").strip().lower()
    return value or "all"


def _parse_specialty_key(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    return text or None


def _geography_candidates(
    *,
    state_raw: Any,
    city_raw: Any,
    zip5_raw: Any,
) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    state = str(state_raw or "").strip().upper()
    city = str(city_raw or "").strip().lower()
    zip5 = str(zip5_raw or "").strip()

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
    result = await session.execute(select(func.to_regclass(f"{PRICING_SCHEMA}.{table_name}")))
    return bool(result.scalar())


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

    query = select(provider_table).where(where_clause)
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "total_allowed_amount")
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
    provider_data = _row_to_dict(provider_row)

    peer_filters = [provider_table.c.year == year]
    state = str(provider_data.get("state") or "").strip().upper()
    if state:
        peer_filters.append(func.upper(provider_table.c.state) == state)
    peer_clause = and_(*peer_filters)

    peer_count_result = await session.execute(
        select(func.count()).select_from(provider_table).where(peer_clause)
    )
    peer_count = int(peer_count_result.scalar() or 0)
    if peer_count == 0:
        raise sanic.exceptions.NotFound("No peer group available for scoring")

    total_allowed_amount = float(provider_data.get("total_allowed_amount") or 0.0)
    total_services = float(provider_data.get("total_services") or 0.0)
    total_beneficiaries = float(provider_data.get("total_beneficiaries") or 0.0)

    cost_rank_result = await session.execute(
        select(func.count()).select_from(provider_table).where(
            and_(peer_clause, provider_table.c.total_allowed_amount <= total_allowed_amount)
        )
    )
    volume_rank_result = await session.execute(
        select(func.count()).select_from(provider_table).where(
            and_(peer_clause, provider_table.c.total_services <= total_services)
        )
    )
    bene_rank_result = await session.execute(
        select(func.count()).select_from(provider_table).where(
            and_(peer_clause, provider_table.c.total_beneficiaries <= total_beneficiaries)
        )
    )

    cost_rank = min(max(float(cost_rank_result.scalar() or 0) / peer_count, 0.0), 1.0)
    volume_rank = min(max(float(volume_rank_result.scalar() or 0) / peer_count, 0.0), 1.0)
    bene_rank = min(max(float(bene_rank_result.scalar() or 0) / peer_count, 0.0), 1.0)

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

    service_count = int(service_count_result.scalar() or 0)
    location_count = int(location_count_result.scalar() or 0)
    consistency_score = min(service_count / 20.0, 1.0)
    efficiency_score = min(max(1.0 - cost_rank, 0.0), 1.0)
    access_score = min(location_count / 5.0, 1.0)
    data_confidence_score = min(total_services / 1000.0, 1.0)

    quality_proxy_score = round(
        100.0
        * (
            0.30 * volume_rank
            + 0.20 * consistency_score
            + 0.20 * efficiency_score
            + 0.15 * access_score
            + 0.15 * data_confidence_score
        ),
        2,
    )

    return response.json(
        {
            "npi": provider_npi,
            "year_used": year,
            "year_source": year_source,
            "estimated_cost_level": _cost_level_from_percentile(cost_rank),
            "quality_proxy_score": quality_proxy_score,
            "confidence": "medium",
            "components": {
                "volume_score": round(volume_rank, 4),
                "consistency_score": round(consistency_score, 4),
                "efficiency_score": round(efficiency_score, 4),
                "access_score": round(access_score, 4),
                "data_confidence_score": round(data_confidence_score, 4),
            },
            "peer_group": {
                "year": year,
                "state": state or None,
                "peer_count": peer_count,
                "cost_percentile": round(cost_rank, 4),
                "volume_percentile": round(volume_rank, 4),
                "bene_percentile": round(bene_rank, 4),
            },
        }
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
    zip5 = str(args.get("zip5", "")).strip()
    specialty_key = _parse_specialty_key(args.get("specialty"))
    setting_key = _parse_setting_key(args.get("setting"))

    year, year_source = await _resolve_year(session, provider_procedure_cost_profile_table, year)
    internal_codes, code_context = await _resolve_internal_codes_for_request(
        session,
        code_value,
        args,
        default_system=default_code_system,
    )
    geography_candidates = _geography_candidates(state_raw=state, city_raw=city, zip5_raw=zip5)

    selected_profile: dict[str, Any] | None = None
    selected_peer: dict[str, Any] | None = None
    selected_scope: str | None = None
    selected_value: str | None = None
    selected_specialty: str | None = None
    profile_found_without_peer = False

    for geography_scope, geography_value in geography_candidates:
        profile_filters = [
            provider_procedure_cost_profile_table.c.npi == provider_npi,
            provider_procedure_cost_profile_table.c.year == year,
            provider_procedure_cost_profile_table.c.procedure_code.in_(internal_codes),
            provider_procedure_cost_profile_table.c.geography_scope == geography_scope,
            provider_procedure_cost_profile_table.c.geography_value == geography_value,
            provider_procedure_cost_profile_table.c.setting_key == setting_key,
        ]
        if specialty_key:
            profile_filters.append(provider_procedure_cost_profile_table.c.specialty_key == specialty_key)

        profile_result = await session.execute(
            select(provider_procedure_cost_profile_table)
            .where(and_(*profile_filters))
            .order_by(
                provider_procedure_cost_profile_table.c.claim_count.desc().nullslast(),
                provider_procedure_cost_profile_table.c.avg_submitted_charge.desc().nullslast(),
            )
            .limit(1)
        )
        profile_row = profile_result.first()
        if profile_row is None:
            continue

        profile_found_without_peer = True
        profile_payload = _row_to_dict(profile_row)
        provider_specialty = str(profile_payload.get("specialty_key") or "").strip().lower()

        specialty_candidates: list[str] = []
        if specialty_key:
            specialty_candidates.append(specialty_key)
        elif provider_specialty:
            specialty_candidates.append(provider_specialty)
        specialty_candidates.append("__all__")
        specialty_candidates = list(dict.fromkeys(specialty_candidates))

        for peer_specialty in specialty_candidates:
            peer_result = await session.execute(
                select(procedure_peer_stats_table)
                .where(
                    and_(
                        procedure_peer_stats_table.c.procedure_code == profile_payload["procedure_code"],
                        procedure_peer_stats_table.c.year == profile_payload["year"],
                        procedure_peer_stats_table.c.geography_scope == geography_scope,
                        procedure_peer_stats_table.c.geography_value == geography_value,
                        procedure_peer_stats_table.c.specialty_key == peer_specialty,
                        procedure_peer_stats_table.c.setting_key == setting_key,
                    )
                )
                .limit(1)
            )
            peer_row = peer_result.first()
            if peer_row is None:
                continue
            selected_profile = profile_payload
            selected_peer = _row_to_dict(peer_row)
            selected_scope = geography_scope
            selected_value = geography_value
            selected_specialty = peer_specialty
            break

        if selected_profile is not None:
            break

    if selected_profile is None:
        if profile_found_without_peer:
            raise sanic.exceptions.NotFound("Peer group is not available for this provider procedure in the requested region.")
        raise sanic.exceptions.NotFound("Provider procedure cost profile not found.")

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
    procedure_internal_code = int(selected_profile["procedure_code"])
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
                "setting_key": setting_key,
                "min_claim_count": _as_float(selected_peer.get("min_claim_count")),
                "max_claim_count": _as_float(selected_peer.get("max_claim_count")),
            },
            "query": {
                "state": state or None,
                "city": city or None,
                "zip5": zip5 or None,
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
    zip5 = str(args.get("zip5", "")).strip()
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

    filters = [
        provider_procedure_table.c.year == year,
        provider_table.c.year == year,
        provider_table.c.npi == provider_procedure_table.c.npi,
    ]
    if state:
        filters.append(func.upper(provider_table.c.state) == state)
    if city:
        filters.append(func.lower(provider_table.c.city).like(f"%{city}%"))
    if zip5:
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
