# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Sequence

from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy import text

from api.endpoint.pagination import parse_pagination
from db.models import (EntityAddressUnified, NPIDataOtherIdentifier,
                       NPIPhoneStaffing, PartDPharmacyActivity,
                       PharmacyLicenseRecord)

blueprint = Blueprint("reports", url_prefix="/reports", version=1)
logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 200
_ZIP_PATTERN = re.compile(r"^\d{5}$")
_MARKET_ID_PATTERN = re.compile(r"^(state|city|county|zip):[a-zA-Z0-9:-]+$")
_SLUG_RE = re.compile(r"[^a-z0-9]+")
_CHAIN_NAME_TEMPLATE = (
    "LOWER("
    "COALESCE({alias}provider_first_name,'') || ' ' || "
    "COALESCE({alias}provider_last_name,'') || ' ' || "
    "COALESCE({alias}provider_organization_name,'') || ' ' || "
    "COALESCE({alias}provider_other_organization_name,'') || ' ' || "
    "COALESCE({alias}do_business_as_text,'')"
    ")"
)

_ALLOWED_SCOPES = {"state", "city", "county", "zip"}
_ALLOWED_ORDERS = {"asc", "desc"}
_ALLOWED_SORTS = {
    "pharmacy_count",
    "pharmacies_per_100k",
    "access_score",
    "active_medicare_share",
    "license_coverage_share",
}

_TABLE_EXISTS_CACHE_TTL_SECONDS = 300.0
_TABLE_EXISTS_CACHE: dict[str, tuple[float, bool]] = {}
ADDRESS_SERVING_SOURCE_LEGACY = "legacy"
ADDRESS_SERVING_SOURCE_UNIFIED = "entity_address_unified"

_CHAIN_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("CVS", ("cvs", "caremark", "omnicare", "longs drug")),
    ("Walgreens", ("walgreen", "duane reade", "boots alliance")),
    ("Walmart", ("wal-mart", "walmart", "sams club", "sam's club")),
    ("Rite Aid", ("rite aid",)),
    ("Costco", ("costco",)),
    ("Kroger", ("kroger",)),
    ("Albertsons", ("albertsons", "safeway", "vons", "jewel osco", "acme markets", "shaw's")),
    ("Publix", ("publix",)),
    ("H-E-B", ("h-e-b", "heb")),
    ("Giant Eagle", ("giant eagle",)),
]

_US_STATE_DIMENSION: list[tuple[str, str]] = [
    ("AL", "Alabama"),
    ("AK", "Alaska"),
    ("AZ", "Arizona"),
    ("AR", "Arkansas"),
    ("CA", "California"),
    ("CO", "Colorado"),
    ("CT", "Connecticut"),
    ("DE", "Delaware"),
    ("DC", "District of Columbia"),
    ("FL", "Florida"),
    ("GA", "Georgia"),
    ("HI", "Hawaii"),
    ("ID", "Idaho"),
    ("IL", "Illinois"),
    ("IN", "Indiana"),
    ("IA", "Iowa"),
    ("KS", "Kansas"),
    ("KY", "Kentucky"),
    ("LA", "Louisiana"),
    ("ME", "Maine"),
    ("MD", "Maryland"),
    ("MA", "Massachusetts"),
    ("MI", "Michigan"),
    ("MN", "Minnesota"),
    ("MS", "Mississippi"),
    ("MO", "Missouri"),
    ("MT", "Montana"),
    ("NE", "Nebraska"),
    ("NV", "Nevada"),
    ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),
    ("NM", "New Mexico"),
    ("NY", "New York"),
    ("NC", "North Carolina"),
    ("ND", "North Dakota"),
    ("OH", "Ohio"),
    ("OK", "Oklahoma"),
    ("OR", "Oregon"),
    ("PA", "Pennsylvania"),
    ("RI", "Rhode Island"),
    ("SC", "South Carolina"),
    ("SD", "South Dakota"),
    ("TN", "Tennessee"),
    ("TX", "Texas"),
    ("UT", "Utah"),
    ("VT", "Vermont"),
    ("VA", "Virginia"),
    ("WA", "Washington"),
    ("WV", "West Virginia"),
    ("WI", "Wisconsin"),
    ("WY", "Wyoming"),
]
_US_STATE_CODES = {code for code, _ in _US_STATE_DIMENSION}
_US_STATE_NORMALIZATION = {
    **{code: code for code, _ in _US_STATE_DIMENSION},
    **{name.upper(): code for code, name in _US_STATE_DIMENSION},
    "D.C.": "DC",
    "WASHINGTON DC": "DC",
}


@dataclass(frozen=True, slots=True)
class _MarketSummaryQuery:
    scope: str
    sort: str
    order: str
    as_of: datetime.date
    include_staffing: bool
    limit: int
    offset: int
    state: str | None = None
    city: str | None = None
    county: str | None = None
    zip_code: str | None = None
    chain: str | None = None
    market_id: str | None = None


_MARKET_INTEGER_METRICS = (
    ("pharmacy_count", "pharmacy_count"),
    ("active_medicare_pharmacy_count", "active_medicare_pharmacy_count"),
    ("chain_count", "chain_count"),
    ("independent_count", "independent_count"),
    ("mail_order_count", "mail_order_count"),
    ("retail_count", "retail_count"),
    ("license_coverage_count", "license_coverage_count"),
    ("disciplinary_flag_count", "disciplinary_flag_count"),
    ("ncpdp_registered_count", "ncpdp_registered_count"),
    ("medicaid_identifier_count", "medicaid_identifier_count"),
    ("railroad_medicare_identifier_count", "railroad_medicare_identifier_count"),
    ("ptan_identifier_count", "ptan_identifier_count"),
    ("clia_identifier_count", "clia_identifier_count"),
    ("medicare_identifier_count", "medicare_identifier_count"),
    ("medicare_license_count", "medicare_identifier_count"),
    ("other_identifier_npi_count", "other_identifier_npi_count"),
    ("population", "population"),
)
_MARKET_FLOAT_METRICS = (
    ("pharmacies_per_100k", "pharmacies_per_100k"),
    ("active_medicare_share", "active_medicare_share"),
    ("license_coverage_share", "license_coverage_share"),
    ("mail_order_share", "mail_order_share"),
    ("ncpdp_registered_share", "ncpdp_registered_share"),
    ("medicaid_identifier_share", "medicaid_identifier_share"),
    ("railroad_medicare_identifier_share", "railroad_medicare_identifier_share"),
    ("ptan_identifier_share", "ptan_identifier_share"),
    ("clia_identifier_share", "clia_identifier_share"),
    ("medicare_identifier_share", "medicare_identifier_share"),
    ("medicare_license_share", "medicare_identifier_share"),
    ("other_identifier_share", "other_identifier_share"),
    ("chain_concentration", "chain_concentration"),
    ("access_score", "access_score"),
)


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _qualified_table_name(table) -> str:
    schema = table.schema or "mrf"
    return f"{schema}.{table.name}"


def _address_serving_source() -> str:
    return str(
        os.getenv("HLTHPRT_ADDRESS_SERVING_SOURCE") or ADDRESS_SERVING_SOURCE_UNIFIED
    ).strip().lower()


def _pharmacy_address_table_sql(source: str | None = None) -> str:
    if (source or _address_serving_source()) == ADDRESS_SERVING_SOURCE_UNIFIED:
        return "mrf.entity_address_unified"
    return "mrf.npi_address"


def _is_pharmacy_address_unified(address_table_sql: str) -> bool:
    return address_table_sql.endswith(".entity_address_unified")


def _pharmacy_address_zip5_expr(alias: str, address_table_sql: str) -> str:
    if _is_pharmacy_address_unified(address_table_sql):
        return f"{alias}.zip5"
    return f"LEFT(COALESCE({alias}.postal_code, ''), 5)"


def _pharmacy_address_order_expr(alias: str, address_table_sql: str) -> str:
    if _is_pharmacy_address_unified(address_table_sql):
        return (
            f"{alias}.npi, ({alias}.telephone_number IS NOT NULL) DESC, "
            f"({alias}.address_precision = 'street') DESC, {alias}.source_count DESC NULLS LAST, "
            f"{alias}.checksum ASC"
        )
    return f"{alias}.npi, ({alias}.telephone_number IS NOT NULL) DESC, {alias}.checksum ASC"


async def _is_table_available(session, table) -> bool:
    qualified = _qualified_table_name(table)
    now = time.monotonic()
    cached = _TABLE_EXISTS_CACHE.get(qualified)
    if cached and (now - cached[0]) <= _TABLE_EXISTS_CACHE_TTL_SECONDS:
        return cached[1]
    result = await session.execute(text("SELECT to_regclass(:name)"), {"name": qualified})
    exists = bool(result.scalar())
    if exists:
        _TABLE_EXISTS_CACHE[qualified] = (now, True)
    else:
        _TABLE_EXISTS_CACHE.pop(qualified, None)
    return exists


async def _resolve_pharmacy_address_table_sql(session) -> str:
    source = _address_serving_source()
    if source != ADDRESS_SERVING_SOURCE_UNIFIED:
        return _pharmacy_address_table_sql(ADDRESS_SERVING_SOURCE_LEGACY)
    if await _is_table_available(session, EntityAddressUnified.__table__):
        return _pharmacy_address_table_sql(ADDRESS_SERVING_SOURCE_UNIFIED)
    return _pharmacy_address_table_sql(ADDRESS_SERVING_SOURCE_LEGACY)


def _slugify(value: str | None) -> str:
    text_value = str(value or "").strip().lower()
    if not text_value:
        return "unknown"
    slug = _SLUG_RE.sub("-", text_value).strip("-")
    return slug or "unknown"


def _parse_npi(npi: str) -> int:
    try:
        parsed = int(str(npi).strip())
    except ValueError as exc:
        raise InvalidUsage("NPI must be numeric") from exc
    if parsed <= 0:
        raise InvalidUsage("NPI must be positive")
    return parsed


def _parse_date_param(value: str | None, param_name: str) -> datetime.date | None:
    if value in (None, "", "null"):
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    try:
        return datetime.date.fromisoformat(text_value)
    except ValueError as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be a valid ISO date (YYYY-MM-DD)") from exc


def _parse_scope(value: str | None, *, default: str) -> str:
    if value in (None, "", "null"):
        return default
    scope = str(value).strip().lower()
    if scope not in _ALLOWED_SCOPES:
        raise InvalidUsage("scope must be one of: state, city, county, zip")
    return scope


def _parse_sort(value: str | None, *, default: str) -> str:
    if value in (None, "", "null"):
        return default
    sort = str(value).strip().lower()
    if sort not in _ALLOWED_SORTS:
        allowed = ", ".join(sorted(_ALLOWED_SORTS))
        raise InvalidUsage(f"sort must be one of: {allowed}")
    return sort


def _parse_order(value: str | None, *, default: str) -> str:
    if value in (None, "", "null"):
        return default
    order = str(value).strip().lower()
    if order not in _ALLOWED_ORDERS:
        raise InvalidUsage("order must be one of: asc, desc")
    return order


def _is_boolean_parameter_enabled(value: str | None, *, default: bool = False) -> bool:
    if value in (None, "", "null"):
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _parse_optional_zip(value: str | None) -> str | None:
    if value in (None, "", "null"):
        return None
    zip_code = str(value).strip()
    if not _ZIP_PATTERN.fullmatch(zip_code):
        raise InvalidUsage("zip must be a 5-digit ZIP code")
    return zip_code


def _canonical_chain(value: str | None) -> str | None:
    if value in (None, "", "null"):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered in {"independent", "indy", "ind"}:
        return "independent"
    for canonical, aliases in _CHAIN_RULES:
        if lowered == canonical.lower() or lowered in aliases:
            return canonical
    for canonical, aliases in _CHAIN_RULES:
        if any(alias in lowered for alias in aliases):
            return canonical
    return raw


def _chain_case_sql(name_expr: str) -> str:
    lines = ["CASE"]
    for canonical, aliases in _CHAIN_RULES:
        like_clauses = []
        for alias in aliases:
            escaped_alias = alias.replace("'", "''")
            like_clauses.append(f"{name_expr} LIKE '%{escaped_alias}%'")
        match_clause = " OR ".join(like_clauses)
        escaped_canonical = canonical.replace("'", "''")
        lines.append(f"    WHEN {match_clause} THEN '{escaped_canonical}'")
    lines.append("    ELSE NULL")
    lines.append("END")
    return "\n".join(lines)


def _scope_sql(scope: str) -> dict[str, str]:
    if scope == "state":
        return {
            "market_id": "CONCAT('state:', UPPER(COALESCE(s.state_name, 'NA')))",
            "market_name": "COALESCE(s.state_name, 'NA')",
            "market_state": "UPPER(COALESCE(s.state_name, 'NA'))",
            "market_city": "NULL::text",
            "market_county": "NULL::text",
            "market_zip": "NULL::text",
        }
    if scope == "county":
        return {
            "market_id": (
                "CONCAT('county:', UPPER(COALESCE(s.state_name, 'NA')), ':', "
                "TRIM(BOTH '-' FROM REGEXP_REPLACE(LOWER(COALESCE(s.county_name, 'unknown')), '[^a-z0-9]+', '-', 'g')))"
            ),
            "market_name": "COALESCE(s.county_name, 'Unknown County')",
            "market_state": "UPPER(COALESCE(s.state_name, 'NA'))",
            "market_city": "NULL::text",
            "market_county": "COALESCE(s.county_name, 'Unknown County')",
            "market_zip": "NULL::text",
        }
    if scope == "zip":
        return {
            "market_id": "CONCAT('zip:', COALESCE(s.zip5, '00000'))",
            "market_name": "COALESCE(s.zip5, '00000')",
            "market_state": "UPPER(COALESCE(s.state_name, 'NA'))",
            "market_city": "COALESCE(s.city_name, 'Unknown City')",
            "market_county": "COALESCE(s.county_name, 'Unknown County')",
            "market_zip": "COALESCE(s.zip5, '00000')",
        }
    return {
        "market_id": (
            "CONCAT('city:', UPPER(COALESCE(s.state_name, 'NA')), ':', "
            "TRIM(BOTH '-' FROM REGEXP_REPLACE(LOWER(COALESCE(s.city_name, 'unknown')), '[^a-z0-9]+', '-', 'g')))"
        ),
        "market_name": "COALESCE(s.city_name, 'Unknown City')",
        "market_state": "UPPER(COALESCE(s.state_name, 'NA'))",
        "market_city": "COALESCE(s.city_name, 'Unknown City')",
        "market_county": "NULL::text",
        "market_zip": "NULL::text",
    }


def _hydrate_top_chains(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
    return []


def _score_components(mapping: dict[str, Any]) -> dict[str, float]:
    pharmacy_count = float(mapping.get("pharmacy_count") or 0)
    population = float(mapping.get("population") or 0)
    density = 0.0
    if population > 0:
        density = (pharmacy_count * 100000.0) / population
    density_norm = min(max(density / 40.0, 0.0), 1.0)
    medicare_share = float(mapping.get("active_medicare_share") or 0.0)
    mail_share = float(mapping.get("mail_order_share") or 0.0)
    license_share = float(mapping.get("license_coverage_share") or 0.0)
    chain_concentration = float(mapping.get("chain_concentration") or 0.0)
    competition_factor = 1.0 - min(max(chain_concentration, 0.0), 1.0)
    access_score = 100.0 * (
        0.35 * density_norm
        + 0.25 * medicare_share
        + 0.10 * mail_share
        + 0.15 * license_share
        + 0.15 * competition_factor
    )
    return {
        "density_per_100k": round(density, 2),
        "density_norm": round(density_norm, 4),
        "medicare_share": round(medicare_share, 4),
        "mail_order_share": round(mail_share, 4),
        "license_share": round(license_share, 4),
        "competition_factor": round(competition_factor, 4),
        "access_score": round(access_score, 2),
    }


def _sort_expression(sort: str) -> str:
    expression_by_sort = {
        "pharmacy_count": "f.pharmacy_count",
        "pharmacies_per_100k": "f.pharmacies_per_100k",
        "access_score": "f.access_score",
        "active_medicare_share": "f.active_medicare_share",
        "license_coverage_share": "f.license_coverage_share",
    }
    return expression_by_sort.get(sort, "f.access_score")


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_name_like_filters(args: Any) -> list[str]:
    names: list[str] = []
    if hasattr(args, "getlist"):
        names.extend(args.getlist("name_like"))
    elif hasattr(args, "getall"):
        try:
            names.extend(args.getall("name_like"))
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("failed to read report name_like filters with getall: %s", exc)
    single = args.get("name_like") if hasattr(args, "get") else None
    if single:
        names.append(single)
    normalized_names: list[str] = []
    seen_names: set[str] = set()
    for value in names:
        text_value = str(value or "").strip().lower()
        if not text_value or text_value in seen_names:
            continue
        seen_names.add(text_value)
        normalized_names.append(text_value)
    return normalized_names


def _name_like_clauses(alias: str, names: list[str], base_param: str = "name_like") -> tuple[str, dict[str, Any]]:
    if not names:
        return "", {}
    prefix = alias if alias.endswith(".") or not alias else f"{alias}."
    expr = _CHAIN_NAME_TEMPLATE.format(alias=prefix)
    clauses: list[str] = []
    parameter_map: dict[str, Any] = {}
    for idx, name in enumerate(names):
        param_name = f"{base_param}_{idx}"
        clauses.append(f"({expr} LIKE :{param_name})")
        parameter_map[param_name] = f"%{name}%"
    return f"({' OR '.join(clauses)})", parameter_map


def _ensure_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _ensure_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _normalize_us_state_code(value: Any) -> str | None:
    text_value = str(value or "").strip().upper()
    if not text_value:
        return None
    text_value = re.sub(r"\s+", " ", text_value)
    return _US_STATE_NORMALIZATION.get(text_value)


def _canonicalize_pharmacy_state_stats_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stats_by_state = {
        code: {
            "state": code,
            "nppes_pharmacies": 0,
            "nppes_pharmacists": 0,
            "active_pharmacists": 0,
            "active_pharmacies": 0,
            "aca_pharmacies": 0,
        }
        for code, _ in _US_STATE_DIMENSION
    }
    for row in rows:
        state_code = _normalize_us_state_code(row.get("state"))
        if state_code not in _US_STATE_CODES:
            continue
        state_payload = stats_by_state[state_code]
        state_payload["nppes_pharmacies"] += _coerce_int(row.get("nppes_pharmacies"))
        state_payload["nppes_pharmacists"] += _coerce_int(row.get("nppes_pharmacists"))
        state_payload["active_pharmacists"] += _coerce_int(row.get("active_pharmacists"))
        state_payload["active_pharmacies"] += _coerce_int(row.get("active_pharmacies"))
        state_payload["aca_pharmacies"] += _coerce_int(row.get("aca_pharmacies"))
    return [stats_by_state[code] for code, _ in _US_STATE_DIMENSION]


def _is_match_all_name_filters(names: Sequence[str]) -> bool:
    normalized_names = [str(name or "").strip() for name in names if str(name or "").strip()]
    if not normalized_names:
        return False
    return all(set(name) <= {"%"} for name in normalized_names)


def _build_chain_summary_sql(
    *,
    names: list[str],
    has_staffing_helper: bool,
    include_states: bool,
    address_table_sql: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build chain summary SQL."""
    address_table_sql = address_table_sql or _pharmacy_address_table_sql()
    address_order_expr = _pharmacy_address_order_expr("a", address_table_sql)
    should_match_all_names = _is_match_all_name_filters(names)
    if should_match_all_names:
        name_clause = ""
        parameter_map = {}
    else:
        name_clause, parameter_map = _name_like_clauses("d", names)
    if not name_clause and not should_match_all_names:
        raise InvalidUsage("At least one name_like value is required")
    parameter_map["include_states"] = include_states
    schema = "mrf"
    staffing_table = _qualified_table_name(NPIPhoneStaffing.__table__)
    if has_staffing_helper:
        staffing_cte = f"""
staffing_by_phone AS (
    SELECT
        state_name,
        telephone_number,
        pharmacist_count
    FROM {staffing_table}
)"""
    else:
        staffing_cte = f"""
staffing_by_phone AS (
    SELECT
        a.state_name,
        REGEXP_REPLACE(a.telephone_number, '[^0-9]', '', 'g') AS telephone_number,
        COUNT(DISTINCT a.npi)::int AS pharmacist_count
    FROM {address_table_sql} AS a
    CROSS JOIN pharmacist_taxonomy AS pt
    WHERE a.type = 'primary'
      AND a.state_name IS NOT NULL
      AND a.state_name <> ''
      AND a.telephone_number IS NOT NULL
      AND a.telephone_number <> ''
      AND a.taxonomy_array && pt.codes
    GROUP BY a.state_name, REGEXP_REPLACE(a.telephone_number, '[^0-9]', '', 'g')
)"""

    sql = f"""
WITH pharmacy_taxonomy AS (
    SELECT ARRAY_AGG(int_code) AS codes
    FROM {schema}.nucc_taxonomy
    WHERE classification = 'Pharmacy'
),
pharmacist_taxonomy AS (
    SELECT ARRAY_AGG(int_code) AS codes
    FROM {schema}.nucc_taxonomy
    WHERE classification = 'Pharmacist'
),
{staffing_cte},
chain_pharmacies AS (
    SELECT DISTINCT ON (a.npi)
        a.npi::bigint AS npi,
        COALESCE(NULLIF(a.state_name, ''), 'NA') AS state_name,
        REGEXP_REPLACE(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g') AS telephone_number,
        CASE
            WHEN a.plans_network_array IS NULL THEN FALSE
            WHEN a.plans_network_array @@ '0'::query_int THEN FALSE
            ELSE TRUE
        END AS has_insurance,
        COALESCE(s.pharmacist_count, 0)::int AS pharmacist_count
    FROM {address_table_sql} AS a
    CROSS JOIN pharmacy_taxonomy AS pt
    {"JOIN " + schema + ".npi AS d ON d.npi = a.npi" if not should_match_all_names else ""}
    LEFT JOIN staffing_by_phone AS s
      ON s.state_name = a.state_name
     AND s.telephone_number = REGEXP_REPLACE(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g')
    WHERE a.type = 'primary'
      AND a.taxonomy_array && pt.codes
      {("AND " + name_clause) if not should_match_all_names else ""}
    ORDER BY {address_order_expr}
),
summary AS (
    SELECT
        COUNT(*)::int AS pharmacy_npi_count,
        COUNT(*) FILTER (WHERE has_insurance)::int AS insured_pharmacy_npi_count,
        COUNT(*) FILTER (WHERE pharmacist_count > 0)::int AS active_pharmacy_count,
        COALESCE(SUM(pharmacist_count), 0)::int AS pharmacist_count
    FROM chain_pharmacies
),
histogram AS (
    SELECT
        CASE
            WHEN pharmacist_count = 0 THEN '0'
            WHEN pharmacist_count = 1 THEN '1'
            WHEN pharmacist_count = 2 THEN '2'
            WHEN pharmacist_count = 3 THEN '3'
            WHEN pharmacist_count = 4 THEN '4'
            WHEN pharmacist_count = 5 THEN '5'
            WHEN pharmacist_count = 6 THEN '6'
            WHEN pharmacist_count = 7 THEN '7'
            WHEN pharmacist_count = 8 THEN '8'
            WHEN pharmacist_count = 9 THEN '9'
            WHEN pharmacist_count = 10 THEN '10'
            WHEN pharmacist_count = 11 THEN '11'
            WHEN pharmacist_count = 12 THEN '12'
            WHEN pharmacist_count = 13 THEN '13'
            WHEN pharmacist_count = 14 THEN '14'
            WHEN pharmacist_count = 15 THEN '15'
            WHEN pharmacist_count = 16 THEN '16'
            WHEN pharmacist_count = 17 THEN '17'
            WHEN pharmacist_count = 18 THEN '18'
            WHEN pharmacist_count = 19 THEN '19'
            WHEN pharmacist_count = 20 THEN '20'
            WHEN pharmacist_count = 21 THEN '21'
            WHEN pharmacist_count = 22 THEN '22'
            WHEN pharmacist_count = 23 THEN '23'
            WHEN pharmacist_count = 24 THEN '24'
            WHEN pharmacist_count = 25 THEN '25'
            ELSE '25+'
        END AS pharmacist_group,
        CASE WHEN pharmacist_count <= 25 THEN pharmacist_count ELSE 26 END AS sort_key,
        COUNT(*)::int AS pharmacy_count
    FROM chain_pharmacies
    GROUP BY pharmacist_group, sort_key
),
state_rows AS (
    SELECT
        state_name AS state,
        COUNT(*) FILTER (WHERE pharmacist_count > 0)::int AS active_pharmacy_count,
        COUNT(*)::int AS pharmacy_npi_count,
        COUNT(*) FILTER (WHERE has_insurance)::int AS insured_pharmacy_npi_count,
        COALESCE(SUM(pharmacist_count), 0)::int AS pharmacist_count
    FROM chain_pharmacies
    GROUP BY state_name
)
SELECT
    json_build_object(
        'pharmacy_npi_count', s.pharmacy_npi_count,
        'insured_pharmacy_npi_count', s.insured_pharmacy_npi_count,
        'active_pharmacy_count', s.active_pharmacy_count,
        'pharmacist_count', s.pharmacist_count
    ) AS summary,
    COALESCE(
        (
            SELECT json_agg(
                json_build_object('label', h.pharmacist_group, 'count', h.pharmacy_count)
                ORDER BY h.sort_key ASC
            )
            FROM histogram AS h
        ),
        '[]'::json
    ) AS histogram,
    CASE
        WHEN :include_states THEN COALESCE(
            (
                SELECT json_agg(
                    json_build_object(
                        'state', sr.state,
                        'active_pharmacy_count', sr.active_pharmacy_count,
                        'pharmacy_npi_count', sr.pharmacy_npi_count,
                        'insured_pharmacy_npi_count', sr.insured_pharmacy_npi_count,
                        'pharmacist_count', sr.pharmacist_count
                    )
                    ORDER BY sr.active_pharmacy_count DESC, sr.state ASC
                )
                FROM state_rows AS sr
            ),
            '[]'::json
        )
        ELSE '[]'::json
    END AS states
FROM summary AS s
"""
    return sql, parameter_map


async def _query_chain_summary(
    session,
    *,
    names: list[str],
    include_states: bool = True,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], bool]:
    has_staffing_helper = await _is_table_available(session, NPIPhoneStaffing.__table__)
    address_table_sql = await _resolve_pharmacy_address_table_sql(session)
    sql, params = _build_chain_summary_sql(
        names=names,
        has_staffing_helper=has_staffing_helper,
        include_states=include_states,
        address_table_sql=address_table_sql,
    )
    result = await session.execute(text(sql), params)
    row = result.mappings().first() or {}
    summary = _ensure_json_dict(row.get("summary"))
    histogram_entries = [
        item for item in _ensure_json_list(row.get("histogram")) if isinstance(item, dict)
    ]
    states = [item for item in _ensure_json_list(row.get("states")) if isinstance(item, dict)]
    return summary, histogram_entries, states, has_staffing_helper


def _pharmacy_market_methodology(as_of: datetime.date, include_staffing: bool) -> dict[str, Any]:
    return {
        "version": "v1",
        "as_of": as_of.isoformat(),
        "sources": [
            "NPPES (pharmacy NPI/address and organization names)",
            "CMS Part D pharmacy activity (medicare_active, mail_order, pharmacy_type)",
            "State board pharmacy licenses (active license and disciplinary flags)",
            "NPPES other identifiers (NCPDP and Medicare/PTAN markers)",
            "ZIP/city/county population lookups",
        ],
        "access_score_formula": {
            "density_norm_weight": 0.35,
            "medicare_share_weight": 0.25,
            "mail_order_share_weight": 0.10,
            "license_share_weight": 0.15,
            "competition_weight": 0.15,
            "density_normalization_per_100k_cap": 40.0,
        },
        "notes": [
            "Chain classification uses organization-name heuristics.",
            "NCPDP/Medicare/Medicaid/PTAN/CLIA identifier counts are inferred from NPPES other identifier issuer/type markers.",
            "Pharmacies per 100k uses ZIP population rollups deduplicated by ZIP within each market.",
            "Staffing proxy is derived from pharmacist/pharmacy phone-match heuristics only when include_staffing=true.",
        ],
        "include_staffing": include_staffing,
    }


def _pharmacy_chain_summary_methodology(*, staffing_helper_available: bool) -> dict[str, Any]:
    return {
        "version": "v1",
        "sources": [
            "NPPES/NPI provider names and primary addresses",
            "NUCC taxonomy classification mapping",
            "HealthPorta NPI staffing helper derived from pharmacist primary addresses",
        ],
        "notes": [
            "Chain matching uses organization and DBA name-like filters supplied by the caller.",
            "Pharmacy counts are based on primary-address pharmacy NPIs matched to the chain filters.",
            "Active pharmacy count means at least one pharmacist matched on the same state and phone number.",
            "Insured pharmacy count means the primary NPI address carries a non-zero plans_network_array.",
        ],
        "staffing_helper_available": staffing_helper_available,
        "staffing_mode": "helper_table" if staffing_helper_available else "live_fallback_query",
    }


def _build_pharmacy_state_stats_sql(
    *,
    has_staffing_helper: bool,
    address_table_sql: str | None = None,
) -> str:
    """Build pharmacy state stats SQL."""
    address_table_sql = address_table_sql or _pharmacy_address_table_sql()
    address_order_expr = _pharmacy_address_order_expr("a", address_table_sql)
    schema = "mrf"
    staffing_table = _qualified_table_name(NPIPhoneStaffing.__table__)
    if has_staffing_helper:
        staffing_cte = f"""
staffing_by_phone AS (
    SELECT
        state_name,
        telephone_number,
        pharmacist_count
    FROM {staffing_table}
    WHERE state_name IS NOT NULL
      AND state_name <> ''
      AND telephone_number IS NOT NULL
      AND telephone_number <> ''
)"""
        pharmacist_taxonomy_cte = ""
        pharmacist_counts_cte = """
pharmacist_counts AS (
    SELECT
        COALESCE(NULLIF(state_name, ''), 'NA') AS state,
        SUM(COALESCE(pharmacist_count, 0))::int AS nppes_pharmacists
    FROM staffing_by_phone
    GROUP BY 1
),
active_pharmacist_counts AS (
    SELECT
        p.state_name AS state,
        SUM(COALESCE(s.pharmacist_count, 0))::int AS active_pharmacists
    FROM (
        SELECT DISTINCT state_name, telephone_number
        FROM pharmacy_base
        WHERE telephone_number <> ''
    ) AS p
    JOIN staffing_by_phone AS s
      ON s.state_name = p.state_name
     AND s.telephone_number = p.telephone_number
    GROUP BY p.state_name
)"""
    else:
        staffing_cte = f"""
staffing_by_phone AS (
    SELECT
        a.state_name,
        REGEXP_REPLACE(a.telephone_number, '[^0-9]', '', 'g') AS telephone_number,
        COUNT(DISTINCT a.npi)::int AS pharmacist_count
    FROM {address_table_sql} AS a
    CROSS JOIN pharmacist_taxonomy AS pt
    WHERE a.type = 'primary'
      AND (a.country_code IS NULL OR UPPER(a.country_code) IN ('US', 'USA'))
      AND a.state_name IS NOT NULL
      AND a.state_name <> ''
      AND a.telephone_number IS NOT NULL
      AND a.telephone_number <> ''
      AND a.taxonomy_array && pt.codes
    GROUP BY a.state_name, REGEXP_REPLACE(a.telephone_number, '[^0-9]', '', 'g')
)"""
        pharmacist_taxonomy_cte = """
pharmacist_taxonomy AS (
    SELECT ARRAY_AGG(int_code) AS codes
    FROM {schema}.nucc_taxonomy
    WHERE classification = 'Pharmacist'
),""".format(schema=schema)
        pharmacist_counts_cte = f"""
pharmacist_base AS (
    SELECT DISTINCT ON (a.npi)
        a.npi::bigint AS npi,
        COALESCE(NULLIF(a.state_name, ''), 'NA') AS state_name,
        REGEXP_REPLACE(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g') AS telephone_number
    FROM {address_table_sql} AS a
    CROSS JOIN pharmacist_taxonomy AS pt
    WHERE a.type = 'primary'
      AND (a.country_code IS NULL OR UPPER(a.country_code) IN ('US', 'USA'))
      AND a.taxonomy_array && pt.codes
    ORDER BY {address_order_expr}
),
pharmacist_counts AS (
    SELECT
        state_name AS state,
        COUNT(*)::int AS nppes_pharmacists
    FROM pharmacist_base
    GROUP BY state_name
),
active_pharmacist_counts AS (
    SELECT
        phm.state_name AS state,
        COUNT(DISTINCT phm.npi)::int AS active_pharmacists
    FROM pharmacist_base AS phm
    WHERE EXISTS (
        SELECT 1
        FROM pharmacy_base AS ph
        WHERE ph.state_name = phm.state_name
          AND ph.telephone_number = phm.telephone_number
    )
    GROUP BY phm.state_name
)"""

    return f"""
WITH pharmacy_taxonomy AS (
    SELECT ARRAY_AGG(int_code) AS codes
    FROM {schema}.nucc_taxonomy
    WHERE classification = 'Pharmacy'
),
{pharmacist_taxonomy_cte}
{staffing_cte},
pharmacy_base AS (
    SELECT DISTINCT ON (a.npi)
        a.npi::bigint AS npi,
        COALESCE(NULLIF(a.state_name, ''), 'NA') AS state_name,
        REGEXP_REPLACE(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g') AS telephone_number,
        CASE
            WHEN a.plans_network_array IS NULL THEN FALSE
            WHEN a.plans_network_array @@ '0'::query_int THEN FALSE
            ELSE TRUE
        END AS has_insurance
    FROM {address_table_sql} AS a
    CROSS JOIN pharmacy_taxonomy AS pt
    WHERE a.type = 'primary'
      AND (a.country_code IS NULL OR UPPER(a.country_code) IN ('US', 'USA'))
      AND a.taxonomy_array && pt.codes
    ORDER BY {address_order_expr}
),
pharmacy_counts AS (
    SELECT
        p.state_name AS state,
        COUNT(*)::int AS nppes_pharmacies,
        COUNT(*) FILTER (WHERE p.has_insurance)::int AS aca_pharmacies,
        COUNT(*) FILTER (WHERE COALESCE(s.pharmacist_count, 0) > 0)::int AS active_pharmacies
    FROM pharmacy_base AS p
    LEFT JOIN staffing_by_phone AS s
      ON s.state_name = p.state_name
     AND s.telephone_number = p.telephone_number
    GROUP BY p.state_name
),
{pharmacist_counts_cte},
all_states AS (
    SELECT state FROM pharmacy_counts
    UNION
    SELECT state FROM pharmacist_counts
    UNION
    SELECT state FROM active_pharmacist_counts
)
SELECT
    s.state,
    COALESCE(pc.nppes_pharmacies, 0)::int AS nppes_pharmacies,
    COALESCE(phc.nppes_pharmacists, 0)::int AS nppes_pharmacists,
    COALESCE(apc.active_pharmacists, 0)::int AS active_pharmacists,
    COALESCE(pc.active_pharmacies, 0)::int AS active_pharmacies,
    COALESCE(pc.aca_pharmacies, 0)::int AS aca_pharmacies
FROM all_states AS s
LEFT JOIN pharmacy_counts AS pc ON pc.state = s.state
LEFT JOIN pharmacist_counts AS phc ON phc.state = s.state
LEFT JOIN active_pharmacist_counts AS apc ON apc.state = s.state
ORDER BY s.state ASC
"""


def _pharmacy_state_stats_methodology(*, staffing_helper_available: bool) -> dict[str, Any]:
    return {
        "version": "v1",
        "sources": [
            "NPPES/NPI primary provider addresses",
            "NUCC taxonomy classification mapping",
            "HealthPorta NPI staffing helper derived from pharmacist primary addresses",
        ],
        "notes": [
            "Rows are grouped by primary-address state across pharmacy and pharmacist NPIs.",
            "Active pharmacists require a state-and-phone match to at least one pharmacy primary address.",
            "Active pharmacies require at least one matched pharmacist on the same state and phone number.",
            "ACA pharmacy count means the primary NPI address carries a non-zero plans_network_array.",
            "Output is normalized to US states plus District of Columbia (51 rows); unknown labels are excluded.",
        ],
        "staffing_helper_available": staffing_helper_available,
        "staffing_mode": "helper_table" if staffing_helper_available else "live_fallback_query",
    }

async def _query_pharmacy_state_stats(session) -> tuple[list[dict[str, Any]], bool]:
    has_staffing_helper = await _is_table_available(session, NPIPhoneStaffing.__table__)
    address_table_sql = await _resolve_pharmacy_address_table_sql(session)
    sql = _build_pharmacy_state_stats_sql(
        has_staffing_helper=has_staffing_helper,
        address_table_sql=address_table_sql,
    )
    result = await session.execute(text(sql))
    rows = []
    for row in result.mappings().all():
        rows.append(
            {
                "state": str(row.get("state") or "").upper(),
                "nppes_pharmacies": _coerce_int(row.get("nppes_pharmacies")),
                "nppes_pharmacists": _coerce_int(row.get("nppes_pharmacists")),
                "active_pharmacists": _coerce_int(row.get("active_pharmacists")),
                "active_pharmacies": _coerce_int(row.get("active_pharmacies")),
                "aca_pharmacies": _coerce_int(row.get("aca_pharmacies")),
            }
        )
    return _canonicalize_pharmacy_state_stats_rows(rows), has_staffing_helper


def _build_pharmacy_state_stats_payload(
    states: list[dict[str, Any]],
    *,
    staffing_helper_available: bool,
) -> dict[str, Any]:
    return {
        "states": states,
        "methodology": _pharmacy_state_stats_methodology(
            staffing_helper_available=staffing_helper_available
        ),
    }


def _build_market_sql(
    *,
    scope: str,
    sort: str,
    order: str,
    include_staffing: bool,
    has_partd: bool,
    has_license: bool,
    has_other_id: bool,
    market_id_filter: str | None,
    state: str | None,
    city: str | None,
    county: str | None,
    zip_code: str | None,
    chain: str | None,
    address_table_sql: str | None = None,
) -> tuple[str, str, dict[str, Any]]:
    """Build market SQL."""
    address_table_sql = address_table_sql or _pharmacy_address_table_sql()
    address_zip5_expr = _pharmacy_address_zip5_expr("a", address_table_sql)
    parameter_map: dict[str, Any] = {}
    base_filters: list[str] = [
        "a.type = 'primary'",
        "a.taxonomy_array && pc.codes",
    ]
    if state:
        base_filters.append("a.state_name = :state")
        parameter_map["state"] = state
    if city:
        base_filters.append("a.city_name = :city")
        parameter_map["city"] = city
    if county:
        base_filters.append("LOWER(COALESCE(g.county_name, '')) = :county")
        parameter_map["county"] = county.lower()
    if zip_code:
        base_filters.append(f"{address_zip5_expr} = :zip_code")
        parameter_map["zip_code"] = zip_code

    scoped_filters: list[str] = []
    if chain:
        if chain == "independent":
            scoped_filters.append("s.chain_name IS NULL")
        else:
            scoped_filters.append("s.chain_name = :chain")
            parameter_map["chain"] = chain

    partd_table = _qualified_table_name(PartDPharmacyActivity.__table__)
    if has_partd:
        partd_cte = f"""
partd_by_npi AS (
    SELECT
        (CASE WHEN npi >= 1000000000 THEN (npi / 10) ELSE npi END)::bigint AS npi9,
        BOOL_OR(medicare_active) FILTER (
            WHERE effective_from <= :as_of
              AND (effective_to IS NULL OR effective_to >= :as_of)
        ) AS medicare_active,
        BOOL_OR(COALESCE(mail_order, FALSE)) FILTER (
            WHERE effective_from <= :as_of
              AND (effective_to IS NULL OR effective_to >= :as_of)
        ) AS mail_order,
        MAX(pharmacy_type) FILTER (
            WHERE effective_from <= :as_of
              AND (effective_to IS NULL OR effective_to >= :as_of)
        ) AS pharmacy_type
    FROM {partd_table}
    GROUP BY 1
)"""
        partd_join = "LEFT JOIN partd_by_npi pd ON pd.npi9 = (a.npi / 10)"
        partd_active = "COALESCE(pd.medicare_active, FALSE)"
        partd_mail_order = "COALESCE(pd.mail_order, FALSE)"
        partd_type = "pd.pharmacy_type"
    else:
        partd_cte = """
partd_by_npi AS (
    SELECT
        NULL::bigint AS npi9,
        FALSE AS medicare_active,
        FALSE AS mail_order,
        NULL::text AS pharmacy_type
    WHERE FALSE
)"""
        partd_join = ""
        partd_active = "FALSE"
        partd_mail_order = "FALSE"
        partd_type = "NULL::text"

    license_table = _qualified_table_name(PharmacyLicenseRecord.__table__)
    if has_license:
        license_cte = f"""
license_by_npi AS (
    SELECT
        npi::bigint AS npi,
        BOOL_OR(
            LOWER(COALESCE(license_status, '')) = 'active'
            AND (license_expiration_date IS NULL OR license_expiration_date >= :as_of)
        ) AS has_active_state_license,
        BOOL_OR(COALESCE(disciplinary_flag, FALSE)) AS disciplinary_flag_any
    FROM {license_table}
    GROUP BY npi
)"""
        license_join = "LEFT JOIN license_by_npi pl ON pl.npi = a.npi"
        license_active = "COALESCE(pl.has_active_state_license, FALSE)"
        disciplinary_flag = "COALESCE(pl.disciplinary_flag_any, FALSE)"
    else:
        license_cte = """
license_by_npi AS (
    SELECT
        NULL::bigint AS npi,
        FALSE AS has_active_state_license,
        FALSE AS disciplinary_flag_any
    WHERE FALSE
)"""
        license_join = ""
        license_active = "FALSE"
        disciplinary_flag = "FALSE"

    other_id_table = _qualified_table_name(NPIDataOtherIdentifier.__table__)
    if has_other_id:
        other_id_cte = f"""
identifier_by_npi AS (
    SELECT
        b.npi,
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%ncpdp%'
        ) AS has_ncpdp_identifier,
        BOOL_OR(
            REGEXP_REPLACE(COALESCE(oi.other_provider_identifier_type_code, ''), '^0+', '') = '1'
        ) AS has_medicaid_identifier,
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%railroad medicare%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%rr medicare%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%medicare railroad%'
        ) AS has_railroad_medicare_identifier,
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%ptan%'
            OR (
                REGEXP_REPLACE(COALESCE(oi.other_provider_identifier_type_code, ''), '^0+', '') = '1'
                AND (
                    LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%medicare%'
                    OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%rr medicare%'
                    OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%railroad medicare%'
                )
            )
        ) AS has_ptan_identifier,
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%clia%'
        ) AS has_clia_identifier,
        BOOL_OR(
            REGEXP_REPLACE(COALESCE(oi.other_provider_identifier_type_code, ''), '^0+', '') = '5'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%medicare%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%ptan%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%rr medicare%'
        ) AS has_medicare_identifier,
        COUNT(*) FILTER (
            WHERE COALESCE(oi.other_provider_identifier, '') <> ''
        )::int AS other_identifier_count
    FROM (
        SELECT DISTINCT npi
        FROM base
    ) b
    LEFT JOIN {other_id_table} oi ON oi.npi = b.npi
    GROUP BY b.npi
)"""
        other_id_join = "LEFT JOIN identifier_by_npi ib ON ib.npi = s.npi"
        other_id_select = (
            "COALESCE(ib.has_ncpdp_identifier, FALSE) AS has_ncpdp_identifier, "
            "COALESCE(ib.has_medicaid_identifier, FALSE) AS has_medicaid_identifier, "
            "COALESCE(ib.has_railroad_medicare_identifier, FALSE) AS has_railroad_medicare_identifier, "
            "COALESCE(ib.has_ptan_identifier, FALSE) AS has_ptan_identifier, "
            "COALESCE(ib.has_clia_identifier, FALSE) AS has_clia_identifier, "
            "COALESCE(ib.has_medicare_identifier, FALSE) AS has_medicare_identifier, "
            "COALESCE(ib.other_identifier_count, 0)::int AS other_identifier_count"
        )
    else:
        other_id_cte = """
identifier_by_npi AS (
    SELECT
        NULL::bigint AS npi,
        FALSE AS has_ncpdp_identifier,
        FALSE AS has_medicaid_identifier,
        FALSE AS has_railroad_medicare_identifier,
        FALSE AS has_ptan_identifier,
        FALSE AS has_clia_identifier,
        FALSE AS has_medicare_identifier,
        0::int AS other_identifier_count
    WHERE FALSE
)"""
        other_id_join = ""
        other_id_select = (
            "FALSE AS has_ncpdp_identifier, "
            "FALSE AS has_medicaid_identifier, "
            "FALSE AS has_railroad_medicare_identifier, "
            "FALSE AS has_ptan_identifier, "
            "FALSE AS has_clia_identifier, "
            "FALSE AS has_medicare_identifier, "
            "0::int AS other_identifier_count"
        )

    scope_cfg = _scope_sql(scope)
    chain_case = _chain_case_sql(
        "LOWER(COALESCE(n.provider_organization_name, '') || ' ' || COALESCE(n.do_business_as_text, ''))"
    )

    scoped_where = " AND ".join(scoped_filters) if scoped_filters else "1=1"
    if market_id_filter:
        scoped_where = f"{scoped_where} AND swm.market_id = :market_id"
        parameter_map["market_id"] = market_id_filter

    staffing_cte = ""
    staffing_join = ""
    staffing_select = "NULL::int AS estimated_pharmacist_count_proxy"
    if include_staffing:
        staffing_cte = """
, pharmacist_codes AS (
    SELECT COALESCE(ARRAY_AGG(int_code), ARRAY[]::integer[]) AS codes
    FROM mrf.nucc_taxonomy
    WHERE classification = 'Pharmacist'
)
, pharmacist_addresses AS (
    SELECT
        p.npi::bigint AS npi,
        UPPER(COALESCE(p.state_name, '')) AS state_code,
        REGEXP_REPLACE(COALESCE(p.telephone_number, ''), '[^0-9]', '', 'g') AS phone_digits
    FROM {address_table_sql} p
    CROSS JOIN pharmacist_codes pc
    WHERE p.type = 'primary'
      AND p.taxonomy_array && pc.codes
      AND REGEXP_REPLACE(COALESCE(p.telephone_number, ''), '[^0-9]', '', 'g') <> ''
)
, staffing AS (
    SELECT
        swm.market_id,
        COALESCE(SUM(match_counts.pharmacist_count), 0)::int AS estimated_pharmacist_count_proxy
    FROM (
        SELECT DISTINCT
            market_id,
            npi,
            UPPER(COALESCE(state_name, '')) AS state_code,
            phone_digits
        FROM filtered_markets
        WHERE phone_digits <> ''
    ) swm
    LEFT JOIN LATERAL (
        SELECT COUNT(DISTINCT pa.npi)::int AS pharmacist_count
        FROM pharmacist_addresses pa
        WHERE pa.state_code = swm.state_code
          AND pa.phone_digits = swm.phone_digits
    ) match_counts ON TRUE
    GROUP BY swm.market_id
)"""
        staffing_join = "LEFT JOIN staffing st ON st.market_id = f.market_id"
        staffing_select = "COALESCE(st.estimated_pharmacist_count_proxy, 0)::int AS estimated_pharmacist_count_proxy"

    cte = f"""
WITH pharmacy_codes AS (
    SELECT COALESCE(ARRAY_AGG(int_code), ARRAY[]::integer[]) AS codes
    FROM mrf.nucc_taxonomy
    WHERE classification = 'Pharmacy'
),
{partd_cte},
{license_cte},
base AS (
    SELECT
        a.npi::bigint AS npi,
        a.city_name,
        a.state_name,
        {address_zip5_expr} AS zip5,
        g.county_name,
        COALESCE(g.population, 0)::bigint AS zip_population,
        REGEXP_REPLACE(COALESCE(a.telephone_number, ''), '[^0-9]', '', 'g') AS phone_digits,
        n.provider_organization_name,
        n.do_business_as_text,
        {chain_case} AS chain_name,
        {partd_active} AS medicare_active,
        {partd_mail_order} AS mail_order,
        {partd_type} AS pharmacy_type,
        {license_active} AS has_active_state_license,
        {disciplinary_flag} AS disciplinary_flag
    FROM {address_table_sql} a
    JOIN mrf.npi n ON n.npi = a.npi
    LEFT JOIN mrf.geo_zip_lookup g ON g.zip_code = {address_zip5_expr}
    {partd_join}
    {license_join}
    CROSS JOIN pharmacy_codes pc
    WHERE {" AND ".join(base_filters)}
),
{other_id_cte},
scoped AS (
    SELECT
        s.*,
        {other_id_select}
    FROM base s
    {other_id_join}
    WHERE {scoped_where if not market_id_filter else "1=1"}
),
scoped_with_market AS (
    SELECT
        s.*,
        {scope_cfg["market_id"]} AS market_id,
        '{scope}'::text AS market_scope,
        {scope_cfg["market_name"]} AS market_name,
        {scope_cfg["market_state"]} AS market_state,
        {scope_cfg["market_city"]} AS market_city,
        {scope_cfg["market_county"]} AS market_county,
        {scope_cfg["market_zip"]} AS market_zip
    FROM scoped s
),
filtered_markets AS (
    SELECT *
    FROM scoped_with_market swm
    WHERE {("swm.market_id = :market_id" if market_id_filter else "1=1")}
),
grouped AS (
    SELECT
        market_id,
        MIN(market_scope) AS market_scope,
        MIN(market_name) AS market_name,
        MIN(market_state) AS market_state,
        MIN(market_city) AS market_city,
        MIN(market_county) AS market_county,
        MIN(market_zip) AS market_zip,
        COUNT(DISTINCT npi)::int AS pharmacy_count,
        COUNT(DISTINCT npi) FILTER (WHERE medicare_active)::int AS active_medicare_pharmacy_count,
        COUNT(DISTINCT npi) FILTER (WHERE chain_name IS NOT NULL)::int AS chain_count,
        COUNT(DISTINCT npi) FILTER (WHERE chain_name IS NULL)::int AS independent_count,
        COUNT(DISTINCT npi) FILTER (WHERE mail_order IS TRUE)::int AS mail_order_count,
        COUNT(DISTINCT npi) FILTER (WHERE mail_order IS NOT TRUE)::int AS retail_count,
        COUNT(DISTINCT npi) FILTER (WHERE has_active_state_license)::int AS license_coverage_count,
        COUNT(DISTINCT npi) FILTER (WHERE disciplinary_flag)::int AS disciplinary_flag_count,
        COUNT(DISTINCT npi) FILTER (WHERE has_ncpdp_identifier)::int AS ncpdp_registered_count,
        COUNT(DISTINCT npi) FILTER (WHERE has_medicaid_identifier)::int AS medicaid_identifier_count,
        COUNT(DISTINCT npi) FILTER (WHERE has_railroad_medicare_identifier)::int AS railroad_medicare_identifier_count,
        COUNT(DISTINCT npi) FILTER (WHERE has_ptan_identifier)::int AS ptan_identifier_count,
        COUNT(DISTINCT npi) FILTER (WHERE has_clia_identifier)::int AS clia_identifier_count,
        COUNT(DISTINCT npi) FILTER (WHERE has_medicare_identifier)::int AS medicare_identifier_count,
        COUNT(DISTINCT npi) FILTER (WHERE other_identifier_count > 0)::int AS other_identifier_npi_count
    FROM filtered_markets
    GROUP BY market_id
),
population_agg AS (
    SELECT
        market_id,
        COALESCE(SUM(zip_population), 0)::bigint AS population
    FROM (
        SELECT DISTINCT market_id, zip5, zip_population
        FROM filtered_markets
    ) z
    GROUP BY market_id
),
chain_counts AS (
    SELECT
        market_id,
        chain_name,
        COUNT(DISTINCT npi)::int AS pharmacy_count
    FROM filtered_markets
    WHERE chain_name IS NOT NULL
    GROUP BY market_id, chain_name
),
chain_top AS (
    SELECT
        market_id,
        COALESCE(
            json_agg(
                json_build_object('chain', chain_name, 'pharmacy_count', pharmacy_count)
                ORDER BY pharmacy_count DESC, chain_name ASC
            ),
            '[]'::json
        ) AS top_chains
    FROM (
        SELECT
            market_id,
            chain_name,
            pharmacy_count,
            ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY pharmacy_count DESC, chain_name ASC) AS rn
        FROM chain_counts
    ) ranked
    WHERE rn <= 5
    GROUP BY market_id
),
chain_max AS (
    SELECT
        market_id,
        COALESCE(MAX(pharmacy_count), 0)::int AS max_chain_count
    FROM chain_counts
    GROUP BY market_id
),
final AS (
    SELECT
        g.market_id,
        g.market_scope,
        g.market_name,
        g.market_state AS state,
        g.market_city AS city,
        g.market_county AS county,
        g.market_zip AS zip_code,
        g.pharmacy_count,
        g.active_medicare_pharmacy_count,
        g.chain_count,
        g.independent_count,
        g.mail_order_count,
        g.retail_count,
        g.license_coverage_count,
        g.disciplinary_flag_count,
        g.ncpdp_registered_count,
        g.medicaid_identifier_count,
        g.railroad_medicare_identifier_count,
        g.ptan_identifier_count,
        g.clia_identifier_count,
        g.medicare_identifier_count,
        g.other_identifier_npi_count,
        COALESCE(p.population, 0)::bigint AS population,
        CASE
            WHEN COALESCE(p.population, 0) > 0
            THEN ROUND((g.pharmacy_count::numeric * 100000.0) / p.population, 2)
            ELSE NULL
        END AS pharmacies_per_100k,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.active_medicare_pharmacy_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS active_medicare_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.license_coverage_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS license_coverage_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.mail_order_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS mail_order_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.ncpdp_registered_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS ncpdp_registered_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.medicaid_identifier_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS medicaid_identifier_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.railroad_medicare_identifier_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS railroad_medicare_identifier_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.ptan_identifier_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS ptan_identifier_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.clia_identifier_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS clia_identifier_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.medicare_identifier_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS medicare_identifier_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((g.other_identifier_npi_count::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS other_identifier_share,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND((COALESCE(cm.max_chain_count, 0)::numeric / g.pharmacy_count), 4)
            ELSE 0
        END AS chain_concentration,
        CASE
            WHEN g.pharmacy_count > 0
            THEN ROUND(
                100.0 * (
                    0.35 * LEAST(
                        COALESCE((g.pharmacy_count::numeric * 100000.0) / NULLIF(p.population, 0), 0) / 40.0,
                        1.0
                    )
                    + 0.25 * (g.active_medicare_pharmacy_count::numeric / g.pharmacy_count)
                    + 0.10 * (g.mail_order_count::numeric / g.pharmacy_count)
                    + 0.15 * (g.license_coverage_count::numeric / g.pharmacy_count)
                    + 0.15 * (1.0 - (COALESCE(cm.max_chain_count, 0)::numeric / g.pharmacy_count))
                ),
                2
            )
            ELSE 0
        END AS access_score,
        COALESCE(ct.top_chains, '[]'::json) AS top_chains
    FROM grouped g
    LEFT JOIN population_agg p ON p.market_id = g.market_id
    LEFT JOIN chain_top ct ON ct.market_id = g.market_id
    LEFT JOIN chain_max cm ON cm.market_id = g.market_id
)
{staffing_cte}
"""
    count_sql = cte + "\nSELECT COUNT(*)::int AS total FROM final f;"
    data_sql = (
        cte
        + f"""
SELECT
    f.*,
    {staffing_select},
    COUNT(*) OVER()::int AS total_count
FROM final f
{staffing_join}
ORDER BY {_sort_expression(sort)} {order.upper()} NULLS LAST, f.market_id ASC
LIMIT :limit OFFSET :offset;
"""
    )
    return count_sql, data_sql, parameter_map


async def _build_market_summary_query_sql(session, query: _MarketSummaryQuery):
    has_partd = await _is_table_available(session, PartDPharmacyActivity.__table__)
    has_license = await _is_table_available(session, PharmacyLicenseRecord.__table__)
    has_other_id = await _is_table_available(session, NPIDataOtherIdentifier.__table__)
    address_table_sql = await _resolve_pharmacy_address_table_sql(session)
    count_sql, data_sql, parameter_map = _build_market_sql(
        scope=query.scope,
        sort=query.sort,
        order=query.order,
        include_staffing=query.include_staffing,
        has_partd=has_partd,
        has_license=has_license,
        has_other_id=has_other_id,
        market_id_filter=query.market_id,
        state=query.state,
        city=query.city,
        county=query.county,
        zip_code=query.zip_code,
        chain=query.chain,
        address_table_sql=address_table_sql,
    )
    parameter_map = dict(parameter_map)
    parameter_map.update(
        {
            "as_of": query.as_of,
            "limit": query.limit,
            "offset": query.offset,
        }
    )
    return count_sql, data_sql, parameter_map


async def _load_market_summary_rows(session, query: _MarketSummaryQuery):
    count_sql, data_sql, parameter_map = await _build_market_summary_query_sql(session, query)
    market_rows = (await session.execute(text(data_sql), parameter_map)).mappings().all()
    total = _coerce_int(market_rows[0].get("total_count")) if market_rows else 0
    if not market_rows and query.offset > 0:
        total = _coerce_int((await session.execute(text(count_sql), parameter_map)).scalar())
    return total, market_rows


def _normalize_market_summary_metrics(market_row_dict, *, include_staffing: bool):
    metric_by_name = {
        metric_name: _coerce_int(market_row_dict.get(column_name))
        for metric_name, column_name in _MARKET_INTEGER_METRICS
    }
    metric_by_name.update(
        {
            metric_name: _coerce_float(market_row_dict.get(column_name))
            for metric_name, column_name in _MARKET_FLOAT_METRICS
        }
    )
    metric_by_name["estimated_pharmacist_count_proxy"] = (
        _coerce_int(market_row_dict.get("estimated_pharmacist_count_proxy"))
        if include_staffing
        else None
    )
    metric_by_name["top_chains"] = _hydrate_top_chains(market_row_dict.get("top_chains"))
    score = _score_components(metric_by_name)
    metric_by_name.update(
        {
            "pharmacies_per_100k": score["density_per_100k"],
            "active_medicare_share": score["medicare_share"],
            "mail_order_share": score["mail_order_share"],
            "license_coverage_share": score["license_share"],
            "access_score": score["access_score"],
        }
    )
    return metric_by_name


def _normalize_market_summary(market_row, query: _MarketSummaryQuery) -> dict[str, Any]:
    market_row_dict = dict(market_row)
    market_row_dict.pop("total_count", None)
    return {
        "market_id": str(market_row_dict.get("market_id") or ""),
        "market_scope": str(market_row_dict.get("market_scope") or query.scope),
        "market_name": market_row_dict.get("market_name"),
        "state": market_row_dict.get("state"),
        "city": market_row_dict.get("city"),
        "county": market_row_dict.get("county"),
        "zip_code": market_row_dict.get("zip_code"),
        "metrics": _normalize_market_summary_metrics(
            market_row_dict,
            include_staffing=query.include_staffing,
        ),
    }


async def _query_market_summaries(
    session,
    query: _MarketSummaryQuery,
) -> tuple[int, list[dict[str, Any]]]:
    """Query market summaries."""
    total, market_rows = await _load_market_summary_rows(session, query)
    return total, [_normalize_market_summary(market_row, query) for market_row in market_rows]


async def _fetch_pharmacy_context(session, *, npi: int, as_of: datetime.date) -> dict[str, Any] | None:
    """Fetch pharmacy context."""
    has_partd = await _is_table_available(session, PartDPharmacyActivity.__table__)
    has_license = await _is_table_available(session, PharmacyLicenseRecord.__table__)
    has_other_id = await _is_table_available(session, NPIDataOtherIdentifier.__table__)
    partd_table = _qualified_table_name(PartDPharmacyActivity.__table__)
    license_table = _qualified_table_name(PharmacyLicenseRecord.__table__)
    other_id_table = _qualified_table_name(NPIDataOtherIdentifier.__table__)
    address_table = await _resolve_pharmacy_address_table_sql(session)
    address_zip5_expr = _pharmacy_address_zip5_expr("a", address_table)
    chain_case = _chain_case_sql(
        "LOWER(COALESCE(n.provider_organization_name, '') || ' ' || COALESCE(n.do_business_as_text, ''))"
    )
    partd_cte = ""
    partd_join = ""
    partd_fields = "FALSE AS medicare_active, FALSE AS mail_order, NULL::text AS pharmacy_type"
    if has_partd:
        partd_cte = f"""
, partd_by_npi AS (
    SELECT
        (CASE WHEN npi >= 1000000000 THEN (npi / 10) ELSE npi END)::bigint AS npi9,
        BOOL_OR(medicare_active) FILTER (
            WHERE effective_from <= :as_of
              AND (effective_to IS NULL OR effective_to >= :as_of)
        ) AS medicare_active,
        BOOL_OR(COALESCE(mail_order, FALSE)) FILTER (
            WHERE effective_from <= :as_of
              AND (effective_to IS NULL OR effective_to >= :as_of)
        ) AS mail_order,
        MAX(pharmacy_type) FILTER (
            WHERE effective_from <= :as_of
              AND (effective_to IS NULL OR effective_to >= :as_of)
        ) AS pharmacy_type
    FROM {partd_table}
    GROUP BY 1
)"""
        partd_join = "LEFT JOIN partd_by_npi pd ON pd.npi9 = (a.npi / 10)"
        partd_fields = "COALESCE(pd.medicare_active, FALSE) AS medicare_active, COALESCE(pd.mail_order, FALSE) AS mail_order, pd.pharmacy_type"

    license_cte = ""
    license_join = ""
    license_fields = "FALSE AS has_active_state_license, FALSE AS disciplinary_flag_any"
    if has_license:
        license_cte = f"""
, license_by_npi AS (
    SELECT
        npi::bigint AS npi,
        BOOL_OR(
            LOWER(COALESCE(license_status, '')) = 'active'
            AND (license_expiration_date IS NULL OR license_expiration_date >= :as_of)
        ) AS has_active_state_license,
        BOOL_OR(COALESCE(disciplinary_flag, FALSE)) AS disciplinary_flag_any
    FROM {license_table}
    GROUP BY npi
)"""
        license_join = "LEFT JOIN license_by_npi pl ON pl.npi = a.npi"
        license_fields = (
            "COALESCE(pl.has_active_state_license, FALSE) AS has_active_state_license, "
            "COALESCE(pl.disciplinary_flag_any, FALSE) AS disciplinary_flag_any"
        )

    other_id_cte = ""
    other_id_join = ""
    other_id_fields = (
        "FALSE AS has_ncpdp_identifier, "
        "FALSE AS has_medicaid_identifier, "
        "FALSE AS has_railroad_medicare_identifier, "
        "FALSE AS has_ptan_identifier, "
        "FALSE AS has_clia_identifier, "
        "FALSE AS has_medicare_identifier, "
        "0::int AS other_identifier_count"
    )
    if has_other_id:
        other_id_join = f"""
LEFT JOIN LATERAL (
    SELECT
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%ncpdp%'
        ) AS has_ncpdp_identifier,
        BOOL_OR(
            REGEXP_REPLACE(COALESCE(oi.other_provider_identifier_type_code, ''), '^0+', '') = '1'
        ) AS has_medicaid_identifier,
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%railroad medicare%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%rr medicare%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%medicare railroad%'
        ) AS has_railroad_medicare_identifier,
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%ptan%'
            OR (
                REGEXP_REPLACE(COALESCE(oi.other_provider_identifier_type_code, ''), '^0+', '') = '1'
                AND (
                    LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%medicare%'
                    OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%rr medicare%'
                    OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%railroad medicare%'
                )
            )
        ) AS has_ptan_identifier,
        BOOL_OR(
            LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%clia%'
        ) AS has_clia_identifier,
        BOOL_OR(
            REGEXP_REPLACE(COALESCE(oi.other_provider_identifier_type_code, ''), '^0+', '') = '5'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%medicare%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%ptan%'
            OR LOWER(COALESCE(oi.other_provider_identifier_issuer, '')) LIKE '%rr medicare%'
        ) AS has_medicare_identifier,
        COUNT(*) FILTER (
            WHERE COALESCE(oi.other_provider_identifier, '') <> ''
        )::int AS other_identifier_count
    FROM {other_id_table} oi
    WHERE oi.npi = a.npi
) ib ON TRUE"""
        other_id_fields = (
            "COALESCE(ib.has_ncpdp_identifier, FALSE) AS has_ncpdp_identifier, "
            "COALESCE(ib.has_medicaid_identifier, FALSE) AS has_medicaid_identifier, "
            "COALESCE(ib.has_railroad_medicare_identifier, FALSE) AS has_railroad_medicare_identifier, "
            "COALESCE(ib.has_ptan_identifier, FALSE) AS has_ptan_identifier, "
            "COALESCE(ib.has_clia_identifier, FALSE) AS has_clia_identifier, "
            "COALESCE(ib.has_medicare_identifier, FALSE) AS has_medicare_identifier, "
            "COALESCE(ib.other_identifier_count, 0)::int AS other_identifier_count"
        )

    sql = f"""
WITH pharmacy_codes AS (
    SELECT COALESCE(ARRAY_AGG(int_code), ARRAY[]::integer[]) AS codes
    FROM mrf.nucc_taxonomy
    WHERE classification = 'Pharmacy'
)
{partd_cte}
{license_cte}
{other_id_cte}
SELECT
    a.npi::bigint AS npi,
    a.city_name,
    a.state_name,
    {address_zip5_expr} AS zip_code,
    g.county_name,
    n.provider_organization_name,
    n.do_business_as_text,
    {chain_case} AS chain_name,
    {partd_fields},
    {license_fields},
    {other_id_fields}
FROM {address_table} a
JOIN mrf.npi n ON n.npi = a.npi
LEFT JOIN mrf.geo_zip_lookup g ON g.zip_code = {address_zip5_expr}
{partd_join}
{license_join}
{other_id_join}
CROSS JOIN pharmacy_codes pc
WHERE a.type = 'primary'
  AND a.npi = :npi
  AND a.taxonomy_array && pc.codes
LIMIT 1;
"""
    pharmacy_row = (
        await session.execute(text(sql), {"npi": npi, "as_of": as_of})
    ).mappings().first()
    if not pharmacy_row:
        return None
    pharmacy_row_dict = dict(pharmacy_row)
    return {
        "npi": _coerce_int(pharmacy_row_dict.get("npi")),
        "name": pharmacy_row_dict.get("provider_organization_name")
        or pharmacy_row_dict.get("do_business_as_text"),
        "chain_name": pharmacy_row_dict.get("chain_name"),
        "state": pharmacy_row_dict.get("state_name"),
        "city": pharmacy_row_dict.get("city_name"),
        "county": pharmacy_row_dict.get("county_name"),
        "zip_code": pharmacy_row_dict.get("zip_code"),
        "medicare_active": bool(pharmacy_row_dict.get("medicare_active")),
        "mail_order": bool(pharmacy_row_dict.get("mail_order")),
        "pharmacy_type": pharmacy_row_dict.get("pharmacy_type"),
        "has_active_state_license": bool(
            pharmacy_row_dict.get("has_active_state_license")
        ),
        "disciplinary_flag_any": bool(pharmacy_row_dict.get("disciplinary_flag_any")),
        "has_ncpdp_identifier": bool(pharmacy_row_dict.get("has_ncpdp_identifier")),
        "has_medicaid_identifier": bool(
            pharmacy_row_dict.get("has_medicaid_identifier")
        ),
        "has_railroad_medicare_identifier": bool(
            pharmacy_row_dict.get("has_railroad_medicare_identifier")
        ),
        "has_ptan_identifier": bool(pharmacy_row_dict.get("has_ptan_identifier")),
        "has_clia_identifier": bool(pharmacy_row_dict.get("has_clia_identifier")),
        "has_medicare_identifier": bool(
            pharmacy_row_dict.get("has_medicare_identifier")
        ),
        "other_identifier_count": _coerce_int(
            pharmacy_row_dict.get("other_identifier_count")
        ),
    }


@blueprint.get("/pharmacies/markets")
async def list_pharmacy_markets(request):
    """List pharmacy market summaries under the requested filters."""
    session = _get_session(request)
    args = request.args
    args.get("page")
    args.get("page_size")
    args.get("limit")
    args.get("offset")
    args.get("start")
    pagination = parse_pagination(
        args,
        default_limit=DEFAULT_PAGE_SIZE,
        max_limit=MAX_PAGE_SIZE,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )
    state = args.get("state")
    city = args.get("city")
    county = args.get("county")
    zip_code = _parse_optional_zip(args.get("zip"))
    default_scope = "city"
    if zip_code:
        default_scope = "zip"
    elif county:
        default_scope = "county"
    elif state and not city:
        default_scope = "state"

    scope = _parse_scope(args.get("scope"), default=default_scope)
    sort = _parse_sort(args.get("sort"), default="access_score")
    order = _parse_order(args.get("order"), default="desc")
    as_of = _parse_date_param(args.get("as_of"), "as_of") or datetime.date.today()
    include_staffing = _is_boolean_parameter_enabled(args.get("include_staffing"), default=False)
    chain = _canonical_chain(args.get("chain"))

    total, market_items = await _query_market_summaries(
        session,
        _MarketSummaryQuery(
            scope=scope,
            sort=sort,
            order=order,
            as_of=as_of,
            include_staffing=include_staffing,
            limit=pagination.limit,
            offset=pagination.offset,
            state=(str(state).strip().upper() if state else None),
            city=(str(city).strip().upper() if city else None),
            county=(str(county).strip() if county else None),
            zip_code=zip_code,
            chain=chain,
        ),
    )
    return response.json(
        {
            "as_of": as_of.isoformat(),
            "scope": scope,
            "sort": sort,
            "order": order,
            "page": pagination.page,
            "page_size": pagination.limit,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "total": total,
            "filters": {
                "state": state.upper() if state else None,
                "city": city,
                "county": county,
                "zip": zip_code,
                "chain": chain,
            },
            "items": market_items,
            "methodology": _pharmacy_market_methodology(as_of, include_staffing),
        }
    )


@blueprint.get("/pharmacies/markets/<market_id>")
async def get_pharmacy_market_by_id(request, market_id):
    """Return one pharmacy market summary by stable market identifier."""
    session = _get_session(request)
    market_id = str(market_id or "").strip()
    if not _MARKET_ID_PATTERN.fullmatch(market_id):
        raise InvalidUsage("market_id must follow one of: state:<...>, city:<...>, county:<...>, zip:<...>")
    scope = market_id.split(":", 1)[0].lower()
    if scope not in _ALLOWED_SCOPES:
        raise InvalidUsage("Unsupported market_id scope")
    as_of = _parse_date_param(request.args.get("as_of"), "as_of") or datetime.date.today()
    include_staffing = _is_boolean_parameter_enabled(request.args.get("include_staffing"), default=False)
    total, market_items = await _query_market_summaries(
        session,
        _MarketSummaryQuery(
            scope=scope,
            sort="access_score",
            order="desc",
            as_of=as_of,
            include_staffing=include_staffing,
            limit=1,
            offset=0,
            market_id=market_id,
        ),
    )
    if total <= 0 or not market_items:
        raise NotFound("Unknown market_id")
    return response.json(
        {
            "as_of": as_of.isoformat(),
            "item": market_items[0],
            "methodology": _pharmacy_market_methodology(as_of, include_staffing),
        }
    )


def _parse_pharmacy_access_rankings_pagination(args):
    return parse_pagination(
        args,
        default_limit=DEFAULT_PAGE_SIZE,
        max_limit=MAX_PAGE_SIZE,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )


def _rank_pharmacy_access_markets(market_items, offset: int) -> list[dict]:
    ranked_markets = []
    for rank, market_item in enumerate(market_items, start=offset + 1):
        ranked_market_dict = dict(market_item)
        ranked_market_dict["rank"] = rank
        ranked_markets.append(ranked_market_dict)
    return ranked_markets


@blueprint.get("/pharmacies/rankings/access")
async def list_pharmacy_access_rankings(request):
    """Rank pharmacy markets by the selected access metric."""
    session = _get_session(request)
    args = request.args
    args.get("page")
    args.get("page_size")
    args.get("limit")
    args.get("offset")
    args.get("start")
    pagination = _parse_pharmacy_access_rankings_pagination(args)
    state = args.get("state")
    city = args.get("city")
    county = args.get("county")
    zip_code = _parse_optional_zip(args.get("zip"))
    default_scope = "city"
    if zip_code:
        default_scope = "zip"
    elif county:
        default_scope = "county"
    elif state and not city:
        default_scope = "state"

    scope = _parse_scope(args.get("scope"), default=default_scope)
    as_of = _parse_date_param(args.get("as_of"), "as_of") or datetime.date.today()
    include_staffing = _is_boolean_parameter_enabled(args.get("include_staffing"), default=False)
    chain = _canonical_chain(args.get("chain"))
    total, market_items = await _query_market_summaries(
        session,
        _MarketSummaryQuery(
            scope=scope,
            sort="access_score",
            order="desc",
            as_of=as_of,
            include_staffing=include_staffing,
            limit=pagination.limit,
            offset=pagination.offset,
            state=(str(state).strip().upper() if state else None),
            city=(str(city).strip().upper() if city else None),
            county=(str(county).strip() if county else None),
            zip_code=zip_code,
            chain=chain,
        ),
    )
    ranked_markets = _rank_pharmacy_access_markets(
        market_items, pagination.offset
    )
    return response.json(
        {
            "as_of": as_of.isoformat(),
            "scope": scope,
            "page": pagination.page,
            "page_size": pagination.limit,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "total": total,
            "items": ranked_markets,
            "methodology": _pharmacy_market_methodology(as_of, include_staffing),
        }
    )


@blueprint.get("/pharmacies/chains/summary")
async def get_pharmacy_chain_summary(request):
    """Return pharmacy-chain coverage and access summary metrics."""
    session = _get_session(request)
    request.args.get("name_like")
    names = _extract_name_like_filters(request.args)
    if not names:
        raise InvalidUsage("At least one name_like value is required")
    include_states = _is_boolean_parameter_enabled(request.args.get("include_states"), default=True)
    summary, histogram_entries, states, staffing_helper_available = await _query_chain_summary(
        session,
        names=names,
        include_states=include_states,
    )
    return response.json(
        {
            "filters": {
                "name_like": names,
                "include_states": include_states,
            },
            "summary": {
                "pharmacy_npi_count": _coerce_int(summary.get("pharmacy_npi_count")),
                "insured_pharmacy_npi_count": _coerce_int(summary.get("insured_pharmacy_npi_count")),
                "active_pharmacy_count": _coerce_int(summary.get("active_pharmacy_count")),
                "pharmacist_count": _coerce_int(summary.get("pharmacist_count")),
            },
            "histogram": [
                {
                    "label": str(histogram_entry.get("label")),
                    "count": _coerce_int(histogram_entry.get("count")),
                }
                for histogram_entry in histogram_entries
                if histogram_entry.get("label") is not None
            ],
            "states": [
                {
                    "state": str(state_entry.get("state") or "").upper(),
                    "active_pharmacy_count": _coerce_int(
                        state_entry.get("active_pharmacy_count")
                    ),
                    "pharmacy_npi_count": _coerce_int(
                        state_entry.get("pharmacy_npi_count")
                    ),
                    "insured_pharmacy_npi_count": _coerce_int(
                        state_entry.get("insured_pharmacy_npi_count")
                    ),
                    "pharmacist_count": _coerce_int(
                        state_entry.get("pharmacist_count")
                    ),
                }
                for state_entry in states
                if state_entry.get("state")
            ],
            "methodology": _pharmacy_chain_summary_methodology(
                staffing_helper_available=staffing_helper_available
            ),
        }
    )


@blueprint.get("/pharmacies/state-stats", name="pharmacy_state_stats")
async def get_pharmacy_state_stats(request):
    """Return state-level pharmacy access summary metrics."""
    session = _get_session(request)
    states, staffing_helper_available = await _query_pharmacy_state_stats(session)
    return response.json(
        _build_pharmacy_state_stats_payload(
            states,
            staffing_helper_available=staffing_helper_available,
        )
    )


@blueprint.get("/pharmacies/<npi>/market-context")
async def get_pharmacy_market_context(request, npi):
    """Return market context for one pharmacy NPI."""
    session = _get_session(request)
    parsed_npi = _parse_npi(npi)
    as_of = _parse_date_param(request.args.get("as_of"), "as_of") or datetime.date.today()
    include_staffing = _is_boolean_parameter_enabled(request.args.get("include_staffing"), default=False)

    pharmacy = await _fetch_pharmacy_context(session, npi=parsed_npi, as_of=as_of)
    if not pharmacy:
        raise NotFound("Pharmacy not found for supplied NPI")

    scope = "city"
    if pharmacy.get("zip_code"):
        scope = "zip"
    if pharmacy.get("city") and pharmacy.get("state"):
        scope = "city"
    elif pharmacy.get("county") and pharmacy.get("state"):
        scope = "county"
    elif pharmacy.get("state"):
        scope = "state"

    total, market_items = await _query_market_summaries(
        session,
        _MarketSummaryQuery(
            scope=scope,
            sort="access_score",
            order="desc",
            as_of=as_of,
            include_staffing=include_staffing,
            limit=1,
            offset=0,
            state=(str(pharmacy.get("state") or "").strip().upper() or None),
            city=(str(pharmacy.get("city") or "").strip().upper() or None),
            county=(str(pharmacy.get("county") or "").strip() or None),
            zip_code=(str(pharmacy.get("zip_code") or "").strip() or None),
        ),
    )
    market = market_items[0] if total > 0 and market_items else None
    return response.json(
        {
            "npi": parsed_npi,
            "as_of": as_of.isoformat(),
            "market_scope": scope,
            "pharmacy": pharmacy,
            "market": market,
            "methodology": _pharmacy_market_methodology(as_of, include_staffing),
        }
    )
