# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import sanic.exceptions
from asyncpg import UndefinedTableError
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import Float, and_, case, cast, func, or_, select
from sqlalchemy.exc import ProgrammingError

from api.for_human import attributes_labels, benefits_labels
from db.connection import db as sa_db
from db.models import (GeoZipLookup, ImportLog, Issuer, Plan, PlanAttributes,
                       PlanBenefits, PlanDrugStats, PlanFormulary,
                       PlanNetworkTierRaw, PlanNPIRaw, PlanPrices,
                       PlanRatingAreas, PlanSearchSummary)
from db.tiger_models import ZipState

plan_table = Plan.__table__
plan_attributes_table = PlanAttributes.__table__
plan_benefits_table = PlanBenefits.__table__
plan_formulary_table = PlanFormulary.__table__
plan_drug_stats_table = PlanDrugStats.__table__
plan_network_tier_table = PlanNetworkTierRaw.__table__
plan_npi_table = PlanNPIRaw.__table__
plan_prices_table = PlanPrices.__table__
geo_zip_lookup_table = GeoZipLookup.__table__
plan_rating_areas_table = PlanRatingAreas.__table__
zip_state_table = ZipState.__table__
import_log_table = ImportLog.__table__
issuer_table = Issuer.__table__
plan_search_summary_table = PlanSearchSummary.__table__


SUMMARY_BOOLEAN_COLUMNS = {
    "has_adult_dental": plan_search_summary_table.c.has_adult_dental,
    "has_child_dental": plan_search_summary_table.c.has_child_dental,
    "has_adult_vision": plan_search_summary_table.c.has_adult_vision,
    "has_child_vision": plan_search_summary_table.c.has_child_vision,
    "telehealth_supported": plan_search_summary_table.c.telehealth_supported,
    "is_hsa": plan_search_summary_table.c.is_hsa,
    "is_dental_only": plan_search_summary_table.c.is_dental_only,
    "is_catastrophic": plan_search_summary_table.c.is_catastrophic,
    "is_on_exchange": plan_search_summary_table.c.is_on_exchange,
    "is_off_exchange": plan_search_summary_table.c.is_off_exchange,
}

PRICE_RATE_COLUMNS = (
    "individual_rate",
    "individual_tobacco_rate",
    "couple",
    "primary_subscriber_and_one_dependent",
    "primary_subscriber_and_two_dependents",
    "primary_subscriber_and_three_or_more_dependents",
    "couple_and_one_dependent",
    "couple_and_two_dependents",
    "couple_and_three_or_more_dependents",
)

blueprint = Blueprint("plan", url_prefix="/plan", version=1)


def _parse_bool(value: Optional[str], param_name: str) -> Optional[bool]:
    if value in (None, "", "null"):
        return None
    if isinstance(value, bool):
        return value
    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    raise InvalidUsage(f"Parameter '{param_name}' must be boolean-like")


def _parse_float(value: Optional[str], param_name: str) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be numeric") from exc


def _append_filter(applied_filters: Dict[str, Any], key: str, value: Any):
    if value is None or value == "":
        return
    applied_filters[key] = value


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _row_to_dict(row):
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        try:
            return dict(mapping)
        except (TypeError, ValueError):
            pass
    if hasattr(row, "keys") and hasattr(row, "__getitem__"):
        try:
            return {key: row[key] for key in row.keys()}
        except (TypeError, ValueError):
            pass
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except (TypeError, ValueError):
        return {}


def _result_rows(result):
    if hasattr(result, "all"):
        try:
            return result.all()
        except TypeError:
            pass
    return list(result)


def _result_scalar(result):
    if hasattr(result, "scalar"):
        return result.scalar()
    rows = _result_rows(result)
    if not rows:
        return None
    first = rows[0]
    if isinstance(first, (tuple, list)):
        return first[0]
    if isinstance(first, dict):
        return next(iter(first.values()))
    return first




def _plan_level_plan_id_column(table):
    return func.coalesce(table.c.plan_id, func.left(func.coalesce(table.c.full_plan_id, ""), 14))



def _collect_price_bounds(result):
    bounds = {}
    for row in _result_rows(result):
        row_dict = _row_to_dict(row)
        plan_id = row_dict.get("plan_id")
        year = row_dict.get("year")
        if not plan_id or year is None:
            continue
        key = (plan_id, year)
        values = []
        for column in PRICE_RATE_COLUMNS:
            value = row_dict.get(column)
            if value is None:
                continue
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                continue
        if not values:
            continue
        min_val = min(values)
        max_val = max(values)
        entry = bounds.setdefault(key, {"min": min_val, "max": max_val})
        entry["min"] = min(entry["min"], min_val)
        entry["max"] = max(entry["max"], max_val)
    return bounds


def _default_boolean_facets():
    return {name: {"true": 0, "false": 0, "null": 0} for name in SUMMARY_BOOLEAN_COLUMNS}


def _empty_facets():
    return {
        "plan_types": [],
        "metal_levels": [],
        "csr_variations": [],
        "boolean_filters": _default_boolean_facets(),
    }


async def _compute_facets(session, filtered_plans):
    def _summary_join():
        return filtered_plans.join(
            plan_search_summary_table,
            and_(
                plan_search_summary_table.c.plan_id == filtered_plans.c.plan_id,
                plan_search_summary_table.c.year == filtered_plans.c.year,
            ),
        )

    async def _grouped_counts(column):
        stmt = (
            select(column.label("value"), func.count().label("count"))
            .select_from(_summary_join())
            .group_by(column)
            .order_by(func.count().desc(), column)
        )
        result = await session.execute(stmt)
        entries = []
        for row in _result_rows(result):
            row_dict = _row_to_dict(row)
            value = row_dict.get("value")
            if value in (None, ""):
                continue
            entries.append({"value": value, "count": int(row_dict.get("count") or 0)})
        return entries

    plan_type_facets = await _grouped_counts(plan_search_summary_table.c.plan_type)
    metal_level_facets = await _grouped_counts(plan_search_summary_table.c.metal_level)
    csr_facets = await _grouped_counts(plan_search_summary_table.c.csr_variation)

    boolean_expressions = []
    for name, column in SUMMARY_BOOLEAN_COLUMNS.items():
        boolean_expressions.extend(
            [
                func.sum(case((column.is_(True), 1), else_=0)).label(f"{name}_true"),
                func.sum(case((column.is_(False), 1), else_=0)).label(f"{name}_false"),
                func.sum(case((column.is_(None), 1), else_=0)).label(f"{name}_null"),
            ]
        )
    boolean_stmt = select(*boolean_expressions).select_from(_summary_join())
    boolean_result = await session.execute(boolean_stmt)
    boolean_rows = _result_rows(boolean_result)
    boolean_row = _row_to_dict(boolean_rows[0]) if boolean_rows else {}
    boolean_facets = _default_boolean_facets()
    for name in SUMMARY_BOOLEAN_COLUMNS:
        boolean_facets[name] = {
            "true": int(boolean_row.get(f"{name}_true") or 0),
            "false": int(boolean_row.get(f"{name}_false") or 0),
            "null": int(boolean_row.get(f"{name}_null") or 0),
        }

    return {
        "plan_types": plan_type_facets,
        "metal_levels": metal_level_facets,
        "csr_variations": csr_facets,
        "boolean_filters": boolean_facets,
    }


def _normalize_attribute_map(attribute_map):
    cleaned = {}
    if not attribute_map:
        return cleaned
    for name, value in attribute_map.items():
        entry = {}
        if isinstance(value, dict):
            entry.update(value)
        else:
            entry["attr_value"] = value
        if "attr_value" not in entry:
            entry["attr_value"] = None
        if name in attributes_labels and "human_attr_name" not in entry:
            entry["human_attr_name"] = attributes_labels[name]
        cleaned[name] = entry
    return cleaned


def _get_list_param(args, name):
    values = []
    getlist = getattr(args, "getlist", None)
    if callable(getlist):
        raw_values = getlist(name)
    else:
        raw = args.get(name)
        if raw is None:
            raw_values = []
        elif isinstance(raw, (list, tuple)):
            raw_values = list(raw)
        else:
            raw_values = [raw]
    for raw_value in raw_values:
        if raw_value is None:
            continue
        if isinstance(raw_value, (list, tuple)):
            tokens = raw_value
        else:
            tokens = str(raw_value).split(",")
        for token in tokens:
            cleaned = token.strip()
            if cleaned:
                values.append(cleaned)
    return values


def _summary_attributes_to_dict(payload):
    if payload is None:
        return {}
    data = payload
    if isinstance(payload, str):
        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
            return {}
    if not isinstance(data, list):
        return {}
    mapping = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        name = entry.get("attr_name")
        if not name:
            continue
        mapping[name] = {"attr_value": entry.get("attr_value")}
    return mapping


def _summary_benefits_to_dict(payload):
    if payload is None:
        return {}
    data = payload
    if isinstance(payload, str):
        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
            return {}
    if not isinstance(data, list):
        return {}
    mapping = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        name = entry.get("benefit_name")
        if not name:
            continue
        mapping[name] = dict(entry)
        if name in benefits_labels:
            mapping[name]["human_attr_name"] = benefits_labels[name]
    return mapping


async def _states_for_zip(session, zip_code: str) -> list[str]:
    digits = "".join(ch for ch in zip_code if ch.isdigit())
    if len(digits) < 3:
        return []
    padded_zip = digits.rjust(5, "0")
    geo_stmt = (
        select(func.distinct(geo_zip_lookup_table.c.state))
        .where(geo_zip_lookup_table.c.zip_code == padded_zip)
    )
    try:
        geo_result = await session.execute(geo_stmt)
    except ProgrammingError as exc:
        if not isinstance(getattr(exc, "orig", None), UndefinedTableError):
            raise
        geo_states = []
    else:
        geo_states = sorted(
            value for value in (row[0] for row in _result_rows(geo_result)) if value
        )
        if geo_states:
            return geo_states

    zip3 = digits[:3]
    stmt = (
        select(func.distinct(plan_rating_areas_table.c.state))
        .where(plan_rating_areas_table.c.zip3 == zip3)
    )
    result = await session.execute(stmt)
    states = sorted(
        value for value in (row[0] for row in _result_rows(result)) if value
    )
    if states:
        return states

    if len(digits) < 5:
        return []

    fallback_stmt = (
        select(zip_state_table.c.stusps)
        .where(zip_state_table.c.zip == padded_zip)
    )
    try:
        fallback_result = await session.execute(fallback_stmt)
    except ProgrammingError:
        return []
    return sorted(
        value for value in (row[0] for row in _result_rows(fallback_result)) if value
    )




@blueprint.get("/")
async def index_status(request):
    session = _get_session(request)

    plan_count_res = await session.execute(
        select(sa_db.func.count(plan_table.c.plan_id))
    )
    plan_count = _result_scalar(plan_count_res) or 0

    import_error_res = await session.execute(
        select(sa_db.func.count(import_log_table.c.checksum))
    )
    import_errors = _result_scalar(import_error_res) or 0

    network_count_res = await session.execute(
        select(sa_db.func.count(sa_db.func.distinct(plan_npi_table.c.checksum_network)))
    )
    network_count = _result_scalar(network_count_res) or 0

    data = {
        "date": datetime.utcnow().isoformat(),
        "release": request.app.config.get("RELEASE"),
        "environment": request.app.config.get("ENVIRONMENT"),
        "plan_count": plan_count,
        "plans_network_count": network_count,
        "import_log_errors": import_errors,
    }
    return response.json(data, default=str)


@blueprint.get("/all")
async def all_plans(request):
    session = _get_session(request)
    result = await session.execute(select(plan_table))
    rows = [_row_to_dict(row) for row in _result_rows(result)]
    return response.json(rows, default=str)


@blueprint.get("/all/variants")
async def all_plans_variants(request):
    session = _get_session(request)
    limit = request.args.get("limit")
    offset = request.args.get("offset")

    stmt = select(
        plan_table.c.marketing_name,
        plan_table.c.plan_id,
        plan_attributes_table.c.full_plan_id,
        plan_table.c.year,
    ).select_from(
        plan_table.join(
            plan_attributes_table,
            and_(
                plan_table.c.plan_id == plan_attributes_table.c.plan_id,
                plan_table.c.year == plan_attributes_table.c.year,
            ),
        )
    )

    if limit:
        stmt = stmt.limit(int(limit))
        if offset:
            stmt = stmt.offset(int(offset))

    result = await session.execute(stmt)
    data = []
    for row in _result_rows(result):
        row_dict = _row_to_dict(row)
        data.append(
            {
                "marketing_name": row_dict.get("marketing_name"),
                "plan_id": row_dict.get("plan_id"),
                "full_plan_id": row_dict.get("full_plan_id"),
                "year": row_dict.get("year"),
            }
        )
    return response.json(data, default=str)


async def _fetch_network_entry(session, checksum):
    stmt = (
        select(
            plan_network_tier_table.c.plan_id,
            plan_network_tier_table.c.year,
            plan_network_tier_table.c.checksum_network,
            plan_network_tier_table.c.network_tier,
            issuer_table.c.issuer_id,
            issuer_table.c.issuer_name,
            issuer_table.c.issuer_marketing_name,
            issuer_table.c.state.label("issuer_state"),
        )
        .select_from(
            plan_network_tier_table.join(
                issuer_table,
                issuer_table.c.issuer_id == plan_network_tier_table.c.issuer_id,
            )
        )
        .where(plan_network_tier_table.c.checksum_network == checksum)
    )

    result = await session.execute(stmt)
    rows = _result_rows(result)
    if not rows:
        return None

    plans = []
    seen = set()
    first_row = _row_to_dict(rows[0])
    for row in rows:
        row_dict = _row_to_dict(row)
        key = (row_dict.get("plan_id"), row_dict.get("year"))
        if key not in seen:
            plans.append({"plan_id": row_dict.get("plan_id"), "year": row_dict.get("year")})
            seen.add(key)

    issuer_marketing_name = first_row.get("issuer_marketing_name") or ""
    network_tier = first_row.get("network_tier") or "N/A"
    display_name = network_tier.replace("-", " ").replace("  ", " ")

    return {
        "plans": plans,
        "checksum": first_row.get("checksum_network"),
        "network_tier": display_name,
        "issuer": first_row.get("issuer_id"),
        "issuer_name": first_row.get("issuer_name"),
        "issuer_marketing_name": issuer_marketing_name,
        "issuer_display_name": issuer_marketing_name or first_row.get("issuer_name"),
        "issuer_state": first_row.get("issuer_state"),
    }


@blueprint.get("/network/id/<checksum>")
async def get_network_by_checksum(request, checksum):
    session = _get_session(request)
    entry = await _fetch_network_entry(session, int(checksum))
    if entry is None:
        raise sanic.exceptions.NotFound
    return response.json(entry, default=str)


@blueprint.get("/network/multiple/<checksums>")
async def get_networks_by_checksums(request, checksums):
    session = _get_session(request)
    values = [value.strip() for value in checksums.split(",") if value.strip()]
    payload = []
    seen = set()

    for value in values:
        try:
            checksum = int(value)
        except ValueError:
            continue
        if checksum in seen:
            continue
        seen.add(checksum)
        entry = await _fetch_network_entry(session, checksum)
        if entry:
            payload.append(entry)

    if not payload:
        raise sanic.exceptions.NotFound

    return response.json(payload, default=str)


@blueprint.get("/network/autocomplete")
async def get_autocomplete_list(request):
    session = _get_session(request)
    text = request.args.get("query")
    if not text:
        return response.json({"plans": []}, default=str)

    state = request.args.get("state")
    zip_code = request.args.get("zip_code")

    stmt = select(plan_table).where(
        or_(
            plan_table.c.marketing_name.ilike(f"%{text.lower()}%"),
            plan_table.c.plan_id.ilike(f"%{text.lower()}%"),
        )
    )

    if state:
        stmt = stmt.where(plan_table.c.state == state)
    elif zip_code:
        zip_states = await _states_for_zip(session, zip_code)
        if not zip_states:
            return response.json({"plans": []}, default=str)
        stmt = stmt.where(plan_table.c.state.in_(zip_states))

    stmt = stmt.limit(100)

    result = await session.execute(stmt)
    rows = _result_rows(result)
    if not rows:
        return response.json({"plans": []}, default=str)

    plans = {}
    plan_ids = []
    for row in rows:
        row_dict = _row_to_dict(row)
        plan_id = row_dict.get("plan_id")
        if plan_id not in plans:
            row_copy = dict(row_dict)
            row_copy["network_checksum"] = {}
            plans[plan_id] = row_copy
            plan_ids.append(plan_id)

    network_stmt = select(
        plan_network_tier_table.c.plan_id,
        plan_network_tier_table.c.checksum_network,
        plan_network_tier_table.c.network_tier,
    ).where(plan_network_tier_table.c.plan_id.in_(plan_ids))

    network_result = await session.execute(network_stmt)
    for plan_id, checksum, tier in _result_rows(network_result):
        if plan_id in plans:
            plans[plan_id]["network_checksum"][str(checksum)] = tier

    filtered = [plan for plan in plans.values() if plan["network_checksum"]]
    return response.json({"plans": filtered}, default=str)


@blueprint.get("/search", name="find_a_plan")
async def find_a_plan(request):
    args = request.args
    age_raw = args.get("age")
    rating_area = args.get("rating_area")
    zip_code = (args.get("zip_code") or "").strip()
    year_raw = args.get("year")
    limit_raw = args.get("limit") or 100
    page_raw = args.get("page") or 1
    order = (args.get("order") or "asc").lower()
    order_by = (args.get("order_by") or "plan_id").lower()
    issuer_id_raw = args.get("issuer_id")
    plan_id_filter = (args.get("plan_id") or "").strip()
    query_text = (args.get("q") or "").strip()
    state = (args.get("state") or "").strip().upper()

    bool_filter_inputs = {
        "has_adult_dental": _parse_bool(args.get("has_adult_dental"), "has_adult_dental"),
        "has_child_dental": _parse_bool(args.get("has_child_dental"), "has_child_dental"),
        "has_adult_vision": _parse_bool(args.get("has_adult_vision"), "has_adult_vision"),
        "has_child_vision": _parse_bool(args.get("has_child_vision"), "has_child_vision"),
        "telehealth_supported": _parse_bool(args.get("telehealth_supported"), "telehealth_supported"),
        "is_hsa": _parse_bool(args.get("is_hsa"), "is_hsa"),
        "is_dental_only": _parse_bool(args.get("is_dental_only"), "is_dental_only"),
        "is_catastrophic": _parse_bool(args.get("is_catastrophic"), "is_catastrophic"),
        "is_on_exchange": _parse_bool(args.get("is_on_exchange"), "is_on_exchange"),
        "is_off_exchange": _parse_bool(args.get("is_off_exchange"), "is_off_exchange"),
    }

    deductible_min = _parse_float(args.get("deductible_min"), "deductible_min")
    deductible_max = _parse_float(args.get("deductible_max"), "deductible_max")
    moop_min = _parse_float(args.get("moop_min"), "moop_min")
    moop_max = _parse_float(args.get("moop_max"), "moop_max")
    premium_min = _parse_float(args.get("premium_min"), "premium_min")
    premium_max = _parse_float(args.get("premium_max"), "premium_max")
    plan_types = [value.upper() for value in _get_list_param(args, "plan_types")]
    metal_levels = _get_list_param(args, "metal_levels")
    csr_variations = _get_list_param(args, "csr_variations")

    try:
        limit = int(limit_raw)
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 200))

    try:
        page = int(page_raw)
    except (TypeError, ValueError):
        page = 1
    page = max(page, 1)
    offset = (page - 1) * limit

    year = None
    if year_raw:
        try:
            year = int(year_raw)
        except ValueError as exc:
            raise sanic.exceptions.BadRequest from exc

    age = None
    if age_raw:
        try:
            age = int(age_raw)
        except ValueError as exc:
            raise sanic.exceptions.BadRequest from exc

    issuer_id = None
    if issuer_id_raw:
        try:
            issuer_id = int(issuer_id_raw)
        except ValueError as exc:
            raise sanic.exceptions.BadRequest from exc

    issuer_ids_param = []
    for value in _get_list_param(args, "issuer_ids"):
        try:
            issuer_ids_param.append(int(value))
        except ValueError as exc:
            raise sanic.exceptions.BadRequest("issuer_ids must be integers") from exc

    if state and len(state) != 2:
        raise sanic.exceptions.BadRequest("state must be a 2-letter code")

    session = _get_session(request)

    applied_filters: Dict[str, Any] = {}
    warnings: List[Dict[str, str]] = []
    facets = _empty_facets()

    plan_filters = []
    summary_filters = []

    if state:
        plan_filters.append(plan_table.c.state == state)
        _append_filter(applied_filters, "state", state)

    if year is not None:
        plan_filters.append(plan_table.c.year == year)
        _append_filter(applied_filters, "year", year)

    issuer_filters = []
    if issuer_id is not None:
        issuer_filters.append(issuer_id)
    issuer_filters.extend(issuer_ids_param)
    issuer_filters = sorted(set(issuer_filters))
    if issuer_filters:
        plan_filters.append(plan_table.c.issuer_id.in_(issuer_filters))
        if len(issuer_filters) == 1:
            _append_filter(applied_filters, "issuer_id", issuer_filters[0])
        else:
            _append_filter(applied_filters, "issuer_ids", issuer_filters)

    if plan_id_filter:
        plan_filters.append(plan_table.c.plan_id.ilike(f"%{plan_id_filter}%"))
        _append_filter(applied_filters, "plan_id", plan_id_filter)

    if query_text:
        plan_filters.append(plan_table.c.marketing_name.ilike(f"%{query_text}%"))
        _append_filter(applied_filters, "q", query_text)

    if zip_code:
        _append_filter(applied_filters, "zip_code", zip_code)
        zip_states = await _states_for_zip(session, zip_code)
        if not zip_states:
            warnings.append(
                {"code": "zip_not_found", "message": f"Could not resolve states for ZIP '{zip_code}'"}
            )
            return response.json(
                {
                    "total": 0,
                    "results": [],
                    "issuers": [],
                    "facets": facets,
                    "applied_filters": applied_filters,
                    "warnings": warnings,
                },
                default=str,
            )
        plan_filters.append(plan_table.c.state.in_(zip_states))

    if rating_area:
        _append_filter(applied_filters, "rating_area", rating_area)
    if age is not None:
        _append_filter(applied_filters, "age", age)
    _append_filter(applied_filters, "limit", limit)
    _append_filter(applied_filters, "page", page)
    _append_filter(applied_filters, "order", order)
    _append_filter(applied_filters, "order_by", order_by)

    price_filter_required = bool(age is not None or rating_area)
    if price_filter_required:
        price_conditions = [
            plan_prices_table.c.plan_id == plan_table.c.plan_id,
            plan_prices_table.c.year == plan_table.c.year,
        ]
        if rating_area:
            price_conditions.append(plan_prices_table.c.rating_area_id == rating_area)
        if age is not None:
            price_conditions.append(plan_prices_table.c.min_age <= age)
            price_conditions.append(plan_prices_table.c.max_age >= age)
        price_subquery = select(plan_prices_table.c.plan_id).where(and_(*price_conditions))
        plan_filters.append(price_subquery.exists())

    for key, value in bool_filter_inputs.items():
        if value is None:
            continue
        column = SUMMARY_BOOLEAN_COLUMNS.get(key)
        if column is None:
            continue
        summary_filters.append(column.is_(True) if value else column.is_(False))
        _append_filter(applied_filters, key, value)

    if deductible_min is not None:
        summary_filters.append(plan_search_summary_table.c.deductible_inn_individual >= deductible_min)
        _append_filter(applied_filters, "deductible_min", deductible_min)
    if deductible_max is not None:
        summary_filters.append(plan_search_summary_table.c.deductible_inn_individual <= deductible_max)
        _append_filter(applied_filters, "deductible_max", deductible_max)
    if moop_min is not None:
        summary_filters.append(plan_search_summary_table.c.moop_inn_individual >= moop_min)
        _append_filter(applied_filters, "moop_min", moop_min)
    if moop_max is not None:
        summary_filters.append(plan_search_summary_table.c.moop_inn_individual <= moop_max)
        _append_filter(applied_filters, "moop_max", moop_max)
    if premium_min is not None:
        summary_filters.append(plan_search_summary_table.c.premium_min >= premium_min)
        _append_filter(applied_filters, "premium_min", premium_min)
    if premium_max is not None:
        summary_filters.append(plan_search_summary_table.c.premium_max <= premium_max)
        _append_filter(applied_filters, "premium_max", premium_max)
    if plan_types:
        summary_filters.append(plan_search_summary_table.c.plan_type.in_(plan_types))
        _append_filter(applied_filters, "plan_types", plan_types)
    if metal_levels:
        lowered_levels = [value.lower() for value in metal_levels]
        summary_filters.append(func.lower(plan_search_summary_table.c.metal_level).in_(lowered_levels))
        _append_filter(applied_filters, "metal_levels", metal_levels)
    if csr_variations:
        lowered_csr = [value.lower() for value in csr_variations]
        summary_filters.append(func.lower(plan_search_summary_table.c.csr_variation).in_(lowered_csr))
        _append_filter(applied_filters, "csr_variations", csr_variations)

    join_condition = and_(
        plan_table.c.plan_id == plan_search_summary_table.c.plan_id,
        plan_table.c.year == plan_search_summary_table.c.year,
    )
    base_join = plan_table.join(plan_search_summary_table, join_condition)

    combined_filters = plan_filters + summary_filters
    filtered_plan_stmt = select(plan_table.c.plan_id.label("plan_id"), plan_table.c.year.label("year")).select_from(
        base_join
    )
    if combined_filters:
        filtered_plan_stmt = filtered_plan_stmt.where(and_(*combined_filters))
    filtered_plans = filtered_plan_stmt.distinct().cte("filtered_plans")

    total_result = await session.execute(select(func.count()).select_from(filtered_plans))
    total = _result_scalar(total_result) or 0
    if total > 0:
        facets = await _compute_facets(session, filtered_plans)

    order_map = {
        "plan_id": plan_table.c.plan_id,
        "marketing_name": plan_table.c.marketing_name,
        "state": plan_table.c.state,
        "year": plan_table.c.year,
        "deductible": plan_search_summary_table.c.deductible_inn_individual,
        "moop": plan_search_summary_table.c.moop_inn_individual,
        "premium_min": plan_search_summary_table.c.premium_min,
        "premium_max": plan_search_summary_table.c.premium_max,
    }
    order_column = order_map.get(order_by, plan_table.c.plan_id)
    order_expression = order_column.desc() if order == "desc" else order_column.asc()
    secondary_order = [] if order_column is plan_table.c.plan_id else [plan_table.c.plan_id.asc()]

    select_columns = [
        plan_table,
        plan_search_summary_table.c.market_coverage,
        plan_search_summary_table.c.is_on_exchange,
        plan_search_summary_table.c.is_off_exchange,
        plan_search_summary_table.c.is_hsa,
        plan_search_summary_table.c.is_dental_only,
        plan_search_summary_table.c.is_catastrophic,
        plan_search_summary_table.c.deductible_inn_individual,
        plan_search_summary_table.c.moop_inn_individual,
        plan_search_summary_table.c.has_adult_dental,
        plan_search_summary_table.c.has_child_dental,
        plan_search_summary_table.c.has_adult_vision,
        plan_search_summary_table.c.has_child_vision,
        plan_search_summary_table.c.telehealth_supported,
        plan_search_summary_table.c.premium_min,
        plan_search_summary_table.c.premium_max,
        plan_search_summary_table.c.plan_type,
        plan_search_summary_table.c.metal_level,
        plan_search_summary_table.c.csr_variation,
        plan_search_summary_table.c.attributes,
        plan_search_summary_table.c.plan_benefits,
    ]

    from_clause = (
        plan_table.join(
            filtered_plans,
            and_(plan_table.c.plan_id == filtered_plans.c.plan_id, plan_table.c.year == filtered_plans.c.year),
        ).join(
            plan_search_summary_table,
            and_(
                plan_search_summary_table.c.plan_id == filtered_plans.c.plan_id,
                plan_search_summary_table.c.year == filtered_plans.c.year,
            ),
        )
    )

    data_stmt = select(*select_columns).select_from(from_clause)
    data_stmt = data_stmt.order_by(order_expression, *secondary_order).offset(offset).limit(limit)
    data_result = await session.execute(data_stmt)
    plans_rows = _result_rows(data_result)

    results = []
    plan_keys = []
    result_lookup = {}
    for row in plans_rows:
        row_dict = _row_to_dict(row)
        plan_id = row_dict.get("plan_id")
        plan_year = row_dict.get("year")
        if not plan_id or plan_year is None:
            continue
        plan_key = (plan_id, plan_year)
        entry = {key: value for key, value in row_dict.items() if key not in {"attributes", "plan_benefits"}}
        entry.update({
            "price_range": {"min": None, "max": None},
            "has_adult_dental": row_dict.get("has_adult_dental"),
            "has_child_dental": row_dict.get("has_child_dental"),
            "has_adult_vision": row_dict.get("has_adult_vision"),
            "has_child_vision": row_dict.get("has_child_vision"),
            "telehealth_supported": row_dict.get("telehealth_supported"),
            "is_hsa": row_dict.get("is_hsa"),
            "is_dental_only": row_dict.get("is_dental_only"),
            "is_catastrophic": row_dict.get("is_catastrophic"),
            "is_on_exchange": row_dict.get("is_on_exchange"),
            "is_off_exchange": row_dict.get("is_off_exchange"),
            "market_coverage": row_dict.get("market_coverage"),
            "deductible_inn_individual": row_dict.get("deductible_inn_individual"),
            "moop_inn_individual": row_dict.get("moop_inn_individual"),
            "premium_min": row_dict.get("premium_min"),
            "premium_max": row_dict.get("premium_max"),
            "plan_type": row_dict.get("plan_type"),
            "metal_level": row_dict.get("metal_level"),
            "csr_variation": row_dict.get("csr_variation"),
        })
        entry["attributes"] = _normalize_attribute_map(_summary_attributes_to_dict(row_dict.get("attributes")))
        entry["plan_benefits"] = _summary_benefits_to_dict(row_dict.get("plan_benefits"))
        results.append(entry)
        result_lookup[plan_key] = entry
        plan_keys.append(plan_key)

    if not results:
        return response.json(
            {
                "total": int(total),
                "results": [],
                "issuers": [],
                "facets": facets,
                "applied_filters": applied_filters,
                "warnings": warnings,
            },
            default=str,
        )

    plan_ids = sorted({key[0] for key in plan_keys})
    plan_years = sorted({key[1] for key in plan_keys})

    price_bounds = {}
    if plan_ids:
        price_stmt = select(plan_prices_table).where(plan_prices_table.c.plan_id.in_(plan_ids))
        if plan_years:
            price_stmt = price_stmt.where(plan_prices_table.c.year.in_(plan_years))
        if rating_area:
            price_stmt = price_stmt.where(plan_prices_table.c.rating_area_id == rating_area)
        if age is not None:
            price_stmt = price_stmt.where(
                and_(plan_prices_table.c.min_age <= age, plan_prices_table.c.max_age >= age)
            )
        if state:
            price_stmt = price_stmt.where(plan_prices_table.c.state == state)
        price_result = await session.execute(price_stmt)
        price_bounds = _collect_price_bounds(price_result)

    for key, entry in result_lookup.items():
        price_info = price_bounds.get(key)
        if price_info:
            entry["price_range"] = {
                "min": price_info["min"],
                "max": price_info["max"],
            }

    issuer_join = (
        plan_table.join(
            filtered_plans,
            and_(plan_table.c.plan_id == filtered_plans.c.plan_id, plan_table.c.year == filtered_plans.c.year),
        ).join(
            issuer_table,
            issuer_table.c.issuer_id == plan_table.c.issuer_id,
            isouter=True,
        )
    )
    issuer_stmt = select(
        plan_table.c.issuer_id.label("issuer_id"),
        func.max(issuer_table.c.issuer_name).label("issuer_name"),
        func.count().label("plan_count"),
    ).select_from(issuer_join)
    issuer_stmt = issuer_stmt.group_by(plan_table.c.issuer_id).order_by(
        func.count().desc(), plan_table.c.issuer_id
    )
    issuer_result = await session.execute(issuer_stmt)
    issuers = []
    for row in _result_rows(issuer_result):
        row_dict = _row_to_dict(row)
        issuers.append(
            {
                "issuer_id": row_dict.get("issuer_id"),
                "issuer_name": row_dict.get("issuer_name"),
                "plan_count": row_dict.get("plan_count"),
            }
        )

    return response.json(
        {
            "total": int(total),
            "results": results,
            "issuers": issuers,
            "facets": facets,
            "applied_filters": applied_filters,
            "warnings": warnings,
        },
        default=str,
    )

@blueprint.get("/price/<plan_id>", name="get_price_plan_by_plan_id")
@blueprint.get("/price/<plan_id>/year", name="get_price_plan_by_plan_id_and_year")
async def get_price_plan(request, plan_id, year=None, variant=None):
    session = _get_session(request)
    age = request.args.get("age")
    request.args.get("rating_area")
    _ = variant

    if year:
        try:
            int(year)
        except ValueError as exc:
            raise sanic.exceptions.BadRequest from exc

    if age:
        try:
            int(age)
        except ValueError as exc:
            raise sanic.exceptions.BadRequest from exc

    stmt = select(plan_prices_table).where(plan_prices_table.c.plan_id == plan_id)
    result = await session.execute(stmt)
    data = [_row_to_dict(row) for row in _result_rows(result)]
    return response.json(data, default=str)


@blueprint.post("/price/bulk", name="get_price_plans_bulk")
async def get_price_plans_bulk(request):
    session = _get_session(request)
    payload = request.json or {}
    if not isinstance(payload, dict):
        raise sanic.exceptions.BadRequest("Request body must be a JSON object")

    raw_plan_ids = payload.get("plan_ids")
    if not isinstance(raw_plan_ids, (list, tuple, set)):
        raise sanic.exceptions.BadRequest("plan_ids must be an array of plan IDs")

    plan_ids = []
    for value in raw_plan_ids:
        if value is None:
            continue
        plan_ids.append(str(value).strip())
    plan_ids = [pid for pid in plan_ids if pid]
    if not plan_ids:
        raise sanic.exceptions.BadRequest("plan_ids cannot be empty")

    year = payload.get("year")
    if year is not None:
        try:
            year = int(year)
        except (TypeError, ValueError) as exc:
            raise sanic.exceptions.BadRequest("year must be numeric") from exc

    age = payload.get("age")
    if age is not None:
        try:
            age = int(age)
        except (TypeError, ValueError) as exc:
            raise sanic.exceptions.BadRequest("age must be numeric") from exc

    rating_area = payload.get("rating_area")
    if rating_area is not None:
        rating_area = str(rating_area).strip()
        if not rating_area:
            rating_area = None

    stmt = select(plan_prices_table).where(plan_prices_table.c.plan_id.in_(plan_ids))
    if year is not None:
        stmt = stmt.where(plan_prices_table.c.year == year)
    if rating_area:
        stmt = stmt.where(plan_prices_table.c.rating_area_id == rating_area)
    if age is not None:
        stmt = stmt.where(
            and_(plan_prices_table.c.min_age <= age, plan_prices_table.c.max_age >= age)
        )

    result = await session.execute(stmt)
    rows = [_row_to_dict(row) for row in _result_rows(result)]

    data = {pid: [] for pid in plan_ids}
    for row in rows:
        pid = row.get("plan_id")
        if pid in data:
            data[pid].append(row)

    missing = [pid for pid, entries in data.items() if not entries]

    return response.json({"results": data, "missing": missing}, default=str)


@blueprint.get("/id/<plan_id>", name="get_plan_by_plan_id")
@blueprint.get("/id/<plan_id>/<year>", name="get_plan_by_plan_id_and_year")
@blueprint.get("/id/<plan_id>/<year>/<variant>", name="get_plan_variant_by_plan_id_and_year")
async def get_plan(request, plan_id, year=None, variant=None):
    session = _get_session(request)

    stmt = select(plan_table).where(plan_table.c.plan_id == plan_id)
    if year:
        stmt = stmt.where(plan_table.c.year == int(year))
    stmt = stmt.limit(1)

    plan_result = await session.execute(stmt)
    plan_row = plan_result.first()
    if not plan_row:
        raise sanic.exceptions.NotFound

    plan_data = _row_to_dict(plan_row)
    plan_data.setdefault("network_checksum", {})

    network_stmt = select(
        plan_network_tier_table.c.checksum_network,
        plan_network_tier_table.c.network_tier,
    ).where(
        and_(
            plan_network_tier_table.c.plan_id == plan_data.get("plan_id"),
            plan_network_tier_table.c.year == plan_data.get("year"),
        )
    )
    network_result = await session.execute(network_stmt)
    for checksum, tier in _result_rows(network_result):
        plan_data.setdefault("network_checksum", {})[str(checksum)] = tier

    issuer_stmt = select(issuer_table.c.issuer_name).where(
        issuer_table.c.issuer_id == plan_data.get("issuer_id")
    )
    issuer_result = await session.execute(issuer_stmt)
    plan_data["issuer_name"] = _result_scalar(issuer_result)

    formulary_stmt = select(plan_formulary_table).where(
        and_(
            plan_formulary_table.c.plan_id == plan_id,
            plan_formulary_table.c.year == plan_data.get("year"),
        )
    )
    formulary_result = await session.execute(formulary_stmt)
    plan_data["formulary"] = [_row_to_dict(row) for row in _result_rows(formulary_result)]

    drug_count_stmt = (
        select(func.coalesce(plan_drug_stats_table.c.total_drugs, 0))
        .where(plan_drug_stats_table.c.plan_id == plan_id)
    )
    drug_count_result = await session.execute(drug_count_stmt)
    plan_data["formulary_drug_count"] = int(_result_scalar(drug_count_result) or 0)
    plan_data["formulary_has_drug_data"] = bool(plan_data["formulary_drug_count"])
    plan_data["formulary_uri"] = f"{plan_id}/{plan_data.get('year')}"

    if not year:
        return response.json(plan_data, default=str)

    variant_stmt = select(plan_attributes_table.c.full_plan_id).where(
        and_(
            plan_attributes_table.c.plan_id == plan_id,
            plan_attributes_table.c.year == int(year),
        )
    ).order_by(plan_attributes_table.c.full_plan_id.asc())

    variant_result = await session.execute(variant_stmt)

    def _unique(values):
        seen = set()
        ordered = []
        for value in values:
            if value is None:
                continue
            if isinstance(value, (list, tuple)):
                if not value:
                    continue
                value = value[0]
            elif isinstance(value, str):
                trimmed = value.strip()
                if trimmed.startswith("(") and trimmed.endswith(")"):
                    trimmed = trimmed[1:-1].strip()
                    if trimmed.endswith(","):
                        trimmed = trimmed[:-1]
                    if (trimmed.startswith("'") and trimmed.endswith("'")) or (
                        trimmed.startswith('"') and trimmed.endswith('"')
                    ):
                        trimmed = trimmed[1:-1]
                    value = trimmed
                else:
                    value = trimmed
            normalized = str(value).strip()
            if normalized and normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)
        return ordered

    variants = _unique([row[0] if isinstance(row, (list, tuple)) else row for row in _result_rows(variant_result)])

    plan_data["attributes"] = {}
    plan_data["plan_benefits"] = {}

    def _normalize_single(value):
        cleaned = _unique([value])
        return cleaned[0] if cleaned else None

    plan_attr_stmt = select(plan_attributes_table).where(
        and_(
            plan_attributes_table.c.year == int(year),
            or_(
                plan_attributes_table.c.plan_id == plan_id,
                plan_attributes_table.c.full_plan_id.like(f"{plan_id}%"),
            ),
        )
    )
    plan_attr_result = await session.execute(plan_attr_stmt)
    for row in _result_rows(plan_attr_result):
        row_dict = _row_to_dict(row)
        full_id = _normalize_single(row_dict.get("full_plan_id"))
        if full_id and full_id not in variants:
            variants.append(full_id)
        name = row_dict.get("attr_name")
        if name:
            entry = {"attr_value": row_dict.get("attr_value")}
            if name in attributes_labels:
                entry["human_attr_name"] = attributes_labels[name]
            plan_data["attributes"][name] = entry

    plan_benefit_stmt = select(plan_benefits_table).where(
        and_(
            plan_benefits_table.c.year == int(year),
            or_(
                plan_benefits_table.c.plan_id == plan_id,
                plan_benefits_table.c.full_plan_id.like(f"{plan_id}%"),
            ),
        )
    )
    plan_benefit_result = await session.execute(plan_benefit_stmt)

    def _format_value(copay, coins):
        values = []
        if copay and copay != "Not Applicable":
            values.append(copay)
        if coins and coins != "Not Applicable":
            values.append(coins)
        return ", ".join(values) if values else None

    for row in _result_rows(plan_benefit_result):
        row_dict = _row_to_dict(row)
        full_id = _normalize_single(row_dict.get("full_plan_id"))
        if full_id and full_id not in variants:
            variants.append(full_id)
        name = row_dict.get("benefit_name")
        if name:
            benefit_entry = row_dict.copy()
            for key in ("full_plan_id", "year", "plan_id"):
                benefit_entry.pop(key, None)
            benefit_entry["in_network_tier1"] = _format_value(
                benefit_entry.pop("copay_inn_tier1", None),
                benefit_entry.pop("coins_inn_tier1", None),
            )
            benefit_entry["in_network_tier2"] = _format_value(
                benefit_entry.pop("copay_inn_tier2", None),
                benefit_entry.pop("coins_inn_tier2", None),
            )
            benefit_entry["out_network"] = _format_value(
                benefit_entry.pop("copay_outof_net", None),
                benefit_entry.pop("coins_outof_net", None),
            )
            if name in benefits_labels:
                benefit_entry["human_attr_name"] = benefits_labels[name]
            plan_data["plan_benefits"][name] = benefit_entry

    if not variants:
        fallback_stmt = select(plan_attributes_table.c.full_plan_id).where(
            and_(
                plan_attributes_table.c.full_plan_id.like(f"{plan_id}%"),
                plan_attributes_table.c.year == int(year),
            )
        ).order_by(plan_attributes_table.c.full_plan_id.asc())
        fallback_result = await session.execute(fallback_stmt)
        variants = _unique([row[0] if isinstance(row, (list, tuple)) else row for row in _result_rows(fallback_result)])

    variants = _unique(variants)
    plan_data["variants"] = variants

    active_variant = _normalize_single(variant) if variant else (variants[0] if variants else None)
    if variant and (active_variant not in variants):
        raise sanic.exceptions.NotFound

    plan_data["active_variant"] = active_variant
    plan_data["variant_attributes"] = {}
    plan_data["variant_benefits"] = {}

    if active_variant:
        attr_stmt = select(plan_attributes_table).where(
            and_(
                plan_attributes_table.c.full_plan_id == active_variant,
                plan_attributes_table.c.year == int(year),
            )
        )
        attr_result = await session.execute(attr_stmt)
        for row in _result_rows(attr_result):
            row_dict = _row_to_dict(row)
            name = row_dict.get("attr_name")
            entry = {"attr_value": row_dict.get("attr_value")}
            if name in attributes_labels:
                entry["human_attr_name"] = attributes_labels[name]
            plan_data["variant_attributes"][name] = entry
            if name not in plan_data["attributes"]:
                plan_data["attributes"][name] = entry

        benefit_stmt = select(plan_benefits_table).where(
            and_(
                plan_benefits_table.c.full_plan_id == active_variant,
                plan_benefits_table.c.year == int(year),
            )
        )
        benefit_result = await session.execute(benefit_stmt)
        for row in _result_rows(benefit_result):
            row_dict = _row_to_dict(row)
            name = row_dict.get("benefit_name")
            benefit_entry = row_dict.copy()
            for key in ("full_plan_id", "year", "plan_id"):
                benefit_entry.pop(key, None)
            benefit_entry["in_network_tier1"] = _format_value(
                benefit_entry.pop("copay_inn_tier1", None),
                benefit_entry.pop("coins_inn_tier1", None),
            )
            benefit_entry["in_network_tier2"] = _format_value(
                benefit_entry.pop("copay_inn_tier2", None),
                benefit_entry.pop("coins_inn_tier2", None),
            )
            benefit_entry["out_network"] = _format_value(
                benefit_entry.pop("copay_outof_net", None),
                benefit_entry.pop("coins_outof_net", None),
            )
            if name in benefits_labels:
                benefit_entry["human_attr_name"] = benefits_labels[name]
            plan_data["variant_benefits"][name] = benefit_entry
            if name not in plan_data["plan_benefits"]:
                plan_data["plan_benefits"][name] = benefit_entry
    if active_variant and not plan_data["variant_attributes"] and plan_data["attributes"]:
        plan_data["variant_attributes"] = {k: dict(v) for k, v in plan_data["attributes"].items()}
    if active_variant and not plan_data["variant_benefits"] and plan_data["plan_benefits"]:
        plan_data["variant_benefits"] = {k: dict(v) for k, v in plan_data["plan_benefits"].items()}

    plan_data["attributes"] = _normalize_attribute_map(plan_data.get("attributes"))
    plan_data["variant_attributes"] = _normalize_attribute_map(plan_data.get("variant_attributes"))

    return response.json(plan_data, default=str)
