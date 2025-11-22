# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

import sanic.exceptions
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import Float, and_, case, cast, func, or_, select
from asyncpg import UndefinedTableError
from sqlalchemy.exc import ProgrammingError

from api.for_human import attributes_labels, benefits_labels
from db.connection import db as sa_db
from db.models import (
    ImportLog,
    Issuer,
    Plan,
    PlanAttributes,
    PlanBenefits,
    GeoZipLookup,
    PlanDrugStats,
    PlanFormulary,
    PlanNetworkTierRaw,
    PlanNPIRaw,
    PlanPrices,
    PlanRatingAreas,
)
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

BOOLEAN_TRUE_VALUES = ("yes", "true", "1")
ADULT_DENTAL_KEYWORDS = ("dental", "adult")
CHILD_DENTAL_KEYWORDS = ("dental", "child")
ADULT_VISION_KEYWORDS = (("vision", "adult"), ("eye", "adult"))
CHILD_VISION_KEYWORDS = (("vision", "child"), ("eye", "child"))
TELEHEALTH_KEYWORDS = (("telehealth",), ("telemedicine",), ("virtual", "visit"), ("virtual", "care"))
EXCHANGE_ON_KEYWORDS = ("on exchange", "on/off exchange", "on and off", "on & off", "both")
EXCHANGE_OFF_KEYWORDS = ("off exchange", "on/off exchange", "on and off", "on & off", "both")

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


def _normalize_keyword_groups(keywords: Sequence[Sequence[str]] | Sequence[str]) -> List[tuple[str, ...]]:
    if not keywords:
        return []
    first = keywords[0] if isinstance(keywords, Sequence) else keywords
    if keywords and isinstance(keywords[0], (tuple, list)):
        groups = keywords  # type: ignore[assignment]
    else:
        groups = (keywords,)  # type: ignore[assignment]
    normalized: List[tuple[str, ...]] = []
    for group in groups:  # type: ignore[arg-type]
        values = tuple(word.strip().lower() for word in group if isinstance(word, str) and word.strip())
        if values:
            normalized.append(values)
    return normalized


def _keyword_condition(column, keywords: Sequence[Sequence[str]] | Sequence[str]):
    groups = _normalize_keyword_groups(keywords)
    if not groups:
        return None
    lowered = func.lower(func.coalesce(column, ""))
    group_expressions = []
    for group in groups:
        terms = [lowered.like(f"%{token}%") for token in group]
        if terms:
            group_expressions.append(and_(*terms))
    if not group_expressions:
        return None
    if len(group_expressions) == 1:
        return group_expressions[0]
    return or_(*group_expressions)


def _text_matches_any(column, keywords: Iterable[str]):
    comparisons = []
    lowered = func.lower(func.coalesce(column, ""))
    for keyword in keywords:
        token = (keyword or "").strip().lower()
        if not token:
            continue
        comparisons.append(lowered.like(f"%{token}%"))
    if not comparisons:
        return None
    if len(comparisons) == 1:
        return comparisons[0]
    return or_(*comparisons)


def _benefit_flag_case(keywords: Sequence[Sequence[str]] | Sequence[str]):
    condition = _keyword_condition(plan_benefits_table.c.benefit_name, keywords)
    if condition is None:
        return func.bool_or(func.false())
    covered = and_(plan_benefits_table.c.is_covered.is_(True), condition)
    return func.bool_or(
        case(
            (covered, True),
            else_=None,
        )
    )


def _build_attribute_subquery(filtered_plans):
    attr_plan_id = _plan_level_plan_id_column(plan_attributes_table)
    market_text_expr = func.trim(_attribute_text_case("MarketCoverage"))
    metal_level_expr = func.trim(_attribute_text_case("MetalLevel"))
    on_condition = _text_matches_any(market_text_expr, EXCHANGE_ON_KEYWORDS)
    off_condition = _text_matches_any(market_text_expr, EXCHANGE_OFF_KEYWORDS)
    metal_level_lower = func.lower(func.coalesce(metal_level_expr, ""))
    join_condition = and_(
        attr_plan_id == filtered_plans.c.plan_id,
        plan_attributes_table.c.year == filtered_plans.c.year,
    )
    return (
        select(
            filtered_plans.c.plan_id.label("plan_id"),
            filtered_plans.c.year.label("year"),
            market_text_expr.label("market_coverage"),
            (case((on_condition, True), else_=False) if on_condition is not None else func.false()).label(
                "is_on_exchange"
            ),
            (case((off_condition, True), else_=False) if off_condition is not None else func.false()).label(
                "is_off_exchange"
            ),
            _attribute_boolean_case("IsHSAEligible").label("is_hsa"),
            _attribute_boolean_case("DentalOnlyPlan").label("is_dental_only"),
            (case((metal_level_lower.like("%catastrophic%"), True), else_=False)).label("is_catastrophic"),
            _attribute_numeric_case("MEHBDedInnTier1Individual").label("deductible_inn_individual"),
            _attribute_numeric_case("MEHBInnTier1IndividualMOOP").label("moop_inn_individual"),
        )
        .select_from(plan_attributes_table.join(filtered_plans, join_condition))
        .group_by(filtered_plans.c.plan_id, filtered_plans.c.year)
        .subquery()
    )


def _build_benefit_flag_subquery(filtered_plans):
    benefit_plan_id = _plan_level_plan_id_column(plan_benefits_table)
    join_condition = and_(
        benefit_plan_id == filtered_plans.c.plan_id,
        plan_benefits_table.c.year == filtered_plans.c.year,
    )
    return (
        select(
            filtered_plans.c.plan_id.label("plan_id"),
            filtered_plans.c.year.label("year"),
            _benefit_flag_case(ADULT_DENTAL_KEYWORDS).label("has_adult_dental"),
            _benefit_flag_case(CHILD_DENTAL_KEYWORDS).label("has_child_dental"),
            _benefit_flag_case(ADULT_VISION_KEYWORDS).label("has_adult_vision"),
            _benefit_flag_case(CHILD_VISION_KEYWORDS).label("has_child_vision"),
            _benefit_flag_case(TELEHEALTH_KEYWORDS).label("telehealth_supported"),
        )
        .select_from(plan_benefits_table.join(filtered_plans, join_condition))
        .group_by(filtered_plans.c.plan_id, filtered_plans.c.year)
        .subquery()
    )


def _attribute_text_case(attr_name, lower=False):
    value = func.coalesce(plan_attributes_table.c.attr_value, "")
    if lower:
        value = func.lower(func.trim(value))
    return func.max(
        case(
            (plan_attributes_table.c.attr_name == attr_name, value),
            else_=None,
        )
    )


def _attribute_boolean_case(attr_name, truthy_values=None, exact_match=None):
    truthy_values = truthy_values or BOOLEAN_TRUE_VALUES
    value = func.lower(func.trim(func.coalesce(plan_attributes_table.c.attr_value, "")))
    condition = plan_attributes_table.c.attr_name == attr_name
    if exact_match:
        condition = and_(condition, value == exact_match)
    else:
        condition = and_(condition, value.in_(truthy_values))
    return func.bool_or(
        case(
            (condition, True),
            else_=None,
        )
    )


def _attribute_numeric_case(attr_name):
    raw_value = func.coalesce(plan_attributes_table.c.attr_value, "")
    numeric_value = cast(
        func.nullif(func.regexp_replace(raw_value, "[^0-9.]", "", "g"), ""),
        Float,
    )
    return func.max(
        case(
            (plan_attributes_table.c.attr_name == attr_name, numeric_value),
            else_=None,
        )
    )


def _apply_bool_filter(filters, expression, value):
    if value is None:
        return
    if value:
        filters.append(expression.is_(True))
    else:
        filters.append(expression.is_(False))


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


async def _benefit_metadata_availability(
    session,
    specs: Sequence[tuple[str, Sequence[Sequence[str]] | Sequence[str]]],
    states: Optional[Sequence[str]],
    year: Optional[int],
) -> Dict[str, bool]:
    evaluated_specs = []
    for key, keywords in specs:
        condition = _keyword_condition(plan_benefits_table.c.benefit_name, keywords)
        if condition is None:
            continue
        evaluated_specs.append((key, condition))

    if not evaluated_specs:
        return {}

    join_condition = and_(
        _plan_level_plan_id_column(plan_benefits_table) == plan_table.c.plan_id,
        plan_benefits_table.c.year == plan_table.c.year,
    )
    select_columns = []
    for key, condition in evaluated_specs:
        covered = and_(plan_benefits_table.c.is_covered.is_(True), condition)
        select_columns.append(
            func.bool_or(
                case(
                    (covered, True),
                    else_=None,
                )
            ).label(key)
        )

    stmt = select(*select_columns).select_from(plan_benefits_table.join(plan_table, join_condition))
    if states:
        stmt = stmt.where(plan_table.c.state.in_(list(states)))
    if year is not None:
        stmt = stmt.where(plan_table.c.year == year)

    result = await session.execute(stmt)
    row = result.first()
    row_dict = _row_to_dict(row) if row else {}
    availability = {}
    for key, _ in evaluated_specs:
        value = row_dict.get(key)
        availability[key] = bool(value)
    return availability


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

    if state and len(state) != 2:
        raise sanic.exceptions.BadRequest("state must be a 2-letter code")

    session = _get_session(request)

    applied_filters: Dict[str, Any] = {}
    warnings: List[Dict[str, str]] = []

    plan_filters = []

    if state:
        plan_filters.append(plan_table.c.state == state)
        _append_filter(applied_filters, "state", state)

    if year is not None:
        plan_filters.append(plan_table.c.year == year)
        _append_filter(applied_filters, "year", year)

    if issuer_id is not None:
        plan_filters.append(plan_table.c.issuer_id == issuer_id)
        _append_filter(applied_filters, "issuer_id", issuer_id)

    if plan_id_filter:
        plan_filters.append(plan_table.c.plan_id.ilike(f"%{plan_id_filter}%"))
        _append_filter(applied_filters, "plan_id", plan_id_filter)

    if query_text:
        plan_filters.append(plan_table.c.marketing_name.ilike(f"%{query_text}%"))
        _append_filter(applied_filters, "q", query_text)

    state_scope: Optional[List[str]] = [state] if state else None

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
                    "applied_filters": applied_filters,
                    "warnings": warnings,
                },
                default=str,
            )
        plan_filters.append(plan_table.c.state.in_(zip_states))
        state_scope = zip_states

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

    filtered_plan_stmt = select(plan_table.c.plan_id.label("plan_id"), plan_table.c.year.label("year"))
    if plan_filters:
        filtered_plan_stmt = filtered_plan_stmt.where(and_(*plan_filters))
    filtered_plans = filtered_plan_stmt.cte("filtered_plans")

    attrs_subq = _build_attribute_subquery(filtered_plans)
    benefit_flags_subq = _build_benefit_flag_subquery(filtered_plans)

    coverage_specs = [
        ("has_adult_dental", ADULT_DENTAL_KEYWORDS),
        ("has_child_dental", CHILD_DENTAL_KEYWORDS),
        ("has_adult_vision", ADULT_VISION_KEYWORDS),
        ("has_child_vision", CHILD_VISION_KEYWORDS),
        ("telehealth_supported", TELEHEALTH_KEYWORDS),
    ]
    bool_filters_to_apply: Dict[str, bool] = {}
    requested_specs = [
        (key, keywords)
        for key, keywords in coverage_specs
        if bool_filter_inputs.get(key) is not None
    ]
    coverage_metadata = {}
    if requested_specs:
        coverage_metadata = await _benefit_metadata_availability(session, requested_specs, state_scope, year)
    for key, _keywords in coverage_specs:
        requested_value = bool_filter_inputs.get(key)
        if requested_value is None:
            continue
        if not coverage_metadata.get(key):
            warnings.append(
                {
                    "code": f"{key}_unsupported",
                    "message": f"No coverage metadata available to evaluate '{key}' for the requested region/year.",
                }
            )
            continue
        bool_filters_to_apply[key] = requested_value
        _append_filter(applied_filters, key, requested_value)

    for key in ("is_hsa", "is_dental_only", "is_catastrophic", "is_on_exchange", "is_off_exchange"):
        value = bool_filter_inputs.get(key)
        if value is None:
            continue
        bool_filters_to_apply[key] = value
        _append_filter(applied_filters, key, value)

    plan_join = plan_table.join(
        filtered_plans,
        and_(
            plan_table.c.plan_id == filtered_plans.c.plan_id,
            plan_table.c.year == filtered_plans.c.year,
        ),
    )
    from_clause = (
        plan_join.outerjoin(
            attrs_subq,
            and_(plan_table.c.plan_id == attrs_subq.c.plan_id, plan_table.c.year == attrs_subq.c.year),
        ).outerjoin(
            benefit_flags_subq,
            and_(plan_table.c.plan_id == benefit_flags_subq.c.plan_id, plan_table.c.year == benefit_flags_subq.c.year),
        )
    )

    filters = []

    column_map = {
        "has_adult_dental": benefit_flags_subq.c.has_adult_dental,
        "has_child_dental": benefit_flags_subq.c.has_child_dental,
        "has_adult_vision": benefit_flags_subq.c.has_adult_vision,
        "has_child_vision": benefit_flags_subq.c.has_child_vision,
        "telehealth_supported": benefit_flags_subq.c.telehealth_supported,
        "is_hsa": attrs_subq.c.is_hsa,
        "is_dental_only": attrs_subq.c.is_dental_only,
        "is_catastrophic": attrs_subq.c.is_catastrophic,
        "is_on_exchange": attrs_subq.c.is_on_exchange,
        "is_off_exchange": attrs_subq.c.is_off_exchange,
    }

    for key, value in bool_filters_to_apply.items():
        column = column_map.get(key)
        if column is None:
            continue
        _apply_bool_filter(filters, column, value)

    if deductible_min is not None:
        filters.append(attrs_subq.c.deductible_inn_individual >= deductible_min)
        _append_filter(applied_filters, "deductible_min", deductible_min)
    if deductible_max is not None:
        filters.append(attrs_subq.c.deductible_inn_individual <= deductible_max)
        _append_filter(applied_filters, "deductible_max", deductible_max)
    if moop_min is not None:
        filters.append(attrs_subq.c.moop_inn_individual >= moop_min)
        _append_filter(applied_filters, "moop_min", moop_min)
    if moop_max is not None:
        filters.append(attrs_subq.c.moop_inn_individual <= moop_max)
        _append_filter(applied_filters, "moop_max", moop_max)

    final_condition = and_(*filters) if filters else None

    count_subquery = select(plan_table.c.plan_id, plan_table.c.year).select_from(from_clause)
    if final_condition is not None:
        count_subquery = count_subquery.where(final_condition)
    count_stmt = select(func.count()).select_from(count_subquery.distinct().subquery())

    total_result = await session.execute(count_stmt)
    total = _result_scalar(total_result) or 0

    order_map = {
        "plan_id": plan_table.c.plan_id,
        "marketing_name": plan_table.c.marketing_name,
        "state": plan_table.c.state,
        "year": plan_table.c.year,
        "deductible": attrs_subq.c.deductible_inn_individual,
        "moop": attrs_subq.c.moop_inn_individual,
    }
    order_column = order_map.get(order_by, plan_table.c.plan_id)
    order_expression = order_column.desc() if order == "desc" else order_column.asc()
    secondary_order = [] if order_column is plan_table.c.plan_id else [plan_table.c.plan_id.asc()]

    select_columns = [
        plan_table,
        attrs_subq.c.market_coverage,
        attrs_subq.c.is_on_exchange,
        attrs_subq.c.is_off_exchange,
        attrs_subq.c.is_hsa,
        attrs_subq.c.is_dental_only,
        attrs_subq.c.is_catastrophic,
        attrs_subq.c.deductible_inn_individual,
        attrs_subq.c.moop_inn_individual,
        benefit_flags_subq.c.has_adult_dental,
        benefit_flags_subq.c.has_child_dental,
        benefit_flags_subq.c.has_adult_vision,
        benefit_flags_subq.c.has_child_vision,
        benefit_flags_subq.c.telehealth_supported,
    ]

    data_stmt = select(*select_columns).select_from(from_clause)
    if final_condition is not None:
        data_stmt = data_stmt.where(final_condition)
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
        entry = {
            **row_dict,
            "price_range": {"min": None, "max": None},
            "attributes": {},
            "plan_benefits": {},
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
        }
        results.append(entry)
        result_lookup[plan_key] = entry
        plan_keys.append(plan_key)

    if not results:
        return response.json(
            {"total": int(total), "results": [], "issuers": [], "applied_filters": applied_filters, "warnings": warnings},
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
        entry["attributes"] = _normalize_attribute_map(entry.get("attributes"))

    attribute_filters = []
    benefit_filters = []
    if plan_ids:
        attribute_filters.append(
            or_(
                plan_attributes_table.c.plan_id.in_(plan_ids),
                func.left(plan_attributes_table.c.full_plan_id, 14).in_(plan_ids),
            )
        )
        benefit_filters.append(
            or_(
                plan_benefits_table.c.plan_id.in_(plan_ids),
                func.left(plan_benefits_table.c.full_plan_id, 14).in_(plan_ids),
            )
        )
    if plan_years:
        attribute_filters.append(plan_attributes_table.c.year.in_(plan_years))
        benefit_filters.append(plan_benefits_table.c.year.in_(plan_years))

    if attribute_filters:
        attr_stmt = select(plan_attributes_table).where(and_(*attribute_filters))
        attr_result = await session.execute(attr_stmt)
        for row in _result_rows(attr_result):
            row_dict = _row_to_dict(row)
            plan_id = row_dict.get("plan_id") or (row_dict.get("full_plan_id") or "")[:14]
            plan_year = row_dict.get("year")
            target = result_lookup.get((plan_id, plan_year))
            if not target:
                continue
            attr_name = row_dict.get("attr_name")
            if attr_name:
                entry = {"attr_value": row_dict.get("attr_value")}
                if attr_name in attributes_labels:
                    entry["human_attr_name"] = attributes_labels[attr_name]
                target["attributes"][attr_name] = entry

    if benefit_filters:
        benefits_stmt = select(plan_benefits_table).where(and_(*benefit_filters))
        benefits_result = await session.execute(benefits_stmt)
        for row in _result_rows(benefits_result):
            row_dict = _row_to_dict(row)
            plan_id = row_dict.get("plan_id") or (row_dict.get("full_plan_id") or "")[:14]
            plan_year = row_dict.get("year")
            target = result_lookup.get((plan_id, plan_year))
            if not target:
                continue
            benefit_name = row_dict.get("benefit_name")
            if benefit_name:
                benefit_data = dict(row_dict)
                benefit_data["benefit_name"] = benefit_name
                target["plan_benefits"][benefit_name] = benefit_data

    issuer_join = from_clause.join(
        issuer_table, issuer_table.c.issuer_id == plan_table.c.issuer_id, isouter=True
    )
    issuer_stmt = select(
        plan_table.c.issuer_id.label("issuer_id"),
        func.max(issuer_table.c.issuer_name).label("issuer_name"),
        func.count(func.distinct(plan_table.c.plan_id)).label("plan_count"),
    ).select_from(issuer_join)
    if final_condition is not None:
        issuer_stmt = issuer_stmt.where(final_condition)
    issuer_stmt = issuer_stmt.group_by(plan_table.c.issuer_id).order_by(
        func.count(func.distinct(plan_table.c.plan_id)).desc(), plan_table.c.issuer_id
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
