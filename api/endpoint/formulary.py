# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy import and_, func, or_, select

from api.tier_utils import normalize_drug_tier_slug
from db.models import (Issuer, Plan, PlanDrugRaw, PlanDrugStats,
                       PlanDrugTierStats, PlanFormulary)

blueprint = Blueprint("formulary", url_prefix="/formulary", version=1)

FORMULARY_ID_SEPARATOR = ":"


def _encode_formulary_path(plan_id: str, year: int) -> str:
    return f"{plan_id}/{year}"
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _parse_positive_int(value: Optional[str], param_name: str) -> Optional[int]:
    if value in (None, "", "null"):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be an integer") from exc
    if parsed < 0:
        raise InvalidUsage(f"Parameter '{param_name}' must be non-negative")
    return parsed


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


def _encode_formulary_id(plan_id: str, year: int) -> str:
    return f"{plan_id}{FORMULARY_ID_SEPARATOR}{year}"


def _decode_formulary_id(formulary_id: str) -> Tuple[str, int]:
    if FORMULARY_ID_SEPARATOR not in formulary_id:
        raise InvalidUsage("Formulary identifier must include a year, e.g. PLANID:2025")
    plan_id, raw_year = formulary_id.rsplit(FORMULARY_ID_SEPARATOR, 1)
    if not plan_id:
        raise InvalidUsage("Formulary identifier is missing plan id")
    try:
        year = int(raw_year)
    except ValueError as exc:
        raise InvalidUsage("Formulary identifier year must be numeric") from exc
    return plan_id, year


def _normalise_sort(value: Optional[str], allowed: Iterable[str], default: str) -> str:
    if not value:
        return default
    lowered = value.lower()
    if lowered not in allowed:
        raise InvalidUsage(f"Unsupported sort field '{value}'. Allowed: {', '.join(allowed)}")
    return lowered


def _normalise_order(value: Optional[str]) -> str:
    if not value:
        return "asc"
    lowered = value.lower()
    if lowered not in {"asc", "desc"}:
        raise InvalidUsage("Order must be 'asc' or 'desc'")
    return lowered


async def _collect_distinct_strings(session, stmt):
    result = await session.execute(stmt)
    values = []
    for row in result.all():
        val = row[0]
        if val is not None:
            values.append(val)
    return sorted(set(values))


def _build_tier_options(values: Iterable[Any]) -> List[Dict[str, str]]:
    seen = set()
    options: List[Dict[str, str]] = []
    for raw in values:
        label = (raw or "UNKNOWN") if raw is not None else "UNKNOWN"
        label_str = str(label)
        slug = normalize_drug_tier_slug(label_str)
        if slug in seen:
            continue
        options.append({"tier_slug": slug, "tier_label": label_str})
        seen.add(slug)
    return options


def _build_tier_breakdown(rows: Iterable[Tuple[Any, Any]]) -> List[Dict[str, Any]]:
    totals: Dict[str, int] = {}
    labels: Dict[str, str] = {}
    for label, count in rows:
        label_str = str(label or "UNKNOWN")
        slug = normalize_drug_tier_slug(label_str)
        totals[slug] = totals.get(slug, 0) + int(count or 0)
        labels.setdefault(slug, label_str)
    ordered = sorted(totals.items(), key=lambda item: (-item[1], item[0]))
    return [
        {"tier_slug": slug, "tier_label": labels[slug], "drug_count": count}
        for slug, count in ordered
    ]


def _build_pharmacy_breakdown(rows: Iterable[Tuple[Any, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for label, count in rows:
        items.append(
            {
                "pharmacy_type": label or "UNKNOWN",
                "count": int(count or 0),
            }
        )
    return items


async def _formulary_exists(session, plan_id: str, year: int) -> bool:
    plan_table = Plan.__table__
    stmt = (
        select(func.count())
        .select_from(plan_table)
        .where(and_(plan_table.c.plan_id == plan_id, plan_table.c.year == year))
    )
    result = await session.execute(stmt)
    return bool(result.scalar())


def _plan_filters(
    plan_table,
    plan_drug_table,
    args: Dict[str, str],
) -> List[Any]:
    filters: List[Any] = []

    if args.get("issuer_id"):
        issuer_id = _parse_positive_int(args.get("issuer_id"), "issuer_id")
        if issuer_id is not None:
            filters.append(plan_table.c.issuer_id == issuer_id)

    if args.get("plan_id"):
        filters.append(plan_table.c.plan_id.ilike(f"%{args.get('plan_id')}%"))

    if args.get("state"):
        filters.append(plan_table.c.state == args.get("state").upper())

    if args.get("year"):
        year = _parse_positive_int(args.get("year"), "year")
        if year is not None:
            filters.append(plan_table.c.year == year)

    if args.get("drug"):
        keyword = args.get("drug").strip()
        if keyword:
            filters.append(
                or_(
                    plan_drug_table.c.drug_name.ilike(f"%{keyword}%"),
                    plan_drug_table.c.rxnorm_id == keyword,
                )
            )

    return filters


def _hydrate_formulary_row(row) -> Dict[str, Any]:
    mapping = getattr(row, "_mapping", row)
    last_updated = mapping.get("last_updated")
    if last_updated is not None:
        last_updated = last_updated.isoformat()
    plan_id = mapping["plan_id"]
    year = mapping["year"]
    return {
        "formulary_id": _encode_formulary_id(plan_id, year),
        "formulary_uri": _encode_formulary_path(plan_id, year),
        "plan_id": plan_id,
        "year": year,
        "marketing_name": mapping.get("marketing_name"),
        "state": mapping.get("state"),
        "issuer": {
            "issuer_id": mapping.get("issuer_id"),
            "issuer_name": mapping.get("issuer_name"),
            "issuer_marketing_name": mapping.get("issuer_marketing_name"),
        },
        "drug_count": int(mapping.get("drug_count") or 0),
        "last_updated": last_updated,
    }


@blueprint.get("/ids")
async def list_formularies(request):
    session = _get_session(request)

    args = request.args
    page = max(1, _parse_positive_int(args.get("page"), "page") or 1)
    raw_page_size = _parse_positive_int(args.get("page_size"), "page_size") or DEFAULT_PAGE_SIZE
    page_size = min(max(1, raw_page_size), MAX_PAGE_SIZE)
    offset = (page - 1) * page_size

    plan_table = Plan.__table__
    issuer_table = Issuer.__table__
    plan_drug_table = PlanDrugRaw.__table__
    plan_drug_stats_table = PlanDrugStats.__table__

    base_from = (
        plan_table.join(issuer_table, plan_table.c.issuer_id == issuer_table.c.issuer_id)
        .outerjoin(plan_drug_table, plan_drug_table.c.plan_id == plan_table.c.plan_id)
        .outerjoin(plan_drug_stats_table, plan_drug_stats_table.c.plan_id == plan_table.c.plan_id)
    )

    issuer_arg = args.get("issuer_id")
    plan_arg = args.get("plan_id")
    state_arg = args.get("state")
    year_arg = args.get("year")
    drug_arg = args.get("drug")

    filter_input = {
        "issuer_id": issuer_arg,
        "plan_id": plan_arg,
        "state": state_arg,
        "year": year_arg,
        "drug": drug_arg,
    }

    filters = _plan_filters(plan_table, plan_drug_table, filter_input)
    filter_condition = and_(*filters) if filters else None

    distinct_stmt = (
        select(plan_table.c.plan_id, plan_table.c.year)
        .select_from(base_from)
        .distinct()
    )
    if filter_condition is not None:
        distinct_stmt = distinct_stmt.where(filter_condition)

    count_result = await session.execute(
        select(func.count()).select_from(distinct_stmt.subquery())
    )
    total = count_result.scalar() or 0

    data_stmt = (
        select(
            plan_table.c.plan_id,
            plan_table.c.year,
            plan_table.c.marketing_name,
            plan_table.c.state,
            plan_table.c.issuer_id,
            issuer_table.c.issuer_name,
            issuer_table.c.issuer_marketing_name,
            func.coalesce(plan_drug_stats_table.c.total_drugs, 0).label("drug_count"),
            plan_drug_stats_table.c.last_updated_on.label("last_updated"),
        )
        .select_from(base_from)
    )
    if filter_condition is not None:
        data_stmt = data_stmt.where(filter_condition)

    data_stmt = (
        data_stmt.group_by(
            plan_table.c.plan_id,
            plan_table.c.year,
            plan_table.c.marketing_name,
            plan_table.c.state,
            plan_table.c.issuer_id,
            issuer_table.c.issuer_name,
            issuer_table.c.issuer_marketing_name,
            plan_drug_stats_table.c.total_drugs,
            plan_drug_stats_table.c.last_updated_on,
        )
        .order_by(plan_table.c.plan_id.asc(), plan_table.c.year.asc())
        .offset(offset)
        .limit(page_size)
    )

    result = await session.execute(data_stmt)
    items = [_hydrate_formulary_row(row) for row in result.all()]

    return response.json(
        {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
        }
    )


@blueprint.get("/id/<formulary_id>")
async def get_formulary(request, formulary_id):
    session = _get_session(request)
    plan_id, year = _decode_formulary_id(formulary_id)

    plan_table = Plan.__table__
    issuer_table = Issuer.__table__
    plan_drug_stats_table = PlanDrugStats.__table__
    plan_drug_tier_stats_table = PlanDrugTierStats.__table__
    plan_formulary_table = PlanFormulary.__table__

    detail_stmt = (
        select(
            plan_table.c.plan_id,
            plan_table.c.year,
            plan_table.c.marketing_name,
            plan_table.c.state,
            plan_table.c.summary_url,
            plan_table.c.marketing_url,
            plan_table.c.issuer_id,
            issuer_table.c.issuer_name,
            issuer_table.c.issuer_marketing_name,
        )
        .select_from(
            plan_table.join(issuer_table, plan_table.c.issuer_id == issuer_table.c.issuer_id)
        )
        .where(and_(plan_table.c.plan_id == plan_id, plan_table.c.year == year))
    )

    detail_result = await session.execute(detail_stmt)
    detail_row = detail_result.first()
    if detail_row is None:
        raise NotFound("Unknown formulary identifier")

    drug_counts_stmt = (
        select(
            func.coalesce(plan_drug_stats_table.c.total_drugs, 0).label("drug_count"),
            plan_drug_stats_table.c.last_updated_on.label("last_updated"),
        )
        .where(plan_drug_stats_table.c.plan_id == plan_id)
    )
    stats_result = await session.execute(drug_counts_stmt)
    stats_row = stats_result.first() or {"drug_count": 0, "last_updated": None}

    tiers_stmt = (
        select(plan_drug_tier_stats_table.c.drug_tier)
        .where(plan_drug_tier_stats_table.c.plan_id == plan_id)
        .order_by(plan_drug_tier_stats_table.c.drug_tier)
    )
    tiers_result = await session.execute(tiers_stmt)
    tiers = _build_tier_options(row[0] for row in tiers_result.all())

    pharmacy_stmt = (
        select(func.distinct(plan_formulary_table.c.pharmacy_type))
        .where(
            and_(
                plan_formulary_table.c.plan_id == plan_id,
                plan_formulary_table.c.year == year,
            )
        )
        .order_by(plan_formulary_table.c.pharmacy_type)
    )
    pharmacy_types = await _collect_distinct_strings(session, pharmacy_stmt)

    last_updated = stats_row["last_updated"]
    if last_updated is not None:
        last_updated = last_updated.isoformat()

    payload = {
        "formulary_id": formulary_id,
        "formulary_uri": _encode_formulary_path(plan_id, year),
        "plan": {
            "plan_id": detail_row.plan_id,
            "year": detail_row.year,
            "marketing_name": detail_row.marketing_name,
            "state": detail_row.state,
            "summary_url": detail_row.summary_url,
            "marketing_url": detail_row.marketing_url,
        },
        "issuer": {
            "issuer_id": detail_row.issuer_id,
            "issuer_name": detail_row.issuer_name,
            "issuer_marketing_name": detail_row.issuer_marketing_name,
        },
        "available_tiers": tiers,
        "available_pharmacy_types": pharmacy_types,
        "drug_count": int(stats_row["drug_count"] or 0),
        "last_updated": last_updated,
    }

    return response.json(payload)


@blueprint.get("/id/<formulary_id>/drugs")
async def list_formulary_drugs(request, formulary_id):
    session = _get_session(request)
    plan_id, year = _decode_formulary_id(formulary_id)
    if not await _formulary_exists(session, plan_id, year):
        raise NotFound("Unknown formulary identifier")

    args = request.args
    page = max(1, _parse_positive_int(args.get("page"), "page") or 1)
    raw_page_size = _parse_positive_int(args.get("page_size"), "page_size") or DEFAULT_PAGE_SIZE
    page_size = min(max(1, raw_page_size), MAX_PAGE_SIZE)
    offset = (page - 1) * page_size

    tier_filter = args.get("tier")
    pharmacy_filter = args.get("pharmacy_type")
    auth_filter = _parse_bool(args.get("authorization_required"), "authorization_required")
    step_filter = _parse_bool(args.get("step_therapy"), "step_therapy")
    quantity_filter = _parse_bool(args.get("quantity_limit"), "quantity_limit")

    plan_drug_table = PlanDrugRaw.__table__
    filters: List[Any] = [plan_drug_table.c.plan_id == plan_id]

    if tier_filter:
        filters.append(plan_drug_table.c.drug_tier == tier_filter)
    if auth_filter is not None:
        filters.append(plan_drug_table.c.prior_authorization == auth_filter)
    if step_filter is not None:
        filters.append(plan_drug_table.c.step_therapy == step_filter)
    if quantity_filter is not None:
        filters.append(plan_drug_table.c.quantity_limit == quantity_filter)

    plan_formulary_table = PlanFormulary.__table__

    if pharmacy_filter:
        filters.append(
            plan_drug_table.c.plan_id.in_(
                select(plan_formulary_table.c.plan_id).where(
                    and_(
                        plan_formulary_table.c.plan_id == plan_id,
                        plan_formulary_table.c.year == year,
                        plan_formulary_table.c.pharmacy_type == pharmacy_filter,
                    )
                )
            )
        )

    count_stmt = select(func.count()).select_from(plan_drug_table).where(and_(*filters))
    count_result = await session.execute(count_stmt)
    total = count_result.scalar() or 0

    sort_field = _normalise_sort(args.get("sort"), {"name", "tier"}, "name")
    sort_order = _normalise_order(args.get("order"))
    order_column = (
        plan_drug_table.c.drug_name if sort_field == "name" else plan_drug_table.c.drug_tier
    )
    if sort_order == "desc":
        order_column = order_column.desc()

    data_stmt = (
        select(
            plan_drug_table.c.rxnorm_id,
            plan_drug_table.c.drug_name,
            plan_drug_table.c.drug_tier,
            plan_drug_table.c.prior_authorization,
            plan_drug_table.c.step_therapy,
            plan_drug_table.c.quantity_limit,
            plan_drug_table.c.last_updated_on,
        )
        .where(and_(*filters))
        .order_by(order_column, plan_drug_table.c.rxnorm_id.asc())
        .offset(offset)
        .limit(page_size)
    )

    result = await session.execute(data_stmt)
    rows = result.all()

    pharmacy_stmt = (
        select(func.distinct(plan_formulary_table.c.pharmacy_type))
        .where(
            and_(
                plan_formulary_table.c.plan_id == plan_id,
                plan_formulary_table.c.year == year,
            )
        )
        .order_by(plan_formulary_table.c.pharmacy_type)
    )
    pharmacy_types = await _collect_distinct_strings(session, pharmacy_stmt)

    items = []
    for row in rows:
        mapping = row._mapping
        last_updated_on = mapping["last_updated_on"]
        if last_updated_on is not None:
            last_updated_on = last_updated_on.isoformat()
        items.append(
            {
                "rxnorm_id": mapping["rxnorm_id"],
                "drug_name": mapping["drug_name"],
                "drug_tier": mapping["drug_tier"],
                "drug_tier_slug": normalize_drug_tier_slug(mapping["drug_tier"]),
                "prior_authorization": mapping["prior_authorization"],
                "step_therapy": mapping["step_therapy"],
                "quantity_limit": mapping["quantity_limit"],
                "last_updated": last_updated_on,
            }
        )

    return response.json(
        {
            "formulary_id": formulary_id,
            "formulary_uri": _encode_formulary_path(plan_id, year),
            "page": page,
            "page_size": page_size,
            "total": total,
            "available_pharmacy_types": pharmacy_types,
            "items": items,
        }
    )


@blueprint.get("/id/<formulary_id>/drugs/<rxnorm_id>")
async def get_formulary_drug(request, formulary_id, rxnorm_id):
    session = _get_session(request)
    plan_id, year = _decode_formulary_id(formulary_id)
    if not await _formulary_exists(session, plan_id, year):
        raise NotFound("Unknown formulary identifier")

    plan_drug_table = PlanDrugRaw.__table__

    stmt = (
        select(
            plan_drug_table.c.rxnorm_id,
            plan_drug_table.c.drug_name,
            plan_drug_table.c.drug_tier,
            plan_drug_table.c.prior_authorization,
            plan_drug_table.c.step_therapy,
            plan_drug_table.c.quantity_limit,
            plan_drug_table.c.last_updated_on,
        )
        .where(
            and_(
                plan_drug_table.c.plan_id == plan_id,
                plan_drug_table.c.rxnorm_id == rxnorm_id,
            )
        )
    )

    result = await session.execute(stmt)
    row = result.first()
    if row is None:
        raise NotFound("Drug not found within formulary")

    mapping = row._mapping
    last_updated = mapping["last_updated_on"]
    if last_updated is not None:
        last_updated = last_updated.isoformat()

    plan_formulary_table = PlanFormulary.__table__
    pharmacy_stmt = (
        select(func.distinct(plan_formulary_table.c.pharmacy_type))
        .where(
            and_(
                plan_formulary_table.c.plan_id == plan_id,
                plan_formulary_table.c.year == year,
            )
        )
        .order_by(plan_formulary_table.c.pharmacy_type)
    )
    pharmacy_types = await _collect_distinct_strings(session, pharmacy_stmt)

    payload = {
        "formulary_id": formulary_id,
        "formulary_uri": _encode_formulary_path(plan_id, year),
        "rxnorm_id": mapping["rxnorm_id"],
        "drug_name": mapping["drug_name"],
        "drug_tier": mapping["drug_tier"],
        "drug_tier_slug": normalize_drug_tier_slug(mapping["drug_tier"]),
        "prior_authorization": mapping["prior_authorization"],
        "step_therapy": mapping["step_therapy"],
        "quantity_limit": mapping["quantity_limit"],
        "available_pharmacy_types": pharmacy_types,
        "last_updated": last_updated,
        "linked_plans": [
            {
                "plan_id": plan_id,
                "year": year,
            }
        ],
    }

    return response.json(payload)


@blueprint.get("/id/<formulary_id>/summary")
async def get_formulary_summary(request, formulary_id):
    session = _get_session(request)
    plan_id, year = _decode_formulary_id(formulary_id)
    if not await _formulary_exists(session, plan_id, year):
        raise NotFound("Unknown formulary identifier")

    plan_drug_stats_table = PlanDrugStats.__table__
    plan_drug_tier_stats_table = PlanDrugTierStats.__table__
    plan_formulary_table = PlanFormulary.__table__

    stats_stmt = (
        select(
            func.coalesce(plan_drug_stats_table.c.total_drugs, 0).label("total_drugs"),
            func.coalesce(plan_drug_stats_table.c.auth_required, 0).label("auth_required"),
            func.coalesce(plan_drug_stats_table.c.auth_not_required, 0).label("auth_not_required"),
            func.coalesce(plan_drug_stats_table.c.step_required, 0).label("step_required"),
            func.coalesce(plan_drug_stats_table.c.step_not_required, 0).label("step_not_required"),
            func.coalesce(plan_drug_stats_table.c.quantity_limit, 0).label("quantity_limit"),
            func.coalesce(plan_drug_stats_table.c.quantity_no_limit, 0).label("quantity_no_limit"),
        )
        .where(plan_drug_stats_table.c.plan_id == plan_id)
    )
    stats_result = await session.execute(stats_stmt)
    stats_row = stats_result.first()
    stats_mapping = getattr(stats_row, "_mapping", {}) if stats_row else {}
    total = int(stats_mapping.get("total_drugs") or 0)

    auth_counts = {
        "required": int(stats_mapping.get("auth_required") or 0),
        "not_required": int(stats_mapping.get("auth_not_required") or 0),
    }
    step_counts = {
        "required": int(stats_mapping.get("step_required") or 0),
        "not_required": int(stats_mapping.get("step_not_required") or 0),
    }
    quantity_counts = {
        "has_limit": int(stats_mapping.get("quantity_limit") or 0),
        "no_limit": int(stats_mapping.get("quantity_no_limit") or 0),
    }

    tier_stmt = (
        select(
            plan_drug_tier_stats_table.c.drug_tier,
            plan_drug_tier_stats_table.c.drug_count,
        )
        .where(plan_drug_tier_stats_table.c.plan_id == plan_id)
    )
    tier_rows = (await session.execute(tier_stmt)).all()
    tier_breakdown = _build_tier_breakdown([(row[0], row[1]) for row in tier_rows])

    pharmacy_stmt = (
        select(plan_formulary_table.c.pharmacy_type, func.count())
        .where(
            and_(
                plan_formulary_table.c.plan_id == plan_id,
                plan_formulary_table.c.year == year,
            )
        )
        .group_by(plan_formulary_table.c.pharmacy_type)
    )
    pharmacy_rows = (await session.execute(pharmacy_stmt)).all()
    pharmacy_counts = _build_pharmacy_breakdown([(row[0], row[1]) for row in pharmacy_rows])

    return response.json(
        {
            "formulary_id": formulary_id,
            "formulary_uri": _encode_formulary_path(plan_id, year),
            "total_drugs": int(total),
            "tiers": tier_breakdown,
            "authorization_requirements": auth_counts,
            "step_therapy": step_counts,
            "quantity_limits": quantity_counts,
            "pharmacy_types": pharmacy_counts,
        }
    )


@blueprint.get("/drugs/<rxnorm_id>")
async def cross_formulary_drug(request, rxnorm_id):
    session = _get_session(request)
    args = request.args

    plan_table = Plan.__table__
    plan_drug_table = PlanDrugRaw.__table__
    issuer_table = Issuer.__table__

    filters = [plan_drug_table.c.rxnorm_id == rxnorm_id]

    if args.get("year"):
        year = _parse_positive_int(args.get("year"), "year")
        if year is not None:
            filters.append(plan_table.c.year == year)

    if args.get("state"):
        filters.append(plan_table.c.state == args.get("state").upper())

    if args.get("issuer_id"):
        issuer_id = _parse_positive_int(args.get("issuer_id"), "issuer_id")
        if issuer_id is not None:
            filters.append(plan_table.c.issuer_id == issuer_id)

    stmt = (
        select(
            plan_table.c.plan_id,
            plan_table.c.year,
            plan_table.c.marketing_name,
            plan_table.c.state,
            plan_table.c.issuer_id,
            issuer_table.c.issuer_name,
            plan_drug_table.c.drug_tier,
            plan_drug_table.c.prior_authorization,
            plan_drug_table.c.step_therapy,
            plan_drug_table.c.quantity_limit,
        )
        .select_from(
            plan_drug_table.join(plan_table, plan_table.c.plan_id == plan_drug_table.c.plan_id).join(
                issuer_table, plan_table.c.issuer_id == issuer_table.c.issuer_id
            )
        )
        .where(and_(*filters))
        .order_by(plan_table.c.plan_id.asc(), plan_table.c.year.asc())
    )

    result = await session.execute(stmt)
    rows = result.all()
    if not rows:
        raise NotFound("Drug not present in any known formulary")

    items = []
    for row in rows:
        mapping = row._mapping
        items.append(
            {
                "formulary_id": _encode_formulary_id(mapping["plan_id"], mapping["year"]),
                "formulary_uri": _encode_formulary_path(mapping["plan_id"], mapping["year"]),
                "plan_id": mapping["plan_id"],
                "year": mapping["year"],
                "plan_marketing_name": mapping["marketing_name"],
                "state": mapping["state"],
                "issuer": {
                    "issuer_id": mapping["issuer_id"],
                    "issuer_name": mapping["issuer_name"],
                },
                "drug_tier": mapping["drug_tier"],
                "drug_tier_slug": normalize_drug_tier_slug(mapping["drug_tier"]),
                "prior_authorization": mapping["prior_authorization"],
                "step_therapy": mapping["step_therapy"],
                "quantity_limit": mapping["quantity_limit"],
            }
        )

    return response.json({"rxnorm_id": rxnorm_id, "formularies": items})


@blueprint.get("/statistics")
async def formulary_statistics(request):
    session = _get_session(request)
    args = request.args

    plan_table = Plan.__table__
    issuer_table = Issuer.__table__
    plan_drug_stats_table = PlanDrugStats.__table__
    plan_drug_tier_stats_table = PlanDrugTierStats.__table__

    filters: List[Any] = []
    if args.get("year"):
        year = _parse_positive_int(args.get("year"), "year")
        if year is not None:
            filters.append(plan_table.c.year == year)

    if args.get("state"):
        filters.append(plan_table.c.state == args.get("state").upper())

    if args.get("issuer_id"):
        issuer_id = _parse_positive_int(args.get("issuer_id"), "issuer_id")
        if issuer_id is not None:
            filters.append(plan_table.c.issuer_id == issuer_id)

    filter_condition = and_(*filters) if filters else None

    stats_from = plan_table.join(
        plan_drug_stats_table, plan_drug_stats_table.c.plan_id == plan_table.c.plan_id
    )
    issuer_from = stats_from.join(issuer_table, plan_table.c.issuer_id == issuer_table.c.issuer_id)
    tier_from = plan_table.join(
        plan_drug_tier_stats_table, plan_drug_tier_stats_table.c.plan_id == plan_table.c.plan_id
    )

    top_issuers_stmt = (
        select(
            issuer_table.c.issuer_id,
            issuer_table.c.issuer_name,
            func.coalesce(func.sum(plan_drug_stats_table.c.total_drugs), 0).label("drug_count"),
        )
        .select_from(issuer_from)
        .group_by(issuer_table.c.issuer_id, issuer_table.c.issuer_name)
        .order_by(func.sum(plan_drug_stats_table.c.total_drugs).desc())
        .limit(5)
    )
    if filter_condition is not None:
        top_issuers_stmt = top_issuers_stmt.where(filter_condition)

    tier_stmt = (
        select(
            plan_drug_tier_stats_table.c.drug_tier,
            func.coalesce(func.sum(plan_drug_tier_stats_table.c.drug_count), 0),
        )
        .select_from(tier_from)
        .group_by(plan_drug_tier_stats_table.c.drug_tier)
    )
    if filter_condition is not None:
        tier_stmt = tier_stmt.where(filter_condition)

    total_drugs_stmt = (
        select(
            func.coalesce(func.sum(plan_drug_stats_table.c.total_drugs), 0).label("total_drugs"),
            func.coalesce(func.sum(plan_drug_stats_table.c.auth_required), 0).label("auth_required"),
            func.coalesce(func.sum(plan_drug_stats_table.c.auth_not_required), 0).label("auth_not_required"),
        )
        .select_from(stats_from)
    )
    if filter_condition is not None:
        total_drugs_stmt = total_drugs_stmt.where(filter_condition)

    distinct_formulary_stmt = (
        select(plan_table.c.plan_id, plan_table.c.year)
        .select_from(plan_table)
        .distinct()
    )
    if filter_condition is not None:
        distinct_formulary_stmt = distinct_formulary_stmt.where(filter_condition)

    total_formulary_stmt = select(func.count()).select_from(distinct_formulary_stmt.subquery())

    top_issuers = []
    for row in (await session.execute(top_issuers_stmt)).all():
        top_issuers.append(
            {
                "issuer_id": row[0],
                "issuer_name": row[1],
                "drug_count": int(row[2] or 0),
            }
        )

    tier_rows = (await session.execute(tier_stmt)).all()
    tier_distribution = _build_tier_breakdown([(row[0], row[1]) for row in tier_rows])

    total_drugs_result = await session.execute(total_drugs_stmt)
    totals_row = total_drugs_result.first()
    totals_map = getattr(totals_row, "_mapping", {}) if totals_row else {}
    total_drugs = int(totals_map.get("total_drugs") or 0)
    authorization_distribution = {
        "required": int(totals_map.get("auth_required") or 0),
        "not_required": int(totals_map.get("auth_not_required") or 0),
    }

    total_formulary_result = await session.execute(total_formulary_stmt)
    total_formularies = total_formulary_result.scalar() or 0

    return response.json(
        {
            "total_formularies": int(total_formularies),
            "total_drugs": int(total_drugs),
            "top_issuers": top_issuers,
            "tier_distribution": tier_distribution,
            "authorization_distribution": authorization_distribution,
        }
    )


@blueprint.get("/plan/<plan_id>/drug/<rxnorm_id>")
async def check_plan_drug(request, plan_id, rxnorm_id):
    session = _get_session(request)
    plan_table = Plan.__table__
    plan_drug_table = PlanDrugRaw.__table__

    year_param = request.args.get("year")
    if year_param:
        year = _parse_positive_int(year_param, "year")
        if year is None:
            raise InvalidUsage("Parameter 'year' must be provided when specified")
    else:
        max_year_stmt = (
            select(func.max(plan_table.c.year))
            .where(plan_table.c.plan_id == plan_id)
        )
        max_year_result = await session.execute(max_year_stmt)
        year = max_year_result.scalar()
        if year is None:
            raise NotFound("Plan not found")

    plan_exists_stmt = (
        select(func.count())
        .select_from(plan_table)
        .where(and_(plan_table.c.plan_id == plan_id, plan_table.c.year == year))
    )
    if not (await session.execute(plan_exists_stmt)).scalar():
        raise NotFound("Plan not found")

    drug_stmt = (
        select(
            plan_drug_table.c.plan_id,
            plan_drug_table.c.rxnorm_id,
            plan_drug_table.c.drug_name,
            plan_drug_table.c.drug_tier,
            plan_drug_table.c.prior_authorization,
            plan_drug_table.c.step_therapy,
            plan_drug_table.c.quantity_limit,
            plan_drug_table.c.last_updated_on,
        )
        .where(
            and_(
                plan_drug_table.c.plan_id == plan_id,
                plan_drug_table.c.rxnorm_id == rxnorm_id,
            )
        )
    )

    drug_result = await session.execute(drug_stmt)
    drug_row = drug_result.first()

    covered = drug_row is not None
    details = None
    if covered:
        mapping = drug_row._mapping
        last_updated = mapping["last_updated_on"]
        if last_updated is not None:
            last_updated = last_updated.isoformat()
        details = {
            "drug_name": mapping["drug_name"],
            "drug_tier": mapping["drug_tier"],
            "drug_tier_slug": normalize_drug_tier_slug(mapping["drug_tier"]),
            "prior_authorization": mapping["prior_authorization"],
            "step_therapy": mapping["step_therapy"],
            "quantity_limit": mapping["quantity_limit"],
            "last_updated": last_updated,
        }

    return response.json(
        {
            "formulary_id": _encode_formulary_id(plan_id, year),
            "formulary_uri": _encode_formulary_path(plan_id, year),
            "plan_id": plan_id,
            "year": year,
            "rxnorm_id": rxnorm_id,
            "covered": covered,
            "details": details,
        }
    )
