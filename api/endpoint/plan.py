# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from datetime import datetime
import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import and_, func, or_, select

from api.for_human import attributes_labels, benefits_labels
from db.connection import db as sa_db
from db.models import (
    ImportLog,
    Issuer,
    Plan,
    PlanAttributes,
    PlanBenefits,
    PlanDrugRaw,
    PlanFormulary,
    PlanNetworkTierRaw,
    PlanNPIRaw,
    PlanPrices,
)
from db.tiger_models import ZipState

plan_table = Plan.__table__
plan_attributes_table = PlanAttributes.__table__
plan_benefits_table = PlanBenefits.__table__
plan_formulary_table = PlanFormulary.__table__
plan_network_tier_table = PlanNetworkTierRaw.__table__
plan_npi_table = PlanNPIRaw.__table__
plan_prices_table = PlanPrices.__table__
import_log_table = ImportLog.__table__
issuer_table = Issuer.__table__
zip_state_table = ZipState.__table__


blueprint = Blueprint("plan", url_prefix="/plan", version=1)


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
        stmt = stmt.select_from(
            plan_table.join(
                zip_state_table,
                plan_table.c.state == zip_state_table.c.stusps,
            )
        ).where(zip_state_table.c.zip == zip_code)

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
    age = args.get("age")
    args.get("rating_area")
    args.get("zip_code")
    year = args.get("year")
    limit = args.get("limit") or 100
    page = args.get("page") or 1
    args.get("order", "asc")
    args.get("order_by", "plan_id")

    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 100

    try:
        page = max(int(page) - 1, 0)
    except (TypeError, ValueError):
        page = 0

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

    session = _get_session(request)

    total_result = await session.execute(select(sa_db.func.count()))
    total = _result_scalar(total_result) or 0

    plans_result = await session.execute(select(plan_table))
    plans_rows = [_row_to_dict(row) for row in _result_rows(plans_result)]

    results = []
    found_ids = []
    for row in plans_rows:
        plan_id = row.get("plan_id")
        if not plan_id:
            continue
        found_ids.append(plan_id)
        results.append(
            {
                **row,
                "price_range": {
                    "min": float(row.get("min_rate", 0.0)),
                    "max": float(row.get("max_rate", 0.0)),
                },
                "attributes": {},
                "plan_benefits": {},
            }
        )

    if not results:
        return response.json({"total": total, "results": []}, default=str)

    attr_result = await session.execute(select(plan_attributes_table))
    for row in _result_rows(attr_result):
        row_dict = _row_to_dict(row)
        plan_id = row_dict.get("plan_id") or (row_dict.get("full_plan_id") or "")[:-3]
        for entry in results:
            if entry.get("plan_id") == plan_id:
                entry["attributes"][row_dict.get("attr_name")] = row_dict.get("attr_value")

    benefits_result = await session.execute(select(plan_benefits_table))
    for row in _result_rows(benefits_result):
        row_dict = _row_to_dict(row)
        plan_id = row_dict.get("plan_id") or (row_dict.get("full_plan_id") or "")[:-3]
        for entry in results:
            if entry.get("plan_id") == plan_id:
                benefit_name = row_dict.get("benefit_name")
                benefit_data = row_dict.copy()
                benefit_data["benefit_name"] = benefit_name
                entry["plan_benefits"][benefit_name] = benefit_data

    return response.json({"total": total, "results": results}, default=str)


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

    drug_count_stmt = select(func.count(func.distinct(PlanDrugRaw.__table__.c.rxnorm_id))).where(
        PlanDrugRaw.__table__.c.plan_id == plan_id
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
    return response.json(plan_data, default=str)
