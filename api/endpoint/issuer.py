# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import and_, func, select, text
from sqlalchemy.exc import SQLAlchemyError

from api.endpoint.pagination import parse_pagination
from api.tier_utils import normalize_drug_tier_slug
from db.connection import db as sa_db
from db.models import ImportLog, Issuer, Plan, PlanDrugStats, PlanDrugTierStats, PlanNetworkTierRaw
from process.registry_issuer_resolution import (
    RegistryIssuerResolutionError,
    RegistryIssuerResolutionUnavailable,
    _selectors,
    read_registry_issuer_resolutions,
)

import_log_table = ImportLog.__table__
issuer_table = Issuer.__table__
plan_table = Plan.__table__
plan_network_tier_table = PlanNetworkTierRaw.__table__
plan_drug_stats_table = PlanDrugStats.__table__
plan_drug_tier_table = PlanDrugTierStats.__table__


blueprint = Blueprint("issuer", url_prefix="/issuer", version=1)


def _registry_issuer_query(request):
    if request.body or set(request.args) - {"issuer_ids", "reporting_year"}:
        raise ValueError("invalid issuer registry query")
    if len(request.args.getlist("issuer_ids")) != 1 or len(request.args.getlist("reporting_year")) > 1:
        raise ValueError("invalid issuer registry selector")
    raw_ids = request.args.get("issuer_ids", "").split(",")
    if not 1 <= len(raw_ids) <= 200:
        raise ValueError("invalid issuer registry batch")
    issuer_ids = []
    for issuer_id in raw_ids:
        if not issuer_id.isascii() or not issuer_id.isdecimal() or not 1 <= len(issuer_id) <= 5:
            raise ValueError("invalid issuer registry identity")
        if len(issuer_id) != 5 and str(int(issuer_id)) != issuer_id:
            raise ValueError("invalid issuer registry identity")
        issuer_ids.append(issuer_id if len(issuer_id) == 5 else int(issuer_id))
    raw_year = request.args.get("reporting_year")
    if raw_year is not None and (len(raw_year) != 4 or not raw_year.isascii() or not raw_year.isdecimal()):
        raise ValueError("invalid issuer registry reporting period")
    year = int(raw_year) if raw_year is not None else None
    try:
        return _selectors(issuer_ids, year), year
    except RegistryIssuerResolutionError as error:
        raise ValueError("invalid issuer registry selector") from error


@blueprint.get("/registry", ignore_body=False)
async def issuer_registry(request):
    """Return dated company/group evidence for a bounded issuer batch."""
    headers_by_name = {"Cache-Control": "private, no-store"}
    try:
        issuer_ids, reporting_year = _registry_issuer_query(request)
        async with request.ctx.sa_session.begin():
            await request.ctx.sa_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            connection = await request.ctx.sa_session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            issuers = await read_registry_issuer_resolutions(driver, issuer_ids, reporting_year=reporting_year)
        result = response.json({"issuers": issuers}, headers=headers_by_name)
        if len(result.body) > 8 * 1024 * 1024:
            raise RegistryIssuerResolutionUnavailable("Issuer response exceeds its complete bound")
        return result
    except RegistryIssuerResolutionError, RegistryIssuerResolutionUnavailable, SQLAlchemyError:
        return response.json({"error": {"code": "issuer_registry_unavailable"}}, status=503, headers=headers_by_name)
    except ValueError:
        return response.json(
            {"error": {"code": "issuer_registry_request_invalid"}}, status=400, headers=headers_by_name
        )


def _row_to_dict(row):
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        try:
            row_by_field = dict(mapping)
        except TypeError, ValueError:
            row_by_field = None
        if row_by_field is not None:
            return row_by_field
    if hasattr(row, "keys") and hasattr(row, "__getitem__"):
        try:
            row_by_field = {key: row[key] for key in row.keys()}
        except TypeError, ValueError:
            row_by_field = None
        if row_by_field is not None:
            return row_by_field
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except TypeError, ValueError:
        return {}


async def _load_issuer_plans(session, issuer_id):
    plans_stmt = (
        select(
            plan_table,
            plan_network_tier_table.c.checksum_network.label("network_checksum"),
            plan_network_tier_table.c.network_tier.label("network_tier_value"),
        )
        .select_from(
            plan_table.outerjoin(
                plan_network_tier_table,
                and_(
                    plan_table.c.plan_id == plan_network_tier_table.c.plan_id,
                    plan_table.c.year == plan_network_tier_table.c.year,
                ),
            )
        )
        .where(plan_table.c.issuer_id == issuer_id)
        .order_by(plan_table.c.year.desc(), plan_table.c.marketing_name.asc())
    )
    plans_result = await session.execute(plans_stmt)
    plans = []
    plan_by_key = {}
    for plan_result_row in plans_result:
        row_dict = _row_to_dict(plan_result_row)
        plan_key = (row_dict.get("plan_id"), row_dict.get("year"))
        plan_data_by_field = plan_by_key.get(plan_key)
        if plan_data_by_field is None:
            plan_data_by_field = {column.name: row_dict.get(column.name) for column in plan_table.c}
            plan_data_by_field["network"] = {"cmsgov_network": plan_data_by_field.get("network")}
            plan_by_key[plan_key] = plan_data_by_field
            plans.append(plan_data_by_field)
        checksum_network = row_dict.get("network_checksum")
        network_tier = row_dict.get("network_tier_value")
        if checksum_network is not None:
            display_name = (network_tier or "N/A").replace("-", " ").replace("  ", " ")
            plan_data_by_field["network"].update(
                {
                    "network_tier": network_tier or "N/A",
                    "display_name": display_name,
                    "checksum": checksum_network,
                }
            )
    return plans


def _build_issuer_drug_summary(stats_map, tier_rows):
    tiers = []
    for tier_summary_row in tier_rows:
        label = tier_summary_row[0] or "UNKNOWN"
        tiers.append(
            {
                "tier_slug": normalize_drug_tier_slug(label),
                "tier_label": label,
                "drug_count": int(tier_summary_row[1] or 0),
            }
        )
    tiers.sort(key=lambda entry: (-entry["drug_count"], entry["tier_slug"]))
    return {
        "total_drugs": int(stats_map.get("total_drugs") or 0),
        "authorization": {
            "required": int(stats_map.get("auth_required") or 0),
            "not_required": int(stats_map.get("auth_not_required") or 0),
        },
        "step_therapy": {
            "required": int(stats_map.get("step_required") or 0),
            "not_required": int(stats_map.get("step_not_required") or 0),
        },
        "quantity_limits": {
            "has_limit": int(stats_map.get("quantity_limit") or 0),
            "no_limit": int(stats_map.get("quantity_no_limit") or 0),
        },
        "tiers": tiers,
    }


async def _load_issuer_drug_summary(session, issuer_id):
    stats_stmt = (
        select(
            func.coalesce(func.sum(plan_drug_stats_table.c.total_drugs), 0).label("total_drugs"),
            func.coalesce(func.sum(plan_drug_stats_table.c.auth_required), 0).label("auth_required"),
            func.coalesce(func.sum(plan_drug_stats_table.c.auth_not_required), 0).label("auth_not_required"),
            func.coalesce(func.sum(plan_drug_stats_table.c.step_required), 0).label("step_required"),
            func.coalesce(func.sum(plan_drug_stats_table.c.step_not_required), 0).label("step_not_required"),
            func.coalesce(func.sum(plan_drug_stats_table.c.quantity_limit), 0).label("quantity_limit"),
            func.coalesce(func.sum(plan_drug_stats_table.c.quantity_no_limit), 0).label("quantity_no_limit"),
        )
        .select_from(plan_drug_stats_table.join(plan_table, plan_drug_stats_table.c.plan_id == plan_table.c.plan_id))
        .where(plan_table.c.issuer_id == issuer_id)
    )
    stats_row = (await session.execute(stats_stmt)).first()
    stats_map = getattr(stats_row, "_mapping", {}) if stats_row else {}
    tier_stmt = (
        select(
            plan_drug_tier_table.c.drug_tier,
            func.coalesce(func.sum(plan_drug_tier_table.c.drug_count), 0).label("drug_count"),
        )
        .select_from(plan_drug_tier_table.join(plan_table, plan_drug_tier_table.c.plan_id == plan_table.c.plan_id))
        .where(plan_table.c.issuer_id == issuer_id)
        .group_by(plan_drug_tier_table.c.drug_tier)
    )
    tier_rows = (await session.execute(tier_stmt)).all()
    return _build_issuer_drug_summary(stats_map, tier_rows)


def _parse_issuer_list_args(args, state):
    requested_state = state or args.get("state")
    state_filter = requested_state.upper() if requested_state else None
    if state_filter and len(state_filter) != 2:
        raise sanic.exceptions.BadRequest("state must be a 2-letter code")
    query_text = str(args.get("q") or "").strip().lower()
    # Explicit access keeps route/query introspection in sync with OpenAPI.
    args.get("page")
    args.get("limit")
    args.get("offset")
    args.get("start")
    args.get("page_size")
    pagination = None
    if any(args.get(name) not in (None, "", "null") for name in ("page", "limit", "offset", "start", "page_size")):
        pagination = parse_pagination(
            args,
            default_limit=50,
            max_limit=200,
            default_page=1,
            allow_offset=True,
            allow_start=True,
            allow_page_size=True,
        )
    return state_filter, query_text, pagination


async def _load_issuer_count_maps(session, state_filter):
    error_stmt = select(
        import_log_table.c.issuer_id,
        sa_db.func.count(import_log_table.c.issuer_id),
    ).group_by(import_log_table.c.issuer_id)
    if state_filter:
        error_stmt = error_stmt.select_from(
            import_log_table.join(issuer_table, import_log_table.c.issuer_id == issuer_table.c.issuer_id)
        ).where(issuer_table.c.state == state_filter)
    error_rows = await session.execute(error_stmt)
    error_count_by_issuer = {error_count_row[0]: error_count_row[1] for error_count_row in error_rows}
    plan_stmt = select(
        plan_table.c.issuer_id,
        sa_db.func.count(plan_table.c.issuer_id),
    ).group_by(plan_table.c.issuer_id)
    if state_filter:
        plan_stmt = plan_stmt.select_from(
            plan_table.join(issuer_table, plan_table.c.issuer_id == issuer_table.c.issuer_id)
        ).where(issuer_table.c.state == state_filter)
    plan_rows = await session.execute(plan_stmt)
    plan_count_by_issuer = {plan_count_row[0]: plan_count_row[1] for plan_count_row in plan_rows}
    return error_count_by_issuer, plan_count_by_issuer


@blueprint.get("/id/<issuer_id>")
async def get_issuer_data(request, issuer_id):
    """Return the requested issuer and its related plan metadata."""
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")

    issuer_id = int(issuer_id)

    issuer_result = await session.execute(select(issuer_table).where(issuer_table.c.issuer_id == issuer_id))
    issuer_row = issuer_result.first()
    if issuer_row is None:
        raise sanic.exceptions.NotFound

    issuer_data = _row_to_dict(issuer_row)

    error_result = await session.execute(
        select(sa_db.func.count(import_log_table.c.checksum)).where(import_log_table.c.issuer_id == issuer_id)
    )
    issuer_data["import_errors"] = error_result.scalar() or 0

    plans = await _load_issuer_plans(session, issuer_id)
    issuer_data["plans"] = plans
    issuer_data["plans_count"] = len(plans)
    issuer_data["drug_summary"] = await _load_issuer_drug_summary(session, issuer_id)
    return response.json(issuer_data, default=str)


@blueprint.get("/", name="issuer_list")
@blueprint.get("/state/<state>")
async def get_issuers(request, state=None):
    """List issuers, optionally restricted to one state."""
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")

    # Explicit access keeps route/query introspection in sync with OpenAPI.
    request.args.get("state")
    request.args.get("q")
    request.args.get("page")
    request.args.get("limit")
    request.args.get("offset")
    request.args.get("start")
    request.args.get("page_size")
    state_filter, query_text, pagination = _parse_issuer_list_args(request.args, state)

    issuer_stmt = select(issuer_table)
    if state_filter:
        issuer_stmt = issuer_stmt.where(issuer_table.c.state == state_filter)
    issuer_stmt = issuer_stmt.order_by(issuer_table.c.state.asc(), issuer_table.c.issuer_name.asc())

    issuer_rows = await session.execute(issuer_stmt)
    issuers = [_row_to_dict(issuer_result_row) for issuer_result_row in issuer_rows]

    if not issuers:
        raise sanic.exceptions.NotFound

    if query_text:
        issuers = [
            issuer
            for issuer in issuers
            if query_text in str(issuer.get("issuer_name") or "").lower()
            or query_text in str(issuer.get("issuer_marketing_name") or "").lower()
        ]
        if not issuers:
            raise sanic.exceptions.NotFound

    error_count_by_issuer, plan_count_by_issuer = await _load_issuer_count_maps(session, state_filter)

    for issuer in issuers:
        issuer_id = issuer.get("issuer_id")
        issuer["import_errors"] = error_count_by_issuer.get(issuer_id, 0)
        issuer["plan_count"] = plan_count_by_issuer.get(issuer_id, 0)

    if pagination is not None:
        start = max(0, pagination.offset)
        end = start + pagination.limit
        issuers = issuers[start:end]

    return response.json(issuers, default=str)
