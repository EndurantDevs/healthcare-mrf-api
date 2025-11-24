# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import and_, func, select

from api.tier_utils import normalize_drug_tier_slug
from db.connection import db as sa_db
from db.models import (ImportLog, Issuer, Plan, PlanDrugStats,
                       PlanDrugTierStats, PlanNetworkTierRaw)

import_log_table = ImportLog.__table__
issuer_table = Issuer.__table__
plan_table = Plan.__table__
plan_network_tier_table = PlanNetworkTierRaw.__table__
plan_drug_stats_table = PlanDrugStats.__table__
plan_drug_tier_table = PlanDrugTierStats.__table__


blueprint = Blueprint("issuer", url_prefix="/issuer", version=1)


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


@blueprint.get("/id/<issuer_id>")
async def get_issuer_data(request, issuer_id):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")

    issuer_id = int(issuer_id)

    issuer_result = await session.execute(
        select(issuer_table).where(issuer_table.c.issuer_id == issuer_id)
    )
    issuer_row = issuer_result.first()
    if issuer_row is None:
        raise sanic.exceptions.NotFound

    issuer_data = _row_to_dict(issuer_row)

    error_result = await session.execute(
        select(sa_db.func.count(import_log_table.c.checksum)).where(
            import_log_table.c.issuer_id == issuer_id
        )
    )
    issuer_data["import_errors"] = error_result.scalar() or 0

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
    seen = {}

    for row in plans_result:
        row_dict = _row_to_dict(row)
        plan_key = (row_dict.get("plan_id"), row_dict.get("year"))

        plan_entry = seen.get(plan_key)
        if plan_entry is None:
            plan_entry = {
                column.name: row_dict.get(column.name)
                for column in plan_table.c
            }
            plan_entry["network"] = {"cmsgov_network": plan_entry.get("network")}
            seen[plan_key] = plan_entry
            plans.append(plan_entry)

        checksum_network = row_dict.get("network_checksum")
        network_tier = row_dict.get("network_tier_value")
        if checksum_network is not None:
            display_name = (network_tier or "N/A").replace("-", " ").replace("  ", " ")
            plan_entry["network"].update(
                {
                    "network_tier": network_tier or "N/A",
                    "display_name": display_name,
                    "checksum": checksum_network,
                }
            )

    issuer_data["plans"] = plans
    issuer_data["plans_count"] = len(plans)

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
        .select_from(
            plan_drug_stats_table.join(
                plan_table, plan_drug_stats_table.c.plan_id == plan_table.c.plan_id
            )
        )
        .where(plan_table.c.issuer_id == issuer_id)
    )
    stats_result = await session.execute(stats_stmt)
    stats_row = stats_result.first()
    stats_map = getattr(stats_row, "_mapping", {}) if stats_row else {}
    drug_summary = {
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
        "tiers": [],
    }

    tier_stmt = (
        select(
            plan_drug_tier_table.c.drug_tier,
            func.coalesce(func.sum(plan_drug_tier_table.c.drug_count), 0).label("drug_count"),
        )
        .select_from(
            plan_drug_tier_table.join(
                plan_table, plan_drug_tier_table.c.plan_id == plan_table.c.plan_id
            )
        )
        .where(plan_table.c.issuer_id == issuer_id)
        .group_by(plan_drug_tier_table.c.drug_tier)
    )
    tier_rows = (await session.execute(tier_stmt)).all()
    tiers = []
    for row in tier_rows:
        label = row[0] or "UNKNOWN"
        tiers.append(
            {
                "tier_slug": normalize_drug_tier_slug(label),
                "tier_label": label,
                "drug_count": int(row[1] or 0),
            }
        )
    tiers.sort(key=lambda entry: (-entry["drug_count"], entry["tier_slug"]))
    drug_summary["tiers"] = tiers

    issuer_data["drug_summary"] = drug_summary

    return response.json(issuer_data, default=str)


@blueprint.get("/", name="issuer_list")
@blueprint.get("/state/<state>")
async def get_issuers(request, state=None):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")

    state_filter = state.upper() if state else None

    issuer_stmt = select(issuer_table)
    if state_filter:
        issuer_stmt = issuer_stmt.where(issuer_table.c.state == state_filter)
    issuer_stmt = issuer_stmt.order_by(
        issuer_table.c.state.asc(), issuer_table.c.issuer_name.asc()
    )

    issuer_rows = await session.execute(issuer_stmt)
    issuers = [_row_to_dict(row) for row in issuer_rows]

    if not issuers:
        raise sanic.exceptions.NotFound

    error_stmt = select(
        import_log_table.c.issuer_id,
        sa_db.func.count(import_log_table.c.issuer_id),
    ).group_by(import_log_table.c.issuer_id)
    if state_filter:
        error_stmt = error_stmt.select_from(
            import_log_table.join(
                issuer_table, import_log_table.c.issuer_id == issuer_table.c.issuer_id
            )
        ).where(issuer_table.c.state == state_filter)

    error_rows = await session.execute(error_stmt)
    error_counts = {row[0]: row[1] for row in error_rows}

    plan_stmt = select(
        plan_table.c.issuer_id,
        sa_db.func.count(plan_table.c.issuer_id),
    ).group_by(plan_table.c.issuer_id)
    if state_filter:
        plan_stmt = plan_stmt.select_from(
            plan_table.join(
                issuer_table, plan_table.c.issuer_id == issuer_table.c.issuer_id
            )
        ).where(issuer_table.c.state == state_filter)

    plan_rows = await session.execute(plan_stmt)
    plan_counts = {row[0]: row[1] for row in plan_rows}

    for issuer in issuers:
        issuer_id = issuer.get("issuer_id")
        issuer["import_errors"] = error_counts.get(issuer_id, 0)
        issuer["plan_count"] = plan_counts.get(issuer_id, 0)

    return response.json(issuers, default=str)
