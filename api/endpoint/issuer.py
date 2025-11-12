# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import and_, select

from db.connection import db as sa_db
from db.models import ImportLog, Issuer, Plan, PlanNetworkTierRaw

import_log_table = ImportLog.__table__
issuer_table = Issuer.__table__
plan_table = Plan.__table__
plan_network_tier_table = PlanNetworkTierRaw.__table__


blueprint = Blueprint("issuer", url_prefix="/issuer", version=1)


def _row_to_dict(row):
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


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

    return response.json(issuer_data)


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

    return response.json(issuers)