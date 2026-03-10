# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import re
from typing import Any

from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy import and_, func, or_, select, text

from api.endpoint.pagination import parse_pagination
from db.models import (CodeCrosswalk, PartDFormularySnapshot, PartDImportRun, PartDMedicationCost,
                       PartDPharmacyActivity)

blueprint = Blueprint("partd_formulary", url_prefix="/formulary/partd", version=1)

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 200
_NON_DIGIT = re.compile(r"[^0-9]+")


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _parse_date_param(value: str | None, param_name: str) -> datetime.date | None:
    if value in (None, "", "null"):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.date.fromisoformat(text)
    except ValueError as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be a valid ISO date (YYYY-MM-DD)") from exc


def _parse_int_param(value: str | None, param_name: str) -> int | None:
    if value in (None, "", "null"):
        return None
    try:
        return int(str(value).strip())
    except ValueError as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be an integer") from exc


def _parse_npi(npi: str) -> int:
    try:
        parsed = int(str(npi).strip())
    except ValueError as exc:
        raise InvalidUsage("NPI must be numeric") from exc
    if parsed <= 0:
        raise InvalidUsage("NPI must be positive")
    return parsed


def _parse_plan_ids(raw: str | None) -> list[str]:
    if raw in (None, "", "null"):
        return []
    items = []
    for value in str(raw).split(","):
        normalized = value.strip()
        if normalized:
            items.append(normalized[:32])
    return sorted(set(items))


def _normalize_code(code_system: str, code: str) -> tuple[str, str]:
    normalized_system = str(code_system or "").strip().lower()
    if normalized_system not in {"rxnorm", "rxcui", "ndc"}:
        raise InvalidUsage("code_system must be one of: rxnorm, rxcui, ndc")
    normalized_code = _NON_DIGIT.sub("", str(code or "").strip())
    if not normalized_code:
        raise InvalidUsage("code must contain at least one digit")
    if normalized_system in {"rxnorm", "rxcui"}:
        return "RXNORM", normalized_code
    return "NDC", normalized_code


def _qualified_table_name(table) -> str:
    schema = table.schema or "mrf"
    return f"{schema}.{table.name}"


def _bind_in_clause(prefix: str, values: list[str], params: dict[str, Any]) -> str:
    placeholders: list[str] = []
    for idx, value in enumerate(values):
        key = f"{prefix}_{idx}"
        params[key] = value
        placeholders.append(f":{key}")
    if not placeholders:
        return "NULL"
    return ", ".join(placeholders)


@blueprint.get("/import/status")
async def get_partd_import_status(request):
    session = _get_session(request)
    run_id = request.args.get("run_id")

    run_table = PartDImportRun.__table__
    if run_id:
        run_stmt = select(run_table).where(run_table.c.run_id == run_id)
    else:
        run_stmt = select(run_table).order_by(run_table.c.started_at.desc().nullslast()).limit(1)

    run_row = (await session.execute(run_stmt)).first()
    if run_row is None:
        raise NotFound("No Part D imports found")

    run_data = dict(run_row._mapping)
    snapshot_table = PartDFormularySnapshot.__table__
    run_snapshots = (
        await session.execute(
            select(
                func.count().label("snapshot_count"),
                func.coalesce(func.sum(snapshot_table.c.row_count_activity), 0).label("activity_rows"),
                func.coalesce(func.sum(snapshot_table.c.row_count_pricing), 0).label("pricing_rows"),
            ).where(snapshot_table.c.run_id == run_data["run_id"])
        )
    ).first()
    snapshot_map = run_snapshots._mapping if run_snapshots else {}

    return response.json(
        {
            "run_id": run_data.get("run_id"),
            "import_id": run_data.get("import_id"),
            "status": run_data.get("status"),
            "started_at": run_data.get("started_at").isoformat() if run_data.get("started_at") else None,
            "finished_at": run_data.get("finished_at").isoformat() if run_data.get("finished_at") else None,
            "source_summary": run_data.get("source_summary") or {},
            "error_text": run_data.get("error_text"),
            "snapshots": {
                "count": int(snapshot_map.get("snapshot_count") or 0),
                "activity_rows": int(snapshot_map.get("activity_rows") or 0),
                "pricing_rows": int(snapshot_map.get("pricing_rows") or 0),
            },
        }
    )


@blueprint.get("/snapshots")
async def list_partd_snapshots(request):
    session = _get_session(request)
    args = request.args
    args.get("page")
    args.get("page_size")
    pagination = parse_pagination(
        args,
        default_limit=DEFAULT_PAGE_SIZE,
        max_limit=MAX_PAGE_SIZE,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )

    source_type = request.args.get("source_type")
    run_id = request.args.get("run_id")
    snapshot_table = PartDFormularySnapshot.__table__
    filters = []
    if source_type:
        filters.append(snapshot_table.c.source_type == source_type.lower())
    if run_id:
        filters.append(snapshot_table.c.run_id == run_id)

    where_clause = and_(*filters) if filters else None

    count_stmt = select(func.count()).select_from(snapshot_table)
    if where_clause is not None:
        count_stmt = count_stmt.where(where_clause)
    total = (await session.execute(count_stmt)).scalar() or 0

    data_stmt = select(snapshot_table)
    if where_clause is not None:
        data_stmt = data_stmt.where(where_clause)
    data_stmt = data_stmt.order_by(
        snapshot_table.c.release_date.desc().nullslast(),
        snapshot_table.c.snapshot_id.desc(),
    ).offset(pagination.offset).limit(pagination.limit)

    rows = (await session.execute(data_stmt)).all()
    items = []
    for row in rows:
        mapping = row._mapping
        items.append(
            {
                "snapshot_id": mapping.get("snapshot_id"),
                "run_id": mapping.get("run_id"),
                "source_type": mapping.get("source_type"),
                "source_url": mapping.get("source_url"),
                "artifact_name": mapping.get("artifact_name"),
                "release_date": mapping.get("release_date").isoformat() if mapping.get("release_date") else None,
                "cutoff_month": mapping.get("cutoff_month").isoformat() if mapping.get("cutoff_month") else None,
                "status": mapping.get("status"),
                "row_count_activity": int(mapping.get("row_count_activity") or 0),
                "row_count_pricing": int(mapping.get("row_count_pricing") or 0),
                "imported_at": mapping.get("imported_at").isoformat() if mapping.get("imported_at") else None,
                "metadata": mapping.get("metadata_json") or {},
            }
        )

    return response.json(
        {
            "page": pagination.page,
            "page_size": pagination.limit,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "total": total,
            "items": items,
        }
    )


@blueprint.get("/pharmacies/<npi>/activity")
async def get_pharmacy_partd_activity(request, npi):
    session = _get_session(request)
    parsed_npi = _parse_npi(npi)
    as_of = _parse_date_param(request.args.get("as_of"), "as_of") or datetime.date.today()
    limit = _parse_int_param(request.args.get("limit"), "limit") or 50
    limit = max(1, min(limit, MAX_PAGE_SIZE))

    activity_table = PartDPharmacyActivity.__table__
    activity_table_name = _qualified_table_name(activity_table)
    rows = (
        await session.execute(
            text(
                f"""
                SELECT
                    a.snapshot_id,
                    p.plan_id,
                    p.contract_id,
                    p.segment_id,
                    a.year,
                    a.medicare_active,
                    a.pharmacy_name,
                    a.pharmacy_type,
                    a.mail_order,
                    a.effective_from,
                    a.effective_to,
                    a.source_type
                FROM {activity_table_name} a
                CROSS JOIN LATERAL unnest(a.plan_ids, a.contract_ids, a.segment_ids)
                    AS p(plan_id, contract_id, segment_id)
                WHERE a.npi = :npi
                  AND a.effective_from <= :as_of
                  AND (a.effective_to IS NULL OR a.effective_to >= :as_of)
                ORDER BY a.effective_from DESC, p.plan_id ASC
                LIMIT :limit;
                """
            ),
            {"npi": parsed_npi, "as_of": as_of, "limit": limit},
        )
    ).mappings().all()

    if not rows:
        return response.json(
            {
                "npi": parsed_npi,
                "as_of": as_of.isoformat(),
                "medicare_active": False,
                "medicare_active_as_of": None,
                "source_type": None,
                "plan_ids": [],
                "items": [],
            }
        )

    items = []
    active = False
    source_type = None
    for mapping in rows:
        if source_type is None and mapping.get("source_type"):
            source_type = mapping.get("source_type")
        item_active = bool(mapping.get("medicare_active"))
        active = active or item_active
        items.append(
            {
                "snapshot_id": mapping.get("snapshot_id"),
                "plan_id": mapping.get("plan_id"),
                "contract_id": mapping.get("contract_id"),
                "segment_id": mapping.get("segment_id"),
                "year": mapping.get("year"),
                "medicare_active": item_active,
                "pharmacy_name": mapping.get("pharmacy_name"),
                "pharmacy_type": mapping.get("pharmacy_type"),
                "mail_order": mapping.get("mail_order"),
                "effective_from": mapping.get("effective_from").isoformat() if mapping.get("effective_from") else None,
                "effective_to": mapping.get("effective_to").isoformat() if mapping.get("effective_to") else None,
                "source_type": mapping.get("source_type"),
            }
        )

    plan_ids = sorted({item["plan_id"] for item in items if item.get("plan_id")})
    return response.json(
        {
            "npi": parsed_npi,
            "as_of": as_of.isoformat(),
            "medicare_active": active,
            "medicare_active_as_of": as_of.isoformat() if active else None,
            "source_type": source_type,
            "plan_ids": plan_ids,
            "items": items,
        }
    )


@blueprint.get("/pharmacies/<npi>/medications/<code_system>/<code>/costs")
async def get_pharmacy_medication_costs(request, npi, code_system, code):
    session = _get_session(request)
    parsed_npi = _parse_npi(npi)
    normalized_system, normalized_code = _normalize_code(code_system, code)
    args = request.args
    args.get("page")
    args.get("page_size")
    pagination = parse_pagination(
        args,
        default_limit=DEFAULT_PAGE_SIZE,
        max_limit=MAX_PAGE_SIZE,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )

    year = _parse_int_param(request.args.get("year"), "year")
    month_date = _parse_date_param(request.args.get("month"), "month")
    if month_date is not None:
        month_date = datetime.date(month_date.year, month_date.month, 1)

    requested_plan_ids = _parse_plan_ids(request.args.get("plan_ids"))
    activity_table = PartDPharmacyActivity.__table__
    activity_table_name = _qualified_table_name(activity_table)
    if requested_plan_ids:
        effective_plan_ids = requested_plan_ids
    else:
        as_of = month_date or datetime.date.today()
        plan_rows = (
            await session.execute(
                text(
                    f"""
                    SELECT DISTINCT p.plan_id
                    FROM {activity_table_name} a
                    CROSS JOIN LATERAL unnest(a.plan_ids) AS p(plan_id)
                    WHERE a.npi = :npi
                      AND a.medicare_active IS TRUE
                      AND a.effective_from <= :as_of
                      AND (a.effective_to IS NULL OR a.effective_to >= :as_of)
                    ORDER BY p.plan_id ASC;
                    """
                ),
                {"npi": parsed_npi, "as_of": as_of},
            )
        ).mappings().all()
        effective_plan_ids = sorted({row.get("plan_id") for row in plan_rows if row.get("plan_id")})

    cost_table = PartDMedicationCost.__table__
    cost_table_name = _qualified_table_name(cost_table)
    query_params: dict[str, Any] = {
        "normalized_code": normalized_code,
    }
    where_parts: list[str] = []

    if normalized_system == "RXNORM":
        crosswalk_table = CodeCrosswalk.__table__
        direct_ndc_rows = (
            await session.execute(
                select(crosswalk_table.c.to_code).where(
                    and_(
                        func.upper(crosswalk_table.c.from_system).in_(["RXNORM", "RXCUI"]),
                        crosswalk_table.c.from_code == normalized_code,
                        func.upper(crosswalk_table.c.to_system) == "NDC",
                    )
                )
            )
        ).all()
        reverse_ndc_rows = (
            await session.execute(
                select(crosswalk_table.c.from_code).where(
                    and_(
                        func.upper(crosswalk_table.c.to_system).in_(["RXNORM", "RXCUI"]),
                        crosswalk_table.c.to_code == normalized_code,
                        func.upper(crosswalk_table.c.from_system) == "NDC",
                    )
                )
            )
        ).all()
        crosswalk_ndcs = sorted(
            {
                _NON_DIGIT.sub("", str(row[0] or "").strip())
                for row in (direct_ndc_rows + reverse_ndc_rows)
                if _NON_DIGIT.sub("", str(row[0] or "").strip())
            }
        )
        code_clauses = [
            "(c.code_system = 'RXNORM' AND c.code = :normalized_code)",
            "c.rxnorm_id = :normalized_code",
        ]
        if crosswalk_ndcs:
            ndc_in = _bind_in_clause("crosswalk_ndc", crosswalk_ndcs, query_params)
            code_clauses.append(f"c.ndc11 IN ({ndc_in})")
        where_parts.append(f"({' OR '.join(code_clauses)})")
    else:
        where_parts.append(
            "((c.code_system = 'NDC' AND c.code = :normalized_code) OR c.ndc11 = :normalized_code)"
        )

    if not effective_plan_ids:
        return response.json(
            {
                "npi": parsed_npi,
                "query": {
                    "input_code_system": code_system,
                    "input_code": code,
                    "normalized_code_system": normalized_system,
                    "normalized_code": normalized_code,
                    "match_strategy": "no_active_plans_for_pharmacy",
                    "year": year,
                    "month": month_date.isoformat() if month_date else None,
                },
                "plans": {
                    "requested_plan_ids": requested_plan_ids,
                    "effective_plan_ids": [],
                    "selection_mode": "requested" if requested_plan_ids else "pharmacy_active_medicare_plans",
                },
                "page": pagination.page,
                "page_size": pagination.limit,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "total": 0,
                "items": [],
            }
        )

    plan_in = _bind_in_clause("plan_id", effective_plan_ids, query_params)
    where_parts.append(f"c.plan_ids && ARRAY[{plan_in}]::varchar[]")
    where_parts.append(f"p.plan_id IN ({plan_in})")
    if year is not None:
        query_params["year"] = year
        where_parts.append("c.year = :year")
    if month_date is not None:
        query_params["month_date"] = month_date
        where_parts.append("c.effective_from <= :month_date")
        where_parts.append("(c.effective_to IS NULL OR c.effective_to >= :month_date)")

    total = (
        await session.execute(
            text(
                f"""
                SELECT COUNT(*) AS total
                FROM {cost_table_name} c
                CROSS JOIN LATERAL unnest(c.plan_ids, c.contract_ids, c.segment_ids)
                    AS p(plan_id, contract_id, segment_id)
                WHERE {' AND '.join(where_parts)};
                """
            ),
            query_params,
        )
    ).scalar() or 0

    data_params = dict(query_params)
    data_params["offset"] = pagination.offset
    data_params["limit"] = pagination.limit
    data_rows = (
        await session.execute(
            text(
                f"""
                SELECT
                    c.snapshot_id,
                    c.plan_ids,
                    p.plan_id,
                    p.contract_id,
                    p.segment_id,
                    c.year,
                    c.code_system,
                    c.code,
                    c.normalized_code,
                    c.rxnorm_id,
                    c.ndc11,
                    c.days_supply,
                    c.drug_name,
                    c.tier,
                    c.pharmacy_type,
                    c.mail_order,
                    c.cost_type,
                    c.cost_amount,
                    c.effective_from,
                    c.effective_to,
                    c.source_type
                FROM {cost_table_name} c
                CROSS JOIN LATERAL unnest(c.plan_ids, c.contract_ids, c.segment_ids)
                    AS p(plan_id, contract_id, segment_id)
                WHERE {' AND '.join(where_parts)}
                ORDER BY p.plan_id ASC, c.cost_type ASC, c.effective_from DESC NULLS LAST
                OFFSET :offset
                LIMIT :limit;
                """
            ),
            data_params,
        )
    ).mappings().all()

    items = []
    normalized_match_strategy = "plan_level_ndc" if normalized_system == "NDC" else "plan_level_rxnorm"
    for mapping in data_rows:
        items.append(
            {
                "snapshot_id": mapping.get("snapshot_id"),
                "plan_id": mapping.get("plan_id"),
                "plan_ids": mapping.get("plan_ids") or [],
                "contract_id": mapping.get("contract_id"),
                "segment_id": mapping.get("segment_id"),
                "year": mapping.get("year"),
                "code_system": mapping.get("code_system"),
                "code": mapping.get("code"),
                "normalized_code": mapping.get("normalized_code"),
                "rxnorm_id": mapping.get("rxnorm_id"),
                "ndc11": mapping.get("ndc11"),
                "days_supply": mapping.get("days_supply"),
                "drug_name": mapping.get("drug_name"),
                "tier": mapping.get("tier"),
                "pharmacy_type": mapping.get("pharmacy_type"),
                "mail_order": mapping.get("mail_order"),
                "cost_type": mapping.get("cost_type"),
                "cost_amount": mapping.get("cost_amount"),
                "effective_from": mapping.get("effective_from").isoformat() if mapping.get("effective_from") else None,
                "effective_to": mapping.get("effective_to").isoformat() if mapping.get("effective_to") else None,
                "source_type": mapping.get("source_type"),
            }
        )

    return response.json(
        {
            "npi": parsed_npi,
            "query": {
                "input_code_system": code_system,
                "input_code": code,
                "normalized_code_system": normalized_system,
                "normalized_code": normalized_code,
                "match_strategy": normalized_match_strategy,
                "year": year,
                "month": month_date.isoformat() if month_date else None,
            },
            "plans": {
                "requested_plan_ids": requested_plan_ids,
                "effective_plan_ids": effective_plan_ids,
                "selection_mode": "requested" if requested_plan_ids else "pharmacy_active_medicare_plans",
                "pricing_level": "plan",
            },
            "page": pagination.page,
            "page_size": pagination.limit,
            "limit": pagination.limit,
            "offset": pagination.offset,
            "total": total,
            "items": items,
        }
    )
