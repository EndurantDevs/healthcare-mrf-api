# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import sanic.exceptions
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import and_, func, or_, select

from api.endpoint.pagination import parse_pagination
from db.models import CodeCatalog, CodeCrosswalk

blueprint = Blueprint("codes", url_prefix="/codes", version=1)

code_catalog_table = CodeCatalog.__table__
code_crosswalk_table = CodeCrosswalk.__table__

MAX_LIMIT = 200


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


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_safe_value(value) for key, value in row.items()}


def _normalize_order(raw_order: Any):
    order = str(raw_order or "asc").strip().lower()
    if order not in {"asc", "desc"}:
        raise InvalidUsage("Parameter 'order' must be either 'asc' or 'desc'")
    return order


@blueprint.get("/")
async def list_codes(request):
    session = _get_session(request)
    args = request.args
    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)

    code_system = str(args.get("code_system", "")).strip().upper()
    q = str(args.get("q", "")).strip().lower()
    source = str(args.get("source", "")).strip().lower()
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "code").strip().lower()

    filters = []
    if code_system:
        filters.append(func.upper(code_catalog_table.c.code_system) == code_system)
    if source:
        filters.append(func.lower(code_catalog_table.c.source) == source)
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(code_catalog_table.c.code).like(q_like),
                func.lower(code_catalog_table.c.display_name).like(q_like),
                func.lower(code_catalog_table.c.short_description).like(q_like),
            )
        )

    where_clause = and_(*filters) if filters else None
    count_query = select(func.count()).select_from(code_catalog_table)
    if where_clause is not None:
        count_query = count_query.where(where_clause)
    count_result = await session.execute(count_query)
    total = int(count_result.scalar() or 0)

    query = select(code_catalog_table)
    if where_clause is not None:
        query = query.where(where_clause)

    order_fields = {
        "code": code_catalog_table.c.code,
        "code_system": code_catalog_table.c.code_system,
        "display_name": code_catalog_table.c.display_name,
        "code_checksum": code_catalog_table.c.code_checksum,
    }
    order_column = order_fields.get(order_by)
    if order_column is None:
        allowed = ", ".join(sorted(order_fields.keys()))
        raise InvalidUsage(f"Unsupported order_by '{order_by}'. Allowed: {allowed}")
    query = query.order_by(order_column.desc() if order == "desc" else order_column.asc())
    query = query.limit(pagination.limit).offset(pagination.offset)

    result = await session.execute(query)
    items = [_json_safe_row(_row_to_dict(row)) for row in result]

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
                "code_system": code_system or None,
                "q": q or None,
                "source": source or None,
                "order_by": order_by,
                "order": order,
            },
        }
    )


@blueprint.get("/<code_system>/<code>")
async def get_code(request, code_system: str, code: str):
    session = _get_session(request)
    normalized_system = str(code_system).strip().upper()
    normalized_code = str(code).strip().upper()
    if not normalized_system or not normalized_code:
        raise InvalidUsage("Path parameters 'code_system' and 'code' are required")

    query = select(code_catalog_table).where(
        and_(
            func.upper(code_catalog_table.c.code_system) == normalized_system,
            func.upper(code_catalog_table.c.code) == normalized_code,
        )
    )
    result = await session.execute(query)
    row = result.first()
    if row is None:
        raise sanic.exceptions.NotFound("Code not found")
    return response.json(_json_safe_row(_row_to_dict(row)))


@blueprint.get("/<code_system>/<code>/related")
async def get_related_codes(request, code_system: str, code: str):
    session = _get_session(request)
    normalized_system = str(code_system).strip().upper()
    normalized_code = str(code).strip().upper()
    if not normalized_system or not normalized_code:
        raise InvalidUsage("Path parameters 'code_system' and 'code' are required")

    forward_query = select(code_crosswalk_table).where(
        and_(
            func.upper(code_crosswalk_table.c.from_system) == normalized_system,
            func.upper(code_crosswalk_table.c.from_code) == normalized_code,
        )
    )
    reverse_query = select(code_crosswalk_table).where(
        and_(
            func.upper(code_crosswalk_table.c.to_system) == normalized_system,
            func.upper(code_crosswalk_table.c.to_code) == normalized_code,
        )
    )

    forward_result = await session.execute(forward_query)
    reverse_result = await session.execute(reverse_query)

    forward_items = [_json_safe_row(_row_to_dict(row)) for row in forward_result]
    reverse_items = [_json_safe_row(_row_to_dict(row)) for row in reverse_result]

    return response.json(
        {
            "input_code": {"code_system": normalized_system, "code": normalized_code},
            "related": {
                "forward": forward_items,
                "reverse": reverse_items,
            },
        }
    )
