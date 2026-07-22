# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import sanic.exceptions
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import and_, exists, func, or_, select

from api.code_systems import (
    canonical_catalog_code,
    is_restricted_terminology_system,
    normalize_code_system,
    restricted_terminology_public_enabled,
)
from api.endpoint.pagination import parse_pagination
from db.models import CodeCatalog, CodeCrosswalk, CodeSynonym

blueprint = Blueprint("codes", url_prefix="/codes", version=1)

code_catalog_table = CodeCatalog.__table__
code_crosswalk_table = CodeCrosswalk.__table__
code_synonym_table = CodeSynonym.__table__

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


def _normalize_code_system(raw_system: Any) -> str:
    return normalize_code_system(raw_system)


def _canonical_code_for_system(code_system: str, raw_code: Any) -> str:
    return canonical_catalog_code(code_system, raw_code)


def _restricted_public_filter(column):
    if restricted_terminology_public_enabled():
        return None
    return func.upper(column).notin_(("SNOMEDCT_US",))


@blueprint.get("/")
async def list_codes(request):
    """List normalized reference codes with bounded filtering and paging."""

    session = _get_session(request)
    args = request.args
    pagination = parse_pagination(args, default_limit=25, max_limit=MAX_LIMIT)

    code_system = _normalize_code_system(args.get("code_system"))
    q = str(args.get("q", "")).strip().lower()
    source_name = str(args.get("source", "")).strip().lower()
    order = _normalize_order(args.get("order"))
    order_by = str(args.get("order_by") or "code").strip().lower()

    filters = []
    restricted_filter = _restricted_public_filter(code_catalog_table.c.code_system)
    if restricted_filter is not None:
        filters.append(restricted_filter)
    if code_system:
        filters.append(func.upper(code_catalog_table.c.code_system) == code_system)
    if source_name:
        filters.append(func.lower(code_catalog_table.c.source) == source_name)
    if q:
        q_like = f"%{q}%"
        filters.append(
            or_(
                func.lower(code_catalog_table.c.code).like(q_like),
                func.lower(code_catalog_table.c.display_name).like(q_like),
                func.lower(code_catalog_table.c.short_description).like(q_like),
                exists(
                    select(1).where(
                        and_(
                            func.upper(code_synonym_table.c.code_system)
                            == func.upper(code_catalog_table.c.code_system),
                            func.upper(code_synonym_table.c.code) == func.upper(code_catalog_table.c.code),
                            func.lower(code_synonym_table.c.synonym).like(q_like),
                        )
                    )
                ),
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

    order_column_by_name = {
        "code": code_catalog_table.c.code,
        "code_system": code_catalog_table.c.code_system,
        "display_name": code_catalog_table.c.display_name,
    }
    order_column = order_column_by_name.get(order_by)
    if order_column is None:
        allowed = ", ".join(sorted(order_column_by_name.keys()))
        raise InvalidUsage(f"Unsupported order_by '{order_by}'. Allowed: {allowed}")
    query = query.order_by(order_column.desc() if order == "desc" else order_column.asc())
    query = query.limit(pagination.limit).offset(pagination.offset)

    code_result = await session.execute(query)
    code_items = [
        _json_safe_row(_row_to_dict(code_row)) for code_row in code_result
    ]

    return response.json(
        {
            "items": code_items,
            "pagination": {
                "total": total,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {
                "code_system": code_system or None,
                "q": q or None,
                "source": source_name or None,
                "order_by": order_by,
                "order": order,
            },
        }
    )


@blueprint.get("/<code_system>/<code>")
async def get_code(request, code_system: str, code: str):
    """Return one normalized code-system entry when it exists."""

    session = _get_session(request)
    normalized_system = _normalize_code_system(code_system)
    normalized_code = _canonical_code_for_system(normalized_system, code)
    if not normalized_system or not normalized_code:
        raise InvalidUsage("Path parameters 'code_system' and 'code' are required")
    if is_restricted_terminology_system(normalized_system) and not restricted_terminology_public_enabled():
        raise sanic.exceptions.NotFound("Code not found")

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
    """Return bounded crosswalk relationships for one reference code."""

    session = _get_session(request)
    normalized_system = _normalize_code_system(code_system)
    normalized_code = _canonical_code_for_system(normalized_system, code)
    if not normalized_system or not normalized_code:
        raise InvalidUsage("Path parameters 'code_system' and 'code' are required")
    if is_restricted_terminology_system(normalized_system) and not restricted_terminology_public_enabled():
        raise sanic.exceptions.NotFound("Code not found")

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
    if not restricted_terminology_public_enabled():
        forward_query = forward_query.where(func.upper(code_crosswalk_table.c.to_system).notin_(("SNOMEDCT_US",)))
        reverse_query = reverse_query.where(func.upper(code_crosswalk_table.c.from_system).notin_(("SNOMEDCT_US",)))

    forward_result = await session.execute(forward_query)
    reverse_result = await session.execute(reverse_query)

    forward_items = [
        _json_safe_row(_row_to_dict(crosswalk_row))
        for crosswalk_row in forward_result
    ]
    reverse_items = [
        _json_safe_row(_row_to_dict(crosswalk_row))
        for crosswalk_row in reverse_result
    ]

    return response.json(
        {
            "input_code": {"code_system": normalized_system, "code": normalized_code},
            "related": {
                "forward": forward_items,
                "reverse": reverse_items,
            },
        }
    )
