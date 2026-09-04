# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from sanic import Blueprint, response
from sqlalchemy import func, or_, select

from api.endpoint.pagination import parse_bool_alias, parse_pagination
from db.models import NUCCTaxonomy

blueprint = Blueprint('nucc', url_prefix='/nucc', version=1)


@blueprint.get('/')
async def index_status_nucc(_request):
    """Return the current NUCC index status."""
    return response.json({})


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _nucc_filters(args, table):
    query_text = str(args.get("q") or "").strip()
    code = str(args.get("code") or "").strip()
    filters = []
    applied_filter_by_name = {}
    if code:
        filters.append(table.c.code.ilike(f"%{code}%"))
        applied_filter_by_name["code"] = code
    if query_text:
        applied_filter_by_name["q"] = query_text
        filters.append(
            or_(
                table.c.code.ilike(f"%{query_text}%"),
                table.c.display_name.ilike(f"%{query_text}%"),
                table.c.classification.ilike(f"%{query_text}%"),
                table.c.specialization.ilike(f"%{query_text}%"),
                table.c.grouping.ilike(f"%{query_text}%"),
                table.c.section.ilike(f"%{query_text}%"),
            )
        )
    return filters, applied_filter_by_name


def _ordered_nucc_query(table, filters, order):
    statement = select(NUCCTaxonomy)
    if filters:
        statement = statement.where(*filters)
    order_columns = (
        (table.c.display_name.desc(), table.c.code.desc())
        if order == "desc"
        else (table.c.display_name.asc(), table.c.code.asc())
    )
    return statement.order_by(*order_columns)


@blueprint.get('/all')
async def all_of_nucc(request):
    """Return the filtered NUCC taxonomy collection."""
    session = _get_session(request)
    args = request.args
    table = NUCCTaxonomy.__table__
    order = str(args.get("order") or "asc").strip().lower()
    if order not in {"asc", "desc"}:
        order = "asc"
    include_meta = parse_bool_alias(args, "include_meta", "paginate", default=False)
    has_pagination_args = any(
        args.get(name) not in (None, "", "null")
        for name in ("limit", "offset", "page", "start", "page_size")
    )

    filters, applied_filter_by_name = _nucc_filters(args, table)

    count_stmt = select(func.count()).select_from(table)
    if filters:
        count_stmt = count_stmt.where(*filters)
    total_result = await session.execute(count_stmt)
    total = int(total_result.scalar() or 0)

    stmt = _ordered_nucc_query(table, filters, order)
    pagination = None
    if include_meta or has_pagination_args:
        pagination = parse_pagination(
            args,
            default_limit=50,
            max_limit=200,
            default_page=1,
            allow_offset=True,
            allow_start=True,
            allow_page_size=True,
        )
        stmt = stmt.offset(pagination.offset).limit(pagination.limit)

    taxonomy_result = await session.execute(stmt)
    taxonomy_rows = taxonomy_result.scalars().all()
    taxonomy_items = [
        taxonomy_row.to_json_dict() for taxonomy_row in taxonomy_rows
    ]

    if include_meta and pagination is not None:
        return response.json(
            {
                "total": total,
                "page": pagination.page,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "items": taxonomy_items,
                "applied_filters": {
                    "limit": pagination.limit,
                    "page": pagination.page,
                    "offset": pagination.offset,
                    **applied_filter_by_name,
                },
            }
        )

    return response.json(taxonomy_items)
