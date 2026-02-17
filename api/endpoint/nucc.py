# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from sanic import Blueprint, response
from sqlalchemy import func, or_, select

from api.endpoint.pagination import parse_bool_alias, parse_pagination
from db.models import NUCCTaxonomy

blueprint = Blueprint('nucc', url_prefix='/nucc', version=1)


@blueprint.get('/')
async def index_status_nucc(_request):
    return response.json({})


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


@blueprint.get('/all')
async def all_of_nucc(request):
    session = _get_session(request)
    args = request.args
    table = NUCCTaxonomy.__table__

    q = str(args.get("q") or "").strip()
    code = str(args.get("code") or "").strip()
    order = str(args.get("order") or "asc").strip().lower()
    if order not in {"asc", "desc"}:
        order = "asc"

    include_meta = parse_bool_alias(args, "include_meta", "paginate", default=False)
    has_pagination_args = any(
        args.get(name) not in (None, "", "null")
        for name in ("limit", "offset", "page", "start", "page_size")
    )

    filters = []
    applied_filters = {}
    if code:
        filters.append(table.c.code.ilike(f"%{code}%"))
        applied_filters["code"] = code
    if q:
        applied_filters["q"] = q
        filters.append(
            or_(
                table.c.code.ilike(f"%{q}%"),
                table.c.display_name.ilike(f"%{q}%"),
                table.c.classification.ilike(f"%{q}%"),
                table.c.specialization.ilike(f"%{q}%"),
                table.c.grouping.ilike(f"%{q}%"),
                table.c.section.ilike(f"%{q}%"),
            )
        )

    count_stmt = select(func.count()).select_from(table)
    if filters:
        count_stmt = count_stmt.where(*filters)
    total_result = await session.execute(count_stmt)
    total = int(total_result.scalar() or 0)

    stmt = select(NUCCTaxonomy)
    if filters:
        stmt = stmt.where(*filters)
    if order == "desc":
        stmt = stmt.order_by(table.c.display_name.desc(), table.c.code.desc())
    else:
        stmt = stmt.order_by(table.c.display_name.asc(), table.c.code.asc())
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

    result = await session.execute(stmt)
    rows = result.scalars().all()
    items = [row.to_json_dict() for row in rows]

    if include_meta and pagination is not None:
        return response.json(
            {
                "total": total,
                "page": pagination.page,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "items": items,
                "applied_filters": {
                    "limit": pagination.limit,
                    "page": pagination.page,
                    "offset": pagination.offset,
                    **applied_filters,
                },
            }
        )

    return response.json(items)
