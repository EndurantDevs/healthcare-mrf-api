# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from datetime import datetime

from asyncpg import UndefinedTableError
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from db.models import GeoZipLookup
from db.tiger_models import Zip_zcta5, ZipState


class TigerUnavailableError(RuntimeError):
    """Raised when the TIGER schema is unavailable."""


geo_zip_table = GeoZipLookup.__table__
zip_state_table = ZipState.__table__
zip_zcta5_table = Zip_zcta5.__table__
blueprint = Blueprint('geo', url_prefix='/geo', version=1)


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _row_mapping(row):
    if row is None:
        return None
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:  # pragma: no cover - defensive
        return None


def _serialize_geo_row(row_mapping):
    if row_mapping is None:
        return None
    return {
        "zip_code": row_mapping.get("zip_code"),
        "lat": row_mapping.get("latitude"),
        "long": row_mapping.get("longitude"),
        "state": row_mapping.get("state"),
        "city": row_mapping.get("city"),
        "state_name": row_mapping.get("state_name"),
        "county_name": row_mapping.get("county_name"),
        "timezone": row_mapping.get("timezone"),
    }


async def _lookup_zip_from_tiger(session, zip_code):
    stmt = (
        select(
            zip_zcta5_table.c.zcta5ce,
            zip_zcta5_table.c.intptlat,
            zip_zcta5_table.c.intptlon,
            zip_state_table.c.stusps,
        )
        .select_from(
            zip_zcta5_table.join(
                zip_state_table,
                zip_zcta5_table.c.statefp == zip_state_table.c.statefp,
            )
        )
        .where(zip_zcta5_table.c.zcta5ce == zip_code)
    )

    try:
        result = await session.execute(stmt)
    except ProgrammingError as exc:
        if isinstance(getattr(exc, "orig", None), UndefinedTableError):
            raise TigerUnavailableError() from exc
        raise

    data = result.first()
    if not data:
        return None

    try:
        return {
            'zip_code': data[0],
            'lat': float(data[1]),
            'long': float(data[2]),
            'state': data[3],
        }
    except (IndexError, ValueError, TypeError):
        return None


@blueprint.get('/get')
async def get_geo_status(request):
    request.args.get("zip_code")
    request.args.get("lat")
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        # 'database': await _check_db()
    }

    return response.json(data)


@blueprint.get('/zip/<zip_code>', name='get_geo_by_zip')
async def get_geo(request, zip_code):
    zip_code = zip_code.strip().rjust(5, '0')
    session = _get_session(request)

    local_stmt = (
        select(
            geo_zip_table.c.zip_code.label("zip_code"),
            geo_zip_table.c.city.label("city"),
            geo_zip_table.c.state.label("state"),
            geo_zip_table.c.state_name.label("state_name"),
            geo_zip_table.c.county_name.label("county_name"),
            geo_zip_table.c.latitude.label("latitude"),
            geo_zip_table.c.longitude.label("longitude"),
            geo_zip_table.c.timezone.label("timezone"),
        )
        .where(geo_zip_table.c.zip_code == zip_code)
    )
    local_result = await session.execute(local_stmt)
    local_row = _row_mapping(local_result.first())
    if local_row:
        payload = _serialize_geo_row(local_row)
        if payload.get("lat") is None:
            payload["lat"] = local_row.get("latitude")
        if payload.get("long") is None:
            payload["long"] = local_row.get("longitude")
        return response.json(payload)

    try:
        payload = await _lookup_zip_from_tiger(session, zip_code)
    except TigerUnavailableError:
        return response.json({"error": "tiger schema not available"}, status=503)
    if payload is None:
        return response.json({'error': 'Not found'}, status=404)
    return response.json(payload)


@blueprint.get('/city', name='get_geo_by_city')
async def get_geo_by_city(request):
    city = request.args.get("city")
    if not city:
        raise InvalidUsage("city query parameter is required")
    state = (request.args.get("state") or "").strip().upper()
    session = _get_session(request)

    stmt = (
        select(
            geo_zip_table.c.zip_code.label("zip_code"),
            geo_zip_table.c.city.label("city"),
            geo_zip_table.c.state.label("state"),
            geo_zip_table.c.state_name.label("state_name"),
            geo_zip_table.c.county_name.label("county_name"),
            geo_zip_table.c.latitude.label("latitude"),
            geo_zip_table.c.longitude.label("longitude"),
            geo_zip_table.c.timezone.label("timezone"),
        )
        .where(geo_zip_table.c.city_lower == city.strip().lower())
        .order_by(geo_zip_table.c.state.asc(), geo_zip_table.c.zip_code.asc())
        .limit(500)
    )
    if state:
        stmt = stmt.where(geo_zip_table.c.state == state)

    result = await session.execute(stmt)
    rows = [_row_mapping(row) for row in result.all() if _row_mapping(row)]
    if not rows:
        return response.json({"items": []}, status=404)

    items = []
    for row in rows:
        items.append(
            {
                "zip_code": row.get("zip_code"),
                "state": row.get("state"),
                "state_name": row.get("state_name"),
                "city": row.get("city"),
                "county_name": row.get("county_name"),
                "lat": row.get("latitude"),
                "long": row.get("longitude"),
                "timezone": row.get("timezone"),
            }
        )

    return response.json(
        {
            "normalized_city": rows[0].get("city"),
            "state": state or None,
            "items": items,
        }
    )


@blueprint.get('/states', name='list_geo_states')
async def list_geo_states(request):
    session = _get_session(request)
    args = request.args

    sort = (args.get("sort") or "population").lower()
    if sort not in {"population", "zip_count", "state"}:
        raise InvalidUsage("sort must be one of: population, zip_count, state")
    order = (args.get("order") or "desc").lower()
    if order not in {"asc", "desc"}:
        raise InvalidUsage("order must be 'asc' or 'desc'")

    limit_param = args.get("limit")
    try:
        limit = max(1, min(int(limit_param), 100)) if limit_param else None
    except (TypeError, ValueError) as exc:
        raise InvalidUsage("limit must be an integer") from exc

    top_zip_param = args.get("top_zip_limit")
    try:
        top_zip_limit = max(1, min(int(top_zip_param), 10)) if top_zip_param else 5
    except (TypeError, ValueError) as exc:
        raise InvalidUsage("top_zip_limit must be an integer") from exc

    state_col = geo_zip_table.c.state.label("state")
    state_name_col = func.max(geo_zip_table.c.state_name).label("state_name")
    zip_count_col = func.count(func.distinct(geo_zip_table.c.zip_code)).label("zip_count")
    city_count_col = func.count(func.distinct(geo_zip_table.c.city_lower)).label("city_count")
    population_col = func.coalesce(func.sum(func.coalesce(geo_zip_table.c.population, 0)), 0).label("population")
    avg_lat_col = func.avg(geo_zip_table.c.latitude).label("avg_lat")
    avg_long_col = func.avg(geo_zip_table.c.longitude).label("avg_long")

    state_stmt = (
        select(
            state_col,
            state_name_col,
            zip_count_col,
            city_count_col,
            population_col,
            avg_lat_col,
            avg_long_col,
        )
        .group_by(geo_zip_table.c.state)
    )

    order_column_map = {
        "population": population_col,
        "zip_count": zip_count_col,
        "state": state_col,
    }
    order_column = order_column_map[sort]
    if order == "desc":
        state_stmt = state_stmt.order_by(order_column.desc())
    else:
        state_stmt = state_stmt.order_by(order_column.asc())

    if limit is not None:
        state_stmt = state_stmt.limit(limit)

    population_expr = func.coalesce(geo_zip_table.c.population, 0)
    rank_expr = func.row_number().over(
        partition_by=geo_zip_table.c.state,
        order_by=population_expr.desc(),
    )
    top_zip_subquery = (
        select(
            geo_zip_table.c.state.label("state"),
            geo_zip_table.c.zip_code.label("zip_code"),
            geo_zip_table.c.city.label("city"),
            population_expr.label("population"),
            geo_zip_table.c.latitude.label("lat"),
            geo_zip_table.c.longitude.label("long"),
            rank_expr.label("zip_rank"),
        )
    ).subquery()

    top_zip_stmt = (
        select(
            top_zip_subquery.c.state,
            top_zip_subquery.c.zip_code,
            top_zip_subquery.c.city,
            top_zip_subquery.c.population,
            top_zip_subquery.c.lat,
            top_zip_subquery.c.long,
        )
        .where(top_zip_subquery.c.zip_rank <= top_zip_limit)
        .order_by(top_zip_subquery.c.state.asc(), top_zip_subquery.c.population.desc())
    )

    state_rows = (await session.execute(state_stmt)).all()
    top_zip_rows = (await session.execute(top_zip_stmt)).all()

    zip_map = {}
    for row in top_zip_rows:
        mapping = _row_mapping(row)
        state_key = mapping.get("state")
        if not state_key:
            continue
        zip_map.setdefault(state_key, []).append(
            {
                "zip_code": mapping.get("zip_code"),
                "city": mapping.get("city"),
                "population": int(mapping.get("population") or 0),
                "lat": mapping.get("lat"),
                "long": mapping.get("long"),
            }
        )

    states_payload = []
    for row in state_rows:
        mapping = _row_mapping(row)
        state_key = mapping.get("state")
        states_payload.append(
            {
                "state": state_key,
                "state_name": mapping.get("state_name"),
                "zip_count": int(mapping.get("zip_count") or 0),
                "city_count": int(mapping.get("city_count") or 0),
                "population": int(mapping.get("population") or 0),
                "avg_lat": mapping.get("avg_lat"),
                "avg_long": mapping.get("avg_long"),
                "top_zips": zip_map.get(state_key, []),
            }
        )

    return response.json(
        {
            "generated": datetime.utcnow().isoformat(),
            "states": states_payload,
        }
    )


@blueprint.get('/state/<state>/cities', name='get_top_cities_by_state')
async def get_top_cities_by_state(request, state):
    state = state.strip().upper()
    if len(state) != 2:
        raise InvalidUsage("state must be 2-letter abbreviation")
    limit_param = request.args.get("limit")
    try:
        limit = max(1, min(int(limit_param), 200)) if limit_param else 20
    except (TypeError, ValueError) as exc:
        raise InvalidUsage("limit must be an integer") from exc

    session = _get_session(request)
    stmt = (
        select(
            geo_zip_table.c.city.label("city"),
            geo_zip_table.c.state.label("state"),
            func.count(geo_zip_table.c.zip_code).label("zip_count"),
            func.sum(func.coalesce(geo_zip_table.c.population, 0)).label("population"),
            func.avg(geo_zip_table.c.latitude).label("avg_lat"),
            func.avg(geo_zip_table.c.longitude).label("avg_long"),
        )
        .where(geo_zip_table.c.state == state)
        .group_by(geo_zip_table.c.city, geo_zip_table.c.state)
        .order_by(
            func.sum(func.coalesce(geo_zip_table.c.population, 0)).desc(),
            geo_zip_table.c.city.asc(),
        )
        .limit(limit)
    )

    result = await session.execute(stmt)
    rows = [_row_mapping(row) for row in result.all() if _row_mapping(row)]
    if not rows:
        return response.json({"error": "Not found"}, status=404)

    items = []
    for row in rows:
        items.append(
            {
                "city": row.get("city"),
                "state": row.get("state"),
                "zip_count": int(row.get("zip_count") or 0),
                "population": int(row.get("population") or 0),
                "avg_lat": row.get("avg_lat"),
                "avg_long": row.get("avg_long"),
            }
        )

    return response.json({"state": state, "limit": limit, "items": items})
