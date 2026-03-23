# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from datetime import datetime

from asyncpg import UndefinedColumnError, UndefinedTableError
from sanic import Blueprint, response
from sanic.exceptions import InvalidUsage
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from api.endpoint.pagination import parse_pagination
from db.models import (
    GeoZipCensusProfile,
    GeoZipLookup,
    NPIAddress,
    PricingPlacesZcta,
    PricingSviZcta,
)
from db.tiger_models import Zip_zcta5, ZipState


class TigerUnavailableError(RuntimeError):
    """Raised when the TIGER schema is unavailable."""


geo_zip_table = GeoZipLookup.__table__
geo_census_table = GeoZipCensusProfile.__table__
npi_address_table = NPIAddress.__table__
pricing_places_zcta_table = PricingPlacesZcta.__table__
svi_zcta_table = PricingSviZcta.__table__
zip_state_table = ZipState.__table__
zip_zcta5_table = Zip_zcta5.__table__
blueprint = Blueprint('geo', url_prefix='/geo', version=1)

CENSUS_PROFILE_FIELDS = (
    "total_population",
    "median_household_income",
    "bachelors_degree_or_higher_pct",
    "employment_rate_pct",
    "total_housing_units",
    "without_health_insurance_pct",
    "total_employer_establishments",
    "business_employment",
    "business_payroll_annual_k",
    "total_households",
    "hispanic_or_latino",
    "hispanic_or_latino_pct",
    "poverty_rate_pct",
    "median_age",
    "unemployment_rate_pct",
    "labor_force_participation_pct",
    "vacancy_rate_pct",
    "median_home_value",
    "median_gross_rent",
    "commute_mean_minutes",
    "commute_mode_drove_alone_pct",
    "commute_mode_carpool_pct",
    "commute_mode_public_transit_pct",
    "commute_mode_walked_pct",
    "commute_mode_worked_from_home_pct",
    "broadband_access_pct",
    "race_white_alone",
    "race_black_or_african_american_alone",
    "race_american_indian_and_alaska_native_alone",
    "race_asian_alone",
    "race_native_hawaiian_and_other_pacific_islander_alone",
    "race_some_other_race_alone",
    "race_two_or_more_races",
    "race_white_alone_pct",
    "race_black_or_african_american_alone_pct",
    "race_american_indian_and_alaska_native_alone_pct",
    "race_asian_alone_pct",
    "race_native_hawaiian_and_other_pacific_islander_alone_pct",
    "race_some_other_race_alone_pct",
    "race_two_or_more_races_pct",
    "acs_white_alone_pct",
    "acs_black_or_african_american_alone_pct",
    "acs_american_indian_and_alaska_native_alone_pct",
    "acs_asian_alone_pct",
    "acs_native_hawaiian_and_other_pacific_islander_alone_pct",
    "acs_some_other_race_alone_pct",
    "acs_two_or_more_races_pct",
    "acs_hispanic_or_latino_pct",
    "svi_overall",
    "svi_socioeconomic",
    "svi_household",
    "svi_minority",
    "svi_housing",
    "provider_count",
    "provider_density_per_1000",
)


def _get_session(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    return session


def _is_optional_schema_error(exc):
    return isinstance(getattr(exc, "orig", None), (UndefinedTableError, UndefinedColumnError))


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


def _serialize_census_row(row_mapping):
    if row_mapping is None:
        return None
    return {field: row_mapping.get(field) for field in CENSUS_PROFILE_FIELDS}


def _serialize_places_row(row_mapping):
    if row_mapping is None:
        return None
    updated_at = row_mapping.get("updated_at")
    if isinstance(updated_at, datetime):
        updated_at = updated_at.isoformat()
    return {
        "measure_id": row_mapping.get("measure_id"),
        "measure_name": row_mapping.get("measure_name"),
        "data_value": row_mapping.get("data_value"),
        "low_ci": row_mapping.get("low_ci"),
        "high_ci": row_mapping.get("high_ci"),
        "data_value_type": row_mapping.get("data_value_type"),
        "source": row_mapping.get("source"),
        "updated_at": updated_at,
    }


def _density_per_1000(count_value, population):
    if count_value is None:
        return None
    if population in (None, 0):
        return None
    try:
        return (float(count_value) / float(population)) * 1000.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _log_geo_warning(request, message, *args):
    logger = getattr(getattr(request, "app", None), "logger", None)
    if logger is not None and hasattr(logger, "warning"):
        logger.warning(message, *args)


async def _rollback_session(session):
    try:
        await session.rollback()
    except Exception:  # pragma: no cover - defensive
        pass


async def _lookup_svi_profile(session, zip_code):
    stmt = (
        select(
            svi_zcta_table.c.svi_overall,
            svi_zcta_table.c.svi_socioeconomic,
            svi_zcta_table.c.svi_household,
            svi_zcta_table.c.svi_minority,
            svi_zcta_table.c.svi_housing,
        )
        .where(svi_zcta_table.c.zcta == zip_code)
        .order_by(svi_zcta_table.c.year.desc())
        .limit(1)
    )
    try:
        result = await session.execute(stmt)
    except ProgrammingError as exc:
        if _is_optional_schema_error(exc):
            return None
        raise
    row = _row_mapping(result.first())
    if row is None:
        return None
    return {
        "svi_overall": row.get("svi_overall"),
        "svi_socioeconomic": row.get("svi_socioeconomic"),
        "svi_household": row.get("svi_household"),
        "svi_minority": row.get("svi_minority"),
        "svi_housing": row.get("svi_housing"),
    }


async def _lookup_provider_count(session, zip_code):
    stmt = (
        select(func.count(func.distinct(npi_address_table.c.npi)))
        .where(
            npi_address_table.c.type == "primary",
            func.left(npi_address_table.c.postal_code, 5) == zip_code,
        )
    )
    try:
        result = await session.execute(stmt)
    except ProgrammingError as exc:
        if _is_optional_schema_error(exc):
            return None
        raise
    return result.scalar()


async def _lookup_census_profile(session, zip_code):
    stmt = (
        select(
            geo_census_table.c.total_population,
            geo_census_table.c.median_household_income,
            geo_census_table.c.bachelors_degree_or_higher_pct,
            geo_census_table.c.employment_rate_pct,
            geo_census_table.c.total_housing_units,
            geo_census_table.c.without_health_insurance_pct,
            geo_census_table.c.total_employer_establishments,
            geo_census_table.c.business_employment,
            geo_census_table.c.business_payroll_annual_k,
            geo_census_table.c.total_households,
            geo_census_table.c.hispanic_or_latino,
            geo_census_table.c.hispanic_or_latino_pct,
            geo_census_table.c.poverty_rate_pct,
            geo_census_table.c.median_age,
            geo_census_table.c.unemployment_rate_pct,
            geo_census_table.c.labor_force_participation_pct,
            geo_census_table.c.vacancy_rate_pct,
            geo_census_table.c.median_home_value,
            geo_census_table.c.median_gross_rent,
            geo_census_table.c.commute_mean_minutes,
            geo_census_table.c.commute_mode_drove_alone_pct,
            geo_census_table.c.commute_mode_carpool_pct,
            geo_census_table.c.commute_mode_public_transit_pct,
            geo_census_table.c.commute_mode_walked_pct,
            geo_census_table.c.commute_mode_worked_from_home_pct,
            geo_census_table.c.broadband_access_pct,
            geo_census_table.c.race_white_alone,
            geo_census_table.c.race_black_or_african_american_alone,
            geo_census_table.c.race_american_indian_and_alaska_native_alone,
            geo_census_table.c.race_asian_alone,
            geo_census_table.c.race_native_hawaiian_and_other_pacific_islander_alone,
            geo_census_table.c.race_some_other_race_alone,
            geo_census_table.c.race_two_or_more_races,
            geo_census_table.c.race_white_alone_pct,
            geo_census_table.c.race_black_or_african_american_alone_pct,
            geo_census_table.c.race_american_indian_and_alaska_native_alone_pct,
            geo_census_table.c.race_asian_alone_pct,
            geo_census_table.c.race_native_hawaiian_and_other_pacific_islander_alone_pct,
            geo_census_table.c.race_some_other_race_alone_pct,
            geo_census_table.c.race_two_or_more_races_pct,
            geo_census_table.c.acs_white_alone_pct,
            geo_census_table.c.acs_black_or_african_american_alone_pct,
            geo_census_table.c.acs_american_indian_and_alaska_native_alone_pct,
            geo_census_table.c.acs_asian_alone_pct,
            geo_census_table.c.acs_native_hawaiian_and_other_pacific_islander_alone_pct,
            geo_census_table.c.acs_some_other_race_alone_pct,
            geo_census_table.c.acs_two_or_more_races_pct,
            geo_census_table.c.acs_hispanic_or_latino_pct,
        )
        .where(geo_census_table.c.zip_code == zip_code)
    )
    try:
        result = await session.execute(stmt)
    except ProgrammingError as exc:
        if _is_optional_schema_error(exc):
            return None
        raise
    profile = _serialize_census_row(_row_mapping(result.first()))
    if profile is None:
        return None

    svi_profile = await _lookup_svi_profile(session, zip_code)
    if svi_profile:
        profile.update(svi_profile)

    total_population = profile.get("total_population")
    provider_count = await _lookup_provider_count(session, zip_code)
    profile.update(
        {
            "provider_count": provider_count,
            "provider_density_per_1000": _density_per_1000(provider_count, total_population),
        }
    )
    return profile


async def _resolve_places_year(session, zip_code, requested_year):
    if requested_year is not None:
        return requested_year

    stmt = (
        select(func.max(pricing_places_zcta_table.c.year))
        .where(pricing_places_zcta_table.c.zcta == zip_code)
    )
    try:
        result = await session.execute(stmt)
    except ProgrammingError as exc:
        if _is_optional_schema_error(exc):
            return None
        raise
    return result.scalar()


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
        if _is_optional_schema_error(exc):
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
    try:
        census_profile = await _lookup_census_profile(session, zip_code)
    except Exception as exc:  # pragma: no cover - defensive production guardrail
        _log_geo_warning(request, "geo census enrichment failed for zip %s: %s", zip_code, exc)
        await _rollback_session(session)
        census_profile = None

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
    try:
        local_result = await session.execute(local_stmt)
        local_row = _row_mapping(local_result.first())
    except ProgrammingError as exc:
        if not _is_optional_schema_error(exc):
            raise
        local_row = None
    if local_row:
        payload = _serialize_geo_row(local_row)
        if payload.get("lat") is None:
            payload["lat"] = local_row.get("latitude")
        if payload.get("long") is None:
            payload["long"] = local_row.get("longitude")
        payload["census_profile"] = census_profile
        return response.json(payload)

    try:
        payload = await _lookup_zip_from_tiger(session, zip_code)
    except TigerUnavailableError:
        return response.json({"error": "tiger schema not available"}, status=503)
    if payload is None:
        return response.json({'error': 'Not found'}, status=404)
    payload["census_profile"] = census_profile
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
            geo_zip_table.c.population.label("population"),
        )
        .where(geo_zip_table.c.city_lower == city.strip().lower())
        .order_by(
            geo_zip_table.c.state.asc(),
            geo_zip_table.c.population.desc().nullslast(),
            geo_zip_table.c.zip_code.asc(),
        )
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
                "population": row.get("population"),
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


@blueprint.get('/zip/<zip_code>/places', name='get_places_by_zip')
async def get_places_by_zip(request, zip_code):
    zip_code = zip_code.strip().rjust(5, '0')
    args = request.args
    year = args.get("year")
    measure_id = (args.get("measure_id") or "").strip() or None
    session = _get_session(request)

    if year is not None:
        try:
            year = int(year)
        except (TypeError, ValueError) as exc:
            raise InvalidUsage("year must be an integer") from exc

    target_year = await _resolve_places_year(session, zip_code, year)
    if target_year is None:
        return response.json({"error": "Not found"}, status=404)

    stmt = (
        select(
            pricing_places_zcta_table.c.measure_id,
            pricing_places_zcta_table.c.measure_name,
            pricing_places_zcta_table.c.data_value,
            pricing_places_zcta_table.c.low_ci,
            pricing_places_zcta_table.c.high_ci,
            pricing_places_zcta_table.c.data_value_type,
            pricing_places_zcta_table.c.source,
            pricing_places_zcta_table.c.updated_at,
        )
        .where(
            pricing_places_zcta_table.c.zcta == zip_code,
            pricing_places_zcta_table.c.year == target_year,
        )
        .order_by(pricing_places_zcta_table.c.measure_id.asc())
    )
    if measure_id:
        stmt = stmt.where(pricing_places_zcta_table.c.measure_id == measure_id)

    try:
        result = await session.execute(stmt)
    except ProgrammingError as exc:
        if isinstance(getattr(exc, "orig", None), UndefinedTableError):
            return response.json({"error": "Not found"}, status=404)
        raise

    metrics = []
    for row in result.all():
        payload_row = _serialize_places_row(_row_mapping(row))
        if payload_row:
            metrics.append(payload_row)
    if not metrics:
        return response.json({"error": "Not found"}, status=404)

    return response.json(
        {
            "zip_code": zip_code,
            "zcta": zip_code,
            "year": int(target_year),
            "measures": metrics,
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
    # Explicit access keeps route/query introspection in sync with OpenAPI.
    args.get("limit")

    pagination = parse_pagination(
        args,
        default_limit=50,
        max_limit=100,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )
    limit = pagination.limit
    offset = pagination.offset

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

    total_states_stmt = select(func.count()).select_from(
        select(geo_zip_table.c.state).group_by(geo_zip_table.c.state).subquery()
    )
    total_states_result = await session.execute(total_states_stmt)
    total_states = int(total_states_result.scalar() or 0)

    state_stmt = state_stmt.offset(offset).limit(limit)

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
            "total_states": total_states,
            "page": pagination.page,
            "limit": limit,
            "offset": offset,
            "states": states_payload,
        }
    )


@blueprint.get('/state/<state>/cities', name='get_top_cities_by_state')
async def get_top_cities_by_state(request, state):
    state = state.strip().upper()
    if len(state) != 2:
        raise InvalidUsage("state must be 2-letter abbreviation")
    # Explicit access keeps route/query introspection in sync with OpenAPI.
    request.args.get("limit")
    pagination = parse_pagination(
        request.args,
        default_limit=20,
        max_limit=200,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )
    limit = pagination.limit
    offset = pagination.offset

    session = _get_session(request)
    total_stmt = (
        select(func.count())
        .select_from(
            select(geo_zip_table.c.city)
            .where(geo_zip_table.c.state == state)
            .group_by(geo_zip_table.c.city, geo_zip_table.c.state)
            .subquery()
        )
    )
    total_result = await session.execute(total_stmt)
    total = int(total_result.scalar() or 0)

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
        .offset(offset)
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

    return response.json(
        {
            "state": state,
            "total": total,
            "page": pagination.page,
            "limit": limit,
            "offset": offset,
            "items": items,
        }
    )
