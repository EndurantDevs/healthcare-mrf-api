from datetime import datetime

from sanic import response
from sanic import Blueprint
from sqlalchemy import select
from sqlalchemy.exc import ProgrammingError

from asyncpg import UndefinedTableError

from db.tiger_models import ZipState, Zip_zcta5

zip_state_table = ZipState.__table__
zip_zcta5_table = Zip_zcta5.__table__
blueprint = Blueprint('geo', url_prefix='/geo', version=1)


@blueprint.get('/get')
async def get_geo(request):
    zip_code = float(request.args.get("zip_code"))
    in_lat = float(request.args.get("lat"))
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
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")

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
            return response.json({"error": "tiger schema not available"}, status=503)
        raise
    data = result.first()

    if not data:
        return response.json({'error': 'Not found'}, status=404)

    try:
        data = {
            'zip_code': data[0],
            'lat': float(data[1]),
            'long': float(data[2]),
            'state': data[3],
        }
    except (IndexError, ValueError):
        return response.json({'error': 'Not found'}, status=404)

    return response.json(data)
