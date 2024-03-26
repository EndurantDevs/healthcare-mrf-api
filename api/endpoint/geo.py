from datetime import datetime

from asyncpg.exceptions import PostgresError
from asyncpg.exceptions import InterfaceError
from sanic import response
from sanic import Blueprint

from db.models import Issuer
from db.tiger_models import db, ZipState, State, Zip_zcta5
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
    data = (await db.select(
        [
            Zip_zcta5.zcta5ce,
            Zip_zcta5.intptlat,
            Zip_zcta5.intptlon,
            ZipState.stusps,
        ]
    ).select_from(
        Zip_zcta5.join(ZipState, Zip_zcta5.statefp == ZipState.statefp)
    ).where(
        Zip_zcta5.zcta5ce == zip_code
    ).gino.first())

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