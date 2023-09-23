from datetime import datetime

from asyncpg.exceptions import PostgresError
from asyncpg.exceptions import InterfaceError
from sanic import response
from sanic import Blueprint

from db.models import Issuer

blueprint = Blueprint('geo', url_prefix='/geo', version=1)


@blueprint.get('/get')
async def get_geo(request):
    zip_code = float(request.args.get("zip_code"))
    in_lat = float(request.args.get("lat"))
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'database': await _check_db()
    }

    return response.json(data)