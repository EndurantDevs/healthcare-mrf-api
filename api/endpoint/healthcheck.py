from datetime import datetime

from asyncpg.exceptions import PostgresError
from asyncpg.exceptions import InterfaceError
from sanic import response
from sanic import Blueprint

from db.models import Issuer

blueprint = Blueprint('healthcheck', url_prefix='/healthcheck', version=1)


@blueprint.get('/')
async def healthcheck(request):
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'database': await _check_db()
    }

    return response.json(data)


async def _check_db():
    try:
        await Issuer.load(Issuer.issuer_id).limit(1).gino.first()
        return {
            'status': 'OK'
        }
    except (PostgresError, InterfaceError, ConnectionRefusedError) as ex:
        return {
            'status': 'Fail',
            'details': str(ex)
        }
