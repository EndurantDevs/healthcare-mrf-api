from datetime import datetime

from sanic import response
from sanic import Blueprint
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from db.tables import issuer_table

blueprint = Blueprint('healthcheck', url_prefix='/healthcheck', version=1)


@blueprint.get('/')
async def healthcheck(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'database': await _check_db(session)
    }

    return response.json(data)


async def _check_db(session):
    try:
        await session.execute(select(issuer_table.c.issuer_id).limit(1))
        return {
            'status': 'OK'
        }
    except (SQLAlchemyError, ConnectionRefusedError) as ex:
        return {
            'status': 'Fail',
            'details': str(ex)
        }
