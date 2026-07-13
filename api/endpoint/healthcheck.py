# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from datetime import datetime

from sanic import Blueprint, response
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from db.models import Issuer, PTG2V3SnapshotLayout

blueprint = Blueprint('healthcheck', url_prefix='/healthcheck', version=1)


@blueprint.get('/')
async def healthcheck(request):
    """Report release metadata and database connectivity."""

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


@blueprint.get('/live')
async def liveness(request):
    """Confirm that the API process can answer requests."""

    return response.json({
        'status': 'OK',
        'release': request.app.config.get('RELEASE'),
    })


@blueprint.get('/ready')
async def readiness(request):
    """Require database access and the strict V3 schema before serving traffic."""

    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        return response.json(
            {'status': 'Fail', 'details': 'SQLAlchemy session unavailable'},
            status=503,
        )

    database = await _check_db(session)
    v3_schema = (
        await _check_v3_schema(session)
        if database['status'] == 'OK'
        else {'status': 'Fail', 'details': 'database unavailable'}
    )
    ready = database['status'] == 'OK' and v3_schema['status'] == 'OK'
    return response.json(
        {
            'status': 'OK' if ready else 'Fail',
            'database': database,
            'ptg_v3_schema': v3_schema,
            'release': request.app.config.get('RELEASE'),
        },
        status=200 if ready else 503,
    )


async def _check_db(session):
    try:
        await session.execute(select(Issuer.__table__.c.issuer_id).limit(1))
        return {
            'status': 'OK'
        }
    except (SQLAlchemyError, ConnectionRefusedError) as ex:
        return {
            'status': 'Fail',
            'details': str(ex)
        }


async def _check_v3_schema(session):
    try:
        await session.execute(
            select(PTG2V3SnapshotLayout.__table__.c.snapshot_key).limit(1)
        )
        return {'status': 'OK'}
    except (SQLAlchemyError, ConnectionRefusedError) as ex:
        return {
            'status': 'Fail',
            'details': str(ex),
        }
