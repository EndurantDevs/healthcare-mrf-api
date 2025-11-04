import asyncio
from datetime import datetime
from api.for_human import attributes_labels
import urllib.parse
from sqlalchemy.sql import func

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from sqlalchemy import select

from db.models import db, NUCCTaxonomy

blueprint = Blueprint('nucc', url_prefix='/nucc', version=1)


@blueprint.get('/')
async def index_status_nucc(request):

    return response.json({})

@blueprint.get('/all')
async def all_of_nucc(request):
    session = getattr(request.ctx, "sa_session", None)
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")

    stmt = select(NUCCTaxonomy)
    result = await session.execute(stmt)
    rows = result.scalars().all()
    return response.json([row.to_json_dict() for row in rows])
