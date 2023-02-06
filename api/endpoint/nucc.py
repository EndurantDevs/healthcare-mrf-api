import asyncio
from datetime import datetime
from api.for_human import attributes_labels
import urllib.parse
from sqlalchemy.sql import func

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, NUCCTaxonomy

blueprint = Blueprint('nucc', url_prefix='/nucc', version=1)


@blueprint.get('/')
async def index_status_nucc(request):

    return response.json({})

@blueprint.get('/all')
async def all_of_nucc(request):
    # plan_data = await NUCCTaxonomy.query.gino.all()
    data = []
    async with db.transaction():
        async for p in NUCCTaxonomy.query.gino.iterate():
            data.append(p.to_json_dict())
    return response.json(data)
