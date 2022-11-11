import asyncio
from datetime import datetime
import urllib.parse

import sanic.exceptions
from sanic import response
from sanic import Blueprint
from sqlalchemy import literal

from db.models import db, Issuer, Plan, PlanFormulary, PlanTransparency, ImportLog, ImportHistory

blueprint = Blueprint('import', url_prefix='/import', version=1)


@blueprint.get('/')
async def last_import_stats(request):
    states_data = {}
    states_data['issuer_number'] = {}
    states_data['plan_number'] = {}

    async with db.transaction():
        states_data['issuer_number'] = dict(await db.select([Plan.state, db.func.count(db.func.distinct(Plan.issuer_id))]).group_by(Plan.state).gino.all())
        states_data['plan_number'] = dict(await db.select([Plan.state, db.func.count(db.func.distinct(Plan.plan_id))]).group_by(Plan.state).gino.all())

        data = {
            'plans_count': await db.func.count(Plan.plan_id).gino.scalar(),
            'import_log_errors': await db.func.count(ImportLog.checksum).gino.scalar(),
            'import_date': await ImportHistory.select('when').order_by(ImportHistory.when.desc()).limit(1).gino.scalar(),
            'issuers_number': await db.func.count(Issuer.issuer_id).gino.scalar(),
            'issuers_by_state': states_data['issuer_number'],
            'plans_by_state': states_data['plan_number'],
        }

    return response.json(data, default=str)


@blueprint.get('/issuer/<issuer_id>')
async def issuer_import_data(request, issuer_id):
    data = await Issuer.query.where(Issuer.issuer_id == int(issuer_id)).gino.first()
    if not data:
        raise sanic.exceptions.NotFound
    data = data.to_json_dict()

    err_log = await ImportLog.query.where(ImportLog.issuer_id == int(issuer_id)).gino.all()
    data['err_log'] = []
    data['import_errors_count'] = len(err_log)

    for e in err_log:
        data['err_log'].append(e.to_json_dict())
    return response.json(data)