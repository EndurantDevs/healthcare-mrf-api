import asyncio
from datetime import datetime
import urllib.parse

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, Plan, PlanFormulary, Issuer, ImportLog

blueprint = Blueprint('plan', url_prefix='/plan', version=1)


@blueprint.get('/')
async def index_status(request):
    async def get_plan_count():
        async with db.acquire():
            return await db.func.count(Plan.plan_id).gino.scalar()

    async def get_import_error_count():
        async with db.acquire():
            return await db.func.count(ImportLog.checksum).gino.scalar()


    plan_count, import_error_count = await asyncio.gather(get_plan_count(), get_import_error_count())
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'product_count': plan_count,
        'import_log_errors': import_error_count,
    }

    return response.json(data)

@blueprint.get('/all')
async def all_plans(request):
    plan_data = await Plan.query.gino.all()
    data = []
    for p in plan_data:
        data.append(p.to_json_dict())
    return response.json(data)

@blueprint.get('/id/<plan_id>')
@blueprint.get('/id/<plan_id>/<year>')
async def get_plan(request, plan_id, year=None):
    if year:
        data = await Plan.query.where(Plan.plan_id == plan_id).where(Plan.year == int(year)).gino.first()
    else:
        data = await Plan.query.where(Plan.plan_id == plan_id).order_by(Plan.year.desc()).gino.first()
    data = data.to_json_dict()
    if not data:
        raise sanic.exceptions.NotFound
    data['issuer_name'] = await Issuer.select('issuer_name').where(Issuer.issuer_id == data['issuer_id']).gino.scalar()
    data['formulary'] = []
    for x in await PlanFormulary.query.where(PlanFormulary.plan_id == plan_id).where(PlanFormulary.year == data['year']).order_by(PlanFormulary.drug_tier, PlanFormulary.pharmacy_type).gino.all():
        data['formulary'].append(x.to_json_dict())

    return response.json(data)
