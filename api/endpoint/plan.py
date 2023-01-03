import asyncio
from datetime import datetime
from api.for_human import attributes_labels
import urllib.parse
from sqlalchemy.sql import func

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, Plan, PlanFormulary, Issuer, ImportLog, PlanAttributes

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

@blueprint.get('/all/variants')
async def all_plans(request):
    plan_data = await db.select(
        [Plan.marketing_name, Plan.plan_id, PlanAttributes.full_plan_id, Plan.year]).select_from(Plan.join(PlanAttributes, ((Plan.plan_id == func.substr(PlanAttributes.full_plan_id, 1, 14)) & (Plan.year == PlanAttributes.year)))).\
        group_by(PlanAttributes.full_plan_id, Plan.plan_id, Plan.marketing_name, Plan.year).gino.all()
    data = []
    for p in plan_data:
        data.append({'marketing_name': p[0], 'plan_id': p[1], 'full_plan_id': p[2], 'year': p[3]})
    return response.json(data)

@blueprint.get('/id/<plan_id>')
@blueprint.get('/id/<plan_id>/<year>')
@blueprint.get('/id/<plan_id>/<year>/<variant>')
async def get_plan(request, plan_id, year=None, variant=None):
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
    if year:
        data['variants'] = []
        for x in await PlanAttributes.query.distinct(PlanAttributes.full_plan_id.label("distinct_full_plan_id")).where(PlanAttributes.year == int(year)).where(PlanAttributes.full_plan_id.like(plan_id+'%')).order_by(PlanAttributes.full_plan_id.asc()).gino.all():
            data['variants'].append(x.to_json_dict()['full_plan_id'])
    if variant:
        if variant in data['variants']:
            data['variant_attributes'] = {}
            for x in await PlanAttributes.query.where(PlanAttributes.year == int(year)).where(PlanAttributes.full_plan_id == variant).order_by(PlanAttributes.attr_name.asc()).gino.all():
                t = x.to_json_dict()
                k = t['attr_name']
                data['variant_attributes'][k] = {'attr_value': t['attr_value']}
                if l:=attributes_labels.get(k, None):
                    data['variant_attributes'][k]['human_attr_name'] = l
        else:
            raise sanic.exceptions.NotFound

    return response.json(data)
