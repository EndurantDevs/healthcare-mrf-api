import asyncio
from datetime import datetime
from api.for_human import attributes_labels
import urllib.parse
from sqlalchemy.sql import func
from urllib.parse import unquote_plus

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, PlanNetworkTierRaw, PlanNPIRaw, Plan, PlanFormulary, Issuer, ImportLog, PlanAttributes

blueprint = Blueprint('plan', url_prefix='/plan', version=1)


@blueprint.get('/')
async def index_status(request):
    async def get_plan_count():
        async with db.acquire():
            return await db.func.count(Plan.plan_id).gino.scalar()

    async def get_plans_network_count():
        async with db.acquire():
            return await db.func.count(db.func.distinct(PlanNPIRaw.checksum_network)).gino.scalar()

    async def get_import_error_count():
        async with db.acquire():
            return await db.func.count(ImportLog.checksum).gino.scalar()

    plan_count, import_error_count, plans_network_count = await asyncio.gather(get_plan_count(),
                                                                               get_import_error_count(),
                                                                               get_plans_network_count())
    data = {
        'date': datetime.utcnow().isoformat(),
        'release': request.app.config.get('RELEASE'),
        'environment': request.app.config.get('ENVIRONMENT'),
        'plan_count': plan_count,
        'plans_network_count': plans_network_count,
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
async def all_plans_variants(request):
    limit = request.args.get("limit")
    offset = request.args.get("offset")
    plan_data = db.select(
        [Plan.marketing_name, Plan.plan_id, PlanAttributes.full_plan_id, Plan.year]).select_from(
        Plan.join(PlanAttributes, ((Plan.plan_id == func.substr(PlanAttributes.full_plan_id, 1, 14)) & (
                Plan.year == PlanAttributes.year)))). \
        group_by(PlanAttributes.full_plan_id, Plan.plan_id, Plan.marketing_name, Plan.year)
    if limit:
        plan_data = plan_data.limit(int(limit))
        if offset:
            plan_data = plan_data.offset(int(offset))

    plan_data = await plan_data.gino.all()
    data = []
    for p in plan_data:
        data.append({'marketing_name': p[0], 'plan_id': p[1], 'full_plan_id': p[2], 'year': p[3]})
    return response.json(data)


@blueprint.get('/network/autocomplete')
async def get_autocomplete_list(request):
    """
The get_autocomplete_list function is used to return a list of plans that match the query string.
The function takes in a request object and returns a JSON response with the plan data.


:param request: Get the query parameter from the request
:return: The list of plans that match the query
"""
    text = request.args.get("query")
    async def get_plans(text, limit=10):
        q =  await Plan.query.where(db.func.lower(Plan.marketing_name).ilike('%' + text.lower() + '%') | db.func.lower(Plan.plan_id).ilike('%' + text.lower() + '%')).limit(100).gino.all()
        plan_id = []
        data = {}
        for x in q:
            t = x.to_json_dict()
            data[t['plan_id']] = t
            data[t['plan_id']]['network_checksum'] = {}
            plan_id.append(t['plan_id'])
        if not plan_id:
            return []

        for (x, y, z) in await db.select([db.func.array_agg(
                db.func.distinct(PlanNetworkTierRaw.plan_id, PlanNetworkTierRaw.checksum_network, PlanNetworkTierRaw.network_tier))]).where(
                PlanNetworkTierRaw.plan_id == db.func.any(plan_id)).gino.scalar():
            data[x]['network_checksum'][y] = z

        del_array=set()
        for key in data:
            print(key)
            if not data[key]['network_checksum']:
                del_array.add(key)
        for key in del_array:
            del data[key]
        return data.values()

    return response.json({'plans': list(await get_plans(text))})


@blueprint.get('/id/<plan_id>', name="get_plan_by_plan_id")
@blueprint.get('/id/<plan_id>/<year>', name="get_plan_by_plan_id_and_year")
@blueprint.get('/id/<plan_id>/<year>/<variant>', name="get_plan_variant_by_plan_id_and_year")
async def get_plan(request, plan_id, year=None, variant=None):
    q = Plan.query.where(Plan.plan_id == plan_id)

    if year:
        q = q.where(Plan.year == int(year))
    else:
        q = q.order_by(Plan.year.desc())

    data = await q.gino.first()
    if not data:
        raise sanic.exceptions.NotFound
    data = data.to_json_dict()

    data['network_checksum'] = {}
    t_list = await db.select([db.func.array_agg(db.func.distinct(PlanNetworkTierRaw.checksum_network, PlanNetworkTierRaw.network_tier))]).where(
        PlanNetworkTierRaw.plan_id == data['plan_id']).where(PlanNetworkTierRaw.year == data['year']).gino.scalar()
    if t_list:
        for x in t_list:
            data['network_checksum'][x[0]] = x[1]

    data['issuer_name'] = await Issuer.select('issuer_name').where(Issuer.issuer_id == data['issuer_id']).gino.scalar()
    data['formulary'] = []
    for x in await PlanFormulary.query.where(PlanFormulary.plan_id == plan_id).where(
            PlanFormulary.year == data['year']).order_by(PlanFormulary.drug_tier,
                                                         PlanFormulary.pharmacy_type).gino.all():
        data['formulary'].append(x.to_json_dict())
    if year:
        data['variants'] = []
        for x in await PlanAttributes.query.distinct(PlanAttributes.full_plan_id.label("distinct_full_plan_id")).where(
                PlanAttributes.year == int(year)).where(PlanAttributes.full_plan_id.like(plan_id + '%')).order_by(
            PlanAttributes.full_plan_id.asc()).gino.all():
            data['variants'].append(x.to_json_dict()['full_plan_id'])
    if variant:
        if variant in data['variants']:
            data['variant_attributes'] = {}
            for x in await PlanAttributes.query.where(PlanAttributes.year == int(year)).where(
                    PlanAttributes.full_plan_id == variant).order_by(PlanAttributes.attr_name.asc()).gino.all():
                t = x.to_json_dict()
                k = t['attr_name']
                data['variant_attributes'][k] = {'attr_value': t['attr_value']}
                if l := attributes_labels.get(k, None):
                    data['variant_attributes'][k]['human_attr_name'] = l
        else:
            raise sanic.exceptions.NotFound

    return response.json(data)
