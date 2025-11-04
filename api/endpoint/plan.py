import asyncio
from datetime import datetime
from api.for_human import attributes_labels, benefits_labels
import urllib.parse
from sqlalchemy.sql import func
from urllib.parse import unquote_plus

import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, PlanPrices, PlanBenefits, PlanNetworkTierRaw, PlanNPIRaw, Plan, PlanFormulary, Issuer, ImportLog, \
    PlanAttributes
from db.tiger_models import ZipState

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


async def _fetch_network_entry(checksum: int):
    rows = await db.select([
        PlanNetworkTierRaw.plan_id,
        PlanNetworkTierRaw.year,
        PlanNetworkTierRaw.checksum_network,
        PlanNetworkTierRaw.network_tier,
        Issuer.issuer_id,
        Issuer.issuer_name,
        Issuer.issuer_marketing_name,
        Issuer.state,
    ]).select_from(
        PlanNetworkTierRaw.join(Issuer, Issuer.issuer_id == PlanNetworkTierRaw.issuer_id)
    ).where(
        PlanNetworkTierRaw.checksum_network == checksum
    ).gino.all()

    if not rows:
        return None

    plans = []
    seen = set()
    for plan_id, year, *_rest in rows:
        key = (plan_id, year)
        if key not in seen:
            plans.append({"plan_id": plan_id, "year": year})
            seen.add(key)

    _, _, checksum_value, network_tier, issuer_id, issuer_name, issuer_marketing_name, issuer_state = rows[0]
    issuer_marketing_name = issuer_marketing_name or ""
    return {
        "plans": plans,
        "checksum": checksum_value,
        "network_tier": network_tier.replace('-', ' ').replace('  ', ' '),
        "issuer": issuer_id,
        "issuer_name": issuer_name,
        "issuer_marketing_name": issuer_marketing_name,
        "issuer_display_name": issuer_marketing_name or issuer_name,
        "issuer_state": issuer_state,
    }


@blueprint.get('/network/id/<checksum>')
async def get_network_by_checksum(request, checksum):
    entry = await _fetch_network_entry(int(checksum))
    if entry is None:
        raise sanic.exceptions.NotFound
    return response.json(entry)


@blueprint.get('/network/multiple/<checksums>')
async def get_networks_by_checksums(request, checksums):
    values = [value.strip() for value in checksums.split(',') if value.strip()]
    payload = []
    seen_checksums = set()
    for value in values:
        try:
            checksum = int(value)
        except ValueError:
            continue
        if checksum in seen_checksums:
            continue
        seen_checksums.add(checksum)
        entry = await _fetch_network_entry(checksum)
        if entry:
            payload.append(entry)

    if not payload:
        raise sanic.exceptions.NotFound

    return response.json(payload)


@blueprint.get('/network/autocomplete')
async def get_autocomplete_list(request):
    """
The get_autocomplete_list function is used to return a list of plans that match the query string.
The function takes in a request object and returns a JSON response with the plan data.


:param request: Get the query parameter from the request
:return: The list of plans that match the query
"""
    text = request.args.get("query")
    state = request.args.get("state")
    zip_code = request.args.get("zip_code")
    async def get_plans(text, limit=10):
        q = Plan.query.where(db.func.lower(Plan.marketing_name).ilike('%' + text.lower() + '%') | db.func.lower(Plan.plan_id).ilike('%' + text.lower() + '%') | db.func.lower(Issuer.issuer_marketing_name).ilike('%' + text.lower() + '%') | db.func.lower(Issuer.issuer_name).ilike('%' + text.lower() + '%')).limit(100)
        q = q.where(Plan.issuer_id==Issuer.issuer_id)
        q = q.where(Plan.plan_id == PlanNetworkTierRaw.plan_id).where(Plan.year == PlanNetworkTierRaw.year).where(PlanNetworkTierRaw.checksum_network != None)
        if state:
            q = q.where(Plan.state==state)
        elif zip_code:
            q = q.where(ZipState.zip == zip_code).where(Plan.state == ZipState.stusps)
        q = q.limit(limit)
        q = await q.gino.all()
        plan_id = []
        data = {}
        for x in q:
            t = x.to_json_dict()
            data[t['plan_id']] = t
            data[t['plan_id']]['network_checksum'] = {}
            plan_id.append(t['plan_id'])
        if not plan_id:
            return []

        res = await db.select([db.func.array_agg(
            db.func.distinct(PlanNetworkTierRaw.plan_id, PlanNetworkTierRaw.checksum_network,
                             PlanNetworkTierRaw.network_tier))]).where(
            PlanNetworkTierRaw.plan_id == db.func.any(plan_id)).gino.scalar()
        if not res:
            return []

        for (x, y, z) in res:
            data[x]['network_checksum'][y] = z

        del_array=set()
        for key in data:
            if not data[key]['network_checksum']:
                del_array.add(key)
        for key in del_array:
            del data[key]
        return data.values()

    return response.json({'plans': list(await get_plans(text))})


@blueprint.get('/search', name="find_a_plan")
async def find_a_plan(request):
    age = request.args.get("age")
    state = request.args.get("state")
    rating_area = request.args.get("rating_area")
    zip_code = request.args.get("zip_code")
    year = request.args.get("year")
    limit = request.args.get("limit")
    page = request.args.get("page")
    order = request.args.get("order")
    order_by = request.args.get("order_by")


    order_field = {
        'plan_id': Plan.plan_id,
        'marketing_name': Plan.marketing_name,
        'issuer_id': Plan.issuer_id,
        'price': PlanPrices.individual_rate,
    }

    if order_by not in order_field:
        order_by = 'plan_id'

    if order and (t := order.lower() in ('desc', 'asc')):
        order = t
    else:
        order = 'asc'

    if not limit:
        limit = 100
    else:
        try: 
            limit = int(limit)
        except ValueError:
            limit = 100

    if not page:
        page = 0
    else:
        try:
            page = int(page)
        except ValueError:
            page = 1
        page = int(page)-1
        if page < 0:
            page = 0

    q = db.select(
        [
            Plan.plan_id.label('found_plan_id'),
            db.func.min(PlanPrices.individual_rate).label('min_individual_rate'),
            db.func.max(PlanPrices.individual_rate).label('max_individual_rate'),
            Plan.year.label('year')
        ]
    ).where(Plan.plan_id == PlanPrices.plan_id).where(Plan.year == PlanPrices.year)

    count_q = db.select(
        [
            db.func.count(db.distinct(db.tuple_(Plan.plan_id, Plan.year)))
        ]
    ).where(Plan.plan_id == PlanPrices.plan_id).where(Plan.year == PlanPrices.year)

    plan_attr_q = PlanAttributes.query
    plan_benefits_q = PlanBenefits.query

    # .where(
    #PlanNPIRaw.npi == npi).order_by(PlanNPIRaw.issuer_id.desc()).gino.load((PlanNPIRaw, Issuer))

    # async with db.acquire() as conn:
    #     async with conn.transaction():
    #         async for x in q.iterate():
    #             data.append({'npi_info': x[0].to_json_dict(), 'issuer_info': x[1].to_json_dict()})

    if state:
        q = q.where(Plan.state == state)
        count_q = count_q.where(Plan.state == state)
    elif zip_code:
        q = q.where(ZipState.zip == zip_code).where(Plan.state == ZipState.stusps)
        count_q = count_q.where(ZipState.zip == zip_code).where(Plan.state == ZipState.stusps)

    if year:
        try:
            year = int(year)
        except:
            raise sanic.exceptions.BadRequest
        q = q.where(Plan.year == year)
        count_q = count_q.where(Plan.year == year)
        plan_attr_q = plan_attr_q.where(PlanAttributes.year == year)
        plan_benefits_q = plan_benefits_q.where(PlanBenefits.year == year)
    else:
        subq = db.select([db.func.max(Plan.year)]).limit(1).as_scalar()
        q = q.where(Plan.year == subq)
        count_q = count_q.where(Plan.year == subq)
        plan_attr_q = plan_attr_q.where(PlanAttributes.year == subq)
        plan_benefits_q = plan_benefits_q.where(PlanBenefits.year == subq)

    if age:
        try:
            age = int(age)
        except:
            raise sanic.exceptions.BadRequest
        q = q.where(PlanPrices.min_age <= age).where(PlanPrices.max_age >= age)
        count_q = count_q.where(PlanPrices.min_age <= age).where(PlanPrices.max_age >= age)

    if rating_area:
        q = q.where(PlanPrices.rating_area_id == rating_area)
        count_q = count_q.where(PlanPrices.rating_area_id == rating_area)
    async def get_plans_count(q):
        async with db.acquire() as conn:
            async with conn.transaction() as tx:
                # (bind=tx.connection)
                return await q.gino.scalar()

    count = asyncio.create_task(get_plans_count(count_q))

    q = q.limit(limit).offset(limit * page + 1).group_by(Plan.plan_id, Plan.year).order_by(
        order_field[order_by].asc() if order == 'asc' else order_field[order_by].desc())
    q = q.alias("prices_result")

    main_q = db.select([Plan, q.c.min_individual_rate, q.c.max_individual_rate]).select_from(
        Plan.join(q, (Plan.plan_id == q.c.found_plan_id) & (Plan.year == q.c.year))).gino.load(
        (
            Plan,
            db.Column('min_individual_rate',
                      db.Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None)),
            db.Column('max_individual_rate',
                      db.Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
        ))

    res = {}
    count = await count
    found_array = []
    async with db.acquire() as conn:
        async with conn.transaction() as tx:
            async for x in main_q.iterate():
                t = x[0].to_json_dict()
                found_array.append(t['plan_id']+'-00')
                t['price_range'] = {
                    'min': float(x[1]),
                    'max': float(x[2])
                }
                res[t['plan_id']] = t
                res[t['plan_id']]['attributes'] = {}
                res[t['plan_id']]['plan_benefits'] = {}




    plan_attr_q = plan_attr_q.where(PlanAttributes.full_plan_id.in_(found_array))

    async with db.acquire() as conn:
        async with conn.transaction() as tx:
            async for x in plan_attr_q.gino.iterate():
                res[x.full_plan_id[:-3]]['attributes'][x.attr_name] = x.attr_value

    plan_benefits_q = plan_benefits_q.where(PlanBenefits.full_plan_id.in_(found_array))

    async with db.acquire() as conn:
        async with conn.transaction() as tx:
            async for x in plan_benefits_q.gino.iterate():
                res[x.full_plan_id[:-3]]['plan_benefits'][x.benefit_name] = x.to_json_dict()

    return response.json({'total': count, 'results': list(res.values())})



@blueprint.get('/price/<plan_id>', name="get_price_plan_by_plan_id")
@blueprint.get('/price/<plan_id>/year', name="get_price_plan_by_plan_id_and_year")
async def get_price_plan(request, plan_id, year=None, variant=None):
    age = request.args.get("age")
    rating_area = request.args.get("rating_area")

    q = PlanPrices.query.where(PlanPrices.plan_id == plan_id)
    if year:
        try:
            year = int(year)
        except:
            raise sanic.exceptions.BadRequest
        q = q.where(PlanPrices.year == year)

    if age:
        try:
            age = int(age)
        except:
            raise sanic.exceptions.BadRequest
        q = q.where(PlanPrices.min_age <= age).where(PlanPrices.max_age >= age)

    if rating_area:
        q = q.where(PlanPrices.rating_area_id == rating_area)

    q = q.order_by(PlanPrices.year, PlanPrices.rating_area_id, PlanPrices.min_age)

    res = []
    async with db.acquire() as conn:
        async with conn.transaction():
            async for x in q.gino.iterate():
                res.append(x.to_json_dict())

    return response.json(res)


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
            data['variant_benefits'] = {}
            for x in await PlanAttributes.query.where(PlanAttributes.year == int(year)).where(
                    PlanAttributes.full_plan_id == variant).order_by(PlanAttributes.attr_name.asc()).gino.all():
                t = x.to_json_dict()
                k = t['attr_name']
                data['variant_attributes'][k] = {'attr_value': t['attr_value']}
                if l := attributes_labels.get(k, None):
                    data['variant_attributes'][k]['human_attr_name'] = l

            for x in await PlanBenefits.query.where(PlanBenefits.year == int(year)).where(
                    PlanBenefits.full_plan_id == variant).order_by(PlanBenefits.benefit_name.asc()).gino.all():
                t = x.to_json_dict()
                del t['full_plan_id']
                del t['year']
                del t['plan_id']
                k = t['benefit_name']

                t['in_network_tier1'] = None
                t['in_network_tier2'] = None
                t['out_network'] = None

                if t['copay_inn_tier1'] and (t['copay_inn_tier1'] != 'Not Applicable'):
                    t['in_network_tier1'] = t['copay_inn_tier1']
                if t['coins_inn_tier1'] and (t['coins_inn_tier1'] != 'Not Applicable'):
                    if t.get('in_network_tier1', None):
                        if not (t['in_network_tier1'] == 'No Charge' and t['coins_inn_tier1'] == 'No Charge'):
                            t['in_network_tier1'] += ', ' + t['coins_inn_tier1']
                    else:
                        t['in_network_tier1'] = t['coins_inn_tier1']

                if t['copay_inn_tier2'] and (t['copay_inn_tier2'] != 'Not Applicable'):
                   t['in_network_tier2'] = t['copay_inn_tier2']
                if t['coins_inn_tier2'] and (t['coins_inn_tier2'] != 'Not Applicable'):
                    if t.get('in_network_tier2', None):
                        if not (t['in_network_tier2'] == 'No Charge' and t['coins_inn_tier2'] == 'No Charge'):
                            t['in_network_tier2'] += ', ' + t['coins_inn_tier2']
                    else:
                        t['in_network_tier2'] = t['coins_inn_tier2']

                if t['copay_outof_net'] and (t['copay_outof_net'] != 'Not Applicable'):
                   t['out_network'] = t['copay_outof_net']
                if t['coins_outof_net'] and (t['coins_outof_net'] != 'Not Applicable'):
                    if t.get('out_network', None):
                        if not (t['out_network'] == 'No Charge' and t['coins_outof_net'] == 'No Charge'):
                            t['out_network'] += ', ' + t['coins_outof_net']
                    else:
                        t['out_network'] = t['coins_outof_net']


                data['variant_benefits'][k] = t
                if l := benefits_labels.get(k, None):
                    data['variant_benefits'][k]['human_attr_name'] = l
        else:
            raise sanic.exceptions.NotFound

    return response.json(data)
