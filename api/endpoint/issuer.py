import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, Plan, Issuer, ImportLog, PlanNetworkTierRaw

blueprint = Blueprint('issuer', url_prefix='/issuer', version=1)

@blueprint.get('/id/<issuer_id>')
async def get_issuer_data(request, issuer_id):
    data = await Issuer.query.where(Issuer.issuer_id == int(issuer_id)).gino.first()
    if not data:
        raise sanic.exceptions.NotFound
    data = data.to_json_dict()

    # await Plan.query.where(Plan.issuer_id == int(issuer_id)).order_by(Plan.year.desc(), Plan.marketing_name.asc()).gino.all()


    data['plans'] = []
    data['import_errors'] = await db.select([db.func.count(ImportLog.checksum)]).where(ImportLog.issuer_id == int(issuer_id)).gino.scalar()

    plans = (db.select([Plan, PlanNetworkTierRaw]).
             select_from(Plan.join(PlanNetworkTierRaw, Plan.plan_id == PlanNetworkTierRaw.plan_id, isouter=True)).
             where(Plan.issuer_id == int(issuer_id)).
             order_by(Plan.year.desc(), Plan.marketing_name.asc()).gino.load((Plan, PlanNetworkTierRaw)))

    async with db.acquire() as conn:
        async with conn.transaction():
            async for x in plans.iterate():
                plan = x[0].to_json_dict()
                plan['network'] = {'cmsgov_network': plan.get('network')}
                if x[1]:
                    network = x[1].to_json_dict()
                    network_tier = network.get('network_tier', '')
                    network_checksum = network.get('checksum_network', '')
                    if not network_checksum:
                        network_checksum = 0
                    if not network_tier:
                        network_tier = 'N/A'
                    plan['network'].update({
                        'network_tier': network_tier,
                        'display_name': network_tier.replace('-', ' ').replace('  ', ' '),
                        'checksum': network_checksum})
                data['plans'].append(plan)
    data['plans_count'] = len(data['plans'])

    return response.json(data)

@blueprint.get('/', name="issuer_list")
@blueprint.get('/state/<state>')
async def get_issuers(request, state=None):
    data = []
    x = Issuer.query
    if state:
        x = x.where(Issuer.state == state.upper())
    x = x.order_by(Issuer.state.asc()).order_by(Issuer.issuer_name.asc())
    r = await x.gino.all()

    if state:
        error_count = await db.select((ImportLog.issuer_id, db.func.count(ImportLog.issuer_id))).where(
            (ImportLog.issuer_id == Issuer.issuer_id) & (Issuer.state == state.upper())).group_by(
            ImportLog.issuer_id).gino.all()
    else:
        error_count = await db.select((ImportLog.issuer_id, db.func.count(ImportLog.issuer_id))).group_by(
            ImportLog.issuer_id).gino.all()
    error_count_hash = dict()

    for (k,v) in error_count:
        error_count_hash[k] = v

    if state:
        plan_count = await db.select((Plan.issuer_id, db.func.count(Plan.issuer_id))).where(
            (Plan.issuer_id == Issuer.issuer_id) & (Issuer.state == state.upper())).group_by(
                Plan.issuer_id).gino.all()
    else:
        plan_count = await db.select((Plan.issuer_id, db.func.count(Plan.issuer_id))).group_by(
            Plan.issuer_id).gino.all()

    plan_count_hash = dict()

    for (k, v) in plan_count:
        plan_count_hash[k] = v

    for i in r:
        t = i.to_json_dict()
        t['import_errors'] = error_count_hash.get(t['issuer_id'], 0)
        t['plan_count']    = plan_count_hash.get(t['issuer_id'], 0)
        data.append(t)

    if not data:
        raise sanic.exceptions.NotFound

    return response.json(data)