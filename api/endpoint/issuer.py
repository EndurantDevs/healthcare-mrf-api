import sanic.exceptions
from sanic import response
from sanic import Blueprint

from db.models import db, Plan, Issuer, ImportLog

blueprint = Blueprint('issuer', url_prefix='/issuer', version=1)

@blueprint.get('/id/<issuer_id>')
async def get_issuer_data(request, issuer_id):
    data = await Issuer.query.where(Issuer.issuer_id == int(issuer_id)).gino.first()
    if not data:
        raise sanic.exceptions.NotFound
    data = data.to_json_dict()
    plans = await Plan.query.where(Plan.issuer_id == int(issuer_id)).order_by(Plan.year.desc(), Plan.marketing_name.asc()).gino.all()
    data['plans'] = []
    data['plans_count'] = len(plans)
    data['import_errors'] = await db.select([db.func.count(ImportLog.checksum)]).where(ImportLog.issuer_id == int(issuer_id)).gino.scalar()

    for p in plans:
        data['plans'].append(p.to_json_dict())
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