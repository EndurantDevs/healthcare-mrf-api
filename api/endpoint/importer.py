# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=not-callable

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import distinct, func, select

from db.models import ImportHistory, ImportLog, Issuer, Plan, db

blueprint = Blueprint('import', url_prefix='/import', version=1)


async def _collect_import_stats():
    states_data = {"issuer_number": {}, "plan_number": {}}

    async with db.transaction():
        issuer_rows = await db.all(
            select(Plan.state, func.count(distinct(Plan.issuer_id))).group_by(Plan.state)
        )
        plan_rows = await db.all(
            select(Plan.state, func.count(distinct(Plan.plan_id))).group_by(Plan.state)
        )
        states_data["issuer_number"] = {row[0]: row[1] for row in issuer_rows}
        states_data["plan_number"] = {row[0]: row[1] for row in plan_rows}

        return {
            "plans_count": await db.scalar(select(func.count(Plan.plan_id))),
            "import_log_errors": await db.scalar(select(func.count(ImportLog.checksum))),
            "import_date": await db.scalar(
                select(ImportHistory.when)
                .order_by(ImportHistory.when.desc())
                .limit(1)
            ),
            "issuers_number": await db.scalar(select(func.count(Issuer.issuer_id))),
            "issuers_by_state": states_data["issuer_number"],
            "plans_by_state": states_data["plan_number"],
        }


@blueprint.get('/')
async def last_import_stats(_request):
    data = await _collect_import_stats()
    return response.json(data, default=str)


@blueprint.get('/issuer/<issuer_id>')
async def issuer_import_data(_request, issuer_id):
    issuer_stmt = select(Issuer).where(Issuer.issuer_id == int(issuer_id))
    issuer = await db.scalar(issuer_stmt)
    if not issuer:
        raise sanic.exceptions.NotFound
    data = issuer.to_json_dict()

    logs_stmt = select(ImportLog).where(ImportLog.issuer_id == int(issuer_id))
    logs_result = await db.execute(logs_stmt)
    err_log = logs_result.scalars().all()
    data['err_log'] = []
    data['import_errors_count'] = len(err_log)

    for err_entry in err_log:
        data['err_log'].append(err_entry.to_json_dict())
    return response.json(data)
