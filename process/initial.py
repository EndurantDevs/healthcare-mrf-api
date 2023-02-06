import datetime
import asyncio
import os
import glob
import tempfile
import msgpack
from pathlib import Path, PurePath
from arq import create_pool
from arq.connections import RedisSettings
import ijson
import json
from dateutil.parser import parse as parse_date
from aiofile import async_open
from async_unzip.unzipper import unzip
import pylightxl as xl
from sqlalchemy.dialects.postgresql import insert

from process.ext.utils import download_it_and_save, make_class, push_objects, log_error, print_time_info, \
    flush_error_log
from db.models import ImportHistory, ImportLog, Issuer, Plan, PlanFormulary, PlanTransparency, db
from db.connection import init_db
from asyncpg import DuplicateTableError


async def process_plan(ctx, task):
    import_date = ctx['import_date']
    myplan = make_class(Plan, import_date)
    myplanformulary = make_class(PlanFormulary, import_date)
    myimportlog = make_class(ImportLog, import_date)

    print('Starting Plan data download: ', task.get('url'))
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get('url'))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        try:
            await download_it_and_save(task.get('url'), tmp_filename,
                                       context={'issuer_array': task['issuer_array'], 'source': 'plans'},
                                       logger=myimportlog)
        except:
            return

        async with async_open(tmp_filename, 'rb') as afp:
            plan_obj = []
            planformulary_obj = []
            count = 0
            try:
                async for res in ijson.items(afp, "item", use_float=True):
                    for year in res['years']:
                        try:
                            for k in (
                                    'plan_id', 'plan_id_type', 'marketing_name', 'summary_url', 'plan_contact',
                                    'network',
                                    'formulary', 'last_updated_on'):
                                if not (k in res and res[k] is not None):
                                    await log_error('err',
                                                    f"Mandatory field `{k}` is not present or incorrect. Plan ID: "
                                                    f"{res['plan_id']}, year: {year}",
                                                    task.get('issuer_array'), task.get('url'), 'plans', 'json',
                                                    myimportlog)

                            if not int(res['plan_id'][:5]) in task.get('issuer_array'):
                                await log_error('err',
                                                f"File describes the issuer that is not defined/allowed by the index CMS PUF."
                                                f"Issuer of Plan: {int(res['plan_id'][:5])}. Allowed issuer list: {''.join(task.get('issuer_array'))}"
                                                f"Plan ID: {res['plan_id']}, year: {year}",
                                                task.get('issuer_array'), task.get('url'), 'plans', 'json', myimportlog)

                            obj = {
                                'plan_id': res['plan_id'],
                                'plan_id_type': res['plan_id_type'],
                                'year': int(year),
                                'issuer_id': int(res['plan_id'][:5]),
                                'state': str(res['plan_id'][5:7]).upper(),
                                'marketing_name': res['marketing_name'],
                                'summary_url': res['summary_url'],
                                'marketing_url': res.get('marketing_url', ''),
                                'formulary_url': res.get('formulary_url', ''),
                                'plan_contact': res['plan_contact'],
                                'network': [(k['network_tier']) for k in res['network']],
                                'benefits': res.get('benefits', []),
                                'last_updated_on': datetime.datetime.combine(
                                    parse_date(res['last_updated_on'], fuzzy=True), datetime.datetime.min.time())
                            }
                            plan_obj.append(obj)
                            if count > int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 50)):
                                await push_objects(plan_obj, myplan)
                                plan_obj = []
                                count = 0
                            else:
                                count += 1
                        except:
                            pass
                        await push_objects(plan_obj, myplan)
                        plan_obj = []
                        count = 0

                    count = 0
                    for year in res['years']:
                        if 'formulary' in res and res['formulary']:
                            for formulary in res['formulary']:
                                if formulary and type(formulary) is dict and ('cost_sharing' in formulary) and \
                                        formulary['cost_sharing']:
                                    try:
                                        for k in ('drug_tier', 'mail_order'):
                                            if not (k in formulary and formulary[k] is not None):
                                                await log_error('err',
                                                                f"Mandatory field `{k}` in Formulary (`formulary`) sub-type is "
                                                                f"not present or "
                                                                f"incorrect. Plan ID: "
                                                                f"{res['plan_id']}, year: {year}",
                                                                task.get('issuer_array'), task.get('url'), 'plans',
                                                                'json',
                                                                myimportlog)
                                        for cost_sharing in formulary['cost_sharing']:
                                            for k in ('pharmacy_type', 'copay_amount', 'copay_opt', 'coinsurance_rate',
                                                      'coinsurance_opt'):
                                                if not (k in cost_sharing):
                                                    await log_error('err',
                                                                    f"Mandatory field `{k}` in Cost Sharing (`cost_sharing`) "
                                                                    f"sub-type is not present or "
                                                                    f"incorrect. Plan ID: "
                                                                    f"{res['plan_id']}, year: {year}",
                                                                    task.get('issuer_array'), task.get('url'), 'plans',
                                                                    'json',
                                                                    myimportlog)
                                            obj = {
                                                'plan_id': res['plan_id'],
                                                'year': int(year),
                                                'drug_tier': formulary.get('drug_tier', ''),
                                                'mail_order': True if formulary.get('mail_order', False) else False,
                                                'pharmacy_type': cost_sharing.get('pharmacy_type', ''),
                                                'copay_amount': float(
                                                    cost_sharing.get('copay_amount')) if cost_sharing.get(
                                                    'copay_amount', None) is not None else None,
                                                'copay_opt': cost_sharing.get('copay_opt', ''),
                                                'coinsurance_rate': float(
                                                    cost_sharing.get('coinsurance_rate')) if cost_sharing.get(
                                                    'coinsurance_rate', None) is not None else None,
                                                'coinsurance_opt': cost_sharing.get('coinsurance_opt', ''),
                                            }
                                            planformulary_obj.append(obj)
                                            if count > int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 50)):
                                                await push_objects(planformulary_obj, myplanformulary)
                                                planformulary_obj = []
                                                count = 0
                                            else:
                                                count += 1
                                    except:
                                        pass
                                    await push_objects(planformulary_obj, myplanformulary)
                                    planformulary_obj = []
                                    count = 0
                                else:
                                    await log_error('warn',
                                                    f"Recommended field 'cost_sharing' is not present or incorrect. Plan ID: {res['plan_id']}, year: {year}",
                                                    task.get('issuer_array'), task.get('url'), 'plans', 'json',
                                                    myimportlog)
                        else:
                            await log_error('err',
                                            f"Mandatory field 'formulary' is not present or incorrect. Plan ID: {res['plan_id']}, year: {year}",
                                            task.get('issuer_array'), task.get('url'), 'plans', 'json', myimportlog)
            except ijson.JSONError as exc:
                await log_error('err',
                                f"JSON Parsing Error: {exc}",
                                task.get('issuer_array'), task.get('url'), 'plans', 'json', myimportlog)
                return
            except ijson.IncompleteJSONError as exc:
                await log_error('err',
                                f"Incomplete JSON: can't read expected data. {exc}",
                                task.get('issuer_array'), task.get('url'), 'plans', 'json', myimportlog)
                return
    await flush_error_log(myimportlog)
    return 1


async def process_json_index(ctx, task):
    redis = ctx['redis']
    issuer_array = task['issuer_array']
    myimportlog = make_class(ImportLog, ctx['import_date'])
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get('url'))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        await download_it_and_save(task.get('url'), tmp_filename,
                                   context={'issuer_array': task['issuer_array'], 'source': 'json_index'},
                                   logger=myimportlog)
        async with async_open(tmp_filename, 'rb') as afp:
            try:
                async for url in ijson.items(afp, "plan_urls.item",
                                             use_float=True):  # , 'formulary_urls', 'provider_urls'
                    print(f"Plan URL: {url}")
                    await redis.enqueue_job('process_plan', {'url': url, 'issuer_array': issuer_array})
                    # break
            except ijson.JSONError as exc:
                await log_error('err',
                                f"JSON Parsing Error: {exc}",
                                task.get('issuer_array'), task.get('url'), 'json_index', 'json', myimportlog)
                return
            except ijson.IncompleteJSONError as exc:
                await log_error('err',
                                f"Incomplete JSON: can't read expected data. {exc}",
                                task.get('issuer_array'), task.get('url'), 'index', 'json', myimportlog)
                return


async def init_file(ctx):
    redis = ctx['redis']

    print('Downloading data from: ', os.environ['HLTHPRT_CMSGOV_MRF_URL_PUF'])

    import_date = ctx['import_date']
    ctx['context']['run'] += 1
    myissuer = make_class(Issuer, import_date)
    myplantransparency = make_class(PlanTransparency, import_date)

    with tempfile.TemporaryDirectory() as tmpdirname:
        transparent_files = json.loads(os.environ['HLTHPRT_CMSGOV_PLAN_TRANSPARENCY_URL_PUF'])
        for file in transparent_files:
            p = 'transp.xlsx'
            tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
            await download_it_and_save(file['url'], tmp_filename)
            await unzip(tmp_filename, tmpdirname)

            tmp_filename = glob.glob(f"{tmpdirname}/*.xlsx")[0]
            xls_file = xl.readxl(tmp_filename)
            os.unlink(tmp_filename)

            obj_list = []
            for ws_name in xls_file.ws_names:
                if not ws_name.startswith('Transparency'):
                    continue
                count = 0
                template = {}
                convert = {
                    'State': 'state',
                    'Issuer_Name': 'issuer_name',
                    'Issuer_ID': 'issuer_id',
                    'Is_Issuer_New_to_Exchange? (Yes_or_No)': 'new_issuer_to_exchange',
                    'SADP_Only?': 'sadp_only',
                    'Plan_ID': 'plan_id',
                    'QHP/SADP': 'qhp_sadp',
                    'Plan_Type': 'plan_type',
                    'Metal_Level': 'metal',
                    'URL_Claims_Payment_Policies': 'claims_payment_policies_url'
                }
                for k, v in convert.items():
                    template[v] = -1

                for row in xls_file.ws(ws=ws_name).rows:
                    if count > 2:
                        obj = {}
                        obj['state'] = row[template['state']].upper()
                        obj['issuer_name'] = row[template['issuer_name']]
                        obj['issuer_id'] = int(row[template['issuer_id']])
                        obj['new_issuer_to_exchange'] = True if row[template['new_issuer_to_exchange']] in (
                            'Yes', 'yes', 'y') else False
                        obj['sadp_only'] = True if row[template['sadp_only']] in ('Yes', 'yes', 'y') else False
                        obj['plan_id'] = row[template['plan_id']]
                        obj['year'] = int(file['year'])
                        obj['qhp_sadp'] = row[template['qhp_sadp']]
                        obj['plan_type'] = row[template['plan_type']]
                        obj['metal'] = row[template['metal']]
                        obj['claims_payment_policies_url'] = row[template['claims_payment_policies_url']]

                        obj_list.append(obj)
                        if count > int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 50)):
                            count = 3
                            await push_objects(obj_list, myplantransparency)
                            obj_list = []
                    elif count == 2:
                        i = 0
                        for name in row:
                            if name in convert:
                                template[convert[name]] = i
                            i += 1
                    count += 1

                await push_objects(obj_list, myplantransparency)

        p = 'mrf_puf.xlsx'
        tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
        await download_it_and_save(os.environ['HLTHPRT_CMSGOV_MRF_URL_PUF'], tmp_filename)
        await unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.xlsx")[0]
        xls_file = xl.readxl(tmp_filename)
        ws_name = xls_file.ws_names[-1]
        os.unlink(tmp_filename)

        count = 0
        url_list = []
        obj_list = []
        url2issuer = {}

        for row in xls_file.ws(ws=ws_name).rows:
            if count != 0:
                url_list.append(row[2])
                obj = {}
                obj['state'] = row[0].upper()
                obj['issuer_id'] = int(row[1])
                obj['mrf_url'] = row[2]
                issuer_name = await myplantransparency.select('issuer_name').where(
                    myplantransparency.issuer_id == obj['issuer_id']).gino.scalar()
                obj['issuer_name'] = issuer_name if issuer_name else 'N/A'
                obj['data_contact_email'] = row[3]
                obj_list.append(obj)
                if obj['mrf_url'] in url2issuer:
                    url2issuer[obj['mrf_url']].append(obj['issuer_id'])
                else:
                    url2issuer[obj['mrf_url']] = [obj['issuer_id'], ]
            count += 1
            if not (count % 100):
                await push_objects(obj_list, myissuer)
                obj_list.clear()

        url_list = list(set(url_list))
        await push_objects(obj_list, myissuer)

        for url in url_list:
            await redis.enqueue_job('process_json_index', {'url': url, 'issuer_array': url2issuer[url]})
            # break


async def startup(ctx):
    loop = asyncio.get_event_loop()
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.now()
    ctx['context']['run'] = 0
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    await init_db(db, loop)
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    try:
        obj = ImportHistory
        await ImportHistory.__table__.gino.create()
        if hasattr(ImportHistory, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")
    except DuplicateTableError:
        pass

    tables = {} # for the future complex usage

    for cls in (Issuer, Plan, PlanFormulary, PlanTransparency, ImportLog):
        tables[cls.__main_table__] = make_class(cls, import_date)
        obj = tables[cls.__main_table__]
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__main_table__}_{import_date};")
        await obj.__table__.gino.create()
        if hasattr(obj, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")


    print("Preparing done")


async def shutdown(ctx):
    import_date = ctx['import_date']
    myimportlog = make_class(ImportLog, ctx['import_date'])
    await flush_error_log(myimportlog)
    db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'mrf'
    await db.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    await db.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")

    test = make_class(Plan, import_date)
    plans_count = await db.func.count(test.plan_id).gino.scalar()
    if (not plans_count) or (plans_count < 500):
        print(f"Failed Import: Plans number:{plans_count}")
        exit(1)



    tables = {}
    async with db.transaction():
        for cls in (Issuer, Plan, PlanFormulary, PlanTransparency, ImportLog):
            tables[cls.__main_table__] = make_class(cls, import_date)
            obj = tables[cls.__main_table__]
            table = obj.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{obj.__tablename__} RENAME TO {table};")

            await db.status(f"ALTER INDEX IF EXISTS "
                            f"{db_schema}.{table}_idx_primary RENAME TO "
                            f"{table}_idx_primary_old;")

            await db.status(f"ALTER INDEX IF EXISTS "
                            f"{db_schema}.{obj.__tablename__}_idx_primary RENAME TO "
                            f"{table}_idx_primary;")

    await insert(ImportHistory).values(import_id=import_date, when=db.func.now()).on_conflict_do_update(
        index_elements=ImportHistory.__my_index_elements__,
        index_where=ImportHistory.import_id.__eq__(import_date),
        set_=dict(when=db.func.now())) \
        .gino.model(ImportHistory).status()
    print('Plans in DB: ', await db.func.count(Plan.plan_id).gino.scalar())  # pylint: disable=E1101
    print_time_info(ctx['context']['start'])


async def main():
    redis = await create_pool(RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS')),
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    x = await redis.enqueue_job('init_file')
