import os
import msgpack
import asyncio
import datetime
import pytz
import tempfile
import json
import glob
import re
from arq import create_pool
from arq.connections import RedisSettings
from pathlib import Path, PurePath
from aiocsv import AsyncDictReader

import pylightxl as xl
from aiofile import async_open
from async_unzip.unzipper import unzip

from process.ext.utils import download_it_and_save, make_class, push_objects, log_error, print_time_info, \
    flush_error_log, return_checksum
from dateutil.parser import parse as parse_date
from db.models import Issuer, Plan, PlanAttributes, PlanPrices, db
from db.connection import init_db

from api.for_human import plan_attributes_labels_to_key


latin_pattern= re.compile(r'[^\x00-\x7f]')

async def startup(ctx):
    loop = asyncio.get_event_loop()
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.now()
    ctx['context']['run'] = 0
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    await init_db(db, loop)
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    tables = {}  # for the future complex usage

    for cls in (PlanAttributes, PlanPrices, ):
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
    db_schema = os.getenv('DB_SCHEMA') if os.getenv('DB_SCHEMA') else 'mrf'
    tables = {}

    processing_classes_array = (PlanAttributes, PlanPrices, )

    for cls in processing_classes_array:
        tables[cls.__main_table__] = make_class(cls, import_date)
        obj = tables[cls.__main_table__]

        if hasattr(cls, '__my_additional_indexes__') and cls.__my_additional_indexes__:
            for index in cls.__my_additional_indexes__:
                index_name = index.get('name', '_'.join(index.get('index_elements')))
                using = ""
                if t := index.get('using'):
                    using = f"USING {t} "
                create_index_sql = f"CREATE INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} " \
                                   f"ON {db_schema}.{obj.__tablename__}  {using}" \
                                   f"({', '.join(index.get('index_elements'))});"
                print(create_index_sql)
                x = await db.status(create_index_sql)

        print(f"Post-Index VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};");
        await db.status(f"VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")



    async with db.transaction() as tx:
        for cls in processing_classes_array:
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

            if hasattr(cls, '__my_additional_indexes__') and obj.__my_additional_indexes__:
                for index in obj.__my_additional_indexes__:
                    index_name = index.get('name', '_'.join(index.get('index_elements')))
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.{table}_idx_{index_name} RENAME TO "
                                    f"{table}_idx_{index_name}_old;")
                    await db.status(f"ALTER INDEX IF EXISTS "
                                    f"{db_schema}.{obj.__tablename__}_idx_{index_name} RENAME TO "
                                    f"{table}_idx_{index_name};")

    print_time_info(ctx['context']['start'])


async def save_attributes(ctx, task):
    import_date = ctx['import_date']
    if ('type' in task) and task['type'] == 'PlanPrices':
        myplanattributes = make_class(PlanPrices, import_date)
    else:
        myplanattributes = make_class(PlanAttributes, import_date)
    await push_objects(task['attr_obj_list'], myplanattributes)


async def process_attributes(ctx, task):
    redis = ctx['redis']

    print('Downloading data from: ', task['url'])

    import_date = ctx['import_date']
    myissuer = make_class(Issuer, import_date)
    myplanattributes = make_class(PlanAttributes, import_date)


    with tempfile.TemporaryDirectory() as tmpdirname:
        p = 'attr.csv'
        tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
        await download_it_and_save(task['url'], tmp_filename)
        await unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        total_count = 0
        attr_obj_list = []

        count = 0
        #return 1
        async with async_open(tmp_filename, 'r') as afp:
            async for row in AsyncDictReader(afp, delimiter=","):
                if not (row['StandardComponentId'] and row['PlanId']):
                    continue
                count += 1
                for key in row:
                    if not ((key in ('StandardComponentId',)) and (row[key] is None)) and (t := str(row[key]).strip()):
                        obj = {
                            'full_plan_id': row['PlanId'],
                            'year': int(task['year']),  # int(row['\ufeffBusinessYear'])
                            'attr_name': re.sub(latin_pattern,r'', key),
                            'attr_value': t
                        }

                        attr_obj_list.append(obj)

                if count > 10000:
                    #int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 100)):
                    total_count += count
                    await redis.enqueue_job('save_attributes', {'attr_obj_list': attr_obj_list})
                    # await push_objects(attr_obj_list, myplanattributes)
                    # test = {}
                    # for x in attr_obj_list:
                    #     test[x['full_plan_id']] = 1
                    # print(f"{task['year']}: processed {total_count} + rows {len(attr_obj_list)} -- {row['StandardComponentId']} -- {len(test.keys())}")
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanattributes)

async def process_prices(ctx, task):
    redis = ctx['redis']

    print('Downloading data from: ', task['url'])

    import_date = ctx['import_date']
    myissuer = make_class(Issuer, import_date)
    myplanprices = make_class(PlanPrices, import_date)

    with tempfile.TemporaryDirectory() as tmpdirname:
        p = 'rate.csv'
        tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
        await download_it_and_save(task['url'], tmp_filename)
        await unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]
        total_count = 0
        attr_obj_list = []

        count = 0

        range_regex = re.compile(r'^(\d+)-(\d+)$')
        int_more_regex = re.compile(r'^(\d+) and over$')
        clean_int = re.compile(r'^(\d+)$')
        async with async_open(tmp_filename, 'r') as afp:
            async for row in AsyncDictReader(afp, delimiter=","):
                if not row['PlanId']:
                    continue
                count += 1

                obj = {
                    'plan_id': row['PlanId'],
                    'state': row['StateCode'].upper(),
                    'year': int(task['year']),
                    'rate_effective_date': pytz.utc.localize(
                        parse_date(row['RateEffectiveDate'], fuzzy=True)) if row['RateEffectiveDate'] else None,
                    'rate_expiration_date': pytz.utc.localize(
                        parse_date(row['RateExpirationDate'], fuzzy=True)) if row[
                        'RateExpirationDate'] else None,
                    'rating_area_id': row['RatingAreaId'],
                    'tobacco': row['Tobacco'],
                    'min_age': 0,
                    'max_age': 125,
                    'individual_rate': float(row['IndividualRate']) if row['IndividualRate'] else None,
                    'individual_tobacco_rate': float(row['IndividualTobaccoRate']) if row['IndividualTobaccoRate'] else None,
                    'couple': float(row['Couple']) if row['Couple'] else None,
                    'primary_subscriber_and_one_dependent': float(row['PrimarySubscriberAndOneDependent']) if row['PrimarySubscriberAndOneDependent'] else None,
                    'primary_subscriber_and_two_dependents': float(row['PrimarySubscriberAndTwoDependents']) if row['PrimarySubscriberAndTwoDependents'] else None,
                    'primary_subscriber_and_three_or_more_dependents': float(row[
                        'PrimarySubscriberAndThreeOrMoreDependents']) if row[
                        'PrimarySubscriberAndThreeOrMoreDependents'] else None,
                    'couple_and_one_dependent': float(row['CoupleAndOneDependent']) if row['CoupleAndOneDependent'] else None,
                    'couple_and_two_dependents': float(row['CoupleAndTwoDependents']) if row['CoupleAndTwoDependents'] else None,
                    'couple_and_three_or_more_dependents': float(row['CoupleAndThreeOrMoreDependents']) if row['CoupleAndThreeOrMoreDependents'] else None,
                }

                match row['Age'].strip():
                    case x if t := clean_int.search(x):
                        obj['min_age'] = int(t.group(1))
                        obj['max_age'] = obj['min_age']
                    case x if t := range_regex.search(x):
                        obj['min_age'] = int(t.group(1))
                        obj['max_age'] = int(t.group(2))
                    case x if t := int_more_regex.search(x):
                        obj['min_age'] = int(t.group(1))

                obj['checksum'] = return_checksum(
                    [obj['plan_id'], obj['year'], obj['rate_effective_date'], obj['rate_expiration_date'],
                        obj['rating_area_id'], obj['min_age'], obj['max_age']])

                attr_obj_list.append(obj)

                if count > 1000000:
                    total_count += count
                    await redis.enqueue_job('save_attributes', {'type': 'PlanPrices', 'attr_obj_list': attr_obj_list})
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanprices)



        #     obj_list = []
        #     for ws_name in xls_file.ws_names:
        #         print(ws_name)
        #         if not ws_name.startswith('Transparency'):
        #             continue
        #         count = 0
        #         template = {}
        #         convert = {
        #             'State': 'state',
        #             'Issuer_Name': 'issuer_name',
        #             'Issuer_ID': 'issuer_id',
        #             'Is_Issuer_New_to_Exchange? (Yes_or_No)': 'new_issuer_to_exchange',
        #             'SADP_Only?': 'sadp_only',
        #             'Plan_ID': 'plan_id',
        #             'QHP/SADP': 'qhp_sadp',
        #             'Plan_Type': 'plan_type',
        #             'Metal_Level': 'metal',
        #             'URL_Claims_Payment_Policies': 'claims_payment_policies_url'
        #         }
        #         for k, v in convert.items():
        #             template[v] = -1
        #
        #         for row in xls_file.ws(ws=ws_name).rows:
        #             if count > 2:
        #                 obj = {}
        #                 obj['state'] = row[template['state']].upper()
        #                 obj['issuer_name'] = row[template['issuer_name']]
        #                 obj['issuer_id'] = int(row[template['issuer_id']])
        #                 obj['new_issuer_to_exchange'] = True if row[template['new_issuer_to_exchange']] in (
        #                     'Yes', 'yes', 'y') else False
        #                 obj['sadp_only'] = True if row[template['sadp_only']] in ('Yes', 'yes', 'y') else False
        #                 obj['plan_id'] = row[template['plan_id']]
        #                 obj['year'] = int(file['year'])
        #                 obj['qhp_sadp'] = row[template['qhp_sadp']]
        #                 obj['plan_type'] = row[template['plan_type']]
        #                 obj['metal'] = row[template['metal']]
        #                 obj['claims_payment_policies_url'] = row[template['claims_payment_policies_url']]
        #
        #                 obj_list.append(obj)
        #                 if count > int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 50)):
        #                     count = 3
        #                     await push_objects(obj_list, myplantransparency)
        #                     obj_list = []
        #             elif count == 2:
        #                 i = 0
        #                 for name in row:
        #                     if name in convert:
        #                         template[convert[name]] = i
        #                     i += 1
        #             count += 1
        #
        #         await push_objects(obj_list, myplantransparency)
        #
        # p = 'mrf_puf.xlsx'
        # tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
        # await download_it_and_save(os.environ['HLTHPRT_CMSGOV_MRF_URL_PUF'], tmp_filename)
        # await unzip(tmp_filename, tmpdirname)
        #
        # tmp_filename = glob.glob(f"{tmpdirname}/*.xlsx")[0]
        # xls_file = xl.readxl(tmp_filename)
        # ws_name = xls_file.ws_names[1]
        # os.unlink(tmp_filename)
        #
        # count = 0
        # url_list = []
        # obj_list = []
        # url2issuer = {}
        #
        # for row in xls_file.ws(ws=ws_name).rows:
        #     if count != 0:
        #         url_list.append(row[2])
        #         obj = {}
        #         obj['state'] = row[0].upper()
        #         obj['issuer_id'] = int(row[1])
        #         obj['mrf_url'] = row[2]
        #         issuer_name = await myplantransparency.select('issuer_name').where(
        #             myplantransparency.issuer_id == obj['issuer_id']).gino.scalar()
        #         obj['issuer_name'] = issuer_name if issuer_name else 'N/A'
        #         obj['data_contact_email'] = row[3]
        #         obj_list.append(obj)
        #         if obj['mrf_url'] in url2issuer:
        #             url2issuer[obj['mrf_url']].append(obj['issuer_id'])
        #         else:
        #             url2issuer[obj['mrf_url']] = [obj['issuer_id'], ]
        #     count += 1
        #     if not (count % 100):
        #         await push_objects(obj_list, myissuer)
        #         obj_list.clear()
        #
        # url_list = list(set(url_list))
        # await push_objects(obj_list, myissuer)
        #
        # for url in url_list:
        #     await redis.enqueue_job('process_json_index', {'url': url, 'issuer_array': url2issuer[url]})
        #     # break


async def process_state_attributes(ctx, task):
    redis = ctx['redis']

    print('Downloading data from: ', task['url'])

    import_date = ctx['import_date']
    myplanattributes = make_class(PlanAttributes, import_date)


    with tempfile.TemporaryDirectory() as tmpdirname:
        p = 'attr.csv'
        tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
        await download_it_and_save(task['url'], tmp_filename)
        await unzip(tmp_filename, tmpdirname)

        tmp_filename = glob.glob(f"{tmpdirname}/*Plans*.csv")[0]
        total_count = 0
        attr_obj_list = []

        plan_list = {}

        count = 0
        #return 1

        async with async_open(tmp_filename, 'r') as afp:
            async for row in AsyncDictReader(afp, delimiter=","):
                if not (row['STANDARD COMPONENT ID'] and row['PLAN ID']):
                    continue
                count += 1
                for key in row:
                    if not ((key in ('StandardComponentId',)) and (row[key] is None)) and (t := str(row[key]).strip()):
                        obj = {
                            'full_plan_id': row['PLAN ID'],
                            'year': int(task['year']),  # int(row['\ufeffBusinessYear'])
                            'attr_name': re.sub(latin_pattern,r'', plan_attributes_labels_to_key[key]),
                            'attr_value': t
                        }

                        attr_obj_list.append(obj)

                if count > 10000:
                    #int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 100)):
                    total_count += count
                    await redis.enqueue_job('save_attributes', {'attr_obj_list': attr_obj_list})
                    # await push_objects(attr_obj_list, myplanattributes)
                    # test = {}
                    # for x in attr_obj_list:
                    #     test[x['full_plan_id']] = 1
                    # print(f"{task['year']}: processed {total_count} + rows {len(attr_obj_list)} -- {row['StandardComponentId']} -- {len(test.keys())}")
                    attr_obj_list.clear()
                    count = 0
                else:
                    count += 1

            if attr_obj_list:
                await push_objects(attr_obj_list, myplanattributes)


        # async with async_open(tmp_filename, 'r') as afp:
        #     async for row in AsyncDictReader(afp, delimiter=","):
        #         import pprint
        #         real_key = {}
        #         for key in attributes_labels:
        #             real_key[key.lower()] = key
        #
        #         for key in row:
        #             row[key] = real_key.get(''.join(x for x in key.lower() if not x.isspace()), None)
        #
        #         pprint.pprint(row)
        #         exit(1)
        #
        #         if not (row['STANDARD COMPONENT ID'] and row['PLAN ID']):
        #             continue
        #         count += 1
        #         # for key in row:
        #         # this must be re-written to unify with other Plan Attributes
        #         # too much hardcoding!!
        #         if t := str(row.get('BENEFIT NAME', '').strip()):
        #             if row['IS COVERED'].lower() == 'yes':
        #                 value = 'Covered.'
        #             else:
        #                 value = 'Not Covered.'
        #
        #             if row['QUANTITY LIMIT ON SVC'].lower() == 'yes':
        #                 value += f" {row['LIMIT QUANTITY']} {row['LIMIT UNIT']}."
        #
        #             if t2 := row['EXPLANATION'].strip():
        #                 value += f" {t2}"
        #             obj = {
        #                 'full_plan_id': row['PLAN ID'],
        #                 'year': int(task['year']),  # int(row['\ufeffBusinessYear'])
        #                 'attr_name': re.sub(latin_pattern,r'', t),
        #                 'attr_value': value
        #             }
        #
        #             attr_obj_list.append(obj)
        #
        #         if count > 10000:
        #             #int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 100)):
        #             total_count += count
        #             await redis.enqueue_job('save_attributes', {'attr_obj_list': attr_obj_list})
        #             # await push_objects(attr_obj_list, myplanattributes)
        #             # test = {}
        #             # for x in attr_obj_list:
        #             #     test[x['full_plan_id']] = 1
        #             # print(f"{task['year']}: processed {total_count} + rows {len(attr_obj_list)} -- {row['StandardComponentId']} -- {len(test.keys())}")
        #             attr_obj_list.clear()
        #             count = 0
        #         else:
        #             count += 1
        #
        #     if attr_obj_list:
        #         await push_objects(attr_obj_list, myplanattributes)


async def main():
    redis = await create_pool(RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS')),
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    attribute_files = json.loads(os.environ['HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF'])
    state_attribute_files = json.loads(os.environ['HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF'])

    price_files = json.loads(os.environ['HLTHPRT_CMSGOV_PRICE_PLAN_URL_PUF'])


    print("Starting to process STATE Plan Attribute files..")
    for file in state_attribute_files:
        print("Adding: ", file)
        x = await redis.enqueue_job('process_state_attributes', {'url': file['url'], 'year': file['year']})


    print("Starting to process Plan Attribute files..")
    for file in attribute_files:
        print("Adding: ", file)
        x = await redis.enqueue_job('process_attributes', {'url': file['url'], 'year': file['year']})

    print("Starting to process Plan Prices files..")
    for file in price_files:
        print("Adding: ", file)
        x = await redis.enqueue_job('process_prices', {'url': file['url'], 'year': file['year']})