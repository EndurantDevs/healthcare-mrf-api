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
from aiocsv import AsyncDictReader

from process.ext.utils import download_it_and_save, make_class, push_objects, log_error, print_time_info, \
    flush_error_log, return_checksum
from db.models import PlanNPIRaw, PlanNetworkTierRaw, ImportHistory, ImportLog, Issuer, Plan, PlanFormulary, PlanTransparency, db
from db.connection import init_db
from asyncpg import DuplicateTableError


async def process_plan(ctx, task):
    """
    The process_plan function is responsible for downloading the plan data from the CMS PUF,
        parsing it and saving to a database.

    :param ctx: Pass the import_date to the function
    :param task: Pass the task object to the function
    :return: 1 if there is no error
    """
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
                                                f"File describes the issuer that is not defined/allowed by the index "
                                                f"CMS PUF."
                                                f"Issuer of Plan: {int(res['plan_id'][:5])}. Allowed issuer list: "
                                                f"{', '.join([str(x) for x in task.get('issuer_array')])}"
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
                                'benefits': [json.dumps(x) for x in res.get('benefits', [])],
                                'last_updated_on': datetime.datetime.combine(
                                    parse_date(res['last_updated_on'], fuzzy=True), datetime.datetime.min.time()),
                                'checksum': return_checksum([res['plan_id'].lower(), year], crc=32)
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
                                                                f"Mandatory field `{k}` in Formulary (`formulary`) "
                                                                f"sub-type is "
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
                                                                    f"Mandatory field `{k}` in Cost Sharing ("
                                                                    f"`cost_sharing`) "
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

                                    planformulary_obj = []
                                    count = 0
                                else:
                                    await log_error('warn',
                                                    f"Recommended field 'cost_sharing' is not present or incorrect. "
                                                    f"Plan ID: {res['plan_id']}, year: {year}",
                                                    task.get('issuer_array'), task.get('url'), 'plans', 'json',
                                                    myimportlog)
                        else:
                            await log_error('err',
                                            f"Mandatory field 'formulary' is not present or incorrect. Plan ID: "
                                            f"{res['plan_id']}, year: {year}",
                                            task.get('issuer_array'), task.get('url'), 'plans', 'json', myimportlog)

                await push_objects(plan_obj, myplan)
                await push_objects(planformulary_obj, myplanformulary)
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


async def process_provider(ctx, task):
    """
    The process_provider function is responsible for downloading the provider data from the CMS PUF website,
        parsing it into a JSON object, and then inserting that data into our database.

        The function takes in two arguments: ctx and task. Ctx is a dictionary containing information about the
        current import date (the date of which we are importing data). Task contains information about what URL to
        download from as well as what issuers are allowed to be imported based on our index file.

    :param ctx: Pass the import_date value to the function
    :param task: Pass the url of the file to be downloaded
    :return: 1 if the file is successfully processed
    """
    import_date = ctx['import_date']
    current_year = datetime.datetime.now().year
    myimportlog = make_class(ImportLog, import_date)
    myplan_npi = make_class(PlanNPIRaw, import_date)
    myplan_networktier = make_class(PlanNetworkTierRaw, import_date)

    print('Starting Provider file data download: ', task.get('url'))
    with tempfile.TemporaryDirectory() as tmpdirname:
        p = Path(task.get('url'))
        tmp_filename = str(PurePath(str(tmpdirname), p.name))
        try:
            await download_it_and_save(task.get('url'), tmp_filename,
                                       context={'issuer_array': task['issuer_array'], 'source': 'providers'},
                                       logger=myimportlog)
        except:
            return
        async with async_open(tmp_filename, 'rb') as afp:
            plan_npi_obj_dict = {}
            plan_network_year = {}
            count = 0
            try:
                async for res in ijson.items(afp, "item", use_float=True):
                    my_network_tiers = {}
                    not_good = False
                    my_years = set()
                    if not (res.get('plans', None) and res['plans']):
                        continue
                    for plan in res['plans']:
                        # try:
                        #     for k in (
                        #             'npi', 'type', 'plans', 'addresses', 'last_updated_on'):
                        #         if not (k in res and res[k] is not None):
                        #             await log_error('err',
                        #                             f"Mandatory field `{k}` for providers data is not present or "
                        #                             f"incorrect. Plan ID: "
                        #                             f"{plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                             task.get('issuer_array'), task.get('url'), 'plans', 'json',
                        #                             myimportlog)

                            # if not int(plan['plan_id'][:5]) in task.get('issuer_array'):
                            #     await log_error('err',
                            #                     f"File describes the issuer that is not defined/allowed by the index "
                            #                     f"CMS PUF."
                            #                     f"Issuer of Plan: {int(plan['plan_id'][:5])}. Allowed issuer list: "
                            #                     f"{''.join([str(x) for x in task.get('issuer_array')])}"
                            #                     f"Plan ID: {plan['plan_id']}, NPI: {res.get('npi', None)}",
                            #                     task.get('issuer_array'), task.get('url'), 'providers', 'json',
                            #                     myimportlog)
                        if not (res.get('npi', 0) and res.get('npi').isdigit() and plan.get('plan_id', None) and plan['plan_id'] and plan.get('years', None)
                                and (0 < int(res.get('npi', 0)) < 4294967295)):
                            not_good = True
                            break
                        if not (12 < len(plan['plan_id']) <=14):
                            continue


                        for x in plan.get('years', []):
                            if x and (current_year+1 >= int(x) >= current_year):
                                my_years.add(int(x))

                        issuer_id = int(plan['plan_id'][0:5])
                        for year in my_years:
                            checksum_plan = return_checksum([plan['plan_id'], plan['network_tier'], issuer_id, year])
                            checksum_network = return_checksum([plan['network_tier'], issuer_id, year])
                            plan_network_year[checksum_plan] = {
                                'plan_id': plan['plan_id'],
                                'network_tier': plan['network_tier'],
                                'issuer_id': issuer_id,
                                'year': year,
                                'checksum_network': checksum_network
                            }
                            my_network_tiers[checksum_network] = {
                                'network_tier': plan['network_tier'],
                                'issuer_id': issuer_id,
                                'year': year,
                                'checksum_network': checksum_network
                            }
                    if not_good:
                        continue

                    name = res.get('name', {})
                    if not name:
                        name = {}
                    languages = res.get('languages', [])
                    if not languages:
                        languages = []
                    addresses = res.get('addresses', [])
                    if not addresses:
                        addresses = []

                    obj = {
                        'npi': int(res['npi']),
                        'network_tier': '',
                        'checksum_network': '',
                        'year': 0,
                        'issuer_id': 0,
                        'name_or_facility_name': '',
                        'specialty_or_facility_type': [],
                        'type': str(res.get('type', '')),
                        'prefix': name.get('prefix', None),
                        'first_name': name.get('first', None),
                        'middle_name': name.get('middle', None),
                        'last_name': name.get('last', None),
                        'suffix': name.get('suffix', None),
                        'addresses': [json.dumps(x) for x in addresses],
                        'accepting': res.get('accepting', None),
                        'gender': res.get('gender', None),
                        'languages': [str(x) for x in languages],
                        'last_updated_on': datetime.datetime.combine(
                            parse_date(res['last_updated_on'], fuzzy=True), datetime.datetime.min.time())
                    }

                    if ('facility_name' in res) and res.get('facility_name', None) and res.get('facility_name',
                                                                                               '').strip():
                        # for k in (
                        #         'facility_name', 'facility_type'):
                        #     if not (k in res and res[k] is not None):
                        #         await log_error('err',
                        #                         f"Mandatory field `{k}` for providers data is not present or "
                        #                         f"incorrect. Plan ID: "
                        #                         f"{plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                         task.get('issuer_array'), task.get('url'), 'providers', 'json',
                        #                         myimportlog)

                        obj['name_or_facility_name'] = str(res.get('facility_name', '').strip())
                        obj['specialty_or_facility_type'] = [str(x) for x in res.get('facility_type', [])]
                    else:
                        # for k in (
                        #         'name', 'first', 'last', 'speciality', 'accepting'):
                        #     if not (k in res and res[k] is not None):
                        #         await log_error('err',
                        #                         f"Mandatory field `{k}` for providers data is not present or "
                        #                         f"incorrect. Plan ID: "
                        #                         f"{plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                         task.get('issuer_array'), task.get('url'), 'providers', 'json',
                        #                         myimportlog)

                        obj['name_or_facility_name'] = ''
                        for k in ('prefix', 'first', 'middle', 'last', 'suffix'):
                            if (k in name) and (name.get(k, None)):
                                obj['name_or_facility_name'] += f"{name.get(k, '').strip()} "
                        obj['name_or_facility_name'] = obj['name_or_facility_name'].strip()
                        obj['specialty_or_facility_type'] = [str(x) for x in res.get('specialty', [])]


                    for x in my_network_tiers.values():
                        obj['network_tier'] = x['network_tier']
                        obj['checksum_network'] = x['checksum_network']
                        obj['issuer_id'] = x['issuer_id']
                        obj['year'] = x['year']
                        plan_npi_obj_dict['_'.join([str(obj['npi']), str(x['checksum_network'])])] = obj

                        # if count > 10 * int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 50)):
                        #     await push_objects(list(plan_npi_obj_dict.values()), myplan_npi)
                        #     plan_npi_obj_dict = {}
                        #     count = 0
                        # else:
                        #     count += 1
                        #     # except Exception as e:
                        #     #     print(repr(e))
                        #     #     # print('res: ', res)
                        #     #     # print('plan: ', plan)
                        #     #     print('WTF>', obj)
                        #     #     pass
                await push_objects(list(plan_npi_obj_dict.values()), myplan_npi)
                await push_objects(list(plan_network_year.values()), myplan_networktier)


            except ijson.JSONError as exc:
                await log_error('err',
                                f"JSON Parsing Error: {exc}",
                                task.get('issuer_array'), task.get('url'), 'providers', 'json', myimportlog)
                return
            except ijson.IncompleteJSONError as exc:
                await log_error('err',
                                f"Incomplete JSON: can't read expected data. {exc}",
                                task.get('issuer_array'), task.get('url'), 'providers', 'json', myimportlog)
                return
    await flush_error_log(myimportlog)
    return 1


async def process_json_index(ctx, task):
    """
    The process_json_index function is called by the process_index function.
    It downloads a JSON file containing URLs to other files, and then queues up jobs for those files.
    The JSON file contains two arrays: plan_urls and provider_urls.  The plan URLs are queued as 'process_plan' jobs,
    and the provider URLs are queued as 'process_provider' jobs.

    :param ctx: Pass the redis connection to the function
    :param task: Pass the url to download and the issuer_array
    :return: A list of urls to the plan and provider json files
    """
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

        async with async_open(tmp_filename, 'rb') as afp:
            try:
                async for url in ijson.items(afp, "provider_urls.item",
                                             use_float=True):  # , 'formulary_urls', 'provider_urls'
                    print(f"Provider URL: {url}")
                    await redis.enqueue_job('process_provider', {'url': url, 'issuer_array': issuer_array})
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


async def import_unknown_state_issuers_data():
    plan_list = {}
    issuer_list = {}

    attribute_files = json.loads(os.environ['HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF'])
    for file in attribute_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            p = 'attr.csv'
            tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
            await download_it_and_save(file['url'], tmp_filename)
            await unzip(tmp_filename, tmpdirname)

            tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]

            count = 0
            # return 1

            async with async_open(tmp_filename, 'r', encoding='utf-8-sig') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not (row['StandardComponentId'] and row['PlanId']):
                        continue
                    for key in row:
                        if not ((key in ('StandardComponentId',)) and (row[key] is None)) and not (
                                f"{row['StandardComponentId']}_{row['BusinessYear']}" in plan_list):
                            plan_list[f"{row['StandardComponentId']}_{row['BusinessYear']}"] = {
                                'plan_id': row['StandardComponentId'],
                                'plan_id_type': 'CMS-HIOS-PLAN-ID',
                                'year': int(row['BusinessYear']),
                                'issuer_id': int(row['IssuerId']),
                                'state': str(row['StateCode']).upper(),
                                'marketing_name': row['PlanMarketingName'],
                                'summary_url': row['URLForSummaryofBenefitsCoverage'],
                                'marketing_url': row['PlanBrochure'],
                                'formulary_url': row['FormularyURL'],
                                'plan_contact': '',
                                'network': [row['NetworkId']],
                                'benefits': [],
                                'last_updated_on': datetime.datetime.combine(
                                    parse_date(row['ImportDate'], fuzzy=True), datetime.datetime.min.time()),
                                'checksum': return_checksum(
                                    [row['StandardComponentId'].lower(), int(row['BusinessYear'])], crc=32)
                            }

                            issuer_list[int(row['IssuerId'])] = {
                                'state': str(row['StateCode']).upper(),
                                'issuer_id': int(row['IssuerId']),
                                'mrf_url': '',
                                'issuer_name': row['IssuerMarketPlaceMarketingName'].strip() if row[
                                    'IssuerMarketPlaceMarketingName'].strip() else row['IssuerId']
                            }
                        # except:
                        #     from pprint import pprint
                        #     pprint(row)

    state_attribute_files = json.loads(os.environ['HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF'])
    for file in state_attribute_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            p = 'attr.csv'
            tmp_filename = str(PurePath(str(tmpdirname), p + '.zip'))
            await download_it_and_save(file['url'], tmp_filename)
            await unzip(tmp_filename, tmpdirname)

            tmp_filename = glob.glob(f"{tmpdirname}/*Plans*.csv")[0]


            count = 0
            # return 1

            async with async_open(tmp_filename, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not (row['STANDARD COMPONENT ID'] and row['PLAN ID']):
                        continue
                    for key in row:
                        if not ((key in ('StandardComponentId',)) and (row[key] is None)) and not (
                                f"{row['STANDARD COMPONENT ID']}_{row['BUSINESS YEAR']}" in plan_list):
                            plan_list[f"{row['STANDARD COMPONENT ID'].upper()}_{row['BUSINESS YEAR']}"] = {
                                'plan_id': row['STANDARD COMPONENT ID'],
                                'plan_id_type': 'STATE-HIOS-PLAN-ID',
                                'year': int(row['BUSINESS YEAR']),
                                'issuer_id': int(row['ISSUER ID']),
                                'state': str(row['STATE CODE']).upper(),
                                'marketing_name': row['PLAN MARKETING NAME'],
                                'summary_url': row['URL FOR SUMMARY OF BENEFITS COVERAGE'],
                                'marketing_url': row['PLAN BROCHURE'],
                                'formulary_url': row['FORMULARY URL'],
                                'plan_contact': '',
                                'network': [row['NETWORK ID']],
                                'benefits': [],
                                'last_updated_on': datetime.datetime.combine(
                                    parse_date(row['IMPORT DATE'], fuzzy=True), datetime.datetime.min.time()),
                                'checksum': return_checksum(
                                    [row['STANDARD COMPONENT ID'].lower(), int(row['BUSINESS YEAR'])], crc=32)
                            }

                            issuer_list[int(row['ISSUER ID'])] = {
                                'state': str(row['STATE CODE']).upper(),
                                'issuer_id': int(row['ISSUER ID']),
                                'mrf_url': '',
                                'issuer_name': row['ISSUER NAME'].strip() if row['ISSUER NAME'].strip() else row[
                                    'ISSUER ID']
                            }

    return (issuer_list, plan_list)


async def init_file(ctx):
    """
    The init_file function is the first function called in this file.
    It downloads a zip file from the CMS website, unzips it, and then parses through each worksheet to create an object for each row of data.
    The objects are then pushed into a database using GINO ORM.

    :param ctx: Pass information between functions
    :return: The following:

    """
    redis = ctx['redis']

    print('Downloading data from: ', os.environ['HLTHPRT_CMSGOV_MRF_URL_PUF'])

    import_date = ctx['import_date']
    ctx['context']['run'] += 1
    myissuer = make_class(Issuer, import_date)
    myplan = make_class(Plan, import_date)
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

        (issuer_list, plan_list) = await import_unknown_state_issuers_data()
        await asyncio.gather(push_objects(list(issuer_list.values()), myissuer),
                             push_objects(list(plan_list.values()), myplan))

        for url in url_list:
            await redis.enqueue_job('process_json_index', {'url': url, 'issuer_array': url2issuer[url]})
            # break


async def startup(ctx):
    """
    The startup function is called once at the beginning of a run.
    It can be used to initialize resources that will be needed by tasks, such as database connections.
    The startup function receives one argument: a dictionary containing the context for this run.

    :param ctx: Pass data between the functions
    :return: A dictionary of context variables

    """
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
    tables = {}  # for the future complex usage
    for cls in (Issuer, Plan, PlanFormulary, PlanTransparency, ImportLog, PlanNPIRaw, PlanNetworkTierRaw):
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
    """
    The shutdown function is called after the import process has completed.
    It should be used to clean up any temporary tables or files that were created during the import process.


    :param ctx: Pass the context of the import process to other functions
    :return: A coroutine
    """
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
        for cls in (Issuer, Plan, PlanFormulary, PlanTransparency, ImportLog, PlanNPIRaw, PlanNetworkTierRaw):
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
    """
    The main function is the entry point of the application.

    :return: A coroutine
    """
    redis = await create_pool(RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS')),
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    x = await redis.enqueue_job('init_file')
