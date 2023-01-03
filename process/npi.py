import os
import msgpack
import asyncio
import datetime
import pytz
import tempfile
import json
from dateutil.parser import parse as parse_date
import glob
import re
from arq import create_pool
from arq.connections import RedisSettings
from pathlib import Path, PurePath
from aiocsv import AsyncDictReader, AsyncReader
import csv


import pylightxl as xl
from aiofile import async_open
from async_unzip.unzipper import unzip

from process.ext.utils import return_checksum, download_it, download_it_and_save, download_it_and_save_nostream, \
    make_class, push_objects, log_error, print_time_info, \
    flush_error_log

from db.models import Issuer, NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, db
from db.connection import init_db

latin_pattern= re.compile(r'[^\x00-\x7f]')


async def process_npi_chunk(ctx, task):
    import_date = ctx['import_date']
    redis = ctx['redis']

    npi_obj_list = []
    npi_taxonomy_list_dict = {}
    npi_other_id_list_dict = {}
    npi_taxonomy_group_list_dict = {}

    npi_csv_map = task['npi_csv_map']
    npi_csv_map_reverse = task['npi_csv_map_reverse']
    count = 0

    for row in task['row_list']:
        obj = {}

        for key in npi_csv_map:
            t = row[key]
            if not t:
                obj[npi_csv_map[key]] = None
                continue
            if npi_csv_map[key] in ('replacement_npi', 'entity_type_code', 'npi',):
                t = int(t)
            elif npi_csv_map[key].endswith('_date'):
                t = pytz.utc.localize(parse_date(t, fuzzy=True))

            obj[npi_csv_map[key]] = t
        # obj = {'npi': int(row['NPI']),
        #     'entity_type_code': int(row['Entity Type Code']) if row['Entity Type Code'] else None,
        #     'replacement_npi': int(row['Replacement NPI']) if row['Replacement NPI'] else None,
        #     'employer_identification_number': row['Employer Identification Number (EIN)'],
        #     'provider_organization_name': row['Provider Organization Name (Legal Business Name)'],
        #     'provider_last_name': row['Provider Last Name (Legal Name)'],
        #     'provider_first_name': row['Provider First Name'],
        #     'provider_middle_name': row['Provider Middle Name'],
        #     'provider_name_prefix_text': row['Provider Name Prefix Text'],
        #     'provider_name_suffix_text': row['Provider Name Suffix Text'],
        #     'provider_credential_text': row['Provider Credential Text'],
        #     'provider_other_organization_name': row['Provider Other Organization Name'],
        #     'provider_other_organization_name_type_code': row['Provider Other Organization Name Type Code'],
        #     'provider_other_last_name': row['Provider Other Last Name'],
        #     'provider_other_first_name': row['Provider Other First Name'],
        #     'provider_other_middle_name': row['Provider Other Middle Name'],
        #     'provider_other_name_prefix_text': row['Provider Other Name Prefix Text'],
        #     'provider_other_name_suffix_text': row['Provider Other Name Suffix Text'],
        #     'provider_other_credential_text': row['Provider Other Credential Text'],
        #     'provider_other_last_name_type_code': row['Provider Other Last Name Type Code'],
        #     'provider_first_line_business_mailing_address': row['Provider First Line Business Mailing Address'],
        #     'provider_second_line_business_mailing_address': row['Provider Second Line Business Mailing Address'],
        #     'provider_business_mailing_address_city_name': row['Provider Business Mailing Address City Name'],
        #     'provider_business_mailing_address_state_name': row['Provider Business Mailing Address State Name'],
        #     'provider_business_mailing_address_postal_code': row['Provider Business Mailing Address Postal Code'],
        #     'provider_business_mailing_address_country_code': row[
        #         'Provider Business Mailing Address Country Code (If outside U.S.)'],
        #     'provider_business_mailing_address_telephone_number': row[
        #         'Provider Business Mailing Address Telephone Number'],
        #     'provider_business_mailing_address_fax_number': row['Provider Business Mailing Address Fax Number'],
        #     'provider_first_line_business_practice_location_address': row[
        #         'Provider First Line Business Practice Location Address'],
        #     'provider_second_line_business_practice_location_address': row[
        #         'Provider Second Line Business Practice Location Address'],
        #     'provider_business_practice_location_address_city_name': row[
        #         'Provider Business Practice Location Address City Name'],
        #     'provider_business_practice_location_address_state_name': row[
        #         'Provider Business Practice Location Address State Name'],
        #     'provider_business_practice_location_address_postal_code': row[
        #         'Provider Business Practice Location Address Postal Code'],
        #     'provider_business_practice_location_address_country_code': row[
        #         'Provider Business Practice Location Address Country Code (If outside U.S.)'],
        #     'provider_business_practice_location_address_telephone_number': row[
        #         'Provider Business Practice Location Address Telephone Number'],
        #     'provider_business_practice_location_address_fax_number': row[
        #         'Provider Business Practice Location Address Fax Number'],
        #     'provider_enumeration_date': pytz.utc.localize(parse_date(row['Provider Enumeration Date'], fuzzy=True)) if
        #     row['Provider Enumeration Date'] else None,
        #     'last_update_date': pytz.utc.localize(parse_date(row['Last Update Date'], fuzzy=True)) if row[
        #         'Last Update Date'] else None,
        #     'npi_deactivation_reason_code': row['NPI Deactivation Reason Code'],
        #     'npi_deactivation_date': pytz.utc.localize(parse_date(row['NPI Deactivation Date'], fuzzy=True)) if row[
        #         'NPI Deactivation Date'] else None,
        #     'npi_reactivation_date': pytz.utc.localize(parse_date(row['NPI Reactivation Date'], fuzzy=True)) if row[
        #         'NPI Reactivation Date'] else None,
        #     'provider_gender_code': row['Provider Gender Code'],
        #     'authorized_official_last_name': row['Authorized Official Last Name'],
        #     'authorized_official_first_name': row['Authorized Official First Name'],
        #     'authorized_official_middle_name': row['Authorized Official Middle Name'],
        #     'authorized_official_title_or_position': row['Authorized Official Title or Position'],
        #     'authorized_official_telephone_number': row['Authorized Official Telephone Number'],
        #     'is_sole_proprietor': row['Is Sole Proprietor'],
        #     'is_organization_subpart': row['Is Organization Subpart'],
        #     'parent_organization_lbn': row['Parent Organization LBN'],
        #     'parent_organization_tin': row['Parent Organization TIN'],
        #     'authorized_official_name_prefix_text': row['Authorized Official Name Prefix Text'],
        #     'authorized_official_name_suffix_text': row['Authorized Official Name Suffix Text'],
        #     'authorized_official_credential_text': row['Authorized Official Credential Text'],
        #     'certification_date': pytz.utc.localize(parse_date(row['Certification Date'], fuzzy=True)) if row[
        #         'Certification Date'] else None}
        npi_obj_list.append(obj)

        for i in range(1, 16):
            if row[f'Healthcare Provider Taxonomy Code_{i}']:
                t = {
                    'npi': int(row[npi_csv_map_reverse['npi']]),
                    'healthcare_provider_taxonomy_code': row[f'Healthcare Provider Taxonomy Code_{i}'],
                    'provider_license_number': row[f'Provider License Number_{i}'],
                    'provider_license_number_state_code': row[f'Provider License Number State Code_{i}'],
                    'healthcare_provider_primary_taxonomy_switch': row[
                        f'Healthcare Provider Primary Taxonomy Switch_{i}']
                }
                checksum = return_checksum(list(t.values()))
                t['checksum'] = checksum
                npi_taxonomy_list_dict[checksum] = t
            else:
                break

        for i in range(1, 51):
            if row[f'Other Provider Identifier_{i}']:
                t = {
                    'npi': int(row[npi_csv_map_reverse['npi']]),
                    'other_provider_identifier': row[f'Other Provider Identifier_{i}'],
                    'other_provider_identifier_type_code': row[f'Other Provider Identifier Type Code_{i}'],
                    'other_provider_identifier_state': row[f'Other Provider Identifier State_{i}'],
                    'other_provider_identifier_issuer': row[f'Other Provider Identifier Issuer_{i}']
                }
                checksum = return_checksum(list(t.values()))
                t['checksum'] = checksum
                npi_other_id_list_dict[checksum] = t
            else:
                break

        for i in range(1, 16):
            if row[f'Healthcare Provider Taxonomy Group_{i}']:
                t = {
                    'npi': int(row[npi_csv_map_reverse['npi']]),
                    'healthcare_provider_taxonomy_group': row[f'Healthcare Provider Taxonomy Group_{i}'],
                }
                checksum = return_checksum(list(t.values()))
                t['checksum'] = checksum
                npi_taxonomy_group_list_dict[checksum] = t
            else:
                break

        # task = {}
        # insert_limit = 10000
        # if len(npi_obj_list) > insert_limit:
        #     # int(os.environ.get('HLTHPRT_SAVE_PER_PACK', 100)):
        #     task['npi_obj_list'] = npi_obj_list
        # if len(npi_taxonomy_list) > insert_limit:
        #     task['npi_taxonomy_list'] = npi_taxonomy_list
        # if len(npi_other_id_list) > insert_limit:
        #     task['npi_other_id_list'] = npi_other_id_list
        # if len(npi_taxonomy_group_list) > insert_limit:
        #     task['npi_taxonomy_group_list'] = npi_taxonomy_group_list
        #
        # if task:
        #     await redis.enqueue_job('save_npi_data', task)
        #     for key in task:
        #         if key == 'npi_obj_list':
        #             npi_obj_list.clear()
        #         elif key == 'npi_taxonomy_list':
        #             npi_taxonomy_list.clear()
        #         elif key == 'npi_other_id_list':
        #             npi_other_id_list.clear()
        #         elif key == 'npi_taxonomy_group_list':
        #             npi_taxonomy_group_list.clear()
        #         else:
        #             print('Some wrong key passed')

    await redis.enqueue_job('save_npi_data', {
        'npi_obj_list': npi_obj_list,
        'npi_taxonomy_list': list(npi_taxonomy_list_dict.values()),
        'npi_other_id_list': list(npi_other_id_list_dict.values()),
        'npi_taxonomy_group_list': list(npi_taxonomy_group_list_dict.values())
    })



async def process_data(ctx):
    import_date = ctx['import_date']
    redis = ctx['redis']
    html_source = await download_it(
        os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    # re./NPPES_Data_Dissemination_110722_111322_Weekly.zip">NPPES Data Dissemination - Weekly Update -
    # 110722_111322</a>

    for p in re.findall(r'(NPPES_Data_Dissemination.*.zip)', html_source.text):
        with tempfile.TemporaryDirectory() as tmpdirname:
            print(f"Found: {p}")
            #await unzip('/users/nick/downloads/NPPES_Data_Dissemination_November_2022.zip', tmpdirname, __debug=True)

            tmp_filename = str(PurePath(str(tmpdirname), p))
            await download_it_and_save(os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + p, tmp_filename,
                                       chunk_size=10 * 1024 * 1024, cache_dir='/tmp')
            print(f"Downloaded: {p}")
            await unzip(tmp_filename, tmpdirname, buffer_size= 10 * 1024 * 1024)

            npi_file = [fn for fn in glob.glob(f"{tmpdirname}/npi*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            pl_file = [fn for fn in glob.glob(f"{tmpdirname}/pl_pfile*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            other_file = [fn for fn in glob.glob(f"{tmpdirname}/other*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            endpoint_file = [fn for fn in glob.glob(f"{tmpdirname}/endpoint*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            for t in (endpoint_file, other_file, pl_file, npi_file):
                print(f"Files: {t}")


            npi_csv_map = {}
            npi_csv_map_reverse = {}

            int_key_re = re.compile(r'.*_\d+$')
            # count = 0
            # now = datetime.datetime.now()
            # async with async_open(npi_file, 'r') as afp:
            #     async for row in AsyncDictReader(afp, delimiter=","):
            #         count += 1
            #         if not count % 100_000:
            #             print(f"Processed: {count}")
            #         pass
            # now2 = datetime.datetime.now()
            # print(f"Processed: {count}")
            # print('Time Delta: ', now2-now)
            # exit(1)
            async with async_open(npi_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    for key in row:
                        if int_key_re.match(key):
                            continue
                        t = re.sub(r"\(.*\)", r"", key.lower()).strip().replace(' ', '_')
                        npi_csv_map[key] = t
                        npi_csv_map_reverse[t] = key
                    break

            # for key in npi_csv_map_reverse:
            #     print(f"'{key}': row['{npi_csv_map_reverse[key]}'],")
            # exit(1)
            count = 0
            total_count = 0


            row_list = []
            async with async_open(npi_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not (row['NPI']):
                        continue
                    count += 1
                    if not count % 100_000:
                        print(f"Processed: {count}")
                    row_list.append(row)
                    if count > 9999:
                        await process_npi_chunk(ctx, {'row_list': row_list,
                            'npi_csv_map': npi_csv_map,
                            'npi_csv_map_reverse': npi_csv_map_reverse})
                        # await redis.enqueue_job('process_npi_chunk', {'row_list': row_list,
                        #     'npi_csv_map': npi_csv_map,
                        #     'npi_csv_map_reverse': npi_csv_map_reverse})
                        row_list.clear()
                        count = 0
                    else:
                        count += 1

            print(f"Processed: {count}")


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

    for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy,):
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
    async with db.transaction():
        for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, ):
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

    print_time_info(ctx['context']['start'])


async def save_npi_data(ctx, task):
    import_date = ctx['import_date']
    x = []
    for key in task:
        if key == 'npi_obj_list':
            mynpidata = make_class(NPIData, import_date)
            x.append(push_objects(task['npi_obj_list'], mynpidata, rewrite=True))
        elif key == 'npi_taxonomy_list':
            mynpidatataxonomy = make_class(NPIDataTaxonomy, import_date)
            x.append(push_objects(task['npi_taxonomy_list'], mynpidatataxonomy, rewrite=True))
        elif key == 'npi_other_id_list':
            mynpidataotheridentifier = make_class(NPIDataOtherIdentifier, import_date)
            x.append(push_objects(task['npi_other_id_list'], mynpidataotheridentifier, rewrite=True))
        elif key == 'npi_taxonomy_group_list':
            mynpidatataxonomygroup = make_class(NPIDataTaxonomyGroup, import_date)
            x.append(push_objects(task['npi_taxonomy_group_list'], mynpidatataxonomygroup, rewrite=True))
        else:
            print('Some wrong key passed')
    await asyncio.gather(*x)


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

                if count > 100:
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

async def main():
    redis = await create_pool(RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS')),
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    x = await redis.enqueue_job('process_data')
