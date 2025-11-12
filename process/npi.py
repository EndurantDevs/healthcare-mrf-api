# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
import asyncio
import datetime
import pytz
import tempfile
import json
from dateutil.parser import parse as parse_date
import glob
import re
import zipfile
from arq import create_pool
from pathlib import Path, PurePath
from aiocsv import AsyncDictReader, AsyncReader
from asyncpg import DuplicateTableError
from sqlalchemy import select, func
import csv


import pylightxl as xl
from aiofile import async_open
from async_unzip.unzipper import unzip

from process.ext.utils import return_checksum, download_it, download_it_and_save, download_it_and_save_nostream, \
    make_class, push_objects, log_error, print_time_info, \
    flush_error_log, my_init_db

from db.models import AddressArchive, NPIAddress, NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, \
    NPIDataTaxonomy, db
from db.connection import init_db
from process.serialization import serialize_job, deserialize_job
from process.redis_config import build_redis_settings

latin_pattern= re.compile(r'[^\x00-\x7f]')

internal_tasks = set()

TEST_NPI_MAX_FILES = 1
TEST_NPI_ROWS = 1000
TEST_NPI_OTHER_ROWS = 500


def is_test_mode(ctx: dict) -> bool:
    return bool(ctx.get("context", {}).get("test_mode"))

async def process_npi_chunk(ctx, task):
    import_date = ctx['import_date']
    redis = ctx['redis']

    npi_obj_list = []
    npi_taxonomy_list_dict = {}
    npi_other_id_list_dict = {}
    npi_taxonomy_group_list_dict = {}
    npi_address_list_dict = {}

    npi_csv_map = task['npi_csv_map']
    npi_csv_map_reverse = task['npi_csv_map_reverse']
    count = 0

    for row in task['row_list']:
        obj = {}

        for key in npi_csv_map:
            t = row[key]
            if not t or str(t).upper() == '<UNAVAIL>':
                obj[npi_csv_map[key]] = None
                continue
            if npi_csv_map[key] in ('replacement_npi', 'entity_type_code', 'npi',):
                t = int(t)
            elif npi_csv_map[key].endswith('_date'):
                t = pytz.utc.localize(parse_date(t, fuzzy=True))

            obj[npi_csv_map[key]] = t

        npi_obj_list.append(obj)

        if (row['Provider First Line Business Practice Location Address']):
            obj = {
                'first_line': row['Provider First Line Business Practice Location Address'],
                'second_line': row['Provider Second Line Business Practice Location Address'],
                'city_name': row.get('Provider Business Practice Location Address City Name', '').upper(),
                'state_name': row.get('Provider Business Practice Location Address State Name', '').upper(),
                'postal_code': row['Provider Business Practice Location Address Postal Code'],
                'country_code': row['Provider Business Practice Location Address Country Code (If outside U.S.)'],
            }

            obj.update({
                'checksum': return_checksum(list(obj.values())), #addresses have blank symbols
                'npi': int(row['NPI']),
                'type': 'primary',
                'telephone_number': row['Provider Business Practice Location Address Telephone Number'],
                'fax_number': row['Provider Business Practice Location Address Fax Number'],
                'date_added':  pytz.utc.localize(parse_date(row['Last Update Date'], fuzzy=True)) if row[
                'Last Update Date'] else None
            })
            npi_address_list_dict['_'.join([str(obj['npi']), str(obj['checksum']), obj['type'],])] = obj

        if (row['Provider First Line Business Mailing Address']):
            obj = {
                'first_line': row['Provider First Line Business Mailing Address'],
                'second_line': row['Provider Second Line Business Mailing Address'],
                'city_name': row.get('Provider Business Mailing Address City Name', '').upper(),
                'state_name': row.get('Provider Business Mailing Address State Name', '').upper(),
                'postal_code': row['Provider Business Mailing Address Postal Code'],
                'country_code': row['Provider Business Mailing Address Country Code (If outside U.S.)'],
            }

            obj.update({
                'checksum': return_checksum(list(obj.values())), # addresses have blank symbols
                'npi': int(row['NPI']),
                'type': 'mail',
                'telephone_number': row['Provider Business Mailing Address Telephone Number'],
                'fax_number': row['Provider Business Mailing Address Fax Number'],
                'date_added': pytz.utc.localize(parse_date(row['Last Update Date'], fuzzy=True)) if row[
                    'Last Update Date'] else None
            })
            
            npi_address_list_dict['_'.join([str(obj['npi']), str(obj['checksum']), obj['type'],])] = obj

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

    t = {
            'npi_obj_list': npi_obj_list,
            'npi_taxonomy_list': list(npi_taxonomy_list_dict.values()),
            'npi_other_id_list': list(npi_other_id_list_dict.values()),
            'npi_taxonomy_group_list': list(npi_taxonomy_group_list_dict.values()),
            'npi_address_list': list(npi_address_list_dict.values()),
        }
    if task.get('direct'):
        task = asyncio.create_task(save_npi_data(ctx, t))
        internal_tasks.add(task)
        task.add_done_callback(internal_tasks.discard)
        print(f'Processing.. {len(npi_obj_list)} rows directly')
    else:
        await redis.enqueue_job('save_npi_data', t)



async def process_data(ctx, task=None):  # pragma: no cover
    # Track whether any work actually ran so shutdown can distinguish "no jobs" from a bad import
    task = task or {}
    ctx.setdefault('context', {})
    if 'test_mode' in task:
        ctx['context']['test_mode'] = bool(task.get('test_mode'))
    test_mode = bool(ctx['context'].get('test_mode', False))

    ctx['context']['run'] = ctx['context'].get('run', 0) + 1

    import_date = ctx['import_date']
    redis = ctx['redis']
    print(os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    html_source = await download_it(
        os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    # re./NPPES_Data_Dissemination_110722_111322_Weekly.zip">NPPES Data Dissemination - Weekly Update -
    # 110722_111322</a>
    count_files = 0
    SQL_CHUNK_SIZE = 299999
    file_limit = TEST_NPI_MAX_FILES if test_mode else None
    for file_idx, p in enumerate(re.findall(r'(NPPES_Data_Dissemination.*_V2.zip)', html_source)):
        if file_limit and file_idx >= file_limit:
            break
        count_files = count_files + 1
        current_sql_chunk_size = SQL_CHUNK_SIZE
        if test_mode:
            current_sql_chunk_size = min(current_sql_chunk_size, TEST_NPI_ROWS)
        print(f"Round {count_files} for {p}")
        with tempfile.TemporaryDirectory() as tmpdirname:
            print(f"Found: {p}")
            #await unzip('/users/nick/downloads/NPPES_Data_Dissemination_November_2022.zip', tmpdirname, __debug=True)

            tmp_filename = str(PurePath(str(tmpdirname), p))
            await download_it_and_save(os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + p, tmp_filename,
                                       chunk_size=10 * 1024 * 1024, cache_dir='/tmp')
            print(f"Downloaded: {p}")
            
            if os.environ.get("DEBUG"):
                print(f"DEBUG: Downloaded file {tmp_filename}, size: {os.path.getsize(tmp_filename)} bytes")
                if os.path.getsize(tmp_filename) > 100 * 1024 * 1024:
                    print(f"File {tmp_filename} is too big, skipping")
                    continue
            else:
                print(f"Downloaded file size: {os.path.getsize(tmp_filename)} bytes")

            try:
                await unzip(tmp_filename, tmpdirname, buffer_size= 10 * 1024 * 1024)
            except:
                print(f"Failed to unzip {tmp_filename}, trying with zipfile")
                with zipfile.ZipFile(tmp_filename, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)

            npi_file = [fn for fn in glob.glob(f"{tmpdirname}/npi*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            pl_file = [fn for fn in glob.glob(f"{tmpdirname}/pl_pfile*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            other_file = [fn for fn in glob.glob(f"{tmpdirname}/other*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            
            if count_files > 1:
                # Collect all NPIs from npi_file and pl_file
                current_sql_chunk_size = SQL_CHUNK_SIZE // 26
                npi_set = set()
                async with async_open(npi_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))
                            
                for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataTaxonomy, NPIAddress):
                    table = make_class(cls, import_date)
                    npi_list = list(npi_set)
                    chunk_size = 1000
                    npi_column = getattr(table, 'npi')
                    for i in range(0, len(npi_list), chunk_size):
                        chunk = npi_list[i:i + chunk_size]
                        delete_stmt = db.delete(table.__table__).where(npi_column.in_(chunk))
                        if cls is NPIAddress:
                            delete_stmt = delete_stmt.where((table.type == 'primary') | (table.type == 'mail'))
                        await delete_stmt.status()
                print(f"Cleaned up models for {len(npi_set)} NPIs due to multiple files.")
                
                npi_set.clear()
                            
                async with async_open(pl_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))
                
                for cls in (NPIAddress,):
                    table = make_class(cls, import_date)
                    npi_list = list(npi_set)
                    chunk_size = 10000
                    npi_column = getattr(table, 'npi')
                    for i in range(0, len(npi_list), chunk_size):
                        chunk = npi_list[i:i + chunk_size] 
                        delete_stmt = db.delete(table.__table__).where(npi_column.in_(chunk))
                        if cls is NPIAddress:
                            delete_stmt = delete_stmt.where(table.type == 'secondary')
                        await delete_stmt.status()
                        
                npi_set.clear()
                        
                async with async_open(other_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))
                
                for cls in (NPIDataOtherIdentifier,):
                    table = make_class(cls, import_date)
                    npi_list = list(npi_set)
                    chunk_size = 10000
                    npi_column = getattr(table, 'npi')
                    for i in range(0, len(npi_list), chunk_size):
                        chunk = npi_list[i:i + chunk_size] 
                        delete_stmt = db.delete(table.__table__).where(npi_column.in_(chunk))
                        await delete_stmt.status()
                        
                print(f"Cleaned up models for {len(npi_set)} NPIs due to multiple files.")

            
            
            
            
            endpoint_file = [fn for fn in glob.glob(f"{tmpdirname}/endpoint*.csv")
                if not os.path.basename(fn).endswith('_fileheader.csv')][0]
            for t in (endpoint_file, other_file, pl_file, npi_file):
                print(f"Files: {t}")


            npi_csv_map = {}
            npi_csv_map_reverse = {}

            int_key_re = re.compile(r'.*_\d+$')

            async with async_open(npi_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    for key in row:
                        if int_key_re.match(key) or ' Address' in key:
                            continue
                        t = re.sub(r"\(.*\)", r"", key.lower()).strip().replace(' ', '_')
                        npi_csv_map[key] = t
                        npi_csv_map_reverse[t] = key
                    break
            count = 0
            total_count = 0


            row_list = []
            coros = []
            processed_rows = 0
            async with async_open(npi_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not (row['NPI']):
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Processed: {count}")
                    row_list.append(row)
                    processed_rows += 1
                    if count > current_sql_chunk_size:
                        print(f"Sending to DB: {count}")
                        coros.append(asyncio.create_task(process_npi_chunk(ctx, {'row_list': row_list.copy(),
                            'npi_csv_map': npi_csv_map,
                            'npi_csv_map_reverse': npi_csv_map_reverse,
                            'direct': True,
                            })))
                        row_list.clear()
                        count = 0
                    else:
                        count += 1
                    if test_mode and processed_rows >= TEST_NPI_ROWS:
                        break

            coros.append(asyncio.create_task(process_npi_chunk(ctx, {'row_list': row_list.copy(),
                'npi_csv_map': npi_csv_map,
                'npi_csv_map_reverse': npi_csv_map_reverse,
                'direct': True,
            })))
            # await asyncio.gather(*coros)
            # while internal_tasks:
            #     print(f"Tasks remaining: {len(internal_tasks)}")
            #     await asyncio.sleep(2)
            # coros.clear()
            row_list.clear()

            npi_other_org_list_dict = {}

            async with async_open(other_file, 'r') as afp:
                processed_other = 0
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row['NPI']:
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Other Names Processed: {count}")
                    obj = {
                        'npi': int(row['NPI']),
                        'other_provider_identifier': row['Provider Other Organization Name'],
                        'other_provider_identifier_type_code': row['Provider Other Organization Name Type Code'],
                        'other_provider_identifier_state': None,
                        'other_provider_identifier_issuer': None,
                    }
                    checksum = return_checksum(list(obj.values()))
                    obj['checksum'] = checksum
                    npi_other_org_list_dict[checksum] = obj

                    if count > current_sql_chunk_size:
                        print(f"Sending to DB: {count}")
                        coros.append(asyncio.create_task(save_npi_data(ctx, {'npi_other_id_list': list(npi_other_org_list_dict.copy().values())})))
                        npi_other_org_list_dict.clear()
                        count = 0
                    else:
                        count += 1
                    processed_other += 1
                    if test_mode and processed_other >= TEST_NPI_OTHER_ROWS:
                        break

            coros.append(asyncio.create_task(save_npi_data(ctx, {'npi_other_id_list': list(npi_other_org_list_dict.copy().values())})))
            #await asyncio.gather(*coros)
            #coros.clear()
            npi_other_org_list_dict.clear()


            npi_address_list_dict = {}
            async with async_open(pl_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not (row['NPI'] or row['Provider Secondary Practice Location Address- Address Line 1']):
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Secondary Addresses Processed: {count}")
                    obj = {
                        'first_line': row['Provider Secondary Practice Location Address- Address Line 1'],
                        'second_line': row['Provider Secondary Practice Location Address-  Address Line 2'],
                        'city_name': row.get('Provider Secondary Practice Location Address - City Name', '').upper(),
                        'state_name': row.get('Provider Secondary Practice Location Address - State Name', '').upper(),
                        'postal_code': row['Provider Secondary Practice Location Address - Postal Code'],
                        'country_code': row['Provider Secondary Practice Location Address - Country Code (If outside U.S.)'],
                    }

                    obj.update({
                        'checksum': return_checksum(list(obj.values())),  # addresses have blank symbols
                        'npi': int(row['NPI']),
                        'type': 'secondary',
                        'telephone_number': row['Provider Secondary Practice Location Address - Telephone Number'],
                        'fax_number': row['Provider Practice Location Address - Fax Number'],
                        'date_added': pytz.utc.localize(datetime.datetime.now())
                    })
                    npi_address_list_dict['_'.join([str(obj['npi']), str(obj['checksum']), obj['type'], ])] = obj

                    if count > current_sql_chunk_size:
                        print(f"Sending Secondary to DB: {count}")
                        coros.append(asyncio.create_task(save_npi_data(ctx, {'npi_address_list': list(npi_address_list_dict.copy().values())})))
                        # await redis.enqueue_job('save_npi_data', {
                        #     'npi_address_list': list(npi_address_list_dict.values()),
                        # })
                        npi_address_list_dict.clear()
                        count = 0
                    else:
                        count += 1
            # await redis.enqueue_job('save_npi_data', {
            #     'npi_address_list': list(npi_address_list_dict.values()),
            # })
            coros.append(asyncio.create_task(save_npi_data(ctx, {'npi_address_list': list(npi_address_list_dict.copy().values())})))
            await asyncio.gather(*coros)
            npi_address_list_dict.clear()

            print(f"Processed: {count}")


async def startup(ctx):  # pragma: no cover
    await my_init_db(db)
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.utcnow()
    ctx['context']['run'] = 0
    ctx['context']['test_mode'] = False
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    tables = {}  # for the future complex usage

    try:
        obj = AddressArchive
        await db.create_table(AddressArchive.__table__, checkfirst=True)
        if hasattr(AddressArchive, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")
    except DuplicateTableError:
        pass

    for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIAddress):
        tables[cls.__main_table__] = make_class(cls, import_date)
        obj = tables[cls.__main_table__]
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__main_table__}_{import_date};")
        await db.create_table(obj.__table__, checkfirst=True)
        if hasattr(obj, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")

        if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
            for index in cls.__my_initial_indexes__:
                index_name = index.get("name", "_".join(index.get("index_elements")))
                using = ""
                if t := index.get("using"):
                    using = f"USING {t} "

                unique = ' '
                if index.get('unique'):
                    unique = ' UNIQUE '
                where = ''
                if index.get('where'):
                    where = f' WHERE {index.get("where")} '
                create_index_sql = (
                    f"CREATE{unique}INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} "
                    f"ON {db_schema}.{obj.__tablename__}  {using}"
                    f"({', '.join(index.get('index_elements'))}){where};"
                )
                print(create_index_sql)
                x = await db.status(create_index_sql)

    print("Preparing done")
    

async def refresh_do_business_as(target_table: str | None = None):
    """
    Populate the NPI.do_business_as array from other identifier entries (type code 3).
    """
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    table = target_table or NPIData.__tablename__

    reset_sql = (
        f"UPDATE {db_schema}.{table} "
        f"SET do_business_as = ARRAY[]::varchar[], do_business_as_text = ''"
    )
    await db.status(reset_sql)

    update_sql = f"""
        WITH sub AS (
            SELECT
                npi,
                ARRAY_AGG(DISTINCT other_provider_identifier ORDER BY other_provider_identifier) AS names,
                STRING_AGG(DISTINCT other_provider_identifier, ' ' ORDER BY other_provider_identifier) AS search_text
            FROM {db_schema}.npi_other_identifier
            WHERE other_provider_identifier_type_code = '3'
            GROUP BY npi
        )
        UPDATE {db_schema}.{table} AS n
        SET
            do_business_as = sub.names,
            do_business_as_text = COALESCE(sub.search_text, '')
        FROM sub
        WHERE n.npi = sub.npi;
    """
    await db.status(update_sql)




async def shutdown(ctx):  # pragma: no cover
    import_date = ctx['import_date']
    context = ctx.get('context') or {}
    if not context.get('run'):
        print("No NPI jobs ran in this worker session; skipping shutdown validation.")
        return

    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    tables = {}

    test = make_class(NPIAddress, import_date)
    npi_address_count = await db.scalar(select(func.count(test.npi)))
    if context.get("test_mode"):
        print(f"Test mode: imported {npi_address_count} NPI addresses (no minimum enforced).")
    else:
        if (not npi_address_count) or (npi_address_count < 5000000):
            print(f"Failed Import: Address number:{npi_address_count}")
            exit(1)

    processing_classes_array = (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIAddress,)
    async with db.transaction() as tx:
        for cls in processing_classes_array:
            tables[cls.__main_table__] = make_class(cls, import_date)
            obj = tables[cls.__main_table__]
            if cls is NPIDataOtherIdentifier:
                print('Updating NPI do_business_as arrays from other identifiers...')
                target_npi_cls = tables.get(NPIData.__main_table__)
                target_table_name = target_npi_cls.__tablename__ if target_npi_cls else NPIData.__tablename__
                await refresh_do_business_as(target_table=target_table_name)
            if cls is NPIAddress:
                print("Updating NUCC Taxonomy for NPI Addresses...")
                await db.status(f"""WITH x AS (
    SELECT
        int_code, code as target_code
    FROM
        {db_schema}.nucc_taxonomy
    )
    UPDATE {db_schema}.{obj.__tablename__} as addr SET taxonomy_array=b.res FROM (
    select npi, ARRAY_AGG(x.int_code) as res from {db_schema}.npi_taxonomy_{import_date}
    INNER JOIN x ON healthcare_provider_taxonomy_code = x.target_code
    GROUP BY npi) as b WHERE addr.npi = b.npi;""")

                print("Updating NPI Addresses Geo from Archive...")
                await db.status(
                    f"UPDATE {db_schema}.{obj.__tablename__} as a SET formatted_address = b.formatted_address, lat = b.lat, "
                    f"long = b.long, "
                    f"place_id = b.place_id FROM {db_schema}.address_archive as b WHERE a.checksum = b.checksum")

                print("Updating NPI Plan-Network Array from Plans Import Data...")
                await db.status(
                    f"""UPDATE {db_schema}.{obj.__tablename__} as a 
                    SET
                        plans_network_array = n_list
                    FROM (SELECT
                        npi, ARRAY_AGG(DISTINCT checksum_network) as n_list
                        FROM
                        {db_schema}.plan_npi_raw
                        GROUP BY npi) as b
                    WHERE
                        a.npi = b.npi;""")

            if hasattr(cls, '__my_additional_indexes__') and cls.__my_additional_indexes__:
                for index in cls.__my_additional_indexes__:
                    index_name = index.get('name', '_'.join(index.get('index_elements')))
                    using = ""
                    if t:=index.get('using'):
                        using = f"USING {t} "
                    create_index_sql = f"CREATE INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} " \
                                    f"ON {db_schema}.{obj.__tablename__}  {using}" \
                                    f"({', '.join(index.get('index_elements'))});"
                    print(create_index_sql)
                    x = await db.status(create_index_sql)
                    
    # Run VACUUM FULL ANALYZE in parallel for all tables
    async def vacuum_table(obj):
        print(f"Post-Index VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")
        await db.execute_ddl(f"VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")

    vacuum_tasks = [
        vacuum_table(tables[cls.__main_table__])
        for cls in processing_classes_array
    ]
    await asyncio.gather(*vacuum_tasks)

    async with db.transaction() as tx:
        for cls in processing_classes_array:
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

            move_indexes = []
            if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                move_indexes += cls.__my_initial_indexes__
            if hasattr(cls, '__my_additional_indexes__') and obj.__my_additional_indexes__:
                move_indexes += obj.__my_additional_indexes__

            for index in move_indexes:
                index_name = index.get('name', '_'.join(index.get('index_elements')))
                await db.status(f"ALTER INDEX IF EXISTS "
                                f"{db_schema}.{table}_idx_{index_name} RENAME TO "
                                f"{table}_idx_{index_name}_old;")
                await db.status(f"ALTER INDEX IF EXISTS "
                                f"{db_schema}.{obj.__tablename__}_idx_{index_name} RENAME TO "
                                f"{table}_idx_{index_name};")

    print_time_info(ctx['context']['start'])


async def save_npi_data(ctx, task):
    import_date = ctx['import_date']
    x = []
    for key in task:
        match key:
            case 'npi_obj_list':
                mynpidata = make_class(NPIData, import_date)
                x.append(push_objects(task['npi_obj_list'], mynpidata, rewrite=True))
            case 'npi_taxonomy_list':
                mynpidatataxonomy = make_class(NPIDataTaxonomy, import_date)
                x.append(push_objects(task['npi_taxonomy_list'], mynpidatataxonomy, rewrite=True))
            case 'npi_other_id_list':
                mynpidataotheridentifier = make_class(NPIDataOtherIdentifier, import_date)
                unique = list({item['checksum']: item for item in task['npi_other_id_list']}.values())
                x.append(push_objects(unique, mynpidataotheridentifier))
            case 'npi_taxonomy_group_list':
                mynpidatataxonomygroup = make_class(NPIDataTaxonomyGroup, import_date)
                x.append(push_objects(task['npi_taxonomy_group_list'], mynpidatataxonomygroup, rewrite=True))
            case 'npi_address_list':
                mynpiaddress = make_class(NPIAddress, import_date)
                x.append(push_objects(task['npi_address_list'], mynpiaddress, rewrite=True))
            case _:
                print('Some wrong key passed')
    await asyncio.gather(*x)


async def main(test_mode: bool = False):  # pragma: no cover
    redis = await create_pool(build_redis_settings(),
                              job_serializer=serialize_job,
                              job_deserializer=deserialize_job)
    await redis.enqueue_job('process_data', {'test_mode': test_mode}, _queue_name='arq:NPI')