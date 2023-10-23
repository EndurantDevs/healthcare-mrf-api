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
from asyncpg import DuplicateTableError
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

latin_pattern= re.compile(r'[^\x00-\x7f]')


async def process_npi_chunk(ctx, task):
    import_date = ctx['import_date']
    redis = ctx['redis']

    npi_obj_list = []
    npi_taxonomy_list_dict = {}
    npi_other_id_list_dict = {}
    npi_taxonomy_group_list_dict = {}
    npi_taxonomy_group_list_dict = {}
    npi_address_list_dict = {}

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

        npi_obj_list.append(obj)

        if (row['Provider First Line Business Practice Location Address']):
            obj = {
                'first_line': row['Provider First Line Business Practice Location Address'],
                'second_line': row['Provider Second Line Business Practice Location Address'],
                'city_name': row['Provider Business Practice Location Address City Name'],
                'state_name': row['Provider Business Practice Location Address State Name'],
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
                'city_name': row['Provider Business Mailing Address City Name'],
                'state_name': row['Provider Business Mailing Address State Name'],
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

    await redis.enqueue_job('save_npi_data', {
        'npi_obj_list': npi_obj_list,
        'npi_taxonomy_list': list(npi_taxonomy_list_dict.values()),
        'npi_other_id_list': list(npi_other_id_list_dict.values()),
        'npi_taxonomy_group_list': list(npi_taxonomy_group_list_dict.values()),
        'npi_address_list': list(npi_address_list_dict.values()),
    })



async def process_data(ctx):
    import_date = ctx['import_date']
    redis = ctx['redis']
    html_source = await download_it(
        os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    # re./NPPES_Data_Dissemination_110722_111322_Weekly.zip">NPPES Data Dissemination - Weekly Update -
    # 110722_111322</a>

    for p in re.findall(r'(NPPES_Data_Dissemination.*.zip)', html_source.text):
        if p.endswith('021223_Weekly.zip') or p.endswith('020523_Weekly.zip') or p.endswith(
                '030523_Weekly.zip') or p.endswith('031223_Weekly.zip'):
            continue

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
                        row_list.clear()
                        count = 0
                    else:
                        count += 1

            npi_address_list_dict = {}
            async with async_open(pl_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not (row['NPI'] or row['Provider Secondary Practice Location Address- Address Line 1']):
                        continue
                    count += 1
                    if not count % 100_000:
                        print(f"Processed: {count}")
                    obj = {
                        'first_line': row['Provider Secondary Practice Location Address- Address Line 1'],
                        'second_line': row['Provider Secondary Practice Location Address-  Address Line 2'],
                        'city_name': row['Provider Secondary Practice Location Address - City Name'],
                        'state_name': row['Provider Secondary Practice Location Address - State Name'],
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

                    if count > 9999:
                        await redis.enqueue_job('save_npi_data', {
                            'npi_address_list': list(npi_address_list_dict.values()),
                        })
                        npi_address_list_dict = {}
                        count = 0
                    else:
                        count += 1
            await redis.enqueue_job('save_npi_data', {
                'npi_address_list': list(npi_address_list_dict.values()),
            })

            print(f"Processed: {count}")


async def startup(ctx):
    await my_init_db(db)
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.now()
    ctx['context']['run'] = 0
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    tables = {}  # for the future complex usage

    try:
        obj = AddressArchive
        await AddressArchive.__table__.gino.create()
        if hasattr(AddressArchive, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")
    except DuplicateTableError:
        pass

    for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIAddress):
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

    test = make_class(NPIAddress, import_date)
    npi_address_count = await db.func.count(test.npi).gino.scalar()
    if (not npi_address_count) or (npi_address_count < 5000000):
        print(f"Failed Import: Address number:{npi_address_count}")
        exit(1)

    processing_classes_array = (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIAddress,)

    for cls in processing_classes_array:
        tables[cls.__main_table__] = make_class(cls, import_date)
        obj = tables[cls.__main_table__]
        if cls is NPIAddress:
            print("Updating NUCC Taxonomy for NPI Addresses...")
            await db.status(f"""WITH x AS (
SELECT
    int_code, code as target_code
FROM
    {db_schema}.nucc_taxonomy
)
UPDATE {db_schema}.{obj.__tablename__} as addr SET taxonomy_array=b.res FROM (
select npi, ARRAY_AGG(x.int_code) as res from mrf.npi_taxonomy_{import_date}
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

        print(f"Post-Index VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};");
        await db.status(f"VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")

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
                x.append(push_objects(task['npi_other_id_list'], mynpidataotheridentifier, rewrite=True))
            case 'npi_taxonomy_group_list':
                mynpidatataxonomygroup = make_class(NPIDataTaxonomyGroup, import_date)
                x.append(push_objects(task['npi_taxonomy_group_list'], mynpidatataxonomygroup, rewrite=True))
            case 'npi_address_list':
                mynpiaddress = make_class(NPIAddress, import_date)
                x.append(push_objects(task['npi_address_list'], mynpiaddress, rewrite=True))
            case _:
                print('Some wrong key passed')
    await asyncio.gather(*x)


async def main():
    redis = await create_pool(RedisSettings.from_dsn(os.environ.get('HLTHPRT_REDIS_ADDRESS')),
                              job_serializer=msgpack.packb,
                              job_deserializer=lambda b: msgpack.unpackb(b, raw=False))
    x = await redis.enqueue_job('process_data')
