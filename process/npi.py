# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import glob
import hashlib
import os
import re
import sys
import tempfile
import zipfile
from pathlib import PurePath

import pytz
from aiocsv import AsyncDictReader
from aiofile import async_open
from arq import create_pool
from arq.connections import RedisSettings
from async_unzip.unzipper import unzip
from asyncpg import DuplicateTableError
from dateutil.parser import parse as parse_date
from sqlalchemy import func, select

from db.models import (AddressArchive, NPIAddress, NPIData,
                       NPIDataOtherIdentifier, NPIDataTaxonomy,
                       NPIDataTaxonomyGroup, db)
from process.ext.utils import (download_it, download_it_and_save,
                               ensure_database, make_class, my_init_db,
                               print_time_info, push_objects, return_checksum)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

latin_pattern= re.compile(r'[^\x00-\x7f]')

TEST_NPI_MAX_FILES = 1
TEST_NPI_ROWS = 1000
TEST_NPI_OTHER_ROWS = 500
TEST_NPI_SECONDARY_ROWS = 1000
NPI_QUEUE_NAME = "arq:NPI"
DEFAULT_NPI_MAX_PENDING_SAVE_TASKS = 4
POSTGRES_IDENTIFIER_MAX_LENGTH = 63


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


async def _ensure_required_extensions() -> None:
    for extension in ("pg_trgm", "intarray", "btree_gin"):
        await db.status(f"CREATE EXTENSION IF NOT EXISTS {extension};")


def is_test_mode(ctx: dict) -> bool:
    return bool(ctx.get("context", {}).get("test_mode"))

async def process_npi_chunk(ctx, task):
    redis = ctx['redis']

    npi_obj_list = []
    npi_taxonomy_list_dict = {}
    npi_other_id_list_dict = {}
    npi_taxonomy_group_list_dict = {}
    npi_address_list_dict = {}

    npi_csv_map = task['npi_csv_map']
    npi_csv_map_reverse = task['npi_csv_map_reverse']
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

        if row['Provider First Line Business Practice Location Address']:
            obj = {
                'first_line': row['Provider First Line Business Practice Location Address'],
                'second_line': row['Provider Second Line Business Practice Location Address'],
                'city_name': row.get('Provider Business Practice Location Address City Name', '').upper(),
                'state_name': row.get('Provider Business Practice Location Address State Name', '').upper(),
                'postal_code': row['Provider Business Practice Location Address Postal Code'],
                'country_code': row['Provider Business Practice Location Address Country Code (If outside U.S.)'],
            }

            obj.update({
                'checksum': return_checksum(list(obj.values())),  # addresses have blank symbols
                'npi': int(row['NPI']),
                'type': 'primary',
                'telephone_number': row['Provider Business Practice Location Address Telephone Number'],
                'fax_number': row['Provider Business Practice Location Address Fax Number'],
                'date_added': pytz.utc.localize(parse_date(row['Last Update Date'], fuzzy=True))
                if row['Last Update Date']
                else None,
            })
            npi_address_list_dict['_'.join([str(obj['npi']), str(obj['checksum']), obj['type'],])] = obj

        if row['Provider First Line Business Mailing Address']:
            obj = {
                'first_line': row['Provider First Line Business Mailing Address'],
                'second_line': row['Provider Second Line Business Mailing Address'],
                'city_name': row.get('Provider Business Mailing Address City Name', '').upper(),
                'state_name': row.get('Provider Business Mailing Address State Name', '').upper(),
                'postal_code': row['Provider Business Mailing Address Postal Code'],
                'country_code': row['Provider Business Mailing Address Country Code (If outside U.S.)'],
            }

            obj.update({
                'checksum': return_checksum(list(obj.values())),  # addresses have blank symbols
                'npi': int(row['NPI']),
                'type': 'mail',
                'telephone_number': row['Provider Business Mailing Address Telephone Number'],
                'fax_number': row['Provider Business Mailing Address Fax Number'],
                'date_added': pytz.utc.localize(parse_date(row['Last Update Date'], fuzzy=True))
                if row['Last Update Date']
                else None,
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

    payload = {
        'npi_obj_list': npi_obj_list,
        'npi_taxonomy_list': list(npi_taxonomy_list_dict.values()),
        'npi_other_id_list': list(npi_other_id_list_dict.values()),
        'npi_taxonomy_group_list': list(npi_taxonomy_group_list_dict.values()),
        'npi_address_list': list(npi_address_list_dict.values()),
    }
    if task.get('direct'):
        await save_npi_data(ctx, payload)
        print(f'Processing.. {len(npi_obj_list)} rows directly')
    else:
        await redis.enqueue_job('save_npi_data', payload, _queue_name=NPI_QUEUE_NAME)



async def process_data(ctx, task=None):  # pragma: no cover
    # Track whether any work actually ran so shutdown can distinguish "no jobs" from a bad import
    task = task or {}
    ctx.setdefault('context', {})
    if 'test_mode' in task:
        ctx['context']['test_mode'] = bool(task.get('test_mode'))
    test_mode = bool(ctx['context'].get('test_mode', False))
    await ensure_database(test_mode)
    await _ensure_required_extensions()

    import_date = ctx['import_date']
    print(os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    html_source = await download_it(
        os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NPPES_DOWNLOAD_URL_FILE'])
    # re./NPPES_Data_Dissemination_110722_111322_Weekly.zip">NPPES Data Dissemination - Weekly Update -
    # 110722_111322</a>
    count_files = 0
    sql_chunk_size = 299999
    max_pending_save_tasks = _env_positive_int(
        "HLTHPRT_NPI_MAX_PENDING_SAVE_TASKS",
        DEFAULT_NPI_MAX_PENDING_SAVE_TASKS,
    )

    async def enqueue_or_flush(coros: list, coro) -> None:
        coros.append(asyncio.create_task(coro))
        if len(coros) >= max_pending_save_tasks:
            await asyncio.gather(*coros)
            coros.clear()

    file_limit = TEST_NPI_MAX_FILES if test_mode else None
    for file_idx, p in enumerate(re.findall(r'(NPPES_Data_Dissemination.*_V2.zip)', html_source)):
        if file_limit and file_idx >= file_limit:
            break
        count_files = count_files + 1
        current_sql_chunk_size = sql_chunk_size
        if test_mode:
            current_sql_chunk_size = min(current_sql_chunk_size, TEST_NPI_ROWS)
        print(f"Round {count_files} for {p}")
        with tempfile.TemporaryDirectory() as tmpdirname:
            print(f"Found: {p}")
            # await unzip('/users/nick/downloads/NPPES_Data_Dissemination_November_2022.zip', tmpdirname, __debug=True)

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
                await unzip(tmp_filename, tmpdirname, buffer_size=10 * 1024 * 1024)
            except Exception:  # pylint: disable=broad-exception-caught
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
                current_sql_chunk_size = sql_chunk_size // 26
                npi_set: set[int] = set()
                async with async_open(npi_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))

                for cls in (NPIData, NPIDataTaxonomyGroup, NPIDataTaxonomy, NPIAddress):
                    table = make_class(cls, import_date)
                    npi_list = list(npi_set)
                    chunk_size = 1000
                    npi_column = getattr(table, 'npi')
                    for index in range(0, len(npi_list), chunk_size):
                        chunk = npi_list[index:index + chunk_size]
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

                table = make_class(NPIAddress, import_date)
                npi_list = list(npi_set)
                chunk_size = 10000
                npi_column = getattr(table, 'npi')
                for index in range(0, len(npi_list), chunk_size):
                    chunk = npi_list[index:index + chunk_size]
                    delete_stmt = db.delete(table.__table__).where(npi_column.in_(chunk))
                    delete_stmt = delete_stmt.where(table.type == 'secondary')
                    await delete_stmt.status()

                npi_set.clear()

                async with async_open(other_file, 'r') as afp:
                    async for row in AsyncDictReader(afp, delimiter=","):
                        if row.get('NPI'):
                            npi_set.add(int(row['NPI']))

                table = make_class(NPIDataOtherIdentifier, import_date)
                npi_list = list(npi_set)
                chunk_size = 10000
                npi_column = getattr(table, 'npi')
                for index in range(0, len(npi_list), chunk_size):
                    chunk = npi_list[index:index + chunk_size]
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


            row_list = []
            coros = []
            processed_rows = 0
            async with async_open(npi_file, 'r') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row['NPI']:
                        continue
                    if not count % current_sql_chunk_size:
                        print(f"Processed: {count}")
                    row_list.append(row)
                    processed_rows += 1
                    if count > current_sql_chunk_size:
                        print(f"Sending to DB: {count}")
                        payload = {
                            'row_list': row_list.copy(),
                            'npi_csv_map': npi_csv_map,
                            'npi_csv_map_reverse': npi_csv_map_reverse,
                            'direct': True,
                        }
                        await enqueue_or_flush(coros, process_npi_chunk(ctx, payload))
                        row_list.clear()
                        count = 0
                    else:
                        count += 1
                    if test_mode and processed_rows >= TEST_NPI_ROWS:
                        break

            payload = {
                'row_list': row_list.copy(),
                'npi_csv_map': npi_csv_map,
                'npi_csv_map_reverse': npi_csv_map_reverse,
                'direct': True,
            }
            await enqueue_or_flush(coros, process_npi_chunk(ctx, payload))
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
                        other_payload = {
                            'npi_other_id_list': list(npi_other_org_list_dict.copy().values())
                        }
                        await enqueue_or_flush(coros, save_npi_data(ctx, other_payload))
                        npi_other_org_list_dict.clear()
                        count = 0
                    else:
                        count += 1
                    processed_other += 1
                    if test_mode and processed_other >= TEST_NPI_OTHER_ROWS:
                        break

            other_payload = {'npi_other_id_list': list(npi_other_org_list_dict.copy().values())}
            await enqueue_or_flush(coros, save_npi_data(ctx, other_payload))
            npi_other_org_list_dict.clear()


            npi_address_list_dict = {}
            async with async_open(pl_file, 'r') as afp:
                processed_secondary = 0
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row['NPI'] and not row['Provider Secondary Practice Location Address- Address Line 1']:
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
                        await enqueue_or_flush(
                            coros,
                            save_npi_data(ctx, {'npi_address_list': list(npi_address_list_dict.copy().values())}),
                        )
                        # await redis.enqueue_job('save_npi_data', {
                        #     'npi_address_list': list(npi_address_list_dict.values()),
                        # })
                        npi_address_list_dict.clear()
                        count = 0
                    else:
                        count += 1
                    processed_secondary += 1
                    if test_mode and processed_secondary >= TEST_NPI_SECONDARY_ROWS:
                        print(f"Test mode: stopping secondary address scan at {processed_secondary} rows.")
                        break
            # await redis.enqueue_job('save_npi_data', {
            #     'npi_address_list': list(npi_address_list_dict.values()),
            # })
            await enqueue_or_flush(
                coros,
                save_npi_data(ctx, {'npi_address_list': list(npi_address_list_dict.copy().values())}),
            )
            await asyncio.gather(*coros)
            npi_address_list_dict.clear()

            print(f"Processed: {count}")

    # Mark this job as successfully completed; shutdown finalization depends on this.
    ctx['context']['run'] = ctx['context'].get('run', 0) + 1


async def startup(ctx):  # pragma: no cover
    await my_init_db(db)
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.utcnow()
    ctx['context']['run'] = 0
    ctx['context']['test_mode'] = False
    await ensure_database(False)
    ctx['import_date'] = datetime.datetime.now().strftime("%Y%m%d")
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    tables = {}  # for the future complex usage

    await _ensure_required_extensions()

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
                await db.status(create_index_sql)

    print("Preparing done")

async def refresh_do_business_as(
    target_table: str | None = None,
    source_table: str | None = None,
    test_mode: bool | None = None,
):
    """
    Populate the NPI.do_business_as array from other identifier entries (type code 3).
    """
    await ensure_database(bool(test_mode))
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    table = target_table or NPIData.__tablename__
    source = source_table or NPIDataOtherIdentifier.__tablename__

    source_exists = await db.scalar(
        f"SELECT to_regclass('{db_schema}.{source}');"
    )
    if not source_exists:
        print(f"Skipping do_business_as refresh: source table {db_schema}.{source} does not exist.")
        return

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
            FROM {db_schema}.{source}
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
    await ensure_database(bool(context.get("test_mode")))

    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    tables = {}

    npi_address_stage_table = f"{NPIAddress.__tablename__}_{import_date}"
    npi_address_stage_exists = await db.scalar(
        f"SELECT to_regclass('{db_schema}.{npi_address_stage_table}');"
    )
    if not npi_address_stage_exists:
        print(
            f"Staging table {db_schema}.{npi_address_stage_table} is missing; "
            "skipping NPI shutdown finalization."
        )
        return

    test = make_class(NPIAddress, import_date)
    npi_address_count = await db.scalar(select(func.count(test.npi)))  # pylint: disable=not-callable
    if context.get("test_mode"):
        print(f"Test mode: imported {npi_address_count} NPI addresses (no minimum enforced).")
    else:
        if not npi_address_count or npi_address_count < 5_000_000:
            print(f"Failed Import: Address number:{npi_address_count}")
            sys.exit(1)

    processing_classes_array = (NPIData, NPIDataTaxonomyGroup, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIAddress,)

    async def table_exists(table_name: str) -> bool:
        exists = await db.scalar(f"SELECT to_regclass('{db_schema}.{table_name}');")
        return bool(exists)

    postgis_available: bool | None = None

    async def has_postgis() -> bool:
        nonlocal postgis_available
        if postgis_available is None:
            geography_type = await db.scalar("SELECT to_regtype('geography');")
            st_makepoint = await db.scalar("SELECT to_regprocedure('st_makepoint(double precision, double precision)');")
            postgis_available = bool(geography_type and st_makepoint)
            if not postgis_available:
                print("PostGIS is unavailable; geo GIST index creation will be skipped.")
        return postgis_available

    async def archive_index(index_name: str) -> str:
        archived_name = _archived_identifier(index_name)
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_name};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{index_name} RENAME TO {archived_name};"
        )
        return archived_name

    async with db.transaction():
        for cls in processing_classes_array:
            tables[cls.__main_table__] = make_class(cls, import_date)
            obj = tables[cls.__main_table__]
            if cls is NPIDataOtherIdentifier:
                print('Updating NPI do_business_as arrays from other identifiers...')
                target_npi_cls = tables.get(NPIData.__main_table__)
                target_table_name = target_npi_cls.__tablename__ if target_npi_cls else NPIData.__tablename__
                source_table_name = obj.__tablename__
                await refresh_do_business_as(
                    target_table=target_table_name,
                    source_table=source_table_name,
                    test_mode=bool(context.get("test_mode")),
                )
            if cls is NPIAddress:
                npi_taxonomy_table = f"npi_taxonomy_{import_date}"
                if await table_exists(npi_taxonomy_table) and await table_exists("nucc_taxonomy"):
                    print("Updating NUCC Taxonomy for NPI Addresses...")
                    await db.status(f"""WITH x AS (
    SELECT
        int_code, code as target_code
    FROM
        {db_schema}.nucc_taxonomy
    )
    UPDATE {db_schema}.{obj.__tablename__} as addr SET taxonomy_array=b.res FROM (
    select npi, ARRAY_AGG(x.int_code) as res from {db_schema}.{npi_taxonomy_table}
    INNER JOIN x ON healthcare_provider_taxonomy_code = x.target_code
    GROUP BY npi) as b WHERE addr.npi = b.npi;""")
                else:
                    print(
                        f"Skipping NUCC taxonomy update: "
                        f"required tables missing ({db_schema}.{npi_taxonomy_table} and/or {db_schema}.nucc_taxonomy)."
                    )

                if await table_exists("address_archive"):
                    print("Updating NPI Addresses Geo from Archive...")
                    await db.status(
                        f"UPDATE {db_schema}.{obj.__tablename__} as a SET formatted_address = b.formatted_address, "
                        f"lat = b.lat, long = b.long, place_id = b.place_id "
                        f"FROM {db_schema}.address_archive as b WHERE a.checksum = b.checksum"
                    )
                else:
                    print(f"Skipping NPI geo update: source table {db_schema}.address_archive is missing.")

                if await table_exists("plan_npi_raw"):
                    print("Updating NPI Plan-Network Array from Plans Import Data...")
                    await db.status(
                        f"""UPDATE {db_schema}.{obj.__tablename__} as a
SET
    plans_network_array = n_list
FROM (
    SELECT
        npi,
        ARRAY_AGG(DISTINCT checksum_network) as n_list
    FROM {db_schema}.plan_npi_raw
    GROUP BY npi
) as b
WHERE
    a.npi = b.npi;"""
                    )
                else:
                    print(f"Skipping NPI plan-network update: source table {db_schema}.plan_npi_raw is missing.")

                if await table_exists("pricing_provider_procedure"):
                    print("Updating NPI procedures_array from pricing provider procedures...")
                    await db.status(
                        f"""UPDATE {db_schema}.{obj.__tablename__} AS a
SET
    procedures_array = b.codes
FROM (
    SELECT
        npi,
        ARRAY_AGG(DISTINCT procedure_code ORDER BY procedure_code) AS codes
    FROM {db_schema}.pricing_provider_procedure
    GROUP BY npi
) AS b
WHERE
    a.npi = b.npi;"""
                    )
                else:
                    print(
                        f"Skipping NPI procedures_array update: source table "
                        f"{db_schema}.pricing_provider_procedure is missing."
                    )

                if await table_exists("pricing_provider_prescription"):
                    print("Updating NPI medications_array from pricing provider prescriptions...")
                    await db.status(
                        f"""UPDATE {db_schema}.{obj.__tablename__} AS a
SET
    medications_array = b.codes
FROM (
    SELECT
        npi,
        ARRAY_AGG(DISTINCT rx_code::INTEGER ORDER BY rx_code::INTEGER) AS codes
    FROM {db_schema}.pricing_provider_prescription
    WHERE
        rx_code_system = 'HP_RX_CODE'
        AND rx_code ~ '^-?[0-9]+$'
    GROUP BY npi
) AS b
WHERE
    a.npi = b.npi;"""
                    )
                else:
                    print(
                        f"Skipping NPI medications_array update: source table "
                        f"{db_schema}.pricing_provider_prescription is missing."
                    )

            if hasattr(cls, '__my_additional_indexes__') and cls.__my_additional_indexes__:
                for index in cls.__my_additional_indexes__:
                    index_name = index.get('name', '_'.join(index.get('index_elements')))
                    if index_name.startswith("geo_index_") and not await has_postgis():
                        print(
                            f"Skipping index {obj.__tablename__}_idx_{index_name}: "
                            "requires PostGIS (geography + ST_MakePoint)."
                        )
                        continue
                    using = ""
                    if t := index.get('using'):
                        using = f"USING {t} "
                    where_clause = ""
                    if where := index.get('where'):
                        where_clause = f" WHERE {where}"
                    create_index_sql = (
                        f"CREATE INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} "
                        f"ON {db_schema}.{obj.__tablename__}  {using}"
                        f"({', '.join(index.get('index_elements'))}){where_clause};"
                    )
                    print(create_index_sql)
                    await db.status(create_index_sql)

    # Run VACUUM FULL ANALYZE in parallel for all tables
    async def vacuum_table(obj):
        print(f"Post-Index VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")
        await db.execute_ddl(f"VACUUM FULL ANALYZE {db_schema}.{obj.__tablename__};")

    vacuum_tasks = [
        vacuum_table(tables[cls.__main_table__])
        for cls in processing_classes_array
    ]
    await asyncio.gather(*vacuum_tasks)

    async with db.transaction():
        for cls in processing_classes_array:
            obj = tables[cls.__main_table__]
            table = obj.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{obj.__tablename__} RENAME TO {table};")

            await archive_index(f"{table}_idx_primary")

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
                await archive_index(f"{table}_idx_{index_name}")
                await db.status(f"ALTER INDEX IF EXISTS "
                                f"{db_schema}.{obj.__tablename__}_idx_{index_name} RENAME TO "
                                f"{table}_idx_{index_name};")

    print_time_info(ctx['context']['start'])


async def save_npi_data(ctx, task):
    import_date = ctx['import_date']
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
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
    for coro in x:
        await coro


async def main(test_mode: bool = False):  # pragma: no cover
    redis = await create_pool(build_redis_settings(),
                              job_serializer=serialize_job,
                              job_deserializer=deserialize_job)
    payload = {'test_mode': bool(test_mode)}
    await redis.enqueue_job('process_data', payload, _queue_name=NPI_QUEUE_NAME)
