# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
import asyncio
import datetime
import tempfile
import re
from arq import create_pool
from pathlib import PurePath
from aiocsv import AsyncDictReader
from aiofile import async_open

from process.ext.utils import download_it, download_it_and_save, \
    make_class, push_objects, print_time_info, return_checksum

from db.models import NUCCTaxonomy, db
from db.connection import init_db
from process.serialization import serialize_job, deserialize_job
from process.redis_config import build_redis_settings

latin_pattern= re.compile(r'[^\x00-\x7f]')

TEST_NUCC_ROWS = 500
TEST_NUCC_MAX_FILES = 1


def is_test_mode(ctx: dict) -> bool:
    return bool(ctx.get("context", {}).get("test_mode"))


async def process_data(ctx, task=None):
    task = task or {}
    import_date = ctx['import_date']
    ctx.setdefault('context', {})
    test_mode = bool(task.get('test_mode', ctx['context'].get('test_mode', False)))
    ctx['context']['test_mode'] = test_mode
    html_source = await download_it(
        os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_FILE'])

    for file_index, p in enumerate(re.findall(r'\"(.*?nucc_taxonomy.*?\.csv)\"', html_source)):
        if test_mode and file_index >= TEST_NUCC_MAX_FILES:
            break
        with tempfile.TemporaryDirectory() as tmpdirname:
            print(f"Found: {p}")
            file_name = p.split('/')[-1]
            tmp_filename = str(PurePath(str(tmpdirname), file_name))
            await download_it_and_save(os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_DIR'] + p, tmp_filename,
                                       chunk_size=10 * 1024 * 1024, cache_dir='/tmp')
            print(f"Downloaded: {p}")
            csv_map, csv_map_reverse = ({}, {})
            async with async_open(tmp_filename, 'r', encoding='utf-8-sig') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    for key in row:
                        t = re.sub(r"\(.*\)", r"", key.lower()).strip().replace(' ', '_')
                        csv_map[key] = t
                        csv_map_reverse[t] = key
                    break

            count = 0


            row_list = []
            nucc_taxonomy_cls = make_class(NUCCTaxonomy, import_date)
            async with async_open(tmp_filename, 'r', encoding='utf-8-sig') as afp:
                async for row in AsyncDictReader(afp, delimiter=","):
                    if not row['Code']:
                        continue
                    count += 1
                    if test_mode and count > TEST_NUCC_ROWS:
                        break
                    if not count % 100_000:
                        print(f"Processed: {count}")
                    obj = {}
                    for key, mapped_key in csv_map.items():
                        t = row[key]
                        if not t:
                            obj[mapped_key] = None
                            continue
                        obj[mapped_key] = t
                    obj['int_code'] = return_checksum([obj['code'],], crc=32)
                    row_list.append(obj)
                    if count % 9999 == 0:
                        await push_objects(row_list, nucc_taxonomy_cls)
                        row_list.clear()


            await push_objects(row_list, nucc_taxonomy_cls)
            print(f"Processed: {count}")
        return 1


async def startup(ctx):
    loop = asyncio.get_event_loop()
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.utcnow()
    ctx['context']['run'] = 0
    ctx['context']['test_mode'] = False
    ctx['import_date'] = datetime.datetime.utcnow().strftime("%Y%m%d")
    await init_db(db, loop)
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    tables = {}  # for the future complex usage

    for cls in (NUCCTaxonomy,):
        tables[cls.__main_table__] = make_class(cls, import_date)
        obj = tables[cls.__main_table__]
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__main_table__}_{import_date};")
        await db.create_table(obj.__table__, checkfirst=True)
        if hasattr(obj, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary ON "
                f"{db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});")

    print("Preparing done")


async def shutdown(ctx):
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    tables = {}
    async with db.transaction():
        for cls in (NUCCTaxonomy, ):
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


async def main(test_mode: bool = False):
    redis = await create_pool(build_redis_settings(),
                              job_serializer=serialize_job,
                              job_deserializer=deserialize_job)
    await redis.enqueue_job('process_data', {'test_mode': test_mode})
