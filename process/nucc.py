# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import os
import re
import tempfile
from pathlib import PurePath

from aiocsv import AsyncDictReader
from aiofile import async_open
from arq import create_pool

from db.connection import init_db
from db.models import NUCCTaxonomy, db
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import mark_control_run
from process.ext.utils import (download_it, download_it_and_save,
                               ensure_database, make_class, print_time_info,
                               push_objects, return_checksum)
from process.live_progress import enqueue_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

latin_pattern= re.compile(r'[^\x00-\x7f]')

TEST_NUCC_ROWS = 500
TEST_NUCC_MAX_FILES = 1
NUCC_QUEUE_NAME = "arq:NUCC"


def is_test_mode(ctx: dict) -> bool:
    """Return whether the worker is running in test mode."""
    return bool(ctx.get("context", {}).get("test_mode"))


async def process_nucc_data(ctx, task=None):
    """Process one queued NUCC taxonomy import task."""
    task = task or {}
    await raise_if_cancelled(ctx, task)
    import_date = ctx['import_date']
    ctx.setdefault('context', {})
    context = ctx['context']
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    test_mode = bool(task.get('test_mode', context.get('test_mode', False)))
    context['test_mode'] = test_mode
    await ensure_database(test_mode)
    html_source = await download_it(
        os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_FILE'])

    source_files = re.findall(r'\"(.*?nucc_taxonomy.*?\.csv)\"', html_source)
    selected_files = source_files[:TEST_NUCC_MAX_FILES] if test_mode else source_files
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="nucc",
            status="running",
            phase="nucc sources discovered",
            unit="files",
            done=0,
            total=len(selected_files),
            message=f"{len(selected_files)} source files discovered",
        )
    for file_index, source_file in enumerate(selected_files):
        if run_id:
            enqueue_live_progress(
                run_id=run_id,
                importer="nucc",
                status="running",
                phase="nucc downloading source",
                unit="files",
                done=file_index,
                total=len(selected_files),
                message=f"downloading file {file_index + 1}/{len(selected_files)}",
                label=source_file,
            )
        with tempfile.TemporaryDirectory() as tmpdirname:
            print(f"Found: {source_file}")
            file_name = source_file.split('/')[-1]
            tmp_filename = str(PurePath(str(tmpdirname), file_name))
            await download_it_and_save(
                os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_DIR'] + source_file,
                tmp_filename,
                chunk_size=10 * 1024 * 1024,
                cache_dir='/tmp',
            )
            print(f"Downloaded: {source_file}")
            csv_map, csv_map_reverse = ({}, {})
            async with async_open(tmp_filename, 'r', encoding='utf-8-sig') as afp:
                async for header_row in AsyncDictReader(afp, delimiter=","):
                    for key in header_row:
                        normalized_column_name = (
                            re.sub(r"\(.*\)", r"", key.lower())
                            .strip()
                            .replace(' ', '_')
                        )
                        csv_map[key] = normalized_column_name
                        csv_map_reverse[normalized_column_name] = key
                    break

            count = 0


            row_list = []
            nucc_taxonomy_cls = make_class(NUCCTaxonomy, import_date)
            async with async_open(tmp_filename, 'r', encoding='utf-8-sig') as afp:
                async for taxonomy_row in AsyncDictReader(afp, delimiter=","):
                    if not taxonomy_row['Code']:
                        continue
                    count += 1
                    if test_mode and count > TEST_NUCC_ROWS:
                        break
                    if not count % 100_000:
                        print(f"Processed: {count}")
                        await raise_if_cancelled(ctx, task)
                    if run_id and count and count % (100 if test_mode else 100_000) == 0:
                        enqueue_live_progress(
                            run_id=run_id,
                            importer="nucc",
                            status="running",
                            phase="nucc parsing rows",
                            unit="rows",
                            done=count,
                            total=TEST_NUCC_ROWS if test_mode else None,
                            message=f"parsed {count} rows",
                            label=source_file,
                        )
                    taxonomy_dict = {}
                    for key, mapped_key in csv_map.items():
                        cell_value = taxonomy_row[key]
                        if not cell_value:
                            taxonomy_dict[mapped_key] = None
                            continue
                        taxonomy_dict[mapped_key] = cell_value
                    taxonomy_dict['int_code'] = return_checksum(
                        [taxonomy_dict['code']], crc=32
                    )
                    row_list.append(taxonomy_dict)
                    if count % 9999 == 0:
                        await raise_if_cancelled(ctx, task)
                        await push_objects(row_list, nucc_taxonomy_cls)
                        row_list.clear()


            await raise_if_cancelled(ctx, task)
            await push_objects(row_list, nucc_taxonomy_cls)
            print(f"Processed: {count}")
            context["run"] = context.get("run", 0) + 1
            context["rows"] = count
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
                    importer="nucc",
                    status="running",
                    phase="nucc source processed",
                    unit="files",
                    done=file_index + 1,
                    total=len(selected_files),
                    message=f"processed file {file_index + 1}/{len(selected_files)}",
                    label=source_file,
                )
        return 1


process_data = process_nucc_data
process_data.__name__ = "process_data"


async def startup(ctx):
    """Initialize resources required by the NUCC worker."""
    loop = asyncio.get_event_loop()
    ctx['context'] = {}
    ctx['context']['start'] = datetime.datetime.utcnow()
    ctx['context']['run'] = 0
    ctx['context']['test_mode'] = False
    ctx['import_date'] = datetime.datetime.utcnow().strftime("%Y%m%d")
    await init_db(db, loop)
    await ensure_database(False)
    import_date = ctx['import_date']
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'

    tables_by_name = {}  # for future multi-table imports

    for cls in (NUCCTaxonomy,):
        tables_by_name[cls.__main_table__] = make_class(cls, import_date)
        table_model = tables_by_name[cls.__main_table__]
        await db.status(
            f"DROP TABLE IF EXISTS "
            f"{db_schema}.{table_model.__main_table__}_{import_date};"
        )
        await db.create_table(table_model.__table__, checkfirst=True)
        if hasattr(table_model, "__my_index_elements__"):
            await db.status(
                f"CREATE UNIQUE INDEX {table_model.__tablename__}_idx_primary ON "
                f"{db_schema}.{table_model.__tablename__} "
                f"({', '.join(table_model.__my_index_elements__)});"
            )

    print("Preparing done")


async def publish_nucc_generation(ctx):
    """Finalize the NUCC run and release worker resources."""
    import_date = ctx['import_date']
    context = ctx.get("context") or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    await ensure_database(bool(context.get("test_mode")))
    db_schema = os.getenv('HLTHPRT_DB_SCHEMA') if os.getenv('HLTHPRT_DB_SCHEMA') else 'mrf'
    tables_by_name = {}
    stage_rows = 0
    async with db.transaction():
        for cls in (NUCCTaxonomy, ):
            tables_by_name[cls.__main_table__] = make_class(cls, import_date)
            table_model = tables_by_name[cls.__main_table__]
            table = table_model.__main_table__
            if run_id:
                stage_rows = int(
                    await db.scalar(
                        f"SELECT COUNT(*) FROM "
                        f"{db_schema}.{table_model.__tablename__};"
                    )
                    or 0
                )
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
            await db.status(
                f"ALTER TABLE IF EXISTS {db_schema}.{table_model.__tablename__} "
                f"RENAME TO {table};"
            )

            await db.status(f"ALTER INDEX IF EXISTS "
                            f"{db_schema}.{table}_idx_primary RENAME TO "
                            f"{table}_idx_primary_old;")

            await db.status(f"ALTER INDEX IF EXISTS "
                            f"{db_schema}.{table_model.__tablename__}_idx_primary RENAME TO "
                            f"{table}_idx_primary;")

    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="nucc published",
        progress_message="succeeded",
        progress={
            "unit": "rows",
            "done": stage_rows,
            "total": stage_rows,
            "pct": 100,
            "message": "succeeded",
            "phase": "nucc published",
        },
        metrics={"rows": stage_rows},
    )
    print_time_info(ctx['context']['start'])


shutdown = publish_nucc_generation
shutdown.__name__ = "shutdown"


async def main(test_mode: bool = False):
    """Run the NUCC taxonomy import entry point."""
    redis = await create_pool(build_redis_settings(),
                              job_serializer=serialize_job,
                              job_deserializer=deserialize_job)
    await redis.enqueue_job('process_data', {'test_mode': test_mode}, _queue_name=NUCC_QUEUE_NAME)
