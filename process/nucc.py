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


async def _prepare_nucc_import(ctx: dict, task: dict) -> tuple[str, str, bool]:
    """Initialize one NUCC import and return its execution settings."""
    await raise_if_cancelled(ctx, task)
    import_date = ctx['import_date']
    ctx.setdefault('context', {})
    context = ctx['context']
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    test_mode = bool(task.get('test_mode', context.get('test_mode', False)))
    context['test_mode'] = test_mode
    await ensure_database(test_mode)
    return import_date, run_id, test_mode


async def _discover_nucc_source_files(test_mode: bool) -> list[str]:
    """Download the NUCC index and select source files for this run."""
    html_source = await download_it(
        os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_DIR'] + os.environ['HLTHPRT_NUCC_DOWNLOAD_URL_FILE'])
    source_files = re.findall(r'\"(.*?nucc_taxonomy.*?\.csv)\"', html_source)
    return source_files[:TEST_NUCC_MAX_FILES] if test_mode else source_files


def _report_nucc_sources_discovered(run_id: str, selected_files: list[str]) -> None:
    """Publish the discovered-source progress event when this run is tracked."""
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


async def _read_nucc_csv_map(tmp_filename: str) -> dict[str, str]:
    """Read the NUCC header and normalize its column names."""
    csv_map = {}
    async with async_open(tmp_filename, 'r', encoding='utf-8-sig') as afp:
        async for header_row in AsyncDictReader(afp, delimiter=","):
            csv_map = {
                key: re.sub(r"\(.*\)", r"", key.lower()).strip().replace(' ', '_')
                for key in header_row
            }
            break
    return csv_map


def _nucc_taxonomy_row(taxonomy_row: dict, csv_map: dict[str, str]) -> dict:
    """Normalize one non-empty NUCC taxonomy row for staging."""
    taxonomy_dict = {
        mapped_key: taxonomy_row[key] or None
        for key, mapped_key in csv_map.items()
    }
    taxonomy_dict['int_code'] = return_checksum([taxonomy_dict['code']], crc=32)
    return taxonomy_dict


async def _stage_nucc_taxonomy_rows(
    ctx: dict,
    task: dict,
    tmp_filename: str,
    csv_map: dict[str, str],
    nucc_taxonomy_cls,
    *,
    test_mode: bool,
    run_id: str,
    source_file: str,
) -> int:
    """Parse and stage taxonomy rows, retaining original cancellation and batch points."""
    count = 0
    row_list = []
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
            row_list.append(_nucc_taxonomy_row(taxonomy_row, csv_map))
            if count % 9999 == 0:
                await raise_if_cancelled(ctx, task)
                await push_objects(row_list, nucc_taxonomy_cls)
                row_list.clear()
    await raise_if_cancelled(ctx, task)
    await push_objects(row_list, nucc_taxonomy_cls)
    print(f"Processed: {count}")
    return count


def _report_nucc_source_progress(
    run_id: str,
    source_file: str,
    *,
    file_index: int,
    file_count: int,
    completed: bool,
) -> None:
    """Publish a source-file progress event for a tracked NUCC run."""
    enqueue_live_progress(
        run_id=run_id,
        importer="nucc",
        status="running",
        phase="nucc source processed" if completed else "nucc downloading source",
        unit="files",
        done=file_index + int(completed),
        total=file_count,
        message=(
            f"processed file {file_index + 1}/{file_count}"
            if completed
            else f"downloading file {file_index + 1}/{file_count}"
        ),
        label=source_file,
    )


async def _process_nucc_source(
    ctx: dict,
    task: dict,
    source_file: str,
    *,
    import_date: str,
    file_index: int,
    file_count: int,
) -> None:
    """Download, parse, and stage one NUCC source file."""
    context = ctx['context']
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    test_mode = bool(context.get('test_mode'))
    if run_id:
        _report_nucc_source_progress(
            run_id,
            source_file,
            file_index=file_index,
            file_count=file_count,
            completed=False,
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
        csv_map = await _read_nucc_csv_map(tmp_filename)
        nucc_taxonomy_cls = make_class(NUCCTaxonomy, import_date)
        count = await _stage_nucc_taxonomy_rows(
            ctx,
            task,
            tmp_filename,
            csv_map,
            nucc_taxonomy_cls,
            test_mode=test_mode,
            run_id=run_id,
            source_file=source_file,
        )
        context["run"] = context.get("run", 0) + 1
        context["rows"] = count
        if run_id:
            _report_nucc_source_progress(
                run_id,
                source_file,
                file_index=file_index,
                file_count=file_count,
                completed=True,
            )


async def process_nucc_data(ctx, task=None):
    """Process one queued NUCC taxonomy import task."""
    task = task or {}
    import_date, run_id, test_mode = await _prepare_nucc_import(ctx, task)
    selected_files = await _discover_nucc_source_files(test_mode)
    _report_nucc_sources_discovered(run_id, selected_files)
    for file_index, source_file in enumerate(selected_files):
        await _process_nucc_source(
            ctx,
            task,
            source_file,
            import_date=import_date,
            file_index=file_index,
            file_count=len(selected_files),
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
    context = ctx.get("context") or {}
    if not context.get("run"):
        return
    import_date = ctx['import_date']
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

    terminal_progress_by_name = {
        "unit": "rows",
        "done": stage_rows,
        "total": stage_rows,
        "pct": 100,
        "message": "succeeded",
        "phase": "nucc published",
    }
    terminal_metrics_by_name = {"rows": stage_rows}
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="nucc published",
        progress_message="succeeded",
        progress=terminal_progress_by_name,
        metrics=terminal_metrics_by_name,
    )
    print_time_info(ctx['context']['start'])
    return {**terminal_metrics_by_name, "terminal_progress": terminal_progress_by_name}


shutdown = publish_nucc_generation
shutdown.__name__ = "shutdown"


async def main(test_mode: bool = False):
    """Run the NUCC taxonomy import entry point."""
    redis = await create_pool(build_redis_settings(),
                              job_serializer=serialize_job,
                              job_deserializer=deserialize_job)
    await redis.enqueue_job('process_data', {'test_mode': test_mode}, _queue_name=NUCC_QUEUE_NAME)
