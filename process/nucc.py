# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import hashlib
import os
import re
import tempfile
from dataclasses import replace
from pathlib import Path, PurePath
from uuid import uuid4

from aiocsv import AsyncDictReader
from aiofiles import open as async_open
from arq import create_pool
from sqlalchemy import text

from db.connection import Database, init_db
from db.models import NUCCTaxonomy, db
from process import nucc_protected_source as protected_source
from process import reference_family_archive as native
from process.control_cancel import raise_if_cancelled
from process.ext.utils import (
    download_it,
    download_it_and_save,
    ensure_database,
    make_class,
    print_time_info,
    return_checksum,
)
from process.import_status_events import isoformat_utc
from process.live_progress import enqueue_live_progress
from process.provider_directory_cms_native_inputs import prepare_local_native_input_publication
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

latin_pattern = re.compile(r"[^\x00-\x7f]")

TEST_NUCC_ROWS = 500
TEST_NUCC_MAX_FILES = 1
NUCC_QUEUE_NAME = "arq:NUCC"


def is_test_mode(ctx: dict) -> bool:
    """Return whether the worker is running in test mode."""
    return bool(ctx.get("context", {}).get("test_mode"))


def _nucc_database(ctx):
    return ctx.get("_nucc_database", db)


def _nucc_test_database_name():
    suffix = os.getenv("HLTHPRT_TEST_DATABASE_SUFFIX")
    if not suffix or suffix != suffix.strip():
        raise ValueError("taxonomy test mode requires an isolated database suffix")
    return os.getenv("HLTHPRT_DB_DATABASE", "postgres") + suffix


async def _prepare_nucc_import(ctx: dict, task: dict) -> tuple[str, str, bool]:
    """Initialize one NUCC import and return its execution settings."""
    await raise_if_cancelled(ctx, task)
    ctx["import_date"]
    ctx.setdefault("context", {})
    context = ctx["context"]
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    test_mode = bool(task.get("test_mode", context.get("test_mode", False)))
    context["test_mode"] = test_mode
    if test_mode:
        if run_id:
            raise ValueError("taxonomy test mode requires an isolated unmanaged job")
        database = Database()
        database._database_override = _nucc_test_database_name()
        ctx["_nucc_database"] = database
        await database.connect()
    else:
        await ensure_database(False)
    async with _nucc_database(ctx).transaction() as session:
        protected = await protected_source.is_protected_source(session)
        if protected and test_mode:
            raise ValueError("protected taxonomy test mode requires an isolated database")
        if not protected and not test_mode:
            await native.bind_nucc_native_attempt(session, ctx, schema_name=_nucc_schema())
    if protected:
        context["nucc_source_stage_bound"] = await protected_source.wait_nucc_source_stage(_nucc_database(ctx), ctx)
    elif context.get("nucc_native_stage") is None:
        await _prepare_plain_stage(ctx)
    context["nucc_source_files"] = []
    return ctx["import_date"], run_id, test_mode


def _nucc_schema():
    """Use the same configured schema for preparation, COPY and handoff."""
    return os.environ.get("HLTHPRT_DB_SCHEMA") or "mrf"


async def _write_nucc_batch(ctx, model, rows):
    """Use bounded native COPY for every admitted producer, without INSERT fallback."""
    if not rows:
        return
    context = ctx.get("context") or {}
    async with _nucc_database(ctx).transaction() as session:
        if context.get("nucc_source_stage_bound") is not None:
            await protected_source.copy_nucc_source_batch(session, context["nucc_source_stage_bound"], rows)
        elif (stage := context.get("nucc_native_stage")) is not None:
            await native.copy_nucc_native_batch(session, stage, model, rows)
        else:
            stage = context["nucc_plain_stage"]
            await _require_plain_stage(session, stage)
            columns = tuple(NUCCTaxonomy.__table__.columns.keys())
            await native.native_copy_record_batch(
                session,
                NUCCTaxonomy,
                schema_name=stage["schema"],
                table_name=stage["name"],
                columns=columns,
                records=[tuple(row.get(column) for column in columns) for row in rows],
            )


async def _discover_nucc_source_files(test_mode: bool) -> list[str]:
    """Download the NUCC index and select source files for this run."""
    html_source = await download_it(
        os.environ["HLTHPRT_NUCC_DOWNLOAD_URL_DIR"] + os.environ["HLTHPRT_NUCC_DOWNLOAD_URL_FILE"]
    )
    source_files = re.findall(r"\"(.*?nucc_taxonomy.*?\.csv)\"", html_source)
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
    async with async_open(tmp_filename, "r", encoding="utf-8-sig", newline="") as afp:
        async for header_row in AsyncDictReader(afp, delimiter=","):
            csv_map = {key: re.sub(r"\(.*\)", r"", key.lower()).strip().replace(" ", "_") for key in header_row}
            break
    return csv_map


def _nucc_taxonomy_row(taxonomy_row: dict, csv_map: dict[str, str]) -> dict:
    """Normalize one non-empty NUCC taxonomy row for staging."""
    taxonomy_dict = {mapped_key: taxonomy_row[key] or None for key, mapped_key in csv_map.items()}
    taxonomy_dict["int_code"] = return_checksum([taxonomy_dict["code"]], crc=32)
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
    async with async_open(tmp_filename, "r", encoding="utf-8-sig", newline="") as afp:
        async for taxonomy_row in AsyncDictReader(afp, delimiter=","):
            if not taxonomy_row["Code"]:
                continue
            if test_mode and count >= TEST_NUCC_ROWS:
                break
            count += 1
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
            if len(row_list) == protected_source.MAX_BATCH_ROWS:
                await raise_if_cancelled(ctx, task)
                await _write_nucc_batch(ctx, nucc_taxonomy_cls, row_list)
                row_list.clear()
    await raise_if_cancelled(ctx, task)
    await _write_nucc_batch(ctx, nucc_taxonomy_cls, row_list)
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
    context = ctx["context"]
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    test_mode = bool(context.get("test_mode"))
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
        file_name = source_file.split("/")[-1]
        tmp_filename = str(PurePath(str(tmpdirname), file_name))
        await download_it_and_save(
            os.environ["HLTHPRT_NUCC_DOWNLOAD_URL_DIR"] + source_file,
            tmp_filename,
            chunk_size=10 * 1024 * 1024,
            cache_dir="/tmp",
        )
        print(f"Downloaded: {source_file}")
        input_by_field = _nucc_input_fingerprint(tmp_filename, source_file)
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
        context["nucc_source_files"].append({**input_by_field, "row_count": count})
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


def _nucc_input_fingerprint(filename, source_file):
    """Bound actual input bytes before parsing and retain their digest for the Source census."""
    if not 0 < Path(filename).stat().st_size <= 8 * 1024**2:
        raise native.ReferenceFamilyArchiveError("NUCC source file exceeds its bounds")
    source_bytes = Path(filename).read_bytes()
    return {
        "url": os.environ["HLTHPRT_NUCC_DOWNLOAD_URL_DIR"] + source_file,
        "sha256": hashlib.sha256(source_bytes).hexdigest(),
        "bytes": len(source_bytes),
    }


async def process_nucc_data(ctx, task=None):
    """Process one queued NUCC taxonomy import task."""
    task = task or {}
    context = ctx.get("context") or {}
    is_tracked = context.get("_control_attempt_id") is not None
    test_mode = bool(task.get("test_mode", context.get("test_mode", False)))
    if not is_tracked and not test_mode:
        await protected_source.create_managed_source_run(test_mode=False, queue_context=ctx, task=task)
        return 1
    job_context_by_field = ctx if is_tracked else {**ctx, "context": {**context, "run": 0}}
    return await _process_admitted_nucc_data(job_context_by_field, task, publish_here=not is_tracked)


async def _process_admitted_nucc_data(ctx, task, *, publish_here):
    """Keep each admitted candidate local to its real control attempt or isolated test job."""
    try:
        import_date, run_id, test_mode = await _prepare_nucc_import(ctx, task)
        selected_files = await _discover_nucc_source_files(test_mode)
        if not selected_files:
            raise native.ReferenceFamilyArchiveError("NUCC source files are unavailable")
        # The index includes historical releases; one snapshot uses the first selected release.
        selected_files = selected_files[:1]
        _report_nucc_sources_discovered(run_id, selected_files)
        await _process_nucc_source(
            ctx,
            task,
            selected_files[0],
            import_date=import_date,
            file_index=0,
            file_count=len(selected_files),
        )
        if publish_here:
            await publish_nucc_generation(ctx)
        return 1
    except BaseException:
        if not (ctx.get("context") or {}).get("nucc_native_commit_unknown"):
            await _cleanup_plain_stage(ctx)
        raise
    finally:
        if (database := ctx.get("_nucc_database")) is not None:
            await database.disconnect()


process_data = process_nucc_data
process_data.__name__ = "process_data"


async def startup(ctx):
    """Initialize resources required by the NUCC worker."""
    loop = asyncio.get_event_loop()
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = False
    ctx["import_date"] = datetime.datetime.utcnow().strftime("%Y%m%d")
    await init_db(db, loop)
    await ensure_database(False)
    print("Preparing done")


async def publish_nucc_generation(ctx):
    """Finalize the NUCC run and release worker resources."""
    context = ctx.get("context") or {}
    if not context.get("run"):
        return
    if not context.get("test_mode"):
        await ensure_database(False)
    if context.get("nucc_source_stage_bound") is not None:
        return await _handoff_protected_source(ctx)
    if context.get("nucc_native_stage") is not None:
        return await _handoff_nucc_generation(ctx)
    return await _publish_plain_generation(ctx)


def _plain_relation(stage):
    return f"{native._quoted(stage['schema'])}.{native._quoted(stage['name'])}"


async def _plain_run(session, ctx):
    """Fence the original managed attempt before candidate or publication work."""
    if not (ctx.get("context") or {}).get("_control_attempt_id"):
        return None
    run, attempt, started, _suffix = native._nucc_attempt(ctx)
    observed = (
        (
            await session.execute(
                text(f"""SELECT node_id,status,progress,metrics,heartbeat_at,finished_at
 FROM {native._quoted(_nucc_schema())}.import_run WHERE run_id=:run AND importer='nucc'
 AND engine='healthcare-mrf-api' FOR UPDATE"""),
                {"run": run},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        observed is None
        or observed["status"] != "running"
        or any(
            (observed["progress"] or {}).get(key) != value
            for key, value in (("attempt_id", attempt), ("attempt_started_at", started))
        )
        or (observed["metrics"] or {}).get("nucc_handoff") is not None
    ):
        raise native.ReferenceFamilyArchiveError("NUCC original plain attempt changed")
    return observed


async def _prepare_plain_stage(ctx):
    """Allocate only this actual attempt's ordinary heap; defer every index until COPY completes."""
    context = ctx["context"]
    suffix = native._nucc_attempt(ctx)[3] if context.get("_control_attempt_id") else uuid4().hex
    stage_by_field = {"schema": _nucc_schema(), "name": "nucc_taxonomy_s_" + suffix}
    native._schema_name(stage_by_field["schema"])
    async with _nucc_database(ctx).transaction() as session:
        await _plain_run(session, ctx)
        await native.require_nucc_unretained_rotation(session, schema_name=stage_by_field["schema"])
        await native._create_model_heaps(
            session,
            native.reference_family_spec("nucc"),
            stage_by_field["schema"],
            create_indexes=False,
            ordinary_heaps=True,
            physical_names={"nucc_taxonomy": stage_by_field["name"]},
        )
        stage_by_field["oid"] = await native._relation_oid(session, stage_by_field["schema"], stage_by_field["name"])
        stage_by_field["owner_oid"] = await session.scalar(
            text("SELECT (SELECT oid FROM pg_roles WHERE rolname=current_user)::bigint")
        )
        if await native._relation_oid(session, stage_by_field["schema"], "nucc_taxonomy") is None:
            await _nucc_database(ctx).create_table(NUCCTaxonomy.__table__, checkfirst=True)
        stage_by_field["previous_oid"] = await native._relation_oid(session, stage_by_field["schema"], "nucc_taxonomy")
        await _require_plain_stage(session, stage_by_field)
        context["nucc_plain_stage"] = stage_by_field
    ctx["import_date"] = stage_by_field["name"].removeprefix("nucc_taxonomy_")


async def _require_plain_stage(session, stage):
    """Reject a changed, attached, inherited, hooked or differently owned ordinary candidate."""
    observed = await session.scalar(
        text("""SELECT c.oid=:oid AND c.relowner=:owner AND c.relkind='r'
 AND c.relpersistence='p' AND NOT c.relispartition AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
 AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AND session_user=current_user
 AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
 FROM pg_class c WHERE c.oid=to_regclass(:relation)"""),
        {"oid": stage["oid"], "owner": stage["owner_oid"], "relation": _plain_relation(stage)},
    )
    if observed is not True:
        raise native.ReferenceFamilyArchiveError("NUCC ordinary candidate identity changed")
    await native.require_native_read_catalog(session, (stage["oid"],))


def _plain_result(rows):
    return {
        "rows": rows,
        "terminal_progress": {
            "unit": "rows",
            "done": rows,
            "total": rows,
            "pct": 100,
            "message": "succeeded",
            "phase": "nucc published",
        },
    }


def _plain_committed_times(times_by_field):
    fields = ("heartbeat_at", "finished_at")
    if any(type(times_by_field.get(field)) is not datetime.datetime for field in fields):
        raise native.ReferenceFamilyArchiveError("NUCC committed database timestamps differ")
    return {field: isoformat_utc(times_by_field[field], timespec="microseconds") for field in fields}


async def _commit_plain_run(session, ctx, result_by_field):
    """Commit terminal success in the same native transaction as the table rotation."""
    if not (ctx.get("context") or {}).get("_control_attempt_id"):
        return None
    import json

    run, attempt, started, _suffix = native._nucc_attempt(ctx)
    progress_by_field = {**result_by_field["terminal_progress"], "attempt_id": attempt, "attempt_started_at": started}
    times = (
        (
            await session.execute(
                text(f"""UPDATE {native._quoted(_nucc_schema())}.import_run SET
 status='succeeded',phase_detail='nucc published',progress=CAST(:progress AS json),error=NULL,
 metrics=(COALESCE(metrics::jsonb,'{{}}'::jsonb)||jsonb_build_object('rows',CAST(:rows AS bigint)))::json,
 heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',finished_at=clock_timestamp() AT TIME ZONE 'UTC'
 WHERE run_id=:run AND importer='nucc' AND engine='healthcare-mrf-api' AND status='running'
 AND progress->>'attempt_id'=:attempt AND progress->>'attempt_started_at'=:started
 AND metrics->>'nucc_handoff' IS NULL RETURNING heartbeat_at,finished_at"""),
                {
                    "run": run,
                    "attempt": attempt,
                    "started": started,
                    "progress": json.dumps(progress_by_field),
                    "rows": result_by_field["rows"],
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    if times is None:
        raise native.ReferenceFamilyArchiveError("NUCC plain publication attempt changed")
    return _plain_committed_times(times)


async def _read_plain_commit(ctx, stage, result_by_field):
    """An exact succeeded original attempt and its actual published heap resolve a lost COMMIT ACK."""
    run, attempt, started, _suffix = native._nucc_attempt(ctx)
    async with _nucc_database(ctx).transaction() as session:
        observed = (
            (
                await session.execute(
                    text(f"""SELECT status,progress,metrics,heartbeat_at,finished_at,error
 FROM {native._quoted(_nucc_schema())}.import_run WHERE run_id=:run AND importer='nucc'
 AND engine='healthcare-mrf-api' FOR UPDATE"""),
                    {"run": run},
                )
            )
            .mappings()
            .one_or_none()
        )
        if observed is None:
            raise native.ReferenceFamilyArchiveError("NUCC publication readback lacks its original run")
        if (
            observed["status"] == "running"
            and (observed["progress"] or {}).get("attempt_id") == attempt
            and (observed["progress"] or {}).get("attempt_started_at") == started
            and await native._relation_oid(session, stage["schema"], stage["name"]) == stage["oid"]
        ):
            return None
        if (
            observed["status"] != "succeeded"
            or observed["finished_at"] is None
            or observed["error"] is not None
            or (observed["metrics"] or {}).get("rows") != result_by_field["rows"]
            or (observed["progress"] or {}).get("attempt_id") != attempt
            or (observed["progress"] or {}).get("attempt_started_at") != started
            or (observed["progress"] or {}).get("phase") != "nucc published"
            or await native._relation_oid(session, stage["schema"], "nucc_taxonomy") != stage["oid"]
        ):
            raise native.ReferenceFamilyArchiveError("NUCC publication readback differs")
        published_by_field = {**stage, "name": "nucc_taxonomy"}
        await _require_plain_stage(session, published_by_field)
        if (await native._validate_nucc_table_set(session, stage["schema"], "nucc_taxonomy"))[
            "row_count"
        ] != result_by_field["rows"]:
            raise native.ReferenceFamilyArchiveError("NUCC published census changed")
        return _plain_committed_times(observed)


async def _finish_plain_stage(session, stage, expected_rows):
    """Finish indexes and set checks under the isolated candidate lock before the short serving-table cutover."""
    await _require_plain_stage(session, stage)
    await session.execute(text(f"LOCK TABLE ONLY {_plain_relation(stage)} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    await native._create_model_indexes(
        session,
        native.reference_family_spec("nucc"),
        stage["schema"],
        create_constraints=True,
        physical_names={"nucc_taxonomy": stage["name"]},
    )
    count = (await native._validate_nucc_table_set(session, stage["schema"], stage["name"]))["row_count"]
    if count != expected_rows:
        raise native.ReferenceFamilyArchiveError("NUCC parsed and staged census differ")
    await native.require_nucc_unretained_rotation(session, schema_name=stage["schema"])
    await session.execute(
        text(f"LOCK TABLE ONLY {native._quoted(stage['schema'])}.nucc_taxonomy IN ACCESS EXCLUSIVE MODE")
    )
    if await native._relation_oid(session, stage["schema"], "nucc_taxonomy") != stage["previous_oid"]:
        raise native.ReferenceFamilyArchiveError("NUCC plain predecessor changed")
    return count


async def _publish_plain_generation(ctx):
    """Finish the isolated heap and atomically rotate it with the exact Source attempt."""
    context = ctx["context"]
    stage = context["nucc_plain_stage"]
    result_by_field = None
    times = None
    try:
        async with _nucc_database(ctx).transaction() as session:
            await _plain_run(session, ctx)
            count = await _finish_plain_stage(session, stage, context["rows"])
            await prepare_local_native_input_publication(
                session,
                ledger_schema=stage["schema"],
                relations=(("nucc_taxonomy", stage["name"]),),
            )
            schema = native._quoted(stage["schema"])
            await session.execute(text(f"DROP TABLE IF EXISTS {schema}.nucc_taxonomy_old"))
            await session.execute(text(f"ALTER TABLE IF EXISTS {schema}.nucc_taxonomy RENAME TO nucc_taxonomy_old"))
            await session.execute(text(f"ALTER TABLE {_plain_relation(stage)} RENAME TO nucc_taxonomy"))
            result_by_field = _plain_result(count)
            times = await _commit_plain_run(session, ctx, result_by_field)
    except BaseException:
        if result_by_field is None or not context.get("_control_attempt_id"):
            await _cleanup_plain_stage(ctx)
            raise
        context["nucc_native_commit_unknown"] = True
        times = await _read_plain_commit(ctx, stage, result_by_field)
        if times is None:
            context.pop("nucc_native_commit_unknown", None)
            await _cleanup_plain_stage(ctx)
            raise
    context.pop("nucc_native_commit_unknown", None)
    if times is not None:
        context.update(
            {
                "control_run_terminal_committed": True,
                "preserve_control_run_finished_at": True,
                "_control_committed_result": result_by_field,
                "_control_committed_heartbeat_at": times["heartbeat_at"],
                "_control_committed_finished_at": times["finished_at"],
            }
        )
    context["run"] = 0
    print_time_info(context["start"])
    return result_by_field


async def _cleanup_plain_stage(ctx):
    """Drop only this failed attempt's verified unpublished heap; retain uncertain commits."""
    stage = (ctx.get("context") or {}).get("nucc_plain_stage")
    if stage is None:
        return
    async with _nucc_database(ctx).transaction() as session:
        if await native._relation_oid(session, stage["schema"], stage["name"]) is None:
            return
        if await native._relation_oid(session, stage["schema"], "nucc_taxonomy") == stage["oid"]:
            return
        await _require_plain_stage(session, stage)
        await session.execute(text(f"DROP TABLE {_plain_relation(stage)}"))


async def _handoff_protected_source(ctx):
    """Ready input is not publication; only native closed readback arms the handoff guard."""
    context = ctx["context"]
    stage = context["nucc_source_stage_bound"]
    try:
        async with _nucc_database(ctx).transaction() as session:
            ready = await protected_source.record_nucc_source_ready(session, stage, context["nucc_source_files"])
            stage = replace(stage, ready=ready)
            context["nucc_source_stage_bound"] = stage
            context["nucc_native_commit_unknown"] = True
    except BaseException:
        if stage.ready is None:
            raise
    try:
        handoff = await protected_source.wait_nucc_source_handoff(_nucc_database(ctx), stage)
    except protected_source.NuccSourceReadyNotCommittedError:
        context.pop("nucc_native_commit_unknown", None)
        raise
    result_by_field = {"rows": handoff["row_count"], "nucc_handoff": handoff}
    context.pop("nucc_native_commit_unknown", None)
    context["_control_committed_result"] = result_by_field
    context["control_run_handoff_committed"] = True
    return result_by_field


async def _handoff_nucc_generation(ctx):
    """Resolve an uncertain COMMIT before the control wrapper can change the real run."""
    context_by_field = ctx["context"]
    handoff_by_field = None
    try:
        async with _nucc_database(ctx).transaction() as session:
            stage_model = make_class(NUCCTaxonomy, ctx["import_date"])
            ready = await native.prepare_nucc_native_publication_stage(
                session,
                stage_model=stage_model,
                schema_name=_nucc_schema(),
                native_stage=context_by_field["nucc_native_stage"],
            )
            handoff_by_field = await native.record_nucc_native_handoff(
                session,
                ctx,
                schema_name=_nucc_schema(),
                stage_model=stage_model,
                ready=ready,
            )
    except BaseException:
        if handoff_by_field is None:
            raise
        try:
            persisted = await native.reconcile_nucc_native_handoff(_nucc_database(ctx), handoff_by_field)
        except BaseException:
            # A lost readback is not evidence that the owning COMMIT rolled back.
            context_by_field["nucc_native_commit_unknown"] = True
            raise
        if persisted is None:
            raise
    result_by_field = {"rows": handoff_by_field["row_count"], "nucc_handoff": handoff_by_field}
    context_by_field["_control_committed_result"] = result_by_field
    context_by_field["control_run_handoff_committed"] = True
    return result_by_field


shutdown = publish_nucc_generation
shutdown.__name__ = "shutdown"


async def main(test_mode: bool = False):
    """Run the NUCC taxonomy import entry point."""
    if not test_mode:
        return await protected_source.create_managed_source_run(test_mode=test_mode)
    _nucc_test_database_name()
    redis = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    await redis.enqueue_job("process_data", {"test_mode": test_mode}, _queue_name=NUCC_QUEUE_NAME)
