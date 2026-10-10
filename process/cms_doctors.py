# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import logging
import os
import secrets
import tempfile
from pathlib import Path, PurePath

from arq import create_pool

from db.models import CMSDoctorEducation, CMSDoctorGroupSite, DoctorClinicianAddress, db
from process.cms_doctors_artifact import (
    retain_doctors_artifact,
    validate_doctors_artifact_root,
    verify_doctors_artifact,
)
from process.cms_doctors_education import (
    discard_education_stage,
    import_doctor_education,
    open_doctors_csv,
    swap_education_stage,
    validate_education_stage,
)
from process.cms_doctors_groups import (
    discard_group_site_stage,
    import_group_site_rows,
    swap_group_site_stage,
    validate_group_site_stage,
)
from process.cms_doctors_organizations import bind_group_site_organizations
from process.cms_doctors_rows import doctor_address_row
from process.cms_doctors_sites import bind_cms_doctors_sites
from process.cms_doctors_source_provenance import read_doctors_source_provenance
from process.control_cancel import raise_if_cancelled
from process.control_lifecycle import mark_control_run
from process.entity_address_cutover_contract import lock_live_serving_relations, wait_for_publication_lock
from process.ext.address_canon import resolve_into_archive, source_enabled, stamp_address_keys
from process.ext.utils import ensure_database, make_class, my_init_db, print_time_info, push_objects
from process.redis_config import build_redis_settings
from process.reference_family_archive import _lock_family
from process.reference_family_result_generation import publish_local_reference_family_generation
from process.serialization import deserialize_job, serialize_job

logger = logging.getLogger(__name__)

CMS_DOCTORS_QUEUE_NAME = "arq:CMSDoctors"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

CMS_PROVIDER_DATA_JSON_URL = "https://data.cms.gov/provider-data/data.json"
CMS_PROVIDER_METASTORE_DATASET_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{dataset_id}"
)
DEFAULT_DOCTORS_DATASET_ID = os.getenv("HLTHPRT_CMS_DOCTORS_DATASET_ID", "mj5m-pzi6").lower()
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_MIN_ROWS = 10_000
DEFAULT_TEST_ROWS = 5000
CMS_DOCTORS_ADDRESS_FIELDS = {
    "first_line": "address_line1",
    "second_line": "address_line2",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "country": "'US'",
}


def _stage_index_name(stage_table: str, index_name: str) -> str:
    return _archived_identifier(f"{stage_table}_idx_{index_name}", suffix="")


async def _create_stage_indexes(stage_cls, db_schema: str) -> None:
    if hasattr(stage_cls, "__my_index_elements__") and stage_cls.__my_index_elements__:
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_stage_index_name(stage_cls.__tablename__, 'primary')} "
            f"ON {db_schema}.{stage_cls.__tablename__} "
            f"({', '.join(stage_cls.__my_index_elements__)});"
        )

    if hasattr(stage_cls, "__my_additional_indexes__") and stage_cls.__my_additional_indexes__:
        for index in stage_cls.__my_additional_indexes__:
            index_name = index.get("name", "_".join(index.get("index_elements")))
            using = f"USING {index.get('using')} " if index.get("using") else ""
            where = f" WHERE {index.get('where')}" if index.get("where") else ""
            await db.status(
                f"CREATE INDEX IF NOT EXISTS "
                f"{_stage_index_name(stage_cls.__tablename__, index_name)} "
                f"ON {db_schema}.{stage_cls.__tablename__} {using}"
                f"({', '.join(index.get('index_elements'))}){where};"
            )


def _normalize_import_id(raw: str | None) -> str:
    if raw:
        cleaned = "".join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S") + secrets.token_hex(4)


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _validate_schema_name(schema: str) -> str:
    cleaned = (schema or "").strip()
    if not cleaned or not (cleaned[0].isalpha() or cleaned[0] == "_"):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not all(ch.isalnum() or ch == "_" for ch in cleaned):
        raise ValueError(f"Invalid schema name: {schema!r}")
    return cleaned


async def _ensure_schema_exists(db_schema: str) -> None:
    db_schema = _validate_schema_name(db_schema)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        exists = bool(await db.scalar(f"SELECT to_regnamespace('{db_schema}') IS NOT NULL;"))
        if exists:
            logger.warning(
                "Schema %s already exists but CREATE SCHEMA failed (%s); continuing",
                db_schema,
                exc,
            )
            return
        raise


def _distribution_urls(dataset: dict) -> list[str]:
    urls: list[str] = []
    for dist in dataset.get("distribution", []):
        url = str(dist.get("downloadURL", "")).strip()
        if url and (url.lower().endswith((".csv", ".zip")) or "dac_nationaldownloadablefile" in url.lower()):
            urls.append(url)
    return urls


async def _first_reachable_url(client, urls: list[str]) -> str | None:
    for url in urls:
        try:
            async with client.head(url, allow_redirects=True, timeout=60) as response:
                if response.status < 400:
                    return url
                logger.warning("CMS Doctors source candidate returned HTTP %s: %s", response.status, url)
        except Exception as exc:
            logger.warning("CMS Doctors source candidate probe failed: %s (%s)", url, exc)
    return None


async def _fetch_doctors_download_url(client) -> str:
    metastore_url = CMS_PROVIDER_METASTORE_DATASET_URL.format(dataset_id=DEFAULT_DOCTORS_DATASET_ID)
    try:
        async with client.get(metastore_url, timeout=60) as response:
            response.raise_for_status()
            dataset = await response.json(content_type=None)
        url = await _first_reachable_url(client, _distribution_urls(dataset))
        if url:
            return url
    except Exception as exc:
        logger.warning("Could not resolve CMS Doctors metastore URL, falling back to catalog: %s", exc)

    async with client.get(CMS_PROVIDER_DATA_JSON_URL, timeout=60) as response:
        response.raise_for_status()
        catalog = await response.json(content_type=None)

    selected_dataset = None
    for dataset in catalog.get("dataset", []):
        identifier = str(dataset.get("identifier", "")).lower()
        landing_page = str(dataset.get("landingPage", "")).lower()
        title = str(dataset.get("title", "")).lower()
        description = str(dataset.get("description", "")).lower()
        if (
            identifier == DEFAULT_DOCTORS_DATASET_ID
            or f"/dataset/{DEFAULT_DOCTORS_DATASET_ID}" in landing_page
            or (
                DEFAULT_DOCTORS_DATASET_ID == "mj5m-pzi6"
                and "national downloadable file" in title
                and "doctors and clinicians" in description
            )
        ):
            selected_dataset = dataset
            break

    if not selected_dataset:
        raise ValueError("Could not find CMS Doctors dataset in provider-data catalog.")

    candidates = _distribution_urls(selected_dataset)
    url = await _first_reachable_url(client, candidates)
    if url:
        return url

    raise ValueError("Could not find CMS Doctors CSV/ZIP download URL in dataset.")


async def _consume_doctors_reader(
    reader,
    *,
    ctx,
    task,
    stage_cls,
    batch_size: int,
    test_mode: bool,
    test_row_limit: int,
) -> int:
    """Normalize reader rows and persist bounded, deduplicated batches."""
    accepted_rows = 0
    provider_batch_rows = []
    seen_checksums: set[int] = set()
    observation_time = ctx.get("context", {}).get("education", {}).get("downloaded_at")
    now = datetime.datetime.fromisoformat(observation_time) if observation_time else datetime.datetime.utcnow()
    for provider_row in reader:
        address_row = doctor_address_row(provider_row, now)
        if address_row is None:
            continue
        address_checksum = address_row["address_checksum"]
        if address_checksum in seen_checksums:
            continue
        seen_checksums.add(address_checksum)
        provider_batch_rows.append(address_row)
        if len(provider_batch_rows) >= batch_size:
            await raise_if_cancelled(ctx, task)
            await push_objects(provider_batch_rows, stage_cls)
            accepted_rows += len(provider_batch_rows)
            provider_batch_rows.clear()
        if test_mode and accepted_rows + len(provider_batch_rows) >= test_row_limit:
            break
    if provider_batch_rows:
        await raise_if_cancelled(ctx, task)
        await push_objects(provider_batch_rows, stage_cls)
        accepted_rows += len(provider_batch_rows)
    return accepted_rows


async def _import_doctors_source(
    source_path: str,
    *,
    ctx,
    task,
    stage_cls,
    batch_size: int,
    test_mode: bool,
    test_row_limit: int,
) -> int:
    """Open a downloaded CSV or ZIP and stream its rows through one importer."""
    reader_kwargs_by_name = {
        "ctx": ctx,
        "task": task,
        "stage_cls": stage_cls,
        "batch_size": batch_size,
        "test_mode": test_mode,
        "test_row_limit": test_row_limit,
    }
    with open_doctors_csv(source_path) as reader:
        return await _consume_doctors_reader(
            reader,
            **reader_kwargs_by_name,
        )


async def _download_doctors_source(client, url: str, source_path: str) -> None:
    """Stream one CMS Doctors source into a temporary local file."""
    async with client.get(url, timeout=600) as response:
        response.raise_for_status()
        with open(source_path, "wb") as destination:
            async for chunk in response.content.iter_chunked(10 * 1024 * 1024):
                destination.write(chunk)


async def _stage_doctors_sidecars(source_path, url, ctx, task, test_mode, *, source_manifest=None):
    """Stage the retained source, education, and group-site records together."""
    if not test_mode and source_manifest is None:
        ctx["context"]["artifact"] = retain_doctors_artifact(source_path, url)
    manifest_kwargs = {"source_manifest": source_manifest} if source_manifest is not None else {}
    ctx["context"]["education"] = await import_doctor_education(
        source_path,
        url,
        ctx,
        task,
        source_manifest["dataset_id"] if source_manifest is not None else DEFAULT_DOCTORS_DATASET_ID,
        **manifest_kwargs,
    )
    if not test_mode and (
        ctx["context"]["artifact"]["content_sha256"] != ctx["context"]["education"]["content_sha256"]
    ):
        raise RuntimeError("cms_doctors_artifact_digest_mismatch")
    ctx["context"]["group_site"] = await import_group_site_rows(
        source_path,
        ctx,
        task,
        ctx["context"]["education"],
    )
    group_stage = make_class(CMSDoctorGroupSite, ctx["import_date"])
    await _create_stage_indexes(
        group_stage,
        _validate_schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"),
    )


async def _replay_doctors_source(ctx, task, stage_cls, batch_size, test_mode, test_row_limit):
    """Replay exact retained bytes with their original closed source observations."""
    original = read_doctors_source_provenance(task["cms_doctors_retained_source_provenance"])
    source_path = verify_doctors_artifact(original["artifact"])
    ctx["context"]["artifact"] = dict(original["artifact"])
    source_manifest_by_field = {
        key: field_value
        for key, field_value in original["education"].items()
        if key not in {"source_rows", "education_rows"}
    }
    await _stage_doctors_sidecars(
        source_path,
        original["artifact"]["source_url"],
        ctx,
        task,
        test_mode,
        source_manifest=source_manifest_by_field,
    )
    accepted_rows = await _import_doctors_source(
        source_path,
        ctx=ctx,
        task=task,
        stage_cls=stage_cls,
        batch_size=batch_size,
        test_mode=test_mode,
        test_row_limit=test_row_limit,
    )
    verify_doctors_artifact(original["artifact"])
    if not test_mode and (
        ctx["context"]["education"] != original["education"]
        or ctx["context"]["group_site"] != original["group_site"]
        or accepted_rows != original["rows"]
    ):
        raise RuntimeError("cms_doctors_retained_source_counts_changed")
    ctx["context"]["retained_source"] = original
    return accepted_rows


async def import_cms_doctors_data(ctx, task=None):
    """Import the current distribution or replay verified original source evidence."""

    task = task or {}
    await raise_if_cancelled(ctx, task)
    ctx.setdefault("context", {})

    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)
    if not test_mode:
        validate_doctors_artifact_root()

    import_date = ctx["import_date"]
    stage_cls = make_class(DoctorClinicianAddress, import_date)
    batch_size = int(os.getenv("HLTHPRT_CMS_DOCTORS_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))
    test_row_limit = int(os.getenv("HLTHPRT_CMS_DOCTORS_TEST_ROWS", str(DEFAULT_TEST_ROWS)))

    client = None
    accepted_rows = 0

    try:
        if "cms_doctors_retained_source_provenance" in task:
            accepted_rows = await _replay_doctors_source(ctx, task, stage_cls, batch_size, test_mode, test_row_limit)
        else:
            import aiohttp

            client = aiohttp.ClientSession()
            url = await _fetch_doctors_download_url(client)
            logger.info("Found CMS Doctors source: %s", url)

            # Download to temp file to avoid loading large files into memory
            with tempfile.TemporaryDirectory() as tmpdir:
                source_ext = ".zip" if url.lower().endswith(".zip") else ".csv"
                source_path = os.path.join(tmpdir, f"cms_doctors{source_ext}")

                await _download_doctors_source(client, url, source_path)
                await _stage_doctors_sidecars(source_path, url, ctx, task, test_mode)
                accepted_rows += await _import_doctors_source(
                    source_path,
                    ctx=ctx,
                    task=task,
                    stage_cls=stage_cls,
                    batch_size=batch_size,
                    test_mode=test_mode,
                    test_row_limit=test_row_limit,
                )
    except BaseException:
        await discard_group_site_stage(ctx)
        await discard_education_stage(ctx)
        raise
    finally:
        if client is not None:
            await client.close()

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
    logger.info("CMS Doctors import done: %d rows accepted", accepted_rows)


process_data = import_cms_doctors_data
process_data.__name__ = "process_data"


async def startup(ctx):
    """Initialize database and control-run context for CMS Doctors workers."""

    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = False
    await ensure_database(False)

    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    ctx["import_date"] = _normalize_import_id(override_import_id)
    import_date = ctx["import_date"]
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"

    stage_cls = make_class(DoctorClinicianAddress, import_date)

    await _ensure_schema_exists(db_schema)
    await db.create_table(stage_cls.__table__, checkfirst=False)
    await _create_stage_indexes(stage_cls, db_schema)

    logger.info("CMS Doctors startup ready: schema=%s import_date=%s", db_schema, import_date)


async def _resolve_cms_doctors_addresses(ctx, stage_cls, db_schema: str):
    if not source_enabled("cms_doctors"):
        return None

    async def _cancel_check():
        await raise_if_cancelled(ctx, {})

    await stamp_address_keys(
        stage_cls.__tablename__,
        CMS_DOCTORS_ADDRESS_FIELDS,
        schema=db_schema,
        cancel_check=_cancel_check,
    )
    address_stats = await resolve_into_archive(
        stage_cls.__tablename__,
        CMS_DOCTORS_ADDRESS_FIELDS,
        source_bit=2,
        priority=1,
        schema=db_schema,
        cancel_check=_cancel_check,
    )
    logger.info("CMS Doctors canonical address resolve complete: %s", address_stats)
    return address_stats


async def _publish_cms_doctors_stage(stage_cls, db_schema: str, import_date: str):
    """Keep ordinary publication on the same three-table transactional apply path."""
    max_attempts = 1 if db._transaction_binding() is not None else 4
    for attempt in range(1, max_attempts + 1):
        try:
            async with db.transaction():
                return await _apply_cms_doctors_stage(stage_cls, db_schema, import_date)
        except Exception as error:
            await wait_for_publication_lock(error, attempt, max_attempts=max_attempts)


async def _lock_cms_doctors_publication(session, stage_cls, db_schema, import_date):
    """Drain readers briefly, retaining immediate private-stage ownership checks."""
    models = (DoctorClinicianAddress, CMSDoctorEducation, CMSDoctorGroupSite)
    optional_names = tuple(name for model in models for name in (model.__main_table__, model.__main_table__ + "_old"))
    existing_names = (
        (
            await session.execute(
                db.text(
                    "SELECT name FROM unnest(CAST(:names AS text[])) AS relations(name) "
                    "WHERE to_regclass(format('%I.%I',CAST(:schema AS text),name)) IS NOT NULL"
                ),
                {"schema": db_schema, "names": list(optional_names)},
            )
        )
        .scalars()
        .all()
    )
    stages = (stage_cls.__tablename__, *(make_class(model, import_date).__tablename__ for model in models[1:]))
    await _lock_family(session, db_schema, tuple(sorted(stages)), "ACCESS EXCLUSIVE", nowait=True)
    live_names = {model.__main_table__ for model in models}
    await lock_live_serving_relations(db.status, db_schema, live_names.intersection(existing_names))
    retired_names = tuple(sorted(set(existing_names) - live_names))
    if retired_names:
        await _lock_family(session, db_schema, retired_names, "ACCESS EXCLUSIVE", nowait=True)


async def _apply_cms_doctors_stage(stage_cls, db_schema: str, import_date: str):
    """Lock and swap every Doctors relation without committing the owner."""
    if db._transaction_binding() is None:
        raise RuntimeError("cms_doctors_publication_requires_transaction")
    await _lock_cms_doctors_publication(db._transaction_binding().session, stage_cls, db_schema, import_date)
    return await _apply_locked_cms_doctors_stage(stage_cls, db_schema, import_date)


async def _apply_locked_cms_doctors_stage(stage_cls, db_schema: str, import_date: str):
    """Swap the already-locked family and advance authority in the owner's transaction."""
    if db._transaction_binding() is None:
        raise RuntimeError("cms_doctors_publication_requires_transaction")
    table = DoctorClinicianAddress.__main_table__
    await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
    await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
    await db.status(f"ALTER TABLE {db_schema}.{stage_cls.__tablename__} RENAME TO {table};")
    archived = _archived_identifier(f"{table}_idx_primary")
    await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived};")
    await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{table}_idx_primary RENAME TO {archived};")
    await db.status(
        f"ALTER INDEX IF EXISTS {db_schema}.{_stage_index_name(stage_cls.__tablename__, 'primary')} "
        f"RENAME TO {table}_idx_primary;"
    )
    for index in getattr(stage_cls, "__my_additional_indexes__", ()):
        index_name = index.get("name", "_".join(index.get("index_elements")))
        old_live_name = f"{table}_idx_{index_name}"
        archived_live_name = _archived_identifier(old_live_name)
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_live_name};")
        await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{old_live_name} RENAME TO {archived_live_name};")
        await db.status(
            f"ALTER INDEX IF EXISTS {db_schema}.{_stage_index_name(stage_cls.__tablename__, index_name)} "
            f"RENAME TO {old_live_name};"
        )
    await swap_education_stage(import_date, db_schema)
    await swap_group_site_stage(import_date, db_schema)
    return await publish_local_reference_family_generation(db, importer_id="cms-doctors", schema_name=db_schema)


async def _finish_cms_doctors_test_run(ctx, db_schema: str, stage_rows: int) -> dict:
    """Discard this test run's stages and report success without publishing."""
    for model in (DoctorClinicianAddress, CMSDoctorEducation, CMSDoctorGroupSite):
        stage_cls = make_class(model, ctx["import_date"])
        await db.status(f"DROP TABLE IF EXISTS {db_schema}.{stage_cls.__tablename__}")
    context = ctx.get("context") or {}
    context.pop("education_stage_owned", None)
    context.pop("group_site_stage_owned", None)
    metrics_by_name = {"rows": stage_rows, "education": context.get("education"), "published": False}
    await mark_control_run(
        str(context.get("control_run_id") or ctx.get("control_run_id") or ""),
        status="succeeded",
        phase_detail="cms-doctors test completed without publication",
        progress_message="succeeded",
        metrics=metrics_by_name,
    )
    return metrics_by_name


async def _validate_cms_doctors_publication_sources(import_date, db_schema, context):
    """Validate the staged family against the retained source artifact."""
    education_manifest = context.get("education")
    if not education_manifest:
        raise RuntimeError("cms_education_manifest_missing")
    await validate_education_stage(import_date, db_schema, education_manifest)
    group_receipt = context.get("group_site")
    if not group_receipt or group_receipt["source_rows"] != education_manifest["source_rows"]:
        raise RuntimeError("cms_group_site_source_rows_mismatch")
    await validate_group_site_stage(import_date, db_schema, group_receipt)
    artifact_receipt = context.get("artifact")
    if (
        not isinstance(artifact_receipt, dict)
        or artifact_receipt.get("content_sha256") != education_manifest["content_sha256"]
    ):
        raise RuntimeError("cms_doctors_artifact_receipt_mismatch")
    verify_doctors_artifact(artifact_receipt)
    return education_manifest, group_receipt


async def _prepare_cms_doctors_sources(ctx, stage_cls, db_schema, stage_rows):
    """Complete source, stable identity, and address preparation before any serving swap."""
    context = ctx.get("context") or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    if stage_rows < DEFAULT_MIN_ROWS:
        raise RuntimeError(f"CMS Doctors stage row count {stage_rows} below minimum {DEFAULT_MIN_ROWS}; aborting.")
    education_manifest, group_receipt = await _validate_cms_doctors_publication_sources(
        ctx["import_date"],
        db_schema,
        context,
    )
    organization_groups = await bind_group_site_organizations(ctx, ctx["import_date"], db_schema, group_receipt)
    sites = await bind_cms_doctors_sites(ctx, ctx["import_date"], db_schema, group_receipt)
    retained_source = context.get("retained_source")
    if retained_source and (
        organization_groups != retained_source["organization_groups"] or sites != retained_source["sites"]
    ):
        raise RuntimeError("cms_doctors_retained_source_binding_counts_changed")
    address_stats = await _resolve_cms_doctors_addresses(ctx, stage_cls, db_schema)
    await raise_if_cancelled(ctx, {"run_id": run_id})
    return {
        "rows": stage_rows,
        "education": education_manifest,
        "group_site": group_receipt,
        "organization_groups": organization_groups,
        "sites": sites,
        **({"artifact": context["artifact"]} if context.get("artifact") else {}),
        **({"address_resolve": address_stats.__dict__} if address_stats else {}),
    }


def _cms_doctors_terminal_progress(stage_rows):
    """Describe the completed publication for the control run."""
    return {
        "unit": "rows",
        "done": stage_rows,
        "total": stage_rows,
        "pct": 100,
        "message": "succeeded",
        "phase": "cms-doctors published",
    }


async def _publish_cms_doctors_generation(ctx):
    """Publish a completed CMS Doctors stage or record its terminal failure."""
    import_date = ctx.get("import_date")
    context = ctx.get("context") or {}
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    if not context.get("run"):
        logger.info("No CMS Doctors jobs ran; skipping shutdown.")
        return

    await ensure_database(bool(context.get("test_mode")))
    db_schema = _validate_schema_name(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    stage_cls = make_class(DoctorClinicianAddress, import_date)
    stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_cls.__tablename__};") or 0)
    if context.get("test_mode"):
        logger.info("CMS Doctors test mode: staged rows=%d", stage_rows)
        return await _finish_cms_doctors_test_run(ctx, db_schema, stage_rows)
    terminal_metrics_by_name = await _prepare_cms_doctors_sources(ctx, stage_cls, db_schema, stage_rows)
    await _publish_cms_doctors_stage(stage_cls, db_schema, import_date)
    context.pop("education_stage_owned", None)
    context.pop("group_site_stage_owned", None)

    logger.info("CMS Doctors publish complete: %d rows", stage_rows)
    print_time_info(context.get("start"))
    terminal_progress_by_name = _cms_doctors_terminal_progress(stage_rows)
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="cms-doctors published",
        progress_message="succeeded",
        progress=terminal_progress_by_name,
        metrics=terminal_metrics_by_name,
    )
    return {
        **terminal_metrics_by_name,
        "terminal_progress": terminal_progress_by_name,
    }


async def publish_cms_doctors_generation(ctx):
    """Publish the CMS family and release remaining owned sidecar stages."""
    try:
        return await _publish_cms_doctors_generation(ctx)
    finally:
        await discard_group_site_stage(ctx)
        await discard_education_stage(ctx)


shutdown = publish_cms_doctors_generation
shutdown.__name__ = "shutdown"


async def main(test_mode: bool = False, retained_source_manifest: str | None = None):
    """Queue the CMS Doctors import with the requested bounded test mode."""

    payload = {"test_mode": bool(test_mode)}
    if retained_source_manifest is not None:
        with Path(retained_source_manifest).open("rb") as manifest_file:
            manifest_bytes = manifest_file.read(64 * 1024 + 1)
        if len(manifest_bytes) > 64 * 1024:
            raise ValueError("cms_doctors_retained_source_manifest_too_large")
        source_provenance = manifest_bytes.decode("utf-8").strip()
        read_doctors_source_provenance(source_provenance)
        payload["cms_doctors_retained_source_provenance"] = source_provenance
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    await redis.enqueue_job("process_data", payload, _queue_name=CMS_DOCTORS_QUEUE_NAME)
