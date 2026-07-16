# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
import glob
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path, PurePath
from urllib.parse import unquote, urlparse

import ijson
import pylightxl as xl
from aiocsv import AsyncDictReader
from aiofile import async_open
from arq import create_pool
from asyncpg import DuplicateTableError
from dateutil.parser import parse as parse_date
from sqlalchemy import func, literal, or_, select, text
from sqlalchemy.exc import IntegrityError, ProgrammingError

from db.models import (ImportHistory, ImportLog, Issuer, NPIAddress,
                       NPIDataOtherIdentifier, NPIDataTaxonomyGroup, MRFAddress,
                       MRFAddressEvidence, Plan, PlanBenefitsMarketplace,
                       PlanDrugRaw, PlanDrugStats, PlanDrugTierStats,
                       PlanFormulary, PlanNetworkTierRaw, PlanNPIRaw,
                       PlanTransparency, db)
from process.ext.address_canon import (
    address_key_v1,
    propagate_child_address_keys,
    resolve_into_archive,
    source_enabled,
    stamp_address_keys,
)
from process.ext.archive import unzip
from process.ext.contact_canon import canonicalize_batch as canonicalize_contact_batch
from process.ext.utils import (download_it_and_save, ensure_database,
                               flush_error_log, get_import_schema, log_error,
                               make_class, my_init_db, print_time_info,
                               push_objects, return_checksum)
from process.openaddresses import refresh_archive_geocodes_from_openaddresses
from process.control_lifecycle import mark_control_run
from process.live_progress import enqueue_live_progress
from process.plan_summary import rebuild_plan_search_summary
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

TEST_PLAN_TRANSPARENCY_ROWS = 25
TEST_UNKNOWN_STATE_ROWS = 50
TEST_PLAN_URLS = 2
TEST_PROVIDER_URLS = 2
TEST_FORMULARY_URLS = 2
TEST_PLAN_RECORDS = 30
TEST_PROVIDER_RECORDS = 60
TEST_DRUG_RECORDS = 60
TEST_MIN_PLAN_COUNT = 1

PLAN_ID_MAX_LENGTH = getattr(Plan.__table__.c.plan_id.type, "length", 14)

logger = logging.getLogger(__name__)
MRF_QUEUE_NAME = "arq:MRF"
MRF_FINISH_QUEUE_NAME = "arq:MRF_finish"
MRF_RECOVERABLE_FUNCTIONS = frozenset(
    {"process_json_index", "process_plan", "process_provider", "process_formulary"}
)
_POSTGRES_SETTING_RE = re.compile(r"^\d+(?:\.\d+)?\s*[A-Za-z]*$")
_MRF_ADDRESS_SUMMARY_DEFERRED_INDEX_NAMES = {
    "address_sources",
    "source_issuer_ids",
    "source_issuer_names",
}


def _mrf_url_job_id(kind: str, scope: str, url: str) -> str:
    digest = hashlib.sha256(f"{kind}\0{scope}\0{url}".encode("utf-8")).hexdigest()[:32]
    return f"mrf:{kind}:{scope}:{digest}"


async def _mark_mrf_provider_file_progress(ctx: dict, *, url: str | None, processed_providers: int) -> None:
    run_id = str(ctx.get("context", {}).get("control_run_id") or "").strip()
    if not run_id:
        return
    message = f"processed provider file with {processed_providers:,} provider record(s)"
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="mrf provider jobs running",
        progress_message=message,
        metrics={
            "last_provider_url": url,
            "last_provider_records": processed_providers,
        },
        progress={
            "unit": "provider_files",
            "total": 1,
            "done": 0,
            "pct": 0,
            "message": message,
            "phase": "mrf provider jobs running",
        },
    )


def is_test_mode(ctx: dict) -> bool:
    """Return whether the import context requests bounded test behavior."""

    return bool(ctx.get("context", {}).get("test_mode"))


def _is_truthy(value, truthy=("yes", "y", "true")) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in truthy
    return bool(value)


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or str(raw_value).strip() == "":
        return default
    try:
        value = int(str(raw_value).strip())
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid integer env %s=%r; using %s", name, raw_value, default)
        return default
    return max(value, minimum)


def _mrf_plan_flush_rows(test_mode: bool = False) -> int:
    default = min(TEST_PLAN_RECORDS, 10) if test_mode else 2000
    if os.environ.get("HLTHPRT_MRF_PLAN_FLUSH_ROWS") is not None:
        return _env_int("HLTHPRT_MRF_PLAN_FLUSH_ROWS", default)
    return _env_int("HLTHPRT_SAVE_PER_PACK", default)


def _mrf_provider_flush_rows(test_mode: bool = False) -> int:
    default = min(TEST_PROVIDER_RECORDS, 25) if test_mode else 50000
    return _env_int("HLTHPRT_MRF_PROVIDER_FLUSH_ROWS", default)


def _mrf_formulary_flush_rows(test_mode: bool = False) -> int:
    default = min(TEST_DRUG_RECORDS, 100) if test_mode else 50000
    return _env_int("HLTHPRT_MRF_FORMULARY_FLUSH_ROWS", default)


def _safe_int(raw, default: int = 0) -> int:
    if raw is None:
        return default
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _mrf_run_state_ttl_seconds() -> int:
    return _env_int("HLTHPRT_MRF_RUN_STATE_TTL_SECONDS", 7 * 24 * 60 * 60)


def _mrf_run_state_id(ctx: dict) -> str:
    context = ctx.get("context", {})
    return str(context.get("control_run_id") or context.get("import_date") or "").strip()


def _mrf_state_key(run_id: str, name: str) -> str:
    return f"mrf:run:{run_id}:{name}"


def _decode_redis_text(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _is_mrf_finalize_job_id(job_id: str, import_date: str) -> bool:
    base = f"shutdown_mrf_{import_date}"
    return job_id == base or job_id.startswith(f"{base}_wait_") or job_id.startswith(f"{base}_lock_wait_")


async def _cleanup_mrf_finalize_jobs(redis, import_date: str) -> int:
    """Remove obsolete MRF finish jobs for an import date after finalization."""
    if not redis or not import_date:
        return 0
    base_job_id = f"shutdown_mrf_{import_date}"
    job_ids = {base_job_id}
    try:
        queued_job_ids = await redis.zrange(MRF_FINISH_QUEUE_NAME, 0, -1)
    except Exception:  # pragma: no cover - cleanup must not fail publish
        logger.debug("Unable to inspect MRF finish queue for cleanup", exc_info=True)
        queued_job_ids = []
    for raw_job_id in queued_job_ids or []:
        job_id = _decode_redis_text(raw_job_id)
        if _is_mrf_finalize_job_id(job_id, import_date):
            job_ids.add(job_id)

    removed = 0
    for job_id in sorted(job_ids):
        try:
            removed += int(await redis.zrem(MRF_FINISH_QUEUE_NAME, job_id) or 0)
            await redis.delete(
                f"arq:job:{job_id}",
                f"arq:result:{job_id}",
                f"arq:retry:{job_id}",
            )
        except Exception:  # pragma: no cover - best-effort cleanup only
            logger.debug("Unable to clean obsolete MRF finalize job %s", job_id, exc_info=True)
    return removed


def _mrf_job_scope(ctx: dict) -> str:
    return _mrf_run_state_id(ctx) or datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")


def _mrf_task_work_id(ctx: dict, task: dict, kind: str) -> str:
    work_id = str(task.get("work_id") or "").strip()
    if work_id:
        return work_id
    return _mrf_url_job_id(kind, _mrf_job_scope(ctx), str(task.get("url") or task.get("input_url") or kind))


async def _init_mrf_run_state(redis, run_id: str) -> None:
    if not redis or not run_id:
        return
    total_key = _mrf_state_key(run_id, "total_work")
    done_key = _mrf_state_key(run_id, "done_work")
    expected_key = _mrf_state_key(run_id, "expected_work")
    pending_key = _mrf_state_key(run_id, "pending_work")
    recovery_attempts_key = _mrf_state_key(run_id, "recovery_attempts")
    lock_key = _mrf_state_key(run_id, "finalize_lock")
    finalized_key = _mrf_state_key(run_id, "finalized")
    await redis.delete(
        total_key,
        done_key,
        expected_key,
        pending_key,
        recovery_attempts_key,
        lock_key,
        finalized_key,
    )
    await redis.set(total_key, "0")
    await redis.expire(total_key, _mrf_run_state_ttl_seconds())


async def _increment_mrf_total_work(redis, run_id: str, delta: int) -> None:
    if not redis or not run_id or delta <= 0:
        return
    total_key = _mrf_state_key(run_id, "total_work")
    await redis.incrby(total_key, int(delta))
    await redis.expire(total_key, _mrf_run_state_ttl_seconds())


async def _has_registered_mrf_work(
    redis,
    run_id: str,
    work_id: str,
    *,
    function_name: str | None = None,
    task: dict | None = None,
) -> bool:
    if not redis or not run_id or not work_id:
        return False
    if function_name and task is not None:
        pending_key = _mrf_state_key(run_id, "pending_work")
        payload = serialize_job({"function": function_name, "task": task})
        await redis.hsetnx(pending_key, work_id, payload)
        await redis.expire(pending_key, _mrf_run_state_ttl_seconds())
    expected_key = _mrf_state_key(run_id, "expected_work")
    added = await redis.sadd(expected_key, work_id)
    await redis.expire(expected_key, _mrf_run_state_ttl_seconds())
    if added:
        await _increment_mrf_total_work(redis, run_id, 1)
    return bool(added)


async def _mark_mrf_work_done(ctx: dict, work_id: str) -> None:
    redis = ctx.get("redis")
    run_id = _mrf_run_state_id(ctx)
    if not redis or not run_id or not work_id:
        return
    done_key = _mrf_state_key(run_id, "done_work")
    await redis.sadd(done_key, work_id)
    await redis.expire(done_key, _mrf_run_state_ttl_seconds())
    await redis.hdel(_mrf_state_key(run_id, "pending_work"), work_id)


async def _get_mrf_run_progress(redis, run_id: str) -> tuple[int, int]:
    if not redis or not run_id:
        return (0, 0)
    total_key = _mrf_state_key(run_id, "total_work")
    done_key = _mrf_state_key(run_id, "done_work")
    total_work = _safe_int(await redis.get(total_key), 0)
    done_work = _safe_int(await redis.scard(done_key), 0)
    return total_work, done_work


async def _recover_missing_mrf_work(redis, run_id: str) -> dict:
    expected_key = _mrf_state_key(run_id, "expected_work")
    done_key = _mrf_state_key(run_id, "done_work")
    pending_key = _mrf_state_key(run_id, "pending_work")
    attempts_key = _mrf_state_key(run_id, "recovery_attempts")
    raw_missing = await redis.sdiff(expected_key, done_key)
    missing_ids = sorted(_decode_redis_text(raw_work_id) for raw_work_id in raw_missing)
    batch_size = _env_int("HLTHPRT_MRF_RECOVERY_BATCH_SIZE", 100)
    max_attempts = _env_int("HLTHPRT_MRF_RECOVERY_MAX_ATTEMPTS", 3)
    recovery_by_state = {
        "missing": len(missing_ids),
        "recovered": 0,
        "active": 0,
        "unrecoverable": [],
        "exhausted": [],
    }

    for work_id in missing_ids[:batch_size]:
        raw_payload = await redis.hget(pending_key, work_id)
        if not raw_payload:
            recovery_by_state["unrecoverable"].append(work_id)
            continue
        if await _is_mrf_arq_job_active(redis, work_id):
            recovery_by_state["active"] += 1
            continue
        attempts = _safe_int(await redis.hget(attempts_key, work_id), 0)
        if attempts >= max_attempts:
            recovery_by_state["exhausted"].append(work_id)
            continue
        try:
            work_payload = deserialize_job(raw_payload)
        except Exception:  # pragma: no cover - corrupt Redis payload
            recovery_by_state["unrecoverable"].append(work_id)
            continue
        function_name = str(work_payload.get("function") or "") if isinstance(work_payload, dict) else ""
        task = work_payload.get("task") if isinstance(work_payload, dict) else None
        if function_name not in MRF_RECOVERABLE_FUNCTIONS or not isinstance(task, dict):
            recovery_by_state["unrecoverable"].append(work_id)
            continue

        await redis.delete(f"arq:result:{work_id}", f"arq:retry:{work_id}")
        job = await redis.enqueue_job(
            function_name,
            task,
            _queue_name=MRF_QUEUE_NAME,
            _job_id=work_id,
        )
        if job is None:
            recovery_by_state["active"] += 1
            continue
        await redis.hincrby(attempts_key, work_id, 1)
        await redis.expire(attempts_key, _mrf_run_state_ttl_seconds())
        recovery_by_state["recovered"] += 1

    return recovery_by_state


async def _is_mrf_arq_job_active(redis, work_id: str) -> bool:
    if await redis.exists(f"arq:job:{work_id}", f"arq:in-progress:{work_id}"):
        return True
    return await redis.zscore(MRF_QUEUE_NAME, work_id) is not None


async def mrf_worker_shutdown(ctx: dict) -> None:
    """Move a drained parser run into the finalizer lane."""
    redis = ctx.get("redis")
    run_id = _mrf_run_state_id(ctx)
    if not redis or not run_id:
        return
    if await redis.zcard(MRF_QUEUE_NAME):
        return
    total_work, done_work = await _get_mrf_run_progress(redis, run_id)
    if not total_work:
        return
    message = f"parser queue drained at {done_work}/{total_work} work item(s)"
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="mrf finalizing parser queue drained",
        progress_message=message,
        progress={
            "unit": "work_items",
            "total": total_work,
            "done": done_work,
            "pct": min(90, int((done_work / max(total_work, 1)) * 90)),
            "message": message,
            "phase": "mrf finalizing parser queue drained",
        },
    )


async def _has_claimed_mrf_finalize_lock(redis, run_id: str) -> bool:
    lock_key = _mrf_state_key(run_id, "finalize_lock")
    lock_set = await redis.set(lock_key, "1", ex=_mrf_run_state_ttl_seconds(), nx=True)
    return bool(lock_set)


def _mrf_size_bytes(name: str, default_bytes: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None and name.endswith("_BYTES"):
        raw_value = os.environ.get(f"{name[:-6]}_MB")
    if raw_value is None or str(raw_value).strip() == "":
        return default_bytes
    value = str(raw_value).strip().lower()
    if value.isdigit():
        parsed = int(value)
        return parsed * 1024 * 1024 if parsed < 1024 else parsed
    match = re.fullmatch(r"(\d+)\s*([kmgt]?i?b?)", value)
    if not match:
        logger.warning("Ignoring invalid byte-size env %s=%r; using %s", name, raw_value, default_bytes)
        return default_bytes
    number = int(match.group(1))
    suffix = match.group(2).replace("ib", "").replace("b", "")
    multiplier = {
        "": 1,
        "k": 1024,
        "m": 1024 * 1024,
        "g": 1024 * 1024 * 1024,
        "t": 1024 * 1024 * 1024 * 1024,
    }.get(suffix, 1)
    return number * multiplier


def _is_mrf_file_chunking_enabled(kind: str, ctx: dict | None = None) -> bool:
    context_value = None
    if ctx:
        context_value = ctx.get("context", {}).get("mrf_file_chunking")
    raw_value = str(context_value or os.environ.get("HLTHPRT_MRF_FILE_CHUNKING", "providers,formularies")).strip()
    if not raw_value:
        return False
    lowered = raw_value.lower()
    if lowered in {"0", "false", "no", "off", "none"}:
        return False
    if lowered in {"1", "true", "yes", "on", "all"}:
        return True
    aliases_by_kind = {
        "plan": {"plan", "plans"},
        "provider": {"provider", "providers"},
        "formulary": {"formulary", "formularies", "drug", "drugs"},
    }
    enabled_aliases = {item.strip().lower() for item in re.split(r"[,;\s]+", lowered) if item.strip()}
    return bool(enabled_aliases & aliases_by_kind.get(kind, {kind}))


def _mrf_chunk_dir(ctx: dict, kind: str, source_url: str) -> Path:
    base_dir = Path(
        os.environ.get("HLTHPRT_MRF_CHUNK_WORKDIR")
        or os.environ.get("HLTHPRT_WORKER_STATE_DIR")
        or str(PurePath(tempfile.gettempdir(), "healthporta-mrf-chunks"))
    )
    run_scope = re.sub(r"[^0-9A-Za-z_.-]+", "_", _mrf_job_scope(ctx)).strip("_") or "run"
    source_digest = hashlib.sha256(source_url.encode("utf-8")).hexdigest()[:16]
    return base_dir / run_scope / kind / source_digest


def _write_mrf_chunk(chunks: list[dict], path: Path, records: list[bytes]) -> None:
    with path.open("wb") as handle:
        handle.write(b"[")
        for idx, record in enumerate(records):
            if idx:
                handle.write(b",")
            handle.write(record)
        handle.write(b"]")
    chunks.append(
        {
            "path": str(path),
            "record_count": len(records),
            "byte_count": path.stat().st_size,
        }
    )


def _split_json_array_file_to_chunks(source_path: str, chunk_dir: Path, kind: str, target_bytes: int) -> list[dict]:
    """Split a top-level JSON array into bounded valid array files."""

    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunks: list[dict] = []
    buffered_records: list[bytes] = []
    chunk_bytes = 2
    record_buffer = bytearray()
    is_in_array = False
    has_record_started = False
    is_in_string = False
    is_escaped = False
    depth = 0
    is_array_done = False

    def flush_record(current_chunk_bytes: int) -> int:
        """Flush the buffered record into the active output chunk."""

        if not record_buffer:
            return current_chunk_bytes
        payload = bytes(record_buffer).strip()
        record_buffer.clear()
        if not payload:
            return current_chunk_bytes
        projected = current_chunk_bytes + len(payload) + (1 if buffered_records else 0)
        if buffered_records and projected > target_bytes:
            _write_mrf_chunk(chunks, chunk_dir / f"{kind}_{len(chunks):05d}.json", buffered_records)
            buffered_records.clear()
            current_chunk_bytes = 2
        buffered_records.append(payload)
        current_chunk_bytes += len(payload) + (1 if len(buffered_records) > 1 else 0)
        return current_chunk_bytes

    with open(source_path, "rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block or is_array_done:
                break
            for byte in block:
                if not is_in_array:
                    if byte in b" \t\r\n":
                        continue
                    if byte != ord("["):
                        return []
                    is_in_array = True
                    continue

                if not has_record_started:
                    if byte in b" \t\r\n,":
                        continue
                    if byte == ord("]"):
                        is_array_done = True
                        break
                    has_record_started = True
                    depth = 0
                    is_in_string = False
                    is_escaped = False

                record_buffer.append(byte)

                if is_in_string:
                    if is_escaped:
                        is_escaped = False
                    elif byte == ord("\\"):
                        is_escaped = True
                    elif byte == ord('"'):
                        is_in_string = False
                    continue

                if byte == ord('"'):
                    is_in_string = True
                elif byte in (ord("{"), ord("[")):
                    depth += 1
                elif byte in (ord("}"), ord("]")):
                    depth -= 1
                    if depth == 0:
                        has_record_started = False
                        chunk_bytes = flush_record(chunk_bytes)

    if has_record_started:
        raise ValueError(f"Unable to split {source_path}; unterminated top-level JSON record")
    if buffered_records:
        _write_mrf_chunk(chunks, chunk_dir / f"{kind}_{len(chunks):05d}.json", buffered_records)
    if len(chunks) <= 1:
        shutil.rmtree(chunk_dir, ignore_errors=True)
        return []
    return chunks


async def _has_enqueued_mrf_file_chunks(ctx: dict, task: dict, tmp_filename: str, kind: str, function_name: str) -> bool:
    if task.get("input_url") or not _is_mrf_file_chunking_enabled(kind, ctx):
        return False
    file_size = os.path.getsize(tmp_filename)
    min_bytes = _mrf_size_bytes("HLTHPRT_MRF_CHUNK_MIN_BYTES", 512 * 1024 * 1024)
    if file_size < min_bytes:
        return False
    target_bytes = _mrf_size_bytes("HLTHPRT_MRF_CHUNK_TARGET_BYTES", 256 * 1024 * 1024)
    source_url = str(task.get("source_url") or task.get("url") or "")
    chunk_dir = _mrf_chunk_dir(ctx, kind, source_url)
    chunks = _split_json_array_file_to_chunks(tmp_filename, chunk_dir, kind, target_bytes)
    if not chunks:
        return False

    redis = ctx["redis"]
    run_scope = _mrf_job_scope(ctx)
    for idx, chunk in enumerate(chunks):
        work_id = _mrf_url_job_id(f"{kind}-chunk", run_scope, f"{source_url}:{idx}")
        chunk_task_dict = {
            **task,
            "url": source_url,
            "source_url": source_url,
            "input_url": Path(chunk["path"]).absolute().as_uri(),
            "work_id": work_id,
            "chunk_index": idx,
            "chunk_count": len(chunks),
        }
        if not await _has_registered_mrf_work(
            redis,
            run_scope,
            work_id,
            function_name=function_name,
            task=chunk_task_dict,
        ):
            continue
        await redis.enqueue_job(
            function_name, chunk_task_dict, _queue_name=MRF_QUEUE_NAME, _job_id=work_id
        )
    logger.info(
        "Enqueued %s %s chunk job(s) for %s (%s bytes)",
        len(chunks),
        kind,
        source_url,
        file_size,
    )
    return True


def _cleanup_mrf_chunk_file(task: dict) -> None:
    input_url = str(task.get("input_url") or "")
    if not input_url.startswith("file://") or _is_truthy(os.environ.get("HLTHPRT_MRF_KEEP_CHUNKS"), ("1", "true", "yes", "on")):
        return
    parsed = urlparse(input_url)
    try:
        Path(unquote(parsed.path)).unlink(missing_ok=True)
    except OSError as exc:
        logger.warning("Failed to remove MRF chunk file %s: %s", parsed.path, exc)


async def _mark_mrf_task_terminal(ctx: dict, task: dict, kind: str, *, cleanup_chunk: bool = False) -> None:
    await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, kind))
    if cleanup_chunk:
        _cleanup_mrf_chunk_file(task)


def _cleanup_mrf_run_chunks(ctx: dict) -> None:
    if _is_truthy(os.environ.get("HLTHPRT_MRF_KEEP_CHUNKS"), ("1", "true", "yes", "on")):
        return
    base_dir = Path(
        os.environ.get("HLTHPRT_MRF_CHUNK_WORKDIR")
        or os.environ.get("HLTHPRT_WORKER_STATE_DIR")
        or str(PurePath(tempfile.gettempdir(), "healthporta-mrf-chunks"))
    )
    run_scope = re.sub(r"[^0-9A-Za-z_.-]+", "_", _mrf_job_scope(ctx)).strip("_") or "run"
    shutil.rmtree(base_dir / run_scope, ignore_errors=True)


def _transparency_zip_path(tmpdirname: str, file_idx: int, file: dict) -> str:
    year = re.sub(r"[^0-9A-Za-z_]+", "_", str(file.get("year") or file_idx)).strip("_")
    if not year:
        year = str(file_idx)
    return str(PurePath(str(tmpdirname), f"transparency_{file_idx}_{year}.zip"))


def _postgres_setting_value(env_name: str, default: str) -> str:
    value = os.environ.get(env_name, default)
    value = str(value if value is not None else default).strip()
    if not value or not _POSTGRES_SETTING_RE.fullmatch(value):
        raise ValueError(f"{env_name} must be a PostgreSQL numeric setting, e.g. '512MB' or '60min'")
    return value


def _clean_name_part(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _coerce_plan_year(candidate) -> int | None:
    if candidate is None:
        return None
    try:
        if isinstance(candidate, int):
            year_value = candidate
        elif isinstance(candidate, float):
            if not candidate.is_integer():
                return None
            year_value = int(candidate)
        else:
            year_text = str(candidate).strip()
            if not year_text:
                return None
            if year_text.endswith(".0"):
                year_text = year_text[:-2]
            year_value = int(year_text)
    except (TypeError, ValueError):
        return None
    return year_value if 1900 <= year_value <= 2200 else None


def _extract_plan_years(plan_obj: dict) -> list[int]:
    raw_years = plan_obj.get("years")
    if raw_years is None:
        raw_years = plan_obj.get("year")
    if raw_years is None:
        return []

    if isinstance(raw_years, (list, tuple, set)):
        candidates = list(raw_years)
    else:
        candidates = [raw_years]

    years: list[int] = []
    for candidate in candidates:
        year_value = _coerce_plan_year(candidate)
        if year_value is not None and year_value not in years:
            years.append(year_value)

    return years


def _normalize_upper(value):
    cleaned = _clean_name_part(value)
    return cleaned.upper() if cleaned else None


def _coerce_text(value):
    cleaned = _clean_name_part(value)
    return cleaned or None


def _parse_timestamp(value):
    if not value:
        return None
    return datetime.datetime.combine(parse_date(value, fuzzy=True), datetime.datetime.min.time())


def _create_index_sql(table_name: str, index: dict, db_schema: str) -> str:
    index_name = _index_name(index)
    using = f"USING {index.get('using')} " if index.get("using") else ""
    unique = "UNIQUE " if index.get("unique") else ""
    where = f" WHERE {index.get('where')}" if index.get("where") else ""
    return (
        f"CREATE {unique}INDEX IF NOT EXISTS {table_name}_idx_{index_name} "
        f"ON {db_schema}.{table_name} {using}({', '.join(index.get('index_elements'))}){where};"
    )


def _index_name(index: dict) -> str:
    return index.get("name", "_".join(index.get("index_elements")))


def _drop_index_sql(table_name: str, index: dict, db_schema: str) -> str:
    return f"DROP INDEX IF EXISTS {db_schema}.{table_name}_idx_{_index_name(index)};"


def _mrf_address_summary_deferred_indexes(address_cls) -> list[dict]:
    disabled_values = {"0", "false", "no", "off"}
    raw = os.getenv("HLTHPRT_MRF_ADDRESS_SUMMARY_DEFER_SOURCE_INDEXES", "true").strip().lower()
    if raw in disabled_values:
        return []
    if not _is_truthy(os.getenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST"), ("yes", "y", "true", "1")):
        return list(getattr(address_cls, "__my_initial_indexes__", []) or []) + list(
            getattr(address_cls, "__my_additional_indexes__", []) or []
        )
    return [
        index
        for index in getattr(address_cls, "__my_additional_indexes__", []) or []
        if _index_name(index) in _MRF_ADDRESS_SUMMARY_DEFERRED_INDEX_NAMES
    ]


async def _create_named_indexes(stage_cls, db_schema: str) -> None:
    for attr_name in ("__my_initial_indexes__", "__my_additional_indexes__"):
        for index in getattr(stage_cls, attr_name, []) or []:
            await db.status(_create_index_sql(stage_cls.__tablename__, index, db_schema))


def _benefit_label_from_key(key):
    cleaned = _coerce_text(key) or "unknown"
    return cleaned.replace("_", " ").replace("-", " ").strip().title()


def _serialize_jsonable(value):
    try:
        return json.loads(json.dumps(value))
    except (TypeError, ValueError):
        return {"value": str(value)}


def _issuer_display_name(issuer_id, issuer_name=None, issuer_marketing_name=None, issuer_url=None):
    for candidate in (issuer_name, issuer_marketing_name):
        value = _coerce_text(candidate)
        if value and value.upper() != "N/A":
            return value

    host = urlparse(_coerce_text(issuer_url) or "").hostname
    if host:
        return host.lower()

    return str(issuer_id)


def _normalize_benefit_value(value):
    if isinstance(value, bool):
        return ("true" if value else "false", value, None, None)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (str(value), None, float(value), None)
    if isinstance(value, str):
        cleaned = value.strip()
        return (cleaned or None, None, None, None)
    if value is None:
        return (None, None, None, None)
    return (json.dumps(value, sort_keys=True), None, None, _serialize_jsonable(value))


def _iter_marketplace_benefit_entries(benefit_item, position):
    if isinstance(benefit_item, dict):
        named_key = _coerce_text(
            benefit_item.get("benefit_name") or benefit_item.get("name") or benefit_item.get("key")
        )
        has_value_field = "value" in benefit_item
        extra_keys = {
            key for key in benefit_item.keys()
            if key not in {"benefit_name", "name", "key", "label", "display_name", "value"}
        }
        if named_key and has_value_field and not extra_keys:
            yield {
                "benefit_name": named_key,
                "benefit_label": _coerce_text(benefit_item.get("label") or benefit_item.get("display_name"))
                or _benefit_label_from_key(named_key),
                "benefit_value": benefit_item.get("value"),
                "benefit_position": position,
                "benefit_item_json": _serialize_jsonable(benefit_item),
            }
            return
        for key, benefit_value in benefit_item.items():
            yield {
                "benefit_name": str(key).strip(),
                "benefit_label": _benefit_label_from_key(key),
                "benefit_value": benefit_value,
                "benefit_position": position,
                "benefit_item_json": _serialize_jsonable(benefit_item),
            }
        return

    yield {
        "benefit_name": f"benefit_{position}",
        "benefit_label": f"Benefit {position + 1}",
        "benefit_value": benefit_item,
        "benefit_position": position,
        "benefit_item_json": _serialize_jsonable(benefit_item),
    }


def _normalize_marketplace_benefits(plan_id, year, issuer_id, benefits, last_updated_on):
    if isinstance(benefits, dict):
        benefits = [benefits]
    if not isinstance(benefits, list):
        return []

    benefit_rows = []
    for position, benefit_item in enumerate(benefits):
        for entry in _iter_marketplace_benefit_entries(benefit_item, position):
            benefit_value_text, benefit_value_bool, benefit_value_number, benefit_value_json = _normalize_benefit_value(
                entry["benefit_value"]
            )
            checksum = return_checksum(
                [
                    plan_id,
                    year,
                    entry["benefit_position"],
                    entry["benefit_name"],
                    json.dumps(entry["benefit_item_json"], sort_keys=True),
                ]
            )
            benefit_rows.append(
                {
                    "plan_id": plan_id,
                    "year": int(year),
                    "issuer_id": issuer_id,
                    "benefit_position": entry["benefit_position"],
                    "benefit_name": entry["benefit_name"],
                    "benefit_label": entry["benefit_label"],
                    "benefit_value_text": benefit_value_text,
                    "benefit_value_bool": benefit_value_bool,
                    "benefit_value_number": benefit_value_number,
                    "benefit_value_json": benefit_value_json,
                    "benefit_item_json": entry["benefit_item_json"],
                    "last_updated_on": last_updated_on,
                    "checksum": checksum,
                }
            )
    return benefit_rows


def _marketplace_contact_row(address):
    return (
        address.get("telephone_number"),
        address.get("fax_number"),
        address.get("country_code") or "US",
    )


def _apply_marketplace_contact_fields(address, canonical_contact):
    address["phone_number"] = canonical_contact.get("phone_number")
    address["phone_extension"] = canonical_contact.get("phone_extension")
    address["fax_number_digits"] = canonical_contact.get("fax_number_digits") or canonical_contact.get("fax_number")
    address["fax_extension"] = canonical_contact.get("fax_extension")
    return address


def _normalize_marketplace_address_entry_base(address):
    if not isinstance(address, dict):
        return None

    first_line = _coerce_text(address.get("address") or address.get("address1") or address.get("line1"))
    city_name = _normalize_upper(address.get("city"))
    state_name = _normalize_upper(address.get("state"))
    postal_code = _coerce_text(address.get("zip") or address.get("postal_code"))
    if not first_line or not city_name or not state_name or not postal_code:
        return None

    second_line = _coerce_text(
        address.get("address_2") or address.get("address2") or address.get("line2")
    )
    country_code = _normalize_upper(address.get("country") or address.get("country_code"))
    telephone_number = _coerce_text(address.get("phone") or address.get("telephone"))
    fax_number = _coerce_text(address.get("fax") or address.get("fax_number"))
    checksum = return_checksum(
        [
            first_line or "",
            second_line or "",
            city_name or "",
            state_name or "",
            postal_code or "",
            country_code or "",
        ]
    )
    return {
        "checksum": checksum,
        "first_line": first_line,
        "second_line": second_line,
        "city_name": city_name,
        "state_name": state_name,
        "postal_code": postal_code,
        "country_code": country_code,
        "telephone_number": telephone_number,
        "fax_number": fax_number,
        "formatted_address": ", ".join(
            [
                part
                for part in (
                    " ".join(part for part in (first_line, second_line) if part).strip(),
                    " ".join(part for part in (city_name, state_name, postal_code) if part).strip(),
                )
                if part
            ]
        ),
    }


def _normalize_marketplace_address_entry(address):
    normalized = _normalize_marketplace_address_entry_base(address)
    if not normalized:
        return None
    canonical_contact = canonicalize_contact_batch([_marketplace_contact_row(normalized)])[0]
    return _apply_marketplace_contact_fields(normalized, canonical_contact)


def _build_mrf_address_rows(provider_record, network_tiers, import_id, source_url, last_updated_on, issuer_lookup=None):
    """Build canonical address and evidence rows from one MRF provider record."""
    addresses = provider_record.get("addresses", []) or []
    if not isinstance(addresses, list):
        return [], []

    issuer_lookup = issuer_lookup or {}
    npi = int(provider_record["npi"])
    address_type = "practice"
    addresses_by_key = {}
    evidence_by_checksum = {}
    try:
        import_date_value = datetime.datetime.strptime(str(import_id)[:8], "%Y%m%d").date()
    except (TypeError, ValueError):
        import_date_value = None

    normalized_addresses = [
        normalized
        for address in addresses
        if (normalized := _normalize_marketplace_address_entry_base(address))
    ]
    canonical_contacts = canonicalize_contact_batch(
        _marketplace_contact_row(normalized)
        for normalized in normalized_addresses
    )

    for normalized, canonical_contact in zip(normalized_addresses, canonical_contacts):
        normalized = _apply_marketplace_contact_fields(normalized, canonical_contact)
        computed_address_key = address_key_v1(
            normalized["first_line"],
            normalized["second_line"],
            normalized["city_name"],
            normalized["state_name"],
            normalized["postal_code"],
            normalized["country_code"] or "US",
        )
        address_key = (npi, address_type, normalized["checksum"])
        for network in network_tiers.values():
            issuer_id = network["issuer_id"]
            issuer_name = issuer_lookup.get(issuer_id) or str(issuer_id)
            source_record_id = ":".join(
                [
                    str(npi),
                    address_type,
                    str(normalized["checksum"]),
                    str(network["checksum_network"]),
                ]
            )
            evidence_checksum = return_checksum(
                [
                    npi,
                    address_type,
                    normalized["checksum"],
                    network["issuer_id"],
                    network["year"],
                    network["checksum_network"],
                    import_id,
                    source_url,
                    source_record_id,
                ]
            )
            evidence_by_checksum[evidence_checksum] = {
                "evidence_checksum": evidence_checksum,
                "npi": npi,
                "type": address_type,
                "checksum": normalized["checksum"],
                "issuer_id": issuer_id,
                "issuer_name": issuer_name,
                "year": network["year"],
                "checksum_network": network["checksum_network"],
                "network_tier": network["network_tier"],
                "import_id": str(import_id),
                "import_date": import_date_value,
                "address_source": "marketplace_provider",
                "source_table": "plan_npi_raw",
                "source_url": source_url,
                "source_record_id": source_record_id,
                "first_line": normalized["first_line"],
                "second_line": normalized["second_line"],
                "city_name": normalized["city_name"],
                "state_name": normalized["state_name"],
                "postal_code": normalized["postal_code"],
                "country_code": normalized["country_code"],
                "telephone_number": normalized["telephone_number"],
                "phone_number": normalized["phone_number"],
                "phone_extension": normalized["phone_extension"],
                "fax_number_digits": normalized["fax_number_digits"],
                "fax_extension": normalized["fax_extension"],
                "observed_at": last_updated_on,
                "address_key": computed_address_key,
            }

        addresses_by_key[address_key] = {
            "npi": npi,
            "type": address_type,
            "checksum": normalized["checksum"],
            "first_line": normalized["first_line"],
            "second_line": normalized["second_line"],
            "city_name": normalized["city_name"],
            "state_name": normalized["state_name"],
            "postal_code": normalized["postal_code"],
            "country_code": normalized["country_code"],
            "telephone_number": normalized["telephone_number"],
            "fax_number": normalized["fax_number"],
            "phone_number": normalized["phone_number"],
            "phone_extension": normalized["phone_extension"],
            "fax_number_digits": normalized["fax_number_digits"],
            "fax_extension": normalized["fax_extension"],
            "formatted_address": normalized["formatted_address"],
            "date_added": last_updated_on.date() if last_updated_on else None,
            "address_key": computed_address_key,
        }

    return list(addresses_by_key.values()), list(evidence_by_checksum.values())


_MRF_ADDRESS_INSERT_COLUMNS = (
    "npi",
    "type",
    "checksum",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "country_code",
    "telephone_number",
    "fax_number",
    "phone_number",
    "phone_extension",
    "fax_number_digits",
    "fax_extension",
    "formatted_address",
    "date_added",
    "address_key",
)


async def _push_mrf_address_rows(rows, cls) -> None:
    if not _is_truthy(os.getenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST"), ("yes", "y", "true", "1")):
        return
    # Address provenance arrays are rebuilt from mrf_address_evidence during
    # finalization, so duplicate aggregate address rows and early provenance
    # arrays do not need writes or GIN index maintenance during ingestion.
    insert_rows = [
        {column: row[column] for column in _MRF_ADDRESS_INSERT_COLUMNS if column in row}
        for row in rows
    ]
    await push_objects(insert_rows, cls, rewrite=False, use_copy=False)


async def _push_mrf_duplicate_tolerant_rows(rows, cls) -> None:
    if not rows:
        return
    if _is_truthy(os.getenv("HLTHPRT_MRF_COPY_FIRST_DUPLICATE_TOLERANT_INSERTS"), ("yes", "y", "true", "1")):
        await push_objects(rows, cls)
        return
    await push_objects(rows, cls, rewrite=False, use_copy=False)


async def _refresh_mrf_address_summary(import_date: str, db_schema: str) -> None:
    """Refresh the materialized MRF address summary for one import date."""
    address_cls = make_class(MRFAddress, import_date, schema_override=db_schema)
    evidence_cls = make_class(MRFAddressEvidence, import_date, schema_override=db_schema)
    deferred_indexes = _mrf_address_summary_deferred_indexes(address_cls)
    work_mem = _postgres_setting_value("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "1GB")
    statement_timeout = os.environ.get("HLTHPRT_MRF_ADDRESS_SUMMARY_STATEMENT_TIMEOUT")
    upsert_sql = f"""
        INSERT INTO {db_schema}.{address_cls.__tablename__} (
            npi,
            type,
            checksum,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            phone_number,
            phone_extension,
            fax_number_digits,
            fax_extension,
            formatted_address,
            date_added,
            address_key,
            address_sources,
            source_record_ids,
            source_import_ids,
            source_import_dates,
            source_issuer_ids,
            source_issuer_names,
            source_urls,
            source_count
        )
        SELECT
            npi,
            type,
            checksum,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            phone_number,
            phone_extension,
            fax_number_digits,
            fax_extension,
            formatted_address,
            date_added,
            address_key,
            COALESCE(address_sources, ARRAY[]::varchar[]) AS address_sources,
            COALESCE(source_record_ids, ARRAY[]::varchar[]) AS source_record_ids,
            COALESCE(source_import_ids, ARRAY[]::varchar[]) AS source_import_ids,
            COALESCE(source_import_dates, ARRAY[]::date[]) AS source_import_dates,
            COALESCE(source_issuer_ids, ARRAY[]::integer[]) AS source_issuer_ids,
            COALESCE(source_issuer_names, ARRAY[]::varchar[]) AS source_issuer_names,
            COALESCE(source_urls, ARRAY[]::varchar[]) AS source_urls,
            COALESCE(source_count, 0) AS source_count
          FROM (
                SELECT
                    npi,
                    type,
                    checksum,
                    MIN(first_line) FILTER (WHERE first_line IS NOT NULL AND first_line <> '') AS first_line,
                    MIN(second_line) FILTER (WHERE second_line IS NOT NULL AND second_line <> '') AS second_line,
                    MIN(city_name) FILTER (WHERE city_name IS NOT NULL AND city_name <> '') AS city_name,
                    MIN(state_name) FILTER (WHERE state_name IS NOT NULL AND state_name <> '') AS state_name,
                    MIN(postal_code) FILTER (WHERE postal_code IS NOT NULL AND postal_code <> '') AS postal_code,
                    COALESCE(
                        MIN(country_code) FILTER (WHERE country_code IS NOT NULL AND country_code <> ''),
                        'US'
                    ) AS country_code,
                    MIN(telephone_number) FILTER (WHERE telephone_number IS NOT NULL AND telephone_number <> '') AS telephone_number,
                    NULL::varchar AS fax_number,
                    MIN(phone_number) FILTER (WHERE phone_number IS NOT NULL AND phone_number <> '') AS phone_number,
                    MIN(phone_extension) FILTER (WHERE phone_extension IS NOT NULL AND phone_extension <> '') AS phone_extension,
                    MIN(fax_number_digits) FILTER (WHERE fax_number_digits IS NOT NULL AND fax_number_digits <> '') AS fax_number_digits,
                    MIN(fax_extension) FILTER (WHERE fax_extension IS NOT NULL AND fax_extension <> '') AS fax_extension,
                    concat_ws(
                        ', ',
                        NULLIF(
                            concat_ws(
                                ' ',
                                MIN(first_line) FILTER (WHERE first_line IS NOT NULL AND first_line <> ''),
                                MIN(second_line) FILTER (WHERE second_line IS NOT NULL AND second_line <> '')
                            ),
                            ''
                        ),
                        NULLIF(
                            concat_ws(
                                ' ',
                                MIN(city_name) FILTER (WHERE city_name IS NOT NULL AND city_name <> ''),
                                MIN(state_name) FILTER (WHERE state_name IS NOT NULL AND state_name <> ''),
                                MIN(postal_code) FILTER (WHERE postal_code IS NOT NULL AND postal_code <> '')
                            ),
                            ''
                        )
                    ) AS formatted_address,
                    MIN(observed_at)::date AS date_added,
                    MIN(address_key::text) FILTER (WHERE address_key IS NOT NULL)::uuid AS address_key,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT address_source ORDER BY address_source), NULL)::varchar[] AS address_sources,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_record_id ORDER BY source_record_id), NULL)::varchar[] AS source_record_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT import_id ORDER BY import_id), NULL)::varchar[] AS source_import_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT import_date ORDER BY import_date), NULL)::date[] AS source_import_dates,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT issuer_id ORDER BY issuer_id), NULL)::integer[] AS source_issuer_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT issuer_name ORDER BY issuer_name), NULL)::varchar[] AS source_issuer_names,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT source_url ORDER BY source_url), NULL)::varchar[] AS source_urls,
                    COUNT(DISTINCT evidence_checksum)::int AS source_count
                FROM {db_schema}.{evidence_cls.__tablename__}
                GROUP BY npi, type, checksum
          ) AS src
        ON CONFLICT (npi, type, checksum) DO UPDATE
           SET first_line = EXCLUDED.first_line,
               second_line = EXCLUDED.second_line,
               city_name = EXCLUDED.city_name,
               state_name = EXCLUDED.state_name,
               postal_code = EXCLUDED.postal_code,
               country_code = EXCLUDED.country_code,
               telephone_number = EXCLUDED.telephone_number,
               fax_number = EXCLUDED.fax_number,
               phone_number = EXCLUDED.phone_number,
               phone_extension = EXCLUDED.phone_extension,
               fax_number_digits = EXCLUDED.fax_number_digits,
               fax_extension = EXCLUDED.fax_extension,
               formatted_address = EXCLUDED.formatted_address,
               date_added = EXCLUDED.date_added,
               address_key = COALESCE({address_cls.__tablename__}.address_key, EXCLUDED.address_key),
               address_sources = EXCLUDED.address_sources,
               source_record_ids = EXCLUDED.source_record_ids,
               source_import_ids = EXCLUDED.source_import_ids,
               source_import_dates = EXCLUDED.source_import_dates,
               source_issuer_ids = EXCLUDED.source_issuer_ids,
               source_issuer_names = EXCLUDED.source_issuer_names,
               source_urls = EXCLUDED.source_urls,
               source_count = EXCLUDED.source_count;
        """
    async with db.transaction() as session:
        await session.execute(text(f"SET LOCAL work_mem = '{work_mem}';"))
        if statement_timeout is not None:
            timeout = _postgres_setting_value("HLTHPRT_MRF_ADDRESS_SUMMARY_STATEMENT_TIMEOUT", "0")
            await session.execute(text(f"SET LOCAL statement_timeout = '{timeout}';"))
        for index in deferred_indexes:
            await session.execute(text(_drop_index_sql(address_cls.__tablename__, index, db_schema)))
        await session.execute(text(f"ANALYZE {db_schema}.{evidence_cls.__tablename__};"))
        await session.execute(text(upsert_sql))
        for index in deferred_indexes:
            await session.execute(text(_create_index_sql(address_cls.__tablename__, index, db_schema)))
        if deferred_indexes:
            await session.execute(text(f"ANALYZE {db_schema}.{address_cls.__tablename__};"))


async def _prepare_import_tables(import_date: str, test_mode: bool) -> None:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)

    await db.create_table(ImportHistory.__table__, checkfirst=True)
    if hasattr(ImportHistory, "__my_index_elements__") and ImportHistory.__my_index_elements__:
        cols = ", ".join(ImportHistory.__my_index_elements__)
        try:
            await db.status(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                + f"{ImportHistory.__tablename__}_idx_primary ON "
                + f"{db_schema}.{ImportHistory.__tablename__} ({cols});"
            )
        except IntegrityError:
            logger.debug("Import-history primary index already has conflicting rows")

    for cls in (
        Issuer,
        Plan,
        PlanFormulary,
        PlanBenefitsMarketplace,
        PlanTransparency,
        PlanDrugRaw,
        PlanDrugStats,
        PlanDrugTierStats,
        ImportLog,
        PlanNPIRaw,
        PlanNetworkTierRaw,
        MRFAddress,
        MRFAddressEvidence,
    ):
        staging_cls = make_class(cls, import_date, schema_override=db_schema)
        try:
            await db.status("DROP TABLE IF EXISTS " + f"{db_schema}.{staging_cls.__tablename__};")
        except ProgrammingError:
            logger.debug("Import staging table could not be dropped: %s", staging_cls.__tablename__)
        try:
            await db.create_table(staging_cls.__table__, checkfirst=True)
        except (ProgrammingError, DuplicateTableError, IntegrityError):
            logger.debug("Import staging table already exists: %s", staging_cls.__tablename__)
        if hasattr(staging_cls, "__my_index_elements__") and staging_cls.__my_index_elements__:
            cols = ", ".join(staging_cls.__my_index_elements__)
            try:
                await db.status(
                    "CREATE UNIQUE INDEX IF NOT EXISTS "
                    + f"{staging_cls.__tablename__}_idx_primary ON "
                    + f"{db_schema}.{staging_cls.__tablename__} ({cols});"
                )
            except IntegrityError:
                logger.debug("Import staging primary index has conflicting rows: %s", staging_cls.__tablename__)
        if cls in {PlanBenefitsMarketplace, MRFAddress, MRFAddressEvidence}:
            await _create_named_indexes(staging_cls, db_schema)

    print("Preparing done")


async def process_plan(ctx, task):
    """
    The process_plan function is responsible for downloading the plan data from the CMS PUF,
        parsing it and saving to a database.

    :param ctx: Pass the import_date to the function
    :param task: Pass the task object to the function
    :return: 1 if there is no error
    """
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    is_test_mode_enabled = is_test_mode(ctx)
    plan_limit = TEST_PLAN_RECORDS if is_test_mode_enabled else None
    plan_flush_rows = _mrf_plan_flush_rows(is_test_mode_enabled)
    source_url = str(task.get("source_url") or task.get("url") or "")
    download_url = str(task.get("input_url") or source_url)
    await ensure_database(is_test_mode_enabled)

    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", is_test_mode_enabled)
    myplan = make_class(Plan, import_date, schema_override=db_schema)
    myplanformulary = make_class(PlanFormulary, import_date, schema_override=db_schema)
    myplanbenefitsmarketplace = make_class(PlanBenefitsMarketplace, import_date, schema_override=db_schema)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)

    print("Starting Plan data download: ", source_url)
    with tempfile.TemporaryDirectory() as tmpdirname:
        source_path = Path(urlparse(download_url).path if download_url.startswith("file://") else source_url)
        tmp_filename = str(PurePath(str(tmpdirname), source_path.name))
        try:
            await download_it_and_save(
                download_url,
                tmp_filename,
                context={"issuer_array": task["issuer_array"], "source": "plans"},
                logger=myimportlog,
            )
        except Exception as exc:
            logger.warning("Failed to download plan data from %s: %s", source_url, exc)
            await _mark_mrf_task_terminal(ctx, task, "plan", cleanup_chunk=True)
            return

        if await _has_enqueued_mrf_file_chunks(ctx, task, tmp_filename, "plan", "process_plan"):
            await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, "plan"))
            return 1

        async with async_open(tmp_filename, "rb") as afp:
            plan_rows = []
            plan_formulary_rows = []
            marketplace_benefit_rows = []
            count = 0
            processed_plans = 0
            should_stop_processing = False
            try:
                async for plan_entry in ijson.items(afp, "item", use_float=True):
                    if should_stop_processing:
                        break
                    if not isinstance(plan_entry, dict):
                        await log_error(
                            "err",
                            f"Malformed plan entry type: {type(plan_entry).__name__}. Expected object.",
                            task.get("issuer_array"),
                            task.get("url"),
                            "plans",
                            "json",
                            myimportlog,
                        )
                        continue
                    plan_id_raw = plan_entry.get("plan_id")
                    plan_id_value = str(plan_id_raw).strip() if plan_id_raw is not None else ""
                    years = _extract_plan_years(plan_entry)
                    if not years:
                        await log_error(
                            "err",
                            f"Mandatory field `years` is not present or incorrect. Plan ID: "
                            f"{plan_id_value or 'UNKNOWN'}",
                            task.get("issuer_array"),
                            task.get("url"),
                            "plans",
                            "json",
                            myimportlog,
                        )
                        continue
                    if PLAN_ID_MAX_LENGTH and len(plan_id_value) > PLAN_ID_MAX_LENGTH:
                        for year in years:
                            await log_error(
                                "err",
                                f"Plan ID length exceeds {PLAN_ID_MAX_LENGTH} characters. "
                                f"Plan ID: {plan_id_value}, year: {year}",
                                task.get("issuer_array"),
                                task.get("url"),
                                "plans",
                                "json",
                                myimportlog,
                            )
                        continue
                    for year in years:
                        if should_stop_processing:
                            break
                        try:
                            for k in (
                                "plan_id",
                                "plan_id_type",
                                "marketing_name",
                                "summary_url",
                                "plan_contact",
                                "network",
                                "formulary",
                                "last_updated_on",
                            ):
                                if k not in plan_entry or plan_entry[k] is None:
                                    await log_error(
                                        "err",
                                        f"Mandatory field `{k}` is not present or incorrect. Plan ID: "
                                        f"{plan_id_value or 'UNKNOWN'}, year: {year}",
                                        task.get("issuer_array"),
                                        task.get("url"),
                                        "plans",
                                        "json",
                                        myimportlog,
                                    )

                            if int(plan_id_value[:5]) not in task.get("issuer_array"):
                                await log_error(
                                    "err",
                                    f"File describes the issuer that is not defined/allowed by the index "
                                    f"CMS PUF."
                                    f"Issuer of Plan: {int(plan_id_value[:5])}. Allowed issuer list: "
                                    f"{', '.join([str(x) for x in task.get('issuer_array')])}"
                                    f"Plan ID: {plan_id_value}, year: {year}",
                                    task.get("issuer_array"),
                                    task.get("url"),
                                    "plans",
                                    "json",
                                    myimportlog,
                                )

                            network_entries = plan_entry.get("network", [])
                            if isinstance(network_entries, dict):
                                network_entries = [network_entries]
                            formulary_entries = plan_entry.get("formulary", [])
                            if isinstance(formulary_entries, dict):
                                formulary_entries = [formulary_entries]
                            if not isinstance(formulary_entries, list):
                                formulary_entries = []
                            benefits_entries = plan_entry.get("benefits", [])
                            if isinstance(benefits_entries, dict):
                                benefits_entries = [benefits_entries]
                            if not isinstance(benefits_entries, list):
                                benefits_entries = []
                            last_updated_on = _parse_timestamp(plan_entry["last_updated_on"])

                            plan_row_dict = {
                                "plan_id": plan_id_value,
                                "plan_id_type": plan_entry["plan_id_type"],
                                "year": int(year),
                                "issuer_id": int(plan_id_value[:5]),
                                "state": str(plan_id_value[5:7]).upper(),
                                "marketing_name": plan_entry["marketing_name"],
                                "summary_url": plan_entry["summary_url"],
                                "marketing_url": plan_entry.get("marketing_url", ""),
                                "formulary_url": plan_entry.get("formulary_url", ""),
                                "plan_contact": plan_entry["plan_contact"],
                                "network": [(k["network_tier"]) for k in network_entries if isinstance(k, dict)],
                                "benefits": [json.dumps(x) for x in benefits_entries],
                                "last_updated_on": last_updated_on,
                                "checksum": return_checksum([plan_id_value.lower(), year], crc=32),
                            }
                            plan_rows.append(plan_row_dict)
                            marketplace_benefit_rows.extend(
                                _normalize_marketplace_benefits(
                                    plan_id_value,
                                    year,
                                    int(plan_id_value[:5]),
                                    benefits_entries,
                                    last_updated_on,
                                )
                            )
                            processed_plans += 1
                            if plan_limit and processed_plans >= plan_limit:
                                should_stop_processing = True
                                break
                            if count > plan_flush_rows:
                                await asyncio.gather(
                                    push_objects(plan_rows, myplan),
                                    push_objects(marketplace_benefit_rows, myplanbenefitsmarketplace),
                                )
                                plan_rows.clear()
                                marketplace_benefit_rows.clear()
                                count = 0
                            else:
                                count += 1
                        except Exception as exc:
                            logger.debug(
                                "Skipping malformed plan entry plan_id=%s year=%s: %s",
                                plan_entry.get("plan_id"),
                                year,
                                exc,
                            )

                    count = 0
                    for year in years:
                        if should_stop_processing:
                            break
                        if formulary_entries:
                            for formulary in formulary_entries:
                                if (
                                    isinstance(formulary, dict)
                                    and ("cost_sharing" in formulary)
                                    and formulary["cost_sharing"]
                                ):
                                    try:
                                        for k in ("drug_tier", "mail_order"):
                                            if k not in formulary or formulary[k] is None:
                                                await log_error(
                                                    "err",
                                                    f"Mandatory field `{k}` in Formulary (`formulary`) "
                                                    f"sub-type is "
                                                    f"not present or "
                                                    f"incorrect. Plan ID: "
                                                    f"{plan_id_value}, year: {year}",
                                                    task.get("issuer_array"),
                                                    task.get("url"),
                                                    "plans",
                                                    "json",
                                                    myimportlog,
                                                )
                                        for cost_sharing in formulary["cost_sharing"]:
                                            for k in (
                                                "pharmacy_type",
                                                "copay_amount",
                                                "copay_opt",
                                                "coinsurance_rate",
                                                "coinsurance_opt",
                                            ):
                                                if k not in cost_sharing:
                                                    await log_error(
                                                        "err",
                                                        f"Mandatory field `{k}` in Cost Sharing ("
                                                        f"`cost_sharing`) "
                                                        f"sub-type is not present or "
                                                        f"incorrect. Plan ID: "
                                                        f"{plan_id_value}, year: {year}",
                                                        task.get("issuer_array"),
                                                        task.get("url"),
                                                        "plans",
                                                        "json",
                                                        myimportlog,
                                                    )
                                            formulary_row_dict = {
                                                "plan_id": plan_id_value,
                                                "year": int(year),
                                                "drug_tier": formulary.get("drug_tier", ""),
                                                "mail_order": bool(formulary.get("mail_order")),
                                                "pharmacy_type": cost_sharing.get("pharmacy_type", ""),
                                                "copay_amount": (
                                                    float(cost_sharing.get("copay_amount"))
                                                    if cost_sharing.get("copay_amount", None) is not None
                                                    else None
                                                ),
                                                "copay_opt": cost_sharing.get("copay_opt", ""),
                                                "coinsurance_rate": (
                                                    float(cost_sharing.get("coinsurance_rate"))
                                                    if cost_sharing.get("coinsurance_rate", None) is not None
                                                    else None
                                                ),
                                                "coinsurance_opt": cost_sharing.get("coinsurance_opt", ""),
                                            }
                                            plan_formulary_rows.append(formulary_row_dict)
                                            if count > plan_flush_rows:
                                                await _push_mrf_duplicate_tolerant_rows(
                                                    plan_formulary_rows, myplanformulary
                                                )
                                                plan_formulary_rows.clear()
                                                count = 0
                                            else:
                                                count += 1
                                    except Exception as exc:
                                        logger.debug(
                                            "Skipping cost sharing entry for plan %s year=%s: %s",
                                            plan_entry.get("plan_id"),
                                            year,
                                            exc,
                                        )

                                    plan_formulary_rows.clear()
                                    count = 0
                                else:
                                    await log_error(
                                        "warn",
                                        f"Recommended field 'cost_sharing' is not present or incorrect. "
                                        f"Plan ID: {plan_id_value}, year: {year}",
                                        task.get("issuer_array"),
                                        task.get("url"),
                                        "plans",
                                        "json",
                                        myimportlog,
                                    )
                        else:
                            await log_error(
                                "err",
                                f"Mandatory field 'formulary' is not present or incorrect. Plan ID: "
                                f"{plan_id_value}, year: {year}",
                                task.get("issuer_array"),
                                task.get("url"),
                                "plans",
                                "json",
                                myimportlog,
                            )
                    if should_stop_processing:
                        break

                await asyncio.gather(
                    _push_mrf_duplicate_tolerant_rows(plan_rows, myplan),
                    _push_mrf_duplicate_tolerant_rows(plan_formulary_rows, myplanformulary),
                    _push_mrf_duplicate_tolerant_rows(marketplace_benefit_rows, myplanbenefitsmarketplace),
                )
            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "plans",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "plan", cleanup_chunk=True)
                return
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "plans",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "plan", cleanup_chunk=True)
                return
    await flush_error_log(myimportlog)
    await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, "plan"))
    _cleanup_mrf_chunk_file(task)
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
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    is_test_mode_enabled = is_test_mode(ctx)
    provider_limit = TEST_PROVIDER_RECORDS if is_test_mode_enabled else None
    provider_flush_rows = _mrf_provider_flush_rows(is_test_mode_enabled)
    source_url = str(task.get("source_url") or task.get("url") or "")
    download_url = str(task.get("input_url") or source_url)
    await ensure_database(is_test_mode_enabled)

    current_year = datetime.datetime.now().year
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", is_test_mode_enabled)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    myissuer = make_class(Issuer, import_date, schema_override=db_schema)
    myplan_npi = make_class(PlanNPIRaw, import_date, schema_override=db_schema)
    myplan_networktier = make_class(PlanNetworkTierRaw, import_date, schema_override=db_schema)
    mymrfaddress = make_class(MRFAddress, import_date, schema_override=db_schema)
    mymrfaddressevidence = make_class(MRFAddressEvidence, import_date, schema_override=db_schema)
    issuer_rows = await db.select(
        myissuer.issuer_id,
        myissuer.issuer_name,
        myissuer.issuer_marketing_name,
        myissuer.mrf_url,
    ).all()
    issuer_lookup = {
        int(issuer_row.issuer_id): _issuer_display_name(
            issuer_row.issuer_id,
            issuer_name=issuer_row.issuer_name,
            issuer_marketing_name=issuer_row.issuer_marketing_name,
            issuer_url=issuer_row.mrf_url,
        )
        for issuer_row in issuer_rows
        if issuer_row.issuer_id is not None
    }

    print("Starting Provider file data download: ", source_url)
    with tempfile.TemporaryDirectory() as tmpdirname:
        source_path = Path(urlparse(download_url).path if download_url.startswith("file://") else source_url)
        tmp_filename = str(PurePath(str(tmpdirname), source_path.name))
        try:
            await download_it_and_save(
                download_url,
                tmp_filename,
                context={"issuer_array": task["issuer_array"], "source": "providers"},
                logger=myimportlog,
            )
        except Exception as exc:
            logger.warning("Failed to download provider data from %s: %s", source_url, exc)
            await _mark_mrf_task_terminal(ctx, task, "provider", cleanup_chunk=True)
            return
        if await _has_enqueued_mrf_file_chunks(ctx, task, tmp_filename, "provider", "process_provider"):
            await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, "provider"))
            return 1

        async with async_open(tmp_filename, "rb") as afp:
            plan_npi_obj_dict = {}
            plan_networks_by_checksum = {}
            mrf_address_obj_dict = {}
            mrf_address_evidence_dict = {}
            count = 0
            processed_providers = 0
            try:
                async for provider_record in ijson.items(afp, "item", use_float=True):
                    if provider_limit and processed_providers >= provider_limit:
                        break
                    network_tiers_by_checksum = {}
                    has_invalid_plan = False
                    my_years = set()
                    if not provider_record or not provider_record.get("plans"):
                        continue
                    for plan in provider_record["plans"]:
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
                        npi_raw = provider_record.get("npi", "")
                        has_valid_npi = npi_raw and npi_raw.isdigit() and 0 < int(npi_raw) < 4294967295
                        has_plan_id = bool(plan.get("plan_id"))
                        has_years = bool(plan.get("years"))
                        if not has_valid_npi or not has_plan_id or not has_years:
                            has_invalid_plan = True
                            break
                        if len(plan["plan_id"]) <= 12 or len(plan["plan_id"]) > 14:
                            continue

                        for x in plan.get("years", []):
                            if x and (current_year + 1 >= int(x) >= current_year):
                                my_years.add(int(x))

                        issuer_id = int(plan["plan_id"][0:5])
                        for year in my_years:
                            checksum_plan = return_checksum([plan["plan_id"], plan["network_tier"], issuer_id, year])
                            checksum_network = return_checksum([plan["network_tier"], issuer_id, year])
                            plan_networks_by_checksum[checksum_plan] = {
                                "plan_id": plan["plan_id"],
                                "network_tier": plan["network_tier"],
                                "issuer_id": issuer_id,
                                "year": year,
                                "checksum_network": checksum_network,
                            }
                            network_tiers_by_checksum[checksum_network] = {
                                "network_tier": plan["network_tier"],
                                "issuer_id": issuer_id,
                                "year": year,
                                "checksum_network": checksum_network,
                            }
                    if has_invalid_plan:
                        continue

                    provider_name_dict = provider_record.get("name", {})
                    if not provider_name_dict:
                        provider_name_dict = {}
                    languages = provider_record.get("languages", [])
                    if not languages:
                        languages = []
                    addresses = provider_record.get("addresses", [])
                    if not addresses:
                        addresses = []
                    last_updated_on = _parse_timestamp(provider_record["last_updated_on"])

                    provider_row_dict = {
                        "npi": int(provider_record["npi"]),
                        "network_tier": "",
                        "checksum_network": "",
                        "year": 0,
                        "issuer_id": 0,
                        "name_or_facility_name": "",
                        "specialty_or_facility_type": [],
                        "type": str(provider_record.get("type", "")),
                        "prefix": provider_name_dict.get("prefix", None),
                        "first_name": provider_name_dict.get("first", None),
                        "middle_name": provider_name_dict.get("middle", None),
                        "last_name": provider_name_dict.get("last", None),
                        "suffix": provider_name_dict.get("suffix", None),
                        "addresses": [json.dumps(x) for x in addresses],
                        "accepting": provider_record.get("accepting", None),
                        "gender": provider_record.get("gender", None),
                        "languages": [str(x) for x in languages],
                        "last_updated_on": last_updated_on,
                    }

                    if (
                        ("facility_name" in provider_record)
                        and provider_record.get("facility_name", None)
                        and str(provider_record.get("facility_name", "")).strip()
                    ):
                        # for k in (
                        #         'facility_name', 'facility_type'):
                        #     if not (k in res and res[k] is not None):
                        #         await log_error('err',
                        #                         f"Mandatory field `{k}` for providers data is not present or "
                        #                         f"incorrect. Plan ID: "
                        #                         f"{plan['plan_id']}, NPI: {res.get('npi', None)}",
                        #                         task.get('issuer_array'), task.get('url'), 'providers', 'json',
                        #                         myimportlog)

                        provider_row_dict["name_or_facility_name"] = str(
                            provider_record.get("facility_name", "").strip()
                        )
                        provider_row_dict["specialty_or_facility_type"] = [
                            str(x) for x in provider_record.get("facility_type", [])
                        ]
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

                        provider_row_dict["name_or_facility_name"] = ""
                        for k in ("prefix", "first", "middle", "last", "suffix"):
                            if (k in provider_name_dict) and (provider_name_dict.get(k, None)):
                                cleaned = _clean_name_part(provider_name_dict.get(k))
                                if cleaned:
                                    provider_row_dict["name_or_facility_name"] += f"{cleaned} "
                        provider_row_dict["name_or_facility_name"] = provider_row_dict[
                            "name_or_facility_name"
                        ].strip()
                        provider_row_dict["specialty_or_facility_type"] = [
                            str(x) for x in provider_record.get("specialty", [])
                        ]

                    for x in network_tiers_by_checksum.values():
                        provider_row_dict["network_tier"] = x["network_tier"]
                        provider_row_dict["checksum_network"] = x["checksum_network"]
                        provider_row_dict["issuer_id"] = x["issuer_id"]
                        provider_row_dict["year"] = x["year"]
                        plan_npi_obj_dict[
                            "_".join([str(provider_row_dict["npi"]), str(x["checksum_network"])])
                        ] = provider_row_dict.copy()

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
                    address_rows, evidence_rows = _build_mrf_address_rows(
                        provider_record,
                        network_tiers_by_checksum,
                        import_date,
                        source_url,
                        last_updated_on,
                        issuer_lookup=issuer_lookup,
                    )
                    for address_row in address_rows:
                        mrf_address_obj_dict[
                            "_".join(
                                [
                                    str(address_row["npi"]),
                                    str(address_row["checksum"]),
                                    address_row["type"],
                                ]
                            )
                        ] = address_row
                    for evidence_row in evidence_rows:
                        mrf_address_evidence_dict[str(evidence_row["evidence_checksum"])] = evidence_row

                    processed_providers += 1
                    count += 1
                    if count > provider_flush_rows:
                        await asyncio.gather(
                            _push_mrf_duplicate_tolerant_rows(list(plan_npi_obj_dict.values()), myplan_npi),
                            _push_mrf_duplicate_tolerant_rows(
                                list(plan_networks_by_checksum.values()), myplan_networktier
                            ),
                            _push_mrf_address_rows(list(mrf_address_obj_dict.values()), mymrfaddress),
                            _push_mrf_duplicate_tolerant_rows(
                                list(mrf_address_evidence_dict.values()),
                                mymrfaddressevidence,
                            ),
                        )
                        count = 0
                        plan_npi_obj_dict.clear()
                        plan_networks_by_checksum.clear()
                        mrf_address_obj_dict.clear()
                        mrf_address_evidence_dict.clear()

                await asyncio.gather(
                    _push_mrf_duplicate_tolerant_rows(list(plan_npi_obj_dict.values()), myplan_npi),
                    _push_mrf_duplicate_tolerant_rows(
                        list(plan_networks_by_checksum.values()), myplan_networktier
                    ),
                    _push_mrf_address_rows(list(mrf_address_obj_dict.values()), mymrfaddress),
                    _push_mrf_duplicate_tolerant_rows(
                        list(mrf_address_evidence_dict.values()),
                        mymrfaddressevidence,
                    ),
                )
                plan_npi_obj_dict.clear()
                plan_networks_by_checksum.clear()
                mrf_address_obj_dict.clear()
                mrf_address_evidence_dict.clear()

            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "providers",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "provider", cleanup_chunk=True)
                return
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "providers",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "provider", cleanup_chunk=True)
                return
    await flush_error_log(myimportlog)
    await _mark_mrf_provider_file_progress(
        ctx,
        url=source_url,
        processed_providers=processed_providers,
    )
    await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, "provider"))
    _cleanup_mrf_chunk_file(task)
    return 1


def _parse_optional_bool(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "y", "1"}:
            return True
        if lowered in {"false", "no", "n", "0"}:
            return False
    return bool(value)


def _chunked(values, chunk_size=500):
    items = [value for value in set(values) if value]
    for idx in range(0, len(items), chunk_size):
        yield items[idx: idx + chunk_size]


_PLAN_DRUG_STATS_COLUMNS = (
    "total_drugs",
    "auth_required",
    "auth_not_required",
    "step_required",
    "step_not_required",
    "quantity_limit",
    "quantity_no_limit",
    "last_updated_on",
)


def _plan_drug_stats_select(plan_drug_table, plan_ids):
    return (
        select(
            plan_drug_table.c.plan_id.label("plan_id"),
            func.count().label("total_drugs"),
            func.count().filter(plan_drug_table.c.prior_authorization.is_(True)).label("auth_required"),
            func.count()
            .filter(
                or_(
                    plan_drug_table.c.prior_authorization.is_(False),
                    plan_drug_table.c.prior_authorization.is_(None),
                )
            )
            .label("auth_not_required"),
            func.count().filter(plan_drug_table.c.step_therapy.is_(True)).label("step_required"),
            func.count()
            .filter(
                or_(
                    plan_drug_table.c.step_therapy.is_(False),
                    plan_drug_table.c.step_therapy.is_(None),
                )
            )
            .label("step_not_required"),
            func.count().filter(plan_drug_table.c.quantity_limit.is_(True)).label("quantity_limit"),
            func.count()
            .filter(
                or_(
                    plan_drug_table.c.quantity_limit.is_(False),
                    plan_drug_table.c.quantity_limit.is_(None),
                )
            )
            .label("quantity_no_limit"),
            func.max(plan_drug_table.c.last_updated_on).label("last_updated_on"),
        )
        .where(plan_drug_table.c.plan_id.in_(plan_ids))
        .group_by(plan_drug_table.c.plan_id)
    )


def _plan_drug_stats_upsert(stats_table, stats_select):
    stats_insert = db.insert(stats_table).from_select(
        ("plan_id", *_PLAN_DRUG_STATS_COLUMNS),
        stats_select,
    )
    return stats_insert.on_conflict_do_update(
        index_elements=[stats_table.c.plan_id],
        set_={
            column_name: getattr(stats_insert.excluded, column_name)
            for column_name in _PLAN_DRUG_STATS_COLUMNS
        },
    )


def _plan_drug_tier_upsert(plan_drug_table, tier_table, plan_ids):
    tier_label = func.coalesce(plan_drug_table.c.drug_tier, literal("UNKNOWN"))
    tier_select = (
        select(
            plan_drug_table.c.plan_id.label("plan_id"),
            tier_label.label("drug_tier"),
            func.count().label("drug_count"),
        )
        .where(plan_drug_table.c.plan_id.in_(plan_ids))
        .group_by(plan_drug_table.c.plan_id, tier_label)
    )
    tier_insert = db.insert(tier_table).from_select(
        ("plan_id", "drug_tier", "drug_count"),
        tier_select,
    )
    return tier_insert.on_conflict_do_update(
        index_elements=[tier_table.c.plan_id, tier_table.c.drug_tier],
        set_={"drug_count": tier_insert.excluded.drug_count},
    )


async def _refresh_plan_drug_statistics(plan_ids, import_date, db_schema):
    """Refresh aggregate drug statistics for the selected plans."""
    plan_ids = [plan_id for plan_id in set(plan_ids) if plan_id]
    if not plan_ids:
        return

    plan_drug_table = make_class(PlanDrugRaw, import_date, schema_override=db_schema).__table__
    stats_table = make_class(PlanDrugStats, import_date, schema_override=db_schema).__table__
    tier_table = make_class(PlanDrugTierStats, import_date, schema_override=db_schema).__table__
    for plan_id_chunk in _chunked(plan_ids):
        stats_select = _plan_drug_stats_select(plan_drug_table, plan_id_chunk)
        await _plan_drug_stats_upsert(stats_table, stats_select).status()
        await _plan_drug_tier_upsert(plan_drug_table, tier_table, plan_id_chunk).status()


async def _refresh_all_plan_drug_statistics(import_date, db_schema):
    plan_drug_cls = make_class(PlanDrugRaw, import_date, schema_override=db_schema)
    qualified_name = f"{db_schema}.{plan_drug_cls.__tablename__}"
    exists = await db.scalar("SELECT to_regclass(:qualified_name)", qualified_name=qualified_name)
    if not exists:
        logger.info("Skipping plan-drug stats refresh; %s does not exist", qualified_name)
        return

    rows = await db.all(
        text(
            f"""
            SELECT DISTINCT plan_id
              FROM {qualified_name}
             WHERE plan_id IS NOT NULL
            """
        )
    )
    plan_ids = []
    for row in rows:
        if hasattr(row, "plan_id"):
            plan_ids.append(row.plan_id)
        else:
            plan_ids.append(row[0])
    await _refresh_plan_drug_statistics(plan_ids, import_date, db_schema)


async def _plan_summary_dependencies_ready(db_schema: str) -> tuple[bool, list[str]]:
    missing_tables = []
    for table_name in ("plan_attributes", "plan_benefits", "plan_prices"):
        qualified_name = f"{db_schema}.{table_name}"
        exists = await db.scalar("SELECT to_regclass(:qualified_name)", qualified_name=qualified_name)
        if not exists:
            missing_tables.append(table_name)
    return (len(missing_tables) == 0, missing_tables)


async def process_formulary(ctx, task):
    """
    Download and store formulary (drugs.json) data for an issuer.
    """
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    is_test_mode_enabled = is_test_mode(ctx)
    drug_limit = TEST_DRUG_RECORDS if is_test_mode_enabled else None
    formulary_flush_rows = _mrf_formulary_flush_rows(is_test_mode_enabled)
    source_url = str(task.get("source_url") or task.get("url") or "")
    download_url = str(task.get("input_url") or source_url)
    await ensure_database(is_test_mode_enabled)

    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", is_test_mode_enabled)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    myplan_drug = make_class(PlanDrugRaw, import_date, schema_override=db_schema)

    print("Starting Formulary file data download: ", source_url)
    with tempfile.TemporaryDirectory() as tmpdirname:
        source_path = Path(urlparse(download_url).path if download_url.startswith("file://") else source_url)
        tmp_filename = str(PurePath(str(tmpdirname), source_path.name))
        try:
            await download_it_and_save(
                download_url,
                tmp_filename,
                context={"issuer_array": task["issuer_array"], "source": "formulary"},
                logger=myimportlog,
            )
        except Exception as exc:
            logger.warning("Failed to download formulary data from %s: %s", source_url, exc)
            await _mark_mrf_task_terminal(ctx, task, "formulary", cleanup_chunk=True)
            return

        if await _has_enqueued_mrf_file_chunks(ctx, task, tmp_filename, "formulary", "process_formulary"):
            await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, "formulary"))
            return 1

        drug_rows = []
        processed = 0
        try:
            async with async_open(tmp_filename, "rb") as afp:
                async for formulary_record in ijson.items(afp, "item", use_float=True):
                    rxnorm_id = str(formulary_record.get("rxnorm_id", "")).strip()
                    drug_name = str(formulary_record.get("drug_name", "")).strip()
                    plans = formulary_record.get("plans") or []
                    if not rxnorm_id or not drug_name or not isinstance(plans, list) or not plans:
                        await log_error(
                            "err",
                            f"Missing required drug fields. rxnorm_id={rxnorm_id!r}",
                            task.get("issuer_array"),
                            task.get("url"),
                            "formulary",
                            "json",
                            myimportlog,
                        )
                        continue

                    for plan_entry in plans:
                        plan_id = str(plan_entry.get("plan_id", "")).strip()
                        plan_id_type = str(plan_entry.get("plan_id_type", "")).strip()
                        if not plan_id or not plan_id_type:
                            await log_error(
                                "err",
                                f"Plan entry missing identifiers for rxnorm_id={rxnorm_id}",
                                task.get("issuer_array"),
                                task.get("url"),
                                "formulary",
                                "json",
                                myimportlog,
                            )
                            continue
                        drug_tier = plan_entry.get("drug_tier")
                        if isinstance(drug_tier, str):
                            drug_tier = drug_tier.strip().upper()
                        drug_row_dict = {
                            "plan_id": plan_id,
                            "plan_id_type": plan_id_type,
                            "rxnorm_id": rxnorm_id,
                            "drug_name": drug_name,
                            "drug_tier": drug_tier,
                            "prior_authorization": _parse_optional_bool(plan_entry.get("prior_authorization")),
                            "step_therapy": _parse_optional_bool(plan_entry.get("step_therapy")),
                            "quantity_limit": _parse_optional_bool(plan_entry.get("quantity_limit")),
                            "last_updated_on": None,
                        }
                        if formulary_record.get("last_updated_on"):
                            try:
                                drug_row_dict["last_updated_on"] = datetime.datetime.combine(
                                    parse_date(formulary_record["last_updated_on"], fuzzy=True),
                                    datetime.datetime.min.time(),
                                )
                            except (ValueError, TypeError):
                                drug_row_dict["last_updated_on"] = None
                        drug_rows.append(drug_row_dict)

                    processed += 1
                    if drug_limit and processed >= drug_limit:
                        break
                    if len(drug_rows) > formulary_flush_rows:
                        await _push_mrf_duplicate_tolerant_rows(drug_rows, myplan_drug)
                        drug_rows.clear()

        except ijson.IncompleteJSONError as exc:
            await log_error(
                "err",
                f"Incomplete JSON: can't read expected data. {exc}",
                task.get("issuer_array"),
                task.get("url"),
                "formulary",
                "json",
                myimportlog,
            )
            await _mark_mrf_task_terminal(ctx, task, "formulary", cleanup_chunk=True)
            return
        except ijson.JSONError as exc:
            await log_error(
                "err",
                f"JSON Parsing Error: {exc}",
                task.get("issuer_array"),
                task.get("url"),
                "formulary",
                "json",
                myimportlog,
            )
            await _mark_mrf_task_terminal(ctx, task, "formulary", cleanup_chunk=True)
            return

        if drug_rows:
            await _push_mrf_duplicate_tolerant_rows(drug_rows, myplan_drug)

    await flush_error_log(myimportlog)
    await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, "formulary"))
    _cleanup_mrf_chunk_file(task)
    return 1


async def save_mrf_data(ctx, task):
    """Persist one queued batch of normalized MRF records."""
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    pending_writes = []
    print("Got task for saving MRF data")
    for key in task:
        match key:
            case "plan_npi":
                myplan_npi = make_class(PlanNPIRaw, import_date, schema_override=db_schema)
                pending_writes.append(push_objects(task["plan_npi"], myplan_npi, rewrite=True))
            case "plan_networktier":
                myplan_networktier = make_class(PlanNetworkTierRaw, import_date, schema_override=db_schema)
                pending_writes.append(push_objects(task["plan_networktier"], myplan_networktier, rewrite=True))
            case "plan_drugs":
                myplan_drugs = make_class(PlanDrugRaw, import_date, schema_override=db_schema)
                await push_objects(task["plan_drugs"], myplan_drugs, rewrite=True)
            case "plan_benefits_marketplace":
                myplanbenefitsmarketplace = make_class(
                    PlanBenefitsMarketplace, import_date, schema_override=db_schema
                )
                pending_writes.append(
                    push_objects(task["plan_benefits_marketplace"], myplanbenefitsmarketplace)
                )
            case "mrf_address":
                mymrfaddress = make_class(MRFAddress, import_date, schema_override=db_schema)
                pending_writes.append(_push_mrf_address_rows(task["mrf_address"], mymrfaddress))
            case "mrf_address_evidence":
                mymrfaddressevidence = make_class(MRFAddressEvidence, import_date, schema_override=db_schema)
                pending_writes.append(push_objects(task["mrf_address_evidence"], mymrfaddressevidence))
            case "npi_other_id_list":
                mynpidataotheridentifier = make_class(
                    NPIDataOtherIdentifier, import_date, schema_override=db_schema
                )
                pending_writes.append(
                    push_objects(task["npi_other_id_list"], mynpidataotheridentifier, rewrite=True)
                )
            case "npi_taxonomy_group_list":
                mynpidatataxonomygroup = make_class(
                    NPIDataTaxonomyGroup, import_date, schema_override=db_schema
                )
                pending_writes.append(
                    push_objects(task["npi_taxonomy_group_list"], mynpidatataxonomygroup, rewrite=True)
                )
            case "npi_address_list":
                mynpiaddress = make_class(NPIAddress, import_date, schema_override=db_schema)
                pending_writes.append(push_objects(task["npi_address_list"], mynpiaddress, rewrite=True))
            case "context":
                continue
            case _:
                print("Some wrong key passed")
    await asyncio.gather(*pending_writes)


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
    redis = ctx["redis"]
    issuer_array = task["issuer_array"]
    print(f"CTX: {ctx} \n TASK: {task}")
    if "context" in task:
        ctx["context"] = task["context"]
    import_date = ctx["context"]["import_date"]
    job_scope = str(ctx["context"].get("control_run_id") or import_date)
    is_test_mode_enabled = is_test_mode(ctx)
    await ensure_database(is_test_mode_enabled)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", is_test_mode_enabled)

    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    with tempfile.TemporaryDirectory() as tmpdirname:
        source_path = Path(task.get("url"))
        tmp_filename = str(PurePath(str(tmpdirname), source_path.name))
        try:
            await download_it_and_save(
                task.get("url"),
                tmp_filename,
                context={"issuer_array": task["issuer_array"], "source": "json_index"},
                logger=myimportlog,
            )
        except Exception as exc:
            logger.warning("Failed to download MRF index data from %s: %s", task.get("url"), exc)
            await _mark_mrf_task_terminal(ctx, task, "index")
            return
        plan_limit = TEST_PLAN_URLS if is_test_mode_enabled else None
        provider_limit = TEST_PROVIDER_URLS if is_test_mode_enabled else None
        formulary_limit = TEST_FORMULARY_URLS if is_test_mode_enabled else None
        enqueued_plans = 0
        enqueued_providers = 0
        enqueued_formularies = 0

        async with async_open(tmp_filename, "rb") as afp:
            try:
                async for url in ijson.items(
                    afp, "plan_urls.item", use_float=True
                ):  # , 'formulary_urls', 'provider_urls'
                    print(f"Plan URL: {url}")
                    work_id = _mrf_url_job_id("plan", job_scope, str(url))
                    plan_task_dict = {
                        "url": url,
                        "issuer_array": issuer_array,
                        "context": ctx["context"],
                        "work_id": work_id,
                    }
                    if not await _has_registered_mrf_work(
                        redis,
                        job_scope,
                        work_id,
                        function_name="process_plan",
                        task=plan_task_dict,
                    ):
                        continue
                    await redis.enqueue_job(
                        "process_plan",
                        plan_task_dict,
                        _queue_name=MRF_QUEUE_NAME,
                        _job_id=work_id,
                    )
                    # break
                    enqueued_plans += 1
                    if plan_limit and enqueued_plans >= plan_limit:
                        break
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "json_index",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "index")
                return
        async with async_open(tmp_filename, "rb") as afp:
            try:
                async for url in ijson.items(
                    afp, "formulary_urls.item", use_float=True
                ):
                    print(f"Formulary URL: {url}")
                    work_id = _mrf_url_job_id("formulary", job_scope, str(url))
                    formulary_task_dict = {
                        "url": url,
                        "issuer_array": issuer_array,
                        "context": ctx["context"],
                        "work_id": work_id,
                    }
                    if not await _has_registered_mrf_work(
                        redis,
                        job_scope,
                        work_id,
                        function_name="process_formulary",
                        task=formulary_task_dict,
                    ):
                        continue
                    await redis.enqueue_job(
                        "process_formulary",
                        formulary_task_dict,
                        _queue_name=MRF_QUEUE_NAME,
                        _job_id=work_id,
                    )
                    enqueued_formularies += 1
                    if formulary_limit and enqueued_formularies >= formulary_limit:
                        break
            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "index",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "index")
                return
            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "json_index",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "index")
                return

        seen_provider_urls = set()
        async with async_open(tmp_filename, "rb") as afp:
            try:
                async for url in ijson.items(
                    afp, "provider_urls.item", use_float=True
                ):  # , 'formulary_urls', 'provider_urls'
                    url = str(url).strip()
                    if not url or url in seen_provider_urls:
                        continue
                    seen_provider_urls.add(url)
                    print(f"Provider URL: {url}")
                    work_id = _mrf_url_job_id("provider", job_scope, url)
                    provider_task_dict = {
                        "url": url,
                        "issuer_array": issuer_array,
                        "context": ctx["context"],
                        "work_id": work_id,
                    }
                    if not await _has_registered_mrf_work(
                        redis,
                        job_scope,
                        work_id,
                        function_name="process_provider",
                        task=provider_task_dict,
                    ):
                        continue
                    await redis.enqueue_job(
                        "process_provider",
                        provider_task_dict,
                        _queue_name=MRF_QUEUE_NAME,
                        _job_id=work_id,
                    )
                    # break
                    enqueued_providers += 1
                    if provider_limit and enqueued_providers >= provider_limit:
                        break
            except ijson.IncompleteJSONError as exc:
                await log_error(
                    "err",
                    f"Incomplete JSON: can't read expected data. {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "index",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "index")
                return

            except ijson.JSONError as exc:
                await log_error(
                    "err",
                    f"JSON Parsing Error: {exc}",
                    task.get("issuer_array"),
                    task.get("url"),
                    "json_index",
                    "json",
                    myimportlog,
                )
                await _mark_mrf_task_terminal(ctx, task, "index")
                return

        await _mark_mrf_work_done(ctx, _mrf_task_work_id(ctx, task, "index"))


async def import_unknown_state_issuers_data(test_mode: bool = False):
    """Import issuer and plan identities missing explicit state attribution."""

    plans_by_key = {}
    issuers_by_id = {}

    attribute_files = json.loads(os.environ["HLTHPRT_CMSGOV_PLAN_ATTRIBUTES_URL_PUF"])
    processed_rows = 0
    row_limit = TEST_UNKNOWN_STATE_ROWS if test_mode else None
    for file in attribute_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            archive_name = "attr.csv"
            tmp_filename = str(PurePath(str(tmpdirname), archive_name + ".zip"))
            await download_it_and_save(file["url"], tmp_filename)
            try:
                await unzip(tmp_filename, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for %s: %s", tmp_filename, exc)
                with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            tmp_filename = glob.glob(f"{tmpdirname}/*.csv")[0]

            async with async_open(tmp_filename, "r", encoding="utf-8-sig") as afp:
                async for attribute_row in AsyncDictReader(afp, delimiter=","):
                    if not attribute_row["StandardComponentId"] or not attribute_row["PlanId"]:
                        continue
                    plan_key = f"{attribute_row['StandardComponentId']}_{attribute_row['BusinessYear']}"
                    if plan_key in plans_by_key:
                        continue
                    plans_by_key[plan_key] = {
                        "plan_id": attribute_row["StandardComponentId"],
                        "plan_id_type": "CMS-HIOS-PLAN-ID",
                        "year": int(attribute_row["BusinessYear"]),
                        "issuer_id": int(attribute_row["IssuerId"]),
                        "state": str(attribute_row["StateCode"]).upper(),
                        "marketing_name": attribute_row["PlanMarketingName"],
                        "summary_url": attribute_row["URLForSummaryofBenefitsCoverage"],
                        "marketing_url": attribute_row["PlanBrochure"],
                        "formulary_url": attribute_row["FormularyURL"],
                        "plan_contact": "",
                        "network": [attribute_row["NetworkId"]],
                        "benefits": [],
                        "last_updated_on": datetime.datetime.combine(
                            parse_date(attribute_row["ImportDate"], fuzzy=True), datetime.datetime.min.time()
                        ),
                        "checksum": return_checksum(
                            [
                                attribute_row["StandardComponentId"].lower(),
                                int(attribute_row["BusinessYear"]),
                            ],
                            crc=32,
                        ),
                    }

                    issuers_by_id[int(attribute_row["IssuerId"])] = {
                        "state": str(attribute_row["StateCode"]).upper(),
                        "issuer_id": int(attribute_row["IssuerId"]),
                        "mrf_url": "",
                        "data_contact_email": "",
                        "issuer_marketing_name": "",
                        "issuer_name": (
                            attribute_row["IssuerMarketPlaceMarketingName"].strip()
                            if attribute_row["IssuerMarketPlaceMarketingName"].strip()
                            else attribute_row["IssuerId"]
                        ),
                    }
                    # except:
                    #     from pprint import pprint
                    #     pprint(row)

    state_attribute_files = json.loads(os.environ["HLTHPRT_CMSGOV_STATE_PLAN_ATTRIBUTES_URL_PUF"])
    for file in state_attribute_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            archive_name = "attr.csv"
            tmp_filename = str(PurePath(str(tmpdirname), archive_name + ".zip"))
            await download_it_and_save(file["url"], tmp_filename)
            try:
                await unzip(tmp_filename, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for state attributes %s: %s", tmp_filename, exc)
                with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            csv_files = glob.glob(f"{tmpdirname}/*Plans*.csv")
            tmp_filename = csv_files[0] if csv_files else glob.glob(f"{tmpdirname}/*.csv")[0]

            def to_camel_case(s):
                """Convert one whitespace-delimited label to title-cased text."""

                parts = s.split()
                return "".join(word.capitalize() for word in parts)

            field_name_by_label = {
                "STANDARD COMPONENT ID": "STANDARD COMPONENT ID",
                "PLAN ID": "PLAN ID",
                "BUSINESS YEAR": "BUSINESS YEAR",
                "ISSUER ID": "ISSUER ID",
                "STATE CODE": "STATE CODE",
                "PLAN MARKETING NAME": "PLAN MARKETING NAME",
                "URL FOR SUMMARY OF BENEFITS COVERAGE": "URL FOR SUMMARY OF BENEFITS COVERAGE",
                "PLAN BROCHURE": "PLAN BROCHURE",
                "FORMULARY URL": "FORMULARY URL",
                "IMPORT DATE": "IMPORT DATE",
                "NETWORK ID": "NETWORK ID",
                "ISSUER NAME": "ISSUER NAME",
            }

            async with async_open(tmp_filename, "r", encoding="utf-8-sig") as afp:
                async for attribute_row in AsyncDictReader(afp, delimiter=","):
                    if attribute_row.get("STANDARD COMPONENT ID") and attribute_row.get("PLAN ID"):
                        continue
                    for key in field_name_by_label:
                        field_name_by_label[key] = to_camel_case(field_name_by_label[key])
                    break

            async with async_open(tmp_filename, "r", encoding="utf-8-sig") as afp:
                async for attribute_row in AsyncDictReader(afp, delimiter=","):
                    standard_component_id = attribute_row.get(field_name_by_label["STANDARD COMPONENT ID"])
                    plan_identifier = attribute_row.get(field_name_by_label["PLAN ID"])
                    business_year = attribute_row.get(field_name_by_label["BUSINESS YEAR"])
                    if standard_component_id and plan_identifier:
                        continue
                    if not standard_component_id or business_year is None:
                        continue

                    plan_key = f"{standard_component_id.upper()}_{business_year}"
                    if plan_key in plans_by_key:
                        continue

                    issuer_id_value = attribute_row.get(field_name_by_label["ISSUER ID"])
                    plans_by_key[plan_key] = {
                        "plan_id": standard_component_id,
                        "plan_id_type": "STATE-HIOS-PLAN-ID",
                        "year": int(business_year),
                        "issuer_id": int(issuer_id_value),
                        "state": str(attribute_row.get(field_name_by_label["STATE CODE"])).upper(),
                        "marketing_name": attribute_row.get(field_name_by_label["PLAN MARKETING NAME"]),
                        "summary_url": attribute_row.get(
                            field_name_by_label["URL FOR SUMMARY OF BENEFITS COVERAGE"]
                        ),
                        "marketing_url": attribute_row.get(field_name_by_label["PLAN BROCHURE"]),
                        "formulary_url": attribute_row.get(field_name_by_label["FORMULARY URL"]),
                        "plan_contact": "",
                        "network": [attribute_row.get(field_name_by_label["NETWORK ID"])],
                        "benefits": [],
                        "last_updated_on": datetime.datetime.combine(
                            parse_date(attribute_row.get(field_name_by_label["IMPORT DATE"]), fuzzy=True),
                            datetime.datetime.min.time(),
                        ),
                        "checksum": return_checksum(
                            [
                                standard_component_id.lower(),
                                int(business_year),
                            ],
                            crc=32,
                        ),
                    }

                    issuer_name_value = (attribute_row.get(field_name_by_label["ISSUER NAME"]) or "").strip()
                    issuers_by_id[int(issuer_id_value)] = {
                        "state": str(attribute_row.get(field_name_by_label["STATE CODE"])).upper(),
                        "issuer_id": int(issuer_id_value),
                        "mrf_url": "",
                        "data_contact_email": "",
                        "issuer_marketing_name": "",
                        "issuer_name": issuer_name_value or issuer_id_value,
                    }

                    processed_rows += 1
                    if row_limit and processed_rows >= row_limit:
                        break
                if row_limit and processed_rows >= row_limit:
                    break
        if row_limit and processed_rows >= row_limit:
            break

    return (issuers_by_id, plans_by_key)


async def _read_rate_review_issuer_names(csv_files: list[str], row_limit: int | None) -> tuple[dict, int]:
    issuers_by_id = {}
    processed_rows = 0
    for csv_path in csv_files:
        async with async_open(csv_path, "r", encoding="utf-8-sig") as afp:
            async for rate_row in AsyncDictReader(afp, delimiter=","):
                issuers_by_id[int(rate_row["ISSUER_ID"])] = {
                    "state": str(rate_row["STATE"]).upper(),
                    "issuer_id": int(rate_row["ISSUER_ID"]),
                    "mrf_url": "",
                    "data_contact_email": "",
                    "issuer_marketing_name": "",
                    "issuer_name": (
                        rate_row["COMPANY"].strip()
                        if rate_row["COMPANY"].strip()
                        else rate_row["ISSUER_ID"]
                    ),
                }
                processed_rows += 1
                if row_limit and processed_rows >= row_limit:
                    return issuers_by_id, processed_rows
    return issuers_by_id, processed_rows


async def update_issuer_names_data(test_mode: bool = False):
    """Refresh issuer names from bounded federal rate-review sources."""

    issuers_by_id = {}
    rate_review_files = json.loads(os.environ["HLTHPRT_CMSGOV_RATE_REVIEW_URL_PUF"])
    processed_rows = 0
    row_limit = TEST_UNKNOWN_STATE_ROWS if test_mode else None
    for file in rate_review_files:
        with tempfile.TemporaryDirectory() as tmpdirname:
            archive_name = "some_file"
            tmp_filename = str(PurePath(str(tmpdirname), archive_name + ".zip"))
            await download_it_and_save(file["url"], tmp_filename)
            print(f"Trying to unpack1: {tmp_filename}")

            # temp solution
            with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                zip_ref.extractall(tmpdirname)

            print(glob.glob(f"{tmpdirname}/*PUF*.csv"))

            csv_files = glob.glob(f"{tmpdirname}/*PUF*.csv")
            remaining_rows = row_limit - processed_rows if row_limit else None
            file_issuers, file_row_count = await _read_rate_review_issuer_names(
                csv_files,
                remaining_rows,
            )
            issuers_by_id.update(file_issuers)
            processed_rows += file_row_count
        if row_limit and processed_rows >= row_limit:
            break

    return issuers_by_id


async def init_file(ctx, task=None):
    """
    The init_file function is the first function called in this file.
    It downloads a zip file from the CMS website, unzips it, and then parses through each worksheet to create an
    object for each row of data.
    The objects are then pushed into a database using SQLAlchemy async sessions.

    :param ctx: Pass information between functions
    :return: The following:

    """
    task = task or {}
    is_test_mode_enabled = bool(task.get("test_mode"))
    run_id = str(task.get("run_id") or "").strip() or None
    redis = ctx["redis"]
    ctx.setdefault("context", {})
    ctx["context"]["test_mode"] = is_test_mode_enabled
    if run_id:
        ctx["context"]["control_run_id"] = run_id
    if "mrf_file_chunking" in task:
        ctx["context"]["mrf_file_chunking"] = task["mrf_file_chunking"]
    await ensure_database(is_test_mode_enabled)

    mrf_source = os.environ["HLTHPRT_CMSGOV_MRF_URL_PUF"]
    try:
        if mrf_source.strip().startswith("["):
            parsed_urls = json.loads(mrf_source)
        else:
            parsed_urls = [mrf_source]
    except json.JSONDecodeError as exc:
        raise RuntimeError("Invalid HLTHPRT_CMSGOV_MRF_URL_PUF; must be JSON array or single URL") from exc
    mrf_urls = [str(url).strip() for url in parsed_urls if str(url).strip()]
    if not mrf_urls:
        raise RuntimeError("HLTHPRT_CMSGOV_MRF_URL_PUF did not provide any usable URLs")
    print("Downloading data from: ", ", ".join(mrf_urls))
    await mark_control_run(
        run_id,
        status="running",
        phase_detail="mrf preparing import tables",
        progress_message="preparing import tables",
        progress={"unit": "phase", "total": 4, "done": 0, "pct": 0, "message": "preparing import tables"},
    )

    import_date = ctx["context"]["import_date"]
    await _prepare_import_tables(import_date, is_test_mode_enabled)
    ctx["context"]["run"] += 1
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", is_test_mode_enabled)
    myissuer = make_class(Issuer, import_date, schema_override=db_schema)
    myplan = make_class(Plan, import_date, schema_override=db_schema)
    myplantransparency = make_class(PlanTransparency, import_date, schema_override=db_schema)

    with tempfile.TemporaryDirectory() as tmpdirname:
        transparent_files = json.loads(os.environ["HLTHPRT_CMSGOV_PLAN_TRANSPARENCY_URL_PUF"])
        for file_idx, file in enumerate(transparent_files):
            if is_test_mode_enabled and file_idx >= 1:
                break
            tmp_filename = _transparency_zip_path(tmpdirname, file_idx, file)
            await download_it_and_save(file["url"], tmp_filename)

            try:
                await unzip(tmp_filename, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for transparency file %s: %s", tmp_filename, exc)
                with zipfile.ZipFile(tmp_filename, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            tmp_filename = glob.glob(f"{tmpdirname}/*.xlsx")[0]
            xls_file = xl.readxl(tmp_filename)
            os.unlink(tmp_filename)

            obj_list = []
            for ws_name in xls_file.ws_names:
                if not ws_name.startswith("Transparency"):
                    continue
                count = 0
                column_index_by_name = {}
                column_name_by_heading = {
                    "State": "state",
                    "Issuer_Name": "issuer_name",
                    "Issuer_ID": "issuer_id",
                    "Is_Issuer_New_to_Exchange? (Yes_or_No)": "new_issuer_to_exchange",
                    "SADP_Only?": "sadp_only",
                    "Plan_ID": "plan_id",
                    "QHP/SADP": "qhp_sadp",
                    "Plan_Type": "plan_type",
                    "Metal_Level": "metal",
                    "URL_Claims_Payment_Policies": "claims_payment_policies_url",
                }
                for _, column_name in column_name_by_heading.items():
                    column_index_by_name[column_name] = -1

                for worksheet_row in xls_file.ws(ws=ws_name).rows:
                    if count > 2:
                        transparency_row_dict = {}
                        transparency_row_dict["state"] = str(
                            worksheet_row[column_index_by_name["state"]].upper()
                        )
                        transparency_row_dict["issuer_name"] = str(
                            worksheet_row[column_index_by_name["issuer_name"]]
                        )
                        transparency_row_dict["issuer_id"] = int(
                            worksheet_row[column_index_by_name["issuer_id"]]
                        )
                        transparency_row_dict["new_issuer_to_exchange"] = _is_truthy(
                            worksheet_row[column_index_by_name["new_issuer_to_exchange"]], ("yes", "y")
                        )
                        transparency_row_dict["sadp_only"] = _is_truthy(
                            worksheet_row[column_index_by_name["sadp_only"]], ("yes", "y")
                        )
                        transparency_row_dict["plan_id"] = str(
                            worksheet_row[column_index_by_name["plan_id"]]
                        )
                        transparency_row_dict["year"] = int(file["year"])
                        transparency_row_dict["qhp_sadp"] = str(
                            worksheet_row[column_index_by_name["qhp_sadp"]]
                        )
                        transparency_row_dict["plan_type"] = str(
                            worksheet_row[column_index_by_name["plan_type"]]
                        )
                        transparency_row_dict["metal"] = str(
                            worksheet_row[column_index_by_name["metal"]]
                        )
                        transparency_row_dict["claims_payment_policies_url"] = str(
                            worksheet_row[column_index_by_name["claims_payment_policies_url"]]
                        )

                        obj_list.append(transparency_row_dict)
                        if count > int(os.environ.get("HLTHPRT_SAVE_PER_PACK", 50)):
                            count = 3
                            await push_objects(obj_list, myplantransparency)
                            obj_list = []
                        if is_test_mode_enabled and len(obj_list) >= TEST_PLAN_TRANSPARENCY_ROWS:
                            break
                    elif count == 2:
                        i = 0
                        for name in worksheet_row:
                            if name in column_name_by_heading:
                                column_index_by_name[column_name_by_heading[name]] = i
                            i += 1
                    count += 1

                await push_objects(obj_list, myplantransparency)
                if is_test_mode_enabled and len(obj_list) >= TEST_PLAN_TRANSPARENCY_ROWS:
                    break

        (issuers_by_id, plans_by_key) = await import_unknown_state_issuers_data(
            test_mode=is_test_mode_enabled
        )
        issuers_by_id.update(await update_issuer_names_data(test_mode=is_test_mode_enabled))
        if is_test_mode_enabled:
            issuers_by_id = dict(list(issuers_by_id.items())[:TEST_UNKNOWN_STATE_ROWS])
            plans_by_key = dict(list(plans_by_key.items())[:TEST_UNKNOWN_STATE_ROWS])

        url_list: set[str] = set()
        issuer_ids_by_url = {}

        for url_idx, source_url in enumerate(mrf_urls):
            zip_name = f"mrf_puf_{url_idx}.zip"
            zip_path = str(PurePath(str(tmpdirname), zip_name))
            await download_it_and_save(source_url, zip_path)
            try:
                await unzip(zip_path, tmpdirname)
            except Exception as exc:
                logger.debug("Fallback unzip for MRF file %s: %s", zip_path, exc)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

            extracted_files = glob.glob(f"{tmpdirname}/*.xlsx")
            if not extracted_files:
                continue

            for workbook_path in extracted_files:
                xls_file = xl.readxl(workbook_path)
                ws_name = xls_file.ws_names[-1]
                os.unlink(workbook_path)

                count = 0
                obj_list = []

                for worksheet_row in xls_file.ws(ws=ws_name).rows:
                    if count != 0:
                        row_urls = []
                        raw_url = worksheet_row[2]
                        if raw_url:
                            raw_url = str(raw_url).strip()
                            if raw_url.startswith("["):
                                try:
                                    row_urls = json.loads(raw_url)
                                except json.JSONDecodeError:
                                    row_urls = [raw_url]
                            else:
                                row_urls = [raw_url]
                        row_urls = [str(row_url).strip() for row_url in row_urls if str(row_url).strip()]
                        if not row_urls:
                            count += 1
                            continue

                        issuer_row_dict = {
                            "state": worksheet_row[0].upper(),
                            "issuer_id": int(worksheet_row[1]),
                            "issuer_marketing_name": "",
                            "data_contact_email": (
                                (worksheet_row[3] or "").strip() if worksheet_row[3] else ""
                            ),
                        }
                        issuer_stmt = select(myplantransparency.issuer_name).where(
                            myplantransparency.issuer_id == issuer_row_dict["issuer_id"]
                        )
                        issuer_name = await db.scalar(issuer_stmt)
                        issuer_row_dict["issuer_name"] = issuer_name if issuer_name else "N/A"
                        for single_url in row_urls:
                            issuer_row_dict["mrf_url"] = single_url
                            obj_list.append(issuer_row_dict.copy())
                            issuer_ids_by_url.setdefault(single_url, []).append(issuer_row_dict["issuer_id"])
                            url_list.add(single_url)
                            existing_issuer = issuers_by_id.get(issuer_row_dict["issuer_id"])
                            if existing_issuer:
                                if not existing_issuer.get("mrf_url"):
                                    existing_issuer["mrf_url"] = single_url
                                if issuer_row_dict["data_contact_email"] and not existing_issuer.get(
                                    "data_contact_email"
                                ):
                                    existing_issuer["data_contact_email"] = issuer_row_dict[
                                        "data_contact_email"
                                    ]
                                if issuer_row_dict["issuer_name"] and not existing_issuer.get("issuer_name"):
                                    existing_issuer["issuer_name"] = issuer_row_dict["issuer_name"]
                                if issuer_row_dict["issuer_marketing_name"] and not existing_issuer.get(
                                    "issuer_marketing_name"
                                ):
                                    existing_issuer["issuer_marketing_name"] = issuer_row_dict[
                                        "issuer_marketing_name"
                                    ]
                            else:
                                issuers_by_id[issuer_row_dict["issuer_id"]] = issuer_row_dict.copy()
                    count += 1

                # obj_list mirrors legacy behaviour (kept for potential reuse), but inserts are handled via issuer_list.

            try:
                os.unlink(zip_path)
            except FileNotFoundError:
                logger.debug("Issuer archive was already removed: %s", zip_path)

        await asyncio.gather(
            push_objects(list(issuers_by_id.values()), myissuer),
            push_objects(list(plans_by_key.values()), myplan),
        )
        enqueue_live_progress(
            run_id=run_id,
            importer="mrf",
            status="running",
            phase="mrf issuer data staged",
            unit="phase",
            total=4,
            done=2,
            pct=50,
            message=f"staged {len(issuers_by_id)} issuers and {len(plans_by_key)} plans",
        )

        max_urls = TEST_PLAN_URLS if is_test_mode_enabled else None
        selected_urls = sorted(url_list)[:max_urls] if max_urls else sorted(url_list)
        state_run_id = _mrf_run_state_id(ctx)
        await _init_mrf_run_state(redis, state_run_id)
        await mark_control_run(
            run_id,
            status="running",
            phase_detail="mrf index jobs enqueuing",
            progress_message=f"enqueuing {len(selected_urls)} index job(s)",
            metrics={
                "index_url_count": len(selected_urls),
                "issuer_count": len(issuers_by_id),
                "plan_count": len(plans_by_key),
            },
            progress={
                "unit": "index_jobs",
                "total": len(selected_urls),
                "done": 0,
                "pct": 50,
                "message": f"enqueuing {len(selected_urls)} index job(s)",
            },
        )

        for idx, url in enumerate(selected_urls):
            work_id = _mrf_url_job_id("index", _mrf_job_scope(ctx), url)
            index_task_dict = {
                "url": url,
                "issuer_array": issuer_ids_by_url[url],
                "context": ctx["context"],
                "work_id": work_id,
            }
            if not await _has_registered_mrf_work(
                redis,
                state_run_id,
                work_id,
                function_name="process_json_index",
                task=index_task_dict,
            ):
                continue
            await redis.enqueue_job(
                "process_json_index",
                index_task_dict,
                _queue_name=MRF_QUEUE_NAME,
                _job_id=work_id,
            )
            enqueue_live_progress(
                run_id=run_id,
                importer="mrf",
                status="running",
                phase="mrf index jobs enqueued",
                unit="index_jobs",
                total=len(selected_urls),
                done=idx + 1,
                message=f"enqueued {idx + 1}/{len(selected_urls)} index job(s)",
            )

        shutdown_job_id = f"shutdown_mrf_{ctx['context']['import_date']}"
        await redis.enqueue_job(
            "shutdown",
            {"context": ctx["context"], "test_mode": is_test_mode_enabled},
            _job_id=shutdown_job_id,
            _queue_name=MRF_FINISH_QUEUE_NAME,
        )
        # break


async def startup(ctx):
    """Initialize database and control metadata for the initial import worker."""

    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    control_run_id = os.environ.get("HLTHPRT_CONTROL_RUN_ID")
    if control_run_id:
        ctx["context"]["control_run_id"] = control_run_id
    override_import_id = os.environ.get("HLTHPRT_IMPORT_ID_OVERRIDE")
    if override_import_id:
        ctx["context"]["import_date"] = override_import_id
    else:
        ctx["context"]["import_date"] = datetime.datetime.utcnow().strftime("%Y%m%d")


async def shutdown(ctx, task):
    """
    The shutdown function is called after the import process has completed.
    It should be used to clean up any temporary tables or files that were created during the import process.


    :param ctx: Pass the context of the import process to other functions
    :return: A coroutine
    """
    if "context" in task:
        ctx["context"] = task["context"]
    run_id = str(ctx.get("context", {}).get("control_run_id") or "").strip() or None
    import_date = ctx["context"]["import_date"]
    is_test_mode_enabled = is_test_mode(ctx)
    redis = ctx.get("redis")
    state_run_id = _mrf_run_state_id(ctx)
    if redis and state_run_id:
        finalized_key = _mrf_state_key(state_run_id, "finalized")
        if await redis.get(finalized_key):
            logger.info("MRF run %s already finalized; skipping duplicate shutdown", state_run_id)
            await _cleanup_mrf_finalize_jobs(redis, import_date)
            return 1

        total_work, done_work = await _get_mrf_run_progress(redis, state_run_id)
        if total_work and done_work < total_work:
            wait_count = _safe_int(task.get("mrf_finalize_waits"), 0)
            max_waits = _env_int("HLTHPRT_MRF_FINISH_MAX_REQUEUES", 360)
            delay_seconds = _env_int("HLTHPRT_MRF_FINISH_REQUEUE_SECONDS", 60)
            recovery = await _recover_missing_mrf_work(redis, state_run_id)
            pct = min(89, int((done_work / max(total_work, 1)) * 85))
            message = f"waiting for parser jobs {done_work}/{total_work}"
            if recovery["recovered"]:
                message += f"; requeued {recovery['recovered']} missing work item(s)"
            if recovery["active"]:
                message += f"; {recovery['active']} still active"
            blocked_ids = [*recovery["unrecoverable"], *recovery["exhausted"]]
            failure_waits = _env_int("HLTHPRT_MRF_RECOVERY_FAILURE_WAITS", 5)
            if blocked_ids and wait_count >= failure_waits:
                error_dict = {
                    "code": "mrf_parser_recovery_failed",
                    "message": "MRF parser work could not be recovered",
                    "done_work": done_work,
                    "total_work": total_work,
                    "unrecoverable_work_ids": recovery["unrecoverable"][:20],
                    "exhausted_work_ids": recovery["exhausted"][:20],
                }
                await mark_control_run(
                    run_id,
                    status="failed",
                    phase_detail="mrf parser recovery failed",
                    progress_message=error_dict["message"],
                    error=error_dict,
                    progress={
                        "unit": "work_items",
                        "total": total_work,
                        "done": done_work,
                        "pct": pct,
                        "message": error_dict["message"],
                        "phase": "mrf parser recovery failed",
                        "recovery": recovery,
                    },
                )
                raise RuntimeError(
                    f"MRF parser recovery failed: done={done_work} total={total_work}"
                )
            if wait_count >= max_waits:
                error_dict = {
                    "code": "mrf_parser_jobs_incomplete",
                    "message": "MRF finalization waited too long for parser jobs",
                    "done_work": done_work,
                    "total_work": total_work,
                }
                await mark_control_run(
                    run_id,
                    status="failed",
                    phase_detail="mrf parser jobs incomplete",
                    progress_message=error_dict["message"],
                    error=error_dict,
                    progress={
                        "unit": "work_items",
                        "total": total_work,
                        "done": done_work,
                        "pct": pct,
                        "message": error_dict["message"],
                        "phase": "mrf parser jobs incomplete",
                        "recovery": recovery,
                    },
                )
                raise RuntimeError(
                    f"MRF finalization waited too long for parser jobs: done={done_work} total={total_work}"
                )
            phase = "mrf parser recovery queued" if recovery["recovered"] else "mrf parser jobs running"
            await mark_control_run(
                run_id,
                status="running",
                phase_detail=phase,
                progress_message=message,
                progress={
                    "unit": "work_items",
                    "total": total_work,
                    "done": done_work,
                    "pct": pct,
                    "message": message,
                    "phase": phase,
                    "recovery": recovery,
                },
            )
            await redis.enqueue_job(
                "shutdown",
                {
                    "context": ctx["context"],
                    "test_mode": is_test_mode_enabled,
                    "mrf_finalize_waits": wait_count + 1,
                },
                _job_id=f"shutdown_mrf_{import_date}_wait_{wait_count + 1}",
                _queue_name=MRF_FINISH_QUEUE_NAME,
                _defer_by=delay_seconds,
            )
            return 1

        if total_work and not await _has_claimed_mrf_finalize_lock(redis, state_run_id):
            wait_count = _safe_int(task.get("mrf_finalize_waits"), 0)
            delay_seconds = _env_int("HLTHPRT_MRF_FINISH_REQUEUE_SECONDS", 60)
            await redis.enqueue_job(
                "shutdown",
                {
                    "context": ctx["context"],
                    "test_mode": is_test_mode_enabled,
                    "mrf_finalize_waits": wait_count + 1,
                },
                _job_id=f"shutdown_mrf_{import_date}_lock_wait_{wait_count + 1}",
                _queue_name=MRF_FINISH_QUEUE_NAME,
                _defer_by=delay_seconds,
            )
            return 1

    await mark_control_run(
        run_id,
        status="running",
        phase_detail="mrf finalizing",
        progress_message="finalizing",
        progress={"unit": "phase", "total": 4, "done": 3, "pct": 90, "message": "finalizing"},
    )
    await ensure_database(is_test_mode_enabled)
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", is_test_mode_enabled)
    myimportlog = make_class(ImportLog, import_date, schema_override=db_schema)
    await flush_error_log(myimportlog)
    await db.status("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    await db.status("CREATE EXTENSION IF NOT EXISTS btree_gin;")

    test = make_class(Plan, import_date, schema_override=db_schema)
    plans_count = await db.scalar(select(func.count(test.plan_id)))
    if is_test_mode_enabled:
        print(f"Test mode: imported {plans_count} plan rows (no minimum enforced).")
    else:
        if not plans_count or plans_count < 500:
            print(f"Failed Import: Plans number:{plans_count}")
            sys.exit(1)

    await _refresh_all_plan_drug_statistics(import_date, db_schema)
    await _refresh_mrf_address_summary(import_date, db_schema)
    address_stats = None
    if source_enabled("mrf") and not is_test_mode_enabled:
        mrf_address_stage = make_class(MRFAddress, import_date, schema_override=db_schema)
        mrf_evidence_stage = make_class(MRFAddressEvidence, import_date, schema_override=db_schema)
        address_field_map = {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city_name",
            "state": "state_name",
            "zip": "postal_code",
            "country": "COALESCE(NULLIF(country_code, ''), 'US')",
        }
        should_repair_existing_address_keys = _is_truthy(
            os.environ.get("HLTHPRT_ADDRESS_CANON_REPAIR_EXISTING"),
            ("yes", "y", "true", "1"),
        )
        await stamp_address_keys(
            mrf_address_stage.__tablename__,
            address_field_map,
            schema=db_schema,
            update_existing=should_repair_existing_address_keys,
        )
        await propagate_child_address_keys(
            mrf_evidence_stage.__tablename__,
            mrf_address_stage.__tablename__,
            schema=db_schema,
            skip_when_child_fully_keyed=not should_repair_existing_address_keys,
        )
        await stamp_address_keys(
            mrf_evidence_stage.__tablename__,
            address_field_map,
            schema=db_schema,
            update_existing=False,
        )
        address_stats = await resolve_into_archive(
            mrf_address_stage.__tablename__,
            address_field_map,
            source_bit=16,
            priority=5,
            schema=db_schema,
        )
        logger.info("MRF canonical address resolve complete: %s", address_stats)
        if _is_truthy(os.environ.get("HLTHPRT_MRF_OPENADDRESSES_BACKFILL"), ("yes", "y", "true", "1")):
            oa_stats = await refresh_archive_geocodes_from_openaddresses(schema=db_schema)
            logger.info(
                "OpenAddresses archive backfill after MRF canonical resolve: exact=%s fuzzy=%s relaxed=%s",
                oa_stats.exact_updates,
                oa_stats.fuzzy_updates,
                oa_stats.relaxed_updates,
            )
        else:
            logger.info("Skipping OpenAddresses archive backfill during MRF publish")
    elif is_test_mode_enabled:
        logger.info("Skipping MRF archive address resolve in test mode")

    staging_tables_by_main_name = {}
    async with db.transaction():
        for cls in (
            Issuer,
            Plan,
            PlanFormulary,
            PlanBenefitsMarketplace,
            PlanTransparency,
            PlanDrugRaw,
            PlanDrugStats,
            PlanDrugTierStats,
            ImportLog,
            PlanNPIRaw,
            PlanNetworkTierRaw,
            MRFAddress,
            MRFAddressEvidence,
        ):
            staging_tables_by_main_name[cls.__main_table__] = make_class(
                cls, import_date, schema_override=db_schema
            )
            staging_cls = staging_tables_by_main_name[cls.__main_table__]
            table = staging_cls.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
            await db.status(
                f"ALTER TABLE IF EXISTS {db_schema}.{staging_cls.__tablename__} RENAME TO {table};"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS " f"{db_schema}.{table}_idx_primary RENAME TO " f"{table}_idx_primary_old;"
            )

            await db.status(
                f"ALTER INDEX IF EXISTS "
                f"{db_schema}.{staging_cls.__tablename__}_idx_primary RENAME TO "
                f"{table}_idx_primary;"
            )

            if cls in {PlanBenefitsMarketplace, MRFAddress, MRFAddressEvidence}:
                move_indexes = []
                if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                    move_indexes += list(cls.__my_initial_indexes__)
                if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
                    move_indexes += list(cls.__my_additional_indexes__)
                for index in move_indexes:
                    index_name = index.get("name", "_".join(index.get("index_elements")))
                    await db.status(
                        f"ALTER INDEX IF EXISTS "
                        f"{db_schema}.{table}_idx_{index_name} RENAME TO "
                        f"{table}_idx_{index_name}_old;"
                    )
                    await db.status(
                        f"ALTER INDEX IF EXISTS "
                        f"{db_schema}.{staging_cls.__tablename__}_idx_{index_name} RENAME TO "
                        f"{table}_idx_{index_name};"
                    )

    upsert_history = (
        db.insert(ImportHistory)
        .values(import_id=import_date, when=db.func.now())
        .on_conflict_do_update(
            index_elements=ImportHistory.__my_index_elements__,
            index_where=ImportHistory.import_id == import_date,
            set_={"when": db.func.now()},
        )
    )
    await upsert_history.status()
    print("Plans in DB: ", await db.scalar(select(func.count(Plan.plan_id))))
    if is_test_mode_enabled:
        summary_ready, missing_summary_tables = await _plan_summary_dependencies_ready(db_schema)
        if not summary_ready:
            print(
                "Skipping plan search summary rebuild in test mode; missing tables: "
                + ", ".join(missing_summary_tables)
            )
            summary_rows = 0
        else:
            summary_rows = await rebuild_plan_search_summary(test_mode=is_test_mode_enabled)
    else:
        summary_rows = await rebuild_plan_search_summary(test_mode=is_test_mode_enabled)
    print("Plan search summary rows: ", summary_rows)
    start_time = ctx.get("context", {}).get("start")
    if start_time:
        print_time_info(start_time)
    else:
        logger.info("MRF finish context missing start time; skipping elapsed time output")
    if redis and state_run_id:
        await redis.set(_mrf_state_key(state_run_id, "finalized"), "1", ex=_mrf_run_state_ttl_seconds())
        _cleanup_mrf_run_chunks(ctx)
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="mrf import published",
        progress_message="succeeded",
        metrics={
            "plans_count": plans_count,
            "summary_rows": summary_rows,
            **({"address_resolve": address_stats.__dict__} if address_stats else {}),
        },
        progress={"unit": "phase", "total": 4, "done": 4, "pct": 100, "message": "succeeded", "phase": "mrf import published"},
    )
    if redis:
        await _cleanup_mrf_finalize_jobs(redis, import_date)


async def main(test_mode: bool = False):
    """
    The main function is the entry point of the application.

    :return: A coroutine
    """
    redis = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    await redis.enqueue_job("init_file", {"test_mode": test_mode}, _queue_name=MRF_QUEUE_NAME)


async def finish_main(test_mode: bool = False, import_id: str | None = None):
    """Queue finalization for one MRF import run."""
    redis = await create_pool(build_redis_settings(), job_serializer=serialize_job, job_deserializer=deserialize_job)
    resolved_import_id = import_id or os.environ.get("HLTHPRT_IMPORT_ID_OVERRIDE") or datetime.datetime.utcnow().strftime(
        "%Y%m%d"
    )
    finish_context_dict = {
        "import_date": resolved_import_id,
        "test_mode": bool(test_mode),
    }
    await redis.enqueue_job(
        "shutdown",
        {"context": finish_context_dict, "test_mode": bool(test_mode)},
        _queue_name=MRF_FINISH_QUEUE_NAME,
        _job_id=f"shutdown_mrf_{resolved_import_id}",
    )
