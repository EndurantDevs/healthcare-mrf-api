# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded hospital-price refresh worker and command entrypoint."""

from __future__ import annotations

import asyncio
import tempfile
import uuid
from pathlib import Path
from typing import Any, Sequence

from process.control_cancel import ImportCancelledError, raise_if_cancelled
from process.ext.utils import ensure_database
from process.formulary_fhir.async_safety import drain_operation
from process.hospital_hpt_registry import selected_hospital_hpt_registry
from process.hospital_price_acquisition import (
    Attempt,
    Candidate,
    DownloadedSource,
    candidates_from_locators,
    download_source,
    error_details,
    fetch_locator,
    positive_env,
    run_native_parser,
    sync_registry,
)
from process.hospital_price_native import (
    detect_hospital_mrf_format,
    hospital_price_version_id,
)
from process import hospital_price_runtime as _runtime
from process.hospital_price_store import (
    admit_attempts,
    fail_attempts as _fail_attempts,
    has_existing_version,
    publish_existing,
    stage_content,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.input_artifact_retention import (
    artifact_lease_context,
    guard_artifact_lease,
)


HOSPITAL_PRICE_QUEUE_NAME = "arq:HospitalPrices"
DEFAULT_FETCH_CONCURRENCY = 8
DEFAULT_LOAD_CONCURRENCY = 2
DEFAULT_ATTEMPT_LEASE_SECONDS = 300
DEFAULT_ATTEMPT_HEARTBEAT_SECONDS = 60

_bounded = _runtime.bounded
_cancel_and_drain = _runtime.cancel_and_drain
_guard_cancellation = _runtime.guard_cancellation
_hospital_resource_lock = _runtime.hospital_resource_lock
_locator_groups = _runtime.locator_groups
_progress = _runtime.progress
_require_disk_capacity = _runtime.require_disk_capacity
_resource_limits = _runtime.resource_limits


async def _start_attempts(
    candidates: Sequence[Candidate], run_attempts: list[Attempt],
    lease_owner: str, lease_seconds: int,
) -> tuple[tuple[Attempt, ...], int]:
    if not candidates:
        return (), 0
    by_hospital = {candidate.hospital_id: candidate for candidate in candidates}
    rows = await admit_attempts(
        candidates, lease_owner=lease_owner, lease_seconds=lease_seconds
    )
    attempts = tuple(
        Attempt(
            str(attempt_id), str(hospital_id), by_hospital[str(hospital_id)].hospital_name,
            by_hospital[str(hospital_id)].source_url, int(expected_generation),
            by_hospital[str(hospital_id)].locator_name,
        )
        for hospital_id, attempt_id, expected_generation in rows
    )
    run_attempts.extend(attempts)
    return attempts, len(candidates) - len(attempts)


async def _ensure_content(
    ctx: dict[str, Any], task: dict[str, Any], store: PTG2ArtifactStore, raw: Any,
    max_decompressed_bytes: int,
    max_output_bytes: int,
) -> str:
    version_id = hospital_price_version_id(raw.raw_sha256)
    await raise_if_cancelled(ctx, task)
    if await has_existing_version(version_id, raw.raw_sha256, raw.byte_count):
        return version_id
    source_format = await drain_operation(
        asyncio.to_thread(
            detect_hospital_mrf_format, raw.raw_path, max_decompressed_bytes
        ),
        preserve_cancellation=True,
    )
    with tempfile.TemporaryDirectory(
        prefix=f"hospital-mrf-{raw.raw_sha256[:12]}-", dir=store.tmp_dir
    ) as output:
        receipt = await run_native_parser(
            Path(raw.raw_path), Path(output), version_id, source_format, raw.byte_count,
            max_decompressed_bytes, max_output_bytes,
        )
        await raise_if_cancelled(ctx, task)
        await stage_content(receipt, raw)
    return version_id


async def _publish_download(
    ctx: dict[str, Any], task: dict[str, Any], downloaded_source: DownloadedSource,
    version_id: str,
) -> tuple[int, int, int, int]:
    attempts = downloaded_source.attempts
    raw = downloaded_source.raw
    if raw is None:
        return 0, 0, 0, 0
    try:
        await raise_if_cancelled(ctx, task)
        published, superseded, unchanged = await publish_existing(
            version_id, raw.raw_sha256, attempts
        )
        return published, superseded, unchanged, 0
    except (ImportCancelledError, asyncio.CancelledError):
        await _fail_attempts(attempts, "cancelled", "hospital import cancelled")
        raise
    except Exception as exc:
        code, detail = error_details(exc)
        return 0, 0, 0, await _fail_attempts(attempts, code, detail)


async def _resolve_attempts(
    locator_results: Sequence[Any], run_attempts: list[Attempt],
    lease_owner: str, lease_seconds: int,
) -> tuple[dict[str, list[Attempt]], int, int]:
    candidates = candidates_from_locators(locator_results)
    attempts, active = await _start_attempts(
        candidates, run_attempts, lease_owner, lease_seconds
    )
    by_hospital = {candidate.hospital_id: candidate for candidate in candidates}
    failed_attempts_by_error: dict[tuple[str, str], list[Attempt]] = {}
    by_url: dict[str, list[Attempt]] = {}
    for attempt in attempts:
        candidate = by_hospital[attempt.hospital_id]
        if candidate.initial_error_code:
            key = (candidate.initial_error_code,
                   candidate.initial_error_detail or candidate.initial_error_code)
            failed_attempts_by_error.setdefault(key, []).append(attempt)
        else:
            by_url.setdefault(attempt.source_url, []).append(attempt)
    failed = 0
    for (code, detail), failed_attempts in failed_attempts_by_error.items():
        failed += await _fail_attempts(failed_attempts, code, detail)
    return by_url, active, failed


async def _download_worker(
    source_jobs: asyncio.Queue[Any], downloads: asyncio.Queue[Any],
    store: PTG2ArtifactStore, owner: str, max_raw_bytes: int,
    required_free_bytes: int,
) -> None:
    while True:
        source_job = await source_jobs.get()
        if source_job is None:
            return
        with artifact_lease_context(owner=owner, store=store) as lease:
            acknowledgement = asyncio.get_running_loop().create_future()

            async def _download_and_wait(
                source_job: Any = source_job,
                acknowledgement: asyncio.Future[None] = acknowledgement,
            ) -> None:
                async def _wait_for_acknowledgement() -> None:
                    await acknowledgement

                _require_disk_capacity(store, required_free_bytes)
                downloaded = await download_source(source_job, store, max_raw_bytes)
                await downloads.put((downloaded, acknowledgement))
                await drain_operation(
                    _wait_for_acknowledgement(), preserve_cancellation=True
                )

            await guard_artifact_lease(lease, _download_and_wait())


async def _content_ingest_error(
    ctx: dict[str, Any], task: dict[str, Any], store: PTG2ArtifactStore, raw: Any,
    lock_by_digest: dict[str, asyncio.Lock],
    ingest_error_by_digest: dict[str, tuple[str | None, str | None]],
    max_decompressed_bytes: int,
    max_output_bytes: int,
) -> tuple[str | None, str | None]:
    digest = raw.raw_sha256
    async with lock_by_digest.setdefault(digest, asyncio.Lock()):
        if digest not in ingest_error_by_digest:
            try:
                await _ensure_content(
                    ctx, task, store, raw,
                    max_decompressed_bytes, max_output_bytes,
                )
                ingest_error_by_digest[digest] = (None, None)
            except (ImportCancelledError, asyncio.CancelledError):
                raise
            except Exception as exc:
                ingest_error_by_digest[digest] = error_details(exc)
        return ingest_error_by_digest[digest]


async def _load_worker(
    ctx: dict[str, Any], task: dict[str, Any], store: PTG2ArtifactStore,
    downloads: asyncio.Queue[Any],
    content_pipeline: tuple[
        dict[str, asyncio.Lock], dict[str, tuple[str | None, str | None]], dict[str, int]
    ],
    progress_context: tuple[str | None, int, int],
    max_decompressed_bytes: int,
    max_output_bytes: int,
) -> None:
    lock_by_digest, ingest_error_by_digest, metrics_by_name = content_pipeline
    run_id, progress_base, total = progress_context
    while True:
        queued_download = await downloads.get()
        if queued_download is None:
            return
        downloaded_source, acknowledgement = queued_download
        try:
            if downloaded_source.raw is None:
                if not acknowledgement.done():
                    acknowledgement.set_result(None)
                metrics_by_name["failed"] += await _fail_attempts(
                    downloaded_source.attempts,
                    downloaded_source.error_code or "download_failed",
                    downloaded_source.error_detail,
                )
            else:
                digest = downloaded_source.raw.raw_sha256
                error_code, error_detail = await _content_ingest_error(
                    ctx, task, store, downloaded_source.raw,
                    lock_by_digest, ingest_error_by_digest,
                    max_decompressed_bytes, max_output_bytes,
                )
                if not acknowledgement.done():
                    acknowledgement.set_result(None)
                if error_code:
                    outcome = (
                        0, 0, 0, await _fail_attempts(
                            downloaded_source.attempts, error_code, error_detail
                        ),
                    )
                else:
                    outcome = await _publish_download(
                        ctx, task, downloaded_source,
                        hospital_price_version_id(digest),
                    )
                metrics_by_name["published"] += outcome[0]
                metrics_by_name["superseded"] += outcome[1]
                metrics_by_name["unchanged"] += outcome[2]
                metrics_by_name["failed"] += outcome[3]
            metrics_by_name["processed"] += len(downloaded_source.attempts)
            _progress(
                run_id, "load", progress_base + metrics_by_name["processed"], total,
                "validating and publishing hospital prices",
            )
        finally:
            if not acknowledgement.done():
                acknowledgement.set_result(None)


async def _close_downloads(
    download_tasks: Sequence[asyncio.Task[Any]],
    downloads: asyncio.Queue[Any], load_count: int,
) -> None:
    await asyncio.gather(*download_tasks)
    for _unused in range(load_count):
        await downloads.put(None)


async def _stream_sources(
    ctx: dict[str, Any], task: dict[str, Any], store: PTG2ArtifactStore,
    attempts_by_url: dict[str, list[Attempt]],
    concurrency: tuple[int, int], progress_context: tuple[str | None, int, int],
    resource_context: tuple[str, int, int, int, int],
) -> dict[str, int]:
    fetches, loads = concurrency
    (
        owner, max_raw_bytes, max_decompressed_bytes,
        max_output_bytes, required_free_bytes,
    ) = resource_context
    source_jobs: asyncio.Queue[Any] = asyncio.Queue()
    for url, attempts in attempts_by_url.items():
        source_jobs.put_nowait((url, tuple(attempts)))
    for _unused in range(fetches):
        source_jobs.put_nowait(None)
    downloads: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(fetches, 2 * loads))
    metrics_by_name = {
        "processed": 0, "published": 0, "superseded": 0,
        "unchanged": 0, "failed": 0,
    }
    content_pipeline = ({}, {}, metrics_by_name)
    download_tasks = [
        asyncio.create_task(_download_worker(
            source_jobs, downloads, store, owner, max_raw_bytes, required_free_bytes
        ))
        for _unused in range(fetches)
    ]
    load_tasks = [
        asyncio.create_task(_load_worker(
            ctx, task, store, downloads,
            content_pipeline, progress_context,
            max_decompressed_bytes, max_output_bytes,
        ))
        for _unused in range(loads)
    ]
    closer = asyncio.create_task(_close_downloads(download_tasks, downloads, loads))

    async def _join_workers() -> None:
        await asyncio.gather(closer, *load_tasks)

    join_task = asyncio.create_task(_join_workers())
    try:
        await asyncio.shield(join_task)
    except BaseException:
        await _cancel_and_drain(
            (*download_tasks, *load_tasks, closer),
            wait_for=(join_task,),
            preserve_cancellation=False,
        )
        raise
    return {**metrics_by_name, "contents": len(content_pipeline[1])}


async def _run_import(
    ctx: dict[str, Any], task: dict[str, Any], hospitals: Sequence[dict[str, str]], store: PTG2ArtifactStore,
    run_attempts: list[Attempt], lease_owner: str, lease_seconds: int,
) -> dict[str, int]:
    run_id = str(task.get("run_id") or "").strip() or None
    total = len(hospitals)
    locator_groups = _locator_groups(hospitals)
    requested_fetches = positive_env(
        "HLTHPRT_HOSPITAL_PRICE_FETCH_CONCURRENCY", DEFAULT_FETCH_CONCURRENCY
    )
    (
        fetches, loads, max_raw, max_decompressed, max_output, required_free,
    ) = _resource_limits(
        store,
        requested_fetches,
        positive_env(
            "HLTHPRT_HOSPITAL_PRICE_LOAD_CONCURRENCY", DEFAULT_LOAD_CONCURRENCY
        ),
        len(locator_groups),
    )
    _progress(run_id, "registry", 0, total, "registering hospital identities")
    await sync_registry(hospitals)
    await raise_if_cancelled(ctx, task)
    _progress(run_id, "locators", 0, total, "checking hospital MRF locators")
    with artifact_lease_context(owner=lease_owner, store=store) as locator_lease:
        locator_results = await guard_artifact_lease(
            locator_lease,
            _bounded(locator_groups, fetches, lambda item: fetch_locator(item, store)),
        )
    attempts_by_url, active, failed = await _resolve_attempts(
        locator_results, run_attempts, lease_owner, lease_seconds
    )
    await raise_if_cancelled(ctx, task)
    _progress(run_id, "sources", failed, total, "downloading hospital price files")
    pipeline_metrics_by_name = await _stream_sources(
        ctx, task, store, attempts_by_url, (fetches, loads),
        (run_id, failed + active, total),
        (lease_owner, max_raw, max_decompressed, max_output, required_free),
    )
    failed += pipeline_metrics_by_name["failed"]
    metrics_by_name = {
        "selected": total, "locators": len(locator_groups),
        "mrf_urls": len(attempts_by_url),
        "contents": pipeline_metrics_by_name["contents"],
        "published": pipeline_metrics_by_name["published"],
        "unchanged": pipeline_metrics_by_name["unchanged"],
        "superseded": pipeline_metrics_by_name["superseded"],
        "failed": failed, "active": active,
    }
    _progress(
        run_id, "complete", sum(metrics_by_name[name] for name in (
            "published", "unchanged", "superseded", "failed", "active"
        )), total, "hospital price refresh completed")
    if (metrics_by_name["published"] + metrics_by_name["unchanged"] != total
            or metrics_by_name["failed"] or metrics_by_name["active"]
            or metrics_by_name["superseded"]):
        ctx.setdefault("context", {})["hospital_price_metrics"] = metrics_by_name
        raise RuntimeError("hospital price refresh did not complete every selected hospital")
    return metrics_by_name


async def _finish_failed_attempts(
    attempts: Sequence[Attempt], error_code: str, detail: str
) -> None:
    cleanup = asyncio.create_task(_fail_attempts(attempts, error_code, detail))
    try:
        await asyncio.shield(cleanup)
    except asyncio.CancelledError:
        await cleanup


async def process_data(
    ctx: dict[str, Any], task: dict[str, Any] | None = None
) -> dict[str, int]:
    """Refresh one registry hospital or every registered hospital."""

    return await refresh_hospital_prices(ctx, task)


async def refresh_hospital_prices(
    ctx: dict[str, Any], task: dict[str, Any] | None = None
) -> dict[str, int]:
    """Run the guarded hospital-price refresh lifecycle."""

    task_by_name = dict(task or {})
    hospitals = await asyncio.to_thread(
        selected_hospital_hpt_registry, task_by_name, runtime=True
    )
    await ensure_database(False)
    store, attempts = PTG2ArtifactStore(), []
    owner = f"hospital-prices:{task_by_name.get('run_id') or uuid.uuid4().hex}"
    if len(owner) > 128:
        raise ValueError("hospital price run owner is invalid")
    lease_seconds = max(
        positive_env(
            "HLTHPRT_HOSPITAL_PRICE_ATTEMPT_LEASE_SECONDS",
            DEFAULT_ATTEMPT_LEASE_SECONDS,
        ),
        2,
    )
    heartbeat_seconds = min(
        positive_env(
            "HLTHPRT_HOSPITAL_PRICE_ATTEMPT_HEARTBEAT_SECONDS",
            DEFAULT_ATTEMPT_HEARTBEAT_SECONDS,
        ),
        max(lease_seconds // 3, 1),
    )
    try:
        async def _locked_import() -> dict[str, int]:
            async with _hospital_resource_lock(store):
                return await _run_import(
                    ctx, task_by_name, hospitals, store, attempts,
                    owner, lease_seconds,
                )

        return await _guard_cancellation(
            ctx, task_by_name, _locked_import(),
            attempts, owner, lease_seconds, heartbeat_seconds,
        )
    except (ImportCancelledError, asyncio.CancelledError):
        await _finish_failed_attempts(attempts, "cancelled", "hospital import cancelled")
        raise
    except Exception as exc:
        code, detail = error_details(exc)
        await _finish_failed_attempts(attempts, code, detail)
        raise


async def main(
    *,
    hospital_id: str | None = None,
    hospital_ids: Sequence[str] | None = None,
    all_hospitals: bool = False,
) -> dict[str, int]:
    """Run a direct selected-hospital or all-hospital refresh."""

    return await process_data(
        {},
        {
            "hospital_id": hospital_id,
            "hospital_ids": list(hospital_ids) if hospital_ids is not None else None,
            "all_hospitals": all_hospitals,
        },
    )
