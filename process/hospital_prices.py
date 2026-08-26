# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded hospital-price refresh worker and command entrypoint."""

from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import uuid
from contextlib import suppress
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence

from process.control_cancel import ImportCancelledError, raise_if_cancelled
from process.ext.utils import ensure_database
from process.formulary_fhir.async_safety import drain_operation
from process.hospital_hpt_registry import selected_hospital_hpt_registry
from process.hospital_price_acquisition import (
    Attempt,
    Candidate,
    canonicalize_url,
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
from process.hospital_price_pipeline import (
    acknowledge_queued_downloads as _acknowledge_queued_downloads,
    download_worker as _run_download_worker,
    load_worker as _run_load_worker,
    ordered_source_jobs as _ordered_source_jobs,
    refreshed_locator_candidates as _run_refreshed_locator_candidates,
    stream_sources as _run_source_pipeline,
)
from process import hospital_price_runtime as _runtime
from process.hospital_price_scratch import (
    HOSPITAL_SOURCE_TMP_PREFIX,
    owned_tmp_root as _owned_tmp_root,
    sweep_transient_source_roots as _sweep_transient_source_roots,
    unlink_transient_source as _unlink_transient_source,
)
from process.hospital_price_store import (
    admit_attempts,
    fail_attempts as _fail_attempts,
    garbage_collect_superseded_versions,
    has_existing_version,
    publish_existing,
    rebind_attempt_sources,
    stage_content,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.input_artifact_retention import (
    artifact_lease_context,
    guard_artifact_lease,
)


HOSPITAL_PRICE_QUEUE_NAME = "arq:HospitalPrices"
DEFAULT_ATTEMPT_LEASE_SECONDS = 300
DEFAULT_ATTEMPT_HEARTBEAT_SECONDS = 60

_bounded = _runtime.bounded
_cancel_and_drain = _runtime.cancel_and_drain
_guard_cancellation = _runtime.guard_cancellation
_hospital_resource_lock = _runtime.hospital_resource_lock
_locator_groups = _runtime.locator_groups
_progress = _runtime.progress
_require_disk_capacity = _runtime.require_disk_capacity
_resource_limits = _runtime.configured_resource_limits


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
            locator_url=by_hospital[str(hospital_id)].locator_url,
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
    try:
        raw_path = Path(raw.raw_path).resolve(strict=True)
        scratch_root = Path(store.tmp_dir).resolve(strict=True)
        relative_raw = raw_path.relative_to(scratch_root)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "hospital source artifact is outside hospital source scratch"
        ) from exc
    if (
        len(relative_raw.parts) < 3
        or not relative_raw.parts[0].startswith(HOSPITAL_SOURCE_TMP_PREFIX)
        or relative_raw.parts[1] != "raw"
    ):
        raise RuntimeError(
            "hospital source artifact is outside task-owned raw scratch"
        )
    source_root = scratch_root / relative_raw.parts[0]
    with tempfile.TemporaryDirectory(prefix="projection-", dir=source_root) as output:
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
    by_canonical_url: dict[str, tuple[str, list[Attempt]]] = {}
    for attempt in attempts:
        candidate = by_hospital[attempt.hospital_id]
        if candidate.initial_error_code:
            key = (candidate.initial_error_code,
                   candidate.initial_error_detail or candidate.initial_error_code)
            failed_attempts_by_error.setdefault(key, []).append(attempt)
        else:
            canonical_url = canonicalize_url(attempt.source_url)
            source_url, grouped_attempts = by_canonical_url.setdefault(
                canonical_url, (attempt.source_url, [])
            )
            grouped_attempts.append(attempt)
    failed = 0
    for (code, detail), failed_attempts in failed_attempts_by_error.items():
        failed += await _fail_attempts(failed_attempts, code, detail)
    attempts_by_source_url = dict(by_canonical_url.values())
    return attempts_by_source_url, active, failed


async def _refreshed_source_job(
    source_job: tuple[str, tuple[Attempt, ...]], store: PTG2ArtifactStore,
) -> tuple[str, tuple[Attempt, ...]] | None:
    """Refetch every locator before sharing one refreshed source download."""

    url, attempts = source_job
    canonical_url = canonicalize_url(url)
    prior_urls = {url, *(attempt.source_url for attempt in attempts)}
    refreshed_candidate_by_hospital: dict[str, Candidate] = {}
    for locator_url in dict.fromkeys(
        attempt.locator_url for attempt in attempts if attempt.locator_url
    ):
        locator_attempts = tuple(
            attempt for attempt in attempts if attempt.locator_url == locator_url
        )
        candidates = await _run_refreshed_locator_candidates(
            locator_url,
            locator_attempts,
            store,
            canonical_url,
            prior_urls,
            SimpleNamespace(
                canonicalize_url=canonicalize_url,
                fetch_locator=fetch_locator,
                candidates_from_locators=candidates_from_locators,
            ),
        )
        if candidates is None:
            return None
        refreshed_candidate_by_hospital.update(candidates)
    if set(refreshed_candidate_by_hospital) != {
        attempt.hospital_id for attempt in attempts
    }:
        return None
    bindings = tuple(
        (attempt, refreshed_candidate_by_hospital[attempt.hospital_id])
        for attempt in attempts
    )
    await rebind_attempt_sources(bindings)
    for attempt, candidate in bindings:
        attempt.source_url = candidate.source_url
        attempt.locator_name = candidate.locator_name
        attempt.locator_url = candidate.locator_url
    return bindings[0][1].source_url, attempts


async def _download_worker(
    source_jobs: asyncio.Queue[Any], downloads: asyncio.Queue[Any],
    store: PTG2ArtifactStore, owner: str, max_raw_bytes: int,
    required_free_bytes: int,
) -> None:
    """Download private sources while their artifact leases remain held."""

    await _run_download_worker(
        source_jobs, downloads, store, owner, max_raw_bytes, required_free_bytes,
        SimpleNamespace(
            source_prefix=HOSPITAL_SOURCE_TMP_PREFIX,
            store_factory=PTG2ArtifactStore,
            artifact_lease_context=artifact_lease_context,
            guard_artifact_lease=guard_artifact_lease,
            require_disk_capacity=_require_disk_capacity,
            download_source=download_source,
            refreshed_source_job=_refreshed_source_job,
        ),
    )


async def _fetch_transient_locator(
    item: Any, store: PTG2ArtifactStore, owner: str
) -> Any:
    """Fetch one locator without retaining its downloaded text artifacts."""

    with tempfile.TemporaryDirectory(
        prefix=HOSPITAL_SOURCE_TMP_PREFIX, dir=store.tmp_dir
    ) as source_root:
        source_store = PTG2ArtifactStore(source_root)
        with artifact_lease_context(owner=owner, store=source_store) as lease:
            return await guard_artifact_lease(
                lease, fetch_locator(item, source_store)
            )


async def _cleanup_transient_source(store: PTG2ArtifactStore, raw: Any) -> None:
    """Drain exact source deletion through repeated task cancellation."""

    await drain_operation(
        asyncio.to_thread(_unlink_transient_source, store, raw),
        preserve_cancellation=True,
        should_prefer_operation_error=True,
    )


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
    """Validate and publish sources while acknowledging private source leases."""

    await _run_load_worker(
        ctx, task, store, downloads, content_pipeline, progress_context,
        (max_decompressed_bytes, max_output_bytes),
        SimpleNamespace(
            fail_attempts=_fail_attempts,
            content_ingest_error=_content_ingest_error,
            cleanup_transient_source=_cleanup_transient_source,
            publish_download=_publish_download,
            version_id=hospital_price_version_id,
            progress=_progress,
        ),
    )


async def _stream_sources(
    ctx: dict[str, Any], task: dict[str, Any], store: PTG2ArtifactStore,
    attempts_by_url: dict[str, list[Attempt]],
    concurrency: tuple[int, int], progress_context: tuple[str | None, int, int],
    resource_context: tuple[str, int, int, int, int],
) -> dict[str, int]:
    return await _run_source_pipeline(
        ctx, task, store, attempts_by_url, concurrency, progress_context,
        resource_context,
        SimpleNamespace(
            download_worker=_download_worker,
            load_worker=_load_worker,
            cancel_and_drain=_cancel_and_drain,
        ),
    )


async def _run_import(
    ctx: dict[str, Any], task: dict[str, Any], hospitals: Sequence[dict[str, str]], store: PTG2ArtifactStore,
    run_attempts: list[Attempt], lease_owner: str, lease_seconds: int,
) -> dict[str, int]:
    await drain_operation(
        asyncio.to_thread(_sweep_transient_source_roots, store),
        preserve_cancellation=True,
        should_prefer_operation_error=True,
    )
    await ensure_database(False)
    run_id = str(task.get("run_id") or "").strip() or None
    total = len(hospitals)
    locator_groups = _locator_groups(hospitals)
    (
        fetches, loads, max_raw, max_decompressed, max_output, required_free,
    ) = _resource_limits(
        store,
        len(locator_groups),
    )
    _progress(run_id, "registry", 0, total, "registering hospital identities")
    await sync_registry(hospitals)
    await raise_if_cancelled(ctx, task)
    _progress(run_id, "locators", 0, total, "checking hospital MRF locators")
    locator_results = await _bounded(
        locator_groups,
        fetches,
        lambda item: _fetch_transient_locator(item, store, lease_owner),
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
    await garbage_collect_superseded_versions()
    if (metrics_by_name["published"] + metrics_by_name["unchanged"] != total
            or metrics_by_name["failed"] or metrics_by_name["active"]
            or metrics_by_name["superseded"]):
        ctx.setdefault("context", {})["hospital_price_metrics"] = metrics_by_name
        raise RuntimeError("hospital price refresh did not complete every selected hospital")
    _progress(
        run_id, "complete", sum(metrics_by_name[name] for name in (
            "published", "unchanged", "superseded", "failed", "active"
        )), total, "hospital price refresh completed")
    return metrics_by_name


async def _finish_failed_attempts(
    attempts: Sequence[Attempt], error_code: str, detail: str
) -> None:
    await drain_operation(
        _fail_attempts(attempts, error_code, detail), preserve_cancellation=False
    )


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

    async def _locked_import() -> dict[str, int]:
        async with _hospital_resource_lock(store):
            try:
                return await _run_import(ctx, task_by_name, hospitals, store, attempts, owner, lease_seconds)
            except (asyncio.CancelledError, Exception) as exc:
                failure = (("cancelled", "hospital import cancelled")
                           if isinstance(exc, (ImportCancelledError, asyncio.CancelledError))
                           else error_details(exc))
                with suppress(BaseException):
                    await _finish_failed_attempts(attempts, *failure)
                if attempts:
                    with suppress(BaseException):
                        await drain_operation(garbage_collect_superseded_versions(), preserve_cancellation=False)
                raise

    return await _guard_cancellation(
        ctx, task_by_name, _locked_import(), attempts, owner, lease_seconds, heartbeat_seconds,
    )


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
