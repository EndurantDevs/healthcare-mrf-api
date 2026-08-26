# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Queue coordination for bounded hospital source ingestion."""

from __future__ import annotations

import asyncio
import tempfile
from types import SimpleNamespace
from typing import Any, Sequence

from process.formulary_fhir.async_safety import drain_operation
from process.ptg_parts.canonical import canonicalize_url


def ordered_source_jobs(
    attempts_by_url: dict[str, list[Any]],
) -> list[tuple[str, list[Any]]]:
    """Put expiring canonical variants before stable source URLs."""

    return sorted(
        attempts_by_url.items(),
        key=lambda source_job: canonicalize_url(source_job[0]) == source_job[0],
    )


async def refreshed_locator_candidates(
    locator_url: str,
    locator_attempts: tuple[Any, ...],
    store: Any,
    canonical_url: str,
    prior_urls: set[str],
    operations: SimpleNamespace,
) -> dict[str, Any] | None:
    """Resolve one locator only when every requested hospital remains bound."""

    hospitals = tuple(
        {
            "hospital_id": attempt.hospital_id,
            "name": attempt.hospital_name,
            "locator_name": attempt.locator_name or attempt.hospital_name,
            "cms_hpt_url": locator_url,
        }
        for attempt in locator_attempts
    )
    locator_result = await operations.fetch_locator((locator_url, hospitals), store)
    matching_records = tuple(
        locator_record for locator_record in (locator_result.records or ())
        if locator_record.mrf_url not in prior_urls
        and operations.canonicalize_url(locator_record.mrf_url) == canonical_url
    )
    if not matching_records:
        return None
    filtered_result = type(locator_result)(
        url=locator_result.url,
        locator_id=locator_result.locator_id,
        observation_id=locator_result.observation_id,
        hospitals=locator_result.hospitals,
        records=matching_records,
        error_code=locator_result.error_code,
        error_detail=locator_result.error_detail,
    )
    candidate_by_hospital = {
        candidate.hospital_id: candidate
        for candidate in operations.candidates_from_locators((filtered_result,))
        if candidate.initial_error_code is None
    }
    expected_hospital_ids = {attempt.hospital_id for attempt in locator_attempts}
    return (
        candidate_by_hospital
        if set(candidate_by_hospital) == expected_hospital_ids
        else None
    )


async def download_worker(
    source_jobs: asyncio.Queue[Any],
    downloads: asyncio.Queue[Any],
    store: Any,
    owner: str,
    max_raw_bytes: int,
    required_free_bytes: int,
    operations: SimpleNamespace,
) -> None:
    """Download sources while retaining each private lease through consumption."""

    while True:
        source_job = await source_jobs.get()
        if source_job is None:
            return
        with tempfile.TemporaryDirectory(
            prefix=operations.source_prefix, dir=store.tmp_dir
        ) as source_root:
            source_store = operations.store_factory(source_root)
            with operations.artifact_lease_context(
                owner=owner, store=source_store
            ) as lease:
                acknowledgement = asyncio.get_running_loop().create_future()

                async def _download_and_wait() -> None:
                    async def _wait_for_acknowledgement() -> None:
                        await acknowledgement

                    operations.require_disk_capacity(store, required_free_bytes)
                    downloaded = await operations.download_source(
                        source_job, source_store, max_raw_bytes
                    )
                    if downloaded.raw is None and downloaded.auth_refresh_required:
                        refreshed_job = await operations.refreshed_source_job(
                            source_job, source_store
                        )
                        if refreshed_job is not None:
                            downloaded = await operations.download_source(
                                refreshed_job, source_store, max_raw_bytes,
                                exact_url_only=True,
                            )
                    await downloads.put((downloaded, acknowledgement))
                    await drain_operation(
                        _wait_for_acknowledgement(), preserve_cancellation=True
                    )

                await operations.guard_artifact_lease(lease, _download_and_wait())


def _acknowledge(acknowledgement: asyncio.Future[None]) -> None:
    if not acknowledgement.done():
        acknowledgement.set_result(None)


async def _load_download(
    ctx: dict[str, Any],
    task: dict[str, Any],
    store: Any,
    downloaded_source: Any,
    acknowledgement: asyncio.Future[None],
    content_pipeline: tuple[dict[str, Any], dict[str, Any], dict[str, int]],
    parser_limits: tuple[int, int],
    operations: SimpleNamespace,
) -> tuple[int, int, int, int]:
    if downloaded_source.raw is None:
        _acknowledge(acknowledgement)
        failed = await operations.fail_attempts(
            downloaded_source.attempts,
            downloaded_source.error_code or "download_failed",
            downloaded_source.error_detail,
        )
        return 0, 0, 0, failed
    locks_by_digest, errors_by_digest, _metrics_by_name = content_pipeline
    max_decompressed_bytes, max_output_bytes = parser_limits
    digest = downloaded_source.raw.raw_sha256
    try:
        error_code, error_detail = await operations.content_ingest_error(
            ctx, task, store, downloaded_source.raw,
            locks_by_digest, errors_by_digest,
            max_decompressed_bytes, max_output_bytes,
        )
    finally:
        await operations.cleanup_transient_source(store, downloaded_source.raw)
    _acknowledge(acknowledgement)
    if error_code:
        failed = await operations.fail_attempts(
            downloaded_source.attempts, error_code, error_detail
        )
        return 0, 0, 0, failed
    return await operations.publish_download(
        ctx, task, downloaded_source, operations.version_id(digest)
    )


async def load_worker(
    ctx: dict[str, Any],
    task: dict[str, Any],
    store: Any,
    downloads: asyncio.Queue[Any],
    content_pipeline: tuple[dict[str, Any], dict[str, Any], dict[str, int]],
    progress_context: tuple[str | None, int, int],
    parser_limits: tuple[int, int],
    operations: SimpleNamespace,
) -> None:
    """Validate, publish, and acknowledge queued private source artifacts."""

    metrics_by_name = content_pipeline[2]
    run_id, progress_base, total = progress_context
    while True:
        queued_download = await downloads.get()
        if queued_download is None:
            return
        downloaded_source, acknowledgement = queued_download
        try:
            outcome = await _load_download(
                ctx, task, store, downloaded_source, acknowledgement,
                content_pipeline, parser_limits, operations,
            )
            for metric_name, increment in zip(
                ("published", "superseded", "unchanged", "failed"), outcome
            ):
                metrics_by_name[metric_name] += increment
            metrics_by_name["processed"] += len(downloaded_source.attempts)
            operations.progress(
                run_id, "load", progress_base + metrics_by_name["processed"], total,
                "validating and publishing hospital prices",
            )
        finally:
            _acknowledge(acknowledgement)


async def close_downloads(
    download_tasks: Sequence[asyncio.Task[Any]],
    downloads: asyncio.Queue[Any],
    load_count: int,
) -> None:
    """Close loaders only after every source producer has terminated."""

    await asyncio.gather(*download_tasks)
    for _unused in range(load_count):
        await downloads.put(None)


def acknowledge_queued_downloads(downloads: asyncio.Queue[Any]) -> None:
    """Release producers whose downloads were never dequeued by a loader."""

    while True:
        try:
            queued_download = downloads.get_nowait()
        except asyncio.QueueEmpty:
            return
        if queued_download is None:
            continue
        _downloaded_source, acknowledgement = queued_download
        _acknowledge(acknowledgement)


def _source_job_queue(
    attempts_by_url: dict[str, list[Any]], fetch_count: int,
) -> asyncio.Queue[Any]:
    source_jobs: asyncio.Queue[Any] = asyncio.Queue()
    for url, attempts in ordered_source_jobs(attempts_by_url):
        source_jobs.put_nowait((url, tuple(attempts)))
    for _unused in range(fetch_count):
        source_jobs.put_nowait(None)
    return source_jobs


async def _join_pipeline(
    closer: asyncio.Task[Any], load_tasks: Sequence[asyncio.Task[Any]],
) -> None:
    await asyncio.gather(closer, *load_tasks)


async def stream_sources(
    ctx: dict[str, Any],
    task: dict[str, Any],
    store: Any,
    attempts_by_url: dict[str, list[Any]],
    concurrency: tuple[int, int],
    progress_context: tuple[str | None, int, int],
    resource_context: tuple[str, int, int, int, int],
    operations: SimpleNamespace,
) -> dict[str, int]:
    """Run the bounded download/load pipeline and drain all owned tasks."""

    fetch_count, load_count = concurrency
    max_raw, max_decompressed, max_output, required_free = resource_context[1:]
    source_jobs = _source_job_queue(attempts_by_url, fetch_count)
    downloads: asyncio.Queue[Any] = asyncio.Queue(
        maxsize=max(fetch_count, 2 * load_count)
    )
    metrics_by_name = {
        "processed": 0, "published": 0, "superseded": 0,
        "unchanged": 0, "failed": 0,
    }
    content_pipeline = ({}, {}, metrics_by_name)
    download_tasks = [
        asyncio.create_task(operations.download_worker(
            source_jobs, downloads, store, resource_context[0], max_raw, required_free
        ))
        for _unused in range(fetch_count)
    ]
    load_tasks = [
        asyncio.create_task(operations.load_worker(
            ctx, task, store, downloads, content_pipeline, progress_context,
            max_decompressed, max_output,
        ))
        for _unused in range(load_count)
    ]
    closer = asyncio.create_task(close_downloads(
        download_tasks, downloads, load_count
    ))
    join_task = asyncio.create_task(_join_pipeline(closer, load_tasks))
    try:
        await asyncio.shield(join_task)
    except BaseException:
        owned_tasks = (*download_tasks, *load_tasks, closer)
        for owned_task in owned_tasks:
            if not owned_task.done() and owned_task.cancelling() == 0:
                owned_task.cancel()
        acknowledge_queued_downloads(downloads)
        await operations.cancel_and_drain(
            owned_tasks, wait_for=(join_task,), preserve_cancellation=False
        )
        raise
    return {**metrics_by_name, "contents": len(content_pipeline[1])}
