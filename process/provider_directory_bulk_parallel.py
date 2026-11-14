# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound independent bulk transfers without sharing physical Source sessions."""

import asyncio
from contextlib import asynccontextmanager, nullcontext, suppress
from dataclasses import dataclass, replace
from functools import partial
from typing import Any

from process import provider_directory_bulk_spool as spool
from process.provider_directory_fhir_response import AdaptiveRequestSlots
from process.provider_directory_generic_source_policy import source_copy_executor

_TRANSIENT_STREAM_ERRORS = frozenset(
    "bulk_export_transport_" + name
    for name in (
        "clientpayloaderror",
        "clientconnectionerror",
        "clientconnectionreseterror",
        "clientconnectorerror",
        "clientoserror",
        "serverconnectionerror",
        "serverdisconnectederror",
        "servertimeouterror",
        "connectiontimeouterror",
        "sockettimeouterror",
        "timeouterror",
        "connectionreseterror",
        "connectionabortederror",
    )
)


def is_transient_output_error(directory, error):
    """Let healthy transfers finish after retryable output failures."""
    return (
        error in _TRANSIENT_STREAM_ERRORS
        or directory._is_bulk_export_capability_refresh_error(error)
        or error
        in {
            "bulk_export_output_http_429",
            "bulk_export_output_http_500",
            "bulk_export_output_http_502",
            "bulk_export_output_http_503",
            "bulk_export_output_http_504",
        }
    )


def preferred_output_error(directory, current_error, candidate_error):
    """Keep fatal failures first, then capability renewal, then ordinary retries."""
    if not candidate_error:
        return current_error
    if not current_error:
        return candidate_error
    if not is_transient_output_error(directory, current_error):
        return current_error
    if directory._is_bulk_export_capability_refresh_error(candidate_error) or not is_transient_output_error(
        directory, candidate_error
    ):
        return candidate_error
    return current_error


async def stream_output(directory, session, source_record, identity, manifest, output_pair, options, stream_state):
    """Retry a broken stream from its committed boundary under the original guard."""
    output, checkpoint = output_pair
    is_spooled = options.range_resume_enabled and spool.is_enabled(source_record)
    if is_spooled and (
        spool_error := await _spooled_export(directory, session, source_record, identity, manifest, options)
    ):
        return spool_error
    attempts = directory._source_fetch_retry_attempts()
    for attempt in range(attempts):
        error = await directory._stream_one_checkpointed_bulk_output(
            session, source_record, identity, manifest, output, checkpoint, options, stream_state
        )
        if is_spooled and error is None:
            validator = directory._stored_bulk_output_validator(checkpoint)
            spool.spooled_output(identity.checkpoint_id, output.output_index, validator).path.unlink(missing_ok=True)
        if not options.range_resume_enabled or error not in _TRANSIENT_STREAM_ERRORS or attempt + 1 >= attempts:
            return error
        validator = directory._stored_bulk_output_validator(checkpoint)
        if validator is None:
            return error
        await options.ownership_probe()
        await asyncio.sleep(directory._source_fetch_retry_delay_seconds(attempt))
        await options.ownership_probe()
        checkpoints, checkpoint_error = await directory._validated_bulk_output_checkpoints(
            identity, manifest, options.ownership_probe
        )
        if checkpoint_error:
            return checkpoint_error
        resumed = directory._bulk_output_checkpoint_for_manifest_output(identity.checkpoint_id, output, checkpoints)
        if directory._stored_bulk_output_validator(resumed) != validator:
            return "bulk_export_output_validator_mismatch"
        if resumed["committed_bytes"] < checkpoint["committed_bytes"]:
            return "bulk_export_output_validator_checkpoint_corrupt"
        checkpoint = resumed
        directory._bulk_export_log(
            "output_retry",
            resource=identity.resource_type,
            output_index=output.output_index,
            attempt=attempt + 2,
            committed_bytes=checkpoint["committed_bytes"],
            error=error,
        )


def holds_copy(source_record, checkpoint_id, output_checkpoint, validator):
    """Whether this validated output is already saved, so its link is no longer needed."""
    return (
        validator is not None
        and spool.is_enabled(source_record)
        and spool.spooled_output(checkpoint_id, output_checkpoint["output_index"], validator).path.exists()
    )


# A slow transfer still observes cancellation and the deadline while it stays below one frame.
_COPY_PROBE_SECONDS = 1.0


@dataclass(frozen=True)
class _CopyContext:
    directory: Any
    session: Any
    source_record: dict
    identity: Any
    manifest: Any
    options: Any


async def _spooled_export(directory, session, source_record, identity, manifest, options):
    """Save every incomplete output of one export once, before any of them is parsed."""
    tasks_by_checkpoint = state(directory, source_record).setdefault("spool_exports", {})
    task = tasks_by_checkpoint.get(identity.checkpoint_id)
    if task is None:
        context = _CopyContext(directory, session, source_record, identity, manifest, options)
        task = asyncio.create_task(_spool_export(context))
        tasks_by_checkpoint[identity.checkpoint_id] = task
    try:
        # Every stream of the export waits on one shared task; a cancelled stream stops it only through the drain.
        error = await asyncio.shield(task)
    except asyncio.CancelledError:
        await _cancel_and_drain([task])
        raise
    if error and tasks_by_checkpoint.get(identity.checkpoint_id) is task:
        del tasks_by_checkpoint[identity.checkpoint_id]  # a later attempt downloads what is still missing
    return error


async def _spool_export(context):
    directory, checkpoint_id = context.directory, context.identity.checkpoint_id
    checkpoints = await directory._load_bulk_output_checkpoints(checkpoint_id)
    spooled_outputs = []
    for output in context.manifest.outputs:
        checkpoint = directory._bulk_output_checkpoint_for_manifest_output(checkpoint_id, output, checkpoints)
        # Completion clears the ETag capability and keeps only its proof, so a completed output has no validator.
        if directory._is_complete_bulk_output_proof(checkpoint):
            continue
        validator = directory._stored_bulk_output_validator(checkpoint)
        if validator is None:
            return "bulk_export_spool_validator_missing"
        spooled_outputs.append((output, spool.spooled_output(checkpoint_id, output.output_index, validator)))
    # The shared transfer budget bounds how many downloads run at once.
    tasks = [asyncio.create_task(_spool_output(context, pair)) for pair in spooled_outputs]
    if not tasks:
        return None
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    except asyncio.CancelledError:
        await _cancel_and_drain(tasks)
        raise
    if pending:  # a download raised: stop the others before the failure propagates
        await _cancel_and_drain(pending)
    for task in done:
        if not task.cancelled() and task.exception() is not None:
            raise task.exception()
    output_error = None
    for task in tasks:
        output_error = preferred_output_error(directory, output_error, task.result())
    return output_error


async def _cancel_and_drain(tasks):
    """Cancel each download once and wait until every one has closed its copy, even if the waiter is cancelled again.

    Checkpoint ownership is released only after this returns, so no copy is still being written when a retry starts.
    """
    for task in tasks:
        task.cancel()
    while not all(task.done() for task in tasks):
        with suppress(asyncio.CancelledError):
            await asyncio.wait(tasks)


async def _spool_output(context, spooled_pair):
    directory, spooled = context.directory, spooled_pair[1]
    attempts = directory._source_fetch_retry_attempts()
    for attempt in range(attempts):
        error = None if spooled.path.exists() else await _timed_download(context, spooled_pair)
        if error is None:
            fetch_url = _output_request(context, spooled_pair, 0)[1].fetch_url
            state(directory, context.source_record).setdefault("spool", {})[fetch_url] = spooled
            return None
        if not is_transient_output_error(directory, error) or attempt + 1 >= attempts:
            return error
        await asyncio.sleep(directory._source_fetch_retry_delay_seconds(attempt))
    return "bulk_export_spool_incomplete"


async def _timed_download(context, spooled_pair):
    """Download one copy and log its size and duration, the download rate the link window depends on."""
    directory, (output, spooled) = context.directory, spooled_pair
    started = directory.time.monotonic()
    error = await _download_copy(context, spooled_pair)
    if error is None:
        directory._bulk_export_log(
            "spool_output",
            source_id=context.source_record.get("source_id"),
            output_index=output.output_index,
            raw_bytes=spooled.content_length,
            stored_bytes=spooled.path.stat().st_size,
            seconds=round(directory.time.monotonic() - started, 1),
        )
    return error


def _output_request(context, spooled_pair, offset):
    output, spooled = spooled_pair
    resume = context.directory.BulkOutputResumeOptions(None, offset, spooled.etag, spooled.content_length)
    output_request = context.directory._bulk_output_stream_request(
        context.source_record,
        output.url,
        requires_access_token=context.manifest.requires_access_token,
        resume_options=resume,
    )
    return resume, output_request


async def _download_copy(context, spooled_pair):
    """Download one output into its copy through the same request budget and response checks as a stream."""
    directory = context.directory
    writer = await asyncio.to_thread(spool.SpoolWriter, spooled_pair[1])
    try:
        error = await _receive_copy(context, spooled_pair, writer)
        if error is None:
            await asyncio.to_thread(writer.finish)
        return error
    except asyncio.CancelledError, directory.ImportCancelledError, directory.ResourceImportControlPlaneFailure:
        raise
    except spool.SpoolLengthError as exc:
        await asyncio.to_thread(writer.discard)
        return str(exc)
    except Exception as exc:
        if (error := directory._bulk_stream_error(exc)) is None:
            raise
        return error
    finally:
        await asyncio.to_thread(writer.close)


async def _receive_copy(context, spooled_pair, writer):
    """Append the bytes past the copy's last durable frame, probing on a time cadence and renewing per frame."""
    if writer.durable_bytes >= spooled_pair[1].content_length:
        return None
    directory, options = context.directory, context.options
    resume, output_request = _output_request(context, spooled_pair, writer.durable_bytes)
    timeout = directory.aiohttp.ClientTimeout(total=None, sock_connect=options.timeout, sock_read=options.timeout)
    async with request(
        directory,
        context.source_record,
        context.session,
        output_request.fetch_url,
        headers=output_request.headers_by_name,
        timeout=timeout,
        allow_redirects=False,
        ssl=directory._ssl_context(),
    ) as response:
        if error := directory._bulk_response_error_for_request(response, resume):
            return error
        probed_at, renewed_bytes = directory.time.monotonic(), writer.durable_bytes
        async for chunk in response.content.iter_chunked(spool.READ_BYTES):
            await asyncio.to_thread(writer.write, chunk)
            if directory.time.monotonic() - probed_at >= _COPY_PROBE_SECONDS:
                await _stream_probe(context)
                probed_at = directory.time.monotonic()
            if writer.durable_bytes > renewed_bytes:
                renewed_bytes = writer.durable_bytes
                await _renew_lease(context)
    return None


async def _stream_probe(context):
    """Check cancellation, the deadline and the held lock as the stream path does while consuming bytes."""
    probe = context.options.ownership_probe
    await (probe.stream_probe if isinstance(probe, context.directory._BulkCheckpointProbes) else probe)()


async def _renew_lease(context):
    """No rows are written while copies download, so each durable frame renews the checkpoint lease."""
    async with write_slot(context.directory, context.source_record):
        await context.directory._refresh_bulk_export_rows_written(context.identity)


async def run_outputs(directory, identity, options, stream_state, pending_outputs, stream_output):
    """Let healthy files finish after a transient failure; retain fatal cancellation."""
    active_by_task = {}
    pending = iter(pending_outputs)
    output_error = None
    try:
        directory._fill_active_bulk_tasks(active_by_task, pending, options.output_concurrency, stream_output)
        while active_by_task:
            completed, _ = await asyncio.wait(active_by_task, return_when=asyncio.FIRST_COMPLETED)
            error = directory._completed_bulk_output_error(completed, active_by_task)
            if error:
                output_error = preferred_output_error(directory, output_error, error)
                if not options.range_resume_enabled or not is_transient_output_error(directory, error):
                    await directory._cancel_bulk_output_tasks(active_by_task)
                    break
            directory._fill_active_bulk_tasks(active_by_task, pending, options.output_concurrency, stream_output)
        if output_error:
            await options.ownership_probe()
            await directory._record_bulk_export_checkpoint_error(
                identity, output_error, terminal=directory._is_bulk_export_error_terminal(output_error)
            )
        return stream_state, output_error
    finally:
        if active_by_task:
            await directory._cancel_bulk_output_tasks(active_by_task)


def output_concurrency(directory, source_record):
    """Return only the existing validator-bound output concurrency setting."""
    if not directory._is_bulk_range_resume_enabled(source_record):
        return directory.BULK_EXPORT_DEFAULT_OUTPUT_CONCURRENCY
    configured = directory._source_metadata(source_record).get(
        "provider_directory_bulk_export_output_concurrency", directory.BULK_EXPORT_DEFAULT_OUTPUT_CONCURRENCY
    )
    if isinstance(configured, bool):
        raise ValueError("bulk_export_output_concurrency_invalid")
    try:
        concurrency = int(configured)
    except (TypeError, ValueError) as exc:
        raise ValueError("bulk_export_output_concurrency_invalid") from exc
    if not 1 <= concurrency <= directory.BULK_EXPORT_MAX_OUTPUT_CONCURRENCY:
        raise ValueError("bulk_export_output_concurrency_invalid")
    return concurrency


def state(directory, source_record):
    """Retain run-local transfer and writer limits across shallow resource copies."""
    if "_provider_directory_bulk_parallel" not in source_record:
        source_record["_provider_directory_bulk_parallel"] = {
            "requests": AdaptiveRequestSlots(output_concurrency(directory, source_record)),
            "writer": asyncio.Lock(),
        }
    return source_record["_provider_directory_bulk_parallel"]


def prepare_scan(directory, source_record, concurrency):
    """Create run-local limits before the resource scheduler copies its input."""
    shared = state(directory, source_record)
    if "resources" not in shared:
        shared["requests"].limit = max(concurrency, shared["requests"].limit)
        shared["resources"] = AdaptiveRequestSlots(concurrency)


def resource_slots(source_record, concurrency):
    """Reuse the adaptive export allowance for a bulk resource wave."""
    shared = source_record.get("_provider_directory_bulk_parallel", {})
    return shared.get("resources") or asyncio.Semaphore(concurrency)


def write_slot(directory, source_record):
    """Reserve Source capacity before opening an atomic output batch."""
    # One atomic payload/checkpoint batch leaves the second Source session
    # available for ownership observations. Never retain a session over HTTP.
    return state(directory, source_record)["writer"] if directory.is_source_storage_required() else nullcontext()


def _throttle(directory, source_record, shared, response):
    headers_by_name = {str(key).lower(): str(value) for key, value in response.headers.items()}
    delay = directory._source_retry_after_seconds(
        {directory.SOURCE_RETRY_AFTER_FIELD: headers_by_name.get("retry-after")}
    )
    pacing = source_record.setdefault("_provider_directory_request_pacing_state", {})
    now = directory.time.monotonic()
    deadline = pacing.get("retry_after_monotonic", 0)
    if deadline <= now:
        shared["requests"].reduce()
        if "resources" in shared:
            shared["resources"].reduce()
    pacing["retry_after_monotonic"] = max(deadline, now + (delay if delay is not None else 5))


@asynccontextmanager
async def _paced_response(directory, source_record, session, url, shared, options):
    """Hold the transfer slot through response consumption and cancellation."""
    while True:
        async with shared["requests"] as slots:
            await directory._pace_source_request(source_record)
            if slots.active > slots.limit:
                continue
            try:
                async with session.get(url, **options) as response:
                    yield response
                    return
            except TimeoutError:
                slots.reduce()
                raise


@asynccontextmanager
async def request(directory, source_record, session, url, *, export_start=False, **options):
    """Share one adaptive transfer budget; retry only rejected, unread responses."""
    shared = state(directory, source_record)
    spooled = shared.get("spool", {}).get(url)
    offset = spool.served_offset(spooled, options.get("headers") or {}) if spooled is not None else None
    if offset is not None:
        yield spool.SpooledResponse(spooled, offset)
        return
    attempts = 0
    while True:
        async with _paced_response(directory, source_record, session, url, shared, options) as response:
            is_limited = response.status in {429, 503, 504}
            if is_limited:
                _throttle(directory, source_record, shared, response)
            # An export start can have an unknown acceptance outcome.
            # Retry only an explicit rate-limit rejection for that GET.
            is_retryable = is_limited and (not export_start or response.status == 429)
            if is_retryable and attempts < 2:
                attempts += 1
                continue
            yield response
            return


def is_bulk_scan(directory, source_record, options):
    """Apply the existing effective bulk selection to this resource scan."""
    return directory._is_source_bulk_export_effective(
        source_record,
        options.bulk_export,
        per_resource_limit=options.per_resource_limit,
        checkpoint_context=source_record.get("_pagination_checkpoint_context"),
    )


def has_parallel_transport(directory, source_record, resource_types):
    """Require independently addressed exports with immutable output resume."""
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    return bool(
        base
        and directory._is_bulk_range_resume_enabled(source_record)
        and resource_types
        and all(
            (url := directory._bulk_export_start_url(source_record, resource))
            and directory._is_safe_bulk_https_url(url)
            and directory._is_same_bulk_url_origin(url, base)
            for resource in resource_types
        )
    )


def effective_resource_concurrency(directory, source_record, options):
    """Keep the existing durable-scan contract and reserve ordinary guard capacity."""
    requested = min(
        directory._validated_resource_scan_concurrency(options.requested_concurrency),
        directory._source_resource_scan_concurrency(source_record),
    )
    is_bulk = is_bulk_scan(directory, source_record, options)
    if is_bulk:
        # Each export holds one ordinary advisory-lock backend until it drains.
        requested = min(requested, max(1, directory._provider_directory_database_pool_capacity() - 1))
    base = directory._canonical_base(source_record.get("canonical_api_base") or source_record.get("api_base"))
    checkpoint = source_record.get("_pagination_checkpoint_context")
    resources = directory.resource_scan_types(directory, source_record, options.resource_types, options)
    is_safe = bool(
        requested > 1
        and directory.current_version_census_contract(source_record) is None
        and directory._has_bound_resource_scan_checkpoint(source_record, checkpoint)
        and checkpoint.canonical_api_base == base
        and len(options.resource_types) == len(set(options.resource_types))
        and options.per_resource_limit <= 0
        and options.page_limit <= 0
        and options.page_count > 0
        and options.stream_batch_size > 0
        and options.source_concurrency == 1
        and options.linked_resource_limit <= 0
        and not options.stale_cleanup
        and options.checkpointing_enabled
        and options.deferred_materialization
        and not directory._is_practitioner_role_reverse_lookup_planned(source_record, list(options.resource_types))
        and (
            directory.is_source_storage_required()
            or directory._provider_directory_database_pool_capacity() >= requested + 1
        )
        and (
            has_parallel_transport(directory, source_record, resources)
            if is_bulk
            else directory._has_parallel_resource_transport(source_record, resources, options.page_count)
        )
    )
    if not is_safe:
        return 1
    try:
        if directory._bound_upsert_session() is not None:
            return 1
    except RuntimeError:
        return 1
    return min(requested, len(resources))


async def refresh_capabilities(directory, session, source_record, identity, checkpoint, fetch_options, runtime_probe):
    """Read the saved export, or request capabilities when no status was retained."""
    try:
        status_url = directory._bulk_checkpoint_status_url(checkpoint) if checkpoint is not None else None
    except (RuntimeError, ValueError) as exc:
        return None, str(exc), 0
    if status_url is not None:
        status_payload, refresh_error, polls = await directory._poll_bulk_capability_refresh_request(
            session,
            source_record,
            identity,
            status_url,
            timeout=fetch_options.timeout,
            max_pending_seconds=fetch_options.bulk_export_max_pending_seconds,
            runtime_probe=runtime_probe,
            checkpoint=pending_refresh_checkpoint(directory, checkpoint),
        )
        if refresh_error is None:
            manifest, manifest_error = directory._validated_bulk_capability_refresh_manifest(
                source_record, identity, status_payload
            )
            return replace(manifest, status_url=status_url) if manifest is not None else None, manifest_error, polls
        return None, refresh_error, polls
    return await directory._request_bulk_capability_refresh_manifest(
        session,
        source_record,
        identity,
        timeout=fetch_options.timeout,
        max_pending_seconds=fetch_options.bulk_export_max_pending_seconds,
        runtime_probe=runtime_probe,
    )


async def renew_capabilities(
    directory, session, source_record, identity, checkpoint, output_checkpoints, fetch_options, runtime_probe
):
    """Allow one fresh export after retained status or output capabilities expire."""
    polls = 0
    for saved_checkpoint in (checkpoint, None):
        try:
            if saved_checkpoint is None or directory._bulk_checkpoint_status_url(saved_checkpoint) is None:
                await directory._begin_bulk_capability_refresh(identity, checkpoint, output_checkpoints)
        except directory.ImportCancelledError, directory.ResourceImportControlPlaneFailure:
            raise
        except (RuntimeError, ValueError) as exc:
            if str(exc) in directory.BULK_EXPORT_FENCING_ERRORS:
                raise
            return await directory._record_bulk_capability_refresh_error(identity, str(exc), polls, terminal=True)
        manifest, refresh_error, request_polls = await directory._request_bulk_capability_refresh_for_options(
            session, source_record, identity, fetch_options, runtime_probe, checkpoint=saved_checkpoint
        )
        polls += request_polls
        if refresh_error is None and manifest is not None:
            try:
                refresh_error = await directory._validate_and_persist_bulk_capabilities(
                    session,
                    source_record,
                    identity,
                    checkpoint,
                    manifest,
                    output_checkpoints,
                    fetch_options,
                    runtime_probe,
                )
            except directory.ImportCancelledError, directory.ResourceImportControlPlaneFailure:
                raise
            except (RuntimeError, ValueError) as exc:
                if str(exc) in directory.BULK_EXPORT_FENCING_ERRORS:
                    raise
                return await directory._record_bulk_capability_refresh_error(identity, str(exc), polls, terminal=True)
        elif refresh_error is None:
            refresh_error = "bulk_export_manifest_unavailable"
        if (
            saved_checkpoint is not None
            and directory._clean_text(saved_checkpoint.get("status_url_ciphertext"))
            and refresh_error
            in {
                "bulk_export_manifest_transaction_time_stale",
                "bulk_export_status_http_404",
                "bulk_export_status_http_410",
                "bulk_export_output_http_404",
                "bulk_export_output_http_410",
                "bulk_export_output_url_expired",
            }
        ):
            continue
        if refresh_error is not None:
            return await directory._record_bulk_capability_refresh_error(identity, refresh_error, polls)
        return manifest, None, polls


def batch_transaction(directory, progress_handler):
    """Open the one owner transaction that commits a bulk batch and its byte boundary.

    It is the only transaction whose verification may reuse a recent storage capture and whose
    checkpoint controls are captured once per interval.
    """
    if not progress_handler:
        return nullcontext()
    return directory.source_checkpoint_transaction(directory.db, schema=directory._schema(), is_batch_loading=True)


async def begin_capability_refresh(
    directory,
    identity,
    checkpoint,
    output_checkpoints,
    *,
    fresh_export=True,
):
    """Fence retained polling and reserve attempts only for a new remote export."""

    async with directory.source_checkpoint_transaction(
        directory.db,
        schema=directory._schema(),
        bulk_identity=identity,
    ):
        refresh_error = directory._clean_text(checkpoint.get("error"))
        if not directory._is_bulk_export_capability_refresh_error(refresh_error):
            raise ValueError("bulk_export_output_capability_refresh_not_required")
        incomplete_outputs = [
            output for output in output_checkpoints if output.get("state") != directory.BULK_EXPORT_OUTPUT_COMPLETE
        ]
        attempt_limit = directory._bulk_capability_refresh_attempt_limit()
        if not incomplete_outputs or (
            fresh_export
            and any(int(output.get("attempt_count") or 0) >= attempt_limit for output in incomplete_outputs)
        ):
            raise ValueError("bulk_export_output_capability_refresh_exhausted")
        async with directory.db.acquire() as connection:
            parent_count = await connection.status(
                f"""
                UPDATE {directory._bulk_acquisition_checkpoint_table_ref()}
                   SET lease_expires_at =
                           now() + make_interval(secs => :lease_seconds),
                       updated_at = now()
                 WHERE checkpoint_id = :checkpoint_id
                   AND owner_run_id = :owner_run_id
                   AND state = :retryable_state
                   AND error = :refresh_error;
                """,
                checkpoint_id=identity.checkpoint_id,
                owner_run_id=identity.owner_run_id,
                retryable_state=directory.BULK_EXPORT_CHECKPOINT_RETRYABLE,
                refresh_error=refresh_error,
                lease_seconds=directory._bulk_checkpoint_lease_seconds(),
            )
            if directory._coerce_rowcount(parent_count) != 1:
                raise RuntimeError("bulk_export_checkpoint_ownership_lost")
            if not fresh_export:
                return
            await reserve_refresh_attempts(directory, connection, identity, attempt_limit, len(incomplete_outputs))


async def reserve_refresh_attempts(directory, connection, identity, attempt_limit, incomplete_count):
    """Reserve the existing incomplete-output counters within the fenced transaction."""
    await directory.require_checkpoint_executor(connection, bulk_identity=identity)
    connection = source_copy_executor(connection)
    output_count = await connection.status(
        f"""
        UPDATE {directory._bulk_output_checkpoint_table_ref()}
           SET attempt_count = attempt_count + 1,
               updated_at = now()
         WHERE checkpoint_id = :checkpoint_id
           AND state <> :complete_state
           AND attempt_count < :attempt_limit;
        """,
        checkpoint_id=identity.checkpoint_id,
        complete_state=directory.BULK_EXPORT_OUTPUT_COMPLETE,
        attempt_limit=attempt_limit,
    )
    if directory._coerce_rowcount(output_count) != incomplete_count:
        raise RuntimeError("bulk_export_output_capability_refresh_exhausted")


async def persist_refresh_status(directory, identity, status_url):
    """Retain an accepted replacement before polling, under its checkpoint lease."""
    async with directory.source_checkpoint_transaction(
        directory.db, schema=directory._schema(), bulk_identity=identity
    ):
        checkpoint_row = await directory.db.first(
            f"""
            UPDATE {directory._bulk_acquisition_checkpoint_table_ref()}
               SET status_url_ciphertext = :status_url_ciphertext,
                   status_url_hash = :status_url_hash,
                   accepted_at = now(), next_poll_at = NULL,
                   lease_expires_at = now() + make_interval(secs => :lease_seconds),
                   updated_at = now()
             WHERE checkpoint_id = :checkpoint_id
               AND owner_run_id = :owner_run_id
               AND state = :retryable_state
               AND error = ANY(:refresh_errors)
         RETURNING accepted_at, manifest_received_at, next_poll_at;
            """,
            checkpoint_id=identity.checkpoint_id,
            owner_run_id=identity.owner_run_id,
            retryable_state=directory.BULK_EXPORT_CHECKPOINT_RETRYABLE,
            refresh_errors=list(directory.BULK_EXPORT_CAPABILITY_REFRESH_ERRORS),
            status_url_ciphertext=directory._encrypt_bulk_capability(status_url),
            status_url_hash=directory.hashlib.sha256(status_url.encode()).hexdigest(),
            lease_seconds=directory._bulk_checkpoint_lease_seconds(),
        )
        checkpoint = directory._pagination_checkpoint_row_mapping(checkpoint_row)
        if not checkpoint:
            raise RuntimeError("bulk_export_checkpoint_ownership_lost")
        return checkpoint


def pending_refresh_checkpoint(directory, checkpoint):
    """Reuse the acceptance epoch only while a replacement manifest is pending."""
    accepted_at = checkpoint.get("accepted_at")
    received_at = checkpoint.get("manifest_received_at")
    if accepted_at is None or received_at is None:
        return None
    accepted_at = directory._bulk_export_utc_datetime(accepted_at, error="bulk_export_checkpoint_accepted_at_invalid")
    received_at = directory._bulk_export_utc_datetime(
        received_at, error="bulk_export_checkpoint_manifest_received_at_invalid"
    )
    return checkpoint if accepted_at > received_at else None


async def poll_capability_refresh(directory, session, source_record, identity, status_location, poll_options, timeout):
    """Poll with the retained replacement deadline and durable pacing boundary."""
    try:
        status_url = directory._resolved_bulk_export_status_url(source_record, identity.start_url, status_location)
    except ValueError as exc:
        return None, str(exc), 0
    options = replace(
        poll_options,
        lease_handler=partial(directory._refresh_bulk_export_rows_written, identity),
        accepted_at=poll_options.accepted_at or directory._bulk_export_now_utc(),
        next_poll_handler=partial(directory._persist_bulk_export_next_poll_at, identity)
        if poll_options.accepted_at is not None
        else None,
    )
    return await directory._bulk_export_poll_manifest(
        session, source_record, status_url, resource_type=identity.resource_type, timeout=timeout, poll_options=options
    )


async def persist_rate_limit(directory, context, status_code, headers, request_error):
    """Keep the final rate-limit delay across retries without extending acceptance."""
    callback = context.options.next_poll_handler
    if status_code == 429 and request_error is None and callback is not None:
        await callback(
            directory._next_bulk_export_poll_at(
                headers.get("retry-after"),
                context.poll_seconds,
                directory._bulk_export_now_utc(),
                context.pending_deadline,
            )
        )


async def poll_response(directory, session, context, timeout):
    """Bound pacing, transport retries and body reads by the accepted deadline."""
    remaining = (
        (context.pending_deadline - directory._bulk_export_now_utc()).total_seconds()
        if context.pending_deadline is not None
        else None
    )
    if remaining is not None and remaining <= 0:
        return None, {}, None, directory.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    pending_timeout = asyncio.timeout(remaining)
    try:
        async with pending_timeout:
            result = await directory._bulk_http_get_json(
                session,
                context.source_record,
                context.status_url,
                timeout=min(timeout, remaining) if remaining is not None else timeout,
            )
    except TimeoutError:
        if not pending_timeout.expired():
            raise
        return None, {}, None, directory.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    if context.pending_deadline is not None and directory._bulk_export_now_utc() >= context.pending_deadline:
        return None, {}, None, directory.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    return result


async def request_capability_refresh(
    directory, session, source_record, identity, timeout, max_pending_seconds, runtime_probe
):
    """Retain accepted replacement locations and validate completed manifests."""
    await runtime_probe()
    (
        status_code,
        status_payload,
        status_location,
        request_error,
    ) = await directory._start_bulk_capability_refresh_request(session, source_record, identity, timeout=timeout)
    if request_error is not None:
        return None, request_error, 0
    if status_code is None:
        return None, "bulk_export_acceptance_outcome_unknown", 0
    if status_code not in {200, 202}:
        return None, f"bulk_export_status_http_{status_code}", 0
    status_url = None
    if status_location is not None or status_code == 202:
        try:
            status_url = directory._resolved_bulk_export_status_url(source_record, identity.start_url, status_location)
        except ValueError as exc:
            return None, str(exc), 0
    polls = 0
    if status_code == 202:
        checkpoint = await persist_refresh_status(directory, identity, status_url)
        status_payload, poll_error, polls = await directory._poll_bulk_capability_refresh_request(
            session,
            source_record,
            identity,
            status_url,
            timeout=timeout,
            max_pending_seconds=max_pending_seconds,
            runtime_probe=runtime_probe,
            checkpoint=checkpoint,
        )
        if poll_error is not None:
            return None, poll_error, polls
    if not directory._is_bulk_export_status_payload(status_payload):
        return None, "bulk_export_status_non_bulk_payload", polls
    payload_error = directory._bulk_export_payload_error(status_payload)
    if payload_error is not None:
        return None, payload_error, polls
    manifest, manifest_error = directory._validated_bulk_capability_refresh_manifest(
        source_record, identity, status_payload
    )
    return replace(manifest, status_url=status_url) if manifest is not None else None, manifest_error, polls
