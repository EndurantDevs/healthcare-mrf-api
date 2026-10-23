# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare retained originals once; never reacquire a source during resume."""

from __future__ import annotations

import asyncio

from process.custom_import import build_source as staging
from process.custom_import.capture import _iter_verified_records
from process.custom_import.prepared_source_codec import iter_prepared_source_parts
from process.custom_import.prepared_source_context import load_prepared_source_context
from process.custom_import.prepared_source_seal import seal_prepared_source
from process.custom_import.prepared_source_store import (
    append_prepared_part,
    begin_prepared_source,
    mark_prepared_stream_eof,
)
from process.custom_import.raw_capture_store import load_replayable_raw_bundle
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.source_batch import _read_session


async def _next_part(parts):
    """Drain one bounded worker handoff before cancellation closes its decoder."""

    completed_parts = []

    async def complete():
        """Retain the worker result while cancellation drains its pending read."""
        completed_parts.append(await asyncio.to_thread(next, parts, None))

    primary = await staging._drain_cleanup(complete(), None)
    if primary is not None:
        raise primary
    return completed_parts[0]


async def _originals(session_factory, context):
    async with _read_session(session_factory) as session:
        await staging._set_timeout(session, context.statement_timeout_ms)
        captures = await load_replayable_raw_bundle(
            session,
            capture_bundle_id=context.capture_bundle_id,
            dataset_id=context.dataset_id,
            definition_revision_id=context.definition_revision_id,
            schema_revision_id=context.schema_revision_id,
            limits=context.policy.capture_limits,
        )
    captures_by_stream = {capture.manifest.stream_id: capture for capture in captures}
    if set(captures_by_stream) != {stream_id for stream_id, _ in context.stream_slots}:
        raise CandidateRunnerError("prepared_source_original_scope_invalid")
    return captures_by_stream


async def _prepare_stream(session_factory, context, stream, capture, slot):
    """Compare a retained prefix once, then close only after true EOF and cleanup."""

    cleanup_failures = []
    # The retained loader already verified hashes, original byte limits and stream membership.
    decoded = _iter_verified_records(
        capture, stream, limits=context.policy.capture_limits, _cleanup_failures=cleanup_failures
    )
    parts = iter_prepared_source_parts(
        decoded,
        limits=context.policy.capture_limits,
        part_byte_limit=context.policy.prepared["part_byte_limit"],
        part_row_limit=context.policy.prepared["part_row_limit"],
    )
    primary = None
    try:
        ordinal = 0
        while (part := await _next_part(parts)) is not None:
            ordinal += 1
            await append_prepared_part(
                session_factory,
                context,
                stream_slot=slot,
                part_ordinal=ordinal,
                part=part,
            )
    except BaseException as error:
        primary = error
    primary = staging._close_iterator(parts, primary)
    primary = staging._close_iterator(decoded, primary)
    for failure in cleanup_failures:
        if primary is not failure and (primary is None or primary.__cause__ is not failure):
            primary = staging._source_cleanup_failure(primary, failure)
    if primary is not None:
        raise primary
    await mark_prepared_stream_eof(session_factory, context, stream_slot=slot)


async def prepare_retained_source(session_factory, *, execution_id, fence, token, source_permit):
    """Resolve persisted scope, prepare originals or observe a completed seal."""

    context = await load_prepared_source_context(
        session_factory,
        execution_id=execution_id,
        fence=fence,
        token=token,
        source_permit=source_permit,
    )
    state = await begin_prepared_source(session_factory, context)
    if state.state == "pending":
        captures = await _originals(session_factory, context)
        streams_by_id = {stream.stream_id: stream for stream in context.definition.source_streams}
        for stream_id, slot in context.stream_slots:
            if not state.progress[slot - 1].eof:
                await _prepare_stream(session_factory, context, streams_by_id[stream_id], captures[stream_id], slot)
    return await seal_prepared_source(session_factory, context)
