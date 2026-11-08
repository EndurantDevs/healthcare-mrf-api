# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-bound sealed capture input for the shard writer, without SOURCE SQL pages."""

import asyncio
import json
import threading
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from sqlalchemy import select

from db.models.custom_import import CustomImportCaptureBundle, CustomImportExecution
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import.build_graph import _renew_while_reading
from process.custom_import.build_source import _drain_cleanup, _page_session, _set_timeout
from process.custom_import.capture_store import _identity, _load_parquet_part_metadata, open_segmented_parquet_parts
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_store import _candidate, _execute
from process.custom_import.family_shard_facts import SourceFactLimits, _Work
from process.custom_import.family_shard_input import (
    CaptureInputIdentity,
    SealedCaptureInput,
    ShardInputLimits,
    cleanup_spool,
    partition_capture,
)
from process.custom_import.family_shard_input_records import partition_preparations
from process.custom_import.family_shard_source_plan import build_source_plan, cleanup_source_plan
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy


@dataclass(frozen=True)
class SourcePreparationLimits:
    input: ShardInputLimits
    plan: SourceFactLimits

    def __post_init__(self):
        if type(self.input) is not ShardInputLimits or type(self.plan) is not SourceFactLimits:
            raise ValueError("source preparation requires bounded input and plan work")


@dataclass(frozen=True)
class PreparedCaptureSource:
    """Completed descriptors stay owned until all shard retries and owner checks end."""

    source: SealedCaptureInput
    partition: object
    plan: object


def _source_header(request, bundle, receipts):
    return SealedCaptureInput(
        CaptureInputIdentity(
            request.dataset_id,
            request.definition_revision_id,
            request.schema_revision_id,
            bundle.capture_bundle_id,
            bundle.source_binding_revision_id,
            request.definition.digest,
            request.definition.schema_digest,
            None if bundle.source_binding_sha256 is None else bytes(bundle.source_binding_sha256).hex(),
            bytes(bundle.manifest_sha256).hex(),
        ),
        bundle.snapshot_token,
        bytes(bundle.source_request_sha256),
        bytes(bundle.statement_sha256),
        bundle.canonical_manifest,
        SegmentedCapturePolicy.from_mapping(json.loads(bundle.canonical_policy)),
        tuple(sorted(receipts.items())),
    )


async def _read_source_header(session, request):
    execution = (
        await _execute(
            session,
            select(CustomImportExecution)
            .where(CustomImportExecution.execution_id == request.execution_id)
            .execution_options(populate_existing=True),
        )
    ).scalar_one()
    bundle = await session.get(CustomImportCaptureBundle, execution.capture_bundle_id, populate_existing=True)
    if bundle.source_binding_revision_id != execution.source_binding_revision_id:
        raise FamilyBlockUnavailable("source preparation capture binding differs")
    receipts, _, _ = await _load_parquet_part_metadata(
        session,
        _identity(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
        ),
        bundle.capture_bundle_id,
    )
    source = _source_header(request, bundle, receipts)
    source.headers(request.definition)
    return source


async def _load_source(session_factory, request, registry, family_id):
    layout = FamilyBlockLayout(request.definition, registry.child_collection_slots)
    async with _page_session(session_factory, request) as (session, _):
        await _candidate(session, request, family_id, layout, {"loading"})
        return await _read_source_header(session, request)


async def _load_retained_source(session_factory, request, build_id, family_id, layout):
    """Reload sealed headers under the same original live build, without SOURCE work."""
    async with _page_session(session_factory, request, build_id) as (session, build):
        await checkpoint._native_scope(session, request, build_id, family_id, layout, writing=False)
        if request.continuation is not None and (
            request.continuation.build_id != build_id
            or build.phase not in {"graph", "output", "verifying", "verified"}
            or build.source_frozen_at is None
        ):
            raise FamilyBlockUnavailable("continued SOURCE headers require the completed original build")
        return await _read_source_header(session, request)


@asynccontextmanager
async def _capture_parts(session_factory, request, source, work, path):
    """Keep only one immutable Parquet part and one bounded metadata frame live."""
    async with session_factory() as session, session.begin():
        await _set_timeout(session, request.statement_timeout_ms)
        async with open_segmented_parquet_parts(
            session,
            capture_bundle_id=source.identity.capture_bundle_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
        ) as captured:
            by_id = {receipt.stream_id: slot for slot, receipt in source.streams}
            with path.open("xb") as part_metadata:

                async def _observed():
                    async for part in captured:
                        work.frame(
                            part_metadata, (by_id[part.receipt.stream_id], part.ordinal, part.record_count), path
                        )
                        yield part

                parts = _observed()
                try:
                    yield parts
                finally:
                    await parts.aclose()


class _PlanStopped(InterruptedError):
    """The owning async operation stopped this file stage before completion."""


async def _prepare_plan(call):
    """Drain file work before cancellation can remove its exclusively owned root."""
    stopped = threading.Event()

    def _check():
        if stopped.is_set():
            raise _PlanStopped("SOURCE plan preparation stopped")

    task = asyncio.create_task(asyncio.to_thread(call, check=_check))
    try:
        return await asyncio.shield(task)
    except BaseException as failure:
        if task.done() and not task.cancelled() and task.exception() is failure:
            raise
        stopped.set()

        async def _cleanup():
            try:
                plan = await task
            except _PlanStopped:
                return
            cleanup_source_plan(plan)

        raise await _drain_cleanup(_cleanup(), failure)


async def prepare_captured_source(
    session_factory, request, registry, family_id, *, root, register_cleanup, limits, build_id=None
):
    """Complete real capture EOF, original page/hash planning and closed reader exit.

    Completed input and plan files remain available to the owner for native ID
    allocation and unfinished-shard retries. No source rows or counters enter
    PostgreSQL here; the caller still owes admission, COPY and closed validation.
    """
    if request.continuation is not None:
        raise FamilyBlockUnavailable("processing continuation requires its retained SOURCE artifacts")
    if type(limits) is not SourcePreparationLimits:
        raise ValueError("source preparation resource budgets differ")
    root = Path(root)
    capture_input = await _load_source(session_factory, request, registry, family_id)
    with ExitStack() as incomplete, ExitStack() as temporary:
        work = _Work(root / "part-metadata", limits.plan, register_cleanup)
        temporary.callback(work.close)
        path = work.path()
        async with _renew_while_reading(session_factory, request):
            partition = await partition_capture(
                _capture_parts(session_factory, request, capture_input, work, path),
                definition=request.definition,
                source=capture_input,
                root=root / "input",
                register_cleanup=register_cleanup,
                limits=limits.input,
                preparations=(
                    None
                    if build_id is None
                    else partition_preparations(request, registry, build_id, limits.plan.memory_bytes)
                ),
            )
            incomplete.callback(cleanup_spool, partition)
            plan = await _prepare_plan(
                partial(
                    build_source_plan,
                    partition,
                    request,
                    registry,
                    work.rows(path),
                    root=root / "plan",
                    register_cleanup=register_cleanup,
                    limits=limits.plan,
                )
            )
            incomplete.callback(cleanup_source_plan, plan)
        temporary.close()
        incomplete.pop_all()
        return PreparedCaptureSource(capture_input, partition, plan)
