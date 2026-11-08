# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner composition from completed families through immutable OUTPUT closure."""

import asyncio
import stat
import threading
from contextlib import AsyncExitStack, closing
from pathlib import Path

from sqlalchemy import text

from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import family_block_store as store
from process.custom_import import family_shard_output_checkpoint as checkpoint
from process.custom_import.build_graph import _snapshot
from process.custom_import.build_source import _WINDOW, _drain_cleanup, _page_session, _prepare_statement
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_validate import CandidateScope, check_closed_identity
from process.custom_import.family_shard_closed_verification import (
    ClosedShardInputs,
    _native_values,
    _winner_batches,
    selected_shard_families,
)
from process.custom_import.family_shard_common import _offload
from process.custom_import.family_shard_graph_control import open_shard_output
from process.custom_import.family_shard_member_winners import build_original_member_winners
from process.custom_import.family_shard_output_control import complete_shard_output
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_projection_complete import complete_shard_query_projection
from process.custom_import.family_shard_projection_stage import stage_shard_query_projection
from process.custom_import.family_shard_projection_verify import verify_closed_query_projection
from process.custom_import.family_shard_retained_stage import RetainedFamilyStage
from process.custom_import.family_shard_source_stage import _retain, _retain_cpu
from process.custom_import.family_shard_source_values import prepared_source_family_inputs
from process.custom_import.family_shard_winner_validation import RetainedWinnerInput
from process.custom_import.family_shard_winners import cleanup_winner_output
from process.custom_import.materialization import GenerationIdentity
from process.custom_import.runner_types import CancellationRequested


class ShardOutputStage:
    """Keep genuine original producers alive across native OUTPUT retries."""

    def __init__(self, retained, *, work):
        if type(retained) is not RetainedFamilyStage or type(work) is not PlanWork:
            raise ValueError("OUTPUT stage requires retained owners and accounted work")
        self.retained, self.source, self.work = retained, retained.source, work
        self.root, self.limits = Path(work.root).absolute(), work.limits
        self.stop, self._stack = threading.Event(), AsyncExitStack()
        self._root_identity, self._busy, self._closed = None, False, False
        self.pending = self.generation = self.prepared = self.member_winners = None
        self.winner_receipt = self.projection_output = self.projection = self.inputs = None
        self._projection_ready = False

    def current(self):
        """Check only thread-safe stop/deadline state during CPU work."""
        if self.stop.is_set():
            raise CancellationRequested("OUTPUT stage CPU work stopped")
        self.retained.current()
        if self.work.check is not None:
            self.work.check()

    def _ensure_root(self):
        if self._root_identity is None:
            if self.root.exists() or self.root.is_symlink():
                raise FileExistsError("OUTPUT stage root already exists")
            self.work.register_cleanup(self.root)
            self.root.mkdir(mode=0o700)
            observed = self.root.lstat()
            self._root_identity = observed.st_dev, observed.st_ino
        observed = self.root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != self._root_identity:
            raise FamilyBlockUnavailable("OUTPUT stage root custody differs")

    async def close_terminal(self):
        """Close only this stage after its work and downstream finality have drained."""
        if self._busy:
            raise ValueError("OUTPUT advance must drain before cleanup")
        if self._closed:
            return
        primary = await _drain_cleanup(self._stack.aclose(), None)
        if primary is None and self._root_identity is not None:
            self._ensure_root()
            if checkpoint.is_enabled(self):
                await _offload(checkpoint.remove_root, self)
            else:
                self.root.rmdir()
        if primary is not None:
            raise primary
        self._closed = True


def _work(owner, name):
    return PlanWork(owner.root / name, checkpoint.remaining(owner), owner.work.register_cleanup, owner.current)


def _close_members(outputs):
    try:
        outputs[0].close()
    finally:
        cleanup_winner_output(outputs[1])


def _prepare_members(owner):
    context = owner.retained.context
    originals = RetainedWinnerInput(owner.retained.destination, owner.retained.original)
    with selected_shard_families(
        context, tuple(owner.source.workers), owner.prepared, originals, work=_work(owner, "selected")
    ) as families:
        return build_original_member_winners(
            context.common,
            owner.generation,
            context.final_plan,
            families,
            len(owner.source.workers) + 1,
            work=_work(owner, "members"),
            member_shard_id=len(owner.source.workers) + 4,
        )


def _winner_receipt(owner):
    common, limits = owner.retained.context.common, owner.source.limits.allocation
    proof = store.ShardOutputProof(
        common.layout, len(owner.source.workers) + 5, bytes.fromhex(common.source.source.partition_sha256), 0
    )
    with closing(_winner_batches(owner.member_winners[1], limits, owner)) as batches:
        for batch in batches:
            for role, rows in batch.items():
                proof.add(role, rows)
    return proof.receipt()


async def _copy(factory, owner, batches, receipt):
    common, limits = owner.retained.context.common, owner.source.limits.allocation
    with closing(batches) as source_batches:
        if await checkpoint.has_copy_receipt(factory, owner, receipt):
            return
        await store.copy_shard(
            factory,
            common.request,
            common.source.snapshot_family_id,
            common.layout,
            source_batches,
            receipt,
            maximum_rows=limits.maximum_batch_records,
            maximum_bytes=limits.maximum_batch_bytes,
        )
    owner.current()


def _batches(owner, output):
    limits = owner.source.limits.allocation
    return output.batches(maximum_rows=limits.maximum_batch_records, maximum_bytes=limits.maximum_batch_bytes)


def _prepare_source(owner):
    context = owner.retained.context
    manager = prepared_source_family_inputs(
        context.common, owner.source.admission, owner.retained.final_plan, work=_work(owner, "source-families")
    )
    prepared = manager.__enter__()
    owner._stack.push_async_callback(_offload, manager.__exit__, None, None, None)
    return prepared


async def _load_output(factory, projection_factory, owner):
    common, build_id = owner.retained.context.common, owner.source.context.build_id
    await checkpoint.commit(factory, owner, "inputs")
    if owner.generation is None:
        identity = await open_shard_output(
            factory, common, build_id, owner.retained.final_plan, owner.source.admission, check=owner.current
        )
        owner.generation = GenerationIdentity(
            identity,
            common.request.dataset_id,
            common.request.definition_revision_id,
            common.request.schema_revision_id,
        )
    await checkpoint.commit(factory, owner, "generation")
    await _retain_cpu(owner, "prepared", lambda: _prepare_source(owner))
    await checkpoint.commit(factory, owner, "source-families")
    await _retain_cpu(owner, "member_winners", lambda: _prepare_members(owner), _close_members)
    await _retain_cpu(owner, "winner_receipt", lambda: _winner_receipt(owner))
    await checkpoint.commit(factory, owner, "member-winners")
    await _copy(factory, owner, _batches(owner, owner.member_winners[0]), owner.member_winners[0].receipt)
    await _copy(
        factory,
        owner,
        _winner_batches(owner.member_winners[1], owner.source.limits.allocation, owner),
        owner.winner_receipt,
    )
    if not owner._projection_ready:
        await _retain(
            owner,
            "projection_output",
            lambda: stage_shard_query_projection(
                projection_factory,
                common,
                build_id,
                work=_work(owner, "projection"),
                limits=owner.source.limits.allocation,
                shard_id=len(owner.source.workers) + 6,
            ),
            lambda output: None if output is None else output.close(),
        )
        owner._projection_ready = True
    await checkpoint.commit(factory, owner, "projection")
    if owner.projection_output is not None:
        await _copy(factory, owner, _batches(owner, owner.projection_output), owner.projection_output.receipt)
    if checkpoint.is_enabled(owner):
        await checkpoint.observe(factory, owner, require_complete=True)
    await checkpoint.commit(factory, owner, "close-ready")


async def _closed_projection(factory, owner):
    context, request = owner.retained.context, owner.source.context.request
    async with _page_session(factory, request, context.build_id) as (authority, _build):
        window = authority.info[_WINDOW]

    async def prepare(session):
        """Use the exact current owner window for every native projection read."""
        owner.current()
        await _prepare_statement(session)

    async with asyncio.timeout_at(window.deadline), factory() as session, session.begin():
        session.info[_WINDOW] = window
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await prepare(session)
        descriptor, binding = await store._candidate(
            session,
            request,
            context.common.source.snapshot_family_id,
            context.common.layout,
            {"closed", "verified", "logged"},
            locked=False,
        )
        snapshot = await session.get(CustomImportSnapshotFamily, binding.family_id, populate_existing=True)
        if snapshot is None:
            raise FamilyBlockUnavailable("closed projection snapshot is missing")
        scope = CandidateScope(
            binding.family_id,
            owner.generation.generation_id,
            request.dataset_id,
            request.definition_revision_id,
            request.schema_revision_id,
            request.execution_id,
            dict(context.common.source.source.scope[0])["capture_bundle_id"],
            request.fence,
            producer_token_sha256(request),
            request.expected_base_generation_id,
        )
        identity = check_closed_identity(
            binding, scope, _native_values(snapshot), _native_values(descriptor), phase=descriptor.phase
        )
        return await verify_closed_query_projection(
            session,
            binding,
            owner.generation,
            identity,
            work=_work(owner, "projection-verify"),
            limits=owner.source.limits.allocation,
            prepare=prepare,
        )


def _closed_inputs(owner):
    source_stage, retained = owner.source, owner.retained
    if any(
        artifact is None
        for artifact in (
            owner.generation,
            owner.prepared,
            owner.member_winners,
            owner.winner_receipt,
            owner.projection,
            source_stage.events,
            source_stage.common_receipt,
            source_stage.event_receipt,
            retained.common_receipt,
        )
    ):
        raise FamilyBlockUnavailable("OUTPUT completion requires its original completed artifacts")
    return ClosedShardInputs(
        tuple(source_stage.workers),
        owner.prepared,
        source_stage.metadata.query_keys,
        source_stage.events,
        RetainedWinnerInput(retained.destination, retained.original),
        retained.encoded,
        retained.stored,
        *owner.member_winners,
        owner.projection_output,
        owner.projection,
        source_stage.common_receipt,
        source_stage.event_receipt,
        retained.common_receipt,
        owner.winner_receipt,
    )


async def _loading_phase(factory, owner):
    context = owner.retained.context
    async with _page_session(factory, context.common.request, context.build_id) as (session, _build):
        descriptor, _binding = await store._candidate(
            session,
            context.common.request,
            context.common.source.snapshot_family_id,
            context.common.layout,
            {"loading", "closed"},
            locked=False,
        )
        return descriptor.phase


async def _prove_projection(factory, owner):
    try:
        await _retain(owner, "projection", lambda: _closed_projection(factory, owner))
    except BaseException:
        path = owner.root / "projection-verify"
        if owner.pending == "projection" and not path.exists() and not path.is_symlink():
            owner.pending = None
        raise


async def advance_shard_output(factory, owner, *, projection_factory=None, index_factory=None):
    """Close real output once, preserving original owners for final verification."""
    if type(owner) is not ShardOutputStage or owner._closed or owner._busy or not owner.retained.complete:
        raise ValueError("OUTPUT stage requires its completed retained owner")
    if owner.pending is not None:
        raise FamilyBlockUnavailable("unfinished OUTPUT artifact requires retained recovery")
    owner._busy = True
    owner.stop.clear()
    try:
        owner.current()
        owner._ensure_root()
        common, build_id = owner.retained.context.common, owner.source.context.build_id
        build = await _snapshot(factory, common.request, build_id)
        if build.phase not in {"graph", "output", "verifying"}:
            raise FamilyBlockUnavailable("OUTPUT stage cannot reopen a later immutable phase")
        if build.phase in {"graph", "output"}:
            if await _loading_phase(factory, owner) == "loading":
                await _load_output(factory, projection_factory or factory, owner)
                await store.close_candidate(factory, common.request, common.source.snapshot_family_id, common.layout)
            elif owner.generation is None or owner.prepared is None or owner.member_winners is None:
                raise FamilyBlockUnavailable("closed OUTPUT requires its original loading artifacts")
            await store.prepare_indexes(
                index_factory or factory, common.request, common.source.snapshot_family_id, common.layout
            )
            await _prove_projection(projection_factory or factory, owner)
            await complete_shard_query_projection(factory, common, build_id, owner.projection)
            await complete_shard_output(
                factory,
                common,
                build_id,
                owner.retained.final_plan,
                owner.source.admission,
                owner.member_winners[1],
                check=owner.current,
            )
        elif owner.projection is None:
            await _prove_projection(projection_factory or factory, owner)
        owner.inputs = _closed_inputs(owner)
        owner.current()
        return owner
    finally:
        owner._busy = False
