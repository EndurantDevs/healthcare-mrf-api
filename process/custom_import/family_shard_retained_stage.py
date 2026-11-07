# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained original/destination/output custody before generation opening.

This composes existing import APIs, never manufactures proof or empty input.
Protected cold reopening ends before native GRAPH plan advancement.
"""

import stat
import threading
from contextlib import AsyncExitStack, closing
from dataclasses import replace
from functools import partial
from pathlib import Path

from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_checkpoint as checkpoint
from process.custom_import import family_shard_source_stage as source_stage
from process.custom_import.build_source import _drain_cleanup
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_shard_base import PinnedBase
from process.custom_import.family_shard_base_values import RetainedWork, build_retained_input, cleanup_retained_input
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded
from process.custom_import.family_shard_output import FamilyOutputWork
from process.custom_import.family_shard_plan import FinalPlanDescriptor, PlanWork
from process.custom_import.family_shard_retained_control import complete_retained_pack_counters
from process.custom_import.family_shard_retained_encoding import build_retained_output
from process.custom_import.family_shard_retained_homes import register_retained_homes
from process.custom_import.family_shard_retained_storage_validation import prove_retained_output
from process.custom_import.family_shard_retained_validation import verify_retained_destination
from process.custom_import.family_shard_source_journal import SourceAllocationJournal
from process.custom_import.runner_types import CancellationRequested


def _require_source(source):
    if type(source) is not source_stage.SourceAdmissionStage or source._closed or source._busy:
        raise ValueError("retained stage requires the live completed SOURCE owner")
    if source.phase != "graph" or source.homes is None:
        raise ValueError("retained stage requires completed SOURCE admission and homes")
    if (
        type(source.allocation_journal) is not SourceAllocationJournal
        or source.allocation_journal.build_id != source.context.build_id
    ):
        raise FamilyBlockUnavailable("retained stage requires original warm SOURCE journal custody")
    source_stage._require_originals(
        source,
        (
            "prepared",
            "allocated",
            "metadata",
            "routed",
            "tasks",
            "common_context",
            "admission",
            "counts",
            "common_output",
            "common_receipt",
            "events",
            "event_receipt",
        ),
    )


class RetainedFamilyStage:
    """Keep retained evidence live across COPY/home/tail errors and finality.

    Construct before awaiting advance. The caller owns the SOURCE stage, final
    plan and actual base pin. Their lifetimes extend through terminal validation.
    The work callback is thread safe; cleanup affects only this stage's resources.
    """

    def __init__(self, source, final_plan, base, *, work):
        _require_source(source)
        if type(final_plan) is not FinalPlanDescriptor or (base is not None and type(base) is not PinnedBase):
            raise ValueError("retained stage requires its genuine final plan and base")
        if type(work) is not PlanWork:
            raise ValueError("retained stage requires accounted resource work")
        self.source, self.final_plan, self.base, self.work = source, final_plan, base, work
        self.source_journal = source.allocation_journal
        self.root, self._root_identity = Path(work.root).absolute(), None
        self.stop, self._stack = threading.Event(), AsyncExitStack()
        self.pending, self._busy, self._closed = None, False, False
        self.retained_input = self.context = self.destination = self.original = None
        self.encoded = self.stored = self.common_receipt = self.homes = self.pack_tail = None
        self.tail_started, self.complete = False, False
        self._checkpoint_stage, self._home_intent = -1, None
        self._common_copied = self._encoded_copied = False
        self._closed_costs = {}

    def current(self):
        """Keep caller/SOURCE bounds and the current joined CPU stop flag."""
        if self.stop.is_set():
            raise CancellationRequested("retained stage CPU work stopped")
        if self.source.allocation_journal is not self.source_journal:
            raise FamilyBlockUnavailable("retained original SOURCE journal owner changed")
        self.source.current()
        if self.work.check is not None:
            self.work.check()

    def _ensure_root(self):
        if self._root_identity is None:
            if self.root.exists() or self.root.is_symlink():
                raise FileExistsError("retained stage root exists; original custody is required")
            checkpoint.retained_limits(self)
            self.work.register_cleanup(self.root)
            self.root.mkdir(mode=0o700)
            observed = self.root.lstat()
            self._root_identity = observed.st_dev, observed.st_ino
        observed = self.root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != self._root_identity:
            raise FamilyBlockUnavailable("retained stage root custody differs")

    async def close_terminal(self):
        """Remove only owned retained work after all consumers and CPU work end."""
        if self._busy:
            raise ValueError("retained advance must drain before terminal cleanup")
        if self._closed:
            return
        self._closed = True
        primary = await _drain_cleanup(self._stack.aclose(), None)
        if self._root_identity is not None:
            try:
                self._ensure_root()
                self.root.rmdir()
            except BaseException as error:
                primary = primary or error
        if primary is not None:
            raise primary


def _allocation_work(owner, name, *, writing=True):
    limits = owner.source.limits.allocation
    if writing and name != "destination":
        remaining = checkpoint.retained_limits(owner).max_spool_bytes
        if remaining < max(limits.spool.max_record_bytes, limits.spool.memory_bytes):
            raise SourceFactBudgetExceeded("retained work leaves insufficient original allocation allowance")
        limits = replace(
            limits, spool=replace(limits.spool, max_spool_bytes=min(limits.spool.max_spool_bytes, remaining))
        )
    return allocation.RetainedAllocationWork(owner.root / name, limits, owner.work.register_cleanup, owner.current)


def _fact_work(owner, name):
    return PlanWork(owner.root / name, checkpoint.retained_limits(owner), owner.work.register_cleanup, owner.current)


def _output_work(owner):
    limits = owner.source.limits.worker.output
    maximum = min(limits.max_spool_bytes, checkpoint.retained_limits(owner).max_spool_bytes)
    if maximum < max(limits.max_record_bytes, limits.fragment_bytes, limits.memory_bytes):
        raise SourceFactBudgetExceeded("retained work leaves insufficient original output allowance")
    return FamilyOutputWork(
        owner.root / "encoded", replace(limits, max_spool_bytes=maximum), owner.work.register_cleanup, owner.current
    )


async def _destination(factory, owner):
    journal = owner.source_journal
    journal._retained_bundle_owner = owner if getattr(owner.source, "owner_profile", None) is not None else None
    try:
        return await allocation.allocate_retained_destination(
            factory, owner.context, work=_allocation_work(owner, "destination"), journal=journal
        )
    finally:
        journal._retained_bundle_owner = None


async def _build_input(owner):
    return await build_retained_input(
        owner.base,
        owner.final_plan,
        work=RetainedWork(
            owner.root / "input", checkpoint.retained_limits(owner), owner.work.register_cleanup, owner.current
        ),
    )


async def _verify_original(owner):
    return await verify_retained_destination(owner.context, owner.destination, work=_allocation_work(owner, "original"))


async def _prepare_retained(factory, owner):
    await source_stage._retain(
        owner,
        "retained_input",
        partial(_build_input, owner),
        cleanup_retained_input,
    )
    await checkpoint.commit_retained_custody(factory, owner, "input")
    if owner.context is None:
        source_owner = owner.source
        owner.context = allocation.RetainedAllocationContext(
            source_owner.common_context,
            source_owner.context.build_id,
            source_owner.admission,
            owner.final_plan,
            owner.retained_input,
            owner.base,
            source_owner.common_output,
            source_owner.events,
        )
    await source_stage._retain(
        owner,
        "destination",
        partial(_destination, factory, owner),
        allocation.cleanup_retained_destination,
    )
    await checkpoint.commit_retained_custody(factory, owner, "destination")
    await source_stage._retain(
        owner,
        "original",
        partial(_verify_original, owner),
    )
    await checkpoint.commit_retained_custody(factory, owner, "original")


def _build_encoded(owner):
    return build_retained_output(
        owner.context,
        owner.destination,
        owner.original,
        owner.source.metadata.query_keys,
        work=_output_work(owner),
        shard_id=len(owner.source.workers) + 3,
    )


async def _prove_stored(owner):
    return await prove_retained_output(
        owner.context,
        owner.destination,
        owner.original,
        owner.encoded,
        owner.source.metadata.query_keys,
        work=_fact_work(owner, "stored"),
    )


async def _encode_retained(owner):
    await allocation._check_base(owner.context)
    await source_stage._retain_cpu(
        owner,
        "encoded",
        partial(_build_encoded, owner),
        lambda output: output.close(),
    )
    await allocation._check_base(owner.context)
    await checkpoint.commit_retained_custody(getattr(owner, "_factory", None), owner, "encoded")
    await source_stage._retain(
        owner,
        "stored",
        partial(_prove_stored, owner),
    )
    await checkpoint.commit_retained_custody(getattr(owner, "_factory", None), owner, "stored")


def _batches(owner, output):
    limits = owner.source.limits.allocation
    with closing(
        output.batches(maximum_rows=limits.maximum_batch_records, maximum_bytes=limits.maximum_batch_bytes)
    ) as copied:
        for batch in copied:
            owner.current()
            yield batch
    owner.current()


def _common_receipt(owner):
    source = owner.source
    proof = ShardOutputProof(
        source.context.layout, len(source.workers) + 2, bytes.fromhex(source.allocated.source.partition_sha256), 0
    )
    for batch in _batches(owner, owner.destination):
        for role, cells in batch.items():
            proof.add(role, cells)
    return proof.receipt()


async def _copy_and_home(factory, owner):
    await source_stage._retain_cpu(owner, "common_receipt", partial(_common_receipt, owner))
    if not owner._common_copied:
        await source_stage._copy(factory, owner.source, _batches(owner, owner.destination), owner.common_receipt)
        owner._common_copied = True
    await checkpoint.commit_retained_custody(factory, owner, "common-copy")
    if not owner._encoded_copied:
        await source_stage._copy(factory, owner.source, _batches(owner, owner.encoded), owner.encoded.receipt)
        owner._encoded_copied = True
    await checkpoint.commit_retained_custody(factory, owner, "encoded-copy")
    if owner.homes is None:
        if getattr(owner.source, "owner_profile", None) is not None:
            await checkpoint.begin_homes(factory, owner)
            census = await checkpoint.home_census(factory, owner)
            if census[:2] != census[2:]:
                await register_retained_homes(
                    factory, owner.context, owner.destination, work=_allocation_work(owner, "homes")
                )
            await checkpoint.finish_homes(factory, owner)
        else:
            owner.homes = await register_retained_homes(
                factory, owner.context, owner.destination, work=_allocation_work(owner, "homes")
            )


def _require_tail_inputs(owner):
    if any(
        getattr(owner, name) is None
        for name in (
            "retained_input",
            "context",
            "destination",
            "original",
            "encoded",
            "stored",
            "common_receipt",
            "homes",
        )
    ):
        raise FamilyBlockUnavailable("retained tail retry requires all original completed artifacts")


async def _finish_tail(factory, owner):
    _require_tail_inputs(owner)
    owner.current()
    owner.tail_started = True
    owner.pack_tail = await complete_retained_pack_counters(
        factory,
        owner.context,
        owner.destination,
        owner.original,
        owner.stored,
        owner.encoded,
        owner.common_receipt,
        work=_allocation_work(owner, "tail"),
    )
    owner.complete = True


async def advance_retained_families(factory, owner):
    """Complete retained storage, preserving exact IDs and tail-safe retries.

    Once the tail call starts, retry only that same-artifact control operation.
    Never re-enter allocation/home prefix guards after an uncertain tail commit.
    Generation opening and later immutable phases remain coordinator-owned.
    """
    if type(owner) is not RetainedFamilyStage or owner._closed or owner._busy:
        raise ValueError("retained stage is unavailable or already advancing")
    if owner.source.context.request.continuation is not None:
        raise FamilyBlockUnavailable("continuation cannot allocate or replay the retained prefix")
    owner._busy = True
    owner._factory = factory
    owner.stop.clear()
    try:
        owner.current()
        if owner.source._closed or owner.source._busy:
            raise FamilyBlockUnavailable("retained SOURCE owners are unavailable")
        _require_source(owner.source)
        phase = await source_stage._native_phase(factory, owner.source)
        if phase != "graph":
            raise FamilyBlockUnavailable("retained stage requires native GRAPH before generation opening")
        if owner.base is not None:
            await owner.base.check()
        if owner.pending is not None and not (
            owner.pending == "destination"
            and owner.destination is None
            and owner.context is not None
            and owner.retained_input is not None
            and owner.context.retained_input is owner.retained_input
            and owner.context.common is owner.source.common_context
            and owner.context.events is owner.source.events
            and type(owner.source.allocation_journal) is SourceAllocationJournal
            and owner.source.allocation_journal.build_id == owner.context.build_id
            and owner.source.context.request.continuation is None
        ):
            raise FamilyBlockUnavailable(
                "unfinished retained artifact requires original custody; regeneration is refused"
            )
        if owner.pending == "destination":
            _require_source(owner.source)
        owner._ensure_root()
        await checkpoint.commit_retained_custody(factory, owner, "plan")
        if owner.tail_started and not owner.complete and getattr(owner.source, "owner_profile", None) is not None:
            await checkpoint.observe(factory, owner)
        if not owner.tail_started:
            await _prepare_retained(factory, owner)
            await _encode_retained(owner)
            await _copy_and_home(factory, owner)
        if not owner.complete:
            await _finish_tail(factory, owner)
        await checkpoint.commit_retained_custody(factory, owner, "tail")
        owner.current()
        return owner
    finally:
        owner._busy = False
