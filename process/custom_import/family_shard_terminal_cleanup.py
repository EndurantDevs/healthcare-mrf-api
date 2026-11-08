# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Seal completed owner custody before callbacks can destroy its original proofs."""

import os
import time
from dataclasses import dataclass

from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_terminal_files as evidence
from process.custom_import.build_source import _drain_cleanup
from process.custom_import.family_shard_common import _offload
from process.custom_import.family_shard_retirement_native import read_terminal_scope
from process.custom_import.family_shard_retirement_policy import original_policy


@dataclass(frozen=True)
class TerminalCustody:
    """Trusted caller inputs; no client-supplied file manifest or retirement authority."""

    parent: dict
    profile: dict
    factory: object


def bind(job, parent, profile, factory):
    """Bind once from the authenticated fixed owner caller before any owner work."""
    if job._terminal_custody is not None or job.root != handoff.OWNER_PARENT / "owner" or not callable(factory):
        handoff.unavailable()
    job._terminal_custody = TerminalCustody(parent, profile, factory)


async def prepare(job):
    """Drain workers and seal only a completed, committed owner before any deletion."""
    if job._terminal_custody is None or job._terminal_inventory is not None or not job.complete:
        return
    if job._root_identity is None:
        return
    for owner in (job.source, job.retained, job.output_stage):
        if owner is not None and owner._busy:
            raise ValueError("owner stages must drain before terminal census")
    if job.source is not None:
        primary = None
        for run in reversed(job.source.runs):
            primary = await _drain_cleanup(run.stop(terminate=True), primary)
        if primary is not None:
            raise primary
    bound = job._terminal_custody
    native = await read_terminal_scope(bound.factory, bound.parent, bound.profile)
    await _offload(_seal, job, native)


def _seal(job, native):
    """Keep a finite original-lock check independent of expired writer/cancel controls."""
    deadline = time.monotonic() + 3600

    def check():
        """Keep census bounded under the retained original lifetime lock."""
        if time.monotonic() >= deadline:
            handoff.unavailable()
        job._owner_lock.check()

    check()
    root = job.root / "source/allocation-journal/checkpoints"
    owner_sha, _bytes = checkpoint._owner_file(root, native.scope, job.source_limits.checkpoint, create=False)
    snapshot = checkpoint.CommittedOwnerCheckpoint(
        str(root), native.scope, native.head, owner_sha, job.source_limits.checkpoint
    )
    policy, original = original_policy(snapshot, job.root, job.source_limits, job.projection_work_mem_mb, check)
    bound = job._terminal_custody
    context = evidence.original_context(job.root, bound.parent, bound.profile, native, job._owner_lock)
    if original.lock_identity != context.lock_identity or original.directories[0][1] != context.root_identity:
        handoff.unavailable()
    evidence.seal(context, policy, check)
    job._terminal_inventory = context


def drain(job):
    """After callbacks release their resources, consume only the sealed exact remainder."""
    context = job._terminal_inventory
    if context is None:
        return
    entries, _digest = evidence.read(context)
    evidence.files._check_remaining(context, entries, job._owner_lock.check)
    evidence.files._consume_entries(context, entries, job._owner_lock.check)
    evidence.mark_drained(context, job._owner_lock.check)


def finish(job):
    """Complete state removal after the original lifetime lock/root have gone."""
    if job._terminal_inventory is not None:
        evidence.finish(job._terminal_inventory)


def is_interrupted_root_finished(job):
    """Retry only this process's sealed drain, never recreate a missing lifetime lock."""
    context = job._terminal_inventory
    if context is None:
        return False
    if os.path.lexists(context.root):
        if os.path.lexists(context.root / ".owner-job.lock"):
            return False
        evidence.files._directory(context.root, context.root_identity)
        directory = evidence.files._state_directory(context)
        try:
            _entries, digest = evidence.read(context)
            if (
                os.listdir(context.root)
                or evidence.files._read_file(directory, evidence.DRAINED, maximum=32)[2] != digest
            ):
                handoff.unavailable()
        finally:
            os.close(directory)
        job._owner_lock.remove_terminal_root()
    finish(job)
    job._closed = True
    return True
