# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Seal durable owner shards through the ordinary generation finality path."""

import datetime as dt
import time
from contextlib import asynccontextmanager
from types import MappingProxyType

from db.models.custom_import import CustomImportExecution, CustomImportLease
from process.custom_import import build_source, publication
from process.custom_import.continuation_runtime import producer_token_sha256, verify_and_bind_continuation
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_materialization import (
    BlockMaterializationModels,
    materialization,
    materialization_work,
)
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost


async def unchanged_block_base(session, request, build, canonical):
    """Keep identical output only when the current base already serves blocks."""
    from process.custom_import.build_output import _unchanged_base

    unchanged = await _unchanged_base(session, request, build, canonical)
    if unchanged is None:
        return None
    base, _seal = unchanged
    # Reuse the protected resolver: missing/retired relations must refuse rather
    # than being mistaken for a legacy layout, even after the semantic check.
    bound = await publication._finality_materialization_models(session, base)
    if bound is None or type(bound) is MappingProxyType:
        return None
    if type(bound) is not BlockMaterializationModels:
        raise CandidateRunnerError("no-change base physical binding is unresolved")
    binding = bound.family_block_binding
    if (
        type(binding) is not FamilyBlockBinding
        or bound.closed_at is None
        or base.generation_id != request.expected_base_generation_id
        or (base.dataset_id, base.definition_revision_id, base.schema_revision_id)
        != (request.dataset_id, request.definition_revision_id, request.schema_revision_id)
        or binding.layout.definition.digest != request.definition.digest
    ):
        raise CandidateRunnerError("no-change base block binding differs")
    return unchanged


async def seal_shard_build(session_factory, request, build_id, *, work):
    """Reconstruct once inside finality, retaining exact terminal retry behavior."""
    from process.custom_import.build_output import _replay, _terminal_seal

    if type(work) is not PlanWork:
        raise CandidateRunnerError("shard finality requires accounted owner work")
    if work.check is not None:
        work.check()
    replay = await _replay(session_factory, request, build_id)
    if replay is not None:
        return replay
    return await _terminal_seal(session_factory, request, build_id, None, block_work=work)


@asynccontextmanager
async def finality_materialization(session, request, build, generation, *, work):
    """Keep reconstruction and terminal writes under one current bounded lease."""
    if type(work) is not PlanWork:
        raise CandidateRunnerError("shard finality requires accounted owner work")
    await build_source._prepare_statement(session)
    execution = await session.get(CustomImportExecution, request.execution_id)
    await build_source._prepare_statement(session)
    lease = await session.get(CustomImportLease, request.execution_id)
    seal_request = publication._GenerationSealRequest(
        request.dataset_id, generation.generation_id, request.fence, producer_token_sha256(request)
    )
    publication._validate_generation_producing_authority(execution, generation, seal_request)
    if request.continuation is not None:
        await build_source._prepare_statement(session)
        now = await publication._database_now(session)
        await verify_and_bind_continuation(session, request, execution, lease, now, build=build)
    token_sha256, continuation_id = publication._sealing_lease_identity(seal_request, request)
    await build_source._prepare_statement(session)
    started = time.monotonic()
    renewed_at = await publication._renew_finality_lease(
        session,
        execution,
        lease,
        fence=request.fence,
        token_sha256=token_sha256,
        continuation_id=continuation_id,
        conflict_message="shard finality requires a current running lease",
    )
    expires_at = build_source._page_expiry(request, lease.expires_at)
    if request.operation_deadline is not None:
        expires_at = min(expires_at, renewed_at + dt.timedelta(seconds=request.operation_deadline - started))
    deadline = started + (expires_at - renewed_at).total_seconds()
    session.info[build_source._WINDOW] = build_source._PageWindow(deadline, request.statement_timeout_ms)

    def check():
        """Keep pure CPU work inside the current owner deadline."""
        if time.monotonic() >= deadline:
            raise LeaseAuthorityLost("shard finality owner deadline elapsed")
        if work.check is not None:
            work.check()

    async with publication._finality_scan_window(session, now=renewed_at, expires_at=expires_at):
        check()
        with materialization_work(session, work.root, work.limits, work.register_cleanup, check=check):
            bound = await publication._finality_materialization_models(session, generation)
            if type(bound) is not BlockMaterializationModels:
                raise CandidateRunnerError("shard finality requires its protected durable block binding")
            canonical = await materialization(session, generation, bound)
            check()
            yield canonical
            check()
