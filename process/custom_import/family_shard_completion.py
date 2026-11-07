# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete original shard verification, durability and ordinary finality."""

import asyncio

from sqlalchemy import text

from process.custom_import import build_output
from process.custom_import import family_block_store as store
from process.custom_import.build_graph import _renew_while_reading, _require_budget, _snapshot
from process.custom_import.build_source import _WINDOW, _page_session, _prepare_statement
from process.custom_import.family_shard_closed_verification import verify_closed_shard_build
from process.custom_import.family_shard_finality import seal_shard_build
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_verification import record_shard_verification


async def _record_verified_snapshot(factory, context, proof, reader_activation=None):
    """Bind the same original proof through durability and snapshot write closure."""
    request, layout, build_id = context.common.request, context.common.layout, context.build_id
    counts_by_role = dict(proof.role_counts)
    if counts_by_role.pop("shard_receipt", None) != proof.completed_shards:
        raise ValueError("closed receipt census differs from completed shards")
    transition_by_field = {
        "validation_sha256": proof.validation_sha256,
        "expected_completed_shards": proof.completed_shards,
        "source_record_count": proof.source_record_count,
        "role_counts": counts_by_role,
    }
    await store.accept_validation(factory, request, proof.identity, layout, **transition_by_field)
    await store.make_logged(factory, request, proof.identity, layout, **transition_by_field)
    if reader_activation is not None:
        await reader_activation.install(factory, context, identity=proof.identity)
    await record_shard_verification(
        factory,
        request,
        build_id,
        layout,
        proof.identity,
        validation_sha256=proof.validation_sha256,
        counts=proof.counts,
    )
    await build_output._freeze_output_snapshot(factory, request, build_id)


async def complete_closed_shard_build(factory, context, inputs, *, work, limits, reader_activation=None):
    """Keep original owners alive while completing the one closed proof and seal.

    The caller has completed projection and OUTPUT closure. It owns every
    original input, output and base pin until this operation returns.
    Its existing work root contains the independently owned verification and finality children.
    """
    if type(work) is not PlanWork:
        raise ValueError("shard completion requires accounted owner work")
    request, build_id = context.common.request, context.build_id
    replay = await build_output._replay(factory, request, build_id)
    if replay is not None:
        return replay
    build = await _snapshot(factory, request, build_id)
    finality_work = PlanWork(work.root / "finality", work.limits, work.register_cleanup, work.check)
    if build.phase == "verified":
        if reader_activation is not None:
            await reader_activation.verify_candidate(factory, context)
        await build_output._freeze_output_snapshot(factory, request, build_id)
        return await seal_shard_build(factory, request, build_id, work=finality_work)
    async with _renew_while_reading(factory, request):
        # Acquire and release fresh authority before the independent immutable read.
        async with _page_session(factory, request, build_id) as (authority, _build):
            window = authority.info[_WINDOW]

        def current():
            """Keep CPU and SQL inside the original authorized read window."""
            _require_budget(window.deadline)
            if work.check is not None:
                work.check()

        async def prepare(session):
            """Bound native statements to the same immutable read window."""
            current()
            await _prepare_statement(session)

        async with asyncio.timeout_at(window.deadline), factory() as session, session.begin():
            session.info[_WINDOW] = window
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await prepare(session)
            proof = await verify_closed_shard_build(
                session,
                context,
                inputs,
                work=PlanWork(work.root / "verification", work.limits, work.register_cleanup, current),
                limits=limits,
                prepare=prepare,
            )
        await _record_verified_snapshot(factory, context, proof, reader_activation)
    return await seal_shard_build(factory, request, build_id, work=finality_work)
