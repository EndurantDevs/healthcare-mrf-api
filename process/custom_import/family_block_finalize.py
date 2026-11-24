# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Seal the frozen compact candidate with its native durability and proof."""

import asyncio

from sqlalchemy import BigInteger, and_, case, func, literal, or_, select, text, union_all
from sqlalchemy.dialects.postgresql import dialect

from db.models.custom_import import (
    CustomImportBuildFamily,
    CustomImportBuildOccurrence,
    CustomImportBuildVerification,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportRootRecord,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_graph, build_output, publication
from process.custom_import.build_source import (
    _WINDOW,
    _flush_page,
    _PageWindow,
    _prepare_statement,
    _resolve_build_snapshot,
)
from process.custom_import.compact_materialization import verify_block_materialization
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import _NEW
from process.custom_import.runner_types import LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_tables

_FROZEN_FIELDS = ("source_frozen_at", "graph_frozen_at", "output_frozen_at")


def _check_frozen(build, proof):
    if (
        build.phase != "verifying"
        or build.verified_at is not None
        or build.generation_id != proof.generation_id
        or any(getattr(build, name) != getattr(proof, name) for name in _FROZEN_FIELDS)
    ):
        raise FamilyBlockUnavailable("compact verification requires the current frozen build")


async def _closed_layout(session, build, binding):
    await _prepare_statement(session)
    descriptor = await session.get(CustomImportFamilyBlockLayout, binding.family_id)
    if descriptor is None or descriptor.phase != "closed" or descriptor.closed_at != build.output_frozen_at:
        raise FamilyBlockUnavailable("compact verification requires the current closed layout")
    return descriptor


async def _role_counts(session, binding):
    await _prepare_statement(session)
    statements = [
        select(literal(role), func.count()).select_from(table) for role, table in binding.tables_by_role.items()
    ]
    return dict((await session.execute(union_all(*statements))).tuples().all())


async def _read_verified_materialization(session_factory, request, registry, binding, proof, limits):
    async with build_graph._session(session_factory, transaction=True) as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        build, deadline = await session.run_sync(
            lambda sync: build_graph._read_authority(sync, request, proof.build_id)
        )
        _check_frozen(build, proof)
        session.info[_WINDOW] = _PageWindow(deadline, request.statement_timeout_ms)
        await _closed_layout(session, build, binding)
        await _prepare_statement(session)
        generation = await session.get(CustomImportGeneration, proof.generation_id)
        if generation is None:
            raise FamilyBlockUnavailable("compact verification requires its frozen generation")
        async with asyncio.timeout_at(deadline) as clock:

            def renew(window):
                """Follow committed lease renewals during the immutable audit."""
                build_graph._require_budget(clock.when())
                clock.reschedule(window.deadline)
                session.info[_WINDOW] = window

            def check():
                """Bound decoding work as well as database statements."""
                build_graph._require_budget(clock.when())

            async with build_graph._renew_while_reading(session_factory, request, on_renew=renew):
                materialization = await verify_block_materialization(
                    session,
                    request,
                    registry,
                    generation,
                    binding,
                    proof,
                    limits=limits,
                    check=check,
                )
                return materialization, await _role_counts(session, binding)


async def _prepare_candidate(session, build, binding, proof, materialization, role_counts):
    _check_frozen(build, proof)
    descriptor = await _closed_layout(session, build, binding)
    quote = dialect().identifier_preparer.format_table
    for role in _NEW:
        await _prepare_statement(session)
        await session.execute(text(f"ALTER TABLE {quote(binding.tables_by_role[role])} SET LOGGED"))
    used_names = {table.name for table in binding.tables_by_role.values()}
    staging_tables = [
        quote(table) for name, table in snapshot_tables(binding.family_id).items() if name not in used_names
    ]
    await _prepare_statement(session)
    await session.execute(text("TRUNCATE TABLE " + ", ".join(staging_tables)))
    await _prepare_statement(session)
    stamp = await publication._database_now(session)
    proof_fields = ("build_id", "generation_id", *_FROZEN_FIELDS, *build_output._COUNT_NAMES)
    proof_by_field = {name: getattr(proof, name) for name in proof_fields}
    await _prepare_statement(session)
    await session.execute(
        CustomImportBuildVerification.__table__.insert().values(
            **proof_by_field, verification_state="complete", scan_stage="complete", verified_at=stamp
        )
    )
    build.phase, build.verified_at = "verified", stamp
    descriptor.phase = "logged"
    descriptor.validation_sha256 = materialization.materialization_sha256
    descriptor.source_record_count = build.source_occurrence_count
    descriptor.role_counts = role_counts
    await _flush_page(session)


async def finalize_block_output(session_factory, request, registry, binding, proof, *, limits):
    """Audit once, then persist proof and seal in the normal owner transaction."""
    try:
        materialization, role_counts = await _read_verified_materialization(
            session_factory, request, registry, binding, proof, limits
        )
    except TimeoutError as error:
        raise LeaseAuthorityLost("compact materialization exceeded its build window") from error

    async def prepare_candidate(session, build):
        """Promote only this isolated candidate before the terminal seal is written."""
        await _prepare_candidate(session, build, binding, proof, materialization, role_counts)

    return await build_output._terminal_seal(
        session_factory,
        request,
        proof.build_id,
        materialization,
        prepare_candidate=prepare_candidate,
        compact_candidate=True,
    )


def _first_source_root_occurrences(occurrence, build_id):
    return (
        select(
            occurrence.c.root_record_id,
            occurrence.c.occurrence_id,
            occurrence.c.root_revision_id,
            occurrence.c.resolved_rejection_id,
        )
        .where(
            occurrence.c.build_id == build_id,
            occurrence.c.origin == "source",
            occurrence.c.record_kind == "root",
            occurrence.c.root_record_id.is_not(None),
        )
        .distinct(occurrence.c.root_record_id)
        .order_by(occurrence.c.root_record_id, occurrence.c.occurrence_id)
        .subquery()
    )


async def _prepare_compact_base_plan(session, build, base):
    """Select SOURCE and carried roots from one closed admission and its base."""
    if build.plan_complete_at is not None:
        return
    if build.plan_page_sequence or build.selected_family_count:
        raise FamilyBlockUnavailable("compact base requires a fresh snapshot plan")
    candidate_id = await _resolve_build_snapshot(session, build.build_id)
    tables = snapshot_tables(candidate_id)
    roots = _first_source_root_occurrences(tables[CustomImportBuildOccurrence.__tablename__], build.build_id)
    plan = tables[CustomImportBuildFamily.__tablename__]
    members = snapshot_tables(base.family_id)[CustomImportGenerationFamily.__tablename__]
    is_rejected = roots.c.resolved_rejection_id.is_not(None)
    identities = CustomImportRootRecord.__table__
    choices = (
        select(
            literal(build.build_id, BigInteger()),
            roots.c.root_record_id,
            identities.c.logical_key_sha256,
            case((is_rejected, "retained"), else_="source"),
            case((~is_rejected, roots.c.occurrence_id)),
            case((is_rejected, members.c.family_revision_id)),
        )
        .select_from(roots)
        .join(
            identities,
            and_(identities.c.root_record_id == roots.c.root_record_id, identities.c.dataset_id == build.dataset_id),
        )
        .outerjoin(
            members,
            and_(
                members.c.root_record_id == roots.c.root_record_id,
                members.c.generation_id == build.base_generation_id,
                members.c.dataset_id == build.dataset_id,
                members.c.definition_revision_id == base.definition_revision_id,
                members.c.schema_revision_id == base.schema_revision_id,
            ),
        )
        .where(
            or_(
                and_(~is_rejected, roots.c.root_revision_id.is_not(None)),
                and_(is_rejected, members.c.family_revision_id.is_not(None)),
            )
        )
    )
    columns = (
        "build_id",
        "root_record_id",
        "root_key_sha256",
        "selection_kind",
        "source_root_occurrence_id",
        "base_family_revision_id",
    )
    await _prepare_statement(session)
    inserted = await session.execute(plan.insert().from_select(columns, choices, include_defaults=False))
    build.plan_stage, build.plan_page_sequence = "complete", 1
    build.selected_family_count = inserted.rowcount
    await _prepare_statement(session)
    build.plan_complete_at = await session.scalar(select(func.clock_timestamp()))
    await _flush_page(session)


async def _existing_layouts(session, build):
    parent = CustomImportSnapshotFamily
    layout = CustomImportFamilyBlockLayout
    await _prepare_statement(session)
    rows = await session.execute(
        select(parent, layout.phase)
        .join(layout, parent.family_id == layout.family_id)
        .where(
            parent.dataset_id == build.dataset_id,
            or_(
                parent.generation_id == literal(build.base_generation_id, BigInteger()),
                and_(parent.execution_id == build.execution_id, parent.producing_fence == build.producing_fence),
            ),
        )
    )
    base = None
    for snapshot, phase in rows:
        if (snapshot.execution_id, snapshot.producing_fence) == (build.execution_id, build.producing_fence):
            raise FamilyBlockUnavailable("compact build requires a fresh candidate")
        if phase != "logged":
            raise FamilyBlockUnavailable("compact base must be sealed")
        base = snapshot
    return base


async def try_build_block_output(session_factory, request, build_id, outcomes, *, limits):
    """Use the lean writer for a fresh owner snapshot."""
    from process.custom_import.build_source import _page_session
    from process.custom_import.failure_diagnostics import FailurePhase, failure_phase
    from process.custom_import.family_block_graph import load_block_graph
    from process.custom_import.family_block_layout import FamilyBlockLayout
    from process.custom_import.family_block_write import initialize_block_layout

    replay = await build_output._replay(session_factory, request, build_id)
    if replay is not None:
        return replay
    async with build_graph._renew_while_reading(session_factory, request):
        build = await build_graph._snapshot(session_factory, request, build_id)
        if build.phase == "rejected":
            return None
        async with _page_session(session_factory, request, build_id) as (session, current):
            compact_base = await _existing_layouts(session, current)
            eligible = current.phase == "graph" and current.refresh_mode == "snapshot" and current.complete_scope
            if compact_base is not None:
                if not eligible:
                    raise FamilyBlockUnavailable("compact base requires a complete snapshot")
                await _prepare_compact_base_plan(session, current, compact_base)
        if not eligible:
            return None
        if compact_base is not None:
            build = await build_graph._snapshot(session_factory, request, build_id)
        build, registry = await build_graph._prepare_graph_plan(session_factory, request, build_id, build)
        async with _page_session(session_factory, request, build_id) as (session, current):
            binding = await initialize_block_layout(
                session,
                current,
                FamilyBlockLayout(request.definition, registry.child_collection_slots),
                allow_retained=compact_base is not None,
            )
        if binding is None:
            if compact_base is not None:
                raise FamilyBlockUnavailable("legacy fallback cannot read the compact base")
            return None
        _, proof = await load_block_graph(
            session_factory,
            request,
            registry,
            build_id,
            binding,
            limits=limits,
            outcomes=outcomes,
            compact_base=compact_base,
        )
    with failure_phase(FailurePhase.OUTPUT):
        return await finalize_block_output(session_factory, request, registry, binding, proof, limits=limits)
