# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Load the compact GRAPH phase and open its native output generation."""

from contextlib import closing

from sqlalchemy import BigInteger, func, literal, select
from sqlalchemy.schema import DropTable
from sqlalchemy.util.concurrency import await_

from db.models.custom_import import CustomImportBuildFamily, CustomImportBuildOccurrence, CustomImportBuildVerification
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from process.custom_import import build_graph
from process.custom_import.build_source import _page_session, _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_identity import allocate_block_identities
from process.custom_import.family_block_projection import write_block_projections
from process.custom_import.family_block_query_input import (
    query_input_table,
    write_query_dictionary,
    write_query_records,
)
from process.custom_import.family_block_retained import copy_retained_families
from process.custom_import.family_block_source import pack_source_page, read_block_roots
from process.custom_import.family_block_structure import check_block_structure
from process.custom_import.family_block_winners import write_block_winners
from process.custom_import.family_block_write import BlockCopyBuffer, _lock_open_layout, copy_block_batch
from process.custom_import.storage_layout import snapshot_tables


async def _copy_source_page(session_factory, reader, request, registry, build_id, binding, roots, limits):
    identities_by_root = await allocate_block_identities(session_factory, request, build_id, binding, roots)

    def pack_and_copy(sync_session):
        """Flush typed query cells and block rows from the same bounded stream."""
        pending = BlockCopyBuffer()
        scalar_counts = [0, 0]

        def account(root_count, child_count):
            """Count scalar cells from the same checked encoding stream."""
            scalar_counts[0] += root_count
            scalar_counts[1] += child_count

        def copy(batch):
            """Write one batch after its read transaction has ended."""
            if sync_session.in_transaction():
                raise FamilyBlockUnavailable("compact COPY requires the frozen read window to be closed")
            await_(
                copy_block_batch(
                    session_factory, request, build_id, binding.family_id, binding.layout, batch, physical=True
                )
            )

        def append(role, cells):
            """Charge one row and flush a full batch."""
            if batch := pending.append(role, cells):
                copy(batch)

        packed_rows = pack_source_page(
            sync_session,
            request,
            registry,
            binding,
            roots,
            identities_by_root,
            limits=limits,
            stage_output=(append, account),
        )
        with closing(packed_rows):
            for role, cells in packed_rows:
                append(role, cells)
        if batch := pending.take():
            copy(batch)

        return tuple(scalar_counts)

    return await reader.run_sync(pack_and_copy)


def _completed_plan_statement(binding, build_id):
    native_tables = snapshot_tables(binding.family_id)
    plan, occurrence = (
        native_tables[CustomImportBuildFamily.__tablename__],
        native_tables[CustomImportBuildOccurrence.__tablename__],
    )
    family = binding.tables_by_role["family"]
    return (
        plan.update()
        .where(
            plan.c.build_id == build_id,
            plan.c.selection_kind == "source",
            plan.c.family_revision_id.is_(None),
            plan.c.complete_at.is_(None),
            plan.c.root_record_id == family.c.root_record_id,
            occurrence.c.build_id == plan.c.build_id,
            occurrence.c.occurrence_id == plan.c.source_root_occurrence_id,
            occurrence.c.root_record_id == plan.c.root_record_id,
            occurrence.c.root_revision_id == family.c.root_revision_id,
            occurrence.c.resolved_rejection_id.is_(None),
            occurrence.c.origin == "source",
            occurrence.c.record_kind == "root",
        )
        .values(
            family_revision_id=family.c.family_revision_id,
            attached_child_count=family.c.child_count,
            complete_at=func.statement_timestamp(),
        )
    )


def _completed_retained_statement(binding, build_id):
    plan = snapshot_tables(binding.family_id)[CustomImportBuildFamily.__tablename__]
    family = binding.tables_by_role["family"]
    return (
        plan.update()
        .where(
            plan.c.build_id == build_id,
            plan.c.selection_kind == "retained",
            plan.c.family_revision_id.is_(None),
            plan.c.complete_at.is_(None),
            plan.c.root_record_id == family.c.root_record_id,
            family.c.retained.is_(True),
            family.c.family_revision_id == plan.c.base_family_revision_id,
        )
        .values(
            family_revision_id=family.c.family_revision_id,
            attached_child_count=family.c.child_count,
            complete_at=func.statement_timestamp(),
        )
    )


async def _close_graph(session_factory, request, build_id, binding, *, has_compact_base=False):
    async with _page_session(session_factory, request, build_id) as (session, build):
        await _lock_open_layout(session, build, binding.family_id, binding.layout)
        if build.phase != "graph" or build.completed_family_count:
            raise FamilyBlockUnavailable("compact graph completion requires a fresh planned candidate")
        await _prepare_statement(session)
        counted = await session.scalar(select(func.count()).select_from(binding.tables_by_role["family"]))
        if counted != build.selected_family_count:
            raise FamilyBlockUnavailable("compact graph population differs from its admitted plan")
        await _prepare_statement(session)
        completed = (await session.execute(_completed_plan_statement(binding, build_id))).rowcount
        if has_compact_base:
            await _prepare_statement(session)
            completed += (await session.execute(_completed_retained_statement(binding, build_id))).rowcount
        if completed != counted:
            raise FamilyBlockUnavailable("compact graph identity population differs from its admitted plan")
        if binding.layout.definition.query.child_collection is not None:
            await _finish_query_input(session, binding)
        build.completed_family_count = counted
        return build.capture_bundle_id


async def _finish_query_input(session, binding):
    source = query_input_table(binding.family_id, binding.layout)
    dictionary = binding.tables_by_role["key_dictionary"]
    records = binding.tables_by_role["query_record"]
    await _prepare_statement(session)
    await session.execute(write_query_dictionary(dictionary, source))
    await _prepare_statement(session)
    inserted = await session.execute(write_query_records(records, dictionary, source))
    await _prepare_statement(session)
    expected = await session.scalar(select(func.count()).select_from(source))
    if inserted.rowcount != expected:
        raise FamilyBlockUnavailable("compact query records differ from the admitted population")
    await _prepare_statement(session)
    await session.execute(DropTable(source))


async def _attach_block_members(session_factory, request, build_id, binding, generation_id):
    async with _page_session(session_factory, request, build_id) as (session, build):
        await _lock_open_layout(session, build, binding.family_id, binding.layout)
        if build.phase != "output" or build.generation_id != generation_id or build.generation_family_count:
            raise FamilyBlockUnavailable("compact membership requires a fresh output generation")
        family = binding.tables_by_role["family"]
        members = binding.tables_by_role["generation_family"]
        ids = (generation_id, build.dataset_id, build.definition_revision_id, build.schema_revision_id)
        rows = select(
            *(literal(identity, BigInteger) for identity in ids), family.c.root_record_id, family.c.family_revision_id
        )
        await _prepare_statement(session)
        inserted = await session.execute(
            members.insert().from_select(tuple(members.c.keys()), rows, include_defaults=False)
        )
        if inserted.rowcount != build.selected_family_count:
            raise FamilyBlockUnavailable("compact membership differs from the admitted plan")
        build.generation_family_count = inserted.rowcount


async def _close_block_output(session_factory, request, build_id, binding, outcomes, scalar_counts):
    async with _page_session(session_factory, request, build_id) as (session, build):
        await _lock_open_layout(session, build, binding.family_id, binding.layout)
        await build_graph._call(session, "freeze_custom_import_build_output", (build_id,))
        await _prepare_statement(session)
        await session.refresh(build, ("phase", "output_frozen_at"))
        if build.phase != "verifying" or build.output_frozen_at is None:
            raise FamilyBlockUnavailable("compact output did not freeze")
        await _prepare_statement(session)
        descriptor = await session.get(CustomImportFamilyBlockLayout, binding.family_id)
        descriptor.phase, descriptor.closed_at = "closed", build.output_frozen_at
        descriptor.source_accepted_family_count = outcomes.accepted_family_count
        descriptor.source_rejection_count = outcomes.rejection_count
        plan = snapshot_tables(binding.family_id)[CustomImportBuildFamily.__tablename__]
        await _prepare_statement(session)
        expected_children = await session.scalar(
            select(func.coalesce(func.sum(plan.c.attached_child_count), 0)).where(plan.c.build_id == build_id)
        )
        await check_block_structure(session, binding, build, expected_children=expected_children)
        return CustomImportBuildVerification(
            build_id=build_id,
            generation_id=build.generation_id,
            source_frozen_at=build.source_frozen_at,
            graph_frozen_at=build.graph_frozen_at,
            output_frozen_at=build.output_frozen_at,
            root_count=build.selected_family_count,
            family_count=build.selected_family_count,
            generation_family_count=build.generation_family_count,
            family_child_count=int(expected_children),
            winner_count=build.winner_count,
            profile_count=len(request.definition.selection_profiles),
            root_scalar_count=scalar_counts[0],
            child_scalar_count=scalar_counts[1],
        )


async def load_block_graph(
    session_factory, request, registry, build_id, binding, *, limits, outcomes, compact_base=None
):
    """Consume one complete SOURCE plan and close its native output generation."""
    after_root_id = root_scalars = child_scalars = 0
    async with build_graph._session(session_factory) as reader:
        while roots := await reader.run_sync(
            lambda sync: read_block_roots(
                sync,
                request,
                build_id,
                binding,
                after_root_id=after_root_id,
                has_compact_base=compact_base is not None,
            )
        ):
            root_count, child_count = await _copy_source_page(
                session_factory, reader, request, registry, build_id, binding, roots, limits
            )
            root_scalars += root_count
            child_scalars += child_count
            after_root_id = roots[-1].plan.root_record_id
    if compact_base is not None:
        root_count, child_count = await copy_retained_families(
            session_factory, request, registry, build_id, binding, compact_base, limits
        )
        root_scalars += root_count
        child_scalars += child_count
    capture_bundle_id = await _close_graph(
        session_factory, request, build_id, binding, has_compact_base=compact_base is not None
    )
    generation_id = await build_graph._open_output(session_factory, request, build_id, capture_bundle_id)
    await _attach_block_members(session_factory, request, build_id, binding, generation_id)
    await write_block_winners(session_factory, request, build_id, binding)
    await write_block_projections(session_factory, request, build_id, binding, generation_id)
    proof = await _close_block_output(
        session_factory, request, build_id, binding, outcomes, (root_scalars, child_scalars)
    )
    return generation_id, proof
