# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Finish aggregate OUTPUT accounting; closed structural verification follows.

Original complete plans and winner spools supply expectations. Native row
counts do not replace the later independent role and semantic comparisons.
"""

from contextlib import closing
from dataclasses import dataclass

from sqlalchemy import func, select

from db.models.custom_import import CustomImportGeneration
from process.custom_import import materialization
from process.custom_import.build_source import _flush_page, _page_session, _prepare_statement
from process.custom_import.family_block_binding import FamilyBlockUnavailable, validate_family_block_indexes
from process.custom_import.family_block_store import _candidate, _execute
from process.custom_import.family_shard_common import _offload
from process.custom_import.family_shard_graph_control import _complete_plan, _generation_values, _source_digest
from process.custom_import.family_shard_winners import WinnerOutputDescriptor, read_winner_rows
from process.custom_import.query_projection_finality import load_generation_evidence


@dataclass(frozen=True)
class _OutputCounts:
    selected: int
    contexts: int
    winners: int


def _winner_counts(context, planned, winners, check):
    request = context.request
    if type(winners.generation) is not tuple or len(winners.generation) != 4:
        raise FamilyBlockUnavailable("output winner generation shape differs")
    expected_generation = (
        winners.generation[0],
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
    )
    expected_scopes = tuple(
        (slot, scope.collection_slot)
        for slot, scope in enumerate(
            materialization._profile_scopes(request.definition, context.layout.collection_slots), 1
        )
    )
    if (
        winners.layout_sha256 != context.layout.digest
        or winners.definition_sha256 != request.definition.digest
        or winners.generation != expected_generation
        or type(winners.generation[0]) is not int
        or not 0 < winners.generation[0] < 2**63
        or winners.profile_scopes != expected_scopes
        or winners.profile_slots != tuple(slot for slot, _scope in expected_scopes)
    ):
        raise FamilyBlockUnavailable("output winner scope or profile coverage differs")
    observed = 0
    with closing(read_winner_rows(winners, check=check)) as winner_rows:
        for _row in winner_rows:
            observed += 1
    contexts = sum(stream[2] for _slot, streams, _count in winners.profiles for stream in streams)
    if observed != winners.winner_count or not 0 <= observed <= contexts < 2**63:
        raise FamilyBlockUnavailable("output winner or original context count differs")
    return _OutputCounts(planned.selected_count, contexts, observed)


def _complete_inputs(context, final_plan, admission, winners, check):
    if type(winners) is not WinnerOutputDescriptor:
        raise FamilyBlockUnavailable("output requires the original completed winner spool")
    planned = _complete_plan(context, final_plan, admission, check)
    return planned, _winner_counts(context, planned, winners, check)


def _build_state(context, build, generation_id, counts):
    if (
        build.phase not in {"output", "verifying"}
        or build.generation_id != generation_id
        or build.capture_bundle_id != dict(context.source.source.scope[0])["capture_bundle_id"]
        or build.source_occurrence_count != context.source.source.counts[0]
        or build.candidate_error_count != 0
        or build.source_frozen_at is None
        or build.graph_frozen_at is None
        or build.verified_at is not None
        or build.plan_stage != "complete"
        or build.plan_complete_at is None
        or (build.selected_family_count, build.completed_family_count) != (counts.selected, counts.selected)
        or any(
            getattr(build, name) is not None
            for name in (
                "output_after_profile_slot",
                "output_after_entity_binding_id",
                "output_after_context_key_sha256",
            )
        )
    ):
        raise FamilyBlockUnavailable("output requires its exact completed graph and original SOURCE")
    expected = (counts.selected, counts.contexts, counts.winners)
    actual = (build.generation_family_count, build.candidate_context_count, build.winner_count)
    if build.phase == "output":
        if build.output_frozen_at is not None or actual != (0, 0, 0):
            raise FamilyBlockUnavailable("output has earlier partial accounting")
    elif build.output_frozen_at is None or actual != expected:
        raise FamilyBlockUnavailable("output retry frozen accounting differs")


async def _scoped_count(session, table, generation):
    predicate_items = tuple(
        table.c[name] == getattr(generation, name)
        for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id")
    )
    counts = (
        await _execute(session, select(func.count(), func.count().filter(*predicate_items)).select_from(table))
    ).one()
    if counts[0] != counts[1]:
        raise FamilyBlockUnavailable("output native generation scope differs")
    return counts[0]


async def _native_output(session, context, binding, build, planned, counts):
    generation = (
        await _execute(
            session,
            select(CustomImportGeneration)
            .where(CustomImportGeneration.generation_id == build.generation_id)
            .with_for_update()
            .execution_options(populate_existing=True),
        )
    ).scalar_one_or_none()
    values_by_name = _generation_values(context, build, planned, await _source_digest(session, context))
    if generation is None or any(getattr(generation, name) != value for name, value in values_by_name.items()):
        raise FamilyBlockUnavailable("output generation header differs from the original plan")
    family = binding.tables_by_role["family"]
    family_count = (await _execute(session, select(func.count()).select_from(family))).scalar_one()
    member_count = await _scoped_count(session, binding.tables_by_role["generation_family"], generation)
    winner_count = await _scoped_count(session, binding.tables_by_role["winner"], generation)
    if (family_count, member_count, winner_count) != (counts.selected, counts.selected, counts.winners):
        raise FamilyBlockUnavailable("output committed family, membership or winner census differs")
    await validate_family_block_indexes(session, binding, prepare=_prepare_statement)
    await _prepare_statement(session)
    await load_generation_evidence(
        session, generation, context.request.definition, frozen=False, family_block_binding=binding
    )


async def complete_shard_output(session_factory, context, build_id, final_plan, admission, winners, *, check=None):
    """Freeze exact aggregate counters; this records no structural acceptance.

    The candidate must already be closed and indexed. Full independent source,
    retained, decoded-family, role, home and projection proofs remain required.
    """
    if check is not None and not callable(check):
        raise ValueError("output cancellation check differs")
    planned, counts = await _offload(_complete_inputs, context, final_plan, admission, winners, check)
    async with _page_session(session_factory, context.request, build_id) as (session, build):
        _, binding = await _candidate(
            session, context.request, context.source.snapshot_family_id, context.layout, {"closed"}
        )
        _build_state(context, build, winners.generation[0], counts)
        await _native_output(session, context, binding, build, planned, counts)
        if build.phase == "output":
            build.generation_family_count = counts.selected
            build.candidate_context_count = counts.contexts
            build.winner_count = counts.winners
            build.output_frozen_at = (await _execute(session, select(func.clock_timestamp()))).scalar_one()
            build.phase = "verifying"
            await _flush_page(session)
        return build.generation_id
