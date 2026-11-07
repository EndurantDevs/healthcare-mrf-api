# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Open provisional output from complete owner plans and native family totals.

This does not freeze candidate writes, certify stored families, or seal a
generation. Complete membership, winner and closed-candidate proofs follow.
"""

from contextlib import closing
from dataclasses import dataclass

from sqlalchemy import func, select

from db.models.custom_import import CustomImportCaptureBundle, CustomImportGeneration
from process.custom_import.build_source import _call, _flush_page, _page_session, _prepare_statement
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_store import _candidate, _execute
from process.custom_import.family_shard_common import SourceCommonContext, _offload
from process.custom_import.family_shard_events import _scope as _admission_scope
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.family_shard_plan import FinalPlanDescriptor, read_family_plan
from process.custom_import.publication import _capture_source_bundle_digest
from process.custom_import.runner_codec import candidate_hash_ordered


@dataclass(frozen=True)
class _GraphPlan:
    candidate_sha256: bytes
    source_count: int
    retained_count: int

    @property
    def selected_count(self):
        """Count only families selected from SOURCE or the pinned base."""
        return self.source_count + self.retained_count


def _plan_scope(context, final_plan, admission):
    if type(context) is not SourceCommonContext or type(final_plan) is not FinalPlanDescriptor:
        raise FamilyBlockUnavailable("graph output requires complete owner plan inputs")
    request = context.request
    _admission_scope(request, context.layout, context.source, admission)
    expected = (
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        request.fence,
        producer_token_sha256(request),
        request.expected_base_generation_id,
        request.expected_pointer_version,
        request.definition.refresh_mode,
        request.complete_scope,
    )
    if (
        _pack_row(final_plan.request_scope) != _pack_row(expected)
        or _pack_row(admission.source_scope) != _pack_row(context.source.source.scope)
        or final_plan.admission_sha256 != admission.sha256
        or admission.counts_by_kind["candidate_errors"] != 0
    ):
        raise FamilyBlockUnavailable("graph output plan scope or admission differs")
    base_scope = final_plan.base_scope
    if request.expected_base_generation_id is None:
        if base_scope is not None:
            raise FamilyBlockUnavailable("graph output has unexpected base scope")
    elif (
        type(base_scope) is not tuple
        or len(base_scope) != 2
        or type(base_scope[0]) is not int
        or base_scope[0] != request.expected_base_generation_id
        or (base_scope[1] is not None and (type(base_scope[1]) is not int or not 0 < base_scope[1] < 2**63))
    ):
        raise FamilyBlockUnavailable("graph output requires the original pinned base scope")


def _selected_hashes(final_plan, counts_by_kind, check):
    previous = None
    with closing(read_family_plan(final_plan, check=check)) as plans:
        for plan in plans:
            digest = plan.typed_key[1]
            if previous is not None and digest == previous:
                raise FamilyBlockUnavailable("graph output root-key digest collision")
            previous = digest
            if plan.selection_kind != "absent":
                counts_by_kind[plan.selection_kind] += 1
                yield digest


def _complete_plan(context, final_plan, admission, check):
    _plan_scope(context, final_plan, admission)
    if check is not None:
        check()
    counts_by_kind = dict(source=0, retained=0)
    request = context.request
    with closing(_selected_hashes(final_plan, counts_by_kind, check)) as hashes:
        digest = candidate_hash_ordered(
            execution_id=request.execution_id,
            fence=request.fence,
            base_generation_id=request.expected_base_generation_id,
            root_key_hashes=hashes,
        )
    if any(final_plan.counts_by_kind[name] != count for name, count in counts_by_kind.items()):
        raise FamilyBlockUnavailable("graph output selected plan census differs")
    return _GraphPlan(digest, counts_by_kind["source"], counts_by_kind["retained"])


def _build_state(context, build, planned):
    capture_by_name = dict(context.source.source.scope[0])
    if (
        build.phase not in {"graph", "output"}
        or build.candidate_error_count != 0
        or build.source_frozen_at is None
        or build.source_occurrence_count != context.source.source.counts[0]
        or build.capture_bundle_id != capture_by_name["capture_bundle_id"]
        or build.output_frozen_at is not None
        or build.verified_at is not None
    ):
        raise FamilyBlockUnavailable("graph output requires completed original SOURCE and admission")
    if build.phase == "graph":
        is_unplanned = (
            build.plan_stage == "base"
            and build.plan_complete_at is None
            and (build.selected_family_count, build.completed_family_count) == (0, 0)
        )
        is_completed = (
            build.plan_stage == "complete"
            and build.plan_complete_at is not None
            and (build.selected_family_count, build.completed_family_count)
            == (planned.selected_count, planned.selected_count)
        )
        if build.generation_id is not None or build.graph_frozen_at is not None or not (is_unplanned or is_completed):
            raise FamilyBlockUnavailable("graph output has earlier partial plan metadata")
    elif (
        build.generation_id is None
        or build.graph_frozen_at is None
        or build.plan_stage != "complete"
        or build.plan_complete_at is None
        or (build.selected_family_count, build.completed_family_count)
        != (planned.selected_count, planned.selected_count)
    ):
        raise FamilyBlockUnavailable("graph output retry plan metadata differs")


async def _family_census(session, binding, planned):
    family = binding.tables_by_role["family"]
    actual = (
        await _execute(
            session,
            select(
                func.count(),
                func.count().filter(family.c.retained.is_(False)),
                func.count().filter(family.c.retained.is_(True)),
            ).select_from(family),
        )
    ).one()
    if tuple(actual) != (planned.selected_count, planned.source_count, planned.retained_count):
        raise FamilyBlockUnavailable("graph output native source/retained family census differs")


async def _source_digest(session, context):
    request = context.request
    expected_by_name = dict(context.source.source.scope[0])
    bundle = (
        await _execute(
            session,
            select(CustomImportCaptureBundle)
            .where(CustomImportCaptureBundle.capture_bundle_id == expected_by_name["capture_bundle_id"])
            .execution_options(populate_existing=True),
        )
    ).scalar_one_or_none()
    if (
        bundle is None
        or bytes(bundle.manifest_sha256).hex() != expected_by_name["manifest_sha256"]
        or bundle.source_binding_revision_id != expected_by_name["source_binding_revision_id"]
        or (None if bundle.source_binding_sha256 is None else bytes(bundle.source_binding_sha256).hex())
        != expected_by_name["source_binding_sha256"]
    ):
        raise FamilyBlockUnavailable("graph output original capture manifest differs")
    await _prepare_statement(session)
    return await _capture_source_bundle_digest(
        session,
        capture_bundle_id=expected_by_name["capture_bundle_id"],
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )


def _generation_values(context, build, planned, source_digest):
    request = context.request
    return dict(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=build.capture_bundle_id,
        base_generation_id=request.expected_base_generation_id,
        base_dataset_id=None if request.expected_base_generation_id is None else request.dataset_id,
        source_bundle_sha256=source_digest,
        candidate_sha256=planned.candidate_sha256,
        root_count=planned.selected_count,
        family_count=planned.selected_count,
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
    )


async def _open_generation(session, build, values_by_name):
    generation = (
        await _execute(
            session,
            select(CustomImportGeneration)
            .where(
                CustomImportGeneration.execution_id == values_by_name["execution_id"],
                CustomImportGeneration.producing_fence == values_by_name["producing_fence"],
            )
            .with_for_update()
            .execution_options(populate_existing=True),
        )
    ).scalar_one_or_none()
    if build.phase == "output":
        if (
            generation is None
            or generation.generation_id != build.generation_id
            or any(getattr(generation, name) != expected_value for name, expected_value in values_by_name.items())
        ):
            raise FamilyBlockUnavailable("graph output retry generation header differs")
        return generation.generation_id
    if generation is not None:
        raise FamilyBlockUnavailable("graph output has an earlier unbound generation")
    stamp = (await _execute(session, select(func.clock_timestamp()))).scalar_one()
    generation = CustomImportGeneration(**values_by_name)
    session.add(generation)
    await _flush_page(session)
    build.generation_id, build.graph_frozen_at, build.phase = generation.generation_id, stamp, "output"
    await _flush_page(session)
    return generation.generation_id


async def _commit_graph_plan(session_factory, context, build_id, planned):
    """Commit an authenticated GRAPH checkpoint before opening a generation."""
    async with _page_session(session_factory, context.request, build_id) as (session, build):
        _, binding = await _candidate(
            session, context.request, context.source.snapshot_family_id, context.layout, {"loading"}
        )
        _build_state(context, build, planned)
        await _family_census(session, binding, planned)
        if build.phase == "graph":
            generation = await _execute(
                session,
                select(CustomImportGeneration.generation_id)
                .where(
                    CustomImportGeneration.execution_id == context.request.execution_id,
                    CustomImportGeneration.producing_fence == context.request.fence,
                )
                .with_for_update(),
            )
            if generation.scalar_one_or_none() is not None:
                raise FamilyBlockUnavailable("graph output has an earlier unbound generation")
        source_digest = await _source_digest(session, context)
        if build.phase == "graph" and build.plan_stage == "base":
            build.plan_stage = "complete"
            build.plan_complete_at = (await _execute(session, select(func.clock_timestamp()))).scalar_one()
            build.selected_family_count = build.completed_family_count = planned.selected_count
            await _flush_page(session)
        return source_digest


async def open_shard_output(session_factory, context, build_id, final_plan, admission, *, check=None):
    """Bind one provisional generation; no closed validation or seal is claimed."""
    if check is not None and not callable(check):
        raise ValueError("graph output cancellation check differs")
    planned = await _offload(_complete_plan, context, final_plan, admission, check)
    source_digest = await _commit_graph_plan(session_factory, context, build_id, planned)
    if check is not None:
        check()
    async with _page_session(session_factory, context.request, build_id) as (session, build):
        _, binding = await _candidate(
            session, context.request, context.source.snapshot_family_id, context.layout, {"loading"}
        )
        _build_state(context, build, planned)
        if build.plan_stage != "complete" or build.plan_complete_at is None:
            raise FamilyBlockUnavailable("graph output committed plan checkpoint differs")
        await _family_census(session, binding, planned)
        values_by_name = _generation_values(context, build, planned, source_digest)
        generation_id = await _open_generation(session, build, values_by_name)
        family_id = (
            await _call(
                session,
                "bind_custom_import_snapshot_generation",
                (
                    ("bigint", context.request.execution_id),
                    ("bigint", context.request.fence),
                    ("bytea", producer_token_sha256(context.request)),
                    ("bigint", generation_id),
                ),
            )
        ).scalar_one()
        if type(family_id) is not int or family_id != context.source.snapshot_family_id:
            raise FamilyBlockUnavailable("graph output bound another snapshot family")
        return generation_id
