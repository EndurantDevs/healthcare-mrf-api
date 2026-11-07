# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Persist exact SOURCE response outcomes independently of full input totals."""

from sqlalchemy import select

from db.models.custom_import import CustomImportBuildAttempt
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import family_block_store as store
from process.custom_import import publication
from process.custom_import.build_counts import SourceOutcomeCounts
from process.custom_import.build_graph import _session, _verify_request
from process.custom_import.build_source import _page_session, _set_timeout
from process.custom_import.continuation_runtime import producer_token_sha256, verify_terminal_continuation
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT, FamilyBlockLayout
from process.custom_import.family_block_records import _integer
from process.custom_import.family_shard_control import _completion_scope
from process.custom_import.runner_registry import load_registry


def _pair(counts):
    if type(counts) is not SourceOutcomeCounts:
        raise FamilyBlockUnavailable("SOURCE outcomes require the actual completed count result")
    pair = counts.accepted_family_count, counts.rejection_count
    for value in pair:
        _integer(value, "SOURCE outcome count", zero=True)
    return pair


def _read_pair(descriptor):
    pair = descriptor.source_accepted_family_count, descriptor.source_rejection_count
    if any(value is None for value in pair):
        raise FamilyBlockUnavailable("terminal SOURCE outcome counts are missing")
    counts = SourceOutcomeCounts(*pair)
    _pair(counts)
    return counts


async def bind_source_outcomes(factory, context, build_id, admission, common, receipt, counts, *, check=None):
    """Bind the real reducer result after confirmed SOURCE and before admission.

    All original producers remain live. This does not repeat semantic work or
    accept a worker count; only the trusted owner invokes it after SOURCE EOF.
    """
    expected_pair = _pair(counts)
    _completion_scope(context, admission, common)
    expected_receipt = store._checked_receipt(context.layout, receipt)
    if expected_receipt["source_record_count"] != 0 or expected_receipt["partition_sha256"] != bytes.fromhex(
        context.source.source.partition_sha256
    ):
        raise FamilyBlockUnavailable("SOURCE outcome common receipt scope differs")
    if check is not None:
        check()
    async with _page_session(factory, context.request, build_id) as (session, build):
        descriptor, binding = await store._candidate(
            session, context.request, context.source.snapshot_family_id, context.layout, {"loading"}
        )
        if (
            build.phase not in {"admission", "graph", "rejected"}
            or build.source_frozen_at is None
            or build.source_occurrence_count != context.source.source.counts[0]
            or build.capture_bundle_id != dict(context.source.source.scope[0])["capture_bundle_id"]
        ):
            raise FamilyBlockUnavailable("SOURCE outcome binding requires its exact frozen native source")
        table = binding.tables_by_role["shard_receipt"]
        committed = (
            (await store._execute(session, select(table).where(table.c.shard_id == expected_receipt["shard_id"])))
            .mappings()
            .one_or_none()
        )
        if committed is None or {name: committed[name] for name in expected_receipt} != expected_receipt:
            raise FamilyBlockUnavailable("SOURCE outcome binding common COPY differs")
        previous_pair = descriptor.source_accepted_family_count, descriptor.source_rejection_count
        if previous_pair == (None, None):
            descriptor.source_accepted_family_count, descriptor.source_rejection_count = expected_pair
        elif previous_pair != expected_pair:
            raise FamilyBlockUnavailable("SOURCE outcome binding retry counts differ")
        if check is not None:
            check()
    return counts


async def _terminal_authority(session, request, build_id):
    """Reuse terminal seal/no-change and continuation authority without a writer lease."""
    await _set_timeout(session, request.statement_timeout_ms)
    await publication._begin_finality_operation(session)
    await publication._locked_dataset(session, request.dataset_id)
    execution = await publication._locked_execution(
        session, execution_id=request.execution_id, dataset_id=request.dataset_id
    )
    lease = await publication._locked_lease(session, request.execution_id)
    build = await session.get(CustomImportBuildAttempt, build_id, with_for_update=True)
    _verify_request(build, request, execution)
    if build.phase != "verified" or build.generation_id is None or build.source_frozen_at is None:
        raise FamilyBlockUnavailable("SOURCE outcome replay requires its finalized native build")
    generation = await publication._locked_generation(
        session, dataset_id=request.dataset_id, generation_id=build.generation_id
    )
    seal = await publication._validated_generation_seal(session, generation)
    if request.continuation is not None or getattr(lease, "continuation_id", None) is not None:
        now = await publication._database_now(session)
        await verify_terminal_continuation(session, request, execution, lease, now, build=build)
    if execution.state == "no_change":
        no_change = publication._NoChangeRequest(
            request.dataset_id,
            request.execution_id,
            build.base_generation_id,
            build.base_pointer_version,
            build.generation_id,
            request.fence,
            producer_token_sha256(request),
        )
        if await publication._replayed_no_change_receipt(session, no_change, execution) is None:
            raise FamilyBlockUnavailable("SOURCE outcomes require the original no-change receipt")
    elif execution.state == "completed":
        publication._generation_seal_replay(
            seal,
            generation,
            publication._GenerationSealRequest(
                request.dataset_id, build.generation_id, request.fence, producer_token_sha256(request)
            ),
        )
    else:
        raise FamilyBlockUnavailable("SOURCE outcome producer is not terminal")
    return build


def _check_family(request, build, family, descriptor, layout):
    if (
        family.frozen_at is None
        or (
            family.dataset_id,
            family.definition_revision_id,
            family.schema_revision_id,
            family.execution_id,
            family.capture_bundle_id,
            family.generation_id,
            family.producing_fence,
            bytes(family.producing_token_sha256),
        )
        != (
            request.dataset_id,
            request.definition_revision_id,
            request.schema_revision_id,
            request.execution_id,
            build.capture_bundle_id,
            build.generation_id,
            request.fence,
            producer_token_sha256(request),
        )
        or descriptor.family_id != family.family_id
        or descriptor.phase != "logged"
        or descriptor.layout_contract != FAMILY_BLOCK_LAYOUT_CONTRACT
        or bytes(descriptor.definition_sha256) != bytes.fromhex(request.definition.digest)
        or bytes(descriptor.layout_sha256) != layout.digest
        or descriptor.collection_slots != dict(layout.collection_slots)
    ):
        raise FamilyBlockUnavailable("terminal SOURCE outcome candidate identity differs")


async def terminal_source_outcomes(factory, request, build_id):
    """Read the original execution's pair, including a no-change candidate.

    A publication may refer to the base generation. Its outcomes never replace
    those of the original candidate identified by this execution and fence.
    """
    async with _session(factory, transaction=True) as session:
        build = await _terminal_authority(session, request, build_id)
        registry = await load_registry(session, request)
        layout = FamilyBlockLayout(request.definition, registry.child_collection_slots)
        registered = (
            await session.execute(
                select(CustomImportSnapshotFamily, CustomImportFamilyBlockLayout)
                .join(
                    CustomImportFamilyBlockLayout,
                    CustomImportFamilyBlockLayout.family_id == CustomImportSnapshotFamily.family_id,
                )
                .where(
                    CustomImportSnapshotFamily.execution_id == request.execution_id,
                    CustomImportSnapshotFamily.producing_fence == request.fence,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).one_or_none()
        if registered is None:
            raise FamilyBlockUnavailable("terminal SOURCE outcome candidate is missing")
        family, descriptor = registered
        _check_family(request, build, family, descriptor, layout)
        return _read_pair(descriptor)
