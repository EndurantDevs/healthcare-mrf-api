# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Existing guarded build metadata for the trusted-owner shard writer."""

from contextlib import closing

from sqlalchemy import exists, func, insert, select

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildStream,
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportField,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportSelectionProfile,
)
from process.custom_import.build_source import _assert_build_identity, _flush_page, _page_session
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_store import _candidate, _execute, _relation_barrier
from process.custom_import.materialization import DefinitionIdentity, selection_profile_models
from process.custom_import.runner_registry import (
    has_profile_mismatch,
    load_current_pointer,
    load_registry,
    lock_execution,
)


async def _profiles(session, request, registry):
    identity = DefinitionIdentity(request.dataset_id, request.definition_revision_id, request.schema_revision_id)
    expected = selection_profile_models(
        request.definition, identity=identity, child_collection_slots=registry.child_collection_slots
    )
    actual = (
        (
            await _execute(
                session,
                select(CustomImportSelectionProfile)
                .where(
                    CustomImportSelectionProfile.dataset_id == request.dataset_id,
                    CustomImportSelectionProfile.definition_revision_id == request.definition_revision_id,
                    CustomImportSelectionProfile.schema_revision_id == request.schema_revision_id,
                )
                .order_by(CustomImportSelectionProfile.profile_slot),
            )
        )
        .scalars()
        .all()
    )
    if not actual:
        session.add_all(expected)
        await _flush_page(session)
    elif len(actual) != len(expected) or any(
        has_profile_mismatch(stored, planned) for stored, planned in zip(actual, expected, strict=True)
    ):
        raise FamilyBlockUnavailable("persisted selection profiles do not match the definition")


def _check_capture(request, registry, capture, streams, projected_count):
    scope = request.dataset_id, request.definition_revision_id, request.schema_revision_id
    if (
        capture is None
        or capture.capture_state != "sealed"
        or capture.payload_contract != "custom-import/parquet-parts/v2"
        or (capture.dataset_id, capture.definition_revision_id, capture.schema_revision_id) != scope
        or not 1 <= capture.stream_count <= 9
        or capture.stream_count != len(registry.stream_slots)
        or len(request.definition.child_collections) > 8
        or projected_count > 20
    ):
        raise FamilyBlockUnavailable("sealed shard capture or definition shape differs")
    if len(streams) != capture.stream_count or {stream.stream_slot for stream in streams} != set(
        registry.stream_slots.values()
    ):
        raise FamilyBlockUnavailable("sealed shard capture stream census differs")
    if any(
        stream.capture_state != "sealed"
        or stream.payload_contract != capture.payload_contract
        or (stream.dataset_id, stream.definition_revision_id, stream.schema_revision_id) != scope
        for stream in streams
    ):
        raise FamilyBlockUnavailable("sealed shard capture stream identity differs")


async def _capture(session, request, registry, capture_id):
    capture = (
        await _execute(
            session,
            select(CustomImportCaptureBundle)
            .where(CustomImportCaptureBundle.capture_bundle_id == capture_id)
            .execution_options(populate_existing=True),
        )
    ).scalar_one_or_none()
    streams = (
        (
            await _execute(
                session,
                select(CustomImportCapture)
                .where(CustomImportCapture.capture_bundle_id == capture_id)
                .order_by(CustomImportCapture.stream_slot)
                .limit(10)
                .execution_options(populate_existing=True),
            )
        )
        .scalars()
        .all()
    )
    projected_count = (
        await _execute(
            session,
            select(func.count())
            .select_from(CustomImportField)
            .where(
                CustomImportField.schema_revision_id == request.schema_revision_id,
                CustomImportField.projection_slot > 0,
            ),
        )
    ).scalar_one()
    _check_capture(request, registry, capture, streams, projected_count)


async def _fresh_base(session, request):
    pointer = await load_current_pointer(session, request.dataset_id)
    actual = (None, 0) if pointer is None else (pointer.generation_id, pointer.version)
    if actual != (request.expected_base_generation_id, request.expected_pointer_version):
        raise FamilyBlockUnavailable("shard build base pointer changed")
    if (
        pointer is not None
        and not (
            await _execute(
                session,
                select(exists().where(CustomImportGenerationSeal.generation_id == pointer.generation_id)),
            )
        ).scalar_one()
    ):
        raise FamilyBlockUnavailable("shard build base is not sealed")


async def _empty_candidate(session, request, binding):
    # No earlier provisional COPY may become accepted build input retroactively.
    occupied_checks = [exists().select_from(table) for table in binding.tables_by_role.values()]
    occupied_checks.append(
        exists().where(
            CustomImportGeneration.execution_id == request.execution_id,
            CustomImportGeneration.producing_fence == request.fence,
        )
    )
    if any((await _execute(session, select(*occupied_checks))).one()):
        raise FamilyBlockUnavailable("shard build has earlier candidate data")


def _new_build(request, execution):
    return CustomImportBuildAttempt(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=execution.capture_bundle_id,
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
        request_identity_sha256=execution.request_identity_sha256,
        base_generation_id=request.expected_base_generation_id,
        base_pointer_version=request.expected_pointer_version,
        refresh_mode=request.definition.refresh_mode,
        complete_scope=request.complete_scope,
        page_row_limit=request.page_row_limit,
        page_byte_limit=request.page_byte_limit,
        statement_timeout_ms=request.statement_timeout_ms,
        build_deadline_at=request.build_deadline_at,
    )


async def begin_shard_build(session_factory, request, family_id, layout):
    """Begin or reload guarded metadata without creating the legacy page tables.

    This records no completed phase or verification. The ordinary INSERTs retain
    the existing owner-only guards; hosted workers gain no write path.
    """
    build_id = request.continuation.build_id if request.continuation is not None else None
    async with _page_session(session_factory, request, build_id) as (session, _):
        descriptor, binding = await _candidate(
            session, request, family_id, layout, {"loading", "closed", "verified", "logged"}
        )
        execution = await lock_execution(session, request)
        registry = await load_registry(session, request)
        await _capture(session, request, registry, execution.capture_bundle_id)
        await _profiles(session, request, registry)
        build = (
            await _execute(
                session,
                select(CustomImportBuildAttempt)
                .where(
                    CustomImportBuildAttempt.execution_id == request.execution_id,
                    CustomImportBuildAttempt.producing_fence == request.fence,
                )
                .with_for_update()
                .execution_options(populate_existing=True),
            )
        ).scalar_one_or_none()
        if build is not None:
            _assert_build_identity(build, request, execution)
            return build.build_id, registry
        if request.continuation is not None:
            raise FamilyBlockUnavailable("retained shard build is missing")
        if descriptor.phase != "loading":
            raise FamilyBlockUnavailable("new shard build requires an open candidate")
        await _fresh_base(session, request)
        # Never adopt an in-flight provisional COPY invisible to this census.
        await _relation_barrier(session, binding.tables_by_role, loading=False, nowait=True)
        await _empty_candidate(session, request, binding)
        build = _new_build(request, execution)
        session.add(build)
        await _flush_page(session)
        await _execute(
            session,
            insert(CustomImportBuildStream).from_select(
                ["build_id", "stream_slot"],
                select(build.build_id, CustomImportCapture.stream_slot).where(
                    CustomImportCapture.capture_bundle_id == execution.capture_bundle_id
                ),
            ),
        )
        return build.build_id, registry


def _completion_scope(context, admission, common):
    from process.custom_import.family_shard_admission import AdmissionOutputDescriptor
    from process.custom_import.family_shard_common import SourceCommonContext, SourceCommonOutput
    from process.custom_import.family_shard_output import _pack_row

    if type(context) is not SourceCommonContext or type(common) is not SourceCommonOutput:
        raise FamilyBlockUnavailable("SOURCE completion requires its closed common output")
    source_plan = context.source.source
    if (
        type(admission) is not AdmissionOutputDescriptor
        or type(admission.complete_scope) is not bool
        or (
            admission.source_sha256,
            admission.allocation_sha256,
            admission.source_scope,
            admission.snapshot_family_id,
            admission.definition_sha256,
            admission.complete_scope,
        )
        != (
            source_plan.sha256,
            context.source.sha256,
            source_plan.scope,
            context.source.snapshot_family_id,
            context.request.definition.digest,
            context.request.complete_scope,
        )
    ):
        raise FamilyBlockUnavailable("SOURCE completion admission identity differs")
    scope_by_name = dict(source_plan.scope[0]) | {
        "family_id": context.source.snapshot_family_id,
        "source_plan_sha256": source_plan.sha256.hex(),
        "allocation_sha256": context.source.sha256.hex(),
        "execution_id": context.request.execution_id,
        "producing_fence": context.request.fence,
        "producing_token_sha256": producer_token_sha256(context.request).hex(),
        "complete_scope": context.request.complete_scope,
        "source_records": source_plan.counts[0],
        "source_initial_rejections": source_plan.counts[4],
        "next_admission_rejection_ordinal": source_plan.counts[4],
        "admission_events_pending": True,
    }
    proof_by_name = common.proof_inputs
    if any(
        key not in proof_by_name or _pack_row(proof_by_name[key]) != _pack_row(expected_value)
        for key, expected_value in scope_by_name.items()
    ):
        raise FamilyBlockUnavailable("SOURCE completion common identity differs")


def _common_receipt(context, common, receipt, limits, check):
    from process.custom_import.family_block_store import ShardOutputProof, _checked_receipt

    source = context.source.source
    expected = _checked_receipt(context.layout, receipt)
    if expected["source_record_count"] != 0 or expected["partition_sha256"] != bytes.fromhex(source.partition_sha256):
        raise FamilyBlockUnavailable("SOURCE common receipt scope differs")
    proof = ShardOutputProof(context.layout, expected["shard_id"], expected["partition_sha256"], 0)
    with closing(
        common.batches(maximum_rows=limits.maximum_batch_records, maximum_bytes=limits.maximum_batch_bytes)
    ) as batches:
        for batch in batches:
            if check is not None:
                check()
            for role, rows in batch.items():
                proof.add(role, rows)
    if proof.receipt() != expected:
        raise FamilyBlockUnavailable("SOURCE common receipt differs from complete output")
    return expected


def _source_completion(context, admission, common, receipt, limits, check):
    from process.custom_import.family_block_validate import check_source_coverage
    from process.custom_import.family_shard_admission import read_admission_occurrences
    from process.custom_import.family_shard_source_plan import read_source_parts

    _completion_scope(context, admission, common)
    expected = _common_receipt(context, common, receipt, limits, check)
    source = context.source.source
    counts_by_stream = {slot: [0, 0] for slot, _count in source.source_counts}

    def _parts():
        with closing(read_source_parts(source)) as parts:
            for slot, ordinal, count in parts:
                counts_by_stream[slot][0] = ordinal
                counts_by_stream[slot][1] += count
                yield slot, ordinal, count

    def _occurrences():
        with closing(read_admission_occurrences(admission, check=check)) as occurrences:
            for occurrence in occurrences:
                row = occurrence.record
                yield row.stream_slot, row.part_ordinal, row.row_ordinal, row.source_ordinal

    with closing(_parts()) as parts, closing(_occurrences()) as occurrences:
        if check_source_coverage(parts, occurrences, dict(source.source_counts)) != source.counts[0]:
            raise FamilyBlockUnavailable("SOURCE complete occurrence count differs")
    return expected, counts_by_stream


async def _source_capture_counts(session, context, counts_by_stream):
    source_plan, request = context.source.source, context.request
    capture_by_name = dict(source_plan.scope[0])
    bundle = (
        await _execute(
            session,
            select(CustomImportCaptureBundle)
            .where(CustomImportCaptureBundle.capture_bundle_id == capture_by_name["capture_bundle_id"])
            .execution_options(populate_existing=True),
        )
    ).scalar_one()
    actual_binding = None if bundle.source_binding_sha256 is None else bytes(bundle.source_binding_sha256).hex()
    if (
        bundle.capture_state != "sealed"
        or (bundle.dataset_id, bundle.definition_revision_id, bundle.schema_revision_id)
        != (request.dataset_id, request.definition_revision_id, request.schema_revision_id)
        or bytes(bundle.manifest_sha256).hex() != capture_by_name["manifest_sha256"]
        or bundle.source_binding_revision_id != capture_by_name["source_binding_revision_id"]
        or actual_binding != capture_by_name["source_binding_sha256"]
    ):
        raise FamilyBlockUnavailable("SOURCE completion sealed capture differs")
    captures = (
        await _execute(
            session,
            select(
                CustomImportCapture.stream_slot,
                CustomImportCapture.payload_part_count,
                CustomImportCapture.committed_record_count,
            )
            .where(CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id)
            .order_by(CustomImportCapture.stream_slot),
        )
    ).all()
    if {slot: [parts, record_count] for slot, parts, record_count in captures} != counts_by_stream:
        raise FamilyBlockUnavailable("SOURCE completion capture stream totals differ")


async def _freeze_source_streams(session, build, context, counts_by_stream, now):
    streams = (
        (
            await _execute(
                session,
                select(CustomImportBuildStream)
                .where(CustomImportBuildStream.build_id == build.build_id)
                .order_by(CustomImportBuildStream.stream_slot)
                .execution_options(populate_existing=True),
            )
        )
        .scalars()
        .all()
    )
    pack_table = context.layout.tables(context.source.snapshot_family_id)["pack"]
    packs_by_slot = dict(
        (
            await _execute(session, select(pack_table.c.stream_slot, func.count()).group_by(pack_table.c.stream_slot))
        ).all()
    )
    if {stream.stream_slot for stream in streams} != set(counts_by_stream):
        raise FamilyBlockUnavailable("SOURCE completion build stream census differs")
    if sum(packs_by_slot.values()) != context.source.source.counts[1]:
        raise FamilyBlockUnavailable("SOURCE completion native pack count differs")
    for stream in streams:
        if (
            stream.next_part_ordinal,
            stream.next_part_row_ordinal,
            stream.next_source_ordinal,
            stream.next_pack_ordinal,
            stream.replay_verified_at,
        ) != (1, 0, 0, 0, None):
            raise FamilyBlockUnavailable("SOURCE aggregate counters were already changed")
        parts, record_count = counts_by_stream[stream.stream_slot]
        stream.next_part_ordinal, stream.next_source_ordinal = parts + 1, record_count
        stream.next_pack_ordinal = packs_by_slot.get(stream.stream_slot, 0)
        stream.replay_verified_at = now
    # Existing native stream guards require SOURCE throughout these updates.
    await _flush_page(session)


async def complete_shard_source(session_factory, context, build_id, admission, common, receipt, *, limits, check=None):
    """Freeze genuine complete SOURCE metadata; final structural acceptance is separate."""
    from process.custom_import.family_shard_common import _offload

    expected, counts_by_stream = await _offload(_source_completion, context, admission, common, receipt, limits, check)
    request, source_plan = context.request, context.source.source
    async with _page_session(session_factory, request, build_id) as (session, build):
        _, binding = await _candidate(session, request, context.source.snapshot_family_id, context.layout, {"loading"})
        table = binding.tables_by_role["shard_receipt"]
        actual = (
            (await _execute(session, select(table).where(table.c.shard_id == expected["shard_id"])))
            .mappings()
            .one_or_none()
        )
        if actual is None or {key: actual[key] for key in expected} != expected:
            raise FamilyBlockUnavailable("SOURCE completion common COPY is not committed")
        await _source_capture_counts(session, context, counts_by_stream)
        if build.phase == "admission":
            if (build.source_occurrence_count, build.next_rejection_ordinal) != (
                source_plan.counts[0],
                source_plan.counts[4],
            ):
                raise FamilyBlockUnavailable("SOURCE completion retry totals differ")
            return build.phase
        if build.phase != "source" or (
            build.source_occurrence_count,
            build.next_rejection_ordinal,
            build.candidate_error_count,
        ) != (0, 0, 0):
            raise FamilyBlockUnavailable("SOURCE aggregate phase or counters differ")
        now = (await _execute(session, select(func.clock_timestamp()))).scalar_one()
        await _freeze_source_streams(session, build, context, counts_by_stream, now)
        build.source_occurrence_count = source_plan.counts[0]
        build.next_rejection_ordinal = source_plan.counts[4]
        build.source_frozen_at, build.phase = now, "admission"
        return build.phase
