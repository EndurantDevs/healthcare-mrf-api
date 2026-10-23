# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only generic begin and EOF using existing build and snapshot guards."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy import func, select, text

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildStream,
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPack,
    CustomImportRejection,
)
from process.custom_import import build_source as staging
from process.custom_import.execution import lease_token_sha256
from process.custom_import.prepared_source_context import PreparedSourceContext, require_prepared_source_context
from process.custom_import.prepared_source_reader import sealed_header
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_schema


@dataclass(frozen=True, kw_only=True)
class PreparedSourceBuildRequest(staging.SourceBuildRequest):
    prepared_source: PreparedSourceContext = field(repr=False)


def build_request(context, *, expected_base_generation_id=None, expected_pointer_version=0, complete_scope=True):
    """Build the trusted native request before entering the existing begin seam."""
    if not isinstance(context, PreparedSourceContext):
        raise CandidateRunnerError("prepared_source_build_scope_invalid")
    bounds = context.policy.build.to_mapping()
    bounds.pop("build_deadline_seconds")
    return PreparedSourceBuildRequest(
        **{
            name: getattr(context, name)
            for name in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "lease_token",
                "fence",
                "definition",
                "build_deadline_at",
                "authorization_expires_at",
            )
        },
        expected_base_generation_id=expected_base_generation_id,
        expected_pointer_version=expected_pointer_version,
        complete_scope=complete_scope,
        prepared_source=context,
        **bounds,
    )


async def require_build_scope(session, request):
    """A typed value is not authority: recheck pins, owners and seal under page locks."""
    context = request.prepared_source
    expected = build_request(
        context,
        expected_base_generation_id=request.expected_base_generation_id,
        expected_pointer_version=request.expected_pointer_version,
        complete_scope=request.complete_scope,
    )
    if (
        request != expected
        or request.authorization_expires_at != context.authorization_expires_at
        or request.continuation is not None
    ):
        raise CandidateRunnerError("prepared_source_build_scope_invalid")
    await require_prepared_source_context(session, context)
    await staging._prepare_statement(session)
    connection = await session.connection()
    declared = CustomImportBuildAttempt.__table__.schema
    schema = (connection.sync_connection.get_execution_options().get("schema_translate_map") or {}).get(
        declared, declared
    )
    if not schema:
        raise CandidateRunnerError("prepared_source_build_owner_required")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    identity = f"{quoted}.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)"
    if (
        await session.scalar(
            text(
                "SELECT pg_catalog.pg_get_userbyid(proowner)=CURRENT_USER FROM pg_catalog.pg_proc WHERE oid=pg_catalog.to_regprocedure(:identity)"
            ),
            {"identity": identity},
        )
        is not True
    ):
        raise CandidateRunnerError("prepared_source_build_owner_required")
    await staging._prepare_statement(session)
    lease = await session.get(CustomImportLease, request.execution_id)
    if lease is None or lease.continuation_id is not None:
        raise CandidateRunnerError("prepared_source_build_scope_invalid")
    await staging._prepare_statement(session)
    return await sealed_header(session, context, lock=True)


async def _require_capture_streams(session, context, registry):
    """Require the complete sealed original stream set before opening a build."""
    await staging._prepare_statement(session)
    bundle = await session.get(CustomImportCaptureBundle, context.capture_bundle_id)
    await staging._prepare_statement(session)
    captures = (
        await session.execute(
            select(CustomImportCapture.stream_slot, CustomImportCapture.capture_state)
            .where(CustomImportCapture.capture_bundle_id == context.capture_bundle_id)
            .limit(10)
        )
    ).all()
    if (
        bundle.stream_count != len(registry.stream_slots)
        or len(captures) != bundle.stream_count
        or {capture.stream_slot for capture in captures} != set(registry.stream_slots.values())
        or any(capture.capture_state != "sealed" for capture in captures)
    ):
        raise CandidateRunnerError("custom_import_build_structure_mismatch")


async def _require_new_attempt(session, request):
    """Check pointer CAS, base seal and absence of prior current-fence material."""
    await staging._prepare_statement(session)
    pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
    if (None if pointer is None else pointer.generation_id, 0 if pointer is None else pointer.pointer_version) != (
        request.expected_base_generation_id,
        request.expected_pointer_version,
    ):
        raise CandidateRunnerError("custom_import_build_pointer_mismatch")
    if pointer is not None:
        await staging._prepare_statement(session)
        if await session.get(CustomImportGenerationSeal, pointer.generation_id) is None:
            raise CandidateRunnerError("custom_import_build_identity_mismatch")
    for model in (CustomImportPack, CustomImportGeneration, CustomImportRejection):
        await staging._prepare_statement(session)
        if (
            await session.scalar(
                select(model.execution_id)
                .where(model.execution_id == request.execution_id, model.producing_fence == request.fence)
                .limit(1)
            )
            is not None
        ):
            raise CandidateRunnerError("custom_import_build_retry_conflict")


async def begin_build(session, request, registry):
    """Preserve begin retry/CAS/clean-attempt checks without changing the old function."""
    context = request.prepared_source
    await require_build_scope(session, request)
    await staging._prepare_statement(session)
    execution = await session.get(CustomImportExecution, request.execution_id)
    await _require_capture_streams(session, context, registry)
    await staging._prepare_statement(session)
    build = await session.scalar(
        select(CustomImportBuildAttempt)
        .where(
            CustomImportBuildAttempt.execution_id == request.execution_id,
            CustomImportBuildAttempt.producing_fence == request.fence,
        )
        .with_for_update()
    )
    if build is not None:
        staging._assert_build_identity(build, request, execution)
        await staging._call(session, "lock_custom_import_build", (("bigint", build.build_id),))
        return build.build_id
    await _require_new_attempt(session, request)
    build = CustomImportBuildAttempt(
        **{
            name: getattr(request, name)
            for name in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "execution_id",
                "complete_scope",
                "page_row_limit",
                "page_byte_limit",
                "statement_timeout_ms",
                "build_deadline_at",
            )
        },
        capture_bundle_id=context.capture_bundle_id,
        producing_fence=request.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
        request_identity_sha256=context.request_identity_sha256,
        base_generation_id=request.expected_base_generation_id,
        base_pointer_version=request.expected_pointer_version,
        refresh_mode=request.definition.refresh_mode,
    )
    session.add(build)
    await staging._flush_page(session)
    session.add_all(
        CustomImportBuildStream(build_id=build.build_id, stream_slot=slot) for slot in registry.stream_slots.values()
    )
    await staging._flush_page(session)
    await staging._call(session, "lock_custom_import_build", (("bigint", build.build_id),))
    return build.build_id


async def finish_part(session, request, build, stream, ordinal):
    """Close one exact prepared part only after COPY closure and successful reader EOF."""
    header, progress = await require_build_scope(session, request)
    await staging._prepare_statement(session)
    await session.refresh(stream)
    family_id = await staging._resolve_build_snapshot(session, build.build_id)
    await staging._prepare_statement(session)
    connection = await session.connection()
    quote = connection.dialect.identifier_preparer.quote_schema
    candidate = quote(snapshot_schema(family_id))
    declared = CustomImportBuildAttempt.__table__.schema
    schema = (connection.sync_connection.get_execution_options().get("schema_translate_map") or {}).get(
        declared, declared
    )
    control = quote(schema)
    await session.execute(text(f"LOCK TABLE {candidate}.source_bulk_landing IN SHARE ROW EXCLUSIVE MODE"))
    await staging._prepare_statement(session)
    if await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {candidate}.source_bulk_landing) OR EXISTS(SELECT 1 FROM {control}.source_bulk_authorization WHERE build_id=:build AND accepting)"
        ),
        {"build": build.build_id},
    ):
        raise CandidateRunnerError("source_bulk_incomplete_batch")
    if build.phase != "source":
        raise CandidateRunnerError("custom_import_build_phase_mismatch")
    if ordinal < stream.next_part_ordinal:
        return stream.next_part_ordinal
    from db.models.custom_import_prepared_source import CustomImportPreparedSourcePart as Part

    await staging._prepare_statement(session)
    part = (
        await session.execute(
            select(Part.first_source_ordinal, Part.record_count).where(
                Part.execution_id == request.execution_id,
                Part.stream_slot == stream.stream_slot,
                Part.part_ordinal == ordinal,
                Part.dataset_id == request.dataset_id,
                Part.definition_revision_id == request.definition_revision_id,
                Part.schema_revision_id == request.schema_revision_id,
            )
        )
    ).one_or_none()
    total = progress[stream.stream_slot - 1]
    if (
        part is None
        or ordinal != stream.next_part_ordinal
        or ordinal > total.part_count
        or part.record_count != stream.next_part_row_ordinal
        or stream.next_source_ordinal != part.first_source_ordinal - 1 + part.record_count
    ):
        raise CandidateRunnerError("custom_import_build_incomplete")
    stream.next_part_ordinal, stream.next_part_row_ordinal = ordinal + 1, 0
    if ordinal == total.part_count:
        if stream.next_source_ordinal != total.record_count:
            raise CandidateRunnerError("custom_import_build_incomplete")
        await staging._prepare_statement(session)
        stream.replay_verified_at = await session.scalar(select(func.clock_timestamp()))
    await staging._flush_page(session)
    return stream.next_part_ordinal


async def stage_local_source(session_factory, request, build_id):
    """Use the same signed SOURCE operation for a trusted native generic build."""
    from process.custom_import import admission_sql, source_batch

    async with staging._page_session(session_factory, request, build_id) as (session, build):
        await require_build_scope(session, request)
        direct_admission = await admission_sql._has_admission_owner(session)
        phase = build.phase
    if phase != "source":
        return direct_admission
    for _, slot in request.prepared_source.stream_slots:
        while True:
            async with staging._page_session(session_factory, request, build_id) as (session, build):
                stream = await source_batch._cursor(session, build_id, slot)
                if build.phase != "source":
                    return direct_admission
                if stream.replay_verified_at is not None:
                    break
                cursor = source_batch.SourceCursor.from_model(stream)
            receipt = await source_batch.serve_source_batch(
                session_factory,
                execution_id=request.execution_id,
                build_id=build_id,
                fence=request.fence,
                stream_slot=slot,
                expected_cursor=cursor,
                lease_token=request.lease_token,
                source_permit=request.prepared_source.source_permit,
            )
            if receipt.phase != "source":
                return direct_admission
            if receipt.stream_complete:
                break
    async with source_batch._page(session_factory, request, build_id, request.prepared_source.source_permit) as (
        session,
        build,
    ):
        context = staging._StreamContext(request, None, build_id, None)
        await source_batch._freeze_if_verified(session, context, build)
    return direct_admission
