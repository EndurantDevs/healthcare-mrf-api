# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native generic SOURCE begin, isolated COPY, exact retry and owner refusals."""

from __future__ import annotations

import gzip
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildOccurrence,
    CustomImportBuildStream,
    CustomImportCaptureBundle,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPack,
    CustomImportRootRevision,
)
from db.models.custom_import_prepared_source import CustomImportPreparedSource, CustomImportPreparedSourcePart
from process.custom_import import build_source as staging
from process.custom_import import source_batch
from process.custom_import.execution import resume_execution
from process.custom_import.prepared_source_build import build_request
from process.custom_import.prepared_source_context import load_prepared_source_context
from process.custom_import.prepared_source_store import begin_prepared_source
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.custom_import_postgres_support import isolated_publication_case
from tests.custom_import_prepared_source_store_postgres_cases import _context, _material, _run_driver
from tests.test_custom_import_build_source_postgres import _candidate_models
from tests.test_custom_import_raw_capture_postgres import _sealed_originals


async def _ready(case, *, part_rows=1, page_rows=1, with_child=False):
    material = _material()
    if with_child:
        material.captures = _sealed_originals(
            material.definition,
            {
                "providers": material.captures[0].payload,
                "rates": gzip.compress(b'{"Provider ID":"1234567893","Service Code":"A100","Amount":12.50}\n', mtime=0),
            },
        )
    policy = material.binding.processing_policy
    material.binding = replace(
        material.binding,
        processing_policy=replace(
            policy,
            build=replace(policy.build, page_row_limit=page_rows),
            prepared=dict(policy.prepared) | {"part_row_limit": part_rows},
        ),
    )
    context = await _context(case, material)
    await _run_driver(case, context)
    return context, build_request(context)


async def _assert_retained_source(session, context, request, build_id):
    """Check candidate contents, completed cursors and original provenance after replay."""
    models = await session.run_sync(_candidate_models, request)
    occurrences = (
        await session.scalars(
            select(models[CustomImportBuildOccurrence]).order_by(models[CustomImportBuildOccurrence].source_ordinal)
        )
    ).all()
    assert [
        (retained_row.source_part_ordinal, retained_row.part_row_ordinal, retained_row.source_ordinal)
        for retained_row in occurrences
    ] == [
        (1, 0, 0),
        (2, 0, 1),
    ]
    roots = (
        await session.scalars(
            select(models[CustomImportRootRevision]).order_by(models[CustomImportRootRevision].source_ordinal)
        )
    ).all()
    assert len(roots) == 2
    assert [retained_row.canonical_payload for retained_row in roots] == [
        staging._prepare_row(
            request,
            request.definition.source_streams[0],
            {"npi": npi, "display_name": f"Synthetic Provider {label}"},
        ).payload
        for npi, label in (("1234567893", "A"), ("1234567802", "B"))
    ]
    packs = (await session.scalars(select(models[CustomImportPack]))).all()
    assert len(packs) == 2 and all(pack.capture_bundle_id == context.capture_bundle_id for pack in packs)
    cursors = (
        await session.scalars(
            select(CustomImportBuildStream)
            .where(CustomImportBuildStream.build_id == build_id)
            .order_by(CustomImportBuildStream.stream_slot)
        )
    ).all()
    assert [
        (
            retained_row.next_part_ordinal,
            retained_row.next_part_row_ordinal,
            retained_row.next_source_ordinal,
            retained_row.next_pack_ordinal,
        )
        for retained_row in cursors
    ] == [(3, 0, 2, 2), (2, 0, 0, 0)]
    assert all(retained_row.replay_verified_at is not None for retained_row in cursors)
    assert (await session.get(CustomImportCaptureBundle, context.capture_bundle_id)).payload_contract is None
    assert (await session.get(CustomImportPreparedSource, context.execution_id)).state == "sealed"
    assert await session.scalar(select(func.count()).select_from(CustomImportGeneration)) == 0


async def test_native_generic_begin_source_admission_and_exact_replay():
    async with isolated_publication_case() as case:
        context, request = await _ready(case)
        build_id, _ = await staging._begin_build(case.sessions, request)
        assert (await staging._begin_build(case.sessions, request))[0] == build_id
        staged_source = await staging.stage_segmented_source(case.sessions, request)
        assert (
            staged_source.build_id,
            staged_source.phase,
            staged_source.source_occurrence_count,
            staged_source.candidate_error_count,
        ) == (
            build_id,
            "graph",
            2,
            0,
        )
        assert await staging.stage_segmented_source(case.sessions, request) == staged_source
        async with case.sessions() as session:
            await _assert_retained_source(session, context, request, build_id)


async def test_native_partial_part_resume_rejects_stale_ack(monkeypatch):
    async with isolated_publication_case() as case:
        context, request = await _ready(case, part_rows=2)
        build_id, _ = await staging._begin_build(case.sessions, request)
        monkeypatch.setattr(source_batch, "MAX_BATCH_ROWS", 1)
        arguments_by_name = dict(
            execution_id=request.execution_id,
            build_id=build_id,
            fence=request.fence,
            stream_slot=1,
            lease_token=request.lease_token,
            source_permit=context.source_permit,
        )
        before = source_batch.SourceCursor(1, 0, 0, 0)
        first = await source_batch.serve_source_batch(case.sessions, expected_cursor=before, **arguments_by_name)
        assert first.after == source_batch.SourceCursor(1, 1, 1, 1)
        with pytest.raises(source_batch.SourceCursorConflict):
            await source_batch.serve_source_batch(case.sessions, expected_cursor=before, **arguments_by_name)
        second = await source_batch.serve_source_batch(case.sessions, expected_cursor=first.after, **arguments_by_name)
        assert second.after.next_source_ordinal == 2
        # A capped batch may need a zero-row request to establish true decoder EOF.
        if not second.stream_complete:
            second = await source_batch.serve_source_batch(
                case.sessions, expected_cursor=second.after, **arguments_by_name
            )
        assert second.stream_complete and second.after == source_batch.SourceCursor(2, 0, 2, 2)


async def test_native_begin_refuses_unsealed_and_changed_bounds_without_creating_build():
    async with isolated_publication_case() as case:
        context = await _context(case, _material())
        await begin_prepared_source(case.sessions, context)
        request = build_request(context)
        with pytest.raises(CandidateRunnerError):
            await staging._begin_build(case.sessions, request)
        await _run_driver(case, context)
        with pytest.raises(CandidateRunnerError):
            await staging._begin_build(case.sessions, replace(request, page_row_limit=1))
        async with case.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CustomImportBuildAttempt)) == 0


async def test_native_begin_owner_must_match_even_when_source_owners_match():
    async with isolated_publication_case() as case:
        context, request = await _ready(case)
        async with case.sessions() as session, session.begin():
            quoted = session.bind.dialect.identifier_preparer.quote_schema(case.schema_name)
            signature = f"{quoted}.begin_custom_import_build(bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)"
            await session.execute(text(f"ALTER FUNCTION {signature} OWNER TO pg_read_all_data"))
        try:
            with pytest.raises(CandidateRunnerError, match="owner_required"):
                await staging._begin_build(case.sessions, request)
            async with case.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(CustomImportBuildAttempt)) == 0
        finally:
            async with case.sessions() as session, session.begin():
                await session.execute(text(f"ALTER FUNCTION {signature} OWNER TO CURRENT_USER"))


async def test_native_generic_pipeline_seals_publishes_and_reads_real_source():
    from process.custom_import import build_graph, build_output, read_core
    from tests.test_custom_import_build_output_postgres import _activate
    from tests.test_custom_import_read_core_postgres import _service
    from tests.test_custom_import_staged_verification_postgres import _install as _install_staged
    from tests.test_custom_import_verification_arms_postgres import _install as _install_arms
    from tests.test_custom_import_verification_root_ranges_postgres import _install as _install_ranges

    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_staged, case.schema_name)
            await connection.run_sync(_install_arms, case.schema_name)
            await connection.run_sync(_install_ranges, case.schema_name, 16_384)
        context, request = await _ready(case, page_rows=8, with_child=True)
        staged_source = await staging.stage_segmented_source(case.sessions, request)
        assert staged_source.phase == "graph" and staged_source.source_occurrence_count == 3
        generation_id = await build_graph.build_graph(case.sessions, request, staged_source.build_id)
        sealed = await build_output.build_output(case.sessions, request, staged_source.build_id)
        assert sealed.generation_id == generation_id
        assert sealed.seal.family_count == 2 and sealed.seal.family_child_count == 1
        assert sealed.seal.winner_count == 1
        await _activate(case, request, generation_id)
        pinned_target = read_core.PinnedReadTarget(
            request.dataset_id, generation_id, request.definition_revision_id, request.schema_revision_id, "default"
        )
        async with case.sessions() as session:
            service = _service()
            authorization = read_core.ExtensionReadAuthorization("synthetic-prepared-read")
            page = await service.search(
                session,
                authorization=authorization,
                request=read_core.SearchRequest(target=pinned_target, page_size=10),
            )
            assert page.total == len(page.items) == 1
            selected = page.items[0]
            detail = await service.root_detail(
                session, authorization=authorization, target=pinned_target, winner=selected.winner
            )
        assert page.total == len(page.items) == 1
        assert {field.field_id: field.value for field in page.items[0].root_fields} == {
            "npi": "1234567893",
            "display_name": "Synthetic Provider A",
        }
        expected_child_by_field = {"service_code": "A100", "amount": Decimal("12.50")}
        assert {field.field_id: field.value for field in selected.context_fields} == expected_child_by_field
        assert all(field.state == "value" for field in selected.context_fields)
        assert detail.winner.family_revision_id == selected.winner.family_revision_id
        assert len(detail.children) == 1
        child = detail.children[0]
        assert child.collection == "rates" and child.child_revision_id == selected.context_child_revision_id
        assert {field.field_id: field.value for field in child.fields} == expected_child_by_field
        assert all(field.state == "value" for field in child.fields)


async def test_native_copy_rollback_and_false_eof_preserve_cursor(monkeypatch):
    async with isolated_publication_case() as case:
        context, request = await _ready(case)
        build_id, _ = await staging._begin_build(case.sessions, request)
        with pytest.raises(CandidateRunnerError, match="incomplete"):
            await staging._finish_part(case.sessions, request, build_id, 1, 1)
        copy = staging._copy_source_landing

        async def fail_after_copy(*args):
            await copy(*args)
            raise RuntimeError("synthetic post-COPY failure")

        monkeypatch.setattr(staging, "_copy_source_landing", fail_after_copy)
        with pytest.raises(RuntimeError, match="post-COPY"):
            await source_batch.serve_source_batch(
                case.sessions,
                execution_id=request.execution_id,
                build_id=build_id,
                fence=request.fence,
                stream_slot=1,
                expected_cursor=source_batch.SourceCursor(1, 0, 0, 0),
                lease_token=request.lease_token,
                source_permit=context.source_permit,
            )
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            assert await session.scalar(select(func.count()).select_from(models[CustomImportBuildOccurrence])) == 0
            cursor = await session.get(CustomImportBuildStream, (build_id, 1))
            assert source_batch.SourceCursor.from_model(cursor) == source_batch.SourceCursor(1, 0, 0, 0)
            assert (await session.get(CustomImportBuildAttempt, build_id)).source_occurrence_count == 0
        monkeypatch.setattr(staging, "_copy_source_landing", copy)
        assert (await staging.stage_segmented_source(case.sessions, request)).phase == "graph"


@pytest.mark.parametrize("corruption", ("part_owner", "part_position", "header_pin"))
async def test_native_prepared_corruption_refuses_source_before_progress(corruption):
    async with isolated_publication_case() as case:
        context, request = await _ready(case)
        build_id, _ = await staging._begin_build(case.sessions, request)
        async with case.sessions() as session, session.begin():
            if corruption == "header_pin":
                header = await session.get(CustomImportPreparedSource, context.execution_id)
                header.raw_manifest_sha256 = b"x" * 32
            else:
                part = await session.get(CustomImportPreparedSourcePart, (context.execution_id, 1, 1))
                if corruption == "part_owner":
                    part.dataset_id += 100
                else:
                    part.first_source_ordinal += 1
        with pytest.raises((CandidateRunnerError, ValueError)):
            await source_batch.serve_source_batch(
                case.sessions,
                execution_id=request.execution_id,
                build_id=build_id,
                fence=request.fence,
                stream_slot=1,
                expected_cursor=source_batch.SourceCursor(1, 0, 0, 0),
                lease_token=request.lease_token,
                source_permit=context.source_permit,
            )
        async with case.sessions() as session:
            assert (await session.get(CustomImportBuildAttempt, build_id)).source_occurrence_count == 0


async def test_native_new_fence_build_reuses_seal_without_resetting_deadline():
    async with isolated_publication_case() as case:
        context, request = await _ready(case)
        first_id, _ = await staging._begin_build(case.sessions, request)
        async with case.sessions() as session, session.begin():
            header = await session.get(CustomImportPreparedSource, context.execution_id)
            retained = header.prepared_sha256, header.producer_fence, header.version
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == context.execution_id)
                .values(expires_at=func.clock_timestamp() - text("interval '1 second'"))
            )
        token = f"synthetic-prepared-next-fence-{uuid4().hex}"
        async with case.sessions() as session, session.begin():
            lease = await resume_execution(session, execution_id=context.execution_id, token=token, lease_seconds=60)
        assert lease is not None and lease.fence > context.fence
        resumed = await load_prepared_source_context(
            case.sessions,
            execution_id=context.execution_id,
            fence=lease.fence,
            token=token,
            source_permit=context.source_permit,
        )
        resumed_request = build_request(resumed)
        assert resumed_request.build_deadline_at == request.build_deadline_at
        second_id, _ = await staging._begin_build(case.sessions, resumed_request)
        assert second_id != first_id
        with pytest.raises(LeaseAuthorityLost):
            await staging._begin_build(case.sessions, request)
        assert (await staging.stage_segmented_source(case.sessions, resumed_request)).phase == "graph"
        async with case.sessions() as session:
            header = await session.get(CustomImportPreparedSource, context.execution_id)
            assert (header.prepared_sha256, header.producer_fence, header.version) == retained
