# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native proof for same-build processing after a committed partial family."""

import asyncio
import datetime as dt
import hashlib
from dataclasses import replace

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildFamily,
    CustomImportBuildStream,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
)
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as graph_sets
from process.custom_import import build_output as output
from process.custom_import import build_source as source
from process.custom_import import execution as lifecycle
from process.custom_import.build_continuation import authorize_build_continuation, start_build_continuation
from process.custom_import.continuation_runtime import continuation_epoch_binding_statement, producer_token_sha256
from process.custom_import.runner_types import LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_output_postgres import _activate, _complete, _records, _request_for
from tests.test_custom_import_build_source_postgres import _source_case


async def _partial_graph(case, request, monkeypatch):
    staged = await source.stage_segmented_source(case.sessions, request)
    original_receipts, original_heartbeat = graph_sets._child_receipts, graph._heartbeat
    committed_counts = []

    def observed(groups, receipts):
        updates = original_receipts(groups, receipts)
        committed_counts.append(sum(len(group.children) for group in groups))
        return updates

    async def interrupted(*arguments):
        await original_heartbeat(*arguments)
        if committed_counts:
            raise ConnectionError("synthetic committed child page acknowledgement")

    with monkeypatch.context() as interrupted_page:
        interrupted_page.setattr(graph_sets, "MAX_BATCH_ROWS", 2 * request.page_row_limit)
        interrupted_page.setattr(graph_sets, "_child_receipts", observed)
        interrupted_page.setattr(graph, "_heartbeat", interrupted)
        with pytest.raises(ConnectionError, match="committed child page"):
            await graph.build_graph(case.sessions, request, staged.build_id)
    assert committed_counts and 0 < committed_counts[0] < 14
    return staged.build_id


async def _retained_state(case, request, build_id):
    async with case.sessions() as session:
        snapshot = (
            await session.scalars(
                select(CustomImportSnapshotFamily).where(
                    CustomImportSnapshotFamily.execution_id == request.execution_id,
                    CustomImportSnapshotFamily.producing_fence == request.fence,
                )
            )
        ).one()
        plan = snapshot_models(snapshot.family_id)[CustomImportBuildFamily]
        families = (
            await session.scalars(select(plan).where(plan.build_id == build_id).order_by(plan.root_record_id))
        ).all()
        cursors = (
            await session.scalars(
                select(CustomImportBuildStream)
                .where(CustomImportBuildStream.build_id == build_id)
                .order_by(CustomImportBuildStream.stream_slot)
            )
        ).all()
        return (
            snapshot.family_id,
            tuple(
                (family.root_record_id, family.family_revision_id, family.attached_child_count, family.complete_at)
                for family in families
            ),
            tuple(
                (
                    cursor.stream_slot,
                    cursor.next_part_ordinal,
                    cursor.next_source_ordinal,
                    cursor.replay_verified_at,
                )
                for cursor in cursors
            ),
        )


async def _authorization(case, request, build_id, nonce):
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, request.execution_id)
        build = await session.get(CustomImportBuildAttempt, build_id)
        family_id = await session.scalar(
            select(CustomImportSnapshotFamily.family_id).where(
                CustomImportSnapshotFamily.execution_id == request.execution_id,
                CustomImportSnapshotFamily.producing_fence == request.fence,
            )
        )
        now = await session.scalar(select(func.clock_timestamp()))
        authorization_dict = {
            "contract": "custom-import/build-continuation/v1",
            "authorization_id": hashlib.sha256(b"synthetic-authorization").hexdigest(),
            "engine_execution_id": str(request.execution_id),
            "build_id": build_id,
            "snapshot_family_id": family_id,
            "capture_bundle_id": build.capture_bundle_id,
            "producer_fence": request.fence,
            "producer_token_sha256": producer_token_sha256(request).hex(),
            "request_identity_sha256": bytes(execution.request_identity_sha256).hex(),
            "expected_authority_id": None,
            "base_generation_id": build.base_generation_id,
            "base_pointer_version": build.base_pointer_version,
            "worker_identity_sha256": hashlib.sha256(b"synthetic-worker").hexdigest(),
            "predecessor_identity_sha256": hashlib.sha256(b"synthetic-predecessor").hexdigest(),
            "nonce_sha256": lifecycle.lease_token_sha256(nonce).hex(),
            "processing_deadline_removed": True,
            "launch_expires_at": (now + dt.timedelta(minutes=5)).isoformat(),
        }
        pins_dict = {
            name: getattr(execution, name)
            for name in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "source_binding_revision_id",
                "idempotency_key",
                "request_identity_sha256",
            )
        }
        return authorization_dict, pins_dict


async def _native_gate(case, request, build_id, mode):
    async with case.sessions() as session, session.begin():
        await source._set_timeout(session, 2_000)
        if mode != "missing":
            continuation_id = request.continuation.continuation_id + (mode == "wrong_epoch")
            nonce_sha256 = lifecycle.lease_token_sha256(request.lease_token)
            if mode == "old_nonce":
                nonce_sha256 = producer_token_sha256(request)
            await session.execute(continuation_epoch_binding_statement(continuation_id, nonce_sha256))
        return (
            await session.execute(
                text(f'SELECT * FROM "{case.schema_name}".lock_custom_import_build(:build_id)'),
                {"build_id": build_id},
            )
        ).one()


async def _transfer(case, request, build_id):
    async with case.sessions() as session:
        now = await session.scalar(select(func.clock_timestamp()))
    await asyncio.sleep(max(0, (request.build_deadline_at - now).total_seconds()) + 0.02)
    async with case.sessions() as session, session.begin():
        await session.execute(
            update(CustomImportLease)
            .where(CustomImportLease.execution_id == request.execution_id)
            .values(expires_at=func.clock_timestamp() - text("interval '1 second'"))
        )
    nonce = hashlib.sha256(b"synthetic-new-worker-nonce").digest()
    authorization, pins = await _authorization(case, request, build_id, nonce)
    async with case.sessions() as session, session.begin():
        receipt = await authorize_build_continuation(session, authorization, **pins)
    async with case.sessions() as session, session.begin():
        assert (
            await lifecycle.heartbeat_execution(
                session, execution_id=request.execution_id, fence=request.fence, token=request.lease_token
            )
            is None
        )
        with pytest.raises(lifecycle.IdempotencyConflict):
            await lifecycle.request_cancellation(session, execution_id=request.execution_id)
    async with case.sessions() as session, session.begin():
        continued = await start_build_continuation(session, authorization, nonce, lease_seconds=120)
    assert continued.continuation.continuation_id == receipt["continuation_id"]
    return continued


async def _assert_completed(case, original, continued, build_id, sealed, before):
    family_id, partial_families, source_cursors = before
    after = await _retained_state(case, continued, build_id)
    assert after[0] == family_id and after[2] == source_cursors
    after_family_by_root = {family[0]: family[1] for family in after[1]}
    assert all(after_family_by_root[family[0]] == family[1] for family in partial_families if family[1] is not None)
    assert all(family[3] is not None for family in after[1])
    async with case.sessions() as session:
        build = await session.get(CustomImportBuildAttempt, build_id)
        generation = await session.get(CustomImportGeneration, sealed.generation_id)
        seal = await session.get(CustomImportGenerationSeal, sealed.generation_id)
        assert build.build_deadline_at == original.build_deadline_at
        assert build.producing_fence == original.fence == generation.producing_fence == seal.sealing_fence
        assert bytes(build.producing_token_sha256) == producer_token_sha256(original)
        assert (
            bytes(generation.producing_token_sha256)
            == bytes(seal.sealing_token_sha256)
            == producer_token_sha256(original)
        )
        assert (await session.get(CustomImportExecution, original.execution_id)).state in {"completed", "no_change"}
    with pytest.raises(LeaseAuthorityLost):
        await output.build_output(case.sessions, continued, build_id)


@pytest.mark.parametrize("retained", [False, True])
async def test_partial_graph_continues_same_snapshot_through_finality(monkeypatch, retained):
    async with _source_case() as case:
        reference_request = await _request_for(case, _records(2, 13), page_rows=32)
        _, reference = await _complete(case, reference_request)
        if retained:
            await _activate(case, reference_request, reference.generation_id)
        records_by_stream = {"providers": [[]], "rates": [[]]} if retained else _records(2, 13)
        request = await _request_for(
            case,
            records_by_stream,
            seed=reference_request,
            base=reference.generation_id if retained else None,
            version=1 if retained else 0,
            page_rows=32,
        )
        async with case.sessions() as session:
            now = await session.scalar(select(func.clock_timestamp()))
        request = replace(request, build_deadline_at=now + dt.timedelta(seconds=15))
        build_id = await _partial_graph(case, request, monkeypatch)
        before = await _retained_state(case, request, build_id)
        assert any(family[2] > 0 and family[3] is None for family in before[1])
        continued = await _transfer(case, request, build_id)
        for mode in ("missing", "old_nonce", "wrong_epoch"):
            with pytest.raises(DBAPIError):
                await _native_gate(case, continued, build_id, mode)
        await _native_gate(case, continued, build_id, "current")
        with pytest.raises(LeaseAuthorityLost):
            async with source._page_session(case.sessions, replace(continued, lease_seconds=1), build_id):
                await asyncio.sleep(1.05)
        await graph.build_graph(case.sessions, continued, build_id)
        sealed = await output.build_output(case.sessions, continued, build_id)
        assert sealed.seal.effective_output_sha256 == reference.seal.effective_output_sha256
        assert (sealed.no_change is not None) == retained
        await _assert_completed(case, request, continued, build_id, sealed, before)
        if not retained:
            await _activate(case, continued, sealed.generation_id)
