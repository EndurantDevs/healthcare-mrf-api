# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native sealed-byte staging, durable retry, and cross-pack family admission."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, select, update

import process.custom_import.build_source as staging
from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildOccurrence,
    CustomImportBuildStream,
    CustomImportChildRevision,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPack,
    CustomImportRejection,
    CustomImportRootRevision,
)
from process.custom_import import execution as lifecycle
from process.custom_import.capture import capture_stream
from process.custom_import.capture_pending import seal_pending_parquet_bundle
from process.custom_import.definition_store import register_definition
from process.custom_import.runner_codec import digest_text, pack_hash
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests.custom_import_postgres_support import _migration, isolated_publication_case
from tests.test_custom_import_capture_pending_postgres import _receipt, _retain, _start_attempt
from tests.test_custom_import_snowflake_bundle import _definition

_FIRST_NPI = "1003000126"
_SECOND_NPI = "1234567893"


def _root(npi=_FIRST_NPI, score=7):
    return dict(npi=npi, score=score, enabled=True)


def _child(npi=_FIRST_NPI, key="a", amount="2.0"):
    return dict(detail_npi=npi, detail_id=key, amount=amount)


def _install_build_schema(connection, schema_name):
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261002010000_custom_import_bounded_build.py"
    migration = _migration(path, "custom_import_build_source_test_migration")
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


@asynccontextmanager
async def _source_case():
    async with isolated_publication_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_build_schema, case.schema_name)
        yield case


def _sealed_parts(pending_request, definition, records_by_stream):
    parts_by_stream = {}
    type_by_name = {"string": pa.string(), "integer": pa.int64(), "boolean": pa.bool_(), "decimal": pa.string()}
    for stream in definition.source_streams:
        schema = pa.schema(
            [
                (field.field_id, type_by_name[field.value_type])
                for field in definition.fields
                if field.collection == stream.child_collection
            ]
        )
        parts = []
        for records in records_by_stream[stream.stream_id]:
            table = pa.Table.from_pylist(records, schema=schema)
            output = BytesIO()
            pq.write_table(table, output, compression="NONE")
            sealed = capture_stream(
                BytesIO(output.getvalue()),
                stream,
                source_snapshot_token=pending_request.source_snapshot_token,
                limits=pending_request.policy.part_limits,
            )
            parts.append((sealed, table.num_rows, table.nbytes))
        parts_by_stream[stream.stream_id] = tuple(parts)
    return parts_by_stream


async def _retained_request(case, *, records_by_stream=None):
    definition = _definition()
    async with case.sessions() as session, session.begin():
        registered = await register_definition(session, "synthetic_source_staging", definition)
    pending, registration = await _start_attempt(case, seed=registered)
    records_by_stream = records_by_stream or {
        "root_source": [[_root(), _root(_SECOND_NPI)], []],
        "detail_source": [[_child()], [_child(_SECOND_NPI, "b")]],
    }
    parts = _sealed_parts(pending, definition, records_by_stream)
    await _retain(case, pending, registration, parts)
    async with case.sessions() as session, session.begin():
        await seal_pending_parquet_bundle(
            session,
            request=pending,
            capture_bundle_id=registration.capture_bundle_id,
            receipts=tuple(_receipt(pending, name, captures) for name, captures in parts.items()),
        )
        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    return staging.SourceBuildRequest(
        dataset_id=pending.dataset_id,
        definition_revision_id=pending.definition_revision_id,
        schema_revision_id=pending.schema_revision_id,
        execution_id=pending.execution_id,
        lease_token=pending.token,
        fence=pending.fence,
        definition=definition,
        expected_base_generation_id=None,
        expected_pointer_version=0,
        complete_scope=definition.refresh_mode == "snapshot",
        page_row_limit=1,
        page_byte_limit=16_384,
        statement_timeout_ms=2_000,
        build_deadline_at=now + dt.timedelta(seconds=180),
        lease_seconds=120,
    )


async def _assert_unpublished(session, request):
    execution = await session.get(CustomImportExecution, request.execution_id)
    assert execution.finished_at is None
    assert (await session.execute(select(func.count()).select_from(CustomImportGeneration))).scalar_one() == 0


async def _assert_graph_staged(case, request, result):
    assert result.phase == "graph" and result.source_occurrence_count == 4 and result.candidate_error_count == 0
    async with case.sessions() as session, session.begin():
        build = await session.get(CustomImportBuildAttempt, result.build_id)
        assert build.phase == "graph" and build.source_frozen_at is not None and build.generation_id is None
        cursors = (
            await session.scalars(
                select(CustomImportBuildStream).where(CustomImportBuildStream.build_id == result.build_id)
            )
        ).all()
        assert len(cursors) == 2
        assert all(cursor.next_part_ordinal == 3 and cursor.replay_verified_at is not None for cursor in cursors)
        assert all(cursor.next_source_ordinal == 2 and cursor.next_pack_ordinal == 2 for cursor in cursors)
        roots = (await session.scalars(select(CustomImportRootRevision))).all()
        children = (await session.scalars(select(CustomImportChildRevision))).all()
        assert len(roots) == len(children) == 2
        packs = (await session.scalars(select(CustomImportPack))).all()
        assert len(packs) == 4 and all(pack.record_count == 1 for pack in packs)
        for revision, label, domain in [(root, "root", "root-payload") for root in roots] + [
            (child, "details", "child-payload") for child in children
        ]:
            pack = next(pack for pack in packs if pack.pack_id == revision.pack_id)
            assert bytes(revision.payload_sha256) == digest_text(domain, revision.canonical_payload)
            assert bytes(pack.pack_sha256) == pack_hash(label, [bytes(revision.payload_sha256)])
        await _assert_unpublished(session, request)
        assert (await session.get(CustomImportExecution, request.execution_id)).state == "running"


async def test_multipart_pages_stage_without_generation_and_repeat_exactly():
    async with _source_case() as case:
        request = await _retained_request(case)
        result = await staging.stage_segmented_source(case.sessions, request)
        await _assert_graph_staged(case, request, result)
        assert await staging.stage_segmented_source(case.sessions, request) == result
        await _assert_graph_staged(case, request, result)


async def test_uncertain_page_commit_reloads_and_compares_before_append(monkeypatch):
    original = staging._store_page
    acknowledgement = SimpleNamespace(has_failed=False)

    async def lost_acknowledgement(*args, **kwargs):
        await original(*args, **kwargs)
        if not acknowledgement.has_failed:
            acknowledgement.has_failed = True
            raise ConnectionError("synthetic lost page commit acknowledgement")

    monkeypatch.setattr(staging, "_store_page", lost_acknowledgement)
    async with _source_case() as case:
        request = await _retained_request(case)
        with pytest.raises(ConnectionError, match="acknowledgement"):
            await staging.stage_segmented_source(case.sessions, request)
        async with case.sessions() as session, session.begin():
            build = (await session.scalars(select(CustomImportBuildAttempt))).one()
            assert build.phase == "source" and build.source_occurrence_count == 1
            assert build.source_frozen_at is None
            assert (await session.execute(select(func.count()).select_from(CustomImportPack))).scalar_one() == 1
        result = await staging.stage_segmented_source(case.sessions, request)
        await _assert_graph_staged(case, request, result)


async def test_partial_final_eof_retries_without_duplicate_rows(monkeypatch):
    original = staging._finish_part
    acknowledgement = SimpleNamespace(has_failed=False)

    async def lose_first_final_acknowledgement(factory, request, build_id, slot, ordinal):
        await original(factory, request, build_id, slot, ordinal)
        if slot == 1 and ordinal == 2 and not acknowledgement.has_failed:
            acknowledgement.has_failed = True
            raise ConnectionError("synthetic first-stream final acknowledgement lost")

    monkeypatch.setattr(staging, "_finish_part", lose_first_final_acknowledgement)
    async with _source_case() as case:
        request = await _retained_request(case)
        with pytest.raises(ConnectionError, match="first-stream final"):
            await staging.stage_segmented_source(case.sessions, request)
        async with case.sessions() as session, session.begin():
            build = (await session.scalars(select(CustomImportBuildAttempt))).one()
            cursors = (
                await session.scalars(select(CustomImportBuildStream).order_by(CustomImportBuildStream.stream_slot))
            ).all()
            assert build.phase == "source" and build.source_occurrence_count == 4 and build.source_frozen_at is None
            assert cursors[0].next_part_ordinal == 3 and cursors[0].replay_verified_at is not None
            assert cursors[1].next_part_ordinal == 2 and cursors[1].replay_verified_at is None
            assert (await session.execute(select(func.count()).select_from(CustomImportPack))).scalar_one() == 4
        outcome = await staging.stage_segmented_source(case.sessions, request)
        await _assert_graph_staged(case, request, outcome)


@pytest.mark.parametrize("authority_loss", ["cancellation", "new_fence"])
async def test_live_authority_loss_stops_after_last_committed_page(monkeypatch, authority_loss):
    original = staging._store_page
    async with _source_case() as case:
        request = await _retained_request(case)

        async def revoke_after_commit(*args, **kwargs):
            await original(*args, **kwargs)
            async with case.sessions() as session, session.begin():
                if authority_loss == "cancellation":
                    await lifecycle.request_cancellation(session, execution_id=request.execution_id)
                else:
                    await session.execute(
                        update(CustomImportLease)
                        .where(CustomImportLease.execution_id == request.execution_id)
                        .values(expires_at=func.clock_timestamp() - dt.timedelta(seconds=1))
                    )
                    grant = await lifecycle.resume_execution(
                        session, execution_id=request.execution_id, token=b"synthetic-new-owner"
                    )
                    assert grant.fence == request.fence + 1

        monkeypatch.setattr(staging, "_store_page", revoke_after_commit)
        with pytest.raises((CancellationRequested, LeaseAuthorityLost)):
            await staging.stage_segmented_source(case.sessions, request)
        async with case.sessions() as session, session.begin():
            build = (await session.scalars(select(CustomImportBuildAttempt))).one()
            assert build.phase == "source" and build.source_occurrence_count == 1 and build.source_frozen_at is None
            assert all(
                cursor.replay_verified_at is None
                for cursor in (await session.scalars(select(CustomImportBuildStream))).all()
            )
            await _assert_unpublished(session, request)


async def test_global_admission_handles_duplicates_invalid_presence_and_orphan_precedence():
    records_by_stream = {
        "root_source": [[_root(), _root(_SECOND_NPI, score=None)], [_root()]],
        "detail_source": [
            [_child(), _child(_SECOND_NPI, amount=None)],
            [_child(), _child("absent-parent", amount=None)],
        ],
    }
    async with _source_case() as case:
        request = await _retained_request(case, records_by_stream=records_by_stream)
        outcome = await staging.stage_segmented_source(case.sessions, request)
        assert (
            outcome.phase == "rejected" and outcome.source_occurrence_count == 7 and outcome.candidate_error_count == 1
        )
        async with case.sessions() as session, session.begin():
            occurrences = (
                await session.scalars(
                    select(CustomImportBuildOccurrence).order_by(CustomImportBuildOccurrence.occurrence_id)
                )
            ).all()
            rejections_by_id = {
                rejection.rejection_id: rejection
                for rejection in (await session.scalars(select(CustomImportRejection))).all()
            }
            codes = [rejections_by_id[occurrence.resolved_rejection_id].code for occurrence in occurrences]
            assert codes == [
                "duplicate_root_key",
                "required_field_null",
                "duplicate_root_key",
                "duplicate_child_key",
                "required_field_null",
                "duplicate_child_key",
                "orphan_child",
            ]
            invalid_root = occurrences[1]
            assert invalid_root.raw_parent_key_canonical is not None and invalid_root.root_record_id is not None
            orphan = occurrences[-1]
            assert rejections_by_id[orphan.rejection_id].code == "required_field_null"
            assert orphan.resolved_rejection_id != orphan.rejection_id
            await _assert_unpublished(session, request)


async def test_late_reader_close_failure_cannot_freeze_source(monkeypatch):
    original = staging.open_segmented_parquet_parts

    @asynccontextmanager
    async def failed_close(*args, **kwargs):
        async with original(*args, **kwargs) as parts:
            yield parts
        raise RuntimeError("synthetic late durable cursor close failure")

    monkeypatch.setattr(staging, "open_segmented_parquet_parts", failed_close)
    async with _source_case() as case:
        request = await _retained_request(case)
        with pytest.raises(RuntimeError, match="late durable cursor"):
            await staging.stage_segmented_source(case.sessions, request)
        async with case.sessions() as session, session.begin():
            build = (await session.scalars(select(CustomImportBuildAttempt))).one()
            assert build.phase == "source" and build.source_occurrence_count == 4 and build.source_frozen_at is None
            assert all(
                cursor.replay_verified_at is None
                for cursor in (await session.scalars(select(CustomImportBuildStream))).all()
            )
            await _assert_unpublished(session, request)
