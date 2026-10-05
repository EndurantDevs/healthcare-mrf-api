# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded page COPY preserves outcomes, source order, and rollback behavior."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import event, func, select
from sqlalchemy.exc import DBAPIError

import process.custom_import.build_source as staging
from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildOccurrence,
    CustomImportBuildStream,
    CustomImportChildRevision,
    CustomImportPack,
    CustomImportRejection,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from process.custom_import.family import _is_valid_npi
from tests.test_custom_import_build_source_postgres import (
    _candidate_models,
    _child,
    _retained_request,
    _root,
    _source_case,
)


def _synthetic_npi(index):
    prefix = f"199999{index:03d}"
    return next(prefix + str(digit) for digit in range(10) if _is_valid_npi(prefix + str(digit)))


async def _root_page(case, records):
    request = replace(
        await _retained_request(case, records_by_stream={"root_source": [records], "detail_source": [[]]}),
        page_row_limit=32,
    )
    build_id, registry = await staging._begin_build(case.sessions, request)
    stream = request.definition.source_streams[0]
    context = staging._StreamContext(request, registry, build_id, stream)
    page = staging._SourcePage(1, 0, 0, tuple(staging._prepare_row(request, stream, row) for row in records))
    return context, page


@pytest.mark.parametrize("record_count", [1, 32])
async def test_source_page_client_statements_are_batched(monkeypatch, record_count):
    async with _source_case() as case:
        context, page = await _root_page(case, [_root(_synthetic_npi(index)) for index in range(record_count)])
        statements = []
        copy_landing = AsyncMock(wraps=staging._copy_source_landing)
        monkeypatch.setattr(staging, "_copy_source_landing", copy_landing)

        def count_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement)

        event.listen(case.engine.sync_engine, "before_cursor_execute", count_statement)
        try:
            assert await staging._store_single_page(case.sessions, context, page) == record_count
        finally:
            event.remove(case.engine.sync_engine, "before_cursor_execute", count_statement)
        source_calls = Counter(
            name
            for statement in statements
            for name in ("source_bulk_authorize", "resolve_custom_import_source_batch_snapshot", "source_set_finalize")
            if f".{name}(" in statement
        )
        copy_landing.assert_awaited_once()
        assert len(copy_landing.await_args.args[1]) == record_count
        assert source_calls == {
            "source_bulk_authorize": 1,
            "resolve_custom_import_source_batch_snapshot": 1,
            "source_set_finalize": 1,
        }
        assert len(statements) <= 96
        for model in (CustomImportPack, CustomImportRootRecord, CustomImportRootRevision, CustomImportBuildOccurrence):
            assert not any(model.__tablename__ in statement for statement in statements)
        print(
            json.dumps(
                dict(
                    rows=record_count,
                    client_statements=len(statements),
                    source_calls=dict(source_calls),
                    copy_calls=copy_landing.await_count,
                )
            )
        )
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, context.request)
            occurrence_model = models[CustomImportBuildOccurrence]
            occurrences = (
                await session.scalars(select(occurrence_model).order_by(occurrence_model.occurrence_id))
            ).all()
            assert [occurrence.source_ordinal for occurrence in occurrences] == list(range(record_count))
            assert [occurrence.part_row_ordinal for occurrence in occurrences] == list(range(record_count))
            assert all(occurrence.rejection_id is None for occurrence in occurrences)
            for model, expected_count in (
                (CustomImportPack, 1),
                (CustomImportRootRecord, record_count),
                (CustomImportRootRevision, record_count),
            ):
                assert await session.scalar(select(func.count()).select_from(models[model])) == expected_count
            assert await session.scalar(select(func.count()).select_from(CustomImportPack)) == 0


async def test_mixed_outcomes_keep_rejection_ordinals_and_retry_prefix():
    records = [_root(), _root(score=None), _root(_synthetic_npi(1)), _root(npi=None), _root(score=None)]
    async with _source_case() as case:
        context, page = await _root_page(case, records)
        assert await staging._store_single_page(case.sessions, context, page) == len(records)
        await staging._compare_committed_page(case.sessions, context, page)
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, context.request)
            occurrence_model = models[CustomImportBuildOccurrence]
            rejection_model = models[CustomImportRejection]
            occurrences = (
                await session.scalars(select(occurrence_model).order_by(occurrence_model.occurrence_id))
            ).all()
            rejections = (
                await session.scalars(select(rejection_model).order_by(rejection_model.rejection_ordinal))
            ).all()
            assert [row.source_ordinal for row in occurrences] == list(range(5))
            assert [row.source_ordinal for row in rejections] == [1, 3, 4]
            assert [row.rejection_ordinal for row in rejections] == [0, 1, 2]
            assert [row.rejection_id for row in occurrences if row.rejection_id is not None] == [
                row.rejection_id for row in rejections
            ]
            assert occurrences[0].root_record_id == occurrences[1].root_record_id == occurrences[4].root_record_id
            assert occurrences[3].root_record_id is None
            build = await session.get(CustomImportBuildAttempt, context.build_id)
            assert build.source_occurrence_count == 5 and build.next_rejection_ordinal == 3
        result = await staging.stage_segmented_source(case.sessions, context.request)
        assert result.phase == "rejected" and result.source_occurrence_count == 5
        assert await staging.stage_segmented_source(case.sessions, context.request) == result


async def test_source_row_validation_failure_rolls_back_entire_page():
    async with _source_case() as case:
        context, page = await _root_page(case, [_root(), _root(score=None), _root(_synthetic_npi(1))])
        forged = replace(page.records[-1], raw_key=(page.records[-1].raw_key[0], bytes(32)))
        with pytest.raises(DBAPIError, match="source_bulk_row_mismatch"):
            await staging._store_single_page(
                case.sessions, context, replace(page, records=(*page.records[:-1], forged))
            )
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, context.request)
            for model in (
                CustomImportPack,
                CustomImportRootRecord,
                CustomImportRootRevision,
                CustomImportRejection,
                CustomImportBuildOccurrence,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
                assert await session.scalar(select(func.count()).select_from(models[model])) == 0
            build = await session.get(CustomImportBuildAttempt, context.build_id)
            cursor = await session.get(CustomImportBuildStream, (context.build_id, context.stream_slot))
            assert build.source_occurrence_count == build.next_rejection_ordinal == 0
            assert cursor.next_source_ordinal == cursor.next_pack_ordinal == 0
        result = await staging.stage_segmented_source(case.sessions, context.request)
        assert result.phase == "graph" and result.source_occurrence_count == 3


@pytest.mark.parametrize("collision_location", ["same_page", "stored_root"])
async def test_forged_root_digest_preserves_keys_and_prefix(collision_location):
    async with _source_case() as case:
        context, page = await _root_page(case, [_root(), _root(_synthetic_npi(1))])
        first, second = page.records
        forged = replace(second, typed_key=(second.typed_key[0], first.typed_key[1]))
        if collision_location == "stored_root":
            assert await staging._store_single_page(case.sessions, context, replace(page, records=(first,))) == 1
            page = replace(page, first_row=1, first_source=1, records=(forged,))
        else:
            page = replace(page, records=(first, forged))
        with pytest.raises(DBAPIError, match="source_bulk_row_mismatch"):
            await staging._store_single_page(case.sessions, context, page)
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, context.request)
            expected_records = [first] if collision_location == "stored_root" else []
            root_records = (await session.scalars(select(models[CustomImportRootRecord]))).all()
            roots = (await session.scalars(select(models[CustomImportRootRevision]))).all()
            occurrences = (await session.scalars(select(models[CustomImportBuildOccurrence]))).all()
            assert [(root.canonical_logical_key, bytes(root.logical_key_sha256)) for root in root_records] == [
                record.typed_key for record in expected_records
            ]
            assert [(root.canonical_payload, bytes(root.payload_sha256)) for root in roots] == [
                (record.payload, record.payload_hash) for record in expected_records
            ]
            assert [occurrence.source_ordinal for occurrence in occurrences] == list(range(len(expected_records)))
            build = await session.get(CustomImportBuildAttempt, context.build_id)
            cursor = await session.get(CustomImportBuildStream, (context.build_id, context.stream_slot))
            assert build.source_occurrence_count == len(expected_records)
            assert cursor.next_source_ordinal == cursor.next_pack_ordinal == len(expected_records)


async def test_batched_child_revisions_preserve_source_and_global_admission():
    records_by_stream = {
        "root_source": [[_root()]],
        "detail_source": [[_child(key="a"), _child(key="b"), _child(key="c", amount=None), _child(key="b")]],
    }
    async with _source_case() as case:
        request = replace(await _retained_request(case, records_by_stream=records_by_stream), page_row_limit=32)
        outcome = await staging.stage_segmented_source(case.sessions, request)
        assert outcome.phase == "graph" and outcome.source_occurrence_count == 5
        async with case.sessions() as session:
            models = await session.run_sync(_candidate_models, request)
            occurrence_model = models[CustomImportBuildOccurrence]
            child_model = models[CustomImportChildRevision]
            occurrences = (
                await session.scalars(select(occurrence_model).order_by(occurrence_model.occurrence_id))
            ).all()
            children = (await session.scalars(select(child_model).order_by(child_model.source_ordinal))).all()
            rejection_code_by_id = {
                rejection.rejection_id: rejection.code
                for rejection in (await session.scalars(select(models[CustomImportRejection]))).all()
            }
            assert [child.source_ordinal for child in children] == [0, 1, 3]
            assert [occurrence.source_ordinal for occurrence in occurrences[1:]] == list(range(4))
            assert [
                None
                if occurrence.resolved_rejection_id is None
                else rejection_code_by_id[occurrence.resolved_rejection_id]
                for occurrence in occurrences[1:]
            ] == [None, "duplicate_child_key", "required_field_null", "duplicate_child_key"]
        assert await staging.stage_segmented_source(case.sessions, request) == outcome
