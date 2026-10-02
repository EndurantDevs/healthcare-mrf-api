# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded page inserts preserve outcomes, source order, and rollback behavior."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace

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
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source_postgres import _child, _retained_request, _root, _source_case


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
async def test_source_page_client_statements_are_batched(record_count):
    async with _source_case() as case:
        context, page = await _root_page(case, [_root(_synthetic_npi(index)) for index in range(record_count)])
        statements = []

        def count_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement)

        event.listen(case.engine.sync_engine, "before_cursor_execute", count_statement)
        try:
            await staging._store_page(case.sessions, context, page)
        finally:
            event.remove(case.engine.sync_engine, "before_cursor_execute", count_statement)
        inserts = Counter(
            statement.split("(", 1)[0].split(".")[-1].strip().strip('"')
            for statement in statements
            if statement.startswith("INSERT INTO ")
        )
        root_lookups = sum(
            statement.startswith("SELECT ") and "custom_import_root_record" in statement for statement in statements
        )
        print(
            json.dumps(
                dict(
                    rows=record_count,
                    client_statements=len(statements),
                    inserts=dict(inserts),
                    root_lookups=root_lookups,
                )
            )
        )
        async with case.sessions() as session:
            occurrences = (
                await session.scalars(
                    select(CustomImportBuildOccurrence).order_by(CustomImportBuildOccurrence.occurrence_id)
                )
            ).all()
            assert [occurrence.source_ordinal for occurrence in occurrences] == list(range(record_count))
            assert [occurrence.part_row_ordinal for occurrence in occurrences] == list(range(record_count))
        assert root_lookups == 1
        assert inserts == {
            "custom_import_pack": 1,
            "custom_import_root_record": 1,
            "custom_import_root_revision": 1,
            "custom_import_build_occurrence": 1,
        }


async def test_mixed_outcomes_keep_rejection_ordinals_and_retry_prefix():
    records = [_root(), _root(score=None), _root(_synthetic_npi(1)), _root(npi=None), _root(score=None)]
    async with _source_case() as case:
        context, page = await _root_page(case, records)
        await staging._store_page(case.sessions, context, page)
        await staging._compare_committed_page(case.sessions, context, page)
        async with case.sessions() as session:
            occurrences = (
                await session.scalars(
                    select(CustomImportBuildOccurrence).order_by(CustomImportBuildOccurrence.occurrence_id)
                )
            ).all()
            rejections = (
                await session.scalars(select(CustomImportRejection).order_by(CustomImportRejection.rejection_ordinal))
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


async def test_late_occurrence_guard_failure_rolls_back_entire_page():
    async with _source_case() as case:
        context, page = await _root_page(case, [_root(), _root(score=None), _root(_synthetic_npi(1))])
        forged = replace(page.records[-1], raw_key=(page.records[-1].raw_key[0], bytes(32)))
        with pytest.raises(DBAPIError, match="custom_import_build_identity_mismatch"):
            await staging._store_page(case.sessions, context, replace(page, records=(*page.records[:-1], forged)))
        async with case.sessions() as session:
            for model in (
                CustomImportPack,
                CustomImportRootRecord,
                CustomImportRootRevision,
                CustomImportRejection,
                CustomImportBuildOccurrence,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            build = await session.get(CustomImportBuildAttempt, context.build_id)
            cursor = await session.get(CustomImportBuildStream, (context.build_id, context.stream_slot))
            assert build.source_occurrence_count == build.next_rejection_ordinal == 0
            assert cursor.next_source_ordinal == cursor.next_pack_ordinal == 0
        result = await staging.stage_segmented_source(case.sessions, context.request)
        assert result.phase == "graph" and result.source_occurrence_count == 3


@pytest.mark.parametrize("collision_location", ["same_page", "stored_root"])
async def test_root_digest_collision_compares_complete_canonical_key(collision_location):
    async with _source_case() as case:
        context, page = await _root_page(case, [_root(), _root(_synthetic_npi(1))])
        first, second = page.records
        forged = replace(second, typed_key=(second.typed_key[0], first.typed_key[1]))
        if collision_location == "stored_root":
            await staging._store_page(case.sessions, context, replace(page, records=(first,)))
            page = replace(page, first_row=1, first_source=1, records=(forged,))
        else:
            page = replace(page, records=(first, forged))
        with pytest.raises(CandidateRunnerError, match="digest collision"):
            await staging._store_page(case.sessions, context, page)
        async with case.sessions() as session:
            build = await session.get(CustomImportBuildAttempt, context.build_id)
            assert build.source_occurrence_count == int(collision_location == "stored_root")


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
            occurrences = (
                await session.scalars(
                    select(CustomImportBuildOccurrence).order_by(CustomImportBuildOccurrence.occurrence_id)
                )
            ).all()
            children = (
                await session.scalars(
                    select(CustomImportChildRevision).order_by(CustomImportChildRevision.source_ordinal)
                )
            ).all()
            rejection_code_by_id = {
                rejection.rejection_id: rejection.code
                for rejection in (await session.scalars(select(CustomImportRejection))).all()
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
