# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real capture/admission accounting over current candidate snapshots."""

from __future__ import annotations

import json
from collections import Counter
from copy import deepcopy
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event, text

from process.custom_import import build_counts, build_graph, build_source
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner import reject_duplicate_canonical_root_keys
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests import test_custom_import_build_output_postgres as output_fixture
from tests.custom_import_postgres_support import _migration
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_identical_children_postgres import _membership_definition, _membership_records
from tests.test_custom_import_segmented_capture_postgres import _POLICY
from tests.test_custom_import_snowflake_shared_capture import _shared_definition


def _root(npi="1003000126", score="1", enabled=True):
    return dict(npi=npi, score=score, enabled=enabled)


def _child(npi="1003000126", key="a", amount="2"):
    return dict(detail_npi=npi, detail_id=key, amount=amount)


async def _stage(case, definition, roots, children, *, page_rows=8):
    request = await output_fixture._request_for(
        case,
        {
            "root_source": [roots],
            "detail_source": [children[start : start + 100] for start in range(0, len(children), 100)] or [[]],
        },
        definition=definition,
        page_rows=page_rows,
    )
    staged = await build_source.stage_segmented_source(case.sessions, request)
    return request, staged


def _expected(definition, roots, children):
    eager = reject_duplicate_canonical_root_keys(
        definition, assemble_root_families(definition, roots, {"details": children})
    )
    return build_counts.SourceOutcomeCounts(len(eager.families), len(eager.rejections))


@pytest.mark.parametrize("child_first", [False, True])
async def test_native_mixed_errors_count_first_root_and_malformed_occurrences(monkeypatch, child_first):
    document = json.loads(_shared_definition().canonical)
    if child_first:
        document["streams"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    roots = [_root(), _root(score=None), _root("1234567893"), _root(npi=None), _root(npi=None)]
    child_records = [_child(amount="bad"), _child(key="b"), _child("absent", amount=None)]
    async with _source_case() as case:
        request, staged = await _stage(case, definition, roots, child_records)
        monkeypatch.setattr(build_counts, "MAX_BATCH_ROWS", 2)
        actual = await build_counts.count_source_outcomes(case.sessions, request, staged.build_id)
        assert actual == _expected(definition, roots, child_records) == build_counts.SourceOutcomeCounts(1, 4)


@pytest.mark.parametrize("invalid_second", [False, True])
async def test_native_distinct_raw_decimal_keys_share_a_typed_identity(monkeypatch, invalid_second):
    document = json.loads(_shared_definition().canonical)
    document["schema"]["root"]["logical_key"] = ["score"]
    document["schema"]["children"][0]["parent_key"] = [{"child": "amount", "root": "score"}]
    definition = CustomImportDefinition.from_mapping(document)
    roots = [_root(score="1"), _root(score="1.0", enabled=None if invalid_second else True)]
    async with _source_case() as case:
        request, staged = await _stage(case, definition, roots, [])
        monkeypatch.setattr(build_counts, "MAX_BATCH_ROWS", 2)
        actual = await build_counts.count_source_outcomes(case.sessions, request, staged.build_id)
        assert actual == _expected(definition, roots, []) == build_counts.SourceOutcomeCounts(0, 2)


@pytest.mark.parametrize("child_count,row_cap", [(1, 100_000), (1001, 100_000), (1001, 1024)])
async def test_native_accounting_queries_scale_by_physical_pages(monkeypatch, child_count, row_cap):
    policy_document = deepcopy(_POLICY)
    policy_document["stream_budget"].update(maximum_parts=16, maximum_records=2000)
    policy_document["bundle_budget"].update(maximum_parts=20, maximum_records=2200)
    policy = SegmentedCapturePolicy.from_mapping(policy_document)
    original = output_fixture._start_attempt

    async def start(case, **kwargs):
        return await original(case, policy=policy, **kwargs)

    monkeypatch.setattr(output_fixture, "_start_attempt", start)
    definition, roots = _shared_definition(), [_root()]
    child_records = [_child(key=f"child_{index}") for index in range(child_count)]
    async with _source_case() as case:
        request, staged = await _stage(case, definition, roots, child_records, page_rows=256)
        monkeypatch.setattr(build_counts, "MAX_BATCH_ROWS", row_cap)
        calls = Counter()

        def count_calls(_connection, _cursor, statement, _parameters, _context, _many):
            calls["statements"] += 1
            calls["aggregate"] += "source_outcome_evidence AS MATERIALIZED" in statement
            calls["metadata"] += statement.startswith("WITH source_outcome_metadata AS MATERIALIZED")

        event.listen(case.engine.sync_engine, "before_cursor_execute", count_calls)
        try:
            actual = await build_counts.count_source_outcomes(case.sessions, request, staged.build_id)
        finally:
            event.remove(case.engine.sync_engine, "before_cursor_execute", count_calls)
        assert actual == _expected(definition, roots, child_records) == build_counts.SourceOutcomeCounts(1, 0)
        page_count = (child_count + row_cap // 2) // (row_cap // 2)
        assert calls["aggregate"] == page_count and calls["metadata"] == 2 * (page_count + 1)
        assert calls["statements"] <= 20 + 24 * (page_count + 1)


@pytest.mark.parametrize("missing", [False, True])
async def test_native_membership_error_is_one_family_code(monkeypatch, missing):
    definition, source_records = _membership_definition(), _membership_records(missing=missing)
    async with _source_case() as case:
        request = await output_fixture._request_for(case, source_records, definition=definition)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        monkeypatch.setattr(build_counts, "MAX_BATCH_ROWS", 2)
        actual = await build_counts.count_source_outcomes(case.sessions, request, staged.build_id)
        assert actual == (build_counts.SourceOutcomeCounts(1, 1) if missing else build_counts.SourceOutcomeCounts(2, 0))


async def test_native_retained_copies_do_not_change_source_outcomes():
    async with _source_case() as case:
        first = await output_fixture._request_for(case, output_fixture._records(2))
        _, sealed = await output_fixture._complete(case, first)
        await output_fixture._activate(case, first, sealed.generation_id)
        request = await output_fixture._request_for(
            case, {"providers": [[]], "rates": [[]]}, seed=first, base=sealed.generation_id, version=1
        )
        staged = await build_source.stage_segmented_source(case.sessions, request)
        before = await build_counts.count_source_outcomes(case.sessions, request, staged.build_id)
        await build_graph.build_graph(case.sessions, request, staged.build_id)
        after = await build_counts.count_source_outcomes(case.sessions, request, staged.build_id)
        assert before == after == build_counts.SourceOutcomeCounts(0, 0)


_ADMISSION_INDEX_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20261010000000_custom_import_admission_indexes.py"
)


def _install_admission_indexes(connection, schema, *, downgrade=False):
    migration = _migration(_ADMISSION_INDEX_PATH, "admission_index_native")
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    (migration.downgrade if downgrade else migration.upgrade)()


async def _index_function_catalog(case):
    async with case.sessions() as session:
        return (
            (
                await session.execute(
                    text("""
                    SELECT to_jsonb(p)-'prosrc' metadata FROM pg_proc p
                    JOIN pg_namespace n ON n.oid=p.pronamespace
                    WHERE n.nspname=:schema AND p.proname IN (
                        'prepare_custom_import_snapshot_indexes','verify_custom_import_snapshot_indexes')
                    ORDER BY p.proname
                """),
                    {"schema": case.schema_name},
                )
            )
            .scalars()
            .all()
        )


@pytest.mark.parametrize("grant_worker", (False, True))
async def test_index_schedule_preserves_function_oids_and_acls_through_reversal(grant_worker):
    async with _source_case() as case:
        if grant_worker:
            async with case.engine.begin() as connection:
                await connection.execute(
                    text(
                        f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".'
                        "prepare_custom_import_snapshot_indexes(bigint,text) TO pg_monitor"
                    )
                )
        original = await _index_function_catalog(case)
        assert len(original) == 2
        if grant_worker:
            assert any(any(acl.startswith("pg_monitor=X/") for acl in row["proacl"] or ()) for row in original)
        for downgrade in (False, False, True, False):
            async with case.engine.begin() as connection:
                await connection.run_sync(_install_admission_indexes, case.schema_name, downgrade=downgrade)
            assert await _index_function_catalog(case) == original


@pytest.mark.parametrize(
    "name,grantee",
    (
        ("prepare_custom_import_snapshot_indexes", "PUBLIC"),
        ("verify_custom_import_snapshot_indexes", "pg_monitor"),
    ),
)
@pytest.mark.parametrize("downgrade", (False, True))
async def test_index_schedule_rejects_unexpected_grants_without_catalog_changes(name, grantee, downgrade):
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.execute(
                text(f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".{name}(bigint,text) TO {grantee}')
            )
        original = await _index_function_catalog(case)
        with pytest.raises(RuntimeError, match="custom_import_child_presence_identity_mismatch"):
            async with case.engine.begin() as connection:
                await connection.run_sync(_install_admission_indexes, case.schema_name, downgrade=downgrade)
        assert await _index_function_catalog(case) == original


async def test_candidate_admitted_before_upgrade_can_complete_after_upgrade():
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_admission_indexes, case.schema_name, downgrade=True)
        request = await output_fixture._request_for(
            case, _membership_records(), definition=_membership_definition(), page_rows=32
        )
        staged = await build_source.stage_segmented_source(case.sessions, request)
        assert staged.phase == "graph"
        async with case.sessions() as session:
            assert (
                await session.scalar(
                    text(f"""
                    SELECT count(*) FROM "{case.schema_name}".custom_import_snapshot_family f
                    JOIN pg_namespace n ON n.nspname='ci_snapshot_'||f.family_id::text
                    JOIN pg_class c ON c.relnamespace=n.oid AND c.relname='custom_import_build_graph_child_idx'
                    WHERE f.execution_id=:execution_id
                """),
                    {"execution_id": request.execution_id},
                )
                == 0
            )
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_admission_indexes, case.schema_name)
        _, completed = await output_fixture._complete(case, request)
        await output_fixture._assert_legacy_parity(case, request, completed)


@pytest.mark.parametrize("reverse", (False, True))
async def test_configured_membership_uses_early_slot_index_and_candidate_statistics(reverse):
    document = json.loads(_membership_definition(reverse=reverse).canonical)
    if reverse:
        document["schema"]["children"].reverse()
    definition_json = json.dumps(document).replace('"details"', '"visits"').replace('"other"', '"results"')
    definition = CustomImportDefinition.from_mapping(json.loads(definition_json))
    assert {collection.name for collection in definition.child_collections} == {"visits", "results"}
    records_by_stream = _membership_records(inner_count=25)
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_admission_indexes, case.schema_name)
        request = await output_fixture._request_for(case, records_by_stream, definition=definition, page_rows=32)
        staged = await build_source.stage_segmented_source(case.sessions, request)
        assert staged.candidate_error_count == 0
        async with case.sessions() as session:
            statistics = (
                await session.execute(
                    text(f"""
                        SELECT i.indisvalid,c.reltuples FROM "{case.schema_name}".custom_import_snapshot_family f
                        JOIN pg_namespace n ON n.nspname='ci_snapshot_'||f.family_id::text
                        JOIN pg_class idx ON idx.relnamespace=n.oid AND idx.relname='custom_import_build_graph_child_idx'
                        JOIN pg_index i ON i.indexrelid=idx.oid
                        JOIN pg_class c ON c.oid=i.indrelid
                        WHERE f.execution_id=:execution_id
                    """),
                    {"execution_id": request.execution_id},
                )
            ).one()
            assert statistics.indisvalid and statistics.reltuples == staged.source_occurrence_count
        _, completed = await output_fixture._complete(case, request)
        await output_fixture._assert_legacy_parity(case, request, completed)
