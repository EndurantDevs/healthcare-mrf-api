# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real capture/admission accounting over current candidate snapshots."""

from __future__ import annotations

import json
from collections import Counter
from copy import deepcopy

import pytest
from sqlalchemy import event

from process.custom_import import build_counts, build_graph, build_source
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner import reject_duplicate_canonical_root_keys
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests import test_custom_import_build_output_postgres as output_fixture
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
