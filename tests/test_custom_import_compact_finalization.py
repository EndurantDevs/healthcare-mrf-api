# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compact frozen finalization keeps exact coverage separate from bounded audits."""

from __future__ import annotations

import datetime as dt
import json
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import build_output as output
from process.custom_import import compact_materialization as compact
from process.custom_import import publication
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_output import _candidate, _context_row, _generation, _group
from tests.test_custom_import_output_bulk_verification import _family, _projection_records


def _proof(**changes):
    """Represent database-derived complete counts, not worker-declared expectations."""
    values_by_name = dict(
        build_id=7,
        root_count=3,
        family_count=3,
        generation_family_count=3,
        family_child_count=5,
        winner_count=6,
        profile_count=2,
        root_scalar_count=5,
        child_scalar_count=8,
        source_frozen_at=dt.datetime(2030, 1, 1, tzinfo=dt.UTC),
        graph_frozen_at=dt.datetime(2030, 1, 2, tzinfo=dt.UTC),
        output_frozen_at=dt.datetime(2030, 1, 3, tzinfo=dt.UTC),
    )
    return SimpleNamespace(**(values_by_name | changes))


def _coverage(proof=None, *, contexts=((1, 3, 2), (2, 5, 4))):
    """Keep both root and child profiles independent of origin-specific scalar closure."""
    return compact._check_coverage(proof or _proof(), [(1, 5, 10)], [(1, 0), (2, 1)], contexts)


def _sql(statement):
    return str(statement.compile(dialect=postgresql.dialect()))


def test_exact_population_coverage_keeps_both_profile_scopes():
    assert _coverage() == {1: 2, 2: 4}


@pytest.mark.parametrize(
    "changes,contexts,reason",
    [
        ({}, ((1, 2, 2), (2, 5, 4)), "context"),
        ({}, ((1, 3, 2), (2, 4, 4)), "context"),
        ({"winner_count": 5}, ((1, 3, 2), (2, 5, 4)), "winner"),
        ({"family_child_count": 4}, ((1, 3, 2), (2, 5, 4)), "family"),
    ],
)
def test_exact_population_coverage_rejects_membership_omissions(changes, contexts, reason):
    with pytest.raises(CandidateRunnerError, match=reason):
        _coverage(_proof(**changes), contexts=contexts)


@pytest.mark.parametrize("child", [False, True])
def test_exact_scalar_sql_matches_present_identity_and_state(child):
    statement = compact._present_scalars_query(
        snapshot_models(41), _generation(_request()), child=child, revision_ids=(11, 99)
    )
    compilation = statement.compile(dialect=postgresql.dialect())
    compiled = str(compilation)
    assert compiled.count("jsonb_array_elements") == 1
    assert compiled.count(" AS MATERIALIZED") == 2
    assert " AS JSONB)" in compiled and "EXISTS (SELECT" in compiled
    assert compilation.params["coverage_revision_ids"] == (11, 99)
    assert "coverage_revision_ids" in compiled.split("decoded_fields AS MATERIALIZED", 1)[0]
    assert "FROM unnest(" in compiled and "admitted_revisions.revision_id" in compiled
    assert "custom_import_family_child" not in compiled and "custom_import_generation_family" not in compiled
    assert "custom_import_pack.execution_id =" in compiled
    assert "custom_import_pack.producing_fence =" in compiled
    assert "custom_import_pack.producing_token_sha256 =" in compiled
    assert "decoded_fields.field_name" in compiled and "decoded_fields.value_state" in compiled
    assert "ORDER BY" not in compiled
    assert "IS DISTINCT FROM" in compiled
    assert "LEFT OUTER JOIN" in compiled
    assert "field_slot =" in compiled and "value_state =" in compiled
    assert "FILTER (WHERE" in compiled
    assert "projection_slot >" in compiled and "projection_slot IS NOT NULL" not in compiled
    assert "\\u0000" in compilation.params.values() and "\\u0001" in compilation.params.values()
    assert len(statement.selected_columns) == 3
    assert not statement._order_by_clauses
    assert "string_value" not in compiled and "decimal_value" not in compiled
    assert "ci_snapshot_41" in compiled
    actual_sql = _sql(statement.selected_columns[2].element)
    assert "FROM unnest(" in actual_sql and "admitted_revisions.revision_id" in actual_sql
    assert "count(*)" in actual_sql and "WHERE" not in actual_sql
    assert "value_state" not in actual_sql and "field_slot" not in actual_sql
    assert "custom_import_field" not in actual_sql and "decoded_fields" not in actual_sql


@pytest.mark.parametrize("child", [False, True])
def test_coverage_metadata_keeps_native_keys_and_missing_lookup_visible(child):
    factory = compact._child_coverage_query if child else compact._root_coverage_query
    statement, keys, models = factory(snapshot_models(41), 7, _generation(_request()))
    compiled = _sql(statement)
    assert models == () and len(statement.selected_columns) == 6
    assert statement.selected_columns[4].name == "octet_length"
    assert "LEFT OUTER JOIN" in compiled and "GROUP BY" not in compiled and "DISTINCT" not in compiled
    assert not statement._order_by_clauses
    assert [key.key for key in keys] == (
        ["family_revision_id", "collection_slot", "child_revision_id"] if child else ["family_revision_id"]
    )
    assert "custom_import_generation_family.generation_id =" in compiled
    assert "custom_import_generation_family.definition_revision_id =" in compiled
    assert "custom_import_generation_family.schema_revision_id =" in compiled
    where_sql = compiled.rsplit("\nWHERE ", 1)[-1]
    assert "canonical_payload" not in where_sql
    assert "custom_import_family_revision.producing_fence" not in where_sql
    from_sql = compiled.split("\nFROM ", 1)[-1].split("\nWHERE ", 1)[0]
    assert ", ci_snapshot_41." not in from_sql
    assert "CASE WHEN" in compiled and "custom_import_build_family.selection_kind" in compiled
    assert "custom_import_build_family.selection_kind =" in where_sql
    assert "retained" in statement.compile(dialect=postgresql.dialect()).params.values()
    assert "custom_import_build_family.build_id =" in from_sql
    for name in ("root_record_id", "family_revision_id"):
        assert f"custom_import_build_family.{name} = ci_snapshot_41.custom_import_generation_family.{name}" in from_sql


def test_revision_work_batches_split_one_family_and_preserve_sparse_exact_ids():
    rows = [(8, 1, 4, 7, 30, True), (8, 1, 4, 99, 30, True), (8, 1, 4, 10_000, 30, True), (9, 2, 2, 9, 30, True)]
    population_by_slot = {}
    assert list(compact._revision_batches(_request(page_byte_limit=60), rows, population_by_slot)) == [
        (7, 99),
        (10_000, 9),
    ]
    assert population_by_slot == {1: (3, 4), 2: (1, 2)}
    assert list(compact._revision_batches(_request(), [], {})) == []
    repeated_rows = [(8, 1, 4, 7, 1, True), (9, 1, 4, 7, 1, True)]
    assert list(compact._revision_batches(_request(), repeated_rows, {})) == [(7, 7)]


def test_empty_coverage_keeps_declared_scopes_without_running_scalar_aggregates(monkeypatch):
    state = _coverage_reader(monkeypatch, [])
    proof = _proof(
        root_count=0,
        family_count=0,
        generation_family_count=0,
        family_child_count=0,
        root_scalar_count=0,
        child_scalar_count=0,
        winner_count=0,
    )
    assert compact._presence_coverage(None, _request(), 7, _generation(_request()), proof, child=False) == []
    children = compact._presence_coverage(None, _request(), 7, _generation(_request()), proof, child=True)
    assert compact._check_coverage(proof, children, [(1, 0), (2, 1)], []) == {1: 0, 2: 0}
    assert not state.is_closed and state.batches == []


def test_revision_work_admission_preserves_logical_maximum_without_transport_charge(monkeypatch):
    request = _request(page_byte_limit=output.MAX_BATCH_BYTES)
    assert list(compact._revision_batches(request, [(1, 0, 1, 11, request.page_byte_limit, True)], {})) == [(11,)]
    assert compact._coverage_read_budget().variable_columns == ()
    assert compact._coverage_read_budget().reserve_bytes > 0
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 4)
    rows = [(1, 1, 1, revision, 1, True) for revision in (7, 8, 9)]
    assert list(compact._revision_batches(request, rows, {})) == [(7,), (8,), (9,)]


def test_revision_work_caps_root_only_fanout_and_rejects_oversized_payload(monkeypatch):
    request = _request(page_byte_limit=60)
    document = json.loads(request.definition.canonical)
    document["streams"] = document["streams"][:1]
    document["schema"]["children"] = []
    document["aliases"].pop("rates")
    document["query"].pop("child")
    document["query"]["order"] = []
    document["selection_profiles"][0]["selection"][0]["field"] = "npi"
    document["selection_profiles"][0]["context_dimensions"] = []
    request = replace(request, definition=CustomImportDefinition.from_mapping(document))
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", len(request.definition.root_fields) * 2)
    rows = [(1, 0, 1, revision, 1, True) for revision in (7, 8, 9)]
    assert list(compact._revision_batches(request, rows, {})) == [(7, 8), (9,)]
    with pytest.raises(CandidateRunnerError, match="admitted byte page"):
        list(compact._revision_batches(request, [(1, 0, 1, 11, 61, True)], {}))


@pytest.mark.parametrize("revision,size", [(None, 10), (11, None), (11, -1), (11, True)])
@pytest.mark.parametrize("retained", [False, True])
def test_revision_metadata_rejects_missing_lookup_or_invalid_size(revision, size, retained):
    with pytest.raises(CandidateRunnerError, match="coverage lookup"):
        list(compact._revision_batches(_request(), [(1, 1, 1, revision, size, retained)], {}))


@pytest.mark.parametrize("retained", [False, None, "source", "retained", "unknown", 0, 1])
def test_revision_metadata_rejects_unknown_or_missing_selection_plan(retained):
    with pytest.raises(CandidateRunnerError, match="selection plan"):
        list(compact._revision_batches(_request(), [(1, 1, 1, 11, 10, retained)], {}))


def test_retained_revision_reader_rejects_source_rows_instead_of_silently_skipping_them():
    with pytest.raises(CandidateRunnerError, match="selection plan"):
        list(compact._revision_batches(_request(), [(8, 2, 2, 99, 60, False)], {}))


def _coverage_reader(monkeypatch, revision_rows, *, should_cancel=False, mismatch=False, scalar_counts=None):
    """Return all-origin SQL counters but transport only retained revision metadata."""
    state = SimpleNamespace(is_closed=False, batches=[], population_queries=[])

    def records(*_args, **_kwargs):
        try:
            yield from (row for row in revision_rows if row[-1] is True)
            if should_cancel:
                raise CancellationRequested("candidate execution is canceling")
        finally:
            state.is_closed = True

    def aggregate(_session, _request_value, _build_id, query):
        statement = query(snapshot_models(41))
        parameters = statement.compile(dialect=postgresql.dialect()).params
        if "coverage_revision_ids" not in parameters:
            state.population_queries.append(parameters)
            if not revision_rows or "coverage_after_0" in parameters:
                return []
            populations_by_scope = {}
            for family, slot, root, revision, _size, retained in revision_rows:
                origin = "retained" if retained else "source"
                count, representative = populations_by_scope.get((slot, origin), (0, root))
                populations_by_scope[slot, origin] = count + 1, min(representative, root)
            cursor = max((row[0], row[1], row[3]) if row[1] else (row[2],) for row in revision_rows)
            return [
                (slot, origin, count, root, *cursor) for (slot, origin), (count, root) in populations_by_scope.items()
            ]
        identifiers = parameters["coverage_revision_ids"]
        state.batches.append(identifiers)
        expected = 2 * len(identifiers)
        return [scalar_counts or (expected, expected - int(mismatch), expected)]

    monkeypatch.setattr(output, "_output_rows", records)
    monkeypatch.setattr(compact, "_aggregate", aggregate)
    return state


@pytest.mark.parametrize("child", [False, True])
def test_presence_batches_account_for_complete_eof_and_selected_child_populations(monkeypatch, child):
    request = _request(page_byte_limit=60)
    slot = int(child)
    rows = [(7 + index, slot, 4 + index, revision, 30, True) for index, revision in enumerate((11, 99, 10_000))]
    state = _coverage_reader(monkeypatch, rows)
    proof = _proof(family_child_count=3)
    actual = compact._presence_coverage(None, request, 7, _generation(request), proof, child=child)
    assert actual == [(slot, 3, 4)]
    assert state.batches == [(11, 99), (10_000,)] and state.is_closed


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("retained", [False, True])
def test_presence_decodes_only_retained_ids_without_comparing_subset_to_full_counts(monkeypatch, child, retained):
    request = _request(page_byte_limit=60)
    slot = int(child)
    rows = [(7, slot, 4, 11, 30, False), (8, slot, 5, 99, 30, retained), (9, slot, 6, 10_000, 30, retained)]
    state = _coverage_reader(monkeypatch, rows)
    proof = _proof(family_child_count=3, root_scalar_count=100, child_scalar_count=200)
    assert compact._presence_coverage(None, request, 7, _generation(request), proof, child=child) == [(slot, 3, 4)]
    assert state.batches == ([(99, 10_000)] if retained else [])
    assert (proof.root_scalar_count, proof.child_scalar_count) == (100, 200)
    assert state.is_closed is retained


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("counts", [(2, 1, 1), (2, 1, 2), (2, 2, 3), (0, 0, 1), (2, 0, 2)])
def test_retained_presence_rejects_missing_wrong_state_and_compensated_extra_cells(monkeypatch, child, counts):
    rows = [(7, int(child), 4, 11, 30, True)]
    state = _coverage_reader(monkeypatch, rows, scalar_counts=counts)
    with pytest.raises(CandidateRunnerError, match="scalar coverage"):
        compact._presence_coverage(
            None, _request(), 7, _generation(_request()), _proof(family_count=1, family_child_count=1), child=child
        )
    assert state.is_closed


@pytest.mark.parametrize("counts", [(0, 0, 0), (2, 2, 2)])
def test_retained_presence_accepts_missing_optional_cells_and_exact_null_states(monkeypatch, counts):
    rows = [(7, 1, 4, 11, 30, True)]
    state = _coverage_reader(monkeypatch, rows, scalar_counts=counts)
    assert compact._presence_coverage(
        None, _request(), 7, _generation(_request()), _proof(family_child_count=1), child=True
    ) == [(1, 1, 4)]
    assert state.is_closed


@pytest.mark.parametrize("failure", ["missing_row", "scalar_mismatch", "cancel"])
def test_presence_coverage_fails_closed_and_retry_starts_with_fresh_counts(monkeypatch, failure):
    request = _request(page_byte_limit=60)
    rows = [(7, 1, 4, revision, 30, True) for revision in (11, 99, 10_000)]
    state = _coverage_reader(
        monkeypatch, rows, should_cancel=failure == "cancel", mismatch=failure == "scalar_mismatch"
    )
    proof = _proof(family_child_count=4 if failure == "missing_row" else 3)
    error = CancellationRequested if failure == "cancel" else CandidateRunnerError
    with pytest.raises(error):
        compact._presence_coverage(None, request, 7, _generation(request), proof, child=True)
    assert state.is_closed is (failure != "missing_row")
    resumed = _coverage_reader(monkeypatch, rows)
    actual = compact._presence_coverage(
        None, request, 7, _generation(request), _proof(family_child_count=3), child=True
    )
    assert actual == [(1, 3, 4)]
    assert resumed.batches == [(11, 99), (10_000,)] and resumed.is_closed


def test_context_coverage_uses_unique_winner_group_without_global_distinct():
    statement = compact._context_coverage_query(snapshot_models(41), 7, _generation(_request()), None)
    compiled = _sql(statement)
    assert "LEFT OUTER JOIN" in compiled and "profile_slot IS NULL" in compiled
    assert "DISTINCT" not in compiled and "GROUP BY" in compiled
    assert "coverage_page AS MATERIALIZED" in compiled
    assert compiled.index("LIMIT") < compiled.index("LEFT OUTER JOIN") < compiled.index("GROUP BY")
    assert output.MAX_BATCH_ROWS in statement.compile(dialect=postgresql.dialect()).params.values()
    for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id"):
        assert f"custom_import_winner.{name} =" in compiled
    for name in ("profile_slot", "entity_binding_id", "context_key_sha256"):
        assert f"custom_import_winner.{name} = coverage_page.{name}" in compiled


@pytest.mark.parametrize("is_missing", [False, True])
def test_context_coverage_keeps_groups_spanning_pages_and_rejects_missing_winners(monkeypatch, is_missing):
    pages = iter([[(1, 2, 0, 10_000)], [(1, 3, int(is_missing), 90_000), (2, 1, 0, 90_000)], []])
    aggregate = Mock(side_effect=lambda *_args: next(pages))
    metadata = Mock(side_effect=AssertionError("context rows must not be transported"))
    monkeypatch.setattr(compact, "_aggregate", aggregate)
    monkeypatch.setattr(output, "_output_rows", metadata)
    if is_missing:
        with pytest.raises(CandidateRunnerError, match="winner group coverage"):
            compact._context_coverage(None, _request(), 7, _generation(_request()))
    else:
        assert compact._context_coverage(None, _request(), 7, _generation(_request())) == {1: 5, 2: 1}
    assert aggregate.call_count == (2 if is_missing else 3)
    metadata.assert_not_called()


@pytest.mark.parametrize("child", [False, True])
def test_population_query_bounds_native_prefix_before_origin_grouping(child):
    after = (8, 1, 99) if child else (8,)
    statement = compact._population_query(snapshot_models(41), 7, _generation(_request()), child=child, after=after)
    compilation = statement.compile(dialect=postgresql.dialect())
    compiled = str(compilation)
    assert "coverage_page AS MATERIALIZED" in compiled and "coverage_tail AS" in compiled
    assert compiled.index("LIMIT") < compiled.index("LEFT OUTER JOIN") < compiled.index("GROUP BY")
    assert output.MAX_BATCH_ROWS in compilation.params.values()
    assert "OFFSET" not in compiled and "DISTINCT" not in compiled
    for forbidden in ("canonical_payload", "custom_import_root_revision", "custom_import_child_revision", "scalar"):
        assert forbidden not in compiled
    for name in ("generation_id", "dataset_id", "definition_revision_id", "schema_revision_id"):
        assert f"custom_import_generation_family.{name} =" in compiled
    assert "min(coverage_page.root_record_id)" in compiled
    assert "custom_import_build_family.root_record_id = coverage_page.root_record_id" in compiled
    assert "custom_import_build_family.family_revision_id = coverage_page.family_revision_id" in compiled
    for index, value in enumerate(after):
        assert compilation.params[f"coverage_after_{index}"] == value
    page_sql = compiled.split("coverage_tail AS", 1)[0]
    source = "custom_import_family_child" if child else "custom_import_generation_family"
    fields = ("family_revision_id", "collection_slot", "child_revision_id") if child else ("root_record_id",)
    full_key = "(" + ", ".join(f"ci_snapshot_41.{source}.{field}" for field in fields) + ")"
    assert full_key + " > (" in page_sql
    assert f"{source}.{fields[0]} >=" in page_sql
    assert "selection_kind" not in page_sql


def _aggregate_pages(monkeypatch, pages):
    """Record each bound cursor while returning only synthetic SQL aggregate rows."""
    pending, cursors = iter(pages), []

    def aggregate(_session, _request_value, _build_id, query):
        statement = query(snapshot_models(41))
        parameters = statement.compile(dialect=postgresql.dialect()).params
        cursors.append(
            tuple(
                parameters[f"coverage_after_{index}"] for index in range(3) if f"coverage_after_{index}" in parameters
            )
        )
        page = next(pending)
        if isinstance(page, Exception):
            raise page
        return page

    monkeypatch.setattr(compact, "_aggregate", aggregate)
    return cursors


def test_population_aggregates_merge_origins_and_minimum_roots_across_full_tuple_pages(monkeypatch):
    pages = [
        [(1, "source", 2, 9, 8, 1, 99), (1, "retained", 1, 4, 8, 1, 99)],
        [(1, "source", 3, 2, 8, 2, 11), (2, "retained", 1, 5, 8, 2, 11)],
        [],
    ]
    cursors = _aggregate_pages(monkeypatch, pages)
    metadata = Mock(side_effect=AssertionError("SOURCE revision rows must not be transported"))
    monkeypatch.setattr(output, "_output_rows", metadata)
    actual = compact._population_coverage(
        None, _request(), 7, _generation(_request()), _proof(family_child_count=7), child=True
    )
    assert actual == ({1: (6, 2), 2: (1, 5)}, {1: (1, 4), 2: (1, 5)})
    assert cursors == [(), (8, 1, 99), (8, 2, 11)]
    metadata.assert_not_called()


@pytest.mark.parametrize(
    "bad_page",
    [
        [(1, "source", 1, 4, 8, 1, 6)],
        [(1, "source", 1, 4, 8, 1, 7)],
        [(1, "source", 1, 4, 8, 1, None)],
        [(1, "source", 1, 4, 8, 1, True)],
        [(1, "source", 0, 4, 8, 1, 8)],
        [(1, "source", -1, 4, 8, 1, 8)],
        [(1, "source", True, 4, 8, 1, 8)],
        [(1, "source", output.MAX_BATCH_ROWS + 1, 4, 8, 1, 8)],
        [(1, "source", 1, 4, 8, 1, 8), (1, "retained", 1, 4, 8, 1, 9)],
    ],
)
def test_aggregate_pages_reject_regression_invalid_counts_and_inconsistent_full_cursors(monkeypatch, bad_page):
    _aggregate_pages(monkeypatch, [[(1, "source", 1, 4, 8, 1, 7)], bad_page])
    with pytest.raises(CandidateRunnerError, match="aggregate page"):
        compact._population_coverage(None, _request(), 7, _generation(_request()), _proof(), child=True)


@pytest.mark.parametrize("slot,origin,root", [(1, None, 4), (1, "unknown", 4), (0, "source", 4), (1, "source", None)])
def test_population_aggregates_reject_missing_plan_or_invalid_scope(monkeypatch, slot, origin, root):
    _aggregate_pages(monkeypatch, [[(slot, origin, 1, root, 8, 1, 7)]])
    with pytest.raises(CandidateRunnerError, match="selection plan"):
        compact._population_coverage(None, _request(), 7, _generation(_request()), _proof(), child=True)


@pytest.mark.parametrize("observed", [2, 4])
def test_population_eof_rejects_missing_and_extra_native_identities(monkeypatch, observed):
    cursors = _aggregate_pages(monkeypatch, [[(0, "source", observed, 4, 8)], []])
    with pytest.raises(CandidateRunnerError, match="exact structural counts"):
        compact._population_coverage(None, _request(), 7, _generation(_request()), _proof(), child=False)
    assert cursors == [(), (8,)]


@pytest.mark.parametrize("change", ["missing_row", "changed_root"])
def test_retained_reader_has_independent_eof_count_and_representative_proof(monkeypatch, change):
    rows = [(7, 1, 4, 11, 30, True), (8, 1, 5, 99, 30, True)]
    _coverage_reader(monkeypatch, rows)
    changed = rows[:1] if change == "missing_row" else [(7, 1, 99, 11, 30, True), rows[1]]
    monkeypatch.setattr(output, "_output_rows", lambda *_args, **_kwargs: (row for row in changed))
    with pytest.raises(CandidateRunnerError, match="exact structural counts"):
        compact._presence_coverage(
            None, _request(), 7, _generation(_request()), _proof(family_child_count=2), child=True
        )


def test_context_aggregate_cancellation_discards_partial_counts_and_retry_restarts(monkeypatch):
    request = _request()
    initial = _aggregate_pages(monkeypatch, [[(1, 2, 0, 10_000)], CancellationRequested("canceling")])
    with pytest.raises(CancellationRequested):
        compact._context_coverage(None, request, 7, _generation(request))
    assert initial == [(), (10_000,)]
    resumed = _aggregate_pages(monkeypatch, [[(1, 2, 0, 10_000)], [(1, 3, 0, 90_000)], []])
    assert compact._context_coverage(None, request, 7, _generation(request)) == {1: 5}
    assert resumed == [(), (10_000,), (90_000,)]


def test_aggregate_pages_refresh_read_authority_and_binding_through_empty_eof(monkeypatch):
    pages = iter([[(1, 2, 0, 10_000)], [(1, 3, 0, 90_000)], []])
    boundaries = []

    @contextmanager
    def transaction(*_args):
        boundaries.append("enter")
        try:
            yield object(), 20
        finally:
            boundaries.append("exit")

    binding = Mock(return_value=(snapshot_models(41),))
    prepare, budget = Mock(), Mock()
    session = SimpleNamespace(execute=Mock(side_effect=lambda _query: SimpleNamespace(all=lambda: next(pages))))
    monkeypatch.setattr(output, "_read_transaction", transaction)
    monkeypatch.setattr(output, "_build_storage_models", binding)
    monkeypatch.setattr(output, "_prepare_read", prepare)
    monkeypatch.setattr(output, "_require_budget", budget)
    assert compact._context_coverage(session, _request(), 7, _generation(_request())) == {1: 5}
    assert boundaries == ["enter", "exit"] * 3
    assert binding.call_count == prepare.call_count == budget.call_count == session.execute.call_count == 3


@pytest.mark.parametrize("unknown_profile", [False, True])
def test_complete_coverage_combines_context_counts_and_all_summary_winner_profiles(monkeypatch, unknown_profile):
    child_scopes = [(1, 5, 10)]
    monkeypatch.setattr(compact, "_presence_coverage", lambda *_args, child: child_scopes if child else [])
    monkeypatch.setattr(compact, "_aggregate", lambda *_args: [(1, 0), (2, 1)])
    monkeypatch.setattr(compact, "_context_coverage", lambda *_args: {1: 3, 2: 5})
    population_by_profile = {1: 2, 2: 4, **({3: 1} if unknown_profile else {})}
    arguments = (None, _request(), 7, _generation(_request()), _proof(), population_by_profile)
    if unknown_profile:
        with pytest.raises(CandidateRunnerError, match="context coverage"):
            compact._complete_coverage(*arguments)
    else:
        assert compact._complete_coverage(*arguments) == (child_scopes, population_by_profile)


def test_summary_queries_do_not_select_payloads_or_scalars():
    models, generation = snapshot_models(41), _generation(_request())
    for factory in (lambda models, generation: compact._family_query(models, generation, 7), compact._winner_query):
        statement, keys, row_models = factory(models, generation)
        compiled = _sql(statement)
        assert keys and row_models == ()
        assert "canonical_payload" not in compiled
        assert "custom_import_root_scalar" not in compiled
        assert "custom_import_child_scalar" not in compiled
    assert "context_key_sha256" in str(compact._winner_query(models, generation)[1][-1])


@pytest.mark.parametrize("child", [False, True])
def test_sample_projection_filter_is_in_sql_before_physical_paging(child):
    statement, _keys, _models = output._projection_query(
        snapshot_models(41), _generation(_request()), child=child, revision_ids=(11, 12)
    )
    compiled = statement.compile(dialect=postgresql.dialect())
    assert "ANY" in str(compiled)
    assert compiled.params["sample_revision_ids"] == (11, 12)


def test_sample_payload_order_and_byte_reservation():
    request = _request()
    bounds = compact._sample_read_budget((1, 2), {1: {(1, 11)}, 2: {(1, 12)}})
    assert bounds.reserve_bytes == output.MAX_BATCH_BYTES // 2 + 4 * 128
    statement, keys, models = compact._sample_child_query(snapshot_models(41), _generation(request), (11, 12))
    assert not statement._order_by_clauses
    assert len(keys) == 1 and "child_revision_id" in str(keys[0])
    assert len(models) == 2 and "root_record" in models[1].__table__.name
    statement, _keys, _models = compact._sample_family_query(snapshot_models(41), 7, _generation(request), (1,))
    assert not statement._order_by_clauses


def test_seeded_sample_is_order_independent_bounded_and_resume_stable():
    first = compact._Sample(b"a" * 32, 5)
    reverse = compact._Sample(b"a" * 32, 5)
    changed = compact._Sample(b"b" * 32, 5)
    for number in range(100):
        first.add(number.to_bytes(4, "big"), number)
        changed.add(number.to_bytes(4, "big"), number)
    for number in reversed(range(100)):
        reverse.add(number.to_bytes(4, "big"), number)
    assert first.values() == reverse.values()
    assert len(first.values()) == 5
    assert first.values() != changed.values()
    generation = _generation(_request())
    assert compact._seed(generation, _proof()) == compact._seed(generation, _proof())
    assert compact._seed(generation, _proof()) != compact._seed(generation, _proof(build_id=8))


def test_child_sampling_uses_bounded_indexed_successors_and_stable_scope_order(monkeypatch):
    scopes = [(2, 22, 1, 100, 90_000_000), (1, 11, 1, 7, 7)]
    queries = []

    def aggregate(_session, _request_value, _build_id, factory):
        statement = factory(snapshot_models(41))
        queries.append(statement)
        parameters = statement.compile(dialect=postgresql.dialect()).params
        if "sample_pivots" not in parameters:
            assert parameters["sample_roots"] == (1, 2)
            assert parameters["sample_collections"] == (1,)
            return scopes
        return list(enumerate(parameters["sample_pivots"], 1))

    monkeypatch.setattr(compact, "_aggregate", aggregate)
    arguments = (None, _request(), 7, _generation(_request()), (1, 2), (1,), b"s" * 32)
    selected = compact._sample_children(*arguments)
    scopes.reverse()
    assert compact._sample_children(*arguments) == selected
    assert sum(map(len, selected.values())) <= compact.CHILD_SAMPLE_LIMIT
    scope_sql = _sql(queries[0])
    assert scope_sql.count("JOIN LATERAL") == scope_sql.count("LIMIT") == 2
    assert "child_revision_id DESC" in scope_sql
    assert "family_revision_id = ci_snapshot_41.custom_import_generation_family.family_revision_id" in scope_sql
    assert "collection_slot = collections.collection_slot" in scope_sql
    assert not any(token in scope_sql for token in ("count(", "min(", "max(", "GROUP BY", "OFFSET"))
    probe_sql = _sql(queries[1])
    assert "JOIN LATERAL" in probe_sql and "LIMIT" in probe_sql
    assert "child_revision_id >=" in probe_sql
    assert "OFFSET" not in probe_sql and "random(" not in probe_sql


def test_child_probes_seed_scope_selection_without_favoring_low_root_ids():
    scopes = [
        (root, root + 100, collection, root * 10, root * 10 + 9) for root in range(1, 97) for collection in (1, 2)
    ]
    first = compact._child_probes(scopes, b"a" * 32)
    reverse = compact._child_probes(reversed(scopes), b"a" * 32)
    changed = compact._child_probes(scopes, b"b" * 32)
    assert first == reverse
    for collection in (1, 2):
        chosen_root_ids = {root for root, _family, slot, _pivot in first if slot == collection}
        changed_root_ids = {root for root, _family, slot, _pivot in changed if slot == collection}
        assert len(chosen_root_ids) == compact.CHILD_SAMPLE_LIMIT
        assert chosen_root_ids != changed_root_ids
        assert max(chosen_root_ids) > compact.CHILD_SAMPLE_LIMIT


@pytest.mark.parametrize("roots,collections", [((), (1,)), ((1,), ()), ((), ())])
def test_child_sampling_skips_empty_populations_without_reading(monkeypatch, roots, collections):
    aggregate = Mock(side_effect=AssertionError("empty population must not query children"))
    monkeypatch.setattr(compact, "_aggregate", aggregate)
    assert compact._sample_children(None, _request(), 7, _generation(_request()), roots, collections, b"s" * 32) == {}
    aggregate.assert_not_called()


def test_winner_group_sizes_count_only_an_indexed_capped_prefix():
    candidates = [(2, 11, b"b" * 32), (1, 9, b"a" * 32)]
    statement = compact._winner_group_counts_query(snapshot_models(41), 7, candidates)
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert compiled.params["sample_profiles"] == [2, 1]
    assert compiled.params["sample_entities"] == [11, 9]
    assert compiled.params["sample_contexts"] == [b"b" * 32, b"a" * 32]
    assert compact.COMPLETE_WINNER_GROUP_LIMIT + 1 in compiled.params.values()
    assert sql.count("JOIN LATERAL") == sql.count("LIMIT") == 1
    assert "count(bounded.candidate_context_id)" in sql
    assert sql.index("JOIN LATERAL") < sql.index("ORDER BY") < sql.index("LIMIT") < sql.index("GROUP BY")
    assert "ORDER BY ci_snapshot_41.custom_import_build_candidate_context.candidate_context_id" in sql
    for field in ("profile_slot", "entity_binding_id", "context_key_sha256"):
        assert f"custom_import_build_candidate_context.{field} = chosen.{field}" in sql
    assert "OFFSET" not in sql and "canonical_context_key" not in sql


def test_evidence_reports_actual_partial_coverage_and_empty_registered_scopes():
    evidence = compact._evidence(
        b"s" * 32, (1, 2), {1: {10}}, {}, {("root", 0): 100, ("child", 1): 5, ("child", 2): 0, ("winner", 1): 6}
    )
    assert evidence["contract"] == compact.SAMPLE_CONTRACT
    assert len(evidence["selection_sha256"]) == 64
    assert [(item["sampled"], item["capped"]) for item in evidence["coverage"]] == [
        (2, True),
        (1, True),
        (0, False),
        (0, True),
    ]
    publication._validate_verification_evidence(compact.MATERIALIZATION_CONTRACT, evidence)


def _family_summary(root_id=1, *, origin="source"):
    """Use synthetic semantic commitments independently of allocation identities."""
    return (
        root_id,
        b"r" * 32,
        b"k" * 32,
        "logical-root",
        b"f" * 32,
        5,
        b"p" * 32,
        1,
        "entity",
        b"e" * 32,
        "entity-value",
        origin,
    )


def _winner_summary():
    return (1, 9, b"c" * 32, 19, 0, None, "entity", b"e" * 32, "entity-value", b"k" * 32, b"r" * 32, b"", b"f" * 32)


def test_effective_commitments_ignore_allocation_and_attempt_provenance():
    family = _family_summary()
    retained = (999, *family[1:7], 88, *family[8:-1], "retained")
    assert compact._family_document(family, effective=True) == compact._family_document(retained, effective=True)
    assert compact._family_document(family, effective=False) != compact._family_document(retained, effective=False)
    winner = _winner_summary()
    copied_winner = (winner[0], 999, winner[2], 888, *winner[4:])
    assert compact._winner_document(winner) == compact._winner_document(copied_winner)
    mutated = (*family[:4], b"x" * 32, *family[5:])
    assert compact._family_document(family, effective=True) != compact._family_document(mutated, effective=True)


def test_narrow_stream_hashes_all_rows_and_keeps_retained_origin_sample(monkeypatch):
    families = [_family_summary(index + 1, origin="retained" if index == 99 else "source") for index in range(100)]
    families = [(number, number.to_bytes(32, "big"), *family[2:]) for number, family in enumerate(families, 1)]
    monkeypatch.setattr(compact, "_compact_rows", lambda *_args: iter(families))
    monkeypatch.setattr(compact, "_winner_rows", lambda *_args: (row for row in [_winner_summary()]))
    digests = (publication._new_digest("test-content"), publication._new_digest("test-effective"))
    roots, winners, winner_populations = compact._summaries(
        None, _request(), 7, _generation(_request()), _proof(family_count=100, winner_count=1), digests, b"s" * 32
    )
    assert 1 in roots and 100 in roots
    assert len(roots) <= compact.ROOT_SAMPLE_LIMIT + 2
    assert winners[1].values() == [_winner_summary()]
    assert winner_populations == {1: 1}
    expected = publication._new_digest("test-effective")
    for family in families:
        publication._add_digest_record(expected, "family_commitment", compact._family_document(family, effective=True))
    publication._add_digest_record(expected, "winner_commitment", compact._winner_document(_winner_summary()))
    assert expected.digest() == digests[1].digest()


def _prepare_compact_finish(monkeypatch, request):
    """Stub storage boundaries while retaining final digest and evidence orchestration."""

    @contextmanager
    def frozen_read(*_args):
        yield object(), 100

    generation = _generation(request)
    generation.source_bundle_sha256, generation.candidate_sha256 = b"s" * 32, b"c" * 32
    generation.root_count = generation.family_count = 3
    monkeypatch.setattr(output, "_read_transaction", frozen_read)
    monkeypatch.setattr(output, "_proof_matches", Mock())
    monkeypatch.setattr(output, "_source_digest", lambda *_args: b"s" * 32)
    monkeypatch.setattr(output, "_candidate_digest", lambda *_args, **_kwargs: (b"c" * 32, 3))
    monkeypatch.setattr(output, "_identity_material", Mock())
    monkeypatch.setattr(output, "_shape_material", lambda *_args: 2)
    monkeypatch.setattr(output, "_attempt_material", Mock())
    monkeypatch.setattr(compact, "_complete_coverage", lambda *_args: ([(1, 5, 2)], {1: 2, 2: 4}))
    monkeypatch.setattr(compact, "_summaries", lambda *_args: ([1], {}, {1: 2, 2: 4}))
    monkeypatch.setattr(compact, "_sample_children", Mock(return_value={2: {(1, 10)}}))
    monkeypatch.setattr(compact, "_verify_revisions", lambda *_args: {1: {10}})
    monkeypatch.setattr(compact, "_verify_winners", lambda *_args: {1: [(1, 9, b"k" * 32)]})
    monkeypatch.setattr(output, "_exhaustive_materialization", Mock(side_effect=AssertionError("unexpected fallback")))
    return generation


def test_frozen_finish_binds_evidence_without_changing_effective_hash(monkeypatch):
    request = _request()
    generation = _prepare_compact_finish(monkeypatch, request)
    arguments = (None, request, _registry(request.definition), 7, generation)
    first = compact.frozen_materialization(*arguments, _proof())
    resumed = compact.frozen_materialization(*arguments, _proof())
    later = compact.frozen_materialization(*arguments, _proof(output_frozen_at=dt.datetime(2030, 1, 4, tzinfo=dt.UTC)))
    assert first == resumed
    assert first.materialization_contract == compact.MATERIALIZATION_CONTRACT
    assert first.effective_output_sha256 == later.effective_output_sha256
    assert first.materialization_sha256 != later.materialization_sha256
    assert [(item["kind"], item["slot"]) for item in first.verification_evidence["coverage"]] == [
        ("root", 0),
        ("child", 1),
        ("winner", 1),
        ("winner", 2),
    ]
    assert first.verification_evidence["coverage"][0]["sampled"] == 2
    assert compact._sample_children.call_args.args[-3:-1] == ((1, 2), (1,))
    output._exhaustive_materialization.assert_not_called()


def test_frozen_finish_preserves_frozen_authority_failure(monkeypatch):
    request = _request()
    generation = _prepare_compact_finish(monkeypatch, request)
    monkeypatch.setattr(output, "_proof_matches", Mock(side_effect=CandidateRunnerError("frozen authority lost")))
    with pytest.raises(CandidateRunnerError, match="frozen authority"):
        compact.frozen_materialization(None, request, _registry(request.definition), 7, generation, _proof())


def _sampled_family(monkeypatch, request, *, oversized=False):
    """Provide real canonical records and projections while replacing only database reads."""
    family = _family(request, 1, ({"rate_npi": "0000000001", "service_code": "S", "amount": Decimal("1")},))
    family_record, children = family
    if oversized:
        family_record[1].child_count = compact.COMPLETE_FAMILY_CHILD_LIMIT + 1
    root_projections = _projection_records(request, [family], child=False)
    child_projections = _projection_records(request, [family], child=True)

    def records(_session, _request_value, _build_id, query, **_kwargs):
        statement = query(snapshot_models(41))[0]
        parameters = statement.compile(dialect=postgresql.dialect()).params
        if "sample_revision_ids" in parameters:
            projections = child_projections if "child_scalar" in _sql(statement) else root_projections
            yield from projections
        elif "sample_children" in parameters:
            yield from ((child, family_record[3]) for _root, _collection, child, _valid in children)
        else:
            yield family_record

    monkeypatch.setattr(output, "_output_rows", records)
    complete_family = Mock()
    monkeypatch.setattr(output, "_verify_family_page", complete_family)
    return family_record, children[0][2], root_projections, child_projections, complete_family


@pytest.mark.parametrize("oversized", [False, True])
def test_revision_audit_checks_all_selected_cells_without_giant_family_fallback(monkeypatch, oversized):
    request = _request()
    family, child, _roots, _children, complete = _sampled_family(monkeypatch, request, oversized=oversized)
    selected_by_root = {1: {(child.collection_slot, child.child_revision_id)}}
    actual = compact._verify_revisions(
        None, request, _registry(request.definition), 7, _generation(request), (1,), selected_by_root
    )
    assert actual == {child.collection_slot: {child.child_revision_id}}
    assert complete.call_count == int(not oversized)
    assert family[3].root_record_id == 1


@pytest.mark.parametrize("child", [False, True])
def test_revision_audit_rejects_changed_sampled_values(monkeypatch, child):
    request = _request()
    _family_record, revision, roots, children, _complete = _sampled_family(monkeypatch, request, oversized=True)
    (children if child else roots)[0][0].string_value = "changed"
    with pytest.raises(CandidateRunnerError, match="typed scalar projection differs"):
        compact._verify_revisions(
            None,
            request,
            _registry(request.definition),
            7,
            _generation(request),
            (1,),
            {1: {(1, revision.child_revision_id)}},
        )


def _bulk_revision_fixture(monkeypatch, request):
    """Track bulk stream lifetimes while checking real canonical values and scalars."""
    families = [
        _family(request, root, ({"rate_npi": f"{root:010d}", "service_code": "S", "amount": Decimal("1")},))
        for root in range(1, 5)
    ]
    for family_record, _children in families[2:]:
        family_record[5].selection_kind = "retained"
    fixture = SimpleNamespace(
        families=families,
        child_rows=[(children[0][2], family_record[3]) for family_record, children in families],
        root_projections=_projection_records(request, families, child=False),
        child_projections=_projection_records(request, families, child=True),
        calls=[],
        active=set(),
        complete=Mock(),
    )

    def records(_session, _request_value, _build_id, query, *, bounds):
        statement = query(snapshot_models(41))[0]
        parameters = statement.compile(dialect=postgresql.dialect()).params
        if "sample_revision_ids" in parameters:
            is_child = "child_scalar" in _sql(statement)
            kind, identifiers = (
                ("child_projection" if is_child else "root_projection"),
                parameters["sample_revision_ids"],
            )
            rows = fixture.child_projections if is_child else fixture.root_projections
        elif "sample_children" in parameters:
            kind, identifiers, rows = "child_payload", parameters["sample_children"], fixture.child_rows
        else:
            kind, identifiers = "root_payload", parameters["sample_roots"]
            rows = [
                family_record
                for family_record, _children in fixture.families
                if family_record[3].root_record_id in identifiers
            ]
        assert not fixture.active
        fixture.calls.append((kind, tuple(identifiers), bounds))
        fixture.active.add(kind)
        try:
            yield from rows
        finally:
            fixture.active.remove(kind)

    monkeypatch.setattr(output, "_output_rows", records)
    monkeypatch.setattr(output, "_verify_family_page", fixture.complete)
    return fixture


def _bulk_revision_audit(request, fixture, *, roots=(1, 2, 3, 4), children_by_root=None):
    """Run the real audit against a bounded multi-root fixture."""
    if children_by_root is None:
        children_by_root = {
            family_record[3].root_record_id: {(child_rows[0][2].collection_slot, child_rows[0][2].child_revision_id)}
            for family_record, child_rows in fixture.families
        }
    return compact._verify_revisions(
        None, request, _registry(request.definition), 7, _generation(request), roots, children_by_root
    )


def test_bulk_revision_audit_closes_payloads_and_limits_complete_families(monkeypatch):
    request = _request()
    fixture = _bulk_revision_fixture(monkeypatch, request)
    fixture.families[0][0][1].child_count = compact.COMPLETE_FAMILY_CHILD_LIMIT + 1
    assert _bulk_revision_audit(request, fixture) == {1: {100_001, 200_001, 300_001, 400_001}}
    assert [kind for kind, _identifiers, _bounds in fixture.calls] == [
        "root_payload",
        "root_projection",
        "child_payload",
        "child_projection",
        "root_payload",
    ]
    assert len(fixture.calls[1][1]) == len(fixture.calls[2][1]) == len(fixture.calls[3][1]) == 4
    assert fixture.calls[0][2].row_limit is None and fixture.calls[-1][2].row_limit == 1
    assert all(bounds.reserve_bytes > output.MAX_BATCH_BYTES // 2 for _kind, _ids, bounds in fixture.calls)
    rehashed_roots = [call.args[-1][0][3].root_record_id for call in fixture.complete.call_args_list]
    assert rehashed_roots == [2, 3]
    assert fixture.active == set()


@pytest.mark.parametrize("change", ["wrong_root", "missing_child", "duplicate_child", "unselected_root"])
def test_bulk_revision_audit_rejects_changed_sample_mapping(monkeypatch, change):
    request = _request()
    fixture = _bulk_revision_fixture(monkeypatch, request)
    children_by_root = {root: {(1, 100_000 * root + 1)} for root in range(1, 5)}
    if change == "wrong_root":
        fixture.child_rows[0] = (fixture.child_rows[0][0], fixture.families[1][0][3])
    elif change == "missing_child":
        fixture.child_rows.pop(0)
    elif change == "duplicate_child":
        children_by_root[2].add((1, 100_001))
    else:
        children_by_root[5] = {(1, 500_001)}
    with pytest.raises(CandidateRunnerError, match="selected root|sample coverage"):
        _bulk_revision_audit(request, fixture, children_by_root=children_by_root)
    assert fixture.active == set()


def test_bulk_revision_audit_rejects_missing_root_before_projection_reads(monkeypatch):
    request = _request()
    fixture = _bulk_revision_fixture(monkeypatch, request)
    children_by_root = {root: {(1, 100_000 * root + 1)} for root in range(1, 5)}
    fixture.families.pop()
    with pytest.raises(CandidateRunnerError, match="sample coverage"):
        _bulk_revision_audit(request, fixture, children_by_root=children_by_root)
    assert [kind for kind, _identifiers, _bounds in fixture.calls] == ["root_payload"]
    assert fixture.active == set()


@pytest.mark.parametrize("child", [False, True])
def test_bulk_revision_audit_checks_values_beyond_first_selected_revision(monkeypatch, child):
    request = _request()
    fixture = _bulk_revision_fixture(monkeypatch, request)
    (fixture.child_projections if child else fixture.root_projections)[-1][0].string_value = "changed"
    with pytest.raises(CandidateRunnerError, match="typed scalar projection differs"):
        _bulk_revision_audit(request, fixture)
    assert fixture.active == set()


def _winner_sample(request, candidate):
    registry, group = _group(request, candidate)
    sample = compact._Sample(b"s" * 32, 1)
    sample.add(
        b"key",
        (*group, candidate.family_revision_id, candidate.context_collection_slot, candidate.context_child_revision_id),
    )
    return registry, group, {group[0]: sample}


@pytest.mark.parametrize("group_size", [1, 256, 257])
def test_ranking_audit_reduces_complete_groups_or_explicitly_omits_oversized(monkeypatch, group_size):
    request, candidate = _request(), _candidate()
    registry, group, samples = _winner_sample(request, candidate)
    monkeypatch.setattr(compact, "_aggregate", lambda *_args: [(*group, group_size)])
    records = Mock(side_effect=lambda *_args, **_kwargs: (record for record in [_context_row(request, candidate)]))
    monkeypatch.setattr(output, "_context_rows", records)
    actual = compact._verify_winners(None, request, registry, 7, _generation(request), samples)
    is_oversized = group_size > compact.COMPLETE_WINNER_GROUP_LIMIT
    assert actual == ({} if is_oversized else {1: [group]})
    assert records.call_count == int(not is_oversized)


def test_ranking_audit_rejects_missing_sampled_group_before_reading(monkeypatch):
    request, candidate = _request(), _candidate()
    registry, _group_key, samples = _winner_sample(request, candidate)
    monkeypatch.setattr(compact, "_aggregate", lambda *_args: [])
    records = Mock(side_effect=AssertionError("missing group must not reach ranking"))
    monkeypatch.setattr(output, "_context_rows", records)
    with pytest.raises(CandidateRunnerError, match="no complete candidate group"):
        compact._verify_winners(None, request, registry, 7, _generation(request), samples)
    records.assert_not_called()


def test_ranking_audit_rejects_wrong_selected_candidate(monkeypatch):
    request, candidate = _request(), _candidate()
    registry, group, samples = _winner_sample(request, candidate)
    better = replace(candidate, family_revision_id=99, context_child_revision_id=100, family_sha256=b"\0" * 32)
    monkeypatch.setattr(compact, "_aggregate", lambda *_args: [(*group, 2)])
    monkeypatch.setattr(
        output,
        "_context_rows",
        lambda *_args, **_kwargs: (
            record for record in [_context_row(request, candidate), _context_row(request, better)]
        ),
    )
    with pytest.raises(CandidateRunnerError, match="surviving-tie"):
        compact._verify_winners(None, request, registry, 7, _generation(request), samples)


@pytest.mark.asyncio
@pytest.mark.parametrize("base_contract,candidate_contract", [("v1", "v2"), ("v2", "v1")])
async def test_mixed_materialization_contracts_publish_normally_instead_of_false_no_change(
    monkeypatch, base_contract, candidate_contract
):
    prefix = "custom-import/materialization/"
    seal = SimpleNamespace(materialization_contract=prefix + base_contract, effective_output_sha256=b"d" * 32)
    candidate = SimpleNamespace(materialization_contract=prefix + candidate_contract, effective_output_sha256=b"d" * 32)
    monkeypatch.setattr(output, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(publication, "_locked_generation", AsyncMock(return_value=SimpleNamespace(generation_id=9)))
    monkeypatch.setattr(publication, "_validated_generation_seal", AsyncMock(return_value=seal))
    pointer = AsyncMock(side_effect=AssertionError("different algorithms are not comparable"))
    monkeypatch.setattr(publication, "_locked_pointer", pointer)
    assert await output._unchanged_base(None, _request(), SimpleNamespace(base_generation_id=9), candidate) is None
    pointer.assert_not_called()
