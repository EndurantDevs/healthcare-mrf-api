# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execute the relational reduction on synthetic evidence; native admission has separate proofs."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import asynccontextmanager, nullcontext
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql, sqlite

from process.custom_import import build_counts as counts
from process.custom_import import build_source as source
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner import reject_duplicate_canonical_root_keys
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_source import _child, _request, _root


def _evidence(request, record, *, child=False, initial=None, resolved=None, **changes):
    prepared = source._prepare_row(request, request.definition.source_streams[int(child)], record)
    return (
        dict(
            record_kind="child" if child else "root",
            collection_slot=int(child),
            raw_parent_key_sha256=None if prepared.raw_key is None else prepared.raw_key[1],
            raw_parent_key_canonical=None if prepared.raw_key is None else prepared.raw_key[0],
            root_record_id=None if prepared.typed_key is None else prepared.typed_key[1].hex(),
            root_revision_id=None if prepared.rejection or child else 1,
            initial=prepared.rejection.code if prepared.rejection else initial,
            resolved=resolved,
            origin="source",
            build_id=5,
        )
        | changes
    )


class _BitOr:
    def __init__(self):
        self.value = 0

    def step(self, value):
        self.value |= value

    def finalize(self):
        return self.value


def _evaluate_outcome_query(request, occurrence_rows, *, events=None):
    """Run the production SQL expression, without simulating its grouping in Python."""
    with sqlite3.connect(":memory:") as connection:
        connection.row_factory = sqlite3.Row
        connection.create_collation("C", lambda first, second: (first > second) - (first < second))
        connection.create_function("octet_length", 1, lambda value: None if value is None else len(value.encode()))
        connection.create_aggregate("bit_or", 1, _BitOr)
        connection.execute("ATTACH DATABASE ':memory:' AS ci_snapshot_23")
        connection.execute("ATTACH DATABASE ':memory:' AS mrf")
        connection.execute(
            "CREATE TABLE ci_snapshot_23.custom_import_build_occurrence (occurrence_id INTEGER, "
            "build_id INTEGER,origin TEXT,record_kind TEXT,collection_slot INTEGER,raw_parent_key_sha256 BLOB,"
            "raw_parent_key_canonical TEXT,root_record_id TEXT,root_revision_id INTEGER,rejection_id INTEGER,resolved_rejection_id INTEGER)"
        )
        connection.execute("CREATE TABLE ci_snapshot_23.custom_import_rejection (rejection_id INTEGER,code TEXT)")
        connection.execute(
            "CREATE TABLE mrf.custom_import_child_collection (dataset_id INTEGER,schema_revision_id INTEGER,"
            "collection_slot INTEGER,collection_name TEXT)"
        )
        connection.executemany(
            "INSERT INTO mrf.custom_import_child_collection VALUES (?,?,?,?)",
            [
                (request.dataset_id, request.schema_revision_id, index, collection.name)
                for index, collection in enumerate(request.definition.child_collections, 1)
            ],
        )
        for ordinal, evidence_row in enumerate(occurrence_rows, 1):
            values_by_column = {**evidence_row, "occurrence_id": ordinal}
            missing_rejection = values_by_column.pop("missing_rejection", False)
            for offset, name in enumerate(("initial", "resolved")):
                code = values_by_column.pop(name)
                identifier = 2 * ordinal + offset if code is not None else None
                values_by_column["rejection_id" if name == "initial" else "resolved_rejection_id"] = identifier
                if code is not None and not missing_rejection:
                    connection.execute(
                        "INSERT INTO ci_snapshot_23.custom_import_rejection VALUES (?,?)", (identifier, code)
                    )
            connection.execute(
                "INSERT INTO ci_snapshot_23.custom_import_build_occurrence ("
                + ",".join(values_by_column)
                + ") VALUES ("
                + ",".join("?" for _ in values_by_column)
                + ")",
                list(values_by_column.values()),
            )
        return _execute_count_pages(connection, request, events)


def _execute_count_pages(connection, request, events):
    def execute(statement):
        compiled = statement.compile(dialect=sqlite.dialect(), compile_kwargs={"render_postcompile": True})
        if events is not None:
            events.append(str(compiled))
        records = connection.execute(str(compiled), [compiled.params[key] for key in compiled.positiontup]).fetchall()
        values = [SimpleNamespace(**record) for record in records]
        return SimpleNamespace(
            one=lambda: values[0],
            all=lambda: values,
            scalar_one_or_none=lambda: None if not records else records[0][0],
        )

    def bind(*_args):
        if events is not None:
            events.append("bind")
        return snapshot_models(23), None

    session = SimpleNamespace(execute=execute)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(
            counts,
            "_read_transaction",
            lambda *_args: nullcontext(
                (
                    SimpleNamespace(phase="graph", source_frozen_at=object()),
                    time.monotonic() + 30,
                )
            ),
        )
        patch.setattr(counts, "_build_storage_models", bind)
        patch.setattr(counts, "_prepare_read", lambda *_args: None)
        return counts._count_source(counts._CountScan(session, request, 5))


def _assert_counts(result, accepted, rejected):
    assert result == counts.SourceOutcomeCounts(accepted, rejected)


def test_outcome_counts_are_immutable():
    result = counts.SourceOutcomeCounts(2, 3)
    with pytest.raises(FrozenInstanceError):
        result.rejection_count = 4


@pytest.mark.parametrize("phase", ["graph", "rejected", "output", "verifying", "verified"])
def test_admitted_phase_includes_rejected_without_a_family_plan(phase):
    counts._require_admitted(SimpleNamespace(phase=phase, source_frozen_at=object()))


@pytest.mark.parametrize("phase,frozen", [("source", None), ("admission", object()), ("graph", None)])
def test_mutable_source_or_unfinished_admission_cannot_count(phase, frozen):
    with pytest.raises(CandidateRunnerError, match="completed source admission"):
        counts._require_admitted(SimpleNamespace(phase=phase, source_frozen_at=frozen))


@pytest.mark.parametrize("row_cap", [2, 100_000])
def test_first_root_only_errors_match_eager_with_child_first_evidence(monkeypatch, row_cap):
    monkeypatch.setattr(counts, "MAX_BATCH_ROWS", row_cap)
    request = _request()
    roots = [_root(), _root(score=None), _root("1234567893"), None, _root(npi=None)]
    child_records = [_child(amount="bad"), _child(), _child("absent", amount=None), None]
    legacy = reject_duplicate_canonical_root_keys(
        request.definition, assemble_root_families(request.definition, roots, {"details": child_records})
    )
    occurrence_rows = [
        _evidence(request, child, child=True, resolved="orphan_child" if index == 2 else None)
        for index, child in enumerate(child_records)
    ] + [_evidence(request, root) for root in roots]
    result = _evaluate_outcome_query(request, occurrence_rows)
    _assert_counts(result, len(legacy.families), len(legacy.rejections))
    assert result == counts.SourceOutcomeCounts(1, 4)


@pytest.mark.parametrize("rejected_second", [False, True])
def test_typed_decimal_collisions_preserve_invalid_family_codes(monkeypatch, rejected_second):
    monkeypatch.setattr(counts, "MAX_BATCH_ROWS", 2)
    document = json.loads(_request().definition.canonical)
    document["schema"]["root"]["logical_key"] = ["score"]
    document["schema"]["children"][0]["parent_key"] = [{"child": "amount", "root": "score"}]
    request = _request(definition=CustomImportDefinition.from_mapping(document))
    roots = [_root(score=1), _root(score="1.0") | ({"enabled": None} if rejected_second else {})]
    legacy = reject_duplicate_canonical_root_keys(
        request.definition, assemble_root_families(request.definition, roots, {"details": []})
    )
    _assert_counts(
        _evaluate_outcome_query(request, [_evidence(request, root) for root in roots]),
        len(legacy.families),
        len(legacy.rejections),
    )
    assert (len(legacy.families), len(legacy.rejections)) == (0, 2)


@pytest.mark.parametrize(
    "initial,resolved,rejected",
    [
        (None, None, 0),
        (None, "duplicate_child_key", 1),
        ("field_type_invalid", "field_type_invalid", 1),
        ("required_field_null", "orphan_child", 0),
        ("child_not_object", "child_not_object", 0),
        ("orphan_child", "orphan_child", 0),
        ("child_key_missing", "child_key_missing", 1),
        (None, "child_membership_missing", 1),
    ],
)
def test_child_error_priority_and_candidate_only_exclusion(monkeypatch, initial, resolved, rejected):
    monkeypatch.setattr(counts, "MAX_BATCH_ROWS", 2)
    request = _request()
    occurrence_rows = [
        _evidence(request, _root()),
        _evidence(request, _child(), child=True, initial=initial, resolved=resolved),
    ]
    _assert_counts(_evaluate_outcome_query(request, occurrence_rows), 1 - rejected, rejected)


@pytest.mark.parametrize(
    "initial,resolved", [("future_error", None), (None, "future_error"), (None, "required_field_null")]
)
def test_unknown_or_unbacked_child_codes_fail_closed(initial, resolved):
    request = _request()
    with pytest.raises(CandidateRunnerError, match="unknown or unbacked rejection"):
        _evaluate_outcome_query(
            request,
            [_evidence(request, _root()), _evidence(request, _child(), child=True, initial=initial, resolved=resolved)],
        )


@pytest.mark.parametrize("size", [1, 1001])
def test_large_family_reduces_duplicate_codes_and_excludes_retained_and_foreign_build(monkeypatch, size):
    monkeypatch.setattr(counts, "MAX_BATCH_ROWS", 64)
    request = _request()
    occurrence_rows = [_evidence(request, _root())]
    occurrence_rows.extend(
        _evidence(request, _child(), child=True, resolved="duplicate_child_key") for _ in range(size)
    )
    occurrence_rows.extend(
        [
            _evidence(request, _child(amount=None), child=True),
            _evidence(request, _root(), origin="retained", initial="future_error"),
            _evidence(request, _root(), build_id=6, initial="future_error"),
        ]
    )
    _assert_counts(_evaluate_outcome_query(request, occurrence_rows), 0, 2)


@pytest.mark.parametrize("child", [False, True])
def test_complete_raw_identity_is_required_for_same_digest(monkeypatch, child):
    monkeypatch.setattr(counts, "MAX_BATCH_ROWS", 2)
    request = _request()
    first = _evidence(request, _root())
    collision = _evidence(request, _child() if child else _root(), child=child)
    collision["raw_parent_key_canonical"] += "different"
    with pytest.raises(CandidateRunnerError, match="raw-key digest collision"):
        _evaluate_outcome_query(request, [first, collision])


def test_empty_source_and_missing_typed_or_unknown_collection_evidence():
    request = _request()
    _assert_counts(_evaluate_outcome_query(request, []), 0, 0)
    with pytest.raises(CandidateRunnerError, match="typed root revision"):
        _evaluate_outcome_query(request, [_evidence(request, _root(), root_revision_id=None)])
    with pytest.raises(CandidateRunnerError, match="collection differs"):
        _evaluate_outcome_query(request, [_evidence(request, _child(), child=True, collection_slot=99)])


@pytest.mark.parametrize("family_id", [23, 24])
def test_postgres_query_uses_bound_hot_tables_without_payloads(family_id):
    statement = counts._partial_query(5, snapshot_models(family_id), 20, None, (b"x" * 32, 20), False, (1,))
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert f"ci_snapshot_{family_id}.custom_import_build_occurrence" in sql
    assert sql.count(f"ci_snapshot_{family_id}.custom_import_rejection AS") == 2
    assert "mrf.custom_import_build_occurrence" not in sql and "mrf.custom_import_rejection" not in sql
    assert "canonical_payload" not in sql and "canonical_evidence" not in sql
    assert "OFFSET" not in sql and "EXISTS" in sql
    assert "custom_import_build_occurrence.raw_parent_key_sha256," in sql and " <= " in sql
    assert "MATERIALIZED" in sql and 'COLLATE "C"' in sql
    assert sql.count("custom_import_build_occurrence.record_kind =") == 2
    assert "custom_import_build_occurrence.occurrence_id = source_outcome_ids.occurrence_id" in sql
    assert 'raw_parent_key_canonical COLLATE "C") != first_root.raw_key' in sql


@pytest.mark.parametrize("row_cap", [4, 100_000])
def test_fresh_binding_and_query_counts_follow_physical_pages(monkeypatch, row_cap):
    monkeypatch.setattr(counts, "MAX_BATCH_ROWS", row_cap)
    request, events = _request(page_row_limit=1), []
    records = [_evidence(request, _root())] + [
        _evidence(request, _child(key=str(index)), child=True) for index in range(8)
    ]
    assert _evaluate_outcome_query(request, records, events=events) == counts.SourceOutcomeCounts(1, 0)
    page_count = (len(records) + row_cap // 2 - 1) // (row_cap // 2)
    assert sum("source_outcome_evidence AS MATERIALIZED" in item for item in events) == page_count
    assert sum(item.startswith("WITH source_outcome_metadata") for item in events) == 2 * (page_count + 1)
    assert events.count("bind") == 2 * (page_count + 1)
    assert all(
        events[index - 1] == "bind"
        for index, item in enumerate(events)
        if item.startswith("WITH source_outcome_metadata") and index > 3
    )


def test_byte_windows_reserve_only_one_boundary_family(monkeypatch):
    request = _request()
    root = _evidence(request, _root())
    summary_bytes = len(root["raw_parent_key_canonical"].encode()) + counts._SUMMARY_BYTES
    byte_cap, budgets, events = 3 * summary_bytes, [], []
    monkeypatch.setattr(counts, "MAX_BATCH_BYTES", byte_cap)
    original = counts._metadata_query

    def metadata_query(*arguments):
        budgets.append(arguments[-1])
        return original(*arguments)

    monkeypatch.setattr(counts, "_metadata_query", metadata_query)
    occurrence_rows = [root] + [_evidence(request, _child(), child=True) for _ in range(8)]
    assert _evaluate_outcome_query(request, occurrence_rows, events=events) == counts.SourceOutcomeCounts(1, 0)
    unkeyed_pages = (len(occurrence_rows) + byte_cap // counts._SUMMARY_BYTES - 1) // (
        byte_cap // counts._SUMMARY_BYTES
    )
    assert budgets == [byte_cap] + [byte_cap - summary_bytes] * 4 + [byte_cap] * (unkeyed_pages + 1)
    assert sum("source_outcome_evidence AS MATERIALIZED" in statement for statement in events) == 4


def test_oversized_raw_key_stops_before_evidence_query():
    request, events = _request(), []
    root = _evidence(request, _root())
    with pytest.raises(CandidateRunnerError, match="admitted byte page"):
        _evaluate_outcome_query(replace(request, page_byte_limit=1), [root], events=events)
    assert not any("source_outcome_evidence AS MATERIALIZED" in statement for statement in events)


@pytest.mark.parametrize("child", [False, True])
def test_missing_rejection_row_fails_even_without_a_root_family(child):
    request = _request()
    evidence = _evidence(
        request, _child() if child else _root(), child=child, initial="required_field_null", missing_rejection=True
    )
    with pytest.raises(CandidateRunnerError, match="unknown or unbacked rejection"):
        _evaluate_outcome_query(request, [evidence])


def test_typed_duplicates_require_distinct_complete_raw_keys(monkeypatch):
    monkeypatch.setattr(counts, "MAX_BATCH_ROWS", 2)
    request = _request()
    first = _evidence(request, _root())
    second = first | {"raw_parent_key_sha256": b"x" * 32}
    assert _evaluate_outcome_query(request, [first, second]) == counts.SourceOutcomeCounts(2, 0)


def test_unkeyed_scan_bounds_all_source_ids_before_filtering():
    model = snapshot_models(23)[counts.CustomImportBuildOccurrence]
    statement = counts._indexed_source_rows(model, 5, 20, (None, 4), True)
    sql = str(statement.select().compile(dialect=postgresql.dialect()))
    predicates = sql.split("WHERE", 1)[1]
    assert "raw_parent_key_sha256" not in predicates
    assert "origin =" in predicates and "occurrence_id" in predicates and "LIMIT" in predicates


@pytest.mark.parametrize("failure_type", [CancellationRequested, LeaseAuthorityLost])
async def test_counts_require_fresh_authority_after_final_sql_and_close(monkeypatch, failure_type):
    events = []
    snapshot = AsyncMock(side_effect=[SimpleNamespace(phase="rejected", source_frozen_at=object()), failure_type()])
    monkeypatch.setattr(counts, "_snapshot", snapshot)
    monkeypatch.setattr(counts, "_count_source", lambda _scan: counts.SourceOutcomeCounts(1, 2))

    @asynccontextmanager
    async def renewal(*_args):
        yield

    @asynccontextmanager
    async def sessions(*_args):
        async def run_sync(callback):
            return callback(None)

        try:
            yield SimpleNamespace(run_sync=run_sync)
        finally:
            events.append("closed")

    monkeypatch.setattr(counts, "_renew_while_reading", renewal)
    monkeypatch.setattr(counts, "_session", sessions)
    with pytest.raises(failure_type):
        await counts.count_source_outcomes(None, _request(), 5)
    assert events == ["closed"] and snapshot.await_count == 2
