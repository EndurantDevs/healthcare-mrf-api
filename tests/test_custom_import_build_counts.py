# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy outcome parity, bounded evidence queries, and live read authority."""

from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager, nullcontext
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import dialect

from db.models.custom_import import CustomImportBuildOccurrence
from process.custom_import import build_counts as counts
from process.custom_import import build_graph as graph
from process.custom_import import build_source as source
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import assemble_root_families
from process.custom_import.runner import reject_duplicate_canonical_root_keys
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _child, _request, _root


def _scan(**changes):
    return counts._CountScan(None, _request(**changes), 5)


def _root_evidence(request=None, record=None, *, identifier=10, initial=None):
    request = request or _request()
    prepared = source._prepare_row(request, request.definition.source_streams[0], record or _root())
    return SimpleNamespace(
        occurrence_id=identifier,
        raw_parent_key_canonical=prepared.raw_key[0],
        raw_parent_key_sha256=prepared.raw_key[1],
        root_record_id=1,
        root_revision_id=None if prepared.rejection else identifier,
        initial_code=initial if prepared.rejection is None else prepared.rejection.code,
    )


def _child_evidence(root, initial=None, resolved=None):
    return SimpleNamespace(
        raw_parent_key_canonical=root.raw_parent_key_canonical,
        initial_code=initial,
        resolved_code=resolved,
    )


def _sql(statement):
    return str(statement.compile(dialect=dialect()))


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


@pytest.mark.parametrize(
    "initial,resolved,expected",
    [
        (None, None, None),
        (None, "duplicate_child_key", "duplicate_child_key"),
        ("field_type_invalid", "field_type_invalid", "field_type_invalid"),
        ("required_field_null", "orphan_child", None),
        ("child_not_object", "child_not_object", None),
        ("orphan_child", "orphan_child", None),
        ("child_key_missing", "child_key_missing", "child_key_missing"),
    ],
)
def test_child_local_error_priority_and_candidate_only_exclusion(initial, resolved, expected):
    root = _root_evidence()
    assert counts._child_code(_child_evidence(root, initial, resolved), root) == expected


@pytest.mark.parametrize(
    "initial,resolved", [("future_error", None), (None, "future_error"), (None, "required_field_null")]
)
def test_unknown_or_unbacked_child_codes_fail_closed(initial, resolved):
    root = _root_evidence()
    with pytest.raises(CandidateRunnerError):
        counts._child_code(_child_evidence(root, initial, resolved), root)


def test_same_raw_digest_never_substitutes_for_complete_parent_identity():
    root = _root_evidence()
    child = _child_evidence(root)
    child.raw_parent_key_canonical += "different"
    with pytest.raises(CandidateRunnerError, match="raw-key digest collision"):
        counts._child_code(child, root)


def test_malformed_root_occurrences_are_not_candidate_only_child_counts(monkeypatch):
    evidence_rows = [
        (1, "child", "child_not_object"),
        (2, "root", "root_not_object"),
        (3, "root", "root_key_missing"),
        (4, "child", "orphan_child"),
        (5, "root", "root_key_missing"),
        (6, "root", "required_field_null"),
    ]
    reader = Mock(return_value=(row for row in evidence_rows))
    monkeypatch.setattr(counts, "_rows", reader)
    assert counts._malformed_root_count(_scan()) == 3
    statement = reader.call_args.args[1]
    assert "canonical_evidence" not in _sql(statement)
    assert "record_kind =" not in _sql(statement)
    assert statement.compile(dialect=dialect()).params["origin_1"] == "source"


def test_first_root_only_errors_match_legacy_with_child_first_evidence(monkeypatch):
    request = _request()
    roots = [_root(), _root(score=None), _root("1234567893"), None, _root(npi=None)]
    child_records = [_child(amount="bad"), _child(), _child("absent", amount=None), None]
    legacy = reject_duplicate_canonical_root_keys(
        request.definition, assemble_root_families(request.definition, roots, {"details": child_records})
    )
    first, other = _root_evidence(identifier=3), _root_evidence(record=roots[2], identifier=5)
    keyed_roots = iter((first, other, None))
    monkeypatch.setattr(counts, "_next_root", lambda *_args: next(keyed_roots))
    monkeypatch.setattr(counts, "_collection_slots", lambda _scan: (1,))
    monkeypatch.setattr(counts, "_malformed_root_count", lambda _scan: 2)
    monkeypatch.setattr(counts, "_has_other_root", lambda _scan, root, *, typed: root is first and not typed)

    def rows(_scan, statement, _keys, *_args, **_kwargs):
        values = statement.compile(dialect=dialect()).params
        if values["raw_parent_key_sha256_1"] == first.raw_parent_key_sha256 and " IS NULL" in _sql(statement):
            return (row for row in (_child_evidence(first, "field_type_invalid", "field_type_invalid"),))
        return (row for row in ())

    monkeypatch.setattr(counts, "_rows", rows)
    outcome_counts = counts._count_source(counts._CountScan(None, request, 5))
    assert outcome_counts == counts.SourceOutcomeCounts(len(legacy.families), len(legacy.rejections))
    assert outcome_counts == counts.SourceOutcomeCounts(1, 4)
    assert [entry.code for entry in legacy.rejections] == [
        "root_not_object",
        "root_key_missing",
        "duplicate_root_key",
        "field_type_invalid",
    ]


@pytest.mark.parametrize("rejected_second", [False, True])
def test_distinct_raw_decimal_keys_sharing_typed_identity_match_legacy(monkeypatch, rejected_second):
    document = json.loads(_request().definition.canonical)
    document["schema"]["root"]["logical_key"] = ["score"]
    document["schema"]["children"][0]["parent_key"] = [{"child": "amount", "root": "score"}]
    request = _request(definition=CustomImportDefinition.from_mapping(document))
    second = {**_root(score="1.0"), "enabled": None} if rejected_second else _root(score="1.0")
    roots = [_root(score=1), second]
    legacy = reject_duplicate_canonical_root_keys(
        request.definition, assemble_root_families(request.definition, roots, {"details": []})
    )
    evidence_rows = [_root_evidence(request, record, identifier=index + 1) for index, record in enumerate(roots)]
    assert evidence_rows[0].raw_parent_key_sha256 != evidence_rows[1].raw_parent_key_sha256
    monkeypatch.setattr(counts, "_has_other_root", lambda *_args, typed: typed)
    monkeypatch.setattr(counts, "_rows", lambda *_args, **_kwargs: (row for row in ()))
    codes = [counts._family_codes(counts._CountScan(None, request, 5), root, (1,)) for root in evidence_rows]
    assert (
        counts.SourceOutcomeCounts(sum(not code for code in codes), sum(map(len, codes)))
        == (counts.SourceOutcomeCounts(len(legacy.families), len(legacy.rejections)))
        == counts.SourceOutcomeCounts(0, 2)
    )


def test_large_family_reduces_to_fixed_codes_and_visits_null_key_stage(monkeypatch):
    root, stages = _root_evidence(), []
    monkeypatch.setattr(counts, "_has_other_root", lambda *_args, **_kwargs: False)

    def rows(_scan, statement, keys, _columns, *, bounds):
        stages.append(tuple(key.name for key in keys))
        assert bounds.reserve_bytes >= len(root.raw_parent_key_canonical.encode("utf-8")) + 32
        if " IS NOT NULL" in _sql(statement):
            return (_child_evidence(root, None, "duplicate_child_key") for _index in range(1001))
        return (row for row in (_child_evidence(root, "required_field_null", "required_field_null"),))

    monkeypatch.setattr(counts, "_rows", rows)
    assert counts._family_codes(_scan(), root, (1,)) == {"duplicate_child_key", "required_field_null"}
    assert stages == [("child_key_sha256", "occurrence_id"), ("occurrence_id",)]


@pytest.mark.parametrize("keyed", [False, True])
def test_child_query_projects_only_bounded_evidence_with_exact_source_scope(keyed):
    statement, keys, columns = counts._child_statement(_scan(), _root_evidence(), 7, keyed=keyed)
    compiled = statement.compile(dialect=dialect())
    assert compiled.params["build_id_1"] == 5 and compiled.params["origin_1"] == "source"
    assert compiled.params["collection_slot_1"] == 7 and compiled.params["record_kind_1"] == "child"
    assert ("child_key_sha256 IS NOT NULL" if keyed else "child_key_sha256 IS NULL") in str(compiled)
    assert [column.name for column in columns] == ["raw_parent_key_canonical", "child_key_sha256", "code", "code"]
    assert [key.name for key in keys] == (["child_key_sha256", "occurrence_id"] if keyed else ["occurrence_id"])
    assert "canonical_evidence" not in str(compiled) and "canonical_payload" not in str(compiled)


@pytest.mark.parametrize("typed", [False, True])
def test_duplicate_exists_is_source_scoped_and_compares_full_raw_identity(monkeypatch, typed):
    session = SimpleNamespace(execute=Mock(return_value=SimpleNamespace(scalar_one=lambda: True)))
    monkeypatch.setattr(counts, "_read_transaction", lambda *_args: nullcontext())
    assert counts._has_other_root(counts._CountScan(session, _request(), 5), _root_evidence(), typed=typed)
    statement = session.execute.call_args.args[0]
    compiled = statement.compile(dialect=dialect())
    assert "EXISTS" in str(compiled) and compiled.params["origin_1"] == "source"
    assert ("raw_parent_key_canonical !=" if typed else "raw_parent_key_canonical =") in str(compiled)
    assert ("root_record_id_1" if typed else "raw_parent_key_sha256_1") in compiled.params


def test_next_root_skips_complete_raw_bucket_and_reads_only_first_root(monkeypatch):
    reader = Mock(return_value=(row for row in ()))
    monkeypatch.setattr(counts, "_rows", reader)
    assert counts._next_root(_scan(), b"x" * 32) is None
    statement = reader.call_args.args[1]
    compiled = statement.compile(dialect=dialect())
    assert compiled.params["origin_1"] == "source" and compiled.params["record_kind_1"] == "root"
    assert "raw_parent_key_sha256 >" in str(compiled) and "OFFSET" not in str(compiled)
    assert reader.call_args.kwargs["bounds"].row_limit == 1
    assert [key.name for key in reader.call_args.args[2]] == ["raw_parent_key_sha256", "occurrence_id"]


def test_error_code_bytes_are_admitted_before_fetching_payload(monkeypatch):
    request = _request(page_byte_limit=127)
    session = SimpleNamespace(
        execute=Mock(return_value=SimpleNamespace(all=lambda: [(1, 128)])),
        info={"custom_import_build_read_deadline": time.monotonic() + 30},
    )
    monkeypatch.setattr(graph, "_read_transaction", lambda *_args: nullcontext((None, time.monotonic() + 30)))
    occurrence = CustomImportBuildOccurrence
    statement = select(occurrence.occurrence_id, occurrence.raw_parent_key_canonical)
    with pytest.raises(CandidateRunnerError, match="byte page"):
        list(
            counts._rows(
                counts._CountScan(session, request, 5),
                statement,
                (occurrence.occurrence_id,),
                (occurrence.raw_parent_key_canonical,),
            )
        )
    session.execute.assert_called_once()
    assert "octet_length" in _sql(session.execute.call_args.args[0])


@pytest.mark.parametrize("failure_type", [CancellationRequested, LeaseAuthorityLost])
async def test_counts_require_fresh_authority_after_scan_and_close(monkeypatch, failure_type):
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


def test_missing_typed_revision_cannot_be_counted_as_accepted(monkeypatch):
    root = _root_evidence()
    root.root_revision_id = None
    monkeypatch.setattr(counts, "_has_other_root", lambda *_args, **_kwargs: False)
    with pytest.raises(CandidateRunnerError, match="typed root revision"):
        counts._family_codes(_scan(), root, ())


def test_collection_names_remain_definition_bounded(monkeypatch):
    monkeypatch.setattr(counts, "_rows", lambda *_args, **_kwargs: (row for row in ((9, "details"),)))
    assert counts._collection_slots(_scan()) == (9,)
    monkeypatch.setattr(
        counts, "_rows", lambda *_args, **_kwargs: (row for row in ((9, "details"), (10, "unexpected")))
    )
    with pytest.raises(CandidateRunnerError, match="differs"):
        counts._collection_slots(_scan())


@pytest.mark.parametrize("scan_kind", ["malformed", "collections", "children", "next_root"])
def test_count_iterators_close_explicitly_on_early_error_or_return(monkeypatch, scan_kind):
    closed_readers = []
    root = _root_evidence()
    records_by_kind = {
        "malformed": (1, "root", "unknown_error"),
        "collections": (1, "unknown_collection"),
        "children": _child_evidence(root, "unknown_error"),
        "next_root": root,
    }

    def rows(*_args, **_kwargs):
        try:
            yield records_by_kind[scan_kind]
            raise AssertionError("the rejected or satisfied scan must not consume another record")
        finally:
            closed_readers.append(scan_kind)

    monkeypatch.setattr(counts, "_rows", rows)
    operations_by_kind = {
        "malformed": lambda: counts._malformed_root_count(_scan()),
        "collections": lambda: counts._collection_slots(_scan()),
        "children": lambda: counts._collect_child_errors(_scan(), root, 1, False, 0),
        "next_root": lambda: counts._next_root(_scan(), None),
    }
    with pytest.raises(CandidateRunnerError) if scan_kind != "next_root" else nullcontext():
        operations_by_kind[scan_kind]()
    assert closed_readers == [scan_kind]
