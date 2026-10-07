# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete winner groups and frozen proof identity precede ordinary finality."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildCandidateContext,
    CustomImportBuildVerification,
    CustomImportChildRevision,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportRootRevision,
    CustomImportRootScalar,
)
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import materialization as material
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRegistry, CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _read_session, _registry, _request
from tests.test_custom_import_build_graph_prepare import child_row, root_row
from tests.test_custom_import_definition import _root
from tests.test_custom_import_materialization import _child_candidate


def _generation(request):
    return CustomImportGeneration(
        generation_id=10,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=20,
        producing_fence=request.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )


def _candidate(family_id=30, child_id=40, digest=b"f" * 32):
    candidate = _child_candidate(
        family_revision_id=family_id, child_revision_id=child_id, semantic_suffix="synthetic", service_code="A100"
    )
    return replace(candidate, family_sha256=digest)


def _group(request, candidate):
    registry = CandidateRegistry({"rates": 7}, {"providers": 1, "rates": 2}, 1)
    contract = material._winner_candidate_contract(
        request.definition, material._profile_scopes(request.definition, registry.child_collection_slots)
    )
    normalized = material._normalize_winner_candidate(candidate, contract)
    _, digest = material._context_key(request.definition.selection_profiles[0], normalized, contract.fields_by_id)
    return registry, (1, candidate.entity_binding_id, digest)


@pytest.mark.asyncio
async def test_membership_retry_pages_over_completed_keys(monkeypatch):
    pages = [
        SimpleNamespace(after_root_record_id=2, rows_processed=2, inserted_count=1),
        SimpleNamespace(after_root_record_id=3, rows_processed=1, inserted_count=1),
        SimpleNamespace(after_root_record_id=3, rows_processed=0, inserted_count=0),
    ]
    session = SimpleNamespace(add_all=Mock(side_effect=AssertionError("memberships require protected set writes")))
    commits = []

    @asynccontextmanager
    async def _page(*_args):
        yield session, SimpleNamespace(generation_id=10)
        commits.append(True)

    async def _typed_call(_session, name, arguments):
        index = len(commits)
        assert name == "membership_batch_finalize"
        assert arguments == (("bigint", 7), ("bigint", (0, 2, 3)[index]))
        return SimpleNamespace(one=lambda: pages[index])

    monkeypatch.setattr(output, "_page_session", _page)
    monkeypatch.setattr(output, "_typed_call", _typed_call)
    monkeypatch.setattr(output, "_heartbeat", AsyncMock())
    await output._attach_families(None, _request(page_row_limit=2), 7)
    assert len(commits) == 3
    session.add_all.assert_not_called()


def _winner_page_lookup(monkeypatch, request, first):
    monkeypatch.setattr(
        output,
        "_context_rows",
        lambda *_args, **kwargs: (row for row in (_context_row(request, first),)),
    )
    lookup = Mock(return_value=(99,))
    monkeypatch.setattr(output, "_selected_winner_context_ids", lookup)
    return lookup


def test_reducer_exhausts_one_large_group_including_losers(monkeypatch):
    request = _request()
    first = _candidate()
    registry, _group_key = _group(request, first)
    exhausted_flags = []

    def _candidates(*_args, **kwargs):
        for index in range(5000):
            yield _candidate(30 + index, 40 + index, (5000 - index).to_bytes(32, "big"))
        exhausted_flags.append(True)

    monkeypatch.setattr(output, "_context_candidates", _candidates)
    lookup = _winner_page_lookup(monkeypatch, request, first)
    assert output._winner_batch(None, request, registry, 7, _generation(request), None) == ((99,), (1,))
    assert exhausted_flags == [True]
    assert lookup.call_args.args[3][0].family_revision_id == 5029
    assert lookup.call_count == 1


@pytest.mark.parametrize("has_better", [False, True])
def test_surviving_tie_rules_are_the_existing_comparator(monkeypatch, has_better):
    request = _request()
    first = _candidate()
    conflict = replace(first, family_revision_id=31, context_child_revision_id=41)
    better = _candidate(32, 42, b"\x00" * 32)
    registry, _group_key = _group(request, first)
    candidates = (first, conflict, better) if has_better else (first, conflict)
    monkeypatch.setattr(output, "_context_candidates", lambda *_args, **kwargs: iter(candidates))
    lookup = _winner_page_lookup(monkeypatch, request, first)
    if not has_better:
        with pytest.raises(material.WinnerMaterializationError, match="conflicting physical identity"):
            output._winner_batch(None, request, registry, 7, _generation(request), None)
        lookup.assert_not_called()
        return
    assert output._winner_batch(None, request, registry, 7, _generation(request), None) == ((99,), (1,))
    assert lookup.call_args.args[3][0].family_revision_id == 32
    assert lookup.call_count == 1


def test_late_group_failure_cannot_produce_a_winner(monkeypatch):
    request = _request()
    first = _candidate()
    registry, _group_key = _group(request, first)

    def _candidates(*_args, **kwargs):
        yield first
        raise CandidateRunnerError("late candidate validation")

    monkeypatch.setattr(output, "_context_candidates", _candidates)
    lookup = _winner_page_lookup(monkeypatch, request, first)
    with pytest.raises(CandidateRunnerError, match="late candidate"):
        output._winner_batch(None, request, registry, 7, _generation(request), None)
    lookup.assert_not_called()


def _context_row(request, candidate, stored=None):
    registry, group = _group(request, candidate)
    contract = material._winner_candidate_contract(
        request.definition, material._profile_scopes(request.definition, registry.child_collection_slots)
    )
    canonical, digest = material._context_key(
        request.definition.selection_profiles[0],
        material._normalize_winner_candidate(candidate, contract),
        contract.fields_by_id,
    )
    context = CustomImportBuildCandidateContext(
        candidate_context_id=candidate.context_child_revision_id,
        profile_slot=group[0],
        entity_binding_id=candidate.entity_binding_id,
        context_collection_slot=candidate.context_collection_slot,
        canonical_context_key=canonical,
        context_key_sha256=digest,
    )
    family = CustomImportFamilyRevision(
        family_revision_id=candidate.family_revision_id,
        family_sha256=candidate.family_sha256,
        entity_binding_id=candidate.entity_binding_id,
    )
    root = CustomImportRootRevision(canonical_payload=output.record_payload(request.definition.root_fields, _root()))
    child = CustomImportChildRevision(
        child_revision_id=candidate.context_child_revision_id,
        collection_slot=candidate.context_collection_slot,
        child_key_sha256=candidate.context_child_key_sha256,
        canonical_payload=output.record_payload(
            output.fields_by_collection(request.definition)["rates"],
            {"rate_npi": "1234567893", **candidate.values_by_field},
        ),
    )
    return (context, family, root, child, *(stored or (None, None, None)))


def _output_read_session(monkeypatch, pages):
    monkeypatch.setattr(output, "_build_storage_models", lambda *_args: (snapshot_models(41), None))
    session = _read_session(monkeypatch, pages)
    monkeypatch.setattr(output, "_read_transaction", graph._read_transaction)
    monkeypatch.setattr(output, "_prepare_read", graph._prepare_read)
    return session


def _context_metadata(context_records):
    return [
        (
            *output._group_key(context_record[0]),
            context_record[0].candidate_context_id,
            output._model_bytes(context_record[:4]),
            len(context_record[0].canonical_context_key.encode("utf-8")),
        )
        for context_record in context_records
    ]


def _context_session(monkeypatch, rows, page_rows=2):
    pages = []
    for start in range(0, len(rows), page_rows):
        page = rows[start : start + page_rows]
        pages.extend((_context_metadata(page), page))
    return _output_read_session(monkeypatch, [*pages, []])


def test_ordered_verification_reads_pages_across_complete_groups(monkeypatch):
    request = _request(page_row_limit=2)
    first = _candidate()
    best = _candidate(32, 42, b"\x00" * 32)
    later_candidates = [replace(_candidate(40 + index, 50 + index), entity_binding_id=42 + index) for index in range(3)]
    stored = (best.family_revision_id, 7, best.context_child_revision_id)
    rows = [_context_row(request, candidate, stored) for candidate in (first, _candidate(31, 41), best)]
    rows.extend(
        _context_row(request, candidate, (candidate.family_revision_id, 7, candidate.context_child_revision_id))
        for candidate in later_candidates
    )
    session = _context_session(monkeypatch, rows)
    lookup = Mock(side_effect=AssertionError("winner verification must not query individual groups"))
    monkeypatch.setattr(output, "_one_row", lookup)
    registry, _ = _group(request, first)
    assert output._verify_winner_groups(session, request, registry, 7, _generation(request)) == 4
    assert session.execute.call_count == 7
    metadata, payload = [
        call.args[0].compile(dialect=postgresql.dialect()) for call in session.execute.call_args_list[:2]
    ]
    assert metadata.params["param_1"] == (output.MAX_BATCH_ROWS - 1) // 5 and "octet_length" in str(metadata)
    assert "ANY" in str(payload) and "context_ids" in payload.params
    assert "LEFT OUTER JOIN ci_snapshot_41.custom_import_winner" in str(payload)
    assert "custom_import_winner.generation_id" in str(payload)
    assert "custom_import_winner.context_key_sha256" in str(payload)
    lookup.assert_not_called()


@pytest.mark.parametrize("stored", [(None, None, None), (99, 7, 40), (30, 0, None), (30, 7, 99)])
def test_ordered_verification_rejects_missing_or_changed_winner(monkeypatch, stored):
    request, candidate = _request(), _candidate()
    session = _context_session(monkeypatch, [_context_row(request, candidate, stored)])
    registry, _ = _group(request, candidate)
    with pytest.raises(CandidateRunnerError, match="surviving-tie reduction"):
        output._verify_winner_groups(session, request, registry, 7, _generation(request))


@pytest.mark.parametrize("has_better", [False, True])
def test_ordered_groups_preserve_surviving_tie_conflicts(monkeypatch, has_better):
    request, first = _request(page_row_limit=2), _candidate()
    candidates = [first, _candidate(31, 41)]
    if has_better:
        candidates.append(_candidate(32, 42, b"\x00" * 32))
    session = _context_session(monkeypatch, [_context_row(request, candidate) for candidate in candidates])
    registry, _ = _group(request, first)
    winners = output._ordered_winners(session, request, registry, 7, _generation(request))
    if not has_better:
        with pytest.raises(material.WinnerMaterializationError, match="conflicting physical identity"):
            next(winners)
    else:
        assert [winner.family_revision_id for winner in winners] == [32]


@pytest.mark.parametrize("failure", ["collision", "payload"])
def test_ordered_group_rechecks_losers_before_yield(monkeypatch, failure):
    request, first = _request(), _candidate()
    other = _candidate(31, 41)
    if failure == "collision":
        context_key = material._context_key
        monkeypatch.setattr(material, "_context_key", lambda *args: (context_key(*args)[0], b"c" * 32))
        other = replace(other, values_by_field={"service_code": "B200", "amount": Decimal("99")})
    rows = [_context_row(request, candidate) for candidate in (first, other)]
    if failure == "payload":
        rows[1][3].canonical_payload = "{}"
    session = _context_session(monkeypatch, rows, page_rows=1)
    registry, _ = _group(request, first)
    failure_type = material.WinnerMaterializationError if failure == "collision" else CandidateRunnerError
    with pytest.raises(failure_type):
        next(output._ordered_winners(session, request, registry, 7, _generation(request)))


def test_ordered_group_resume_uses_fresh_read_and_complete_cursor(monkeypatch):
    request, first = _request(page_row_limit=2), _candidate()
    later = replace(_candidate(31, 41), entity_binding_id=42)
    rows = [_context_row(request, candidate) for candidate in (first, later)]
    registry, _ = _group(request, first)
    session = _context_session(monkeypatch, rows)
    winners = output._ordered_winners(session, request, registry, 7, _generation(request))
    winner = next(winners)
    monkeypatch.setattr(graph.time, "monotonic", lambda: 21)
    with pytest.raises(LeaseAuthorityLost):
        next(winners)
    resumed = _context_session(monkeypatch, rows[1:])
    after = (winner.profile_slot, winner.entity_binding_id, winner.context_key_sha256)
    assert [
        winner.family_revision_id
        for winner in output._ordered_winners(resumed, request, registry, 7, _generation(request), after=after)
    ] == [31]
    parameters = resumed.execute.call_args_list[0].args[0].compile().params
    assert all(value in parameters.values() for value in after)
    assert " > " in str(resumed.execute.call_args_list[0].args[0])


def test_ordered_context_metadata_rejects_oversized_payload_before_read(monkeypatch):
    request, candidate = _request(page_byte_limit=1), _candidate()
    row = _context_row(request, candidate)
    session = _context_session(monkeypatch, [row])
    registry, _ = _group(request, candidate)
    with pytest.raises(CandidateRunnerError, match="byte page"):
        next(output._ordered_winners(session, request, registry, 7, _generation(request)))
    assert session.execute.call_count == 1


def _winner(request, candidate=None):
    candidate = _candidate() if candidate is None else candidate
    registry, group = _group(request, candidate)
    return output._complete_group_winner(request, registry, _generation(request), group, iter((candidate,)))


def test_context_reads_coalesce_logical_pages(monkeypatch):
    request, first = _request(page_row_limit=2), _candidate()
    candidates = [
        replace(
            first, family_revision_id=100 + index, context_child_revision_id=200 + index, entity_binding_id=300 + index
        )
        for index in range(257)
    ]
    context_records = [_context_row(request, candidate) for candidate in candidates]
    request = replace(
        request, page_byte_limit=max(output._model_bytes(context_record[:4]) for context_record in context_records)
    )
    session = _context_session(monkeypatch, context_records, page_rows=257)
    registry, _ = _group(request, first)
    assert len(list(output._ordered_winners(session, request, registry, 7, _generation(request)))) == 257
    assert session.execute.call_count == 3  # One metadata/payload pair and EOF.
    compiled_queries = [call.args[0].compile(dialect=postgresql.dialect()) for call in session.execute.call_args_list]
    assert len(compiled_queries[1].params["context_ids"]) == 257
    assert len(compiled_queries[1].params) == 2
    assert not any(binding.expanding for binding in compiled_queries[1].binds.values())
    assert "ANY" in str(compiled_queries[1])


def test_lookup_uses_six_native_parameters(monkeypatch):
    request = _request(page_row_limit=2)
    first = _winner(request)
    winners = [
        replace(
            first,
            entity_binding_id=100 + index,
            family_revision_id=500 + index,
            context_child_revision_id=None if index % 2 else 800 + index,
        )
        for index in range(257)
    ]
    session = _output_read_session(monkeypatch, [[(index + 1, 1000 + index) for index in range(257)]])
    assert output._selected_winner_context_ids(session, request, 7, winners) == tuple(range(1000, 1257))
    session.execute.assert_called_once()
    compiled = session.execute.call_args.args[0].compile(dialect=postgresql.dialect())
    assert len(compiled.params) == 6 and not any(binding.expanding for binding in compiled.binds.values())
    assert compiled.params["child_ids"][1] is None
    assert "WITH ORDINALITY" in str(compiled) and "IS NOT DISTINCT FROM" in str(compiled)
    assert "LIMIT 258" in str(compiled) and "VALUES" not in str(compiled)


def test_winner_batch_preserves_logical_sizes(monkeypatch):
    request, first = _request(page_row_limit=256, page_byte_limit=96), _candidate()
    registry, _ = _group(request, first)
    selected_winner = _winner(request)
    closed_streams = []

    def stream(*_args, **_kwargs):
        try:
            yield from (replace(selected_winner, family_revision_id=100 + index) for index in range(257))
        finally:
            closed_streams.append(True)

    def lookup(_session, _request, _build_id, winners):
        assert closed_streams == [True]
        return tuple(winner.family_revision_id for winner in winners)

    monkeypatch.setattr(output, "_ordered_winners", stream)
    lookup_call = Mock(side_effect=lookup)
    monkeypatch.setattr(output, "_selected_winner_context_ids", lookup_call)
    selected, sizes = output._winner_batch(None, request, registry, 7, _generation(request), None)
    assert selected == tuple(range(100, 357)) and sizes == (3,) * 85 + (2,)
    lookup_call.assert_called_once()


def test_batch_full_never_completes_partial_group(monkeypatch):
    request, first = _request(), _candidate()
    later = replace(_candidate(31, 41), entity_binding_id=first.entity_binding_id + 1)
    best = replace(later, family_revision_id=32, context_child_revision_id=42, family_sha256=b"\x00" * 32)
    context_records = [_context_row(request, candidate) for candidate in (first, later, best)]
    registry, _ = _group(request, first)
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 6)
    lookup = Mock(
        side_effect=lambda _session, _request, _build, winners: tuple(winner.family_revision_id for winner in winners)
    )
    monkeypatch.setattr(output, "_selected_winner_context_ids", lookup)
    session = _context_session(monkeypatch, context_records, page_rows=1)
    assert output._winner_batch(session, request, registry, 7, _generation(request), None) == ((30,), (1,))
    assert session.execute.call_count == 4
    resumed = _context_session(monkeypatch, context_records[1:], page_rows=1)
    assert output._winner_batch(
        resumed, request, registry, 7, _generation(request), output._group_key(context_records[0][0])
    ) == ((32,), (1,))
    assert resumed.execute.call_count == 5


@pytest.mark.parametrize("failure", ["missing", "duplicate", "order", "binding"])
def test_context_payload_requires_complete_ordered_keys(monkeypatch, failure):
    request = _request()
    first = _context_row(request, _candidate())
    second = _context_row(request, replace(_candidate(31, 41), entity_binding_id=first[0].entity_binding_id + 1))
    metadata = _context_metadata([first, second])
    actual = {"missing": [first], "duplicate": [first, first], "order": [second, first], "binding": [first, second]}[
        failure
    ]
    if failure == "binding":
        second[0].entity_binding_id += 1
    session = _output_read_session(monkeypatch, [metadata, actual])
    with pytest.raises(CandidateRunnerError, match="frozen build page changed"):
        list(output._context_rows(session, request, 7))


@pytest.mark.parametrize("column,bad_value", [(0, None), (1, True), (2, None), (3, 0), (4, None), (5, 0)])
def test_context_metadata_requires_native_types(monkeypatch, column, bad_value):
    request = _request()
    metadata = list(_context_metadata([_context_row(request, _candidate())])[0])
    metadata[column] = bad_value
    session = _output_read_session(monkeypatch, [[tuple(metadata)]])
    with pytest.raises(CandidateRunnerError, match="invalid native"):
        list(output._context_rows(session, request, 7))
    session.execute.assert_called_once()


def test_context_payload_rejects_null_identity(monkeypatch):
    request = _request()
    context_record = _context_row(request, _candidate())
    metadata = _context_metadata([context_record])
    context_record[0].context_key_sha256 = None
    session = _output_read_session(monkeypatch, [metadata, [context_record]])
    with pytest.raises(CandidateRunnerError, match="invalid native"):
        list(output._context_rows(session, request, 7))


@pytest.mark.parametrize("child_id", [True, 0, -1, "1"])
def test_lookup_rejects_invalid_child_types(monkeypatch, child_id):
    request = _request()
    session = _output_read_session(monkeypatch, [])
    with pytest.raises(CandidateRunnerError, match="native child"):
        output._selected_winner_context_ids(
            session, request, 7, [replace(_winner(request), context_child_revision_id=child_id)]
        )
    session.execute.assert_not_called()


@pytest.mark.parametrize(
    "actual",
    [
        [],
        [(1, 90)],
        [(1, 90), (1, 91)],
        [(1, 90), (2, 90)],
        [(None, 90), (2, 91)],
        [(True, 90), (2, 91)],
        [(1, None), (2, 91)],
    ],
)
def test_lookup_rejects_missing_duplicate_or_null(monkeypatch, actual):
    request = _request()
    first = _winner(request)
    session = _output_read_session(monkeypatch, [actual])
    with pytest.raises(CandidateRunnerError, match="unique"):
        output._selected_winner_context_ids(session, request, 7, [first, replace(first, family_revision_id=31)])


@pytest.mark.parametrize("operation", ["context", "lookup"])
def test_output_reads_reject_stale_bindings(monkeypatch, operation):
    request = _request()
    session = _output_read_session(monkeypatch, [])
    monkeypatch.setattr(output, "_build_storage_models", Mock(side_effect=CandidateRunnerError("stale binding")))
    with pytest.raises(CandidateRunnerError, match="stale binding"):
        if operation == "context":
            list(output._context_rows(session, request, 7))
        else:
            output._selected_winner_context_ids(session, request, 7, [_winner(request)])
    session.execute.assert_not_called()


def test_lookup_rechecks_read_deadline(monkeypatch):
    request = _request()
    session = _output_read_session(monkeypatch, [[(1, 90)]])

    def expire(*_args):
        monkeypatch.setattr(graph.time, "monotonic", lambda: 21)

    monkeypatch.setattr(output, "_prepare_read", expire)
    with pytest.raises(LeaseAuthorityLost):
        output._selected_winner_context_ids(session, request, 7, [_winner(request)])


def test_context_capacity_reserves_winners_and_arrays(monkeypatch):
    request = _request(page_row_limit=1, page_byte_limit=10)
    winner = _winner(request)
    budget = output._WinnerReadBudget([winner] * 3, pending_bytes=250)
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 100)
    monkeypatch.setattr(output, "MAX_BATCH_BYTES", 10_000)
    row_limit, byte_limit = output._context_capacity(budget)
    assert row_limit * 5 + len(budget.winners) * 3 + 1 <= 100
    assert (
        byte_limit
        + 250
        + output._LOOKUP_ARRAY_HEADERS
        + 3 * output._winner_buffer_bytes(winner.canonical_context_key, winner.profile_id)
        == 10_000
    )
    metadata = [(1, 2, b"x" * 32, identity, 10, 1) for identity in (3, 4)]
    size = 100 + 10 + output._WINNER_BUFFER_BYTES + 1 + len(request.definition.selection_profiles[0].profile_id)
    assert len(output._admitted_context_keys(metadata, request, budget, size * 2, 100)) == 2
    assert len(output._admitted_context_keys(metadata, request, budget, size * 2 - 1, 100)) == 1
    with pytest.raises(output._WinnerBatchFull):
        output._admitted_context_keys(metadata, request, budget, size - 1, 100)
    with pytest.raises(CandidateRunnerError, match="physical batch"):
        output._admitted_context_keys(metadata, request, output._WinnerReadBudget([]), size - 1, 100)


def _proof_tuple():
    request = _request()
    generation = _generation(request)
    stamp = dt.datetime(2029, 1, 1, tzinfo=dt.UTC)
    build = CustomImportBuildAttempt(
        build_id=7,
        generation_id=generation.generation_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=20,
        producing_fence=request.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
        phase="verified",
        source_frozen_at=stamp,
        graph_frozen_at=stamp,
        output_frozen_at=stamp,
        verified_at=stamp,
    )
    proof = CustomImportBuildVerification(
        build_id=7,
        generation_id=generation.generation_id,
        verification_state="complete",
        scan_stage="complete",
        source_frozen_at=stamp,
        graph_frozen_at=stamp,
        output_frozen_at=stamp,
        verified_at=stamp,
    )
    return build, generation, proof


@pytest.mark.parametrize(
    "model_index,field,bad_value",
    [
        (0, "phase", "verifying"),
        (0, "generation_id", 11),
        (1, "dataset_id", 2),
        (1, "definition_revision_id", 3),
        (1, "schema_revision_id", 4),
        (1, "execution_id", 5),
        (1, "capture_bundle_id", 21),
        (1, "producing_fence", 2),
        (1, "producing_token_sha256", b"z" * 32),
        (2, "build_id", 8),
        (2, "generation_id", 11),
        (2, "verification_state", "scanning"),
        (2, "scan_stage", "families"),
        (2, "source_frozen_at", None),
        (2, "graph_frozen_at", None),
        (2, "output_frozen_at", None),
        (2, "verified_at", None),
    ],
)
def test_structural_proof_binds_exact_frozen_identity(model_index, field, bad_value):
    models = _proof_tuple()
    output._proof_matches(*models)
    setattr(models[model_index], field, bad_value)
    with pytest.raises(CandidateRunnerError, match="exact complete frozen"):
        output._proof_matches(*models)


@pytest.mark.parametrize(
    "pointer_generation,pointer_version,digest,is_unchanged",
    [
        (9, 2, b"d" * 32, True),
        (10, 2, b"d" * 32, False),
        (9, 3, b"d" * 32, False),
        (9, 2, b"e" * 32, False),
        (None, None, b"d" * 32, False),
    ],
)
@pytest.mark.asyncio
async def test_no_change_requires_original_pointer_and_digest(
    monkeypatch, pointer_generation, pointer_version, digest, is_unchanged
):
    request = _request(expected_base_generation_id=9, expected_pointer_version=2)
    build = SimpleNamespace(base_generation_id=9, base_pointer_version=2)
    base = SimpleNamespace(generation_id=9)
    seal = SimpleNamespace(effective_output_sha256=digest)
    pointer = (
        None
        if pointer_generation is None
        else SimpleNamespace(generation_id=pointer_generation, pointer_version=pointer_version)
    )
    monkeypatch.setattr(output, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(output.publication, "_locked_generation", AsyncMock(return_value=base))
    monkeypatch.setattr(output.publication, "_validated_generation_seal", AsyncMock(return_value=seal))
    monkeypatch.setattr(output.publication, "_locked_pointer", AsyncMock(return_value=pointer))
    compare = Mock(wraps=output.publication._require_expected_pointer)
    monkeypatch.setattr(output.publication, "_require_expected_pointer", compare)
    actual = await output._unchanged_base(None, request, build, SimpleNamespace(effective_output_sha256=b"d" * 32))
    assert (actual is not None) is is_unchanged
    assert compare.call_count == int(is_unchanged)


def test_scalar_validation_rejects_missing_extra_or_changed_rows(monkeypatch):
    expected = CustomImportRootScalar(root_revision_id=1, field_slot=2, value_state="value", string_value="one")
    changed = CustomImportRootScalar(root_revision_id=1, field_slot=2, value_state="value", string_value="two")
    revision = CustomImportRootRevision(root_revision_id=1)
    monkeypatch.setattr(output, "_expected_projections", lambda *_args, **_kwargs: (expected,))
    monkeypatch.setattr(output.material, "_root_scalar_model", lambda projection: projection)
    for actual in ((None,), (expected, expected), (changed,)):
        monkeypatch.setattr(
            output, "_output_rows", lambda *_args, **_kwargs: iter((row, revision, b"k" * 32) for row in actual)
        )
        with pytest.raises(CandidateRunnerError, match="typed scalar"):
            list(output._verified_projection_rows(None, _request(), None, 7, None, child=False))


def test_scalar_validation_compares_typed_decimal_values_not_storage_scale(monkeypatch):
    expected = CustomImportRootScalar(
        root_revision_id=1, field_slot=2, field_type="decimal", value_state="value", decimal_value=Decimal("12")
    )
    actual = CustomImportRootScalar(
        root_revision_id=1,
        field_slot=2,
        field_type="decimal",
        value_state="value",
        decimal_value=Decimal("12.000000000000"),
    )
    revision = CustomImportRootRevision(root_revision_id=1)
    monkeypatch.setattr(output, "_expected_projections", lambda *_args, **_kwargs: (expected,))
    monkeypatch.setattr(output.material, "_root_scalar_model", lambda projection: projection)
    monkeypatch.setattr(
        output, "_output_rows", lambda *_args, **_kwargs: (row for row in ((actual, revision, b"k" * 32),))
    )
    assert len(list(output._verified_projection_rows(None, _request(), None, 7, None, child=False))) == 1


@pytest.mark.parametrize("canonical,profile", [(None, "default"), ("{}", True), (b"{}", "default")])
def test_winner_lookup_rejects_non_native_text_before_read(monkeypatch, canonical, profile):
    request = _request()
    winner = replace(_winner(request), canonical_context_key=canonical, profile_id=profile)
    session = _output_read_session(monkeypatch, [])
    with pytest.raises(CandidateRunnerError, match="invalid native text"):
        output._selected_winner_context_ids(session, request, 7, [winner])
    session.execute.assert_not_called()


@pytest.mark.parametrize("boundary", ["rows", "bytes"])
def test_winner_lookup_rejects_oversize_batch_before_read(monkeypatch, boundary):
    request = _request()
    winner = _winner(request)
    session = _output_read_session(monkeypatch, [])
    if boundary == "rows":
        monkeypatch.setattr(output, "MAX_BATCH_ROWS", 2)
    else:
        monkeypatch.setattr(
            output,
            "MAX_BATCH_BYTES",
            output._LOOKUP_ARRAY_HEADERS
            + output._winner_buffer_bytes(winner.canonical_context_key, winner.profile_id)
            - 1,
        )
    with pytest.raises(CandidateRunnerError, match="lookup exceeds its physical batch"):
        output._selected_winner_context_ids(session, request, 7, [winner])
    assert output._selected_winner_context_ids(session, request, 7, []) == ()
    session.execute.assert_not_called()


@pytest.mark.parametrize("corruption", ["fixed_bytes", "metadata_rows", "duplicate_id"])
def test_context_physical_bounds_prevent_payload_read(monkeypatch, corruption):
    request = _request()
    first = _context_row(request, _candidate())
    second = _context_row(request, replace(_candidate(31, 41), entity_binding_id=first[0].entity_binding_id + 1))
    if corruption == "duplicate_id":
        second[0].candidate_context_id = first[0].candidate_context_id
    metadata = _context_metadata([first, second])
    session = _output_read_session(monkeypatch, [metadata])
    if corruption == "fixed_bytes":
        monkeypatch.setattr(output, "MAX_BATCH_BYTES", output._LOOKUP_ARRAY_HEADERS + 1)
    elif corruption == "metadata_rows":
        monkeypatch.setattr(output, "MAX_BATCH_ROWS", 6)
    message = {
        "fixed_bytes": "exceeds the admitted physical batch",
        "metadata_rows": "metadata exceeds its physical batch",
        "duplicate_id": "identities are not unique",
    }[corruption]
    with pytest.raises(CandidateRunnerError, match=message):
        list(output._context_rows(session, request, 7))
    assert session.execute.call_count == int(corruption != "fixed_bytes")


def test_incomplete_first_winner_group_closes_without_lookup(monkeypatch):
    closed_flags = []

    def stream(*_args, **_kwargs):
        try:
            yield from ()
            raise output._WinnerBatchFull
        finally:
            closed_flags.append(True)

    lookup = Mock()
    monkeypatch.setattr(output, "_ordered_winners", stream)
    monkeypatch.setattr(output, "_selected_winner_context_ids", lookup)
    request = _request()
    with pytest.raises(CandidateRunnerError, match="without a complete group"):
        output._winner_batch(None, request, _registry(request.definition), 7, _generation(request), None)
    assert closed_flags == [True]
    lookup.assert_not_called()


@pytest.mark.parametrize("corruption", ["unexpected_root", "missing_child", "oversize"])
def test_frozen_child_stream_rejects_unexpected_or_oversize_record(monkeypatch, corruption):
    request = _request()
    root = root_row(request, 1, started=True, root_values_by_field=_root())
    family_records = [(CustomImportGenerationFamily(), root[5], root[1], root[2], root[4], root[0])]
    child = child_row(request, root, dict(rate_npi="1234567893", service_code="A100", amount=Decimal("1")), 40)
    if corruption == "unexpected_root":
        child = (99, *child[1:])
    elif corruption == "missing_child":
        child = (*child[:2], None, True)
    else:
        request = replace(request, page_byte_limit=1)
    closed_flags = []

    def stream(*_args, **_kwargs):
        try:
            yield child
        finally:
            closed_flags.append(True)

    monkeypatch.setattr(output, "_output_rows", stream)
    message = "byte page" if corruption == "oversize" else "unexpected family or collection"
    with pytest.raises(CandidateRunnerError, match=message):
        list(output._family_child_rows(None, request, 7, _generation(request), family_records))
    assert closed_flags == [True]


@pytest.mark.parametrize("corruption", [None, "digest", "count", "extra_child"])
def test_frozen_family_reduction_requires_exact_digest_and_complete_child_stream(monkeypatch, corruption):
    request = _request()
    root = root_row(request, 1, started=True, root_values_by_field=_root())
    family = root[5]
    family_records = [(CustomImportGenerationFamily(), family, root[1], root[2], root[4], root[0])]
    if corruption == "digest":
        family.family_sha256 = b"x" * 32
    elif corruption == "count":
        family.child_count = 1
    child_rows = []
    if corruption == "extra_child":
        other = root_row(request, 99, started=True, root_values_by_field=_root())
        child_rows.append(
            child_row(request, other, dict(rate_npi="1234567893", service_code="A100", amount=Decimal("1")), 40)
        )
    closed_flags = []

    def stream(*_args, **_kwargs):
        try:
            yield from child_rows
        finally:
            closed_flags.append(True)

    session = SimpleNamespace(info={"custom_import_build_read_deadline": 20})
    monkeypatch.setattr(output, "_family_child_rows", stream)
    monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
    if corruption is None:
        output._verify_family_page(
            session, request, _registry(request.definition), 7, _generation(request), family_records
        )
    else:
        message = "unexpected family or collection" if corruption == "extra_child" else "digest or child count differs"
        with pytest.raises(CandidateRunnerError, match=message):
            output._verify_family_page(
                session, request, _registry(request.definition), 7, _generation(request), family_records
            )
    assert closed_flags == [True]


def test_projection_revision_transition_cannot_hide_missing_scalar(monkeypatch):
    first = CustomImportRootRevision(root_revision_id=1)
    second = CustomImportRootRevision(root_revision_id=2)
    scalars = [CustomImportRootScalar(root_revision_id=1, field_slot=slot) for slot in (1, 2)]
    expected = Mock(return_value=tuple(scalars))
    monkeypatch.setattr(output, "_expected_projections", expected)
    monkeypatch.setattr(output.material, "_root_scalar_model", lambda projection: projection)
    monkeypatch.setattr(
        output,
        "_output_rows",
        lambda *_args, **_kwargs: (row for row in ((scalars[0], first, b"k" * 32), (None, second, b"l" * 32))),
    )
    with pytest.raises(CandidateRunnerError, match="typed scalar projection differs"):
        list(output._verified_projection_rows(None, _request(), None, 7, None, child=False))
    expected.assert_called_once()
