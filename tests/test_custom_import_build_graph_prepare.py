# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Page-wide preparation through production imports and a read-only DB double."""

from __future__ import annotations

import copy
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from db.models.custom_import import (
    CustomImportBuildFamily,
    CustomImportEntityBinding,
    CustomImportFamilyRevision,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from process.custom_import import build_graph as graph
from process.custom_import import build_graph_prepare_page as prepare
from process.custom_import import runner_codec, runner_graph
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.runner_codec import (
    digest_text,
    new_family_hash,
    record_payload,
    root_key_contract_hash,
    root_key_document,
    root_key_hash,
)
from process.custom_import.runner_graph import entity_value_digest
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRunnerError,
    LeaseAuthorityLost,
    StoredCandidateChild,
)
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _registry, _request, _revision
from tests.test_custom_import_definition import _raw_definition, _root


def root_models(request, root_id, root_values_by_field, retained):
    """Build canonical root fixtures with the real key and payload codecs."""

    root_payload = record_payload(request.definition.root_fields, root_values_by_field)
    root_record = CustomImportRootRecord(
        root_record_id=root_id,
        dataset_id=request.dataset_id,
        key_contract_sha256=root_key_contract_hash(request.definition),
        canonical_logical_key=root_key_document(request.definition, root_values_by_field),
        logical_key_sha256=root_key_hash(request.definition, root_values_by_field),
    )
    root = CustomImportRootRevision(
        root_revision_id=1000 + root_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id - int(retained),
        schema_revision_id=request.schema_revision_id,
        root_record_id=root_id,
        pack_id=2000 + root_id,
        source_ordinal=root_id,
        canonical_payload=root_payload,
        payload_sha256=digest_text("root-payload", root_payload),
    )
    return root, root_record


def root_row(
    request, root_id, *, retained=False, root_values_by_field=None, child_values_by_collection=None, started=False
):
    root_values_by_field = _root(f"{root_id:010d}") if root_values_by_field is None else root_values_by_field
    child_values_by_collection = child_values_by_collection or {
        collection.name: () for collection in request.definition.child_collections
    }
    root, root_record = root_models(request, root_id, root_values_by_field, retained)
    plan = CustomImportBuildFamily(
        build_id=7,
        root_record_id=root_id,
        root_key_sha256=root_record.logical_key_sha256,
        selection_kind="retained" if retained else "source",
        source_root_occurrence_id=None if retained else 3000 + root_id,
        base_family_revision_id=4000 + root_id if retained else None,
        family_revision_id=5000 + root_id if started else None,
        attached_child_count=0,
    )
    binding = CustomImportEntityBinding(
        entity_binding_id=6000 + root_id,
        dataset_id=request.dataset_id,
        adapter_id="npi",
        canonical_value=root_values_by_field["npi"],
        value_sha256=entity_value_digest(root_values_by_field["npi"]),
    )
    family = CustomImportFamilyRevision(
        family_revision_id=4000 + root_id,
        dataset_id=request.dataset_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=root_id,
        root_revision_id=root.root_revision_id,
        entity_binding_id=binding.entity_binding_id,
        family_sha256=new_family_hash(
            request.definition,
            RootFamily((root_values_by_field["npi"],), root_values_by_field, child_values_by_collection),
        ),
        child_count=sum(len(part) for part in child_values_by_collection.values()),
    )
    current = None
    if started:
        current = CustomImportFamilyRevision(
            family_revision_id=plan.family_revision_id,
            entity_binding_id=binding.entity_binding_id,
            family_sha256=family.family_sha256,
            child_count=family.child_count,
        )
    return (
        plan,
        root,
        root_record,
        family if retained else None,
        binding if retained or started else None,
        current,
        True,
    )


def child_row(request, root, values, child_id, collection="rates"):
    registry = _registry(request.definition)
    child = _revision(request.definition, collection, values, child_id, registry.child_collection_slots[collection])
    child.root_record_id = root[0].root_record_id
    child.canonical_parent_key = root[2].canonical_logical_key
    child.parent_key_sha256 = root[2].logical_key_sha256
    return root[0].root_record_id, collection, child, True


@pytest.mark.parametrize("retained", [False, True])
def test_root_identity_guards_each_encode_the_checked_key_once(monkeypatch, retained):
    request = _request()
    row = root_row(request, 1, retained=retained)
    encoder = Mock(wraps=runner_codec.root_key_document)
    for module in (prepare, runner_graph, runner_codec):
        monkeypatch.setattr(module, "root_key_document", encoder)

    family_input = prepare._root_input(request, row)

    assert family_input.root is row[1] and family_input.record is row[2]
    assert family_input.values["npi"] == "0000000001"
    assert encoder.call_count == (2 if retained else 1)


@pytest.mark.parametrize("corruption", [None, "parent_key_sha256", "canonical_child_key", "child_key_sha256"])
def test_stored_child_identity_reuses_checked_key_and_preserves_errors(monkeypatch, corruption):
    request = _request()
    row = root_row(request, 1)
    child_values_by_field = {"rate_npi": "0000000001", "service_code": 'A"\\\nΔ', "amount": Decimal("12.500")}
    child = child_row(request, row, child_values_by_field, 1)[2]
    if corruption is not None:
        setattr(child, corruption, "[]" if corruption == "canonical_child_key" else b"x" * 32)
    encoder = Mock(wraps=runner_codec.child_key_document)
    for module in (runner_graph, runner_codec):
        monkeypatch.setattr(module, "child_key_document", encoder)
    stored_child = StoredCandidateChild("rates", child, child_values_by_field)

    if corruption is None:
        runner_graph.verify_stored_child(request, row[2], stored_child)
    else:
        with pytest.raises(
            CandidateRunnerError, match="^current generation child identity does not match its payload$"
        ):
            runner_graph.verify_stored_child(request, row[2], stored_child)
    encoder.assert_called_once_with(request.definition, "rates", child_values_by_field)


def root_metadata(rows):
    return [
        (row[0].root_record_id, graph._model_bytes(model for model in row[:-1] if model is not None)) for row in rows
    ]


def child_metadata(rows):
    return [
        (
            root_id,
            collection,
            child.child_key_sha256,
            child.child_revision_id,
            graph._model_bytes((child,)) + len(collection),
        )
        for root_id, collection, child, _valid in rows
    ]


def ordered(rows):
    return sorted(rows, key=lambda row: (row[0], row[1], row[2].child_key_sha256, row[2].child_revision_id))


def child_pages(rows, limit):
    pages = []
    for offset in range(0, len(rows), limit):
        part = rows[offset : offset + limit]
        pages.extend((child_metadata(part), part))
    return [*pages, []]


def read_session(monkeypatch, pages, *, fail_transaction=None):
    session = SimpleNamespace(info={}, transactions=0, closed=0, execute=Mock())
    pending = iter(pages)

    def execute(statement):
        joined_rows = _joined_root_rows(next(pending))
        if statement._limit_clause is not None:
            joined_rows = joined_rows[: statement._limit_clause.value]
        physical_ids = statement.compile(dialect=postgresql.dialect()).params.get("physical_ids")
        if physical_ids is not None:
            joined_rows = [
                row
                for row in joined_rows
                if (row[0].root_record_id if isinstance(row[0], CustomImportBuildFamily) else row[2].child_revision_id)
                in physical_ids
            ]
            joined_rows = [
                (*row, row[0].root_record_id)
                if isinstance(row[0], CustomImportBuildFamily)
                else (*row, row[0], row[1], row[2].child_key_sha256, row[2].child_revision_id)
                for row in joined_rows
            ]
        return SimpleNamespace(all=lambda: joined_rows)

    session.execute.side_effect = execute

    @contextmanager
    def transaction(_session, _request, _build_id):
        session.transactions += 1
        if session.transactions == fail_transaction:
            raise CancellationRequested("candidate execution is canceling")
        session.info["custom_import_build_read_deadline"] = 20
        try:
            yield None, 20
        finally:
            session.closed += 1

    monkeypatch.setattr(graph, "_read_transaction", transaction)
    monkeypatch.setattr(graph, "_build_storage_models", Mock(return_value=(snapshot_models(17), snapshot_models(18))))
    monkeypatch.setattr(graph, "_prepare_read", Mock())
    monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
    return session


def _joined_root_rows(rows):
    """Supply the SQL aliases while keeping the canonical fixture row helpers."""

    return (
        [
            (row[0], None, *row[1:]) if row[0].selection_kind == "retained" else (row[0], row[1], None, *row[2:])
            for row in rows
        ]
        if rows and isinstance(rows[0][0], CustomImportBuildFamily)
        else rows
    )


def track_streams(monkeypatch):
    original = graph._read_snapshot_rows
    streams = []

    def read_rows(*args, **kwargs):
        stream = original(*args, **kwargs)
        streams.append(stream)
        return stream

    monkeypatch.setattr(graph, "_read_snapshot_rows", read_rows)
    return streams


@pytest.mark.parametrize("retained", [False, True])
def test_physical_preparation_combines_many_validated_logical_root_pages(monkeypatch, retained):
    request = _request(page_row_limit=32)
    rows = [root_row(request, root_id, retained=retained) for root_id in range(1, 401)]
    session = read_session(monkeypatch, [root_metadata(rows), rows] + ([] if retained else [[]]))
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert len(result) == 400 and [item.plan.root_record_id for item in result] == list(range(1, 401))
    assert all(item.child_count == 0 for item in result)
    assert session.execute.call_count == (2 if retained else 3)
    assert session.transactions == session.closed == (1 if retained else 2)
    queries = [call.args[0].compile(dialect=postgresql.dialect()) for call in session.execute.call_args_list]
    assert "ANY (%(physical_ids)s::BIGINT[])" in str(queries[1])
    assert len(queries[1].params["physical_ids"]) == 400 and len(queries[1].params) < 30
    assert session.execute.call_args_list[0].args[0]._limit_clause.value <= prepare.MAX_BATCH_ROWS


def test_physical_source_digest_keeps_canonical_parity_in_one_child_read_window(monkeypatch):
    request = _request(page_row_limit=32)
    values = [
        dict(rate_npi=f"{root_id:010d}", service_code="A100", amount=Decimal("1.000000000001"))
        for root_id in range(1, 81)
    ]
    rows = [
        root_row(request, root_id, child_values_by_collection={"rates": (value,)}, started=True)
        for root_id, value in enumerate(values, 1)
    ]
    children = ordered(
        [
            child_row(request, row, value, root_id)
            for root_id, (row, value) in enumerate(zip(rows, values, strict=True), 1)
        ]
    )
    session = read_session(monkeypatch, [root_metadata(rows), rows, child_metadata(children), children])
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert len(result) == 80 and all(item.child_count == 1 for item in result)
    assert [item.family_sha256 for item in result] == [row[5].family_sha256 for row in rows]
    assert session.execute.call_count == 4 and session.transactions == session.closed == 2
    child_query = session.execute.call_args_list[3].args[0].compile(dialect=postgresql.dialect())
    assert "ANY (%(physical_ids)s::BIGINT[])" in str(child_query)
    assert len(child_query.params["physical_ids"]) == 80


def test_physical_root_metadata_byte_prefix_is_bounded_before_payload(monkeypatch):
    request = _request(page_row_limit=32)
    rows = [root_row(request, root_id, retained=True) for root_id in range(1, 41)]
    session = read_session(monkeypatch, [root_metadata(rows), rows])
    monkeypatch.setattr(prepare, "MAX_BATCH_BYTES", 8192)
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert 0 < len(result) < len(rows) and session.transactions == session.closed == 1
    admitted_bytes = sum(
        graph._model_bytes(model for model in row[:-1] if model is not None) for row in rows[: len(result)]
    )
    assert admitted_bytes <= 4096
    payload = session.execute.call_args_list[1].args[0].compile(dialect=postgresql.dialect())
    assert payload.params["physical_ids"] == tuple(range(1, len(result) + 1))


def test_cancel_between_root_and_child_physical_reads_never_returns_prepared_writes(monkeypatch):
    request = _request(page_row_limit=32)
    rows = [root_row(request, root_id) for root_id in range(1, 41)]
    session = read_session(monkeypatch, [root_metadata(rows), rows], fail_transaction=2)
    with pytest.raises(CancellationRequested):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert session.execute.call_count == 2 and session.transactions == 2 and session.closed == 1


@pytest.mark.parametrize("count", [1, 2, 8])
@pytest.mark.parametrize("retained", [False, True])
def test_empty_family_reads_are_constant_for_one_root_page(monkeypatch, count, retained):
    request = _request()
    rows = [root_row(request, index + 1, retained=retained) for index in range(count)]
    pages = [root_metadata(rows), rows] + ([] if retained else [[]])
    session = read_session(monkeypatch, pages)
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert [item.plan.root_record_id for item in result] == list(range(1, count + 1))
    assert all(item.child_count == 0 for item in result)
    assert session.execute.call_count == (2 if retained else 3)
    assert session.transactions == (1 if retained else 2)
    assert session.closed == session.transactions
    if retained:
        assert all(item.family_sha256 == row[3].family_sha256 for item, row in zip(result, rows))
    else:
        assert all(len(item.family_sha256) == 32 for item in result)


def test_mixed_page_retains_order_identity_and_legacy_digest(monkeypatch):
    request = _request(page_row_limit=8)
    rate_values_by_field = {"rate_npi": "0000000002", "service_code": "A100", "amount": Decimal("123456.123456789012")}
    retained = root_row(request, 1, retained=True)
    source = root_row(request, 2, child_values_by_collection={"rates": (rate_values_by_field,)}, started=True)
    source[0].last_child_collection_slot = 1
    source[0].last_child_key_sha256 = b"a" * 32
    source[0].last_input_child_revision_id = 19
    empty = root_row(request, 3)
    rows = [retained, source, empty]
    child_rows = [child_row(request, source, rate_values_by_field, 20)]
    session = read_session(monkeypatch, [root_metadata(rows), rows, *child_pages(child_rows, 5)])
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert [item.plan.root_record_id for item in result] == [1, 2, 3]
    assert [item.child_count for item in result] == [0, 1, 0]
    assert result[0].entity_binding_id == retained[4].entity_binding_id
    assert result[0].root.definition_revision_id == request.definition_revision_id - 1
    assert result[1].family_sha256 == source[5].family_sha256
    assert result[1].plan is source[0]
    assert result[1].plan.last_input_child_revision_id == 19
    assert session.execute.call_count == 5
    # Every query is nonlocking; the child payload is one global root set.
    queries = [str(call.args[0].compile(dialect=postgresql.dialect())) for call in session.execute.call_args_list]
    assert all("FOR UPDATE" not in query for query in queries)
    assert "root_record_id IN" in queries[2]


def test_large_family_streams_across_global_child_pages(monkeypatch):
    request = _request(page_row_limit=6)
    values = [dict(rate_npi="0000000001", service_code=f"C{index:04}", amount=Decimal(index)) for index in range(31)]
    first = root_row(request, 1, child_values_by_collection={"rates": tuple(values)}, started=True)
    empty = root_row(request, 2)
    rows = [first, empty]
    children = ordered([child_row(request, first, value, index + 1) for index, value in enumerate(values)])
    session = read_session(monkeypatch, [root_metadata(rows), rows, *child_pages(children, 4)])
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert result[0].family_sha256 == first[5].family_sha256
    assert result[0].child_count == 31
    assert result[1].child_count == 0
    assert session.execute.call_count == 2 + 2 * 8 + 1
    assert session.transactions == 1 + 8 + 1
    assert session.closed == session.transactions
    child_queries = session.execute.call_args_list[2::2]
    assert all(call.args[0]._limit_clause.value == 4 for call in child_queries)


def test_read_count_scales_with_root_pages_and_durable_resume(monkeypatch):
    request = _request(page_row_limit=4)
    rows = [root_row(request, index, retained=True) for index in range(1, 8)]
    pages = []
    for offset in range(0, len(rows), 2):
        part = rows[offset : offset + 2]
        pages.extend((root_metadata(part), part))
    session = read_session(monkeypatch, [*pages, []])
    result, after = [], 0
    while page := prepare.prepare_family_page(session, request, _registry(request.definition), 7, after):
        result.extend(page)
        after = page[-1].plan.root_record_id
    assert len(result) == 7
    assert session.execute.call_count == 2 * 4 + 1
    assert session.transactions == 5
    queries = [str(call.args[0].compile(dialect=postgresql.dialect())) for call in session.execute.call_args_list]
    assert all("complete_at IS NULL" in query for query in queries)
    assert all("root_record_id) >" in query for query in queries)


def test_entire_root_reservation_shrinks_only_the_prefix(monkeypatch):
    request = _request(page_row_limit=8)
    first, second = root_row(request, 1), root_row(request, 2)
    rows = [first, second]
    child_values_by_field = dict(rate_npi="0000000001", service_code="X" * 900, amount=None)
    child = child_row(request, first, child_values_by_field, 10)
    root_size = root_metadata([first])[0][-1]
    child_size = child_metadata([child])[0][-1]
    request = replace(request, page_byte_limit=root_size + child_size + 1)
    assert sum(row[-1] for row in root_metadata(rows)) < request.page_byte_limit
    session = read_session(
        monkeypatch,
        [root_metadata(rows), rows, child_metadata([child]), *child_pages([child], 7)],
    )
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert [item.plan.root_record_id for item in result] == [1]
    assert result[0].child_count == 1
    assert first[0].family_revision_id is None and second[0].family_revision_id is None
    assert session.execute.call_count == 6


def test_single_root_child_pair_over_budget_fails_before_payload(monkeypatch):
    request = _request()
    row = root_row(request, 1)
    child = child_row(request, row, dict(rate_npi="0000000001", service_code="X", amount=None), 10)
    request = replace(request, page_byte_limit=root_metadata([row])[0][-1] + child_metadata([child])[0][-1] - 1)
    session = read_session(monkeypatch, [root_metadata([row]), [row], child_metadata([child])])
    with pytest.raises(CandidateRunnerError, match="one build record"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 3
    assert session.closed == session.transactions


def test_oversized_root_is_not_truncated_or_fetched(monkeypatch):
    request = _request()
    row = root_row(request, 1)
    request = replace(request, page_byte_limit=root_metadata([row])[0][-1] - 1)
    session = read_session(monkeypatch, [root_metadata([row])])
    with pytest.raises(CandidateRunnerError, match="one build record"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 1


def test_root_model_reservation_counts_joined_tuples(monkeypatch):
    request = _request(page_row_limit=4)
    rate_values_by_field = dict(rate_npi="0000000002", service_code="exact", amount=None)
    retained = root_row(request, 1, retained=True, started=True)
    source_root = root_row(request, 2, child_values_by_collection={"rates": (rate_values_by_field,)}, started=True)
    assert len([model for model in retained[:-1] if model is not None]) == 6
    assert len([model for model in source_root[:-1] if model is not None]) == 5
    root_rows = [retained, source_root]
    child = child_row(request, source_root, rate_values_by_field, 10)
    root_bytes = sum(size for _root_id, size in root_metadata(root_rows))
    child_bytes = child_metadata([child])[0][-1]
    request = replace(request, page_byte_limit=root_bytes + child_bytes)
    session = read_session(monkeypatch, [root_metadata(root_rows), root_rows, *child_pages([child], 2)])
    original = graph._read_snapshot_rows
    bounds = []

    def read_rows(*args, **kwargs):
        bounds.append(kwargs["bounds"])
        return original(*args, **kwargs)

    monkeypatch.setattr(graph, "_read_snapshot_rows", read_rows)
    family_inputs = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert len(family_inputs) == 2
    assert family_inputs[1].child_count == 1
    assert bounds[0].row_limit == 2
    assert bounds[1].row_limit == 2
    assert bounds[1].reserve_bytes == root_bytes
    assert bounds[1].reserve_bytes + child_bytes == request.page_byte_limit
    assert graph._model_bytes((retained[3], retained[4], retained[5], source_root[4], source_root[5])) > 0
    assert session.execute.call_count == 5


def test_each_root_decode_rechecks_the_current_time_window(monkeypatch):
    request = _request()
    rows = [root_row(request, index, retained=True, started=True) for index in (1, 2)]
    session = read_session(monkeypatch, [root_metadata(rows), rows])
    original = prepare._root_input
    decoded_root_ids = []

    def expire_after_first(*args):
        decoded_root_ids.append(args[1][0].root_record_id)
        result = original(*args)
        monkeypatch.setattr(graph.time, "monotonic", lambda: 21)
        return result

    monkeypatch.setattr(prepare, "_root_input", expire_after_first)
    with pytest.raises(LeaseAuthorityLost, match="deadline"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert decoded_root_ids == [1]
    assert session.execute.call_count == 2


@pytest.mark.parametrize("scope", [False, None])
def test_missing_full_identity_is_rejected_without_child_reads(monkeypatch, scope):
    request = _request()
    row = (*root_row(request, 1)[:-1], scope)
    session = read_session(monkeypatch, [root_metadata([row]), [row]])
    with pytest.raises(CandidateRunnerError, match="identity or provenance"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 2


@pytest.mark.parametrize(
    "corruption", ["scope", "parent", "payload", "unexpected_root", "retry", "retry_digest", "retry_progress"]
)
def test_child_and_retry_mismatches_close_the_stream(monkeypatch, corruption):
    streams = track_streams(monkeypatch)
    request = _request()
    child_values_by_field = dict(rate_npi="0000000001", service_code="X", amount=None)
    row = root_row(request, 1, child_values_by_collection={"rates": (child_values_by_field,)}, started=True)
    child = child_row(request, row, child_values_by_field, 10)
    if corruption == "scope":
        child = (*child[:-1], False)
    elif corruption == "parent":
        child[2].parent_key_sha256 = b"x" * 32
    elif corruption == "payload":
        child[2].payload_sha256 = b"x" * 32
    elif corruption == "unexpected_root":
        child = (2, *child[1:])
    else:
        model, field, value = {
            "retry": (row[5], "child_count", 2),
            "retry_digest": (row[5], "family_sha256", b"x" * 32),
            "retry_progress": (row[0], "attached_child_count", 2),
        }[corruption]
        setattr(model, field, value)
    session = read_session(monkeypatch, [root_metadata([row]), [row], *child_pages([child], 15)])
    with pytest.raises(CandidateRunnerError):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.closed == session.transactions
    assert all(stream.gi_frame is None for stream in streams)


@pytest.mark.parametrize(
    ("model_index", "field", "value"),
    [
        (1, "payload_sha256", b"x" * 32),
        (2, "key_contract_sha256", b"x" * 32),
        (2, "canonical_logical_key", "[]"),
        (2, "logical_key_sha256", b"x" * 32),
    ],
)
def test_noncanonical_root_identity_stops_before_child_reads(monkeypatch, model_index, field, value):
    request = _request()
    row = root_row(request, 1)
    setattr(row[model_index], field, value)
    encoder = Mock(wraps=runner_codec.root_key_document)
    monkeypatch.setattr(prepare, "root_key_document", encoder)
    monkeypatch.setattr(runner_codec, "root_key_document", encoder)
    session = read_session(monkeypatch, [root_metadata([row]), [row]])
    with pytest.raises(CandidateRunnerError, match="root payload or identity is not canonical"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 2
    assert session.closed == session.transactions == 1
    assert row[0].family_revision_id is None
    assert encoder.call_count == (1 if field in {"canonical_logical_key", "logical_key_sha256"} else 0)


def test_source_preparation_requires_space_for_both_root_and_child(monkeypatch):
    request = _request(page_row_limit=1)
    row = root_row(request, 1)
    session = read_session(monkeypatch, [root_metadata([row]), [row]])
    with pytest.raises(CandidateRunnerError, match="row space for a root and child"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 2 and session.closed == session.transactions == 1


@pytest.mark.parametrize("corruption", ["collection_slot", "byte_budget"])
def test_physical_child_preparation_still_enforces_logical_limits(monkeypatch, corruption):
    request = _request()
    row = root_row(request, 1)
    child = child_row(request, row, dict(rate_npi="0000000001", service_code="A", amount=None), 10)
    if corruption == "collection_slot":
        child[2].collection_slot = 99
        message = "collection differs from the validated registry"
    else:
        pair_bytes = graph._model_bytes((row[0], row[1], row[2], child[2])) + len("rates")
        request = replace(request, page_byte_limit=pair_bytes - 1)
        message = "one build record exceeds the admitted byte page"
    session = read_session(monkeypatch, [root_metadata([row]), [row], child_metadata([child]), [child]])
    with pytest.raises(CandidateRunnerError, match=message):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert session.execute.call_count == 4 and session.closed == session.transactions == 2
    assert row[0].family_revision_id is None


@pytest.mark.parametrize(
    ("child_root_id", "collection", "message"),
    [
        (0, "rates", "canonical family order"),
        (2, "rates", "unexpected family or collection"),
        (1, "unknown", "unexpected family or collection"),
    ],
)
def test_child_stream_rejects_out_of_scope_groups_after_closing_reads(monkeypatch, child_root_id, collection, message):
    streams = track_streams(monkeypatch)
    request = _request()
    row = root_row(request, 1)
    child = child_row(request, row, dict(rate_npi="0000000001", service_code="A", amount=None), 10)
    child = (child_root_id, collection, *child[2:])
    session = read_session(monkeypatch, [root_metadata([row]), [row], *child_pages([child], 15)])
    with pytest.raises(CandidateRunnerError, match=message):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.closed == session.transactions
    assert all(stream.gi_frame is None for stream in streams)
    assert row[0].family_revision_id is None


def test_physical_duplicate_root_metadata_is_rejected_before_payload(monkeypatch):
    request = _request()
    row = root_row(request, 1, retained=True)
    session = read_session(monkeypatch, [root_metadata([row, row])])
    with pytest.raises(CandidateRunnerError, match="duplicate identities"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert session.execute.call_count == 1 and session.closed == session.transactions == 1


@pytest.mark.parametrize("payload_order", [(0,), (1, 0), (0, 0)])
def test_physical_root_payload_must_match_the_entire_metadata_prefix(monkeypatch, payload_order):
    request = _request()
    rows = [root_row(request, root_id, retained=True) for root_id in (1, 2)]
    session = read_session(monkeypatch, [root_metadata(rows), [rows[index] for index in payload_order]])
    with pytest.raises(CandidateRunnerError, match="changed during its read"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert session.execute.call_count == 2 and session.closed == session.transactions == 1


def test_physical_child_pages_preserve_a_later_family_group_and_its_digest(monkeypatch):
    request = _request(page_row_limit=32)
    values = [dict(rate_npi="0000000002", service_code=f"A{index}", amount=None) for index in range(9)]
    empty = root_row(request, 1)
    populated = root_row(request, 2, child_values_by_collection={"rates": tuple(values)}, started=True)
    rows = [empty, populated]
    children = ordered([child_row(request, populated, value, index + 1) for index, value in enumerate(values)])
    session = read_session(monkeypatch, [root_metadata(rows), rows, *child_pages(children, 8)])
    monkeypatch.setattr(prepare, "MAX_BATCH_ROWS", 16)
    result = prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0, physical=True)
    assert [item.child_count for item in result] == [0, 9]
    assert result[1].family_sha256 == populated[5].family_sha256
    assert session.execute.call_count == 6 and session.closed == session.transactions == 3
    assert session.execute.call_args_list[4].args[0]._limit_clause.value == 8


def test_cancellation_at_next_child_page_closes_stream(monkeypatch):
    streams = track_streams(monkeypatch)
    request = _request(page_row_limit=4)
    values = [dict(rate_npi="0000000001", service_code=f"X{i}", amount=None) for i in range(4)]
    row = root_row(request, 1)
    children = ordered([child_row(request, row, value, index + 1) for index, value in enumerate(values)])
    session = read_session(monkeypatch, [root_metadata([row]), [row], *child_pages(children, 3)], fail_transaction=3)
    with pytest.raises(CancellationRequested):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 4
    assert session.closed == 2
    assert all(stream.gi_frame is None for stream in streams)


def test_pure_digest_work_observes_deadline(monkeypatch):
    request = _request()
    row = root_row(request, 1, retained=True)
    session = read_session(monkeypatch, [root_metadata([row]), [row]])
    original = prepare._root_input

    def expire(*args):
        result = original(*args)
        monkeypatch.setattr(graph.time, "monotonic", lambda: 21)
        return result

    monkeypatch.setattr(prepare, "_root_input", expire)
    with pytest.raises(LeaseAuthorityLost, match="deadline"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 2


def multi_collection_definition():
    raw = _raw_definition()
    extra = copy.deepcopy(raw["schema"]["children"][0])
    extra["name"] = "alpha"
    field_ids_by_name = {"rate_npi": "alpha_npi", "service_code": "alpha_code", "amount": "alpha_amount"}
    extra["parent_key"][0]["child"] = "alpha_npi"
    extra["child_key"] = ["alpha_code"]
    for field in extra["fields"]:
        field["id"] = field_ids_by_name[field["id"]]
        field["slot"] += 3
        field.pop("projection_slot", None)
    raw["schema"]["children"].append(extra)
    raw["streams"].append(
        dict(id="alpha", kind="child", child="alpha", format="ndjson", compression="none", snapshot_token="snapshot_id")
    )
    raw["aliases"]["alpha"] = {"Provider ID": "alpha_npi", "Code": "alpha_code", "Amount": "alpha_amount"}
    return CustomImportDefinition.from_mapping(raw)


def test_collection_name_order_not_slot_order_and_scalar_domain_parity(monkeypatch):
    definition = multi_collection_definition()
    request = _request(definition=definition, page_row_limit=8)
    child_values_by_collection = {
        "rates": (
            dict(rate_npi="0000000001", service_code="missing"),
            dict(rate_npi="0000000001", service_code="null", amount=None),
            dict(rate_npi="0000000001", service_code="decimal", amount=Decimal("999999999999999999.123456789012")),
        ),
        "alpha": (dict(alpha_npi="0000000001", alpha_code="escaped\x00and\\u0000", alpha_amount=None),),
    }
    row = root_row(request, 1, child_values_by_collection=child_values_by_collection, started=True)
    all_children = ordered(
        [
            child_row(request, row, value, 100 * slot + index, collection)
            for slot, (collection, values) in enumerate(child_values_by_collection.items(), 1)
            for index, value in enumerate(values)
        ]
    )
    assert all_children[0][1] == "alpha"
    assert _registry(definition).child_collection_slots["alpha"] == 2
    session = read_session(monkeypatch, [root_metadata([row]), [row], *child_pages(all_children, 7)])
    result = prepare.prepare_family_page(session, request, _registry(definition), 7, 0)
    assert result[0].child_count == 4
    assert result[0].family_sha256 == row[5].family_sha256


def test_compiled_queries_bind_full_identity_and_duplicate_policy():
    raw = _raw_definition()
    raw["streams"][1]["duplicate_policy"] = "collapse_identical"
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    registry = _registry(request.definition)
    candidate_models = snapshot_models(17)
    roots, _, _ = prepare._pending_statement(request, 7, candidate_models, snapshot_models(18))
    children, keys, _ = prepare._children_statement(request, registry, 7, (1, 2), candidate_models)
    root_sql = str(roots.compile(dialect=postgresql.dialect()))
    child_sql = str(children.order_by(*keys).compile(dialect=postgresql.dialect()))
    for term in (
        "custom_import_generation_family",
        "custom_import_generation_seal",
        "base_family.root_record_id",
        "base_family.schema_revision_id",
        "current_family.producing_token_sha256",
        "custom_import_entity_binding.dataset_id",
        "custom_import_pack.capture_bundle_id",
        "custom_import_pack.producing_fence",
        "custom_import_build_stream.next_pack_ordinal",
        "custom_import_root_revision.source_ordinal",
        "custom_import_build_family.complete_at IS NULL",
        "custom_import_build_attempt.plan_complete_at IS NOT NULL",
        "scope_valid",
    ):
        assert term in root_sql
    for term in (
        "custom_import_child_revision.dataset_id",
        "custom_import_child_revision.definition_revision_id",
        "custom_import_child_revision.schema_revision_id",
        "custom_import_child_revision.root_record_id",
        "custom_import_child_revision.canonical_parent_key",
        "custom_import_child_revision.source_ordinal",
        "custom_import_source_stream.collection_slot",
        "custom_import_pack.producing_token_sha256",
        "later_occurrence.source_ordinal >",
        "later_occurrence.raw_parent_key_sha256",
        'COLLATE "C"',
    ):
        assert term in child_sql
    assert "later_occurrence.resolved_rejection_id" not in child_sql
    assert "FOR UPDATE" not in root_sql + child_sql
    assert all(key.class_ is candidate_models[prepare.Occurrence] for key in (keys[0], keys[2], keys[3]))
    old, _ = graph._child_statement(
        CustomImportBuildFamily(build_id=7, root_record_id=1, selection_kind="source"), request.definition
    )
    original_duplicate_clause = str(
        old._where_criteria[-1].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )
    page_duplicate_clause = str(
        children._where_criteria[-1].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )
    assert original_duplicate_clause.replace("mrf.", "ci_snapshot_17.") == page_duplicate_clause.replace(
        "later_occurrence", "custom_import_build_occurrence_1"
    )


@pytest.mark.parametrize("retained", [False, True])
def test_source_retry_and_retained_entity_bindings_are_verified(monkeypatch, retained):
    request = _request()
    row = root_row(request, 1, retained=retained, started=True)
    row[4].canonical_value = "0000000002"
    session = read_session(monkeypatch, [root_metadata([row]), [row]])
    with pytest.raises(CandidateRunnerError, match="entity binding"):
        prepare.prepare_family_page(session, request, _registry(request.definition), 7, 0)
    assert session.execute.call_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_graph_caller_consumes_full_pages_and_heartbeats_per_page(monkeypatch, cancel):
    request = _request()
    inputs = [prepare._root_input(request, root_row(request, index, retained=True)) for index in range(1, 6)]
    observed_cursors = []
    pages = iter((tuple(inputs[:3]), tuple(inputs[3:]), ()))

    async def run_sync(callback):
        return callback(object())

    session = SimpleNamespace(run_sync=run_sync)

    @asynccontextmanager
    async def read_context(*args):
        yield session

    @asynccontextmanager
    async def write_context(*args):
        yield session, None

    def next_page(_session, _request, _registry, _build_id, after):
        observed_cursors.append(after)
        return next(pages)

    collaborators_by_name = dict(
        _snapshot=AsyncMock(
            return_value=SimpleNamespace(phase="graph", generation_id=None, plan_complete_at=1, capture_bundle_id=9)
        ),
        _session=read_context,
        _page_session=write_context,
        load_registry=AsyncMock(return_value=_registry(request.definition)),
        _next_family_inputs=next_page,
        _consume_family_page=AsyncMock(),
        _heartbeat=AsyncMock(),
        _open_output=AsyncMock(return_value=11),
        _prepare_snapshot_indexes=AsyncMock(),
    )
    for name, collaborator in collaborators_by_name.items():
        monkeypatch.setattr(graph, name, collaborator)
    if cancel:
        graph._consume_family_page.side_effect = CancellationRequested("canceling")
        with pytest.raises(CancellationRequested):
            await graph._build_graph(object(), request, 7)
        assert observed_cursors == [0]
        assert graph._consume_family_page.await_count == 1
        assert graph._heartbeat.await_count == 0
        assert graph._open_output.await_count == 0
        return
    assert await graph._build_graph(object(), request, 7) == 11
    assert observed_cursors == [0, 3, 5]
    assert graph._heartbeat.await_count == 2
    assert graph._consume_family_page.await_count == 2
    assert [
        [prepared_family.plan.root_record_id for prepared_family in call.args[-1]]
        for call in graph._consume_family_page.await_args_list
    ] == [[1, 2, 3], [4, 5]]
