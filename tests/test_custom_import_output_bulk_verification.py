# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen semantic checks scale with bounded pages while preserving v1 digests."""

from __future__ import annotations

import math
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.dialects import postgresql

from db.models.custom_import import CustomImportGenerationFamily
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import publication
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_prepare import child_row, ordered, root_row
from tests.test_custom_import_build_output import _generation
from tests.test_custom_import_definition import _raw_definition


def _family(request, root_id, child_values=(), *, root_values=None):
    base = root_row(
        request,
        root_id,
        started=True,
        root_values_by_field=root_values,
        child_values_by_collection={"rates": child_values},
    )
    plan, root, record, _base, binding, family, _valid = base
    member = CustomImportGenerationFamily(
        generation_id=10,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=root_id,
        family_revision_id=family.family_revision_id,
    )
    children = ordered(
        [child_row(request, base, values, 100_000 * root_id + index + 1) for index, values in enumerate(child_values)]
    )
    for _root, _collection, child, _valid in children:
        child.dataset_id = request.dataset_id
        child.schema_revision_id = request.schema_revision_id
    return (member, family, root, record, binding, plan), children


def _read_session(monkeypatch, responses, *, cancel_at=None):
    pending = iter(responses)
    session = SimpleNamespace(info={}, transactions=0, closed=0)

    @contextmanager
    def transaction(*_args):
        session.transactions += 1
        if session.transactions == cancel_at:
            raise CancellationRequested("candidate execution is canceling")
        session.info["custom_import_build_read_deadline"] = 20
        try:
            yield None, 20
        finally:
            session.closed += 1

    session.execute = Mock(side_effect=lambda _statement: SimpleNamespace(all=lambda: next(pending)))
    binding = Mock(return_value=(snapshot_models(41), None))
    monkeypatch.setattr(graph, "_read_transaction", transaction)
    monkeypatch.setattr(graph, "_prepare_read", lambda *_args: None)
    monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
    monkeypatch.setattr(output, "_build_storage_models", binding)
    session.binding = binding
    return session


def _family_responses(request, families):
    families = sorted(families, key=lambda item: item[0][3].logical_key_sha256)
    responses = []
    limit = max(1, (graph.MAX_BATCH_ROWS - 1) // 2)
    for offset in range(0, len(families), limit):
        page = families[offset : offset + limit]
        family_records = [family_record for family_record, _children in page]
        responses.extend(
            (
                [
                    (
                        family_record[3].logical_key_sha256,
                        graph._model_bytes(model for model in family_record if model is not None),
                    )
                    for family_record in family_records
                ],
                [
                    (*family_record, family_record[3].logical_key_sha256, len(family_records))
                    for family_record in family_records
                ],
            )
        )
        children = ordered([child for _row, children in page for child in children])
        child_limit = max(1, (graph.MAX_BATCH_ROWS - len(family_records) - 1) // 2)
        for index in range(0, len(children), child_limit):
            child_page = children[index : index + child_limit]
            responses.extend(
                (
                    [
                        (
                            root_id,
                            collection,
                            child.child_key_sha256,
                            child.child_revision_id,
                            graph._model_bytes((child,)) + len(collection),
                        )
                        for root_id, collection, child, _valid in child_page
                    ],
                    [
                        (
                            *child_record,
                            child_record[0],
                            child_record[1],
                            child_record[2].child_key_sha256,
                            child_record[2].child_revision_id,
                            len(child_page),
                        )
                        for child_record in child_page
                    ],
                )
            )
        responses.append([])
    return [*responses, []]


def _digests():
    return (
        publication._new_digest(publication._MATERIALIZATION_DOMAIN),
        publication._new_digest(publication._EFFECTIVE_OUTPUT_DOMAIN),
    )


def _family_material(monkeypatch, request, families, **session_options):
    session = _read_session(monkeypatch, _family_responses(request, families), **session_options)
    digests = _digests()
    count = output._family_material(session, request, _registry(request.definition), 7, _generation(request), digests)
    expected = _digests()
    for row, _children in sorted(families, key=lambda item: item[0][3].logical_key_sha256):
        for index, digest in enumerate(expected):
            publication._add_family_revision_material(digest, row[:5], {}, effective_output=bool(index))
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in expected]
    assert count == len(families)
    assert session.binding.call_count == session.transactions == session.closed
    return session


@pytest.mark.parametrize("count", [1, 8, 32])
def test_empty_families_use_one_root_page_and_one_child_scan(monkeypatch, count):
    request = _request(page_row_limit=64, page_byte_limit=1_000_000)
    session = _family_material(monkeypatch, request, [_family(request, index + 1) for index in range(count)])
    assert session.execute.call_count == 4
    assert session.transactions == 3
    statements = [str(call.args[0].compile(dialect=postgresql.dialect())) for call in session.execute.call_args_list]
    assert "LEFT OUTER JOIN ci_snapshot_41.custom_import_build_family" in statements[0]
    assert "ci_snapshot_41.custom_import_family_child" in statements[2]
    assert "COLLATE" in statements[2]
    assert all("FOR UPDATE" not in statement for statement in statements)


@pytest.mark.parametrize("physical_limit", [64, 100_000])
def test_large_family_spans_physical_pages(monkeypatch, physical_limit):
    monkeypatch.setattr(graph, "MAX_BATCH_ROWS", physical_limit)
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", physical_limit)
    request = _request(page_row_limit=64, page_byte_limit=1_000_000)
    values = tuple(
        {"rate_npi": "0000000001", "service_code": f"S{index:05d}", "amount": Decimal("12.50")} for index in range(4097)
    )
    session = _family_material(monkeypatch, request, [_family(request, 1, values)])
    assert session.execute.call_count == 2 * math.ceil(len(values) / ((physical_limit - 2) // 2)) + 4
    assert session.transactions == math.ceil(len(values) / ((physical_limit - 2) // 2)) + 3


def test_multiple_root_pages_preserve_digest_order_and_empty_gaps(monkeypatch):
    monkeypatch.setattr(graph, "MAX_BATCH_ROWS", 8)
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 8)
    request = _request(page_row_limit=8)
    families = [
        _family(
            request,
            index,
            () if index % 2 else ({"rate_npi": f"{index:010d}", "service_code": "S", "amount": Decimal(index)},),
        )
        for index in range(1, 13)
    ]
    session = _family_material(monkeypatch, request, families)
    assert session.execute.call_count == 21


def test_family_prefix_retries_preserve_material_order(monkeypatch):
    request = _request(page_row_limit=4)
    families = sorted(
        [
            _family(request, index, ({"rate_npi": f"{index:010d}", "service_code": "S", "amount": Decimal(index)},))
            for index in (1, 2)
        ],
        key=lambda item: item[0][3].logical_key_sha256,
    )
    roots = [family_record for family_record, _children in families]
    request = replace(
        request,
        page_byte_limit=max(
            sum(graph._model_bytes(family_record) for family_record in roots),
            *(
                graph._model_bytes(family_record) + graph._model_bytes((children[0][2],)) + len(children[0][1])
                for family_record, children in families
            ),
        )
        + 1,
    )
    ordinary = _family_responses(request, families)
    monkeypatch.setattr(graph, "MAX_BATCH_ROWS", 4)
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 4)
    split = _family_responses(request, families)
    # The rejected child metadata is followed by a retry with the already-read
    # root prefix. Its dropped suffix is read again after that prefix succeeds.
    responses = [*ordinary[:2], *split[2:]]
    monkeypatch.setattr(graph, "MAX_BATCH_ROWS", 100_000)
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 100_000)
    verify = output._verify_family_page
    calls = []

    def limited_roots(*args):
        calls.append(len(args[-1]))
        if len(args[-1]) == 2:
            raise CandidateRunnerError("one build record exceeds the admitted byte page")
        return verify(*args)

    monkeypatch.setattr(output, "_verify_family_page", limited_roots)
    session = _read_session(monkeypatch, responses)
    assert (
        output._family_material(session, request, _registry(request.definition), 7, _generation(request), _digests())
        == 2
    )
    assert session.execute.call_count == len(responses)
    assert session.binding.call_count == session.transactions == session.closed
    assert calls == [2, 1, 1]


@pytest.mark.parametrize("child", [False, True])
def test_oversized_verification_tuple_is_rejected_before_payload_fetch(monkeypatch, child):
    request = _request(page_byte_limit=1)
    families = [_family(request, 1, ({"rate_npi": "0000000001", "service_code": "S", "amount": Decimal("1")},))]
    records = _projection_records(request, families, child=child)
    session = _read_session(monkeypatch, _projection_responses(request, records, child=child))
    with pytest.raises(CandidateRunnerError, match="one build record exceeds"):
        list(
            output._verified_projection_rows(
                session, request, _registry(request.definition), 7, _generation(request), child=child
            )
        )
    assert session.execute.call_count == session.binding.call_count == 1


@pytest.mark.parametrize("changed", ["payload", "child_hash", "parent", "count", "family_hash", "plan"])
def test_family_reduction_rejects_changed_frozen_semantics(monkeypatch, changed):
    request = _request()
    row, children = _family(request, 1, ({"rate_npi": "0000000001", "service_code": "S", "amount": Decimal("1")},))
    changes_by_name = {
        "payload": (children[0][2], "canonical_payload", "{}"),
        "child_hash": (children[0][2], "child_key_sha256", b"x" * 32),
        "parent": (children[0][2], "canonical_parent_key", "{}"),
        "count": (row[1], "child_count", row[1].child_count + 1),
        "family_hash": (row[1], "family_sha256", b"x" * 32),
        "plan": (row[5], "family_revision_id", row[5].family_revision_id + 1),
    }
    setattr(*changes_by_name[changed])
    with pytest.raises(CandidateRunnerError):
        _family_material(monkeypatch, request, [(row, children)])


def test_missing_later_plan_is_rejected_before_child_budget(monkeypatch):
    request = _request()
    first = _family(request, 1, ({"rate_npi": "0000000001", "service_code": "S", "amount": Decimal("1")},))
    second, children = _family(request, 2)
    with pytest.raises(CandidateRunnerError, match="selected plan"):
        _family_material(monkeypatch, request, [first, ((*second[:5], None), children)])


def _projection_records(request, families, *, child):
    registry = _registry(request.definition)
    projection_records = []
    for row, children in sorted(families, key=lambda item: item[0][3].logical_key_sha256):
        revisions = [item[2] for item in children] if child else [row[2]]
        for revision in revisions:
            projections = output._expected_projections(request, registry, revision, child=child)
            projection_records.extend((scalar, revision, row[3].logical_key_sha256) for scalar in projections or [None])
    return projection_records


def _projection_responses(request, records, *, child):
    responses = []
    limit = max(1, (graph.MAX_BATCH_ROWS - 1) // 2)
    for offset in range(0, len(records), limit):
        page = records[offset : offset + limit]
        metadata = []
        for scalar, revision, root_key in page:
            keys = (root_key, revision.collection_slot, revision.child_key_sha256) if child else (root_key,)
            metadata.append(
                (
                    *keys,
                    0 if scalar is None else scalar.field_slot,
                    graph._model_bytes((revision,) if scalar is None else (scalar, revision)) + 32,
                )
            )
        responses.extend((metadata, [(*record, *identity[:-1], len(page)) for record, identity in zip(page, metadata)]))
    return [*responses, []]


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("physical_limit", [3, 100_000])
def test_scalars_compare_and_hash_in_global_pages(monkeypatch, child, physical_limit):
    monkeypatch.setattr(graph, "MAX_BATCH_ROWS", physical_limit)
    request = _request(page_row_limit=3)
    families = [
        _family(
            request, index, ({"rate_npi": f"{index:010d}", "service_code": "S", "amount": Decimal("12.500000000000")},)
        )
        for index in range(1, 14)
    ]
    records = _projection_records(request, families, child=child)
    session = _read_session(monkeypatch, _projection_responses(request, records, child=child))
    digests, expected = _digests(), _digests()
    assert output._scalar_material(
        session, request, _registry(request.definition), 7, _generation(request), digests, child=child
    ) == len(records)
    for scalar, revision, root_key in records:
        document_by_field = {
            "root_key_sha256": bytes(root_key).hex(),
            "scalar": publication._materialization_document(scalar),
        }
        if child:
            document_by_field["child_key_sha256"] = bytes(revision.child_key_sha256).hex()
        for digest in expected:
            publication._add_digest_record(digest, "child_scalar" if child else "root_scalar", document_by_field)
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in expected]
    assert session.execute.call_count == 2 * math.ceil(len(records) / ((physical_limit - 1) // 2)) + 1
    assert session.binding.call_count == session.transactions == session.closed


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("change", ["all_missing", "last_missing", "extra", "changed", "owner"])
def test_scalar_stream_rejects_missing_extra_changed_and_wrong_owner(monkeypatch, child, change):
    request = _request(page_row_limit=1)
    families = [_family(request, 1, ({"rate_npi": "0000000001", "service_code": "S", "amount": Decimal("1")},))]
    records = _projection_records(request, families, child=child)
    if change == "all_missing":
        records = [(None, records[0][1], records[0][2])]
    elif change == "last_missing":
        records.pop()
    elif change == "extra":
        records.append(records[-1])
    elif change == "changed":
        records[0][0].string_value = "wrong"
    else:
        records[0][0].dataset_id += 1
    session = _read_session(monkeypatch, _projection_responses(request, records, child=child))
    with pytest.raises(CandidateRunnerError, match="typed scalar|duplicate keys"):
        list(
            output._verified_projection_rows(
                session, request, _registry(request.definition), 7, _generation(request), child=child
            )
        )


def test_nullable_missing_fields_and_explicit_null_keep_distinct_scalar_rows(monkeypatch):
    raw = _raw_definition()
    raw["schema"]["root"]["fields"][1]["nullable"] = True
    request = _request(definition=CustomImportDefinition.from_mapping(raw), page_row_limit=2)
    families = [
        _family(request, 1, root_values={"npi": "0000000001"}),
        _family(request, 2, root_values={"npi": "0000000002", "display_name": None}),
    ]
    records = _projection_records(request, families, child=False)
    assert len(records) == 3
    assert sum(row[0].value_state == "null" for row in records) == 1
    session = _read_session(monkeypatch, _projection_responses(request, records, child=False))
    assert (
        len(
            list(
                output._verified_projection_rows(
                    session, request, _registry(request.definition), 7, _generation(request), child=False
                )
            )
        )
        == 3
    )


@pytest.mark.parametrize("projected", [False, True])
def test_child_null_missing_and_no_projected_fields_remain_visible(monkeypatch, projected):
    raw = _raw_definition()
    if not projected:
        raw["query"].pop("child")
        raw["query"]["order"] = []
        raw["selection_profiles"][0]["selection"][0]["field"] = "npi"
        raw["selection_profiles"][0]["context_dimensions"] = []
        for field in raw["schema"]["children"][0]["fields"]:
            field.pop("projection_slot", None)
    request = _request(definition=CustomImportDefinition.from_mapping(raw), page_row_limit=1)
    families = [
        _family(
            request,
            1,
            (
                {"rate_npi": "0000000001", "service_code": "M"},
                {"rate_npi": "0000000001", "service_code": "N", "amount": None},
            ),
        )
    ]
    projection_records = _projection_records(request, families, child=True)
    session = _read_session(monkeypatch, _projection_responses(request, projection_records, child=True))
    actual_records = list(
        output._verified_projection_rows(
            session, request, _registry(request.definition), 7, _generation(request), child=True
        )
    )
    assert len(actual_records) == (3 if projected else 0)
    assert sum(projection_record[0] is None for projection_record in projection_records) == (0 if projected else 2)
    assert session.binding.call_count == session.transactions == session.closed


@pytest.mark.parametrize("failure", ["cancel", "deadline"])
def test_family_page_checks_fresh_authority_and_does_not_hash_partial_result(monkeypatch, failure):
    request = _request(page_row_limit=4)
    families = [
        _family(
            request,
            1,
            tuple(
                {"rate_npi": "0000000001", "service_code": f"S{index}", "amount": Decimal(index)} for index in range(8)
            ),
        )
    ]
    session = _read_session(
        monkeypatch, _family_responses(request, families), cancel_at=3 if failure == "cancel" else None
    )
    if failure == "deadline":
        original = output._verified_root

        def expired(*args):
            result = original(*args)
            monkeypatch.setattr(graph.time, "monotonic", lambda: 21)
            return result

        monkeypatch.setattr(output, "_verified_root", expired)
    digests = _digests()
    with pytest.raises(CancellationRequested if failure == "cancel" else LeaseAuthorityLost):
        output._family_material(session, request, _registry(request.definition), 7, _generation(request), digests)
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in _digests()]
    if failure == "deadline":
        monkeypatch.setattr(output, "_verified_root", original)
    _family_material(monkeypatch, request, families)
