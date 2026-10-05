# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bulk graph callers preserve typed projections and the protected write boundary."""

from __future__ import annotations

import datetime as dt
import json
from contextlib import nullcontext
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.models.custom_import import (
    CustomImportBuildFamily,
    CustomImportChildScalar,
    CustomImportFamilyRevision,
    CustomImportRootRecord,
    CustomImportRootRevision,
)
from process.custom_import import build_graph as graph
from process.custom_import import build_graph_source_page as page
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_codec import digest_text, record_payload, root_key_document, root_key_hash
from process.custom_import.runner_graph import entity_value_digest
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _registry, _request, _revision
from tests.test_custom_import_definition import _raw_definition, _root


def _source_root_input(request, values=None, *, child_count=0):
    values = values or _root()
    payload = record_payload(request.definition.root_fields, values)
    root = CustomImportRootRevision(
        root_revision_id=11,
        root_record_id=8,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        pack_id=10,
        source_ordinal=9,
        canonical_payload=payload,
        payload_sha256=digest_text("root-payload", payload),
    )
    plan = CustomImportBuildFamily(build_id=7, root_record_id=8, selection_kind="source", source_root_occurrence_id=12)
    return SimpleNamespace(
        plan=plan,
        root=root,
        record=CustomImportRootRecord(root_record_id=8),
        values=values,
        family_sha256=b"f" * 32,
        child_count=child_count,
        entity_binding_id=None,
    )


@pytest.mark.parametrize("child_count", [0, 3])
async def test_source_root_start_is_one_protected_call_without_orm(monkeypatch, child_count):
    request = _request()
    inputs = _source_root_input(request, child_count=child_count)
    session = SimpleNamespace(get=AsyncMock(), add=Mock(), add_all=Mock(), flush=AsyncMock())
    call = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: 55))
    monkeypatch.setattr(graph, "_page_session", lambda *_: nullcontext((session, object())))
    monkeypatch.setattr(graph, "_source_call", call)
    await graph._start_family(None, request, _registry(request.definition), inputs)
    call.assert_awaited_once()
    assert call.await_args.args[:2] == (session, "source_root_start")
    args = call.await_args.args[2]
    assert args[:12] == (
        ("bigint", 7),
        ("bigint", 4),
        ("bigint", 1),
        ("bytea", lease_token_sha256(request.lease_token)),
        ("bigint", 8),
        ("bigint", 12),
        ("bigint", 11),
        ("bytea", inputs.root.payload_sha256),
        ("bytea", b"f" * 32),
        ("bigint", child_count),
        ("text", "1234567893"),
        ("bytea", entity_value_digest("1234567893")),
    )
    session.get.assert_not_awaited()
    session.add.assert_not_called()
    session.add_all.assert_not_called()
    session.flush.assert_not_awaited()


def test_existing_root_context_helper_preserves_native_types_and_optional_states():
    raw = _raw_definition()
    extras = [
        ("root_amount", "decimal"),
        ("visits", "integer"),
        ("enabled", "boolean"),
        ("day", "date"),
        ("instant", "timestamp"),
    ]
    for slot, (name, kind) in enumerate(extras, 6):
        raw["schema"]["root"]["fields"].append(
            {"id": name, "slot": slot, "type": kind, "nullable": True, "projection_slot": slot - 1}
        )
        raw["query"]["root_fields"].append(name)
    raw["selection_profiles"][0]["selection"] = [{"field": "display_name", "direction": "asc", "nulls": "last"}]
    raw["selection_profiles"][0]["context_dimensions"] = ["display_name", "root_amount"]
    request = _request(definition=CustomImportDefinition.from_mapping(raw), page_row_limit=256)
    root_values = _root() | dict(
        root_amount=Decimal("123456789012345678.123456789012"),
        visits=(1 << 63) - 1,
        enabled=False,
        day=dt.date(2024, 2, 29),
        instant=dt.datetime(2024, 2, 29, 1, 2, 3, 456789, tzinfo=dt.timezone(dt.timedelta(hours=2))),
    )
    args = graph._source_root_arguments(
        request, _registry(request.definition), _source_root_input(request, root_values)
    )
    by_slot = {slot: i for i, slot in enumerate(args[12][1])}
    assert args[17][1][by_slot[6]] == root_values["root_amount"]
    assert args[16][1][by_slot[7]] == root_values["visits"]
    assert args[18][1][by_slot[8]] is False
    assert args[19][1][by_slot[9]] == root_values["day"]
    assert args[20][1][by_slot[10]] == root_values["instant"].astimezone(dt.UTC)
    assert args[21][1] == (1,)
    states = []
    for variant in [
        root_values,
        root_values | {"root_amount": None},
        {key: root_value for key, root_value in root_values.items() if key != "root_amount"},
    ]:
        variant_args = graph._source_root_arguments(
            request, _registry(request.definition), _source_root_input(request, variant)
        )
        states.append(json.loads(variant_args[22][1][0])["dimensions"][1]["value_state"])
        assert (6 in variant_args[12][1]) == ("root_amount" in variant)
    assert states == ["value", "null", "missing"]


@pytest.mark.parametrize("corruption", ["payload_hash", "values", "selection", "fanout"])
async def test_invalid_root_prevents_protected_call(monkeypatch, corruption):
    request = _request()
    inputs = _source_root_input(request)
    if corruption == "payload_hash":
        inputs.root.payload_sha256 = b"x" * 32
    elif corruption == "values":
        inputs.values = _root(name="Different Synthetic Provider")
    elif corruption == "selection":
        inputs.entity_binding_id = 99
    else:
        request = _request(page_row_limit=1)
    call = AsyncMock()
    monkeypatch.setattr(graph, "_page_session", lambda *_: nullcontext((object(), object())))
    monkeypatch.setattr(graph, "_source_call", call)
    with pytest.raises(CandidateRunnerError):
        await graph._start_family(None, request, _registry(request.definition), inputs)
    call.assert_not_awaited()


async def test_cancellation_propagates_inside_existing_owned_page(monkeypatch):
    import asyncio

    request = _request()
    call = AsyncMock(side_effect=asyncio.CancelledError("synthetic cancellation"))
    monkeypatch.setattr(graph, "_page_session", lambda *_: nullcontext((object(), object())))
    monkeypatch.setattr(graph, "_source_call", call)
    with pytest.raises(asyncio.CancelledError):
        await graph._start_family(None, request, _registry(request.definition), _source_root_input(request))
    call.assert_awaited_once()


def _input(*, definition=None):
    request = _request(
        definition=definition or CustomImportDefinition.from_mapping(_raw_definition()), page_row_limit=256
    )
    registry = _registry(request.definition)
    plan = CustomImportBuildFamily(build_id=7, root_record_id=8, selection_kind="source")
    progress = SimpleNamespace(attached_child_count=0, complete_at=None, family_revision_id=9)
    family = CustomImportFamilyRevision(
        family_revision_id=9, entity_binding_id=10, root_record_id=8, root_revision_id=11, family_sha256=b"f" * 32
    )
    root_values = _root()
    record = CustomImportRootRecord(
        root_record_id=8,
        canonical_logical_key=root_key_document(request.definition, root_values),
        logical_key_sha256=root_key_hash(request.definition, root_values),
    )
    return request, registry, SimpleNamespace(plan=plan, values=root_values, record=record), progress, family


def _child(request, family_input, values, child_id=12):
    child = _revision(request.definition, "rates", values, child_id)
    child.canonical_parent_key = family_input.record.canonical_logical_key
    child.parent_key_sha256 = family_input.record.logical_key_sha256
    return child


def _typed_child_definition():
    raw = _raw_definition()
    for slot, (field_id, kind) in enumerate(
        (("note", "string"), ("visits", "integer"), ("enabled", "boolean"), ("day", "date"), ("instant", "timestamp")),
        6,
    ):
        raw["schema"]["children"][0]["fields"].append(
            {"id": field_id, "slot": slot, "type": kind, "nullable": True, "projection_slot": slot - 1}
        )
    raw["selection_profiles"][0]["context_dimensions"].append("amount")
    return CustomImportDefinition.from_mapping(raw)


async def test_native_typed_arrays_preserve_precision_states_and_current_context(monkeypatch):
    request, registry, family_input, progress, family = _input(definition=_typed_child_definition())
    child_values_by_field = dict(
        rate_npi="1234567893",
        service_code="A100",
        amount=Decimal("123456789012345678.123456789012"),
        note="synthetic",
        visits=(1 << 63) - 1,
        enabled=False,
        day=dt.date(2024, 2, 29),
        instant=dt.datetime(2024, 2, 29, 1, 2, 3, 456789, tzinfo=dt.timezone(dt.timedelta(hours=2))),
    )
    value_sets = [
        child_values_by_field,
        {"rate_npi": child_values_by_field["rate_npi"], "service_code": "B100", "amount": None},
        {"rate_npi": child_values_by_field["rate_npi"], "service_code": "C100"},
    ]
    child_revisions = [
        _child(request, family_input, child_values_by_field, 12 + position)
        for position, child_values_by_field in enumerate(value_sets)
    ]
    projections = []
    for child in child_revisions:
        projections.extend(
            await graph._child_page_models(
                None, request, registry, SimpleNamespace(build_id=7), family_input, child, family
            )
        )
    call = AsyncMock(return_value=SimpleNamespace(one=lambda: "receipt"))
    monkeypatch.setattr(page, "_call", call)
    assert (
        await page.append_source_child_page(None, family_input.plan, progress, family, child_revisions, projections)
        == "receipt"
    )
    arguments = call.await_args.args[2]
    assert arguments[:5] == (("bigint", 7), ("bigint", 8), ("bigint", 9), ("bigint", 0), ("bigint[]", [12, 13, 14]))
    scalars_by_column = {column: arguments[5 + index][1] for index, (_, column) in enumerate(page._SCALAR_COLUMNS)}
    position_by_child_field = {
        (child_id, slot): i
        for i, (child_id, slot) in enumerate(
            zip(scalars_by_column["child_revision_id"], scalars_by_column["field_slot"], strict=True)
        )
    }
    assert scalars_by_column["decimal_value"][position_by_child_field[12, 5]] == child_values_by_field["amount"]
    assert scalars_by_column["integer_value"][position_by_child_field[12, 7]] == child_values_by_field["visits"]
    assert scalars_by_column["boolean_value"][position_by_child_field[12, 8]] is False
    assert scalars_by_column["date_value"][position_by_child_field[12, 9]] == child_values_by_field["day"]
    assert scalars_by_column["timestamp_value"][position_by_child_field[12, 10]] == child_values_by_field[
        "instant"
    ].astimezone(dt.UTC)
    assert scalars_by_column["value_state"][position_by_child_field[13, 5]] == "null"
    assert (14, 5) not in position_by_child_field
    contexts = [json.loads(context) for context in arguments[17][1]]
    assert [context["dimensions"][1]["value_state"] for context in contexts] == ["value", "null", "missing"]


@pytest.mark.parametrize("child_count", [0, 3])
async def test_source_append_uses_one_boundary_and_no_orm_write(monkeypatch, child_count):
    request, registry, family_input, progress, family = _input()
    child_revisions = [
        _child(request, family_input, dict(rate_npi="1234567893", service_code=f"A{i}", amount=Decimal("1.01")), 12 + i)
        for i in range(child_count)
    ]
    session = SimpleNamespace(get=AsyncMock(side_effect=[progress, family]), add_all=Mock())
    call = AsyncMock(return_value=SimpleNamespace(one=lambda: SimpleNamespace(complete=child_count == 0)))
    monkeypatch.setattr(page, "_call", call)
    monkeypatch.setattr(graph, "_page_session", lambda *_: nullcontext((session, SimpleNamespace(build_id=7))))
    monkeypatch.setattr(graph, "_prepare_statement", AsyncMock())
    flush = AsyncMock()
    monkeypatch.setattr(graph, "_flush_page", flush)
    await graph._append_child_batch(None, request, registry, family_input, progress, child_revisions)
    call.assert_awaited_once()
    assert session.get.await_count == 2
    session.add_all.assert_not_called()
    assert not hasattr(graph, "_commit_family")
    flush.assert_not_awaited()


async def test_eof_requires_protected_boundary_completion(monkeypatch):
    request, registry, family_input, progress, family = _input()
    session = SimpleNamespace(get=AsyncMock(side_effect=[progress, family]))
    monkeypatch.setattr(
        page, "_call", AsyncMock(return_value=SimpleNamespace(one=lambda: SimpleNamespace(complete=False)))
    )
    monkeypatch.setattr(graph, "_page_session", lambda *_: nullcontext((session, SimpleNamespace(build_id=7))))
    monkeypatch.setattr(graph, "_prepare_statement", AsyncMock())
    with pytest.raises(CandidateRunnerError, match="family input ended"):
        await graph._append_child_batch(None, request, registry, family_input, progress, [])


@pytest.mark.parametrize("corrupt", ["payload", "key", "parent"])
async def test_child_codec_failure_prevents_boundary(monkeypatch, corrupt):
    request, registry, family_input, progress, family = _input()
    child = _child(request, family_input, dict(rate_npi="1234567893", service_code="A100", amount=Decimal("1.01")))
    setattr(
        child,
        {"payload": "payload_sha256", "key": "child_key_sha256", "parent": "parent_key_sha256"}[corrupt],
        b"x" * 32,
    )
    session = SimpleNamespace(get=AsyncMock(side_effect=[progress, family]))
    call = AsyncMock()
    monkeypatch.setattr(page, "_call", call)
    monkeypatch.setattr(graph, "_page_session", lambda *_: nullcontext((session, SimpleNamespace(build_id=7))))
    monkeypatch.setattr(graph, "_prepare_statement", AsyncMock())
    with pytest.raises(CandidateRunnerError):
        await graph._append_child_batch(None, request, registry, family_input, progress, [child])
    call.assert_not_awaited()


async def test_page_fanout_failure_prevents_boundary(monkeypatch):
    request, registry, family_input, progress, family = _input()
    child = _child(request, family_input, dict(rate_npi="1234567893", service_code="A100", amount=Decimal("1.01")))
    request = _request(page_row_limit=1)
    session = SimpleNamespace(get=AsyncMock(side_effect=[progress, family]))
    call = AsyncMock()
    monkeypatch.setattr(page, "_call", call)
    monkeypatch.setattr(graph, "_page_session", lambda *_: nullcontext((session, SimpleNamespace(build_id=7))))
    monkeypatch.setattr(graph, "_prepare_statement", AsyncMock())
    with pytest.raises(CandidateRunnerError, match="fanout exceeds"):
        await graph._append_child_batch(None, request, registry, family_input, progress, [child])
    call.assert_not_awaited()


@pytest.mark.parametrize(
    "state,mutation,rejected",
    [
        ("value", "omit", True),
        ("null", "omit", True),
        ("missing", "omit", False),
        ("value", "null", True),
        ("missing", "null", True),
    ],
)
async def test_nullable_projection_presence_state_contract_with_escaped_nul(monkeypatch, state, mutation, rejected):
    raw = _raw_definition()
    raw["schema"]["children"][0]["fields"].append(
        {"id": "unprojected_note", "slot": 6, "type": "string", "nullable": True}
    )
    request, registry, family_input, progress, family = _input(definition=CustomImportDefinition.from_mapping(raw))
    child_values_by_field = dict(rate_npi="1234567893", service_code="A100", unprojected_note="synthetic\x00note")
    if state != "missing":
        child_values_by_field["amount"] = Decimal("1.01") if state == "value" else None
    child = _child(request, family_input, child_values_by_field)
    assert "\\u0000" in child.canonical_payload
    projections = await graph._child_page_models(
        None, request, registry, SimpleNamespace(build_id=7), family_input, child, family
    )
    expected_states = {
        (projection.child_revision_id, projection.field_slot, projection.value_state)
        for projection in projections
        if isinstance(projection, CustomImportChildScalar)
    }
    supplied_projections = [
        projection
        for projection in projections
        if not isinstance(projection, CustomImportChildScalar) or projection.field_slot != 5
    ]
    if mutation == "null":
        supplied_projections.append(
            CustomImportChildScalar(
                child_revision_id=child.child_revision_id, field_slot=5, field_type="decimal", value_state="null"
            )
        )

    # A host contract double compares native array rows against the existing trusted
    # projection helper. SQL structure below requires the same symmetric comparison;
    # actual_states PostgreSQL rejection still needs the migration owner's native proof.
    async def protected_boundary(_session, _function, arguments):
        actual_states = set(zip(arguments[5][1], arguments[6][1], arguments[8][1], strict=True))
        if actual_states != expected_states:
            raise CandidateRunnerError("custom_import_build_incomplete")
        return SimpleNamespace(one=lambda: "receipt")

    monkeypatch.setattr(page, "_call", AsyncMock(side_effect=protected_boundary))
    operation = page.append_source_child_page(None, family_input.plan, progress, family, [child], supplied_projections)
    if rejected:
        with pytest.raises(CandidateRunnerError, match="custom_import_build_incomplete"):
            await operation
    else:
        assert await operation == "receipt"
