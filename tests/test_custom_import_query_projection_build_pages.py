# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validate complete entity prefixes before child payloads and projection writes."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import literal, select

from process.custom_import import query_projection_build as build
from process.custom_import import query_projection_digest as digest
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_query_projection_pages import _source
from tests.test_custom_import_query_projection_storage import _definition


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["root", "complete"])
async def test_digest_rejects_open_or_completed_rows_before_source_access(monkeypatch, phase):
    source = AsyncMock()
    monkeypatch.setattr(digest, "_source_context", source)
    with pytest.raises(CandidateRunnerError, match="closed, verified rows"):
        await digest.digest_projection_page(object(), object(), object(), SimpleNamespace(phase=phase))
    source.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oversized", [False, True])
async def test_digest_byte_budget_rejects_oversize_and_limits_fetched_contexts(monkeypatch, oversized):
    definition = _definition()
    request = SimpleNamespace(definition=definition, page_row_limit=256, page_byte_limit=100)
    table = digest.query_projection_tables(17, definition)["custom_import_derived_root"]
    page = select(*table.c, literal("entity_a").label("canonical_entity")).cte("semantic_projection_page")
    monkeypatch.setattr(digest, "_ordered_contexts", Mock(return_value=page))
    metadata = [(1, 2024, 101)] if oversized else [(1, 2024, 60), (2, 2024, 60)]
    rows = [{"entity_binding_id": 1, "selected_value": 2024, "canonical_entity": "entity_a", "field_1": None}]
    execute = AsyncMock(
        side_effect=[
            SimpleNamespace(all=lambda: metadata),
            SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows)),
        ]
    )
    monkeypatch.setattr(digest, "_execute", execute)
    operation = digest._digest_page(object(), request, object(), SimpleNamespace(family_id=17), child=False, after=None)
    if oversized:
        with pytest.raises(CandidateRunnerError, match="exceeds the byte page"):
            await operation
        assert execute.await_count == 1
    else:
        assert await operation == rows
        assert execute.await_count == 2
        parameters = execute.await_args.args[1].compile().params
        assert any(value == [(1, 2024)] for value in parameters.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("child", (False, True))
@pytest.mark.parametrize("empty", (False, True))
async def test_write_validates_the_original_entity_prefix_before_payload_or_cursor(monkeypatch, child, empty):
    definition = _definition()
    request = SimpleNamespace(definition=definition, page_row_limit=256, page_byte_limit=1024)
    context, session = SimpleNamespace(definition=definition), object()
    manifest = SimpleNamespace(
        family_id=17,
        phase="child" if child else "root",
        after_entity_binding_id=0,
        after_selected_value=None,
        after_child_key_sha256=None,
        root_count=0,
        child_count=0,
    )
    source_relation = _source(child=child)
    values_by_field = {
        "quality" if child else "score": source_relation.c.metric,
        "quantity" if child else "weight": source_relation.c.weight,
    }
    identity = (3, 2024, b"a" * 32) if child else (3, 2024)
    identities = [] if empty else [identity]
    keys = [(*identity, '["chosen"]')] if child else identities
    validate = AsyncMock(return_value=(source_relation, values_by_field))
    slot = AsyncMock(return_value=1)
    monkeypatch.setattr(build, "_source_context", AsyncMock(return_value=context))
    monkeypatch.setattr(build, "_entity_window", AsyncMock(return_value=[1, 2, 3, 4]))
    monkeypatch.setattr(build, "_child_collection_slot", slot)
    monkeypatch.setattr(build, "root_source", Mock(return_value=(_source(), {})))
    monkeypatch.setattr(build, "child_source", Mock(return_value=(source_relation, values_by_field)))
    monkeypatch.setattr(build, "_bounded_key_metadata", AsyncMock(return_value=identities))
    monkeypatch.setattr(build, "_validated_source", validate)

    async def execute(_session, statement):
        validate.assert_awaited_once()
        return SimpleNamespace(rowcount=len(keys)) if statement.is_insert else SimpleNamespace(all=lambda: keys)

    execute_mock = AsyncMock(side_effect=execute)
    monkeypatch.setattr(build, "_execute", execute_mock)
    await build._write_contexts(session, request, object(), manifest)
    validate.assert_awaited_once_with(
        session, context, [1, 2, 3, 4] if empty else [1, 2, 3], child=child, collection_slot=1 if child else None
    )
    assert slot.await_count == int(child)
    if empty:
        execute_mock.assert_not_awaited()
        assert (manifest.after_entity_binding_id, manifest.after_selected_value) == (4, 2**63 - 1)
        assert manifest.root_count == manifest.child_count == 0
    else:
        assert execute_mock.await_count == 1 + child
        assert (manifest.after_entity_binding_id, manifest.after_selected_value) == identity[:2]
        assert (manifest.root_count, manifest.child_count) == ((0, 1) if child else (1, 0))


@pytest.mark.asyncio
@pytest.mark.parametrize("child", (False, True))
async def test_invalid_complete_prefix_cannot_fetch_payload_write_or_advance(monkeypatch, child):
    definition = _definition()
    source = _source(child=child)
    manifest = SimpleNamespace(phase="child" if child else "root", after_entity_binding_id=0)
    identity = (3, 2024, b"a" * 32) if child else (3, 2024)
    monkeypatch.setattr(build, "_source_context", AsyncMock(return_value=SimpleNamespace(definition=definition)))
    monkeypatch.setattr(build, "_entity_window", AsyncMock(return_value=[1, 2, 3, 4]))
    monkeypatch.setattr(build, "_child_collection_slot", AsyncMock(return_value=1))
    monkeypatch.setattr(build, "root_source", Mock(return_value=(_source(), {})))
    monkeypatch.setattr(build, "child_source", Mock(return_value=(source, {})))
    monkeypatch.setattr(build, "_bounded_key_metadata", AsyncMock(return_value=[identity]))
    monkeypatch.setattr(build, "_validated_source", AsyncMock(side_effect=CandidateRunnerError("ambiguous")))
    execute = AsyncMock()
    monkeypatch.setattr(build, "_execute", execute)
    with pytest.raises(CandidateRunnerError, match="ambiguous"):
        await build._write_contexts(
            object(), SimpleNamespace(definition=definition, page_row_limit=256), object(), manifest
        )
    execute.assert_not_awaited()
    assert manifest.after_entity_binding_id == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("child", (False, True))
@pytest.mark.parametrize("oversized", (False, True))
async def test_discovery_fetches_only_fixed_metadata_with_the_existing_byte_bound(monkeypatch, child, oversized):
    source = _source(child=child)
    identity = (1, 2024, b"a" * 32) if child else (1, 2024)
    execute = AsyncMock(
        return_value=SimpleNamespace(all=lambda: [(*identity, 201 if oversized else 150), (*identity, 150)])
    )
    monkeypatch.setattr(build, "_execute", execute)
    request = SimpleNamespace(definition=_definition(), page_row_limit=256, page_byte_limit=200)
    manifest = SimpleNamespace(phase="child" if child else "root", after_entity_binding_id=0)
    operation = build._bounded_key_metadata(object(), source, request, manifest)
    if oversized:
        with pytest.raises(CandidateRunnerError, match="byte page"):
            await operation
    else:
        accepted = await operation
        assert accepted == [identity]
    execute.assert_awaited_once()
    assert "canonical_child_key" not in execute.await_args.args[1].selected_columns
