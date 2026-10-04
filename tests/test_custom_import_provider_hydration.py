# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native pages hydrate only the exact query's bounded selected winners."""

from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import read_core
from process.custom_import.read_contracts import (
    CustomImportReadAuthorizationError,
    CustomImportReadRequestError,
    CustomImportReadUnavailableError,
    ExtensionReadAuthorization,
)
from tests import test_custom_import_provider_query as query_fixture


@pytest.mark.parametrize(
    "entities", [[], ("1000000001",) * 2, ("1",), (True,), ("１" * 10,), tuple(str(1000000000 + n) for n in range(201))]
)
def test_provider_hydration_rejects_unbounded_or_noncanonical_page(entities):
    with pytest.raises(CustomImportReadRequestError):
        read_core._validate_npi_page(entities)


def test_provider_hydration_accepts_full_native_page():
    read_core._validate_npi_page(tuple(str(1000000000 + n) for n in range(200)))
    read_core._validate_npi_page(())


@pytest.mark.asyncio
async def test_provider_hydration_authorizes_before_storage():
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(CustomImportReadAuthorizationError):
        await read_core.CustomImportReadService(authorizer=None).hydrate_npi_page(
            session,
            authorization=ExtensionReadAuthorization("synthetic"),
            pinned_target=query_fixture._target(),
            prepared=None,
            entity_values=(),
        )
    session.execute.assert_not_awaited()


def _install_context(monkeypatch):
    context = query_fixture._context()

    @asynccontextmanager
    async def bounded(session, *, timeout_ms):
        yield

    monkeypatch.setattr(read_core, "_bounded_read_window", bounded)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=context))
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())
    return context


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["query_fingerprint", "authorization_scope_sha256"])
async def test_provider_hydration_rejects_different_prepared_identity(monkeypatch, changed):
    context = _install_context(monkeypatch)
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    session = SimpleNamespace(execute=AsyncMock())
    authorization = ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)
    with pytest.raises(CustomImportReadUnavailableError, match="query identity"):
        await service.hydrate_npi_page(
            session,
            authorization=authorization,
            pinned_target=context.target,
            prepared=replace(prepared, **{changed: "0" * 64}),
            entity_values=(),
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_hydration_rejects_prepared_target_reuse(monkeypatch):
    context = _install_context(monkeypatch)
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    session = SimpleNamespace(execute=AsyncMock())
    authorization = ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)
    next_target = replace(context.target, generation_id=context.target.generation_id + 1)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=replace(context, target=next_target)))
    with pytest.raises(CustomImportReadUnavailableError, match="query identity"):
        await service.hydrate_npi_page(
            session, authorization=authorization, pinned_target=next_target, prepared=prepared, entity_values=()
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_hydration_selects_one_winner_per_npi_before_single_batch(monkeypatch):
    context = _install_context(monkeypatch)
    service = read_core.CustomImportReadService(authorizer=query_fixture._Allow())
    selected_values = (object(), object(), object(), object())
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [(*selected_values, "1000000001")]))
    )
    hydrated = object()
    batch = AsyncMock(return_value=(hydrated,))
    monkeypatch.setattr(read_core, "_hydrate_provider_page_items", batch)
    authorization = ExtensionReadAuthorization("synthetic")
    prepared = await service.prepare_npi_entity_relation(session, authorization=authorization, target=context.target)

    result = await service.hydrate_npi_page(
        session,
        authorization=authorization,
        pinned_target=context.target,
        prepared=prepared,
        entity_values=("1000000001", "1000000000"),
    )

    assert result == {"1000000001": hydrated}
    batch.assert_awaited_once_with(session, context, (selected_values,), read_core._scope_digest(service._authorize(authorization, context.target)))
    statement = session.execute.await_args.args[0]
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    assert "DISTINCT ON (" in compiled
    assert "canonical_value IN" in compiled
    assert "ORDER BY" in compiled and "root_record_id ASC" in compiled
    read_core.verify_published_generation.assert_awaited_once_with(session, context.target)


def _selected_family_rows(families):
    """Supply exact selected winners to the existing batched detail hydrator."""

    return tuple(
        (
            SimpleNamespace(entity_binding_id=index + 1, context_key_sha256=b"a" * 32),
            family,
            SimpleNamespace(root_revision_id=family.root_record_id),
            None,
        )
        for index, family in enumerate(families)
    )


@pytest.mark.asyncio
async def test_complete_family_children_are_correlated_and_batched():
    """One membership and scalar query retain every selected collection and family."""

    field = SimpleNamespace(
        field_id="metric", field_slot=1, projection_slot=1, collection="facts", value_type="decimal", nullable=True
    )
    note = SimpleNamespace(
        field_id="note", field_slot=2, projection_slot=2, collection="notes", value_type="string", nullable=True
    )
    context = SimpleNamespace(
        target=query_fixture._target(),
        definition=SimpleNamespace(root_fields=(), child_fields=(field, note)),
        collection_slots_by_name={"facts": 1, "notes": 2},
        collection_names_by_slot={1: "facts", 2: "notes"},
    )
    families = (
        SimpleNamespace(family_revision_id=11, root_record_id=1, child_count=2),
        SimpleNamespace(family_revision_id=12, root_record_id=2, child_count=1),
    )
    membership_rows = tuple(
        (
            SimpleNamespace(family_revision_id=family_id, collection_slot=collection_slot),
            SimpleNamespace(child_revision_id=child_id),
        )
        for family_id, collection_slot, child_id in ((11, 1, 101), (11, 2, 102), (12, 1, 103))
    )
    scalar_rows = (
        SimpleNamespace(
            child_revision_id=101, field_slot=1, field_type="decimal", value_state="value", decimal_value=Decimal("2.5")
        ),
        SimpleNamespace(child_revision_id=102, field_slot=2, field_type="string", value_state="null"),
    )
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=(
                SimpleNamespace(all=lambda: membership_rows),
                SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: scalar_rows)),
            )
        )
    )

    details = await read_core._hydrate_selected_families(session, context, _selected_family_rows(families), "a" * 64)
    children_by_family = {detail.winner.family_revision_id: detail.children for detail in details}

    assert [(child.collection, child.child_revision_id) for child in children_by_family[11]] == [("facts", 101), ("notes", 102)]
    assert children_by_family[11][0].fields[0].value == Decimal("2.5")
    assert children_by_family[11][1].fields[0].state == "null"
    assert children_by_family[12][0].child_revision_id == 103 and children_by_family[12][0].fields[0].state == "missing"
    assert session.execute.await_count == 2
    membership_query = session.execute.await_args_list[0].args[0].compile(dialect=postgresql.dialect())
    assert "family_revision_id," in str(membership_query) and "root_record_id) IN" in str(membership_query)
    assert [(11, 1), (12, 2)] in membership_query.params.values()
    assert context.target.dataset_id in membership_query.params.values()
    assert context.target.schema_revision_id in membership_query.params.values()


@pytest.mark.asyncio
@pytest.mark.parametrize("counts", [(1001,), (501, 500), (-1,)])
async def test_family_child_limit_rejects_before_membership_reads(counts):
    session = SimpleNamespace(execute=AsyncMock())
    families = tuple(SimpleNamespace(child_count=count) for count in counts)
    with pytest.raises(CustomImportReadUnavailableError, match="bounded detail child limit"):
        await read_core._hydrate_selected_families(
            session, None, tuple((None, family) for family in families), "a" * 64
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_family_membership_mismatch_fails_before_scalar_reads(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: ())))
    family = SimpleNamespace(family_revision_id=11, root_record_id=1, child_count=1)
    monkeypatch.setattr(read_core, "_root_scalar_rows", AsyncMock(return_value={}))
    children = AsyncMock()
    monkeypatch.setattr(read_core, "_child_scalar_rows", children)
    with pytest.raises(CustomImportReadUnavailableError, match="membership is incomplete"):
        await read_core._hydrate_selected_families(
            session,
            query_fixture._context(),
            _selected_family_rows((family,)),
            "a" * 64,
        )
    children.assert_not_awaited()
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_page_child_expansion_deduplicates_families_and_retains_item_order(
    monkeypatch,
):
    family_a = SimpleNamespace(family_revision_id=11, child_count=1)
    family_b = SimpleNamespace(family_revision_id=12, child_count=1)
    selected_rows = ((None, family_b), (None, family_a), (None, family_b))
    provider_items = tuple(
        read_core.SearchItem(
            read_core.WinnerLocator(1, selected_row[1].family_revision_id, 2, b"a" * 32),
            (),
            None,
            (),
        )
        for selected_row in selected_rows
    )
    children_by_family = {
        11: (read_core.ReadChild("facts", 101, ()),),
        12: (read_core.ReadChild("notes", 102, ()),),
    }
    hydrate_children = AsyncMock(
        return_value=tuple(
            SimpleNamespace(
                winner=SimpleNamespace(family_revision_id=family_id),
                root_fields=(),
                children=children,
            )
            for family_id, children in children_by_family.items()
        )
    )
    monkeypatch.setattr(read_core, "_hydrate_selected_families", hydrate_children)
    monkeypatch.setattr(read_core, "_hydrate_search_page_items", AsyncMock(return_value=provider_items))

    hydrated_items = await read_core._hydrate_provider_page_items("session", "context", selected_rows, "a" * 64)

    hydrate_children.assert_awaited_once_with("session", "context", (selected_rows[2], selected_rows[1]), "a" * 64)
    assert [provider_item.winner for provider_item in hydrated_items] == [
        provider_item.winner for provider_item in provider_items
    ]
    assert [provider_item.children for provider_item in hydrated_items] == [
        children_by_family[12],
        children_by_family[11],
        children_by_family[12],
    ]


def _ordinary_family_projection(selected_row):
    child_projections = tuple(
        read_core.ReadChild("facts", child + 1, (read_core.ReadFieldValue("text", "string", "value", "x" * 300),))
        for child in range(42)
    )
    return SimpleNamespace(
        winner=SimpleNamespace(family_revision_id=selected_row[1].family_revision_id), root_fields=(), children=child_projections
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("family_count", (25, 50))
async def test_complete_ordinary_page_batches_per_family_without_dropping_children(monkeypatch, family_count):
    """Keep every exact family on normal pages while bounding each read batch."""

    families = tuple(
        SimpleNamespace(family_revision_id=index + 1, root_record_id=index + 1, child_count=42)
        for index in range(family_count)
    )
    selected_rows = tuple(reversed(_selected_family_rows(families)))
    provider_items = tuple(
        read_core.SearchItem(read_core.WinnerLocator(1, selected_row[1].family_revision_id, 2, b"a" * 32), (), None, ())
        for selected_row in selected_rows
    )
    batches = []

    async def hydrate(session, context, family_rows, scope):
        assert session == "session" and context == "context" and scope == "a" * 64
        batches.append(tuple(selected_row[1].family_revision_id for selected_row in family_rows))
        assert sum(selected_row[1].child_count for selected_row in family_rows) <= 1000
        return tuple(_ordinary_family_projection(selected_row) for selected_row in family_rows)

    monkeypatch.setattr(read_core, "_hydrate_selected_families", hydrate)
    monkeypatch.setattr(read_core, "_hydrate_search_page_items", AsyncMock(return_value=provider_items))
    hydrated_items = await read_core._hydrate_provider_page_items("session", "context", selected_rows, "a" * 64)
    assert tuple(family for batch in batches for family in batch) == tuple(
        selected_row[1].family_revision_id for selected_row in selected_rows
    )
    assert len(batches) == (2 if family_count == 25 else 3)
    assert [provider_item.winner for provider_item in hydrated_items] == [provider_item.winner for provider_item in provider_items]
    assert all(len(provider_item.children) == 42 and provider_item.children[-1].child_revision_id == 42 for provider_item in hydrated_items)


@pytest.mark.asyncio
@pytest.mark.parametrize("counts", ((42, 1001), (42, -1)))
async def test_ordinary_page_preflights_every_family_before_hydration(monkeypatch, counts):
    family_rows = tuple(
        (None, SimpleNamespace(family_revision_id=index + 1, child_count=count)) for index, count in enumerate(counts)
    )
    hydrate = AsyncMock()
    monkeypatch.setattr(read_core, "_hydrate_selected_families", hydrate)
    with pytest.raises(CustomImportReadUnavailableError, match="bounded detail child limit"):
        await read_core._hydrate_provider_page_items(None, None, family_rows, "a" * 64)
    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_page_rejects_oversized_family_before_retaining_next_batch(
    monkeypatch,
):
    family_rows = tuple((None, SimpleNamespace(family_revision_id=index + 1, child_count=600)) for index in range(2))
    hydrate = AsyncMock(
        return_value=(
            SimpleNamespace(
                root_fields=(
                    read_core.ReadFieldValue(
                        "text",
                        "string",
                        "value",
                        "x" * read_core.MAX_FAMILY_RESPONSE_BYTES,
                    ),
                ),
                children=(),
            ),
        )
    )
    monkeypatch.setattr(read_core, "_hydrate_selected_families", hydrate)
    with pytest.raises(CustomImportReadUnavailableError, match="response exceeds"):
        await read_core._hydrate_provider_page_items(None, None, family_rows, "a" * 64)
    hydrate.assert_awaited_once_with(None, None, (family_rows[0],), "a" * 64)


@pytest.mark.asyncio
async def test_empty_provider_page_reads_no_child_membership():
    session = SimpleNamespace(execute=AsyncMock())
    assert await read_core._hydrate_provider_page_items(session, query_fixture._context(), (), "a" * 64) == ()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("counts", ((0, 0), (500, 500), (1000, 0)))
async def test_child_page_bound_is_inclusive(counts):
    families = tuple(
        SimpleNamespace(family_revision_id=index + 1, root_record_id=index + 1, child_count=count)
        for index, count in enumerate(counts)
    )
    membership_rows = tuple(
        (
            SimpleNamespace(family_revision_id=index + 1, collection_slot=1),
            SimpleNamespace(child_revision_id=index * 1000 + child_index + 1),
        )
        for index, count in enumerate(counts)
        for child_index in range(count)
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: membership_rows)))
    context = SimpleNamespace(
        target=query_fixture._target(),
        definition=SimpleNamespace(root_fields=(), child_fields=()),
        collection_slots_by_name={"facts": 1},
        collection_names_by_slot={1: "facts"},
    )

    details = await read_core._hydrate_selected_families(session, context, _selected_family_rows(families), "a" * 64)
    children_by_family = {detail.winner.family_revision_id: detail.children for detail in details}

    assert tuple(len(children_by_family[index + 1]) for index in range(2)) == counts
    assert session.execute.await_count == int(sum(counts) > 0)


@pytest.mark.asyncio
async def test_summary_hydration_does_not_expand_children(monkeypatch):
    context = query_fixture._context()
    winner = SimpleNamespace(entity_binding_id=3, context_key_sha256=b"a" * 32)
    family = SimpleNamespace(family_revision_id=2, root_record_id=1)
    root = SimpleNamespace(root_revision_id=4)
    monkeypatch.setattr(read_core, "_root_scalar_rows", AsyncMock(return_value={}))
    monkeypatch.setattr(read_core, "_child_scalar_rows", AsyncMock(return_value={}))
    children = AsyncMock(side_effect=AssertionError("summary expanded full-family children"))
    monkeypatch.setattr(read_core, "_hydrate_selected_families", children)

    summaries = await read_core._hydrate_search_page_items(None, context, ((winner, family, root, None),))

    assert len(summaries) == 1 and summaries[0].children == ()
    children.assert_not_awaited()
