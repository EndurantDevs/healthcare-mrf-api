# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execute routed ORM reads against distinct in-memory synthetic row stores.

These host fixtures test query semantics, not PostgreSQL constraints, registry
authority, OID validation, publication, or concurrent snapshot retirement.
"""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import Column, MetaData, Table, create_engine
from sqlalchemy.orm import Session

from db.models import custom_import as models
from process.custom_import import grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.storage_layout import snapshot_models
from tests import custom_import_grouped_child_support as fixture

_SCOPE = read_core.ExtensionReadScope("synthetic:snapshot-read")
_NPI = "1234567893"
_HOT = (
    models.CustomImportWinner,
    models.CustomImportFamilyRevision,
    models.CustomImportRootRevision,
    models.CustomImportGenerationFamily,
    models.CustomImportEntityBinding,
    models.CustomImportFamilyChild,
    models.CustomImportChildRevision,
    models.CustomImportRootScalar,
    models.CustomImportChildScalar,
)


def _tables(connection, schema, source_models):
    metadata = MetaData()
    tables_by_model = {
        model: Table(
            model.__tablename__,
            metadata,
            *(Column(column.name, column.type, primary_key=column.primary_key) for column in model.__table__.columns),
            schema=schema,
        )
        for model in source_models
    }
    metadata.create_all(connection)
    return tables_by_model


def _populate(connection, tables_by_model, context, offset, current_score):
    """Populate three periods/groups and their distinct sibling values."""

    def insert(model, **values_by_column):
        scope_by_name = {"dataset_id": 1, "definition_revision_id": 3, "schema_revision_id": 4}
        scope_by_column = {key: scope for key, scope in scope_by_name.items() if key in tables_by_model[model].c}
        connection.execute(tables_by_model[model].insert(), scope_by_column | values_by_column)

    insert(models.CustomImportEntityBinding, entity_binding_id=501, adapter_id="npi", canonical_value=_NPI)
    for index, (period, group, score) in enumerate(
        ((2024, "segment_a", Decimal("100")), (2025, "segment_a", current_score), (2025, "segment_b", Decimal("5")))
    ):
        family_id, root_id, root_revision_id = offset + 101 + index, offset + 201 + index, offset + 301 + index
        children = (("other", Decimal("9"), Decimal("1"), 10), ("chosen", Decimal("2"), Decimal("9"), 30))
        if index != 1:
            children = (("chosen", Decimal("500"), Decimal("1"), 0),)
        insert(
            models.CustomImportFamilyRevision,
            family_revision_id=family_id,
            root_record_id=root_id,
            root_revision_id=root_revision_id,
            entity_binding_id=501,
            child_count=len(children),
        )
        insert(models.CustomImportRootRevision, root_revision_id=root_revision_id, root_record_id=root_id)
        insert(
            models.CustomImportGenerationFamily,
            generation_id=context.target.generation_id,
            family_revision_id=family_id,
            root_record_id=root_id,
        )
        root_values_by_field = {
            "npi": _NPI,
            "display_name": f"Synthetic {family_id}",
            "period": period,
            "segment": group,
            "score": score,
        }
        _root_scalars(insert, context, root_id, root_revision_id, root_values_by_field)
        _children(insert, context, family_id, root_id, offset + 1001 + index * 10, children)
        _winners(insert, context, family_id, period, group, is_latest=index == 1)


def _root_scalars(insert, context, root_id, revision_id, values_by_field):
    for field_id, scalar_value in values_by_field.items():
        field = context.definition.fields_by_id[field_id]
        insert(
            models.CustomImportRootScalar,
            root_revision_id=revision_id,
            root_record_id=root_id,
            field_slot=field.field_slot,
            field_type=field.value_type,
            value_state="value",
            **{read_core._SCALAR_COLUMNS[field.value_type]: scalar_value},
        )


def _children(insert, context, family_id, root_id, first_child_id, children):
    for child_index, (key, amount, quality, ordinal) in reversed(tuple(enumerate(children))):
        child_id = first_child_id + child_index
        insert(
            models.CustomImportFamilyChild,
            family_revision_id=family_id,
            root_record_id=root_id,
            collection_slot=1,
            child_revision_id=child_id,
        )
        insert(
            models.CustomImportChildRevision,
            child_revision_id=child_id,
            root_record_id=root_id,
            collection_slot=1,
            source_ordinal=ordinal,
        )
        for field_id, scalar_value in {"service_code": key, "amount": amount, "quality": quality}.items():
            field = context.definition.fields_by_id[field_id]
            insert(
                models.CustomImportChildScalar,
                child_revision_id=child_id,
                root_record_id=root_id,
                collection_slot=1,
                field_slot=field.field_slot,
                field_type=field.value_type,
                value_state="value",
                **{read_core._SCALAR_COLUMNS[field.value_type]: scalar_value},
            )


def _winners(insert, context, family_id, period, group, *, is_latest):
    profiles = [(1, "families_by_period", {"period": period, "segment": group})]
    if is_latest:
        profiles.append((2, "latest_period", {}))
    for profile_slot, profile_id, context_by_name in profiles:
        insert(
            models.CustomImportWinner,
            generation_id=context.target.generation_id,
            profile_slot=profile_slot,
            entity_binding_id=501,
            family_revision_id=family_id,
            context_key_sha256=grouped_read._profile_context_digest(context, profile_id, context_by_name),
            context_collection_slot=0,
        )


@pytest.fixture
def row_store():
    engine = create_engine("sqlite://")
    try:
        with engine.connect() as connection:
            for schema in ("mrf", "ci_snapshot_17", "ci_snapshot_18"):
                connection.exec_driver_sql(f"ATTACH DATABASE ':memory:' AS {schema}")
            controls_by_model = _tables(
                connection, "mrf", (models.CustomImportGeneration, models.CustomImportGenerationSeal)
            )
            contexts_by_family_id = {}
            for family_id, generation_id, offset, score in (
                (None, 2, 20000, "99"),
                (17, 2, 0, "2"),
                (18, 20, 10000, "7"),
            ):
                context = replace(
                    fixture.context(), storage_models=None if family_id is None else snapshot_models(family_id)
                )
                context = replace(context, target=replace(context.target, generation_id=generation_id))
                contexts_by_family_id[family_id] = context
                schema = "mrf" if family_id is None else f"ci_snapshot_{family_id}"
                _populate(connection, _tables(connection, schema, _HOT), context, offset, Decimal(score))
            for generation_id in (2, 20):
                scope_by_name = {
                    "generation_id": generation_id,
                    "dataset_id": 1,
                    "definition_revision_id": 3,
                    "schema_revision_id": 4,
                }
                connection.execute(controls_by_model[models.CustomImportGeneration].insert(), scope_by_name)
                connection.execute(
                    controls_by_model[models.CustomImportGenerationSeal].insert(),
                    scope_by_name | {"seal_contract": "custom-import-generation-seal/v1"},
                )
            connection.commit()
            with Session(connection) as session:
                yield session, contexts_by_family_id
    finally:
        engine.dispose()


def _rows(session, context, query):
    return session.execute(grouped_read.prepare_relation(context, query, _SCOPE).statement).all()


def test_grouped_selection_reads_each_exact_leaf_and_retains_legacy_storage(row_store):
    session, contexts = row_store
    query = fixture.query(
        context_filters=(read_core.ReadFilter("segment", "eq", "segment_a"),),
        order_terms=(read_core.ReadOrderTerm("score", "desc", "last"),),
    )
    assert _rows(session, contexts[17], query) == [(_NPI, Decimal("2"))]
    assert _rows(session, contexts[18], query) == [(_NPI, Decimal("7"))]
    assert _rows(session, contexts[None], query) == [(_NPI, Decimal("99"))]
    assert _rows(session, contexts[17], query) == [(_NPI, Decimal("2"))]
    filtered = replace(query, filters=(read_core.ReadFilter("score", "gt", "50"),))
    assert _rows(session, contexts[17], filtered) == []
    assert _rows(
        session,
        contexts[17],
        replace(filtered, context_filters=query.context_filters + (read_core.ReadFilter("period", "eq", 2024),)),
    ) == [(_NPI, Decimal("100"))]


def test_child_predicates_preserve_sibling_and_period_identity(row_store):
    session, contexts = row_store
    context = contexts[17]
    selectors = (
        read_core.ReadFilter("segment", "eq", "segment_a"),
        read_core.ReadFilter("service_code", "eq", "chosen"),
    )
    query = fixture.query(context_filters=selectors, order_terms=(read_core.ReadOrderTerm("amount", "desc", "last"),))
    assert _rows(session, context, query) == [(_NPI, Decimal("2"))]
    assert _rows(session, context, replace(query, filters=(read_core.ReadFilter("amount", "gt", "8"),))) == []
    assert _rows(session, context, replace(query, filters=(read_core.ReadFilter("quality", "lt", "2"),))) == []


@pytest.mark.asyncio
async def test_snapshot_entity_detail_and_grouped_page_use_actual_alias_rows(row_store, monkeypatch):
    session, contexts = row_store
    context = contexts[17]
    asynchronous = SimpleNamespace(
        execute=AsyncMock(side_effect=session.execute), scalar=AsyncMock(side_effect=session.scalar)
    )
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())
    query = fixture.query(family_entitlement="full_family")
    prepared = grouped_read.prepare_relation(context, query, _SCOPE)
    page = await grouped_read.hydrate_page(asynchronous, context, query, prepared, (_NPI,), _SCOPE)
    selected = page[_NPI]
    assert selected.selection_value == 2025
    assert [group for group, _ in selected.families] == ["segment_a", "segment_b"]
    assert [family.winner.family_revision_id for _, family in selected.families] == [102, 103]
    first = selected.families[0][1]
    assert [child.child_revision_id for child in first.children] == [1011, 1012]
    assert [child.fields[0].value for child in first.children] == ["other", "chosen"]
    assert next(field.value for field in first.root_fields if field.field_id == "score") == Decimal("2")
    request = read_core.RootDetailRequest(
        context.target,
        read_core.EntityLocator("npi", _NPI),
        family_entitlement="full_family",
        context_filters=(),
        grouped_entity_selection=fixture.query().grouped_entity_selection,
        grouped_child_query=fixture.query().grouped_child_query,
    )
    detail = await grouped_read.hydrate_detail(asynchronous, context, request, _SCOPE)
    assert detail.families == selected.families
    assert read_core.verify_published_generation.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id, expected_family", [(None, 20102), (17, 102), (18, 10102)])
async def test_entity_locator_uses_the_context_entity_and_family_tables(row_store, family_id, expected_family):
    session, contexts = row_store
    context = contexts[family_id]
    helper = replace(context, target=replace(context.target, profile_id="latest_period"), profile_slot=2)
    asynchronous = SimpleNamespace(execute=AsyncMock(side_effect=session.execute))
    locator = await read_core._entity_winner_locator(asynchronous, helper, read_core.EntityLocator("npi", _NPI))
    assert locator.family_revision_id == expected_family and locator.entity_binding_id == 501
    assert asynchronous.execute.await_count == 2


@pytest.mark.asyncio
async def test_snapshot_exact_count_offset_order_and_locator_use_same_relation(row_store):
    session, contexts = row_store
    document = fixture.definition_document()
    document["query"].pop("entity_selection")
    context = replace(contexts[17], definition=CustomImportDefinition.from_mapping(document))
    request = read_core.SearchRequest(
        context.target,
        filters=(read_core.ReadFilter("segment", "eq", "segment_a"),),
        order_terms=(read_core.ReadOrderTerm("score", "desc", "last"),),
        page_size=1,
    )
    plan = read_core._normalize_search_plan(request, context)
    statement = read_core._filtered_winner_statement(context, plan.filters)
    asynchronous = SimpleNamespace(
        execute=AsyncMock(side_effect=session.execute), scalar=AsyncMock(side_effect=session.scalar)
    )
    assert await read_core._exact_count(asynchronous, statement) == 2
    first = await read_core._page_winner_rows(asynchronous, statement, context, plan, 0)
    second = await read_core._page_winner_rows(asynchronous, statement, context, plan, 1)
    assert first[0][1].family_revision_id == 101 and second[0][1].family_revision_id == 102
    locator = read_core._winner_locator(second[0][0], second[0][1])
    assert await read_core._selected_winner_row(asynchronous, context, locator) == second[0]
    items = await read_core._hydrate_search_page_items(asynchronous, context, first + second)
    assert [next(field.value for field in item.root_fields if field.field_id == "score") for item in items] == [
        Decimal("100"),
        Decimal("2"),
    ]
