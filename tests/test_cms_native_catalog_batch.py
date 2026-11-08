# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh native catalog batching preserves exact inventories and physical fingerprints."""

import copy
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_layout as layout
from process.provider_directory_cms_preparation import OwnedRelation


def _physical_relation(relation, toast_oid=0):
    return dict(
        relation_oid=relation.oid,
        schema_name=relation.schema,
        relation_name=relation.relation,
        relkind="r",
        relpersistence=relation.persistence,
        table_am="heap",
        relreplident="d",
        relrowsecurity=False,
        relforcerowsecurity=False,
        relispartition=False,
        user_trigger_count=0,
        rule_count=0,
        inheritance_count=0,
        toast_oid=toast_oid,
        toast_relkind="t" if toast_oid else None,
        toast_relpersistence=relation.persistence if toast_oid else None,
        toast_am="heap" if toast_oid else None,
    )


def _catalog_backend():
    directory = importlib.import_module("process.provider_directory_fhir")
    relations = tuple(OwnedRelation("synthetic", f"heap_{oid}", oid, 0, "p") for oid in (11, 21, 31))
    relation_maps = [_physical_relation(relation, relation.oid + 1) for relation in relations]
    catalog_by_name = dict(attributes=[], indexes=[], constraints=[], triggers=[], database_oid=7)
    for relation_map in relation_maps:
        for relation_oid in (relation_map["relation_oid"], relation_map["toast_oid"]):
            catalog_by_name["attributes"].append(dict(relation_oid=relation_oid, attnum=1, attname="identity"))

    async def first(_statement, **parameters):
        if "relation_oid" in parameters:
            return next(item for item in relation_maps if item["relation_oid"] == parameters["relation_oid"])
        return {
            **catalog_by_name,
            **{
                name: [item for item in catalog_by_name[name] if item["relation_oid"] in parameters["relation_oids"]]
                for name in ("attributes", "indexes", "constraints", "triggers")
            },
        }

    database = SimpleNamespace(first=AsyncMock(side_effect=first), all=AsyncMock(return_value=relation_maps))

    async def relation_row(*arguments):
        return await directory._profile_capacity_relation_row(*arguments, database=database)

    backend = SimpleNamespace(
        db=database,
        _pagination_checkpoint_row_mapping=lambda catalog_record: catalog_record,
        _validate_profile_capacity_relation_row=directory._validate_profile_capacity_relation_row,
        _profile_capacity_relation_row=relation_row,
        _identity_hash=directory._identity_hash,
        _profile_capacity_tablespaces=lambda *_arguments: (42,),
    )
    return backend, relations, relation_maps, catalog_by_name


@pytest.mark.asyncio
async def test_batch_replaces_two_queries_per_heap_with_two_fresh_queries():
    backend, relations, _relation_maps, _catalog_by_name = _catalog_backend()
    individual_by_coordinate = {
        (relation.schema, relation.relation, relation.oid): await layout._observe_catalog(backend, relation)
        for relation in relations
    }
    assert backend.db.first.await_count == 2 * len(relations)
    backend.db.first.reset_mock()
    batched_by_coordinate = await layout.observe_native_catalog_batch(backend, relations)
    assert batched_by_coordinate == individual_by_coordinate
    backend.db.all.assert_awaited_once()
    backend.db.first.assert_awaited_once()
    assert backend.db.all.await_args.kwargs == {"relation_oids": [11, 21, 31]}
    assert backend.db.first.await_args.kwargs == {"relation_oids": [11, 12, 21, 22, 31, 32]}
    for relation in relations:
        coordinate = (relation.schema, relation.relation, relation.oid)
        assert layout._observed_layout(
            backend, relation, *batched_by_coordinate[coordinate]
        ) == layout._observed_layout(backend, relation, *individual_by_coordinate[coordinate])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,replacement,position",
    [
        ("relation_oid", 99, 0),
        ("schema_name", "different", 0),
        ("relation_name", "different", 0),
        ("relpersistence", "u", 0),
        ("toast_oid", 21, 0),
        ("toast_oid", 12, 1),
    ],
)
async def test_batch_rejects_changed_relation_inventory(field, replacement, position):
    backend, relations, relation_maps, _catalog_by_name = _catalog_backend()
    relation_maps[position][field] = replacement
    with pytest.raises(RuntimeError):
        await layout.observe_native_catalog_batch(backend, relations)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "duplicate"])
async def test_batch_rejects_incomplete_relation_inventory(change):
    backend, relations, relation_maps, _catalog_by_name = _catalog_backend()
    if change == "missing":
        relation_maps.pop()
    else:
        relation_maps.append(copy.deepcopy(relation_maps[0]))
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.observe_native_catalog_batch(backend, relations)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["duplicate_oid", "duplicate_coordinate", "boolean_oid"])
async def test_batch_rejects_ambiguous_requested_inventory_before_sql(change):
    backend, relations, _relation_maps, _catalog_by_name = _catalog_backend()
    if change == "duplicate_oid":
        relations = (*relations, relations[0])
    elif change == "duplicate_coordinate":
        relations = (*relations, replace(relations[0], oid=99))
    else:
        relations = (replace(relations[0], oid=True),)
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.observe_native_catalog_batch(backend, relations)
    backend.db.all.assert_not_awaited()
    backend.db.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["duplicate_attribute", "foreign_attribute", "foreign_index", "duplicate_index"])
async def test_batch_rejects_foreign_or_duplicate_catalog_members(change):
    backend, relations, _relation_maps, catalog_by_name = _catalog_backend()
    if change == "duplicate_attribute":
        catalog_by_name["attributes"].append(copy.deepcopy(catalog_by_name["attributes"][0]))
    elif change == "foreign_attribute":
        catalog_by_name["attributes"].append(dict(relation_oid=99, attnum=1))
    else:
        index_by_field = dict(
            relation_oid=99 if change == "foreign_index" else 11, index_oid=101, physical_present=True
        )
        catalog_by_name["indexes"] = [index_by_field] * (2 if change == "duplicate_index" else 1)
    backend.db.first.side_effect = None
    backend.db.first.return_value = catalog_by_name
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.observe_native_catalog_batch(backend, relations)


@pytest.mark.asyncio
@pytest.mark.parametrize("section", ["attributes", "indexes", "constraints", "triggers"])
async def test_batch_rejects_foreign_oid_in_every_catalog_section(section):
    backend, relations, _relation_maps, catalog_by_name = _catalog_backend()
    catalog_by_name[section].append(dict(relation_oid=99, physical_present=True))
    backend.db.first.side_effect = None
    backend.db.first.return_value = catalog_by_name
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.observe_native_catalog_batch(backend, relations)


@pytest.mark.asyncio
async def test_empty_native_batch_issues_no_queries():
    backend, _relations, _relation_maps, _catalog_by_name = _catalog_backend()
    assert await layout.observe_native_catalog_batch(backend, ()) == {}
    backend.db.all.assert_not_awaited()
    backend.db.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_uses_only_explicit_executor_and_recaptures_each_call():
    backend, relations, relation_maps, _catalog_by_name = _catalog_backend()
    executor = backend.db
    backend.db = SimpleNamespace(all=AsyncMock(side_effect=AssertionError), first=AsyncMock(side_effect=AssertionError))
    await layout.observe_native_catalog_batch(backend, relations, database=executor)
    relation_maps[0]["schema_name"] = "replaced"
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.observe_native_catalog_batch(backend, relations, database=executor)
    assert executor.all.await_count == executor.first.await_count == 2
    backend.db.all.assert_not_awaited()
    backend.db.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_passive_catalog_rejects_coordinate_change_without_queries():
    backend, relations, _relation_maps, _catalog_by_name = _catalog_backend()
    captured = await layout.observe_native_catalog_batch(backend, relations)
    relation = relations[0]
    observed = captured[(relation.schema, relation.relation, relation.oid)]
    backend.db.all.reset_mock()
    backend.db.first.reset_mock()
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout._observe_catalog(backend, replace(relation, oid=99), observed_catalog=observed)
    backend.db.all.assert_not_awaited()
    backend.db.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", [None, {}, ({},), ({}, None), ({}, None, None)])
async def test_passive_catalog_requires_exact_native_tuple_shape(observed):
    if observed is None:
        observed = (dict(contract="different"), None)
    backend, relations, _relation_maps, _catalog_by_name = _catalog_backend()
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout._observe_catalog(backend, relations[0], observed_catalog=observed)
    backend.db.all.assert_not_awaited()
    backend.db.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", ["entity_address_unified", "entity_address_evidence"])
async def test_retained_passive_catalog_keeps_fingerprint_and_sequence_check(monkeypatch, model_name):
    from tests.test_provider_directory_cms_retained_native_layout import _source_and_clone

    backend, source, relation, _original, _clone = await _source_and_clone(monkeypatch, model_name)
    observed = await layout._observe_catalog(backend, relation)
    expected = await layout.capture_retained_native_layout(backend, relation, source, (model_name,))
    backend.db.first.reset_mock()
    actual = await layout.capture_retained_native_layout(
        backend, relation, source, (model_name,), observed_catalog=observed
    )
    assert actual == expected
    assert backend.db.first.await_count == (1 if model_name == "entity_address_evidence" else 0)
    if model_name == "entity_address_evidence":
        assert "AS evidence_sequences" in backend.db.first.await_args.args[0]


@pytest.mark.asyncio
async def test_scratch_batch_groups_proof_members_and_preserves_fences(monkeypatch):
    from process import provider_directory_artifact_scratch_inputs as scratch

    backend, relations, _relation_maps, _catalog_by_name = _catalog_backend()
    fallback = OwnedRelation("synthetic", "fallback", 99, 0, "p")
    executor, events = object(), []
    proof_by_coordinate = {
        (relation.schema, relation.relation): (object(), executor if relation.oid != 31 else None)
        for relation in relations
    }

    @asynccontextmanager
    async def proof_scope(*_arguments, **_keywords):
        events.append("before")
        yield proof_by_coordinate
        events.append("after")

    async def census(_backend, members, *, database):
        events.append(("census", database))
        return {(relation.schema, relation.relation, relation.oid): relation.oid for relation in members}

    async def capture(_backend, relation, proof, *, database, observed_catalog):
        assert (proof, database) == proof_by_coordinate[(relation.schema, relation.relation)]
        assert observed_catalog == relation.oid
        events.append(relation.oid)
        return layout.NativeRelationLayout(relation.oid, (42,), "a" * 64)

    observe = AsyncMock(side_effect=census)
    monkeypatch.setattr(scratch, "scratch_relation_batch", proof_scope)
    monkeypatch.setattr(layout, "observe_native_catalog_batch", observe)
    monkeypatch.setattr(layout, "_capture_scratch_proof_layout", capture)
    async with layout.scratch_layout_batch(backend, (*relations, fallback)) as captured:
        assert len(captured) == 3
        events.append("ordinary")
    assert events == ["before", ("census", executor), ("census", None), 11, 21, 31, "ordinary", "after"]
    assert observe.await_args_list[0].args[1] == list(relations[:2])
    assert observe.await_args_list[1].args[1] == [relations[2]]
    assert captured == {}
