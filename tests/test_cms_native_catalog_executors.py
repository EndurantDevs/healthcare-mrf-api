# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep fresh physical catalogs separated by their genuine executing backend."""

from types import SimpleNamespace

import pytest

from process import provider_directory_cms_native_layout as layout
from process.provider_directory_cms_preparation import OwnedRelation
from tests.test_provider_directory_cms_nonprofile_capacity import _producer


@pytest.mark.asyncio
async def test_native_catalog_census_groups_only_the_same_executor_and_refreshes(monkeypatch):
    producer = _producer()
    reader, controls = object(), object()
    members = tuple(OwnedRelation("synthetic", name, oid, 100, "p") for oid, name in enumerate("abcde", 1))
    native_by_coordinate = {(member.schema, member.relation): object() for member in members[:4]}
    executor_by_name = {"a": reader, "b": reader, "c": None, "d": controls}
    observers = SimpleNamespace(database_for=lambda member: executor_by_name[member.relation])
    calls = []

    async def capture(directory, group, *, database):
        assert directory is producer.fhir
        calls.append((database, tuple(member.relation for member in group)))
        return {(member.schema, member.relation, member.oid): (len(calls), None) for member in group}

    monkeypatch.setattr(layout, "observe_native_catalog_batch", capture)
    first = await producer._native_catalog_observations(members, native_by_coordinate, observers=observers)
    second = await producer._native_catalog_observations(members, native_by_coordinate, observers=observers)
    assert calls == [(reader, ("a", "b")), (producer.fhir.db, ("c",)), (controls, ("d",))] * 2
    assert first.keys() == second.keys() == {("synthetic", member.relation, member.oid) for member in members[:4]}
    assert first != second


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "foreign", "replaced"])
async def test_native_catalog_census_rejects_changed_complete_inventory(monkeypatch, damage):
    producer = _producer()
    member = OwnedRelation("synthetic", "retained", 11, 100, "p")

    async def capture(_directory, _members, *, database):
        assert database is producer.fhir.db
        if damage == "missing":
            return {}
        coordinate = (
            member.schema,
            "foreign" if damage == "foreign" else member.relation,
            12 if damage == "replaced" else 11,
        )
        return {coordinate: ({}, None)}

    monkeypatch.setattr(layout, "observe_native_catalog_batch", capture)
    with pytest.raises(RuntimeError, match="native_identity_changed"):
        await producer._native_catalog_observations((member,), {(member.schema, member.relation): object()})
