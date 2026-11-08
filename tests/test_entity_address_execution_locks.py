# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh set-based lock metadata retains complete executing-backend identity checks."""

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_preparation_admission as admitted
from process import entity_address_prepared_doctors as doctors


def _database(rows):
    return SimpleNamespace(
        status=AsyncMock(),
        all=AsyncMock(return_value=rows),
        scalar=AsyncMock(side_effect=AssertionError("metadata must use one complete set")),
    )


def _damage(rows, damage):
    if damage == "missing-row":
        return rows[:-1]
    if damage == "extra-row":
        return [*rows, rows[0]]
    if damage == "duplicate":
        return [rows[0], rows[0], *rows[2:]]
    if damage == "reordered":
        return list(reversed(rows))
    if damage in {"missing-oid", "replacement"}:
        return [(*rows[0][:1], None if damage == "missing-oid" else 999, *rows[0][2:]), *rows[1:]]
    if damage == "filenode":
        return [(*rows[0][:2], 999), *rows[1:]]
    return rows


@pytest.mark.asyncio
@pytest.mark.parametrize("count", (1, 4, 100))
async def test_owned_lock_uses_one_fresh_census_after_each_backend_lock(monkeypatch, count):
    """Both small and multirow owned inventories issue one metadata query on every call."""
    scope = SimpleNamespace(
        db_schema="synthetic",
        owned_oids={f"stage_{index:03}": index + 41 for index in range(count)},
        admission=SimpleNamespace(inventory_transition=nullcontext),
    )
    relation_rows = [(f"synthetic.{name}", oid) for name, oid in sorted(scope.owned_oids.items())]
    database = _database(relation_rows)
    prepared_lock, building, ownership = AsyncMock(), Mock(), Mock()
    monkeypatch.setattr(
        admitted,
        "_native",
        lambda: SimpleNamespace(candidate_preparation=SimpleNamespace(lock_prepared_doctors=prepared_lock)),
    )
    monkeypatch.setattr(admitted.source_preparation, "require_building", building)
    monkeypatch.setattr(admitted.source_preparation, "require_owned_relation", ownership)
    token = admitted._ADMISSION.set(scope)
    try:
        for attempt in (1, 2):
            await admitted.lock_owned_relations(database)
            assert database.status.await_count == database.all.await_count == attempt
        assert database.status.await_args.args == (
            "LOCK TABLE " + ", ".join(name for name, _oid in relation_rows) + " IN ACCESS SHARE MODE NOWAIT",
        )
        assert database.all.await_args.kwargs == {"relations": [name for name, _oid in relation_rows]}
        assert ownership.call_count == 2 * count and building.call_count == prepared_lock.await_count == 2
        database.scalar.assert_not_awaited()
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("missing-row", "extra-row", "duplicate", "reordered", "missing-oid", "replacement"))
async def test_owned_lock_rejects_incomplete_or_changed_multirow_census(monkeypatch, damage):
    """No absent, duplicate or replaced heap can disappear while building the observed map."""
    scope = SimpleNamespace(
        db_schema="synthetic",
        owned_oids={"a": 41, "b": 42, "c": 43},
        admission=SimpleNamespace(inventory_transition=nullcontext),
    )
    database = _database(_damage([(f"synthetic.{name}", oid) for name, oid in scope.owned_oids.items()], damage))
    monkeypatch.setattr(
        admitted,
        "_native",
        lambda: SimpleNamespace(candidate_preparation=SimpleNamespace(lock_prepared_doctors=AsyncMock())),
    )
    monkeypatch.setattr(admitted.source_preparation, "require_building", Mock())
    monkeypatch.setattr(admitted.source_preparation, "require_owned_relation", Mock())
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="owned stage changed"):
            await admitted.lock_owned_relations(database)
        database.status.assert_awaited_once()
        database.all.assert_awaited_once()
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", (None, "missing-row", "extra-row", "duplicate", "reordered", "missing-oid", "replacement", "filenode")
)
async def test_doctors_lock_batches_complete_heap_identity_and_geo_filenode(damage):
    """The fixed Doctors family and independent address filenode share one fresh query."""
    stages = (
        ("doctor_clinician_address", "address_stage", 41),
        ("groups", "group_stage", 42),
        ("organizations", "org_stage", 43),
    )
    archive_lock = AsyncMock()
    dependencies = SimpleNamespace(
        archive=SimpleNamespace(lock_read_backend=archive_lock),
        doctors=object(),
        schema="synthetic",
        stage_oids=stages,
        dependency_bindings={"synthetic.doctor_clinician_address": {"relation_oid": 41, "relfilenode": 51}},
    )
    catalog_rows = [(f'"synthetic"."{stage}"', oid, 51) for _target, stage, oid in stages]
    database = _database(_damage(catalog_rows, damage))
    if damage:
        with pytest.raises(
            RuntimeError, match="geo dependency binding changed" if damage == "filenode" else "Doctors identity changed"
        ):
            await doctors.lock_prepared_relations(database, dependencies)
    else:
        await doctors.lock_prepared_relations(database, dependencies)
        await doctors.lock_prepared_relations(database, dependencies)
        assert database.all.await_count == database.status.await_count == archive_lock.await_count == 2
    sql = database.all.await_args.args[0]
    assert "c.relkind='r' AND c.relpersistence='p'" in sql and "WHERE oid=:binding_oid" in sql
    assert database.all.await_args.kwargs == {
        "relations": [catalog_row[0] for catalog_row in catalog_rows],
        "binding_oid": 41,
    }
    assert database.status.await_args.args == (
        'LOCK TABLE ONLY "synthetic"."address_stage", ONLY "synthetic"."group_stage", ONLY "synthetic"."org_stage" IN ACCESS SHARE MODE NOWAIT',
    )
    database.scalar.assert_not_awaited()
