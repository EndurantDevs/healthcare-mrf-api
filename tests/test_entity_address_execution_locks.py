# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh set-based lock metadata retains complete executing-backend identity checks."""

import asyncio
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_geo_projection as projection
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
        owned_relations={f"stage_{index:03}" for index in range(count)},
        admission=SimpleNamespace(inventory_transition=nullcontext),
    )
    relation_names = [f"synthetic.{name}" for name in sorted(scope.owned_relations)]
    database = _database([])
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
            assert database.status.await_count == attempt
            database.all.assert_not_awaited()
        assert database.status.await_args.args == (
            "LOCK TABLE " + ", ".join(relation_names) + " IN ACCESS SHARE MODE NOWAIT",
        )
        assert ownership.call_count == 2 * count and building.call_count == prepared_lock.await_count == 2
        database.scalar.assert_not_awaited()
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        PermissionError("native SELECT denied"),
        TimeoutError("native lock wait"),
        asyncio.CancelledError(),
        RuntimeError("native relation missing"),
        RuntimeError("native DDL collision"),
        RuntimeError("native read failure"),
    ],
)
async def test_named_inventory_propagates_native_lock_refusals(monkeypatch, failure):
    scope = SimpleNamespace(
        db_schema="synthetic",
        owned_relations={"a", "b", "c"},
        admission=SimpleNamespace(inventory_transition=nullcontext),
    )
    database = _database([])
    database.status.side_effect = failure
    monkeypatch.setattr(
        admitted,
        "_native",
        lambda: SimpleNamespace(candidate_preparation=SimpleNamespace(lock_prepared_doctors=AsyncMock())),
    )
    monkeypatch.setattr(admitted.source_preparation, "require_building", Mock())
    monkeypatch.setattr(admitted.source_preparation, "require_owned_relation", Mock())
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(type(failure)) as raised:
            await admitted.lock_owned_relations(database)
        assert raised.value is failure
        database.status.assert_awaited_once()
        database.all.assert_not_awaited()
        database.scalar.assert_not_awaited()
    finally:
        admitted._ADMISSION.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_current_shape", (True, False))
async def test_doctors_lock_holds_named_family_and_geo_inputs(is_current_shape):
    """The fixed Doctors family and the declared geo inputs are locked by name before one shape check."""
    stages = (
        ("doctor_clinician_address", "address_stage"),
        ("groups", "group_stage"),
        ("organizations", "org_stage"),
    )
    archive_lock = AsyncMock()
    binding_by_name = {
        f"{schema or 'synthetic'}.{table}": {"schema_name": schema or "synthetic", "table_name": table}
        for schema, table in projection._PROJECTION_DEPENDENCIES
    }
    dependencies = SimpleNamespace(
        archive=SimpleNamespace(lock_read_backend=archive_lock),
        doctors=object(),
        schema="synthetic",
        stage_relations=stages,
        dependency_bindings=binding_by_name,
    )
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=is_current_shape))
    if is_current_shape:
        await doctors.lock_prepared_relations(database, dependencies)
    else:
        with pytest.raises(RuntimeError, match="geo dependency binding changed"):
            await doctors.lock_prepared_relations(database, dependencies)
    archive_lock.assert_awaited_once_with(database)
    assert [call.args for call in database.status.await_args_list] == [
        (
            'LOCK TABLE ONLY "synthetic"."address_stage", ONLY "synthetic"."group_stage", '
            'ONLY "synthetic"."org_stage" IN ACCESS SHARE MODE NOWAIT',
        ),
        (projection.projection_named_dependency_lock_sql("synthetic", binding_by_name),),
    ]
    database.scalar.assert_awaited_once_with(
        "SELECT " + projection.projection_named_dependency_match_sql("synthetic", binding_by_name)
    )
