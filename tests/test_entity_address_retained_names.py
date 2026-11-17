# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained address names depend on a portable generation rather than local catalogs."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_adoption as adoption
from process import entity_address_snapshot_ownership as ownership
from process.entity_address_retained_names import retained_address_name

GENERATION = {
    "origin_lineage_id": "550e8400-e29b-41d4-a716-446655440000",
    "origin_generation": 7,
    "published_at": "2026-01-01T00:00:00Z",
}


def test_retained_names_preserve_logical_generation_and_table_identity():
    name = retained_address_name("entity_address_unified", GENERATION)
    assert name == retained_address_name(
        "entity_address_unified", {**GENERATION, "published_at": datetime(2026, 1, 1, tzinfo=UTC)}
    )
    assert name != retained_address_name("entity_address_unified", {**GENERATION, "origin_generation": 8})
    assert name != retained_address_name("entity_address_evidence", GENERATION)
    assert len(retained_address_name("a" * 63, GENERATION)) == 63
    assert retained_address_name("a" * 62 + "b", GENERATION) != retained_address_name("a" * 63, GENERATION)


@pytest.mark.parametrize("table", [None, "", "other.table", 'x";DROP', "a" * 64])
def test_invalid_retained_name_never_becomes_sql(table):
    with pytest.raises(ValueError):
        retained_address_name(table, GENERATION)


@pytest.mark.parametrize("value", [None, {}, {**GENERATION, "origin_generation": 0}, {**GENERATION, "oid": 42}])
def test_retained_name_requires_the_complete_logical_generation(value):
    with pytest.raises(ValueError):
        retained_address_name("entity_address_unified", value)


@pytest.mark.parametrize(
    "fault", [None, "generationless", "schema", "contract", "missing", "duplicate", "live", "foreign", "fields"]
)
def test_legacy_cleanup_uses_only_a_complete_named_rotation(fault):
    relations = [
        {"table_name": model.__tablename__, "relation_oid": index} for index, model in enumerate(ownership._models(), 1)
    ]
    retained_entries = [
        {**entry, "retained_name": retained_address_name(entry["table_name"], GENERATION)} for entry in relations
    ]
    rotation_by_field = {
        "contract": "entity-address-table-publication.v2",
        "schema_name": "example",
        "retained_relations": retained_entries,
    }
    if fault == "generationless":
        for entry in retained_entries:
            entry["retained_name"] = entry["table_name"] + "_retained_f1"
    if fault in (None, "generationless"):
        retained_entries[0]["relation_oid"] = 99999
        assert ownership._publication_retained_names("example", relations, {}, rotation=rotation_by_field) == [
            ("example", entry["retained_name"]) for entry in retained_entries
        ]
        return
    if fault == "schema":
        rotation_by_field["schema_name"] = "foreign"
    if fault == "contract":
        rotation_by_field["contract"] = "unsupported"
    if fault == "missing":
        retained_entries.pop()
    if fault == "duplicate":
        retained_entries[-1] = retained_entries[0]
    if fault == "live":
        retained_entries[0]["retained_name"] = retained_entries[0]["table_name"]
    if fault == "foreign":
        retained_entries[0]["retained_name"] = retained_address_name("unrelated", GENERATION)
    if fault == "fields":
        retained_entries[0]["extra"] = True
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="rotation is invalid"):
        ownership._publication_retained_names("example", relations, {}, rotation=rotation_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("controlled", [False, True])
@pytest.mark.parametrize("count", [1, 7])
async def test_actual_rotation_scope_uses_one_incumbent_generation(monkeypatch, controlled, count):
    session = object()
    monkeypatch.setattr(
        adoption.entity_address_unified.db, "_transaction_binding", lambda: SimpleNamespace(session=session)
    )
    capture = AsyncMock(return_value=GENERATION)
    rotate = AsyncMock(return_value=None)
    monkeypatch.setattr(adoption, "_retained_serving_generation", capture)
    monkeypatch.setattr(adoption, "_swap_sealed_stage_table", rotate)
    swaps = [SimpleNamespace(live_cls=object(), stage_cls=object()) for _ in range(count)]
    context = {"snapshot_contract": "entity_address_unified.postgres.v2"} if controlled else {}
    await adoption._publish_result_swaps("example", swaps, context)
    capture.assert_awaited_once_with(session, "example")
    assert rotate.await_count == count
    assert all(call.kwargs["serving_generation"] is GENERATION for call in rotate.await_args_list)


@pytest.mark.asyncio
async def test_marked_recovery_does_not_retain_the_incumbent_twice(monkeypatch):
    from process.provider_directory_serving_recovery import GENERATION_PARAM

    sealed, legacy = AsyncMock(), AsyncMock()
    capture = AsyncMock(side_effect=AssertionError("incumbent was already captured under the cutover locks"))
    monkeypatch.setattr(adoption, "_retained_serving_generation", capture)
    monkeypatch.setattr(adoption, "_swap_sealed_stage_table", sealed)
    monkeypatch.setattr(adoption.entity_address_unified, "_swap_stage_table", legacy)
    swap = SimpleNamespace(live_cls=object(), stage_cls=object())
    await adoption._publish_result_swaps(
        "example", [swap], {GENERATION_PARAM: "existing recovery"}, serving_generation=GENERATION
    )
    legacy.assert_awaited_once_with("example", swap.live_cls, swap.stage_cls)
    sealed.assert_not_awaited()
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_rotation_does_not_leak_retention_output_into_retry(monkeypatch):
    first, second = {"retained_name": "first"}, {"retained_name": "second"}
    rotate = AsyncMock(side_effect=[first, RuntimeError("synthetic lock failure"), first, second])
    monkeypatch.setattr(adoption, "_swap_sealed_stage_table", rotate)
    swaps = [SimpleNamespace(live_cls=object(), stage_cls=object()) for _ in range(2)]
    context_by_field = {"retained_relations": [{"retained_name": "stale"}]}
    with pytest.raises(RuntimeError, match="synthetic lock failure"):
        await adoption._publish_result_swaps("example", swaps, context_by_field, serving_generation=GENERATION)
    assert context_by_field["retained_relations"] == []
    await adoption._publish_result_swaps("example", swaps, context_by_field, serving_generation=GENERATION)
    assert context_by_field["retained_relations"] == [first, second]
