# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical address names and locked publication preserve existing portable contracts."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_adoption as adoption
from process import network_address_artifact as artifact
from process.entity_address_retained_names import retained_address_name

ORIGIN = {
    "origin_lineage_id": "550e8400-e29b-41d4-a716-446655440000",
    "origin_generation": 7,
    "published_at": "2026-01-01T00:00:00Z",
}


def _authority():
    return {"local_lineage_id": "550e8400-e29b-41d4-a716-446655440001", "local_generation": 9, **ORIGIN}


def _descriptor():
    authority = _authority()
    document_by_field = {
        "contract": artifact.CONTRACT,
        "revision": 2,
        "namespace": {"name": "example"},
        "table": {"schema_name": "example", "table_name": artifact._TABLE},
        "retained_table": {"schema_name": "example", "table_name": retained_address_name(artifact._TABLE, ORIGIN)},
        "authority": authority,
        "generation_id": artifact._digest(authority),
        "alias_generation": 2,
    }
    document_by_field["descriptor_sha256"] = artifact._digest(document_by_field)
    return document_by_field


def test_copied_generation_uses_origin_and_only_one_retained_name_helper():
    authority = _authority()
    copied_by_field = {**authority, "local_lineage_id": "550e8400-e29b-41d4-a716-446655440002", "local_generation": 400}
    assert (
        artifact._logical_retained_name(artifact._TABLE, copied_by_field)
        == artifact._logical_retained_name(artifact._TABLE, authority)
        == retained_address_name(artifact._TABLE, ORIGIN)
    )
    assert retained_address_name("entity_address_evidence", ORIGIN) != retained_address_name(artifact._TABLE, ORIGIN)
    assert retained_address_name(artifact._TABLE, {**ORIGIN, "origin_generation": 8}) != retained_address_name(
        artifact._TABLE, ORIGIN
    )
    assert not hasattr(artifact.generation, "retained_entity_address_relation_name")


@pytest.mark.parametrize("table", [None, "", "other.table", 'x";DROP', "a" * 64])
def test_invalid_retained_names_never_reach_sql(table):
    with pytest.raises(ValueError):
        retained_address_name(table, ORIGIN)


@pytest.mark.parametrize("origin", [None, {}, {**ORIGIN, "origin_generation": 0}, {**ORIGIN, "oid": 42}])
def test_complete_origin_required_for_new_name(origin):
    with pytest.raises(ValueError):
        retained_address_name(artifact._TABLE, origin)


@pytest.mark.asyncio
@pytest.mark.parametrize("matches", [True, False])
async def test_capture_locks_names_before_reading_the_origin(monkeypatch, matches):
    from process import entity_address_snapshot_alias as alias

    connection = SimpleNamespace(
        is_in_transaction=lambda: True, execute=AsyncMock(), fetchval=AsyncMock(return_value="repeatable read")
    )

    async def current(_connection, schema):
        assert schema == "example"
        assert connection.execute.await_count == 2
        return _authority(), _authority()

    monkeypatch.setattr(artifact, "_current_authority_documents", current)
    monkeypatch.setattr(artifact, "has_matching_live_address_generation", AsyncMock(return_value=matches))
    monkeypatch.setattr(alias, "_alias_state", AsyncMock(return_value=(None, None, 2)))
    source = artifact.PinnedAddressSource("example", artifact._TABLE, artifact._digest(_authority()))
    if matches:
        assert await artifact.capture_network_address_artifact(connection, source) == _descriptor()
    else:
        with pytest.raises(artifact.NetworkAddressArtifactError, match="source_generation_changed"):
            await artifact.capture_network_address_artifact(connection, source)
    assert all("IN ACCESS SHARE MODE NOWAIT" in call.args[0] for call in connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_recorded_historical_retained_name_is_preserved(monkeypatch):
    value = _descriptor()
    value["retained_table"]["table_name"] = artifact._TABLE + "_retained_recorded"
    value["descriptor_sha256"] = artifact._digest({k: v for k, v in value.items() if k != "descriptor_sha256"})
    before = deepcopy(value)
    monkeypatch.setattr(artifact, "has_matching_live_address_generation", AsyncMock(return_value=False))
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    relation = await artifact.pinned_address_heap(
        connection, value, presence={"authority": True, "live": True, "retained": True}
    )
    assert relation.table_name == value["retained_table"]["table_name"]
    assert value == before


@pytest.mark.asyncio
async def test_two_retained_aliases_refuse_ambiguity(monkeypatch):
    value = _descriptor()
    value["retained_table"]["table_name"] = artifact._TABLE + "_retained_recorded"
    value["descriptor_sha256"] = artifact._digest({k: v for k, v in value.items() if k != "descriptor_sha256"})
    monkeypatch.setattr(artifact, "has_matching_live_address_generation", AsyncMock(return_value=False))
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    with pytest.raises(artifact.NetworkAddressArtifactError, match="retained_table_unavailable"):
        await artifact.pinned_address_heap(
            connection, value, presence={"authority": True, "live": True, "retained": True}
        )


@pytest.mark.asyncio
async def test_rotation_before_first_lock_relocks_the_retained_name(monkeypatch):
    value = _descriptor()
    before = deepcopy(value)
    live = artifact.CompactAddressRelation(**value["table"])
    retained = artifact.CompactAddressRelation(**value["retained_table"])
    resolve = AsyncMock(side_effect=[live, retained, retained])
    monkeypatch.setattr(artifact, "pinned_address_heap", resolve)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        execute=AsyncMock(),
        fetch=AsyncMock(),
    )
    assert await artifact.verify_network_address_artifact(connection, value) == retained
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert statements == [
        f'LOCK TABLE ONLY "example"."{live.table_name}" IN ACCESS SHARE MODE NOWAIT',
        f'LOCK TABLE ONLY "example"."{retained.table_name}" IN ACCESS SHARE MODE NOWAIT',
    ]
    assert value == before


@pytest.mark.asyncio
@pytest.mark.parametrize("controlled", [False, True])
@pytest.mark.parametrize("count", [1, 7])
async def test_one_incumbent_origin_and_peer_inventory_for_the_actual_swap_scope(monkeypatch, controlled, count):
    session = object()
    monkeypatch.setattr(
        adoption.entity_address_unified.db, "_transaction_binding", lambda: SimpleNamespace(session=session)
    )
    capture = AsyncMock(return_value=ORIGIN)
    rotate = AsyncMock(return_value={"schema_name": "example", "relation_name": "recorded_retained"})
    monkeypatch.setattr(adoption, "_retained_serving_generation", capture)
    monkeypatch.setattr(adoption, "_swap_sealed_stage_table", rotate)
    swaps = [SimpleNamespace(live_cls=object(), stage_cls=object()) for _ in range(count)]
    context = {"stage_relations": []} if controlled else {}
    await adoption._publish_result_swaps("example", swaps, context)
    capture.assert_awaited_once_with(session, "example")
    assert rotate.await_count == count
    assert all(call.kwargs["serving_generation"] is ORIGIN for call in rotate.await_args_list)
    assert context["retained_relations"] == [rotate.return_value] * count


@pytest.mark.asyncio
async def test_marked_recovery_keeps_its_distinct_existing_rename(monkeypatch):
    from process.provider_directory_serving_recovery import GENERATION_PARAM

    retain, legacy = AsyncMock(), AsyncMock()
    monkeypatch.setattr(adoption, "_swap_sealed_stage_table", retain)
    monkeypatch.setattr(adoption.entity_address_unified, "_swap_stage_table", legacy)
    swap = SimpleNamespace(live_cls=object(), stage_cls=object())
    await adoption._publish_result_swaps("example", [swap], {GENERATION_PARAM: "recovery"}, serving_generation=ORIGIN)
    legacy.assert_awaited_once_with("example", swap.live_cls, swap.stage_cls)
    retain.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_swap_does_not_leak_context_into_a_retry(monkeypatch):
    entry_by_field = {"schema_name": "example", "relation_name": "recorded_retained"}
    rotate = AsyncMock(side_effect=[entry_by_field, RuntimeError("synthetic late rollback")])
    monkeypatch.setattr(adoption, "_swap_sealed_stage_table", rotate)
    swap = SimpleNamespace(live_cls=object(), stage_cls=object())
    context_by_field = {"retained_relations": [entry_by_field]}
    with pytest.raises(RuntimeError, match="late rollback"):
        await adoption._publish_result_swaps("example", [swap, swap], context_by_field, serving_generation=ORIGIN)
    assert context_by_field["retained_relations"] == []


@pytest.mark.asyncio
async def test_main_only_ordinary_preparation_never_calls_full_family_preparation(monkeypatch):
    monkeypatch.setattr(
        adoption.entity_address_unified.db, "_transaction_binding", lambda: SimpleNamespace(session=object())
    )
    capture = AsyncMock(return_value=ORIGIN)
    full = AsyncMock(side_effect=AssertionError("ordinary preparation must use its actual validated swap scope"))
    monkeypatch.setattr(adoption, "_retained_serving_generation", capture)
    monkeypatch.setattr(adoption, "prepare_entity_address_publisher_reimport", full)
    swap = SimpleNamespace(live_cls=SimpleNamespace(__main_table__=artifact._TABLE), stage_cls=object())
    assert await adoption._prepare_protected_ordinary_rotation("example", [swap], False, {}) == ORIGIN
    with pytest.raises(RuntimeError, match="full isolated replacement"):
        await adoption._prepare_protected_ordinary_rotation("example", [swap], True, {})
    full.assert_not_awaited()
