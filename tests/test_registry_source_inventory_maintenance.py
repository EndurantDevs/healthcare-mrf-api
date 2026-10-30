# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh cleanup snapshots keep full independent custody before and after the fold."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_candidate_custody as custody
from process import registry_source_inventory_maintenance as maintenance
from tests.test_registry_source_inventory_publication_guards import _registered


@pytest.mark.parametrize("count", [0, 1])
async def test_maintenance_folds_sealed_oid_then_reverifies_native_snapshot(monkeypatch, count):
    candidate_by_field = _registered(count)
    final_candidate_by_field = {**candidate_by_field, "state": "published"}
    control, seal = {"schema_oid": 7}, {"seal_sha256": "a" * 64}
    verified_by_field = {"receipt": {"closure": {"relation_oids": {"network_source_inventory": 42}}}}
    snapshots = AsyncMock(
        side_effect=[
            (control, candidate_by_field, seal, verified_by_field),
            (control, final_candidate_by_field, seal, verified_by_field),
        ]
    )
    monkeypatch.setattr(maintenance, "_sealed_snapshot", snapshots)
    physical = AsyncMock(return_value='"fixture".network_source_inventory')
    monkeypatch.setattr(maintenance, "_physical_context", physical)
    entries = tuple(object() for _ in range(count))

    async def fold(connection, relation, root, row_limit, byte_limit, append):
        assert snapshots.await_count == 1 and relation == '"fixture".network_source_inventory'
        assert root.entry_count == count and row_limit == 100 and byte_limit == maintenance.MAX_INVENTORY_PAGE_BYTES
        for entry in entries:
            append(entry)

    monkeypatch.setattr(maintenance, "_fold_inventory", fold)
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="read committed"))
    copy_target = object()
    actual = await maintenance.read_sealed_source_inventory_for_maintenance(
        connection, copy_target, control_schema="control"
    )
    assert actual == (final_candidate_by_field, verified_by_field, entries) and snapshots.await_count == 2
    physical.assert_awaited_once_with(connection, copy_target, 42, "control")


@pytest.mark.parametrize(
    "isolation,active", [("repeatable read", True), ("serializable", True), ("read committed", False)]
)
async def test_maintenance_requires_fresh_active_rc_before_any_custody(monkeypatch, isolation, active):
    snapshots = AsyncMock()
    monkeypatch.setattr(maintenance, "_sealed_snapshot", snapshots)
    connection = SimpleNamespace(is_in_transaction=lambda: active, fetchval=AsyncMock(return_value=isolation))
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await maintenance.read_sealed_source_inventory_for_maintenance(connection, object(), control_schema="control")
    snapshots.assert_not_awaited()


@pytest.mark.parametrize("changed", ["control", "seal", "verified_by_field", "reference"])
async def test_maintenance_rejects_any_post_fold_authority_change(monkeypatch, changed):
    candidate_by_field = _registered(0)
    control, seal = {"schema_oid": 7}, {"seal_sha256": "a" * 64}
    verified_by_field = {"receipt": {"closure": {"relation_oids": {"network_source_inventory": 42}}}}
    final_snapshot_parts = list(copy.deepcopy((control, candidate_by_field, seal, verified_by_field)))
    if changed == "control":
        final_snapshot_parts[0]["schema_oid"] += 1
    elif changed == "seal":
        final_snapshot_parts[2]["seal_sha256"] = "b" * 64
    elif changed == "verified_by_field":
        final_snapshot_parts[3]["evidence"] = "changed"
    else:
        final_snapshot_parts[1] = _registered(1)
    monkeypatch.setattr(
        maintenance,
        "_sealed_snapshot",
        AsyncMock(side_effect=[(control, candidate_by_field, seal, verified_by_field), tuple(final_snapshot_parts)]),
    )
    monkeypatch.setattr(maintenance, "_physical_context", AsyncMock(return_value="inventory"))
    monkeypatch.setattr(maintenance, "_fold_inventory", AsyncMock())
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="read committed"))
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await maintenance.read_sealed_source_inventory_for_maintenance(connection, object(), control_schema="control")


async def test_maintenance_snapshot_uses_full_real_custody_verifier(monkeypatch):
    connection, target = object(), object()
    control, candidate_by_field, seal, verified_by_field = {}, {}, {}, {}
    monkeypatch.setattr(custody, "_control", AsyncMock(return_value=control))
    monkeypatch.setattr(custody, "_candidate", AsyncMock(return_value=candidate_by_field))
    monkeypatch.setattr(custody, "_seal", AsyncMock(return_value=seal))
    verify = AsyncMock(return_value=verified_by_field)
    monkeypatch.setattr(custody, "_verify", verify)
    assert await maintenance._sealed_snapshot(connection, target, "control") == (
        control,
        candidate_by_field,
        seal,
        verified_by_field,
    )
    verify.assert_awaited_once_with(connection, target, "control", control, candidate_by_field, seal)


@pytest.mark.parametrize("stage", ["first_custody", "fold", "second_custody"])
async def test_maintenance_does_not_swallow_custody_or_fold_failure(monkeypatch, stage):
    candidate_by_field = _registered(0)
    verified_by_field = {"receipt": {"closure": {"relation_oids": {"network_source_inventory": 42}}}}
    snapshot = ({}, candidate_by_field, {}, verified_by_field)
    refusal = ValueError("closed_inventory_changed")
    snapshots = AsyncMock(side_effect=[refusal] if stage == "first_custody" else [snapshot, refusal])
    monkeypatch.setattr(maintenance, "_sealed_snapshot", snapshots)
    monkeypatch.setattr(maintenance, "_physical_context", AsyncMock(return_value="inventory"))
    monkeypatch.setattr(maintenance, "_fold_inventory", AsyncMock(side_effect=refusal if stage == "fold" else None))
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="read committed"))
    with pytest.raises(ValueError) as error:
        await maintenance.read_sealed_source_inventory_for_maintenance(connection, object(), control_schema="control")
    assert error.value is refusal
