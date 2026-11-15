# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen candidate catalog and publication authority boundaries."""

from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import entity_address_snapshot_preparation as preparation
from tests.test_entity_address_snapshot_preparation import _bound, _session


def _inventory():
    names = sorted(model.__tablename__ for model in preparation.destination.restore._models())
    owner = preparation.destination.restore.EntityAddressArchiveStageOwnership(
        UUID("00000000-0000-4000-8000-000000000001"),
        "entity_address_archive_00000000000040008000000000000001",
        41,
        tuple((name, index + 100) for index, name in enumerate(names)),
    )
    sequence_by_field = {
        "sequence_name": preparation._EVIDENCE_SEQUENCE,
        "sequence_oid": 201,
        "owner_table": "entity_address_evidence",
        "owner_column": "evidence_id",
    }
    inventory_by_field = {
        "schema_name": owner.schema_name,
        "schema_oid": owner.schema_oid,
        "relations": [{"table_name": name, "relation_oid": oid} for name, oid in owner.relation_oids],
        "sequences": [sequence_by_field],
    }
    proof_by_field = {"state": "frozen", "frozen_owner_oid": 31, "builder_oid": 32, "inventory": inventory_by_field}
    proof_by_field["inventory_sha256"] = preparation._digest(inventory_by_field)
    return owner, proof_by_field


def _heap(oid):
    return {
        "oid": oid,
        "relowner": 31,
        "relkind": "r",
        "relpersistence": "p",
        "filenode": oid + 1000,
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "relispartition": False,
        "unsafe": False,
    }


def _sequence(owner):
    return {
        "oid": 201,
        "relname": preparation._EVIDENCE_SEQUENCE,
        "relowner": 31,
        "relnamespace": owner.schema_oid,
        "refobjid": dict(owner.relation_oids)["entity_address_evidence"],
        "attname": "evidence_id",
    }


@pytest.mark.parametrize("read_only", (False, True))
async def test_catalog_seal_binds_all_heaps_sequence_and_schema(monkeypatch, read_only):
    """Pin the complete declared family without deriving identity fingerprints."""
    owner, proof = _inventory()
    original = preparation._digest(proof)
    session = _session()
    lock, verify = AsyncMock(), AsyncMock(return_value=owner)
    monkeypatch.setattr(preparation.destination.restore, "_lock_owned_restore_relations", lock)
    monkeypatch.setattr(preparation.destination, "verify_entity_address_archive_stage_ownership", verify)
    observed = await preparation._protected_catalog(session, proof, owner, read_only=read_only)
    assert observed == preparation._logical_inventory(proof["inventory"], owner)
    assert not any("oid" in str(key) for key in observed)
    assert preparation._digest(proof) == original
    lock.assert_awaited_once_with(session, owner, read_only=read_only)
    verify.assert_awaited_once_with(session, owner=owner)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("relowner", 32),
        ("relkind", "p"),
        ("relpersistence", "u"),
        ("relrowsecurity", True),
        ("relforcerowsecurity", True),
        ("relispartition", True),
        ("unsafe", True),
        ("missing", None),
    ],
)
async def test_candidate_catalog_drift_refuses_sequence_read(monkeypatch, field, value):
    """Propagate native named-layout refusal before any publication or metadata mutation."""
    owner, proof = _inventory()
    session = _session()
    lock = AsyncMock()
    refusal = preparation.destination.EntityAddressArchiveOwnershipError("candidate layout differs")
    verify = AsyncMock(side_effect=refusal)
    monkeypatch.setattr(preparation.destination.restore, "_lock_owned_restore_relations", lock)
    monkeypatch.setattr(preparation.destination, "verify_entity_address_archive_stage_ownership", verify)
    with pytest.raises(type(refusal), match="layout differs"):
        await preparation._protected_catalog(session, proof, owner)
    lock.assert_awaited_once_with(session, owner, read_only=False)
    verify.assert_awaited_once_with(session, owner=owner)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("relname", "substituted"),
        ("relowner", 32),
        ("relnamespace", 42),
        ("refobjid", 999),
        ("attname", "other_id"),
        ("oid", 202),
        ("missing", None),
    ],
)
async def test_sequence_requires_exact_ownership_and_inventory(field, value):
    """Check the declared sequence purpose; old sequence tokens remain raw compatibility data."""
    owner, proof = _inventory()
    sequence_by_field = proof["inventory"]["sequences"][0]
    original = preparation._digest(proof)
    if field in {"relname", "attname", "missing"}:
        if field == "missing":
            proof["inventory"]["sequences"] = []
        else:
            sequence_by_field["sequence_name" if field == "relname" else "owner_column"] = value
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="sequence differs"):
            preparation._logical_inventory(proof["inventory"], owner)
    else:
        sequence_by_field[field] = value
        logical = preparation._logical_inventory(proof["inventory"], owner)
        assert logical["sequences"] == [
            {
                "sequence_name": preparation._EVIDENCE_SEQUENCE,
                "owner_table": "entity_address_evidence",
                "owner_column": "evidence_id",
            }
        ]
        assert sequence_by_field[field] == value
    assert original


async def test_schema_seal_preserves_settings_and_index_identities(monkeypatch):
    """Preserve legacy bytes while equal named inventory survives opaque token drift."""
    from copy import deepcopy

    owner, proof = _inventory()
    saved = deepcopy(proof)
    expected = preparation._logical_inventory(proof["inventory"], owner)
    proof["inventory"]["schema_oid"] = None
    for relation in proof["inventory"]["relations"]:
        relation["relation_oid"] = True
    proof["inventory"]["sequences"][0]["sequence_oid"] = "opaque"
    proof["inventory_sha256"] = preparation._digest(proof["inventory"])
    assert preparation._require_proof_inventory(proof, owner) == expected
    assert saved["inventory"]["schema_oid"] == owner.schema_oid
    assert proof["inventory"]["schema_oid"] is None


@pytest.mark.parametrize("damage", [None, "missing", "unsafe", "partition", "temporary", "view"])
async def test_publication_state_drains_writers_and_refuses_hooks(damage):
    """Drain writers and require the complete finite state family with native heap layout."""
    names = ("entity_address_geo_assurance_state", "entity_address_result_generation")
    records = [{"name": name, "supported": True} for name in names]
    if damage == "missing":
        records.pop()
    elif damage is not None:
        records[0]["supported"] = False
    session = _session()
    session.scalar.return_value = False
    session.execute.return_value.mappings.return_value.all.return_value = records
    if damage is None:
        await preparation._lock_publication_state(session, "example")
    else:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="state layout"):
            await preparation._lock_publication_state(session, "example")
    assert str(session.execute.await_args_list[0].args[0]).endswith("IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    assert not any(
        "relowner" in str(call.args[0]) or "pg_has_role" in str(call.args[0])
        for call in session.execute.await_args_list
    )


@pytest.mark.parametrize("builder", [None, {"name": '"builder"', "safe": False}, {"name": '"builder"', "safe": True}])
async def test_builder_handback_requires_safe_role_before_any_ddl(monkeypatch, builder):
    """Drop only the emptied owned namespace, without reassigning roles from old metadata."""
    owner, proof = _inventory()
    session = _session(builder)
    drop = AsyncMock()
    monkeypatch.setattr(preparation.destination.restore, "_drop_empty_owned_schema", drop)
    await preparation._restore_builder_owner(session, proof, owner, "example", {})
    drop.assert_awaited_once_with(session, owner)
    session.execute.assert_not_awaited()
