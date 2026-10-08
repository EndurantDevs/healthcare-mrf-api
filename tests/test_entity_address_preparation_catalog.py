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
    owner, proof = _inventory()
    rows = [_heap(oid) for _name, oid in owner.relation_oids]
    session = _session()
    session.scalar.side_effect = [True, False, False]
    session.execute.return_value.mappings.return_value.all.side_effect = [rows, [_sequence(owner)]]
    lock = AsyncMock()
    monkeypatch.setattr(preparation.destination.restore, "_lock_owned_restore_relations", lock)
    monkeypatch.setattr(preparation.destination, "verify_entity_address_archive_stage_ownership", AsyncMock())
    shape_by_field = {"shapes": [[name, oid, {"columns": ["synthetic"]}] for name, oid in owner.relation_oids]}
    monkeypatch.setattr(preparation, "_candidate_schema_identity", AsyncMock(return_value=shape_by_field))
    seal = await preparation._protected_catalog(session, proof, owner, read_only=read_only)
    assert seal == preparation._digest(
        {"relations": rows, "sequence": proof["inventory"]["sequences"][0], "schema": shape_by_field}
    )
    lock.assert_awaited_once_with(session, owner, read_only=read_only)
    assert session.scalar.await_args_list[-1].args[1]["relation_oids"] == [201]


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
    owner, proof = _inventory()
    rows = [_heap(oid) for _name, oid in owner.relation_oids]
    if field == "missing":
        rows.pop()
    else:
        rows[0][field] = value
    session = _session()
    session.scalar.return_value = True
    session.execute.return_value.mappings.return_value.all.return_value = rows
    monkeypatch.setattr(preparation.destination.restore, "_lock_owned_restore_relations", AsyncMock())
    monkeypatch.setattr(preparation.destination, "verify_entity_address_archive_stage_ownership", AsyncMock())
    sequence = AsyncMock()
    monkeypatch.setattr(preparation, "_protected_sequence", sequence)
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="candidate catalog differs"):
        await preparation._protected_catalog(session, proof, owner)
    sequence.assert_not_awaited()


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
    owner, proof = _inventory()
    row = _sequence(owner)
    row[field] = value
    session = _session()
    session.execute.return_value.mappings.return_value.all.return_value = [] if field == "missing" else [row]
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="sequence differs"):
        await preparation._protected_sequence(session, proof, owner)
    session.scalar.assert_not_awaited()


async def test_schema_seal_preserves_settings_and_index_identities(monkeypatch):
    owner, _proof = _inventory()
    session = _session()
    indexes = [{"oid": 301, "indrelid": 100, "relname": "synthetic_idx", "filenode": 302}]
    session.execute.return_value.mappings.return_value.all.return_value = indexes
    receipt = preparation.destination.restore.importlib.import_module("process.entity_address_snapshot_receipt")
    normalize, schema = AsyncMock(), AsyncMock(return_value={"columns": ["synthetic"]})
    monkeypatch.setattr(receipt, "_normalize_receipt_session", normalize)
    monkeypatch.setattr(receipt, "_schema_identity", schema)
    monkeypatch.setattr(preparation.destination.db, "bind_existing_session", _bound)
    monkeypatch.setattr(preparation.destination, "_preserve_receipt_settings", lambda: _bound(session))
    identity = await preparation._candidate_schema_identity(session, owner)
    normalize.assert_awaited_once_with(session, owner.schema_name)
    assert identity["indexes"] == indexes
    assert [(name, oid) for name, oid, _shape in identity["shapes"]] == list(owner.relation_oids)
    assert [call.args for call in schema.await_args_list] == [
        (session, oid, owner.schema_name, name) for name, oid in owner.relation_oids
    ]


@pytest.mark.parametrize("damage", [None, "missing", "unsafe", "partition", "temporary", "view"])
async def test_publication_state_drains_writers_and_refuses_hooks(damage):
    rows = [_heap(401), _heap(402)]
    if damage == "missing":
        rows.pop()
    elif damage is not None:
        field, value = {
            "unsafe": ("unsafe", True),
            "partition": ("relispartition", True),
            "temporary": ("relpersistence", "t"),
            "view": ("relkind", "v"),
        }[damage]
        rows[0][field] = value
    session = _session()
    session.scalar.return_value = None
    session.execute.return_value.mappings.return_value.all.return_value = rows
    if damage is None:
        await preparation._lock_publication_state(session, "example", 31)
    else:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="publication state catalog"):
            await preparation._lock_publication_state(session, "example", 31)
    assert str(session.execute.await_args_list[0].args[0]).endswith("IN SHARE ROW EXCLUSIVE MODE NOWAIT")


@pytest.mark.parametrize("builder", [None, {"name": '"builder"', "safe": False}, {"name": '"builder"', "safe": True}])
async def test_builder_handback_requires_safe_role_before_any_ddl(monkeypatch, builder):
    owner, proof = _inventory()
    names_by_table = {name: name + "_candidate" for name, _oid in owner.relation_oids}
    session = _session(builder)
    drop = AsyncMock()
    monkeypatch.setattr(preparation.destination.restore, "_drop_empty_owned_schema", drop)
    if builder is None or not builder["safe"]:
        with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="builder authority changed"):
            await preparation._restore_builder_owner(session, proof, owner, "example", names_by_table)
        assert session.execute.await_count == 1
        drop.assert_not_awaited()
    else:
        await preparation._restore_builder_owner(session, proof, owner, "example", names_by_table)
        statements = [str(call.args[0]) for call in session.execute.await_args_list[1:]]
        assert sum("ALTER TABLE" in statement for statement in statements) == 7
        assert sum("ALTER SEQUENCE" in statement for statement in statements) == 1
        assert statements == [
            *(f'ALTER TABLE "example"."{name}_candidate" OWNER TO "builder"' for name, _oid in owner.relation_oids),
            'ALTER SEQUENCE "example"."entity_address_evidence_candidate_evidence_id_seq" OWNER TO "builder"',
        ]
        drop.assert_awaited_once_with(session, owner)
