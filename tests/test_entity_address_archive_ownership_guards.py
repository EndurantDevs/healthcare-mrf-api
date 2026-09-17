# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject substituted ownership before deleting an archive's local stage."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import entity_address_snapshot_ownership as ownership


def _owner():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    return ownership.EntityAddressArchiveStageOwnership(
        dataset_id,
        ownership.entity_address_archive_stage_schema(dataset_id),
        99,
        tuple(
            (model.__tablename__, index)
            for index, model in enumerate(
                sorted(ownership._models(), key=lambda model: model.__tablename__),
                start=1,
            )
        ),
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"dataset_id": "not-a-uuid"},
        {"schema_name": "another_stage"},
        {"schema_oid": True},
        {"schema_oid": 0},
        {"relation_oids": {}},
        {"extra": "field"},
    ],
)
def test_owner_token_rejects_substituted_identity(changes):
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="token is invalid"):
        ownership.validate_entity_address_archive_stage_ownership({**_owner().as_dict(), **changes})


@pytest.mark.parametrize(
    "entry", [None, {}, {"table_name": 1, "oid": 1}, {"table_name": "entity_address_unified", "oid": True}]
)
def test_owner_token_rejects_malformed_relation_identity(entry):
    stored = _owner().as_dict()
    stored["relation_oids"][0] = entry
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="token is invalid"):
        ownership.validate_entity_address_archive_stage_ownership(stored)


@pytest.mark.parametrize("mutation", ["duplicate_oid", "missing_relation", "order"])
def test_owner_token_requires_exact_unique_model_family(mutation):
    stored = _owner().as_dict()
    if mutation == "duplicate_oid":
        stored["relation_oids"][0]["oid"] = stored["relation_oids"][1]["oid"]
    elif mutation == "missing_relation":
        stored["relation_oids"].pop()
    else:
        stored["relation_oids"].reverse()
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="token is invalid"):
        ownership.validate_entity_address_archive_stage_ownership(stored)


@pytest.mark.parametrize(
    "support_names", [(), ("duplicate",) * 6, (*("stage_" + str(i) for i in range(5)), "bad-name")]
)
def test_invalid_model_family_cannot_authorize_ownership(monkeypatch, support_names):
    monkeypatch.setattr(
        ownership.entity_address_unified,
        "SUPPORT_TABLE_MODELS",
        tuple(SimpleNamespace(__tablename__=name) for name in support_names),
    )
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="model family is invalid"):
        ownership._models()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "session", [SimpleNamespace(), SimpleNamespace(in_transaction=False), SimpleNamespace(in_transaction=lambda: False)]
)
async def test_cleanup_requires_caller_transaction_before_catalog_access(session):
    session.execute = AsyncMock()
    session.scalar = AsyncMock()
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="caller transaction"):
        await ownership.cleanup_entity_address_archive_stage(session, owner=_owner())
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", [None, "99", 0])
async def test_capture_rejects_unavailable_schema(observed):
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=observed), execute=AsyncMock())
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="schema is unavailable"):
        await ownership.capture_created_entity_address_archive_stage(session, dataset_id=_owner().dataset_id)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing", "extra", "zero_oid"])
async def test_capture_rejects_changed_catalog_family(mutation):
    rows = [{"relname": name, "oid": oid} for name, oid in _owner().relation_oids]
    if mutation == "missing":
        rows.pop()
    elif mutation == "extra":
        rows.append({"relname": "unrelated", "oid": 50})
    else:
        rows[0]["oid"] = 0
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: rows)))
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="relation family differs"):
        await ownership._table_oids(session, 99)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,index_owner,sequence_owner,accepted",
    [
        ("r", None, False, True),
        ("i", 1, False, True),
        ("S", None, True, True),
        ("S", None, False, False),
        ("i", 100, False, False),
        ("v", None, False, False),
    ],
)
async def test_namespace_cleanup_admits_only_owned_relations(monkeypatch, kind, index_owner, sequence_owner, accepted):
    records = [{"oid": 1, "relkind": kind, "index_table_oid": index_owner}]
    monkeypatch.setattr(ownership, "_namespace_relation_records", AsyncMock(return_value=records))
    session = SimpleNamespace(scalar=AsyncMock(return_value=sequence_owner))
    if accepted:
        await ownership._assert_namespace_is_owned(session, schema_oid=99, relation_oids=_owner().relation_oids)
    else:
        with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="unexpected relation"):
            await ownership._assert_namespace_is_owned(session, schema_oid=99, relation_oids=_owner().relation_oids)
    if kind == "S":
        assert session.scalar.await_args.args[1] == {"sequence_oid": 1, "relation_oids": list(range(1, 8))}
    else:
        session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("substitution", ["schema", "table"])
async def test_revalidation_rejects_same_name_replacement(monkeypatch, substitution):
    owner = _owner()
    monkeypatch.setattr(ownership, "_schema_oid", AsyncMock(return_value=100 if substitution == "schema" else 99))
    changed = replace(owner, relation_oids=((owner.relation_oids[0][0], 100), *owner.relation_oids[1:]))
    monkeypatch.setattr(ownership, "_table_oids", AsyncMock(return_value=changed.relation_oids))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="OID differs"):
        await ownership.verify_entity_address_archive_stage_ownership(session, owner=owner)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_does_not_lock_or_delete_a_replacement_namespace():
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=100), execute=AsyncMock())
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="schema OID differs"):
        await ownership.cleanup_entity_address_archive_stage(session, owner=_owner())
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_never_drops_namespace_with_remaining_objects(monkeypatch):
    owner = _owner()
    verified = AsyncMock(return_value=owner)
    monkeypatch.setattr(ownership, "verify_entity_address_archive_stage_ownership", verified)
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(side_effect=[99, 1]), execute=AsyncMock())
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="namespace is not empty"):
        await ownership.cleanup_entity_address_archive_stage(session, owner=owner)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 2
    assert statements[0].startswith("LOCK TABLE ") and statements[0].endswith("ACCESS EXCLUSIVE MODE NOWAIT")
    assert statements[1].startswith("DROP TABLE ") and statements[1].endswith("RESTRICT")
    assert all("DROP SCHEMA" not in statement and "CASCADE" not in statement for statement in statements)
    verified.assert_awaited_once_with(session, owner=owner)
