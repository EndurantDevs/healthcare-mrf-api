# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reject substituted ownership before deleting an archive's local stage."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
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


def _set_owner():
    from process.entity_address_snapshot_alias import AUTHORITY_TABLE

    return replace(_owner(), relation_oids=tuple(sorted((*_owner().relation_oids, (AUTHORITY_TABLE, 8)))))


def test_v2_owner_closed_inventory():
    assert ownership.validate_entity_address_archive_stage_ownership(_set_owner()) == _set_owner()
    forged = replace(_set_owner(), relation_oids=tuple(sorted((*_owner().relation_oids, ("unrelated", 8)))))
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError):
        ownership.validate_entity_address_archive_stage_ownership(forged)


async def test_retained_catalog_reads_native_flags_as_text():
    names = [{"schema_name": "example", "relation_name": "retained_example"}]
    catalog_rows = [
        {"nspname": "example", "relname": "retained_example", "relkind": "r", "relpersistence": "p", "inherited": False}
    ]
    result = Mock()
    result.mappings.return_value.all.return_value = catalog_rows
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    assert await ownership._retained_relation_catalog(session, names) == catalog_rows
    statement, parameters = session.execute.await_args.args
    assert "c.relkind::text AS relkind,c.relpersistence::text AS relpersistence" in str(statement)
    assert parameters == {"names": ['"example"."retained_example"']}
    ownership._require_retained_catalog(catalog_rows, None, names)


async def test_retirement_rechecks_complete_named_inventory_under_locks_before_drop(monkeypatch):
    from process import network_cms_registry_address_capture as capture
    from process import network_compact_retention as retention

    monkeypatch.setattr(capture, "native_driver", AsyncMock(return_value=object()))
    monkeypatch.setattr(retention, "require_network_compact_artifacts_unreferenced", AsyncMock(return_value=True))
    owner = _set_owner()
    relations = [
        {"schema_name": "example", "relation_name": name + "_retained_example"} for name in _owner().relation_names
    ]
    authority_by_field = {"schema_name": owner.schema_name, "relation_name": "entity_address_alias_authority"}
    monkeypatch.setattr(
        ownership, "_validated_publication_owner", lambda *_: ("example", relations, authority_by_field)
    )
    catalog_rows = [
        {
            "nspname": entry["schema_name"],
            "relname": entry["relation_name"],
            "relkind": "r",
            "relpersistence": "p",
            "inherited": False,
        }
        for entry in [*relations, authority_by_field]
    ]
    monkeypatch.setattr(ownership, "_retained_relation_catalog", AsyncMock(side_effect=[catalog_rows, []]))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="named catalog differs"):
        await ownership.cleanup_entity_address_archive_publication(
            session, owner=owner, publication={}, assert_unreferenced=AsyncMock(return_value=True)
        )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 1 and "ACCESS SHARE MODE NOWAIT" in statements[0]
    assert not any("DROP" in statement for statement in statements)


@pytest.mark.parametrize(
    "changes",
    [
        {"dataset_id": "not-a-uuid"},
        {"schema_name": "another_stage"},
        {"dataset_id": None},
        {"relation_oids": []},
        {"relation_oids": {}},
        {"extra": "field"},
    ],
)
def test_owner_token_rejects_substituted_identity(changes):
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="token is invalid"):
        ownership.validate_entity_address_archive_stage_ownership({**_owner().as_dict(), **changes})


@pytest.mark.parametrize("entry", [None, {}, {"table_name": 1, "oid": 1}, {"table_name": False, "oid": True}])
def test_owner_token_rejects_malformed_relation_identity(entry):
    stored = _owner().as_dict()
    stored["relation_oids"][0] = entry
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="token is invalid"):
        ownership.validate_entity_address_archive_stage_ownership(stored)


@pytest.mark.parametrize("mutation", ["duplicate_name", "missing_relation", "order"])
def test_owner_token_requires_exact_unique_model_family(mutation):
    stored = _owner().as_dict()
    if mutation == "duplicate_name":
        stored["relation_oids"][0]["table_name"] = stored["relation_oids"][1]["table_name"]
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
async def test_revalidation_accepts_same_named_family_with_fresh_native_handles(monkeypatch, substitution):
    owner = _owner()
    monkeypatch.setattr(ownership, "_schema_oid", AsyncMock(return_value=100 if substitution == "schema" else 99))
    changed = ((owner.relation_names[0], 100), *owner.relation_oids[1:])
    monkeypatch.setattr(ownership, "_table_oids", AsyncMock(return_value=changed))
    structural = AsyncMock()
    monkeypatch.setattr(ownership, "_assert_namespace_is_owned", structural)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    assert await ownership.verify_entity_address_archive_stage_ownership(session, owner=owner) == owner
    assert structural.await_args.kwargs["relation_oids"] == changed


@pytest.mark.asyncio
async def test_cleanup_refuses_an_incomplete_named_family_before_delete(monkeypatch):
    monkeypatch.setattr(ownership, "_schema_oid", AsyncMock(return_value=100))
    monkeypatch.setattr(ownership, "_table_oids", AsyncMock(return_value=_owner().relation_oids[:-1]))
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=True), execute=AsyncMock())
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="relation family differs"):
        await ownership.cleanup_entity_address_archive_stage(session, owner=_owner())
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 1 and "ACCESS EXCLUSIVE MODE NOWAIT" in statements[0]
    assert not any("DROP" in statement for statement in statements)


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


def test_fresh_owner_emits_only_names_and_old_tokens_roundtrip_without_authority():
    old = _owner().as_dict()
    copied_by_field = {
        **old,
        "schema_oid": 10000,
        "relation_oids": [{**entry, "oid": 10000 + index} for index, entry in enumerate(old["relation_oids"])],
    }
    assert ownership.validate_entity_address_archive_stage_ownership(copied_by_field) == _owner()
    assert ownership.validate_entity_address_archive_stage_ownership(copied_by_field).as_dict() == copied_by_field
    named = ownership.EntityAddressArchiveStageOwnership(
        _owner().dataset_id, _owner().schema_name, named_relations=_owner().relation_names
    )
    assert set(named.as_dict()) == {"dataset_id", "schema_name", "relations"}
    assert ownership.validate_entity_address_archive_stage_ownership(named.as_dict()) == _owner()


def test_current_canonical_publication_names_are_never_retirement_targets():
    owner = _set_owner()
    publication_by_field = {
        "contract": "entity-address-table-publication.v3",
        "schema_name": "mrf",
        "relations": [{"schema_name": "mrf", "relation_name": name} for name in _owner().relation_names],
        "retained_relations": [],
        "alias_authority": {"schema_name": owner.schema_name, "relation_name": "entity_address_alias_authority"},
    }
    with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="remains serving"):
        ownership._validated_publication_owner(owner, publication_by_field)


@pytest.mark.parametrize("has_alias", [False, True])
def test_retirement_alias_matches_the_declared_seven_or_eight_table_edition(has_alias):
    owner = _set_owner() if has_alias else _owner()
    relation_names = [model.__tablename__ + "_retained_previous" for model in ownership._models()]
    publication_by_field = {
        "contract": "entity-address-table-publication.v3",
        "schema_name": "mrf",
        "relations": [{"schema_name": "mrf", "relation_name": name} for name in relation_names],
        "retained_relations": [],
        "alias_authority": None,
    }
    if has_alias:
        with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="inventory differs"):
            ownership._validated_publication_owner(owner, publication_by_field)
        publication_by_field["alias_authority"] = {
            "schema_name": owner.schema_name,
            "relation_name": "entity_address_alias_authority",
        }
    assert (
        ownership._validated_publication_owner(owner, publication_by_field)[2]
        == publication_by_field["alias_authority"]
    )
    if not has_alias:
        publication_by_field["alias_authority"] = {
            "schema_name": owner.schema_name,
            "relation_name": "entity_address_alias_authority",
        }
        with pytest.raises(ownership.EntityAddressArchiveOwnershipError, match="inventory differs"):
            ownership._validated_publication_owner(owner, publication_by_field)
