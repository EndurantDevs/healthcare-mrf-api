# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable recovery snapshots keep data checks within named SQL locks."""

import copy
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_serving_recovery_receipts as receipts


def _receipt(name=receipts.ROOTS[0], *, digest="a"):
    return {
        "native": {
            "schema": "snapshot_example",
            "relname": name,
            "columns": [{"number": 1, "name": "value", "type": "text", "required": True}],
            "indexes": [{"name": name + "_pkey", "valid": True, "keys": ["value"], "native": True}],
            "constraints": [{"name": name + "_pkey", "definition": "PRIMARY KEY (value)"}],
        },
        "rows": 2,
        "data_sha256": digest * 64,
    }


def _legacy_state():
    roots_by_name = {root: _receipt(root) for root in receipts.ROOTS}
    for number, receipt in enumerate(roots_by_name.values(), 101):
        native = receipt["native"]
        native.pop("schema")
        native.update(oid=number, schema_oid=900, relowner=800, acl={"table": [[800, 800, "SELECT", False]]})
        native["indexes"][0].update(oid=number + 100, owner=800, opclasses=[3126], collations=[100])
        native["constraints"][0]["oid"] = number + 200
    return {
        "roots": roots_by_name,
        "metadata": {
            "address": {"singleton": True, "local_generation": 3, "relation_oids": [1, 2, 3, 4, 5, 6, 7]},
            "alias": {"generation": 2},
            "overlay_alias": None,
            "profile": None,
            "doctors": {"importer_id": "cms-doctors", "local_generation": 1, "relation_oids": [71, 72, 73]},
            "geo": {"version": "example-v1", "table_oid": 102, "signature": {"owner": 800}, "bindings": []},
            "relations": [1, 2, 3, 4, 5, 6, 7],
            "common_receipt": {"receipt_id": "current", "payload": {"predecessor_receipt_id": "previous", "oid": 20}},
        },
    }


def test_legacy_metadata_is_portable_without_rewriting():
    original = _legacy_state()
    unchanged = copy.deepcopy(original)
    normalized = receipts.normalize_state(original, "snapshot_example")
    assert original == unchanged
    assert "relation_oids" not in normalized["metadata"]["address"]
    assert normalized["metadata"]["doctors"] == {"importer_id": "cms-doctors", "local_generation": 1}
    assert normalized["metadata"]["geo"] == {
        "version": "example-v1",
        "relation": "snapshot_example.entity_address_unified",
    }
    assert normalized["metadata"]["common_receipt"] == {"receipt_id": "current", "predecessor_receipt_id": "previous"}
    assert normalized["metadata"]["relations"][0] == "snapshot_example.entity_address_unified"
    for root, receipt in normalized["roots"].items():
        old_native = original["roots"][root]["native"]
        assert receipts.retained_name(receipt) == "pdsrv_r_" + format(old_native["oid"], "x")
        assert "oid" not in receipt["native"] and "acl" not in receipt["native"]
        assert "owner" not in receipt["native"]["indexes"][0]


def test_profile_target_metadata_is_portable_without_rewriting():
    original = _legacy_state()
    profile_by_field = {"generation_id": "profile-generation", "source_set_hash": "b" * 64, "profile_rows": 2}
    original["metadata"]["profile"] = {
        **profile_by_field,
        "profile_target_oid": 101,
        "evidence_target_oid": 102,
    }
    unchanged = copy.deepcopy(original)
    expected = receipts.normalize_state(original, "snapshot_example")
    assert expected["metadata"]["profile"] == profile_by_field
    assert original == unchanged
    actual = copy.deepcopy(expected)
    actual["metadata"]["profile"].update(profile_target_oid=201, evidence_target_oid=202)
    receipts.require_state(expected, actual)
    for field, value in (("generation_id", "other"), ("source_set_hash", "c" * 64), ("profile_rows", 3)):
        changed = copy.deepcopy(actual)
        changed["metadata"]["profile"][field] = value
        with pytest.raises(ValueError, match="state_changed"):
            receipts.require_state(expected, changed)


def test_physical_metadata_does_not_authorize_snapshot():
    expected, actual = _receipt(), _receipt()
    expected["native"].update(oid=101, schema_oid=201, relowner=301, acl={"table": []})
    actual["native"].update(oid=999, schema_oid=998, relowner=997, acl={"table": [[997, 997, "SELECT", False]]})
    expected["native"]["indexes"][0].update(oid=501, owner=301, opclasses=[3126])
    actual["native"]["indexes"][0].update(oid=599, owner=997, opclasses=[9326])
    receipts.require_relation(expected, actual)


@pytest.mark.parametrize("damage", ["schema", "name", "column", "index", "constraint", "rows", "digest"])
def test_snapshot_data_and_layout_remain_checked(damage):
    expected, actual = _receipt(), _receipt()
    match damage:
        case "schema" | "name":
            actual["native"]["schema" if damage == "schema" else "relname"] = "other"
        case "column":
            actual["native"]["columns"][0]["type"] = "integer"
        case "index":
            actual["native"]["indexes"][0]["valid"] = False
        case "constraint":
            actual["native"]["constraints"][0]["definition"] = "UNIQUE (value)"
        case "rows":
            actual["rows"] += 1
        case _:
            actual["data_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="changed"):
        receipts.require_relation(expected, actual)


def test_retention_names_follow_generation_and_phase():
    original = _receipt()
    first = receipts.plan_retention(original, "generation-one", "overlay-before")
    again = receipts.plan_retention(original, "generation-one", "overlay-before")
    next_generation = receipts.plan_retention(original, "generation-two", "overlay-before")
    next_phase = receipts.plan_retention(original, "generation-one", "overlay-after")
    assert first == again and "retained_name" not in original["native"]
    assert len({receipts.retained_name(value) for value in (first, next_generation, next_phase)}) == 3
    assert len(receipts.retained_name(first)) <= 63
    assert receipts.retained_name(first["native"]["indexes"][0], index=True).startswith("pdsrv_i_")
    receipts.require_relation(first, next_generation)


def test_state_comparison_ignores_retention_plans():
    expected = receipts.normalize_state(_legacy_state(), "snapshot_example")
    actual = copy.deepcopy(expected)
    for root, receipt in actual["roots"].items():
        actual["roots"][root] = receipts.plan_retention(receipt, "next-generation", "published-after")
    receipts.require_state(expected, actual)
    actual["metadata"]["relations"][0] = "other.entity_address_unified"
    with pytest.raises(ValueError, match="state_invalid"):
        receipts.validate_state(actual)


def _database(events):
    @asynccontextmanager
    async def transaction():
        events.append("enter")
        try:
            yield
        finally:
            events.append(("exit", receipts._VALIDATED.get()))

    return SimpleNamespace(transaction=transaction)


def _mock_capture(monkeypatch, relation_by_name):
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value={"heap_bytes": 100, "total_bytes": 200}))

    async def relation(_database, _schema, name, *, content=True):
        receipt = copy.deepcopy(relation_by_name[name])
        if not content:
            receipt.pop("rows")
            receipt.pop("data_sha256")
        return receipt

    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(None, driver, {})))
    monkeypatch.setattr(receipts, "lock", AsyncMock())
    monkeypatch.setattr(receipts, "relation", relation)


@pytest.mark.asyncio
async def test_ordinary_uses_sql_transaction_without_actor(monkeypatch):
    session = SimpleNamespace(in_transaction=lambda: True)
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    driver = SimpleNamespace(is_in_transaction=lambda: True)
    monkeypatch.setattr(receipts, "_copy_driver", AsyncMock(return_value=driver))
    assert await receipts.ordinary(database) == (session, driver, {})


@pytest.mark.asyncio
async def test_cache_expires_before_transaction_exit(monkeypatch):
    events = []
    database = _database(events)
    original = _receipt()
    _mock_capture(monkeypatch, {original["native"]["relname"]: original})
    with pytest.raises(RuntimeError, match="rollback"):
        async with receipts.validation_scope(database):
            prepared = await receipts.prepare_relation(database, "snapshot_example", receipts.ROOTS[0])
            cache = receipts._VALIDATED.get()
            assert await receipts.recheck_prepared(database, prepared, receipts.ROOTS[0]) == original
            raise RuntimeError("rollback")
    assert cache == {} and events[-1] == ("exit", None)
    with pytest.raises(ValueError, match="preparation_scope_required"):
        await receipts.recheck_prepared(database, prepared, receipts.ROOTS[0])
    async with receipts.validation_scope(database):
        with pytest.raises(ValueError, match="prepared_name_changed"):
            await receipts.recheck_prepared(database, prepared, receipts.ROOTS[0])


@pytest.mark.asyncio
async def test_new_scope_revalidates_exact_content_before_reusing_proof(monkeypatch):
    database = _database([])
    original = _receipt()
    relation_by_name = {receipts.ROOTS[0]: original}
    _mock_capture(monkeypatch, relation_by_name)
    async with receipts.validation_scope(database):
        prepared = await receipts.prepare_relation(database, "snapshot_example", receipts.ROOTS[0])
    async with receipts.validation_scope(database):
        assert await receipts.recheck_prepared(database, prepared, receipts.ROOTS[0], prepare_missing=True) == original
    relation_by_name[receipts.ROOTS[0]] = _receipt(digest="b")
    async with receipts.validation_scope(database):
        with pytest.raises(ValueError, match="content_changed"):
            await receipts.recheck_prepared(database, prepared, receipts.ROOTS[0], prepare_missing=True)


@pytest.mark.asyncio
async def test_cache_follows_only_validated_named_renames(monkeypatch):
    database = _database([])
    original, other = _receipt("candidate"), _receipt("other", digest="b")
    relation_by_name = {"candidate": original, "other": other}
    _mock_capture(monkeypatch, relation_by_name)
    async with receipts.validation_scope(database):
        prepared = await receipts.prepare_relation(database, "snapshot_example", "candidate")
        with pytest.raises(ValueError, match="prepared_name_changed"):
            await receipts.recheck_prepared(database, prepared, "other")
        await receipts.prepare_relation(database, "snapshot_example", "other")
        with pytest.raises(ValueError, match="content_changed"):
            await receipts.recheck_prepared(database, prepared, "other")
        relation_by_name["live"] = copy.deepcopy(original)
        relation_by_name["live"]["native"]["relname"] = "live"
        receipts.record_rename("snapshot_example", "candidate", "live")
        assert await receipts.recheck_prepared(database, prepared, "live") == relation_by_name["live"]
        with pytest.raises(ValueError, match="prepared_name_changed"):
            await receipts.recheck_prepared(database, prepared, "candidate")


@pytest.mark.asyncio
async def test_failed_rename_keeps_cached_name(monkeypatch):
    database = _database([])
    original = _receipt("candidate")
    _mock_capture(monkeypatch, {"candidate": original})
    async with receipts.validation_scope(database):
        await receipts.prepare_relation(database, "snapshot_example", "candidate")
        driver = SimpleNamespace(
            fetchval=AsyncMock(return_value=None), execute=AsyncMock(side_effect=RuntimeError("SQL"))
        )
        monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(None, driver, {})))
        with pytest.raises(RuntimeError, match="SQL"):
            await receipts.rename(database, "snapshot_example", "candidate", "live")
        assert ("snapshot_example", "candidate") in receipts._VALIDATED.get()
        assert ("snapshot_example", "live") not in receipts._VALIDATED.get()


@pytest.mark.asyncio
async def test_nested_validation_reuses_outer_transaction_and_cleanup(monkeypatch):
    """Coupled publication retains its measured stages through inner cutover scopes."""
    events = []
    database = _database(events)
    original = _receipt()
    _mock_capture(monkeypatch, {original["native"]["relname"]: original})
    async with receipts.validation_scope(database):
        prepared = await receipts.prepare_relation(database, "snapshot_example", receipts.ROOTS[0])
        cache = receipts._VALIDATED.get()
        with pytest.raises(RuntimeError, match="inner"):
            async with receipts.validation_scope(database):
                assert receipts._VALIDATED.get() is cache
                assert await receipts.recheck_prepared(database, prepared, receipts.ROOTS[0]) == original
                raise RuntimeError("inner")
        assert receipts._VALIDATED.get() is cache and cache
    assert cache == {} and receipts._VALIDATED.get() is None
    assert events == ["enter", ("exit", None)]
