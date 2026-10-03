# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Migrated CMS hooks are admitted only over a pinned, empty receipt history."""

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import pytest

from process import entity_address_snapshot_preparation as preparation
from tests.test_entity_address_preparation_catalog import _heap
from tests.test_entity_address_snapshot_preparation import _session


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260930100000_cms_npd_serving_receipt.py"
    spec = importlib.util.spec_from_file_location("cms_receipt_migration_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _guards():
    return [
        {
            "trigger_oid": index + 501,
            "constraint_oid": 601 if index == 0 else 0,
            "tgname": name,
            "prosrc": body,
            "safe": True,
        }
        for index, (name, body) in enumerate(preparation._cms_guard_bodies("example").items())
    ]


def test_cms_guard_bodies_match_actual_migration(monkeypatch):
    migration = _migration()
    recorder = Mock()
    monkeypatch.setattr(migration, "op", recorder)
    migration._create_native_transition_guards('"example"')
    migration._create_truncate_guards('"example"')
    statements = [call.args[0] for call in recorder.execute.call_args_list]
    for name, body in preparation._cms_guard_bodies("example").items():
        statement = next(value for value in statements if value.startswith(f'CREATE FUNCTION "example".{name}()'))
        assert " ".join(statement.split("$$")[1].split()) == " ".join(body.split())


async def test_migrated_hooks_are_exact_oid_exemptions_after_receipt_lock():
    session = _session()
    session.scalar.side_effect = [401, True, False]
    session.execute.return_value.mappings.return_value.all.side_effect = [_guards(), [_heap(701), _heap(702)]]
    await preparation._lock_publication_state(session, "example", 31)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0].endswith("IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    assert statements[1] == 'LOCK TABLE ONLY "example"."provider_directory_cms_serving_receipt" IN SHARE MODE NOWAIT'
    assert session.execute.await_args_list[2].args[1] == {"schema": "example", "owner_oid": 31}
    assert session.execute.await_args_list[-1].args[1] == {
        "schema": "example",
        "names": ["entity_address_geo_assurance_state", "entity_address_result_generation"],
        "guard_triggers": [501, 502],
        "guard_constraints": [601],
    }
    assert "t.oid<>ALL(CAST(:guard_triggers AS oid[]))" in statements[-1]
    assert "k.oid<>ALL(CAST(:guard_constraints AS oid[]))" in statements[-1]


async def test_absent_cms_history_does_not_admit_any_hook():
    session = _session()
    session.scalar.return_value = None
    assert await preparation._cms_receipt_guard_oids(session, "example", 31) == ([], [])
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["missing", "extra", "renamed", "body", "authority", "unknown_authority"])
async def test_cms_guard_damage_refuses_publication(damage):
    catalog_rows = _guards()
    if damage == "missing":
        catalog_rows.pop()
    elif damage == "extra":
        catalog_rows.append({**catalog_rows[0], "tgname": "unexpected_guard"})
    else:
        field, value = {
            "renamed": ("tgname", "unexpected_guard"),
            "body": ("prosrc", "BEGIN RETURN NULL; END"),
            "authority": ("safe", False),
            "unknown_authority": ("safe", None),
        }[damage]
        catalog_rows[0][field] = value
    session = _session()
    session.scalar.side_effect = [401, True, False]
    session.execute.return_value.mappings.return_value.all.return_value = catalog_rows
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match="CMS publication guards"):
        await preparation._lock_publication_state(session, "example", 31)
    assert session.execute.await_count == 3


@pytest.mark.parametrize("safe,populated", [(False, False), (None, False), (True, True), (True, None)])
async def test_unsupported_or_populated_receipts_refuse_before_guard_admission(safe, populated):
    session = _session()
    session.scalar.side_effect = [401, safe, populated]
    message = "CMS receipt catalog" if safe is not True else "populated CMS receipt history"
    with pytest.raises(preparation.EntityAddressSnapshotDestinationError, match=message):
        await preparation._lock_publication_state(session, "example", 31)
    assert session.execute.await_count == 2
    assert session.scalar.await_count == (3 if safe is True else 2)


def test_cms_catalog_attestation_keeps_owner_function_and_constraint_envelopes():
    statement = preparation._CMS_GUARDS_SQL
    for predicate in (
        preparation._ORDINARY_PRINCIPAL_SQL,
        "pg_catalog.pg_has_role(principal.oid,p.proowner,'MEMBER')",
        "p.proconfig=ARRAY['search_path=pg_catalog']::text[]",
        "AND NOT p.prosecdef",
        "p.prosupport=0",
        "language.lanname='plpgsql'",
        "t.tgtype=34 AND t.tgconstraint=0",
        "t.tgtype=29 AND t.tgdeferrable AND t.tginitdeferred",
        "k.contype='t'",
        "k.conrelid=c.oid",
        "k.conkey IS NULL",
        "k.confkey IS NULL",
        "k.conbin IS NULL",
    ):
        assert predicate in statement
