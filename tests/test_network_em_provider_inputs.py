# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure custody/control regressions; genuine PostgreSQL qualification is separate."""

import asyncio
import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_em_provider_inputs as inputs
from process.network_membership_copy import MembershipCopyTarget
from process.network_serving_read import PinnedNetworkServingManifest


def family():
    return inputs.EMProviderInputs(
        tuple(
            inputs.EMProviderInputRelation(role, "retained_inputs", "held_" + str(i), "generation-1", 100 + i, 200 + i)
            for i, role in enumerate(inputs._ROLES)
        )
    )


def catalog():
    catalog_rows = []
    for i, role in enumerate(inputs._ROLES):
        columns = [
            dict(
                attnum=n,
                attname=name,
                type_name=kind,
                type_schema="public" if kind == "geometry" else "pg_catalog",
                type_oid=300 + n,
                atttypmod=-1,
                attnotnull=False,
                default_expr=None,
                canonical_geometry=kind == "geometry",
                geometry_srid=4269 if kind == "geometry" else None,
                geometry_type="MultiPolygon" if kind == "geometry" else None,
            )
            for n, (name, kind) in enumerate(inputs._required_columns(role).items(), 1)
        ]
        key = {
            "npi": ["npi"],
            "npi_taxonomy": ["npi", "checksum"],
            "nucc_taxonomy": ["code"],
            "geo_zip_lookup": ["zip_code"],
        }.get(role)
        catalog_rows.append(
            dict(
                role=role,
                schema_oid=10,
                relation_oid=100 + i,
                relfilenode=200 + i,
                owner_oid=20,
                owner_role="retained_owner",
                rolcanlogin=False,
                rolsuper=False,
                rolcreatedb=False,
                rolcreaterole=False,
                rolreplication=False,
                rolbypassrls=False,
                same_owner=True,
                relkind="r",
                relpersistence="p",
                relispartition=False,
                relrowsecurity=False,
                relforcerowsecurity=False,
                has_triggers=False,
                inheritance_ok=True,
                columns_json=json.dumps(columns),
                keys_json=json.dumps([dict(index_oid=400 + i, columns=key)] if key else []),
            )
        )
    return catalog_rows


def connection(rows=None):
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        fetch=AsyncMock(return_value=rows or catalog()),
        fetchval=AsyncMock(side_effect=lambda sql, *args: "repeatable read" if sql.startswith("SHOW") else True),
        fetchrow=AsyncMock(),
        execute=AsyncMock(),
    )


def prepare(conn=None):
    return asyncio.run(
        inputs.prepare_em_provider_inputs(conn or connection(), family(), runtime_roles=("runtime_reader",))
    )


def target():
    ids = [str(UUID(int=i)) for i in range(1, 5)]
    return MembershipCopyTarget(*ids, "network_candidate_" + UUID(ids[-1]).hex)


def candidate(receipt, **updates):
    return dict(
        source_generations={"em_provider_inputs": receipt["receipt_sha256"]},
        validation_json=None,
        state="open",
        **updates,
    )


def manifest(receipt):
    return PinnedNetworkServingManifest(
        17,
        target().candidate_id,
        target().schema_name,
        1,
        {"em_provider_inputs": receipt["receipt_sha256"]},
        3,
        "a" * 64,
        77,
    )


def test_closed_family_renders_only_held_relations_and_shared_geo_bindings():
    held = family()
    assert held.relation("npi") == '"retained_inputs"."held_3"'
    bindings = held.geo_bindings("address_snapshot")
    assert len(bindings) == 6 and bindings["tiger.zcta5"]["table_name"] == "held_7"
    assert bindings["address_snapshot.npi_address"]["relation_oid"] == 104
    with pytest.raises(ValueError):
        held.relation("npi; DROP TABLE x")


@pytest.mark.parametrize("change", ["missing", "extra", "order", "duplicate_oid", "duplicate_name", "list"])
def test_roster_is_exact_sorted_tuple_with_unique_physical_relations(change):
    rows = list(family().relations)
    if change == "missing":
        rows.pop()
    if change == "extra":
        rows.append(rows[-1])
    if change == "order":
        rows.reverse()
    if change == "duplicate_oid":
        rows[1] = replace(rows[1], relation_oid=rows[0].relation_oid)
    if change == "duplicate_name":
        rows[1] = replace(rows[1], table_name=rows[0].table_name)
    with pytest.raises(ValueError):
        inputs.EMProviderInputs(rows if change == "list" else tuple(rows))


@pytest.mark.parametrize(
    "field,value",
    [
        ("role", "unknown"),
        ("schema_name", "a.b"),
        ("table_name", 'x"'),
        ("generation_id", ""),
        ("generation_id", "x" * 129),
        ("generation_id", "x\n"),
        ("relation_oid", True),
        ("relation_oid", 0),
        ("relfilenode", 2**32),
    ],
)
def test_relation_identity_refuses_unbounded_or_executable_values(field, value):
    with pytest.raises(ValueError):
        replace(family().relations[0], **{field: value})


def test_preparation_batches_catalog_and_acl_without_source_rows_locks_or_writes():
    conn = connection()
    receipt = prepare(conn)
    assert inputs._receipt(receipt) == family()
    assert receipt["relations"][6]["shape_sha256"]
    assert len(json.dumps(receipt)) < 32768
    assert conn.fetch.await_count == 1
    sql, encoded = conn.fetch.await_args.args
    assert json.loads(encoded)[3]["table_name"] == "held_3"
    assert "pg_attribute" in sql and "pg_index" in sql and "pg_trigger" in sql
    assert "NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits" in sql
    assert "to_regclass('tiger.zcta5')" in sql
    assert all("SELECT * FROM" not in call.args[0] for call in conn.fetch.await_args_list)
    assert conn.fetchval.await_count == 2
    acl = conn.fetchval.await_args.args
    assert "has_table_privilege" in acl[0] and "pg_default_acl" in acl[0]
    assert acl[1:] == (10, 20, ["runtime_reader"], ["held_" + str(i) for i in range(9)])
    conn.execute.assert_not_awaited()
    assert inputs._required_columns("nucc_taxonomy")["definition"] == "text"


@pytest.mark.parametrize(
    "field,value",
    [
        ("relation_oid", 999),
        ("relfilenode", 999),
        ("same_owner", False),
        ("rolcanlogin", True),
        ("rolsuper", True),
        ("rolbypassrls", True),
        ("relkind", "v"),
        ("relpersistence", "u"),
        ("relispartition", True),
        ("relrowsecurity", True),
        ("relforcerowsecurity", True),
        ("has_triggers", True),
        ("inheritance_ok", False),
    ],
)
def test_native_custody_damage_fails_before_acl_or_writes(field, value):
    rows = catalog()
    rows[0][field] = value
    conn = connection(rows)
    with pytest.raises(ValueError):
        prepare(conn)
    assert conn.fetchval.await_count == 1
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "role,damage",
    [
        ("npi", "missing"),
        ("npi_taxonomy", "type"),
        ("npi_address", "namespace"),
        ("tiger.zcta5", "geometry"),
        ("tiger.zcta5", "srid"),
        ("tiger.zcta5", "geometry_type"),
        ("geo_zip_lookup", "key"),
        ("nucc_taxonomy", "key"),
        ("npi", "columns_bound"),
        ("npi", "keys_bound"),
    ],
)
def test_semantic_columns_and_relevant_unique_keys_are_required(role, damage):
    rows = catalog()
    row = rows[inputs._ROLES.index(role)]
    columns = json.loads(row["columns_json"])
    keys = json.loads(row["keys_json"])
    if damage == "missing":
        columns.pop()
    if damage == "type":
        columns[0]["type_name"] = "varchar"
    if damage == "namespace":
        columns[0]["type_schema"] = "untrusted"
    if damage == "geometry":
        columns[-1]["canonical_geometry"] = False
    if damage == "srid":
        columns[-1]["geometry_srid"] = 4326
    if damage == "geometry_type":
        columns[-1]["geometry_type"] = "Point"
    if damage == "key":
        keys = []
    if damage == "columns_bound":
        columns *= 257
    if damage == "keys_bound":
        keys *= 65
    row.update(columns_json=json.dumps(columns), keys_json=json.dumps(keys))
    with pytest.raises(ValueError):
        prepare(connection(rows))


@pytest.mark.parametrize("roles", [(), ("reader", "reader"), ("z", "a"), ("bad-role",), ["reader"], (None,)])
def test_runtime_roles_are_bounded_closed_sorted_identifiers(roles):
    conn = connection()
    with pytest.raises(ValueError):
        asyncio.run(inputs.prepare_em_provider_inputs(conn, family(), runtime_roles=roles))
    conn.fetch.assert_not_awaited()


def test_repeatable_read_and_actual_privilege_closure_are_required():
    conn = connection()
    conn.is_in_transaction = lambda: False
    with pytest.raises(ValueError):
        prepare(conn)
    conn.fetch.assert_not_awaited()
    conn = connection()
    conn.fetchval.return_value = "read committed"
    conn.fetchval.side_effect = None
    with pytest.raises(ValueError):
        prepare(conn)
    conn.fetch.assert_not_awaited()
    conn = connection()
    conn.fetchval.side_effect = ["repeatable read", False]
    with pytest.raises(ValueError):
        prepare(conn)
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["extra", "revision_bool", "roles_tuple", "missing_role", "digest", "custody_extra"])
def test_receipt_parser_is_closed_and_hash_checked(damage):
    receipt = prepare()
    if damage == "extra":
        receipt["unexpected"] = True
    if damage == "revision_bool":
        receipt["revision"] = True
    if damage == "roles_tuple":
        receipt["runtime_roles"] = ("runtime_reader",)
    if damage == "missing_role":
        receipt["relations"].pop()
    if damage == "digest":
        receipt["receipt_sha256"] = "0" * 64
    if damage == "custody_extra":
        receipt["relations"][0]["sql"] = "SELECT 1"
    with pytest.raises(ValueError):
        inputs._receipt(receipt)


def test_record_updates_validation_only_under_exact_candidate_lock_and_replays(monkeypatch):
    receipt = prepare()
    conn = connection()
    saved = candidate(receipt)
    lock = AsyncMock(return_value=saved)
    monkeypatch.setattr(inputs, "_locked_candidate", lock)
    assert asyncio.run(inputs.record_em_provider_inputs(conn, target(), receipt, "control")) == receipt
    lock.assert_awaited_once_with(conn, target(), '"control"')
    sql, identity, encoded = conn.execute.await_args.args
    assert identity == UUID(target().candidate_id) and json.loads(encoded) == receipt
    assert "SET validation_json=" in sql and "source_generations=" not in sql
    assert "state='open'" in sql
    saved["validation_json"] = {"em_provider_inputs": receipt}
    conn.execute.reset_mock()
    assert asyncio.run(inputs.record_em_provider_inputs(conn, target(), receipt, "control")) == receipt
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["closed", "digest", "collision", "scope"])
def test_record_refuses_closed_scope_digest_or_receipt_collision(monkeypatch, damage):
    receipt = prepare()
    conn = connection()
    saved = candidate(receipt)
    if damage == "closed":
        saved["state"] = "published"
    if damage == "digest":
        saved["source_generations"] = {"em_provider_inputs": "0" * 64}
    if damage == "collision":
        saved["validation_json"] = {"em_provider_inputs": {}}
    lock = AsyncMock(return_value=saved, side_effect=ValueError("wrong scope") if damage == "scope" else None)
    monkeypatch.setattr(inputs, "_locked_candidate", lock)
    with pytest.raises(ValueError):
        asyncio.run(inputs.record_em_provider_inputs(conn, target(), receipt))
    conn.execute.assert_not_awaited()


def resolver(monkeypatch, *, receipt=None):
    receipt = receipt or prepare()
    pinned = manifest(receipt)
    conn = connection()
    conn.fetchrow.return_value = {
        "source_generations": pinned.source_generations,
        "validation_json": {"em_provider_inputs": copy.deepcopy(receipt)},
    }
    resolve = AsyncMock(return_value=pinned)
    monkeypatch.setattr(inputs, "resolve_network_serving_manifest", resolve)
    return conn, pinned, resolve


def test_resolver_rechecks_retained_full_manifest_and_native_custody_readonly(monkeypatch):
    conn, pinned, resolve = resolver(monkeypatch)
    assert asyncio.run(inputs.resolve_em_provider_inputs(conn, pinned, "control")) == family()
    resolve.assert_awaited_once_with(conn, generation_id=17, control_schema="control")
    sql, cid, schema = conn.fetchrow.await_args.args
    assert "state='published'" in sql and "FOR UPDATE" not in sql
    assert (cid, schema) == (UUID(pinned.candidate_id), pinned.schema_name)
    assert conn.fetch.await_count == 1
    conn.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "damage",
    [
        "manifest",
        "missing",
        "candidate_sources",
        "receipt_missing",
        "receipt_digest",
        "native",
        "owner",
        "shape",
        "keys",
        "acl",
    ],
)
def test_resolver_refuses_stale_or_absent_custody_without_fallback(monkeypatch, damage):
    conn, pinned, resolve = resolver(monkeypatch)
    if damage == "manifest":
        resolve.return_value = replace(pinned, manifest_sha256="b" * 64)
    if damage == "missing":
        conn.fetchrow.return_value = None
    if damage == "candidate_sources":
        conn.fetchrow.return_value["source_generations"] = {}
    if damage == "receipt_missing":
        conn.fetchrow.return_value["validation_json"] = {}
    if damage == "receipt_digest":
        conn.fetchrow.return_value["validation_json"]["em_provider_inputs"]["receipt_sha256"] = "0" * 64
    rows = catalog()
    if damage == "native":
        rows[0]["relfilenode"] = 900
    if damage == "owner":
        rows[0]["owner_oid"] = 900
    if damage == "shape":
        columns = json.loads(rows[0]["columns_json"])
        columns[0]["attnotnull"] = True
        rows[0]["columns_json"] = json.dumps(columns)
    if damage == "keys":
        rows[0]["keys_json"] = json.dumps([dict(index_oid=900, columns=["npi"])])
    conn.fetch.return_value = rows
    if damage == "acl":
        conn.fetchval.side_effect = lambda sql, *args: "repeatable read" if sql.startswith("SHOW") else False
    with pytest.raises(inputs.PlanPricingProjectionUnavailable, match="unavailable"):
        asyncio.run(inputs.resolve_em_provider_inputs(conn, pinned))
    conn.execute.assert_not_awaited()


def test_cancellation_remains_cancellation(monkeypatch):
    conn, pinned, resolve = resolver(monkeypatch)
    resolve.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(inputs.resolve_em_provider_inputs(conn, pinned))


@pytest.mark.parametrize("field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"])
def test_record_actual_control_helper_enforces_all_candidate_scope_fields(field):
    receipt = prepare()
    conn = connection()
    requested = target()
    saved = candidate(receipt)
    saved.update(
        {
            name: getattr(requested, name)
            for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        }
    )
    saved[field] = "wrong_schema" if field == "schema_name" else str(UUID(int=99))
    conn.fetchrow.return_value = saved
    with pytest.raises(ValueError):
        asyncio.run(inputs.record_em_provider_inputs(conn, requested, receipt, "control"))
    sql, identity = conn.fetchrow.await_args.args
    assert "FOR UPDATE" in sql and identity == UUID(requested.candidate_id)
    conn.fetchrow.assert_awaited_once()
    conn.execute.assert_not_awaited()


def test_actual_control_lock_accepts_exact_scope_without_mutating_generations():
    receipt = prepare()
    conn = connection()
    requested = target()
    saved = candidate(receipt)
    saved.update(
        {
            name: getattr(requested, name)
            for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        }
    )
    generations = copy.deepcopy(saved["source_generations"])
    conn.fetchrow.return_value = saved
    assert asyncio.run(inputs.record_em_provider_inputs(conn, requested, receipt, "control")) == receipt
    conn.fetchrow.assert_awaited_once()
    conn.execute.assert_awaited_once()
    assert saved["source_generations"] == generations


def test_resolver_refuses_manifest_generation_digest_even_with_valid_receipt(monkeypatch):
    conn, pinned, resolve = resolver(monkeypatch)
    wrong = replace(pinned, source_generations={"em_provider_inputs": "0" * 64})
    resolve.return_value = wrong
    conn.fetchrow.return_value["source_generations"] = wrong.source_generations
    with pytest.raises(inputs.PlanPricingProjectionUnavailable):
        asyncio.run(inputs.resolve_em_provider_inputs(conn, wrong))
    conn.fetch.assert_not_awaited()
    conn.execute.assert_not_awaited()
