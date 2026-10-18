# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Historical-body proof and global mutator trigger attachment boundaries."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import nppes_evidence_callable_contract as contract


def _rows(schema="source_schema"):
    return [
        dict(
            oid=index + 100,
            schema=schema,
            name=name,
            owner=500,
            owner_role="source_owner",
            owner_protected=True,
            language="plpgsql",
            prokind="f",
            prorettype=2279,
            pronargs=0,
            argument_types=[],
            proargnames=None,
            proargmodes=None,
            proallargtypes=None,
            proargdefaults=None,
            pronargdefaults=0,
            provariadic=0,
            prosecdef=True,
            proconfig=["search_path=pg_catalog"],
            prosrc=body,
            proleakproof=False,
            proisstrict=False,
            proretset=False,
            provolatile="v",
            proparallel="u",
            support=0,
            probin=None,
            prosqlbody=None,
            protrftypes=None,
            procost=100,
            prorows=0,
            acl=[[500, 500, "EXECUTE", False]],
            execute_closed=True,
        )
        for index, (name, body) in enumerate(contract.expected_nppes_callable_bodies(schema).items())
    ]


def _links(catalog_rows, archive=True):
    identities_by_key = {catalog_row["name"]: catalog_row["oid"] for catalog_row in catalog_rows}
    links = []
    for name, table, trigger in (
        (
            contract.ARCHIVE,
            "public_evidence_nppes_registry_admission",
            "public_evidence_nppes_registry_admission_integrity_guard",
        ),
        (
            contract.CHAIN,
            "public_evidence_nppes_registry_chain_admission",
            "public_evidence_nppes_registry_chain_integrity_guard",
        ),
    ):
        if name == contract.ARCHIVE and not archive:
            continue
        links.append(
            dict(
                function_oid=identities_by_key[name],
                schema="source_schema",
                table=table,
                name=trigger,
                tgtype=5,
                tgenabled="A",
                tgdeferrable=True,
                tginitdeferred=True,
                tgisinternal=False,
                tgparentid=0,
                tgqual=None,
                tgnargs=0,
                tgargs=b"",
                columns=[],
                tgnewtable=None,
                tgoldtable=None,
            )
        )
    return links


def _connection(rows=None, links=None):
    rows = _rows() if rows is None else rows
    connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=[rows, _links(rows) if links is None else links]),
        execute=AsyncMock(side_effect=AssertionError("capture must never execute DDL")),
        is_in_transaction=lambda: True,
    )
    return connection


def test_renderers_produce_exact_lifecycle_bodies_without_executing_migrations(monkeypatch):
    from alembic import op

    execute = AsyncMock(side_effect=AssertionError("real migration op was invoked"))
    monkeypatch.setattr(op, "execute", execute)
    contract.expected_nppes_callable_bodies.cache_clear()
    bodies = contract.expected_nppes_callable_bodies("source_schema")
    assert len(bodies) == 9 and all(len(name.encode()) <= 63 for name in bodies)
    assert "guard_public_evidence_nppes_registry_chain_archive_admission_ap" in bodies
    assert "npi_deactivation_date <=" in bodies[contract.ARCHIVE]
    assert "npi_reactivation_date <" in bodies[contract.ARCHIVE]
    assert "INSERT INTO" in bodies[contract.ARCHIVE] and "INSERT INTO" in bodies[contract.CHAIN]
    assert all(
        "INSERT INTO" not in body for name, body in bodies.items() if name not in {contract.ARCHIVE, contract.CHAIN}
    )
    execute.assert_not_called()


def test_missing_or_ambiguous_lifecycle_replacement_fails_closed(monkeypatch):
    original = contract._migration

    def migration(key):
        module, statements = original(key)
        if key == "lifecycle":
            module._UPGRADE_REPLACEMENTS = (("missing historical fragment", "replacement"),)
        return module, statements

    contract.expected_nppes_callable_bodies.cache_clear()
    monkeypatch.setattr(contract, "_migration", migration)
    with pytest.raises(ValueError, match="nppes_callable_contract_changed"):
        contract.expected_nppes_callable_bodies("source_schema")
    contract.expected_nppes_callable_bodies.cache_clear()


@pytest.mark.asyncio
@pytest.mark.parametrize("archive_present", [False, True])
async def test_exact_capture_separates_readonly_capabilities_from_mutators(archive_present):
    rows = _rows()
    connection = _connection(rows, _links(rows, archive_present))
    readonly, mutators = await contract.capture_nppes_callable_contract(
        connection, "source_schema", archive_guard_present=archive_present
    )
    assert len(readonly) == 7 and {record["name"] for record in mutators} == {contract.ARCHIVE, contract.CHAIN}
    assert not set(readonly) & {record["oid"] for record in mutators}
    assert all(record["signature"] == '"source_schema"."' + record["name"] + '"()' for record in mutators)
    assert all(record["owner_protected"] and record["execute_closed"] for record in mutators)
    assert connection.fetch.await_args_list[1].args[0] == contract._LINK_SQL
    assert "n.nspname=$" not in contract._LINK_SQL
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("prosrc", "BEGIN RETURN NULL; END;"),
        ("prosecdef", False),
        ("proconfig", None),
        ("proconfig", ["search_path=public"]),
        ("pronargs", 1),
        ("argument_types", [25]),
        ("language", "sql"),
        ("prorettype", 2278),
        ("prokind", "p"),
        ("proretset", True),
        ("proleakproof", True),
        ("proisstrict", True),
        ("proargdefaults", "constant"),
        ("support", 123),
        ("schema", "other_schema"),
        ("proparallel", "s"),
        ("procost", 1),
        ("prorows", 1),
    ],
)
async def test_altered_body_overload_or_native_execution_attributes_are_rejected(field, value):
    rows = _rows()
    rows[0][field] = value
    connection = _connection(rows)
    with pytest.raises(ValueError, match="nppes_callable_contract_changed"):
        await contract.capture_nppes_callable_contract(connection, "source_schema", archive_guard_present=True)
    assert connection.fetch.await_count == 1


@pytest.mark.asyncio
async def test_an_extra_overload_cannot_be_adopted_by_name():
    rows = _rows()
    rows.append(dict(rows[-1], oid=999, pronargs=1, argument_types=[25]))
    with pytest.raises(ValueError):
        await contract.capture_nppes_callable_contract(_connection(rows), "source_schema", archive_guard_present=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", "other_schema"),
        ("table", "mutable_table"),
        ("name", "another_trigger"),
        ("tgqual", "false"),
        ("tgtype", 4),
        ("tgenabled", "D"),
        ("tgdeferrable", False),
        ("tginitdeferred", False),
        ("tgisinternal", True),
        ("tgparentid", 10),
        ("tgargs", b"argument"),
        ("tgnargs", 1),
        ("columns", [1]),
        ("tgnewtable", "inserted_rows"),
    ],
)
async def test_mutator_may_only_attach_to_its_fixed_global_trigger(field, value):
    rows = _rows()
    links = _links(rows)
    links[0][field] = value
    with pytest.raises(ValueError):
        await contract.capture_nppes_callable_contract(
            _connection(rows, links), "source_schema", archive_guard_present=True
        )


@pytest.mark.asyncio
async def test_revoked_execute_does_not_excuse_an_extra_mutator_attachment():
    rows = _rows()
    links = _links(rows, False)
    links.append(dict(links[0], schema="other_schema", table="mutable_table", name="unapproved_trigger"))
    with pytest.raises(ValueError):
        await contract.capture_nppes_callable_contract(
            _connection(rows, links), "source_schema", archive_guard_present=False
        )


@pytest.mark.asyncio
async def test_legacy_mutators_are_returned_as_exact_adoptable_records():
    rows = _rows()
    for row in rows:
        if row["name"] in {contract.ARCHIVE, contract.CHAIN}:
            row.update(
                owner=600,
                owner_role="legacy_owner",
                owner_protected=False,
                execute_closed=False,
                acl=[[0, 600, "EXECUTE", False]],
            )
    readonly, mutators = await contract.capture_nppes_callable_contract(
        _connection(rows), "source_schema", archive_guard_present=True
    )
    assert len(readonly) == 7
    assert all(not record["execute_closed"] and not record["owner_protected"] for record in mutators)
    assert all(record["acl"] == ((0, 600, "EXECUTE", False),) for record in mutators)


@pytest.mark.asyncio
async def test_absent_transaction_and_caller_chosen_identifiers_are_refused_before_io():
    connection = _connection()
    connection.is_in_transaction = lambda: False
    with pytest.raises(ValueError):
        await contract.capture_nppes_callable_contract(connection, "source_schema", archive_guard_present=False)
    connection.is_in_transaction = lambda: True
    with pytest.raises(ValueError):
        await contract.capture_nppes_callable_contract(connection, "source_schema;SELECT", archive_guard_present=False)
    connection.fetch.assert_not_awaited()
