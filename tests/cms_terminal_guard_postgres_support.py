# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native CMS guard layout and routine snapshots shared by the typed-guard PostgreSQL tests."""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest_asyncio

from db import migration_cms_replay_resolution as replay
from db import migration_cms_retirement_resolution as retirement
from db import migration_cms_snapshot_allocator as finalization
from db import migration_cms_terminal_resolution as terminal
from db.migration_generic_source_candidates import (
    upgrade_generic_source_canonical_copy_guard,
)
from db.plan_release_storage import storage_identifier
from tests.cms_native_layout_postgres_support import _reader
from tests.cms_native_layout_postgres_support import native_layout as core_layout
from tests.test_provider_dataset_candidates_postgres import _prepare_copy_guard_catalog

_NAMES = ("guard_pd_generic_dataset_finalization", replay.GUARD)
_ALL_NAMES = (*_NAMES, *retirement.ORIGINAL_SHA256, terminal.GUARD, "guard_pd_dataset_candidate_parent")
_PREPARATION_SCHEMA = "mrf"
_ROUTINES_SQL = """SELECT p.oid::bigint,jsonb_build_object('definition',to_jsonb(p),'language',to_jsonb(l),
 'namespace',jsonb_build_array(n.oid::bigint,n.nspname,n.nspowner::bigint,n.nspacl::text),
 'body',pg_catalog.pg_get_functiondef(p.oid),
 'dependencies',COALESCE((SELECT jsonb_agg(to_jsonb(d) ORDER BY d.refclassid,d.refobjid,d.refobjsubid,d.deptype)
 FROM pg_catalog.pg_depend d WHERE d.classid='pg_catalog.pg_proc'::regclass AND d.objid=p.oid),'[]'::jsonb))::text
 AS native FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
 JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE p.oid=ANY($1::oid[]) ORDER BY p.oid"""


async def _generic_copy_baseline(admin, roles):
    """Reuse the existing native reader fixture and checked V0-to-V1 installer."""
    await admin.execute(f"GRANT USAGE ON SCHEMA mrf TO {storage_identifier(roles['owner'])}")
    await _prepare_copy_guard_catalog(SimpleNamespace(connection=admin, schema='"mrf"'), roles)
    async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, _driver):
        connection = await session.connection()
        await connection.run_sync(
            lambda sync: upgrade_generic_source_canonical_copy_guard(
                sync, "mrf", preparation_schema=_PREPARATION_SCHEMA
            )
        )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def native_layout(core_layout):
    """Add only this module's established reader prerequisites in its owned database."""
    admin = core_layout
    signature = "mrf.guard_pd_dataset_candidate_parent()"
    original = await admin.fetchrow(
        "SELECT pg_catalog.pg_get_functiondef(p.oid) AS definition,pg_catalog.pg_get_userbyid(p.proowner) AS owner,"
        "pg_catalog.to_jsonb(p)::text AS identity FROM pg_catalog.pg_proc p WHERE p.oid=$1::regprocedure",
        signature,
    )
    roles_by_kind = {kind: "terminal_reader_" + uuid4().hex for kind in ("owner", "catalog")}
    roles_by_kind["leak"] = roles_by_kind["owner"]
    assert await admin.fetchval("SELECT pg_catalog.to_regclass('mrf.preparation') IS NULL")
    try:
        for role in (roles_by_kind["owner"], roles_by_kind["catalog"]):
            assert await admin.fetchval("SELECT pg_catalog.to_regrole($1) IS NULL", role)
            await admin.execute("CREATE ROLE " + storage_identifier(role) + " NOLOGIN")
        await _generic_copy_baseline(admin, roles_by_kind)
        yield admin
    finally:
        await admin.execute(f"ALTER FUNCTION {signature} OWNER TO {storage_identifier(original['owner'])}")
        await admin.execute(original["definition"])
        assert (
            await admin.fetchval(
                "SELECT pg_catalog.to_jsonb(p)::text FROM pg_catalog.pg_proc p WHERE p.oid=$1::regprocedure", signature
            )
            == original["identity"]
        )
        if await admin.fetchval("SELECT pg_catalog.to_regclass('mrf.preparation') IS NOT NULL"):
            owner = await admin.fetchval(
                "SELECT relowner::regrole::text FROM pg_catalog.pg_class WHERE oid='mrf.preparation'::regclass"
            )
            assert owner == roles_by_kind["catalog"]
            await admin.execute("DROP TABLE mrf.preparation")
        for role in (roles_by_kind["owner"], roles_by_kind["catalog"]):
            if await admin.fetchval("SELECT pg_catalog.to_regrole($1) IS NOT NULL", role):
                await admin.execute("DROP OWNED BY " + storage_identifier(role))
                await admin.execute("DROP ROLE " + storage_identifier(role))
                assert await admin.fetchval("SELECT pg_catalog.to_regrole($1) IS NULL", role)


async def _routines(connection):
    oids = await connection.fetchval(
        "SELECT array_agg(oid ORDER BY oid) FROM pg_catalog.pg_proc "
        "WHERE pronamespace='mrf'::regnamespace AND proname=ANY($1::text[])",
        list(_ALL_NAMES),
    )
    rows = await connection.fetch(_ROUTINES_SQL, oids)
    return [dict(oid=row["oid"], native=json.loads(row["native"])) for row in rows]


async def _hooks(connection):
    return await connection.fetchval(
        "SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid)::text FROM pg_catalog.pg_trigger t "
        "JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid "
        "WHERE p.pronamespace='mrf'::regnamespace AND p.proname=ANY($1::text[])",
        list(_ALL_NAMES),
    )


async def _upgrade_guards(session, before, versions, retirement_typed, *, terminal_typed=True, cms_parent=True):
    connection = await session.connection()
    for name, typed in zip(_NAMES, versions, strict=True):
        if typed:
            original = next(
                routine["native"]["definition"]
                for routine in before
                if routine["native"]["definition"]["proname"] == name
            )
            expected_by_field = {"oid": int(original["oid"]), "owner_oid": int(original["proowner"])}
            upgrade = (
                finalization.upgrade_cms_generic_finalization_guard
                if name == _NAMES[0]
                else replay.upgrade_cms_replay_guard_resolution
            )
            await connection.run_sync(lambda sync: upgrade(sync, "mrf", expected=expected_by_field))
    if retirement_typed:
        pins_by_name = {
            routine["native"]["definition"]["proname"]: {
                "oid": routine["oid"],
                "owner_oid": int(routine["native"]["definition"]["proowner"]),
            }
            for routine in before
            if routine["native"]["definition"]["proname"] in retirement.ORIGINAL_SHA256
        }
        await connection.run_sync(
            lambda sync: retirement.upgrade_cms_retirement_resolution(sync, "mrf", expected=pins_by_name)
        )
    if terminal_typed:
        original = next(routine for routine in before if routine["native"]["definition"]["proname"] == terminal.GUARD)
        terminal_pin_by_field = dict(oid=original["oid"], owner_oid=int(original["native"]["definition"]["proowner"]))
        await connection.run_sync(
            lambda sync: terminal.upgrade_cms_terminal_guard_resolution(sync, "mrf", expected=terminal_pin_by_field)
        )
    if cms_parent:
        await connection.run_sync(
            lambda sync: upgrade_generic_source_canonical_copy_guard(
                sync, "mrf", preparation_schema=_PREPARATION_SCHEMA, include_cms=True
            )
        )


def _assert_properties(before, routines):
    previous_by_oid = {routine["oid"]: routine["native"] for routine in before}
    for routine in routines:
        original = previous_by_oid[routine["oid"]]
        assert {key: value for key, value in routine["native"].items() if key not in {"definition", "body"}} == {
            key: value for key, value in original.items() if key not in {"definition", "body"}
        }
        assert {key: value for key, value in routine["native"]["definition"].items() if key != "prosrc"} == {
            key: value for key, value in original["definition"].items() if key != "prosrc"
        }
