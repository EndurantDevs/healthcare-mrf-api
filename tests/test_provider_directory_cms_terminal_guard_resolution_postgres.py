# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native terminal guard prerequisite; no complete writer or creator-safety claim."""

from contextlib import nullcontext
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from db import migration_cms_replay_resolution as replay
from db import migration_cms_retirement_resolution as retirement
from db import migration_cms_snapshot_allocator as finalization
from db import migration_cms_terminal_resolution as terminal
from db.migration_generic_source_candidates import (
    upgrade_generic_source_canonical_copy_guard,
    verify_generic_source_canonical_copy_guard,
)
from db.plan_release_storage import storage_identifier
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_preparation_custody as custody
from tests.test_provider_dataset_candidates_postgres import _prepare_copy_guard_catalog
from tests.test_provider_directory_cms_canonical_layout_postgres import native_layout as core_layout
from tests.test_provider_directory_cms_legacy_history_postgres import _reader

pytestmark = pytest.mark.asyncio(loop_scope="module")
_NAMES = ("guard_pd_generic_dataset_finalization", replay.GUARD)
_ALL_NAMES = (*_NAMES, *retirement.ORIGINAL_SHA256, terminal.GUARD, "guard_pd_dataset_candidate_parent")
_PREPARATION_SCHEMA = "mrf"


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
    async with admin.transaction(readonly=True):
        await verify_generic_source_canonical_copy_guard(admin, "mrf", preparation_schema=_PREPARATION_SCHEMA)


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
    return [
        dict(oid=routine["oid"], native=custody._decoded(routine["native"]))
        for routine in await connection.fetch(custody._ROUTINES_SQL, [], oids)
    ]


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


@pytest.mark.parametrize("generic_typed,replay_typed", [(False, False), (True, False), (False, True), (True, True)])
@pytest.mark.parametrize("retirement_typed", [False, True])
@pytest.mark.parametrize("terminal_typed", [False, True])
async def test_native_requires_hardened_frontier(
    native_layout, generic_typed, replay_typed, retirement_typed, terminal_typed
):
    """Old/mixed states refuse; checked replacements preserve properties and hooks."""
    admin = native_layout
    before, hooks = await _routines(admin), await _hooks(admin)
    try:
        async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
            namespace = await driver.fetchval("SELECT 'mrf'::regnamespace::oid::bigint")
            await _upgrade_guards(
                session, before, (generic_typed, replay_typed), retirement_typed, terminal_typed=terminal_typed
            )
            routines = await _routines(driver)
            expectation = (
                nullcontext()
                if generic_typed and replay_typed and retirement_typed and terminal_typed
                else pytest.raises(ValueError, match="terminal_guard_resolution_required")
            )
            with expectation:
                callables.require_cms_terminal_guard_resolution(
                    routines,
                    catalog_schema="mrf",
                    catalog_schema_oid=namespace,
                    preparation_schema=_PREPARATION_SCHEMA,
                )
            assert await _hooks(driver) == hooks
            _assert_properties(before, routines)
    finally:
        for routine in before:
            await admin.execute(routine["native"]["body"])
        assert await _routines(admin) == before and await _hooks(admin) == hooks


async def test_native_generic_parent_refuses_the_cms_terminal(native_layout):
    """Typed legacy guards alone do not establish the CMS statement guard."""
    admin = native_layout
    before, hooks = await _routines(admin), await _hooks(admin)
    try:
        async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
            await _upgrade_guards(session, before, (True, True), True, cms_parent=False)
            routines = await _routines(driver)
            with pytest.raises(ValueError, match="terminal_guard_resolution_required"):
                callables.require_cms_terminal_guard_resolution(
                    routines,
                    catalog_schema="mrf",
                    catalog_schema_oid=await driver.fetchval("SELECT 'mrf'::regnamespace::oid::bigint"),
                    preparation_schema=_PREPARATION_SCHEMA,
                )
            _assert_properties(before, routines)
            assert await _hooks(driver) == hooks
    finally:
        for routine in before:
            await admin.execute(routine["native"]["body"])
        assert await _routines(admin) == before and await _hooks(admin) == hooks


@pytest.mark.parametrize(
    "name",
    [
        "provider_directory_terminal_root_retirement_valid",
        "provider_directory_terminal_root_retirement_v2_valid",
        "guard_provider_directory_terminal_root_retirement_run",
    ],
)
async def test_native_requires_every_typed_retirement_body(native_layout, name):
    """The remaining legacy proof/run dispatchers cannot pass a mixed frontier."""
    admin = native_layout
    before, hooks = await _routines(admin), await _hooks(admin)
    try:
        async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
            await _upgrade_guards(session, before, (True, True), True)
            original = next(routine for routine in before if routine["native"]["definition"]["proname"] == name)
            await driver.execute(original["native"]["body"])
            routines = await _routines(driver)
            with pytest.raises(ValueError, match="terminal_guard_resolution_required"):
                callables.require_cms_terminal_guard_resolution(
                    routines,
                    catalog_schema="mrf",
                    catalog_schema_oid=await driver.fetchval("SELECT 'mrf'::regnamespace::oid::bigint"),
                    preparation_schema=_PREPARATION_SCHEMA,
                )
            _assert_properties(before, routines)
            assert await _hooks(driver) == hooks
    finally:
        for routine in before:
            await admin.execute(routine["native"]["body"])
        assert await _routines(admin) == before and await _hooks(admin) == hooks
