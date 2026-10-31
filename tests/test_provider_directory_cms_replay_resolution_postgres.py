# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real replay guard/validators, valid subset evidence and ordinary-role overloads."""

import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db import migration_cms_replay_resolution as migration
from db.plan_release_storage import storage_identifier, storage_relation
from tests.provider_directory_subset_completion_pg_support import terminal_metadata, valid_evidence_pairs
from tests.test_provider_directory_cms_canonical_layout_postgres import native_layout as native_layout
from tests.test_provider_directory_cms_legacy_history_postgres import _reader

pytestmark = pytest.mark.asyncio(loop_scope="module")
_GUARD = storage_relation("mrf", migration.GUARD) + "()"
_FUNCTIONS = (
    ("provider_directory_subset_replay_evidence_shape_valid", "jsonb,text,jsonb,text"),
    ("provider_directory_subset_coverage_shape_valid", "jsonb,jsonb,text,text"),
)


async def _snapshot(driver):
    snapshot_by_field = dict(
        await driver.fetchrow(
            "SELECT p.oid::bigint,p.proowner::bigint AS owner_oid,p.prosrc,pg_catalog.pg_get_functiondef(p.oid) AS definition,"
            "pg_catalog.to_jsonb(p)-'prosrc' AS identity,pg_catalog.obj_description(p.oid,'pg_proc') AS description,"
            "(SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_trigger t "
            "WHERE t.tgfoid=p.oid) AS hooks FROM pg_catalog.pg_proc p WHERE p.oid=$1::regprocedure",
            _GUARD,
        )
    )
    for key in ("identity", "hooks"):
        if isinstance(snapshot_by_field[key], str):
            snapshot_by_field[key] = json.loads(snapshot_by_field[key])
    return snapshot_by_field


async def _upgrade(session, expected):
    connection = await session.connection()
    await connection.run_sync(
        lambda sync: migration.upgrade_cms_replay_guard_resolution(sync, "mrf", expected=expected)
    )


@asynccontextmanager
async def _overloads(admin, kind):
    role = "replay_resolution_" + uuid4().hex
    has_role, attacker, signatures = False, None, []
    keys = [int(uuid4().hex[:15], 16), int(uuid4().hex[:15], 16)]
    try:
        await admin.execute(f"CREATE ROLE {storage_identifier(role)} LOGIN")
        has_role = True
        await admin.execute(f"GRANT USAGE,CREATE ON SCHEMA mrf TO {storage_identifier(role)}")
        url = make_url(os.environ["HLTHPRT_CMS_LAYOUT_TEST_DSN"]).set(
            drivername="postgresql", username=role, database=await admin.fetchval("SELECT current_database()")
        )
        attacker = await asyncpg.connect(url.render_as_string(hide_password=False))
        assert await attacker.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user") is False
        for index, (name, arguments) in enumerate(_FUNCTIONS):
            if kind == "replay-default" and index == 1 or kind == "coverage-default" and index == 0:
                continue
            if kind == "varchar":
                kinds = arguments.split(",")
                kinds[3 if index == 0 else 2] = "varchar"
                arguments = ",".join(kinds)
                declaration = arguments
            else:
                declaration = arguments + ",extra boolean DEFAULT false"
                arguments += ",boolean"
            function = storage_relation("mrf", name)
            await attacker.execute(
                f"CREATE FUNCTION {function}({declaration}) RETURNS boolean LANGUAGE plpgsql AS $attack$ "
                f"BEGIN PERFORM pg_catalog.pg_advisory_lock({keys[index]}::bigint); RETURN true; END $attack$"
            )
            signatures.append(function + "(" + arguments + ")")
        yield keys
    finally:
        if attacker is not None:
            await attacker.close()
        if has_role:
            for signature in reversed(signatures):
                await admin.execute("DROP FUNCTION " + signature)
            await admin.execute(f"REVOKE USAGE,CREATE ON SCHEMA mrf FROM {storage_identifier(role)}")
            await admin.execute("DROP ROLE " + storage_identifier(role))
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)


async def _attacker_locks(driver, keys):
    """Session locks survive rollback, so a refused write cannot hide attacker execution."""
    return [
        key
        for key in keys
        if await driver.fetchval(
            "SELECT EXISTS(SELECT FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid() "
            "AND locktype='advisory' AND granted AND classid=($1::bigint>>32)::oid "
            "AND objid=($1::bigint&4294967295)::oid AND objsubid=1)",
            key,
        )
    ]


async def _header(driver, *, valid):
    proof, proof_sha, replay, replay_sha = valid_evidence_pairs()
    metadata = terminal_metadata(proof, proof_sha, replay, replay_sha, "run_" + "a" * 32)
    if not valid:
        metadata["server_issued_subset_coverage"] = {}
    await driver.execute(
        "CREATE TEMP TABLE cms_replay_header(completion_proof_required_version integer,status varchar(32),"
        "publication_metadata_json jsonb,completion_proof_json jsonb,completion_proof_sha256 varchar(64));"
        f"CREATE TRIGGER replay_guard BEFORE UPDATE ON cms_replay_header FOR EACH ROW EXECUTE FUNCTION {_GUARD}"
    )
    await driver.execute(
        "INSERT INTO cms_replay_header VALUES(3,'acquiring',$1::jsonb,$2::jsonb,$3)",
        json.dumps(metadata),
        json.dumps(proof),
        proof_sha,
    )


async def _probe_header(driver, keys, *, typed, attack):
    await _header(driver, valid=True)
    try:
        await driver.execute("SAVEPOINT guard_probe")
        if attack in {"replay-default", "coverage-default"}:
            with pytest.raises(asyncpg.AmbiguousFunctionError):
                await driver.execute("UPDATE cms_replay_header SET status='validated'")
            await driver.execute("ROLLBACK TO SAVEPOINT guard_probe")
            assert await driver.fetchval("SELECT status FROM cms_replay_header") == "acquiring"
        else:
            await driver.execute("UPDATE cms_replay_header SET status='validated'")
            assert await driver.fetchval("SELECT status FROM cms_replay_header") == "validated"
        expected_keys = keys if not typed and attack == "varchar" else []
        assert await _attacker_locks(driver, keys) == expected_keys
    finally:
        for key in keys:
            await driver.fetchval("SELECT pg_catalog.pg_advisory_unlock($1::bigint)", key)


@pytest.mark.parametrize(
    "typed,attack",
    [
        (False, "varchar"),
        (True, "varchar"),
        (True, "replay-default"),
        (True, "coverage-default"),
        (False, None),
        (True, None),
    ],
)
async def test_real_guard_dispatches_only_exact_typed_validators(native_layout, typed, attack):
    admin = native_layout
    original = await _snapshot(admin)
    try:
        async with AsyncExitStack() as scopes:
            keys = await scopes.enter_async_context(_overloads(admin, attack)) if attack else []
            async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
                if typed:
                    await _upgrade(session, {key: original[key] for key in ("oid", "owner_oid")})
                    after = await _snapshot(driver)
                    assert after["prosrc"] == migration.replay_guard_bodies("mrf")[1]
                    assert all(after[key] == original[key] for key in ("identity", "hooks", "description"))
                    await _upgrade(session, {key: original[key] for key in ("oid", "owner_oid")})
                    assert await _snapshot(driver) == after
                await _probe_header(driver, keys, typed=typed, attack=attack)
    finally:
        await admin.execute(original["definition"])
        assert await _snapshot(admin) == original


@pytest.mark.parametrize("typed", [False, True])
async def test_invalid_coverage_remains_rejected_without_hidden_attacker_execution(native_layout, typed):
    admin = native_layout
    original = await _snapshot(admin)
    try:
        async with _reader(admin, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
            if typed:
                await _upgrade(session, {key: original[key] for key in ("oid", "owner_oid")})
            await _header(driver, valid=False)
            await driver.execute("SAVEPOINT invalid_coverage")
            with pytest.raises(asyncpg.CheckViolationError):
                await driver.execute("UPDATE cms_replay_header SET status='validated'")
            await driver.execute("ROLLBACK TO SAVEPOINT invalid_coverage")
            assert await driver.fetchval("SELECT status FROM cms_replay_header") == "acquiring"
    finally:
        await admin.execute(original["definition"])
        assert await _snapshot(admin) == original


@pytest.mark.parametrize("field", ["oid", "owner_oid"])
async def test_changed_expected_identity_refuses_before_replacement(native_layout, field):
    async with _reader(native_layout, readonly=False, isolation="READ COMMITTED") as (_, session, driver):
        before = await _snapshot(driver)
        expected_by_field = {key: before[key] for key in ("oid", "owner_oid")}
        expected_by_field[field] += 1
        with pytest.raises(RuntimeError, match="function_changed"):
            await _upgrade(session, expected_by_field)
        assert await _snapshot(driver) == before
