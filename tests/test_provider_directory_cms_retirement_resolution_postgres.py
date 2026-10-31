# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real retirement helpers under hostile ordinary-role overloads, never a live cut."""

import asyncio
import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url

from db import migration_cms_retirement_resolution as migration
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_callable_custody as callables
from tests.provider_directory_terminal_root_retirement_pg_support import TARGET_DATASET_ID, retirement_postgres
from tests.provider_directory_terminal_root_retirement_v2_pg_support import load_v2_migration, run_v2_migration
from tests.tin_npi_connector_postgres_support import POSTGRES_DSN_ENV

pytestmark = pytest.mark.asyncio(loop_scope="module")
_CASES = (
    ("evidence", 1),
    ("evidence", 2),
    ("valid", 1),
    ("valid", 2),
    ("parent", 1),
    ("parent", 2),
    ("run", 1),
)


async def _retire(scenario, version):
    driver, schema = scenario.connection, scenario.schema
    functions = migration.guards if version == 1 else migration.v2
    if version == 2:
        await driver.execute(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET publication_metadata_json="
            'publication_metadata_json||\'{"resource_hash_contract":"semantic_content_v4"}\'::jsonb WHERE dataset_id=$1',
            TARGET_DATASET_ID,
        )
    proof = await driver.fetchval(
        "SELECT " + storage_relation(scenario.schema_name, functions.EVIDENCE_FUNCTION) + "($1::text)",
        TARGET_DATASET_ID,
    )
    marker_by_field = dict(
        contract_version=functions.CONTRACT,
        reason_code=migration.guards.REASON,
        evidence=json.loads(proof),
        minimum_terminal_age_seconds=900,
        retired_at=await driver.fetchval(
            "SELECT pg_catalog.to_char(pg_catalog.transaction_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')"
        ),
    )
    await driver.execute(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET status='acquisition_retired',"
        "publication_metadata_json=publication_metadata_json||pg_catalog.jsonb_build_object($2::text,$3::jsonb) WHERE dataset_id=$1",
        TARGET_DATASET_ID,
        migration.evidence.MARKER if version == 1 else migration.v2.MARKER,
        json.dumps(marker_by_field),
    )
    assert (
        await driver.fetchval(
            "SELECT " + storage_relation(scenario.schema_name, functions.VALID_FUNCTION) + "($1::text)",
            TARGET_DATASET_ID,
        )
        is True
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def retirement_corpus():
    async with AsyncExitStack() as scopes:
        scenarios_by_state = {}
        for state in (0, 1, 2):
            patches = scopes.enter_context(pytest.MonkeyPatch.context())
            scenario = await scopes.enter_async_context(retirement_postgres(patches, current_runtime=False))
            await run_v2_migration(scenario, load_v2_migration(), "upgrade")
            for statement in migration._original_statements(scenario.schema_name):
                await scenario.connection.execute(statement.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
            if state:
                await _retire(scenario, state)
            scenarios_by_state[state] = scenario
        yield scenarios_by_state


async def _snapshots(driver, schema):
    rows = await driver.fetch(
        "SELECT p.proname,pg_catalog.to_jsonb(p)-'prosrc' AS identity,p.prosrc,"
        "pg_catalog.pg_get_functiondef(p.oid) AS definition,pg_catalog.obj_description(p.oid,'pg_proc') AS description,"
        "(SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) ORDER BY t.oid) FROM pg_catalog.pg_trigger t WHERE t.tgfoid=p.oid) AS hooks,"
        "(SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(d) ORDER BY d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype) "
        "FROM pg_catalog.pg_depend d WHERE (d.classid='pg_catalog.pg_proc'::regclass AND d.objid=p.oid) "
        "OR (d.refclassid='pg_catalog.pg_proc'::regclass AND d.refobjid=p.oid)) AS dependencies "
        "FROM pg_catalog.pg_proc p WHERE p.pronamespace=pg_catalog.to_regnamespace($1) AND p.proname=ANY($2::text[]) ORDER BY p.oid",
        schema,
        list(migration.ORIGINAL_SHA256),
    )
    assert len(rows) == 6
    return {row["proname"]: dict(row) for row in rows}


async def _harden(driver, schema, before):
    pairs_by_name = migration.retirement_resolution_bodies(schema)
    for name, (old, new) in pairs_by_name.items():
        original = before[name]
        assert original["prosrc"] == old and original["definition"].count(old) == 1
        await driver.execute(original["definition"].replace(old, new, 1))
    after = await _snapshots(driver, schema)
    for name, original in before.items():
        assert after[name]["prosrc"] == pairs_by_name[name][1]
        assert all(
            after[name][field] == original[field] for field in ("identity", "hooks", "dependencies", "description")
        )
    await _check_consumers(driver, schema)


async def _check_consumers(driver, schema):
    names = [migration.evidence.EVIDENCE_FUNCTION, migration.v2.EVIDENCE_FUNCTION, migration.guards.PARENT_GUARD]
    signatures = [
        storage_relation(schema, name) + ("()" if name == migration.guards.PARENT_GUARD else "(text)") for name in names
    ]
    rows = await driver.fetch(callables._FUNCTION_SQL, signatures, schema, names, [])
    namespace = await driver.fetchval("SELECT oid::bigint FROM pg_catalog.pg_namespace WHERE nspname=$1", schema)
    specs = callables._original_callee_contracts(schema, "preparation_schema")
    assert len(rows) == len(names)
    for row in rows:
        definition = json.loads(row["definition"]) if isinstance(row["definition"], str) else row["definition"]
        callables._require_cms_callee_body(
            dict(definition=definition, language=row["language"]),
            specs[definition["proname"]],
            schema,
            namespace,
        )


@asynccontextmanager
async def _rollback(driver):
    transaction = driver.transaction()
    await transaction.start()
    try:
        yield
    finally:
        await transaction.rollback()


def _target(phase, version):
    functions = migration.guards if version == 1 else migration.v2
    if phase == "evidence":
        return migration.evidence.RELATION_EVIDENCE_FUNCTION, ("text", "text"), "jsonb"
    if phase == "valid":
        return functions.MARKER_FUNCTION, ("text", "jsonb"), "boolean"
    if phase == "parent":
        return functions.ELIGIBLE_FUNCTION, ("text", "integer"), "boolean"
    return migration.guards.RUN_RETIRED_FUNCTION, ("text",), "boolean"


@asynccontextmanager
async def _overload(scenario, phase, version, attack):
    driver, schema = scenario.connection, scenario.schema_name
    role, key = "retirement_attack_" + uuid4().hex, int(uuid4().hex[:15], 16)
    attacker, signature, has_role = None, None, False
    try:
        await driver.execute("CREATE ROLE " + storage_identifier(role) + " LOGIN")
        has_role = True
        await driver.execute(
            "GRANT USAGE,CREATE ON SCHEMA " + storage_identifier(schema) + " TO " + storage_identifier(role)
        )
        url = make_url(os.environ[POSTGRES_DSN_ENV]).set(drivername="postgresql", username=role)
        attacker = await asyncpg.connect(url.render_as_string(hide_password=False))
        assert await attacker.fetchval("SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=session_user") is False
        name, kinds, returns = _target(phase, version)
        if attack == "varchar":
            kinds = ("varchar", *kinds[1:])
            declaration = ",".join(kinds)
        elif attack == "default":
            declaration = ",".join(kinds) + ",extra boolean DEFAULT false"
            kinds = (*kinds, "boolean")
        else:
            kinds = ("varchar", kinds[1] + "[]") if len(kinds) == 2 else ("varchar[]",)
            declaration = "varchar,VARIADIC " + kinds[1] if len(kinds) == 2 else "VARIADIC varchar[]"
        signature = storage_relation(schema, name) + "(" + ",".join(kinds) + ")"
        await attacker.execute(
            "CREATE FUNCTION "
            + storage_relation(schema, name)
            + "("
            + declaration
            + ") RETURNS "
            + returns
            + " LANGUAGE plpgsql AS $attack$ BEGIN PERFORM pg_catalog.pg_advisory_lock("
            + str(key)
            + "::bigint); RAISE EXCEPTION 'retirement_attack_executed'; END $attack$"
        )
        yield key
    finally:
        if attacker is not None:
            await attacker.close()
        if has_role:
            if signature is not None:
                await driver.execute("DROP FUNCTION IF EXISTS " + signature)
            await driver.execute(
                "REVOKE USAGE,CREATE ON SCHEMA " + storage_identifier(schema) + " FROM " + storage_identifier(role)
            )
            await driver.execute("DROP ROLE " + storage_identifier(role))
            assert not await driver.fetchval("SELECT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname=$1)", role)


async def _attacked(driver, key):
    return await driver.fetchval(
        "SELECT EXISTS(SELECT FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid() "
        "AND locktype='advisory' AND granted AND classid=($1::bigint>>32)::oid "
        "AND objid=($1::bigint&4294967295)::oid AND objsubid=1)",
        key,
    )


async def _probe(scenario, phase, version):
    driver, schema = scenario.connection, scenario.schema_name
    functions = migration.guards if version == 1 else migration.v2
    if phase in {"evidence", "valid"}:
        name = functions.EVIDENCE_FUNCTION if phase == "evidence" else functions.VALID_FUNCTION
        return await driver.fetchval("SELECT " + storage_relation(schema, name) + "($1::text)", TARGET_DATASET_ID)
    if phase == "run":
        return await driver.execute(
            f"UPDATE {scenario.schema}.import_run SET summary_json=summary_json||'{{\"probe\":true}}'::jsonb WHERE run_id='run-predecessorless'"
        )
    await _retire(scenario, version)
    return "retired"


@pytest.mark.parametrize("phase,version", _CASES)
@pytest.mark.parametrize("typed", [False, True])
async def test_real_retirement_paths_preserve_results_and_native_identity(retirement_corpus, phase, version, typed):
    scenario = retirement_corpus[version if phase == "valid" else 0]
    driver = scenario.connection
    before = await _snapshots(driver, scenario.schema_name)
    async with _rollback(driver):
        await driver.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        async with _rollback(driver):
            expected = await _probe(scenario, phase, version)
        if typed:
            await _harden(driver, scenario.schema_name, before)
        assert await _probe(scenario, phase, version) == expected
    assert await _snapshots(driver, scenario.schema_name) == before


@pytest.mark.parametrize("phase,version", _CASES)
@pytest.mark.parametrize(
    "typed,attack", [(False, "varchar"), (True, "varchar"), (False, "variadic"), (True, "variadic"), (True, "default")]
)
async def test_hostile_overload_cannot_execute_through_typed_callers(retirement_corpus, phase, version, typed, attack):
    scenario = retirement_corpus[version if phase == "valid" else 0]
    driver = scenario.connection
    before = await _snapshots(driver, scenario.schema_name)
    async with _overload(scenario, phase, version, attack) as key:
        try:
            async with asyncio.timeout(3), _rollback(driver):
                await driver.execute(
                    "SET LOCAL statement_timeout='3s'; SET LOCAL lock_timeout='100ms'; SET LOCAL search_path=pg_catalog,pg_temp"
                )
                await _dispatch_probe(scenario, phase, version, before, typed=typed, attack=attack)
        finally:
            assert await _attacked(driver, key) is (not typed)
            await driver.fetchval("SELECT pg_catalog.pg_advisory_unlock($1::bigint)", key)
    assert await _snapshots(driver, scenario.schema_name) == before


async def _dispatch_probe(scenario, phase, version, before, *, typed, attack):
    driver = scenario.connection
    if attack == "default":
        # SQL-language bodies can refuse ambiguous dispatch during replacement parsing.
        with pytest.raises(asyncpg.AmbiguousFunctionError):
            await _harden(driver, scenario.schema_name, before)
            await _probe(scenario, phase, version)
        return
    if typed:
        await _harden(driver, scenario.schema_name, before)
        assert await _probe(scenario, phase, version) is not None
        return
    with pytest.raises(asyncpg.RaiseError, match="retirement_attack_executed"):
        await _probe(scenario, phase, version)
