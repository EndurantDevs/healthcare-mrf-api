# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standalone retirement recognizes migrated storage without weakening its guards."""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

from db.migration_provider_directory_dataset_candidates import (
    TABLES,
    _convert_relation,
    _deny_direct_writes,
    candidate_functions,
    grant_candidate_api,
)
from process.provider_directory_terminal_root_retirement_contract import TerminalRootRetirementError
from process.provider_directory_terminal_root_retirement_controls import require_retirement_controls
from process.provider_directory_terminal_root_retirement_operator import (
    apply_terminal_root_retirement,
    preview_terminal_root_retirement,
)
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.provider_directory_terminal_root_retirement_pg_support import SqlCapture

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ORPHAN_DATASET_ID = "dataset-predecessorless"


async def _prepare_native_storage(scenario):
    schema = scenario.schema_name
    await scenario.connection.execute(f"""
      ALTER TABLE {scenario.schema}.provider_directory_dataset_resource ADD COLUMN acquired_resource_sha256 text;
      ALTER TABLE {scenario.schema}.provider_directory_endpoint_dataset ALTER COLUMN completion_proof_required_version
        TYPE integer USING completion_proof_required_version::integer;
      ALTER TABLE {scenario.schema}.provider_directory_uhc_flex_practitioner_dataset_resource ADD COLUMN resource_type text;
      CREATE TABLE {scenario.schema}.provider_directory_rooted_graph_dataset_resource
        (dataset_id varchar(96),resource_type varchar(64),resource_id varchar(256),
         PRIMARY KEY(dataset_id,resource_type,resource_id));
      CREATE TABLE {scenario.schema}.pd_dataset_migration_plan(singleton boolean PRIMARY KEY,plan jsonb);
    """)
    administrator = await scenario.connection.fetchval("SELECT quote_ident(session_user)")
    for table in TABLES:
        await scenario.connection.execute(f'ALTER TABLE {scenario.schema}."{table}" OWNER TO {administrator}')
    await scenario.connection.execute(candidate_functions(schema))


def _convert_native_tables(sync, scenario):
    schema = scenario.schema_name
    plan_by_field = {"complete": True, "tables": {}}
    operations = Operations(MigrationContext.configure(sync))
    keys = list(
        sync.scalars(
            text(f"SELECT dataset_id FROM {scenario.schema}.provider_directory_endpoint_dataset ORDER BY dataset_id")
        )
    )
    for table in TABLES:
        relation = f'{scenario.schema}."{table}"'
        original = sync.scalar(text("SELECT CAST(:relation AS regclass)::oid"), {"relation": relation})
        retained_keys = keys if table in TABLES[:4] else []
        if not retained_keys:
            operations.execute(f"ALTER TABLE {relation} ADD CONSTRAINT pd_dataset_history_bound CHECK(false)")
        _convert_relation(operations, schema, table, retained_keys)
        parent = sync.scalar(text("SELECT CAST(:relation AS regclass)::oid"), {"relation": relation})
        plan_by_field["tables"][table] = {"original_oid": original, "parent_oid": parent, "keys": retained_keys}
    grant_candidate_api(operations, schema, {"practitioner": [], "rooted": []})
    sealed = f"{scenario.schema}.retirement_sealed_resource"
    operations.execute(f"CREATE TABLE {sealed}(LIKE {scenario.schema}.{TABLES[0]} INCLUDING ALL)")
    _deny_direct_writes(operations, schema, sealed)
    operations.execute(f"ALTER TABLE {sealed} ENABLE ALWAYS TRIGGER pd_dataset_sealed")
    operations.execute(
        f"ALTER TABLE {scenario.schema}.{TABLES[0]} ATTACH PARTITION {sealed} FOR VALUES IN ('sealed-example')"
    )
    return plan_by_field


async def _convert_storage(scenario, engine):
    await _prepare_native_storage(scenario)
    schema = scenario.schema_name
    async with engine.begin() as connection:
        plan = await connection.run_sync(_convert_native_tables, scenario)
    await scenario.connection.execute(
        f"INSERT INTO {scenario.schema}.pd_dataset_migration_plan VALUES(true,$1::jsonb)", json.dumps(plan)
    )
    evidence_owner = await scenario.connection.fetchval(
        "SELECT quote_ident(pg_get_userbyid(proowner)) FROM pg_proc WHERE oid=$1::regprocedure",
        f"{scenario.schema}.provider_directory_terminal_root_retirement_evidence(text)",
    )
    for table in TABLES:
        await scenario.connection.execute(f'GRANT SELECT ON {scenario.schema}."{table}" TO {evidence_owner}')
    admission = load_migration(
        REPOSITORY_ROOT / "alembic/versions/20260812020000_provider_directory_endpoint_dataset_admission_seal.py",
        "retirement_native_admission_columns",
    )
    for statement in admission._legacy_trigger_sqls(schema, scoped=True):
        if '"pd_trr_dataset_row"' in statement:
            await scenario.connection.execute(statement)
    await _run_parent_update_migration(scenario, "upgrade")
    return plan


def _parent_update_migration():
    return load_migration(
        REPOSITORY_ROOT / "alembic/versions/20261023100000_provider_directory_retirement_parent_updates.py",
        "retirement_parent_update_migration",
    )


async def _run_parent_update_migration(scenario, action):
    migration, capture = _parent_update_migration(), SqlCapture()
    migration.op = capture
    getattr(migration, action)()
    async with scenario.connection.transaction():
        for statement in capture.statements:
            await scenario.connection.execute(statement)


@asynccontextmanager
async def _native_scenario(monkeypatch):
    from tests.test_provider_directory_terminal_root_retirement_postgres import (
        _scenario,
        _standalone_retirement_database,
    )

    async with _scenario(monkeypatch) as scenario:
        async with _standalone_retirement_database(scenario, administrator=True) as database:
            plan = await _convert_storage(scenario, database.engine)
            yield scenario, database, plan


async def assert_migrated_retirement(monkeypatch):
    from tests.test_provider_directory_terminal_root_retirement_postgres import _request

    async with _native_scenario(monkeypatch) as (scenario, database, plan):
        async with _owned_roles(scenario.connection) as roles:
            await _assign_legacy_owners(scenario, plan, roles)
            await _assert_migrated_data(scenario, database, plan, _request)


async def assert_retired_parent_update_coverage(monkeypatch):
    from tests.test_provider_directory_terminal_root_retirement_postgres import _request

    async with _native_scenario(monkeypatch) as (scenario, database, _plan):
        admission = load_migration(
            REPOSITORY_ROOT / "alembic/versions/20260812020000_provider_directory_endpoint_dataset_admission_seal.py",
            "retirement_parent_admission_seal",
        )
        proof = load_migration(
            REPOSITORY_ROOT / "alembic/versions/20260808190000_provider_directory_subset_completion_proof.py",
            "retirement_parent_admission_digest",
        )
        for statement in (
            admission._add_columns_sql(scenario.schema_name),
            proof._payload_canonical_json_function_sql(scenario.schema_name),
            proof._payload_sha256_function_sql(scenario.schema_name),
            admission._digest_function_sql(scenario.schema_name),
            admission._guard_function_sql(scenario.schema_name),
            *admission._install_triggers_sql(scenario.schema_name)[:2],
            f"ALTER TABLE {scenario.schema}.provider_directory_endpoint_dataset ADD COLUMN future_column text",
        ):
            await scenario.connection.execute(statement)
        token = await preview_terminal_root_retirement(_request(), database=database)
        assert (await apply_terminal_root_retirement(_request(token), database=database)).retired
        before = await scenario.snapshot()
        relation = f"{scenario.schema}.provider_directory_endpoint_dataset"
        await _assert_immutable_write(
            scenario.connection,
            f"UPDATE {relation} SET publication_metadata_summary_json='{{}}'::jsonb,"
            f"publication_metadata_sha256={scenario.schema}.provider_directory_endpoint_dataset_admission_metadata_sha256("
            "'{}'::jsonb,1::smallint,'generic'::text,repeat('a',64),ARRAY[]::varchar[]),"
            "content_proof_admission_version=1,content_proof_admission_kind='generic',"
            "content_proof_admission_sha256=repeat('a',64),content_proof_resource_types=ARRAY[]::varchar[] "
            f"WHERE dataset_id='{ORPHAN_DATASET_ID}'",
        )
        for column in (*admission._SEAL_COLUMNS, "future_column"):
            await _assert_immutable_write(
                scenario.connection,
                f'UPDATE {relation} SET "{column}"="{column}" WHERE dataset_id=\'{ORPHAN_DATASET_ID}\'',
            )
        assert await scenario.snapshot() == before


@asynccontextmanager
async def _parent_migration_scenario(monkeypatch, native):
    from tests.test_provider_directory_terminal_root_retirement_postgres import _scenario

    context = _native_scenario if native else _scenario
    async with context(monkeypatch) as result:
        yield result[0] if native else result


async def _parent_control_identity(scenario):
    return await scenario.connection.fetchrow(f"""
        SELECT c.oid,c.relowner,c.relacl::text,t.oid AS trigger_oid,t.tgfoid,t.tgenabled,
               p.proowner,p.proacl::text,p.prosrc,p.proconfig
          FROM pg_class c JOIN pg_trigger t ON t.tgrelid=c.oid
          JOIN pg_proc p ON p.oid=t.tgfoid
         WHERE c.oid='{scenario.schema}.provider_directory_endpoint_dataset'::regclass
           AND t.tgname='pd_trr_dataset_row'
    """)


async def _set_parent_scope(scenario, columns):
    event = "UPDATE" if columns is None else "UPDATE OF " + ",".join(columns)
    relation = f"{scenario.schema}.provider_directory_endpoint_dataset"
    await scenario.connection.execute(
        f"CREATE OR REPLACE TRIGGER pd_trr_dataset_row BEFORE INSERT OR DELETE OR {event} ON {relation} "
        f"FOR EACH ROW EXECUTE FUNCTION {scenario.schema}.guard_provider_directory_terminal_root_retirement_parent();"
        f"ALTER TABLE {relation} ENABLE ALWAYS TRIGGER pd_trr_dataset_row"
    )


async def assert_parent_update_migration_roundtrip(monkeypatch, native):
    async with _parent_migration_scenario(monkeypatch, native) as scenario:
        await _set_parent_scope(scenario, _parent_update_migration()._LEGACY_COLUMNS)
        snapshot_before, identity = await scenario.snapshot(), await _parent_control_identity(scenario)
        # An aborted migration must retain the original scope and every object identity.
        transaction = scenario.connection.transaction()
        await transaction.start()
        try:
            await _run_parent_update_migration(scenario, "upgrade")
            assert await scenario.connection.fetchval("SHOW lock_timeout") == "1s"
            assert await scenario.connection.fetchval("SHOW statement_timeout") == "10s"
        finally:
            await transaction.rollback()
        assert await _parent_control_identity(scenario) == identity
        assert (
            await scenario.connection.fetchval(
                "SELECT cardinality(tgattr::smallint[]) FROM pg_trigger WHERE tgrelid=$1::regclass "
                "AND tgname='pd_trr_dataset_row'",
                f"{scenario.schema}.provider_directory_endpoint_dataset",
            )
            == 17
        )
        for action, width in (("upgrade", 0), ("upgrade", 0), ("downgrade", 17), ("upgrade", 0)):
            await _run_parent_update_migration(scenario, action)
            assert (
                await scenario.connection.fetchval(
                    "SELECT cardinality(tgattr::smallint[]) FROM pg_trigger WHERE tgrelid=$1::regclass "
                    "AND tgname='pd_trr_dataset_row'",
                    f"{scenario.schema}.provider_directory_endpoint_dataset",
                )
                == width
            )
            assert await _parent_control_identity(scenario) == identity
            assert await scenario.snapshot() == snapshot_before


async def assert_retired_parent_downgrade_refused(monkeypatch, native):
    from tests.test_provider_directory_terminal_root_retirement_postgres import (
        _request,
        _standalone_retirement_database,
    )

    async with _parent_migration_scenario(monkeypatch, native) as scenario:
        await _run_parent_update_migration(scenario, "upgrade")
        async with _standalone_retirement_database(scenario, administrator=True) as database:
            token = await preview_terminal_root_retirement(_request(), database=database)
            assert (await apply_terminal_root_retirement(_request(token), database=database)).retired
        rows, identity = await scenario.snapshot(), await _parent_control_identity(scenario)
        with pytest.raises(Exception, match="retirement_parent_downgrade_blocked") as error:
            await _run_parent_update_migration(scenario, "downgrade")
        assert error.value.sqlstate == "55000"
        assert await scenario.snapshot() == rows
        assert await _parent_control_identity(scenario) == identity


async def assert_parent_update_migration_refuses_drift(monkeypatch, case):
    from db import migration_provider_directory_first_acquisition_retirement as first

    async with _native_scenario(monkeypatch) as (scenario, _database, _plan):
        await _set_parent_scope(scenario, _parent_update_migration()._LEGACY_COLUMNS)
        function = f"{scenario.schema}.guard_provider_directory_terminal_root_retirement_parent"
        original = first.parent_sql(scenario.schema_name).replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
        statement_by_case = {
            "body": original.replace("AS $function$\n", "AS $function$\n -- changed body\n", 1),
            "settings": f"ALTER FUNCTION {function}() SET search_path=public",
            "overload": f"CREATE FUNCTION {function}(text) RETURNS boolean LANGUAGE sql AS 'SELECT true'",
            "disabled": f"ALTER TABLE {scenario.schema}.provider_directory_endpoint_dataset DISABLE TRIGGER pd_trr_dataset_row",
        }
        if case == "partial-columns":
            await _set_parent_scope(scenario, ("status",))
        else:
            await scenario.connection.execute(statement_by_case[case])
        rows, identity = await scenario.snapshot(), await _parent_control_identity(scenario)
        with pytest.raises(Exception, match="retirement_parent_control_changed") as error:
            await _run_parent_update_migration(scenario, "upgrade")
        assert error.value.sqlstate == "55000"
        assert await scenario.snapshot() == rows
        assert await _parent_control_identity(scenario) == identity


async def assert_parent_update_migration_bounds_locks(monkeypatch):
    from tests.tin_npi_connector_postgres_support import open_test_connection

    async with _native_scenario(monkeypatch) as (scenario, _database, _plan):
        other = await open_test_connection()
        try:
            async with other.transaction():
                await other.execute(f"LOCK TABLE {scenario.schema}.provider_directory_endpoint_dataset IN SHARE MODE")
                rows, identity = await scenario.snapshot(), await _parent_control_identity(scenario)
                with pytest.raises(Exception, match="lock timeout") as error:
                    await asyncio.wait_for(_run_parent_update_migration(scenario, "upgrade"), timeout=3)
                assert error.value.sqlstate == "55P03"
                assert await scenario.snapshot() == rows
                assert await _parent_control_identity(scenario) == identity
        finally:
            await other.close()


async def _assert_migrated_data(scenario, database, plan, request):
    before = await scenario.snapshot()
    token = await preview_terminal_root_retirement(request(), database=database)
    assert await scenario.snapshot() == before
    result = await apply_terminal_root_retirement(request(token), database=database)
    assert result.retired is True
    after = await scenario.snapshot()
    for relation in before:
        if relation != "provider_directory_endpoint_dataset":
            assert after[relation] == before[relation]
    assert not (await apply_terminal_root_retirement(request(token), database=database)).retired
    history = f'{scenario.schema}."pd_dataset_history_{plan["tables"][TABLES[0]]["original_oid"]}"'
    for relation in (f"{scenario.schema}.{TABLES[0]}", history):
        for statement in (
            f"UPDATE {relation} SET payload_hash=repeat('a',64) WHERE dataset_id='{ORPHAN_DATASET_ID}'",
            f"DELETE FROM {relation} WHERE dataset_id='{ORPHAN_DATASET_ID}'",
            f"INSERT INTO {relation}(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
            f"VALUES('{ORPHAN_DATASET_ID}','Organization','late',repeat('a',64),'{{}}')",
            f"UPDATE {relation} SET dataset_id='dataset-current' WHERE dataset_id='{ORPHAN_DATASET_ID}'",
            f"UPDATE {relation} SET dataset_id='{ORPHAN_DATASET_ID}' WHERE dataset_id='dataset-terminal'",
        ):
            await _assert_immutable_write(scenario.connection, statement)
    assert await scenario.snapshot() == after


@asynccontextmanager
async def _owned_roles(connection):
    roles = ["retirement_owner_" + uuid4().hex, "retirement_actor_" + uuid4().hex]
    created_roles = []
    administrator = await connection.fetchval("SELECT quote_ident(session_user)")
    try:
        for role in roles:
            await connection.execute(f'CREATE ROLE "{role}" LOGIN NOINHERIT')
            created_roles.append(role)
        yield roles
    finally:
        for role in reversed(created_roles):
            await connection.execute(f'REVOKE SET ON PARAMETER session_replication_role FROM "{role}"')
            await connection.execute(f'REASSIGN OWNED BY "{role}" TO {administrator}')
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')


async def _assign_legacy_owners(scenario, plan, roles):
    owner, publisher = roles
    await scenario.connection.execute(f'GRANT "{owner}" TO "{publisher}" WITH INHERIT FALSE, SET TRUE')
    await scenario.connection.execute(f'GRANT USAGE ON SCHEMA {scenario.schema} TO "{owner}","{publisher}"')
    relation_oids = [
        identity[field] for identity in plan["tables"].values() for field in ("original_oid", "parent_oid")
    ]
    relations = await scenario.connection.fetch(
        "SELECT oid::regclass::text AS name FROM pg_class WHERE oid=ANY($1::oid[]) OR oid=$2::regclass",
        relation_oids,
        f"{scenario.schema}.retirement_sealed_resource",
    )
    for relation in relations:
        await scenario.connection.execute(f'ALTER TABLE {relation["name"]} OWNER TO "{owner}"')
    assert await scenario.connection.fetchval("SELECT pg_has_role($1,$2,'SET')", publisher, owner)


async def _assert_role_boundary(scenario, database, plan, case):
    leaf = f'{scenario.schema}."pd_dataset_history_{plan["tables"][TABLES[0]]["original_oid"]}"'
    async with _owned_roles(scenario.connection) as roles:
        actor, writer = roles
        if case == "foreign-owner":
            await scenario.connection.execute(f'ALTER TABLE {leaf} OWNER TO "{actor}"')
        else:
            await scenario.connection.execute(f'GRANT SET ON PARAMETER session_replication_role TO "{actor}"')
            await scenario.connection.execute(f'GRANT USAGE ON SCHEMA {scenario.schema} TO "{writer}"')
            await scenario.connection.execute(f'GRANT UPDATE(payload_hash) ON {leaf} TO "{writer}"')
            await scenario.connection.execute(f'GRANT "{writer}" TO "{actor}" WITH INHERIT FALSE, SET TRUE')
            assert not await scenario.connection.fetchval(
                "SELECT has_any_column_privilege($1,$2,'UPDATE')", actor, leaf
            )
            assert not await scenario.connection.fetchval(
                "SELECT has_parameter_privilege($1,'session_replication_role','SET')", writer
            )
        with pytest.raises(TerminalRootRetirementError, match="state_invalid"):
            async with database.transaction():
                await require_retirement_controls(database)


async def _assert_immutable_write(connection, statement):
    with pytest.raises(Exception, match="immutable"):
        async with connection.transaction():
            await connection.execute(statement)


def _tamper_statement(case, scenario, plan):
    root = f"{scenario.schema}.{TABLES[0]}"
    leaf = f'{scenario.schema}."pd_dataset_history_{plan["tables"][TABLES[0]]["original_oid"]}"'
    trigger_relation = root if case.endswith("root") else leaf
    hook = "a_pd_candidate_insert_new"
    if case.startswith("drop-"):
        return f"DROP TRIGGER {hook} ON {trigger_relation}"
    if case.startswith("disable-"):
        return f"ALTER TABLE {trigger_relation} DISABLE TRIGGER {hook}"
    if case in {"truncate-grant", "trigger-grant"}:
        privilege = "TRUNCATE" if case == "truncate-grant" else "TRIGGER"
        return f"GRANT {privilege} ON {leaf} TO PUBLIC"
    if case == "parent-columns":
        return f"CREATE OR REPLACE TRIGGER pd_trr_dataset_row BEFORE INSERT OR DELETE OR UPDATE OF status ON {scenario.schema}.provider_directory_endpoint_dataset FOR EACH ROW EXECUTE FUNCTION {scenario.schema}.guard_provider_directory_terminal_root_retirement_parent()"
    if case == "legacy-parent-columns":
        columns = ",".join(_parent_update_migration()._LEGACY_COLUMNS)
        return f"CREATE OR REPLACE TRIGGER pd_trr_dataset_row BEFORE INSERT OR DELETE OR UPDATE OF {columns} ON {scenario.schema}.provider_directory_endpoint_dataset FOR EACH ROW EXECUTE FUNCTION {scenario.schema}.guard_provider_directory_terminal_root_retirement_parent(); ALTER TABLE {scenario.schema}.provider_directory_endpoint_dataset ENABLE ALWAYS TRIGGER pd_trr_dataset_row"
    if case == "plan":
        return f"UPDATE {scenario.schema}.pd_dataset_migration_plan SET plan=jsonb_set(plan,'{{complete}}','false')"
    if case == "overload":
        return f"CREATE FUNCTION {scenario.schema}.guard_pd_dataset_candidate_parent(text) RETURNS boolean LANGUAGE sql AS 'SELECT true'"
    if case == "body":
        signature = f"CREATE FUNCTION {scenario.schema}.guard_pd_dataset_candidate_parent()"
        body = candidate_functions(scenario.schema_name).split(signature, 1)[1].split("$fn$;", 1)[0]
        return (
            signature.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION")
            + body.replace("BEGIN", "BEGIN -- changed body", 1)
            + "$fn$;"
        )
    if case == "row-security":
        return f"ALTER TABLE {leaf} ENABLE ROW LEVEL SECURITY"
    if case == "sealed-disabled":
        return f"ALTER TABLE {scenario.schema}.retirement_sealed_resource DISABLE TRIGGER pd_dataset_sealed"
    if case == "partial":
        return f"ALTER TABLE {root} DETACH PARTITION {leaf}; ALTER TABLE {root} RENAME TO partial_resource; ALTER TABLE {leaf} RENAME TO {TABLES[0]}"
    if case == "extra-parent-column":
        from process.network_fhir_source_custody import _UPDATE_COLUMNS

        columns = ",".join(_UPDATE_COLUMNS["tin_npi_connector_endpoint_dataset_guard"]) + ",unrelated_column"
        return f"ALTER TABLE {scenario.schema}.provider_directory_endpoint_dataset ADD COLUMN unrelated_column text; CREATE OR REPLACE TRIGGER pd_trr_dataset_row BEFORE INSERT OR DELETE OR UPDATE OF {columns} ON {scenario.schema}.provider_directory_endpoint_dataset FOR EACH ROW EXECUTE FUNCTION {scenario.schema}.guard_provider_directory_terminal_root_retirement_parent()"
    transition = "wrong_rows" if case == "transition" else "affected_rows"
    image = "OLD" if case == "arguments" else "NEW"
    function = "deny_pd_dataset_candidate_write" if case == "rewire" else "guard_pd_dataset_candidate_parent"
    arguments = "" if case == "rewire" else f"'{TABLES[0]}','{image}'"
    when = " WHEN(false)" if case == "condition" else ""
    return f"CREATE OR REPLACE TRIGGER {hook} AFTER INSERT ON {leaf} REFERENCING NEW TABLE AS {transition} FOR EACH STATEMENT{when} EXECUTE FUNCTION {scenario.schema}.{function}({arguments})"


async def assert_changed_native_boundary(monkeypatch, case):
    from tests.test_provider_directory_terminal_root_retirement_postgres import _request

    async with _native_scenario(monkeypatch) as (scenario, database, plan):
        async with database.transaction():
            await require_retirement_controls(database)
        before = await scenario.snapshot()
        if case in {"foreign-owner", "split-replica"}:
            await _assert_role_boundary(scenario, database, plan, case)
            assert await scenario.snapshot() == before
            return
        if case == "replica":
            async with database.transaction():
                await database.execute("SET LOCAL session_replication_role=replica")
                with pytest.raises(TerminalRootRetirementError, match="state_invalid"):
                    await require_retirement_controls(database)
            assert await scenario.snapshot() == before
            return
        await scenario.connection.execute(_tamper_statement(case, scenario, plan))
        with pytest.raises(TerminalRootRetirementError, match="state_invalid"):
            await preview_terminal_root_retirement(_request(), database=database)
        if case != "extra-parent-column":
            assert await scenario.snapshot() == before
