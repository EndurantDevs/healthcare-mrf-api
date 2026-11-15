# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real duplicate-row retention, configured DCL, replay and savepoint rollback."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from process import cms_native_input_metadata_transition as transition
from tests.test_cms_archive_transaction_migration import _load
from tests.test_cms_native_input_named_revision_migration import _migration
from tests.test_reference_family_archive_postgres import _database_url


async def _raw_bytes(connection, relation):
    return await connection.fetchval(
        f"SELECT array_agg(encode(record_send(snapshot_row),'hex') ORDER BY relation_oid) FROM {relation} snapshot_row"
    )


async def _function_metadata(connection, schema):
    return await connection.fetch(
        "SELECT p.proname,p.prosrc,pg_get_functiondef(p.oid) AS definition,"
        "pg_get_userbyid(p.proowner) AS owner,p.proacl::text AS acl,p.prosecdef,p.proconfig,"
        "pg_get_function_identity_arguments(p.oid) AS arguments,format_type(p.prorettype,NULL) AS result "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname=$1 AND p.proname=ANY($2::text[]) ORDER BY p.proname",
        schema,
        ["cms_native_input_advance", "cms_native_input_revision_guard"],
    )


async def _idle(connection, schema):
    assert connection.is_in_transaction() and schema


async def _setup(connection, schema, archive_schema, roles_by_kind, fault):
    for role in roles_by_kind.values():
        await connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
    for namespace in (schema, archive_schema):
        await connection.execute(f'CREATE SCHEMA "{namespace}"')
        await connection.execute(
            f'GRANT USAGE ON SCHEMA "{namespace}" TO '
            + ",".join(f'"{configured_role}"' for configured_role in roles_by_kind.values())
        )
    statements = []
    historical = _load("20260930120000")
    historical._schema = lambda: f'"{schema}"'
    historical.op = SimpleNamespace(execute=statements.append)
    historical.upgrade()
    for statement in statements:
        await connection.execute(statement)
    await connection.execute(_load("20261019130000")._counter_sql(schema))
    await connection.execute(
        f'ALTER FUNCTION "{schema}".cms_native_input_advance() OWNER TO "{roles_by_kind["counter"]}"'
    )
    await connection.execute(
        f'GRANT EXECUTE ON FUNCTION "{schema}".cms_native_input_advance() TO "{roles_by_kind["writer"]}"'
    )
    await connection.execute(f'CREATE TABLE "{schema}".address_archive_v2(id int PRIMARY KEY,value text)')
    await connection.execute(f"INSERT INTO \"{schema}\".address_archive_v2 VALUES(1,'old')")
    await connection.execute(f'GRANT UPDATE ON "{schema}".address_archive_v2 TO "{roles_by_kind["writer"]}"')
    await connection.execute(
        f'CREATE TRIGGER cms_native_input_mutation AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON "{schema}".address_archive_v2 FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".cms_native_input_advance()'
    )
    await connection.execute(
        f'ALTER TABLE "{schema}".address_archive_v2 ENABLE ALWAYS TRIGGER cms_native_input_mutation'
    )
    await connection.execute(
        f'INSERT INTO "{schema}".cms_native_input_revision(relation_oid,schema_name,table_name,revision) '
        "VALUES(17,$1,'address_archive_v2',7),(19,$1,'openaddresses_geocode',2)",
        schema,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".cms_native_input_revision(relation_oid,schema_name,table_name,revision,mutation_xid) '
        "SELECT 18,CASE WHEN $2::text='missing_name' THEN ''::name ELSE $1::name END,'address_archive_v2',"
        "CASE WHEN $2::text='revision' THEN 9 ELSE 7 END,CASE WHEN $2::text='stamp' THEN pg_current_xact_id() END "
        "WHERE $2::text IS DISTINCT FROM 'not_duplicate'",
        schema,
        fault,
    )


@pytest.fixture
async def native_case(request):
    url = make_url(_database_url()).set(drivername="postgresql")
    connection = await asyncpg.connect(url.render_as_string(hide_password=False), command_timeout=10)
    suffix = uuid4().hex
    schema, archive_schema = "ledger_" + suffix, "archive_" + suffix
    roles_by_kind = {
        kind: kind + "_" + suffix for kind in ("ledger_owner", "archive_owner", "counter", "writer", "reader")
    }
    try:
        async with connection.transaction():
            await _setup(
                connection,
                schema,
                archive_schema,
                roles_by_kind,
                request.node.callspec.params.get("fault") if hasattr(request.node, "callspec") else None,
            )
        arguments_by_name = dict(
            ledger_schema=schema,
            retained_qname=(archive_schema, "revision_history"),
            owner_roles={"ledger": roles_by_kind["ledger_owner"], "archive": roles_by_kind["archive_owner"]},
            counter_role=roles_by_kind["counter"],
            writer_roles=(roles_by_kind["writer"],),
            reader_roles=(roles_by_kind["reader"],),
            assert_idle=_idle,
        )
        yield SimpleNamespace(
            connection=connection,
            schema=schema,
            roles=roles_by_kind,
            arguments=arguments_by_name,
            canonical=f'"{schema}".cms_native_input_revision',
            retained=f'"{archive_schema}".revision_history',
        )
    finally:
        try:
            async with connection.transaction():
                for namespace in (schema, archive_schema):
                    await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
                for role in reversed(tuple(roles_by_kind.values())):
                    await connection.execute(f'DROP ROLE IF EXISTS "{role}"')
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=ANY($1::text[]))", [schema, archive_schema]
            )
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", list(roles_by_kind.values())
            )
        finally:
            await connection.close()


@asynccontextmanager
async def _role(connection, name):
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{name}"')
        yield
        await connection.execute("RESET ROLE")


async def _assert_access(case):
    connection = case.connection
    async with _role(connection, case.roles["reader"]):
        assert await connection.fetchval(f"SELECT count(*) FROM {case.retained}") == 3
        assert await connection.fetchval(f"SELECT count(*) FROM {case.canonical}") == 2
    for role in case.roles.values():
        async with _role(connection, role):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(f"UPDATE {case.retained} SET revision=revision+1")
    async with _role(connection, case.roles["writer"]):
        await connection.execute(
            f"INSERT INTO {case.canonical}(schema_name,table_name) VALUES($1,'additional')", case.schema
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(f"UPDATE {case.canonical} SET revision=revision+1")
        await connection.execute(f"UPDATE \"{case.schema}\".address_archive_v2 SET value='new'")
    assert (
        await connection.fetchval(f"SELECT revision FROM {case.canonical} WHERE table_name='address_archive_v2'") == 8
    )
    assert (
        await connection.fetchval(
            f"SELECT mutation_xid=pg_current_xact_id() FROM {case.canonical} WHERE table_name='address_archive_v2'"
        )
        is True
    )
    async with _role(connection, case.roles["counter"]):
        with pytest.raises(asyncpg.RaiseError, match="revision_immutable"):
            async with connection.transaction():
                await connection.execute(
                    f"UPDATE {case.canonical} SET revision=revision+1,mutation_xid=pg_current_xact_id()"
                )


async def _assert_function_delta(connection, schema, before):
    after = await _function_metadata(connection, schema)
    expected_by_name = dict(
        cms_native_input_advance=transition.inputs._advance_body(schema),
        cms_native_input_revision_guard=transition.inputs._ledger_guard_body(),
    )
    assert len(before) == len(after) == 2
    for old, new in zip(before, after, strict=True):
        original, updated = dict(old), dict(new)
        assert updated.pop("prosrc") == expected_by_name[new["proname"]]
        original.pop("prosrc")
        old_definition, new_definition = original.pop("definition"), updated.pop("definition")
        assert updated == original
        assert new_definition == old_definition.replace(old["prosrc"], new["prosrc"])


@pytest.mark.parametrize("retained_location", ["separate_schema", "same_schema", "visible_archive"])
async def test_equivalent_duplicates_keep_raw_bytes_and_prepare_named_counter(native_case, retained_location):
    case = native_case
    connection = case.connection
    raw = await _raw_bytes(connection, case.canonical)
    functions = await _function_metadata(connection, case.schema)
    async with connection.transaction():
        if retained_location == "same_schema":
            case.arguments["retained_qname"] = (case.schema, "revision_history")
        elif retained_location == "visible_archive":
            archive_schema = case.arguments["retained_qname"][0]
            case.arguments["retained_qname"] = (archive_schema, "cms_native_input_revision")
            await connection.execute(f'SET LOCAL search_path="{archive_schema}","{case.schema}",pg_catalog')
        case.retained = transition.storage_relation(*case.arguments["retained_qname"])
        with pytest.raises(asyncpg.RaiseError, match="named_revision_ambiguous"):
            async with connection.transaction():
                await connection.execute(_migration()._key_sql(case.schema)[0])
        preparation_result = await transition.prepare_cms_native_input_metadata_transition(connection, **case.arguments)
        assert preparation_result["status"] == "prepared" and (
            preparation_result["raw_rows"],
            preparation_result["named_rows"],
        ) == (3, 2)
        assert await _raw_bytes(connection, case.retained) == raw
        await _assert_function_delta(connection, case.schema, functions)
        replay = await transition.prepare_cms_native_input_metadata_transition(connection, **case.arguments)
        assert replay == {**preparation_result, "status": "already_prepared"}
        await connection.execute(
            _migration()._function_sql(
                case.schema, names=("cms_native_input_advance", "cms_native_input_revision_guard")
            )
        )
        for statement in _migration()._key_sql(case.schema):
            await connection.execute(statement)
        assert await _raw_bytes(connection, case.retained) == raw
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal", case.canonical
            )
            == 2
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal", case.retained
            )
            == 0
        )
        await _assert_access(case)
        assert await _raw_bytes(connection, case.retained) == raw
        with pytest.raises(RuntimeError, match="retained_projection_changed"):
            await transition.prepare_cms_native_input_metadata_transition(connection, **case.arguments)
        assert connection.is_in_transaction()


@pytest.mark.parametrize("fault", ["revision", "stamp", "missing_name", "not_duplicate", "collision"])
async def test_ambiguous_or_incomplete_history_refuses_without_raw_mutation(native_case, fault):
    case = native_case
    connection = case.connection
    async with connection.transaction():
        if fault == "collision":
            await connection.execute(f"CREATE TABLE {case.retained}(marker int)")
        raw = await _raw_bytes(connection, case.canonical)
        functions = await _function_metadata(connection, case.schema)
        with pytest.raises(RuntimeError, match="diverge|invalid|duplicates_required|already_exists"):
            await transition.prepare_cms_native_input_metadata_transition(connection, **case.arguments)
        assert await _raw_bytes(connection, case.canonical) == raw
        assert await _function_metadata(connection, case.schema) == functions
        assert connection.is_in_transaction()


@pytest.mark.parametrize("failure", ["idle", "sql"])
async def test_caller_caught_late_failure_restores_all_original_bytes_and_guards(native_case, failure):
    case = native_case
    connection = case.connection
    raw = await _raw_bytes(connection, case.canonical)
    functions = await _function_metadata(connection, case.schema)
    calls = []

    async def idle(driver, schema):
        calls.append(schema)
        if failure == "idle" and len(calls) == 3:
            raise AssertionError("idle state changed after preparation")

    arguments_by_name = {**case.arguments, "assert_idle": idle}
    if failure == "sql":
        arguments_by_name["counter_role"] = "missing_" + uuid4().hex
    async with connection.transaction():
        with pytest.raises(AssertionError if failure == "idle" else asyncpg.UndefinedObjectError):
            await transition.prepare_cms_native_input_metadata_transition(connection, **arguments_by_name)
        assert await _raw_bytes(connection, case.canonical) == raw
        assert await _function_metadata(connection, case.schema) == functions
        assert await connection.fetchval("SELECT to_regclass($1)", case.retained) is None
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal", case.canonical
            )
            == 2
        )
        assert await connection.fetchval("SELECT 1") == 1


async def test_retry_with_changed_writer_applies_requested_grants(native_case):
    case = native_case
    connection = case.connection
    corrected = case.roles["corrected_writer"] = "corrected_writer_" + uuid4().hex
    async with connection.transaction():
        await connection.execute(f'CREATE ROLE "{corrected}" NOLOGIN')
        await connection.execute(f'GRANT USAGE ON SCHEMA "{case.schema}" TO "{corrected}"')
        first = await transition.prepare_cms_native_input_metadata_transition(connection, **case.arguments)
        retry = await transition.prepare_cms_native_input_metadata_transition(
            connection, **{**case.arguments, "writer_roles": (corrected,)}
        )
        assert retry == {**first, "status": "already_prepared"}
        async with _role(connection, corrected):
            await connection.execute(
                f"INSERT INTO {case.canonical}(schema_name,table_name) VALUES($1,'corrected')", case.schema
            )
            assert await connection.fetchval(f"SELECT count(*) FROM {case.canonical}") == 3
        async with _role(connection, case.roles["writer"]):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(
                        f"INSERT INTO {case.canonical}(schema_name,table_name) VALUES($1,'original')", case.schema
                    )
