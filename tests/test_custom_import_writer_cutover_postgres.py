# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native cutover closure, exact grants, historical constraints and rollback."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import.execution import lease_token_sha256
from tests.custom_import_postgres_support import POSTGRES_DSN_ENV, _migration, isolated_publication_case
from tests.test_custom_import_build_source_postgres import _retained_request
from tests.test_custom_import_materialization_set_postgres import _seed

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not os.getenv(POSTGRES_DSN_ENV), reason="native PostgreSQL test DSN not allocated"),
]
_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20261005080000_custom_import_writer_cutover.py"


def _record_role(name, schema, phase):
    """Journal the exact planned name before cluster-level fixture creation."""
    journal = os.getenv("HLTHPRT_CUSTOM_IMPORT_TEST_ROLE_MANIFEST")
    if journal:
        with open(journal, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(dict(name=name, schema=schema, phase=phase)) + "\n")


def _install(connection, schema):
    migration = _migration(_PATH, "writer_cutover_native")
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


@asynccontextmanager
async def _before_cutover(*, roles=False):
    """Use the actual full prerequisite chain, before any guard is retired."""
    async with isolated_publication_case(migration_through="20261005070000") as case:
        names = tuple("cutover_" + uuid.uuid4().hex[:16] for _ in range(2)) if roles else ()
        created_role_names = []
        try:
            for name in names:
                _record_role(name, case.schema_name, "planned")
                async with case.engine.begin() as connection:
                    await connection.execute(text(f'CREATE ROLE "{name}" NOLOGIN'))
                created_role_names.append(name)
                _record_role(name, case.schema_name, "created")
            yield case, names
        finally:
            for name in reversed(created_role_names):
                async with case.engine.begin() as connection:
                    await connection.execute(text(f'DROP OWNED BY "{name}"'))
                    await connection.execute(text(f'DROP ROLE "{name}"'))
                _record_role(name, case.schema_name, "removed")


async def _guards(connection, schema, *, rows=True):
    return set(
        await connection.scalars(
            text(
                "SELECT t.oid FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND NOT t.tgisinternal AND ((t.tgtype&1)=1)=:rows"
            ),
            dict(schema=schema, rows=rows),
        )
    )


async def _constraints(connection, schema):
    return set(
        await connection.scalars(
            text(
                "SELECT k.oid FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema "
                "AND k.contype IN ('c','p','u','f','n','x')"
            ),
            dict(schema=schema),
        )
    )


async def _retired_guard_oids(connection, schema, specs):
    pairs = [{"relation": spec["table"], "guard": spec["trigger"]} for spec in specs]
    return set(
        await connection.scalars(
            text(
                "SELECT t.oid FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "JOIN jsonb_to_recordset(CAST(:pairs AS jsonb)) AS p(relation text,guard text) "
                "ON (p.relation,p.guard)=(c.relname,t.tgname) "
                "WHERE n.nspname=:schema AND NOT t.tgisinternal AND (t.tgtype&1)=1"
            ),
            dict(schema=schema, pairs=json.dumps(pairs)),
        )
    )


async def _assert_denied(connection, statement):
    with pytest.raises(DBAPIError) as denied:
        async with connection.begin_nested():
            await connection.execute(text(statement))
    assert getattr(denied.value.orig, "sqlstate", None) == "42501"


async def test_native_cutover_removes_only_exact_hot_record_guards():
    async with _before_cutover() as (case, _):
        migration = _migration(_PATH, "writer_cutover_inventory")
        specs = migration._guard_specs(migration._previous(migration._VERSIONS[0])._RELATION_NAMES)
        async with case.engine.begin() as connection:
            row_guards = await _guards(connection, case.schema_name)
            retired_guards = await _retired_guard_oids(connection, case.schema_name, specs)
            assert len(retired_guards) == len(specs)
            statement_guards = await _guards(connection, case.schema_name, rows=False)
            constraints = await _constraints(connection, case.schema_name)
            await connection.run_sync(_install, case.schema_name)
            assert await _guards(connection, case.schema_name) == row_guards - retired_guards
            assert await _guards(connection, case.schema_name, rows=False) == statement_guards
            assert await _constraints(connection, case.schema_name) == constraints
            assert (
                await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
                        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema "
                        "AND c.relname IN ('custom_import_generation','custom_import_lease',"
                        "'custom_import_build_attempt','custom_import_build_stream','custom_import_build_verification') "
                        "AND NOT t.tgisinternal"
                    ),
                    dict(schema=case.schema_name),
                )
                > 0
            )


async def test_native_cutover_closes_public_column_sequence_and_inherited_writes():
    async with _before_cutover(roles=True) as (case, (grant_root, worker)):
        schema = case.schema_name
        table = f'"{schema}".custom_import_pack'
        sequence = f'"{schema}".custom_import_pack_pack_id_seq'
        dispatcher = f'"{schema}".freeze_custom_import_build_source(bigint)'
        async with case.engine.begin() as connection:
            await connection.execute(text(f'GRANT "{grant_root}" TO "{worker}"'))
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{grant_root}"'))
            await connection.execute(text(f'GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON {table} TO "{grant_root}"'))
            await connection.execute(text(f"GRANT INSERT(pack_id),UPDATE(pack_id) ON {table} TO PUBLIC"))
            await connection.execute(text(f'GRANT USAGE,UPDATE ON SEQUENCE {sequence} TO "{grant_root}"'))
            await connection.execute(text(f'GRANT EXECUTE ON FUNCTION {dispatcher} TO "{grant_root}"'))
            prior_oid = await connection.scalar(
                text("SELECT to_regprocedure(:identity)::oid"), dict(identity=dispatcher)
            )
            prior_acl = await connection.scalar(
                text("SELECT proacl::text FROM pg_proc WHERE oid=to_regprocedure(:identity)"), dict(identity=dispatcher)
            )
            await connection.run_sync(_install, schema)
            assert (
                await connection.scalar(text("SELECT to_regprocedure(:identity)::oid"), dict(identity=dispatcher))
                == prior_oid
            )
            assert (
                await connection.scalar(
                    text("SELECT proacl::text FROM pg_proc WHERE oid=to_regprocedure(:identity)"),
                    dict(identity=dispatcher),
                )
                == prior_acl
            )
            await connection.execute(text(f'SET LOCAL ROLE "{worker}"'))
            assert await connection.scalar(text(f"SELECT count(*) FROM {table}")) == 0
            await _assert_denied(connection, f"INSERT INTO {table} DEFAULT VALUES")
            await _assert_denied(connection, f"UPDATE {table} SET pack_id=pack_id WHERE false")
            await _assert_denied(connection, f"DELETE FROM {table} WHERE false")
            await _assert_denied(connection, f"TRUNCATE {table}")
            await _assert_denied(connection, f"SELECT nextval('{sequence}')")


async def _install_transitive_callers(connection, schema, root_caller):
    """Exercise text, unusual-name and native edges through an invoker bridge."""
    invoker = f'"{schema}".synthetic_old_invoker()'
    quoted_caller = f'"{schema}"."synthetic.old_caller"()'
    text_caller = f'"{schema}".synthetic_text_caller()'
    conservative_caller = f'"{schema}".synthetic_conservative_caller()'
    native_caller = f'"{schema}".synthetic_native_caller()'
    for identity, callee_expression, security_mode in (
        (invoker, root_caller, "INVOKER"),
        (quoted_caller, invoker, "DEFINER"),
        (text_caller, quoted_caller, "DEFINER"),
        (conservative_caller, "0 /* syntheticXold_caller */", "DEFINER"),
    ):
        await connection.execute(
            text(
                f"CREATE FUNCTION {identity} RETURNS bigint LANGUAGE plpgsql SECURITY {security_mode} "
                f"SET search_path=pg_catalog AS $fn$ BEGIN RETURN {callee_expression}; END $fn$"
            )
        )
    await connection.execute(
        text(
            f"CREATE FUNCTION {native_caller} RETURNS bigint LANGUAGE sql SECURITY DEFINER "
            f"SET search_path=pg_catalog BEGIN ATOMIC SELECT {text_caller}; END"
        )
    )
    return invoker, quoted_caller, text_caller, conservative_caller, native_caller


async def test_native_cutover_denies_obsolete_roots_and_actual_definer_caller():
    async with _before_cutover(roles=True) as (case, (grant_root, worker)):
        schema = case.schema_name
        caller = f'"{schema}".synthetic_old_writer()'
        migration = _migration(_PATH, "writer_cutover_closed_signatures")
        closed_identities = (
            *(f'"{schema}".{signature}' for signature in migration._OBSOLETE),
            *(receipt["identity"] for receipt in migration._reviewed_functions(schema) if not receipt["runtime"]),
        )
        async with case.engine.begin() as connection:
            await connection.execute(text(f'GRANT "{grant_root}" TO "{worker}"'))
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{grant_root}"'))
            await connection.execute(
                text(
                    f"CREATE FUNCTION {caller} RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER "
                    f'SET search_path=pg_catalog AS $fn$ BEGIN RETURN "{schema}".'
                    "commit_custom_import_build_source_page(1,1); END $fn$"
                )
            )
            indirect_callers = await _install_transitive_callers(connection, schema, caller)
            for identity in closed_identities:
                await connection.execute(text(f'GRANT EXECUTE ON FUNCTION {identity} TO "{grant_root}"'))
            await connection.run_sync(_install, schema)
            for identity in (caller, *indirect_callers, *closed_identities):
                assert (
                    await connection.scalar(
                        text("SELECT has_function_privilege(:role,:identity,'EXECUTE')"),
                        dict(role=worker, identity=identity),
                    )
                    is False
                )
            await connection.execute(text(f'SET LOCAL ROLE "{worker}"'))
            await _assert_denied(connection, f"SELECT {caller}")
            await _assert_denied(connection, f"SELECT {indirect_callers[-1]}")


async def _independent_surface(connection, control_schema, independent_schema):
    """Retain local collisions beside real cross-schema callers, not a schema exemption."""
    local_caller = f'"{independent_schema}".synthetic_local_caller()'
    writer = f'"{independent_schema}".synthetic_local_writer()'
    caller = f'"{independent_schema}".synthetic_old_writer()'
    uppercase = f'"{independent_schema}".synthetic_upper_caller()'
    reader = f'"{independent_schema}".synthetic_native_reader()'
    for identity, return_type, body in (
        (local_caller, "bigint", f"RETURN {independent_schema}.commit_custom_import_build_source_page(1,1);"),
        (writer, "void", f"UPDATE {independent_schema}.custom_import_pack SET pack_id=pack_id WHERE false;"),
        (caller, "bigint", f"RETURN {control_schema}.commit_custom_import_build_source_page(1,1);"),
        (uppercase, "bigint", f"RETURN {control_schema.upper()}.COMMIT_CUSTOM_IMPORT_BUILD_SOURCE_PAGE(1,1);"),
    ):
        await connection.execute(
            text(
                f"CREATE FUNCTION {identity} RETURNS {return_type} LANGUAGE plpgsql SECURITY DEFINER "
                f"SET search_path=pg_catalog AS $fn$ BEGIN {body} END $fn$"
            )
        )
    await connection.execute(
        text(
            f"CREATE FUNCTION {reader} RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog "
            f"BEGIN ATOMIC SELECT EXISTS(SELECT 1 FROM {control_schema}.custom_import_pack); END"
        )
    )
    indirect = await _install_transitive_callers(connection, independent_schema, caller)
    changed = f'"{independent_schema}".freeze_custom_import_build_source(bigint)'
    await connection.execute(
        text(
            f'CREATE OR REPLACE FUNCTION "{independent_schema}".freeze_custom_import_build_source(p_build_id bigint) '
            "RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$ BEGIN "
            f"RETURN {control_schema}.commit_custom_import_build_source_page(1,1)::text; END $fn$"
        )
    )
    migration = _migration(_PATH, "independent_cutover_receipts")
    preserved = (
        local_caller,
        writer,
        reader,
        f'"{independent_schema}".source_set_finalize(uuid,integer[])',
        *(f'"{independent_schema}".{signature}' for signature in migration._OBSOLETE),
    )
    return preserved, (caller, uppercase, *indirect, changed)


async def _function_acls(connection, schema):
    return dict(
        (
            await connection.execute(
                text(
                    "SELECT p.oid,p.proacl::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                    "WHERE n.nspname=:schema"
                ),
                {"schema": schema},
            )
        ).all()
    )


async def _assign_independent_owner(connection, control_schema, independent_schema, owner):
    """Transfer only this disposable installation's schema and function ownership."""
    await connection.execute(text(f'ALTER SCHEMA "{independent_schema}" OWNER TO "{owner}"'))
    identities = await connection.scalars(
        text(
            "SELECT format('%I.%I(%s)',n.nspname,p.proname,pg_get_function_identity_arguments(p.oid)) "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=:schema"
        ),
        {"schema": independent_schema},
    )
    for identity in identities:
        await connection.execute(text(f'ALTER FUNCTION {identity} OWNER TO "{owner}"'))
    await connection.execute(text(f'GRANT USAGE ON SCHEMA "{control_schema}" TO "{owner}"'))
    await connection.execute(text(f'GRANT SELECT ON "{control_schema}".custom_import_pack TO "{owner}"'))
    owner_by_schema = dict(
        (
            await connection.execute(
                text(
                    "SELECT n.nspname,p.proowner FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                    "WHERE n.nspname IN (:control,:independent) AND p.proname='begin_custom_import_build'"
                ),
                {"control": control_schema, "independent": independent_schema},
            )
        ).all()
    )
    assert owner_by_schema[independent_schema] != owner_by_schema[control_schema]
    assert owner_by_schema[independent_schema] == await connection.scalar(
        text("SELECT to_regrole(:owner)::oid"), {"owner": owner}
    )


async def _assert_independent_anchor_drift_rejected(connection, control_schema, independent_schema, previous_acls):
    """Missing or altered installation authority never exempts its dynamic bodies."""
    anchor = (
        f'"{independent_schema}".begin_custom_import_build('
        "bigint,bigint,bytea,bigint,bigint,boolean,integer,bigint,integer,timestamptz)"
    )
    for alteration in ("RENAME TO synthetic_missing_build", "SET search_path=public"):
        with pytest.raises(DBAPIError, match="custom_import_cutover_unreviewed_callable_definer"):
            async with connection.begin_nested():
                await connection.execute(text(f"ALTER FUNCTION {anchor} {alteration}"))
                await connection.run_sync(_install, control_schema)
        assert await _function_acls(connection, independent_schema) == previous_acls


async def _assert_cross_schema_writers_rejected(connection, control_schema, independent_schema, previous_acls):
    """Check both native and procedural write bodies leave independent ACLs unchanged."""
    for body in (
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$ BEGIN "
        f"UPDATE {control_schema}.custom_import_pack SET pack_id=pack_id WHERE false; END $fn$",
        "LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog BEGIN ATOMIC "
        f"UPDATE {control_schema}.custom_import_pack SET pack_id=pack_id WHERE false; END",
    ):
        with pytest.raises(DBAPIError, match="custom_import_cutover_unreviewed_callable_definer"):
            async with connection.begin_nested():
                await connection.execute(
                    text(f'CREATE FUNCTION "{independent_schema}".synthetic_cross_writer() RETURNS void {body}')
                )
                await connection.run_sync(_install, control_schema)
        assert await _function_acls(connection, independent_schema) == previous_acls


async def test_native_cutover_preserves_independent_schema_and_closes_cross_schema_callers():
    async with _before_cutover(roles=True) as (case, (independent_owner, worker)), _before_cutover() as (other, _roles):
        async with case.engine.begin() as connection:
            preserved, blocked = await _independent_surface(connection, case.schema_name, other.schema_name)
            await _assign_independent_owner(connection, case.schema_name, other.schema_name, independent_owner)
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{other.schema_name}" TO "{worker}"'))
            for identity in (*preserved, *blocked):
                await connection.execute(text(f'GRANT EXECUTE ON FUNCTION {identity} TO "{worker}"'))
            before = await _function_acls(connection, other.schema_name)
            blocked_oids = {
                await connection.scalar(text("SELECT to_regprocedure(:identity)::oid"), {"identity": identity})
                for identity in blocked
            }
            await _assert_independent_anchor_drift_rejected(connection, case.schema_name, other.schema_name, before)
            await _assert_cross_schema_writers_rejected(connection, case.schema_name, other.schema_name, before)
            await connection.run_sync(_install, case.schema_name)
            after = await _function_acls(connection, other.schema_name)
            assert after.keys() == before.keys()
            assert {oid for oid in before if before[oid] != after[oid]} == blocked_oids
            for identity in preserved:
                assert await connection.scalar(
                    text("SELECT has_function_privilege(:role,:identity,'EXECUTE')"),
                    {"role": worker, "identity": identity},
                )
            await connection.execute(text(f'SET LOCAL ROLE "{worker}"'))
            assert isinstance(
                await connection.scalar(text(f'SELECT "{other.schema_name}".synthetic_native_reader()')), bool
            )
            for identity in blocked:
                await _assert_denied(connection, f"SELECT {identity.replace('(bigint)', '(1)')}")


async def test_native_cutover_reports_unknown_direct_and_indirect_callable_definers():
    async with _before_cutover() as (case, _):
        schema = case.schema_name
        async with case.engine.begin() as connection:
            await connection.execute(
                text(
                    f'CREATE FUNCTION "{schema}".synthetic_unreviewed_writer() RETURNS void '
                    "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$ BEGIN "
                    f'UPDATE "{schema}".custom_import_pack SET pack_id=pack_id WHERE false; END $fn$'
                )
            )
            await connection.execute(
                text(
                    f'CREATE FUNCTION "{schema}".synthetic_unreviewed_caller() RETURNS void '
                    "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$ BEGIN "
                    f'PERFORM "{schema}".synthetic_unreviewed_writer(); END $fn$'
                )
            )
            before = await _guards(connection, schema)
            with pytest.raises(DBAPIError, match="custom_import_cutover_unreviewed_callable_definer") as failure:
                async with connection.begin_nested():
                    await connection.run_sync(_install, schema)
            assert "synthetic_unreviewed_writer" in str(failure.value)
            assert "synthetic_unreviewed_caller" in str(failure.value)
            assert await _guards(connection, schema) == before


async def test_native_cutover_preserves_unrelated_definer_and_rejects_changed_dispatcher():
    async with _before_cutover() as (case, _):
        schema = case.schema_name
        async with case.engine.begin() as connection:
            await connection.execute(
                text(
                    f'CREATE FUNCTION "{schema}".synthetic_unrelated_reader() RETURNS integer '
                    "LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $fn$ SELECT 1 $fn$"
                )
            )
            with pytest.raises(DBAPIError, match="custom_import_cutover_function_mismatch"):
                async with connection.begin_nested():
                    await connection.execute(
                        text(
                            f'CREATE OR REPLACE FUNCTION "{schema}".freeze_custom_import_build_source(p_build_id bigint) '
                            "RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog "
                            "AS $fn$ BEGIN RETURN 'synthetic'; END $fn$"
                        )
                    )
                    await connection.run_sync(_install, schema)
            await connection.run_sync(_install, schema)
            assert await connection.scalar(text(f'SELECT "{schema}".synthetic_unrelated_reader()')) == 1
            assert (
                await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_proc p,LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a "
                        "WHERE p.oid=to_regprocedure(:identity) AND a.grantee=0 AND a.privilege_type='EXECUTE'"
                    ),
                    dict(identity=f'"{schema}".synthetic_unrelated_reader()'),
                )
                == 1
            )


async def test_native_cutover_refuses_active_unregistered_canonical_producer_atomically():
    async with _before_cutover() as (case, _):
        async with case.sessions() as session, session.begin():
            await _seed(session, uuid.uuid4().hex)
        async with case.engine.begin() as connection:
            before = await _guards(connection, case.schema_name)
            with pytest.raises(DBAPIError, match="custom_import_cutover_active_canonical_producer"):
                async with connection.begin_nested():
                    await connection.run_sync(_install, case.schema_name)
            assert await _guards(connection, case.schema_name) == before


async def test_native_cutover_refuses_preexisting_unregistered_build_progress():
    async with _before_cutover() as (case, _):
        request = await _retained_request(case)
        async with case.engine.begin() as connection:
            await connection.execute(text("SET LOCAL statement_timeout='2000ms'"))
            build_id = await connection.scalar(
                text(
                    f'SELECT "{case.schema_name}".begin_custom_import_build('
                    ":execution,:fence,:token,NULL,0,:complete,1,16384,2000,:deadline)"
                ),
                dict(
                    execution=request.execution_id,
                    fence=request.fence,
                    token=lease_token_sha256(request.lease_token),
                    complete=request.complete_scope,
                    deadline=request.build_deadline_at,
                ),
            )
            await connection.execute(
                text(
                    f'UPDATE "{case.schema_name}".custom_import_build_stream SET next_source_ordinal=1 WHERE build_id=:build'
                ),
                dict(build=build_id),
            )
        async with case.engine.begin() as connection:
            before = await _guards(connection, case.schema_name)
            with pytest.raises(DBAPIError, match="custom_import_cutover_active_canonical_build"):
                async with connection.begin_nested():
                    await connection.run_sync(_install, case.schema_name)
            assert await _guards(connection, case.schema_name) == before
