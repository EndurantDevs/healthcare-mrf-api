# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native metadata-drift proof; synthetic pins do not issue CMS authority."""

import json
import os
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_preparation_custody as custody
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding


async def _create_catalog_roles(admin, role_by_kind, password, created_roles):
    """Register each exact role for teardown before attempting its creation."""
    for key, name in role_by_kind.items():
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)
        created_roles.append(name)
        login = "LOGIN" if key in {"publisher", "observer"} else "NOLOGIN"
        secret = (
            " PASSWORD " + await admin.fetchval("SELECT quote_literal($1::text)", password)
            if key == "publisher"
            else ""
        )
        await admin.execute(
            f'CREATE ROLE "{name}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS{secret}'
        )


async def _create_catalog_relations(database, role_by_kind):
    """Keep mixed canonical owners and the original private schema grants."""
    await database.execute(f'CREATE SCHEMA catalog AUTHORIZATION "{role_by_kind["catalog"]}"')
    await database.execute(f'CREATE SCHEMA private_source AUTHORIZATION "{role_by_kind["namespace"]}"')
    await database.execute(f'GRANT USAGE ON SCHEMA catalog,private_source TO "{role_by_kind["owner"]}"')
    await database.execute(f'GRANT CREATE ON SCHEMA private_source TO "{role_by_kind["owner"]}"')
    await database.execute(
        f'GRANT "{role_by_kind["owner"]}" TO "{role_by_kind["publisher"]}" WITH INHERIT FALSE,SET TRUE'
    )
    for schema, roots in (("catalog", custody.CANONICAL_ROOTS), ("private_source", custody.ISOLATED_FHIR_ROOTS)):
        for ordinal, root in enumerate(roots):
            await database.execute(
                f'CREATE TABLE "{schema}"."{root}" (id integer PRIMARY KEY CHECK(id>=0),payload text)'
            )
            actual_owner = role_by_kind["catalog"] if schema == "catalog" and ordinal % 2 else role_by_kind["source"]
            await database.execute(f'ALTER TABLE "{schema}"."{root}" OWNER TO "{actual_owner}"')
            await database.execute(f'GRANT SELECT,INSERT ON "{schema}"."{root}" TO "{role_by_kind["owner"]}"')


async def _create_catalog_routines(database, role_by_kind):
    """Use synthetic signatures and a PL/pgSQL call absent from pg_depend."""
    for name, arguments in custody._CALLEE_FRONTIER.items():
        signature = storage_relation("catalog", name) + "(" + ",".join("pg_catalog." + kind for kind in arguments) + ")"
        returns, returned_sql = (
            ("trigger", "NEW") if name == "guard_pd_dataset_candidate_parent" else ("boolean", "true")
        )
        await database.execute(
            f"CREATE FUNCTION {signature} RETURNS {returns} LANGUAGE plpgsql AS $$BEGIN RETURN {returned_sql}; END$$"
        )
        await database.execute(f'ALTER FUNCTION {signature} OWNER TO "{role_by_kind["catalog"]}"')
    await database.execute(
        "CREATE FUNCTION catalog.custody_fixture_guard() RETURNS trigger LANGUAGE plpgsql "
        "AS $$BEGIN PERFORM catalog.provider_directory_subset_canonical_sha256('{}'::jsonb); RETURN NEW; END$$"
    )
    await database.execute(f'ALTER FUNCTION catalog.custody_fixture_guard() OWNER TO "{role_by_kind["catalog"]}"')
    await database.execute("REVOKE ALL ON FUNCTION catalog.custody_fixture_guard() FROM PUBLIC")
    await database.execute(
        "CREATE TRIGGER custody_fixture_guard BEFORE INSERT ON "
        "catalog.provider_directory_cms_candidate_coverage "
        "FOR EACH ROW EXECUTE FUNCTION catalog.custody_fixture_guard()"
    )


async def _catalog_profile(database, role_by_kind):
    """Capture the same fixture-only binding, policy and native custody pin."""
    coordinates = await database.fetchrow(
        "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,"
        "'catalog'::regnamespace::oid::bigint AS catalog_schema_oid,"
        "'private_source'::regnamespace::oid::bigint AS storage_schema_oid,"
        "(SELECT oid::bigint FROM pg_roles WHERE rolname=$1) AS owner_oid,"
        "(SELECT oid::bigint FROM pg_roles WHERE rolname=$2) AS namespace_owner_oid",
        role_by_kind["source"],
        role_by_kind["namespace"],
    )
    binding = FHIRSourceStorageBinding("catalog", "private_source", **dict(coordinates), enrollment_sha256="a" * 64)
    policy = GenericSourcePolicy(
        "private_source", role_by_kind["owner"], role_by_kind["publisher"], (role_by_kind["observer"],)
    )
    async with database.transaction():
        observed = await custody._observe(database, binding, policy)
    profile_by_field = dict(
        contract=custody.CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(binding),
        policy=json.loads(json.dumps(asdict(policy))),
        **observed,
    )
    return binding, policy, profile_by_field


@asynccontextmanager
async def _native_catalog():
    """Yield an isolated catalog and retire only its registered database and roles."""
    dsn = os.getenv("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_CUSTODY_TEST_DSN for the local native custody proof")
    url = make_url(dsn)
    assert url.host in {"localhost", "127.0.0.1"} and url.port is not None and not url.query
    token = uuid4().hex
    database_name = "hc_cms_custody_" + token
    role_by_kind = {
        key: "cms_custody_" + key + "_" + token[:16]
        for key in ("owner", "publisher", "catalog", "source", "namespace", "observer")
    }
    admin = await asyncpg.connect(dsn)
    database = publisher = None
    created_roles, has_attempted_database = [], False
    password = uuid4().hex
    try:
        assert await admin.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database_name)
        has_attempted_database = True
        await admin.execute(f'CREATE DATABASE "{database_name}" TEMPLATE template0')
        await _create_catalog_roles(admin, role_by_kind, password, created_roles)
        native_dsn = url.set(database=database_name).render_as_string(hide_password=False)
        database = await asyncpg.connect(native_dsn)
        await _create_catalog_relations(database, role_by_kind)
        await _create_catalog_routines(database, role_by_kind)
        binding, policy, profile_by_field = await _catalog_profile(database, role_by_kind)
        publisher = await asyncpg.connect(native_dsn, user=role_by_kind["publisher"], password=password)
        yield SimpleNamespace(
            admin=database,
            publisher=publisher,
            binding=binding,
            policy=policy,
            profile=profile_by_field,
            roles=role_by_kind,
            created_roles=created_roles,
        )
    finally:
        if publisher is not None:
            await publisher.close()
        if database is not None:
            await database.close()
        if has_attempted_database:
            await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database_name)
            assert not await admin.fetchval(
                "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", database_name
            )
        for name in reversed(created_roles):
            await admin.execute(f'DROP ROLE IF EXISTS "{name}"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)
        await admin.close()
        assert admin.is_closed()


async def _compare(case):
    async with case.publisher.transaction(readonly=True):
        await case.publisher.execute(f'SET LOCAL ROLE "{case.policy.owner_role}"')
        return await custody.compare_cms_preparation_custody(
            case.publisher,
            storage_binding=case.binding,
            policy=case.policy,
            cms_profile=case.profile,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("root", custody.TABLES[:4])
async def test_canonical_payload_drift_is_included(root):
    async with _native_catalog() as case:
        assert await _compare(case) == {field: case.profile[field] for field in custody._DIGEST_FIELDS}
        await case.admin.execute(f'ALTER TABLE "catalog"."{root}" ADD COLUMN unexpected integer')
        with pytest.raises(ValueError, match="native_evidence_changed"):
            await _compare(case)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "column",
        "index",
        "constraint",
        "guard_enabled",
        "guard_definition",
        "guard_owner",
        "function_acl",
        "relation_owner",
        "table_acl",
        "column_acl",
        "role_membership",
        "private_create",
        "private_template",
    ],
)
async def test_native_exact_metadata_and_connected_login_drift_refuse_with_cleanup(change):
    async with _native_catalog() as case:
        # Exact mixed catalog owners are accepted as evidence; no Source-owner rewrite occurs.
        assert await _compare(case) == {field: case.profile[field] for field in custody._DIGEST_FIELDS}
        table = "catalog.provider_directory_cms_candidate_coverage"
        match change:
            case "column":
                sql = f"ALTER TABLE {table} ALTER COLUMN payload TYPE varchar(32)"
            case "index":
                sql = f"CREATE INDEX custody_changed_index ON {table}(payload)"
            case "constraint":
                sql = f"ALTER TABLE {table} DROP CONSTRAINT provider_directory_cms_candidate_coverage_id_check"
            case "guard_enabled":
                sql = f"ALTER TABLE {table} DISABLE TRIGGER custody_fixture_guard"
            case "guard_definition":
                sql = (
                    "CREATE OR REPLACE FUNCTION catalog.custody_fixture_guard() RETURNS trigger "
                    "LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$"
                )
            case "guard_owner":
                sql = f'ALTER FUNCTION catalog.custody_fixture_guard() OWNER TO "{case.roles["source"]}"'
            case "function_acl":
                sql = f'GRANT EXECUTE ON FUNCTION catalog.custody_fixture_guard() TO "{case.roles["observer"]}"'
            case "relation_owner":
                sql = f'ALTER TABLE {table} OWNER TO "{case.roles["owner"]}"'
            case "table_acl":
                sql = f'GRANT INSERT ON {table} TO "{case.roles["observer"]}"'
            case "column_acl":
                sql = f'GRANT UPDATE(payload) ON {table} TO "{case.roles["observer"]}"'
            case "role_membership":
                sql = f'GRANT "{case.roles["owner"]}" TO "{case.roles["observer"]}" WITH INHERIT FALSE,SET TRUE'
            case "private_create":
                sql = f'GRANT CREATE ON SCHEMA private_source TO "{case.roles["observer"]}"'
            case "private_template":
                sql = "ALTER TABLE private_source.provider_directory_endpoint_dataset ADD COLUMN unexpected integer"
        await case.admin.execute(sql)
        with pytest.raises(ValueError, match="native_evidence_changed"):
            await _compare(case)


@pytest.mark.asyncio
async def test_public_reads_and_invoker_execute_do_not_pin_unrelated_logins():
    async with _native_catalog() as case:
        await case.admin.execute("GRANT USAGE ON SCHEMA catalog,private_source TO PUBLIC")
        await case.admin.execute("GRANT SELECT ON ALL TABLES IN SCHEMA catalog TO PUBLIC")
        await case.admin.execute("GRANT EXECUTE ON FUNCTION catalog.custody_fixture_guard() TO PUBLIC")
        async with case.admin.transaction():
            case.profile.update(await custody._observe(case.admin, case.binding, case.policy))
        assert await _compare(case) == {field: case.profile[field] for field in custody._DIGEST_FIELDS}
        unrelated = "cms_custody_unrelated_" + uuid4().hex[:16]
        assert not await case.admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", unrelated)
        case.created_roles.append(unrelated)
        await case.admin.execute(
            f'CREATE ROLE "{unrelated}" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
        )
        # Mere PUBLIC consumption is not an authority delta; a real writer grant is.
        assert await _compare(case) == {field: case.profile[field] for field in custody._DIGEST_FIELDS}
        await case.admin.execute(f'GRANT INSERT ON catalog.provider_directory_cms_candidate_coverage TO "{unrelated}"')
        with pytest.raises(ValueError, match="native_evidence_changed"):
            await _compare(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("absent", "overload", "body", "replacement"))
async def test_native_unregistered_plpgsql_callee_drift_refuses(change):
    async with _native_catalog() as case:
        helper = "catalog.provider_directory_subset_canonical_sha256(jsonb)"
        assert await case.admin.fetchval(
            "SELECT NOT EXISTS(SELECT FROM pg_depend WHERE classid='pg_proc'::regclass "
            "AND objid='catalog.custody_fixture_guard()'::regprocedure "
            "AND refclassid='pg_proc'::regclass AND refobjid=$1::regprocedure)",
            helper,
        )
        assert await _compare(case) == {field: case.profile[field] for field in custody._DIGEST_FIELDS}
        if change in {"absent", "replacement"}:
            await case.admin.execute("DROP FUNCTION " + helper)
        if change == "replacement":
            await case.admin.execute(
                f"CREATE FUNCTION {helper} RETURNS boolean LANGUAGE plpgsql AS $$BEGIN RETURN true; END$$"
            )
            await case.admin.execute(f'ALTER FUNCTION {helper} OWNER TO "{case.roles["catalog"]}"')
        elif change == "overload":
            await case.admin.execute(
                "CREATE FUNCTION catalog.provider_directory_subset_canonical_sha256(text) "
                "RETURNS boolean LANGUAGE plpgsql AS $$BEGIN RETURN true; END$$"
            )
        elif change == "body":
            await case.admin.execute(
                f"CREATE OR REPLACE FUNCTION {helper} RETURNS boolean LANGUAGE plpgsql AS $$BEGIN RETURN false; END$$"
            )
        expected = "callees_missing_or_ambiguous" if change in {"absent", "overload"} else "native_evidence_changed"
        with pytest.raises(ValueError, match=expected):
            await _compare(case)


@asynccontextmanager
async def _callable_catalog():
    async with _native_catalog() as case:
        await case.admin.execute(f'GRANT USAGE ON SCHEMA catalog TO "{case.roles["publisher"]}"')
        yield case


async def _callable_owners(case, *, migration=None):
    async with case.publisher.transaction(readonly=True):
        return await custody.require_cms_canonical_callable_owners(
            case.publisher,
            catalog_schema="catalog",
            migration_role=migration or case.roles["namespace"],
        )


@pytest.mark.asyncio
async def test_canonical_callable_owners_are_protected_without_grant_changes():
    async with _callable_catalog() as case:
        before = await case.admin.fetch(
            "SELECT oid,proowner,proacl,prosrc FROM pg_proc WHERE pronamespace='catalog'::regnamespace ORDER BY oid"
        )
        observed = await _callable_owners(case)
        assert len(observed) >= len(custody._CALLEE_FRONTIER)
        assert (
            await case.admin.fetch(
                "SELECT oid,proowner,proacl,prosrc FROM pg_proc WHERE pronamespace='catalog'::regnamespace ORDER BY oid"
            )
            == before
        )


def _callable_owner_changes(case):
    owner = '"' + case.roles["catalog"] + '"'
    actor = '"' + case.roles["observer"] + '"'
    intermediate = '"' + case.roles["source"] + '"'
    return {
        "login": (f"ALTER ROLE {owner} LOGIN",),
        "elevated": (f"ALTER ROLE {owner} CREATEROLE",),
        "runtime": (f"GRANT {owner} TO {actor} WITH INHERIT FALSE,SET TRUE",),
        "indirect": (
            f"GRANT {owner} TO {intermediate} WITH INHERIT FALSE,SET TRUE",
            f"GRANT {intermediate} TO {actor} WITH INHERIT FALSE,SET TRUE",
        ),
        "elevated_membership": (
            f"ALTER ROLE {actor} CREATEDB",
            f"GRANT {actor} TO {owner} WITH INHERIT FALSE,SET TRUE",
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["login", "elevated", "runtime", "indirect", "elevated_membership"])
async def test_canonical_callable_owner_or_replacement_reach_refuses(change):
    async with _callable_catalog() as case:
        async with case.admin.transaction():
            for statement in _callable_owner_changes(case)[change]:
                await case.admin.execute(statement)
        with pytest.raises(ValueError, match="callable_owner_unprotected"):
            await _callable_owners(case)


@pytest.mark.asyncio
async def test_only_declared_migration_login_keeps_callable_maintenance_reach():
    async with _callable_catalog() as case:
        owner = '"' + case.roles["catalog"] + '"'
        actor = '"' + case.roles["observer"] + '"'
        await case.admin.execute(f"GRANT {owner} TO {actor} WITH INHERIT FALSE,SET TRUE")
        with pytest.raises(ValueError, match="callable_owner_unprotected"):
            await _callable_owners(case)
        assert await _callable_owners(case, migration=case.roles["observer"])
