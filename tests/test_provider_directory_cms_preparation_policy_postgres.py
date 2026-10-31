# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native stable-policy components; synthetic CMS templates issue no authority."""

import json
import os
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_preparation_custody as strict
from process import provider_directory_cms_preparation_policy as stable
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope


async def _supplement_catalog(catalog, role_by_kind):
    """Retain genuine dataset storage; fill only absent CMS roots/frontier signatures."""
    connection, schema = catalog.connection, catalog.schema_name
    synthetic_by_kind = {"roots": [], "routines": []}
    for root in stable.ROOTS:
        relation = storage_relation(schema, root)
        if not await connection.fetchval("SELECT to_regclass($1)::oid", relation):
            await connection.execute(f"CREATE TABLE {relation}(id integer PRIMARY KEY,payload text)")
            synthetic_by_kind["roots"].append(root)
        await connection.execute(f'ALTER TABLE {relation} OWNER TO "{role_by_kind["owner"]}"')
    plan = json.loads(
        await connection.fetchval(f"SELECT plan FROM {catalog.schema}.pd_dataset_migration_plan WHERE singleton")
    )
    for identity in plan["tables"].values():
        history = await connection.fetchval(
            "SELECT oid::regclass::text FROM pg_class WHERE oid=$1", identity["original_oid"]
        )
        await connection.execute(f'ALTER TABLE {history} OWNER TO "{role_by_kind["owner"]}"')
    for name, arguments in strict._CALLEE_FRONTIER.items():
        signature = storage_relation(schema, name) + "(" + ",".join("pg_catalog." + kind for kind in arguments) + ")"
        if not await connection.fetchval("SELECT to_regprocedure($1)::oid", signature):
            await connection.execute(f"CREATE FUNCTION {signature} RETURNS boolean LANGUAGE sql AS $$SELECT true$$")
            synthetic_by_kind["routines"].append(name)
    await connection.execute(f'GRANT USAGE,CREATE ON SCHEMA {catalog.schema} TO "{role_by_kind["owner"]}"')
    await connection.execute(
        f'ALTER FUNCTION {catalog.schema}.prepare_pd_generic_dataset_storage() OWNER TO "{role_by_kind["owner"]}"'
    )
    await connection.execute(
        f'GRANT EXECUTE ON FUNCTION {catalog.schema}.guard_pd_dataset_candidate_parent() TO "{role_by_kind["owner"]}"'
    )
    private_schema = "cms_private_" + uuid4().hex
    await connection.execute(f'CREATE SCHEMA "{private_schema}" AUTHORIZATION "{role_by_kind["namespace"]}"')
    for root in strict.ISOLATED_FHIR_ROOTS:
        relation = storage_relation(private_schema, root)
        await connection.execute(f"CREATE TABLE {relation}(id integer PRIMARY KEY,payload text)")
        await connection.execute(f'ALTER TABLE {relation} OWNER TO "{role_by_kind["source"]}"')
    await connection.execute(
        f'GRANT USAGE ON SCHEMA {catalog.schema},"{private_schema}" TO "{role_by_kind["publisher"]}"'
    )
    coordinates_by_field = dict(
        await connection.fetchrow(
            "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,"
            "$1::regnamespace::oid::bigint AS catalog_schema_oid,$2::regnamespace::oid::bigint AS storage_schema_oid,"
            "(SELECT oid::bigint FROM pg_roles WHERE rolname=$3) AS owner_oid,"
            "(SELECT oid::bigint FROM pg_roles WHERE rolname=$4) AS namespace_owner_oid",
            schema,
            private_schema,
            role_by_kind["source"],
            role_by_kind["namespace"],
        )
    )
    binding = FHIRSourceStorageBinding(schema, private_schema, **coordinates_by_field, enrollment_sha256="a" * 64)
    policy = GenericSourcePolicy(
        private_schema, role_by_kind["owner"], role_by_kind["publisher"], (role_by_kind["reader"],)
    )
    return binding, policy, synthetic_by_kind


@asynccontextmanager
async def _native_policy(monkeypatch):
    """Register cleanup first; retire only the exact created database and role_by_kind."""
    dsn = os.getenv("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_CUSTODY_TEST_DSN for local native policy proof")
    url = make_url(dsn)
    assert url.host in {"localhost", "127.0.0.1"} and url.port is not None and not url.query
    token = uuid4().hex
    database = "cms_policy_test_" + token
    role_by_kind = {
        kind: "cms_policy_" + kind + "_" + token[:16]
        for kind in ("owner", "publisher", "source", "namespace", "reader")
    }
    registered_roles, has_attempted_database, publisher = [], False, None
    admin = await asyncpg.connect(dsn, command_timeout=10)
    try:
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
        for role in role_by_kind.values():
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        has_attempted_database = True
        await admin.execute('CREATE DATABASE "' + database + '" TEMPLATE template0')
        for kind, role in role_by_kind.items():
            registered_roles.append(role)
            await admin.execute('CREATE ROLE "' + role + '" ' + ("LOGIN" if kind == "publisher" else "NOLOGIN"))
        monkeypatch.setenv(
            "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN",
            url.set(drivername="postgresql+asyncpg", database=database).render_as_string(hide_password=False),
        )
        async with _lifecycle_scope(monkeypatch, request_failure_budget=False) as catalog:
            binding, policy, synthetic_by_kind = await _supplement_catalog(catalog, role_by_kind)
            publisher = await asyncpg.connect(
                url.set(database=database, username=role_by_kind["publisher"]).render_as_string(hide_password=False)
            )
            try:
                yield SimpleNamespace(
                    catalog=catalog,
                    publisher=publisher,
                    binding=binding,
                    policy=policy,
                    roles=role_by_kind,
                    synthetic=synthetic_by_kind,
                    database=database,
                )
            finally:
                await publisher.close()
                await catalog.connection.execute('DROP SCHEMA "' + binding.storage_schema + '" CASCADE')
    finally:
        if publisher is not None and not publisher.is_closed():
            await publisher.close()
        if has_attempted_database:
            await admin.execute('DROP DATABASE IF EXISTS "' + database + '" WITH (FORCE)')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", database)
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
        for role in reversed(registered_roles):
            await admin.execute('DROP ROLE IF EXISTS "' + role + '"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE usename=ANY($1::text[]))", registered_roles
        )
        await admin.close()


async def _grant_observer_reads(case):
    """The observer needs explicit reads of the plan and every fixed private template."""
    for schema, roots in (
        (case.binding.catalog_schema, stable.ROOTS),
        (case.binding.storage_schema, strict.ISOLATED_FHIR_ROOTS),
    ):
        for root in roots:
            await case.catalog.connection.execute(
                f'GRANT SELECT ON {storage_relation(schema, root)} TO "{case.roles["publisher"]}"'
            )


async def _insert(case, dataset):
    await case.catalog.connection.execute(
        f"INSERT INTO {case.catalog.schema}.provider_directory_endpoint_dataset"
        "(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) "
        "VALUES($1,'endpoint-official','acquiring',false,0,'{}')",
        dataset,
    )


async def _observe(case, retained_oids=()):
    async with case.publisher.transaction(readonly=True):
        return await stable._observe(case.publisher, case.binding, case.policy, retained_oids)


def _profile(case, evidence, retained_oids):
    return dict(
        contract=stable.CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(case.binding),
        policy=json.loads(json.dumps(asdict(case.policy))),
        fixed_descendant_oids=sorted(retained_oids),
        **evidence,
    )


async def _compare(case, profile):
    async with case.publisher.transaction(readonly=True):
        return await stable.compare_cms_preparation_policy(
            case.publisher, storage_binding=case.binding, policy=case.policy, cms_profile=profile
        )


@pytest.mark.asyncio
async def test_native_policy_stays_exact_across_own_and_disjoint_allocations(monkeypatch):
    """Prove real allocation invariance and original publisher privilege requirements."""
    async with _native_policy(monkeypatch) as case:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await _observe(case)
        await _grant_observer_reads(case)
        private = storage_relation(case.binding.storage_schema, strict.ISOLATED_FHIR_ROOTS[0])
        await case.catalog.connection.execute(f'REVOKE SELECT ON {private} FROM "{case.roles["publisher"]}"')
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await _observe(case)
        await case.catalog.connection.execute(f'GRANT SELECT ON {private} TO "{case.roles["publisher"]}"')
        before = await _observe(case)
        async with case.catalog.connection.transaction():
            old_before = await strict._observe(case.catalog.connection, case.binding, case.policy)
        profile = _profile(case, before, [])
        for dataset in ("synthetic-owned-cms", "synthetic-disjoint-cms"):
            await _insert(case, dataset)
            assert await _observe(case) == before
            assert await _compare(case, profile) == before
            async with case.catalog.connection.transaction():
                old_after = await strict._observe(case.catalog.connection, case.binding, case.policy)
            assert old_after["canonical_sha256"] != old_before["canonical_sha256"]
            assert old_after["roles_sha256"] != old_before["roles_sha256"]
        assert await case.publisher.fetchval("SELECT session_user=current_user") is True


@pytest.mark.asyncio
async def test_native_policy_refuses_plan_history_root_and_fixed_descendant_drift(monkeypatch):
    """Committed independent metadata drift refuses; exact restoration recovers."""
    async with _native_policy(monkeypatch) as case:
        await _grant_observer_reads(case)
        await _insert(case, "synthetic-fixed-cms")
        family = await strict._inventory(case.catalog.connection, case.binding.catalog_schema, TABLES[:4])
        retained_oids = [entry["oid"] for entry in family if entry["relname"].startswith("pd_generic_")]
        assert len(retained_oids) == 4
        baseline = await _observe(case, retained_oids)
        profile = _profile(case, baseline, retained_oids)
        plan_table = storage_relation(case.binding.catalog_schema, stable.PLAN)
        original_plan = await case.catalog.connection.fetchval(f"SELECT plan FROM {plan_table} WHERE singleton")
        plan = json.loads(original_plan)
        history = await case.catalog.connection.fetchval(
            "SELECT oid::regclass::text FROM pg_class WHERE oid=$1", plan["tables"][TABLES[0]]["original_oid"]
        )
        child = await case.catalog.connection.fetchval(
            "SELECT oid::regclass::text FROM pg_class WHERE oid=$1", retained_oids[0]
        )
        header = storage_relation(case.binding.catalog_schema, "provider_directory_endpoint_dataset")
        faults = [
            (
                f"UPDATE {plan_table} SET plan=jsonb_set(plan,'{{native_test_marker}}','true') WHERE singleton",
                f"UPDATE {plan_table} SET plan=$1::jsonb WHERE singleton",
                (original_plan,),
            ),
            (
                f"ALTER TABLE {history} SET (autovacuum_enabled=false)",
                f"ALTER TABLE {history} RESET (autovacuum_enabled)",
                (),
            ),
            (
                f"ALTER TABLE {header} SET (autovacuum_enabled=false)",
                f"ALTER TABLE {header} RESET (autovacuum_enabled)",
                (),
            ),
            (
                f"ALTER TABLE {child} SET (autovacuum_enabled=false)",
                f"ALTER TABLE {child} RESET (autovacuum_enabled)",
                (),
            ),
        ]
        for mutate, restore, arguments in faults:
            await case.catalog.connection.execute(mutate)
            try:
                with pytest.raises(ValueError, match="native_evidence_changed"):
                    await _compare(case, profile)
            finally:
                await case.catalog.connection.execute(restore, *arguments)
            assert await _compare(case, profile) == baseline
