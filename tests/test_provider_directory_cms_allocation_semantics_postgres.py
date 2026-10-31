# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real generic allocator shape checks; no CMS issuer or live authority proof."""

import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_allocation_semantics as semantics
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_preparation_policy as stable
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_provider_directory_cms_canonical_layout_postgres import (
    _populate_genuine_allocation,
)
from tests.test_provider_directory_cms_canonical_layout_postgres import (
    native_layout as native_layout,
)
from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope


@asynccontextmanager
async def _native_allocations(monkeypatch):
    """Register exact disposable database/role cleanup before creation."""
    dsn = os.getenv("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_CUSTODY_TEST_DSN for local native allocation proof")
    url = make_url(dsn)
    assert url.host in {"localhost", "127.0.0.1"} and url.port is not None and not url.query
    token = uuid4().hex
    name, owner = "cms_semantics_test_" + token, "cms_semantics_owner_" + token[:16]
    publisher = "cms_semantics_publisher_" + token[:16]
    admin = await asyncpg.connect(dsn)
    has_attempted_database, roles = False, []
    try:
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name)
        for role in (owner, publisher):
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        has_attempted_database = True
        await admin.execute('CREATE DATABASE "' + name + '" TEMPLATE template0')
        roles.append(owner)
        await admin.execute('CREATE ROLE "' + owner + '" NOLOGIN')
        roles.append(publisher)
        await admin.execute('CREATE ROLE "' + publisher + '" LOGIN')
        monkeypatch.setenv(
            "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN",
            url.set(drivername="postgresql+asyncpg", database=name).render_as_string(hide_password=False),
        )
        async with _lifecycle_scope(monkeypatch, request_failure_budget=False) as catalog:
            await _protect_allocator(catalog, owner)
            async with _publisher_reader(catalog, url, name, publisher) as connection:
                yield SimpleNamespace(catalog=catalog, owner=owner, publisher=connection)
    finally:
        if has_attempted_database:
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", name)
            await admin.execute('DROP DATABASE IF EXISTS "' + name + '"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name)
        for role in reversed(roles):
            await admin.execute('DROP ROLE IF EXISTS "' + role + '"')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        await admin.close()


@asynccontextmanager
async def _publisher_reader(catalog, url, database, publisher):
    """Use an original login with parent reads and no child or owner authority."""
    await catalog.connection.execute(f'GRANT USAGE ON SCHEMA {catalog.schema} TO "{publisher}"')
    for root in (*TABLES[:4], "provider_directory_endpoint_dataset", "pd_dataset_migration_plan"):
        await catalog.connection.execute(
            f'GRANT SELECT ON {storage_relation(catalog.schema_name, root)} TO "{publisher}"'
        )
    connection = await asyncpg.connect(
        url.set(database=database, username=publisher).render_as_string(hide_password=False)
    )
    try:
        yield connection
    finally:
        await connection.close()


async def _protect_allocator(catalog, owner):
    """Give the real fixed allocator a disposable protected owner, without changing its body."""
    await catalog.connection.execute(f'GRANT USAGE,CREATE ON SCHEMA {catalog.schema} TO "{owner}"')
    for root in TABLES[:4]:
        await catalog.connection.execute(
            f'ALTER TABLE {storage_relation(catalog.schema_name, root)} OWNER TO "{owner}"'
        )
    await catalog.connection.execute(
        f'ALTER FUNCTION {catalog.schema}.prepare_pd_generic_dataset_storage() OWNER TO "{owner}"'
    )
    await catalog.connection.execute(
        f'GRANT EXECUTE ON FUNCTION {catalog.schema}.guard_pd_dataset_candidate_parent() TO "{owner}"'
    )


async def _insert(catalog, dataset):
    await catalog.connection.execute(
        f"INSERT INTO {catalog.schema}.provider_directory_endpoint_dataset"
        "(dataset_id,endpoint_id,status,is_current,resource_count,publication_metadata_json) "
        "VALUES($1,'endpoint-official','acquiring',false,0,'{}')",
        dataset,
    )


async def _verify(catalog, *, retained=(), connection=None):
    connection = connection or catalog.connection
    plan = json.loads(
        await connection.fetchval(f"SELECT plan FROM {catalog.schema}.pd_dataset_migration_plan WHERE singleton")
    )
    family = await custody._inventory(connection, catalog.schema_name, TABLES[:4])
    return await semantics.require_cms_dynamic_allocations(
        connection,
        catalog.schema_name,
        family=family,
        historical_oids=[plan["tables"][root]["original_oid"] for root in TABLES[:4]],
        retained_oids=retained,
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_genuine_inherited_fk_hooks_are_checked_with_allocator_guards(native_layout):
    """Real migrated FKs survive allocation; native hook drift refuses and rolls back."""
    connection = native_layout
    owner = "cms_semantics_owner_" + uuid4().hex[:16]
    catalog = SimpleNamespace(connection=connection, schema="mrf", schema_name="mrf")
    outer = connection.transaction()
    await outer.start()
    try:
        await connection.execute('CREATE ROLE "' + owner + '" NOLOGIN')
        await _protect_allocator(catalog, owner)
        await _populate_genuine_allocation(connection, "cms-inherited-hooks")
        expected = await _verify(catalog)
        assert len(expected) == 4
        child = await connection.fetchrow(
            "SELECT c.relname,t.tgname FROM pg_class c JOIN pg_trigger t ON t.tgrelid=c.oid "
            "WHERE c.oid=ANY($1::oid[]) AND t.tgisinternal ORDER BY t.oid LIMIT 1",
            expected,
        )
        assert child is not None
        table = storage_relation("mrf", child["relname"])
        hook = '"' + child["tgname"] + '"'
        commands = [
            f"ALTER TABLE ONLY {table} DISABLE TRIGGER {hook}",
            f"ALTER TABLE ONLY {table} ENABLE REPLICA TRIGGER {hook}",
            f"ALTER TABLE ONLY {table} ENABLE ALWAYS TRIGGER {hook}",
        ]
        for statement in commands:
            savepoint = connection.transaction()
            await savepoint.start()
            try:
                await connection.execute(statement)
                with pytest.raises(ValueError, match="cms_allocation_semantics_"):
                    await _verify(catalog)
            finally:
                await savepoint.rollback()
            assert await _verify(catalog) == expected
    finally:
        await outer.rollback()
    assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", owner)


@pytest.mark.asyncio(loop_scope="module")
async def test_stable_fingerprint_checks_real_fk_clones_before_projecting_them(native_layout):
    connection = native_layout
    owner = "cms_semantics_owner_" + uuid4().hex[:16]
    catalog = SimpleNamespace(connection=connection, schema="mrf", schema_name="mrf")
    outer = connection.transaction()
    await outer.start()
    try:
        await connection.execute('CREATE ROLE "' + owner + '" NOLOGIN')
        await _protect_allocator(catalog, owner)
        binding = SimpleNamespace(catalog_schema="mrf")
        policy = GenericSourcePolicy("mrf", owner, "cms_policy_publisher", ("cms_policy_reader",))
        before, _ = await stable._canonical(connection, binding, policy, [])
        await _populate_genuine_allocation(connection, "cms-stable-native-clones")
        after, family = await stable._canonical(connection, binding, policy, [])
        assert after == before
        assert len(family) == len(before["relations"]) + 4
        assert sum(len(relation["native"]["constraints"]) for relation in family) > sum(
            len(relation["native"]["constraints"]) for relation in after["relations"]
        )
        child = await connection.fetchrow(
            "SELECT c.relname,t.tgname FROM pg_class c JOIN pg_trigger t ON t.tgrelid=c.oid "
            "WHERE c.relname LIKE 'pd_generic_%' AND t.tgisinternal ORDER BY t.oid LIMIT 1"
        )
        assert child is not None
        nested = connection.transaction()
        await nested.start()
        try:
            await connection.execute(
                f'ALTER TABLE ONLY {storage_relation("mrf", child["relname"])} DISABLE TRIGGER "{child["tgname"]}"'
            )
            with pytest.raises(ValueError, match="cms_allocation_semantics_"):
                await stable._canonical(connection, binding, policy, [])
        finally:
            await nested.rollback()
        assert (await stable._canonical(connection, binding, policy, []))[0] == before
    finally:
        await outer.rollback()
    assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", owner)


@pytest.mark.asyncio
async def test_real_allocator_and_disjoint_additions_keep_existing_semantics(monkeypatch):
    async with _native_allocations(monkeypatch) as case:
        catalog = case.catalog
        await _insert(catalog, "synthetic-first")
        async with catalog.connection.transaction():
            first = await _verify(catalog)
        assert len(first) == 4
        await _insert(catalog, "synthetic-second")
        async with catalog.connection.transaction():
            second = await _verify(catalog)
        assert len(second) == 8 and set(first) < set(second)
        async with case.publisher.transaction(readonly=True):
            assert await _verify(catalog, connection=case.publisher) == second


@pytest.mark.asyncio
async def test_parent_read_grants_do_not_grant_child_lock_or_copy(monkeypatch):
    import hashlib

    async with _native_allocations(monkeypatch) as case:
        catalog, publisher = case.catalog, case.publisher
        await _insert(catalog, "synthetic-rights")
        child = "pd_generic_" + hashlib.md5((TABLES[0] + ":synthetic-rights").encode()).hexdigest()
        async with publisher.transaction(readonly=True):
            assert len(await _verify(catalog, connection=publisher)) == 4
            assert await publisher.fetchval("SELECT session_user=current_user") is True
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with publisher.transaction():
                await publisher.execute(
                    "LOCK TABLE ONLY " + storage_relation(catalog.schema_name, child) + " IN ACCESS SHARE MODE NOWAIT"
                )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with publisher.transaction():
                await publisher.copy_records_to_table(
                    child,
                    schema_name=catalog.schema_name,
                    columns=("dataset_id", "resource_type", "resource_id", "payload_hash", "payload_json"),
                    records=[("synthetic-rights", "Organization", "synthetic-org", "a" * 64, "{}")],
                )


def _fault_commands(catalog, child):
    """Exercise independent native mutations, each rolled back before the next."""
    table = storage_relation(catalog.schema_name, child)
    return [
        f"ALTER TABLE {table} ALTER COLUMN payload_json SET DEFAULT '{{}}'",
        f"ALTER TABLE {table} ALTER COLUMN payload_json SET STORAGE EXTERNAL",
        f"ALTER TABLE {table} ADD CHECK (resource_id<>'')",
        f"ALTER TABLE {table} ADD CHECK (resource_id<>'') NOT VALID",
        f"ALTER TABLE {table} DROP CONSTRAINT pd_dataset_bound, ADD CONSTRAINT pd_dataset_bound CHECK(dataset_id<>'other')",
        f"CREATE INDEX unwanted_index ON {table}(resource_id)",
        f"ALTER TABLE {table} DISABLE TRIGGER a_pd_candidate_insert_new",
        f"ALTER TABLE {table} ENABLE REPLICA TRIGGER a_pd_candidate_update_old",
        f"DROP TRIGGER a_pd_candidate_delete_old ON {table}",
        f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY",
        f"CREATE POLICY unwanted_policy ON {table} USING(true)",
        f"ALTER TABLE {table} SET (autovacuum_enabled=false)",
        f"GRANT SELECT ON {table} TO PUBLIC",
        f"GRANT UPDATE(payload_json) ON {table} TO PUBLIC",
        f"ALTER TABLE {table} OWNER TO CURRENT_USER",
        f"ALTER TABLE {table} RENAME TO unsupported_candidate",
    ]


@pytest.mark.asyncio
async def test_real_child_drift_refuses_even_with_fresh_family_capture(monkeypatch):
    import hashlib

    async with _native_allocations(monkeypatch) as case:
        catalog = case.catalog
        await _insert(catalog, "synthetic-drift")
        child = "pd_generic_" + hashlib.md5((TABLES[0] + ":synthetic-drift").encode()).hexdigest()
        for command in _fault_commands(catalog, child):
            transaction = catalog.connection.transaction()
            await transaction.start()
            try:
                await catalog.connection.execute(command)
                with pytest.raises(ValueError, match="cms_allocation_semantics_"):
                    await _verify(catalog)
            finally:
                await transaction.rollback()
        async with catalog.connection.transaction():
            assert len(await _verify(catalog)) == 4


@pytest.mark.asyncio
async def test_exact_retained_oid_is_separate_from_supported_dynamic_class(monkeypatch):
    import hashlib

    async with _native_allocations(monkeypatch) as case:
        catalog = case.catalog
        await _insert(catalog, "synthetic-retained")
        child = "pd_generic_" + hashlib.md5((TABLES[0] + ":synthetic-retained").encode()).hexdigest()
        relation = storage_relation(catalog.schema_name, child)
        oid = await catalog.connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
        await catalog.connection.execute(f"ALTER TABLE {relation} RENAME TO unsupported_retained")
        async with catalog.connection.transaction():
            assert len(await _verify(catalog, retained=(oid,))) == 3
            with pytest.raises(ValueError, match="header_changed"):
                await _verify(catalog)
            with pytest.raises(ValueError, match="retained_changed"):
                await _verify(catalog, retained=(2**32 - 1,))


async def _settings(connection):
    return dict(
        await connection.fetchrow(
            "SELECT current_setting('statement_timeout') AS statement_timeout,"
            "current_setting('lock_timeout') AS lock_timeout"
        )
    )


async def _pin_inputs(catalog, publisher, dataset):
    from process.provider_directory_cms_prepared_allocation import _inventory

    workspace = SimpleNamespace(
        connection=publisher, catalog_schema=catalog.schema_name, identity={"dataset_id": dataset}
    )
    return workspace, await _inventory(workspace)


@pytest.mark.asyncio
async def test_actual_parent_read_pin_preserves_scope_caps_and_disjoint_allocation(monkeypatch):
    from process import provider_directory_cms_allocation_pin as pin

    async with _native_allocations(monkeypatch) as case:
        catalog, publisher = case.catalog, case.publisher
        await _insert(catalog, "synthetic-pin-first")
        await _insert(catalog, "synthetic-pin-second")
        observed_settings = []
        original_locks = pin._locks

        async def observe_locks(connection, schema):
            observed_settings.append(await _settings(connection))
            return await original_locks(connection, schema)

        monkeypatch.setattr(pin, "_locks", observe_locks)
        async with publisher.transaction():
            await publisher.execute("SET LOCAL plan_cache_mode=force_generic_plan")
            workspace, inventory = await _pin_inputs(catalog, publisher, "synthetic-pin-first")
            for limit in ("0", "50ms"):
                await publisher.execute("SET LOCAL statement_timeout='1000ms'; SET LOCAL lock_timeout='" + limit + "'")
                settings = await _settings(publisher)
                for _ in range(8):
                    await pin.pin_cms_allocation_targets(workspace, inventory)
                    assert await _settings(publisher) == settings
            await _insert(catalog, "synthetic-pin-third")
            await pin.pin_cms_allocation_targets(workspace, inventory)
            expected_oids = {entry["oid"] for entry in inventory}
            assert await original_locks(publisher, catalog.schema_name) == expected_oids
            assert observed_settings and all(setting["statement_timeout"] == "1s" for setting in observed_settings)
            assert {setting["lock_timeout"] for setting in observed_settings} == {"250ms", "50ms"}
            selected = next(entry for entry in inventory if entry["relname"] not in TABLES[:4])
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with catalog.connection.transaction():
                    await catalog.connection.execute(
                        "LOCK TABLE ONLY "
                        + storage_relation(catalog.schema_name, selected["relname"])
                        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
                    )
            assert await publisher.fetchval("SELECT session_user=current_user") is True


@pytest.mark.asyncio
async def test_actual_pin_scope_failure_rolls_back_only_inner_locks_and_settings(monkeypatch):
    from process import provider_directory_cms_allocation_pin as pin

    async with _native_allocations(monkeypatch) as case:
        catalog, publisher = case.catalog, case.publisher
        await _insert(catalog, "synthetic-pin-expected")
        await _insert(catalog, "synthetic-pin-wrong")
        async with publisher.transaction():
            await publisher.fetchval(f"SELECT count(*) FROM {catalog.schema}.provider_directory_endpoint_dataset")
            await publisher.execute("SET LOCAL statement_timeout='0';SET LOCAL lock_timeout='0'")
            outer_locks, original_settings = (
                await pin._locks(publisher, catalog.schema_name),
                await _settings(publisher),
            )
            workspace, inventory = await _pin_inputs(catalog, publisher, "synthetic-pin-expected")
            workspace.identity["dataset_id"] = "synthetic-pin-wrong"
            with pytest.raises(ValueError, match="scope_changed"):
                await pin.pin_cms_allocation_targets(workspace, inventory)
            assert await pin._locks(publisher, catalog.schema_name) == outer_locks
            assert await _settings(publisher) == original_settings
            assert await publisher.fetchval("SELECT 1") == 1
