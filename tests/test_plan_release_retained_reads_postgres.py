# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Restricted-reader parity across the structural-only migration boundary."""

import hashlib
import json
import secrets
from contextlib import asynccontextmanager
from unittest.mock import Mock

import asyncpg
import pytest
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from api import plan_release_serving as serving
from api import plan_release_serving_resolution as resolution
from api.plan_release_pricing_projection import plan_release_serving_queries, plan_release_serving_sql
from api.plan_release_read_storage import is_retained_binding_read
from db.plan_release_storage import storage_identifier
from process import registry_pricing_release_binding_lookup as lookup
from tests.plan_release_storage_postgres_support import NEW_REVISION, OLD_REVISION, PLAN, RELEASE, storage_case


@asynccontextmanager
async def retained_case(monkeypatch, *, reader_mode="single"):
    async with storage_case(migrated=False) as case:
        admin = await case.connect()
        await admin.execute(f"DELETE FROM {case.relation('plan_release_snapshot_binding')} WHERE binding_ordinal>=2")
        await admin.execute(f"UPDATE {case.relation('plan_release_serving_revision')} SET expected_binding_count=2")
        # This is the existing reader enrollment, before the structural migration.
        await admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{case.schema}" TO "{case.reader}"')
        if reader_mode != "single":
            role = case.reader + ("_other" if reader_mode == "two" else "_group")
            has_login = reader_mode == "two"
            password = ""
            if has_login:
                case.role_passwords[role] = secrets.token_hex(32)
                password = " PASSWORD " + await admin.fetchval(
                    "SELECT quote_literal($1::text)", case.role_passwords[role]
                )
            case.created_roles.append(role)
            await admin.execute(f'CREATE ROLE "{role}" ' + ("LOGIN" if has_login else "NOLOGIN") + password)
            await admin.execute(f'GRANT USAGE ON SCHEMA "{case.schema}" TO "{role}"')
            await admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{case.schema}" TO "{role}"')
            if not has_login:
                await admin.execute(f'GRANT "{role}" TO "{case.reader}"')
                await admin.execute(f'REVOKE SELECT ON ALL TABLES IN SCHEMA "{case.schema}" FROM "{case.reader}"')
        reader = await case.connect(case.reader)
        query = plan_release_serving_sql(case.schema, include_pricing_projection=False, retained=True)
        query = query.replace(
            f"ONLY {case.schema}.plan_release_snapshot_binding_legacy", f"{case.schema}.plan_release_snapshot_binding"
        )
        query = query.replace(":plan_release_id", "$1").replace(":pin_owner_type", "$2")
        before = await reader.fetch(query, RELEASE, "plan_release_serving_revision")
        await case.migrate()
        for module in (serving, resolution, lookup):
            monkeypatch.setattr(module, "PTG2_SCHEMA", case.schema)
        full, plain = plan_release_serving_queries(case.schema)
        monkeypatch.setattr(serving, "_PLAN_RELEASE_SERVING_SQL", full)
        monkeypatch.setattr(serving, "_PLAN_RELEASE_SERVING_WITHOUT_PROJECTION_SQL", plain)
        monkeypatch.setattr(
            resolution,
            "_PLAN_RELEASE_EXISTS_SQL",
            resolution._PLAN_RELEASE_EXISTS_SQL.replace("mrf.", case.schema + "."),
        )
        monkeypatch.setattr(resolution, "_PLAN_RELEASE_GUARD_SQL", plain)
        monkeypatch.setattr(resolution, "_PLAN_RELEASE_SERVING_SQL", plain + "\n LIMIT 9")
        yield case, admin, reader, before


@asynccontextmanager
async def reader_session(case, role=None, *, isolation_level="REPEATABLE READ"):
    role = role or case.reader
    url = URL.create(
        "postgresql+asyncpg",
        username=role,
        password=case.role_passwords[role],
        host=case.config["host"],
        port=case.config["port"],
        database=case.config["database"],
    )
    engine = create_async_engine(url, isolation_level=isolation_level)
    try:
        async with engine.connect() as connection:
            async with connection.begin():
                await connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                assert tuple((await connection.exec_driver_sql("SELECT session_user,current_user")).one()) == (
                    role,
                    role,
                )
                yield connection
    finally:
        await engine.dispose()


async def _catalog(admin, case):
    return await admin.fetch(
        "SELECT oid,relname,relowner,relacl::text FROM pg_class WHERE relnamespace=$1::regnamespace ORDER BY oid",
        case.schema,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reader_mode", ["single", "two", "inherited"])
async def test_retained_readers_resolve_the_exact_pre_migration_release_without_grants(monkeypatch, reader_mode):
    async with retained_case(monkeypatch, reader_mode=reader_mode) as (case, admin, reader, before):
        pin = case.relation("ptg2_snapshot_pin")
        await admin.execute(f'GRANT SELECT ON {pin} TO "{case.publisher}"')
        async with reader.transaction(readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is True
        await admin.execute(f'GRANT INSERT,UPDATE,DELETE ON {pin} TO "{case.publisher}"')
        catalog = await _catalog(admin, case)
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is True
            bulk = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
            assert serving._selection_from_rows(RELEASE, bulk) == serving._selection_from_rows(RELEASE, before)
        async with reader_session(case) as session:
            guard = await resolution.resolve_plan_release_guard_selection(session, RELEASE)
            full = await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True)
            capped = await resolution.resolve_plan_release_serving_resolution(session, RELEASE, projection_only=True)
            assert guard == full == capped.selection == serving._selection_from_rows(RELEASE, before)
            assert capped.state == "ready" and full.binding_set_digest == "a" * 64 and len(full.bindings) == 2
        if reader_mode == "two":
            other = await case.connect(case.reader + "_other")
            async with other.transaction(isolation="repeatable_read", readonly=True):
                assert await is_retained_binding_read(other.fetch, case.schema) is True
                bulk = await lookup._fetch_release_binding_rows(other, [RELEASE], 1024 * 1024)
                assert serving._selection_from_rows(RELEASE, bulk) == serving._selection_from_rows(RELEASE, before)
            async with reader_session(case, case.reader + "_other") as session:
                assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) == full
        assert await _catalog(admin, case) == catalog
        for name in ("plan_release_snapshot_binding", "plan_release_snapshot_pin"):
            assert not await reader.fetchval(
                "SELECT has_table_privilege(current_user,$1,'SELECT')", case.relation(name)
            )


@pytest.mark.asyncio
async def test_provisioned_roots_keep_the_normal_reader_path(monkeypatch):
    async with retained_case(monkeypatch) as (case, admin, reader, before):
        await case.provision()
        async with reader.transaction(readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is False
        async with reader_session(case) as session:
            assert await resolution.resolve_plan_release_guard_selection(
                session, RELEASE
            ) == serving._selection_from_rows(RELEASE, before)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "one_root_grant",
        "root_column_grant",
        "root_owner",
        "detached",
        "bound",
        "foreign_key",
        "legacy_write",
        "pin_write",
        "pin_read",
        "root_shape",
        "root_check",
        "other_reader_write",
        "public_reader",
        "reader_grantable",
    ],
)
async def test_retained_storage_drift_is_refused_without_fallback(monkeypatch, damage):
    reader_mode = "two" if damage == "other_reader_write" else "single"
    async with retained_case(monkeypatch, reader_mode=reader_mode) as (case, admin, reader, _before):
        legacy = case.relation("plan_release_snapshot_binding_legacy")
        binding = case.relation("plan_release_snapshot_binding")
        pin = case.relation("ptg2_snapshot_pin")
        statements_by_damage = {
            "one_root_grant": f'GRANT SELECT ON {binding} TO "{case.reader}"',
            "root_column_grant": f'GRANT SELECT(snapshot_id) ON {binding} TO "{case.reader}"',
            "root_owner": f'ALTER TABLE {binding} OWNER TO "{case.owner}"',
            "detached": f"ALTER TABLE {binding} DETACH PARTITION {legacy}",
            "bound": f"ALTER TABLE {legacy} DROP CONSTRAINT plan_release_legacy_cohort_bound",
            "foreign_key": f"ALTER TABLE {legacy} DROP CONSTRAINT plan_release_snapshot_binding_revision_fkey",
            "legacy_write": f'GRANT UPDATE(snapshot_id) ON {legacy} TO "{case.reader}"',
            "pin_write": f'GRANT DELETE ON {pin} TO "{case.reader}"',
            "pin_read": f'REVOKE SELECT ON {pin} FROM "{case.reader}"',
            "root_shape": f"ALTER TABLE {case.relation('plan_release_snapshot_pin')} ALTER COLUMN reason SET DEFAULT 'changed'",
            "root_check": f"ALTER TABLE {case.relation('plan_release_snapshot_pin')} ADD CHECK(snapshot_id <> 'changed')",
            "other_reader_write": f'GRANT UPDATE(snapshot_id) ON {legacy} TO "{case.reader}_other"',
            "public_reader": f"GRANT SELECT ON {legacy} TO PUBLIC",
            "reader_grantable": f'GRANT SELECT ON {legacy} TO "{case.reader}" WITH GRANT OPTION',
        }
        await admin.execute(statements_by_damage[damage])
        catalog = await _catalog(admin, case)
        async with reader.transaction(readonly=True):
            with pytest.raises((ValueError, asyncpg.InsufficientPrivilegeError)):
                await is_retained_binding_read(reader.fetch, case.schema)
        assert await _catalog(admin, case) == catalog


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["unpinned", "not_current", "incomplete", "unpublished_snapshot", "outside_legacy"])
async def test_retained_release_semantics_still_fail_closed(monkeypatch, damage):
    async with retained_case(monkeypatch) as (case, admin, _reader, _before):
        header = case.relation("plan_release_serving_revision")
        if damage == "outside_legacy":
            await admin.execute(f"UPDATE {header} SET is_current=false")
            await admin.execute(
                f"INSERT INTO {header}(serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,is_current,expected_binding_count,binding_set_digest) VALUES($1,$2,$3,'2026-10','published','published',true,2,$4)",
                NEW_REVISION,
                RELEASE,
                PLAN,
                "b" * 64,
            )
        else:
            statements_by_damage = {
                "unpinned": f"DELETE FROM {case.relation('ptg2_snapshot_pin')}",
                "not_current": f"UPDATE {header} SET is_current=false",
                "incomplete": f"UPDATE {header} SET expected_binding_count=3",
                "unpublished_snapshot": f"UPDATE {case.relation('ptg2_snapshot')} SET status='candidate'",
            }
            await admin.execute(statements_by_damage[damage])
        async with reader_session(case) as session:
            assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) is None
            assert await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True) is None
            assert (
                await resolution.resolve_plan_release_serving_resolution(session, RELEASE, projection_only=True)
            ).state == "unavailable"


@pytest.mark.asyncio
async def test_retained_reader_holds_the_original_leaf_until_transaction_end(monkeypatch):
    async with retained_case(monkeypatch) as (case, admin, reader, _before):
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await is_retained_binding_read(reader.fetch, case.schema) is True
            await admin.execute("SET lock_timeout='100ms'")
            with pytest.raises(asyncpg.LockNotAvailableError):
                await admin.execute(
                    f"ALTER TABLE {case.relation('plan_release_snapshot_binding')} DETACH PARTITION {case.relation('plan_release_snapshot_binding_legacy')}"
                )
            assert (
                await reader.fetchval(f"SELECT count(*) FROM {case.relation('plan_release_snapshot_binding_legacy')}")
                == 2
            )
        await admin.execute(
            f"ALTER TABLE {case.relation('plan_release_snapshot_binding')} DETACH PARTITION {case.relation('plan_release_snapshot_binding_legacy')}"
        )


async def _create_source_tables(case, admin, registry, namespace_owner, builder):
    await admin.execute(f"""
CREATE TABLE {registry}.catalog_runtime_manifest(manifest_key text PRIMARY KEY,policy jsonb NOT NULL);
CREATE TABLE {registry}.hp_plan_release(plan_release_id text PRIMARY KEY,healthporta_plan_id text NOT NULL,
 plan_version_id text,release_month text NOT NULL,expected_binding_count integer NOT NULL,status text NOT NULL);
CREATE TABLE {registry}.hp_plan_serving_revision(serving_revision_id text PRIMARY KEY,plan_release_id text NOT NULL,
 physical_digest text NOT NULL,node_id text NOT NULL,binding_count integer NOT NULL,complete boolean NOT NULL,status text NOT NULL);
CREATE TABLE {registry}.hp_plan_current_release(healthporta_plan_id text PRIMARY KEY,plan_release_id text NOT NULL,
 serving_revision_id text NOT NULL,generation bigint NOT NULL);
CREATE TABLE {registry}.hp_plan_release_binding(
 release_binding_id varchar(64) NOT NULL,plan_release_id varchar(64) NOT NULL,serving_revision_id varchar(64) NOT NULL,
 role varchar(64) NOT NULL,ordinal integer NOT NULL,required boolean NOT NULL,node_id varchar(64) NOT NULL,
 source_key varchar(128) NOT NULL,snapshot_id varchar(96) NOT NULL,source_file_import_id varchar(64),
 received_installation_id uuid,custom_plan_component_id varchar(64),plan_id text NOT NULL,market_type varchar(64) NOT NULL,
 logical_content_id varchar(128),metadata_json jsonb,PRIMARY KEY(serving_revision_id,release_binding_id),
 UNIQUE(serving_revision_id,role,ordinal),CHECK(ordinal>=0),CHECK(role IN ('in_network','allowed_amounts')),
 CHECK((source_file_import_id IS NOT NULL)<>(received_installation_id IS NOT NULL))) PARTITION BY LIST(serving_revision_id);
""")
    owner_by_relation = {
        "catalog_runtime_manifest": namespace_owner,
        "hp_plan_release": namespace_owner,
        "hp_plan_current_release": namespace_owner,
        "hp_plan_serving_revision": builder,
        "hp_plan_release_binding": case.owner,
    }
    for table, owner in owner_by_relation.items():
        await admin.execute(f'ALTER TABLE {registry}.{table} OWNER TO "{owner}"')
    await admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {registry} TO "{case.reader}"')


def _source_binding_payloads(before):
    payloads = []
    for binding_row in before:
        payloads.append(
            {
                "ordinal": binding_row["binding_ordinal"],
                "role": binding_row["role"],
                "snapshot_id": binding_row["snapshot_id"],
                "source_key": binding_row["source_key"],
                "plan_id": binding_row["plan_id"],
                "market_type": binding_row["plan_market_type"] or "",
                "required": binding_row["required"],
                "node_id": "test-node",
                "source_file_import_id": "test-import",
                "logical_content_id": None,
                "received_installation_id": None,
                "custom_plan_component_id": None,
            }
        )
    return payloads


async def _insert_source_revision(case, admin, registry, revision_id, payloads):
    child = "catalog_release_binding_" + hashlib.sha256(revision_id.encode()).hexdigest()[:32]
    await admin.execute(
        f"CREATE TABLE {registry}.{child} PARTITION OF {registry}.hp_plan_release_binding "
        f"FOR VALUES IN ('{revision_id}')"
    )
    await admin.execute(f'ALTER TABLE {registry}.{child} OWNER TO "{case.owner}"')
    await admin.execute(f'GRANT SELECT ON {registry}.{child} TO "{case.reader}"')
    await admin.execute(
        f"INSERT INTO {registry}.hp_plan_serving_revision VALUES($1,$2,$3,'test-node',2,true,'ready')",
        revision_id,
        RELEASE,
        "a" * 64,
    )
    for entry in payloads:
        await admin.execute(
            f"INSERT INTO {registry}.hp_plan_release_binding VALUES($1,$2,$3,$4,$5,$6,'test-node',"
            "$7,$8,'test-import',NULL,NULL,$9,$10,NULL,NULL)",
            "binding-" + str(entry["ordinal"]),
            RELEASE,
            revision_id,
            entry["role"],
            entry["ordinal"],
            entry["required"],
            entry["source_key"],
            entry["snapshot_id"],
            entry["plan_id"],
            entry["market_type"],
        )


@asynccontextmanager
async def canonical_case(monkeypatch, *, control_shape="installed", mixed_case=False, provisioned_roots=False):
    """Model enrolled mixed owners; ordinary Reader receives no new authority."""
    from api import plan_release_source_bindings as source

    async with retained_case(monkeypatch) as (case, admin, reader, before):
        if provisioned_roots:
            await case.provision()
        registry_name = case.preparation_schema + ("_Registry" if mixed_case else "_registry")
        registry = storage_identifier(registry_name)
        namespace_owner, builder = case.owner + "_namespace", case.owner + "_writer"
        for role in (namespace_owner, builder):
            await admin.execute(
                f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
            )
            case.created_roles.append(role)
        is_created = False
        try:
            await admin.execute(f'CREATE SCHEMA {registry} AUTHORIZATION "{namespace_owner}"')
            is_created = True
            monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", registry_name)
            await admin.execute(f'GRANT USAGE ON SCHEMA {registry} TO "{case.reader}","{case.owner}"')
            for table in (
                "plan_release_serving_revision",
                "plan_release_snapshot_binding",
                "plan_release_snapshot_binding_legacy",
                "plan_release_snapshot_pin",
                "ptg2_snapshot_pin",
            ):
                await admin.execute(f'ALTER TABLE {case.relation(table)} OWNER TO "{builder}"')
            await _create_source_tables(case, admin, registry, namespace_owner, builder)
            if control_shape == "constructor":
                await admin.execute(f'ALTER TABLE {registry}.hp_plan_serving_revision OWNER TO "{namespace_owner}"')
            await admin.execute(
                f"INSERT INTO {registry}.catalog_runtime_manifest VALUES('runtime',$1::jsonb)",
                json.dumps(
                    {
                        "owner_role": namespace_owner,
                        "preparation_owner_role": case.owner,
                        "runtime_roles": [case.reader],
                    }
                ),
            )
            await admin.execute(
                f"INSERT INTO {registry}.hp_plan_release VALUES($1,$2,NULL,'2026-10',2,'published')", RELEASE, PLAN
            )
            payloads = _source_binding_payloads(before)
            for revision_id in (OLD_REVISION, NEW_REVISION):
                await _insert_source_revision(case, admin, registry, revision_id, payloads)
            await admin.execute(
                f"INSERT INTO {registry}.hp_plan_current_release VALUES($1,$2,$3,1)", PLAN, RELEASE, OLD_REVISION
            )
            await admin.execute(
                f"UPDATE {case.relation('plan_release_serving_revision')} SET source_manifest=$1::jsonb",
                json.dumps({"binding_storage": source.SOURCE_BINDING_STORAGE, "bindings": payloads}),
            )
            yield case, admin, reader, registry, before, payloads
        finally:
            if is_created:
                await admin.execute(f"DROP SCHEMA {registry} CASCADE")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control_shape,mixed_case", [("installed", False), ("constructor", False), ("installed", True)]
)
async def test_canonical_reader_matches_full_bulk_and_pins_original_revision_across_successor(
    monkeypatch, control_shape, mixed_case
):
    import json

    async with canonical_case(monkeypatch, control_shape=control_shape, mixed_case=mixed_case) as (
        case,
        admin,
        reader,
        registry,
        before,
        payloads,
    ):
        assert await admin.fetchval("SELECT to_regclass($1)", registry + ".hp_plan") is None
        expected = serving._selection_from_rows(RELEASE, before)
        async with reader_session(case) as session:
            selected = await resolution.resolve_plan_release_guard_selection(session, RELEASE)
            full = await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True)
            assert selected == full == expected
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            bulk = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
            assert serving._selection_from_rows(RELEASE, bulk) == expected
        async with reader_session(case) as session:
            admitted = await resolution.resolve_plan_release_guard_selection(session, RELEASE)
            async with admin.transaction():
                await admin.execute(
                    f"UPDATE {registry}.hp_plan_current_release SET serving_revision_id=$1,generation=2", NEW_REVISION
                )
                await admin.execute(
                    f"UPDATE {registry}.hp_plan_serving_revision SET status='superseded' WHERE serving_revision_id=$1",
                    OLD_REVISION,
                )
                await admin.execute(
                    f"UPDATE {case.relation('plan_release_serving_revision')} SET is_current=false,serving_status='retired'"
                )
                await admin.execute(
                    f"INSERT INTO {case.relation('plan_release_serving_revision')}"
                    "(serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,"
                    "is_current,expected_binding_count,binding_set_digest,source_manifest) "
                    "VALUES($1,$2,$3,'2026-10','published','published',true,2,$4,$5::jsonb)",
                    NEW_REVISION,
                    RELEASE,
                    PLAN,
                    "a" * 64,
                    json.dumps({"binding_storage": "source-plan-release-bindings-v1", "bindings": payloads}),
                )
            retained = await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True)
            assert retained == admitted == expected
        for table in ("plan_release_snapshot_binding", "plan_release_snapshot_pin"):
            assert not await reader.fetchval(
                "SELECT has_table_privilege(current_user,$1,'SELECT')", case.relation(table)
            )


@pytest.mark.asyncio
async def test_retired_canonical_release_is_unavailable_only_to_fresh_readers(monkeypatch):
    async with canonical_case(monkeypatch) as (case, admin, reader, registry, before, _payloads):
        expected = serving._selection_from_rows(RELEASE, before)
        async with reader_session(case) as pinned:
            assert await resolution.resolve_plan_release_guard_selection(pinned, RELEASE) == expected
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                before_rows = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
                assert serving._selection_from_rows(RELEASE, before_rows) == expected
                async with admin.transaction():
                    await admin.execute(
                        f"DELETE FROM {registry}.hp_plan_current_release WHERE healthporta_plan_id=$1", PLAN
                    )
                    await admin.execute(
                        f"UPDATE {registry}.hp_plan_serving_revision SET status='superseded' WHERE serving_revision_id=$1",
                        OLD_REVISION,
                    )
                    await admin.execute(
                        f"UPDATE {case.relation('plan_release_serving_revision')} "
                        "SET is_current=false,serving_status='retired' WHERE serving_revision_id=$1",
                        OLD_REVISION,
                    )
                assert await serving.resolve_plan_release_serving(pinned, RELEASE, projection_only=True) == expected
                admitted = await resolution.resolve_plan_release_serving_resolution(
                    pinned, RELEASE, projection_only=True
                )
                assert admitted.state == "ready" and admitted.selection == expected
                retained_rows = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
                assert serving._selection_from_rows(RELEASE, retained_rows) == expected
            async with reader_session(case) as fresh:
                assert await resolution.resolve_plan_release_guard_selection(fresh, RELEASE) is None
                assert await serving.resolve_plan_release_serving(fresh, RELEASE, projection_only=True) is None
                unavailable = await resolution.resolve_plan_release_serving_resolution(
                    fresh, RELEASE, projection_only=True
                )
                assert unavailable.state == "unavailable" and unavailable.selection is None
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                fresh_rows = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
                assert fresh_rows == []
                assert lookup._metadata_by_release_id([RELEASE], fresh_rows) == {RELEASE: None}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("damage", "expected"),
    [
        ("partial_tuple", "plan_release_source_binding_incomplete"),
        ("logical_type", "plan_release_source_binding_incomplete"),
        ("child_write", "plan_release_source_binding_custody"),
        ("marker", "plan_release_binding_storage_invalid"),
        ("header_owner", "plan_release_storage_retained_reader_cohort_changed"),
    ],
)
async def test_canonical_reader_rejects_incomplete_or_unprotected_cohort_without_legacy_fallback(
    monkeypatch, damage, expected
):
    async with canonical_case(monkeypatch) as (case, admin, reader, registry, before, payloads):
        await _damage_source_cohort(case, admin, registry, damage, payloads)
        async with reader_session(case) as session:
            with pytest.raises(ValueError) as caught:
                await resolution.resolve_plan_release_guard_selection(session, RELEASE)
            assert str(caught.value) == expected
        assert not await reader.fetchval(
            "SELECT has_table_privilege(current_user,$1,'SELECT')", case.relation("plan_release_snapshot_binding")
        )


@pytest.mark.asyncio
async def test_unpinned_canonical_cohort_is_unavailable_without_building_a_legacy_query(monkeypatch):
    async with canonical_case(monkeypatch) as (case, admin, reader, registry, before, payloads):
        await admin.execute(f"DELETE FROM {case.relation('ptg2_snapshot_pin')}")
        async with reader_session(case) as session:
            assert await is_retained_binding_read(reader.fetch, case.schema) is True
            legacy_query = Mock(side_effect=AssertionError("legacy fallback is forbidden"))
            monkeypatch.setattr(resolution, "plan_release_serving_sql", legacy_query)
            assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) is None
            legacy_query.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["parent", "child"])
async def test_bulk_claimed_source_missing_relation_refuses_and_recovers_savepoint(monkeypatch, missing):
    async with canonical_case(monkeypatch) as (case, admin, reader, registry, before, payloads):
        relation = registry + ".hp_plan_release_binding"
        if missing == "child":
            relation = registry + ".catalog_release_binding_" + hashlib.sha256(OLD_REVISION.encode()).hexdigest()[:32]
        await admin.execute(f"DROP TABLE {relation} CASCADE")
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(ValueError, match="plan_release_source_binding_storage_missing") as caught:
                await lookup._optional_release_binding_rows(reader, [RELEASE], 1024 * 1024)
            assert isinstance(caught.value.__cause__, asyncpg.UndefinedTableError)
            assert await reader.fetchval("SELECT 1") == 1


async def _damage_source_cohort(case, admin, registry, damage, payloads):
    if damage == "header_owner":
        await admin.execute(f'ALTER TABLE {case.relation("plan_release_serving_revision")} OWNER TO "{case.owner}"')
        return
    if damage == "partial_tuple":
        await admin.execute(
            f"UPDATE {registry}.hp_plan_release_binding SET plan_id='drift' WHERE ordinal=1 AND serving_revision_id=$1",
            OLD_REVISION,
        )
    elif damage == "logical_type":
        await admin.execute(
            f"UPDATE {registry}.hp_plan_release_binding SET logical_content_id='123' WHERE serving_revision_id=$1",
            OLD_REVISION,
        )
        for binding_payload in payloads:
            binding_payload["logical_content_id"] = 123
        await admin.execute(
            f"UPDATE {case.relation('plan_release_serving_revision')} SET source_manifest=$1::jsonb",
            json.dumps({"binding_storage": "source-plan-release-bindings-v1", "bindings": payloads}),
        )
    elif damage == "child_write":
        import hashlib

        child = "catalog_release_binding_" + hashlib.sha256(OLD_REVISION.encode()).hexdigest()[:32]
        await admin.execute(f'GRANT UPDATE ON {registry}.{child} TO "{case.reader}"')
    elif damage == "marker":
        await admin.execute(
            f"UPDATE {case.relation('plan_release_serving_revision')} SET source_manifest=jsonb_set(source_manifest,'{{binding_storage}}','null')"
        )
    else:
        raise AssertionError("unknown canonical cohort damage")


async def _prepare_legacy_successor(case, admin, registry, next_release):
    """Keep the original retained family intact and stage a distinct next month."""
    owner_sql = await admin.fetchval(
        "SELECT quote_ident(pg_get_userbyid(relowner)) FROM pg_class WHERE oid=$1::regclass",
        case.relation("plan_release_snapshot_binding"),
    )
    await admin.execute(f"ALTER TABLE {case.relation('plan_release_serving_revision')} OWNER TO {owner_sql}")
    await admin.execute(f"UPDATE {case.relation('plan_release_serving_revision')} SET source_manifest='{{}}'")
    await admin.execute(f"DELETE FROM {registry}.hp_plan_release_binding WHERE serving_revision_id=$1", OLD_REVISION)
    await admin.execute(
        f"INSERT INTO {registry}.hp_plan_release VALUES($1,$2,NULL,'2026-11',2,'published')", next_release, PLAN
    )
    for table in ("hp_plan_serving_revision", "hp_plan_release_binding"):
        await admin.execute(
            f"UPDATE {registry}.{table} SET plan_release_id=$1 WHERE serving_revision_id=$2", next_release, NEW_REVISION
        )


async def _publish_legacy_successor(case, admin, registry, next_release, payloads):
    """Commit the header/current/pin effects together, without changing storage ACLs."""
    async with admin.transaction():
        await admin.execute(
            f"UPDATE {registry}.hp_plan_current_release SET plan_release_id=$1,serving_revision_id=$2,generation=2",
            next_release,
            NEW_REVISION,
        )
        await admin.execute(
            f"UPDATE {registry}.hp_plan_serving_revision SET status='superseded' WHERE serving_revision_id=$1",
            OLD_REVISION,
        )
        await admin.execute(
            f"UPDATE {registry}.hp_plan_release SET status='superseded' WHERE plan_release_id=$1", RELEASE
        )
        await admin.execute(
            f"UPDATE {case.relation('plan_release_serving_revision')} "
            "SET is_current=false,serving_status='retired',release_status='superseded'"
        )
        await admin.execute(
            f"INSERT INTO {case.relation('plan_release_serving_revision')}"
            "(serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,"
            "is_current,expected_binding_count,binding_set_digest,source_manifest) "
            "VALUES($1,$2,$3,'2026-11','published','published',true,2,$4,$5::jsonb)",
            NEW_REVISION,
            next_release,
            PLAN,
            "a" * 64,
            json.dumps({"binding_storage": "source-plan-release-bindings-v1", "bindings": payloads}),
        )
        await admin.execute(f"DELETE FROM {case.relation('ptg2_snapshot_pin')} WHERE owner_id=$1", OLD_REVISION)
        await admin.execute(
            f"INSERT INTO {case.relation('ptg2_snapshot_pin')} "
            "VALUES('plan_release_serving_revision',$1,'old-snapshot','current',now())",
            NEW_REVISION,
        )


@pytest.mark.asyncio
async def test_request_reader_keeps_legacy_selection_after_canonical_cut(monkeypatch):
    from sanic.request.parameters import RequestParameters
    from sqlalchemy import text

    from api import plan_release_read_boundary as boundary
    from db.connection import Database

    async with canonical_case(monkeypatch, control_shape="constructor") as (
        case,
        admin,
        _reader,
        registry,
        before,
        payloads,
    ):
        next_release = "hprelease_" + "1" * 26
        await _prepare_legacy_successor(case, admin, registry, next_release)
        monkeypatch.setenv("HLTHPRT_DB_READER_USER", case.reader)
        monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", case.role_passwords[case.reader])
        database = Database()
        monkeypatch.setattr(boundary, "db", database)
        selectors = RequestParameters(
            {
                "plan_release_id": [RELEASE],
                boundary.PLAN_RELEASE_REVISION_SELECTOR: [OLD_REVISION],
                boundary.PLAN_RELEASE_DIGEST_SELECTOR: ["a" * 64],
            }
        )
        expected = serving._selection_from_rows(RELEASE, before)
        try:
            async with database.reader_session() as session:
                modes = await session.execute(
                    text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')")
                )
                assert tuple(modes.one()) == ("repeatable read", "on")
                assert await boundary.admit_bound_plan_release_read(session, selectors) is None
                await _publish_legacy_successor(case, admin, registry, next_release, payloads)
                assert (
                    await admin.fetchval(
                        f"SELECT count(*) FROM {case.relation('ptg2_snapshot_pin')} WHERE owner_id=$1", OLD_REVISION
                    )
                    == 0
                )
                assert await boundary.admit_bound_plan_release_read(session, selectors) is None
                assert await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True) == expected
                capped = await resolution.resolve_plan_release_serving_resolution(
                    session, RELEASE, projection_only=True
                )
                assert capped.state == "ready" and capped.selection == expected
            async with database.reader_session() as session:
                assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) is None
                successor = await resolution.resolve_plan_release_guard_selection(session, next_release)
                assert successor is not None and successor.serving_revision_id == NEW_REVISION
                assert successor.bindings == expected.bindings
        finally:
            await database.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("manifest_type", ["json", "jsonb"])
async def test_legacy_json_header_keeps_single_and_bulk_reader_parity(monkeypatch, manifest_type):
    async with retained_case(monkeypatch) as (case, admin, reader, before):
        await case.provision()
        header = case.relation("plan_release_serving_revision")
        await admin.execute(
            f"ALTER TABLE {header} ALTER COLUMN source_manifest TYPE {manifest_type} "
            f"USING source_manifest::{manifest_type}"
        )
        expected = serving._selection_from_rows(RELEASE, before)
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            binding_rows = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
            assert serving._selection_from_rows(RELEASE, binding_rows) == expected
        async with reader_session(case) as session:
            assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) == expected
            assert await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True) == expected
            capped = await resolution.resolve_plan_release_serving_resolution(session, RELEASE, projection_only=True)
            assert capped.state == "ready" and capped.selection == expected
    async with canonical_case(monkeypatch, provisioned_roots=True) as (
        case,
        admin,
        reader,
        _registry,
        before,
        _payloads,
    ):
        header = case.relation("plan_release_serving_revision")
        await admin.execute(
            f"ALTER TABLE {header} ALTER COLUMN source_manifest TYPE {manifest_type} "
            f"USING source_manifest::{manifest_type}"
        )
        expected = serving._selection_from_rows(RELEASE, before)
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            binding_rows = await lookup._fetch_release_binding_rows(reader, [RELEASE], 1024 * 1024)
            assert serving._selection_from_rows(RELEASE, binding_rows) == expected
        async with reader_session(case) as session:
            assert await resolution.resolve_plan_release_guard_selection(session, RELEASE) == expected
            assert await serving.resolve_plan_release_serving(session, RELEASE, projection_only=True) == expected
            capped = await resolution.resolve_plan_release_serving_resolution(session, RELEASE, projection_only=True)
            assert capped.state == "ready" and capped.selection == expected
