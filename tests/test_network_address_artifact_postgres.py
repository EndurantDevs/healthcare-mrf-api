# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact protected address pins survive native rotation and reject custody drift."""

import os
from contextlib import AsyncExitStack
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from process import entity_address_alias_guard as alias_guard
from process import entity_address_snapshot_adoption as adoption
from process import network_address_artifact as artifact
from process.network_address_projection import PinnedAddressSource
from tests.test_address_numeric_grid_alias_runtime_db import _create_alias_probe_schema
from tests.test_network_membership_serving_indexes_postgres import _full_source


def _admin_url():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN is not set")
    return make_url(dsn.replace("postgresql+asyncpg://", "postgresql://"))


async def _cleanup(admin, database, roles, created):
    if created["database"]:
        await admin.execute(f'DROP DATABASE IF EXISTS "{database}"')
    for role in reversed(roles):
        await admin.execute(f'DROP ROLE IF EXISTS "{role}"')
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", roles)


async def _prepare_source(connection, roles):
    schema = "address_example"
    await connection.execute("CREATE EXTENSION postgis")
    await connection.execute(f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{roles["owner"]}"')
    await _create_alias_probe_schema(connection, schema, "network_artifact")
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{roles["owner"]}"')
    for table in ("address_alias_state_v1", "address_alias_v1", "entity_address_unified"):
        await connection.execute(f'ALTER TABLE "{schema}"."{table}" OWNER TO "{roles["owner"]}"')
    for statement in alias_guard.generation_guard_statements(schema):
        await connection.execute(statement)
    for name in alias_guard.GUARDS:
        await connection.execute(f'ALTER FUNCTION "{schema}"."{name}"() OWNER TO "{roles["owner"]}"')
    await connection.execute(
        f'ALTER TABLE "{schema}".entity_address_unified '
        "ADD COLUMN entity_type varchar(64) NOT NULL DEFAULT 'npi',"
        "ADD COLUMN entity_id varchar(128) NOT NULL DEFAULT '1000000491',"
        "ADD COLUMN postal_address text,"
        "ADD COLUMN plans_network_array integer[] NOT NULL DEFAULT '{}',"
        "ADD COLUMN canonical_network_ids integer[] NOT NULL DEFAULT '{}'"
    )
    await connection.execute(
        f"INSERT INTO \"{schema}\".entity_address_unified(location_key,type,npi) VALUES('old-office','practice',1000000491)"
    )
    await connection.execute(f'CREATE INDEX address_npi ON "{schema}".entity_address_unified(npi)')
    await connection.execute(f'ALTER TABLE "{schema}".entity_address_unified RENAME TO retained_addresses')
    await _full_source(SimpleNamespace(connection=connection, control_schema=schema))
    await connection.execute(f'ALTER TABLE "{schema}".retained_addresses RENAME TO entity_address_unified')
    for table in artifact.generation.RELATION_NAMES[1:]:
        await connection.execute(f'CREATE TABLE "{schema}"."{table}" (id integer PRIMARY KEY)')
        await connection.execute(f'ALTER TABLE "{schema}"."{table}" OWNER TO "{roles["owner"]}"')
    await connection.execute(
        f'CREATE TABLE "{schema}".entity_address_result_generation('
        "singleton boolean PRIMARY KEY CHECK(singleton),local_lineage_id uuid NOT NULL,local_generation bigint NOT NULL,"
        "origin_lineage_id uuid NOT NULL,origin_generation bigint NOT NULL,published_at timestamptz NOT NULL,relation_oids bigint[] NOT NULL)"
    )
    await connection.execute(f'ALTER TABLE "{schema}".entity_address_result_generation OWNER TO "{roles["owner"]}"')
    oids = [
        await connection.fetchval("SELECT to_regclass($1)::oid::bigint", f'"{schema}"."{table}"')
        for table in artifact.generation.RELATION_NAMES
    ]
    lineage = uuid4()
    await connection.execute(
        f'INSERT INTO "{schema}".entity_address_result_generation VALUES(true,$1,1,$1,1,$2,$3)',
        lineage,
        datetime(2026, 1, 1, tzinfo=UTC),
        oids,
    )
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{roles["reader"]}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{roles["reader"]}"')
    return schema


@pytest.fixture
async def address_db():
    """Create and remove only one exact disposable database and its UUID roles."""
    url = _admin_url()
    suffix = uuid4().hex
    database = "address_artifact_" + suffix
    role_by_kind = {kind: "artifact_" + kind + "_" + suffix for kind in ("owner", "publisher", "reader", "outsider")}
    password_by_kind = {kind: uuid4().hex for kind in ("publisher", "reader", "outsider")}
    async with AsyncExitStack() as cleanup:
        admin = await asyncpg.connect(url.render_as_string(hide_password=False))
        cleanup.push_async_callback(admin.close)
        created_roles = []
        created_by_kind = {"database": False}
        cleanup.push_async_callback(_cleanup, admin, database, created_roles, created_by_kind)
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", database)
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", list(role_by_kind.values())
        )
        for kind, role in role_by_kind.items():
            login = "NOLOGIN" if kind == "owner" else f"LOGIN PASSWORD '{password_by_kind[kind]}'"
            await admin.execute(
                f'CREATE ROLE "{role}" {login} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
            )
            created_roles.append(role)
        await admin.execute(f'GRANT "{role_by_kind["owner"]}" TO "{role_by_kind["publisher"]}"')
        await admin.execute(f'CREATE DATABASE "{database}" TEMPLATE template0')
        created_by_kind["database"] = True
        connection = await asyncpg.connect(url.set(database=database).render_as_string(hide_password=False))
        cleanup.push_async_callback(connection.close)
        schema = await _prepare_source(connection, role_by_kind)
        publisher = await asyncpg.connect(
            url.set(
                database=database, username=role_by_kind["publisher"], password=password_by_kind["publisher"]
            ).render_as_string(hide_password=False)
        )
        cleanup.push_async_callback(publisher.close)
        reader_url = url.set(database=database, username=role_by_kind["reader"], password=password_by_kind["reader"])
        reader = await asyncpg.connect(reader_url.render_as_string(hide_password=False))
        cleanup.push_async_callback(reader.close)
        async with publisher.transaction(isolation="repeatable_read", readonly=True):
            authority = await artifact.generation.read_entity_address_result_generation_authority(
                artifact._CatalogSession(publisher), schema_name=schema
            )
            generation_id = artifact._digest(artifact._authority_document(authority))
            address_source = PinnedAddressSource(schema, "entity_address_unified", generation_id)
            descriptor = await artifact.capture_network_address_artifact(
                publisher, address_source, runtime_roles=(role_by_kind["reader"],)
            )
        yield SimpleNamespace(
            admin=connection,
            url=url.set(database=database),
            reader_url=reader_url,
            publisher=publisher,
            reader=reader,
            schema=schema,
            roles=role_by_kind,
            source=address_source,
            descriptor=descriptor,
        )


class _NativeDB:
    def __init__(self, connection):
        self.connection = connection
        self.session = artifact._CatalogSession(connection)

    def _transaction_binding(self):
        return SimpleNamespace(session=self.session)

    async def scalar(self, statement, **parameters):
        return await self.session.scalar(text(statement), parameters)

    async def all(self, statement, **parameters):
        return [tuple(row.values()) for row in await self.session.execute(text(statement), parameters)]

    async def status(self, statement):
        return await self.connection.execute(statement)


async def _rotate(resources, monkeypatch):
    schema = resources.schema
    await resources.admin.execute(
        f'CREATE TABLE "{schema}".entity_address_unified_next (LIKE "{schema}".entity_address_unified INCLUDING ALL)'
    )
    await resources.admin.execute(
        f'ALTER TABLE "{schema}".entity_address_unified_next OWNER TO "{resources.roles["owner"]}"'
    )
    await resources.admin.execute(
        f'GRANT SELECT ON "{schema}".entity_address_unified_next TO "{resources.roles["reader"]}"'
    )
    await resources.admin.execute(
        f'INSERT INTO "{schema}".entity_address_unified_next SELECT * FROM "{schema}".entity_address_unified'
    )
    await resources.admin.execute(f'UPDATE "{schema}".entity_address_unified_next SET location_key=$1', "new-office")
    monkeypatch.setattr(adoption.entity_address_unified, "db", _NativeDB(resources.publisher))
    async with resources.publisher.transaction():
        retained = await adoption._swap_sealed_stage_table(
            schema,
            SimpleNamespace(__main_table__="entity_address_unified"),
            SimpleNamespace(__tablename__="entity_address_unified_next", __my_additional_indexes__=[]),
            resources.descriptor["protected_owner_oid"],
        )
        oid = await resources.publisher.fetchval(
            "SELECT to_regclass($1)::oid::bigint", f'"{schema}".entity_address_unified'
        )
        await resources.publisher.execute(
            f'UPDATE "{schema}".entity_address_result_generation SET local_generation=2,origin_generation=2,relation_oids[1]=$1',
            oid,
        )
        await resources.publisher.execute(f'UPDATE "{schema}".address_alias_state_v1 SET generation=generation+1')
    return retained


async def _verify(resources):
    async with resources.reader.transaction(readonly=True):
        return await artifact.verify_network_address_artifact(
            resources.reader, resources.descriptor, runtime_roles=(resources.roles["reader"],)
        )


async def test_rotation_between_catalog_read_and_lock_rejects_replacement_name(address_db, monkeypatch):
    class RotatingConnection:
        def __getattr__(self, name):
            return getattr(address_db.reader, name)

        async def execute(self, statement, *arguments):
            await _rotate(address_db, monkeypatch)
            return await address_db.reader.execute(statement, *arguments)

    async with address_db.reader.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(artifact.NetworkAddressArtifactError, match="table_changed"):
            await artifact._locked_table(RotatingConnection(), address_db.schema, address_db.descriptor["table"]["oid"])
    retained = await _verify(address_db)
    assert retained.table_oid == address_db.descriptor["table"]["oid"]
    assert retained.table_name.endswith("_retained_" + format(retained.table_oid, "x"))


async def test_native_rotation_keeps_pin_for_reader(address_db, monkeypatch):
    async with address_db.reader.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await artifact.capture_network_address_artifact(
                address_db.reader, address_db.source, runtime_roles=(address_db.roles["reader"],)
            )
            == address_db.descriptor
        )
    before = await _verify(address_db)
    retained = await _rotate(address_db, monkeypatch)
    after = await _verify(address_db)
    assert before.table_oid == after.table_oid == retained["relation_oid"]
    assert after.table_name == retained["retained_name"] == f"entity_address_unified_retained_{before.table_oid:x}"
    assert (
        await address_db.reader.fetchval(f'SELECT location_key FROM "{after.schema_name}"."{after.table_name}"')
        == "old-office"
    )
    assert (
        await address_db.reader.fetchval(f'SELECT location_key FROM "{after.schema_name}".entity_address_unified')
        == "new-office"
    )
    assert not await address_db.reader.fetchval(
        "SELECT pg_has_role(current_user,$1::oid,'SET')", address_db.descriptor["protected_owner_oid"]
    )
    async with address_db.publisher.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(artifact.NetworkAddressArtifactError, match="source_generation_changed"):
            await artifact.capture_network_address_artifact(
                address_db.publisher, address_db.source, runtime_roles=(address_db.roles["reader"],)
            )


@pytest.mark.parametrize(
    "damage",
    [
        "new_oid",
        "rewrite",
        "table_owner",
        "owner_login",
        "namespace_owner",
        "namespace_create",
        "table_update",
        "column_update",
        "role_set",
        "elevated_reach",
        "new_index",
        "reindex",
        "column_added",
        "authority_owner",
        "authority_update",
        "authority_index",
        "wrong_name",
    ],
)
async def test_changed_custody_or_structure_rejects(address_db, damage):
    resources = address_db
    admin, schema, roles = resources.admin, resources.schema, resources.roles
    match damage:
        case "new_oid":
            await admin.execute(f'DROP TABLE "{schema}".entity_address_unified')
            await admin.execute(
                f'CREATE TABLE "{schema}".entity_address_unified(location_key text PRIMARY KEY,type text)'
            )
            await admin.execute(f'ALTER TABLE "{schema}".entity_address_unified OWNER TO "{roles["owner"]}"')
            await admin.execute(f'GRANT SELECT ON "{schema}".entity_address_unified TO "{roles["reader"]}"')
        case "rewrite":
            await admin.execute(f'CLUSTER "{schema}".entity_address_unified USING address_npi')
        case "table_owner":
            await admin.execute(f'ALTER TABLE "{schema}".entity_address_unified OWNER TO "{roles["outsider"]}"')
        case "owner_login":
            await admin.execute(f'ALTER ROLE "{roles["owner"]}" LOGIN')
        case "namespace_owner":
            await admin.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{roles["outsider"]}"')
        case "namespace_create":
            await admin.execute(f'GRANT CREATE ON SCHEMA "{schema}" TO "{roles["reader"]}"')
        case "table_update":
            await admin.execute(f'GRANT UPDATE ON "{schema}".entity_address_unified TO "{roles["reader"]}"')
        case "column_update":
            await admin.execute(f'GRANT UPDATE(npi) ON "{schema}".entity_address_unified TO "{roles["reader"]}"')
        case "role_set":
            await admin.execute(f'GRANT "{roles["owner"]}" TO "{roles["reader"]}" WITH INHERIT FALSE, SET TRUE')
        case "elevated_reach":
            quoted_admin = await admin.fetchval("SELECT quote_ident(current_user)")
            await admin.execute(f'GRANT {quoted_admin} TO "{roles["reader"]}"')
        case "new_index":
            await admin.execute(f'CREATE INDEX changed_source ON "{schema}".entity_address_unified(inferred_npi)')
        case "reindex":
            await admin.execute(f'REINDEX INDEX "{schema}".address_npi')
        case "column_added":
            await admin.execute(f'ALTER TABLE "{schema}".entity_address_unified ADD COLUMN changed text')
        case "authority_owner":
            await admin.execute(
                f'ALTER TABLE "{schema}".entity_address_result_generation OWNER TO "{roles["outsider"]}"'
            )
        case "authority_update":
            await admin.execute(f'GRANT UPDATE ON "{schema}".entity_address_result_generation TO "{roles["outsider"]}"')
        case "authority_index":
            await admin.execute(
                f'CREATE INDEX changed_authority ON "{schema}".entity_address_result_generation(local_generation)'
            )
        case "wrong_name":
            await admin.execute(
                f'ALTER TABLE "{schema}".entity_address_unified RENAME TO entity_address_unified_retained_1'
            )
    with pytest.raises((artifact.NetworkAddressArtifactError, RuntimeError)):
        await _verify(resources)


async def test_pure_descriptor_digest_grants_no_generation_or_custody_authority(address_db):
    altered = deepcopy(address_db.descriptor)
    altered["authority"]["origin_generation"] += 1
    with pytest.raises(artifact.NetworkAddressArtifactError, match="generation_invalid"):
        artifact._descriptor(
            {
                **altered,
                "descriptor_sha256": artifact._digest(
                    {key: value for key, value in altered.items() if key != "descriptor_sha256"}
                ),
            }
        )
    with pytest.raises(artifact.NetworkAddressArtifactError, match="descriptor_digest_invalid"):
        artifact._descriptor({**address_db.descriptor, "alias_generation": 999})


async def test_capture_requires_a_repeatable_snapshot(address_db):
    async with address_db.reader.transaction(readonly=True):
        with pytest.raises(artifact.NetworkAddressArtifactError, match="snapshot_required"):
            await artifact.capture_network_address_artifact(
                address_db.reader, address_db.source, runtime_roles=(address_db.roles["reader"],)
            )
