# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact protected address pins survive native rotation and reject custody drift."""

import os
from contextlib import AsyncExitStack
from copy import deepcopy
from datetime import UTC, datetime
from itertools import product
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from process import entity_address_alias_guard as alias_guard
from process import entity_address_snapshot_adoption as adoption
from process import entity_address_snapshot_preparation as preparation
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
            authority, _legacy_key = await artifact._current_authority(publisher, schema)
            generation_id = artifact._digest(authority)
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


class _NativeSession(artifact._CatalogSession):
    async def connection(self):
        return self

    async def get_raw_connection(self):
        return SimpleNamespace(driver_connection=self.native_connection)


class _NativeDB:
    def __init__(self, connection):
        self.connection = connection
        self.session = _NativeSession(connection)

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
    return await _native_swap(resources, monkeypatch)


async def _native_swap(resources, monkeypatch):
    """Swap the prepared next heap in as the native publisher and advance the logical generation."""
    schema = resources.schema
    native_db = _NativeDB(resources.publisher)
    monkeypatch.setattr(adoption.entity_address_unified, "db", native_db)
    # Compact runtimes re-create the retention namespace as administrator; production keeps the owner role.
    previous_owner = await resources.admin.fetchval(
        "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname='hp_snapshot_retention'"
    )
    await resources.admin.execute(f'ALTER SCHEMA hp_snapshot_retention OWNER TO "{resources.roles["owner"]}"')
    try:
        async with resources.publisher.transaction():
            retained = await adoption._swap_sealed_stage_table(
                schema,
                SimpleNamespace(__main_table__="entity_address_unified"),
                SimpleNamespace(__tablename__="entity_address_unified_next", __my_additional_indexes__=[]),
                await preparation._publisher_authority(native_db.session),
            )
            oid = await resources.publisher.fetchval(
                "SELECT to_regclass($1)::oid::bigint", f'"{schema}".entity_address_unified'
            )
            await resources.publisher.execute(
                f'UPDATE "{schema}".entity_address_result_generation '
                "SET local_generation=2,origin_generation=2,relation_oids[1]=$1",
                oid,
            )
            await resources.publisher.execute(f'UPDATE "{schema}".address_alias_state_v1 SET generation=generation+1')
    finally:
        await resources.admin.execute("ALTER SCHEMA hp_snapshot_retention OWNER TO " + previous_owner)
    return retained


async def _verify(resources):
    async with resources.reader.transaction(isolation="repeatable_read", readonly=True):
        return await artifact.verify_network_address_artifact(
            resources.reader, resources.descriptor, runtime_roles=(resources.roles["reader"],)
        )


async def test_rotation_between_catalog_read_and_lock_keeps_the_recorded_generation(address_db, monkeypatch):
    class RotatingConnection:
        def __getattr__(self, name):
            return getattr(address_db.reader, name)

        rotated = False

        async def execute(self, statement, *arguments):
            # Rotate after resolution, before the reader's first named lock.
            if statement.startswith("LOCK TABLE ONLY") and not self.rotated:
                self.rotated = True
                await _rotate(address_db, monkeypatch)
            return await address_db.reader.execute(statement, *arguments)

    async with address_db.reader.transaction(isolation="repeatable_read", readonly=True):
        retained = await artifact.verify_network_address_artifact(RotatingConnection(), address_db.descriptor)
        assert retained.table_name == address_db.descriptor["retained_table"]["table_name"]
        assert (
            await address_db.reader.fetchval(
                f'SELECT location_key FROM "{retained.schema_name}"."{retained.table_name}"'
            )
            == "old-office"
        )
    retained = await _verify(address_db)
    assert retained.table_name == address_db.descriptor["retained_table"]["table_name"]
    assert retained.table_name.startswith("entity_address_unified_retained_")


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
    assert before.table_name == "entity_address_unified"
    assert after.table_name == retained["relation_name"] == address_db.descriptor["retained_table"]["table_name"]
    assert (
        await address_db.reader.fetchval(f'SELECT location_key FROM "{after.schema_name}"."{after.table_name}"')
        == "old-office"
    )
    assert (
        await address_db.reader.fetchval(f'SELECT location_key FROM "{after.schema_name}".entity_address_unified')
        == "new-office"
    )
    # The reader cannot act as the protected owner role, named rather than by a stored identity.
    assert not await address_db.reader.fetchval("SELECT pg_has_role(current_user,$1,'SET')", address_db.roles["owner"])
    async with address_db.publisher.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(artifact.NetworkAddressArtifactError, match="source_generation_changed"):
            await artifact.capture_network_address_artifact(
                address_db.publisher, address_db.source, runtime_roles=(address_db.roles["reader"],)
            )


async def test_changed_custody_or_structure_rejects(address_db):
    """A pinned name that no longer exists is refused; frozen heaps leave nothing else to compare per read."""
    resources = address_db
    await resources.admin.execute(
        f'ALTER TABLE "{resources.schema}".entity_address_unified RENAME TO entity_address_unified_retained_1'
    )
    with pytest.raises(
        artifact.NetworkAddressArtifactError, match="network_address_artifact_retained_table_unavailable"
    ):
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


async def test_historical_key_pin_captures_its_saved_authority(address_db):
    async with address_db.publisher.transaction(isolation="repeatable_read", readonly=True):
        authority, legacy_key = await artifact._current_authority(address_db.publisher, address_db.schema)
        historical = PinnedAddressSource(address_db.schema, "entity_address_unified", legacy_key)
        descriptor = await artifact.capture_network_address_artifact(
            address_db.publisher, historical, runtime_roles=(address_db.roles["reader"],)
        )
    assert descriptor["generation_id"] == legacy_key == artifact._digest(descriptor["legacy_authority"])
    assert descriptor["authority"] == authority and "legacy_authority" not in address_db.descriptor
    assert artifact._descriptor(descriptor) == descriptor
    async with address_db.reader.transaction(isolation="repeatable_read", readonly=True):
        assert await artifact.verify_network_address_artifact(address_db.reader, descriptor) == (
            artifact.CompactAddressRelation(address_db.schema, "entity_address_unified")
        )


async def test_capture_requires_a_repeatable_snapshot(address_db):
    async with address_db.reader.transaction(readonly=True):
        with pytest.raises(artifact.NetworkAddressArtifactError, match="snapshot_required"):
            await artifact.capture_network_address_artifact(
                address_db.reader, address_db.source, runtime_roles=(address_db.roles["reader"],)
            )


async def _prepare_ordinary_rotation(resources, outcome):
    from process.entity_address_retained_names import retained_address_name

    schema, table = resources.schema, "entity_address_unified"
    stage = table + "_next"
    # This empty namespace belongs only to this disposable fixture; no alternate owner is required.
    await resources.admin.execute("DROP SCHEMA hp_snapshot_retention")
    assert not await resources.publisher.fetchval(
        "SELECT pg_has_role(current_user,$1,'SET')", resources.roles["outsider"]
    )
    for support in artifact.generation.RELATION_NAMES[1:]:
        await resources.admin.execute(f'INSERT INTO "{schema}"."{support}" VALUES(1)')
    await resources.admin.execute(f'CREATE TABLE "{schema}"."{stage}" (LIKE "{schema}"."{table}" INCLUDING ALL)')
    await resources.admin.execute(f'INSERT INTO "{schema}"."{stage}" SELECT * FROM "{schema}"."{table}"')
    await resources.admin.execute(f'UPDATE "{schema}"."{stage}" SET location_key=\'new-office\'')
    await resources.admin.execute(f'ALTER TABLE "{schema}"."{stage}" OWNER TO "{resources.roles["owner"]}"')
    await resources.admin.execute(
        f'GRANT UPDATE(location_key) ON "{schema}"."{stage}" TO "{resources.roles["reader"]}"'
    )
    origin_by_field = {
        field: resources.descriptor["authority"][field]
        for field in ("origin_lineage_id", "origin_generation", "published_at")
    }
    retained = retained_address_name(table, origin_by_field)
    if outcome == "collision":
        await resources.admin.execute(f'CREATE TABLE "{schema}"."{retained}"(sentinel integer)')
        await resources.admin.execute(f'INSERT INTO "{schema}"."{retained}" VALUES(9)')
    return schema, table, stage, retained


@pytest.mark.parametrize("outcome", ["commit", "collision", "rollback"])
async def test_ordinary_main_only_rotation_preserves_reads_without_retention_role(address_db, monkeypatch, outcome):
    """Exercise real permissions, named retention and transactional restoration on the actual scope."""
    resources = address_db
    schema, table, stage, retained = await _prepare_ordinary_rotation(resources, outcome)
    database = _NativeDB(resources.publisher)
    monkeypatch.setattr(adoption.entity_address_unified, "db", database)
    swaps = [
        SimpleNamespace(
            live_cls=SimpleNamespace(__main_table__=table),
            stage_cls=SimpleNamespace(__tablename__=stage, __my_additional_indexes__=[]),
        )
    ]
    context_by_field = {"result_generation_mode": "ordinary"}

    async def publish():
        async with resources.publisher.transaction():
            await resources.publisher.execute(
                f'LOCK TABLE "{schema}"."{table}","{schema}"."{stage}" IN ACCESS EXCLUSIVE MODE'
            )
            serving = await adoption._prepare_protected_ordinary_rotation(schema, swaps, False, context_by_field)
            await adoption._require_protected_cutover(schema, swaps, context_by_field)
            await adoption._publish_result_swaps(schema, swaps, context_by_field, serving_generation=serving)
            await resources.publisher.execute(
                f'UPDATE "{schema}".entity_address_result_generation SET local_generation=local_generation+1,'
                "origin_generation=origin_generation+1"
            )
            if outcome == "rollback":
                raise RuntimeError("synthetic late publication rollback")

    if outcome == "commit":
        await publish()
        assert context_by_field["retained_relations"] == [{"schema_name": schema, "relation_name": retained}]
        assert await resources.reader.fetchval(f'SELECT location_key FROM "{schema}"."{retained}"') == "old-office"
        assert await resources.reader.fetchval(f'SELECT location_key FROM "{schema}"."{table}"') == "new-office"
        for role, name in product((resources.publisher, resources.reader), (table, retained)):
            for mutation in (
                f'UPDATE "{schema}"."{name}" SET location_key=\'changed\'',
                f'DELETE FROM "{schema}"."{name}"',
                f'TRUNCATE "{schema}"."{name}"',
                f'INSERT INTO "{schema}"."{name}" SELECT * FROM "{schema}"."{name}" WHERE false',
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await role.execute(mutation)
        selected = await _verify(resources)
        assert selected.table_name == retained
    else:
        with pytest.raises(RuntimeError, match="already exists|late publication rollback"):
            await publish()
        assert await resources.reader.fetchval(f'SELECT location_key FROM "{schema}"."{table}"') == "old-office"
        assert await resources.admin.fetchval(f'SELECT location_key FROM "{schema}"."{stage}"') == "new-office"
        assert await resources.publisher.fetchval("SELECT has_table_privilege($1,'UPDATE')", f'"{schema}"."{stage}"')
        if outcome == "collision":
            assert await resources.admin.fetchval(f'SELECT sentinel FROM "{schema}"."{retained}"') == 9
        else:
            assert not await resources.admin.fetchval("SELECT to_regclass($1) IS NOT NULL", f'"{schema}"."{retained}"')
    for support in artifact.generation.RELATION_NAMES[1:]:
        assert await resources.reader.fetchval(f'SELECT count(*) FROM "{schema}"."{support}"') == 1


async def test_ordinary_read_only_revokes_column_and_sequence_mutation_without_owner_transfer(address_db):
    from process.entity_address_publication_permissions import make_relation_read_only

    resources = address_db
    schema = resources.schema
    table = "ordinary_read_only_probe"
    await resources.publisher.execute(f'CREATE TABLE "{schema}"."{table}"(id bigserial PRIMARY KEY,value text)')
    await resources.publisher.execute(f'INSERT INTO "{schema}"."{table}"(value) VALUES(\'retained\')')
    await resources.publisher.execute(
        f'GRANT SELECT,UPDATE(value) ON "{schema}"."{table}" TO "{resources.roles["reader"]}"'
    )
    await resources.publisher.execute(
        f'GRANT SELECT,UPDATE ON "{schema}"."{table}" TO "{resources.roles["outsider"]}" WITH GRANT OPTION'
    )
    await resources.publisher.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{resources.roles["outsider"]}"')
    async with resources.admin.transaction():
        await resources.admin.execute(f'SET LOCAL ROLE "{resources.roles["outsider"]}"')
        await resources.admin.execute(f'GRANT UPDATE ON "{schema}"."{table}" TO "{resources.roles["reader"]}"')
        await resources.admin.execute(f'UPDATE "{schema}"."{table}" SET value=value')
    sequence = await resources.publisher.fetchval("SELECT pg_get_serial_sequence($1,'id')", f'"{schema}"."{table}"')
    await resources.publisher.execute(f'GRANT USAGE,UPDATE ON SEQUENCE {sequence} TO "{resources.roles["reader"]}"')
    owner = await resources.admin.fetchval(
        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=to_regclass($1)", f'"{schema}"."{table}"'
    )
    with pytest.raises(RuntimeError, match="synthetic permission rollback"):
        async with resources.publisher.transaction():
            await make_relation_read_only(artifact._CatalogSession(resources.publisher), schema, table)
            raise RuntimeError("synthetic permission rollback")
    assert await resources.reader.fetchval("SELECT has_column_privilege($1,'value','UPDATE')", f'"{schema}"."{table}"')
    assert await resources.admin.fetchval(
        "SELECT has_table_privilege($1,$2,'UPDATE')", resources.roles["outsider"], f'"{schema}"."{table}"'
    )
    async with resources.publisher.transaction():
        await make_relation_read_only(artifact._CatalogSession(resources.publisher), schema, table)
    assert (
        await resources.admin.fetchval(
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=to_regclass($1)", f'"{schema}"."{table}"'
        )
        == owner
    )
    for actor in (resources.publisher, resources.reader):
        assert await actor.fetchval(f'SELECT value FROM "{schema}"."{table}"') == "retained"
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await actor.execute(f'UPDATE "{schema}"."{table}" SET value=\'changed\'')
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await actor.fetchval("SELECT nextval($1::regclass)", sequence)
    async with resources.admin.transaction():
        await resources.admin.execute(f'SET LOCAL ROLE "{resources.roles["outsider"]}"')
        assert await resources.admin.fetchval(f'SELECT value FROM "{schema}"."{table}"') == "retained"
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with resources.admin.transaction():
                await resources.admin.execute(f'UPDATE "{schema}"."{table}" SET value=\'changed\'')
