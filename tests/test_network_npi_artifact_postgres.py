# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine six-heap NPI authority with ordinary read-only custom import callers."""

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from process import network_address_artifact as catalog
from process import network_npi_artifact as artifact
from process import npi_result_generation as generation
from process.network_address_projection import PinnedAddressSource
from tests.test_network_address_artifact_postgres import address_db
from tests.test_npi_result_archive_postgres import _run_migration

pytestmark = pytest.mark.asyncio


async def _bootstrap(connection, schema):
    columns = ",".join(name + "_table_oid bigint" for name in generation.RELATION_NAMES)
    await connection.execute(
        f'CREATE TABLE "{schema}".npi_canonical_publication_receipt(publication_ref text PRIMARY KEY,publication_generation bigint,chain_ref text,import_date date,{columns})'
    )
    await connection.execute(
        f'CREATE TABLE "{schema}".npi_canonical_publication_receipt_seal(publication_ref text PRIMARY KEY)'
    )
    async with connection.transaction():
        return await generation.bootstrap_npi_result_generation(catalog._CatalogSession(connection), schema_name=schema)


@pytest.fixture
async def npi_db(address_db):
    schema = "npi_native_" + uuid4().hex
    owner = "npi_native_owner_" + uuid4().hex
    is_role_created = False
    engine = create_async_engine(address_db.url.set(drivername="postgresql+asyncpg"))
    try:
        await address_db.admin.execute(
            f'CREATE ROLE "{owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
        )
        is_role_created = True
        await address_db.admin.execute(f'CREATE SCHEMA "{schema}"')
        for name in generation.RELATION_NAMES:
            await address_db.admin.execute(
                f'CREATE TABLE "{schema}"."{name}"(npi bigint PRIMARY KEY,marker text NOT NULL DEFAULT \'initial\')'
            )
            await address_db.admin.execute(f'INSERT INTO "{schema}"."{name}"(npi) VALUES(1000000004)')
        async with engine.begin() as connection:
            await _run_migration(connection, schema)
        authority = await _bootstrap(address_db.admin, schema)
        subjects = (address_db.roles["publisher"], address_db.roles["reader"])
        for name in (*generation.RELATION_NAMES, generation.TABLE_NAME):
            await address_db.admin.execute(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{owner}"')
            await address_db.admin.execute(f'GRANT SELECT ON "{schema}"."{name}" TO "{subjects[0]}","{subjects[1]}"')
        await address_db.admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{subjects[0]}","{subjects[1]}"')
        yield SimpleNamespace(resources=address_db, schema=schema, owner=owner, authority=authority)
    finally:
        await engine.dispose()
        await address_db.admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        if is_role_created:
            await address_db.admin.execute(f'DROP OWNED BY "{owner}"')
            await address_db.admin.execute(f'DROP ROLE "{owner}"')
            assert not await address_db.admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", owner)


class _CatalogOnly:
    def __init__(self, connection, schema):
        self.connection = connection
        self.schema = schema

    def __getattr__(self, name):
        original = getattr(self.connection, name)
        if name not in {"execute", "fetch", "fetchrow", "fetchval", "prepare"}:
            return original

        async def checked(statement, *args, **kwargs):
            for table in generation.RELATION_NAMES:
                assert f'FROM "{self.schema}"."{table}"' not in str(statement)
            return await original(statement, *args, **kwargs)

        return checked


async def _capture(fixture, connection=None):
    resources = fixture.resources
    connection = connection or resources.publisher
    async with connection.transaction(isolation="repeatable_read"):
        return await artifact.capture_network_npi_artifact(
            _CatalogOnly(connection, fixture.schema), fixture.schema, runtime_roles=(resources.roles["reader"],)
        )


async def _verify(fixture, source, identity):
    async with fixture.resources.publisher.transaction(isolation="repeatable_read"):
        return await artifact.verify_network_npi_artifact(
            fixture.resources.publisher, source, identity, runtime_roles=(fixture.resources.roles["reader"],)
        )


async def test_current_mixed_owner_generation_and_catalog_are_independently_pinned(npi_db):
    source, identity = await _capture(npi_db)
    assert source == PinnedAddressSource(npi_db.schema, "npi", catalog._digest(npi_db.authority.as_dict()))
    assert source.generation_id != npi_db.resources.source.generation_id
    assert identity[:3] == tuple(
        await npi_db.resources.admin.fetchrow(
            "SELECT n.oid::bigint,c.oid::bigint,c.relowner::bigint FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid WHERE n.nspname=$1 AND c.relname='npi'",
            npi_db.schema,
        )
    )
    assert len(identity[3]) == 64 and await _verify(npi_db, source, identity) == identity
    assert await _capture(npi_db, npi_db.resources.reader) == (source, identity)
    async with npi_db.resources.reader.transaction(isolation="repeatable_read", readonly=True):
        assert await artifact.capture_network_npi_artifact(
            npi_db.resources.reader, npi_db.schema, runtime_roles=(npi_db.resources.roles["reader"],)
        ) == (source, identity)
    assert await npi_db.resources.publisher.fetchval("SHOW transaction_read_only") == "off"
    assert not await npi_db.resources.publisher.fetchval(
        "SELECT has_table_privilege(current_user,$1,'UPDATE')", npi_db.schema + ".npi"
    )


async def test_current_pin_requires_the_callers_repeatable_snapshot(npi_db):
    connection = npi_db.resources.publisher
    roles = (npi_db.resources.roles["reader"],)
    with pytest.raises(artifact.NetworkNPIArtifactError, match="snapshot_required"):
        await artifact.capture_network_npi_artifact(connection, npi_db.schema, runtime_roles=roles)
    async with connection.transaction(isolation="read_committed"):
        with pytest.raises(artifact.NetworkNPIArtifactError, match="snapshot_required"):
            await artifact.capture_network_npi_artifact(connection, npi_db.schema, runtime_roles=roles)


@pytest.mark.parametrize("name", generation.RELATION_NAMES)
async def test_same_oid_native_mutation_changes_authority(npi_db, name):
    source, identity = await _capture(npi_db)
    await npi_db.resources.admin.execute(f'UPDATE "{npi_db.schema}"."{name}" SET marker=\'changed\'')
    changed, new_identity = await _capture(npi_db)
    assert changed.generation_id != source.generation_id and new_identity[:3] == identity[:3]
    with pytest.raises(artifact.NetworkNPIArtifactError, match="source_changed"):
        await _verify(npi_db, source, identity)


@pytest.mark.parametrize(
    "damage",
    [
        "unpublished",
        "missing_authority",
        "replaced_heap",
        "disabled_guard",
        "guard_body",
        "guard_owner",
        "no_index",
        "new_index",
        "column",
    ],
)
async def test_native_authority_or_catalog_failure_never_uses_a_label_fallback(npi_db, damage):
    resources = npi_db.resources
    pinned_source, identity = await _capture(npi_db)
    schema = '"' + npi_db.schema + '"'
    match damage:
        case "unpublished":
            await resources.admin.execute(
                f"UPDATE {schema}.npi_result_generation SET origin_lineage_id=NULL,origin_generation=NULL,published_at=NULL,relation_oids=NULL"
            )
        case "missing_authority":
            await resources.admin.execute(f"DROP TABLE {schema}.npi_result_generation")
        case "replaced_heap":
            await resources.admin.execute(f"ALTER TABLE {schema}.npi_address RENAME TO old_npi_address")
            await resources.admin.execute(
                f"CREATE TABLE {schema}.npi_address(LIKE {schema}.old_npi_address INCLUDING ALL)"
            )
            await resources.admin.execute(f'ALTER TABLE {schema}.npi_address OWNER TO "{npi_db.owner}"')
            await resources.admin.execute(
                f'GRANT SELECT ON {schema}.npi_address TO "{resources.roles["publisher"]}","{resources.roles["reader"]}"'
            )
        case "disabled_guard":
            await resources.admin.execute(f"ALTER TABLE {schema}.npi DISABLE TRIGGER {generation.REVISION_TRIGGER}")
        case "guard_body":
            await resources.admin.execute(
                f"CREATE OR REPLACE FUNCTION {schema}.{generation.REVISION_FUNCTION}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN RETURN NULL; END $$"
            )
        case "guard_owner":
            await resources.admin.execute(
                f'ALTER FUNCTION {schema}.{generation.REVISION_FUNCTION}() OWNER TO "{resources.roles["publisher"]}"'
            )
        case "no_index":
            await resources.admin.execute(f"ALTER TABLE {schema}.npi DROP CONSTRAINT npi_pkey")
        case "new_index":
            await resources.admin.execute(f"CREATE INDEX changed_npi_marker ON {schema}.npi(marker)")
        case "column":
            await resources.admin.execute(f"ALTER TABLE {schema}.npi ADD COLUMN changed text")
    with pytest.raises(artifact.NetworkNPIArtifactError):
        await _verify(npi_db, pinned_source, identity)


@pytest.mark.parametrize(
    "damage", ["current_write", "reader_write", "ordinary_write", "current_set", "namespace_owner", "owner_login"]
)
async def test_ordinary_current_and_consumer_mutation_paths_fail_closed(npi_db, damage):
    resources = npi_db.resources
    schema = '"' + npi_db.schema + '"'
    match damage:
        case "current_write":
            await resources.admin.execute(f'GRANT UPDATE ON {schema}.npi TO "{resources.roles["publisher"]}"')
        case "reader_write":
            await resources.admin.execute(f'GRANT UPDATE(marker) ON {schema}.npi TO "{resources.roles["reader"]}"')
        case "ordinary_write":
            await resources.admin.execute(
                f'GRANT UPDATE ON {schema}.npi_result_generation TO "{resources.roles["outsider"]}"'
            )
        case "current_set":
            await resources.admin.execute(
                f'GRANT "{npi_db.owner}" TO "{resources.roles["publisher"]}" WITH INHERIT FALSE, SET TRUE'
            )
        case "namespace_owner":
            await resources.admin.execute(f'ALTER SCHEMA {schema} OWNER TO "{resources.roles["outsider"]}"')
        case "owner_login":
            await resources.admin.execute(f'ALTER ROLE "{npi_db.owner}" LOGIN')
    with pytest.raises(artifact.NetworkNPIArtifactError):
        await _capture(npi_db)


async def test_current_subject_cannot_replace_native_index_executable(npi_db):
    resources = npi_db.resources
    function = "pg_catalog.btint8cmp(bigint,bigint)"
    owner = await resources.admin.fetchval(
        "SELECT quote_ident(r.rolname) FROM pg_proc p JOIN pg_roles r ON r.oid=p.proowner WHERE p.oid=$1::regprocedure",
        function,
    )
    await resources.admin.execute(f'ALTER FUNCTION {function} OWNER TO "{resources.roles["publisher"]}"')
    try:
        with pytest.raises(artifact.NetworkNPIArtifactError):
            await _capture(npi_db)
    finally:
        await resources.admin.execute(f"ALTER FUNCTION {function} OWNER TO {owner}")


async def test_generation_labels_and_self_digests_do_not_authorize(npi_db):
    source, identity = await _capture(npi_db)
    with pytest.raises(artifact.NetworkNPIArtifactError, match="source_changed"):
        await _verify(npi_db, replace(source, generation_id="unqualified-run-label"), identity)
    with pytest.raises(artifact.NetworkNPIArtifactError, match="source_changed"):
        await _verify(npi_db, source, (*identity[:3], "b" * 64))
    with pytest.raises(artifact.NetworkNPIArtifactError, match="identity_invalid"):
        await _verify(npi_db, source, (True, *identity[1:]))
