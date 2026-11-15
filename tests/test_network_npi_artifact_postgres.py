# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine six-heap NPI authority with ordinary read-only custom import callers."""

import re
from dataclasses import asdict, replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
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
    columns = ",".join(f"{name}_table_oid bigint,{name}_row_count bigint" for name in generation.RELATION_NAMES)
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
                # A zero-row privilege probe (LIMIT 0) reads no data; any other read of the relation is refused.
                reference = re.escape(f'FROM "{self.schema}"."{table}"')
                assert re.search(reference + r"(?!\s+LIMIT 0\s*;?\s*$)", str(statement)) is None
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
    # The pin is the logical NPI publication: lineage, serving origin and canonical provenance, not its heaps.
    publication_by_field = {key: entry for key, entry in npi_db.authority.as_dict().items() if key != "relations"}
    assert source == PinnedAddressSource(npi_db.schema, "npi", catalog._digest(publication_by_field))
    assert source.generation_id != npi_db.resources.source.generation_id
    assert identity == asdict(source) and await _verify(npi_db, source, identity) == identity
    assert await _capture(npi_db, npi_db.resources.reader) == (source, identity)
    async with npi_db.resources.reader.transaction(isolation="repeatable_read", readonly=True):
        assert await artifact.capture_network_npi_artifact(
            npi_db.resources.reader, npi_db.schema, runtime_roles=(npi_db.resources.roles["reader"],)
        ) == (source, identity)
    assert await npi_db.resources.publisher.fetchval("SHOW transaction_read_only") == "off"
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with npi_db.resources.publisher.transaction():
            await npi_db.resources.publisher.execute(f'UPDATE "{npi_db.schema}".npi SET marker=marker')


async def test_current_pin_requires_the_callers_repeatable_snapshot(npi_db):
    connection = npi_db.resources.publisher
    roles = (npi_db.resources.roles["reader"],)
    # The shared capture check refuses before any read: first without a transaction, then without a snapshot.
    with pytest.raises(catalog.NetworkAddressArtifactError, match="transaction_required"):
        await artifact.capture_network_npi_artifact(connection, npi_db.schema, runtime_roles=roles)
    async with connection.transaction(isolation="read_committed"):
        with pytest.raises(catalog.NetworkAddressArtifactError, match="snapshot_required"):
            await artifact.capture_network_npi_artifact(connection, npi_db.schema, runtime_roles=roles)


# Heap, guard, index and column tampering on frozen tables only proved removed custody checks.
@pytest.mark.parametrize("damage", ["unpublished", "missing_authority", "no_index"])
async def test_native_authority_or_catalog_failure_never_uses_a_label_fallback(npi_db, damage):
    resources = npi_db.resources
    pinned_source, identity = await _capture(npi_db)
    schema = '"' + npi_db.schema + '"'
    match damage:
        case "unpublished":
            await resources.admin.execute(
                f"UPDATE {schema}.npi_result_generation SET origin_lineage_id=NULL,origin_generation=NULL,published_at=NULL,serving_schema=NULL,serving_relation_names=NULL"
            )
        case "missing_authority":
            await resources.admin.execute(f"DROP TABLE {schema}.npi_result_generation")
        case "no_index":
            await resources.admin.execute(f"ALTER TABLE {schema}.npi DROP CONSTRAINT npi_pkey")
    with pytest.raises(artifact.NetworkNPIArtifactError):
        await _verify(npi_db, pinned_source, identity)


# Role, login and namespace-owner state only proved removed custody checks; PostgreSQL refuses the writes.
@pytest.mark.parametrize("damage", ["current_write", "reader_write", "ordinary_write"])
async def test_ordinary_current_and_consumer_mutation_paths_fail_closed(npi_db, damage):
    resources = npi_db.resources
    schema = '"' + npi_db.schema + '"'
    connection, role, statement = {
        "current_write": (resources.publisher, None, f"UPDATE {schema}.npi SET marker=marker"),
        "reader_write": (resources.reader, None, f"UPDATE {schema}.npi SET marker=marker"),
        "ordinary_write": (
            resources.admin,
            resources.roles["outsider"],
            f"UPDATE {schema}.npi_result_generation SET local_generation=local_generation",
        ),
    }[damage]
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            if role is not None:
                await connection.execute(f'SET LOCAL ROLE "{role}"')
            await connection.execute(statement)


async def test_generation_labels_and_self_digests_do_not_authorize(npi_db):
    source, identity = await _capture(npi_db)
    for claimed_generation in ("unqualified-run-label", "b" * 64):
        claimed = replace(source, generation_id=claimed_generation)
        with pytest.raises(artifact.NetworkNPIArtifactError, match="source_changed"):
            await _verify(npi_db, claimed, asdict(claimed))
    with pytest.raises(artifact.NetworkNPIArtifactError, match="identity_invalid"):
        await _verify(npi_db, source, {**identity, "schema_name": True})
