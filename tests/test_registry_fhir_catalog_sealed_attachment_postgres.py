# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog projection reads retain the native directory attachment's real custody."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.network_candidate_custody import NetworkCandidateCustodyError, seal_network_candidate_custody
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import NetworkServingReadUnavailable
from process.registry_fhir_catalog_custody import verify_fhir_catalog_snapshot
from process.registry_fhir_catalog_grant import FHIRCatalogStandingGrant, build_fhir_catalog_publication_runtime
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_permissions import (
    install_fhir_catalog_preparation_permissions,
    install_fhir_catalog_publication_permissions,
    verify_fhir_catalog_preparation_permissions,
)
from process.registry_fhir_catalog_sync import (
    prepare_fhir_catalog_snapshot,
    resolve_fhir_catalog_projection_generations,
)
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from tests.registry_candidate_custody_split_postgres_support import _assert_handoff, _login, _relations
from tests.registry_candidate_custody_split_postgres_support import split_db as split_db
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_fhir_catalog_observed_source_postgres import observed_source_db as observed_source_db
from tests.test_registry_publication_worker_permissions_postgres import _native_qualified_candidate


async def _publish_sealed_attachment(fixture):
    """Use the supported trusted sealer with the fixture's exact native readers."""
    schema = fixture.fixture.control_schema
    await _assert_handoff(fixture)
    async with _login(fixture, "execution") as execution:
        copy_target, address = await _native_qualified_candidate(fixture, execution, runtime_roles=fixture.readers)

        async def is_authorized(connection, candidate):
            return connection is fixture.fixture.connection and candidate == copy_target

        async def seal(candidate):
            return await seal_network_candidate_custody(
                fixture.fixture.connection,
                candidate,
                control_schema=schema,
                runtime_roles=fixture.readers,
                authorize=is_authorized,
            )

        manifest = await prepare_and_publish_network_candidate(
            execution,
            copy_target,
            address,
            owner_role=fixture.roles_by_kind["physical"],
            loader_roles=(),
            reader_roles=fixture.readers,
            control_schema=schema,
            seal_candidate=seal,
        )
        assert (
            await fixture.fixture.connection.fetchval(
                f'SELECT count(*) FROM "{schema}".network_candidate_custody WHERE candidate_id=$1',
                manifest["candidate_id"],
            )
            == 1
        )
    return manifest, copy_target


async def _assert_read_committed_refusal(fixture, snapshot_by_field, grant):
    async with _login(fixture, "reader") as primary, primary.transaction():
        assert await primary.fetchval("SHOW transaction_isolation") == "read committed"
        with pytest.raises(NetworkServingReadUnavailable, match="manifest verification failed") as refused:
            await resolve_fhir_catalog_projection_generations(primary, snapshot_by_field, grant=grant, descriptors={})
        assert isinstance(refused.value.__cause__, NetworkCandidateCustodyError)


@pytest.mark.asyncio
async def test_sealed_attachment_projection_uses_repeatable_read_registry_snapshot(split_db):
    fixture = split_db
    manifest, copy_target = await _publish_sealed_attachment(fixture)
    snapshot_by_field = {
        "network_generation": manifest["generation_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "source_vector_json": [],
    }
    grant = FHIRCatalogStandingGrant("sealed-directory-projection", 1, fixture.fixture.control_schema, ())
    lock_query = (
        "SELECT EXISTS(SELECT FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE l.pid=$1 AND n.nspname=$2 "
        "AND l.mode='AccessShareLock' AND l.granted)"
    )
    async with _login(fixture, "reader") as registry_reader:
        reader_pid = await registry_reader.fetchval("SELECT pg_backend_pid()")
        async with registry_reader.transaction(isolation="repeatable_read", readonly=True):
            assert await registry_reader.fetchval("SHOW transaction_read_only") == "on"
            assert (
                await resolve_fhir_catalog_projection_generations(
                    registry_reader, snapshot_by_field, grant=grant, descriptors={}
                )
                == ()
            )
            await _assert_read_committed_refusal(fixture, snapshot_by_field, grant)
            assert registry_reader.is_in_transaction()
            assert await fixture.fixture.connection.fetchval(lock_query, reader_pid, copy_target.schema_name)
            assert (
                await resolve_fhir_catalog_projection_generations(
                    registry_reader, snapshot_by_field, grant=grant, descriptors={}
                )
                == ()
            )
        assert not await fixture.fixture.connection.fetchval(lock_query, reader_pid, copy_target.schema_name)


@asynccontextmanager
async def _catalog_roles(fixture):
    """Own only isolated catalog identities and their exact candidate schemas."""
    connection = fixture.fixture.connection
    roles_by_kind = {
        kind: "sealed_catalog_" + kind + "_" + uuid4().hex for kind in ("loader", "sealer", "execution", "owner")
    }
    created_roles = []
    try:
        for kind, role in roles_by_kind.items():
            login = "NOLOGIN" if kind == "owner" else f"LOGIN PASSWORD '{fixture.role_password}'"
            await connection.execute(f'CREATE ROLE "{role}" {login}')
            created_roles.append(role)
        yield roles_by_kind
    finally:
        for role in reversed(created_roles):
            for schema in await connection.fetch(
                "SELECT nspname FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1)", role
            ):
                await connection.execute(f'DROP SCHEMA "{schema["nspname"]}" CASCADE')
                assert await connection.fetchval("SELECT to_regnamespace($1)", schema["nspname"]) is None
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )


async def _prepare_observed_catalog_roles(fixture, observed_source, roles_by_kind):
    """Prepare catalog permissions and retain the independently enrolled directory Reader."""
    connection, control = fixture.fixture.connection, fixture.fixture.control_schema
    await observed_source.admin.execute(f'''ALTER TABLE "{observed_source.schema}".provider_directory_endpoint_dataset
      ADD COLUMN content_proof_resource_types text[],ADD COLUMN publication_metadata_summary_json jsonb,
      ADD COLUMN completion_proof_required_version integer,ADD COLUMN completion_proof_json jsonb,
      ADD COLUMN completion_proof_sha256 text;
      CREATE TABLE "{observed_source.schema}".provider_directory_insurance_network_source_binding(
      source_id text,resource_id text,network_id uuid)''')
    source_reader = observed_source.roles_by_kind["reader"]
    await observed_source.admin.execute(
        f'GRANT SELECT ON "{observed_source.schema}".provider_directory_insurance_network_source_binding TO "{source_reader}"'
    )
    await observed_source.admin.execute(
        f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "{source_reader}"'
    )
    grant = FHIRCatalogStandingGrant(
        "native-observed-sealing",
        1,
        control,
        (observed_source.profile,),
        observation_contract=CATALOG_OBSERVATION_CONTRACT,
    )
    readers = tuple(sorted((roles_by_kind["loader"], roles_by_kind["execution"], fixture.roles_by_kind["api"])))
    consumer = RegistryPublicationConsumerConfig(
        control,
        fixture.roles_by_kind["lifecycle"],
        roles_by_kind["execution"],
        fixture.roles_by_kind["api"],
        fixture.roles_by_kind["queue"],
        roles_by_kind["owner"],
        (roles_by_kind["loader"],),
        readers,
        metadata_runtime=build_fhir_catalog_publication_runtime(grant, None),
        metadata_only=True,
    )
    for kind in ("loader", "sealer", "execution"):
        await connection.execute(
            f'GRANT "{fixture.fixture.roles["reader"]}" TO "{roles_by_kind[kind]}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
        )
    async with connection.transaction():
        await install_fhir_catalog_preparation_permissions(
            connection, consumer, loader_role=roles_by_kind["loader"], sealer_role=roles_by_kind["sealer"]
        )
        await install_fhir_catalog_publication_permissions(connection, consumer)
    return SimpleNamespace(grant=grant, consumer=consumer, loader_role=roles_by_kind["loader"], reader_roles=readers)


@asynccontextmanager
async def _observed_reader(observed_source):
    reader = await observed_source.connect()
    try:
        assert await reader.fetchval(
            "SELECT current_user=session_user AND current_user::text=$1", observed_source.roles_by_kind["reader"]
        )
        yield reader
    finally:
        await reader.close()


async def _assert_committed_observed_catalog(fixture, loader, snapshot_by_field, manifest):
    """Require stored custody and real observations after both Sealer transactions commit."""
    connection, control = fixture.fixture.connection, fixture.fixture.control_schema
    assert snapshot_by_field["network_generation"] == manifest["generation_id"]
    assert snapshot_by_field["manifest_sha256"] == manifest["manifest_sha256"]
    assert len(snapshot_by_field["source_vector_json"]) == 1
    stored = await connection.fetchrow(
        f'SELECT custody_json,source_vector_json,published_at FROM "{control}".registry_fhir_catalog_snapshot '
        "WHERE metadata_generation=$1",
        snapshot_by_field["metadata_generation"],
    )
    assert stored is not None and stored["published_at"] is None
    assert json.loads(stored["custody_json"]) == snapshot_by_field["custody_json"]
    assert json.loads(stored["source_vector_json"]) == snapshot_by_field["source_vector_json"]
    assert snapshot_by_field["custody_json"]["validation"]
    schema = '"' + snapshot_by_field["schema_name"] + '"'
    async with loader.transaction(isolation="repeatable_read", readonly=True):
        await verify_fhir_catalog_snapshot(loader, snapshot_by_field)
        descriptor = json.loads(
            await loader.fetchval(f"SELECT descriptor_json FROM {schema}.registry_fhir_catalog_source")
        )
        assert descriptor["admission"]["kind"] == "catalog_observation"
        assert descriptor["admission"]["contract"] == CATALOG_OBSERVATION_CONTRACT
        assert await loader.fetchval(f"SELECT count(*) FROM {schema}.registry_fhir_catalog_observation") > 0
        assert await loader.fetchval(f"SELECT count(*) FROM {schema}.registry_fhir_catalog_network") > 0
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await loader.execute(f"DELETE FROM {schema}.registry_fhir_catalog_observation WHERE false")


@pytest.mark.asyncio
async def test_observed_catalog_prepares_with_native_sealed_directory(split_db, observed_source_db):
    """Compose real collection, two-slot sealing and native attachment custody under prepared roles."""
    fixture, observed_source = split_db, observed_source_db
    manifest, directory_target = await _publish_sealed_attachment(fixture)
    connection, control = fixture.fixture.connection, fixture.fixture.control_schema
    original_relations = await _relations(connection, directory_target.schema_name)
    original_seal = await connection.fetchrow(f'SELECT * FROM "{control}".network_candidate_custody')
    original_head = await connection.fetchrow(f'SELECT * FROM "{control}".network_serving_control')
    async with _catalog_roles(fixture) as roles_by_kind:
        config = await _prepare_observed_catalog_roles(fixture, observed_source, roles_by_kind)
        logins = SimpleNamespace(dsn=fixture.dsn, roles_by_kind=roles_by_kind, role_password=fixture.role_password)
        source_pool = SimpleNamespace(acquire=lambda: _observed_reader(observed_source))
        permission_options_by_name = {"loader_role": roles_by_kind["loader"], "sealer_role": roles_by_kind["sealer"]}
        async with (
            _login(logins, "loader") as loader,
            asyncpg.create_pool(
                fixture.dsn, user=roles_by_kind["sealer"], password=fixture.role_password, min_size=1, max_size=2
            ) as sealer_pool,
        ):
            async with loader.transaction(readonly=True):
                await verify_fhir_catalog_preparation_permissions(
                    loader, config.consumer, **permission_options_by_name, role=roles_by_kind["loader"]
                )
            async with sealer_pool.acquire() as sealer, sealer.transaction(readonly=True):
                await verify_fhir_catalog_preparation_permissions(
                    sealer, config.consumer, **permission_options_by_name, role=roles_by_kind["sealer"]
                )
            snapshot_by_field, previous, accounting = await prepare_fhir_catalog_snapshot(
                config, loader, source_pool, sealer_pool
            )
            assert snapshot_by_field is not None and previous is None
            assert accounting["accepted_sources"] == 1 and accounting["unavailable_sources"] == 0
            assert sealer_pool.get_size() == sealer_pool.get_max_size() == 2
            await _assert_committed_observed_catalog(fixture, loader, snapshot_by_field, manifest)
    assert await _relations(connection, directory_target.schema_name) == original_relations
    assert await connection.fetchrow(f'SELECT * FROM "{control}".network_candidate_custody') == original_seal
    assert await connection.fetchrow(f'SELECT * FROM "{control}".network_serving_control') == original_head
