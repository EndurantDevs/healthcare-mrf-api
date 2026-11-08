# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog projection reads retain the native directory attachment's real custody."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

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
from process.registry_fhir_catalog_registry_reader import (
    fhir_catalog_registry_connection,
    fhir_catalog_registry_reads,
)
from process.registry_fhir_catalog_sync import (
    _catalog_registry_head,
    _publish_catalog_snapshot,
    prepare_fhir_catalog_snapshot,
    resolve_fhir_catalog_projection_generations,
)
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from tests.fhir_catalog_registry_reader_support import configured_catalog_reader
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


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "actor", "database", "transaction"])
async def test_configured_registry_reader_keeps_native_identity_and_commit_lifetime(split_db, monkeypatch, damage):
    fixture = split_db
    manifest, directory_target = await _publish_sealed_attachment(fixture)
    grant = FHIRCatalogStandingGrant("registry-reader", 1, fixture.fixture.control_schema, ())
    dsn = make_url(fixture.dsn)
    if damage == "database":
        dsn = dsn.set(database="postgres")
    role = fixture.roles_by_kind["execution" if damage == "actor" else "reader"]
    async with (
        configured_catalog_reader(
            monkeypatch, dsn.render_as_string(hide_password=False), role, fixture.role_password
        ) as database,
        _login(fixture, "execution") as writer,
    ):
        if damage == "transaction":
            reader_pool = await database._connect_reader()
            reader_pool.engine.update_execution_options(isolation_level="READ COMMITTED", postgresql_readonly=False)
        if damage is not None:
            with pytest.raises(ValueError, match="registry_reader_changed"):
                async with fhir_catalog_registry_reads(writer), writer.transaction(isolation="repeatable_read"):
                    await _catalog_registry_head(writer, grant)
        else:
            lock_query = (
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=$1 AND relation=to_regclass($2) "
                "AND mode='AccessShareLock' AND granted)"
            )
            async with fhir_catalog_registry_reads(writer):
                async with writer.transaction(isolation="repeatable_read"):
                    _, current, _ = await _catalog_registry_head(writer, grant)
                    assert current.manifest_sha256 == manifest["manifest_sha256"]
                    reader = await fhir_catalog_registry_connection(writer)
                    pid = reader.get_server_pid()
                    relation = f'"{directory_target.schema_name}".network_membership'
                    assert await fixture.fixture.connection.fetchval(lock_query, pid, relation)
                assert not writer.is_in_transaction()
                assert await fixture.fixture.connection.fetchval(lock_query, pid, relation)
            assert not await fixture.fixture.connection.fetchval(lock_query, pid, relation)
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == manifest["generation_id"]
    )
    assert not await fixture.fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT FROM "{fixture.fixture.control_schema}".registry_fhir_catalog_snapshot)'
    )


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


async def _assert_catalog_writer_permissions(logins, directory_target, config, loader, sealer_pool):
    for kind in ("loader", "sealer", "execution"):
        async with _login(logins, kind) as actor:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await actor.fetchval(f'SELECT count(*) FROM "{directory_target.schema_name}".network_membership')
    permission_options_by_name = {
        "loader_role": logins.roles_by_kind["loader"],
        "sealer_role": logins.roles_by_kind["sealer"],
    }
    async with loader.transaction(readonly=True):
        await verify_fhir_catalog_preparation_permissions(
            loader, config.consumer, **permission_options_by_name, role=logins.roles_by_kind["loader"]
        )
    async with sealer_pool.acquire() as sealer, sealer.transaction(readonly=True):
        await verify_fhir_catalog_preparation_permissions(
            sealer, config.consumer, **permission_options_by_name, role=logins.roles_by_kind["sealer"]
        )


async def _assert_publication_loses_custody(
    config, loader, snapshot_by_field, previous, runtime, lifecycle_pool, execution_pool, monkeypatch
):
    from process import registry_fhir_catalog_publication as publication

    finish = publication.finish_registry_publication_request
    ended_calls = []

    async def finish_without_custody(writer, *args, **kwargs):
        await finish(writer, *args, **kwargs)
        reader = await fhir_catalog_registry_connection(writer)
        await reader.execute("ROLLBACK")
        ended_calls.append(True)

    monkeypatch.setattr(publication, "finish_registry_publication_request", finish_without_custody)
    with pytest.raises(ValueError, match="registry_publication_request_refused"):
        await _publish_catalog_snapshot(
            config, loader, snapshot_by_field, previous, runtime, lifecycle_pool, execution_pool
        )
    assert ended_calls == [True]


def _catalog_pool(fixture, role, max_size):
    return asyncpg.create_pool(fixture.dsn, user=role, password=fixture.role_password, min_size=1, max_size=max_size)


@pytest.mark.asyncio
@pytest.mark.parametrize("reader_ended", [False, True])
async def test_observed_catalog_prepares_with_native_sealed_directory(
    split_db, observed_source_db, monkeypatch, reader_ended
):
    """Publish through Reader custody while every catalog writer lacks membership SELECT."""
    manifest, directory_target = await _publish_sealed_attachment(split_db)
    connection, control = split_db.fixture.connection, split_db.fixture.control_schema
    incumbent = SimpleNamespace(
        relations=await _relations(connection, directory_target.schema_name),
        seal=await connection.fetchrow(f'SELECT * FROM "{control}".network_candidate_custody'),
        head=await connection.fetchrow(f'SELECT * FROM "{control}".network_serving_control'),
    )
    async with _catalog_roles(split_db) as roles_by_kind:
        config = await _prepare_observed_catalog_roles(split_db, observed_source_db, roles_by_kind)
        logins = SimpleNamespace(dsn=split_db.dsn, roles_by_kind=roles_by_kind, role_password=split_db.role_password)
        source_pool = SimpleNamespace(acquire=lambda: _observed_reader(observed_source_db))
        async with (
            configured_catalog_reader(
                monkeypatch, split_db.dsn, split_db.roles_by_kind["reader"], split_db.role_password
            ),
            _login(logins, "loader") as loader,
            _catalog_pool(split_db, roles_by_kind["sealer"], 2) as sealer_pool,
            _catalog_pool(split_db, split_db.roles_by_kind["lifecycle"], 1) as lifecycle_pool,
            _catalog_pool(split_db, roles_by_kind["execution"], 1) as execution_pool,
        ):
            await _assert_catalog_writer_permissions(logins, directory_target, config, loader, sealer_pool)
            snapshot_by_field, previous, accounting = await prepare_fhir_catalog_snapshot(
                config, loader, source_pool, sealer_pool
            )
            assert snapshot_by_field is not None and previous is None
            assert accounting["accepted_sources"] == 1 and accounting["unavailable_sources"] == 0
            assert sealer_pool.get_size() == sealer_pool.get_max_size() == 2
            await _assert_committed_observed_catalog(split_db, loader, snapshot_by_field, manifest)
            runtime = build_fhir_catalog_publication_runtime(config.grant, source_pool)
            if reader_ended:
                await _assert_publication_loses_custody(
                    config, loader, snapshot_by_field, previous, runtime, lifecycle_pool, execution_pool, monkeypatch
                )
                await _assert_committed_observed_catalog(split_db, loader, snapshot_by_field, manifest)
            else:
                publication_receipt = await _publish_catalog_snapshot(
                    config, loader, snapshot_by_field, previous, runtime, lifecycle_pool, execution_pool
                )
                assert publication_receipt["state"] == "completed"
                unchanged, previous, _ = await prepare_fhir_catalog_snapshot(config, loader, source_pool, sealer_pool)
                assert unchanged is None and previous["metadata_generation"] == snapshot_by_field["metadata_generation"]
    await _assert_directory_unchanged(
        split_db,
        directory_target,
        incumbent,
        incumbent.head["metadata_generation"] if reader_ended else snapshot_by_field["metadata_generation"],
    )


async def _assert_directory_unchanged(fixture, directory_target, incumbent, expected_metadata):
    connection, control = fixture.fixture.connection, fixture.fixture.control_schema
    assert await _relations(connection, directory_target.schema_name) == incumbent.relations
    assert await connection.fetchrow(f'SELECT * FROM "{control}".network_candidate_custody') == incumbent.seal
    head_by_field = dict(await connection.fetchrow(f'SELECT * FROM "{control}".network_serving_control'))
    assert head_by_field.pop("metadata_generation") == expected_metadata
    assert head_by_field == {
        key: field_value for key, field_value in dict(incumbent.head).items() if key != "metadata_generation"
    }
