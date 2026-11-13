# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named directory projections use pinned Reader transactions and real preparation."""

import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.registry_fhir_catalog_custody import validate_fhir_catalog_snapshot_record
from process.registry_fhir_catalog_grant import FHIRCatalogStandingGrant, build_fhir_catalog_publication_runtime
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_permissions import (
    install_fhir_catalog_preparation_permissions,
    install_fhir_catalog_publication_permissions,
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
from process.registry_publication_worker import _configured_worker_runtime
from tests.fhir_catalog_registry_reader_support import configured_catalog_reader
from tests.registry_candidate_custody_postgres_support import administrative_freeze_callback
from tests.registry_candidate_custody_split_postgres_support import (
    _install_named_worker_permissions,
    _login,
    _prepare_split_dependency,
    _prepare_split_successor,
    _relations,
)
from tests.registry_candidate_custody_split_postgres_support import split_db as split_db
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_fhir_catalog_custody_postgres import cleanup_catalog_preparations
from tests.test_registry_fhir_catalog_observed_source_postgres import observed_source_db as observed_source_db


@pytest.fixture
async def catalog_directory(split_db, monkeypatch):
    """Install genuine preparation controls and remove only this fixture's records."""
    from directory_membership_test_support import (
        cleanup_registry_network_preparations,
        prepare_registry_catalog_foundation,
    )

    common = SimpleNamespace(
        connection=split_db.fixture.connection,
        engine=split_db.fixture.engine,
        created_control_namespaces=[],
    )
    is_prepared = False
    try:
        await prepare_registry_catalog_foundation(common, monkeypatch)
        is_prepared = True
        yield split_db
    finally:
        if is_prepared:
            candidates = await common.connection.fetch(
                f'SELECT candidate_id FROM "{split_db.fixture.control_schema}".network_membership_candidate'
            )
            await cleanup_registry_network_preparations(
                common.connection, [row["candidate_id"] for row in candidates], split_db.fixture.control_schema
            )
        for namespace in reversed(common.created_control_namespaces):
            await common.connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')


async def _publish_sealed_attachment(fixture):
    """Compose, administratively freeze and publish through the existing runtime."""
    schema = fixture.fixture.control_schema
    await _install_named_worker_permissions(fixture)
    async with AsyncExitStack() as resources, _login(fixture, "execution") as execution:
        consumer = await _configured_worker_runtime(fixture.config, resources)
        dependency = await _prepare_split_dependency(fixture)
        copy_target, address = await _prepare_split_successor(fixture, execution, consumer, dependency, 0)
        manifest = await prepare_and_publish_network_candidate(
            execution,
            copy_target,
            address,
            owner_role=fixture.roles_by_kind["physical"],
            loader_roles=(),
            reader_roles=fixture.readers,
            control_schema=schema,
            seal_candidate=administrative_freeze_callback(fixture.fixture, consumer),
        )
        assert (
            await fixture.fixture.connection.fetchval(
                "SELECT state FROM hp_snapshot_retention.preparation WHERE preparation_id=$1", copy_target.candidate_id
            )
            == "published"
        )
    return manifest, copy_target


@pytest.mark.asyncio
async def test_named_attachment_projection_uses_repeatable_read_registry_snapshot(catalog_directory):
    fixture = catalog_directory
    manifest, _ = await _publish_sealed_attachment(fixture)
    snapshot_by_field = {
        "network_generation": manifest["generation_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "source_vector_json": [],
    }
    grant = FHIRCatalogStandingGrant("directory-projection", 1, fixture.fixture.control_schema, ())
    async with _login(fixture, "reader") as registry_reader:
        async with registry_reader.transaction(isolation="repeatable_read", readonly=True):
            assert await registry_reader.fetchval("SHOW transaction_read_only") == "on"
            assert await registry_reader.fetchval("SHOW transaction_isolation") == "repeatable read"
            for _ in range(2):
                assert (
                    await resolve_fhir_catalog_projection_generations(
                        registry_reader, snapshot_by_field, grant=grant, descriptors={}
                    )
                    == ()
                )
        assert not registry_reader.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("configured_read_committed", [False, True])
async def test_configured_registry_reader_keeps_snapshot_through_writer_commit(
    catalog_directory, monkeypatch, configured_read_committed
):
    fixture = catalog_directory
    manifest, _ = await _publish_sealed_attachment(fixture)
    grant = FHIRCatalogStandingGrant("registry-reader", 1, fixture.fixture.control_schema, ())
    async with (
        configured_catalog_reader(
            monkeypatch, fixture.dsn, fixture.roles_by_kind["reader"], fixture.role_password
        ) as database,
        _login(fixture, "execution") as writer,
    ):
        if configured_read_committed:
            reader_pool = await database._connect_reader()
            reader_pool.engine.update_execution_options(isolation_level="READ COMMITTED", postgresql_readonly=False)
        async with fhir_catalog_registry_reads(writer):
            async with writer.transaction(isolation="repeatable_read"):
                _, current, _ = await _catalog_registry_head(writer, grant)
                assert current.manifest_sha256 == manifest["manifest_sha256"]
                reader = await fhir_catalog_registry_connection(writer)
                assert await reader.fetchval("SHOW transaction_isolation") == "repeatable read"
                assert await reader.fetchval("SHOW transaction_read_only") == "on"
            assert not writer.is_in_transaction() and reader.is_in_transaction()
            assert (
                await reader.fetchval(
                    f'SELECT generation_id FROM "{fixture.fixture.control_schema}".network_serving_control WHERE id=1'
                )
                == manifest["generation_id"]
            )
        assert not reader.is_in_transaction()
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
        snapshots = await connection.fetch(
            f'SELECT schema_name FROM "{fixture.fixture.control_schema}".registry_fhir_catalog_snapshot'
        )
        await cleanup_catalog_preparations(connection, [row["schema_name"] for row in snapshots])
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
    await observed_source.admin.execute(f'''ALTER TABLE "{observed_source.header_schema}".provider_directory_endpoint_dataset
      ADD COLUMN content_proof_resource_types text[],ADD COLUMN publication_metadata_summary_json jsonb,
      ADD COLUMN completion_proof_required_version integer,ADD COLUMN completion_proof_json jsonb,
      ADD COLUMN completion_proof_sha256 text;
      CREATE TABLE "{observed_source.schema}".provider_directory_insurance_network_source_binding(
      source_id text,resource_id text,network_id uuid)''')
    source_reader = observed_source.roles_by_kind["reader"]
    await observed_source.admin.execute(
        f'GRANT SELECT ON "{observed_source.schema}".provider_directory_insurance_network_source_binding TO "{source_reader}"'
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
    preparation_roles_by_kind = {
        "namespace_owner_role": roles_by_kind["owner"],
        "publisher_role": roles_by_kind["sealer"],
        "application_roles": sorted(
            set(fixture.config.preparation_roles["application_roles"])
            | set(readers)
            | {roles_by_kind[kind] for kind in ("loader", "sealer", "execution")}
        ),
    }
    return SimpleNamespace(
        grant=grant,
        consumer=consumer,
        loader_role=roles_by_kind["loader"],
        reader_roles=readers,
        preparation_roles=preparation_roles_by_kind,
    )


@asynccontextmanager
async def _observed_reader(observed_source):
    reader = await observed_source.connect()
    try:
        yield reader
    finally:
        await reader.close()


async def _prepare_observed_catalog(fixture, config, loader, source_pool, sealer_pool):
    """Resume the same candidate only after the real administrator has frozen it."""
    from directory_catalog_runtime import freeze_registry_generation

    from process.registry_fhir_catalog_store import catalog_snapshot_preparation, load_named_catalog_preparation

    prepared = await prepare_fhir_catalog_snapshot(config, loader, source_pool, sealer_pool)
    snapshot, _, accounting = prepared
    if snapshot is None or accounting.get("preparation_state") != "handed_off":
        return prepared
    candidate = catalog_snapshot_preparation(snapshot)["target"]["candidate_id"]
    handoff = await load_named_catalog_preparation(
        fixture.fixture.connection, candidate, control_schema=config.grant.control_schema
    )
    admin_config_by_field = {
        **{field: handoff[field] for field in ("kind", "candidate_id", "catalog_schema", "storage_schema")},
        "preparation_roles": config.preparation_roles,
    }
    async with fixture.fixture.engine.connect() as administrator:
        frozen = await freeze_registry_generation(administrator, admin_config_by_field, candidate)
    assert frozen["state"] == "frozen"
    resumed = await prepare_fhir_catalog_snapshot(config, loader, source_pool, sealer_pool)
    assert resumed[0]["metadata_generation"] == snapshot["metadata_generation"]
    assert resumed[2]["preparation_state"] == "validated"
    return resumed


async def _assert_committed_observed_catalog(fixture, loader, snapshot_by_field, manifest):
    """Require committed data validation and real observations after preparation."""
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
    assert snapshot_by_field["custody_json"]["content_sha256"]
    schema = '"' + snapshot_by_field["schema_name"] + '"'
    async with loader.transaction(isolation="repeatable_read", readonly=True):
        await validate_fhir_catalog_snapshot_record(loader, snapshot_by_field)
        descriptor = json.loads(
            await loader.fetchval(f"SELECT descriptor_json FROM {schema}.registry_fhir_catalog_source")
        )
        assert descriptor["admission"]["kind"] == "catalog_observation"
        assert descriptor["admission"]["contract"] == CATALOG_OBSERVATION_CONTRACT
        assert await loader.fetchval(f"SELECT count(*) FROM {schema}.registry_fhir_catalog_observation") > 0
        assert await loader.fetchval(f"SELECT count(*) FROM {schema}.registry_fhir_catalog_network") > 0
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await loader.execute(f"DELETE FROM {schema}.registry_fhir_catalog_observation WHERE false")


async def _assert_catalog_writer_permissions(logins, directory_target):
    for kind in ("loader", "sealer", "execution"):
        async with _login(logins, kind) as actor:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await actor.fetchval(f'SELECT count(*) FROM "{directory_target.schema_name}".network_membership')


async def _assert_publication_requires_active_reader(
    config, loader, snapshot_by_field, previous, runtime, lifecycle_pool, execution_pool, monkeypatch
):
    from process import registry_fhir_catalog_publication as publication

    finish = publication.finish_registry_publication_request
    ended_calls = []

    async def finish_after_reader_rollback(writer, *args, **kwargs):
        await finish(writer, *args, **kwargs)
        reader = await fhir_catalog_registry_connection(writer)
        await reader.execute("ROLLBACK")
        ended_calls.append(True)

    monkeypatch.setattr(publication, "finish_registry_publication_request", finish_after_reader_rollback)
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
    catalog_directory, observed_source_db, monkeypatch, reader_ended
):
    """Keep pinned Reader lifetime while catalog writers lack membership SELECT."""
    split_db = catalog_directory
    manifest, directory_target = await _publish_sealed_attachment(split_db)
    connection, control = split_db.fixture.connection, split_db.fixture.control_schema
    incumbent = SimpleNamespace(
        relations=await _relations(connection, directory_target.schema_name),
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
            await _assert_catalog_writer_permissions(logins, directory_target)
            snapshot_by_field, previous, accounting = await _prepare_observed_catalog(
                split_db, config, loader, source_pool, sealer_pool
            )
            assert snapshot_by_field is not None and previous is None
            assert accounting["accepted_sources"] == 1 and accounting["unavailable_sources"] == 0
            assert sealer_pool.get_size() == sealer_pool.get_max_size() == 2
            await _assert_committed_observed_catalog(split_db, loader, snapshot_by_field, manifest)
            runtime = replace(
                build_fhir_catalog_publication_runtime(config.grant, source_pool),
                preparation_roles=config.preparation_roles,
            )
            if reader_ended:
                await _assert_publication_requires_active_reader(
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
    head_by_field = dict(await connection.fetchrow(f'SELECT * FROM "{control}".network_serving_control'))
    assert head_by_field.pop("metadata_generation") == expected_metadata
    assert head_by_field == {
        key: field_value for key, field_value in dict(incumbent.head).items() if key != "metadata_generation"
    }
