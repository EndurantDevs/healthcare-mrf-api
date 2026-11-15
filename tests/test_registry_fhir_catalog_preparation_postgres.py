# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Normal catalog grants, separate administrator freeze and SQL privilege denials."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from db.registry_fhir_catalog import PROJECTOR_VERSION
from process.registry_fhir_catalog_custody import FHIRCatalogTarget
from process.registry_fhir_catalog_fingerprints import approved_fhir_catalog_fingerprints
from process.registry_fhir_catalog_identity import FHIRNetworkIdentityInput, allocate_fhir_catalog_network_ids
from process.registry_fhir_catalog_permissions import (
    install_fhir_catalog_preparation_permissions,
    install_fhir_catalog_publication_permissions,
)
from process.registry_fhir_catalog_publication import FHIRCatalogPublicationRuntime
from process.registry_fhir_catalog_store import create_fhir_catalog_candidate, create_fhir_catalog_indexes
from process.registry_fhir_catalog_validation import metadata_generation
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_fhir_catalog_custody_postgres import (
    _prepare_common_catalogs,
    cleanup_catalog_preparations,
    freeze_metadata,
)
from tests.test_registry_publication_worker_permissions_postgres import _login
from tests.test_registry_publication_worker_permissions_postgres import worker_permissions as worker_permissions

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def catalog_preparation(worker_permissions, monkeypatch):
    worker = worker_permissions
    connection = worker.fixture.connection
    roles_by_kind = {
        kind: "catalog_prepare_" + kind + "_" + uuid4().hex for kind in ("loader", "sealer", "helper", "bridge")
    }
    created_roles, schemas = [], []
    common = SimpleNamespace(
        connection=connection,
        engine=worker.fixture.engine,
        created_control_namespaces=[],
        created_extensions=[],
    )
    try:
        await _prepare_common_catalogs(common, monkeypatch)
        for kind, name in roles_by_kind.items():
            login = "NOLOGIN" if kind in {"helper", "bridge"} else f"LOGIN PASSWORD '{worker.role_password}'"
            await connection.execute(f'CREATE ROLE "{name}" {login}')
            created_roles.append(name)
        roles_by_kind = {**worker.roles_by_kind, **roles_by_kind}
        config = RegistryPublicationConsumerConfig(
            worker.fixture.control_schema,
            roles_by_kind["lifecycle"],
            roles_by_kind["execution"],
            roles_by_kind["api"],
            roles_by_kind["queue"],
            roles_by_kind["physical"],
            (roles_by_kind["loader"],),
            (roles_by_kind["api"], roles_by_kind["execution"], roles_by_kind["loader"]),
            metadata_runtime=FHIRCatalogPublicationRuntime(AsyncMock(), AsyncMock(), AsyncMock()),
            metadata_only=True,
        )
        # Model the additive migration before the API has read access to its new ledger.
        await connection.execute(
            f'REVOKE SELECT ON "{config.control_schema}".registry_fhir_catalog_snapshot FROM "{roles_by_kind["api"]}"'
        )
        yield SimpleNamespace(
            fixture=worker.fixture,
            seed=worker.seed,
            roles_by_kind=roles_by_kind,
            role_password=worker.role_password,
            config=config,
            options={"loader_role": roles_by_kind["loader"], "sealer_role": roles_by_kind["sealer"]},
            schemas=schemas,
        )
    finally:
        await cleanup_catalog_preparations(connection, schemas)
        for schema in reversed(schemas):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        for namespace in reversed(common.created_control_namespaces):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
        for extension in reversed(common.created_extensions):
            await connection.execute(f'DROP EXTENSION IF EXISTS "{extension}"')
        for role in reversed(created_roles):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')


async def _install(fixture):
    async with fixture.fixture.connection.transaction():
        return await install_fhir_catalog_preparation_permissions(
            fixture.fixture.connection, fixture.config, **fixture.options
        )


async def _load_empty_catalog(fixture):
    """Allocate one identity and prepare an empty candidate as its real Loader."""
    config, roles_by_kind = fixture.config, fixture.roles_by_kind
    async with _login(fixture, "loader") as loader, loader.transaction():
        allocation = await allocate_fhir_catalog_network_ids(
            loader,
            (FHIRNetworkIdentityInput("network-one", "endpoint-one", ("source-one",), "edition-one"),),
            approved_revision=fixture.seed.revision,
            control_schema=config.control_schema,
        )
        assert allocation["created_networks"] == 1
        candidate = uuid4()
        catalog_target = FHIRCatalogTarget(
            str(uuid4()), str(uuid4()), str(uuid4()), str(candidate), "fhir_catalog_" + candidate.hex
        )
        fixture.schemas.append(catalog_target.schema_name)
        await create_fhir_catalog_candidate(
            loader, catalog_target, loader_role=roles_by_kind["loader"], control_schema=config.control_schema
        )
        await create_fhir_catalog_indexes(
            loader, catalog_target, loader_role=roles_by_kind["loader"], control_schema=config.control_schema
        )
        fingerprints = (
            await approved_fhir_catalog_fingerprints(
                loader,
                (fixture.seed.revision,),
                control_schema=config.control_schema,
            )
        )[fixture.seed.revision]
    snapshot_by_field = {
        "schema_name": catalog_target.schema_name,
        "network_generation": None,
        "manifest_sha256": None,
        "metadata_approved_revision": fixture.seed.revision,
        "membership_mapping_sha256": fingerprints["membership"],
        "mapping_sha256": fingerprints["metadata"],
        "standing_grant_sha256": "a" * 64,
        "source_vector_json": [],
        "projector_version": PROJECTOR_VERSION,
        "as_of": date(2026, 1, 1),
    }
    snapshot_by_field["metadata_generation"] = metadata_generation(snapshot_by_field)
    return catalog_target, snapshot_by_field


async def _seal_empty_catalog(fixture, catalog_target, snapshot_by_field):
    """Use the real sealer checkpoint and independent administrator freeze."""
    config, roles = fixture.config, fixture.roles_by_kind
    async with _login(fixture, "sealer") as sealer:
        catalog = SimpleNamespace(
            connection=fixture.fixture.connection,
            publisher=sealer,
            engine=fixture.fixture.engine,
            control_schema=config.control_schema,
            target=catalog_target,
            roles={
                "owner": roles["physical"],
                "loader": roles["loader"],
                "publisher": roles["sealer"],
                "reader": roles["api"],
            },
            preparation_roles={
                "namespace_owner_role": roles["physical"],
                "publisher_role": roles["sealer"],
                "application_roles": sorted(
                    {
                        roles["loader"],
                        roles["sealer"],
                        config.lifecycle_role,
                        config.execution_role,
                        config.api_role,
                        *config.loader_roles,
                        *config.reader_roles,
                    }
                ),
            },
        )
        await freeze_metadata(catalog, catalog_target, snapshot_by_field)


async def test_preparation_preserves_reads_and_restricted_writes(catalog_preparation):
    fixture = catalog_preparation
    connection, config, roles = fixture.fixture.connection, fixture.config, fixture.roles_by_kind
    queries = tuple(
        f'SELECT count(*) FROM "{config.control_schema}".{table}'
        for table in ("network_serving_control", "network_serving_manifest")
    )
    async with _login(fixture, "api") as api:
        initial_counts = [await api.fetchval(query) for query in queries]
    receipt = await _install(fixture)
    assert await _install(fixture) == receipt
    async with _login(fixture, "api") as api:
        assert [await api.fetchval(query) for query in queries] == initial_counts
    async with connection.transaction():
        await install_fhir_catalog_publication_permissions(connection, config)
    catalog_target, snapshot = await _load_empty_catalog(fixture)
    await _seal_empty_catalog(fixture, catalog_target, snapshot)
    async with _login(fixture, "api") as api:
        assert await api.fetchval(f'SELECT count(*) FROM "{config.control_schema}".registry_fhir_catalog_snapshot') == 1
    for kind in ("loader", "sealer"):
        async with _login(fixture, kind) as actor:
            for statement in (
                f'UPDATE "{config.control_schema}".registry_fhir_catalog_snapshot SET published_at=now() WHERE false',
                f'UPDATE "{config.control_schema}".registry_revision_control SET approved_revision=approved_revision WHERE false',
                f'UPDATE "{config.control_schema}".registry_publication_request SET state=state WHERE false',
                f'INSERT INTO "{config.control_schema}".network_registry_alias(created_at) SELECT now() WHERE false',
                f'SELECT * FROM "{fixture.fixture.source_schema}".npi',
                f'CREATE TABLE "{config.control_schema}".unapproved(id integer)',
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await actor.execute(statement)
    for kind in ("loader", "sealer", "api", "lifecycle", "execution"):
        async with _login(fixture, kind) as actor:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await actor.execute(f'SET ROLE "{roles["physical"]}"')


async def test_preparation_requires_transaction(catalog_preparation):
    fixture = catalog_preparation
    with pytest.raises(ValueError, match="preparation_permissions_invalid"):
        await install_fhir_catalog_preparation_permissions(
            fixture.fixture.connection, fixture.config, **fixture.options
        )


async def test_preparation_grants_roll_back(catalog_preparation):
    fixture = catalog_preparation
    with pytest.raises(RuntimeError, match="operator rollback"):
        async with fixture.fixture.connection.transaction():
            await install_fhir_catalog_preparation_permissions(
                fixture.fixture.connection, fixture.config, **fixture.options
            )
            raise RuntimeError("operator rollback")
    async with _login(fixture, "api") as api:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await api.fetchval(f'SELECT count(*) FROM "{fixture.config.control_schema}".registry_fhir_catalog_snapshot')


async def test_loader_requires_candidate_creation_permission(catalog_preparation):
    fixture = catalog_preparation
    await _install(fixture)
    database = await fixture.fixture.connection.fetchval("SELECT quote_ident(current_database())")
    await fixture.fixture.connection.execute(
        f'REVOKE CREATE ON DATABASE {database} FROM "{fixture.roles_by_kind["loader"]}"'
    )
    before = await fixture.fixture.connection.fetchval(
        f'SELECT count(*) FROM "{fixture.config.control_schema}".network_registry_identity'
    )
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await _load_empty_catalog(fixture)
    assert (
        await fixture.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.config.control_schema}".network_registry_identity'
        )
        == before
    )
