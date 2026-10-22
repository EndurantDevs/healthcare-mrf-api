# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual migration and retained eight-file fixtures for CMS admission proofs."""

import asyncio
import importlib
import importlib.util
import json
import os
import re
import shutil
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from db.connection import Database
from process import cms_npd_source as source
from process import control_lifecycle
from tests.test_cms_npd_source import _client, _source

fhir = importlib.import_module("process.provider_directory_fhir")
_MIGRATIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"
_DSN_ENV = "HLTHPRT_CMS_NPD_ADMISSION_TEST_DSN"
_ARTIFACT_ENV = "HLTHPRT_CMS_NPD_ADMISSION_TEST_ARTIFACT_ROOT"

# This is the admission dependency slice, not a fresh-install Alembic chain.
MIGRATION_PREFIXES = (
    "20260610120000",
    "20260611100000",
    "20260628100000",
    "20260628170000",
    "20260629100000",
    "20260629123000",
    "20260630112000",
    "20260701120000",
    "20260710003000",
    "20260710010000",
    "20260710110000",
    "20260710143000",
    "20260711120000",
    "20260712120000",
    "20260713120000",
    "20260713200000",
    "20260713210000",
    "20260713213000",
    "20260713220000",
    "20260713230000",
    "20260713233000",
    "20260713234000",
    "20260713235000",
    "20260713236000",
    "20260713237000",
    "20260714130000",
    "20260714150000",
    "20260714160000",
    "20260720100000",
    "20260720120000",
    "20260721100000",
    "20260728130000",
    "20260729110000",
    "20260730110000",
    "20260801130000",
    "20260807100000",
    "20260808190000",
    "20260808200000",
    "20260808210000",
    "20260809000000",
    "20260809010000",
    "20260809030000",
    "20260810000000",
    "20260810010000",
    "20260810020000",
    "20260810030000",
    "20260810050000",
    "20260810060000",
    "20260810070000",
    "20260810080000",
    "20260810090000",
    "20260810100000",
    "20260810110000",
    "20260810120000_provider",
    "20260810130000",
    "20260811010000",
    "20260811020000",
    "20260811100000",
    "20260811120000",
    "20260811130000",
    "20260812010000",
    "20260812020000",
    "20260812030000",
    "20260813010000",
    "20260814000000",
    "20260816010000",
    "20260818020000",
    "20260824143000",
    "20260830100000",
    "20260904163000",
    "20260904223000",
    "20260914100000",
    "20260914110000",
    "20260929010000",
    "20260929020000",
    "20260929030000",
    "20260930010000",
    "20260930020000",
    "20260930030000_cms_npd_stale_candidate",
    "20260930040000",
    "20260930050000",
    "20260930070000",
    "20260930080000",
    "20260930090000",
    "20260930100000",
    "20260930110000",
    "20260930120000",
    "20260930130000",
    "20260930140000",
    "20261001100000",
)
LEGACY_MIGRATION_PREFIXES = (
    *MIGRATION_PREFIXES[: MIGRATION_PREFIXES.index("20260930100000")],
    "20261015015000",
    "20261022120000",
)


def _database_url():
    raw = os.getenv(_DSN_ENV)
    if not raw:
        pytest.skip(f"set {_DSN_ENV} for the PostgreSQL proof")
    url = make_url(raw)
    allowed_hosts = {"127.0.0.1", "localhost"}
    if os.getenv("CI") == "true":
        allowed_hosts.add("postgres")
    if (
        not url.drivername.startswith("postgresql")
        or url.query
        or url.host not in allowed_hosts
        or url.port is None
        or not 1 <= url.port <= 65535
        or not re.fullmatch(r"hc_cms_admission_test_[0-9a-f]{32}", url.database or "")
    ):
        pytest.fail("CMS admission proof requires a UUID-owned local PostgreSQL test database")
    return url


def _run_migrations(connection, migration_prefixes=None):
    migration_prefixes = MIGRATION_PREFIXES if migration_prefixes is None else migration_prefixes
    context = MigrationContext.configure(connection)
    with context.begin_transaction(), Operations.context(context):
        for prefix in migration_prefixes:
            paths = list(_MIGRATIONS.glob(prefix + "*.py"))
            if len(paths) > 1:
                paths = [path for path in paths if "provider_directory" in path.name]
            assert len(paths) == 1, prefix
            spec = importlib.util.spec_from_file_location("cms_admission_" + paths[0].stem, paths[0])
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            try:
                module.upgrade()
            except Exception as error:
                raise RuntimeError(f"CMS admission migration failed: {paths[0].name}") from error


@asynccontextmanager
async def _owned_database(source_url, template_name="template0"):
    """Create one local UUID database and verify exact cleanup even after setup fails."""
    database_name = "hc_cms_admission_test_" + uuid4().hex
    admin = await asyncpg.connect(source_url.set(drivername="postgresql").render_as_string(hide_password=False))
    is_creation_attempted = False
    try:
        assert not await admin.fetchval("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1)", database_name)
        is_creation_attempted = True
        await admin.execute(f'CREATE DATABASE "{database_name}" TEMPLATE "{template_name}"')
        yield source_url.set(database=database_name), admin
    finally:
        try:
            if is_creation_attempted:
                await admin.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
                assert not await admin.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1)", database_name
                )
                assert not await admin.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=$1)", database_name
                )
        finally:
            await admin.close()
            assert admin.is_closed()


@pytest_asyncio.fixture(scope="module", loop_scope="module", autouse=True)
async def cms_admission_template(request):
    """Migrate one closed baseline and clone every native object for each admission test."""
    migration_prefixes = getattr(request, "param", MIGRATION_PREFIXES)
    source_url = _database_url()
    async with _owned_database(source_url) as (template_url, admin):
        engine = create_async_engine(template_url)
        try:
            with pytest.MonkeyPatch.context() as settings:
                settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
                settings.setenv("DB_SCHEMA", "mrf")
                async with engine.begin() as connection:
                    await connection.exec_driver_sql('CREATE SCHEMA "mrf"')
                async with engine.connect() as connection:
                    await connection.run_sync(_run_migrations, migration_prefixes)
                async with engine.connect() as connection:
                    connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
                    await connection.exec_driver_sql("ANALYZE")
                    await connection.exec_driver_sql("VACUUM (FREEZE)")
        finally:
            await engine.dispose()
        assert not await admin.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=$1)", template_url.database
        )
        await admin.execute(f'ALTER DATABASE "{template_url.database}" ALLOW_CONNECTIONS false')
        with pytest.MonkeyPatch.context() as clone_scope:
            clone_scope.setattr(
                f"{__name__}._admission_database_url",
                partial(
                    _admission_database_url,
                    source_url,
                    template_url.database,
                    template_migration_prefixes=migration_prefixes,
                ),
            )
            yield


@asynccontextmanager
async def _admission_database_url(
    template_source_url=None, template_name=None, *, template_migration_prefixes=None, migration_prefixes
):
    """Use the active module baseline without changing standalone migration fixtures."""
    source_url = _database_url()
    if template_name is None:
        yield source_url, False
        return
    assert source_url == template_source_url
    assert migration_prefixes == template_migration_prefixes, "CMS admission template migration profile mismatch"
    async with _owned_database(source_url, template_name) as (database_url, _admin):
        yield database_url, True


@asynccontextmanager
async def admission_database(monkeypatch, *, migration_prefixes=None):
    """Keep native guards and committed multi-session visibility isolated in each test."""
    migration_prefixes = MIGRATION_PREFIXES if migration_prefixes is None else migration_prefixes
    async with AsyncExitStack() as cleanup:
        url, is_migrated = await cleanup.enter_async_context(
            _admission_database_url(migration_prefixes=migration_prefixes)
        )
        for name, setting_value in {
            "DRIVER": "asyncpg",
            "HOST": url.host,
            "PORT": str(url.port),
            "USER": url.username,
            "PASSWORD": url.password or "",
            "DATABASE": url.database,
            "SCHEMA": "mrf",
        }.items():
            monkeypatch.setenv("HLTHPRT_DB_" + name, setting_value)
        monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        database = Database()
        monkeypatch.setattr(fhir, "db", database)
        control_bindings = cleanup.enter_context(monkeypatch.context())
        control_bindings.setattr(control_lifecycle, "db", database)
        is_schema_created = False
        try:
            await database.connect()
            if not is_migrated:
                async with database.engine.begin() as connection:
                    await connection.exec_driver_sql('CREATE SCHEMA "mrf"')
                is_schema_created = True
                async with database.engine.connect() as connection:
                    await connection.run_sync(_run_migrations, migration_prefixes)
            yield database
        finally:
            try:
                if is_schema_created:
                    await database.status('DROP SCHEMA "mrf" CASCADE')
            finally:
                await database.disconnect()


@asynccontextmanager
async def legacy_source_admission_database(monkeypatch, *, before_provision=None):
    """Run historical publication through actual Source custody and separate administration."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from process.provider_directory_generic_source_policy import bind_generic_source_transaction_factory
    from tests.provider_directory_source_postgres_support import (
        legacy_source_component_migrations,
        source_component_database,
    )

    async with source_component_database(
        monkeypatch,
        bind_factory=False,
        before_provision=before_provision,
        migration_prefixes=legacy_source_component_migrations(),
    ) as component:
        await _provision_cms_epoch_controls(component)
        database = Database(
            engine=component.admin_engine,
            session_factory=async_sessionmaker(component.admin_engine, expire_on_commit=False),
        )
        database._source_component = component
        async with bind_generic_source_transaction_factory(component.factory):
            with monkeypatch.context() as bindings:
                bindings.setattr(fhir, "db", component.database)
                bindings.setattr(control_lifecycle, "db", component.database)
                yield database


@asynccontextmanager
async def protected_admission_database(
    monkeypatch, *, before_provision=None, migration_prefixes=None, publisher_pool_size=None
):
    """Separate genuine SOURCE acquisition from native component administration."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from tests.provider_directory_source_postgres_support import source_component_database

    publisher_options = {} if publisher_pool_size is None else {"publisher_pool_size": publisher_pool_size}
    async with source_component_database(
        monkeypatch,
        bind_factory=False,
        before_provision=before_provision,
        migration_prefixes=migration_prefixes,
        **publisher_options,
    ) as component:
        if migration_prefixes is None or "20261016010000" not in migration_prefixes:
            async with component.admin_engine.begin() as connection:
                await connection.run_sync(_run_migrations, ("20261016010000",))
        await _provision_cms_epoch_controls(component)
        database = Database(
            engine=component.admin_engine,
            session_factory=async_sessionmaker(component.admin_engine, expire_on_commit=False),
        )
        database._source_component = component
        with monkeypatch.context() as source_bindings:
            source_bindings.setattr(fhir, "db", component.database)
            source_bindings.setattr(control_lifecycle, "db", component.database)
            yield database


@asynccontextmanager
async def intake_admission_database(monkeypatch, *, migration_prefixes=None):
    """Enroll original intake calls while keeping fixture administration separate."""
    from process import provider_directory_cms_npd as cms
    from process import provider_directory_generic_source_policy as policy

    assert migration_prefixes in (None, MIGRATION_PREFIXES, LEGACY_MIGRATION_PREFIXES)
    fixture = (
        legacy_source_admission_database
        if migration_prefixes == LEGACY_MIGRATION_PREFIXES
        else protected_admission_database
    )
    with monkeypatch.context() as bindings:
        async with fixture(bindings) as database:
            component = database._source_component
            original = cms._run_acquired

            async def enrolled(*args, **kwargs):
                if policy.is_source_storage_required():
                    return await original(*args, **kwargs)
                async with policy.bind_generic_source_transaction_factory(component.factory):
                    return await original(*args, **kwargs)

            bindings.setattr(cms, "_run_acquired", enrolled)
            if policy.is_source_storage_required():
                yield database
            else:
                async with policy.bind_generic_source_transaction_factory(component.factory):
                    yield database


async def _original_source_identity(component, session, isolation):
    """Prove the original protected Source actor and its native transaction."""
    from process import provider_directory_cms_serving_receipt as serving
    from process import provider_directory_generic_source_policy as policy

    driver = await serving.require_registry_receipt_owner(session, component.policy.schema)
    assert policy.bound_publisher().policy is component.policy
    assert policy.source_copy_session() is session
    assert component.database._transaction_binding().session is session
    assert await driver.fetchval("SHOW transaction_isolation") == isolation
    return driver, driver.get_server_pid(), await driver.fetchval("SELECT pg_current_xact_id()::text")


@asynccontextmanager
async def original_source_transaction(database, *, isolation):
    """Reenter the genuine Source factory only for the original publication block."""
    from process import provider_directory_generic_source_policy as policy

    component = database._source_component
    assert database.engine is component.admin_engine
    assert not policy.is_source_storage_required()
    try:
        async with policy.bind_generic_source_transaction_factory(component.factory):
            async with policy.source_transaction(component.database, isolation=isolation) as session:
                identity = await _original_source_identity(component, session, isolation)
                yield session
                assert await _original_source_identity(component, session, isolation) == identity
    finally:
        assert not policy.is_source_storage_required()
        assert component.database._transaction_binding() is None


async def _provision_cms_epoch_controls(component):
    """Use the real administrator cut and prove ordinary runtime writers are closed."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from process.provider_directory_projection_stage import _copy_driver
    from process.provider_directory_source_cms_epoch_control import (
        EPOCH_CONTROL_ROOTS,
        provision_source_cms_epoch_controls,
    )
    from tests.provider_directory_source_postgres_support import verify_native_source_profile

    async def writers(connection, policy):
        assert policy == component.policy
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class c,pg_roles r WHERE c.relnamespace=$1::regnamespace "
            "AND c.relname=ANY($2::text[]) AND r.rolname=ANY($3::text[]) AND "
            "(has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR "
            "has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')))",
            component.policy.schema,
            list(EPOCH_CONTROL_ROOTS),
            list(component.policy.runtime_roles),
        )

    async with async_sessionmaker(component.admin_engine)() as session, session.begin():
        assert await session.scalar(text("SELECT current_user=session_user"))
        await provision_source_cms_epoch_controls(
            await _copy_driver(session),
            component.policy,
            verify_role_profile=verify_native_source_profile,
            verify_writer_closure=writers,
        )


async def run_protected_admission(database, monkeypatch, run_id, directory, receipt, client):
    """Acquire through the actual original publisher, never the fixture administrator."""
    from process import provider_directory_cms_npd as cms
    from process import provider_directory_generic_source_policy as policy

    component = database._source_component
    assert database.engine is component.admin_engine
    assert component.database.engine.url.database == database.engine.url.database
    assert not policy.is_source_storage_required()
    before = asyncio.all_tasks()
    with monkeypatch.context() as source_bindings:
        source_bindings.setattr(fhir, "db", component.database)
        source_bindings.setattr(control_lifecycle, "db", component.database)
        async with policy.bind_generic_source_transaction_factory(component.factory):
            assert policy.is_source_storage_required()
            result = await cms._run_acquired({"context": {}}, {}, run_id, directory, receipt, client)
            assert policy.is_source_storage_required() and policy.source_transaction_available() is None
            async with component.database.transaction():
                dataset = await component.database.first(
                    "SELECT status,candidate_storage_json FROM mrf.provider_directory_endpoint_dataset "
                    "WHERE dataset_id=:dataset",
                    dataset=result["registry_source_admission"]["network_bindings"]["dataset_id"],
                )
                assert dataset._mapping["status"] == "validated"
                assert dataset._mapping["candidate_storage_json"]["phase"] == "published"
            assert policy.source_transaction_available() is None
            assert not {pending for pending in asyncio.all_tasks() - before if not pending.done()}
    assert not policy.is_source_storage_required()
    return result


@pytest.fixture
def cms_artifact_root():
    """Own one release directory on explicitly configured artifact storage."""
    _database_url()
    configured = os.getenv(_ARTIFACT_ENV)
    if not configured:
        pytest.fail(f"set {_ARTIFACT_ENV} to an existing test-owned artifact directory")
    parent = Path(configured).resolve(strict=True)
    assert parent.is_dir()
    directory = parent / ("cms-admission-fixture-" + uuid4().hex)
    is_created = False
    try:
        directory.mkdir(mode=0o700)
        is_created = True
        yield directory
    finally:
        if is_created:
            shutil.rmtree(directory)
            assert not directory.exists()


def _site_fixture():
    """Keep source-only location identifiers and references in the release."""

    return {
        "id": "site-1",
        "identifier": [{"system": "https://example.test/site", "value": "site-source-1"}],
        "name": "Example Site",
        "address": {"id": "address-source-1", "line": ["1 Sample Street"], "city": "Example City"},
        "managingOrganization": {"reference": "Organization/network-1"},
        "partOf": {"reference": "Location/parent-site", "type": "Location", "display": "Parent Site"},
        "endpoint": [
            {"identifier": {"system": "https://example.test/endpoint", "value": "site-endpoint"}, "type": "Endpoint"}
        ],
    }


def _plan_fixture():
    """Keep rich references and an otherwise unmapped extension."""

    return {
        "id": "plan-1",
        "network": [{"reference": "Organization/network-1"}, {"reference": "Organization/unresolved"}],
        "plan": [
            {
                "network": [{"reference": "Organization/network-1"}],
                "coverageArea": [{"reference": "Location/site-1"}],
            }
        ],
        "coverage": [{"network": [{"reference": "Organization/network-1"}]}],
        "ownedBy": {
            "reference": "Organization/insurer-1",
            "identifier": {"system": "https://example.test/organization", "value": "insurer-source-1"},
            "type": "Organization",
            "display": "Example Insurer",
        },
        "endpoint": [{"reference": "Endpoint/endpoint-1", "display": "Plan Endpoint"}],
        "extension": [
            {"url": "https://example.test/fhir/StructureDefinition/source-note", "valueString": "Synthetic value"}
        ],
    }


def _resource_rows(revision):
    """Build the eight synthetic FHIR resource sets for a release."""
    return {
        "Organization": [
            {
                "id": "network-1",
                "name": "Example Network " + revision,
                "identifier": [{"system": "urn:cms:npd:pseudo-ein", "value": "00-0000000"}],
                "endpoint": [{"reference": "Endpoint/endpoint-1"}],
            },
            {"id": "insurer-1", "name": "Example Insurer"},
        ],
        "Location": [_site_fixture()],
        "Endpoint": [
            {
                "id": "endpoint-1",
                "status": "active",
                "address": "https://example.test/fhir",
                "managingOrganization": {"reference": "Organization/insurer-1"},
            }
        ],
        "HealthcareService": [{"id": "service-1", "providedBy": {"reference": "Organization/network-1"}}],
        "InsurancePlan": [_plan_fixture()],
        "Practitioner": [
            {
                "id": "1234567893",
                "qualification": [
                    {
                        "issuer": {"reference": "Organization/insurer-1"},
                        "period": {"start": "2025-01-01"},
                    }
                ],
            }
        ],
        "PractitionerRole": [
            {
                "id": "role-1",
                "practitioner": {"reference": "Practitioner/1234567893"},
                "organization": {"reference": "Organization/network-1"},
                "location": [{"reference": "Location/site-1"}],
            }
        ],
        "OrganizationAffiliation": [
            {
                "id": "affiliation-1",
                "organization": {"reference": "Organization/insurer-1"},
                "participatingOrganization": {"reference": "Organization/network-1"},
            }
        ],
    }


def retained_release(
    root,
    *,
    revision="first",
    empty_resource_type=None,
    include_missing_network=True,
    include_unplanned_network_role=False,
):
    """Acquire and seal eight compressed files through the real source validator."""
    resources_by_type = _resource_rows(revision)
    if include_unplanned_network_role:
        resources_by_type["Organization"].append(
            {
                "id": "unplanned-network",
                "name": "Unplanned Network",
                "type": [{"text": "ntwk"}],
            }
        )
    if not include_missing_network:
        resources_by_type["InsurancePlan"][0]["network"] = [{"reference": "Organization/network-1"}]
    if empty_resource_type is not None:
        assert empty_resource_type in resources_by_type
        resources_by_type[empty_resource_type] = []
    rows_by_file = {
        name: b"".join(
            json.dumps({"resourceType": kind, **resource_by_field}).encode() + b"\n"
            for resource_by_field in resources_by_type[kind]
        )
        for name, kind in source.RESOURCE_FILES
    }
    manifest, payloads = _source(rows_by_file)
    client, _ = _client(manifest, payloads)
    with client:
        return source.acquire_release(root, client=client)


def release_probe_client(directory):
    """Serve the exact synthetic publisher manifest and range probes without network access."""
    manifest_bytes = (directory / "manifest.json").read_bytes()

    def respond(request):
        filename = request.url.path.rsplit("/", 1)[-1]
        if filename == "manifest.json":
            return httpx.Response(200, content=manifest_bytes)
        assert request.headers["range"] == "bytes=0-0"
        encoded = (directory / filename).read_bytes()
        return httpx.Response(
            206,
            content=encoded[:1],
            headers={
                "etag": '"synthetic-v1"',
                "content-range": f"bytes 0-0/{len(encoded)}",
            },
        )

    return httpx.Client(transport=httpx.MockTransport(respond))
