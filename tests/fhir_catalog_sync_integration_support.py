# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owned native import and registry fixtures for the connected catalog test."""

import asyncio
import copy
import json
import os
import subprocess
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
from sqlalchemy.engine import make_url

from process import provider_directory_cms_serving as source_serving
from process import provider_directory_generic_source_policy as source_policy
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.registry_fhir_catalog_grant import FHIRCatalogStandingGrant, build_fhir_catalog_publication_runtime
from process.registry_fhir_catalog_permissions import install_fhir_catalog_publication_permissions
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile
from process.registry_fhir_catalog_worker import RegistryFHIRCatalogSyncConfig
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from process.registry_publication_permissions import INSERT_COLUMNS
from tests import cms_npd_admission_postgres_support as imported
from tests import cms_registry_complete_publication_postgres_support as native
from tests.test_network_cms_registry_complete_publication_postgres import _paired_native_execution
from tests.test_network_custom_address_source_postgres import _seed, custom_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_fhir_catalog_enrichment import COMPANY, GROUP, context
from tests.test_registry_publication_worker_permissions_postgres import (
    _install,
    _login,
    _native_qualified_candidate,
    _worker_roles,
)


def source_resources(original, revision, *, retain_old):
    resources = copy.deepcopy(native.office_resources(original, revision))
    networks = ["network-new", "network-old"] if retain_old else ["network-new"]
    resources["Organization"] = [{"id": "insurer-1", "name": "Example Insurance"}] + [
        {"id": identifier, "name": identifier + " " + revision, "type": [{"coding": [{"code": "ntwk"}]}]}
        for identifier in networks
    ]
    resources["InsurancePlan"] = [
        {
            "id": "plan-" + identifier,
            "name": "Gold " + identifier,
            "ownedBy": {"reference": "Organization/insurer-1"},
            "network": [{"reference": "Organization/" + identifier}],
        }
        for identifier in networks
    ]

    def references(value):
        if isinstance(value, dict):
            return {key: references(item) for key, item in value.items()}
        if isinstance(value, list):
            return [references(item) for item in value]
        return "Organization/network-new" if value == "Organization/network-1" else value

    return references(resources)


async def import_source(case, directory, *, retain_old, revision):
    """Run actual retained-file intake, then publish its admitted Source head."""
    with case.settings.context() as settings:
        settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        settings.setenv("DB_SCHEMA", "mrf")
        return await _import_source(case, directory, retain_old=retain_old, revision=revision)


async def _import_source(case, directory, *, retain_old, revision):
    directory.mkdir(parents=True, exist_ok=True)
    await case.fixture.connection.execute(f'GRANT CREATE ON SCHEMA mrf TO "{case.component.builder}"')
    with case.settings.context() as settings:
        original = imported._resource_rows
        settings.setattr(
            imported, "_resource_rows", lambda value: source_resources(original, value, retain_old=retain_old)
        )
        root, receipt = imported.retained_release(directory, revision=revision, include_missing_network=True)
        with imported.release_probe_client(root) as client:
            admission_result = await imported.run_protected_admission(
                case.administrator,
                settings,
                "catalog-import-" + uuid4().hex,
                root,
                receipt,
                client,
            )
    fhir, component = imported.fhir, case.component
    async with source_policy.bind_generic_source_transaction_factory(component.factory):
        execution = await native.attested_execution(
            case.administrator, admission_result, case.settings, generation=case.source_generation
        )
        run_id = "run_" + uuid4().hex
        await case.administrator.status(
            "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics) "
            "VALUES(:run,'synthetic','provider-directory-fhir','running','{}','{}')",
            run=run_id,
        )
        while True:
            remaining = await case.fixture.connection.fetchval(
                "SELECT extract(epoch FROM max(expires_at)-clock_timestamp()) "
                "FROM mrf.provider_directory_profile_capacity_lease_consumption"
            )
            if remaining is None or remaining <= 0:
                break
            assert remaining <= 960, "unexpected capacity reservation in the owned fixture"
            # Keep the real quiescence guard and immutable lease ledger intact.
            await asyncio.sleep(min(float(remaining) + 0.1, 30))
        execution = await _paired_native_execution(case.settings, execution)
        await source_serving.publish_current_attested_profile(
            fhir, execution=execution, run_id=run_id, control_run_id=run_id, metrics={}
        )
    case.source_generation += 1
    await case.fixture.connection.execute(f'REVOKE CREATE ON SCHEMA mrf FROM "{case.component.builder}"')
    return admission_result


@asynccontextmanager
async def native_catalog_case(settings, tmp_path):
    base = make_url(os.environ["NETWORK_REGISTRY_TEST_DSN"]).set(drivername="postgresql+asyncpg")
    async with imported._owned_database(base) as (owned, _):
        settings.setenv(imported._DSN_ENV, owned.render_as_string(hide_password=False))
        settings.setenv("HLTHPRT_CMS_NPD_STAGE_WORKERS", "0")
        settings.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", "true")
        settings.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "true")
        settings.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", "2")
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        async with native.source_native_publication_database(settings, tmp_path, commit) as administrator:
            component = administrator._source_component
            settings.setenv(
                "NETWORK_REGISTRY_TEST_DSN", component.admin_engine.url.render_as_string(hide_password=False)
            )
            async with AsyncExitStack() as stack:
                schema_generator = await stack.enter_async_context(aclosing(serving_schema.__wrapped__()))
                schema = await anext(schema_generator)
                for extension in ("intarray", "pg_trgm", "btree_gin", "postgis"):
                    await schema[0].execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
                custom_generator = await stack.enter_async_context(aclosing(custom_db.__wrapped__(schema)))
                fixture = await anext(custom_generator)
                seed = await _seed(fixture)
                worker = await stack.enter_async_context(_worker_roles(fixture, seed))
                await _install(worker)
                case = SimpleNamespace(
                    settings=settings,
                    component=component,
                    administrator=administrator,
                    fixture=fixture,
                    worker=worker,
                    seed=seed,
                    manifest=None,
                    source_generation=1,
                )
                await _organization_maps(case)
                async with _login(worker, "execution") as writer:
                    candidate_target, address = await _native_qualified_candidate(worker, writer)
                    manifest = await prepare_and_publish_network_candidate(
                        writer,
                        candidate_target,
                        address,
                        owner_role=worker.roles_by_kind["physical"],
                        loader_roles=(),
                        reader_roles=(fixture.roles["reader"],),
                        control_schema=fixture.control_schema,
                    )
                case.manifest = manifest
                async with _metadata_roles(case):
                    yield case


async def _create_metadata_roles(case, roles_by_kind, created_roles):
    """Create exact fixture roles and record each one before granting capabilities."""
    connection = case.fixture.connection
    for kind, name in roles_by_kind.items():
        password = case.component.admin_engine.url.password
        credential = (
            ""
            if password is None
            else " PASSWORD " + await connection.fetchval("SELECT quote_literal($1::text)", password)
        )
        await connection.execute(f'CREATE ROLE "{name}" ' + ("NOLOGIN" if kind == "owner" else "LOGIN" + credential))
        created_roles.append(name)
    await connection.execute(
        f'GRANT "{roles_by_kind["owner"]}","{roles_by_kind["loader"]}" TO "{roles_by_kind["sealer"]}"'
    )
    await connection.execute(
        f'GRANT CREATE ON DATABASE "{case.component.admin_engine.url.database}" '
        f'TO "{roles_by_kind["loader"]}","{roles_by_kind["owner"]}","{roles_by_kind["sealer"]}"'
    )


async def _grant_metadata_registry_access(case, roles_by_kind):
    """Grant only the registry preparation columns used by the real fixture pipeline."""
    connection, control = case.fixture.connection, '"' + case.fixture.control_schema + '"'
    for role in (roles_by_kind["loader"], roles_by_kind["sealer"]):
        await connection.execute(f'GRANT USAGE ON SCHEMA {control} TO "{role}"')
        await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA {control} TO "{role}"')
    await connection.execute(
        f'GRANT INSERT(allocation_key) ON {control}.network_registry_identity TO "{roles_by_kind["loader"]}"'
    )
    await connection.execute(
        f'GRANT USAGE ON SEQUENCE {control}.network_registry_identity_network_id_seq TO "{roles_by_kind["loader"]}"'
    )
    await connection.execute(f'GRANT INSERT ON {control}.network_registry_alias TO "{roles_by_kind["loader"]}"')
    await connection.execute(f'GRANT INSERT ON {control}.registry_fhir_catalog_snapshot TO "{roles_by_kind["sealer"]}"')
    await connection.execute(
        f'GRANT INSERT({",".join(sorted(INSERT_COLUMNS))}) ON {control}.registry_publication_request TO "{roles_by_kind["loader"]}"'
    )


async def _grant_metadata_source_reads(case, roles_by_kind):
    """Expose approved registration columns and retain the independent reader role."""
    connection = case.fixture.connection
    await connection.execute(
        f'GRANT SELECT(source_id,endpoint_id) ON mrf.provider_directory_source TO "{case.component.reader}"'
    )
    await connection.execute(
        "GRANT SELECT(endpoint_id,canonical_api_base) ON mrf.provider_directory_api_endpoint "
        f'TO "{case.component.reader}"'
    )
    for role in (
        roles_by_kind["loader"],
        roles_by_kind["sealer"],
        roles_by_kind["execution"],
        case.worker.roles_by_kind["api"],
    ):
        await connection.execute(f'GRANT "{case.fixture.roles["reader"]}" TO "{role}" WITH INHERIT TRUE, SET FALSE')


async def _configure_metadata_runtime(case, roles_by_kind):
    """Install real publication permissions and bind role-specific connection URLs."""
    profile = FHIRCatalogSourceReadProfile(
        "mrf", case.component.owner, tuple(sorted(case.component.policy.runtime_roles))
    )
    grant = FHIRCatalogStandingGrant(
        "catalog-integration",
        1,
        case.fixture.control_schema,
        (profile,),
        (("cms-npd", "insurer-1", COMPANY),),
    )
    readers = tuple(sorted((roles_by_kind["execution"], case.worker.roles_by_kind["api"], roles_by_kind["loader"])))
    consumer = RegistryPublicationConsumerConfig(
        case.fixture.control_schema,
        case.worker.roles_by_kind["lifecycle"],
        roles_by_kind["execution"],
        case.worker.roles_by_kind["api"],
        case.worker.roles_by_kind["queue"],
        roles_by_kind["owner"],
        (roles_by_kind["loader"],),
        readers,
    )
    installed = replace(
        consumer, metadata_runtime=build_fhir_catalog_publication_runtime(grant, None), metadata_only=True
    )
    async with case.fixture.connection.transaction():
        await install_fhir_catalog_publication_permissions(case.fixture.connection, installed)
    base = case.component.admin_engine.url
    urls = tuple(
        base.set(
            username=role,
            password=case.worker.role_password if role == case.worker.roles_by_kind["lifecycle"] else base.password,
        )
        for role in (
            case.component.reader,
            roles_by_kind["loader"],
            roles_by_kind["sealer"],
            case.worker.roles_by_kind["lifecycle"],
            roles_by_kind["execution"],
        )
    )
    case.config = RegistryFHIRCatalogSyncConfig(*urls, consumer, grant, roles_by_kind["loader"], readers)
    case.metadata_roles = roles_by_kind
    case.reader_url = base.set(username=case.worker.roles_by_kind["api"], password=case.worker.role_password)


async def _remove_metadata_roles(case, created_roles):
    """Remove schemas and grants owned by only the exact roles created for this fixture."""
    connection = case.fixture.connection
    for name in reversed(created_roles):
        schemas = await connection.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1)",
            name,
        )
        for entry in schemas:
            await connection.execute('DROP SCHEMA "' + entry["nspname"] + '" CASCADE')
        await connection.execute(f'DROP OWNED BY "{name}"')
        await connection.execute(f'DROP ROLE "{name}"')


@asynccontextmanager
async def _metadata_roles(case):
    """Keep all metadata role setup and exact cleanup under the same fixture lifetime."""
    roles_by_kind = {kind: "catalog_" + kind + "_" + uuid4().hex for kind in ("owner", "loader", "sealer", "execution")}
    created_roles = []
    try:
        await _create_metadata_roles(case, roles_by_kind, created_roles)
        await _grant_metadata_registry_access(case, roles_by_kind)
        await _grant_metadata_source_reads(case, roles_by_kind)
        await _configure_metadata_runtime(case, roles_by_kind)
        yield
    finally:
        await _remove_metadata_roles(case, created_roles)


async def _organization_maps(case):
    records = []
    maps = context()
    for kind, entries in (("company", maps.approved_companies), ("group", maps.approved_groups)):
        for entry in entries:
            records.append(
                (
                    case.seed.revision,
                    kind,
                    entry[kind + "_id"],
                    1,
                    case.seed.revision - 1,
                    json.dumps({**entry, "archived": False}),
                )
            )
    await case.fixture.connection.copy_records_to_table(
        "registry_approved_record",
        schema_name=case.fixture.control_schema,
        columns=("approved_revision", "record_kind", "record_key", "record_revision", "custom_revision", "record_json"),
        records=records,
    )


async def adopt_existing_network(case):
    """Prove legacy-only adoption with a UUID actually allocated by FHIR intake."""
    source = await asyncpg.connect(
        case.config.source_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        legacy = await source.fetchval(
            "SELECT network_id FROM mrf.provider_directory_insurance_network_source_binding "
            "WHERE source_id='cms-npd' AND resource_id='network-old'"
        )
    finally:
        await source.close()
    assert isinstance(legacy, UUID)
    await case.fixture.connection.execute(
        f'INSERT INTO "{case.fixture.control_schema}".network_registry_alias '
        "(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) "
        "VALUES('fhir','cms-npd','legacy_fhir_uuid',$1,'cms-npd',$2,'admitted-source')",
        str(legacy),
        int(case.seed.network["record_id"]),
    )
