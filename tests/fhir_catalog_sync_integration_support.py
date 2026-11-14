# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owned native import and registry fixtures for the connected catalog test."""

import asyncio
import copy
import os
import subprocess
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.plan_release_storage import storage_identifier
from process import provider_directory_cms_serving as source_serving
from process import provider_directory_generic_source_policy as source_policy
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_custom_address_source import prepare_custom_address_source
from process.network_membership_pipeline import prepare_network_candidate
from process.network_membership_publication import publish_network_candidate
from process.registry_candidate_composition import (
    RegistryAddressArtifactCustody,
    RegistryCompositionAddressSources,
    prepare_registry_composition_address_artifact,
)
from process.registry_fhir_catalog_grant import FHIRCatalogStandingGrant, build_fhir_catalog_publication_runtime
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_permissions import (
    install_fhir_catalog_preparation_permissions,
    install_fhir_catalog_publication_permissions,
)
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile
from process.registry_fhir_catalog_worker import RegistryFHIRCatalogSyncConfig, run_fhir_catalog_sync
from process.registry_publication_consumer import RegistryPublicationConsumerConfig
from process.registry_publication_worker import RegistryPublicationWorkerConfig, _configured_worker_runtime
from tests import cms_npd_admission_postgres_support as imported
from tests import cms_registry_complete_publication_postgres_support as native
from tests.registry_candidate_custody_postgres_support import administrative_freeze_callback
from tests.test_network_cms_registry_complete_publication_postgres import (
    SUCCESSOR_LEASE_SECONDS,
    _paired_native_execution,
)
from tests.test_network_custom_address_source_postgres import (
    _restricted_artifact_context,
    _restricted_artifact_roles,
    _seed,
    custom_db,
)
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import (
    _approve,
    _command,
    _company_groups,
    _create,
    _draft,
    _group_row,
)
from tests.test_registry_fhir_catalog_custody_postgres import cleanup_catalog_preparations
from tests.test_registry_fhir_catalog_enrichment import COMPANY, GROUP, context
from tests.test_registry_publication_worker_permissions_postgres import (
    _install,
    _login,
    _native_qualified_candidate,
    _worker_execution_identity,
    _worker_roles,
)


def source_resources(original, revision, *, retain_old):
    resources = copy.deepcopy(native.office_resources(original, revision))
    networks = ["network-new", "network-old"] if retain_old else ["network-auto", "network-new"]
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

    resources = references(resources)
    if retain_old:
        resources["PractitionerRole"][0]["organization"] = {"reference": "Organization/network-old"}
    for role in resources["PractitionerRole"]:
        role["network"] = [dict(role["organization"])]
    for affiliation in resources["OrganizationAffiliation"]:
        affiliation["active"] = False
    return resources


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
            assert remaining <= SUCCESSOR_LEASE_SECONDS, "unexpected capacity reservation in the owned fixture"
            # Keep the real quiescence guard and immutable lease ledger intact.
            await asyncio.sleep(min(float(remaining) + 0.1, 30))
        execution = await _paired_native_execution(case.settings, execution)
        await source_serving.publish_current_attested_profile(
            fhir, execution=execution, run_id=run_id, control_run_id=run_id, metrics={}
        )
    case.source_generation += 1
    await case.fixture.connection.execute(f'REVOKE CREATE ON SCHEMA mrf FROM "{case.component.builder}"')
    return admission_result


async def _prepare_serving_custody(case, stack):
    """Retain the independent artifact actor and its native read grants."""
    settings, component, fixture = case.settings, case.component, case.fixture
    with settings.context() as custody_settings:
        custody_settings.setenv(
            "NETWORK_REGISTRY_TEST_DSN",
            component.admin_engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
        )
        assert case.artifact_actors is not None
        case.address_custody = await stack.enter_async_context(
            _restricted_artifact_context(fixture, actors=case.artifact_actors)
        )
    # The worker already owns loading; the unused artifact loader needs no base access.
    unused_loading = case.address_custody.roles["loading"]
    await fixture.connection.execute(f'REVOKE USAGE ON SCHEMA "{fixture.source_schema}" FROM "{unused_loading}"')
    await fixture.connection.execute(
        f'REVOKE SELECT ON ALL TABLES IN SCHEMA "{fixture.source_schema}" FROM "{unused_loading}"'
    )


@asynccontextmanager
async def _catalog_metadata_actors(connection, enabled):
    """Own all independent origin readers before the native Source component exists."""
    async with AsyncExitStack() as stack:
        artifact = await stack.enter_async_context(_restricted_artifact_roles(connection)) if enabled else None
        execution = await stack.enter_async_context(_worker_execution_identity(connection)) if enabled else None
        yield SimpleNamespace(artifact=artifact, execution=execution)


@asynccontextmanager
async def _catalog_source_database(settings, tmp_path, owned, actors):
    """Admit only the independent identities created before the Source component."""
    metadata_read_roles = (
        tuple(sorted((actors.artifact.roles["execution"], actors.artifact.roles["custodian"], actors.execution.role)))
        if actors.artifact is not None
        else ()
    )
    settings.setenv(imported._DSN_ENV, owned.render_as_string(hide_password=False))
    settings.setenv("HLTHPRT_CMS_NPD_STAGE_WORKERS", "0")
    settings.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", "true")
    settings.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "true")
    settings.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", "2")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    async with native.source_native_publication_database(
        settings, tmp_path, commit, metadata_read_roles=metadata_read_roles
    ) as administrator:
        yield administrator


async def _initial_address_artifact(case, request_id, runtime_roles):
    """Reuse split custody: retain the base Reader contract, then bind all output actors."""
    fixture, custody = case.fixture, case.address_custody
    dependency_id = uuid4()
    fixture.composition_ids.append(dependency_id)
    async with custody.custodian.transaction(isolation="repeatable_read"):
        dependency = await prepare_custom_address_source(
            custody.custodian,
            fixture.base,
            composition_id=str(dependency_id),
            approved_revision=case.seed.revision,
            owner_role=custody.roles["custody"],
            runtime_roles=(fixture.roles["reader"],),
            control_schema=fixture.control_schema,
        )
        approved = await pin_approved_membership_source(
            custody.custodian, approved_revision=case.seed.revision, control_schema=fixture.control_schema
        )
        fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
        return await prepare_registry_composition_address_artifact(
            custody.custodian,
            request_id=request_id,
            approved_revision=case.seed.revision,
            address_sources=RegistryCompositionAddressSources(dependency.address_source),
            source_manifest=None,
            expected_head=0,
            custody=RegistryAddressArtifactCustody(custody.roles["custody"], runtime_roles, fixture.control_schema),
        )


def _directory_preparation_roles(case, readers, frozen_owner):
    """Collect the same application roles before configuring native publication."""
    fixture, worker = case.fixture, case.worker
    applications = (
        set(worker.roles_by_kind.values())
        | set(fixture.roles.values())
        | set(readers)
        | set(case.metadata_roles.values())
    )
    applications.update(case.component.policy.runtime_roles)
    if hasattr(case, "address_custody"):
        applications.update(case.address_custody.roles.values())
    preparation_roles_by_kind = {
        "namespace_owner_role": frozen_owner,
        "publisher_role": worker.roles_by_kind["execution"],
        "application_roles": sorted(applications - {frozen_owner}),
    }
    return preparation_roles_by_kind


async def publish_directory_candidate(case, execution, copy_target, address, readers):
    """Publish only after genuine named handoff, administrator freeze and data validation."""
    fixture, worker = case.fixture, case.worker
    if copy_target.candidate_id not in case.network_preparation_ids:
        case.network_preparation_ids.append(copy_target.candidate_id)
    frozen_owner = (
        case.address_custody.roles["custody"] if hasattr(case, "address_custody") else case.metadata_roles["owner"]
    )
    preparation_roles_by_kind = _directory_preparation_roles(case, readers, frozen_owner)
    consumer = RegistryPublicationConsumerConfig(
        fixture.control_schema,
        worker.roles_by_kind["lifecycle"],
        worker.roles_by_kind["execution"],
        worker.roles_by_kind["api"],
        worker.roles_by_kind["queue"],
        worker.roles_by_kind["physical"],
        (),
        tuple(readers),
    )
    base = case.component.admin_engine.url
    config = RegistryPublicationWorkerConfig(
        base.set(username=worker.roles_by_kind["lifecycle"], password=worker.role_password),
        base.set(username=worker.roles_by_kind["execution"], password=worker.role_password),
        "https://authority.example.test",
        "example-service-token",
        consumer,
        preparation_roles=preparation_roles_by_kind,
    )
    await fixture.connection.execute(
        f'ALTER ROLE "{worker.roles_by_kind["execution"]}" SET temp_file_limit TO '
        f"'{config.sql_limits.temp_file_limit_bytes // 1024}kB'"
    )
    await prepare_network_candidate(
        execution,
        copy_target,
        address,
        owner_role=worker.roles_by_kind["physical"],
        loader_roles=(),
        reader_roles=readers,
        control_schema=fixture.control_schema,
    )
    async with AsyncExitStack() as resources:
        runtime = await _configured_worker_runtime(config, resources)
        await administrative_freeze_callback(fixture, runtime)(copy_target)
    async with execution.transaction(isolation="repeatable_read"):
        return await publish_network_candidate(
            execution, copy_target, control_schema=fixture.control_schema, preparation_roles=preparation_roles_by_kind
        )


async def _publish_initial_manifest(case, *, serving_custody):
    """Build the incumbent data before the real named preparation lifecycle."""
    fixture, worker = case.fixture, case.worker
    readers = (fixture.roles["reader"],)
    options_by_name = {}
    if serving_custody:
        runtime_roles = tuple(
            sorted(
                {
                    *readers,
                    case.address_custody.roles["execution"],
                    *(role for kind, role in worker.roles_by_kind.items() if kind != "queue"),
                    *case.metadata_roles.values(),
                }
            )
        )
        request_id = uuid4()
        artifact = await _initial_address_artifact(case, request_id, runtime_roles)
        options_by_name = dict(request_id=request_id, runtime_roles=readers, prepared_address_artifact=artifact)
    async with _login(worker, "execution") as writer:
        candidate_target, address = await _native_qualified_candidate(worker, writer, **options_by_name)
        return await publish_directory_candidate(case, writer, candidate_target, address, readers)


async def _catalog_control_fixture(stack, settings):
    """Register schema/control cleanup before preparing the native registry fixture."""
    schema_generator = await stack.enter_async_context(aclosing(serving_schema.__wrapped__()))
    schema = await anext(schema_generator)
    for extension in ("intarray", "pg_trgm", "btree_gin", "postgis"):
        await schema[0].execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
    custom_generator = await stack.enter_async_context(aclosing(custom_db.__wrapped__(schema)))
    fixture = await anext(custom_generator)
    from directory_membership_test_support import prepare_registry_catalog_foundation

    common = SimpleNamespace(connection=fixture.connection, engine=fixture.engine, created_control_namespaces=[])

    async def cleanup_controls():
        for namespace in reversed(common.created_control_namespaces):
            await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')

    stack.push_async_callback(cleanup_controls)
    await prepare_registry_catalog_foundation(common, settings)
    return fixture


@asynccontextmanager
async def native_catalog_case(settings, tmp_path, *, organization_maps=True, serving_custody=False):
    """Create one exact owned native Source/registry fixture with registered cleanup."""
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    base = make_url(dsn).set(drivername="postgresql+asyncpg")
    async with (
        imported._owned_database(base) as (owned, actor_admin),
        _catalog_metadata_actors(actor_admin, serving_custody) as actors,
        _catalog_source_database(settings, tmp_path, owned, actors) as administrator,
    ):
        component = administrator._source_component
        settings.setenv("NETWORK_REGISTRY_TEST_DSN", component.admin_engine.url.render_as_string(hide_password=False))
        async with AsyncExitStack() as stack:
            fixture = await _catalog_control_fixture(stack, settings)
            seed = await _seed(fixture)
            worker = await stack.enter_async_context(_worker_roles(fixture, seed, execution_actor=actors.execution))
            await _install(worker)
            case = SimpleNamespace(
                settings=settings,
                component=component,
                administrator=administrator,
                fixture=fixture,
                worker=worker,
                artifact_actors=actors.artifact,
                execution_actor=actors.execution,
                seed=seed,
                manifest=None,
                source_generation=1,
                network_preparation_ids=[],
                company_crosswalks=(("cms-npd", "insurer-1", COMPANY),) if organization_maps else (),
            )
            if organization_maps:
                await _organization_maps(case)
            if serving_custody:
                await _prepare_serving_custody(case, stack)
            async with _metadata_roles(case):
                try:
                    case.manifest = await _publish_initial_manifest(case, serving_custody=serving_custody)
                    yield case
                finally:
                    from directory_membership_test_support import cleanup_registry_network_preparations

                    await cleanup_registry_network_preparations(
                        fixture.connection, case.network_preparation_ids, fixture.control_schema
                    )


async def _create_metadata_roles(case, roles_by_kind, created_roles):
    """Create exact fixture roles and record each one before granting capabilities."""
    connection = case.fixture.connection
    for kind, name in roles_by_kind.items():
        assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)
        created_roles.append(name)
        password = case.component.admin_engine.url.password
        credential = (
            ""
            if password is None
            else " PASSWORD " + await connection.fetchval("SELECT quote_literal($1::text)", password)
        )
        await connection.execute(f'CREATE ROLE "{name}" ' + ("NOLOGIN" if kind == "owner" else "LOGIN" + credential))


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


def _metadata_role_urls(case, roles_by_kind):
    """Keep each metadata role bound to the existing fixture connection URL."""
    base = case.component.admin_engine.url
    urls = tuple(
        base.set(
            username=role,
            password=base.password,
        )
        for role in (
            case.component.reader,
            roles_by_kind["loader"],
            roles_by_kind["sealer"],
            roles_by_kind["lifecycle"],
            roles_by_kind["execution"],
        )
    )
    return base, urls


async def _configure_metadata_runtime(case, roles_by_kind):
    """Install real publication permissions and bind role-specific connection URLs."""
    profile = FHIRCatalogSourceReadProfile("mrf")
    grant = FHIRCatalogStandingGrant(
        "catalog-integration",
        1,
        case.fixture.control_schema,
        (profile,),
        case.company_crosswalks,
        observation_contract=CATALOG_OBSERVATION_CONTRACT,
    )
    readers = tuple(sorted((roles_by_kind["execution"], case.worker.roles_by_kind["api"], roles_by_kind["loader"])))
    consumer = RegistryPublicationConsumerConfig(
        case.fixture.control_schema,
        roles_by_kind["lifecycle"],
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
        await install_fhir_catalog_preparation_permissions(
            case.fixture.connection,
            installed,
            loader_role=roles_by_kind["loader"],
            sealer_role=roles_by_kind["sealer"],
        )
        await install_fhir_catalog_publication_permissions(case.fixture.connection, installed)
    base, urls = _metadata_role_urls(case, roles_by_kind)
    preparation_roles_by_kind = {
        "namespace_owner_role": roles_by_kind["owner"],
        "publisher_role": roles_by_kind["sealer"],
        "application_roles": sorted(
            {role for kind, role in roles_by_kind.items() if kind != "owner"}
            | set(readers)
            | {consumer.api_role, *consumer.loader_roles, *consumer.reader_roles}
        ),
    }
    case.config = RegistryFHIRCatalogSyncConfig(
        *urls, consumer, grant, roles_by_kind["loader"], readers, preparation_roles=preparation_roles_by_kind
    )
    case.metadata_roles = roles_by_kind
    case.reader_url = base.set(username=case.worker.roles_by_kind["api"], password=case.worker.role_password)


async def run_catalog_sync(case, config=None, *, withhold_reader=None):
    """Resume a genuine catalog handoff after the separately owned administrator step."""
    from directory_catalog_runtime import freeze_registry_generation

    from process.registry_fhir_catalog_store import load_named_catalog_preparation

    config = case.config if config is None else config
    result = await run_fhir_catalog_sync(config)
    if result["state"] != "waiting_admin_freeze":
        return result
    handoff = await load_named_catalog_preparation(
        case.fixture.connection, result["candidate_id"], control_schema=config.grant.control_schema
    )
    assert handoff is not None and handoff["candidate_id"] == result["candidate_id"]
    admin_config_by_field = {
        **{field: handoff[field] for field in ("kind", "candidate_id", "catalog_schema", "storage_schema")},
        "preparation_roles": config.preparation_roles,
    }
    async with case.fixture.engine.connect() as administrator:
        frozen = await freeze_registry_generation(administrator, admin_config_by_field, result["candidate_id"])
    assert frozen["state"] in {"frozen", "validated"}
    if withhold_reader is not None:
        schema, role = storage_identifier(handoff["storage_schema"]), storage_identifier(withhold_reader)
        async with case.fixture.engine.begin() as administrator:
            await administrator.exec_driver_sql(f"REVOKE USAGE ON SCHEMA {schema} FROM {role}")
            await administrator.exec_driver_sql(f"REVOKE SELECT ON ALL TABLES IN SCHEMA {schema} FROM {role}")
    resumed = await run_fhir_catalog_sync(config)
    assert resumed["state"] in {"completed", "unchanged"}
    return resumed


async def _remove_metadata_roles(case, created_roles):
    """Remove schemas and grants owned by only the exact roles created for this fixture."""
    connection = case.fixture.connection
    snapshots = await connection.fetch(
        f'SELECT schema_name FROM "{case.fixture.control_schema}".registry_fhir_catalog_snapshot'
    )
    await cleanup_catalog_preparations(connection, [row["schema_name"] for row in snapshots])
    for name in reversed(created_roles):
        if not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name):
            continue
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
    roles_by_kind = {
        kind: "catalog_" + kind + "_" + uuid4().hex for kind in ("owner", "loader", "sealer", "lifecycle", "execution")
    }
    created_roles = []
    try:
        await _create_metadata_roles(case, roles_by_kind, created_roles)
        await _grant_metadata_source_reads(case, roles_by_kind)
        await _configure_metadata_runtime(case, roles_by_kind)
        yield
    finally:
        await _remove_metadata_roles(case, created_roles)


async def _organization_maps(case):
    """Use genuine canonical history so subsequent source-binding approvals remain valid."""
    maps, fixture, actor = context(), case.fixture, case.seed.actor
    group_fields_by_name = {
        key: field_value for key, field_value in maps.approved_groups[0].items() if key != "group_id"
    }
    group = await _draft(
        fixture.engine,
        fixture.control_schema,
        replace(_create("group"), record_id=UUID(GROUP), fields={**group_fields_by_name, "aliases": []}),
        actor,
    )
    company_fields = maps.approved_companies[0]
    company_command = replace(
        _create("company"),
        record_id=UUID(COMPANY),
        fields={
            "display_name": company_fields["display_name"],
            "aliases": company_fields["aliases"],
            "roles": ["employer"],
        },
    )
    company = await _draft(
        fixture.engine,
        fixture.control_schema,
        _company_groups(
            company_command,
            [_group_row(UUID(COMPANY), UUID(GROUP), relationship_kind="verified_ownership", valid_from="2025-01-01")],
        ),
        actor,
    )
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, group, company),
        actor,
    )
    case.seed.revision = approval["approved_revision"]


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
