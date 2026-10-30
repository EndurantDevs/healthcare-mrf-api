# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real generic Source editions, postcommit capture and native refresh workers."""

import importlib
import os
import re
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from functools import partial
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from process import network_membership_publication as network_publication
from process import provider_directory_generic_capture_continuation as continuation
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_generic_source_policy as source
from process import provider_directory_source_local_publication as publication
from process import registry_membership_refresh_runtime as runtime
from process.network_cms_registry_address_capture import native_driver
from process.network_fhir_retained_capture_contract import RetainedFHIRCaptureError
from process.network_generic_source_published_snapshot import _require_source_actors
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_generic_accepted_capture import generic_fhir_capture_id, read_current_generic_fhir_input
from process.registry_generic_capture_continuation import RegistryGenericCaptureTarget
from process.registry_membership_refresh_generic_fence import _TABLES
from tests import generic_fhir_capture_postgres_support as retained
from tests import provider_directory_source_postgres_support as native_source
from tests import registry_membership_refresh_execution_support as execution
from tests import test_network_custom_address_source_postgres as custom
from tests import test_network_serving_schema_postgres as registry
from tests.cms_registry_complete_publication_postgres_support import (
    _bootstrap_native_source,
    source_component_native_migrations,
)
from tests.generic_refresh_fence_postgres_support import fixed_factory
from tests.nppes_candidate_postgres_support import composed_nppes_component
from tests.provider_directory_source_postgres_support import source_component_database
from tests.provider_profile_detail_load_postgres_support import _install_detail_tables
from tests.test_registry_generic_fhir_reuse_postgres import _native_generic_offices
from tests.test_registry_membership_refresh_generic_postgres import _approved_bindings

directory = importlib.import_module("process.provider_directory_fhir")
SOURCE = "generic-successor-source"
ENDPOINT = "generic-successor-endpoint"
BASE = "https://directory.example.test/fhir"


def _edition_rows(edition, dataset):
    """Move one exact office and replace the provider-only practitioner in edition two."""
    from process.provider_directory_organization_hash import canonical_organization_payload
    from process.provider_directory_resource_hash import canonical_practitioner_payload

    resources = retained._contents(SOURCE)
    role = next(normalized_by_field for kind, _, normalized_by_field in resources if kind == "PractitionerRole")
    resources = [
        (kind, key, normalized_by_field) for kind, key, normalized_by_field in resources if kind != "PractitionerRole"
    ]
    office_by_field = {
        **role,
        "resource_id": "role-office",
        "insurance_plan_refs": [],
        "location_refs": ["Location/office-one" if edition == 1 else "Location/office-two"],
    }
    resources.append(("PractitionerRole", "role-office", office_by_field))
    provider_id = "provider-only-" + str(edition)
    npi = "1000000004" if edition == 1 else "1000000491"
    resources.append(("Practitioner", provider_id, {"source_id": SOURCE, "resource_id": provider_id, "npi": npi}))
    role_by_field = {
        **role,
        "resource_id": "role-" + provider_id,
        "insurance_plan_refs": [],
        "practitioner_ref": "Practitioner/" + provider_id,
        "npi": npi,
        "location_refs": [],
    }
    resources.append(("PractitionerRole", "role-" + provider_id, role_by_field))
    resource_rows = []
    for kind, key, normalized_by_field in resources:
        if kind == "Organization":
            normalized_by_field = canonical_organization_payload(normalized_by_field)
        elif kind == "Practitioner":
            normalized_by_field = canonical_practitioner_payload({**normalized_by_field, "names": []})
        resource_rows.append(
            dict(
                dataset_id=dataset,
                resource_type=kind,
                resource_id=key,
                payload_json=normalized_by_field,
                payload_hash=directory.resource_payload_sha256_for_contract(
                    normalized_by_field,
                    directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
                    resource_type=kind,
                ),
            )
        )
    return resource_rows


async def publish_edition(fixture, edition):
    """Acquire with bounded native COPY, then use genuine admission and Source-local promotion."""
    database = fixture.component.database
    dataset = "generic-successor-edition-" + str(edition)
    resource_rows = _edition_rows(edition, dataset)
    selected_resources = tuple(sorted({resource_row["resource_type"] for resource_row in resource_rows}))
    candidate = directory.EndpointDatasetCandidate(
        endpoint_id=ENDPOINT,
        dataset_id=dataset,
        acquisition_root_run_id="generic-successor-root-" + str(edition),
        source_ids=(SOURCE,),
        selected_resources=selected_resources,
        expected_resources=selected_resources,
        import_run_id="generic-successor-root-" + str(edition),
        previous_dataset_id=None if edition == 1 else "generic-successor-edition-1",
        resource_hash_contract=directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-10-01",
        proof_resource_scope=directory._provider_directory_proof_resource_scope(selected_resources),
    )
    async with source.source_transaction(database), database.acquire() as connection:
        await directory._ensure_endpoint_dataset_candidate(candidate)
        for kind in selected_resources:
            await directory._upsert_dataset_resource_rows_on_connection(
                connection,
                [resource_row for resource_row in resource_rows if resource_row["resource_type"] == kind],
                persist_content_proof=True,
                resource_hash_contract=candidate.resource_hash_contract,
                semantic_projection_as_of=candidate.semantic_projection_as_of,
            )
    validated = await directory._validate_endpoint_dataset_candidate(
        candidate,
        {
            kind: dict(
                rows_fetched=sum(resource_row["resource_type"] == kind for resource_row in resource_rows), complete=True
            )
            for kind in selected_resources
        },
    )
    assert validated["validated"] is True and validated["published"] is False
    with artifacts.source_publication_request(directory, [{"source_id": SOURCE}], True):
        await artifacts.publish_source_candidate(directory, candidate)
    assert database._transaction_binding() is None and source._PUBLISHER.get() is None
    capture = fixture.captures[-1]
    assert capture.origin.dataset_id == candidate.dataset_id
    assert capture.origin.root_run_id == candidate.acquisition_root_run_id
    assert capture.capture_id == str(generic_fhir_capture_id(capture.origin))
    fixture.candidates.append(candidate)
    fixture.origin = fixture.captures[0].origin
    return capture


async def _drop_captures(fixture):
    for schema in reversed(fixture.namespaces):
        await fixture.admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await fixture.admin.fetchval("SELECT to_regnamespace($1)", schema) is None


async def _assert_source_writer_excluded(component, fixture, reader, writer, head):
    """Reject the real Source Publisher as capture Writer without granting or changing roles."""
    origin = await read_current_generic_fhir_input(
        reader,
        schema_name=head.schema_name,
        source_id=head.source_id,
        endpoint_id=head.endpoint_id,
    )
    await _require_source_actors(reader, writer, origin, component.policy)
    async with component.database.engine.connect() as connection:
        await connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        rejected = (await connection.get_raw_connection()).driver_connection
        actor_by_field = await rejected.fetchrow(
            "SELECT session_user::text AS login,current_user::text AS actor,current_setting('role') AS role,"
            "pg_has_role(session_user,$1,'SET') AS source_owner_set",
            component.policy.owner_role,
        )
        assert tuple(actor_by_field) == (component.publisher, component.publisher, "none", True)
        with pytest.raises(RetainedFHIRCaptureError):
            await _require_source_actors(reader, rejected, origin, component.policy)
    fixture.source_writer_rejections.append(head.dataset_id)


async def _continuation(component, fixture, stack, monkeypatch):
    component.factory._require_current_policy()
    assert component.factory._native_input_producer is not None
    reader = await fixed_factory(stack, component.admin_engine.url.set(username=component.reader))
    writer = await fixed_factory(stack, component.admin_engine.url.set(username=component.retained_publisher))
    component.factory.generic_fhir_capture_continuation = continuation.GenericSourceCaptureContinuation(
        reader,
        writer,
        RegistryGenericCaptureTarget(fixture.role_by_kind["owner"], (component.reader,)),
    )
    original_publish = publication.publish_validated_source_local_dataset
    original_capture = continuation.capture_committed_generic_fhir_source

    async def published(*args, **kwargs):
        await original_publish(*args, **kwargs)
        assert component.database._transaction_binding() is None
        assert source._PUBLISHER.get() is None and source._OWNER.get() is None
        fixture.commits.append(args[1].dataset_id)

    async def captured(reader, writer, **kwargs):
        head = kwargs["expected_head"]
        assert fixture.commits[-1] == head.dataset_id
        assert component.database._transaction_binding() is None
        assert source._PUBLISHER.get() is None and source._OWNER.get() is None
        await _assert_source_writer_excluded(component, fixture, reader, writer, head)
        namespace = "registry_fhir_capture_" + generic_fhir_capture_id(head).hex
        if namespace not in fixture.namespaces:
            fixture.namespaces.append(namespace)
        capture = await original_capture(reader, writer, **kwargs)
        fixture.captures.append(capture)
        return capture

    monkeypatch.setattr(publication, "publish_validated_source_local_dataset", published)
    monkeypatch.setattr(continuation, "capture_committed_generic_fhir_source", captured)


@asynccontextmanager
async def _owned_source_database(monkeypatch):
    """Allocate genuine UUID storage before the unchanged CMS admission guard runs."""
    from tests import cms_npd_admission_postgres_support as admission

    configured = os.getenv(admission._DSN_ENV) or os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not configured:
        pytest.skip("requires an explicitly selected disposable PostgreSQL database")
    source_url = make_url(configured)
    allowed_hosts = {"127.0.0.1", "localhost"} | ({"postgres"} if os.getenv("CI") == "true" else set())
    if (
        not source_url.drivername.startswith("postgresql")
        or source_url.query
        or source_url.host not in allowed_hosts
        or source_url.port is None
        or not 1 <= source_url.port <= 65535
        or not re.fullmatch(
            r"(?:hc_cms_admission_test|hc_network_registry)_[0-9a-f]{32}|ptg2_v3_lifecycle_test_[a-z0-9_]{8,}",
            source_url.database or "",
        )
    ):
        pytest.fail("generic Source fixture requires a disposable local PostgreSQL test database")
    async with admission._owned_database(source_url.set(drivername="postgresql+asyncpg")) as (database_url, _admin):
        with monkeypatch.context() as settings:
            settings.setenv(admission._DSN_ENV, database_url.render_as_string(hide_password=False))
            assert admission._database_url() == database_url
            yield settings


@asynccontextmanager
async def native_generic_source(monkeypatch):
    """Own one disposable Source database; no manufactured publication or admission metadata."""
    migrations = source_component_native_migrations()
    monkeypatch.setattr(native_source, "bootstrap_source_component", _bootstrap_native_source)
    async with (
        _owned_source_database(monkeypatch) as settings,
        source_component_database(
            settings,
            publisher_pool_size=1,
            migration_prefixes=migrations,
            before_provision=partial(_install_detail_tables, monkeypatch=monkeypatch),
        ) as component,
        AsyncExitStack() as stack,
    ):
        await stack.enter_async_context(composed_nppes_component(component, migration_prefixes=migrations))
        admin = await asyncpg.connect(
            component.admin_engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        stack.push_async_callback(admin.close)
        fixture = SimpleNamespace(
            component=component,
            admin=admin,
            schema=component.policy.schema,
            role_by_kind={"owner": "generic_capture_owner_" + uuid4().hex, "reader": component.reader},
            captures=[],
            namespaces=[],
            commits=[],
            candidates=[],
            source_writer_rejections=[],
        )
        from tests.generic_refresh_fence_postgres_support import _cleanup_roles

        created_roles = []
        stack.push_async_callback(_cleanup_roles, admin, created_roles)
        stack.push_async_callback(_drop_captures, fixture)
        owner = fixture.role_by_kind["owner"]
        await admin.execute(f'CREATE ROLE "{owner}" NOLOGIN')
        created_roles.append(owner)
        await admin.execute(f'GRANT "{owner}" TO "{component.retained_publisher}" WITH INHERIT FALSE, SET TRUE')
        monkeypatch.setattr(directory, "db", component.database)
        await _continuation(component, fixture, stack, monkeypatch)
        async with source.source_transaction(component.database):
            await component.database.status(
                "INSERT INTO mrf.provider_directory_api_endpoint "
                "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
                "VALUES (:endpoint,:base,repeat('c',64),repeat('d',64))",
                endpoint=ENDPOINT,
                base=BASE,
            )
            await component.database.status(
                "INSERT INTO mrf.provider_directory_source "
                "(source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) "
                "VALUES (:source,'Synthetic generic successor',:endpoint,:base,false,false)",
                source=SOURCE,
                endpoint=ENDPOINT,
                base=BASE,
            )
        yield fixture


class GenericRefreshExecution(execution.RefreshExecution):
    async def _assert_source_fenced(self):
        """A real independent writer cannot acquire the selected Source publication locks."""
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with self.connection.transaction():
                await self.connection.fetchval(
                    "SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset "
                    "WHERE endpoint_id=$1 AND is_current FOR UPDATE NOWAIT",
                    ENDPOINT,
                )
        async with self.connection.transaction():
            assert (
                await self.connection.fetchval(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                    ENDPOINT,
                )
                is False
            )


async def _head_actor(source_fixture, stack):
    from tests.generic_refresh_fence_postgres_support import _actors

    roles, password = await _actors(source_fixture.admin, stack)
    head = roles["head"]
    await source_fixture.admin.execute(f'GRANT USAGE ON SCHEMA mrf TO "{head}"')
    for table, columns in _TABLES.items():
        selected = ",".join('"' + column + '"' for column in columns)
        await source_fixture.admin.execute(
            f'GRANT SELECT({selected}),UPDATE("{columns[0]}") ON mrf."{table}" TO "{head}"'
        )
    url = source_fixture.component.admin_engine.url.set(username=head, password=password)
    return await fixed_factory(stack, url), url


@asynccontextmanager
async def generic_refresh_execution(source_fixture, monkeypatch):
    """Reuse the native EAU and independent queue/worker/custodian fixture contracts."""
    component = source_fixture.component
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", component.admin_engine.url.render_as_string(hide_password=False))
    async with aclosing(registry.serving_schema.__wrapped__()) as serving_generator, AsyncExitStack() as stack:
        serving = await anext(serving_generator)
        connection, schema, engine = serving
        identity_sql = "SELECT current_database(),oid FROM pg_database WHERE datname=current_database()"
        assert tuple(await connection.fetchrow(identity_sql)) == tuple(
            await source_fixture.admin.fetchrow(identity_sql)
        )
        assert await connection.fetchval("SELECT current_database()") == component.admin_engine.url.database
        custom_generator = await stack.enter_async_context(aclosing(custom.custom_db.__wrapped__(serving)))
        custom_fixture = await anext(custom_generator)
        head_sessions, head_url = await _head_actor(source_fixture, stack)
        case = SimpleNamespace(
            admin=SimpleNamespace(kw={"bind": engine}),
            names=SimpleNamespace(reader=component.reader, retention_owner=component.retention_owner),
        )
        fixture = GenericRefreshExecution(
            case, (None, SimpleNamespace(aca_head_fence_sessions=head_sessions), schema, None, [])
        )
        fixture.connection = connection
        fixture.cas_checks = []
        original_cas = network_publication._insert_manifest

        async def fenced_cas(*args):
            await fixture._assert_source_fenced()
            result = await original_cas(*args)
            await fixture._assert_source_fenced()
            fixture.cas_checks.append(result["generation_id"])
            return result

        monkeypatch.setattr(network_publication, "_insert_manifest", fenced_cas)
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", schema)
        await execution._worker_roles(fixture, stack)
        runtime_roles = (component.reader,)
        # The address helper retains its exact seed namespace in this registered cleanup list.
        address_source = SimpleNamespace(captures=source_fixture.namespaces, role_by_kind=source_fixture.role_by_kind)
        fixture.address = await _native_generic_offices(custom_fixture, address_source, serving, runtime_roles, stack)
        fixture.address_schema = fixture.address.schema_name
        for role in (fixture.roles_by_kind["execution"], fixture.roles_by_kind["custodian"]):
            await connection.execute(f'GRANT "{component.reader}" TO "{role}" WITH INHERIT TRUE, SET FALSE')
        # Source configuration is scoped independently from the Registry migration schema.
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        yield fixture, serving, stack, head_url


async def start_refresh_runtime(fixture, source_fixture, serving, stack, head_url):
    capture = source_fixture.captures[0]
    _, fixture.approved, fixture.network_id = await retained._approved_capture_recipe(source_fixture, serving, capture)
    reader_url = source_fixture.component.admin_engine.url.set(username=source_fixture.component.reader)
    fixture.outer = await fixed_factory(stack, reader_url)
    async with registry_company_approval_transaction(fixture.outer, control_schema=fixture.control_schema) as session:
        _, enrollment = await _approved_bindings(
            await native_driver(session), fixture.control_schema, fixture.approved, capture
        )
    fixture.enrollment = [enrollment]
    await execution._connect_workers(fixture, stack)
    settings = runtime._settings(
        dict(
            enabled=True,
            app_origin="https://authority.example.invalid",
            service_token="synthetic-refresh-only",
            control_schema=fixture.control_schema,
            fence_dsn=reader_url.render_as_string(hide_password=False),
            source_reader_dsn=reader_url.render_as_string(hide_password=False),
            address_reader_dsn=reader_url.render_as_string(hide_password=False),
            address_schema=fixture.address_schema,
            runtime_roles=[source_fixture.component.reader],
            source_profiles={"generic": {"head_fence_dsn": head_url.render_as_string(hide_password=False)}},
        )
    )
    fixture.bundle = await runtime._build_native_bundle(settings, stack, fixture.roles_by_kind["execution"])
    await fixture.bundle.runtime.authorize._client.aclose()
    fixture.bundle.runtime.authorize._client = httpx.AsyncClient(transport=httpx.MockTransport(fixture.http))
