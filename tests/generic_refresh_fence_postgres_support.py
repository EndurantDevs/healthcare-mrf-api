# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Task-disposable native metadata lock actors around genuinely retained source facts."""

from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_cms_registry_address_capture import native_driver
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_membership_refresh_generic_fence import _TABLES
from process.registry_membership_refresh_source_fence import RegistryMembershipRefreshHeadFences
from process.registry_membership_refresh_sources import RegistryMembershipRefreshSourceReaders
from tests import generic_fhir_capture_postgres_support as source_support
from tests import test_network_serving_schema_postgres as registry_support
from tests.test_registry_membership_refresh_generic_postgres import _approved_bindings


async def _cleanup_roles(admin, roles):
    for role in reversed(roles):
        await admin.execute(f'DROP OWNED BY "{role}"')
        await admin.execute(f'DROP ROLE "{role}"')
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)


async def _actors(admin, stack):
    roles_by_kind = {kind: "generic_fence_" + kind + "_" + uuid4().hex for kind in ("reader", "head", "writer")}
    password = uuid4().hex + uuid4().hex
    created_roles = []
    stack.push_async_callback(_cleanup_roles, admin, created_roles)
    for role in roles_by_kind.values():
        await admin.execute(
            f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        created_roles.append(role)
    return roles_by_kind, password


async def _control_shapes(source_fixture, roles_by_kind):
    admin, schema, owner = source_fixture.admin, source_fixture.schema, source_fixture.role_by_kind["source"]
    await admin.execute(f'SET ROLE "{owner}"')
    try:
        await admin.execute(
            f'CREATE TABLE "{schema}".provider_directory_api_endpoint(endpoint_id varchar(64) PRIMARY KEY)'
        )
        await admin.execute(
            f"CREATE UNIQUE INDEX provider_directory_endpoint_dataset_current_idx "
            f'ON "{schema}".provider_directory_endpoint_dataset(endpoint_id) WHERE is_current = true'
        )
        await admin.execute(
            f'INSERT INTO "{schema}".provider_directory_api_endpoint VALUES($1),($2)',
            source_fixture.origin.endpoint_id,
            "endpoint-foreign",
        )
        await admin.execute(
            f'INSERT INTO "{schema}".provider_directory_source VALUES($1,$2)',
            "source-foreign",
            "endpoint-foreign",
        )
        await admin.execute(
            f'INSERT INTO "{schema}".provider_directory_endpoint_dataset(dataset_id,endpoint_id,acquisition_root_run_id,'
            "dataset_hash,status,is_current,resource_count,published_at) VALUES('dataset-foreign','endpoint-foreign',"
            "'run-foreign',repeat('a',64),'published',true,0,now())"
        )
    finally:
        await admin.execute("RESET ROLE")
    for role in roles_by_kind.values():
        await admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
    await admin.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{roles_by_kind["reader"]}"')
    for table, columns in _TABLES.items():
        selected = ",".join('"' + column + '"' for column in columns)
        await admin.execute(
            f'GRANT SELECT({selected}),UPDATE("{columns[0]}") ON "{schema}"."{table}" TO "{roles_by_kind["head"]}"'
        )
        await admin.execute(f'GRANT SELECT,UPDATE ON "{schema}"."{table}" TO "{roles_by_kind["writer"]}"')


async def fixed_factory(stack, url):
    engine = create_async_engine(
        url,
        pool_size=1,
        max_overflow=0,
        hide_parameters=True,
    )
    stack.push_async_callback(engine.dispose)
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def generic_fence_context(request):
    """Keep reader, lock-only actor and writer credentials/authority genuinely distinct."""
    async with (
        aclosing(source_support.legacy_capture_db.__wrapped__(request)) as source_generator,
        AsyncExitStack() as stack,
    ):
        source_fixture = await anext(source_generator)
        roles_by_kind, password = await _actors(source_fixture.admin, stack)
        await _control_shapes(source_fixture, roles_by_kind)
        capture = await source_support._capture(
            source_fixture,
            runtime_roles=tuple(sorted((source_fixture.role_by_kind["reader"], roles_by_kind["reader"]))),
        )
        async with aclosing(registry_support.serving_schema.__wrapped__()) as registry_generator:
            serving = await anext(registry_generator)
            _, approved, _ = await source_support._approved_capture_recipe(source_fixture, serving, capture)
            _, schema, registry_engine = serving
            await source_fixture.admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{roles_by_kind["reader"]}"')
            await source_fixture.admin.execute(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{roles_by_kind["reader"]}"'
            )
            base = registry_engine.url
            urls_by_kind = {kind: base.set(username=role, password=password) for kind, role in roles_by_kind.items()}
            outer = await fixed_factory(stack, urls_by_kind["reader"])
            child = await fixed_factory(stack, urls_by_kind["reader"])
            head = await fixed_factory(stack, urls_by_kind["head"])
            readers = RegistryMembershipRefreshSourceReaders(
                schema,
                child,
                head_fences=RegistryMembershipRefreshHeadFences(generic_sessions=head),
            )
            async with registry_company_approval_transaction(outer, control_schema=schema) as session:
                bindings, enrollment = await _approved_bindings(await native_driver(session), schema, approved, capture)
            yield SimpleNamespace(
                source=source_fixture,
                capture=capture,
                approved=approved,
                bindings=bindings,
                enrollment=[enrollment],
                outer=outer,
                readers=readers,
                stack=stack,
                schema=schema,
                roles_by_kind=roles_by_kind,
                password=password,
                urls_by_kind=urls_by_kind,
            )


async def actor_connection(context, kind):
    url = context.urls_by_kind[kind].set(drivername="postgresql").render_as_string(hide_password=False)
    connection = await asyncpg.connect(url)
    context.stack.push_async_callback(connection.close)
    return connection
