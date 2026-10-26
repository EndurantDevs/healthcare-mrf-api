# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real queue approval, independent address preparation, sealing and atomic publication."""

import hashlib
import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import make_url

from db.models import NPIData
from db.models.entity_address_unified import EntityAddressUnified
from process import registry_publication_consumer as consumer
from process import registry_publication_worker as worker
from process.network_address_projection import PinnedAddressSource
from process.network_candidate_custody import verify_network_candidate_custody
from process.network_custom_address_source import _from_document, verify_retained_custom_address_source
from process.network_membership_copy import MembershipCopyTarget
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_candidate_custody_preparation import split_registry_candidate_custody
from process.registry_publication_queue import (
    enqueue_registry_publication,
    enqueue_registry_source_rollback_preview,
    read_registry_publication_request,
)
from process.registry_record_store import RegistryActor
from tests.test_network_custom_address_source_postgres import _seed
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _command, _create, _draft
from tests.test_registry_publication_heartbeat_postgres import heartbeat_worker as heartbeat_worker

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def address_worker(heartbeat_worker, serving_schema):
    fixture = heartbeat_worker
    connection, schema, engine = serving_schema
    roles_by_kind = {kind: "prepared_" + kind + "_" + uuid4().hex for kind in ("custodian", "custody", "retained")}
    source_schema = "prepared_source_" + uuid4().hex
    try:
        for extension in ("intarray", "btree_gin", "postgis"):
            await connection.execute(f"CREATE EXTENSION IF NOT EXISTS {extension}")
        for kind, role in roles_by_kind.items():
            await connection.execute(f'CREATE ROLE "{role}" ' + ("LOGIN" if kind == "custodian" else "NOLOGIN"))
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{roles_by_kind["custody"]}"')
        async with engine.begin() as original_admin:
            await split_registry_candidate_custody(
                original_admin,
                owner_role=fixture.config.queue_owner_role,
                custody_owner_role=roles_by_kind["custody"],
                control_schema=schema,
            )
        await _retained_address_fixture(connection, engine, source_schema, roles_by_kind["retained"])
        for role in fixture.config.custody_roles:
            await connection.execute(f'GRANT USAGE ON SCHEMA "{source_schema}" TO "{role}"')
            await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{source_schema}" TO "{role}"')
        database = await connection.fetchval("SELECT quote_ident(current_database())")
        for kind in ("custodian", "custody"):
            await connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{roles_by_kind[kind]}"')
        for role, inherit in ((fixture.config.execution_role, "TRUE"), (roles_by_kind["custody"], "FALSE")):
            await connection.execute(
                f'GRANT "{role}" TO "{roles_by_kind["custodian"]}" WITH ADMIN FALSE, INHERIT {inherit}, SET TRUE'
            )
        yield SimpleNamespace(
            worker=fixture,
            roles_by_kind=roles_by_kind,
            address_sources=RegistryCompositionAddressSources(
                PinnedAddressSource(source_schema, "entity_address_unified", "a" * 64)
            ),
        )
    finally:
        artifacts = await connection.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1)",
            roles_by_kind["custody"],
        )
        for artifact in artifacts:
            await connection.execute('DROP SCHEMA "' + artifact["nspname"] + '" CASCADE')
        await connection.execute(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE')
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for role in reversed(tuple(roles_by_kind.values())):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')


async def _retained_address_fixture(connection, engine, source_schema, owner_role):
    await connection.execute(f'CREATE SCHEMA "{source_schema}"')
    async with engine.begin() as setup:
        for model in (EntityAddressUnified, NPIData):
            table = model.__table__.to_metadata(MetaData(), schema=source_schema)
            await setup.run_sync(lambda sync, table=table: table.create(sync))
    await connection.execute(
        f'INSERT INTO "{source_schema}".entity_address_unified '
        "(location_key,entity_type,entity_id,npi,checksum,type,first_line,city_name,state_name,postal_code,country_code,"
        "plans_network_array,procedures_array,medications_array,canonical_network_ids) VALUES"
        "($1,'npi','1000000004',1000000004,42,'practice','123 Example Street','Sample City','CA','90210','US',"
        "'{42}','{123}','{456}','{}')",
        "a" * 64,
    )
    for table_name in ("entity_address_unified", "npi"):
        await connection.execute(f'ALTER TABLE "{source_schema}".{table_name} OWNER TO "{owner_role}"')
    await connection.execute(f'ALTER SCHEMA "{source_schema}" OWNER TO "{owner_role}"')


@asynccontextmanager
async def _publication_runtime(prepared):
    fixture = prepared.worker
    dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    execution_url = make_url(dsn).set(username=fixture.config.execution_role, password=fixture.password)
    settings = SimpleNamespace(
        consumer=fixture.config,
        execution_url=execution_url,
        custody_url=execution_url.set(username=prepared.roles_by_kind["custodian"], password=""),
        ptg=None,
        sql_limits=worker.RegistryPublicationSQLLimits(),
    )
    async with AsyncExitStack() as stack:
        pools = []
        for role in (fixture.config.lifecycle_role, fixture.config.execution_role):
            pool = await asyncpg.create_pool(
                **worker._pool_options(execution_url.set(username=role), 2, settings.sql_limits)
            )
            stack.push_async_callback(pool.close)
            pools.append(pool)
        async with pools[1].acquire() as executing:
            config = await worker._custody_runtime(settings, stack, fixture.config, executing)
        api = await asyncpg.connect(dsn, user=fixture.config.api_role, password=fixture.password)
        stack.push_async_callback(api.close)
        yield SimpleNamespace(
            api=api,
            execution_pool=pools[1],
            config=config,
            arguments=dict(
                lifecycle_pool=pools[0],
                execution_pool=pools[1],
                authorize=_authorize,
                config=config,
                initial_address_sources=prepared.address_sources,
            ),
        )


async def _authorize(*, actor, selection=None, command=None, **_parameters):
    name, document = ("selection", selection) if selection is not None else ("command", command)
    digest = hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"authorized": True, "actor": actor, name + "_sha256": digest, "policy_revision": 1}


async def _enqueue(runtime, fixture, command, actor, head):
    async with runtime.api.transaction():
        return await enqueue_registry_publication(
            runtime.api,
            command,
            actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=head,
            control_schema=fixture.schema,
        )


async def test_queue_independent_artifact_precedes_real_composition_and_atomic_publication(address_worker):
    prepared = address_worker
    fixture = prepared.worker
    async with _publication_runtime(prepared) as runtime:
        queued = await _enqueue(runtime, fixture, fixture.command, fixture.actor, 0)
        completed = await consumer.consume_registry_publication_once(**runtime.arguments)
        assert completed["state"] == "completed" and completed["request_id"] == queued["request_id"]
        async with (
            runtime.execution_pool.acquire() as executing,
            executing.transaction(isolation="repeatable_read", readonly=True),
        ):
            await _assert_published_address(executing, fixture.schema, queued, prepared, runtime.config)


async def _assert_published_address(executing, schema, queued, prepared, config):
    serving = await resolve_network_serving_manifest(executing, control_schema=schema)
    status = await read_registry_publication_request(executing, UUID(queued["request_id"]), control_schema=schema)
    assert status["state"] == "completed" and status["result"]["serving_generation"] == serving.generation_id
    report = json.loads(
        await executing.fetchval(
            f'SELECT validation_json::text FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(serving.candidate_id),
        )
    )
    document = await executing.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
        report["address_source"]["schema_name"],
    )
    receipt = _from_document(document)
    assert receipt.owner_role == prepared.roles_by_kind["custody"]
    assert await verify_retained_custom_address_source(executing, receipt) == receipt
    assert await executing.fetchval(f'SELECT count(*) FROM "{serving.schema_name}".entity_address_unified') == 1
    assert await executing.fetchval(
        f'SELECT plans_network_array FROM "{serving.schema_name}".entity_address_unified'
    ) == [42]
    assert await executing.fetchval(
        "SELECT NOT pg_has_role($1::name,$2::name,'MEMBER') AND has_database_privilege($2::name,current_database(),'CREATE')",
        config.execution_role,
        prepared.roles_by_kind["custody"],
    )


async def _publish_manual_history(prepared, engine, runtime):
    fixture, schema = prepared.worker, prepared.worker.schema
    source = SimpleNamespace(
        connection=fixture.connection,
        control_schema=schema,
        engine=engine,
        source_schema=prepared.address_sources.base_source.schema_name,
    )
    seed = await _seed(source)
    assert seed.first["record"]["canonical_address_json"]["identity_key"].startswith("v2|")
    command = await _command(fixture.connection, schema, *fixture.command.selection)
    await _enqueue(runtime, fixture, command, seed.actor, 0)
    await consumer.consume_registry_publication_once(**runtime.arguments)
    async with (
        runtime.execution_pool.acquire() as executing,
        executing.transaction(isolation="repeatable_read", readonly=True),
    ):
        first = await resolve_network_serving_manifest(executing, control_schema=schema)
    changed = await _draft(engine, schema, _create(), seed.actor)
    command = await _command(fixture.connection, schema, changed)
    await _enqueue(runtime, fixture, command, seed.actor, first.generation_id)
    await consumer.consume_registry_publication_once(**runtime.arguments)
    async with (
        runtime.execution_pool.acquire() as executing,
        executing.transaction(isolation="repeatable_read", readonly=True),
    ):
        second = await resolve_network_serving_manifest(executing, control_schema=schema)
    assert first.approved_custom_revision < second.approved_custom_revision
    return seed, first, second


async def test_custom_rollback_prepares_artifact_and_seals_without_moving_head(address_worker, serving_schema):
    prepared = address_worker
    schema = prepared.worker.schema
    async with _publication_runtime(prepared) as runtime:
        seed, first, second = await _publish_manual_history(prepared, serving_schema[2], runtime)
        admin = RegistryActor("platform_admin", uuid4(), "system")
        async with runtime.api.transaction():
            queued = await enqueue_registry_source_rollback_preview(
                runtime.api,
                dict(
                    operation="prepare_source_rollback",
                    target_generation=first.generation_id,
                    expected_serving_generation=second.generation_id,
                    expected_approved_revision=second.approved_custom_revision,
                    reason="Review retained source with current custom rows",
                    idempotency_key=uuid4().hex,
                ),
                admin,
                session_token_sha256="a" * 64,
                control_schema=schema,
            )
        completed = await consumer.consume_registry_publication_once(**runtime.arguments)
        assert completed["state"] == "completed" and completed["request_id"] == queued["request_id"]
        async with (
            runtime.execution_pool.acquire() as executing,
            executing.transaction(isolation="repeatable_read", readonly=True),
        ):
            await _assert_custom_preview(executing, schema, queued, seed, first, second)


async def _assert_custom_preview(executing, schema, queued, seed, first, second):
    status = await read_registry_publication_request(executing, UUID(queued["request_id"]), control_schema=schema)
    preview_by_name = status["result"]
    assert status["state"] == "completed"
    assert preview_by_name["source_generation_id"] == first.generation_id
    assert preview_by_name["expected_serving_generation"] == second.generation_id
    assert preview_by_name["approved_custom_revision"] == second.approved_custom_revision
    candidate = await executing.fetchrow(
        f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(preview_by_name["candidate_id"]),
    )
    copy_target = MembershipCopyTarget(
        **{
            field: str(candidate[field])
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        }
    )
    assert candidate["state"] == "ready" and candidate["expected_rows"] == 1
    assert await verify_network_candidate_custody(executing, copy_target, control_schema=schema, required=True)
    assert (
        await executing.fetchval(
            f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership WHERE network_id=$1 AND provider_id=$2',
            seed.network["record_id"],
            str(seed.provider["record_id"]),
        )
        == 1
    )
    assert (
        await executing.fetchval(
            f'SELECT count(*) FROM "{copy_target.schema_name}".entity_address_unified WHERE canonical_network_ids @> ARRAY[$1]::integer[]',
            seed.network["record_id"],
        )
        == 1
    )
    assert (await resolve_network_serving_manifest(executing, control_schema=schema)) == second
