# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent lease maintenance during genuine execution-held queue locks."""

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process import registry_publication_consumer as consumer
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_management_permissions import install_registry_management_permissions
from process.registry_publication_queue import enqueue_registry_publication
from process.registry_publication_worker_permissions import install_registry_publication_worker_permissions
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _command, _create, _draft

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def heartbeat_worker(serving_schema):
    connection, schema, engine = serving_schema
    role_names_by_kind = {
        kind: "heartbeat_" + kind + "_" + uuid4().hex for kind in ("api", "lifecycle", "execution", "queue", "physical")
    }
    password = uuid4().hex
    actor = _actor()
    draft_record = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, draft_record)
    created_role_names = []
    try:
        for kind, role in role_names_by_kind.items():
            created_role_names.append(role)
            options = f" LOGIN PASSWORD '{password}'" if kind in {"api", "lifecycle", "execution"} else " NOLOGIN"
            await connection.execute(f'CREATE ROLE "{role}"{options}')
        await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{role_names_by_kind["queue"]}"')
        config = consumer.RegistryPublicationConsumerConfig(
            control_schema=schema,
            lifecycle_role=role_names_by_kind["lifecycle"],
            execution_role=role_names_by_kind["execution"],
            api_role=role_names_by_kind["api"],
            queue_owner_role=role_names_by_kind["queue"],
            physical_owner_role=role_names_by_kind["physical"],
        )
        async with connection.transaction():
            await install_registry_management_permissions(
                connection, api_role=config.api_role, owner_role=config.queue_owner_role, control_schema=schema
            )
            await install_registry_publication_worker_permissions(
                connection, **config.permission_roles, control_schema=schema
            )
        yield SimpleNamespace(
            connection=connection, schema=schema, config=config, password=password, actor=actor, command=command
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for role in reversed(created_role_names):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", created_role_names
        )


@asynccontextmanager
async def _claimed(fixture):
    dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    async with AsyncExitStack() as stack:
        api = await asyncpg.connect(dsn, user=fixture.config.api_role, password=fixture.password)
        stack.push_async_callback(api.close)
        async with api.transaction():
            await enqueue_registry_publication(
                api,
                fixture.command,
                fixture.actor,
                session_token_sha256="a" * 64,
                expected_serving_generation=0,
                control_schema=fixture.schema,
            )
        pools = []
        for role in (fixture.config.lifecycle_role, fixture.config.execution_role):
            pool = await asyncpg.create_pool(
                dsn,
                user=role,
                password=fixture.password,
                min_size=1,
                max_size=2,
                server_settings={"statement_timeout": "120000", "lock_timeout": "5000"},
            )
            stack.push_async_callback(pool.close)
            pools.append(pool)
        request_record = await consumer._claim(pools[0], fixture.config)
        assert request_record["state"] == "running"
        yield (
            dict(
                lifecycle_pool=pools[0],
                execution_pool=pools[1],
                config=fixture.config,
                authorize=None,
                initial_address_sources=None,
            ),
            request_record,
        )


def _observe_renewals(monkeypatch):
    original = consumer.renew_registry_publication_lease
    contended, renewed = asyncio.Event(), asyncio.Event()
    errors = []

    async def observe(connection, *args, **options):
        await connection.execute("SET LOCAL lock_timeout='50ms'")
        try:
            result = await original(connection, *args, **options)
        except asyncpg.LockNotAvailableError as error:
            errors.append(error.sqlstate)
            contended.set()
            raise
        if result is not None:
            renewed.set()
        return result

    monkeypatch.setattr(consumer, "renew_registry_publication_lease", observe)
    monkeypatch.setattr(consumer, "_HEARTBEAT_SECONDS", 0.01)
    return contended, renewed, errors


async def _run(arguments, request_record):
    return await consumer._run_with_heartbeat(
        arguments["lifecycle_pool"],
        arguments["execution_pool"],
        arguments["config"],
        request_record,
        arguments["authorize"],
        arguments["initial_address_sources"],
    )


async def test_execution_row_lock_keeps_exact_live_lease_then_resumes_renewal(heartbeat_worker, monkeypatch):
    contended, renewed, errors = _observe_renewals(monkeypatch)
    locked, release = asyncio.Event(), asyncio.Event()
    namespace = '"' + heartbeat_worker.schema + '"'

    async def execute(pool, config, request_record, *_args):
        async with pool.acquire() as connection:
            await consumer._verify_execution(connection, config)
            async with connection.transaction():
                await consumer._request(
                    connection, request_record["request_id"], request_record["lease_id"], namespace, lock=True
                )
                locked.set()
                await release.wait()
            await renewed.wait()
            return {"lease_checked": True}

    monkeypatch.setattr(consumer, "_execute_publication_request", execute)
    async with _claimed(heartbeat_worker) as (arguments, request_record):
        pending = asyncio.create_task(_run(arguments, request_record))
        try:
            await asyncio.wait_for(locked.wait(), 5)
            renewed.clear()
            await asyncio.wait_for(contended.wait(), 5)
            # A second attempt also times out, so the first fallback cannot hide an aborted transaction.
            async with asyncio.timeout(5):
                while len(errors) < 2:
                    assert not pending.done()
                    await asyncio.sleep(0.01)
            assert not pending.done() and not renewed.is_set()
            release.set()
            assert await asyncio.wait_for(pending, 5) == {"lease_checked": True}
            assert errors == ["55P03"] * len(errors)
            current = await heartbeat_worker.connection.fetchrow(
                f"SELECT lease_id,lease_expires_at,state FROM {namespace}.registry_publication_request WHERE request_id=$1",
                request_record["request_id"],
            )
            assert current["lease_id"] == request_record["lease_id"] and current["state"] == "running"
            assert current["lease_expires_at"] > request_record["lease_expires_at"]
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("change", ["expiry", "lease"])
async def test_row_contention_never_accepts_expired_or_replaced_lease(heartbeat_worker, monkeypatch, change):
    contended, _renewed, errors = _observe_renewals(monkeypatch)
    release, changed, canceled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    namespace = '"' + heartbeat_worker.schema + '"'
    replacement = uuid4()

    async def execute(pool, config, request_record, *_args):
        try:
            async with pool.acquire() as connection:
                await consumer._verify_execution(connection, config)
                async with connection.transaction():
                    await consumer._request(
                        connection, request_record["request_id"], request_record["lease_id"], namespace, lock=True
                    )
                    await release.wait()
                    if change == "expiry":
                        await connection.execute(
                            f"UPDATE {namespace}.registry_publication_request "
                            "SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
                            request_record["request_id"],
                        )
                    else:
                        await connection.execute(
                            f"UPDATE {namespace}.registry_publication_request SET lease_id=$2 WHERE request_id=$1",
                            request_record["request_id"],
                            replacement,
                        )
                changed.set()
                await asyncio.Event().wait()
        finally:
            canceled.set()

    monkeypatch.setattr(consumer, "_execute_publication_request", execute)
    async with _claimed(heartbeat_worker) as (arguments, request_record):
        pending = asyncio.create_task(_run(arguments, request_record))
        try:
            await asyncio.wait_for(contended.wait(), 5)
            assert not pending.done() and errors == ["55P03"] * len(errors)
            release.set()
            await asyncio.wait_for(changed.wait(), 5)
            with pytest.raises(RegistryApprovalConflict, match="^registry_queue_lease_conflict$"):
                await asyncio.wait_for(pending, 5)
            assert canceled.is_set()
            current = await heartbeat_worker.connection.fetchrow(
                f"SELECT lease_id,lease_expires_at<clock_timestamp() AS expired,state FROM "
                f"{namespace}.registry_publication_request WHERE request_id=$1",
                request_record["request_id"],
            )
            assert current["state"] == "running"
            assert current["expired"] == (change == "expiry")
            assert current["lease_id"] == (replacement if change == "lease" else request_record["lease_id"])
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
