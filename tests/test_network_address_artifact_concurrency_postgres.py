# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native concurrent address readers retain a shared alias-generation fence."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager

import pytest

from process.ext.address_alias_sql import ADDRESS_ALIAS_ADVISORY_LOCK_KEY
from process.network_address_artifact import capture_network_address_artifact
from process.uhc_flex_practitioner_async_safety import drain_operation
from tests.test_network_address_artifact_postgres import address_db as address_db

pytestmark = pytest.mark.asyncio

_ACQUIRE = "SELECT pg_try_advisory_lock_shared(hashtext($1))"
_RELEASE = "SELECT pg_advisory_unlock_shared(hashtext($1))"
_EXCLUSIVE = "SELECT pg_try_advisory_xact_lock(hashtext($1))"
_HELD = """SELECT EXISTS(SELECT FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid()
  AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objsubid=1
  AND classid=((hashtext($1)::bigint>>32)&4294967295)::oid
  AND objid=(hashtext($1)::bigint&4294967295)::oid)"""


async def _unlock_exact(connection, state):
    if state["acquired"]:
        assert await connection.fetchval(_RELEASE, ADDRESS_ALIAS_ADVISORY_LOCK_KEY) is True
        state["acquired"] = False


@asynccontextmanager
async def _session_alias_pin(connection):
    assert not connection.is_in_transaction()
    assert await connection.fetchval(_HELD, ADDRESS_ALIAS_ADVISORY_LOCK_KEY) is False
    lock_state_by_field = {"acquired": False}

    async def acquire():
        lock_state_by_field["acquired"] = await connection.fetchval(_ACQUIRE, ADDRESS_ALIAS_ADVISORY_LOCK_KEY)
        assert lock_state_by_field["acquired"] is True

    async def cleanup():
        await drain_operation(_unlock_exact(connection, lock_state_by_field), preserve_cancellation=False)

    async with AsyncExitStack() as cleanup_stack:
        cleanup_stack.push_async_callback(cleanup)
        await drain_operation(acquire(), preserve_cancellation=True)
        yield


async def _exclusive_writer(connection, expected):
    assert not connection.is_in_transaction()
    async with connection.transaction():
        assert await connection.fetchval(_EXCLUSIVE, ADDRESS_ALIAS_ADVISORY_LOCK_KEY) is expected


async def _capture(resources, connection):
    assert await connection.fetchval("SHOW transaction_isolation") == "repeatable read"
    assert await connection.fetchval("SHOW transaction_read_only") == "on"
    return await capture_network_address_artifact(
        connection, resources.source, runtime_roles=(resources.roles["reader"],)
    )


async def _assert_reusable(resources):
    for connection in (resources.publisher, resources.reader):
        assert not connection.is_in_transaction()
        assert await connection.fetchval("SELECT 1") == 1
        assert await connection.fetchval(_HELD, ADDRESS_ALIAS_ADVISORY_LOCK_KEY) is False
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert await _capture(resources, connection) == resources.descriptor
    await _exclusive_writer(resources.admin, True)


async def test_native_shared_captures_coexist_and_outer_session_pin_excludes_writer(address_db):
    resources = address_db
    await _exclusive_writer(resources.admin, True)
    async with _session_alias_pin(resources.publisher):
        async with resources.publisher.transaction(isolation="repeatable_read", readonly=True):
            async with resources.reader.transaction(isolation="repeatable_read", readonly=True):
                publisher, reader = await asyncio.gather(
                    _capture(resources, resources.publisher), _capture(resources, resources.reader)
                )
                assert publisher == reader == resources.descriptor
                await _exclusive_writer(resources.admin, False)
            await _exclusive_writer(resources.admin, False)
        await _exclusive_writer(resources.admin, False)
    await _exclusive_writer(resources.admin, True)
    await _assert_reusable(resources)


async def _interrupted_capture(resources, fault, captured, release):
    async with _session_alias_pin(resources.publisher):
        async with resources.publisher.transaction(isolation="repeatable_read", readonly=True):
            assert await _capture(resources, resources.publisher) == resources.descriptor
            captured.set()
            await release.wait()
            if fault == "error":
                raise RuntimeError("Synthetic caller failure after native capture")


@pytest.mark.parametrize("fault", ["error", "cancel"])
async def test_caught_failure_releases_exact_pin_and_transaction_locks(address_db, fault):
    resources = address_db
    captured, release = asyncio.Event(), asyncio.Event()
    operation = asyncio.create_task(_interrupted_capture(resources, fault, captured, release))
    try:
        await asyncio.wait_for(captured.wait(), timeout=30)
        await _exclusive_writer(resources.admin, False)
        if fault == "cancel":
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation
        else:
            release.set()
            with pytest.raises(RuntimeError, match="Synthetic caller failure"):
                await operation
    finally:
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
    await _exclusive_writer(resources.admin, True)
    await _assert_reusable(resources)
