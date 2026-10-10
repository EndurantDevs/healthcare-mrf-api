# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migrated native semantic uniqueness through the verified management role."""

import asyncio
import json
from contextlib import suppress

import pytest
from sqlalchemy import text

from process.network_cms_registry_address_capture import native_driver
from process.registry_management_permissions import verify_registry_management_permissions
from process.registry_ptg_office_intent import reserve_registry_ptg_office_prepare_intent
from tests.test_registry_management_permissions_postgres import _arguments, _install
from tests.test_registry_management_permissions_postgres import permissions_db as permissions_db
from tests.test_registry_management_permissions_postgres import provider_db as provider_db
from tests.test_registry_management_permissions_postgres import serving_schema as serving_schema
from tests.test_registry_ptg_office_intent import actor, selector, source_command

pytestmark = pytest.mark.asyncio


async def _reserve(session, fixture, digest="a" * 64):
    return await reserve_registry_ptg_office_prepare_intent(
        session, source_command(), selector(), actor(), session_token_sha256=digest, control_schema=fixture.schema
    )


async def test_native_concurrent_same_semantic_and_lost_ack_keep_one_original_audit(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    await verify_registry_management_permissions(fixture.connection, **_arguments(fixture))
    async with fixture.sessions() as first, fixture.sessions() as second:
        await first.begin()
        await second.begin()
        pending = None
        try:
            for session in (first, second):
                await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
            original = await _reserve(first, fixture)
            second_pid = (await native_driver(second)).get_server_pid()
            pending = asyncio.create_task(_reserve(second, fixture, "b" * 64))
            async with asyncio.timeout(5):
                while not await fixture.connection.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity "
                    "WHERE pid=$1 AND wait_event_type='Lock' AND wait_event='transactionid')",
                    second_pid,
                ):
                    assert not pending.done(), "competing reserve must wait on the actual native unique row"
                    await asyncio.sleep(0.01)
            await first.commit()
            concurrent = await asyncio.wait_for(pending, 5)
            assert concurrent == {**original, "replayed": True}
            await second.commit()
        finally:
            await first.rollback()
            if pending is not None and not pending.done():
                pending.cancel()
                with suppress(asyncio.CancelledError):
                    await pending
            await second.rollback()
    # Simulate an acknowledgment lost after commit: current login rediscovers it.
    async with fixture.sessions() as recovery, recovery.begin():
        await recovery.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        assert await _reserve(recovery, fixture, "c" * 64) == {**original, "replayed": True}
    request_records = await fixture.connection.fetch(f'SELECT * FROM "{fixture.schema}".registry_publication_request')
    assert len(request_records) == 1
    assert request_records[0]["session_token_sha256"] == "a" * 64
    assert json.loads(request_records[0]["actor_json"])["impersonator_id"] is None
    assert json.loads(request_records[0]["command_json"])["selector"] == original["command"]["selector"]


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_native_snapshot_isolation_refuses_without_inserting(permissions_db, isolation):
    fixture = permissions_db
    await _install(fixture)
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation.upper()))
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        with pytest.raises(ValueError, match="requires_read_committed"):
            await _reserve(session, fixture)
        assert await session.scalar(text(f'SELECT count(*) FROM "{fixture.schema}".registry_publication_request')) == 0
        assert await session.scalar(text("SELECT 1")) == 1


async def test_native_caller_rollback_persists_no_intent(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    async with fixture.sessions() as session:
        await session.begin()
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        await _reserve(session, fixture)
        await session.rollback()
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_publication_request') == 0
    )
