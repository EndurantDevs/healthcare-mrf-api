# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Use the original isolated fixture to prove retained native composition ownership."""

import asyncio
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from process import registry_candidate_composition as composition
from process.registry_company_approval_fence import _fence_key
from tests.test_network_custom_address_source_postgres import _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_candidate_composition_postgres import _remove_candidates, _roles

pytestmark = pytest.mark.asyncio


def _native_phases(monkeypatch, fixture, *, should_cancel):
    original_prepare = composition._prepare_composition_snapshot
    original_copy = composition._copy_composition
    retained_pids = []

    async def preparation(session, *args):
        assert type(session) is AsyncSession
        retained_pids.append(await session.scalar(text("SELECT pg_backend_pid()")))
        return await original_prepare(session, *args)

    async def copy(driver, *args):
        assert await driver.fetchval("SELECT pg_backend_pid()") == retained_pids[0]
        async with fixture.connection.transaction():
            assert (
                await fixture.connection.fetchval(
                    "SELECT pg_try_advisory_xact_lock($1)", _fence_key(fixture.control_schema)
                )
                is True
            )
        if should_cancel:
            raise asyncio.CancelledError()
        await original_copy(driver, *args)

    monkeypatch.setattr(composition, "_prepare_composition_snapshot", preparation)
    monkeypatch.setattr(composition, "_copy_composition", copy)
    return retained_pids


@pytest.mark.parametrize("should_cancel", [False, True])
async def test_native_factory_keeps_one_driver_releases_fence_and_resumes(custom_db, monkeypatch, should_cancel):
    fixture = custom_db
    seed, request_id, copy_targets = await _seed(fixture), uuid4(), []
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await composition.pin_approved_membership_source(
            fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    engine = create_async_engine(fixture.engine.url, pool_size=1, max_overflow=0)
    arguments_by_name = {
        "request_id": request_id,
        "approved_revision": seed.revision,
        "expected_head": 0,
        "address_sources": composition.RegistryCompositionAddressSources(fixture.base),
        "writer_roles": _roles(fixture),
        "control_schema": fixture.control_schema,
    }
    try:
        with monkeypatch.context() as phase_patch:
            retained_pids = _native_phases(phase_patch, fixture, should_cancel=should_cancel)
            if should_cancel:
                with pytest.raises(asyncio.CancelledError):
                    await composition.compose_registry_membership_candidate(
                        async_sessionmaker(engine), **arguments_by_name
                    )
            else:
                copy_target, address = await composition.compose_registry_membership_candidate(
                    async_sessionmaker(engine), **arguments_by_name
                )
                copy_targets.append(copy_target)
        copy_target, address = await composition.compose_registry_membership_candidate(
            async_sessionmaker(engine), **arguments_by_name
        )
        copy_targets.append(copy_target)
        assert retained_pids and engine.pool.checkedout() == 0
        assert not await fixture.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_catalog.pg_locks WHERE pid=$1 AND locktype='advisory' AND granted)",
            retained_pids[0],
        )
        candidate = await fixture.connection.fetchrow(
            f'SELECT state,accepted_rows,expected_rows FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        assert tuple(candidate) == ("sealed", 1, 1) and address.schema_name
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
            )
            is None
        )
    finally:
        await engine.dispose()
        await _remove_candidates(fixture, copy_targets, request_id)
