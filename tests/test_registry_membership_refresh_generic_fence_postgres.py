# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine native lock-only actors pin accepted generic heads through the source context."""

import asyncio
from dataclasses import replace

import asyncpg
import pytest
from sqlalchemy import text

from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_generic_accepted_capture import read_current_generic_fhir_head
from process.registry_membership_refresh_contract import RegistryMembershipRefreshUnavailable
from process.registry_membership_refresh_source_fence import RegistryMembershipRefreshHeadFences
from process.registry_membership_refresh_sources import (
    observe_registry_membership_refresh_sources,
    pin_registry_membership_refresh_sources,
)
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from tests.generic_refresh_fence_postgres_support import actor_connection, generic_fence_context

pytestmark = pytest.mark.asyncio


@pytest.fixture(params=["modern"])
async def generic_head_db(request):
    async with generic_fence_context(request) as context:
        yield context


def _pin(context, session, readers=None):
    return pin_registry_membership_refresh_sources(
        session,
        context.enrollment,
        approved_revision=context.approved.approved_revision,
        readers=context.readers if readers is None else readers,
    )


def _assert_pools_released(context):
    assert context.readers.head_fences.generic_sessions.kw["bind"].pool.checkedout() == 0
    assert context.readers.source_reader_sessions.kw["bind"].pool.checkedout() == 0


async def _assert_heads_released(context, writer):
    origin, schema = context.capture.origin, context.source.schema
    async with writer.transaction():
        assert await writer.fetchval(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
            origin.endpoint_id,
        )
        assert await writer.fetchval("SELECT pg_try_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'))")
        for table, key, value in (
            ("provider_directory_api_endpoint", "endpoint_id", origin.endpoint_id),
            ("provider_directory_source", "source_id", origin.source_id),
            ("provider_directory_endpoint_dataset", "dataset_id", origin.dataset_id),
        ):
            assert (
                await writer.fetchval(
                    f'SELECT "{key}" FROM "{schema}"."{table}" WHERE "{key}"=$1 FOR UPDATE NOWAIT',
                    value,
                )
                == value
            )
    _assert_pools_released(context)


async def test_native_full_source_pin_and_observer_use_real_independent_head_actor(generic_head_db):
    context = generic_head_db
    for kind in ("reader", "head"):
        actor = await actor_connection(context, kind)
        assert tuple(await actor.fetchrow("SELECT current_user,session_user,current_setting('role')")) == (
            context.roles_by_kind[kind],
            context.roles_by_kind[kind],
            "none",
        )
        assert not await actor.fetchval(
            "SELECT pg_has_role(current_user,$1,'USAGE') OR pg_has_role(current_user,$1,'SET')",
            context.source.role_by_kind["source"],
        )
        if kind == "head":
            assert not await actor.fetchval(
                "SELECT has_column_privilege(current_user,$1,'status','UPDATE')",
                f'"{context.source.schema}".provider_directory_endpoint_dataset',
            )
            assert not await actor.fetchval(
                "SELECT has_table_privilege(current_user,$1,'SELECT')",
                f'"{context.source.schema}".provider_directory_dataset_resource',
            )
    async with context.readers.source_reader_sessions() as child, child.begin():
        await child.execute(text("SET TRANSACTION READ ONLY"))
        assert await child.scalar(text("SHOW transaction_read_only")) == "on"
        assert not await child.scalar(
            text("SELECT has_any_column_privilege(current_user,:relation,'UPDATE')"),
            {"relation": f'"{context.source.schema}".provider_directory_endpoint_dataset'},
        )
    async with registry_company_approval_transaction(context.outer, control_schema=context.schema) as session:
        observed = await observe_registry_membership_refresh_sources(
            session,
            context.enrollment,
            approved_revision=context.approved.approved_revision,
            readers=context.readers,
        )
        async with _pin(context, session) as pinned:
            assert pinned == observed
            assert pinned.accepted_sources[0]["capture_generation"] == context.capture.capture_sha256
            assert pinned.recipes[0].source_pin.retained_capture == context.capture
            reader = await context.source.connect("reader")
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                head = await read_current_generic_fhir_head(
                    reader,
                    schema_name=context.source.schema,
                    source_id=context.capture.origin.source_id,
                    endpoint_id=context.capture.origin.endpoint_id,
                )
                assert head.dataset_id == context.capture.origin.dataset_id
            head_engine = context.readers.head_fences.generic_sessions.kw["bind"]
            assert head_engine.pool.checkedout() == 1
            assert head_engine.pool is not context.readers.source_reader_sessions.kw["bind"].pool
            assert context.readers.source_reader_sessions.kw["bind"].pool.checkedout() == 1
    _assert_pools_released(context)


async def test_selected_alias_updates_block_while_foreign_endpoint_continues(generic_head_db):
    context = generic_head_db
    writer = await actor_connection(context, "writer")
    origin, schema = context.capture.origin, context.source.schema
    async with registry_company_approval_transaction(context.outer, control_schema=context.schema) as session:
        async with _pin(context, session):
            async with writer.transaction():
                assert not await writer.fetchval(
                    "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                    origin.endpoint_id,
                )
                assert not await writer.fetchval(
                    "SELECT pg_try_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'))"
                )
                await _assert_selected_updates_block(writer, origin, schema)
                assert (
                    await writer.execute(
                        f'UPDATE "{schema}".provider_directory_api_endpoint SET endpoint_id=endpoint_id '
                        "WHERE endpoint_id='endpoint-foreign'"
                    )
                    == "UPDATE 1"
                )
                assert (
                    await writer.execute(
                        f'UPDATE "{schema}".provider_directory_source SET endpoint_id=endpoint_id '
                        "WHERE source_id='source-foreign'"
                    )
                    == "UPDATE 1"
                )
        await _assert_heads_released(context, writer)


async def test_existing_endpoint_publisher_lock_refuses_source_pin_and_releases_alias_fence(generic_head_db):
    context = generic_head_db
    writer = await actor_connection(context, "writer")
    async with writer.transaction():
        await writer.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            context.capture.origin.endpoint_id,
        )
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            async with registry_company_approval_transaction(context.outer, control_schema=context.schema) as session:
                async with _pin(context, session):
                    pytest.fail("A selected publisher held the endpoint fence")
        _assert_pools_released(context)
        assert await writer.fetchval("SELECT pg_try_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'))")
    await _assert_heads_released(context, writer)


@pytest.mark.parametrize(
    "failure",
    ("bad_current_index", "missing_lock_privilege", "missing_metadata_select", "unsafe_login", "unsafe_owner"),
)
async def test_native_head_qualification_fails_closed(generic_head_db, failure):
    context = generic_head_db
    admin, schema, role = context.source.admin, context.source.schema, context.roles_by_kind["head"]
    if failure == "bad_current_index":
        await admin.execute(f'DROP INDEX "{schema}".provider_directory_endpoint_dataset_current_idx')
        await admin.execute(
            f"CREATE INDEX provider_directory_endpoint_dataset_current_idx "
            f'ON "{schema}".provider_directory_endpoint_dataset(endpoint_id) WHERE is_current=true'
        )
    elif failure == "missing_lock_privilege":
        await admin.execute(f'REVOKE UPDATE(endpoint_id) ON "{schema}".provider_directory_api_endpoint FROM "{role}"')
    elif failure == "missing_metadata_select":
        await admin.execute(f'REVOKE SELECT(status) ON "{schema}".provider_directory_endpoint_dataset FROM "{role}"')
    elif failure == "unsafe_login":
        await admin.execute(f'ALTER ROLE "{role}" CREATEDB')
    else:
        await admin.execute(f'ALTER ROLE "{context.source.role_by_kind["source"]}" LOGIN')
    expected_error = (
        RegistryMembershipRefreshUnavailable if failure == "unsafe_login" else RegistryRefreshAnchorUnavailable
    )
    with pytest.raises(expected_error):
        async with registry_company_approval_transaction(context.outer, control_schema=context.schema) as session:
            async with _pin(context, session):
                pytest.fail("Unqualified native metadata authority was accepted")
    await _assert_heads_released(context, await actor_connection(context, "writer"))


async def test_missing_native_head_factory_fails_closed(generic_head_db):
    context = generic_head_db
    readers = replace(context.readers, head_fences=RegistryMembershipRefreshHeadFences())
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        async with registry_company_approval_transaction(context.outer, control_schema=context.schema) as session:
            async with _pin(context, session, readers):
                pytest.fail("A generic source pin requires its native metadata actor")
    _assert_pools_released(context)


async def test_cancelled_source_pin_releases_native_rows_alias_and_endpoint_locks(generic_head_db):
    context = generic_head_db
    entered, waiting = asyncio.Event(), asyncio.Event()

    async def consumer():
        async with registry_company_approval_transaction(context.outer, control_schema=context.schema) as session:
            async with _pin(context, session):
                entered.set()
                await waiting.wait()

    task = asyncio.create_task(consumer())
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await _assert_heads_released(context, await actor_connection(context, "writer"))


async def _assert_selected_updates_block(writer, origin, schema):
    """Keep each failed selected-row update inside its own rollback savepoint."""
    for table, key, field_value, assignment in (
        ("provider_directory_api_endpoint", "endpoint_id", origin.endpoint_id, "endpoint_id=endpoint_id"),
        ("provider_directory_source", "source_id", origin.source_id, "endpoint_id='endpoint-foreign'"),
        ("provider_directory_endpoint_dataset", "dataset_id", origin.dataset_id, "is_current=false"),
    ):
        with pytest.raises(asyncpg.LockNotAvailableError, match="lock timeout"):
            async with writer.transaction():
                await writer.execute("SET LOCAL lock_timeout='100ms'")
                await writer.execute(
                    f'UPDATE "{schema}"."{table}" SET {assignment} WHERE "{key}"=$1',
                    field_value,
                )
