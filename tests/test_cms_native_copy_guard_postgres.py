# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic receipt/guard semantics, not protected CMS finisher authority."""

import asyncio

import asyncpg
import pytest

from db.migration_provider_directory_dataset_candidates import TABLES
from tests.formulary_fhir_twin_admission_pg_support import connect, database_url, quoted, run_migration
from tests.test_provider_dataset_candidates_postgres import (
    _COPY_GUARD_COLUMNS,
    _assert_cms_guard_writes,
    _candidate_guard_metadata,
    _candidate_migration,
    _candidate_test_scope,
    _grant_copy_guard_relation_access,
    _insert_copy_guard_row,
    _install_retirement_child_guards,
    _prepare_copy_guard_catalog,
    _record_cms_copy_guard_publication,
    _seed_copy_guard_dataset,
    _update_canonical_copy_guard,
)


async def _install(context, roles, *, include_cms=False):
    await _install_retirement_child_guards(context)
    await run_migration(context.engine, _candidate_migration(), "upgrade")
    await _prepare_copy_guard_catalog(context, roles)
    await _update_canonical_copy_guard(context, add_column=True)
    if include_cms:
        await _update_canonical_copy_guard(context, include_cms=True)
    await _seed_copy_guard_dataset(context, "synthetic-cms-open", "acquiring", False)
    await _seed_copy_guard_dataset(context, "synthetic-cms-seal", "acquiring", False)


async def _record_native_receipt(context):
    await _record_cms_copy_guard_publication(context, "synthetic-cms-seal", None)
    await context.connection.execute(
        f"UPDATE {context.schema}.preparation SET publication=publication || "
        '\'{"native_publication":{"contract":"generic-source-canonical-copy-v1"}}\'::jsonb'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("include_cms", [False, True])
async def test_cms_native_receipt_closes_all_four_outputs_without_guard_body_change(monkeypatch, include_cms):
    async with _candidate_test_scope(monkeypatch) as (context, roles):
        await _install(context, roles, include_cms=include_cms)
        before = await _candidate_guard_metadata(context)
        await _record_native_receipt(context)
        for root in _COPY_GUARD_COLUMNS:
            await _assert_cms_guard_writes(context, roles["generic"], root, "synthetic-cms-seal", protected=True)
        assert await _candidate_guard_metadata(context) == before


async def _wait_for_header(connection, writer, publisher):
    async with asyncio.timeout(2):
        while not await connection.fetchval(
            "SELECT $2::integer=ANY(pg_catalog.pg_blocking_pids($1::integer))",
            writer.get_server_pid(),
            publisher.get_server_pid(),
        ):
            await asyncio.sleep(0.01)


async def _assert_independent_io(connection, context, role, root):
    header = f"{context.schema}.provider_directory_endpoint_dataset"
    assert (
        await connection.fetchval(f"SELECT status FROM {header} WHERE dataset_id=$1", "synthetic-cms-seal")
        == "acquiring"
    )
    async with connection.transaction():
        await connection.execute(f"SET LOCAL ROLE {quoted(role)}")
        assert (
            await _insert_copy_guard_row(
                connection, f"{context.schema}.{root}", root, "synthetic-cms-open", "independent"
            )
            == "INSERT 0 1"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["read_committed", "repeatable_read"])
async def test_existing_guard_serializes_cms_receipt_commit_without_blocking_other_dataset(monkeypatch, isolation):
    async with _candidate_test_scope(monkeypatch) as (context, roles):
        await _install(context, roles)
        await _grant_copy_guard_relation_access(context, roles)
        writer, observer = await connect(database_url()), await connect(database_url())
        pending = None
        try:
            await writer.execute("SET statement_timeout='3s'")
            await writer.execute(f"SET ROLE {quoted(roles['generic'])}")
            root = TABLES[1]
            relation = f"{context.schema}.{root}"
            header = f"{context.schema}.provider_directory_endpoint_dataset"
            async with writer.transaction(isolation=isolation):
                async with context.connection.transaction():
                    await context.connection.fetchval(
                        f"SELECT dataset_id FROM {header} WHERE dataset_id=$1 FOR UPDATE",
                        "synthetic-cms-seal",
                    )
                    await context.connection.execute(
                        f"UPDATE {header} SET status='published' WHERE dataset_id=$1", "synthetic-cms-seal"
                    )
                    await _record_native_receipt(context)
                    pending = asyncio.create_task(
                        _insert_copy_guard_row(writer, relation, root, "synthetic-cms-seal", "queued")
                    )
                    await _wait_for_header(observer, writer, context.connection)
                    await _assert_independent_io(observer, context, roles["generic"], root)
                refusal = (
                    asyncpg.ObjectNotInPrerequisiteStateError
                    if isolation == "read_committed"
                    else asyncpg.SerializationError
                )
                with pytest.raises(refusal):
                    await pending
            assert not await observer.fetchval(f"SELECT EXISTS(SELECT FROM {relation} WHERE resource_id='queued')")
            assert await observer.fetchval(f"SELECT EXISTS(SELECT FROM {relation} WHERE resource_id='independent')")
        finally:
            if pending is not None:
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            await writer.close()
            await observer.close()
