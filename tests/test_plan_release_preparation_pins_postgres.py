# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native future-headerless custody through ambiguous network admission."""

import asyncio
import json

import asyncpg
import pytest

from process.plan_release_preparation_pin_contract import PreparationPinInputs
from process.plan_release_preparation_pins import publish_prepared_plan_release, retain_preparation_pins
from process.ptg_parts.ptg2_source_snapshot_gc import _CURRENT_SNAPSHOT_IDS_SQL
from tests.plan_release_preparation_pin_postgres_support import prepare_pin_case, seal_native_publication
from tests.plan_release_storage_postgres_support import NEW_REVISION, OLD_REVISION, storage_case


@pytest.mark.asyncio
async def test_future_pin_retention_precedes_network_and_replays_ambiguous_admission():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        query = _CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", case.schema)
        gc_reader = await case.connect()
        assert "new-snapshot" not in {snapshot_row["snapshot_id"] for snapshot_row in await gc_reader.fetch(query)}
        receipt = await retain_preparation_pins(
            prepared.publisher,
            case.schema,
            prepared.scope,
            PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
            authority=prepared.authority,
            **case.profile,
        )
        assert "new-snapshot" in {snapshot_row["snapshot_id"] for snapshot_row in await gc_reader.fetch(query)}
        assert not await prepared.publisher.fetchval(
            f"SELECT EXISTS(SELECT FROM {case.relation('plan_release_serving_revision')} WHERE serving_revision_id=$1)",
            NEW_REVISION,
        )
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {prepared.tables['admission']} SET status='dispatching',dispatches='[{{}}]'::jsonb"
        )
        replay = await retain_preparation_pins(
            prepared.publisher,
            case.schema,
            prepared.scope,
            PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
            authority=prepared.authority,
            **case.profile,
        )
        assert replay == receipt


@pytest.mark.asyncio
async def test_missing_native_authority_does_not_attach_or_accept_late_pins():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        with pytest.raises(ValueError, match="native_authority_required"):
            await retain_preparation_pins(
                prepared.publisher,
                case.schema,
                prepared.scope,
                PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
                **case.profile,
            )
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {prepared.tables['admission']} SET status='dispatching',dispatches='[{{}}]'::jsonb"
        )
        with pytest.raises(ValueError, match="late_pin_admission_refused"):
            await retain_preparation_pins(
                prepared.publisher,
                case.schema,
                prepared.scope,
                PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
                authority=prepared.authority,
                **case.profile,
            )
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.pin_heap.oid)


async def publish_prepared_case(
    prepared, receipt, publication, *, authority=None, expected_current=(OLD_REVISION, "a" * 64)
):
    async with prepared.publisher.transaction():
        await publish_prepared_plan_release(
            prepared.publisher,
            prepared.case.schema,
            publication,
            inputs=PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
            receipt=receipt,
            expected_current=expected_current,
            authority=authority or prepared.authority,
            **prepared.case.profile,
        )


async def retain_case(prepared):
    return await retain_preparation_pins(
        prepared.publisher,
        prepared.case.schema,
        prepared.scope,
        PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
        authority=prepared.authority,
        **prepared.case.profile,
    )


@pytest.mark.asyncio
async def test_native_publication_keeps_exact_pins_and_pinned_reader_history():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        receipt = await retain_case(prepared)
        publication = await seal_native_publication(prepared)
        reader = await case.connect(case.reader)
        header = case.relation("plan_release_serving_revision")
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await reader.fetchval(f"SELECT serving_revision_id FROM {header} WHERE is_current") == OLD_REVISION
            await publish_prepared_case(prepared, receipt, publication)
            assert await reader.fetchval(f"SELECT serving_revision_id FROM {header} WHERE is_current") == OLD_REVISION
        assert await reader.fetchval(f"SELECT serving_revision_id FROM {header} WHERE is_current") == NEW_REVISION
        assert (
            json.loads(await reader.fetchval(f"SELECT source_manifest FROM {header} WHERE is_current"))
            == publication.source_manifest
        )
        assert await prepared.publisher.fetchval("SELECT current_user=session_user")
        admin = await case.connect()
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", receipt.heap.oid)
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.binding_heap.oid)
        assert await admin.fetchval(f"SELECT status FROM {prepared.tables['preparation']}") == "published"
        assert await admin.fetchval(f"SELECT count(*) FROM {prepared.tables['jobs']}") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["before", "after"])
async def test_native_publication_failure_rolls_back_header_binding_and_authority(phase):
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        receipt = await retain_case(prepared)
        publication = await seal_native_publication(prepared)

        async def refused(connection, scope, **options):
            witness = await prepared.authority(connection, scope, **options)
            if options["action"] == "publish" and options["phase"] == phase:
                raise ValueError("publication refused")
            return witness

        with pytest.raises(ValueError, match="publication refused"):
            await publish_prepared_case(prepared, receipt, publication, authority=refused)
        admin = await case.connect()
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.binding_heap.oid)
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", receipt.heap.oid)
        assert await admin.fetchval(f"SELECT status FROM {prepared.tables['preparation']}") == "sealed"
        assert (
            await admin.fetchval(
                f"SELECT serving_revision_id FROM {case.relation('plan_release_serving_revision')} WHERE is_current"
            )
            == OLD_REVISION
        )
        assert await prepared.publisher.fetchval("SELECT current_user=session_user")


@pytest.mark.asyncio
@pytest.mark.parametrize("has_current", [False, True])
async def test_native_publication_lock_timeout_rolls_back_caller_and_retries_fresh(has_current):
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        receipt = await retain_case(prepared)
        publication = await seal_native_publication(prepared)
        blocker = await case.connect()
        header = case.relation("plan_release_serving_revision")
        if not has_current:
            await blocker.execute(f"UPDATE {header} SET is_current=false,serving_status='retired'")
        expected_current = (OLD_REVISION, "a" * 64) if has_current else None
        headers_before = await blocker.fetch(f"SELECT * FROM {header} ORDER BY serving_revision_id")
        preparation_before = await blocker.fetchrow(f"SELECT * FROM {prepared.tables['preparation']}")
        async with blocker.transaction():
            await blocker.execute(f"LOCK TABLE {header} IN ROW EXCLUSIVE MODE")
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with prepared.publisher.transaction():
                    failed_transaction = await prepared.publisher.fetchval("SELECT pg_current_xact_id()::text")
                    await publish_prepared_case(prepared, receipt, publication, expected_current=expected_current)
        assert not prepared.publisher.is_in_transaction()
        assert await prepared.publisher.fetchval("SELECT current_user=session_user")
        assert await blocker.fetch(f"SELECT * FROM {header} ORDER BY serving_revision_id") == headers_before
        assert await blocker.fetchrow(f"SELECT * FROM {prepared.tables['preparation']}") == preparation_before
        assert not await blocker.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.binding_heap.oid)
        assert await blocker.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", receipt.heap.oid)
        async with prepared.publisher.transaction():
            assert await prepared.publisher.fetchval("SELECT pg_current_xact_id()::text") != failed_transaction
            await publish_prepared_case(prepared, receipt, publication, expected_current=expected_current)
        assert await blocker.fetchval(f"SELECT serving_revision_id FROM {header} WHERE is_current") == NEW_REVISION
        assert await blocker.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.binding_heap.oid)
        assert await blocker.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", receipt.heap.oid)
        assert await blocker.fetchval(f"SELECT status FROM {prepared.tables['preparation']}") == "published"


@pytest.mark.asyncio
async def test_cancelled_native_publication_restores_original_role_and_cut():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        receipt = await retain_case(prepared)
        publication = await seal_native_publication(prepared)
        attached = asyncio.Event()

        async def wait_after_attach(connection, scope, **options):
            witness = await prepared.authority(connection, scope, **options)
            if options["action"] == "publish" and options["phase"] == "after":
                attached.set()
                await asyncio.Event().wait()
            return witness

        task = asyncio.create_task(publish_prepared_case(prepared, receipt, publication, authority=wait_after_attach))
        try:
            await asyncio.wait_for(attached.wait(), 5)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert await prepared.publisher.fetchval("SELECT current_user=session_user")
        assert not await prepared.publisher.fetchval(
            "SELECT relispartition FROM pg_class WHERE oid=$1", prepared.binding_heap.oid
        )
        await publish_prepared_case(prepared, receipt, publication)
