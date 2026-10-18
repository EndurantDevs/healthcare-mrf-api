# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Least-privilege native retention denial, lifecycle fencing and rollback."""

import asyncio
import json
from dataclasses import asdict

import asyncpg
import pytest

from db.plan_release_storage import storage_relation
from process.plan_release_preparation_pin_contract import PreparationPinInputs
from process.plan_release_preparation_pin_retirement import (
    cleanup_detached_preparation_pin_heap,
    retire_preparation_pins,
)
from process.plan_release_preparation_pins import retain_preparation_pins
from process.ptg_parts.ptg2_lifecycle_lock import PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE, PTG2_SOURCE_POINTER_GC_LOCK_KEY
from process.ptg_parts.ptg2_source_snapshot_gc import _CURRENT_SNAPSHOT_IDS_SQL
from tests.plan_release_preparation_pin_postgres_support import capture_heap, prepare_pin_case, settle
from tests.plan_release_storage_postgres_support import NEW_REVISION, storage_case


async def _retain(prepared, *, authority=None, connection=None):
    return await retain_preparation_pins(
        connection or prepared.publisher,
        prepared.case.schema,
        prepared.scope,
        PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
        authority=authority or prepared.authority,
        **prepared.case.profile,
    )


@pytest.mark.asyncio
async def test_native_fences_close_gc_gap_and_cancellation_rolls_enrollment_back():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        entered = asyncio.Event()

        async def wait_after_attach(connection, scope, **arguments):
            result = await prepared.authority(connection, scope, **arguments)
            if arguments["phase"] == "after":
                entered.set()
                await asyncio.Event().wait()
            return result

        task = asyncio.create_task(_retain(prepared, authority=wait_after_attach))
        await asyncio.wait_for(entered.wait(), 10)
        gc_actor = await case.connect()
        async with gc_actor.transaction():
            assert not await gc_actor.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtext($1))", PTG2_SOURCE_POINTER_GC_LOCK_KEY
            )
            assert not await gc_actor.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE + ":synthetic_source",
            )
            assert await gc_actor.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                PTG2_SOURCE_LIFECYCLE_LOCK_NAMESPACE + ":unrelated_source",
            )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not await gc_actor.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.pin_heap.oid)
        assert await gc_actor.fetchval(
            f"SELECT inventory->'registered'='null'::jsonb FROM {prepared.tables['preparation']}"
        )
        await _retain(prepared)
        retained_snapshot_ids = {
            snapshot_row["snapshot_id"]
            for snapshot_row in await gc_actor.fetch(_CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", case.schema))
        }
        assert "new-snapshot" in retained_snapshot_ids


@pytest.mark.asyncio
async def test_reader_cannot_attach_and_changed_native_image_cannot_replay():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        reader = await case.connect(case.reader)
        with pytest.raises(ValueError):
            await _retain(prepared, connection=reader)
        await _retain(prepared)
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {storage_relation(prepared.pin_heap.schema, prepared.pin_heap.name)} SET reason='changed-after-seal'"
        )
        with pytest.raises(ValueError, match="heap_receipt_changed"):
            await _retain(prepared)
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.pin_heap.oid)


@pytest.mark.asyncio
async def test_source_antijoin_refuses_authenticated_but_wrong_physical_source():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {storage_relation(prepared.binding_heap.schema, prepared.binding_heap.name)} SET source_key='foreign_source'"
        )
        prepared.binding_heap = await capture_heap(case, prepared.publisher, prepared.binding_heap.name, pin=False)
        await admin.execute(
            f"UPDATE {prepared.tables['preparation']} SET inventory=jsonb_set(inventory,'{{binding_heap}}',$1::jsonb)",
            json.dumps(asdict(prepared.binding_heap)),
        )
        with pytest.raises(ValueError, match="physical_source_antijoin_failed"):
            await _retain(prepared)
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.pin_heap.oid)


@pytest.mark.asyncio
async def test_existing_header_refuses_future_identity_even_when_not_current():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        admin = await case.connect()
        await admin.execute(
            f"INSERT INTO {case.relation('plan_release_serving_revision')}(serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,is_current,expected_binding_count,binding_set_digest) VALUES($1,$2,$3,'2026-10','published','building',false,1,$4)",
            NEW_REVISION,
            "hprelease_" + "4" * 26,
            "hpplan_" + "4" * 26,
            "b" * 64,
        )
        with pytest.raises(ValueError, match="revision_has_serving_history"):
            await _retain(prepared)
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", prepared.pin_heap.oid)


@pytest.mark.asyncio
async def test_cleanup_never_cascades_dependencies_and_replay_refuses_reused_name():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case)
        receipt = await _retain(prepared)
        await settle(prepared, {NEW_REVISION: "failed"}, state="abandoned", intent=True)
        await retire_preparation_pins(
            prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
        )
        admin = await case.connect()
        relation = storage_relation(receipt.heap.schema, receipt.heap.name)
        dependency = storage_relation(case.preparation_schema, "retained_dependency")
        await admin.execute(f"CREATE VIEW {dependency} AS SELECT * FROM {relation}")
        with pytest.raises(asyncpg.DependentObjectsStillExistError):
            await cleanup_detached_preparation_pin_heap(
                prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
            )
        assert await admin.fetchval("SELECT to_regclass($1)::oid", relation) == receipt.heap.oid
        assert (
            await admin.fetchval(
                f"SELECT jsonb_array_length(inventory->'detached') FROM {prepared.tables['preparation']}"
            )
            == 1
        )
        await admin.execute(f"DROP VIEW {dependency}")
        await cleanup_detached_preparation_pin_heap(
            prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
        )
        await admin.execute(f"CREATE TABLE {relation}(synthetic_id int)")
        with pytest.raises(ValueError, match="cleaned_heap_name_reused"):
            await cleanup_detached_preparation_pin_heap(
                prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
            )
        assert await admin.fetchval("SELECT to_regclass($1)", relation) is not None
