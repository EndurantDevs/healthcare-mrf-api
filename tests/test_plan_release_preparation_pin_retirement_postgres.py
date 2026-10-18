# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native preparation-only exchange, abandonment and exact detached cleanup."""

import asyncio
import json
from dataclasses import asdict, replace

import asyncpg
import pytest

from db.plan_release_storage import storage_relation, storage_snapshot_index_sql, storage_table_sql
from process.plan_release_preparation_pin_contract import PreparationPinInputs
from process.plan_release_preparation_pin_retirement import (
    cleanup_detached_preparation_pin_heap,
    exchange_preparation_pins,
    retire_preparation_pins,
)
from process.plan_release_preparation_pins import require_preparation_pins, retain_preparation_pins
from tests.plan_release_preparation_pin_postgres_support import (
    SECOND_REVISION,
    THIRD_REVISION,
    capture_heap,
    prepare_pin_case,
    replacement_heap,
    settle,
)
from tests.plan_release_storage_cohort_support import CAPTURED_TIME
from tests.plan_release_storage_postgres_support import NEW_REVISION, storage_case


async def retained(prepared):
    return await retain_preparation_pins(
        prepared.publisher,
        prepared.case.schema,
        prepared.scope,
        PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
        authority=prepared.authority,
        **prepared.case.profile,
    )


async def exchange(prepared, old, replacement):
    async with prepared.publisher.transaction():
        return await exchange_preparation_pins(
            prepared.publisher,
            prepared.case.schema,
            old,
            replacement,
            authority=prepared.authority,
            **prepared.case.profile,
        )


async def _publish_native(prepared, receipt):
    case = prepared.case
    connection = prepared.publisher
    await require_preparation_pins(connection, case.schema, receipt, authority=prepared.authority, **case.profile)
    await connection.execute(f'SET LOCAL ROLE "{case.owner}"')
    binding = storage_relation(case.preparation_schema, "accepted_binding")
    await connection.execute(storage_table_sql(case.preparation_schema, "accepted_binding", primary_key=False))
    await connection.execute(
        f"INSERT INTO {binding} SELECT * FROM ONLY {storage_relation(prepared.binding_heap.schema, prepared.binding_heap.name)} WHERE serving_revision_id=ANY($1::text[])",
        list(receipt.retained_revision_ids),
    )
    await connection.execute(f"ALTER TABLE {binding} ADD PRIMARY KEY(serving_revision_id,role,binding_ordinal)")
    await connection.execute(
        storage_snapshot_index_sql(case.preparation_schema, "accepted_binding", "accepted_binding_snapshot_idx")
    )
    await connection.execute(f'GRANT SELECT ON {binding} TO "{case.reader}"')
    for index, revision in enumerate(receipt.retained_revision_ids, start=4):
        await connection.execute(
            f"INSERT INTO {case.relation('plan_release_serving_revision')}(serving_revision_id,plan_release_id,healthporta_plan_id,release_month,release_status,serving_status,is_current,expected_binding_count,binding_set_digest,created_at,published_at) VALUES($1,$2,$3,'2026-10','published','published',true,2000,$4,$5,$5)",
            revision,
            "hprelease_" + str(index) * 26,
            "hpplan_" + str(index) * 26,
            "b" * 64,
            CAPTURED_TIME,
        )
    literals = ",".join("'" + revision + "'" for revision in receipt.retained_revision_ids)
    await connection.execute(f"ALTER TABLE {binding} ADD CHECK(serving_revision_id IN ({literals}))")
    await connection.execute(
        f"ALTER TABLE {case.relation('plan_release_snapshot_binding')} ATTACH PARTITION {binding} FOR VALUES IN ({literals})"
    )
    await connection.execute(f"UPDATE {prepared.tables['preparation']} SET status='published',writers_closed=true")
    await connection.execute(f'SET LOCAL ROLE "{case.publisher}"')


@pytest.mark.asyncio
async def test_exchange_keeps_unavailable_and_preserves_detached_until_completed_serving_cut():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case, revisions=(NEW_REVISION, SECOND_REVISION, THIRD_REVISION))
        old = await retained(prepared)
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "unavailable", THIRD_REVISION: "failed"})
        replacement = await replacement_heap(prepared, (NEW_REVISION, SECOND_REVISION))
        new = await exchange(prepared, old, replacement)
        assert new.retained_revision_ids == (NEW_REVISION, SECOND_REVISION)
        assert await exchange(prepared, old, replacement) == new
        admin = await case.connect()
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", old.heap.oid) is False
        assert (
            await admin.fetchval("SELECT to_regclass($1)::oid", storage_relation(old.heap.schema, old.heap.name))
            == old.heap.oid
        )
        with pytest.raises(ValueError, match="serving_cut_not_completed"):
            await cleanup_detached_preparation_pin_heap(
                prepared.publisher, case.schema, old, authority=prepared.authority, **case.profile
            )
        async with prepared.publisher.transaction():
            await _publish_native(prepared, new)
        await cleanup_detached_preparation_pin_heap(
            prepared.publisher, case.schema, old, authority=prepared.authority, **case.profile
        )
        assert await admin.fetchval("SELECT to_regclass($1)", storage_relation(old.heap.schema, old.heap.name)) is None
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", new.heap.oid)
        await cleanup_detached_preparation_pin_heap(
            prepared.publisher, case.schema, old, authority=prepared.authority, **case.profile
        )
        with pytest.raises(ValueError, match="active_pin_custody"):
            await cleanup_detached_preparation_pin_heap(
                prepared.publisher, case.schema, new, authority=prepared.authority, **case.profile
            )


@pytest.mark.asyncio
async def test_explicit_terminal_abandonment_retires_never_served_success_only_after_no_work():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case, revisions=(NEW_REVISION, SECOND_REVISION))
        receipt = await retained(prepared)
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "unavailable"})
        with pytest.raises(ValueError, match="explicit_terminal_abandonment_required"):
            await retire_preparation_pins(
                prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
            )
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "unavailable"}, state="abandoned", intent=True)
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {prepared.tables['jobs']} SET status='running' WHERE run_id=$1", "synthetic-run-" + NEW_REVISION
        )
        with pytest.raises(ValueError, match="admissions_unresolved"):
            await retire_preparation_pins(
                prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
            )
        await admin.execute(f"UPDATE {prepared.tables['jobs']} SET status='succeeded'")
        await retire_preparation_pins(
            prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
        )
        await retire_preparation_pins(
            prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
        )
        assert (
            await admin.fetchval(
                "SELECT to_regclass($1)::oid", storage_relation(receipt.heap.schema, receipt.heap.name)
            )
            == receipt.heap.oid
        )
        await cleanup_detached_preparation_pin_heap(
            prepared.publisher, case.schema, receipt, authority=prepared.authority, **case.profile
        )
        assert (
            await admin.fetchval("SELECT to_regclass($1)", storage_relation(receipt.heap.schema, receipt.heap.name))
            is None
        )


@pytest.mark.asyncio
async def test_pending_retryable_or_live_work_refuses_exchange():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case, revisions=(NEW_REVISION, SECOND_REVISION))
        old = await retained(prepared)
        replacement = await replacement_heap(prepared, (NEW_REVISION,))
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "accepted"})
        with pytest.raises(ValueError, match="admissions_unresolved"):
            await exchange(prepared, old, replacement)
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "failed"})
        admin = await case.connect()
        await admin.execute(
            f"UPDATE {prepared.tables['admission']} SET retryable=true WHERE serving_revision_id=$1", SECOND_REVISION
        )
        with pytest.raises(ValueError, match="admissions_unresolved"):
            await exchange(prepared, old, replacement)
        await admin.execute(f"UPDATE {prepared.tables['admission']} SET retryable=false")
        await admin.execute(
            f"UPDATE {prepared.tables['jobs']} SET status='queued' WHERE run_id=$1", "synthetic-run-" + SECOND_REVISION
        )
        with pytest.raises(ValueError, match="admissions_unresolved"):
            await exchange(prepared, old, replacement)
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", old.heap.oid)
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", replacement.oid)


@pytest.mark.asyncio
async def test_exchange_callback_failure_and_outer_cancellation_restore_custody():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case, revisions=(NEW_REVISION, SECOND_REVISION))
        old = await retained(prepared)
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "failed"})
        replacement = await replacement_heap(prepared, (NEW_REVISION,))

        async def rejecting(connection, scope, **arguments):
            result = await prepared.authority(connection, scope, **arguments)
            if arguments["phase"] == "after":
                raise RuntimeError("synthetic-after-enrollment-rejection")
            return result

        with pytest.raises(RuntimeError, match="synthetic-after"):
            async with prepared.publisher.transaction():
                await exchange_preparation_pins(
                    prepared.publisher, case.schema, old, replacement, authority=rejecting, **case.profile
                )
        admin = await case.connect()
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", old.heap.oid)
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", replacement.oid)
        reached = asyncio.Event()

        async def canceled_cut():
            async with prepared.publisher.transaction():
                new = await exchange_preparation_pins(
                    prepared.publisher, case.schema, old, replacement, authority=prepared.authority, **case.profile
                )
                await _publish_native(prepared, new)
                reached.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(canceled_cut())
        await asyncio.wait_for(reached.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", old.heap.oid)
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", replacement.oid)
        assert not await admin.fetchval(
            f"SELECT EXISTS(SELECT FROM {case.relation('plan_release_serving_revision')} WHERE serving_revision_id=$1)",
            NEW_REVISION,
        )
        assert await exchange(prepared, old, replacement)


@pytest.mark.asyncio
async def test_replacement_rejects_changed_capture_time_and_wrong_registered_owner():
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case, revisions=(NEW_REVISION, SECOND_REVISION))
        old = await retained(prepared)
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "failed"})
        replacement = await replacement_heap(prepared, (NEW_REVISION,))
        with pytest.raises(ValueError, match="registered_pin_custody_changed"):
            await exchange(prepared, replace(old, owner_oid=old.owner_oid + 1), replacement)
        admin = await case.connect()
        relation = storage_relation(replacement.schema, replacement.name)
        await admin.execute(f"UPDATE {relation} SET created_at=created_at+interval '1 second'")
        replacement = await capture_heap(case, prepared.publisher, replacement.name, pin=True)
        await admin.execute(
            f"UPDATE {prepared.tables['preparation']} SET inventory=jsonb_set(inventory,'{{pin_heap}}',$1::jsonb)",
            json.dumps(asdict(replacement)),
        )
        with pytest.raises(ValueError, match="replacement_pin_rows_changed"):
            await exchange(prepared, old, replacement)
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", old.heap.oid)
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", replacement.oid)
