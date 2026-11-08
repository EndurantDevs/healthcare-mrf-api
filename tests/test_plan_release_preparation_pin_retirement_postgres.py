# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native preparation-only exchange, abandonment and exact detached cleanup."""

import asyncio
import json
from dataclasses import asdict, replace

import asyncpg
import pytest

from db.plan_release_storage import storage_relation, storage_snapshot_index_sql, storage_table_sql
from process.plan_release_preparation_pin_contract import (
    PlanReleaseHistoryAuthority,
    PreparationPinHeap,
    PreparationPinInputs,
    digest,
)
from process.plan_release_preparation_pin_retirement import (
    capture_plan_release_history,
    cleanup_detached_preparation_pin_heap,
    exchange_preparation_pins,
    retire_preparation_pins,
    transition_plan_release_history,
)
from process.plan_release_preparation_pins import require_preparation_pins, retain_preparation_pins
from process.ptg_parts.ptg2_source_snapshot_gc import _CURRENT_SNAPSHOT_IDS_SQL
from tests.plan_release_preparation_pin_postgres_support import (
    SECOND_REVISION,
    THIRD_REVISION,
    capture_heap,
    prepare_pin_case,
    replacement_heap,
    seal_native_publication,
    settle,
)
from tests.plan_release_storage_cohort_support import CAPTURED_TIME
from tests.plan_release_storage_postgres_support import NEW_REVISION, OLD_REVISION, storage_case
from tests.test_plan_release_preparation_pins_postgres import publish_prepared_case, retain_case
from tests.test_plan_release_storage_postgres import _storage_catalog_snapshot


async def retained(prepared):
    return await retain_preparation_pins(
        prepared.publisher,
        prepared.case.schema,
        prepared.scope,
        PreparationPinInputs(prepared.binding_heap, prepared.pin_heap),
        authority=prepared.authority,
        **prepared.case.profile,
    )


def history_authority(prepared, publication, expected_current):
    async def authorize(connection, cut, *, action, phase, observed):
        inventory = json.loads(
            await connection.fetchval(f"SELECT inventory FROM {prepared.tables['preparation']} FOR UPDATE")
        )
        detached = inventory.get("history_detached")
        if phase == "after":
            if action == "detach":
                detached = asdict(observed)
            elif action == "reactivate":
                detached = None
            inventory["history_detached"] = detached
            await connection.execute(
                f"UPDATE {prepared.tables['preparation']} SET inventory=$1::jsonb", json.dumps(inventory)
            )
        return PlanReleaseHistoryAuthority(
            digest(cut.payload()),
            digest(publication.payload()),
            action,
            expected_current,
            cut.pin_revision_ids if action == "detach" else (publication.serving_revision_id,),
            PreparationPinHeap(**detached) if detached else None,
        )

    return authorize


async def history_transition(prepared, operation, *, detached_pin_heap=None, manifest=None, revision_id=NEW_REVISION):
    async with prepared.publisher.transaction():
        cut = await capture_plan_release_history(
            prepared.publisher,
            prepared.case.schema,
            revision_id,
            detached_pin_heap=detached_pin_heap,
            **prepared.case.profile,
        )
        current = await prepared.publisher.fetchrow(
            f"SELECT serving_revision_id,binding_set_digest FROM {prepared.case.relation('plan_release_serving_revision')} WHERE healthporta_plan_id=$1 AND is_current",
            cut.publication.healthporta_plan_id,
        )
        expected = tuple(current) if current else None
        publication = replace(cut.publication, source_manifest=manifest) if manifest is not None else cut.publication
        await transition_plan_release_history(
            prepared.publisher,
            prepared.case.schema,
            cut,
            operation=operation,
            publication=publication,
            expected_current=expected,
            authority=history_authority(prepared, publication, expected),
            **prepared.case.profile,
        )
        return cut


async def published_history(case):
    prepared = await prepare_pin_case(case)
    receipt = await retain_case(prepared)
    publication = await seal_native_publication(prepared)
    await publish_prepared_case(prepared, receipt, publication)
    return prepared


@pytest.mark.asyncio
async def test_retained_history_repairs_withdraws_and_reattaches_exact_pin():
    async with storage_case(provisioned=True) as case:
        prepared = await published_history(case)
        manifest_by_field = {"projection": {"receipt": "verified-native-result"}}
        await history_transition(prepared, "repair", manifest=manifest_by_field)
        await history_transition(prepared, "withdraw")
        cut = await history_transition(prepared, "detach")
        admin = await case.connect()
        assert not await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", cut.pin_heap.oid)
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", cut.binding_heap.oid)
        assert (
            await admin.fetchval(
                f"SELECT count(*) FROM {case.relation('plan_release_snapshot_binding')} WHERE serving_revision_id=$1",
                NEW_REVISION,
            )
            == 2000
        )
        referenced = await admin.fetch(_CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", case.schema))
        assert "new-snapshot" in {row["snapshot_id"] for row in referenced}
        restored = await history_transition(prepared, "reactivate", detached_pin_heap=cut.pin_heap)
        assert restored.binding_heap == cut.binding_heap and restored.pin_heap == cut.pin_heap
        assert await admin.fetchval("SELECT relispartition FROM pg_class WHERE oid=$1", cut.pin_heap.oid)
        header = await admin.fetchrow(
            f"SELECT * FROM {case.relation('plan_release_serving_revision')} WHERE is_current"
        )
        assert (
            header["serving_revision_id"] == NEW_REVISION and json.loads(header["source_manifest"]) == manifest_by_field
        )
        assert await prepared.publisher.fetchval("SELECT current_user=session_user")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "damage"),
    [
        ("reactivate", "superset"),
        ("reactivate", "subset"),
        ("reactivate", "noncanonical"),
        ("detach", "subset"),
        ("withdraw", "superset"),
        ("repair", "superset"),
    ],
)
async def test_history_refuses_forged_pin_revision_cohort_without_changing_storage(operation, damage):
    async with storage_case(provisioned=True) as case:
        prepared = await prepare_pin_case(case, revisions=(NEW_REVISION, SECOND_REVISION))
        receipt = await retained(prepared)
        await settle(prepared, {NEW_REVISION: "ready", SECOND_REVISION: "ready"})
        async with prepared.publisher.transaction():
            await _publish_native(prepared, receipt)
        detached = None
        if operation in {"detach", "reactivate"}:
            await history_transition(prepared, "withdraw")
        if operation == "reactivate":
            await history_transition(prepared, "withdraw", revision_id=SECOND_REVISION)
            detached = (await history_transition(prepared, "detach")).pin_heap
        admin = await case.connect()
        before = await _storage_catalog_snapshot(admin, case)
        preparation_before = await admin.fetchrow(f"SELECT * FROM {prepared.tables['preparation']}")
        async with prepared.publisher.transaction():
            cut = await capture_plan_release_history(
                prepared.publisher, case.schema, NEW_REVISION, detached_pin_heap=detached, **case.profile
            )
            assert cut.pin_revision_ids == (NEW_REVISION, SECOND_REVISION)
            changed_revision_ids = {
                "superset": (*cut.pin_revision_ids, THIRD_REVISION),
                "subset": (NEW_REVISION,),
                "noncanonical": (*cut.pin_revision_ids, "invalid'revision"),
            }[damage]
            damaged = replace(cut, pin_revision_ids=changed_revision_ids)
            expected = (NEW_REVISION, cut.publication.binding_set_digest) if cut.header["is_current"] else None
            with pytest.raises(ValueError, match="history_pin_revision_cohort_changed"):
                await transition_plan_release_history(
                    prepared.publisher,
                    case.schema,
                    damaged,
                    operation=operation,
                    publication=cut.publication,
                    expected_current=expected,
                    authority=history_authority(prepared, cut.publication, expected),
                    **case.profile,
                )
            assert (
                await capture_plan_release_history(
                    prepared.publisher, case.schema, NEW_REVISION, detached_pin_heap=detached, **case.profile
                )
                == cut
            )
        assert await _storage_catalog_snapshot(admin, case) == before
        assert await admin.fetchrow(f"SELECT * FROM {prepared.tables['preparation']}") == preparation_before
        assert await prepared.publisher.fetchval("SELECT current_user=session_user")


@pytest.mark.asyncio
async def test_legacy_history_reuses_actual_custody_without_preparation_scope():
    async with storage_case(provisioned=True) as case:
        admin = await case.connect()
        enrolled_oid = await admin.fetchval(
            f"SELECT DISTINCT tableoid::oid FROM {case.relation('plan_release_snapshot_pin')} WHERE serving_revision_id=$1",
            OLD_REVISION,
        )
        shared_before = await admin.fetch(f"SELECT * FROM {case.relation('ptg2_snapshot_pin')}")
        assert enrolled_oid is not None
        prepared = await published_history(case)
        async with prepared.publisher.transaction():
            original = await capture_plan_release_history(prepared.publisher, case.schema, OLD_REVISION, **case.profile)
            await prepared.publisher.execute("SET LOCAL TIME ZONE 'Pacific/Honolulu'")
            expected = (NEW_REVISION, prepared.scope.binding_inputs_digest)
            await transition_plan_release_history(
                prepared.publisher,
                case.schema,
                original,
                operation="withdraw",
                publication=original.publication,
                expected_current=expected,
                authority=history_authority(prepared, original.publication, expected),
                **case.profile,
            )
        detached = await history_transition(prepared, "detach", revision_id=OLD_REVISION)
        restored = await history_transition(
            prepared, "reactivate", revision_id=OLD_REVISION, detached_pin_heap=detached.pin_heap
        )
        assert restored.binding_heap == original.binding_heap and restored.pin_heap == original.pin_heap
        assert restored.pin_heap.oid == enrolled_oid
        assert await admin.fetch(f"SELECT * FROM {case.relation('ptg2_snapshot_pin')}") == shared_before
        assert restored.publication.serving_revision_id not in prepared.scope.revision_ids
        assert (
            await prepared.publisher.fetchval(
                f"SELECT serving_revision_id FROM {case.relation('plan_release_serving_revision')} WHERE is_current"
            )
            == OLD_REVISION
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["filenode", "transaction"])
async def test_retained_history_refuses_stale_heap_or_original_context(damage):
    async with storage_case(provisioned=True) as case:
        prepared = await published_history(case)
        async with prepared.publisher.transaction():
            cut = await capture_plan_release_history(prepared.publisher, case.schema, NEW_REVISION, **case.profile)
            damaged = (
                replace(cut, pin_heap=replace(cut.pin_heap, filenode=cut.pin_heap.filenode + 1))
                if damage == "filenode"
                else replace(cut, original_identity=(0, *cut.original_identity[1:]))
            )
            expected = (NEW_REVISION, cut.publication.binding_set_digest)
            with pytest.raises(ValueError, match="heap_receipt_changed|original_transaction_changed"):
                await transition_plan_release_history(
                    prepared.publisher,
                    case.schema,
                    damaged,
                    operation="withdraw",
                    publication=cut.publication,
                    expected_current=expected,
                    authority=history_authority(prepared, cut.publication, expected),
                    **case.profile,
                )
            assert await prepared.publisher.fetchval(
                f"SELECT is_current FROM {case.relation('plan_release_serving_revision')} WHERE serving_revision_id=$1",
                NEW_REVISION,
            )
        assert await prepared.publisher.fetchval("SELECT current_user=session_user")


@pytest.mark.asyncio
async def test_retained_pin_detach_waits_for_pinned_reader_without_losing_custody():
    async with storage_case(provisioned=True) as case:
        prepared = await published_history(case)
        reader = await case.connect(case.reader)
        pin = storage_relation(prepared.pin_heap.schema, prepared.pin_heap.name)
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await reader.fetchval(f"SELECT count(*) FROM {pin}") == 1
            await history_transition(prepared, "withdraw")
            header = case.relation("plan_release_serving_revision")
            headers_before = await prepared.publisher.fetch(f"SELECT * FROM {header} ORDER BY serving_revision_id")
            preparation_before = await prepared.publisher.fetchrow(f"SELECT * FROM {prepared.tables['preparation']}")
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with prepared.publisher.transaction():
                    failed_transaction = await prepared.publisher.fetchval("SELECT pg_current_xact_id()::text")
                    await history_transition(prepared, "detach")
            assert not prepared.publisher.is_in_transaction()
            assert await prepared.publisher.fetchval("SELECT current_user=session_user")
            assert (
                await prepared.publisher.fetch(f"SELECT * FROM {header} ORDER BY serving_revision_id") == headers_before
            )
            assert (
                await prepared.publisher.fetchrow(f"SELECT * FROM {prepared.tables['preparation']}")
                == preparation_before
            )
            assert await prepared.publisher.fetchval(
                "SELECT relispartition FROM pg_class WHERE oid=$1", prepared.pin_heap.oid
            )
            assert await reader.fetchval(f"SELECT count(*) FROM {pin}") == 1
        cut = await history_transition(prepared, "detach")
        assert cut.original_identity[2] != failed_transaction
        assert not await prepared.publisher.fetchval(
            "SELECT relispartition FROM pg_class WHERE oid=$1", cut.pin_heap.oid
        )
        await history_transition(prepared, "reactivate", detached_pin_heap=cut.pin_heap)
        assert await reader.fetchval(f"SELECT count(*) FROM {pin}") == 1


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
