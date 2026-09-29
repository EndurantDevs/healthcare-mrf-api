# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Caller-owned Doctors publication, exact cleanup, and immutable acknowledgment proofs."""

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import cms_doctors_preparation as preparation
from process.entity_address_cutover_contract import _ServingRelationLockTimeout
from tests.cms_doctors_preparation_postgres_support import (
    append_common_receipt,
    authority,
    doctors_database,
    doctors_snapshot,
    native,
    pending_publisher_locks,
    stage_family,
    stage_oids,
)


@pytest.mark.asyncio
async def test_preparation_finishes_three_stages_without_publishing(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            assert prepared.incumbent_authority == incumbent
            assert len(prepared.stage_oids) == 3
            assert len(prepared.relation_overrides) == 3
            assert not prepared.committed and not prepared.metrics["published"]
            assert ctx["context"]["publication_state"] == "prepared"
            assert await authority(fixture) == incumbent
            async with fixture.database.transaction() as session:
                for model, (_target, stage, oid) in zip(preparation._models(), prepared.stage_oids, strict=True):
                    await preparation._assert_stage(session, fixture.schema, stage, oid, logged=True)
                    await preparation._assert_stage_indexes(
                        session, fixture.schema, native.make_class(model, ctx["import_date"])
                    )
                    assert (
                        await session.scalar(text("SELECT reltuples FROM pg_class WHERE oid=:oid"), {"oid": oid}) == 1
                    )
        assert await stage_oids(fixture, ctx) == ()
        assert await authority(fixture) == incumbent


@pytest.mark.asyncio
async def test_outer_rollback_keeps_all_three_stages_retryable(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            with pytest.raises(RuntimeError, match="requires_transaction"):
                await preparation.apply_prepared_cms_doctors_generation(prepared)
            failed_receipts = []
            with pytest.raises(RuntimeError, match="late failure"):
                await _apply_then_fail(fixture, prepared, failed_receipts)
            assert await authority(fixture) == incumbent
            assert await stage_oids(fixture, ctx) == prepared.stage_oids
            assert not prepared.committed and not prepared.metrics["published"]
            with pytest.raises(RuntimeError, match="commit_unproved"):
                await prepared.mark_committed(**failed_receipts[0])
            async with fixture.database.transaction() as session:
                await preparation.apply_prepared_cms_doctors_generation(prepared)
                receipt = await append_common_receipt(session, fixture, fixture.initial)
            await prepared.mark_committed(**receipt)
            assert prepared.committed and prepared.metrics["published"]
        assert await stage_oids(fixture, ctx) == ()
        assert (await authority(fixture)).relation_oids == tuple(oid for _, _, oid in prepared.stage_oids)


async def _apply_then_fail(fixture, prepared, failed_receipts):
    """Fail after all swaps and receipt creation, before the outer transaction commits."""
    async with fixture.database.transaction() as session:
        applied = await preparation.apply_prepared_cms_doctors_generation(prepared)
        assert applied.local_generation == prepared.incumbent_authority.local_generation + 1
        receipt = await append_common_receipt(session, fixture, fixture.initial)
        failed_receipts.append(receipt)
        with pytest.raises(RuntimeError, match="commit_pending"):
            await prepared.mark_committed(**receipt)
        raise RuntimeError("late failure")


@pytest.mark.asyncio
async def test_common_guard_rejects_independent_doctors_publication(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            with pytest.raises(DBAPIError, match="cms_serving_fresh_receipt_required"):
                async with fixture.database.transaction():
                    await preparation.apply_prepared_cms_doctors_generation(prepared)
            assert await authority(fixture) == incumbent
            assert await stage_oids(fixture, ctx) == prepared.stage_oids
            with pytest.raises(RuntimeError, match="receipt_changed"):
                await prepared.mark_committed(**fixture.initial)


@pytest.mark.asyncio
async def test_historical_proof_consumes_after_a_later_doctors_publication(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        first_ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(first_ctx) as first:
            async with fixture.database.transaction() as session:
                await preparation.apply_prepared_cms_doctors_generation(first)
                first_receipt = await append_common_receipt(session, fixture, fixture.initial)
            assert not first.committed
            second_ctx = await stage_family(fixture.database, fixture.schema)
            async with preparation.prepare_cms_doctors_generation(second_ctx) as second:
                async with fixture.database.transaction() as session:
                    await preparation.apply_prepared_cms_doctors_generation(second)
                    second_receipt = await append_common_receipt(session, fixture, first_receipt)
                await second.mark_committed(**second_receipt)
            assert (await authority(fixture)).local_generation == first.native_receipt.local_generation + 1
            await first.mark_committed(**first_receipt)
            assert first.committed


@pytest.mark.asyncio
async def test_cleanup_preserves_foreign_replacement_with_reused_stage_name(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            _target, stage, original_oid = prepared.stage_oids[0]
            async with fixture.database.transaction() as session:
                await session.execute(text(f'DROP TABLE "{fixture.schema}"."{stage}"'))
                await session.execute(text(f'CREATE TABLE "{fixture.schema}"."{stage}" (marker int)'))
            with pytest.raises(RuntimeError, match="stage_changed"):
                async with fixture.database.transaction():
                    await preparation.apply_prepared_cms_doctors_generation(prepared)
        retained_stages = await stage_oids(fixture, ctx)
        assert len(retained_stages) == 1
        assert retained_stages[0][1] == stage and retained_stages[0][2] != original_oid


@pytest.mark.asyncio
async def test_cancellation_during_preparation_cleans_only_owned_stages(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)

        async def cancel_before_finalization(_prepared):
            raise asyncio.CancelledError

        monkeypatch.setattr(preparation, "_finalize_stages", cancel_before_finalization)
        with pytest.raises(asyncio.CancelledError):
            async with preparation.prepare_cms_doctors_generation(ctx):
                pytest.fail("cancelled preparation must not yield")
        assert await stage_oids(fixture, ctx) == ()
        assert await authority(fixture) == incumbent


async def _apply_until_cancelled(ctx, fixture, is_applied):
    """Wait at a real uncommitted family swap until the test cancels its owner."""
    async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
        async with fixture.database.transaction():
            await preparation.apply_prepared_cms_doctors_generation(prepared)
            is_applied.set()
            await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_cancellation_rolls_back_active_swap_and_cleans_owned_stages(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        is_applied = asyncio.Event()
        task = asyncio.create_task(_apply_until_cancelled(ctx, fixture, is_applied))
        try:
            await asyncio.wait_for(is_applied.wait(), timeout=5)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert await stage_oids(fixture, ctx) == ()
        assert await authority(fixture) == incumbent


@pytest.mark.asyncio
async def test_reader_lock_conflict_fails_without_partial_apply(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
            async with fixture.engine.begin() as reader:
                await reader.execute(text(f'LOCK TABLE "{fixture.schema}".cms_doctor_education IN ACCESS SHARE MODE'))
                await _assert_lock_conflict(fixture, prepared)
            assert await authority(fixture) == incumbent
            assert await stage_oids(fixture, ctx) == prepared.stage_oids
        assert await stage_oids(fixture, ctx) == ()


async def _assert_lock_conflict(fixture, prepared):
    """The native lock policy must abort instead of committing an earlier family swap."""
    with pytest.raises(_ServingRelationLockTimeout, match="serving_relation_lock_timeout"):
        async with fixture.database.transaction():
            await preparation.apply_prepared_cms_doctors_generation(prepared)


@pytest.mark.asyncio
async def test_missing_stage_cleans_remaining_owned_family(monkeypatch):
    async with doctors_database(monkeypatch) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        stages = await stage_oids(fixture, ctx)
        async with fixture.database.transaction() as session:
            await session.execute(text(f'DROP TABLE "{fixture.schema}"."{stages[0][1]}"'))
        with pytest.raises(RuntimeError, match="family_incomplete"):
            async with preparation.prepare_cms_doctors_generation(ctx):
                pytest.fail("incomplete family must not yield")
        assert await stage_oids(fixture, ctx) == ()
        assert await authority(fixture) == incumbent


@pytest.mark.asyncio
async def test_legacy_publication_without_cms_keeps_native_authority(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        for model in preparation._models():
            await native._create_stage_indexes(native.make_class(model, ctx["import_date"]), fixture.schema)
        result = await native._publish_cms_doctors_stage(
            native.make_class(native.DoctorClinicianAddress, ctx["import_date"]), fixture.schema, ctx["import_date"]
        )
        assert result == await authority(fixture)
        assert result.local_generation == incumbent.local_generation + 1
        assert await stage_oids(fixture, ctx) == ()


@pytest.mark.asyncio
async def test_ordinary_publisher_keeps_late_readers_on_incumbent(monkeypatch):
    """A stalled reader exhausts bounded owner attempts without starving later reads."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        ctx = await stage_family(fixture.database, fixture.schema)
        incumbent = await authority(fixture)
        stages = await stage_oids(fixture, ctx)
        for model in preparation._models():
            await native._create_stage_indexes(native.make_class(model, ctx["import_date"]), fixture.schema)
        transaction_ids = []
        original_apply = native._apply_cms_doctors_stage

        async def observed_apply(*args):
            transaction_ids.append(await fixture.database.scalar("SELECT txid_current()"))
            return await original_apply(*args)

        monkeypatch.setattr(native, "_apply_cms_doctors_stage", observed_apply)
        entered, release = asyncio.Event(), asyncio.Event()
        reader = asyncio.create_task(doctors_snapshot(fixture, entered=entered, release=release))
        publisher = None
        try:
            await asyncio.wait_for(entered.wait(), 3)
            publisher = asyncio.create_task(
                native._publish_cms_doctors_stage(
                    native.make_class(native.DoctorClinicianAddress, ctx["import_date"]),
                    fixture.schema,
                    ctx["import_date"],
                )
            )
            pending = await pending_publisher_locks(fixture, publisher)
            assert await doctors_snapshot(fixture) == (("incumbent",) * 3, incumbent)
            assert pending
            with pytest.raises(_ServingRelationLockTimeout, match="serving_relation_lock_timeout"):
                await publisher
            assert len(transaction_ids) > 1 and len(set(transaction_ids)) == len(transaction_ids)
            assert await authority(fixture) == incumbent and await stage_oids(fixture, ctx) == stages
            release.set()
            assert await reader == (("incumbent",) * 3, incumbent)
            published = await native._publish_cms_doctors_stage(
                native.make_class(native.DoctorClinicianAddress, ctx["import_date"]), fixture.schema, ctx["import_date"]
            )
            assert published.local_generation == incumbent.local_generation + 1
            assert published.relation_oids == tuple(oid for _, _, oid in stages)
            assert await doctors_snapshot(fixture) == (("prepared",) * 3, published)
            assert await stage_oids(fixture, ctx) == ()
        finally:
            release.set()
            tasks = [task for task in (reader, publisher) if task is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
