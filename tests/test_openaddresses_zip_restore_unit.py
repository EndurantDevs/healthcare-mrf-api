# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""ZIP restoration readiness, accounting, and bounded worker cleanup."""

import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

openaddresses = importlib.import_module("process.openaddresses")


@pytest.fixture
def zip_restore_db(monkeypatch):
    database = SimpleNamespace(
        scalar=AsyncMock(return_value=True),
        all=AsyncMock(return_value=[]),
        status=AsyncMock(return_value="CREATE INDEX"),
    )
    monkeypatch.setattr(openaddresses, "db", database)
    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_ZIP_RESTORE_REQUIRED", "false")
    return database


@pytest.mark.parametrize(
    "missing_relation",
    ["sample.recovery", "tiger.zcta5", "sample.geo_zip_lookup"],
)
async def test_zip_restore_skips_unavailable_relations(zip_restore_db, missing_relation, caplog):
    zip_restore_db.scalar.side_effect = lambda _statement, qualified_name: qualified_name != missing_relation

    statistics = await openaddresses.restore_openaddresses_zips(
        schema="sample", stage_table="stage", recovery_table="recovery", concurrency=2
    )

    assert statistics == openaddresses.OpenAddressesZipRestoreStats()
    zip_restore_db.all.assert_not_awaited()
    zip_restore_db.status.assert_not_awaited()
    assert zip_restore_db.scalar.await_args.kwargs == {"qualified_name": missing_relation}
    if missing_relation != "sample.recovery":
        assert "Skipping OpenAddresses ZIP restore" in caplog.text


@pytest.mark.parametrize("missing_relation", ["tiger.zcta5", "sample.geo_zip_lookup"])
async def test_required_zip_restore_rejects_missing_reference(monkeypatch, zip_restore_db, missing_relation):
    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_ZIP_RESTORE_REQUIRED", "true")
    zip_restore_db.scalar.side_effect = lambda _statement, qualified_name: qualified_name != missing_relation

    with pytest.raises(RuntimeError, match="ZIP restore requires"):
        await openaddresses.restore_openaddresses_zips(
            schema="sample", stage_table="stage", recovery_table="recovery", concurrency=2
        )

    zip_restore_db.status.assert_not_awaited()
    zip_restore_db.all.assert_not_awaited()


async def test_zip_restore_empty_plan_creates_indexes_without_inserting(zip_restore_db):
    statistics = await openaddresses.restore_openaddresses_zips(
        schema="sample", stage_table="stage", recovery_table="recovery", concurrency=2
    )

    assert statistics == openaddresses.OpenAddressesZipRestoreStats()
    assert zip_restore_db.status.await_count == 2
    statements = [call.args[0] for call in zip_restore_db.status.await_args_list]
    assert all(statement.startswith("CREATE INDEX IF NOT EXISTS") for statement in statements)
    assert "(restore_bucket)" in statements[0]
    assert "(state_code, restore_bucket)" in statements[1]
    count_query = zip_restore_db.all.await_args.args[0]
    assert 'FROM "sample"."recovery"' in count_query
    assert "GROUP BY restore_bucket" in count_query


@pytest.mark.parametrize(("concurrency", "expected_peak"), [(0, 1), (2, 2), (99, 3)])
async def test_zip_restore_bounds_workers_and_accounts_for_unmatched_candidates(
    monkeypatch, zip_restore_db, concurrency, expected_peak
):
    zip_restore_db.all.return_value = [
        {"restore_bucket": 7, "candidates": 4},
        SimpleNamespace(_mapping={"restore_bucket": 1, "candidates": 2}),
        {"restore_bucket": 4, "candidates": 3},
    ]
    active_buckets = set()
    parallelism_samples = []
    inserted_by_bucket = {1: 1, 4: 2, 7: 2}
    progress_events = []

    async def execute_bucket(statement, **parameters):
        if not parameters:
            assert statement.startswith("CREATE INDEX IF NOT EXISTS")
            return "CREATE INDEX"
        bucket = parameters["restore_bucket"]
        assert 'FROM "sample"."recovery"' in statement
        assert 'INSERT INTO "sample"."stage"' in statement
        assert "HAVING count(DISTINCT restored_zip5) = 1" in statement
        active_buckets.add(bucket)
        parallelism_samples.append(len(active_buckets))
        await asyncio.sleep(0)
        active_buckets.remove(bucket)
        return f"INSERT 0 {inserted_by_bucket[bucket]}"

    zip_restore_db.status.side_effect = execute_bucket
    monkeypatch.setattr(openaddresses, "enqueue_live_progress", lambda **event: progress_events.append(event))

    statistics = await openaddresses.restore_openaddresses_zips(
        schema="sample",
        stage_table="stage",
        recovery_table="recovery",
        concurrency=concurrency,
        run_id="synthetic-run",
    )

    assert statistics == openaddresses.OpenAddressesZipRestoreStats(candidates=9, restored=5, discarded=4, shards=3)
    assert max(parallelism_samples) == expected_peak
    assert not active_buckets
    assert [event["done"] for event in progress_events] == [0, 1, 2, 3]
    assert all(event["run_id"] == "synthetic-run" for event in progress_events)
    assert progress_events[-1]["pct"] == 100.0
    assert progress_events[-1]["message"] == "3/3 ZIP restore shards; 5/9 candidates restored"
    assert {call.kwargs["restore_bucket"] for call in zip_restore_db.status.await_args_list if call.kwargs} == {1, 4, 7}


async def test_zip_restore_failure_cancels_and_awaits_sibling(monkeypatch, zip_restore_db):
    zip_restore_db.all.return_value = [
        {"restore_bucket": 1, "candidates": 2},
        {"restore_bucket": 2, "candidates": 3},
    ]
    sibling_started = asyncio.Event()
    sibling_cancelled = asyncio.Event()
    progress_events = []

    async def fail_one_bucket(_statement, **parameters):
        if not parameters:
            return "CREATE INDEX"
        if parameters["restore_bucket"] == 1:
            await sibling_started.wait()
            raise RuntimeError("synthetic insert failure")
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    zip_restore_db.status.side_effect = fail_one_bucket
    monkeypatch.setattr(openaddresses, "enqueue_live_progress", lambda **event: progress_events.append(event))

    async with asyncio.timeout(2):
        with pytest.raises(RuntimeError, match="synthetic insert failure"):
            await openaddresses.restore_openaddresses_zips(
                schema="sample", stage_table="stage", recovery_table="recovery", concurrency=2
            )

    assert sibling_cancelled.is_set()
    assert [event["done"] for event in progress_events] == [0]
    assert "run_id" not in progress_events[0]
