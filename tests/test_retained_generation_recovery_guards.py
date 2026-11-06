# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recovery preserves captured identities and refuses a changed serving tuple."""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

from process import entity_address_result_generation as generations
from process import entity_address_serving_receipt as address_receipts
from process import provider_directory_artifact_read_access as read_access
from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_serving_recovery as recovery
from process.provider_directory_output_snapshot_catalog import encoded
from tests.test_provider_directory_serving_recovery import serving_states

PIN = dict(
    source_id="source", endpoint_id="endpoint", dataset_id="dataset", dataset_hash="a" * 64, parent_run_id="root"
)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_legacy_source_fence_rechecks_the_current_alias_even_without_replacement_binding(monkeypatch, changed):
    current = {**PIN, "dataset_id": "other"} if changed else PIN
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=encoded(current)))
    database = object()
    directory = SimpleNamespace(db=database, _schema=lambda: "sample")
    fence = AsyncMock()
    monkeypatch.setattr(artifacts, "lock_address_source_fence", fence)
    monkeypatch.setattr(recovery.receipts, "ordinary", AsyncMock(return_value=(object(), driver, {})))
    history_by_phase = {"overlay": {"document": {"preparation": {}}}}
    if changed:
        with pytest.raises(ValueError, match="current_source_changed"):
            await recovery._source_fence(directory, PIN, history=history_by_phase)
    else:
        await recovery._source_fence(directory, PIN, history=history_by_phase)
    fence.assert_awaited_once_with(
        directory,
        database,
        "sample",
        {
            "partial_provider_directory_dataset_id": "dataset",
            "partial_provider_directory_source_ids": ["source"],
            "partial_provider_directory_run_id": "root",
        },
    )
    assert driver.fetchval.await_args.args[1] == "source"


@pytest.mark.asyncio
async def test_recovery_with_no_extra_dependencies_does_not_lock_unrelated_relations(monkeypatch):
    baseline, _, _ = serving_states()
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[]), execute=AsyncMock())
    database = SimpleNamespace(scalar=AsyncMock())
    directory = SimpleNamespace(db=database, _schema=lambda: "sample")
    monkeypatch.setattr(recovery, "_source_fence", AsyncMock())
    monkeypatch.setattr(recovery.receipts, "ordinary", AsyncMock(return_value=(object(), driver, {})))
    lock = AsyncMock()
    monkeypatch.setattr(recovery.receipts, "lock", lock)
    assert await recovery._lock_recovery(directory, PIN, baseline, baseline) == recovery.receipts.ROOTS
    driver.execute.assert_not_awaited()
    lock.assert_awaited_once_with(database, "sample", recovery.receipts.ROOTS, mode="SHARE")


@pytest.mark.asyncio
async def test_unchanged_root_oids_do_not_prepare_or_borrow_retained_heaps(monkeypatch):
    baseline, overlay, _ = serving_states()
    proofs_by_root = {
        root: SimpleNamespace(
            receipt_json=encoded(overlay["roots"][root]), elapsed_seconds=0, heap_bytes=1, total_bytes=2
        )
        for root in recovery.receipts.ROOTS
    }
    replaced = recovery.receipts.ROOTS[0]
    retained = copy.deepcopy(baseline["roots"][replaced])
    retained_name = recovery.receipts.retained_name(retained["native"]["oid"])
    retained["native"]["relname"] = retained_name
    retained["native"]["indexes"][0]["name"] = recovery.receipts.retained_name(102, index=True)
    retained["native"]["constraints"][0]["name"] = recovery.receipts.retained_name(102, index=True)
    retained_proof = SimpleNamespace(receipt_json=encoded(retained), elapsed_seconds=0, heap_bytes=1, total_bytes=2)
    prepare = AsyncMock(
        side_effect=lambda _db, _schema, name: retained_proof if name == retained_name else proofs_by_root[name]
    )
    monkeypatch.setattr(recovery.receipts, "prepare_relation", prepare)
    directory = SimpleNamespace(db=object(), _schema=lambda: "sample")
    selected, retained_by_root, preparation = await recovery._prepare_inverse_receipts(
        directory, baseline, overlay, None
    )
    assert selected == proofs_by_root and retained_by_root == {replaced: retained_proof}
    assert prepare.await_args_list == [
        call(directory.db, "sample", replaced),
        call(directory.db, "sample", retained_name),
        call(directory.db, "sample", recovery.receipts.ROOTS[1]),
    ]
    assert set(preparation["digests"]) == {*recovery.receipts.ROOTS, "retained_" + replaced}
    assert all(metrics["rows"] == 2 for metrics in preparation["digests"].values())


@pytest.mark.asyncio
async def test_inverse_preserves_unchanged_heaps_and_handles_absent_optional_callbacks(monkeypatch):
    baseline, overlay, _ = serving_states()
    after = copy.deepcopy(baseline)
    after["metadata"]["address"]["local_generation"] += 1
    database = SimpleNamespace(status=AsyncMock())
    directory = SimpleNamespace(db=database, _schema=lambda: "sample")
    retain, restore = AsyncMock(), AsyncMock()
    monkeypatch.setattr(recovery.receipts, "retain", retain)
    monkeypatch.setattr(recovery.receipts, "restore", restore)
    monkeypatch.setattr(recovery.receipts, "prepared_state", AsyncMock(return_value=after))
    monkeypatch.setattr(recovery, "_restore_geo", AsyncMock())
    monkeypatch.setattr(generations, "publish_local_entity_address_generation", AsyncMock())
    monkeypatch.setattr(
        address_receipts,
        "ordinary_address_receipt_callbacks",
        lambda *_: SimpleNamespace(before_cutover=None, after_publish=None),
    )
    append = AsyncMock()
    monkeypatch.setattr(recovery, "_append", append)
    history_by_phase = {
        "overlay": {"document": {"before": baseline, "after": overlay, "source": PIN, "preparation": {}}}
    }
    proofs_by_root = {root: object() for root in recovery.receipts.ROOTS}
    restored_proof = object()
    replaced = recovery.receipts.ROOTS[0]
    restored_state = await recovery._inverse(
        directory,
        history_by_phase,
        recovery.publication_generation(PIN),
        overlay,
        proofs_by_root,
        {replaced: restored_proof},
        {"elapsed_seconds": 0},
    )
    assert restored_state is after
    retain.assert_awaited_once_with(database, "sample", overlay["roots"][replaced])
    restore.assert_awaited_once_with(database, "sample", baseline["roots"][replaced], prepared=restored_proof)
    generations.publish_local_entity_address_generation.assert_awaited_once_with(database, schema_name="sample")
    assert append.await_args.args[3] == "recovered"
    assert append.await_args.args[5:7] == (overlay, after)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_after_lock", [False, True])
async def test_prepared_address_is_rechecked_without_recapturing_or_rebuilding(monkeypatch, changed_after_lock):
    _, current, _ = serving_states()
    database = SimpleNamespace(status=AsyncMock())
    directory = recovery.importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "db", database)
    stage = object()
    proofs_by_root = {root: object() for root in recovery.receipts.ROOTS}
    retained_access = object()
    context_by_field = {
        recovery.GENERATION_PARAM: recovery.publication_generation(PIN),
        "partial_provider_directory_source_ids": ["source"],
        "partial_provider_directory_dataset_id": "dataset",
        "partial_provider_directory_run_id": "root",
        "serving_recovery_prepared_address": {"proofs": proofs_by_root, "read_access": retained_access, "stage": stage},
        "serving_recovery_wall": SimpleNamespace(
            when=lambda: asyncio.get_running_loop().time() + 20, reschedule=Mock()
        ),
    }
    history_by_phase = {"overlay": {"document": {"source": PIN, "after": current}}}
    monkeypatch.setattr(recovery, "_prepare_coupled_address", AsyncMock(return_value=None))
    monkeypatch.setattr(recovery, "_load_history", AsyncMock(return_value=history_by_phase))
    monkeypatch.setattr(recovery, "_source_fence", AsyncMock())
    monkeypatch.setattr(recovery.receipts, "lock", AsyncMock())
    monkeypatch.setattr(recovery.receipts, "lock_metadata", AsyncMock())
    monkeypatch.setattr(
        recovery.receipts, "ordinary", AsyncMock(return_value=(object(), object(), {"owner": "ordinary"}))
    )
    changed = copy.deepcopy(current)
    changed["metadata"]["alias"]["generation"] += 1
    monkeypatch.setattr(
        recovery.receipts,
        "prepared_state",
        AsyncMock(side_effect=[current, changed if changed_after_lock else current]),
    )
    prepare, access, recheck = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(recovery.receipts, "prepare_relation", prepare)
    monkeypatch.setattr(read_access, "prepare_ordinary_read_access", access)
    monkeypatch.setattr(recovery.receipts, "recheck_prepared", recheck)
    swaps = [
        SimpleNamespace(
            live_cls=SimpleNamespace(__main_table__=recovery.receipts.ROOTS[1]),
            stage_cls=SimpleNamespace(__tablename__="address_next"),
        )
    ]
    if changed_after_lock:
        with pytest.raises(ValueError, match="current_serving_changed"):
            await recovery.prepare_address(database, "sample", swaps, context_by_field)
    else:
        await recovery.prepare_address(database, "sample", swaps, context_by_field)
        assert context_by_field["serving_recovery_read_access"] is retained_access
        assert context_by_field["serving_recovery_stage"] is stage
    recheck.assert_awaited_once_with(database, stage, "address_next")
    prepare.assert_not_awaited()
    access.assert_not_awaited()
