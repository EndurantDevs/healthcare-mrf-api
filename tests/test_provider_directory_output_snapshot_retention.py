# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit orchestration checks; genuine role/native proofs are qualified separately."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_retention as retention
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS, encoded


def relation(target, name, oid):
    return custody.NativeOutputRelation(
        "source_schema",
        15,
        target,
        name,
        oid,
        17,
        (encoded({"name": "key", "type_oid": 25}),),
        (encoded({"type": "p", "oid": oid + 1}),),
        (encoded({"oid": oid + 2, "name": name + "_pk"}),),
        encoded({"kind": "r"}),
        encoded({"table": [[17, 17, "SELECT", False]], "columns": []}),
        None,
    )


def preparations():
    return tuple(
        custody.PreparedOutputReceipt(
            1,
            "b" * 64,
            relation(target, target, 101 + 10 * index),
            relation(target, "isolated_" + str(index), 201 + 10 * index),
        )
        for index, target in enumerate(OUTPUT_ROOTS[:2])
    )


@pytest.fixture
def orchestration(monkeypatch):
    prepared = preparations()
    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    locks = [{"oid": proof.oid} for receipt in prepared for proof in (receipt.incumbent, receipt.successor)]
    connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=[locks, []]),
        execute=AsyncMock(),
        transaction=Mock(return_value=transaction),
        is_closed=lambda: False,
    )
    capability = SimpleNamespace(
        connection=connection, policy=SimpleNamespace(schema="source_schema"), operation_digest="b" * 64
    )
    monkeypatch.setattr(custody, "recheck_output_cut", AsyncMock())
    monkeypatch.setattr(custody, "publish_output_read_access", AsyncMock())
    monkeypatch.setattr(custody, "recheck_output_restore", AsyncMock())

    async def captured(_capability, receipt, name):
        return replace(receipt.incumbent, relation=name), replace(receipt.successor, relation=receipt.incumbent.target)

    monkeypatch.setattr(custody, "capture_retained_output", AsyncMock(side_effect=captured))
    return capability, prepared, transaction, locks


@pytest.mark.parametrize("oid", [True, 0, -1, "12", 2**32])
def test_retained_names_require_exact_native_oid_range(oid):
    with pytest.raises(ValueError, match="oid_invalid"):
        retention.retained_name(oid)


def stored_receipt():
    receipt = preparations()[0]
    name = retention.retained_name(receipt.incumbent.oid)
    return retention.RetainedOutputReceipt(
        1,
        receipt.operation_digest,
        receipt.incumbent.schema,
        receipt.incumbent.target,
        name,
        replace(receipt.incumbent, relation=name),
        replace(receipt.successor, relation=receipt.incumbent.target),
    )


def test_postcut_receipt_roundtrip_keeps_native_index_identity_and_legacy_descriptor():
    receipt = stored_receipt()
    assert retention.RetainedOutputReceipt.from_payload(receipt.to_payload()) == receipt
    assert receipt.old_oid == 101 and receipt.new_oid == 201
    assert receipt.incumbent.descriptor is None
    assert receipt.incumbent.indexes == preparations()[0].incumbent.indexes


@pytest.mark.parametrize("change", ["extra", "schema", "target", "retained_name", "same_oid"])
def test_stored_coordinate_substitution_refuses(change):
    payload = stored_receipt().to_payload()
    if change == "extra":
        payload["skip_checks"] = True
    elif change == "same_oid":
        payload["successor"]["oid"] = payload["incumbent"]["oid"]
    else:
        payload[change] = "changed"
    with pytest.raises(ValueError, match="receipt_invalid"):
        retention.RetainedOutputReceipt.from_payload(payload)


@pytest.mark.asyncio
async def test_authority_refusal_precedes_lock_or_rename_queries(orchestration, monkeypatch):
    capability, prepared, transaction, _ = orchestration
    monkeypatch.setattr(custody, "recheck_output_cut", AsyncMock(side_effect=RuntimeError("denied")))
    with pytest.raises(RuntimeError, match="denied"):
        await retention.retain_prepared_outputs(capability, prepared)
    capability.connection.fetch.assert_not_awaited()
    capability.connection.execute.assert_not_awaited()
    transaction.start.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing_lock", "occupied_name"])
async def test_incomplete_cut_refuses_before_first_rename(orchestration, change):
    capability, prepared, transaction, locks = orchestration
    capability.connection.fetch.side_effect = [locks[:-1]] if change == "missing_lock" else [locks, [{"name": "used"}]]
    with pytest.raises(ValueError, match="complete_live_locks_required|retained_name_occupied"):
        await retention.retain_prepared_outputs(capability, prepared)
    capability.connection.execute.assert_not_awaited()
    transaction.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_forward_rotation_retains_every_old_table_before_installing_successors(orchestration):
    capability, prepared, transaction, _ = orchestration
    receipts = await retention.retain_prepared_outputs(capability, prepared)
    expected = [
        f'ALTER TABLE "source_schema"."{receipt.incumbent.relation}" RENAME TO "{retention.retained_name(receipt.incumbent.oid)}"'
        for receipt in prepared
    ] + [
        f'ALTER TABLE "source_schema"."{receipt.successor.relation}" RENAME TO "{receipt.incumbent.target}"'
        for receipt in prepared
    ]
    assert [call.args[0] for call in capability.connection.execute.await_args_list] == expected
    assert [receipt.old_oid for receipt in receipts] == [101, 111]
    assert [receipt.new_oid for receipt in receipts] == [201, 211]
    transaction.commit.assert_awaited_once()
    transaction.rollback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("postcut rejected"), asyncio.CancelledError()])
async def test_late_capture_failure_rolls_back_the_entire_physical_savepoint(orchestration, monkeypatch, failure):
    capability, prepared, transaction, _ = orchestration
    monkeypatch.setattr(custody, "capture_retained_output", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)):
        await retention.retain_prepared_outputs(capability, prepared)
    assert capability.connection.execute.await_count == 4
    transaction.rollback.assert_awaited_once()
    transaction.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_uses_stored_forward_digest_and_retains_replaced_successor(orchestration, monkeypatch):
    capability, _, transaction, _ = orchestration
    receipt = stored_receipt()
    capability.operation_digest = "c" * 64
    capability.connection.fetch.side_effect = [[{"oid": 101}, {"oid": 201}], []]

    async def restored(_capability, prepared, name):
        return replace(prepared.successor, relation=name), replace(
            prepared.incumbent, relation=prepared.incumbent.target
        )

    monkeypatch.setattr(custody, "capture_restored_output", AsyncMock(side_effect=restored))
    (inverse,) = await retention.restore_retained_outputs(capability, (receipt,))
    checked = custody.recheck_output_restore.await_args.args
    assert checked[1][0].operation_digest == receipt.operation_digest
    assert checked[2] == {receipt.target: receipt.retained_name}
    assert inverse.operation_digest == "c" * 64 and inverse.old_oid == 201 and inverse.new_oid == 101
    assert inverse.successor.descriptor is None and inverse.successor.indexes == receipt.incumbent.indexes
    assert retention.RetainedOutputReceipt.from_payload(inverse.to_payload()) == inverse
    transaction.commit.assert_awaited_once()
