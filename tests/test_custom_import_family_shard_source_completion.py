# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SOURCE aggregate evidence is complete, scoped and byte-exact before control writes."""

from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from process.custom_import import family_shard_control as control
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_shard_admission import cleanup_admission_output
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_common import SourceCommonContext, SourceCommonOutput
from process.custom_import.family_shard_facts import SourceFactLimits
from tests.test_custom_import_family_shard_admission import _build_audit, _inputs
from tests.test_custom_import_family_shard_common import _build_common, _lookup_fixture


def _common_receipt(common, context, allocated):
    proof = ShardOutputProof(context.layout, 17, bytes.fromhex(allocated.source.partition_sha256), 0)
    for batch in common.batches(maximum_rows=32, maximum_bytes=1024**2):
        for role, rows in batch.items():
            proof.add(role, rows)
    return proof.receipt()


@asynccontextmanager
async def _completion_case(tmp_path, monkeypatch, *, empty=False, source_binding=True):
    records = {"root_source": [[]], "detail_source": [[]]} if empty else None
    async with _inputs(tmp_path, monkeypatch, records, complete_scope=False, source_binding=source_binding) as (
        case,
        allocated,
        workers,
    ):
        limits = SourceAllocationLimits(SourceFactLimits(64 * 1024**2, 1024**2, 4096), 32, 1024**2)
        admission = _build_audit(tmp_path, case, allocated, workers, limits.spool, complete_scope=False)
        try:
            context = SourceCommonContext(
                case[1], FamilyBlockLayout(case[1].definition, dict(allocated.source.collection_slots)), allocated
            )
            _lookup_fixture(monkeypatch, context)
            with await _build_common(tmp_path, context, selected=(), limits=limits) as common:
                yield context, admission, common, _common_receipt(common, context, allocated), limits
        finally:
            cleanup_admission_output(admission)


@pytest.mark.parametrize("empty", [False, True])
async def test_complete_original_positions_and_common_bytes_give_only_provisional_source_totals(
    tmp_path, monkeypatch, empty
):
    async with _completion_case(tmp_path, monkeypatch, empty=empty) as (context, admission, common, receipt, limits):
        checked, counts = control._source_completion(context, admission, common, receipt, limits, None)
        assert checked == receipt
        assert counts == {1: [1, int(not empty)], 2: [1, int(not empty)]}
        assert sum(value[1] for value in counts.values()) == context.source.source.counts[0]


@pytest.mark.parametrize("damage", ["source", "allocation", "scope_type", "common_nullable", "common_bool", "receipt"])
async def test_missing_or_wrong_scope_and_receipt_cannot_freeze_source(tmp_path, monkeypatch, damage):
    async with _completion_case(tmp_path, monkeypatch, source_binding=damage != "common_nullable") as (
        context,
        admission,
        common,
        receipt,
        limits,
    ):
        match damage:
            case "source":
                admission = replace(admission, source_sha256=b"x" * 32)
            case "allocation":
                admission = replace(admission, allocation_sha256=b"x" * 32)
            case "scope_type":
                admission = replace(admission, complete_scope=0)
            case "common_nullable":
                proof = common.proof_inputs
                assert proof.pop("source_binding_sha256") is None
                common = SourceCommonOutput(common._replay, proof)
            case "common_bool":
                proof = common.proof_inputs | {"complete_scope": 0}
                common = SourceCommonOutput(common._replay, proof)
            case "receipt":
                receipt = receipt | {"result_sha256": b"x" * 32}
        with pytest.raises(FamilyBlockUnavailable, match="SOURCE"):
            control._source_completion(context, admission, common, receipt, limits, None)


async def test_job_cancellation_precedes_any_source_control_write(tmp_path, monkeypatch):
    async with _completion_case(tmp_path, monkeypatch) as (context, admission, common, receipt, limits):
        failure = InterruptedError("synthetic SOURCE completion stopped")

        def check():
            raise failure

        with pytest.raises(InterruptedError) as caught:
            control._source_completion(context, admission, common, receipt, limits, check)
        assert caught.value is failure
