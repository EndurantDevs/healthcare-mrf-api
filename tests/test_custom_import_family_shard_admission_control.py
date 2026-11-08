# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Admission closure compares original decisions and complete event COPY bytes."""

from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from process.custom_import import family_shard_admission_control as control
from process.custom_import import family_shard_events as events
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactLimits
from tests.test_custom_import_family_shard_events import _allocate, _case, _native_boundary
from tests.test_custom_import_family_shard_plan import _definition
from tests.test_custom_import_family_shard_source_plan import _child, _root


@asynccontextmanager
async def _completion(tmp_path, monkeypatch, rows=None, *, definition=None):
    async with _case(tmp_path, monkeypatch, rows, definition=definition) as native:
        _native_boundary(monkeypatch, native)
        descriptor = await _allocate(tmp_path, native)
        try:
            limits = SourceAllocationLimits(SourceFactLimits(64 * 1024**2, 1024**2, 512), 1, 1024**2)
            context = SourceCommonContext(native.request, native.layout, native.source)
            proof = ShardOutputProof(native.layout, 17, bytes.fromhex(native.source.source.partition_sha256), 0)
            for batch in events.admission_event_batches(descriptor, maximum_rows=1, maximum_bytes=1024**2):
                for role, values in batch.items():
                    proof.add(role, values)
            yield context, native.admission, descriptor, proof.receipt(), limits
        finally:
            events.cleanup_admission_events(descriptor)


@pytest.mark.parametrize("kind", ["valid", "empty", "candidate_error", "snapshot_incomplete"])
async def test_aggregate_original_admission_keeps_empty_and_rejected_inputs(tmp_path, monkeypatch, kind):
    rows = {
        "valid": {"root_source": [[_root()]], "detail_source": [[_child()]]},
        "empty": {"root_source": [[]], "detail_source": [[]]},
        "candidate_error": {"root_source": [[_root(npi=None)]], "detail_source": [[]]},
        "snapshot_incomplete": {"root_source": [[_root()]], "detail_source": [[_child()]]},
    }[kind]
    definition = _definition("snapshot" if kind == "snapshot_incomplete" else "upsert")
    async with _completion(tmp_path, monkeypatch, rows, definition=definition) as arguments:
        checked, errors, counts = control._admission_completion(*arguments, None)
        assert checked == arguments[3]
        assert errors == int(kind in {"candidate_error", "snapshot_incomplete"})
        assert counts == {
            1: [1, int(kind != "empty")],
            2: [1, int(kind in {"valid", "snapshot_incomplete"})],
        }


@pytest.mark.parametrize("damage", ["source", "allocated_scope", "scope_type", "receipt", "late_sha", "counts"])
async def test_wrong_scope_or_late_eof_cannot_complete_admission(tmp_path, monkeypatch, damage):
    async with _completion(tmp_path, monkeypatch) as (context, admission, descriptor, receipt, limits):
        match damage:
            case "source":
                admission = replace(admission, source_sha256=b"x" * 32)
            case "allocated_scope":
                descriptor = replace(descriptor, scope=(b"x" * 32, *descriptor.scope[1:]))
            case "scope_type":
                descriptor = replace(descriptor, scope=(*descriptor.scope[:7], 0, *descriptor.scope[8:]))
            case "receipt":
                receipt = receipt | {"result_sha256": b"x" * 32}
            case "late_sha":
                descriptor = replace(descriptor, sha256=b"x" * 32)
            case "counts":
                descriptor = replace(descriptor, counts=(descriptor.counts[0] + 1, *descriptor.counts[1:]))
        with pytest.raises((FamilyBlockUnavailable, ValueError)):
            control._admission_completion(context, admission, descriptor, receipt, limits, None)


async def test_cancelled_original_join_precedes_admission_metadata_writes(tmp_path, monkeypatch):
    async with _completion(tmp_path, monkeypatch) as arguments:
        failure = InterruptedError("synthetic admission completion stopped")

        def check():
            raise failure

        with pytest.raises(InterruptedError) as caught:
            control._admission_completion(*arguments, check)
        assert caught.value is failure
