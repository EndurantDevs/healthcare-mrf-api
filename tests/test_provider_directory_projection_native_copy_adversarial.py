# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adversarial bounds and driver behavior for native-v2 COPY output."""

from __future__ import annotations

import asyncio
import hashlib
import struct

import pytest

import process.provider_directory_projection_native as native
import process.provider_directory_projection_native_copy as native_copy
from process.provider_directory_projection_copy_summary import (
    NATIVE_COPY_MAGIC,
    NATIVE_COPY_STREAM_MAX_BYTES,
    NATIVE_COPY_SUMMARY_MAX_BYTES,
)
from process.provider_directory_projection_types import (
    PROJECTION_INPUT_BLOCK_MAX_BYTES,
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_native_copy_support import (
    BinaryCopyTransaction,
    FakeProcess,
    native_copy_spool,
    raw_census,
    stream_reader,
)


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def test_default_thread_budget_leaves_capacity_for_postgres(monkeypatch) -> None:
    """Four shard workers default to one native thread each."""

    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_THREADS",
        raising=False,
    )
    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_TOTAL_THREADS",
        raising=False,
    )

    assert native.projection_native_thread_count(4) == 1


async def _consume(context, spool: bytes, transaction=None):
    retained_payload = context.fixture.encoded
    return await native_copy.consume_native_copy_spool(
        stream_reader(spool),
        claim=context.claim,
        child_lease=context.child_lease,
        stage=context.stage,
        transaction=(
            transaction
            if transaction is not None
            else BinaryCopyTransaction(len(context.fixture.resources))
        ),
        framing=context.fixture.framing,
        raw_census_task=asyncio.create_task(
            raw_census(len(retained_payload), _digest(retained_payload))
        ),
    )


def _replace_summary(spool: bytes, replacement) -> bytes:
    length_offset = len(NATIVE_COPY_MAGIC)
    summary_length = struct.unpack(">I", spool[length_offset : length_offset + 4])[0]
    summary_start = length_offset + 4
    summary_end = summary_start + summary_length
    summary_payload = replacement(spool[summary_start:summary_end])
    return (
        spool[:length_offset]
        + struct.pack(">I", len(summary_payload))
        + summary_payload
        + spool[summary_end:]
    )


@pytest.mark.asyncio
async def test_duplicate_summary_key_fails_closed() -> None:
    context = synthetic_projection_context("ndjson")
    spool = _replace_summary(
        native_copy_spool(context),
        lambda summary: summary.replace(
            b'"record_kind":',
            b'"record_kind":"duplicate","record_kind":',
            1,
        ),
    )

    with pytest.raises(ProviderDirectoryProjectionError):
        await _consume(context, spool)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "integer_field",
    (
        "partition_ordinal",
        "input_byte_count",
        "canonical_input_byte_count",
        "resource_count",
        "copy_byte_count",
    ),
)
async def test_summary_boolean_cannot_substitute_for_integer(integer_field) -> None:
    context = synthetic_projection_context("ndjson")
    with pytest.raises(ProviderDirectoryProjectionError):
        await _consume(
            context,
            native_copy_spool(context, summary_overrides={integer_field: True}),
        )


@pytest.mark.asyncio
async def test_canonical_input_count_above_retained_block_bound_fails() -> None:
    context = synthetic_projection_context("ndjson")
    oversized_count = PROJECTION_INPUT_BLOCK_MAX_BYTES + 1
    with pytest.raises(ProviderDirectoryProjectionError):
        await _consume(
            context,
            native_copy_spool(
                context,
                summary_overrides={"canonical_input_byte_count": oversized_count},
            ),
        )


class _TrackedReader:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0
        self.read_sizes = []

    async def readexactly(self, requested_bytes: int) -> bytes:
        self.read_sizes.append(requested_bytes)
        end_offset = self.offset + requested_bytes
        if end_offset > len(self.payload):
            raise asyncio.IncompleteReadError(
                self.payload[self.offset :], requested_bytes
            )
        chunk = self.payload[self.offset:end_offset]
        self.offset = end_offset
        return chunk


@pytest.mark.asyncio
async def test_oversized_summary_header_is_rejected_before_payload_read() -> None:
    reader = _TrackedReader(
        NATIVE_COPY_MAGIC + struct.pack(">I", NATIVE_COPY_SUMMARY_MAX_BYTES + 1)
    )
    with pytest.raises(ProviderDirectoryProjectionError):
        await native_copy._native_copy_header(reader)
    assert reader.read_sizes == [len(NATIVE_COPY_MAGIC), 4]


@pytest.mark.asyncio
async def test_oversized_copy_header_is_rejected_before_copy_read() -> None:
    context = synthetic_projection_context("ndjson")
    spool = native_copy_spool(context)
    length_offset = len(NATIVE_COPY_MAGIC)
    summary_length = struct.unpack(">I", spool[length_offset : length_offset + 4])[0]
    summary_end = length_offset + 4 + summary_length
    reader = _TrackedReader(
        spool[:summary_end] + struct.pack(">Q", NATIVE_COPY_STREAM_MAX_BYTES + 1)
    )
    with pytest.raises(ProviderDirectoryProjectionError):
        await native_copy._native_copy_header(reader)
    assert reader.read_sizes == [len(NATIVE_COPY_MAGIC), 4, summary_length, 8]


class _DriverTransaction:
    def __init__(self, mode: str, copied_count: int) -> None:
        self.mode = mode
        self.copied_count = copied_count

    async def copy_to_table(self, _table_name, **copy_options):
        copy_source = copy_options["source"]
        if self.mode == "partial":
            await copy_source.__anext__()
        elif self.mode != "none":
            consumed_byte_count = 0
            async for copy_chunk in copy_source:
                consumed_byte_count += len(copy_chunk)
            assert consumed_byte_count > 0
        if self.mode == "invalid-status":
            return "INSERT 0"
        return f"COPY {self.copied_count}"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("none", "partial"))
async def test_nonconsuming_or_partial_driver_cannot_verify(mode) -> None:
    context = synthetic_projection_context("ndjson", groups=100)
    transaction = _DriverTransaction(mode, len(context.fixture.resources))
    with pytest.raises(ProviderDirectoryProjectionError):
        await _consume(context, native_copy_spool(context), transaction)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "count"),
    (("invalid-status", 8), ("full", 7)),
)
async def test_invalid_copy_status_or_count_fails_closed(mode, count) -> None:
    context = synthetic_projection_context("ndjson")
    with pytest.raises(ProviderDirectoryProjectionError):
        await _consume(
            context,
            native_copy_spool(context),
            _DriverTransaction(mode, count),
        )


@pytest.mark.asyncio
async def test_missing_binary_copy_driver_fails_closed() -> None:
    context = synthetic_projection_context("ndjson")
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_copy_driver_missing",
    ):
        await _consume(context, native_copy_spool(context), object())


@pytest.mark.asyncio
async def test_timing_exponent_is_telemetry_only() -> None:
    context = synthetic_projection_context("ndjson")
    baseline_proof, baseline_summary, _count = await _consume(
        context, native_copy_spool(context)
    )
    exponent_spool = _replace_summary(
        native_copy_spool(context),
        lambda summary: summary.replace(
            b'"input_read_seconds":0.0',
            b'"input_read_seconds":1e-7',
            1,
        ),
    )
    changed_proof, changed_summary, _count = await _consume(context, exponent_spool)

    assert changed_summary["timings_seconds"] != baseline_summary["timings_seconds"]
    assert changed_proof == baseline_proof


@pytest.mark.asyncio
@pytest.mark.parametrize("timing_token", (b"-1e-7", b"1e999"))
async def test_negative_or_nonfinite_timing_fails_closed(timing_token) -> None:
    context = synthetic_projection_context("ndjson")
    invalid_spool = _replace_summary(
        native_copy_spool(context),
        lambda summary: summary.replace(
            b'"input_read_seconds":0.0',
            b'"input_read_seconds":' + timing_token,
            1,
        ),
    )
    with pytest.raises(ProviderDirectoryProjectionError):
        await _consume(context, invalid_spool)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("short", "long", "changed"))
async def test_runner_rejects_retained_byte_mismatch(monkeypatch, mode) -> None:
    context = synthetic_projection_context("ndjson")

    async def fake_subprocess(*_command, **_options):
        return FakeProcess(native_copy_spool(context))

    async def retained_bytes():
        payload = context.fixture.encoded
        if mode == "short":
            yield payload[:-1]
        elif mode == "long":
            yield payload + b"x"
        else:
            yield bytes((payload[0] ^ 1,)) + payload[1:]

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)
    with pytest.raises(ProviderDirectoryProjectionError):
        await native.run_native_projection_command(
            context.claim,
            context.child_lease,
            context.stage,
            retained_bytes(),
            framing="ndjson",
            transaction=BinaryCopyTransaction(len(context.fixture.resources)),
            materializer_workers=4,
            native_threads=2,
            executable="/synthetic-native-runner",
        )
