# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline trust-boundary proofs for the native-v2 binary COPY bridge."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import struct

import pytest

import process.provider_directory_projection_native as native
from process.provider_directory_projection_copy_summary import NATIVE_COPY_MAGIC
from process.provider_directory_projection_native_copy import (
    consume_native_copy_spool,
)
from process.provider_directory_projection_stage import STAGE_COPY_COLUMNS
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_native_copy_support import (
    BinaryCopyTransaction,
    FakeProcess,
    native_copy_spool,
    native_copy_stream,
    raw_census,
    stream_reader,
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def test_native_executable_must_be_absolute(monkeypatch) -> None:
    """Never resolve a child executable through cwd or inherited PATH."""

    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_BIN",
        raising=False,
    )
    monkeypatch.delenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", raising=False)
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_executable_invalid",
    ):
        native._native_execution_settings(1, {"executable": "ptg2_scanner"})


def test_native_executable_uses_absolute_scanner_fallback(monkeypatch) -> None:
    """The shared scanner image path is the explicit final fallback."""

    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_BIN",
        raising=False,
    )
    scanner_path = "/opt/support/ptg2_scanner/target/release/ptg2_scanner"
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", scanner_path)

    settings = native._native_execution_settings(2, {})

    assert settings.executable == scanner_path
    assert settings.thread_count == 1


async def _consume(context, spool: bytes, *, transaction=None, **overrides):
    fixture = context.fixture
    raw_census_task = asyncio.create_task(
        raw_census(
            overrides.pop("raw_byte_count", len(fixture.encoded)),
            overrides.pop("raw_sha256", _sha256(fixture.encoded)),
        )
    )
    active_transaction = transaction or BinaryCopyTransaction(
        len(fixture.resources)
    )
    outcome = await consume_native_copy_spool(
        stream_reader(spool),
        claim=overrides.pop("claim", context.claim),
        child_lease=overrides.pop("child_lease", context.child_lease),
        stage=context.stage,
        transaction=active_transaction,
        framing=overrides.pop("framing", fixture.framing),
        raw_census_task=raw_census_task,
    )
    assert not overrides
    return (*outcome, active_transaction)


@pytest.mark.asyncio
@pytest.mark.parametrize("framing", ("ndjson", "bundle"))
async def test_native_copy_forwards_exact_pgcopy_and_builds_claimed_proof(
    framing,
) -> None:
    context = synthetic_projection_context(framing)
    spool = native_copy_spool(context)
    copy_stream, _projection_rows = native_copy_stream(context)

    proof, summary_map, copied_count, transaction = await _consume(context, spool)

    assert copied_count == proof.resource_count == len(context.fixture.resources)
    assert bytes(transaction.copy_bytes) == copy_stream
    assert transaction.copy_options_map["columns"] == STAGE_COPY_COLUMNS
    assert transaction.copy_options_map["format"] == "binary"
    assert proof.canonical_row_sha256 == summary_map["canonical_row_sha256"]
    assert proof.proof["producer_proof"]["copy_sha256"] == _sha256(copy_stream)
    assert "timings_seconds" not in proof.proof["producer_proof"]


@pytest.mark.asyncio
async def test_full_artifact_keeps_manifest_identity_distinct_from_canonical_digest(
) -> None:
    manifest_identity = _sha256(b"full-artifact-layout-manifest")
    context = synthetic_projection_context(
        "bundle",
        block_content_sha256=manifest_identity,
        retained_range_ordinal=None,
    )

    proof, summary_map, _copied_count, _transaction = await _consume(
        context,
        native_copy_spool(context),
    )

    assert summary_map["canonical_input_sha256"] != manifest_identity
    assert proof.input_sha256 == manifest_identity


@pytest.mark.asyncio
async def test_range_child_requires_exact_canonical_input_digest() -> None:
    context = synthetic_projection_context(
        "ndjson",
        block_content_sha256=_sha256(b"wrong-range-canonical-digest"),
        retained_range_ordinal=0,
    )

    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_spool_invalid",
    ):
        await _consume(context, native_copy_spool(context))


def _changed_copy_length(spool: bytes, delta: int) -> bytes:
    summary_length_offset = len(NATIVE_COPY_MAGIC)
    summary_length = struct.unpack(
        ">I",
        spool[summary_length_offset : summary_length_offset + 4],
    )[0]
    copy_length_offset = summary_length_offset + 4 + summary_length
    copy_length = struct.unpack(
        ">Q",
        spool[copy_length_offset : copy_length_offset + 8],
    )[0]
    return (
        spool[:copy_length_offset]
        + struct.pack(">Q", copy_length + delta)
        + spool[copy_length_offset + 8 :]
    )


def _mutated_native_copy_case(context, mutation: str) -> tuple[bytes, dict]:
    spool = native_copy_spool(context)
    copy_stream, _projection_rows = native_copy_stream(context)
    changed_byte = bytes((copy_stream[-3] ^ 1,))
    structural_spool_map = {
        "bad_magic": b"X" + spool[1:],
        "truncated": spool[:-1],
        "extra": spool + b"extra",
        "copy_digest": native_copy_spool(
            context,
            copy_stream=copy_stream[:-3] + changed_byte + copy_stream[-2:],
        ),
        "copy_length_short": _changed_copy_length(spool, -1),
        "copy_length_long": _changed_copy_length(spool, 1),
    }
    if mutation in structural_spool_map:
        return structural_spool_map[mutation], {}
    summary_override_map = {
        "unknown_summary_field": {"unknown": True},
        "wrong_contract": {"transform_contract_id": "wrong.v1"},
        "wrong_copy_count": {"copy_byte_count": 21},
    }
    if mutation in summary_override_map:
        return native_copy_spool(
            context,
            summary_overrides=summary_override_map[mutation],
        ), {}
    return spool, {"raw_sha256": "f" * 64}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    (
        "bad_magic",
        "truncated",
        "extra",
        "copy_digest",
        "copy_length_short",
        "copy_length_long",
        "unknown_summary_field",
        "wrong_contract",
        "wrong_copy_count",
        "wrong_raw_census",
    ),
)
async def test_native_copy_bridge_fails_closed_on_untrusted_output(mutation) -> None:
    context = synthetic_projection_context("ndjson")
    spool, consume_options_map = _mutated_native_copy_case(context, mutation)
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_spool_invalid",
    ):
        await _consume(context, spool, **consume_options_map)


@pytest.mark.asyncio
async def test_native_v2_runner_fences_command_threads_and_retained_bytes(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")
    captured_process_map = {}
    processes = []

    async def fake_subprocess(*command, **options):
        captured_process_map["command"] = command
        captured_process_map["options"] = options
        process = FakeProcess(native_copy_spool(context))
        processes.append(process)
        return process

    async def retained_bytes():
        midpoint = len(context.fixture.encoded) // 2
        yield context.fixture.encoded[:midpoint]
        yield context.fixture.encoded[midpoint:]

    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", "must-not-reach-native-child")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)
    transaction = BinaryCopyTransaction(len(context.fixture.resources))
    outcome = await native.run_native_projection_command(
        context.claim,
        context.child_lease,
        context.stage,
        retained_bytes(),
        framing="ndjson",
        transaction=transaction,
        materializer_workers=4,
        native_threads=2,
        executable="/synthetic-native-runner",
    )

    assert captured_process_map["command"] == (
        "/synthetic-native-runner",
        "--provider-directory-materialize-stdio-v2",
        context.claim.recipe_lease.recipe.recipe_id,
        context.claim.shard.partition_id,
        "0",
        "ndjson",
    )
    assert captured_process_map["options"]["env"] == {"RAYON_NUM_THREADS": "2"}
    assert processes[0].stdin.payload == context.fixture.encoded
    assert outcome.copied_record_count == outcome.record_count == 8


@pytest.mark.asyncio
async def test_native_v2_rejects_wrong_recipe_contract_before_launch(
    monkeypatch,
) -> None:
    context = synthetic_projection_context("ndjson")
    wrong_recipe = replace(
        context.claim.recipe_lease.recipe,
        transform_contract_id="wrong-transform.v1",
    )
    wrong_recipe_lease = replace(context.claim.recipe_lease, recipe=wrong_recipe)
    wrong_claim = replace(context.claim, recipe_lease=wrong_recipe_lease)
    wrong_child = replace(context.child_lease, shard_claim=wrong_claim)
    launch_markers = []

    async def fake_subprocess(*_command, **_options):
        launch_markers.append(True)
        return FakeProcess(native_copy_spool(context))

    async def retained_bytes():
        yield context.fixture.encoded

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_subprocess)
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_input_invalid",
    ):
        await native.run_native_projection_command(
            wrong_claim,
            wrong_child,
            context.stage,
            retained_bytes(),
            framing="ndjson",
            transaction=BinaryCopyTransaction(len(context.fixture.resources)),
            materializer_workers=1,
        )
    assert not launch_markers
