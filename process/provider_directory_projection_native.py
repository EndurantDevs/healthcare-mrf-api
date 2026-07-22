# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native child process bridge for retained projection bytes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
import os
from typing import Any, AsyncIterable, Awaitable, Callable, Mapping

from process.provider_directory_projection_copy_summary import (
    NATIVE_CANONICAL_ROW_CONTRACT_ID,
    NATIVE_COPY_COLUMN_CONTRACT_ID,
    NATIVE_COPY_CONTRACT_ID,
    NATIVE_COPY_MAGIC,
    NATIVE_COPY_SPOOL_CONTRACT_ID,
    NATIVE_DECODER_CONTRACT_ID,
    NATIVE_TRANSFORM_CONTRACT_ID,
)
from process.provider_directory_projection_native_copy import (
    consume_native_copy_spool,
)
from process.provider_directory_projection_types import (
    ProjectionProofShard,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProjectionStage,
    ProviderDirectoryProjectionError,
)


_MAX_STDERR_BYTES = 256 * 1024


@dataclass(frozen=True)
class NativeProjectionResult:
    """Exact child census, physical proof, and non-persisted timing telemetry."""

    byte_count: int
    record_count: int
    input_sha256: str
    payload_sha256: str
    shard_proof: ProjectionProofShard
    native_thread_count: int = 1
    materializer_worker_count: int = 1
    copied_record_count: int = 0
    timings_seconds: Mapping[str, float] = field(default_factory=dict)


ProjectionNativeRunner = Callable[..., Awaitable[NativeProjectionResult]]


@dataclass(frozen=True)
class _NativeExecutionSettings:
    thread_count: int
    executable: str


def projection_native_thread_count(
    materializer_workers: int,
    requested_threads: int | None = None,
) -> int:
    """Resolve bounded Rayon parallelism without host-wide oversubscription."""

    if type(materializer_workers) is not int or not 1 <= materializer_workers <= 4:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_worker_count_invalid"
        )
    raw_threads: Any = requested_threads
    if raw_threads is None:
        raw_threads = os.getenv(
            "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_THREADS",
            "1",
        )
    raw_total = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_TOTAL_THREADS",
        "8",
    )
    try:
        thread_count = int(raw_threads)
        total_thread_limit = int(raw_total)
    except (TypeError, ValueError) as error:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_threads_invalid"
        ) from error
    if (
        not 1 <= thread_count <= 8
        or not 1 <= total_thread_limit <= 64
        or materializer_workers * thread_count > total_thread_limit
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_threads_invalid"
        )
    return thread_count


def native_subprocess_environment(native_threads: int) -> dict[str, str]:
    """Return a credential-free child environment with a Rayon cap."""

    if type(native_threads) is not int or not 1 <= native_threads <= 8:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_threads_invalid"
        )
    return {"RAYON_NUM_THREADS": str(native_threads)}


def _native_execution_settings(
    materializer_workers: int,
    native_options_map: Mapping[str, Any],
) -> _NativeExecutionSettings:
    if set(native_options_map) - {"native_threads", "executable"}:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_options_invalid"
        )
    thread_count = projection_native_thread_count(
        materializer_workers,
        native_options_map.get("native_threads"),
    )
    executable = (
        native_options_map.get("executable")
        or os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_BIN")
        or os.getenv("HLTHPRT_PTG2_RUST_SCANNER_BIN")
    )
    if (
        not isinstance(executable, str)
        or not executable.strip()
        or not os.path.isabs(executable)
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_executable_invalid"
        )
    return _NativeExecutionSettings(thread_count, executable)


async def _close_native_stdin(stdin: Any) -> None:
    stdin.close()
    try:
        await stdin.wait_closed()
    except (BrokenPipeError, ConnectionResetError):
        return


async def _pump_retained_bytes(
    byte_stream: AsyncIterable[bytes],
    stdin: Any,
    expected_bytes: int,
) -> tuple[int, str]:
    payload_digest = hashlib.sha256()
    byte_count = 0
    try:
        async for chunk in byte_stream:
            if not isinstance(chunk, bytes) or not chunk:
                raise ProviderDirectoryProjectionError(
                    "provider_directory_projection_retained_chunk_invalid"
                )
            byte_count += len(chunk)
            if byte_count > expected_bytes:
                raise ProviderDirectoryProjectionError(
                    "provider_directory_projection_retained_byte_count_mismatch"
                )
            payload_digest.update(chunk)
            stdin.write(chunk)
            await stdin.drain()
    finally:
        await _close_native_stdin(stdin)
    if byte_count != expected_bytes:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_retained_byte_count_mismatch"
        )
    return byte_count, payload_digest.hexdigest()


async def _drain_stderr(stream: Any) -> tuple[str, bool]:
    retained_bytes = bytearray()
    total_bytes = 0
    while True:
        chunk = await stream.read(8192)
        if not chunk:
            break
        total_bytes += len(chunk)
        if len(retained_bytes) < _MAX_STDERR_BYTES:
            retained_bytes.extend(chunk[: _MAX_STDERR_BYTES - len(retained_bytes)])
    return (
        retained_bytes.decode("utf-8", errors="replace"),
        total_bytes > _MAX_STDERR_BYTES,
    )


async def _stop_process(process: Any) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


async def _validated_process_output(
    process: Any,
    stderr_task: asyncio.Task[tuple[str, bool]],
) -> None:
    return_code = await process.wait()
    diagnostics, stderr_exceeded = await stderr_task
    if return_code != 0 or stderr_exceeded:
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_failed:"
            f"{return_code}:{diagnostics[:512]}"
        )


def _validate_native_copy_input(
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    framing: str,
) -> None:
    recipe = claim.recipe_lease.recipe
    if (
        framing not in {"ndjson", "bundle"}
        or child_lease.shard_claim != claim
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_input_invalid"
        )
    validate_native_projection_recipe(recipe)


def validate_native_projection_recipe(recipe: Any) -> None:
    """Require the fixed native decoder and semantic transform identity."""

    if (
        getattr(recipe, "decoder_contract_id", None) != NATIVE_DECODER_CONTRACT_ID
        or getattr(recipe, "transform_contract_id", None)
        != NATIVE_TRANSFORM_CONTRACT_ID
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_input_invalid"
        )


async def _launch_native_copy_process(
    claim: ProjectionShardClaim,
    framing: str,
    settings: _NativeExecutionSettings,
) -> Any:
    process = await asyncio.create_subprocess_exec(
        settings.executable,
        "--provider-directory-materialize-stdio-v2",
        claim.recipe_lease.recipe.recipe_id,
        claim.shard.partition_id,
        str(claim.shard.partition_ordinal),
        framing,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=native_subprocess_environment(settings.thread_count),
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        await _stop_process(process)
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_pipe_missing"
        )
    return process


async def run_native_projection_command(
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    stage: ProjectionStage,
    byte_stream: AsyncIterable[bytes],
    *,
    framing: str,
    transaction: Any,
    materializer_workers: int,
    **native_options_map: Any,
) -> NativeProjectionResult:
    """Forward one contract-fenced native v2 PostgreSQL COPY stream."""

    _validate_native_copy_input(claim, child_lease, framing)
    settings = _native_execution_settings(materializer_workers, native_options_map)
    process = await _launch_native_copy_process(claim, framing, settings)
    raw_census_task = asyncio.create_task(
        _pump_retained_bytes(
            byte_stream,
            process.stdin,
            child_lease.expected_byte_count,
        )
    )
    stderr_task = asyncio.create_task(_drain_stderr(process.stderr))
    try:
        shard_proof, summary_map, copied_count = await consume_native_copy_spool(
            process.stdout,
            claim=claim,
            child_lease=child_lease,
            stage=stage,
            transaction=transaction,
            framing=framing,
            raw_census_task=raw_census_task,
        )
        await _validated_process_output(process, stderr_task)
    except BaseException:
        raw_census_task.cancel()
        await _stop_process(process)
        stderr_task.cancel()
        await asyncio.gather(raw_census_task, stderr_task, return_exceptions=True)
        raise
    return NativeProjectionResult(
        byte_count=int(summary_map["input_byte_count"]),
        record_count=int(summary_map["resource_count"]),
        input_sha256=child_lease.input_sha256,
        payload_sha256=str(summary_map["input_sha256"]),
        shard_proof=shard_proof,
        native_thread_count=settings.thread_count,
        materializer_worker_count=materializer_workers,
        copied_record_count=copied_count,
        timings_seconds=dict(summary_map["timings_seconds"]),
    )


__all__ = (
    "NATIVE_CANONICAL_ROW_CONTRACT_ID",
    "NATIVE_COPY_COLUMN_CONTRACT_ID",
    "NATIVE_COPY_CONTRACT_ID",
    "NATIVE_COPY_MAGIC",
    "NATIVE_COPY_SPOOL_CONTRACT_ID",
    "NATIVE_DECODER_CONTRACT_ID",
    "NativeProjectionResult",
    "ProjectionNativeRunner",
    "NATIVE_TRANSFORM_CONTRACT_ID",
    "native_subprocess_environment",
    "projection_native_thread_count",
    "run_native_projection_command",
    "validate_native_projection_recipe",
)
