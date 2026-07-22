# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Off-by-default bounded orchestration for independent projection shards."""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
import logging
from typing import Any, AsyncIterable, Awaitable, Callable, Protocol

from db.connection import db
from process.provider_directory_projection_child_read import (
    claimed_projection_child_read_lease,
    heartbeat_projection_child_read_lease,
    verify_projection_child_read_lease,
)
import process.provider_directory_projection_db as projection_db
from process.provider_directory_projection_native import (
    NativeProjectionResult,
    ProjectionNativeRunner,
    projection_native_thread_count,
    run_native_projection_command,
    validate_native_projection_recipe,
)
from process.provider_directory_projection_lease import heartbeat_projection_lease
from process.provider_directory_projection_materializer_config import (
    ProjectionMaterializerSettings,
    is_projection_materializer_enabled,
    projection_materializer_settings,
    projection_materializer_worker_count,
    projection_native_timeout_seconds,
)
from process.provider_directory_projection_stage import (
    assert_projection_stage_partition,
    ensure_projection_stage,
    prepare_projection_stage_partition,
)
from process.provider_directory_projection_types import (
    ProjectionLease,
    ProjectionProofShard,
    ProjectionRetainedChildLease,
    ProjectionShardClaim,
    ProjectionStage,
    ProviderDirectoryProjectionError,
)
from process.provider_directory_projection_workset import (
    claim_projection_shard,
    complete_projection_shard,
    heartbeat_projection_shard,
    projection_shard_input_framing,
)
from process.provider_directory_projection_worker_admission import projection_worker_admission


LOGGER = logging.getLogger(__name__)

class ProjectionChildStreamFactory(Protocol):
    """Open locator-free retained bytes for one exact child generation."""

    def __call__(
        self,
        lease: ProjectionRetainedChildLease,
    ) -> AbstractAsyncContextManager[AsyncIterable[bytes]]: ...


ProjectionFramingResolver = Callable[[ProjectionShardClaim], Awaitable[str]]


async def _heartbeat_loop(
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    stop_event: asyncio.Event,
    *,
    database: Any,
    schema: str,
    lease_seconds: int,
    heartbeat_seconds: float,
) -> None:
    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=heartbeat_seconds)
            return
        except asyncio.TimeoutError:
            await heartbeat_projection_lease(
                claim.recipe_lease,
                lease_seconds=lease_seconds,
                database=database,
                schema=schema,
            )
            await heartbeat_projection_shard(
                claim,
                lease_seconds=lease_seconds,
                database=database,
                schema=schema,
            )
            await heartbeat_projection_child_read_lease(
                child_lease,
                lease_seconds=lease_seconds,
                database=database,
                schema=schema,
            )


async def _stop_heartbeat(
    stop_event: asyncio.Event,
    heartbeat_task: asyncio.Task[None],
) -> None:
    stop_event.set()
    await heartbeat_task


def _validate_native_result(
    native_result: NativeProjectionResult,
    child_lease: ProjectionRetainedChildLease,
    worker_count: int,
    native_threads: int,
) -> None:
    if (
        type(native_result) is not NativeProjectionResult
        or native_result.byte_count != child_lease.expected_byte_count
        or native_result.record_count != child_lease.expected_record_count
        or native_result.input_sha256 != child_lease.input_sha256
        or native_result.payload_sha256 != child_lease.expected_payload_sha256
        or native_result.shard_proof.partition_id
        != child_lease.shard_claim.shard.partition_id
        or native_result.shard_proof.recipe_id
        != child_lease.shard_claim.recipe_lease.recipe.recipe_id
        or native_result.shard_proof.attempt
        != child_lease.shard_claim.recipe_lease.attempt
        or native_result.shard_proof.partition_attempt
        != child_lease.shard_claim.partition_attempt
        or native_result.shard_proof.partition_ordinal
        != child_lease.shard_claim.shard.partition_ordinal
        or native_result.shard_proof.resource_type
        != child_lease.shard_claim.shard.resource_type
        or native_result.shard_proof.input_sha256
        != child_lease.shard_claim.shard.input_sha256
        or native_result.shard_proof.resource_count != native_result.record_count
        or native_result.native_thread_count != native_threads
        or native_result.materializer_worker_count != worker_count
        or native_result.copied_record_count != native_result.record_count
    ):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_result_mismatch"
        )


@dataclass(frozen=True)
class _ShardMaterializerRuntime:
    stage: ProjectionStage
    child_stream_factory: ProjectionChildStreamFactory
    native_runner: ProjectionNativeRunner
    framing_resolver: Callable[..., Awaitable[str]]
    database: Any
    schema: str
    worker_count: int
    native_threads: int
    lease_seconds: int
    heartbeat_seconds: float
    native_timeout_seconds: float
    executable: str | None
    worker_admission: asyncio.Semaphore


async def _await_native_result(
    runner_task: asyncio.Task[NativeProjectionResult],
    heartbeat_task: asyncio.Task[None],
    timeout_seconds: float,
) -> NativeProjectionResult:
    completed_tasks, _pending_tasks = await asyncio.wait(
        {runner_task, heartbeat_task},
        return_when=asyncio.FIRST_COMPLETED,
        timeout=timeout_seconds,
    )
    if not completed_tasks:
        runner_task.cancel()
        await asyncio.gather(runner_task, return_exceptions=True)
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_timeout"
        )
    if heartbeat_task in completed_tasks:
        runner_task.cancel()
        await asyncio.gather(runner_task, return_exceptions=True)
        await heartbeat_task
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_heartbeat_stopped"
        )
    return await runner_task


async def _run_shard_transaction(
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    byte_stream: AsyncIterable[bytes],
    stop_event: asyncio.Event,
    heartbeat_task: asyncio.Task[None],
    runtime: _ShardMaterializerRuntime,
) -> tuple[ProjectionProofShard, NativeProjectionResult]:
    """Run COPY and immutable checkpointing on one owned transaction."""

    async with runtime.database.acquire() as connection:
        await projection_db.set_local_projection_synchronous_commit(connection, "on")
        await projection_db.set_local_projection_wal_compression(connection)
        await prepare_projection_stage_partition(
            runtime.stage,
            claim,
            database=connection,
        )
        native_result = await _run_native_child(
            claim,
            child_lease,
            byte_stream,
            connection,
            heartbeat_task,
            runtime,
        )
        await _stop_heartbeat(stop_event, heartbeat_task)
        shard_proof = await _checkpoint_native_result(
            claim,
            child_lease,
            native_result,
            connection,
            runtime,
        )
    return shard_proof, native_result


async def _run_native_child(
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    byte_stream: AsyncIterable[bytes],
    connection: Any,
    heartbeat_task: asyncio.Task[None],
    runtime: _ShardMaterializerRuntime,
) -> NativeProjectionResult:
    framing = await runtime.framing_resolver(
        claim,
        database=connection,
        schema=runtime.schema,
    )
    runner_task = asyncio.create_task(
        runtime.native_runner(
            claim,
            child_lease,
            runtime.stage,
            byte_stream,
            framing=framing,
            transaction=connection,
            materializer_workers=runtime.worker_count,
            native_threads=runtime.native_threads,
            executable=runtime.executable,
        )
    )
    return await _await_native_result(
        runner_task,
        heartbeat_task,
        runtime.native_timeout_seconds,
    )


async def _checkpoint_native_result(
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    native_result: NativeProjectionResult,
    connection: Any,
    runtime: _ShardMaterializerRuntime,
) -> ProjectionProofShard:
    _validate_native_result(
        native_result,
        child_lease,
        runtime.worker_count,
        runtime.native_threads,
    )
    await assert_projection_stage_partition(
        runtime.stage,
        claim,
        native_result.shard_proof,
        database=connection,
    )
    await verify_projection_child_read_lease(
        child_lease,
        byte_count=native_result.byte_count,
        record_count=native_result.record_count,
        input_sha256=native_result.input_sha256,
        payload_sha256=native_result.payload_sha256,
        database=connection,
        schema=runtime.schema,
    )
    return await complete_projection_shard(
        claim,
        native_result.shard_proof,
        child_lease=child_lease,
        database=connection,
        schema=runtime.schema,
    )


async def _initial_heartbeat(
    claim: ProjectionShardClaim,
    child_lease: ProjectionRetainedChildLease,
    runtime: _ShardMaterializerRuntime,
) -> None:
    await heartbeat_projection_lease(
        claim.recipe_lease,
        lease_seconds=runtime.lease_seconds,
        database=runtime.database,
        schema=runtime.schema,
    )
    await heartbeat_projection_shard(
        claim,
        lease_seconds=runtime.lease_seconds,
        database=runtime.database,
        schema=runtime.schema,
    )
    await heartbeat_projection_child_read_lease(
        child_lease,
        lease_seconds=runtime.lease_seconds,
        database=runtime.database,
        schema=runtime.schema,
    )


async def _materialize_claimed_shard(
    claim: ProjectionShardClaim,
    runtime: _ShardMaterializerRuntime,
) -> ProjectionProofShard:
    """Materialize one exact shard and release only its verified child."""

    async with claimed_projection_child_read_lease(
        claim,
        lease_seconds=runtime.lease_seconds,
        database=runtime.database,
        schema=runtime.schema,
    ) as child_lease:
        await _initial_heartbeat(claim, child_lease, runtime)
        async with runtime.child_stream_factory(child_lease) as byte_stream:
            stop_event = asyncio.Event()
            heartbeat_task = asyncio.create_task(
                _heartbeat_loop(
                    claim,
                    child_lease,
                    stop_event,
                    database=runtime.database,
                    schema=runtime.schema,
                    lease_seconds=runtime.lease_seconds,
                    heartbeat_seconds=runtime.heartbeat_seconds,
                )
            )
            try:
                shard_proof, native_result = await _run_shard_transaction(
                    claim,
                    child_lease,
                    byte_stream,
                    stop_event,
                    heartbeat_task,
                    runtime,
                )
            except BaseException:
                stop_event.set()
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
                raise
        LOGGER.info(
            "provider_directory_projection_shard_materialized",
            extra={
                "partition_id": shard_proof.partition_id,
                "record_count": shard_proof.resource_count,
                "materializer_worker_count": runtime.worker_count,
                "native_thread_count": runtime.native_threads,
                "native_timings_seconds": dict(native_result.timings_seconds),
            },
        )
        return shard_proof


async def _materializer_runtime(
    lease: ProjectionLease,
    child_stream_factory: ProjectionChildStreamFactory,
    native_runner: ProjectionNativeRunner,
    framing_resolver: Callable[..., Awaitable[str]],
    database: Any,
    schema: str,
    settings: ProjectionMaterializerSettings,
) -> _ShardMaterializerRuntime:
    if not is_projection_materializer_enabled(settings.enabled):
        raise ProviderDirectoryProjectionError(
            "provider_directory_projection_native_disabled"
        )
    validate_native_projection_recipe(lease.recipe)
    worker_count = projection_materializer_worker_count(settings.max_workers)
    thread_count = projection_native_thread_count(worker_count, settings.native_threads)
    stage = await ensure_projection_stage(
        lease,
        database=database,
        schema=schema,
    )
    return _ShardMaterializerRuntime(
        stage=stage,
        child_stream_factory=child_stream_factory,
        native_runner=native_runner,
        framing_resolver=framing_resolver,
        database=database,
        schema=schema,
        worker_count=worker_count,
        native_threads=thread_count,
        lease_seconds=settings.lease_seconds,
        heartbeat_seconds=settings.heartbeat_seconds,
        native_timeout_seconds=projection_native_timeout_seconds(
            settings.native_timeout_seconds
        ),
        executable=settings.executable,
        worker_admission=projection_worker_admission(database).semaphore,
    )


async def _projection_worker(
    lease: ProjectionLease,
    admission_id: str,
    runtime: _ShardMaterializerRuntime,
    completed_proofs: list[ProjectionProofShard],
) -> None:
    """Claim independent shards until no immediately claimable work remains."""

    while True:
        async with runtime.worker_admission:
            claim = await claim_projection_shard(
                lease,
                admission_id=admission_id,
                lease_seconds=runtime.lease_seconds,
                database=runtime.database,
                schema=runtime.schema,
            )
            if claim is None:
                return
            completed_proofs.append(await _materialize_claimed_shard(claim, runtime))


async def _run_projection_workers(
    lease: ProjectionLease,
    admission_id: str,
    runtime: _ShardMaterializerRuntime,
) -> tuple[ProjectionProofShard, ...]:
    completed_proofs: list[ProjectionProofShard] = []
    worker_tasks = [
        asyncio.create_task(
            _projection_worker(lease, admission_id, runtime, completed_proofs)
        )
        for _worker_ordinal in range(runtime.worker_count)
    ]
    try:
        await asyncio.gather(*worker_tasks)
    except BaseException:
        for worker_task in worker_tasks:
            worker_task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        raise
    return tuple(
        sorted(
            completed_proofs,
            key=lambda proof: (proof.partition_ordinal, proof.partition_id),
        )
    )


async def materialize_projection_shards(
    lease: ProjectionLease,
    admission_id: str,
    *,
    child_stream_factory: ProjectionChildStreamFactory,
    native_runner: ProjectionNativeRunner = run_native_projection_command,
    framing_resolver: Callable[..., Awaitable[str]] = projection_shard_input_framing,
    database: Any = db,
    schema: str = "mrf",
    **materializer_options_map: Any,
) -> tuple[ProjectionProofShard, ...]:
    """Materialize all currently claimable shards with bounded independence."""

    settings = projection_materializer_settings(materializer_options_map)
    runtime = await _materializer_runtime(
        lease,
        child_stream_factory,
        native_runner,
        framing_resolver,
        database,
        schema,
        settings,
    )
    return await _run_projection_workers(lease, admission_id, runtime)


__all__ = (
    "ProjectionChildStreamFactory",
    "ProjectionFramingResolver",
    "is_projection_materializer_enabled",
    "materialize_projection_shards",
    "projection_materializer_worker_count",
    "projection_native_timeout_seconds",
)
