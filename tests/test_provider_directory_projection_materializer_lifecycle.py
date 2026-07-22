# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded worker, cancellation, crash, and retry orchestration proofs."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

import process.provider_directory_projection_materializer as materializer
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_lifecycle_support import (
    Database,
    HeartbeatCommitRace,
    LifecycleFakes,
    ParallelRunner,
    assert_transaction_settings_precede_stage,
    event_index,
    native_result,
    sibling_child,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)


@pytest.mark.asyncio
async def test_four_workers_are_isolated_and_bound_native_parallelism(
    monkeypatch,
) -> None:
    """Four shards use isolated children/transactions and bounded native threads."""

    base_child = synthetic_projection_context("ndjson").child_lease
    child_leases = tuple(
        sibling_child(base_child, child_ordinal)
        for child_ordinal in range(4)
    )
    lifecycle_events: list[tuple] = []
    database = Database(lifecycle_events)
    stream_factory, framing_resolver = LifecycleFakes(
        child_leases,
        lifecycle_events,
    ).install(monkeypatch)
    parallel_runner = ParallelRunner(lifecycle_events)

    shard_proofs = await materializer.materialize_projection_shards(
        base_child.shard_claim.recipe_lease,
        base_child.shard_claim.admission_id,
        child_stream_factory=stream_factory,
        native_runner=parallel_runner,
        framing_resolver=framing_resolver,
        database=database,
        enabled=True,
        max_workers=4,
        native_threads=2,
        heartbeat_seconds=10,
    )

    assert parallel_runner.maximum_active_count == 4
    assert len({coordinate[0] for coordinate in parallel_runner.coordinates}) == 4
    assert len({coordinate[1] for coordinate in parallel_runner.coordinates}) == 4
    assert len({coordinate[2] for coordinate in parallel_runner.coordinates}) == 4
    assert {coordinate[3:] for coordinate in parallel_runner.coordinates} == {(4, 2)}
    assert [proof.partition_ordinal for proof in shard_proofs] == [0, 1, 2, 3]
    assert_transaction_settings_precede_stage(lifecycle_events)
    for child_lease in child_leases:
        partition_id = child_lease.shard_claim.shard.partition_id
        assert event_index(
            lifecycle_events,
            "verify_child",
            partition_id,
        ) < event_index(
            lifecycle_events,
            "complete_shard",
            partition_id,
        ) < event_index(
            lifecycle_events,
            "release_child",
            partition_id,
        )


def _failing_worker_task(
    child_lease,
    stream_factory,
    framing_resolver,
    database,
    native_runner,
):
    return asyncio.create_task(
        materializer.materialize_projection_shards(
            child_lease.shard_claim.recipe_lease,
            child_lease.shard_claim.admission_id,
            child_stream_factory=stream_factory,
            native_runner=native_runner,
            framing_resolver=framing_resolver,
            database=database,
            enabled=True,
            max_workers=1,
            native_threads=1,
            heartbeat_seconds=10,
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ("crash", "cancel"))
async def test_worker_failure_rolls_back_and_releases_child(
    monkeypatch,
    failure_kind,
) -> None:
    """Crash and cancellation roll back before releasing an unverified child."""

    child_lease = synthetic_projection_context("ndjson").child_lease
    lifecycle_events: list[tuple] = []
    database = Database(lifecycle_events)
    stream_factory, framing_resolver = LifecycleFakes(
        (child_lease,),
        lifecycle_events,
    ).install(monkeypatch)
    runner_started = asyncio.Event()

    async def failing_runner(claim, _child, _stage, _stream, **_options):
        lifecycle_events.append(("runner_start", claim.shard.partition_id))
        runner_started.set()
        if failure_kind == "crash":
            raise RuntimeError("synthetic-native-crash")
        await asyncio.Event().wait()
        raise AssertionError("cancelled runner resumed")

    worker_task = _failing_worker_task(
        child_lease,
        stream_factory,
        framing_resolver,
        database,
        failing_runner,
    )
    await runner_started.wait()
    if failure_kind == "cancel":
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task
    else:
        with pytest.raises(RuntimeError, match="synthetic-native-crash"):
            await worker_task

    partition_id = child_lease.shard_claim.shard.partition_id
    assert any(event[0] == "transaction_rollback" for event in lifecycle_events)
    assert ("release_child", partition_id) in lifecycle_events
    assert ("verify_child", partition_id) not in lifecycle_events
    assert ("complete_shard", partition_id) not in lifecycle_events


async def _run_crashing_attempt(
    child_lease,
    stream_factory,
    framing_resolver,
    database,
) -> None:
    async def crash(*_args, **_options):
        raise RuntimeError("synthetic-first-attempt-crash")

    with pytest.raises(RuntimeError, match="synthetic-first-attempt-crash"):
        await materializer.materialize_projection_shards(
            child_lease.shard_claim.recipe_lease,
            child_lease.shard_claim.admission_id,
            child_stream_factory=stream_factory,
            native_runner=crash,
            framing_resolver=framing_resolver,
            database=database,
            enabled=True,
            max_workers=1,
            native_threads=1,
            heartbeat_seconds=10,
        )


async def _run_successful_retry(
    child_lease,
    stream_factory,
    framing_resolver,
    database,
):
    async def succeed(_claim, claimed_child, _stage, _stream, **_options):
        return replace(
            native_result(claimed_child, "retry"),
            native_thread_count=1,
            materializer_worker_count=1,
        )

    (shard_proof,) = await materializer.materialize_projection_shards(
        child_lease.shard_claim.recipe_lease,
        child_lease.shard_claim.admission_id,
        child_stream_factory=stream_factory,
        native_runner=succeed,
        framing_resolver=framing_resolver,
        database=database,
        enabled=True,
        max_workers=1,
        native_threads=1,
        heartbeat_seconds=10,
    )
    return shard_proof


@pytest.mark.asyncio
async def test_expired_retry_uses_new_generations_and_completes_once(
    monkeypatch,
) -> None:
    """A retry gets new fenced generations and is the only completed attempt."""

    first_child = synthetic_projection_context("ndjson").child_lease
    retry_child = sibling_child(first_child, 0, partition_attempt=2)
    lifecycle_events: list[tuple] = []
    database = Database(lifecycle_events)
    first_stream, first_framing = LifecycleFakes(
        (first_child,),
        lifecycle_events,
    ).install(monkeypatch)
    await _run_crashing_attempt(
        first_child,
        first_stream,
        first_framing,
        database,
    )
    retry_stream, retry_framing = LifecycleFakes(
        (retry_child,),
        lifecycle_events,
    ).install(monkeypatch)
    shard_proof = await _run_successful_retry(
        retry_child,
        retry_stream,
        retry_framing,
        database,
    )

    assert shard_proof.partition_attempt == 2
    assert first_child.child_lease_token != retry_child.child_lease_token
    prepare_attempts = [
        event[2] for event in lifecycle_events if event[0] == "prepare_stage"
    ]
    assert prepare_attempts == [1, 2]
    assert sum(event[0] == "complete_shard" for event in lifecycle_events) == 1


@pytest.mark.asyncio
async def test_heartbeat_finishes_before_durable_stage_and_shard_commit(
    monkeypatch,
) -> None:
    """Native completion cannot turn a harmless final heartbeat into failure."""

    child_lease = synthetic_projection_context("ndjson").child_lease
    lifecycle_events: list[tuple] = []
    database = Database(lifecycle_events)
    stream_factory, framing_resolver = LifecycleFakes(
        (child_lease,),
        lifecycle_events,
    ).install(monkeypatch)
    heartbeat_race = HeartbeatCommitRace(lifecycle_events)
    monkeypatch.setattr(
        materializer,
        "heartbeat_projection_lease",
        heartbeat_race.heartbeat,
    )

    (shard_proof,) = await materializer.materialize_projection_shards(
        child_lease.shard_claim.recipe_lease,
        child_lease.shard_claim.admission_id,
        child_stream_factory=stream_factory,
        native_runner=heartbeat_race.runner,
        framing_resolver=framing_resolver,
        database=database,
        enabled=True,
        max_workers=1,
        native_threads=1,
        heartbeat_seconds=0.001,
    )

    event_names = [event[0] for event in lifecycle_events]
    assert shard_proof.partition_id == child_lease.shard_claim.shard.partition_id
    assert event_names.index("heartbeat_race_finish") < event_names.index(
        "assert_stage"
    ) < event_names.index("complete_shard")


def test_native_result_requires_full_copy_and_matching_proof_count() -> None:
    """In-memory result counts must agree before the DB census is consulted."""

    child_lease = synthetic_projection_context("ndjson").child_lease
    native_projection_result = native_result(child_lease, "strict-result")
    materializer._validate_native_result(
        native_projection_result,
        child_lease,
        4,
        2,
    )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_result_mismatch",
    ):
        materializer._validate_native_result(
            replace(native_projection_result, copied_record_count=0),
            child_lease,
            4,
            2,
        )
    with pytest.raises(
        ProviderDirectoryProjectionError,
        match="provider_directory_projection_native_result_mismatch",
    ):
        materializer._validate_native_result(
            replace(
                native_projection_result,
                shard_proof=replace(
                    native_projection_result.shard_proof,
                    resource_count=native_projection_result.record_count - 1,
                ),
            ),
            child_lease,
            4,
            2,
        )
