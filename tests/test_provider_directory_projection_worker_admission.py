# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-source worker admission and database-pool starvation proofs."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

import process.provider_directory_projection_materializer as materializer
from tests.provider_directory_projection_lifecycle_support import (
    LifecycleFakes,
    native_result,
    sibling_child,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)


class _PoolConnection:
    """Identify one slot in the constrained fake database pool."""

    def __init__(self, identity: int) -> None:
        self.identity = identity


class _ConstrainedPoolDatabase:
    """Expose exactly four connections and record aggregate occupancy."""

    def __init__(self, events: list[tuple], capacity: int) -> None:
        self.events = events
        self._slots = asyncio.Semaphore(capacity)
        self._connection_count = 0
        self.active_count = 0
        self.maximum_active_count = 0

    @asynccontextmanager
    async def acquire(self):
        """Acquire and transactionally release one constrained pool slot."""

        async with self._slots:
            self._connection_count += 1
            connection = _PoolConnection(self._connection_count)
            self.active_count += 1
            self.maximum_active_count = max(
                self.maximum_active_count,
                self.active_count,
            )
            self.events.append(("transaction_begin", connection.identity))
            try:
                yield connection
            except BaseException:
                self.events.append(("transaction_rollback", connection.identity))
                raise
            else:
                self.events.append(("transaction_commit", connection.identity))
            finally:
                self.active_count -= 1


class _MultiSourceLifecycleFakes(LifecycleFakes):
    """Route claims by recipe and attest framing on the held transaction."""

    def __init__(self, children_by_recipe_map, events: list[tuple]) -> None:
        child_leases = tuple(
            child
            for source_children in children_by_recipe_map.values()
            for child in source_children
        )
        super().__init__(child_leases, events)
        self._claims_by_recipe = {
            recipe_id: [child.shard_claim for child in source_children]
            for recipe_id, source_children in children_by_recipe_map.items()
        }
        self.framing_connection_by_partition: dict[str, int] = {}

    async def claim_shard(self, lease, *_args, **_options):
        await asyncio.sleep(0)
        source_claims = self._claims_by_recipe[lease.recipe.recipe_id]
        if not source_claims:
            return None
        claim = source_claims.pop(0)
        self.events.append(("claim_shard", claim.shard.partition_id))
        return claim

    async def framing_resolver(self, claim, **options) -> str:
        connection = options["database"]
        assert isinstance(connection, _PoolConnection)
        self.framing_connection_by_partition[
            claim.shard.partition_id
        ] = connection.identity
        return "ndjson"


class _ReservedCapacityRunner:
    """Hold three native workers while proving the fourth pool slot is usable."""

    def __init__(self, database, lifecycle_fakes) -> None:
        self.database = database
        self.lifecycle_fakes = lifecycle_fakes
        self.three_workers_active = asyncio.Event()
        self.active_count = 0
        self.maximum_active_count = 0
        self.control_connection_acquired = False

    async def __call__(self, claim, child, _stage, _stream, **options):
        transaction = options["transaction"]
        assert (
            self.lifecycle_fakes.framing_connection_by_partition[
                claim.shard.partition_id
            ]
            == transaction.identity
        )
        self.active_count += 1
        self.maximum_active_count = max(
            self.maximum_active_count,
            self.active_count,
        )
        if self.active_count == 3 and not self.three_workers_active.is_set():
            async with self.database.acquire():
                self.control_connection_acquired = True
            self.three_workers_active.set()
        else:
            await asyncio.wait_for(self.three_workers_active.wait(), timeout=2)
        await asyncio.sleep(0)
        self.active_count -= 1
        return replace(
            native_result(child, f"pool-{claim.shard.partition_ordinal}"),
            native_thread_count=1,
            materializer_worker_count=4,
        )


def _source_children(label: str, ordinal_offset: int):
    base_child = synthetic_projection_context(
        "ndjson",
        label=label,
    ).child_lease
    return tuple(
        sibling_child(base_child, ordinal_offset + child_ordinal)
        for child_ordinal in range(4)
    )


@pytest.mark.asyncio
async def test_two_sources_share_global_pool_admission_and_reserve_control_slot(
    monkeypatch,
) -> None:
    """Two four-worker calls finish on pool four without starving control work."""

    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "4")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_GLOBAL_WORKERS", "4")
    first_children = _source_children("pool-source-a", 0)
    second_children = _source_children("pool-source-b", 10)
    children_by_recipe_map = {
        first_children[0].shard_claim.recipe_lease.recipe.recipe_id: first_children,
        second_children[0].shard_claim.recipe_lease.recipe.recipe_id: second_children,
    }
    lifecycle_events: list[tuple] = []
    database = _ConstrainedPoolDatabase(lifecycle_events, capacity=4)
    lifecycle_fakes = _MultiSourceLifecycleFakes(
        children_by_recipe_map,
        lifecycle_events,
    )
    stream_factory, framing_resolver = lifecycle_fakes.install(monkeypatch)
    native_runner = _ReservedCapacityRunner(database, lifecycle_fakes)

    async def run_source(source_children):
        source_claim = source_children[0].shard_claim
        return await materializer.materialize_projection_shards(
            source_claim.recipe_lease,
            source_claim.admission_id,
            child_stream_factory=stream_factory,
            native_runner=native_runner,
            framing_resolver=framing_resolver,
            database=database,
            enabled=True,
            max_workers=4,
            native_threads=1,
            heartbeat_seconds=10,
        )

    source_results = await asyncio.wait_for(
        asyncio.gather(
            run_source(first_children),
            run_source(second_children),
        ),
        timeout=5,
    )

    assert [len(source_proofs) for source_proofs in source_results] == [4, 4]
    assert native_runner.maximum_active_count == 3
    assert native_runner.control_connection_acquired is True
    assert database.maximum_active_count == 4
    assert len(lifecycle_fakes.framing_connection_by_partition) == 8
