# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic orchestration doubles for materializer lifecycle tests."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import hashlib

import process.provider_directory_projection_child_read as child_read
import process.provider_directory_projection_materializer as materializer
from process.provider_directory_projection_contract import (
    projection_input_block,
    projection_proof_shard,
    projection_shard_spec,
)
from process.provider_directory_projection_native import NativeProjectionResult
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.test_provider_directory_physical_projection import _rows


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def sibling_child(base_child, ordinal: int, *, partition_attempt: int = 1):
    """Build another valid child coordinate under the same physical recipe."""

    if ordinal == 0 and partition_attempt == 1:
        return base_child
    label = f"worker-{ordinal}"
    input_block = projection_input_block(
        upstream_artifact_id=digest(f"{label}:artifact"),
        source_object_id=digest(f"{label}:layout"),
        block_kind="ndjson",
        input_contract_id="synthetic-offline-fhir-input.v1",
        record_start=ordinal * base_child.expected_record_count,
        record_count=base_child.expected_record_count,
        content_sha256=digest(f"{label}:canonical"),
        payload_sha256=digest(f"{label}:raw"),
        payload_bytes=base_child.expected_byte_count,
        summary={"resource_count": base_child.expected_record_count},
    )
    shard = projection_shard_spec(
        recipe=base_child.shard_claim.recipe_lease.recipe,
        partition_ordinal=ordinal,
        input_block=input_block,
    )
    claim = replace(
        base_child.shard_claim,
        shard=shard,
        partition_attempt=partition_attempt,
        lease_token=digest(f"{label}:shard-lease:{partition_attempt}"),
    )
    return replace(
        base_child,
        shard_claim=claim,
        binding_id=digest(f"{label}:binding"),
        block_id=input_block.block_id,
        retained_source_item_id=digest(f"{label}:source-item"),
        retained_artifact_sha256=input_block.upstream_artifact_id,
        retained_layout_sha256=input_block.source_object_id,
        input_sha256=input_block.content_sha256,
        expected_payload_sha256=input_block.payload_sha256,
        child_generation=partition_attempt,
        child_lease_token=digest(f"{label}:child-lease:{partition_attempt}"),
    )


def shard_proof(child, label: str):
    """Build one exact shard proof for a synthetic native result."""

    proof_rows = [
        _rows(resource_type, f"{label}-{resource_ordinal}")[0]
        for resource_ordinal, resource_type in enumerate(
            child.shard_claim.recipe_lease.recipe.selected_resources
        )
    ]
    claim = child.shard_claim
    return projection_proof_shard(
        proof_rows,
        recipe=claim.recipe_lease.recipe,
        attempt=claim.recipe_lease.attempt,
        partition_ordinal=claim.shard.partition_ordinal,
        resource_type=claim.shard.resource_type,
        input_sha256=claim.shard.input_sha256,
        partition_id=claim.shard.partition_id,
        partition_attempt=claim.partition_attempt,
    )


def native_result(child, label: str, *, copied_record_count: int | None = None):
    """Build an internally consistent native result for one child."""

    proof = shard_proof(child, label)
    return NativeProjectionResult(
        byte_count=child.expected_byte_count,
        record_count=child.expected_record_count,
        input_sha256=child.input_sha256,
        payload_sha256=child.expected_payload_sha256,
        shard_proof=proof,
        native_thread_count=2,
        materializer_worker_count=4,
        copied_record_count=(
            child.expected_record_count
            if copied_record_count is None
            else copied_record_count
        ),
        timings_seconds={"synthetic_seconds": 0.01},
    )


class Connection:
    """Identify one fake acquired transaction."""

    def __init__(self, identity: int) -> None:
        self.identity = identity


class Database:
    """Record commit and rollback boundaries for fake transactions."""

    def __init__(self, events: list[tuple]) -> None:
        self.events = events
        self.connection_count = 0

    @asynccontextmanager
    async def acquire(self):
        self.connection_count += 1
        connection = Connection(self.connection_count)
        self.events.append(("transaction_begin", connection.identity))
        try:
            yield connection
        except BaseException:
            self.events.append(("transaction_rollback", connection.identity))
            raise
        else:
            self.events.append(("transaction_commit", connection.identity))


class LifecycleFakes:
    """Install isolated claims, heartbeats, streams, stage, and completion fakes."""

    def __init__(self, children, events: list[tuple]) -> None:
        self.claim_queue = [child.shard_claim for child in children]
        self.child_by_partition = {
            child.shard_claim.shard.partition_id: child for child in children
        }
        self.events = events

    async def claim_shard(self, *_args, **_options):
        await asyncio.sleep(0)
        if not self.claim_queue:
            return None
        claim = self.claim_queue.pop(0)
        self.events.append(("claim_shard", claim.shard.partition_id))
        return claim

    async def claim_child(self, claim, **_options):
        self.events.append(("claim_child", claim.shard.partition_id))
        return self.child_by_partition[claim.shard.partition_id]

    async def heartbeat_shard(self, claim, **_options) -> None:
        self.events.append(("heartbeat_shard", claim.shard.partition_id))

    async def heartbeat_recipe(self, lease, **_options) -> None:
        self.events.append(("heartbeat_recipe", lease.recipe.recipe_id))

    async def heartbeat_child(self, child, **_options) -> None:
        self.events.append(
            ("heartbeat_child", child.shard_claim.shard.partition_id)
        )

    async def verify_child(self, child, **_options) -> None:
        self.events.append(("verify_child", child.shard_claim.shard.partition_id))

    async def assert_verified(self, child, **_options) -> None:
        self.events.append(("assert_verified", child.shard_claim.shard.partition_id))

    async def release_child(self, child, **_options) -> None:
        self.events.append(("release_child", child.shard_claim.shard.partition_id))

    async def complete_shard(self, claim, proof, **_options):
        self.events.append(("complete_shard", claim.shard.partition_id))
        return proof

    async def set_synchronous_commit(self, database, value: str) -> None:
        self.events.append(
            ("set_synchronous_commit", value, database.identity)
        )

    async def set_wal_compression(self, database) -> str:
        self.events.append(("set_wal_compression", "zstd", database.identity))
        return "zstd"

    async def ensure_stage(self, *_args, **_options):
        return synthetic_projection_context("ndjson").stage

    async def prepare_stage(self, _stage, claim, *, database) -> None:
        self.events.append(
            (
                "prepare_stage",
                claim.shard.partition_id,
                claim.partition_attempt,
                database.identity,
            )
        )

    async def assert_stage(self, _stage, claim, _proof, *, database) -> None:
        self.events.append(
            ("assert_stage", claim.shard.partition_id, database.identity)
        )

    async def framing_resolver(self, _claim, **_options) -> str:
        return "ndjson"

    @asynccontextmanager
    async def stream_factory(self, child):
        partition_id = child.shard_claim.shard.partition_id
        self.events.append(("stream_open", partition_id))

        async def byte_chunks():
            yield b"synthetic-retained-bytes"

        try:
            yield byte_chunks()
        finally:
            self.events.append(("stream_close", partition_id))

    def _install_claims(self, monkeypatch) -> None:
        monkeypatch.setattr(materializer, "claim_projection_shard", self.claim_shard)
        monkeypatch.setattr(
            child_read,
            "claim_projection_child_read_lease",
            self.claim_child,
        )
        monkeypatch.setattr(
            child_read,
            "assert_verified_projection_child_read_lease",
            self.assert_verified,
        )
        monkeypatch.setattr(
            child_read,
            "release_projection_child_read_lease",
            self.release_child,
        )

    def _install_heartbeats(self, monkeypatch) -> None:
        monkeypatch.setattr(
            materializer,
            "heartbeat_projection_shard",
            self.heartbeat_shard,
        )
        monkeypatch.setattr(
            materializer,
            "heartbeat_projection_lease",
            self.heartbeat_recipe,
        )
        monkeypatch.setattr(
            materializer,
            "heartbeat_projection_child_read_lease",
            self.heartbeat_child,
        )

    def _install_stage_and_completion(self, monkeypatch) -> None:
        monkeypatch.setattr(
            materializer.projection_db,
            "set_local_projection_synchronous_commit",
            self.set_synchronous_commit,
        )
        monkeypatch.setattr(
            materializer.projection_db,
            "set_local_projection_wal_compression",
            self.set_wal_compression,
        )
        monkeypatch.setattr(
            materializer,
            "verify_projection_child_read_lease",
            self.verify_child,
        )
        monkeypatch.setattr(
            materializer,
            "complete_projection_shard",
            self.complete_shard,
        )
        monkeypatch.setattr(
            materializer,
            "ensure_projection_stage",
            self.ensure_stage,
        )
        monkeypatch.setattr(
            materializer,
            "prepare_projection_stage_partition",
            self.prepare_stage,
        )
        monkeypatch.setattr(
            materializer,
            "assert_projection_stage_partition",
            self.assert_stage,
        )

    def install(self, monkeypatch):
        """Install every fake and return the injected stream/framing callables."""

        self._install_claims(monkeypatch)
        self._install_heartbeats(monkeypatch)
        self._install_stage_and_completion(monkeypatch)
        return self.stream_factory, self.framing_resolver


class ParallelRunner:
    """Measure concurrent native calls without mutable closure state."""

    def __init__(self, events: list[tuple]) -> None:
        self.events = events
        self.all_started = asyncio.Event()
        self.active_count = 0
        self.maximum_active_count = 0
        self.coordinates: list[tuple] = []

    async def __call__(self, claim, child, _stage, _stream, **options):
        self.active_count += 1
        self.maximum_active_count = max(
            self.maximum_active_count,
            self.active_count,
        )
        self.coordinates.append(
            (
                claim.shard.partition_id,
                child.child_lease_token,
                options["transaction"].identity,
                options["materializer_workers"],
                options["native_threads"],
            )
        )
        self.events.append(("runner_start", claim.shard.partition_id))
        if self.active_count == 4:
            self.all_started.set()
        await asyncio.wait_for(self.all_started.wait(), timeout=2)
        await asyncio.sleep(0)
        self.active_count -= 1
        return native_result(child, f"worker-{claim.shard.partition_ordinal}")


class HeartbeatCommitRace:
    """Coordinate a background heartbeat ending beside native completion."""

    def __init__(self, events: list[tuple]) -> None:
        self.events = events
        self.call_count = 0
        self.background_started = asyncio.Event()
        self.background_may_finish = asyncio.Event()

    async def heartbeat(self, _lease, **_options) -> None:
        self.call_count += 1
        if self.call_count == 1:
            self.events.append(("heartbeat_initial",))
            return
        self.events.append(("heartbeat_race_start",))
        self.background_started.set()
        await self.background_may_finish.wait()
        self.events.append(("heartbeat_race_finish",))

    async def runner(self, _claim, child, _stage, _stream, **_options):
        await asyncio.wait_for(self.background_started.wait(), timeout=2)
        self.background_may_finish.set()
        return replace(
            native_result(child, "heartbeat-race"),
            native_thread_count=1,
            materializer_worker_count=1,
        )


def event_index(events, event_name: str, partition_id: str) -> int:
    """Return the first index of one partition-scoped lifecycle event."""

    return next(
        event_ordinal
        for event_ordinal, event in enumerate(events)
        if event[:2] == (event_name, partition_id)
    )


def assert_transaction_settings_precede_stage(events: list[tuple]) -> None:
    """Require durable transaction settings before every stage preparation."""

    prepare_events = [event for event in events if event[0] == "prepare_stage"]
    assert len(prepare_events) == 4
    for prepare_event in prepare_events:
        connection_identity = prepare_event[3]
        synchronous_commit_event = (
            "set_synchronous_commit",
            "on",
            connection_identity,
        )
        wal_compression_event = (
            "set_wal_compression",
            "zstd",
            connection_identity,
        )
        assert events.index(synchronous_commit_event) < events.index(
            wal_compression_event
        ) < events.index(prepare_event)


__all__ = (
    "Database",
    "HeartbeatCommitRace",
    "LifecycleFakes",
    "ParallelRunner",
    "assert_transaction_settings_precede_stage",
    "event_index",
    "native_result",
    "sibling_child",
)
