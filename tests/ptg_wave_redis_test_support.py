# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from typing import Any

from process.ptg_wave_redis import (
    PTG_SMALL_WAVE_SLOTS,
    PTGSmallWaveRuntimeIdentity,
    build_ptg_small_wave_manifest,
    register_ptg_small_wave_slot,
)


EXECUTION_DIGEST = "a" * 64
CONFIG_IDENTITY = "b" * 64
KUBERNETES_MANIFEST_IDENTITY = "c" * 64
IMAGE_IDENTITY = "registry.example/ptg-worker@sha256:" + "d" * 64
RUNTIME_IMAGE_IDENTITY = "sha256:" + "e" * 64
RUNTIME_IDENTITY = PTGSmallWaveRuntimeIdentity(
    config_identity=CONFIG_IDENTITY,
    kubernetes_manifest_identity=KUBERNETES_MANIFEST_IDENTITY,
    image_identity=IMAGE_IDENTITY,
    runtime_image_identity=RUNTIME_IMAGE_IDENTITY,
)


class FakePubSub:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.channels: set[str] = set()
        self.messages: list[dict[str, Any]] = []
        self.closed = False

    async def subscribe(self, *channels: str) -> None:
        self.channels.update(channels)
        self.redis.pubsubs.append(self)

    async def unsubscribe(self, *channels: str) -> None:
        self.channels.difference_update(channels)

    async def aclose(self) -> None:
        self.closed = True

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool,
        timeout: float | None,
    ) -> dict[str, Any] | None:
        del ignore_subscribe_messages, timeout
        return self.messages.pop(0) if self.messages else None


class FakePipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.watched_versions: dict[str, int] = {}
        self.in_multi = False
        self.commands: list[tuple[str, tuple[Any, ...]]] = []
        self.read_commands: list[Callable[[], Any]] = []

    async def __aenter__(self) -> "FakePipeline":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def watch(self, *keys: str) -> None:
        self.watched_versions = {
            key: self.redis.versions[key]
            for key in keys
        }

    def multi(self) -> None:
        self.in_multi = True

    def _read_or_queue(self, command: Callable[[], Any]):
        if self.watched_versions:
            async def execute_now():
                return command()

            return execute_now()
        self.read_commands.append(command)
        return self

    def hgetall(self, key: str):
        return self._read_or_queue(lambda: dict(self.redis.hashes[key]))

    def get(self, key: str):
        return self._read_or_queue(lambda: self.redis.read_string(key))

    def exists(self, *keys: str):
        return self._read_or_queue(
            lambda: sum(self.redis.has_key(key) for key in keys)
        )

    def zcard(self, key: str):
        return self._read_or_queue(lambda: len(self.redis.zsets[key]))

    def zrange(
        self,
        key: str,
        start: int,
        stop: int,
        *,
        withscores: bool,
    ):
        assert start == 0 and stop == -1 and withscores is True
        self.redis.zrange_reads.append(key)
        return self._read_or_queue(
            lambda: [
                (job_id.encode(), score)
                for job_id, score in sorted(
                    self.redis.zsets[key].items(),
                    key=lambda member: (member[1], member[0]),
                )
            ]
        )

    def mget(self, keys: tuple[str, ...]):
        return self._read_or_queue(
            lambda: [self.redis.values.get(key) for key in keys]
        )

    def zadd(self, key: str, mapping: dict[str, int]) -> "FakePipeline":
        return self._queue_write("zadd", key, dict(mapping))

    def set(self, key: str, stored_value: bytes) -> "FakePipeline":
        return self._queue_write("set", key, stored_value)

    def hset(
        self,
        key: str,
        field: str,
        stored_value: bytes,
    ) -> "FakePipeline":
        return self._queue_write("hset", key, field, stored_value)

    def publish(self, channel: str, message: bytes) -> "FakePipeline":
        return self._queue_write("publish", channel, message)

    def delete(self, *keys: str) -> "FakePipeline":
        return self._queue_write("delete", *keys)

    def _queue_write(self, command: str, *arguments: Any) -> "FakePipeline":
        assert self.in_multi
        self.commands.append((command, arguments))
        return self

    async def execute(self, *, raise_on_error: bool = True) -> list[Any]:
        if self.read_commands:
            read_results = [read_command() for read_command in self.read_commands]
            if raise_on_error:
                for read_result in read_results:
                    if isinstance(read_result, BaseException):
                        raise read_result
            return read_results
        self._raise_for_watch_conflict()
        self.redis.transactions.append(list(self.commands))
        command_results = [
            self._apply_write(command, arguments)
            for command, arguments in self.commands
        ]
        if self.redis.raise_after_execute:
            self.redis.raise_after_execute = False
            raise TimeoutError("simulated ambiguous EXEC outcome")
        return command_results

    def _raise_for_watch_conflict(self) -> None:
        from redis.exceptions import WatchError

        if self.watched_versions and self.redis.watch_failures_remaining:
            self.redis.watch_failures_remaining -= 1
            raise WatchError("simulated unexecuted WATCH conflict")
        if any(
            self.redis.versions[key] != version
            for key, version in self.watched_versions.items()
        ):
            raise WatchError("watched key changed")

    def _apply_write(self, command: str, arguments: tuple[Any, ...]) -> Any:
        handler_by_command = {
            "zadd": self._apply_zadd,
            "set": self._apply_set,
            "hset": self._apply_hset,
            "publish": self._apply_publish,
            "delete": self._apply_delete,
        }
        try:
            handler = handler_by_command[command]
        except KeyError as exc:
            raise AssertionError(f"unknown fake command {command}") from exc
        return handler(*arguments)

    def _apply_zadd(self, key: str, mapping: dict[str, int]) -> int:
        self.redis.zsets[key].update(mapping)
        self.redis.bump(key)
        return len(mapping)

    def _apply_set(self, key: str, stored_value: bytes) -> int:
        self.redis.values[key] = stored_value
        self.redis.bump(key)
        return 1

    def _apply_hset(self, key: str, field: str, stored_value: bytes) -> int:
        self.redis.hashes[key][field] = stored_value
        self.redis.bump(key)
        return 1

    def _apply_publish(self, channel: str, message: bytes) -> int:
        receiver_count = 0
        for pubsub in self.redis.pubsubs:
            if channel in pubsub.channels:
                pubsub.messages.append({"type": "message", "data": message})
                receiver_count += 1
        return receiver_count

    def _apply_delete(self, *keys: str) -> int:
        deleted_count = 0
        for key in keys:
            has_existing_key = self.redis.has_key(key)
            self.redis.values.pop(key, None)
            self.redis.hashes.pop(key, None)
            self.redis.zsets.pop(key, None)
            if has_existing_key:
                deleted_count += 1
                self.redis.bump(key)
        return deleted_count


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.hashes: defaultdict[str, dict[str, bytes]] = defaultdict(dict)
        self.zsets: defaultdict[str, dict[str, int]] = defaultdict(dict)
        self.versions: defaultdict[str, int] = defaultdict(int)
        self.transactions: list[list[tuple[str, tuple[Any, ...]]]] = []
        self.pubsubs: list[FakePubSub] = []
        self.zrange_reads: list[str] = []
        self.get_reads: list[str] = []
        self.watch_failures_remaining = 0
        self.raise_after_execute = False
        self.aclose_calls = 0

    def pipeline(self, *, transaction: bool) -> FakePipeline:
        assert transaction is True
        return FakePipeline(self)

    def pubsub(self) -> FakePubSub:
        return FakePubSub(self)

    async def get(self, key: str) -> bytes | None:
        scalar = self.read_string(key)
        if isinstance(scalar, BaseException):
            raise scalar
        return scalar

    async def aclose(self) -> None:
        self.aclose_calls += 1

    def has_key(self, key: str) -> bool:
        return (
            key in self.values
            or bool(self.hashes.get(key))
            or bool(self.zsets.get(key))
        )

    def read_string(self, key: str) -> bytes | BaseException | None:
        from redis.exceptions import ResponseError

        self.get_reads.append(key)
        if bool(self.hashes.get(key)) or bool(self.zsets.get(key)):
            return ResponseError("WRONGTYPE simulated non-string key")
        return self.values.get(key)

    def bump(self, key: str) -> None:
        self.versions[key] += 1


def payloads(count: int = 12) -> list[dict[str, Any]]:
    return [
        {
            "run_id": f"wave-run-{ordinal:02d}",
            "params": {
                "plan_ids": [f"plan-{ordinal:02d}"],
                "test_mode": True,
            },
        }
        for ordinal in range(count)
    ]


def job_ids(count: int) -> tuple[str, ...]:
    return tuple(f"durable-job-{ordinal:04d}" for ordinal in range(count))


def manifest(
    count: int = 12,
    *,
    execution_digest: str = EXECUTION_DIGEST,
    ordered_job_ids: tuple[str, ...] | None = None,
):
    return build_ptg_small_wave_manifest(
        payloads(count),
        execution_digest=execution_digest,
        job_ids=job_ids(count) if ordered_job_ids is None else ordered_job_ids,
        enqueue_time_ms=1_700_000_000_000,
        runtime_identity=RUNTIME_IDENTITY,
    )


async def register_all(redis: FakeRedis, wave_manifest: Any) -> None:
    for slot in PTG_SMALL_WAVE_SLOTS:
        await register_ptg_small_wave_slot(
            redis,
            wave_manifest.reference,
            slot=slot,
            pod_uid=f"pod-{slot:02d}",
        )
