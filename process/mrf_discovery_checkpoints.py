# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable source-level checkpoints for MRF catalog discovery."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from sqlalchemy import exists, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import MRFDiscoveryBatch, MRFDiscoverySourceCheckpoint, db


DISCOVERY_PROOF_VERSION = 2
SOURCE_SET_CONTRACT = "hp-mrf-discovery-source-set-v1"
SOURCE_PAYLOAD_SET_CONTRACT = "hp-mrf-discovery-source-payload-set-v1"
CHECKPOINT_STRATEGY_VERSION = "mrf-discovery-source-checkpoint-v1"
CHECKPOINT_INSERT_CHUNK_SIZE = 1_000


@dataclass(frozen=True)
class SourceProcessResult:
    """Count durable outputs produced while processing one source."""

    urls_checked: int = 0
    plans_discovered: int = 0
    files_discovered: int = 0
    bytes_streamed: int = 0


@dataclass(frozen=True)
class SourceBatchSummary:
    """Describe the persisted completion state of one frozen source batch."""

    root_run_id: str
    source_set_count: int
    source_set_sha256: str
    completed_source_count: int
    completed_source_set_sha256: str
    failed_source_count: int
    urls_checked: int
    plans_discovered: int
    files_discovered: int
    bytes_streamed: int

    @property
    def is_complete(self) -> bool:
        """Return whether every frozen source has a successful checkpoint."""

        return (
            self.completed_source_count == self.source_set_count
            and self.failed_source_count == 0
            and self.completed_source_set_sha256 == self.source_set_sha256
        )

    def proof_metrics(self) -> dict[str, Any]:
        """Return the exact proof contract consumed by catalog synchronization."""

        return {
            "discovery_proof_version": DISCOVERY_PROOF_VERSION,
            "discovery_root_run_id": self.root_run_id,
            "source_set_count": self.source_set_count,
            "source_set_sha256": self.source_set_sha256,
            "completed_source_count": self.completed_source_count,
            "completed_source_set_sha256": self.completed_source_set_sha256,
            "failed_source_count": self.failed_source_count,
        }


@dataclass
class SourceBatchProgress:
    """Track source completion callbacks without nested-scope mutation."""

    completed_source_count: int
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class DiscoverySourceBatchIncomplete(RuntimeError):
    """Report a frozen source batch that has not completed exactly once."""

    def __init__(self, summary: SourceBatchSummary) -> None:
        self.summary = summary
        super().__init__(
            "MRF discovery source batch is incomplete: "
            f"completed={summary.completed_source_count}/"
            f"{summary.source_set_count}, failed={summary.failed_source_count}"
        )


class DiscoverySourceBatchMismatch(RuntimeError):
    """Report persisted checkpoint data that violates its frozen digest."""


class DiscoveryCheckpointStoreProtocol(Protocol):
    """Define persistence required by the source checkpoint executor."""

    async def initialize_batch(
        self,
        root_run_id: str,
        owner_run_id: str,
        source_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Freeze or validate the exact source records for a root run."""
        raise NotImplementedError

    async def resume_batch(
        self,
        root_run_id: str,
        owner_run_id: str,
        retry_of_run_id: str,
    ) -> list[dict[str, Any]] | None:
        """Adopt an existing batch for its direct retry child."""
        raise NotImplementedError

    async def pending_sources(self, root_run_id: str) -> list[dict[str, Any]]:
        """Return frozen sources that do not have successful checkpoints."""
        raise NotImplementedError

    async def is_source_claimed(
        self, root_run_id: str, source_id: str, owner_run_id: str
    ) -> bool:
        """Claim a pending source for the current batch owner."""
        raise NotImplementedError

    async def is_source_lease_renewed(
        self, root_run_id: str, source_id: str, owner_run_id: str
    ) -> bool:
        """Renew source ownership when the caller still owns the batch."""
        raise NotImplementedError

    async def is_source_completed(
        self,
        root_run_id: str,
        source_id: str,
        owner_run_id: str,
        source_result: SourceProcessResult,
    ) -> bool:
        """Persist a successful source checkpoint under owner fencing."""
        raise NotImplementedError

    async def is_source_failed(
        self,
        root_run_id: str,
        source_id: str,
        owner_run_id: str,
        error: BaseException,
    ) -> bool:
        """Persist a failed source checkpoint under owner fencing."""
        raise NotImplementedError

    async def summarize_batch(
        self, root_run_id: str, owner_run_id: str
    ) -> SourceBatchSummary:
        """Aggregate persisted checkpoints into the final source proof."""
        raise NotImplementedError


def source_set_sha256(source_ids: list[str] | tuple[str, ...] | set[str]) -> str:
    """Digest canonical source IDs using the catalog synchronization contract."""

    normalized_source_ids = sorted(
        {
            str(source_id).strip()
            for source_id in source_ids
            if str(source_id).strip()
        }
    )
    digest_payload = json.dumps(
        {
            "contract": SOURCE_SET_CONTRACT,
            "source_ids": normalized_source_ids,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(digest_payload).hexdigest()


def _source_payload_set_sha256(
    frozen_sources: list[tuple[str, dict[str, Any], str]],
) -> str:
    digest_payload = json.dumps(
        {
            "contract": SOURCE_PAYLOAD_SET_CONTRACT,
            "sources": [
                [source_id, payload_digest]
                for source_id, _source_payload, payload_digest in frozen_sources
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(digest_payload).hexdigest()


def _checkpoint_lease_seconds() -> int:
    configured_seconds = int(
        os.getenv("HLTHPRT_MRF_DISCOVERY_CHECKPOINT_LEASE_SECONDS", "900")
    )
    return max(60, configured_seconds)


def _checkpoint_lease_deadline(now: dt.datetime) -> dt.datetime:
    return now + dt.timedelta(seconds=_checkpoint_lease_seconds())


def _checkpoint_heartbeat_seconds() -> float:
    configured_seconds = float(
        os.getenv("HLTHPRT_MRF_DISCOVERY_CHECKPOINT_HEARTBEAT_SECONDS", "30")
    )
    return max(5.0, min(configured_seconds, _checkpoint_lease_seconds() / 3))


def _json_default(value: Any) -> str:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported source payload value: {type(value).__name__}")


def _frozen_source_payload(source_record: dict[str, Any]) -> dict[str, Any]:
    return json.loads(
        json.dumps(
            source_record,
            default=_json_default,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _source_payload_sha256(source_payload: dict[str, Any]) -> str:
    encoded_payload = json.dumps(
        source_payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded_payload).hexdigest()


def _unique_source_payloads(
    source_records: list[dict[str, Any]],
) -> list[tuple[str, dict[str, Any], str]]:
    payloads_by_source_id: dict[str, tuple[dict[str, Any], str]] = {}
    for source_record in source_records:
        source_id = str(source_record.get("source_id") or "").strip()
        if not source_id:
            raise ValueError("MRF discovery source record is missing source_id")
        source_payload = _frozen_source_payload(source_record)
        payload_digest = _source_payload_sha256(source_payload)
        existing_payload = payloads_by_source_id.get(source_id)
        if existing_payload and existing_payload[1] != payload_digest:
            raise DiscoverySourceBatchMismatch(
                f"conflicting payloads for MRF discovery source {source_id}"
            )
        payloads_by_source_id[source_id] = (source_payload, payload_digest)
    return [
        (source_id, *payloads_by_source_id[source_id])
        for source_id in sorted(payloads_by_source_id)
    ]


async def _insert_source_checkpoints(
    session: Any,
    checkpoint_values: list[dict[str, Any]],
) -> None:
    """Insert frozen checkpoints without exceeding driver bind limits."""

    for chunk_start in range(0, len(checkpoint_values), CHECKPOINT_INSERT_CHUNK_SIZE):
        checkpoint_chunk = checkpoint_values[
            chunk_start : chunk_start + CHECKPOINT_INSERT_CHUNK_SIZE
        ]
        checkpoint_statement = (
            pg_insert(MRFDiscoverySourceCheckpoint)
            .values(checkpoint_chunk)
            .on_conflict_do_nothing(index_elements=["root_run_id", "source_id"])
        )
        await session.execute(checkpoint_statement)


class DatabaseDiscoveryCheckpointStore:
    """Persist frozen source batches and retry-safe source checkpoints."""

    async def initialize_batch(
        self,
        root_run_id: str,
        owner_run_id: str,
        source_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Freeze a new batch or validate an idempotent reservation."""

        frozen_sources = _unique_source_payloads(source_records)
        source_ids = [source_id for source_id, _, _ in frozen_sources]
        source_digest = source_set_sha256(source_ids)
        payload_set_digest = _source_payload_set_sha256(frozen_sources)
        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        batch_values_by_column = {
            "root_run_id": root_run_id,
            "latest_run_id": owner_run_id,
            "retry_of_run_id": None,
            "strategy_version": CHECKPOINT_STRATEGY_VERSION,
            "status": "running",
            "source_set_count": len(source_ids),
            "source_set_sha256": source_digest,
            "source_payload_set_sha256": payload_set_digest,
            "completed_source_count": 0,
            "failed_source_count": 0,
            "urls_checked": 0,
            "plans_discovered": 0,
            "files_discovered": 0,
            "bytes_streamed": 0,
            "lease_expires_at": _checkpoint_lease_deadline(now),
            "started_at": now,
            "updated_at": now,
        }
        checkpoint_values = [
            {
                "root_run_id": root_run_id,
                "source_id": source_id,
                "owner_run_id": owner_run_id,
                "status": "pending",
                "source_payload": source_payload,
                "source_payload_sha256": payload_digest,
                "lease_expires_at": None,
                "attempt_count": 0,
                "urls_checked": 0,
                "plans_discovered": 0,
                "files_discovered": 0,
                "bytes_streamed": 0,
                "updated_at": now,
            }
            for source_id, source_payload, payload_digest in frozen_sources
        ]
        async with db.session() as session:
            reservation_statement = (
                pg_insert(MRFDiscoveryBatch)
                .values(**batch_values_by_column)
                .on_conflict_do_nothing(index_elements=["root_run_id"])
                .returning(MRFDiscoveryBatch.root_run_id)
            )
            reservation_result = await session.execute(reservation_statement)
            reserved_root_run_id = reservation_result.scalar_one_or_none()
            if reserved_root_run_id:
                await _insert_source_checkpoints(session, checkpoint_values)
            existing_batch = await session.get(MRFDiscoveryBatch, root_run_id)
            if existing_batch is None:
                raise DiscoverySourceBatchMismatch(
                    f"MRF discovery batch reservation was lost: {root_run_id}"
                )
            self._validate_batch_identity(
                existing_batch,
                source_count=len(source_ids),
                source_digest=source_digest,
                payload_set_digest=payload_set_digest,
            )
            if existing_batch.latest_run_id != owner_run_id:
                raise DiscoverySourceBatchMismatch(
                    f"MRF discovery batch is owned by another run: {root_run_id}"
                )
        return await self._load_validated_source_records(root_run_id)

    async def resume_batch(
        self,
        root_run_id: str,
        owner_run_id: str,
        retry_of_run_id: str,
    ) -> list[dict[str, Any]] | None:
        """Transfer unfinished checkpoints to one direct retry child."""

        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        async with db.session() as session:
            existing_batch = await session.get(MRFDiscoveryBatch, root_run_id)
            if existing_batch is None:
                return None
            if existing_batch.latest_run_id != retry_of_run_id:
                raise DiscoverySourceBatchMismatch(
                    "MRF discovery retry is not the direct child of the batch owner"
                )
            stale_owner_exists = await session.scalar(
                select(
                    exists().where(
                        MRFDiscoverySourceCheckpoint.root_run_id == root_run_id,
                        MRFDiscoverySourceCheckpoint.status != "succeeded",
                        MRFDiscoverySourceCheckpoint.owner_run_id != retry_of_run_id,
                    )
                )
            )
            if stale_owner_exists:
                raise DiscoverySourceBatchMismatch(
                    "MRF discovery source checkpoint ownership does not match retry lineage"
                )
            adoption_statement = (
                update(MRFDiscoveryBatch)
                .where(MRFDiscoveryBatch.root_run_id == root_run_id)
                .where(MRFDiscoveryBatch.latest_run_id == retry_of_run_id)
                .values(
                    latest_run_id=owner_run_id,
                    retry_of_run_id=retry_of_run_id,
                    status="running",
                    lease_expires_at=_checkpoint_lease_deadline(now),
                    updated_at=now,
                    completed_at=None,
                )
            )
            adoption_result = await session.execute(adoption_statement)
            if int(adoption_result.rowcount or 0) != 1:
                raise DiscoverySourceBatchMismatch(
                    "MRF discovery batch retry ownership was lost"
                )
            checkpoint_adoption_statement = (
                update(MRFDiscoverySourceCheckpoint)
                .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
                .where(MRFDiscoverySourceCheckpoint.status != "succeeded")
                .where(MRFDiscoverySourceCheckpoint.owner_run_id == retry_of_run_id)
                .values(
                    owner_run_id=owner_run_id,
                    status="pending",
                    lease_expires_at=None,
                    error=None,
                    updated_at=now,
                    completed_at=None,
                )
            )
            await session.execute(checkpoint_adoption_statement)
        return await self._load_validated_source_records(root_run_id)

    async def pending_sources(self, root_run_id: str) -> list[dict[str, Any]]:
        """Load unfinished frozen source payloads in stable order."""

        checkpoint_query = (
            select(MRFDiscoverySourceCheckpoint)
            .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
            .where(MRFDiscoverySourceCheckpoint.status != "succeeded")
            .order_by(MRFDiscoverySourceCheckpoint.source_id)
        )
        async with db.session() as session:
            checkpoint_result = await session.execute(checkpoint_query)
            checkpoint_records = checkpoint_result.scalars().all()
        return [dict(checkpoint.source_payload or {}) for checkpoint in checkpoint_records]

    async def is_source_claimed(
        self, root_run_id: str, source_id: str, owner_run_id: str
    ) -> bool:
        """Claim one unfinished source when its lease permits execution."""

        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        owned_batch_exists = exists().where(
            MRFDiscoveryBatch.root_run_id == root_run_id,
            MRFDiscoveryBatch.latest_run_id == owner_run_id,
            MRFDiscoveryBatch.status == "running",
        )
        claim_statement = (
            update(MRFDiscoverySourceCheckpoint)
            .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
            .where(MRFDiscoverySourceCheckpoint.source_id == source_id)
            .where(MRFDiscoverySourceCheckpoint.status != "succeeded")
            .where(
                or_(
                    MRFDiscoverySourceCheckpoint.status.in_(("pending", "failed")),
                    MRFDiscoverySourceCheckpoint.lease_expires_at.is_(None),
                    MRFDiscoverySourceCheckpoint.lease_expires_at <= now,
                )
            )
            .where(owned_batch_exists)
            .values(
                owner_run_id=owner_run_id,
                status="running",
                attempt_count=MRFDiscoverySourceCheckpoint.attempt_count + 1,
                error=None,
                started_at=now,
                updated_at=now,
                completed_at=None,
                lease_expires_at=_checkpoint_lease_deadline(now),
            )
        )
        async with db.session() as session:
            claim_result = await session.execute(claim_statement)
            if int(claim_result.rowcount or 0) == 1:
                await self._renew_batch_lease(session, root_run_id, owner_run_id, now)
        return int(claim_result.rowcount or 0) == 1

    async def is_source_lease_renewed(
        self, root_run_id: str, source_id: str, owner_run_id: str
    ) -> bool:
        """Renew one running source and its parent batch lease."""

        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        renewal_statement = (
            update(MRFDiscoverySourceCheckpoint)
            .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
            .where(MRFDiscoverySourceCheckpoint.source_id == source_id)
            .where(MRFDiscoverySourceCheckpoint.owner_run_id == owner_run_id)
            .where(MRFDiscoverySourceCheckpoint.status == "running")
            .values(
                lease_expires_at=_checkpoint_lease_deadline(now),
                updated_at=now,
            )
        )
        async with db.session() as session:
            renewal_result = await session.execute(renewal_statement)
            if int(renewal_result.rowcount or 0) == 1:
                await self._renew_batch_lease(session, root_run_id, owner_run_id, now)
        return int(renewal_result.rowcount or 0) == 1

    async def is_source_completed(
        self,
        root_run_id: str,
        source_id: str,
        owner_run_id: str,
        source_result: SourceProcessResult,
    ) -> bool:
        """Mark one owned running source successful with durable counters."""

        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        completion_statement = (
            update(MRFDiscoverySourceCheckpoint)
            .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
            .where(MRFDiscoverySourceCheckpoint.source_id == source_id)
            .where(MRFDiscoverySourceCheckpoint.owner_run_id == owner_run_id)
            .where(MRFDiscoverySourceCheckpoint.status == "running")
            .where(
                exists().where(
                    MRFDiscoveryBatch.root_run_id == root_run_id,
                    MRFDiscoveryBatch.latest_run_id == owner_run_id,
                )
            )
            .values(
                status="succeeded",
                urls_checked=source_result.urls_checked,
                plans_discovered=source_result.plans_discovered,
                files_discovered=source_result.files_discovered,
                bytes_streamed=source_result.bytes_streamed,
                error=None,
                updated_at=now,
                completed_at=now,
                lease_expires_at=None,
            )
        )
        async with db.session() as session:
            completion_result = await session.execute(completion_statement)
            if int(completion_result.rowcount or 0) == 1:
                await self._renew_batch_lease(session, root_run_id, owner_run_id, now)
        return int(completion_result.rowcount or 0) == 1

    async def is_source_failed(
        self,
        root_run_id: str,
        source_id: str,
        owner_run_id: str,
        error: BaseException,
    ) -> bool:
        """Mark one owned running source failed with bounded diagnostics."""

        now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
        error_message = str(error).strip() or error.__class__.__name__
        failure_statement = (
            update(MRFDiscoverySourceCheckpoint)
            .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
            .where(MRFDiscoverySourceCheckpoint.source_id == source_id)
            .where(MRFDiscoverySourceCheckpoint.owner_run_id == owner_run_id)
            .where(MRFDiscoverySourceCheckpoint.status == "running")
            .where(
                exists().where(
                    MRFDiscoveryBatch.root_run_id == root_run_id,
                    MRFDiscoveryBatch.latest_run_id == owner_run_id,
                )
            )
            .values(
                status="failed",
                error={
                    "code": "source_checkpoint_failed",
                    "type": error.__class__.__name__,
                    "message": error_message[:2000],
                },
                updated_at=now,
                completed_at=now,
                lease_expires_at=None,
            )
        )
        async with db.session() as session:
            failure_result = await session.execute(failure_statement)
            if int(failure_result.rowcount or 0) == 1:
                await self._renew_batch_lease(session, root_run_id, owner_run_id, now)
        return int(failure_result.rowcount or 0) == 1

    async def summarize_batch(
        self, root_run_id: str, owner_run_id: str
    ) -> SourceBatchSummary:
        """Build and persist an owner-fenced repeatable-read completion proof."""

        checkpoint_query = (
            select(MRFDiscoverySourceCheckpoint)
            .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
            .order_by(MRFDiscoverySourceCheckpoint.source_id)
        )
        async with db.session() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            existing_batch = await session.get(MRFDiscoveryBatch, root_run_id)
            if existing_batch is None:
                raise DiscoverySourceBatchMismatch(
                    f"MRF discovery batch {root_run_id} does not exist"
                )
            if existing_batch.latest_run_id != owner_run_id:
                raise DiscoverySourceBatchMismatch(
                    "MRF discovery batch ownership changed before final proof"
                )
            checkpoint_result = await session.execute(checkpoint_query)
            checkpoint_records = checkpoint_result.scalars().all()
            completed_records = [
                checkpoint
                for checkpoint in checkpoint_records
                if checkpoint.status == "succeeded"
            ]
            failed_count = sum(
                checkpoint.status == "failed" for checkpoint in checkpoint_records
            )
            summary = SourceBatchSummary(
                root_run_id=root_run_id,
                source_set_count=existing_batch.source_set_count,
                source_set_sha256=existing_batch.source_set_sha256,
                completed_source_count=len(completed_records),
                completed_source_set_sha256=source_set_sha256(
                    [checkpoint.source_id for checkpoint in completed_records]
                ),
                failed_source_count=failed_count,
                urls_checked=sum(checkpoint.urls_checked for checkpoint in completed_records),
                plans_discovered=sum(
                    checkpoint.plans_discovered for checkpoint in completed_records
                ),
                files_discovered=sum(
                    checkpoint.files_discovered for checkpoint in completed_records
                ),
                bytes_streamed=sum(
                    checkpoint.bytes_streamed for checkpoint in completed_records
                ),
            )
            now = dt.datetime.now(dt.UTC).replace(tzinfo=None)
            existing_batch.status = (
                "succeeded"
                if summary.is_complete
                else "failed"
                if summary.failed_source_count
                else "running"
            )
            existing_batch.completed_source_count = summary.completed_source_count
            existing_batch.failed_source_count = summary.failed_source_count
            existing_batch.urls_checked = summary.urls_checked
            existing_batch.plans_discovered = summary.plans_discovered
            existing_batch.files_discovered = summary.files_discovered
            existing_batch.bytes_streamed = summary.bytes_streamed
            existing_batch.updated_at = now
            existing_batch.completed_at = now if summary.is_complete else None
            existing_batch.lease_expires_at = None if summary.is_complete else now
        return summary

    @staticmethod
    async def _renew_batch_lease(
        session: Any,
        root_run_id: str,
        owner_run_id: str,
        now: dt.datetime,
    ) -> None:
        renewal_statement = (
            update(MRFDiscoveryBatch)
            .where(MRFDiscoveryBatch.root_run_id == root_run_id)
            .where(MRFDiscoveryBatch.latest_run_id == owner_run_id)
            .where(MRFDiscoveryBatch.status == "running")
            .values(
                lease_expires_at=_checkpoint_lease_deadline(now),
                updated_at=now,
            )
        )
        renewal_result = await session.execute(renewal_statement)
        if int(renewal_result.rowcount or 0) != 1:
            raise DiscoverySourceBatchMismatch(
                "MRF discovery batch lease ownership was lost"
            )

    async def _load_validated_source_records(
        self, root_run_id: str
    ) -> list[dict[str, Any]]:
        checkpoint_query = (
            select(MRFDiscoverySourceCheckpoint)
            .where(MRFDiscoverySourceCheckpoint.root_run_id == root_run_id)
            .order_by(MRFDiscoverySourceCheckpoint.source_id)
        )
        async with db.session() as session:
            existing_batch = await session.get(MRFDiscoveryBatch, root_run_id)
            if existing_batch is None:
                raise DiscoverySourceBatchMismatch(
                    f"MRF discovery batch {root_run_id} does not exist"
                )
            checkpoint_result = await session.execute(checkpoint_query)
            checkpoint_records = checkpoint_result.scalars().all()
        source_records: list[dict[str, Any]] = []
        for checkpoint in checkpoint_records:
            source_payload_by_field = dict(checkpoint.source_payload or {})
            if (
                _source_payload_sha256(source_payload_by_field)
                != checkpoint.source_payload_sha256
            ):
                raise DiscoverySourceBatchMismatch(
                    f"MRF discovery source payload digest mismatch: {checkpoint.source_id}"
                )
            source_records.append(source_payload_by_field)
        self._validate_batch_identity(
            existing_batch,
            source_count=len(source_records),
            source_digest=source_set_sha256(
                [str(source_record.get("source_id") or "") for source_record in source_records]
            ),
            payload_set_digest=_source_payload_set_sha256(
                [
                    (
                        checkpoint.source_id,
                        dict(checkpoint.source_payload or {}),
                        checkpoint.source_payload_sha256,
                    )
                    for checkpoint in checkpoint_records
                ]
            ),
        )
        return source_records

    @staticmethod
    def _validate_batch_identity(
        batch: MRFDiscoveryBatch,
        *,
        source_count: int,
        source_digest: str,
        payload_set_digest: str,
    ) -> None:
        if (
            batch.strategy_version != CHECKPOINT_STRATEGY_VERSION
            or batch.source_set_count != source_count
            or batch.source_set_sha256 != source_digest
            or batch.source_payload_set_sha256 != payload_set_digest
        ):
            raise DiscoverySourceBatchMismatch(
                f"MRF discovery batch source-set mismatch: {batch.root_run_id}"
            )


async def execute_checkpointed_source_batch(
    *,
    root_run_id: str,
    owner_run_id: str,
    source_records: list[dict[str, Any]],
    concurrency: int,
    process_source: Callable[[dict[str, Any]], Awaitable[SourceProcessResult]],
    checkpoint_store: DiscoveryCheckpointStoreProtocol,
    on_source_checkpoint: Callable[[int, int, str, str], None] | None = None,
) -> SourceBatchSummary:
    """Process unfinished sources and prove completion of the frozen batch."""

    await checkpoint_store.initialize_batch(root_run_id, owner_run_id, source_records)
    pending_source_records = await checkpoint_store.pending_sources(root_run_id)
    total_source_count = len(source_records)
    source_progress = SourceBatchProgress(
        completed_source_count=total_source_count - len(pending_source_records)
    )
    worker_count = min(max(1, int(concurrency)), max(1, len(pending_source_records)))
    source_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    for source_record in pending_source_records:
        source_queue.put_nowait(source_record)
    for _worker_index in range(worker_count):
        source_queue.put_nowait(None)

    async def process_pending_sources() -> None:
        """Drain unfinished sources while preserving independent failures."""

        while True:
            source_record = await source_queue.get()
            try:
                if source_record is None:
                    return
                source_id = str(source_record.get("source_id") or "").strip()
                is_claimed = await checkpoint_store.is_source_claimed(
                    root_run_id, source_id, owner_run_id
                )
                if not is_claimed:
                    continue
                try:
                    source_result = await _process_source_with_lease_heartbeat(
                        root_run_id=root_run_id,
                        owner_run_id=owner_run_id,
                        source_id=source_id,
                        source_record=source_record,
                        process_source=process_source,
                        checkpoint_store=checkpoint_store,
                    )
                    is_completed = await checkpoint_store.is_source_completed(
                        root_run_id,
                        source_id,
                        owner_run_id,
                        source_result,
                    )
                    if not is_completed:
                        raise RuntimeError(
                            f"lost MRF discovery source checkpoint claim: {source_id}"
                        )
                    checkpoint_status = "succeeded"
                except asyncio.CancelledError as error:
                    await checkpoint_store.is_source_failed(
                        root_run_id, source_id, owner_run_id, error
                    )
                    raise
                except Exception as error:  # keep independent sources progressing
                    await checkpoint_store.is_source_failed(
                        root_run_id, source_id, owner_run_id, error
                    )
                    checkpoint_status = "failed"
                async with source_progress.lock:
                    source_progress.completed_source_count += 1
                    if on_source_checkpoint is not None:
                        on_source_checkpoint(
                            source_progress.completed_source_count,
                            total_source_count,
                            source_id,
                            checkpoint_status,
                        )
            finally:
                source_queue.task_done()

    source_workers = [
        asyncio.create_task(process_pending_sources()) for _worker_index in range(worker_count)
    ]
    try:
        await asyncio.gather(*source_workers)
    finally:
        for source_worker in source_workers:
            if not source_worker.done():
                source_worker.cancel()
        await asyncio.gather(*source_workers, return_exceptions=True)

    batch_summary = await checkpoint_store.summarize_batch(root_run_id, owner_run_id)
    if not batch_summary.is_complete:
        raise DiscoverySourceBatchIncomplete(batch_summary)
    return batch_summary


async def _process_source_with_lease_heartbeat(
    *,
    root_run_id: str,
    owner_run_id: str,
    source_id: str,
    source_record: dict[str, Any],
    process_source: Callable[[dict[str, Any]], Awaitable[SourceProcessResult]],
    checkpoint_store: DiscoveryCheckpointStoreProtocol,
) -> SourceProcessResult:
    """Keep source ownership live until its durable writes finish."""

    async def renew_lease() -> None:
        """Renew the source claim until processing reaches durable completion."""

        while True:
            await asyncio.sleep(_checkpoint_heartbeat_seconds())
            is_renewed = await checkpoint_store.is_source_lease_renewed(
                root_run_id, source_id, owner_run_id
            )
            if not is_renewed:
                raise RuntimeError(
                    f"lost MRF discovery source checkpoint lease: {source_id}"
                )

    processing_task = asyncio.create_task(process_source(source_record))
    heartbeat_task = asyncio.create_task(renew_lease())
    try:
        completed_tasks, _pending_tasks = await asyncio.wait(
            {processing_task, heartbeat_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if heartbeat_task in completed_tasks:
            processing_task.cancel()
            await asyncio.gather(processing_task, return_exceptions=True)
            await heartbeat_task
        return await processing_task
    finally:
        if not heartbeat_task.done():
            heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
