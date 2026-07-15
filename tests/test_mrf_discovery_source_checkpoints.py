# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from collections import Counter
from typing import Any

import pytest

from process.mrf_discovery_checkpoints import (
    DiscoverySourceBatchIncomplete,
    DiscoverySourceBatchMismatch,
    SourceBatchSummary,
    SourceProcessResult,
    execute_checkpointed_source_batch,
    source_set_sha256,
)


class InMemoryDiscoveryCheckpointStore:
    def __init__(self) -> None:
        self.root_run_id: str | None = None
        self.latest_run_id: str | None = None
        self.source_records: dict[str, dict[str, Any]] = {}
        self.checkpoints: dict[str, dict[str, Any]] = {}

    async def initialize_batch(
        self,
        root_run_id: str,
        owner_run_id: str,
        source_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        source_records_by_id = {
            str(source_record["source_id"]): dict(source_record)
            for source_record in source_records
        }
        if self.root_run_id is None:
            self.root_run_id = root_run_id
            self.latest_run_id = owner_run_id
            self.source_records = source_records_by_id
            self.checkpoints = {
                source_id: {
                    "status": "pending",
                    "owner_run_id": owner_run_id,
                    "result": SourceProcessResult(),
                }
                for source_id in source_records_by_id
            }
        elif (
            self.root_run_id != root_run_id
            or self.latest_run_id != owner_run_id
            or self.source_records != source_records_by_id
        ):
            raise DiscoverySourceBatchMismatch("in-memory batch identity mismatch")
        return list(self.source_records.values())

    async def resume_batch(
        self,
        root_run_id: str,
        owner_run_id: str,
        retry_of_run_id: str,
    ) -> list[dict[str, Any]] | None:
        if self.root_run_id != root_run_id:
            return None
        if self.latest_run_id != retry_of_run_id:
            raise DiscoverySourceBatchMismatch("retry is not a direct child")
        self.latest_run_id = owner_run_id
        for checkpoint in self.checkpoints.values():
            if checkpoint["status"] != "succeeded":
                checkpoint["owner_run_id"] = owner_run_id
                checkpoint["status"] = "pending"
        return list(self.source_records.values())

    async def pending_sources(self, root_run_id: str) -> list[dict[str, Any]]:
        assert root_run_id == self.root_run_id
        return [
            self.source_records[source_id]
            for source_id, checkpoint in sorted(self.checkpoints.items())
            if checkpoint["status"] != "succeeded"
        ]

    async def is_source_claimed(
        self, root_run_id: str, source_id: str, owner_run_id: str
    ) -> bool:
        checkpoint = self.checkpoints[source_id]
        if (
            root_run_id != self.root_run_id
            or owner_run_id != self.latest_run_id
            or checkpoint["status"] == "succeeded"
        ):
            return False
        checkpoint["owner_run_id"] = owner_run_id
        checkpoint["status"] = "running"
        return True

    async def is_source_lease_renewed(
        self, root_run_id: str, source_id: str, owner_run_id: str
    ) -> bool:
        checkpoint = self.checkpoints[source_id]
        return bool(
            root_run_id == self.root_run_id
            and owner_run_id == self.latest_run_id
            and checkpoint["owner_run_id"] == owner_run_id
            and checkpoint["status"] == "running"
        )

    async def is_source_completed(
        self,
        root_run_id: str,
        source_id: str,
        owner_run_id: str,
        source_result: SourceProcessResult,
    ) -> bool:
        checkpoint = self.checkpoints[source_id]
        if not await self.is_source_lease_renewed(
            root_run_id, source_id, owner_run_id
        ):
            return False
        checkpoint["status"] = "succeeded"
        checkpoint["result"] = source_result
        return True

    async def is_source_failed(
        self,
        root_run_id: str,
        source_id: str,
        owner_run_id: str,
        _error: BaseException,
    ) -> bool:
        checkpoint = self.checkpoints[source_id]
        if not await self.is_source_lease_renewed(
            root_run_id, source_id, owner_run_id
        ):
            return False
        checkpoint["status"] = "failed"
        return True

    async def summarize_batch(
        self, root_run_id: str, owner_run_id: str
    ) -> SourceBatchSummary:
        assert root_run_id == self.root_run_id
        if owner_run_id != self.latest_run_id:
            raise DiscoverySourceBatchMismatch("summary owner changed")
        completed_source_ids = sorted(
            source_id
            for source_id, checkpoint in self.checkpoints.items()
            if checkpoint["status"] == "succeeded"
        )
        completed_results = [
            checkpoint["result"]
            for checkpoint in self.checkpoints.values()
            if checkpoint["status"] == "succeeded"
        ]
        all_source_ids = sorted(self.source_records)
        return SourceBatchSummary(
            root_run_id=root_run_id,
            source_set_count=len(all_source_ids),
            source_set_sha256=source_set_sha256(all_source_ids),
            completed_source_count=len(completed_source_ids),
            completed_source_set_sha256=source_set_sha256(completed_source_ids),
            failed_source_count=sum(
                checkpoint["status"] == "failed"
                for checkpoint in self.checkpoints.values()
            ),
            urls_checked=sum(
                source_result.urls_checked for source_result in completed_results
            ),
            plans_discovered=sum(
                source_result.plans_discovered
                for source_result in completed_results
            ),
            files_discovered=sum(
                source_result.files_discovered
                for source_result in completed_results
            ),
            bytes_streamed=sum(
                source_result.bytes_streamed
                for source_result in completed_results
            ),
        )


def _source_records() -> list[dict[str, Any]]:
    return [
        {"source_id": source_id, "index_url": f"https://example.test/{source_id}"}
        for source_id in ("source_alpha", "source_beta", "source_gamma")
    ]


def test_source_set_digest_matches_catalog_sync_contract():
    canonical_payload = json.dumps(
        {
            "contract": "hp-mrf-discovery-source-set-v1",
            "source_ids": ["source_alpha", "source_beta"],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")

    assert source_set_sha256(
        [" source_beta ", "source_alpha", "source_alpha"]
    ) == hashlib.sha256(canonical_payload).hexdigest()


@pytest.mark.asyncio
async def test_retry_processes_only_the_failed_source_and_proves_full_set():
    checkpoint_store = InMemoryDiscoveryCheckpointStore()
    processing_counts: Counter[str] = Counter()
    source_ids_to_fail_once_set = {"source_beta"}

    async def process_source(source_record: dict[str, Any]) -> SourceProcessResult:
        source_id = str(source_record["source_id"])
        processing_counts[source_id] += 1
        if source_id in source_ids_to_fail_once_set:
            source_ids_to_fail_once_set.remove(source_id)
            raise RuntimeError("simulated source interruption")
        return SourceProcessResult(
            urls_checked=1,
            plans_discovered=2,
            files_discovered=3,
            bytes_streamed=4,
        )

    with pytest.raises(DiscoverySourceBatchIncomplete) as first_failure:
        await execute_checkpointed_source_batch(
            root_run_id="run_root",
            owner_run_id="run_root",
            source_records=_source_records(),
            concurrency=2,
            process_source=process_source,
            checkpoint_store=checkpoint_store,
        )
    assert first_failure.value.summary.completed_source_count == 2
    assert first_failure.value.summary.failed_source_count == 1

    resumed_sources = await checkpoint_store.resume_batch(
        "run_root", "run_retry", "run_root"
    )
    assert resumed_sources == _source_records()
    final_summary = await execute_checkpointed_source_batch(
        root_run_id="run_root",
        owner_run_id="run_retry",
        source_records=resumed_sources,
        concurrency=2,
        process_source=process_source,
        checkpoint_store=checkpoint_store,
    )

    assert processing_counts == {
        "source_alpha": 1,
        "source_beta": 2,
        "source_gamma": 1,
    }
    assert final_summary.is_complete
    assert final_summary.completed_source_count == 3
    assert final_summary.failed_source_count == 0
    assert final_summary.source_set_sha256 == final_summary.completed_source_set_sha256
    assert final_summary.urls_checked == 3
    assert final_summary.plans_discovered == 6
    assert final_summary.files_discovered == 9
    assert final_summary.bytes_streamed == 12
    assert final_summary.proof_metrics()["discovery_proof_version"] == 2


@pytest.mark.asyncio
async def test_retry_rejects_stale_owner_completion():
    checkpoint_store = InMemoryDiscoveryCheckpointStore()
    await checkpoint_store.initialize_batch(
        "run_root", "run_root", _source_records()
    )
    assert await checkpoint_store.is_source_claimed(
        "run_root", "source_alpha", "run_root"
    )
    await checkpoint_store.resume_batch("run_root", "run_retry", "run_root")

    assert not await checkpoint_store.is_source_completed(
        "run_root",
        "source_alpha",
        "run_root",
        SourceProcessResult(urls_checked=1),
    )


@pytest.mark.asyncio
async def test_retry_requires_direct_batch_owner_lineage():
    checkpoint_store = InMemoryDiscoveryCheckpointStore()
    await checkpoint_store.initialize_batch(
        "run_root", "run_root", _source_records()
    )

    with pytest.raises(DiscoverySourceBatchMismatch, match="direct child"):
        await checkpoint_store.resume_batch(
            "run_root", "run_unrelated", "run_unknown"
        )
