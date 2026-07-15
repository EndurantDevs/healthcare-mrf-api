# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import pytest

from process import mrf_source_discovery as discovery
from process.mrf_discovery_checkpoints import SourceBatchSummary, source_set_sha256


def _completed_summary() -> SourceBatchSummary:
    source_digest = source_set_sha256(["source_alpha"])
    return SourceBatchSummary(
        root_run_id="run_root",
        source_set_count=1,
        source_set_sha256=source_digest,
        completed_source_count=1,
        completed_source_set_sha256=source_digest,
        failed_source_count=0,
        urls_checked=1,
        plans_discovered=2,
        files_discovered=3,
        bytes_streamed=4,
    )


@pytest.mark.asyncio
async def test_retry_uses_frozen_source_set_and_publishes_exact_proof(monkeypatch):
    """Reuse the frozen source set and emit the exact completion proof."""

    source_records = [
        {
            "source_id": "source_alpha",
            "payer_id": "payer_alpha",
            "status": "active",
            "index_url": "https://example.test/index.json",
            "metadata_json": {"discovery_run_id": "run_root"},
        }
    ]
    captured_by_key: dict[str, Any] = {}

    class FakeCheckpointStore:
        async def resume_batch(
            self, root_run_id: str, owner_run_id: str, retry_of_run_id: str
        ):
            captured_by_key["resume"] = (
                root_run_id,
                owner_run_id,
                retry_of_run_id,
            )
            return source_records

    async def no_op_async(*_args, **_kwargs):
        return None

    async def fail_if_candidates_load(*_args, **_kwargs):
        raise AssertionError("retry reloaded provider candidates")

    async def capture_retag(records, run_id):
        captured_by_key["retag"] = (records, run_id)

    async def capture_execution(**kwargs):
        captured_by_key["execution"] = kwargs
        return _completed_summary()

    @asynccontextmanager
    async def fake_http_session(**_kwargs):
        yield object()

    monkeypatch.setattr(discovery, "DatabaseDiscoveryCheckpointStore", FakeCheckpointStore)
    monkeypatch.setattr(discovery, "init_db", no_op_async)
    monkeypatch.setattr(discovery, "ensure_database", no_op_async)
    monkeypatch.setattr(discovery, "_ensure_catalog_tables", no_op_async)
    monkeypatch.setattr(discovery, "push_objects", no_op_async)
    monkeypatch.setattr(discovery, "_load_candidates", fail_if_candidates_load)
    monkeypatch.setattr(discovery, "_retag_sources_for_discovery_run", capture_retag)
    monkeypatch.setattr(
        discovery, "execute_checkpointed_source_batch", capture_execution
    )
    monkeypatch.setattr(discovery, "_discovery_http_session", fake_http_session)
    monkeypatch.setattr(discovery, "enqueue_live_progress", lambda **_kwargs: None)

    discovery_summary = await discovery.main(
        provider="master-list",
        check_urls=True,
        crawl=True,
        run_id="run_retry",
        retry_of_run_id="run_parent",
        mrf_discovery_root_run_id="run_root",
    )

    assert captured_by_key["resume"] == ("run_root", "run_retry", "run_parent")
    assert captured_by_key["retag"] == (source_records, "run_retry")
    assert captured_by_key["execution"]["source_records"] == source_records
    assert captured_by_key["execution"]["root_run_id"] == "run_root"
    assert captured_by_key["execution"]["owner_run_id"] == "run_retry"
    assert discovery_summary["discovery_proof_version"] == 2
    assert discovery_summary["source_set_count"] == 1
    assert discovery_summary["completed_source_count"] == 1
    assert discovery_summary["failed_source_count"] == 0
    assert (
        discovery_summary["source_set_sha256"]
        == discovery_summary["completed_source_set_sha256"]
    )
    assert discovery_summary["plans"] == 2
    assert discovery_summary["files"] == 3


@pytest.mark.asyncio
async def test_unbounded_discovery_rejects_empty_source_set(monkeypatch):
    captured_by_key: dict[str, Any] = {}

    async def no_op_async(*_args, **_kwargs):
        return None

    async def empty_candidates(*_args, **_kwargs):
        return []

    async def empty_rows(*_args, **_kwargs):
        return [], []

    async def capture_failure(failure_context):
        captured_by_key["failure"] = failure_context

    monkeypatch.setattr(discovery, "init_db", no_op_async)
    monkeypatch.setattr(discovery, "ensure_database", no_op_async)
    monkeypatch.setattr(discovery, "_ensure_catalog_tables", no_op_async)
    monkeypatch.setattr(discovery, "push_objects", no_op_async)
    monkeypatch.setattr(discovery, "_load_candidates", empty_candidates)
    monkeypatch.setattr(discovery, "_store_candidates", empty_rows)
    monkeypatch.setattr(discovery, "_record_failed_discovery_state", capture_failure)
    monkeypatch.setattr(discovery, "enqueue_live_progress", lambda **_kwargs: None)

    with pytest.raises(
        RuntimeError,
        match="unbounded MRF discovery resolved an empty source set",
    ):
        await discovery.main(
            provider="master-list",
            check_urls=True,
            crawl=True,
            run_id="run_empty",
        )

    assert (
        captured_by_key["failure"]["error_dict"]["code"]
        == "source_discovery_failed"
    )
