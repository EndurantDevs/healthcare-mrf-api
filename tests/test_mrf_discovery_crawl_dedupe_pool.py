# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

from process import mrf_source_discovery as discovery
from process.mrf_discovery_checkpoints import DatabaseDiscoveryCheckpointStore
from process.mrf_source_discovery import (
    DiscoverySourceBatchContext,
    DiscoverySourceProcessingOptions,
    SourceBatchSummary,
    SourceProcessResult,
    _execute_discovery_source_batch,
)


def _duplicate_source_record_list() -> list[dict[str, object]]:
    base_source_dict = {
        "index_url": "https://transparency-in-coverage.example.test/",
        "hosting_platform": "uhc_public_blobs",
        "status": "active",
        "metadata_json": {},
    }
    return [
        {**base_source_dict, "source_id": "source_a"},
        {**base_source_dict, "source_id": "source_b"},
    ]


def _batch_context(process_workers: int, owner_run_id: str) -> DiscoverySourceBatchContext:
    return DiscoverySourceBatchContext(
        root_run_id="run_root",
        owner_run_id=owner_run_id,
        source_records=_duplicate_source_record_list(),
        concurrency=2,
        process_workers=process_workers,
        processing_options=DiscoverySourceProcessingOptions(
            test_mode=False,
            check_urls=True,
            crawl=True,
            observation_run_id=owner_run_id,
            max_toc_bytes=1024,
            crawl_target_limit=None,
        ),
    )


def _completed_batch_summary(batch_option_dict) -> SourceBatchSummary:
    return SourceBatchSummary(
        root_run_id=batch_option_dict["root_run_id"],
        source_set_count=2,
        source_set_sha256="source-digest",
        completed_source_count=2,
        completed_source_set_sha256="source-digest",
        failed_source_count=0,
        urls_checked=2,
        plans_discovered=0,
        files_discovered=0,
        bytes_streamed=0,
    )


class _ProcessPoolCrawlProbe:
    def __init__(self) -> None:
        self.processing_by_source_id = {}
        self.is_closed = False
        self.is_terminated = False

    def construct(self, process_workers):
        assert process_workers == 2
        return self

    async def process_source(self, source_record, processing_options):
        self.processing_by_source_id[source_record["source_id"]] = (
            processing_options.check_urls,
            processing_options.crawl,
        )
        return SourceProcessResult()

    async def execute_batch(self, **batch_option_dict):
        for source_record in batch_option_dict["source_records"]:
            await batch_option_dict["process_source"](source_record)
        return _completed_batch_summary(batch_option_dict)

    async def close(self):
        self.is_closed = True

    async def terminate(self):
        self.is_terminated = True


class _PendingAliasCrawlProbe:
    def __init__(self) -> None:
        self.processing_option_list = []

    @asynccontextmanager
    async def http_session(self, **_option_dict):
        yield object()

    async def process_source(self, _source_record, processing_options, **_option_dict):
        self.processing_option_list.append(processing_options)
        return SourceProcessResult()

    async def execute_batch(self, **batch_option_dict):
        assert [
            source_record["source_id"]
            for source_record in batch_option_dict["source_records"]
        ] == ["source_a", "source_b"]
        await batch_option_dict["process_source"](
            batch_option_dict["source_records"][1]
        )
        return _completed_batch_summary(batch_option_dict)


@pytest.mark.asyncio
async def test_process_batch_checks_duplicate_sources_but_crawls_one(monkeypatch):
    probe = _ProcessPoolCrawlProbe()
    monkeypatch.setattr(discovery, "DiscoverySourceProcessPool", probe.construct)
    monkeypatch.setattr(
        discovery, "execute_checkpointed_source_batch", probe.execute_batch
    )

    await _execute_discovery_source_batch(
        _batch_context(process_workers=2, owner_run_id="run_owner"),
        DatabaseDiscoveryCheckpointStore(),
    )

    assert probe.processing_by_source_id == {
        "source_a": (True, True),
        "source_b": (True, False),
    }
    assert probe.is_closed is True
    assert probe.is_terminated is False


@pytest.mark.asyncio
async def test_retry_pending_alias_keeps_full_batch_crawl_owner(monkeypatch):
    probe = _PendingAliasCrawlProbe()
    monkeypatch.setattr(discovery, "_discovery_http_session", probe.http_session)
    monkeypatch.setattr(
        discovery, "_process_discovery_source_record", probe.process_source
    )
    monkeypatch.setattr(
        discovery, "execute_checkpointed_source_batch", probe.execute_batch
    )

    await _execute_discovery_source_batch(
        _batch_context(process_workers=1, owner_run_id="run_retry"),
        DatabaseDiscoveryCheckpointStore(),
    )

    assert len(probe.processing_option_list) == 1
    assert probe.processing_option_list[0].check_urls is True
    assert probe.processing_option_list[0].crawl is False
