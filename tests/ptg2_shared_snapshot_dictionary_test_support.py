# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import struct
from collections import defaultdict
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis

from process import live_progress
from process.ptg_parts import ptg2_shared_snapshot_publish as shared_snapshot_publish
from process.ptg_parts import ptg2_shared_publish as shared_publish
from process.ptg_parts import live_progress as ptg_live_progress
from process.ptg_parts import ptg2_v4_graph_compiler as graph_compiler
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_price import PreparedSharedPriceKeyMap
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
    _validate_authoritative_mapping_summary,
)

class _SlowProgressDriver:
    def __init__(self, elapsed_seconds):
        self.elapsed_seconds = elapsed_seconds

    async def copy_to_table(self, _target_table, *, source, **_kwargs):
        while True:
            self.elapsed_seconds[0] += 1.0
            if not source.read(1):
                return


class _SlowProgressAcquire:
    def __init__(self, elapsed_seconds):
        self.elapsed_seconds = elapsed_seconds

    async def __aenter__(self):
        return SimpleNamespace(
            raw_connection=SimpleNamespace(
                driver_connection=_SlowProgressDriver(self.elapsed_seconds)
            )
        )

    async def __aexit__(self, *_exc_info):
        return False


def _install_shared_publish_progress_capture(monkeypatch, elapsed_seconds):
    progress_writes = []
    base_time = datetime.datetime(2026, 7, 23, 11, 0, 0)
    fake_redis = AtomicLiveProgressRedis(
        on_progress_write=lambda _key, _ttl, encoded_progress: progress_writes.append(
            (elapsed_seconds[0], json.loads(encoded_progress))
        )
    )
    monkeypatch.setattr(
        shared_publish.db,
        "acquire",
        lambda: _SlowProgressAcquire(elapsed_seconds),
    )
    monkeypatch.setattr(live_progress, "_redis", lambda: fake_redis)
    monkeypatch.setattr(
        live_progress,
        "_utc_now",
        lambda: base_time + datetime.timedelta(seconds=elapsed_seconds[0]),
    )
    monkeypatch.setattr(live_progress, "enqueue_status_event", lambda _event: None)
    return progress_writes


def _publish_progress_to_live(stage_name, counters_by_name):
    ptg_live_progress.write_live_progress(
        phase=f"publishing: {stage_name}",
        unit="publish_steps",
        done=5,
        total=8,
        pct=96,
        counters=dict(counters_by_name),
    )


def _assert_shared_publish_cadence(progress_writes):
    progress_snapshots = [
        progress_snapshot for _at, progress_snapshot in progress_writes
    ]
    progress_gaps = [
        later_at - earlier_at
        for (earlier_at, _earlier), (later_at, _later) in zip(
            progress_writes,
            progress_writes[1:],
        )
    ]
    assert [
        progress_snapshot["counters"]["copy_bytes"]
        for progress_snapshot in progress_snapshots
    ] == [4, 8, 12]
    assert max(progress_gaps) <= 4.0
    assert [
        progress_snapshot["progress_seq"] for progress_snapshot in progress_snapshots
    ] == [1, 2, 3]


class _DenseDictionaryRangeDriver:
    def __init__(self, elapsed_seconds):
        self.elapsed_seconds = elapsed_seconds
        self.statements = []

    async def execute_range_statement(self, statement, parameters):
        self.elapsed_seconds[0] += 4.0
        self.statements.append((str(statement), dict(parameters)))
        range_start = int(parameters["range_start"])
        range_end = int(parameters["range_end"])
        if str(statement).lstrip().startswith("SELECT COUNT"):
            row_count = range_end - range_start
            return SimpleNamespace(
                one=lambda: (
                    row_count,
                    range_start,
                    range_end - 1,
                    True,
                    0,
                )
            )
        return SimpleNamespace()

    async def read_range_scalar(self, statement, parameters):
        if str(statement).lstrip().startswith("SELECT COUNT"):
            return int(parameters["range_end"]) - int(parameters["range_start"])
        return None


def _row_result(rows):
    return SimpleNamespace(all=lambda: tuple(rows))


def _dense_dictionary_progress_stage():
    return shared_snapshot_publish._V4DenseDictionaryStage(
        stage_table="group_stage",
        key_name="provider_group_key",
        expected_count=200_001,
        target_table="ptg2_v3_provider_group",
        columns=("provider_group_key", "provider_group_global_id_128"),
        value_predicate="octet_length(provider_group_global_id_128) = 16",
    )


def _assert_dense_dictionary_range_statements(statements):
    range_statements = [
        statement for statement, _parameters in statements if "range_start" in statement
    ]
    assert len(range_statements) == 6
    assert all(">= :range_start" in statement for statement in range_statements)
    assert all("< :range_end" in statement for statement in range_statements)
    assert not any("COUNT(DISTINCT" in statement for statement in range_statements)
