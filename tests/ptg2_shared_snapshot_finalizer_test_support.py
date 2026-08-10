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

def _patch_disabled_v4_publication(monkeypatch):
    prepared_price = object()

    @asynccontextmanager
    async def transaction():
        yield object()

    state = SimpleNamespace(
        prepared_price=prepared_price,
        prepare_mock=AsyncMock(return_value=(prepared_price, 0.0, None, None)),
        publish_v3_mock=AsyncMock(return_value="v3-publication"),
        compile_v4_mock=AsyncMock(),
        publish_v4_mock=AsyncMock(),
        cleanup_mock=AsyncMock(),
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(shared_snapshot_publish.db, "transaction", transaction)
    replacements_by_name = {
        "touch_shared_layout_build": AsyncMock(),
        "_prepare_price_with_early_finalizer": state.prepare_mock,
        "_publish_prepared_shared_layout": state.publish_v3_mock,
        "compile_provider_graph_v4_rust": state.compile_v4_mock,
        "_publish_v4_graph": state.publish_v4_mock,
        "cleanup_prepared_shared_price_artifacts": state.cleanup_mock,
    }
    for name, replacement in replacements_by_name.items():
        monkeypatch.setattr(shared_snapshot_publish, name, replacement)
    return state

def _early_finalizer_callbacks(state, prepared_price):
    async def prepare_price(*, price_key_ready, **_kwargs):
        price_key_ready(
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        await state.atom_release.wait()
        return prepared_price

    async def run_finalizer(**kwargs):
        state.finalizer_calls.append(kwargs)
        state.finalizer_started.set()
        await state.finalizer_release.wait()
        return {"blocks": {}}

    async def publish_price(prepared):
        assert prepared is prepared_price
        state.price_publish_started.set()
        await state.price_publish_release.wait()
        return "published-price"

    return prepare_price, run_finalizer, publish_price


def _install_early_finalizer_mocks(monkeypatch, tmp_path, state, prepared_price):
    prepare_price, run_finalizer, publish_price = _early_finalizer_callbacks(
        state,
        prepared_price,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_shared_price_artifacts",
        prepare_price,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "export_shared_price_key_map",
        AsyncMock(return_value=tmp_path / "price-key-map.copy"),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "run_v3_direct_finalizer",
        run_finalizer,
    )
    return publish_price


def _assert_early_finalizer_pipeline(pipeline_output, state, prepared_price):
    prepared, prepare_seconds, prepared_finalizer, price_publication = pipeline_output
    assert prepared is prepared_price
    assert prepare_seconds >= 0
    assert prepared_finalizer.summary == {"blocks": {}}
    assert prepared_finalizer.price_key_map_export_seconds >= 0
    assert prepared_finalizer.finalizer_seconds >= 0
    assert state.finalizer_calls[0]["price_key_map_row_count"] == 3
    assert state.finalizer_calls[0]["scratch_durability"] == (
        shared_snapshot_publish.PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY
    )
    assert price_publication.publication == "published-price"
    assert price_publication.publish_seconds >= 0

def _install_cleanup_cancellation_mocks(
    monkeypatch,
    transaction,
    prepare_price,
    publish_prepared,
    cleanup_prepared,
):
    prepare = AsyncMock(side_effect=prepare_price)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(shared_snapshot_publish.db, "transaction", transaction)
    monkeypatch.setattr(
        shared_snapshot_publish,
        "touch_shared_layout_build",
        AsyncMock(),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_shared_price_artifacts",
        prepare,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "export_shared_price_key_map",
        AsyncMock(return_value="price-key-map.copy"),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "run_v3_direct_finalizer",
        AsyncMock(return_value={"blocks": {}}),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "publish_shared_price_artifacts",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "_publish_prepared_shared_layout",
        publish_prepared,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "cleanup_prepared_shared_price_artifacts",
        cleanup_prepared,
    )
    return prepare


def _strict_shared_layout_arguments():
    return {
        "schema_name": "mrf",
        "manifest_stage_table": "manifest_stage",
        "reserved_snapshot_key": 7,
        "build_token": "build-token",
        "expected_coverage_scope_id": b"c" * 32,
        "logical_snapshot_id": "snapshot-id",
        "expected_source_identities": (),
        "serving_run_entries": (),
        "code_dictionary_entries": (),
        "provider_set_metadata_entries": (),
        "source_audit_witness_entries": (),
        "expected_raw_source_sha256": (),
        "graph_artifact_entries": (),
        "provider_identifier_quarantine": {},
    }


async def _cancel_publisher_after_cleanup_starts(
    publish_task,
    state,
    cancel_during_publication,
):
    await asyncio.wait_for(state.publication_started.wait(), timeout=0.5)
    if cancel_during_publication:
        publish_task.cancel()
    await asyncio.wait_for(state.cleanup_started.wait(), timeout=0.5)
    publish_task.cancel()
    await asyncio.sleep(0)
    publish_task.cancel()
    await asyncio.sleep(0)
    assert not publish_task.done()
    state.cleanup_release.set()
    with pytest.raises(asyncio.CancelledError):
        await publish_task
