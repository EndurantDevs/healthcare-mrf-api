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

from tests.ptg2_shared_snapshot_finalizer_test_support import (
    _assert_early_finalizer_pipeline,
    _cancel_publisher_after_cleanup_starts,
    _early_finalizer_callbacks,
    _install_cleanup_cancellation_mocks,
    _install_early_finalizer_mocks,
    _patch_disabled_v4_publication,
    _strict_shared_layout_arguments,
)

@pytest.mark.asyncio
async def test_v4_disabled_publication_keeps_v3_path(
    monkeypatch,
) -> None:
    """Leave the reviewed V3 publication path independent of V4 evidence."""

    state = _patch_disabled_v4_publication(monkeypatch)

    publication = await shared_snapshot_publish.publish_strict_shared_v3_layout(
        schema_name="mrf",
        manifest_stage_table="manifest-stage",
        reserved_snapshot_key=7,
        build_token="token",
        expected_coverage_scope_id=b"c" * 32,
        logical_snapshot_id="snapshot",
        expected_source_identities=(),
        serving_run_entries=(),
        code_dictionary_entries=(),
        provider_set_metadata_entries=(),
        source_audit_witness_entries=(),
        expected_raw_source_sha256=(),
        graph_artifact_entries=(),
        provider_identifier_quarantine={},
        provider_graph_v4=False,
    )

    assert publication == "v3-publication"
    assert state.publish_v3_mock.await_args.kwargs["provider_graph_v4"] is False
    assert (
        state.publish_v3_mock.await_args.kwargs["compressed_acquisition_bytes"] is None
    )
    assert (
        state.publish_v3_mock.await_args.kwargs[
            "empty_npi_tin_only_normalization_count"
        ]
        is None
    )
    state.compile_v4_mock.assert_not_awaited()
    state.publish_v4_mock.assert_not_awaited()
    state.cleanup_mock.assert_awaited_once_with(state.prepared_price)


@pytest.mark.asyncio
async def test_source_witness_and_graph_gate_finalizer_db_admission():
    source_witness_finished = asyncio.Event()
    graph_started = asyncio.Event()
    graph_finished = asyncio.Event()
    lane_events: list[str] = []
    active_db_lanes: list[str] = []

    async def provider_graph() -> str:
        assert source_witness_finished.is_set()
        active_db_lanes.append("provider_graph")
        lane_events.append("provider_graph")
        graph_started.set()
        try:
            await asyncio.sleep(0)
            graph_finished.set()
            return "provider_graph"
        finally:
            active_db_lanes.remove("provider_graph")

    async def finalizer_blocks() -> str:
        assert source_witness_finished.is_set()
        await graph_started.wait()
        assert graph_finished.is_set()
        assert not active_db_lanes
        lane_events.append("finalizer_blocks")
        return "finalizer_blocks"

    async def prepublished_price() -> str:
        assert source_witness_finished.is_set()
        assert graph_finished.is_set()
        lane_events.append("prepublished_price")
        return "prepublished_price"

    async def source_witness() -> str:
        await asyncio.sleep(0)
        assert not lane_events
        source_witness_finished.set()
        return "source_witness"

    lane_outputs = await asyncio.wait_for(
        _run_independent_publication_lanes(
            finalizer_blocks=finalizer_blocks,
            provider_graph=provider_graph,
            price=prepublished_price,
            source_witness=source_witness,
        ),
        timeout=0.5,
    )

    assert lane_events[0] == "provider_graph"
    assert sorted(lane_events[1:]) == ["finalizer_blocks", "prepublished_price"]
    assert not active_db_lanes
    assert lane_outputs == (
        "finalizer_blocks",
        "provider_graph",
        "prepublished_price",
        "source_witness",
    )


@pytest.mark.asyncio
async def test_finalizer_failure_cancels_waiting_price_after_graph():
    price_started = asyncio.Event()
    price_cancelled = asyncio.Event()

    async def fail_finalizer() -> None:
        await price_started.wait()
        raise RuntimeError("synthetic finalizer failure")

    async def wait_for_price() -> None:
        price_started.set()
        try:
            await asyncio.Future()
        finally:
            price_cancelled.set()

    with pytest.raises(ExceptionGroup) as exc_info:
        await asyncio.wait_for(
            _run_independent_publication_lanes(
                finalizer_blocks=fail_finalizer,
                provider_graph=lambda: asyncio.sleep(0),
                price=wait_for_price,
                source_witness=lambda: asyncio.sleep(0),
            ),
            timeout=0.5,
        )

    assert len(exc_info.value.exceptions) == 1
    assert "synthetic finalizer failure" in str(exc_info.value.exceptions[0])
    assert price_cancelled.is_set()


@pytest.mark.asyncio
async def test_finalizer_starts_before_independent_atom_preparation_finishes(
    monkeypatch,
    tmp_path,
):
    """Start finalization and price publication at their exact dependencies."""

    prepared_price = object()
    state = SimpleNamespace(
        atom_release=asyncio.Event(),
        finalizer_started=asyncio.Event(),
        finalizer_release=asyncio.Event(),
        finalizer_calls=[],
        price_publish_started=asyncio.Event(),
        price_publish_release=asyncio.Event(),
    )
    publish_price = _install_early_finalizer_mocks(
        monkeypatch,
        tmp_path,
        state,
        prepared_price,
    )

    pipeline_task = asyncio.create_task(
        shared_snapshot_publish._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            finalizer_inputs=shared_snapshot_publish._EarlyFinalizerInputs(
                raw_work_directory=tmp_path,
                serving_run_entries=(),
                code_dictionary_entries=(),
                provider_set_metadata_entries=(),
                expected_source_identities=(),
            ),
            publish_prepared_price=publish_price,
        )
    )
    await asyncio.wait_for(state.finalizer_started.wait(), timeout=0.5)
    assert not state.atom_release.is_set()
    assert not pipeline_task.done()

    state.atom_release.set()
    await asyncio.wait_for(state.price_publish_started.wait(), timeout=0.5)
    assert not state.finalizer_release.is_set()
    state.price_publish_release.set()
    state.finalizer_release.set()
    _assert_early_finalizer_pipeline(
        await pipeline_task,
        state,
        prepared_price,
    )


@pytest.mark.asyncio
async def test_early_finalizer_failure_cleans_successful_price_preparation(
    monkeypatch,
    tmp_path,
):
    prepared_price = object()
    preparation_completed = asyncio.Event()
    cleanup = AsyncMock()

    async def prepare_price(*, price_key_ready, **_kwargs):
        price_key_ready(
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        preparation_completed.set()
        return prepared_price

    async def run_finalizer(**_kwargs):
        await preparation_completed.wait()
        raise RuntimeError("finalizer failed")

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
    monkeypatch.setattr(
        shared_snapshot_publish,
        "cleanup_prepared_shared_price_artifacts",
        cleanup,
    )

    with pytest.raises(RuntimeError, match="finalizer failed"):
        await shared_snapshot_publish._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            finalizer_inputs=shared_snapshot_publish._EarlyFinalizerInputs(
                raw_work_directory=tmp_path,
                serving_run_entries=(),
                code_dictionary_entries=(),
                provider_set_metadata_entries=(),
                expected_source_identities=(),
            ),
        )

    cleanup.assert_awaited_once_with(prepared_price)


@pytest.mark.asyncio
async def test_cancellation_before_price_key_readiness_drains_preparation(
    monkeypatch,
    tmp_path,
):
    preparation_started = asyncio.Event()
    preparation_cancelled = asyncio.Event()
    export = AsyncMock()

    async def prepare_price(**_kwargs):
        preparation_started.set()
        try:
            await asyncio.Future()
        finally:
            preparation_cancelled.set()

    monkeypatch.setattr(
        shared_snapshot_publish,
        "prepare_shared_price_artifacts",
        prepare_price,
    )
    monkeypatch.setattr(
        shared_snapshot_publish,
        "export_shared_price_key_map",
        export,
    )

    pipeline_task = asyncio.create_task(
        shared_snapshot_publish._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            finalizer_inputs=shared_snapshot_publish._EarlyFinalizerInputs(
                raw_work_directory=tmp_path,
                serving_run_entries=(),
                code_dictionary_entries=(),
                provider_set_metadata_entries=(),
                expected_source_identities=(),
            ),
        )
    )
    await preparation_started.wait()
    pipeline_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pipeline_task

    assert preparation_cancelled.is_set()
    export.assert_not_awaited()


def test_authoritative_mapping_summary_matches_bounded_lane_metadata():
    lanes = (
        SimpleNamespace(
            object_kinds=("a_kind", "b_kind"),
            mapping_count=3,
            unique_block_count=2,
            logical_byte_count=30,
        ),
        SimpleNamespace(
            object_kinds=("c_kind",),
            mapping_count=2,
            unique_block_count=2,
            logical_byte_count=20,
        ),
    )
    summary = SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=5,
        unique_block_count=4,
        entry_count=99,
        logical_byte_count=50,
        canonical_byte_count=400,
        object_kinds=("a_kind", "b_kind", "c_kind"),
    )

    _validate_authoritative_mapping_summary(summary, *lanes)


@pytest.mark.parametrize(
    ("summary_field", "summary_value"),
    [
        ("object_kinds", ("a_kind", "missing_kind")),
        ("mapping_count", 4),
        ("unique_block_count", 3),
        ("logical_byte_count", 49),
    ],
)
def test_authoritative_mapping_summary_rejects_lane_disagreement(
    summary_field,
    summary_value,
):
    summary_values_by_field = {
        "mapping_digest": b"m" * 32,
        "mapping_count": 5,
        "unique_block_count": 4,
        "entry_count": 99,
        "logical_byte_count": 50,
        "canonical_byte_count": 400,
        "object_kinds": ("a_kind", "b_kind", "c_kind"),
    }
    summary_values_by_field[summary_field] = summary_value
    summary = SharedMappingDigestSummary(**summary_values_by_field)
    lanes = (
        SimpleNamespace(
            object_kinds=("a_kind", "b_kind"),
            mapping_count=3,
            unique_block_count=2,
            logical_byte_count=30,
        ),
        SimpleNamespace(
            object_kinds=("c_kind",),
            mapping_count=2,
            unique_block_count=2,
            logical_byte_count=20,
        ),
    )

    with pytest.raises(RuntimeError, match=summary_field):
        _validate_authoritative_mapping_summary(summary, *lanes)


def test_authoritative_mapping_summary_rejects_overlapping_lane_kinds():
    summary = SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=2,
        unique_block_count=2,
        entry_count=2,
        logical_byte_count=2,
        canonical_byte_count=100,
        object_kinds=("a_kind",),
    )
    lane = SimpleNamespace(
        object_kinds=("a_kind",),
        mapping_count=1,
        unique_block_count=1,
        logical_byte_count=1,
    )

    with pytest.raises(RuntimeError, match="overlap object kinds"):
        _validate_authoritative_mapping_summary(summary, lane, lane)

@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_during_publication", [False, True])
async def test_prepared_price_cleanup_survives_repeated_cancellation_on_every_exit(
    monkeypatch,
    cancel_during_publication,
):
    """Finish prepared-price cleanup across every cancellation exit path."""

    prepared_price = SimpleNamespace(price_set_count=3)
    state = SimpleNamespace(
        publication_started=asyncio.Event(),
        cleanup_started=asyncio.Event(),
        cleanup_release=asyncio.Event(),
        cleanup_finished=asyncio.Event(),
    )

    @asynccontextmanager
    async def transaction():
        yield object()

    async def publish_prepared(**_kwargs):
        state.publication_started.set()
        if cancel_during_publication:
            await asyncio.Future()
        return object()

    async def cleanup_prepared(observed_prepared):
        assert observed_prepared is prepared_price
        state.cleanup_started.set()
        await state.cleanup_release.wait()
        state.cleanup_finished.set()

    async def prepare_price(**kwargs):
        kwargs["price_key_ready"](
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        return prepared_price

    prepare = _install_cleanup_cancellation_mocks(
        monkeypatch,
        transaction,
        prepare_price,
        publish_prepared,
        cleanup_prepared,
    )

    publish_task = asyncio.create_task(
        shared_snapshot_publish.publish_strict_shared_v3_layout(
            **_strict_shared_layout_arguments()
        )
    )
    await _cancel_publisher_after_cleanup_starts(
        publish_task,
        state,
        cancel_during_publication,
    )
    assert state.cleanup_finished.is_set()
    assert prepare.await_args.kwargs["price_set_summary_source_count"] is None
