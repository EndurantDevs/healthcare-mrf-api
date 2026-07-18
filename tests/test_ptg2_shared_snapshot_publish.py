# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as shared_snapshot_publish
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_price import PreparedSharedPriceKeyMap
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
    _validate_authoritative_mapping_summary,
)


@pytest.mark.asyncio
async def test_independent_publication_lanes_start_together():
    started_lanes: set[str] = set()
    release = asyncio.Event()

    async def lane(name: str) -> str:
        started_lanes.add(name)
        if len(started_lanes) == 4:
            release.set()
        await asyncio.wait_for(release.wait(), timeout=0.5)
        return name

    lane_outputs = await _run_independent_publication_lanes(
        finalizer_blocks=lambda: lane("finalizer_blocks"),
        provider_graph=lambda: lane("provider_graph"),
        price=lambda: lane("price"),
        source_witness=lambda: lane("source_witness"),
    )

    assert started_lanes == {
        "finalizer_blocks",
        "provider_graph",
        "price",
        "source_witness",
    }
    assert lane_outputs == (
        "finalizer_blocks",
        "provider_graph",
        "price",
        "source_witness",
    )


@pytest.mark.asyncio
async def test_finalizer_starts_before_independent_atom_preparation_finishes(
    monkeypatch,
    tmp_path,
):
    """Start finalization and price publication at their exact dependencies."""

    prepared_price = object()
    atom_release = asyncio.Event()
    finalizer_started = asyncio.Event()
    finalizer_release = asyncio.Event()
    finalizer_calls = []
    price_publish_started = asyncio.Event()
    price_publish_release = asyncio.Event()

    async def prepare_price(*, price_key_ready, **_kwargs):
        price_key_ready(
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        await atom_release.wait()
        return prepared_price

    async def run_finalizer(**kwargs):
        finalizer_calls.append(kwargs)
        finalizer_started.set()
        await finalizer_release.wait()
        return {"blocks": {}}

    async def publish_price(prepared):
        assert prepared is prepared_price
        price_publish_started.set()
        await price_publish_release.wait()
        return "published-price"

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

    pipeline_task = asyncio.create_task(
        shared_snapshot_publish._prepare_price_with_early_finalizer(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            price_set_summary_source_count=1,
            raw_work_directory=tmp_path,
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
            publish_prepared_price=publish_price,
        )
    )
    await asyncio.wait_for(finalizer_started.wait(), timeout=0.5)
    assert not atom_release.is_set()
    assert not pipeline_task.done()

    atom_release.set()
    await asyncio.wait_for(price_publish_started.wait(), timeout=0.5)
    assert not finalizer_release.is_set()
    price_publish_release.set()
    finalizer_release.set()
    (
        prepared,
        prepare_seconds,
        prepared_finalizer,
        prepared_price_publication,
    ) = await pipeline_task
    assert prepared is prepared_price
    assert prepare_seconds >= 0
    assert prepared_finalizer.summary == {"blocks": {}}
    assert prepared_finalizer.price_key_map_export_seconds >= 0
    assert prepared_finalizer.finalizer_seconds >= 0
    assert finalizer_calls[0]["price_key_map_row_count"] == 3
    assert prepared_price_publication.publication == "published-price"
    assert prepared_price_publication.publish_seconds >= 0


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
            raw_work_directory=tmp_path,
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
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
            raw_work_directory=tmp_path,
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            expected_source_identities=(),
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
    publication_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_finished = asyncio.Event()

    @asynccontextmanager
    async def transaction():
        yield object()

    async def publish_prepared(**_kwargs):
        publication_started.set()
        if cancel_during_publication:
            await asyncio.Future()
        return object()

    async def cleanup_prepared(observed_prepared):
        assert observed_prepared is prepared_price
        cleanup_started.set()
        await cleanup_release.wait()
        cleanup_finished.set()

    async def prepare_price(**kwargs):
        kwargs["price_key_ready"](
            PreparedSharedPriceKeyMap(
                schema_name="mrf",
                price_key_map="price_key_map",
                price_set_count=3,
            )
        )
        return prepared_price

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

    publish_task = asyncio.create_task(
        shared_snapshot_publish.publish_strict_shared_v3_layout(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
            reserved_snapshot_key=7,
            build_token="build-token",
            expected_coverage_scope_id=b"c" * 32,
            logical_snapshot_id="snapshot-id",
            expected_source_identities=(),
            serving_run_entries=(),
            code_dictionary_entries=(),
            provider_set_metadata_entries=(),
            source_audit_witness_entries=(),
            expected_raw_source_sha256=(),
            graph_artifact_entries=(),
            provider_identifier_quarantine={},
        )
    )
    await asyncio.wait_for(publication_started.wait(), timeout=0.5)
    if cancel_during_publication:
        publish_task.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=0.5)
    publish_task.cancel()
    await asyncio.sleep(0)
    publish_task.cancel()
    await asyncio.sleep(0)
    assert not publish_task.done()

    cleanup_release.set()
    with pytest.raises(asyncio.CancelledError):
        await publish_task
    assert cleanup_finished.is_set()
    assert prepare.await_args.kwargs["price_set_summary_source_count"] is None
