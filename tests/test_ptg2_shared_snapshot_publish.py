# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from tests.live_progress_atomic_redis import AtomicLiveProgressRedis

from process import live_progress
from process.ptg_parts import ptg2_shared_snapshot_publish as shared_snapshot_publish
from process.ptg_parts import ptg2_shared_publish as shared_publish
from process.ptg_parts import live_progress as ptg_live_progress
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_price import PreparedSharedPriceKeyMap
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
    _validate_authoritative_mapping_summary,
)


def test_snapshot_publish_rejects_missing_summary_mapping():
    with pytest.raises(RuntimeError, match="missing blocks"):
        shared_snapshot_publish._mapping(None, "blocks")


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
        progress_snapshot["progress_seq"]
        for progress_snapshot in progress_snapshots
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
        return False


def _dense_dictionary_progress_stage():
    return shared_snapshot_publish._V4DenseDictionaryStage(
        stage_table="group_stage",
        key_name="provider_group_key",
        expected_count=20_001,
        target_table="ptg2_v3_provider_group",
        columns=("provider_group_key", "provider_group_global_id_128"),
        value_predicate="octet_length(provider_group_global_id_128) = 16",
    )


def _assert_dense_dictionary_range_statements(statements):
    range_statements = [
        statement
        for statement, _parameters in statements
        if "range_start" in statement
    ]
    assert len(range_statements) == 6
    assert all(">= :range_start" in statement for statement in range_statements)
    assert all("< :range_end" in statement for statement in range_statements)
    assert not any("COUNT(DISTINCT" in statement for statement in range_statements)


@pytest.mark.asyncio
async def test_slow_shared_publish_copy_moves_progress_every_four_seconds(
    monkeypatch,
    tmp_path,
):
    """Measured publication bytes must advance progress_seq before COPY ends."""

    copy_path = tmp_path / "provider-graph.copy"
    copy_path.write_bytes(b"abcdefghijkl")
    elapsed_seconds = [0.0]
    progress_writes = _install_shared_publish_progress_capture(
        monkeypatch,
        elapsed_seconds,
    )
    publication_progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "provider graph publication",
        _publish_progress_to_live,
        interval_seconds=4.0,
        clock=lambda: elapsed_seconds[0],
    )
    token = ptg_live_progress.set_live_progress_context(
        run_id="run-slow-shared-publish",
        attempt_id="attempt-1",
        attempt_started_at="2026-07-23T11:00:00Z",
    )
    try:
        await shared_publish._copy_binary_file_to_stage(
            copy_path,
            schema_name="mrf",
            stage_table="provider_graph_stage",
            columns=("payload",),
            progress_callback=publication_progress.add,
        )
        publication_progress.flush()
    finally:
        ptg_live_progress.reset_live_progress_context(token)

    _assert_shared_publish_cadence(progress_writes)


@pytest.mark.asyncio
async def test_dense_v4_dictionary_ranges_move_exact_progress_every_four_seconds():
    """Large dictionaries validate and publish through measured key ranges."""

    elapsed_seconds = [0.0]
    range_driver = _DenseDictionaryRangeDriver(elapsed_seconds)
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=range_driver.execute_range_statement),
        scalar=AsyncMock(side_effect=range_driver.read_range_scalar),
    )
    progress_events = []
    progress = shared_snapshot_publish._MeasuredPublicationProgress(
        "dense dictionary publication",
        lambda stage, counters: progress_events.append(
            (elapsed_seconds[0], stage, dict(counters))
        ),
        interval_seconds=4.0,
        clock=lambda: elapsed_seconds[0],
    )
    dictionary_stage = _dense_dictionary_progress_stage()

    await shared_snapshot_publish._validate_v4_dictionary_stage(
        session,
        schema='"mrf"',
        stage=dictionary_stage,
        progress_callback=progress.add,
    )
    await shared_snapshot_publish._publish_v4_dictionary_stage_ranges(
        session,
        schema='"mrf"',
        snapshot_key=42,
        stage=dictionary_stage,
        progress_callback=progress.add,
    )
    progress.flush()

    emitted_times = [
        observed_at
        for observed_at, _stage, _counters in progress_events
    ]
    assert max(
        later - earlier
        for earlier, later in zip(emitted_times, emitted_times[1:])
    ) <= 4.0
    assert progress_events[-1][2]["validated_dictionary_rows"] == 20_001
    assert progress_events[-1][2]["published_dictionary_rows"] == 20_001
    _assert_dense_dictionary_range_statements(range_driver.statements)


def test_v4_reference_manifest_streams_exact_sorted_coordinates(tmp_path):
    path = tmp_path / "references.jsonl"
    rows = [
        {
            "object_kind": "v4_group_npis_exact_members_v1",
            "block_key": block_key,
            "fragment_no": 0,
            "entry_count": 2,
            "raw_byte_count": 8,
            "stored_byte_count": 8,
            "codec": "none",
            "hash": (bytes([block_key + 1]) * 32).hex(),
        }
        for block_key in (0, 4)
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))

    references = tuple(shared_snapshot_publish._iter_v4_block_references(path))

    assert tuple(reference.block_key for reference in references) == (0, 4)
    assert tuple(reference.entry_count for reference in references) == (2, 2)
    assert references[0].block_hash == b"\x01" * 32


def test_v4_reference_manifest_rejects_noncanonical_order(tmp_path):
    path = tmp_path / "references.jsonl"
    rows = [
        {
            "object_kind": "v4_set_patterns_members_v1",
            "block_key": block_key,
            "fragment_no": 0,
            "entry_count": 1,
            "raw_byte_count": 4,
            "stored_byte_count": 4,
            "codec": "none",
            "hash": (bytes([block_key + 1]) * 32).hex(),
        }
        for block_key in (1, 0)
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))

    with pytest.raises(RuntimeError, match="ordering"):
        tuple(shared_snapshot_publish._iter_v4_block_references(path))


def _v4_graph_publication_fixture(tmp_path):
    artifact = SimpleNamespace(
        name="graph_blocks",
        byte_count=12,
        sha256="a" * 64,
        row_count=1,
    )
    compilation = SimpleNamespace(
        output_artifacts=(artifact,),
        block_copy_path=tmp_path / "graph.copy",
        reference_manifest_path=tmp_path / "references.jsonl",
        selected_layout="pattern",
        summary={"format": "ptg2_provider_graph_v4"},
        relation_summaries=(),
        heavy_bitmaps=(),
        observe={"group_count": 3, "npi_count": 2},
        resource_admission={
            "input_factor_bytes": 512,
            "factor_edge_count": 9,
        },
        block_count=1,
        provider_set_audit_npi_copy_path=tmp_path / "audit.copy",
    )
    cas_publication = SimpleNamespace(
        unique_block_count=1,
        logical_byte_count=12,
        stored_byte_count=12,
    )
    map_summary = SimpleNamespace(
        map_digest=b"m" * 32,
        object_kinds=("v4_set_patterns_members_v1",),
        coordinate_count=1,
        stored_map_byte_count=64,
    )
    return compilation, cas_publication, map_summary


def _patch_v4_graph_publication(
    monkeypatch,
    cas_publication,
    map_summary,
):
    publish_cas_mock = AsyncMock(return_value=cas_publication)
    publish_maps_mock = AsyncMock(return_value=map_summary)
    replacements_by_name = {
        "create_shared_block_stage": AsyncMock(),
        "copy_shared_block_binary_file": AsyncMock(),
        "publish_v4_cas_block_stage": publish_cas_mock,
        "_publish_v4_dictionaries_and_maps": publish_maps_mock,
    }
    for name, replacement in replacements_by_name.items():
        monkeypatch.setattr(shared_snapshot_publish, name, replacement)
    monkeypatch.setattr(
        shared_snapshot_publish.db,
        "status",
        AsyncMock(),
    )
    return publish_cas_mock, publish_maps_mock


@pytest.mark.asyncio
async def test_v4_graph_publish_threads_compressed_acquisition_resources(
    monkeypatch,
    tmp_path,
):
    """Seal acquisition bytes with graph diagnostics, not the CAS stage."""

    compilation, cas_publication, map_summary = (
        _v4_graph_publication_fixture(tmp_path)
    )
    publish_cas_mock, publish_maps_mock = _patch_v4_graph_publication(
        monkeypatch,
        cas_publication,
        map_summary,
    )

    publication = await shared_snapshot_publish._publish_v4_graph(
        compilation,
        schema_name="mrf",
        snapshot_key=17,
        build_token="token",
        compressed_acquisition_bytes=4_096,
        empty_npi_tin_only_normalization_count=2,
    )

    assert publication.stored_byte_count == 76
    assert publish_cas_mock.await_args.kwargs == {
        "schema_name": "mrf",
        "stage_table": publish_cas_mock.await_args.kwargs["stage_table"],
        "snapshot_key": 17,
        "build_token": "token",
    }
    assert publish_maps_mock.await_args.kwargs[
        "compressed_acquisition_bytes"
    ] == 4_096
    assert publish_maps_mock.await_args.kwargs[
        "empty_npi_tin_only_normalization_count"
    ] == 2


def _patch_disabled_v4_publication(monkeypatch):
    prepared_price = object()

    @asynccontextmanager
    async def transaction():
        yield object()

    state = SimpleNamespace(
        prepared_price=prepared_price,
        prepare_mock=AsyncMock(
            return_value=(prepared_price, 0.0, None, None)
        ),
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
        state.publish_v3_mock.await_args.kwargs[
            "compressed_acquisition_bytes"
        ]
        is None
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
    assert finalizer_calls[0]["scratch_durability"] == (
        shared_snapshot_publish.PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY
    )
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
