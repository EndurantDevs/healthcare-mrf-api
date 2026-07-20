# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_shared_blocks import SharedMappingDigestSummary
from process.ptg_parts.ptg2_shared_publish import SharedBlockCopyMetrics


_FINALIZER_KINDS = (
    "by_code_price_dictionary",
    "by_code_price_page_v4",
    "by_code_provider_shard_v1",
)
_GRAPH_KINDS = (
    "graph_group_npis_v1",
    "graph_group_provider_sets_v1",
    "graph_npi_groups_v1",
    "graph_provider_set_groups_v1",
)
_PRICE_KINDS = (
    "price_atoms_v3",
    "price_set_atom_memberships_v3",
    "provider_set_codes_v3",
    "provider_set_count_dictionary",
    "provider_set_page_v3_s2",
)


def _copy_metrics(*, reused: bool) -> SharedBlockCopyMetrics:
    reused_payload_bytes = 2 if reused else 0
    reused_row_count = 1 if reused else 0
    source_copy_bytes = 10 if reused else 7
    source_payload_bytes = 5 if reused else 4
    row_count = 2 if reused else 1
    unique_block_count = row_count
    existing_block_count = reused_row_count
    new_block_count = unique_block_count - existing_block_count
    return SharedBlockCopyMetrics(
        source_copy_bytes=source_copy_bytes,
        staged_copy_bytes=source_copy_bytes - reused_payload_bytes,
        source_payload_bytes=source_payload_bytes,
        staged_payload_bytes=source_payload_bytes - reused_payload_bytes,
        reused_payload_bytes=reused_payload_bytes,
        durable_reused_payload_bytes=reused_payload_bytes,
        same_copy_reused_payload_bytes=0,
        row_count=row_count,
        staged_payload_row_count=new_block_count,
        reused_payload_row_count=reused_row_count,
        durable_reused_row_count=reused_row_count,
        same_copy_reused_row_count=0,
        unique_block_count=unique_block_count,
        existing_block_count=existing_block_count,
        new_block_count=new_block_count,
        duplicate_block_row_count=0,
        metadata_scan_seconds=0.01,
        existence_lookup_seconds=0.02,
        copy_seconds=0.03,
    )


def _finalizer_summary_by_field(tmp_path):
    serving_path = tmp_path / "serving-blocks.copy"
    price_path = tmp_path / "price-blocks.copy"
    serving_path.write_bytes(b"serving")
    price_path.write_bytes(b"price")
    return {
        "output_directory": str(tmp_path),
        "source_count": 2,
        "blocks": {
            "serving": {
                "path": serving_path.name,
                "copy_bytes": serving_path.stat().st_size,
                "copy_sha256": "1" * 64,
            },
            "price_dictionary": {
                "path": price_path.name,
                "copy_bytes": price_path.stat().st_size,
                "copy_sha256": "2" * 64,
            },
            "price_dictionary_encoder": {"encoding": "dense_price_v1"},
            "assigned_encoder": {"encoding": "assigned_v1"},
        },
        "dense_keys": {
            "price": {
                "count": 2,
                "ordering": snapshot_publish.PTG2_V3_PRICE_KEY_ORDER,
            }
        },
        "preservation": {"encoded_records": 19},
        "timings": {"scanner_seconds": 0.25},
    }


def _lane_publications(*, price_kinds, membership_span):
    finalizer = SimpleNamespace(
        object_kinds=_FINALIZER_KINDS,
        mapping_count=3,
        unique_block_count=3,
        logical_byte_count=30,
        stored_byte_count=3,
    )
    graph = SimpleNamespace(
        object_kinds=_GRAPH_KINDS,
        mapping_count=4,
        unique_block_count=4,
        logical_byte_count=40,
        stored_byte_count=4,
        support_digest=b"g" * 32,
        owner_count=5,
        provider_group_count=6,
        npi_count=7,
        block_count=8,
    )
    price = SimpleNamespace(
        object_kinds=tuple(price_kinds),
        mapping_count=len(price_kinds),
        unique_block_count=len(price_kinds),
        logical_byte_count=10 * len(price_kinds),
        stored_byte_count=5,
        support_digest=b"p" * 32,
        atom_key_bits=32,
        price_atom_constant_keys={"setting": 1},
        price_atom_constant_values={"setting": "office"},
        stage_metrics={"new_block_count": 5},
        stream_summaries={
            "price_set_atom_memberships_v3": {
                "block_span": membership_span,
                "block_count": 2,
            },
            "price_atoms_v3": {"block_span": 8, "block_count": 3},
        },
    )
    witness = SimpleNamespace(
        metadata={"contract": "source-witness-v1"},
        support_digest=b"w" * 32,
        stored_byte_count=6,
    )
    return SimpleNamespace(
        finalizer=finalizer,
        graph=graph,
        price=price,
        witness=witness,
    )


def _mapping_summary(lanes):
    mapped_lanes = (lanes.finalizer, lanes.graph, lanes.price)
    return SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=sum(lane.mapping_count for lane in mapped_lanes),
        unique_block_count=sum(lane.unique_block_count for lane in mapped_lanes),
        entry_count=41,
        logical_byte_count=sum(lane.logical_byte_count for lane in mapped_lanes),
        canonical_byte_count=900,
        object_kinds=tuple(
            sorted(kind for lane in mapped_lanes for kind in lane.object_kinds)
        ),
    )


def _publication_mocks(tmp_path, *, price_kinds, membership_span, seal_reused):
    summary_by_field = _finalizer_summary_by_field(tmp_path)
    lanes = _lane_publications(
        price_kinds=price_kinds,
        membership_span=membership_span,
    )
    return SimpleNamespace(
        summary_by_field=summary_by_field,
        prepared_price=SimpleNamespace(price_set_count=2),
        lanes=lanes,
        mapping_summary=_mapping_summary(lanes),
        graph_conversion=SimpleNamespace(cleanup=MagicMock()),
        quarantine=provider_identifier_quarantine_payload({}),
        touch=AsyncMock(),
        export_price=AsyncMock(return_value=tmp_path / "price-key-map.copy"),
        run_finalizer=AsyncMock(return_value=summary_by_field),
        dictionaries=AsyncMock(
            return_value=SimpleNamespace(code_count=11, support_digest=b"d" * 32)
        ),
        export_provider_keys=AsyncMock(return_value=tmp_path / "provider-keys.tsv"),
        convert_graph=AsyncMock(),
        create_stage=AsyncMock(),
        copy_block=AsyncMock(
            side_effect=[
                _copy_metrics(reused=True),
                _copy_metrics(reused=False),
            ]
        ),
        publish_blocks=AsyncMock(return_value=lanes.finalizer),
        publish_graph=AsyncMock(return_value=lanes.graph),
        publish_price=AsyncMock(return_value=lanes.price),
        publish_witness=AsyncMock(return_value=lanes.witness),
        summarize=AsyncMock(return_value=_mapping_summary(lanes)),
        publish_audit=AsyncMock(
            return_value=SimpleNamespace(
                metadata={"contract": "audit-sample-v1", "row_count": 3}
            )
        ),
        seal=AsyncMock(return_value=SimpleNamespace(snapshot_key=17, reused=seal_reused)),
        sealed_audit=AsyncMock(
            return_value={"contract": "persisted-audit-v1", "row_count": 3}
        ),
        db_status=AsyncMock(),
    )


def _patch_publication_dependencies(monkeypatch, mocks):
    mocks.convert_graph.return_value = mocks.graph_conversion

    @asynccontextmanager
    async def transaction():
        yield object()

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setattr(snapshot_publish.db, "transaction", transaction)
    monkeypatch.setattr(snapshot_publish.db, "status", mocks.db_status)
    patch_name_by_target = {
        "touch_shared_layout_build": "touch",
        "export_shared_price_key_map": "export_price",
        "run_v3_direct_finalizer": "run_finalizer",
        "publish_shared_finalizer_dictionaries": "dictionaries",
        "_export_provider_set_key_map": "export_provider_keys",
        "_convert_shared_graph_natively": "convert_graph",
        "create_shared_block_stage": "create_stage",
        "copy_shared_block_binary_file": "copy_block",
        "publish_shared_block_stage": "publish_blocks",
        "publish_shared_graph": "publish_graph",
        "publish_shared_price_artifacts": "publish_price",
        "publish_shared_source_witness": "publish_witness",
        "summarize_shared_snapshot_mappings": "summarize",
        "publish_shared_audit_sample": "publish_audit",
        "seal_shared_layout": "seal",
        "sealed_audit_sample_metadata": "sealed_audit",
    }
    for target_name, mock_name in patch_name_by_target.items():
        monkeypatch.setattr(snapshot_publish, target_name, getattr(mocks, mock_name))


def _prepared_layout_mocks(
    monkeypatch,
    tmp_path,
    *,
    price_kinds=_PRICE_KINDS,
    membership_span=16,
    seal_reused=False,
):
    mocks = _publication_mocks(
        tmp_path,
        price_kinds=price_kinds,
        membership_span=membership_span,
        seal_reused=seal_reused,
    )
    _patch_publication_dependencies(monkeypatch, mocks)
    return mocks


async def _publish_prepared_layout(mocks, tmp_path, **overrides):
    arguments_by_name = {
        "schema_name": "mrf",
        "manifest_stage_table": "manifest_stage",
        "reserved_snapshot_key": 7,
        "build_token": "build-token",
        "expected_coverage_scope_id": b"c" * 32,
        "logical_snapshot_id": "snapshot-id",
        "expected_source_identities": ({"raw_source_sha256": "a" * 64},),
        "serving_run_entries": ({"source_key": 1},),
        "code_dictionary_entries": ({"code_key": 2},),
        "provider_set_metadata_entries": ({"provider_set_key": 3},),
        "source_audit_witness_entries": ({"source_key": 1},),
        "expected_raw_source_sha256": ("a" * 64,),
        "graph_artifact_entries": ({"kind": "graph"},),
        "provider_identifier_quarantine": mocks.quarantine,
        "prepared_price": mocks.prepared_price,
        "publication_started_at": snapshot_publish.time.monotonic(),
        "price_prepare_seconds": 0.5,
        "prepared_work_directory": tmp_path,
    }
    arguments_by_name.update(overrides)
    return await snapshot_publish._publish_prepared_shared_layout(**arguments_by_name)


def _assert_complete_publication(publication, mocks):
    assert publication.snapshot_key == 17
    assert publication.object_kinds == mocks.mapping_summary.object_kinds
    assert publication.mapping_count == 12
    assert publication.unique_block_count == 12
    assert publication.mapping_digest == b"m" * 32
    assert publication.layout_reused_at_seal is False
    assert publication.stored_byte_count == 18
    assert publication.serving_index["shared_snapshot_key"] == 17
    assert publication.serving_index["coverage_scope_id"] == (b"c" * 32).hex()
    assert publication.serving_index["storage_bytes"] == 18
    copy_proof = publication.serving_index["finalizer_block_copy"]
    assert copy_proof["contract"] == "selective_shared_block_copy_v1"
    assert copy_proof["serving"]["reused_payload_bytes"] == 2
    assert copy_proof["price_dictionary"]["reused_payload_bytes"] == 0
    assert copy_proof["total"]["source_copy_bytes"] == 17
    assert copy_proof["total"]["staged_copy_bytes"] == 15
    assert copy_proof["total"]["new_block_count"] == 2
    assert publication.serving_index["audit_sample"] == (
        mocks.publish_audit.return_value.metadata
    )
    assert publication.serving_index["source_witness"] == mocks.lanes.witness.metadata
    assert publication.serving_index["timings"]["price_prepare_seconds"] == 0.5
    assert publication.serving_index["timings"]["scanner_seconds"] == 0.25
    assert publication.serving_index["timings"]["shared_publish_total_seconds"] >= 0


@pytest.mark.asyncio
async def test_prepared_layout_publishes_and_seals(monkeypatch, tmp_path):
    mocks = _prepared_layout_mocks(monkeypatch, tmp_path)

    publication = await _publish_prepared_layout(
        mocks,
        tmp_path,
        prepared_work_directory=None,
        scratch_parent=tmp_path,
    )

    _assert_complete_publication(publication, mocks)
    exported_path = mocks.export_price.await_args.args[1]
    assert exported_path.name == "price-key-map.copy"
    assert exported_path.parent.parent == tmp_path
    assert not exported_path.parent.exists()
    assert mocks.run_finalizer.await_args.kwargs["price_key_map_row_count"] == 2
    assert mocks.copy_block.await_count == 2
    assert all(
        call.kwargs["reuse_existing"] is True
        for call in mocks.copy_block.await_args_list
    )
    mocks.publish_graph.assert_awaited_once_with(
        mocks.graph_conversion,
        schema_name="mrf",
        snapshot_key=7,
        build_token="build-token",
    )
    mocks.graph_conversion.cleanup.assert_called_once_with()
    assert mocks.touch.await_count == 5
    mocks.sealed_audit.assert_not_awaited()
    seal_args = mocks.seal.await_args.kwargs
    assert seal_args["expected_summary"] == mocks.mapping_summary
    assert seal_args["layout_manifest"]["serving_index"]["shared_snapshot_key"] == 7
    assert seal_args["layout_manifest"]["serving_index"]["finalizer_block_copy"] == (
        publication.serving_index["finalizer_block_copy"]
    )


@pytest.mark.asyncio
async def test_prepared_layout_requires_selective_copy_proof(monkeypatch, tmp_path):
    mocks = _prepared_layout_mocks(monkeypatch, tmp_path)
    mocks.copy_block.side_effect = [None, _copy_metrics(reused=False)]

    with pytest.raises(ExceptionGroup) as exc_info:
        await _publish_prepared_layout(mocks, tmp_path)

    assert len(exc_info.value.exceptions) == 1
    assert "did not return selective proof" in str(exc_info.value.exceptions[0])
    mocks.publish_blocks.assert_not_awaited()
    mocks.graph_conversion.cleanup.assert_called_once_with()


@pytest.mark.asyncio
async def test_prepared_layout_reuses_early_results(monkeypatch, tmp_path):
    mocks = _prepared_layout_mocks(monkeypatch, tmp_path, seal_reused=True)
    finalizer = snapshot_publish._PreparedFinalizer(
        summary=mocks.summary_by_field,
        price_key_map_export_seconds=0.1,
        finalizer_seconds=0.2,
        overlap_wall_seconds=0.25,
    )
    price = snapshot_publish._PreparedPricePublication(
        publication=mocks.lanes.price,
        publish_seconds=0.3,
    )

    publication = await _publish_prepared_layout(
        mocks,
        tmp_path,
        prepared_finalizer=finalizer,
        prepared_price_publication=price,
        full_rebuild_scope_digest="f" * 64,
    )

    mocks.export_price.assert_not_awaited()
    mocks.run_finalizer.assert_not_awaited()
    mocks.publish_price.assert_not_awaited()
    mocks.sealed_audit.assert_awaited_once()
    assert publication.layout_reused_at_seal is True
    assert publication.serving_index["audit_sample"]["contract"] == (
        "persisted-audit-v1"
    )
    assert publication.serving_index["full_rebuild_scope_digest"] == "f" * 64
    timings = publication.serving_index["timings"]
    assert timings["price_key_map_export_seconds"] == 0.1
    assert timings["finalizer_seconds"] == 0.2
    assert timings["price_key_ready_finalizer_wall_seconds"] == 0.25
    assert timings["price_publish_seconds"] == 0.3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("prepared_count", "key_order"),
    [(3, snapshot_publish.PTG2_V3_PRICE_KEY_ORDER), (2, "unsupported-order")],
)
async def test_prepared_layout_rejects_price_keys(
    monkeypatch,
    tmp_path,
    prepared_count,
    key_order,
):
    mocks = _prepared_layout_mocks(monkeypatch, tmp_path)
    mocks.prepared_price.price_set_count = prepared_count
    mocks.summary_by_field["dense_keys"]["price"]["ordering"] = key_order
    finalizer = snapshot_publish._PreparedFinalizer(
        summary=mocks.summary_by_field,
        price_key_map_export_seconds=0.1,
        finalizer_seconds=0.2,
        overlap_wall_seconds=0.25,
    )

    with pytest.raises(RuntimeError, match="finalizer price keys disagree"):
        await _publish_prepared_layout(
            mocks,
            tmp_path,
            prepared_finalizer=finalizer,
        )

    mocks.create_stage.assert_not_awaited()
    mocks.publish_graph.assert_not_awaited()
    mocks.publish_price.assert_not_awaited()
    mocks.publish_witness.assert_not_awaited()
    mocks.publish_audit.assert_not_awaited()
    mocks.seal.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_layout_requires_all_kinds(monkeypatch, tmp_path):
    mocks = _prepared_layout_mocks(
        monkeypatch,
        tmp_path,
        price_kinds=_PRICE_KINDS[:-1],
    )

    with pytest.raises(RuntimeError, match="missing required blocks"):
        await _publish_prepared_layout(mocks, tmp_path)

    mocks.graph_conversion.cleanup.assert_called_once_with()
    mocks.publish_audit.assert_not_awaited()
    mocks.seal.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_layout_requires_membership_span(monkeypatch, tmp_path):
    mocks = _prepared_layout_mocks(monkeypatch, tmp_path, membership_span=0)

    with pytest.raises(RuntimeError, match="membership block span must be positive"):
        await _publish_prepared_layout(mocks, tmp_path)

    mocks.graph_conversion.cleanup.assert_called_once_with()
    mocks.publish_audit.assert_not_awaited()
    mocks.seal.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_layout_requires_configured_schema(monkeypatch, tmp_path):
    mocks = _prepared_layout_mocks(monkeypatch, tmp_path)

    with pytest.raises(RuntimeError, match="configured PostgreSQL schema"):
        await _publish_prepared_layout(mocks, tmp_path, schema_name="other")

    mocks.touch.assert_not_awaited()
    mocks.dictionaries.assert_not_awaited()
    mocks.seal.assert_not_awaited()
