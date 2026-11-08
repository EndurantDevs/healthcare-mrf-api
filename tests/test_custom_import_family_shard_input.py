# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual synthetic Parquet, spool custody and original source-key semantics."""

import asyncio
import hashlib
import json
import math
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import msgpack
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import family_shard_input as shards
from process.custom_import.build_source import _source_row_shape
from process.custom_import.capture import CaptureError, capture_stream, iter_parquet_source_batches
from process.custom_import.capture_pending import _ACCOUNTING_FIELDS, _bundle_manifest
from process.custom_import.capture_store import SegmentedParquetPart
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.family import assemble_root_families, merge_families
from process.custom_import.family_raw_key import raw_family_key_evidence
from process.custom_import.family_shard_input_records import decode_record, encode_record, prepare_record, shard_for
from process.custom_import.runner_codec import child_key_document, root_key_evidence_from_tuple
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from process.custom_import.snowflake_bundle_replay import (
    _aggregate_parquet_arrow_bytes,
    _normalized_integer_replay_values,
    _stream_fields,
)
from tests.test_custom_import_capture_pending_postgres import _receipt
from tests.test_custom_import_segmented_capture_postgres import _POLICY
from tests.test_custom_import_snowflake_bundle import _definition

pytestmark = pytest.mark.asyncio


async def test_compressed_partition_uses_one_aggregate_stored_budget(tmp_path):
    rows = [_root() for _ in range(2000)]
    definition, source, parts = _capture_input({"root_source": [rows], "detail_source": [[]]})
    limits = shards.ShardInputLimits(128 * 1024, shard_count=4, chunk_bytes=65536)
    partition = await _partition(tmp_path, definition, source, parts, limits=limits)
    try:
        document = shards.manifest_document(partition)
        assert document["spool_bytes"] > limits.max_spool_bytes
        assert sum(path.stat().st_size for path in partition.root.iterdir()) <= limits.max_spool_bytes
        actual = _all_records(partition)
        assert len(actual) == len(rows)
        assert sorted(record.source_ordinal for record in actual) == list(range(len(rows)))
        assert all(dict(record.values) == rows[0] for record in actual)
        assert all(path.stat().st_mode & 0o777 == 0o400 for path in partition.root.iterdir())
    finally:
        shards.cleanup_spool(partition)


async def test_pending_lanes_share_the_stored_budget_and_flush_before_overcommit(tmp_path):
    from process.custom_import.family_shard_spool import BLOCK_OVERHEAD

    definition, capture_source, _parts = _capture_input({"root_source": [[_root()]], "detail_source": [[]]})
    limits = shards.ShardInputLimits(4096, shard_count=2, chunk_bytes=65536)
    work = shards._Spool(tmp_path / "pending", capture_source, limits, lambda _path: None)
    by_lane = {}
    for ordinal in range(20):
        occurrence = prepare_record(
            definition,
            definition.source_streams[0],
            _root(str(1003000100 + ordinal), score="x" * 2500),
            stream_slot=1,
            collection_slot=0,
            part_ordinal=1,
            row_ordinal=ordinal,
            source_ordinal=ordinal,
        )
        by_lane.setdefault(shard_for(occurrence, 2), occurrence)
        if len(by_lane) == 2:
            break
    assert set(by_lane) == {0, 1}
    assert sum(len(encode_record(occurrence)) + 8 for occurrence in by_lane.values()) > limits.max_spool_bytes
    try:
        for lane, occurrence in by_lane.items():
            work.append(lane, occurrence)
            pending = sum(len(buffer) + BLOCK_OVERHEAD for buffer in work.buffers.values())
            stored = sum(path.stat().st_size for path in work.root.iterdir())
            assert stored + pending <= limits.max_spool_bytes
        assert len(work.buffers) == 1
        assert work.pending_bytes == pending and work.stored_bytes == stored
        assert work.document["spool_bytes"] > limits.max_spool_bytes
    finally:
        work.discard()


@pytest.mark.parametrize("damage", ("mode", "link", "descriptor", "suffix"))
async def test_compressed_partition_requires_closed_storage_custody(tmp_path, damage):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[]]})
    partition = await _partition(tmp_path, definition, source, parts)
    document = shards.manifest_document(partition)
    path = partition.root / document["files"][0]["name"]
    extra = tmp_path / "extra-link"
    try:
        if damage == "mode":
            path.chmod(0o600)
        elif damage == "link":
            os.link(path, extra)
        elif damage == "descriptor":
            path = partition.root / "storage.json"
            path.chmod(0o600)
            path.write_bytes(b"{}")
            path.chmod(0o400)
        else:
            path.chmod(0o600)
            path.write_bytes(path.read_bytes() + b"extra")
            path.chmod(0o400)
        with pytest.raises(ValueError):
            list(shards.read_partition(partition))
    finally:
        if extra.exists():
            extra.unlink()
        shards.cleanup_spool(partition)


async def test_compressed_input_preserves_legacy_logical_frames_and_manifest(tmp_path):
    from process.custom_import.family_shard_spool import BlockReader

    definition, source, parts = _capture_input(
        {"root_source": [[_root(), _root("1234567893", None)]], "detail_source": [[_child(amount="001.00")]]}
    )
    partition = await _partition(tmp_path, definition, source, parts)
    expected = _all_records(partition)
    document = shards.manifest_document(partition)
    manifest = (partition.root / "manifest.json").read_bytes()
    try:
        for entry in document["files"]:
            path = partition.root / entry["name"]
            with path.open("rb") as stream:
                reader = BlockReader(stream, maximum_logical_bytes=entry["bytes"])
                original = bytes(reader.read(entry["bytes"]))
                assert not reader.read(1)
            assert hashlib.sha256(original).hexdigest() == entry["sha256"]
            path.chmod(0o600)
            path.write_bytes(original)
            path.chmod(0o400)
        (partition.root / "storage.json").unlink()
        legacy = replace(partition, storage_sha256=None)
        assert _all_records(legacy) == expected
        assert (partition.root / "manifest.json").read_bytes() == manifest
        assert hashlib.sha256(manifest).hexdigest() == partition.sha256
    finally:
        shards.cleanup_spool(partition)


def _policy():
    document = json.loads(json.dumps(_POLICY))
    document["part_limits"].update(
        maximum_records=10000,
        maximum_compressed_bytes=4 * 1024**2,
        maximum_decoded_bytes=16 * 1024**2,
        maximum_record_bytes=1024**2,
    )
    for name in ("stream_budget", "bundle_budget"):
        document[name] = {key: 100000000 for key in document[name]}
        document[name]["maximum_parts"] = 10000
    document.update(maximum_part_arrow_bytes=16 * 1024**2, maximum_dataset_retained_bytes=10**10)
    return SegmentedCapturePolicy.from_mapping(document)


def _root(npi="1003000126", score=7):
    return dict(npi=npi, score=score, enabled=True)


def _child(npi="1003000126", key="a", amount="1.00"):
    return dict(detail_npi=npi, detail_id=key, amount=amount)


def _synthetic_stream_parts(
    request, definition, stream, stream_rows, slot, decimal_scores, asserted_arrow_delta, asserted_record_delta
):
    fields = tuple(
        sorted(
            (field for field in definition.fields if field.collection == stream.child_collection),
            key=lambda field: field.field_slot,
        )
    )
    types_by_name = {
        "string": pa.string(),
        "integer": pa.decimal128(30, 0) if decimal_scores else pa.int64(),
        "decimal": pa.string(),
        "boolean": pa.bool_(),
    }
    schema = pa.schema([(field.field_id, types_by_name[field.value_type]) for field in fields])
    sealed_parts = []
    for source_rows in stream_rows:
        table = pa.Table.from_pylist(source_rows, schema=schema)
        output = BytesIO()
        pq.write_table(table, output, compression="NONE")
        capture = capture_stream(
            BytesIO(output.getvalue()),
            stream,
            source_snapshot_token=request.source_snapshot_token,
            limits=request.policy.part_limits,
        )
        arrow_bytes = _aggregate_parquet_arrow_bytes(capture, limits=request.policy.part_limits, decoded_bytes=0)
        if slot == 1 and not sealed_parts:
            arrow_bytes += asserted_arrow_delta
        sealed_parts.append(
            (capture, table.num_rows + (asserted_record_delta if slot == 1 and not sealed_parts else 0), arrow_bytes)
        )
    return sealed_parts


def _sealed_capture_input(request, definition, receipts, source_binding):
    bundle = SimpleNamespace(
        snapshot_token=request.source_snapshot_token,
        source_request_sha256=request.source_request_sha256,
        statement_sha256=request.statement_sha256,
        source_binding_sha256=b"b" * 32 if source_binding else None,
    )
    for name in _ACCOUNTING_FIELDS:
        setattr(
            bundle, "committed_" + name, sum(json.loads(receipt.canonical_manifest)[name] for _, receipt in receipts)
        )
    canonical = _bundle_manifest(
        bundle,
        tuple((receipt.stream_id, slot) for slot, receipt in receipts),
        {receipt.stream_id: receipt for _, receipt in receipts},
        request.policy,
    )
    identity = shards.CaptureInputIdentity(
        101,
        102,
        103,
        104,
        105 if source_binding else None,
        definition.digest,
        definition.schema_digest,
        (b"b" * 32).hex() if source_binding else None,
        hashlib.sha256(canonical.encode()).hexdigest(),
    )
    return shards.SealedCaptureInput(
        identity,
        request.source_snapshot_token,
        request.source_request_sha256,
        request.statement_sha256,
        canonical,
        request.policy,
        tuple(receipts),
    )


def _capture_input(
    records_by_stream,
    *,
    definition=None,
    decimal_scores=False,
    asserted_arrow_delta=0,
    asserted_record_delta=0,
    policy=None,
    source_binding=True,
):
    definition = definition or _definition()
    policy = policy or _policy()
    request = SimpleNamespace(
        policy=policy,
        source_request_sha256=b"q" * 32,
        statement_sha256=b"s" * 32,
        source_snapshot_token="synthetic-sealed",
    )
    receipts, parts = [], []
    for slot, stream in enumerate(definition.source_streams, 1):
        sealed_parts = _synthetic_stream_parts(
            request,
            definition,
            stream,
            records_by_stream[stream.stream_id],
            slot,
            decimal_scores,
            asserted_arrow_delta,
            asserted_record_delta,
        )
        receipt = _receipt(request, stream.stream_id, sealed_parts)
        document = json.loads(receipt.canonical_manifest)
        document["source_binding_sha256"] = (b"b" * 32).hex() if source_binding else None
        canonical = canonical_json(document)
        receipt = replace(
            receipt, canonical_manifest=canonical, manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest()
        )
        receipts.append((slot, receipt))
        parts.extend(
            SegmentedParquetPart(receipt, ordinal, capture, count, arrow)
            for ordinal, (capture, count, arrow) in enumerate(sealed_parts, 1)
        )
    return definition, _sealed_capture_input(request, definition, receipts, source_binding), parts


@asynccontextmanager
async def _part_source(parts, *, fail_after=None, close_error=False, observations=None):
    async def records():
        try:
            for number, part in enumerate(parts):
                if number == fail_after:
                    raise RuntimeError("synthetic late capture failure")
                yield part
            if fail_after == len(parts):
                raise RuntimeError("synthetic late capture failure")
        finally:
            if observations is not None:
                observations.append("iterator_closed")

    iterator = records()
    try:
        yield iterator
    finally:
        await iterator.aclose()
        if observations is not None:
            observations.append("context_closed")
        if close_error:
            raise RuntimeError("synthetic reader close failure")


async def _partition(tmp_path, definition, source, parts, *, limits=None, source_context=None, root=None):
    root = tmp_path / "owned-spool" if root is None else root
    registered_paths = []

    def register(path):
        assert path == root.absolute()
        assert not path.exists()
        registered_paths.append(path)

    result = await shards.partition_capture(
        source_context or _part_source(parts),
        definition=definition,
        source=source,
        root=root,
        register_cleanup=register,
        limits=limits or shards.ShardInputLimits(32 * 1024**2, 4, 1024),
    )
    assert registered_paths == [root.absolute()]
    return result


async def test_segmented_capture_preserves_explicit_absent_source_binding(tmp_path):
    definition, source, parts = _capture_input(
        {"root_source": [[_root()]], "detail_source": [[_child()]]}, source_binding=False
    )
    assert source.identity.source_binding_revision_id is None and source.identity.source_binding_sha256 is None
    result = await _partition(tmp_path, definition, source, parts)
    try:
        assert len(_all_records(result)) == 2
    finally:
        shards.cleanup_spool(result)
    with pytest.raises(ValueError, match="binding evidence"):
        replace(source.identity, source_binding_revision_id=105)
    with pytest.raises(ValueError, match="binding evidence"):
        replace(source.identity, source_binding_sha256=(b"b" * 32).hex())


def _all_records(result):
    document = shards.manifest_document(result)
    return [
        record
        for lane in (*range(document["limits"]["shard_count"]), None)
        for record in shards.read_shard(result, lane)
    ]


async def test_actual_parquet_parts_preserve_all_occurrences_and_source_ordinals(tmp_path):
    definition, capture_input, parts = _capture_input(
        {
            "root_source": [[_root(score=Decimal("7"))], [_root("1234567893", Decimal("8"))]],
            "detail_source": [
                [_child(), _child(key="a")],
                [_child(key="b", amount=None), _child("1111111111", "orphan")],
            ],
        },
        decimal_scores=True,
    )
    partition = await _partition(tmp_path, definition, capture_input, parts)
    document, occurrences = shards.manifest_document(partition), _all_records(partition)
    assert document["complete"] and document["records"] == 6 and document["source_records"] == {"1": 2, "2": 4}
    assert sorted((occurrence.stream_slot, occurrence.source_ordinal) for occurrence in occurrences) == [
        (1, 0),
        (1, 1),
        (2, 0),
        (2, 1),
        (2, 2),
        (2, 3),
    ]
    roots = sorted(
        (occurrence for occurrence in occurrences if occurrence.stream_slot == 1),
        key=lambda occurrence: occurrence.source_ordinal,
    )
    assert type(dict(roots[0].values)["score"]) is Decimal
    assert type(roots[0].normalized_values(definition, definition.source_streams[0])["score"]) is int
    children = sorted(
        (occurrence for occurrence in occurrences if occurrence.stream_slot == 2),
        key=lambda occurrence: occurrence.source_ordinal,
    )
    assert children[0].child_key == children[1].child_key
    assert dict(children[0].values)["amount"] == "1.00"
    assert children[2].rejection_code == "required_field_null"
    assert children[3].rejection_code is None  # Orphan classification needs all source roots / the retained base.
    assert len({shard_for(occurrence, 4) for occurrence in (roots[0], *children[:3])}) == 1
    shards.cleanup_spool(partition)
    assert not partition.root.exists()


def _damaged_capture(parts, damage):
    if damage == "missing":
        return parts[:-1], None
    if damage == "duplicate":
        return [parts[0], *parts], None
    if damage == "order":
        return [parts[1], parts[0], *parts[2:]], None
    if damage == "receipt":
        parts[0] = replace(parts[0], receipt=replace(parts[0].receipt, source_snapshot_token="foreign"))
        return parts, None
    if damage == "payload":
        parts[0] = replace(parts[0], capture=replace(parts[0].capture, payload=parts[0].capture.payload + b"x"))
        return parts, None
    if damage == "arrow":
        parts[0] = replace(parts[0], arrow_byte_count=parts[0].arrow_byte_count + 1)
        return parts, None
    if damage == "records":
        parts[0] = replace(parts[0], record_count=parts[0].record_count + 1)
        return parts, None
    return parts, _part_source(
        parts, fail_after=len(parts) if damage == "late" else None, close_error=damage == "close"
    )


@pytest.mark.parametrize(
    "damage", ["missing", "duplicate", "order", "receipt", "payload", "arrow", "records", "late", "close"]
)
async def test_capture_damage_never_produces_completed_spool(tmp_path, damage):
    definition, source, parts = _capture_input(
        {"root_source": [[_root()], [_root("1234567893")]], "detail_source": [[_child()]]}
    )
    parts, context = _damaged_capture(parts, damage)
    with pytest.raises((ValueError, RuntimeError)):
        await _partition(tmp_path, definition, source, parts, source_context=context)
    assert not (tmp_path / "owned-spool").exists()


async def test_interrupted_partition_discards_owned_files_and_restarts_without_duplicates(tmp_path):
    definition, source, parts = _capture_input(
        {
            "root_source": [[_root()], [_root("1234567893")]],
            "detail_source": [[_child(key=f"K{index}") for index in range(30)]],
        }
    )
    with pytest.raises(RuntimeError, match="late capture"):
        await _partition(tmp_path, definition, source, parts, source_context=_part_source(parts, fail_after=2))
    assert not (tmp_path / "owned-spool").exists()
    result = await _partition(tmp_path, definition, source, parts)
    records = _all_records(result)
    assert len(records) == 32
    assert _all_records(result) == records
    assert len({(record.stream_slot, record.source_ordinal) for record in records}) == 32
    shards.cleanup_spool(result)


async def test_large_family_crosses_chunks_without_becoming_an_input_limit(tmp_path):
    definition, source, parts = _capture_input(
        {
            "root_source": [[_root()]],
            "detail_source": [
                [_child(key=f"K{index}") for index in range(70)],
                [_child(key=f"K{index}") for index in range(70, 150)],
            ],
        }
    )
    result = await _partition(tmp_path, definition, source, parts, limits=shards.ShardInputLimits(32 * 1024**2, 4, 1))
    document, records = shards.manifest_document(result), _all_records(result)
    assert len(records) == 151 and len(document["files"]) == 151
    assert len({shard_for(record, 4) for record in records}) == 1
    assert all(entry["bytes"] > document["limits"]["chunk_bytes"] for entry in document["files"])
    shards.cleanup_spool(result)


async def test_invalid_key_lane_preserves_missing_and_null(tmp_path):
    definition, source, parts = _capture_input(
        {"root_source": [[_root(None), _root()]], "detail_source": [[_child(None), _child(key=None)]]}
    )
    result = await _partition(tmp_path, definition, source, parts)
    invalid_occurrences = list(shards.read_shard(result, None))
    assert len(invalid_occurrences) == 2 and {record.rejection_code for record in invalid_occurrences} == {
        "root_key_missing",
        "orphan_child",
    }
    assert shards.manifest_document(result)["invalid_key_records"] == 2
    stream = definition.source_streams[0]
    original = prepare_record(
        definition, stream, _root(), stream_slot=1, collection_slot=0, part_ordinal=1, row_ordinal=0, source_ordinal=0
    )
    missing = replace(original, values=tuple((name, value) for name, value in original.values if name != "score"))
    null = replace(missing, values=((*missing.values, ("score", None))))
    assert "score" not in dict(decode_record(encode_record(missing)).values)
    assert dict(decode_record(encode_record(null)).values)["score"] is None
    shards.cleanup_spool(result)


async def test_invalid_typed_numeric_key_retains_shared_raw_equality():
    document = json.loads(_definition().canonical)
    document["schema"]["root"]["logical_key"] = ["npi", "score"]
    child = document["schema"]["children"][0]
    child["parent_key"].append({"child": "detail_score", "root": "score"})
    child["fields"].append({"id": "detail_score", "slot": 7, "type": "integer", "nullable": False})
    definition = CustomImportDefinition.from_mapping(document)
    stream = definition.source_streams[0]
    records = [
        prepare_record(
            definition,
            stream,
            _root(score=value),
            stream_slot=1,
            collection_slot=0,
            part_ordinal=1,
            row_ordinal=0,
            source_ordinal=0,
        )
        for value in (1, True)
    ]
    assert records[0].raw_parent_key == records[1].raw_parent_key
    assert records[0].typed_parent_key is not None and records[1].typed_parent_key is None
    assert records[1].rejection_code == "field_type_invalid" and shard_for(records[1], 4) is None
    assert decode_record(encode_record(records[1])) == records[1]


@pytest.mark.parametrize("refresh_mode", ["snapshot", "upsert"])
async def test_partition_preserves_new_and_rejected_family_identity_for_base_merge(tmp_path, refresh_mode):
    document = json.loads(_definition().canonical)
    document["refresh_mode"] = refresh_mode
    definition = CustomImportDefinition.from_mapping(document)
    original = assemble_root_families(
        definition, [_root(), _root("1234567893")], {"details": [_child(), _child("1234567893")]}
    )
    previous_by_key = {family.root_key: family for family in original.families}
    source_roots = [_root(score=9), _root("1234567893", score=None)]
    source_child_rows = [_child(amount="2.00")]
    definition, capture_input, parts = _capture_input(
        {"root_source": [source_roots], "detail_source": [source_child_rows]}, definition=definition
    )
    partition = await _partition(tmp_path, definition, capture_input, parts)
    occurrences = _all_records(partition)
    roots = [
        occurrence.normalized_values(definition, definition.source_streams[0])
        for occurrence in occurrences
        if occurrence.stream_slot == 1
    ]
    child_rows = [
        occurrence.normalized_values(definition, definition.source_streams[1])
        for occurrence in occurrences
        if occurrence.stream_slot == 2
    ]
    candidate = assemble_root_families(definition, roots, {"details": child_rows})
    reference = assemble_root_families(definition, source_roots, {"details": source_child_rows})
    assert candidate == reference
    assert merge_families(previous_by_key, candidate, refresh_mode=refresh_mode, complete_scope=True) == merge_families(
        previous_by_key, reference, refresh_mode=refresh_mode, complete_scope=True
    )
    assert (
        previous_by_key[("1234567893",)]
        in merge_families(previous_by_key, candidate, refresh_mode=refresh_mode, complete_scope=True).values()
    )
    shards.cleanup_spool(partition)


@pytest.mark.parametrize("damage", ["file", "manifest", "scope", "directory", "unknown"])
async def test_replay_refuses_changed_owned_artifacts(tmp_path, damage):
    definition, captured_source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    partitioned = await _partition(tmp_path, definition, captured_source, parts)
    owned = partitioned
    document = shards.manifest_document(partitioned)
    if damage == "file":
        path = partitioned.root / document["files"][0]["name"]
        content = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(content[:-1] + bytes([content[-1] ^ 1]))
        path.chmod(0o400)
    elif damage == "manifest":
        (partitioned.root / "manifest.json").chmod(0o600)
        (partitioned.root / "manifest.json").write_bytes(b"{}")
        (partitioned.root / "manifest.json").chmod(0o400)
    elif damage == "scope":
        partitioned = replace(partitioned, identity=replace(partitioned.identity, dataset_id=10))
    elif damage == "directory":
        partitioned = replace(partitioned, inode=partitioned.inode + 1)
    else:
        (partitioned.root / "unrelated.txt").write_text("preserve")
    if damage == "file":
        with pytest.raises(ValueError, match="digest"):
            _all_records(partitioned)
    with pytest.raises(ValueError):
        _all_records(partitioned)
    if damage == "unknown":
        assert (partitioned.root / "unrelated.txt").read_text() == "preserve"
        (partitioned.root / "unrelated.txt").unlink()
    shards.cleanup_spool(owned)


async def test_context_close_precedes_atomic_completion_and_empty_shard_replay(tmp_path):
    definition, source, parts = _capture_input({"root_source": [[]], "detail_source": [[]]})
    observations = []
    result = await _partition(
        tmp_path, definition, source, parts, source_context=_part_source(parts, observations=observations)
    )
    assert observations == ["iterator_closed", "context_closed"]
    assert shards.manifest_document(result)["records"] == 0 and _all_records(result) == []
    assert _all_records(result) == []
    shards.cleanup_spool(result)


async def test_consistently_sealed_wrong_record_count_is_refused_by_actual_decoder(tmp_path):
    definition, source, parts = _capture_input(
        {"root_source": [[_root()]], "detail_source": [[_child()]]},
        asserted_record_delta=1,
    )
    with pytest.raises(ValueError, match="decoded record count"):
        await _partition(tmp_path, definition, source, parts)
    assert not (tmp_path / "owned-spool").exists()


async def test_cancellation_closes_capture_and_discards_only_registered_spool(tmp_path):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    observations = []

    @asynccontextmanager
    async def canceled_source():
        async def records():
            try:
                yield parts[0]
                raise asyncio.CancelledError
            finally:
                observations.append("iterator_closed")

        iterator = records()
        try:
            yield iterator
        finally:
            await iterator.aclose()
            observations.append("context_closed")

    unrelated = tmp_path / "unrelated.txt"
    unrelated.write_text("preserve")
    with pytest.raises(asyncio.CancelledError):
        await _partition(tmp_path, definition, source, parts, source_context=canceled_source())
    assert observations == ["iterator_closed", "context_closed"]
    assert not (tmp_path / "owned-spool").exists() and unrelated.read_text() == "preserve"


async def test_partial_spool_write_failure_discards_owned_files(tmp_path, monkeypatch):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    original = shards._Spool.append

    def failed_append(self, lane, record):
        original(self, lane, record)
        raise OSError("synthetic exhausted spool budget")

    monkeypatch.setattr(shards._Spool, "append", failed_append)
    with pytest.raises(OSError, match="exhausted spool"):
        await _partition(tmp_path, definition, source, parts)
    assert not (tmp_path / "owned-spool").exists()


async def test_existing_directory_is_never_adopted_or_deleted(tmp_path):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    root = tmp_path / "existing"
    root.mkdir()
    sentinel = root / "unrelated.txt"
    sentinel.write_text("preserve")
    registered_paths = []
    with pytest.raises(FileExistsError):
        await shards.partition_capture(
            _part_source(parts),
            definition=definition,
            source=source,
            root=root,
            register_cleanup=registered_paths.append,
            limits=shards.ShardInputLimits(32 * 1024**2, 4, 1024),
        )
    assert sentinel.read_text() == "preserve" and registered_paths == []


@pytest.mark.parametrize(
    "kind,values",
    [
        ("root", _root()),
        ("root", _root(score=True)),
        ("root", _root(score=None)),
        ("root", _root(None)),
        ("root", _root("invalid")),
        ("root", _root(score=Decimal("7"))),
        ("child", _child()),
        ("child", _child(amount="1.0")),
        ("child", _child(amount="invalid")),
        ("child", _child(key=None)),
        ("child", _child(None)),
    ],
)
async def test_record_mapping_matches_existing_source_key_and_rejection_oracle(kind, values):
    definition = _definition()
    stream = next(stream for stream in definition.source_streams if stream.record_kind == kind)
    normalized = _normalized_integer_replay_values(dict(values), _stream_fields(definition, stream))
    _fields, key, code = _source_row_shape(definition, stream, normalized)
    typed = root_key_evidence_from_tuple(definition, key)
    if code is None and typed is None:
        code = "field_type_invalid"
    record = prepare_record(
        definition,
        stream,
        values,
        stream_slot=1 if kind == "root" else 2,
        collection_slot=0 if kind == "root" else 1,
        part_ordinal=1,
        row_ordinal=0,
        source_ordinal=0,
    )
    assert record.raw_parent_key == raw_family_key_evidence(key, maximum_canonical_bytes=268_435_456)
    assert record.typed_parent_key == typed and record.rejection_code == code
    if code is None and kind == "child":
        assert record.child_key == child_key_document(definition, stream.child_collection, normalized)
    assert decode_record(encode_record(record)) == record


async def test_total_spool_budget_failure_preserves_admission_semantics_and_cleans_up(tmp_path):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    with pytest.raises(OSError, match="spool byte budget"):
        await _partition(tmp_path, definition, source, parts, limits=shards.ShardInputLimits(1, 4, 1024))
    assert not (tmp_path / "owned-spool").exists()
    completed = await _partition(tmp_path, definition, source, parts)
    assert len(_all_records(completed)) == 2
    shards.cleanup_spool(completed)


async def test_primary_capture_failure_survives_reader_close_failure(tmp_path):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    observations = []
    with pytest.raises(RuntimeError, match="late capture failure") as failure:
        await _partition(
            tmp_path,
            definition,
            source,
            parts,
            source_context=_part_source(parts, fail_after=1, close_error=True, observations=observations),
        )
    assert observations == ["iterator_closed", "context_closed"]
    assert failure.value._custom_import_retry_blocked is True
    assert not (tmp_path / "owned-spool").exists()


async def test_decoder_allocation_failure_survives_cleanup_failure(tmp_path, monkeypatch):
    definition, source, parts = _capture_input({"root_source": [[_root()]], "detail_source": [[_child()]]})
    original = shards.iter_records
    observations = []

    class FailedDecoder:
        def __init__(self, iterator):
            self.iterator = iterator
            self.started = False

        def __iter__(self):
            return self

        def __next__(self):
            if self.started:
                raise MemoryError("synthetic decode allocation failure")
            self.started = True
            return next(self.iterator)

        def close(self):
            self.iterator.close()
            observations.append("decoder_closed")
            raise OSError("synthetic decoder close failure")

    monkeypatch.setattr(shards, "iter_records", lambda *args, **kwargs: FailedDecoder(original(*args, **kwargs)))
    with pytest.raises(MemoryError, match="decode allocation failure") as failure:
        await _partition(tmp_path, definition, source, parts)
    assert observations == ["decoder_closed"] and failure.value._custom_import_retry_blocked is True
    assert not (tmp_path / "owned-spool").exists()


def _temporal_definition():
    document = json.loads(_definition().canonical)
    document["schema"]["root"]["fields"].extend(
        [
            {"id": "event_date", "slot": 7, "type": "date", "nullable": False},
            {"id": "observed_at", "slot": 8, "type": "timestamp", "nullable": False},
        ]
    )
    return CustomImportDefinition.from_mapping(document)


async def test_actual_parquet_temporal_scalars_roundtrip_at_typed_source_boundary():
    definition = _temporal_definition()
    stream = definition.source_streams[0]
    original_time = datetime(2026, 11, 4, 1, 2, 3, 456789, tzinfo=ZoneInfo("Asia/Kathmandu"))
    table = pa.table(
        {
            "npi": ["1003000126"],
            "score": [7],
            "enabled": [True],
            "event_date": pa.array([date(2026, 11, 4)], type=pa.date32()),
            "observed_at": pa.array([original_time], type=pa.timestamp("us", tz="Asia/Kathmandu")),
        }
    )
    output = BytesIO()
    pq.write_table(table, output, compression="NONE")
    decoded = pq.read_table(BytesIO(output.getvalue())).to_pylist()[0]
    occurrence = prepare_record(
        definition, stream, decoded, stream_slot=1, collection_slot=0, part_ordinal=1, row_ordinal=0, source_ordinal=0
    )
    restored = decode_record(encode_record(occurrence))
    restored_by_field = dict(restored.values)
    assert type(restored_by_field["event_date"]) is date and restored_by_field["event_date"] == decoded["event_date"]
    assert restored_by_field["observed_at"].isoformat() == decoded["observed_at"].isoformat()
    assert restored_by_field["observed_at"].utcoffset() == decoded["observed_at"].utcoffset()
    assert restored_by_field["observed_at"].microsecond == 456789
    assert getattr(restored_by_field["observed_at"].tzinfo, "key", None) == "Asia/Kathmandu"
    _fields, key, code = _source_row_shape(definition, stream, decoded)
    assert occurrence.rejection_code == code
    assert code is None
    assert restored.typed_parent_key == root_key_evidence_from_tuple(definition, key)
    assert restored.raw_parent_key == raw_family_key_evidence(key, maximum_canonical_bytes=268_435_456)
    # The logical typed SOURCE domain is wider than today's physical capture domain.
    capture = capture_stream(
        BytesIO(output.getvalue()), stream, source_snapshot_token="synthetic-sealed", limits=_policy().part_limits
    )
    with pytest.raises(CaptureError, match="unsupported"):
        list(iter_parquet_source_batches(capture, stream, limits=_policy().part_limits))


@pytest.mark.parametrize(
    "timestamp",
    [
        datetime(2026, 11, 1, 1, 30, 0, 987654, tzinfo=ZoneInfo("America/New_York"), fold=1),
        datetime(
            2026,
            11,
            4,
            1,
            2,
            3,
            123456,
            tzinfo=timezone(timedelta(hours=5, minutes=45, seconds=13, microseconds=2200), "synthetic-offset"),
        ),
        datetime(2026, 11, 4, 1, 2, 3, 123456, fold=1),
    ],
)
async def test_timestamp_offset_fold_microseconds_and_original_rejection_are_preserved(timestamp):
    definition = _temporal_definition()
    stream = definition.source_streams[0]
    values_by_field = {**_root(), "event_date": date(2026, 11, 4), "observed_at": timestamp}
    record = prepare_record(
        definition,
        stream,
        values_by_field,
        stream_slot=1,
        collection_slot=0,
        part_ordinal=1,
        row_ordinal=0,
        source_ordinal=0,
    )
    actual = dict(decode_record(encode_record(record)).values)["observed_at"]
    assert actual.isoformat() == timestamp.isoformat() and actual.fold == timestamp.fold
    assert actual.utcoffset() == timestamp.utcoffset() and actual.tzname() == timestamp.tzname()
    assert actual.microsecond == timestamp.microsecond
    assert record.rejection_code == _source_row_shape(definition, stream, values_by_field)[2]


@pytest.mark.parametrize("value", [False, 1, Decimal("-0.00"), -0.0, float("nan"), float("inf")])
async def test_spool_keeps_original_scalar_type_spelling_and_invalid_float_state(value):
    definition = _definition()
    stream = definition.source_streams[0]
    record = prepare_record(
        definition,
        stream,
        _root(score=value),
        stream_slot=1,
        collection_slot=0,
        part_ordinal=1,
        row_ordinal=0,
        source_ordinal=0,
    )
    actual = dict(decode_record(encode_record(record)).values)["score"]
    assert type(actual) is type(value)
    if type(value) is Decimal:
        assert actual.as_tuple() == value.as_tuple()
    elif type(value) is float and math.isnan(value):
        assert math.isnan(actual)
    else:
        assert actual == value
    if type(value) is float:
        assert math.copysign(1, actual) == math.copysign(1, value)
        assert record.rejection_code == "field_type_invalid"


async def test_unknown_scalar_extensions_are_refused():
    definition = _definition()
    record = prepare_record(
        definition,
        definition.source_streams[0],
        _root(),
        stream_slot=1,
        collection_slot=0,
        part_ordinal=1,
        row_ordinal=0,
        source_ordinal=0,
    )
    wire_cells = list(msgpack.unpackb(encode_record(record), raw=False))
    wire_cells[5][0][1] = msgpack.ExtType(99, b"unsupported")
    with pytest.raises(ValueError, match="extension is unsupported"):
        decode_record(msgpack.packb(wire_cells, use_bin_type=True))


@pytest.mark.parametrize("asserted_delta", [-1, 1])
async def test_asserted_arrow_allocation_remains_immutable_without_new_decode_equality(tmp_path, asserted_delta):
    definition, source, parts = _capture_input(
        {"root_source": [[_root()]], "detail_source": [[]]}, asserted_arrow_delta=asserted_delta
    )
    expected_manifest = source.canonical_manifest
    asserted = parts[0].arrow_byte_count
    actual = _aggregate_parquet_arrow_bytes(parts[0].capture, limits=source.policy.part_limits, decoded_bytes=0)
    assert (asserted, actual) == (24 + asserted_delta, 24)
    result = await _partition(tmp_path, definition, source, parts)
    assert source.canonical_manifest == expected_manifest and parts[0].arrow_byte_count == asserted
    assert len(_all_records(result)) == 1
    shards.cleanup_spool(result)


async def test_actual_arrow_decode_uses_existing_independent_part_budget(tmp_path):
    document = _policy().to_mapping()
    document["maximum_part_arrow_bytes"] = 23
    policy = SegmentedCapturePolicy.from_mapping(document)
    definition, source, parts = _capture_input(
        {"root_source": [[_root()]], "detail_source": [[]]}, asserted_arrow_delta=-1, policy=policy
    )
    assert parts[0].arrow_byte_count == 23
    with pytest.raises(ValueError, match="decoded Arrow bytes exceed"):
        await _partition(tmp_path, definition, source, parts)
    assert not (tmp_path / "owned-spool").exists()
