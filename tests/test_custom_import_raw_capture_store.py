# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable raw-object registration checks using a caller-owned fake connection."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
from contextlib import nullcontext
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from db.models.custom_import import CustomImportCapture, CustomImportCaptureBundle, CustomImportDataset
from db.models.custom_import_raw_capture import RAW_OBJECT_CONTRACT, CustomImportCaptureRawObject
from process.custom_import import capture_store, raw_capture_store
from process.custom_import.capture import capture_stream
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.capture_store import CaptureBundleConflict, CapturePayloadUnavailable, CaptureStoreError
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.runner_types import CandidateRegistry
from tests.test_custom_import_definition import _raw_definition

_IDENTITY = {"dataset_id": 1, "definition_revision_id": 2, "schema_revision_id": 3}
_QUOTA = 1024 * 1024


@pytest.mark.parametrize("damage", ["limits_type", "captures_type", "empty", "unsealed", "payload_type", "size"])
async def test_raw_input_shapes_fail_before_authority_quota_or_writes(monkeypatch, damage):
    session, captures, quota = _case(monkeypatch)
    options_by_name = {"limits": object()} if damage == "limits_type" else {}
    captures_by_damage = {
        "limits_type": captures,
        "captures_type": list(captures),
        "empty": (),
        "unsealed": (object(),),
        "payload_type": (replace(captures[0], payload=bytearray(captures[0].payload)),),
        "size": (replace(captures[0], manifest=replace(captures[0].manifest, decoded_bytes=True)),),
    }
    captures = captures_by_damage[damage]
    with pytest.raises(CaptureStoreError):
        await _register(session, captures, **options_by_name)
    assert not session.events and not session.bundles and not session.copy_calls
    quota.assert_not_awaited()


async def test_raw_registration_slot_drift_stops_before_copy(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    monkeypatch.setattr(capture_store, "_validated_streams", AsyncMock(return_value=(("providers", 99),)))
    with pytest.raises(CaptureBundleConflict, match="stream slots have drifted"):
        await raw_capture_store._copy_raw_objects(
            session, SimpleNamespace(stream_slots=(7, 2)), SimpleNamespace(**_IDENTITY), captures
        )
    assert not session.copy_calls


async def test_raw_invalid_persisted_definition_stops_before_authority(monkeypatch):
    session, captures, quota = _case(monkeypatch)
    session.definition.canonical_definition = "not-json"
    with pytest.raises(CaptureBundleConflict, match="definition is invalid"):
        await _register(session, captures)
    assert not session.events
    quota.assert_not_awaited()


def _query_result(query_rows):
    return SimpleNamespace(
        all=lambda: query_rows,
        scalars=lambda: SimpleNamespace(all=lambda: query_rows),
        scalar_one_or_none=lambda: query_rows[0] if query_rows else None,
    )


class _Session:
    new = dirty = deleted = ()
    no_autoflush = nullcontext()

    def __init__(self, definition):
        self.definition = SimpleNamespace(dataset_id=1, schema_revision_id=3, canonical_definition=definition.canonical)
        self.bundles = []
        self.headers = []
        self.raw_records = []
        self.events = []
        self.statements = []
        self.copy_calls = []
        self.has_parquet = False
        self.copy_failure = None
        self.after_copy = None
        self.after_metadata = None
        self.execution_options_by_name = {}
        self.sync_connection = SimpleNamespace(
            get_execution_options=lambda: self.execution_options_by_name,
            dialect=SimpleNamespace(default_schema_name="synthetic_default_schema"),
        )

    def in_transaction(self):
        return True

    async def get(self, _model, _identifier):
        return self.definition

    async def execute(self, statement):
        self.statements.append(statement)
        entity = statement.column_descriptions[0].get("entity")
        if entity is CustomImportDataset:
            self.events.append("lock")
            return _query_result([SimpleNamespace(dataset_id=1)])
        if entity is CustomImportCaptureBundle:
            return _query_result(self.bundles)
        if entity is CustomImportCapture:
            return _query_result(self.headers)
        if entity is CustomImportCaptureRawObject:
            return _query_result(self._raw_query_rows(statement))
        raise AssertionError("unexpected query")

    def _raw_query_rows(self, statement):
        raw_records = sorted(self.raw_records, key=lambda raw_row: raw_row[1])
        if len(statement.selected_columns) != 10:
            self.events.append("payload")
            return raw_records
        self.events.append("metadata")
        metadata_rows = [raw_row[:8] + (len(raw_row[8]), self.has_parquet) for raw_row in raw_records]
        if self.after_metadata is not None:
            self.after_metadata(self)
        return metadata_rows

    def add(self, bundle):
        bundle.capture_bundle_id = 7
        bundle.capture_state = "sealed"
        self.bundles.append(bundle)

    def add_all(self, rows):
        for row in rows:
            row.capture_state = "sealed"
            self.headers.append(row)

    async def flush(self):
        self.events.append("flush")

    async def connection(self):
        return self

    async def get_raw_connection(self):
        return SimpleNamespace(driver_connection=self)

    async def copy_records_to_table(self, table_name, **options):
        self.events.append("copy")
        self.copy_calls.append((table_name, options))
        if self.copy_failure is not None:
            raise self.copy_failure
        self.raw_records.extend(options["records"])
        if self.after_copy is not None:
            self.after_copy(self)


def _original(format_name):
    if format_name == "parquet":
        destination = BytesIO()
        pq.write_table(pa.table({"id": ["1"], "name": ["Synthetic"]}), destination)
        return destination.getvalue()
    return {
        "csv": b"id,name\n1,Synthetic\n",
        "tsv": b"id\tname\n1\tSynthetic\n",
        "json": b'[{"id":"1","name":"Synthetic"}]',
        "ndjson": b'{"id":"1","name":"Synthetic"}\n',
        "xml": b"<rows><row><id>1</id><name>Synthetic</name></row></rows>",
    }[format_name]


def _case(monkeypatch, *, format_name="csv", compression="none", original_payload=None):
    document = _raw_definition()
    for stream in document["streams"]:
        stream.update(format=format_name, compression=compression)
        if format_name == "xml":
            stream["record_path"] = "row"
    definition = CustomImportDefinition.from_mapping(document)
    original = _original(format_name) if original_payload is None else original_payload
    if compression == "gzip":
        original = gzip.compress(original, mtime=0)
    captures = tuple(
        capture_stream(BytesIO(original), stream, source_snapshot_token="synthetic-snapshot")
        for stream in definition.source_streams
    )
    session = _Session(definition)

    async def registry(actual_session, request):
        assert actual_session is session
        assert request.definition.canonical == definition.canonical
        assert (request.dataset_id, request.definition_revision_id, request.schema_revision_id) == (1, 2, 3)
        session.events.append("registry")
        return CandidateRegistry(
            child_collection_slots={"rates": 1}, stream_slots={"providers": 7, "rates": 2}, root_stream_slot=7
        )

    async def quota(actual_session, **options):
        assert actual_session is session
        assert options["dataset_id"] == 1
        assert options["new_byte_count"] == sum(len(capture.payload) for capture in captures)
        session.events.append("quota")

    monkeypatch.setattr(capture_store, "load_registry", registry)
    monkeypatch.setattr(
        raw_capture_store,
        "require_raw_capture_authority",
        AsyncMock(side_effect=lambda _session: session.events.append("authority")),
    )
    quota_check = AsyncMock(side_effect=quota)
    monkeypatch.setattr(raw_capture_store.capture_retention, "validate_dataset_retained_bytes", quota_check)
    return session, captures, quota_check


async def _register(session, captures, **options):
    return await raw_capture_store.register_raw_capture_bundle(
        session, **_IDENTITY, captures=captures, maximum_dataset_retained_bytes=options.pop("quota", _QUOTA), **options
    )


async def _load(session, **options):
    return await raw_capture_store.load_replayable_raw_bundle(session, capture_bundle_id=7, **_IDENTITY, **options)


@pytest.mark.parametrize("format_name", ("csv", "tsv", "json", "ndjson", "xml", "parquet"))
@pytest.mark.parametrize("compression", ("none", "gzip"))
async def test_original_formats_round_trip_without_parquet_relabeling(monkeypatch, format_name, compression):
    session, captures, quota = _case(monkeypatch, format_name=format_name, compression=compression)

    first = await _register(session, tuple(reversed(captures)))
    retained = await _load(session)
    second = await _register(session, captures)

    assert first == capture_store.CaptureBundleRegistration(7, (2, 7), True)
    assert second == replace(first, created=False)
    assert retained == tuple(reversed(captures))
    assert all(
        retained_capture.manifest.format == format_name and retained_capture.manifest.compression == compression
        for retained_capture in retained
    )
    quota.assert_awaited_once_with(
        session,
        dataset_id=1,
        new_byte_count=sum(len(capture.payload) for capture in captures),
        maximum_retained_bytes=_QUOTA,
    )
    assert len(session.copy_calls) == 1
    table_name, options = session.copy_calls[0]
    assert table_name == CustomImportCaptureRawObject.__tablename__
    assert options["schema_name"] == CustomImportCaptureRawObject.__table__.schema
    assert options["columns"] == raw_capture_store._COPY_COLUMNS
    assert [copied_record[1] for copied_record in options["records"]] == [2, 7]
    assert [copied_record[8] for copied_record in options["records"]] == [capture.payload for capture in retained]
    assert session.events.index("lock") < session.events.index("authority") < session.events.index("quota")
    assert session.events.index("quota") < session.events.index("copy")
    assert session.events.index("copy") < session.events.index("metadata") < session.events.index("payload")
    assert all(header.payload_contract is None for header in session.headers)
    assert all(
        header.manifest_sha256 == bytes.fromhex(captures[0].manifest.capture_sha256)
        for header in session.headers
        if header.stream_slot == 7
    )


async def test_empty_originals_are_retained_and_zero_quota_is_valid(monkeypatch):
    session, captures, quota = _case(monkeypatch, original_payload=b"")
    await _register(session, captures, quota=0)
    assert all(capture.payload == b"" for capture in await _load(session))
    assert quota.await_args.kwargs["new_byte_count"] == 0


async def test_new_originals_recheck_authority_before_quota_or_copy(monkeypatch):
    session, captures, quota = _case(monkeypatch)
    check = AsyncMock(side_effect=CaptureStoreError("raw capture authority denied"))
    monkeypatch.setattr(raw_capture_store, "require_raw_capture_authority", check)
    with pytest.raises(CaptureStoreError, match="authority denied"):
        await _register(session, captures)
    check.assert_awaited_once_with(session)
    quota.assert_not_awaited()
    assert not session.copy_calls


@pytest.mark.parametrize(
    ("logical_schema", "schema_translate_map", "expected_schema"),
    (
        ("mrf", None, "mrf"),
        ("mrf", {}, "mrf"),
        ("mrf", {"mrf": "synthetic_raw_schema"}, "synthetic_raw_schema"),
        ("mrf", {"mrf": None}, "synthetic_default_schema"),
        ("mrf", {"mrf": ""}, "synthetic_default_schema"),
        ("mrf", {None: "synthetic_default_schema"}, "mrf"),
        (None, None, None),
        (None, {None: "synthetic_default_schema"}, "synthetic_default_schema"),
        (None, {None: None}, "synthetic_default_schema"),
    ),
)
async def test_copy_uses_caller_schema_translation(monkeypatch, logical_schema, schema_translate_map, expected_schema):
    session, captures, _ = _case(monkeypatch)
    monkeypatch.setattr(CustomImportCaptureRawObject.__table__, "schema", logical_schema)
    session.execution_options_by_name = {"schema_translate_map": schema_translate_map}
    await _register(session, captures)
    assert session.copy_calls[0][1]["schema_name"] == expected_schema


async def test_copy_requires_actual_connection_options(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    session.sync_connection = None
    with pytest.raises(AttributeError):
        await _register(session, captures)
    assert not session.copy_calls


async def test_copy_rejects_missing_translated_default_schema(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    session.execution_options_by_name = {"schema_translate_map": {"mrf": None}}
    session.sync_connection.dialect.default_schema_name = None
    with pytest.raises(CaptureStoreError, match="default schema"):
        await _register(session, captures)
    assert not session.copy_calls


@pytest.mark.parametrize("damage", ("missing", "duplicate", "extra", "payload", "manifest", "stream", "snapshot"))
async def test_invalid_submissions_fail_before_registration(monkeypatch, damage):
    session, captures, quota = _case(monkeypatch)
    if damage == "missing":
        captures = captures[:1]
    elif damage == "duplicate":
        captures = (captures[0], captures[0])
    elif damage == "extra":
        captures += (replace(captures[0], manifest=replace(captures[0].manifest, stream_id="extra")),)
    elif damage == "payload":
        captures = (replace(captures[0], payload=b"x" * len(captures[0].payload)), captures[1])
    else:
        changes = {
            "manifest": {"capture_sha256": "0" * 64},
            "stream": {"format": "json"},
            "snapshot": {"source_snapshot_token": "other-snapshot"},
        }[damage]
        captures = (replace(captures[0], manifest=replace(captures[0].manifest, **changes)), captures[1])
    with pytest.raises(CaptureStoreError):
        await _register(session, captures)
    assert not session.bundles and not session.copy_calls
    quota.assert_not_awaited()


async def test_wrong_definition_owner_is_rejected_before_writes(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    session.definition.dataset_id = 99
    with pytest.raises(CaptureBundleConflict, match="persisted definition"):
        await _register(session, captures)
    assert not session.events


@pytest.mark.parametrize("damage", ("transaction", "dirty", "limits", "quota"))
async def test_registration_boundaries_fail_before_database_work(monkeypatch, damage):
    session, captures, _ = _case(monkeypatch)
    options_by_name = {}
    if damage == "transaction":
        session.in_transaction = lambda: False
    elif damage == "dirty":
        session.dirty = (object(),)
    elif damage == "limits":
        options_by_name["limits"] = replace(CaptureLimits(), maximum_compressed_bytes=65 * 1024 * 1024)
    else:
        options_by_name["quota"] = True
    with pytest.raises(CaptureStoreError):
        await _register(session, captures, **options_by_name)
    assert not session.events


async def test_aggregate_limits_apply_before_registration_and_payload_reads(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    limits = replace(CaptureLimits(), maximum_compressed_bytes=len(captures[0].payload))
    with pytest.raises(CaptureStoreError, match="aggregate"):
        await _register(session, captures, limits=limits)
    assert not session.events
    await _register(session, captures)
    session.events.clear()
    with pytest.raises(CaptureStoreError, match="aggregate"):
        await _load(session, limits=limits)
    assert "payload" not in session.events


async def test_decoded_aggregate_is_bounded_independently_of_compressed_bytes(monkeypatch):
    session, captures, _ = _case(monkeypatch, compression="gzip", original_payload=b"x" * 40)
    limits = replace(CaptureLimits(), maximum_decoded_bytes=60, maximum_record_bytes=60)
    with pytest.raises(CaptureStoreError, match="aggregate"):
        await _register(session, captures, limits=limits)
    assert not session.events


async def test_metadata_only_registration_is_never_backfilled_or_recharged(monkeypatch):
    session, captures, quota = _case(monkeypatch)
    receipts = tuple(raw_capture_store._receipt(capture.manifest) for capture in captures)
    await capture_store.register_capture_bundle(session, **_IDENTITY, receipts=receipts)
    with pytest.raises(CapturePayloadUnavailable):
        await _register(session, captures)
    assert not session.copy_calls
    quota.assert_not_awaited()


@pytest.mark.parametrize(
    "damage",
    ("missing", "extra", "duplicate", "dataset", "definition", "schema", "contract", "length", "digest", "parquet"),
)
async def test_corrupt_raw_metadata_is_rejected_before_payload_read(monkeypatch, damage):
    session, captures, quota = _case(monkeypatch)
    await _register(session, captures)
    quota.reset_mock()
    if damage == "missing":
        session.raw_records.pop()
    elif damage in {"extra", "duplicate"}:
        raw_fields = list(session.raw_records[0])
        if damage == "extra":
            raw_fields[1] = 99
        session.raw_records.append(tuple(raw_fields))
    elif damage == "parquet":
        session.has_parquet = True
    else:
        raw_fields = list(session.raw_records[0])
        field_index, field_value = {
            "dataset": (2, 99),
            "definition": (3, 99),
            "schema": (4, 99),
            "contract": (5, "custom-import/parquet-parts/v1"),
            "length": (6, 0),
            "digest": (7, b"x" * 32),
        }[damage]
        raw_fields[field_index] = field_value
        session.raw_records[0] = tuple(raw_fields)
    session.events.clear()
    with pytest.raises(CaptureBundleConflict):
        await _register(session, captures)
    assert "payload" not in session.events and "copy" not in session.events
    quota.assert_not_awaited()


@pytest.mark.parametrize(
    "damage",
    ("bundle", "header_owner", "header_contract", "manifest_contract", "manifest_hash", "manifest_size", "digest_type"),
)
async def test_corrupt_headers_fail_before_raw_payload_queries(monkeypatch, damage):
    session, captures, _ = _case(monkeypatch)
    await _register(session, captures)
    if damage == "bundle":
        session.bundles[0].manifest_sha256 = b"x" * 32
    else:
        attribute_name, replacement = {
            "header_owner": ("schema_revision_id", 99),
            "header_contract": ("payload_contract", "custom-import/parquet-parts/v1"),
            "manifest_contract": ("canonical_manifest", canonical_json({"contract": "other", "capture": {}})),
            "manifest_hash": ("manifest_sha256", b"x" * 32),
            "digest_type": ("content_sha256", 2**63),
            "manifest_size": ("byte_count", session.headers[0].byte_count + 1),
        }[damage]
        setattr(session.headers[0], attribute_name, replacement)
    session.events.clear()
    with pytest.raises(CaptureStoreError):
        await _load(session)
    assert "metadata" not in session.events and "payload" not in session.events


@pytest.mark.parametrize("damage", ("payload", "rows", "owner", "count"))
async def test_changes_after_preflight_cannot_escape_readback(monkeypatch, damage):
    session, captures, _ = _case(monkeypatch)
    await _register(session, captures)

    def change_payload(actual):
        if damage == "rows":
            actual.raw_records.pop()
            return
        raw_fields = list(actual.raw_records[0])
        if damage == "payload":
            raw_fields[8] = b"x" * len(raw_fields[8])
        elif damage == "owner":
            raw_fields[2] = 99
        else:
            raw_fields[6] += 1
        actual.raw_records[0] = tuple(raw_fields)

    session.after_metadata = change_payload
    with pytest.raises(CaptureBundleConflict):
        await _load(session)


async def test_payload_query_is_pinned_to_preflighted_headers_and_size(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    await _register(session, captures)
    metadata_statement, payload_statement = session.statements[-2:]
    assert "payload" not in metadata_statement.selected_columns.keys()
    compiled = payload_statement.compile()
    predicate = str(payload_statement.whereclause)
    for column in raw_capture_store._METADATA_COLUMNS:
        assert column.name in predicate
    assert "octet_length" in predicate
    assert RAW_OBJECT_CONTRACT in compiled.params.values()
    assert hashlib.sha256(captures[0].payload).digest() in compiled.params.values()


@pytest.mark.parametrize("failure", (RuntimeError("copy failed"), asyncio.CancelledError()))
async def test_copy_failure_or_cancellation_propagates_without_readback(monkeypatch, failure):
    session, captures, _ = _case(monkeypatch)
    session.copy_failure = failure
    with pytest.raises(type(failure)):
        await _register(session, captures)
    assert "payload" not in session.events and not session.raw_records


async def test_quota_failure_stops_before_copy(monkeypatch):
    session, captures, quota = _case(monkeypatch)
    quota.side_effect = CaptureBundleConflict("quota exceeded")
    with pytest.raises(CaptureBundleConflict, match="quota"):
        await _register(session, captures)
    assert not session.copy_calls


async def test_registration_requires_exact_copy_readback(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    session.after_copy = lambda actual: actual.raw_records.pop()
    with pytest.raises(CaptureBundleConflict):
        await _register(session, captures)


async def test_registration_rejects_a_different_complete_readback(monkeypatch):
    session, captures, _ = _case(monkeypatch)
    changed = (replace(captures[0], payload=b"different"), captures[1])
    monkeypatch.setattr(raw_capture_store, "load_replayable_raw_bundle", AsyncMock(return_value=changed))
    with pytest.raises(CaptureBundleConflict, match="submitted originals"):
        await _register(session, captures)
