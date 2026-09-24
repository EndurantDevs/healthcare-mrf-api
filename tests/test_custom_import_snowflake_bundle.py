# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for one-statement Snowflake parent/child acquisition."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from decimal import Decimal
from io import BytesIO
from threading import current_thread
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import process.custom_import.snowflake_bundle as snowflake_bundle
import process.custom_import.snowflake_bundle_replay as snowflake_bundle_replay
import process.custom_import.snowflake_candidate as snowflake_candidate
from process.custom_import.capture import CaptureLimits, iter_records
from process.custom_import.capture_store import CaptureReceipt, ReplayableParquetCapture
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake import (
    SnowflakeApprovedRelation,
    SnowflakeConnectorError,
    SnowflakeCredentialError,
    SnowflakeDeclaredColumn,
    SnowflakeKeyPairCredentials,
    SnowflakeParquetResult,
    SnowflakeRelation,
    SnowflakeResultColumn,
)
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleBinding,
    SnowflakeBundleEncoding,
    SnowflakeBundleError,
    SnowflakeBundleRequest,
    SnowflakeBundleResult,
    SnowflakeBundleStatement,
    SnowflakeBundleStreamMetadata,
    SnowflakeBundleStreamResult,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_candidate import (
    SnowflakeBundleCandidateRequest,
    SnowflakeCandidateError,
    run_snowflake_bundle_candidate,
)
from process.custom_import.snowflake_python import SnowflakePythonConnectorAdapter

_PRIVATE_KEY = b"-----BEGIN PRIVATE KEY-----\nsynthetic-key\n-----END PRIVATE KEY-----"
_SNAPSHOT = "synthetic-release-20260922"
_ROOT_SCHEMA = (
    SnowflakeResultColumn(field_id="npi", source_type="TEXT", nullable=False),
    SnowflakeResultColumn(field_id="score", source_type="FIXED(18,0)", nullable=False),
    SnowflakeResultColumn(field_id="enabled", source_type="BOOLEAN", nullable=False),
)
_DETAIL_SCHEMA = (
    SnowflakeResultColumn(field_id="detail_npi", source_type="TEXT", nullable=False),
    SnowflakeResultColumn(field_id="detail_id", source_type="TEXT", nullable=False),
    SnowflakeResultColumn(field_id="amount", source_type="FIXED(30,12)", nullable=False),
)


@dataclass(frozen=True)
class _ResultOptions:
    root_tokens: tuple[str | None, ...] = (_SNAPSHOT,)
    detail_tokens: tuple[str | None, ...] = (_SNAPSHOT,)
    detail_rows: bool = False
    root_close_error: BaseException | None = None
    on_close: object | None = None
    root_readers: tuple[object, ...] | None = None
    detail_readers: tuple[object, ...] | None = None
    root_score_source_type: str = "FIXED(18,0)"
    root_score_nullable: bool = False
    root_partition_count: int = 1
    detail_partition_count: int = 1


class _Reader:
    def __init__(self, payload: bytes, *, close_error: BaseException | None = None) -> None:
        self._payload = payload
        self._close_error = close_error
        self.close_count = 0

    def read(self, _size: int) -> bytes:
        payload, self._payload = self._payload, b""
        return payload

    def close(self) -> None:
        self.close_count += 1
        if self._close_error is not None:
            raise self._close_error


class _Iterator:
    def __init__(self, owner: _Sources) -> None:
        self._owner = owner
        self._closed = False
        self.close_count = 0

    def __iter__(self) -> _Iterator:
        return self

    def __next__(self) -> object:
        if self._closed or self._owner._index == len(self._owner.readers):
            raise StopIteration
        reader = self._owner.readers[self._owner._index]
        self._owner._index += 1
        return reader

    def close(self) -> None:
        self._closed = True
        self.close_count += 1


class _Sources:
    def __init__(self, readers: tuple[object, ...]) -> None:
        self.readers = readers
        self._index = 0
        self.close_count = 0
        self.iterator: _Iterator | None = None

    def __iter__(self) -> _Iterator:
        if self.iterator is not None:
            raise AssertionError("bundle sources must be consumed once")
        self.iterator = _Iterator(self)
        return self.iterator

    def close(self) -> None:
        self.close_count += 1
        for reader in self.readers[self._index :]:
            reader.close()
        self._index = len(self.readers)


class _CloseOnlyReader:
    def __init__(self) -> None:
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class _Credentials:
    def load_key_pair(self) -> SnowflakeKeyPairCredentials:
        return SnowflakeKeyPairCredentials(
            account="synthetic-account",
            user="synthetic-reader",
            private_key_pem=_PRIVATE_KEY,
        )


class _Adapter:
    def __init__(self, result_factory) -> None:
        self._result_factory = result_factory
        self.statements = []

    def fetch_bundle(self, statement, _credentials) -> SnowflakeBundleResult:
        self.statements.append(statement)
        return self._result_factory()


class _PythonCursor:
    def __init__(self, rows: tuple[tuple[object, ...], ...]) -> None:
        self.description = (
            SimpleNamespace(name="__ci_bundle_row_kind", type_name="FIXED", is_nullable=False, precision=1, scale=0),
            SimpleNamespace(name="__ci_stream_ordinal", type_name="FIXED", is_nullable=False, precision=2, scale=0),
            SimpleNamespace(name="__ci_stream_id", type_name="TEXT", is_nullable=False),
            SimpleNamespace(name="__ci_source_snapshot_token", type_name="TEXT", is_nullable=True),
            SimpleNamespace(name="npi", type_name="TEXT", is_nullable=True),
            SimpleNamespace(name="score", type_name="FIXED", is_nullable=True, precision=18, scale=0),
            SimpleNamespace(name="enabled", type_name="BOOLEAN", is_nullable=True),
            SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True),
            SimpleNamespace(name="detail_id", type_name="TEXT", is_nullable=True),
            SimpleNamespace(name="amount", type_name="FIXED", is_nullable=True, precision=30, scale=12),
        )
        self.sfqid = "synthetic-common-query"
        self._rows = list(rows)
        self.executed: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def close(self) -> None:
        self.closed = True


class _PythonConnection:
    def __init__(self, cursor: _PythonCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _PythonCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def _definition(*, score_nullable: bool = False) -> CustomImportDefinition:
    return CustomImportDefinition.from_mapping(
        {
            "contract": "custom-import/v1",
            "revision": {"definition": 1, "schema": 1},
            "refresh_mode": "snapshot",
            "streams": [
                {
                    "id": "root_source",
                    "kind": "root",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "semantic_snapshot",
                },
                {
                    "id": "detail_source",
                    "kind": "child",
                    "child": "details",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "semantic_snapshot",
                },
            ],
            "schema": {
                "root": {
                    "logical_key": ["npi"],
                    "entity": {"adapter": "npi", "field": "npi"},
                    "fields": [
                        {"id": "npi", "slot": 1, "type": "string", "nullable": False},
                        {"id": "score", "slot": 2, "type": "integer", "nullable": score_nullable},
                        {"id": "enabled", "slot": 3, "type": "boolean", "nullable": False},
                    ],
                },
                "children": [
                    {
                        "name": "details",
                        "parent_key": [{"child": "detail_npi", "root": "npi"}],
                        "child_key": ["detail_id"],
                        "fields": [
                            {"id": "detail_npi", "slot": 4, "type": "string", "nullable": False},
                            {"id": "detail_id", "slot": 5, "type": "string", "nullable": False},
                            {"id": "amount", "slot": 6, "type": "decimal", "nullable": False},
                        ],
                    }
                ],
            },
            "aliases": {"root_source": {}, "detail_source": {}},
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _definition_with_aliases(aliases: dict[str, dict[str, str]]) -> CustomImportDefinition:
    document = json.loads(_definition().canonical)
    document["aliases"] = aliases
    return CustomImportDefinition.from_mapping(document)


def _definition_with_revision(definition_revision: int) -> CustomImportDefinition:
    document = json.loads(_definition().canonical)
    document["revision"]["definition"] = definition_revision
    return CustomImportDefinition.from_mapping(document)


def _relations() -> tuple[SnowflakeApprovedRelation, ...]:
    return (
        SnowflakeApprovedRelation(
            relation=SnowflakeRelation(database="synthetic", schema="public", name="snapshots"),
            columns=(SnowflakeDeclaredColumn(field_id="semantic_snapshot", column_identifier="snapshot_token"),),
        ),
        SnowflakeApprovedRelation(
            relation=SnowflakeRelation(database="synthetic", schema="public", name="roots"),
            columns=(
                SnowflakeDeclaredColumn(field_id="npi", column_identifier="root_npi"),
                SnowflakeDeclaredColumn(field_id="score", column_identifier="root_score"),
                SnowflakeDeclaredColumn(field_id="enabled", column_identifier="root_enabled"),
            ),
        ),
        SnowflakeApprovedRelation(
            relation=SnowflakeRelation(database="synthetic", schema="public", name="details"),
            columns=(
                SnowflakeDeclaredColumn(field_id="detail_npi", column_identifier="detail_parent_npi"),
                SnowflakeDeclaredColumn(field_id="detail_id", column_identifier="detail_key"),
                SnowflakeDeclaredColumn(field_id="amount", column_identifier="detail_amount"),
            ),
        ),
    )


def _bindings(*, child_token_key: str | None = "semantic_snapshot") -> tuple[SnowflakeBundleBinding, ...]:
    snapshot_relation, root_relation, detail_relation = _relations()
    return (
        SnowflakeBundleBinding(
            stream_id="detail_source",
            relation=detail_relation.relation,
            source_snapshot_token_relation=snapshot_relation.relation,
            selected_field_ids=("amount", "detail_id", "detail_npi"),
            semantic_token_metadata_key=child_token_key,
        ),
        SnowflakeBundleBinding(
            stream_id="root_source",
            relation=root_relation.relation,
            source_snapshot_token_relation=snapshot_relation.relation,
            selected_field_ids=("enabled", "npi", "score"),
            semantic_token_metadata_key="semantic_snapshot",
        ),
    )


def _parquet(table: pa.Table) -> bytes:
    destination = BytesIO()
    pq.write_table(table, destination, compression="zstd")
    return destination.getvalue()


def _fixed38_root_payload(score: Decimal | None) -> bytes:
    return _parquet(
        pa.table(
            {
                "npi": pa.array(["1003000126"], type=pa.string()),
                "score": pa.array([score], type=pa.decimal128(38, 0)),
                "enabled": pa.array([True], type=pa.bool_()),
            }
        )
    )


def _fenced_failure_dependencies(monkeypatch, *, transition_state: str, reservation_created: bool = True):
    grant = SimpleNamespace(execution_id=1, fence=1, state="running")

    async def reserved_execution(*_arguments):
        return SimpleNamespace(
            execution_id=1, capture_bundle_id=None, created=reservation_created, state="queued"
        ), grant

    async def current_lease(*_arguments):
        return grant

    class _Session:
        @asynccontextmanager
        async def begin(self):
            yield self

    @asynccontextmanager
    async def session_factory():
        yield _Session()

    transitions = []

    async def finish_execution(_session, **kwargs):
        transitions.append(kwargs)
        return SimpleNamespace(state=transition_state, changed=transition_state == "failed")

    monkeypatch.setattr(snowflake_candidate, "_reserve_bundle_execution", reserved_execution)
    monkeypatch.setattr(snowflake_candidate, "_renew_bundle_lease", current_lease)
    monkeypatch.setattr(snowflake_candidate, "finish_execution", finish_execution)
    return session_factory, transitions


def _assert_fenced_failure(transitions, token: str) -> None:
    assert transitions == [
        {
            "execution_id": 1,
            "fence": 1,
            "token": token,
            "terminal_state": "failed",
            "terminal_reason": "source_capture_failed",
        }
    ]


def _result(
    *,
    query_id: str,
    options: _ResultOptions = _ResultOptions(),
) -> tuple[SnowflakeBundleResult, tuple[_Sources, _Sources]]:
    root_payload = _parquet(
        pa.table(
            {
                "npi": pa.array(["1003000126"], type=pa.string()),
                "score": pa.array([7], type=pa.int64()),
                "enabled": pa.array([True], type=pa.bool_()),
            }
        )
    )
    root_sources = _Sources(
        options.root_readers
        if options.root_readers is not None
        else tuple(
            _Reader(root_payload, close_error=options.root_close_error)
            for _index in range(options.root_partition_count)
        )
    )
    detail_values = (
        {
            "detail_npi": pa.array(["1003000126"], type=pa.string()),
            "detail_id": pa.array(["synthetic-detail"], type=pa.string()),
            "amount": pa.array([Decimal("12.500000000000")], type=pa.decimal128(30, 12)),
        }
        if options.detail_rows
        else {
            "detail_npi": pa.array([], type=pa.string()),
            "detail_id": pa.array([], type=pa.string()),
            "amount": pa.array([], type=pa.decimal128(30, 12)),
        }
    )
    detail_payload = _parquet(pa.table(detail_values))
    detail_sources = _Sources(
        options.detail_readers
        if options.detail_readers is not None
        else tuple(_Reader(detail_payload) for _index in range(options.detail_partition_count))
    )
    return _bundle_result(query_id, options, root_sources, detail_sources), (root_sources, detail_sources)


def _bundle_result(
    query_id: str,
    options: _ResultOptions,
    root_sources: _Sources,
    detail_sources: _Sources,
) -> SnowflakeBundleResult:
    return SnowflakeBundleResult(
        stream_results=(
            SnowflakeBundleStreamResult(
                metadata=SnowflakeBundleStreamMetadata(
                    stream_id="root_source",
                    semantic_token_metadata_key="semantic_snapshot",
                    source_snapshot_tokens=options.root_tokens,
                ),
                parquet_result=SnowflakeParquetResult(
                    source_snapshot_token=_SNAPSHOT,
                    schema=(
                        _ROOT_SCHEMA[0],
                        replace(
                            _ROOT_SCHEMA[1],
                            source_type=options.root_score_source_type,
                            nullable=options.root_score_nullable,
                        ),
                        _ROOT_SCHEMA[2],
                    ),
                    partition_sources=root_sources,
                ),
            ),
            SnowflakeBundleStreamResult(
                metadata=SnowflakeBundleStreamMetadata(
                    stream_id="detail_source",
                    semantic_token_metadata_key="semantic_snapshot",
                    source_snapshot_tokens=options.detail_tokens,
                ),
                parquet_result=SnowflakeParquetResult(
                    source_snapshot_token=_SNAPSHOT,
                    schema=_DETAIL_SCHEMA,
                    partition_sources=detail_sources,
                ),
            ),
        ),
        query_id=query_id,
        on_close=options.on_close,
    )


def _connector(
    adapter: _Adapter,
    *,
    capture_limits: CaptureLimits | None = None,
) -> SnowflakeBundleAcquisitionConnector:
    connector_options_by_name = {
        "approved_relations": _relations(),
        "credential_provider": _Credentials(),
        "adapter": adapter,
    }
    if capture_limits is not None:
        connector_options_by_name["capture_limits"] = capture_limits
    return SnowflakeBundleAcquisitionConnector(**connector_options_by_name)


def _reconstruct(
    connector: SnowflakeBundleAcquisitionConnector,
    request: SnowflakeBundleRequest,
    captures: tuple[ReplayableParquetCapture, ...],
):
    return reconstruct_replayable_parquet_bundle(connector.build_statement(request), captures)


def _reissue_captures(captures, mutate):
    reissued_captures = []
    for capture in captures:
        document = json.loads(capture.receipt.canonical_manifest)
        mutate(document)
        canonical_manifest, manifest_sha256 = snowflake_bundle._canonical_identity("durable-stream-capture", document)
        reissued_captures.append(
            ReplayableParquetCapture(
                receipt=CaptureReceipt(
                    stream_id=capture.receipt.stream_id,
                    source_snapshot_token=capture.receipt.source_snapshot_token,
                    byte_count=capture.receipt.byte_count,
                    content_sha256=capture.receipt.content_sha256,
                    canonical_manifest=canonical_manifest,
                    manifest_sha256=manifest_sha256,
                ),
                parts=capture.parts,
            )
        )
    return tuple(reissued_captures)


def _mutate_invalid_receipt_shape(document, malformed_field):
    match malformed_field:
        case "extra_key":
            document["unexpected"] = True
        case "missing_limit_field":
            del document["capture_limits"]["maximum_records"]
        case "empty_schema":
            document["result_schema"] = []
        case "incomplete_schema_entry":
            del document["result_schema"][0]["nullable"]
        case "duplicate_schema":
            document["result_schema"][1] = document["result_schema"][0]
        case "missing_captures":
            document["captures"] = []
        case "incomplete_capture_entry":
            del document["captures"][0]["content_bytes"]
        case "wrong_capture_size":
            document["captures"][0]["content_bytes"] += 1
        case _:
            raise AssertionError("unsupported malformed receipt field")


def _capture_limits(
    maximum_compressed_bytes: int,
    *,
    maximum_decoded_bytes: int = 1_000_000,
    maximum_records: int = 1_000,
) -> CaptureLimits:
    return CaptureLimits(
        maximum_compressed_bytes=maximum_compressed_bytes,
        maximum_decoded_bytes=maximum_decoded_bytes,
        maximum_record_bytes=maximum_decoded_bytes,
        maximum_records=maximum_records,
        maximum_fields_per_record=1_024,
    )


def test_bundle_contract_guards_reject_invalid_boundary_values(monkeypatch):
    for callback, message in (
        (lambda: snowflake_bundle._field_id("Not_Snake", "field"), "lower_snake_case"),
        (lambda: snowflake_bundle._snapshot_token(""), "snapshot token"),
        (lambda: snowflake_bundle._canonical_identity("test", {"value": {"not-json"}}), "canonically serialized"),
        (lambda: snowflake_bundle._diagnostic_query_id("line\nbreak"), "printable"),
        (lambda: snowflake_bundle._capture_limits(object()), "declared capture-limit"),
        (lambda: snowflake_bundle._receipt_sha256(None), "receipt digest"),
    ):
        with pytest.raises((SnowflakeBundleError, ValueError), match=message):
            callback()

    monkeypatch.setattr(snowflake_bundle, "MAX_MANIFEST_CANONICAL_BYTES", 1)
    with pytest.raises(SnowflakeBundleError, match="canonical byte limit"):
        snowflake_bundle._canonical_identity("test", {"value": "x"})
    monkeypatch.setattr(snowflake_bundle, "MAX_DIAGNOSTIC_QUERY_ID_BYTES", 1)
    with pytest.raises(SnowflakeBundleError, match="byte limit"):
        snowflake_bundle._diagnostic_query_id("xx")
    with pytest.raises(SnowflakeBundleError, match="result-byte bounds"):
        snowflake_bundle._capture_limits(
            replace(
                snowflake_bundle.DEFAULT_CAPTURE_LIMITS, maximum_decoded_bytes=snowflake_bundle.MAX_RESULT_BYTES + 1
            )
        )


def test_bundle_value_objects_fail_closed_for_invalid_shapes():
    snapshot_relation, root_relation, _detail_relation = _relations()
    for callback, message in (
        (lambda: SnowflakeBundleEncoding(True), "bounded positive integer"),
        (lambda: SnowflakeBundleEncoding(1, "none"), "compression"),
        (
            lambda: SnowflakeBundleBinding(
                "root_source", object(), snapshot_relation.relation, ("npi",), "semantic_snapshot"
            ),
            "declared relation",
        ),
        (
            lambda: SnowflakeBundleBinding(
                "root_source", root_relation.relation, object(), ("npi",), "semantic_snapshot"
            ),
            "snapshot relation",
        ),
        (
            lambda: SnowflakeBundleBinding(
                "root_source", root_relation.relation, snapshot_relation.relation, (), "semantic_snapshot"
            ),
            "from 1 through",
        ),
        (
            lambda: SnowflakeBundleBinding(
                "root_source", root_relation.relation, snapshot_relation.relation, ("npi", "npi"), "semantic_snapshot"
            ),
            "unique",
        ),
        (lambda: SnowflakeBundleRequest(object(), _bindings()), "custom-import definition"),
        (lambda: SnowflakeBundleRequest(_definition(), ()), "one binding"),
        (lambda: SnowflakeBundleRequest(_definition(), (object(),)), "declared binding type"),
        (lambda: SnowflakeBundleRequest(_definition(), _bindings(), encoding=object()), "encoding"),
    ):
        with pytest.raises(SnowflakeBundleError, match=message):
            callback()

    document = json.loads(_definition().canonical)
    document["streams"][0]["format"] = "json"
    with pytest.raises(SnowflakeBundleError, match="fixed Parquet result shape"):
        SnowflakeBundleRequest(CustomImportDefinition.from_mapping(document), _bindings())

    bundle_result, partition_sources = _result(query_id="query-value-contracts")
    stream_result = bundle_result.stream_results[0]
    with pytest.raises(SnowflakeBundleError, match="invalid observation count"):
        SnowflakeBundleStreamMetadata("root_source", "semantic_snapshot", [])
    with pytest.raises(SnowflakeBundleError, match="stream metadata is invalid"):
        SnowflakeBundleStreamResult(object(), stream_result.parquet_result)
    with pytest.raises(SnowflakeBundleError, match="must own a Parquet result"):
        SnowflakeBundleStreamResult(stream_result.metadata, object())
    with pytest.raises(SnowflakeBundleError, match="requires one stream result"):
        SnowflakeBundleResult(())
    with pytest.raises(SnowflakeBundleError, match="stream results are invalid"):
        SnowflakeBundleResult((object(),))
    with pytest.raises(SnowflakeBundleError, match="cleanup callback"):
        SnowflakeBundleResult(bundle_result.stream_results, on_close=object())
    bundle_result.close()
    bundle_result.close()
    assert all(partition_source.close_count == 1 for partition_source in partition_sources)


def test_bundle_statement_contracts_reject_drift():
    connector = _connector(_Adapter(lambda: _result(query_id="query-contract")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    statement = connector.build_statement(request)

    for callback, message in (
        (lambda: SnowflakeBundleStatement(object(), (), ()), "declared bundle request"),
        (
            lambda: SnowflakeBundleStatement(request, (), statement.source_snapshot_token_columns_by_stream),
            "columns must match",
        ),
        (
            lambda: SnowflakeBundleStatement(
                request,
                ((SnowflakeDeclaredColumn("npi", "root_npi"),), statement.selected_columns_by_stream[1]),
                statement.source_snapshot_token_columns_by_stream,
            ),
            "do not match",
        ),
        (
            lambda: SnowflakeBundleStatement(
                request,
                statement.selected_columns_by_stream,
                (
                    SnowflakeDeclaredColumn("wrong", "snapshot_token"),
                    statement.source_snapshot_token_columns_by_stream[1],
                ),
            ),
            "does not match",
        ),
        (
            lambda: SnowflakeBundleStatement(request, statement.selected_columns_by_stream, ()),
            "snapshot columns must match",
        ),
        (
            lambda: SnowflakeBundleStatement(
                request,
                (object(), statement.selected_columns_by_stream[1]),
                statement.source_snapshot_token_columns_by_stream,
            ),
            "columns must use",
        ),
        (
            lambda: SnowflakeBundleStatement(
                request,
                statement.selected_columns_by_stream,
                (object(), statement.source_snapshot_token_columns_by_stream[1]),
            ),
            "snapshot columns must use",
        ),
    ):
        with pytest.raises(SnowflakeBundleError, match=message):
            callback()


def test_bundle_acquisition_contracts_reject_drift():
    connector = _connector(_Adapter(lambda: _result(query_id="query-contract")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    statement = connector.build_statement(request)
    acquisition = connector.acquire(request, prepared_statement=statement)

    for callback, message in (
        (
            lambda: replace(acquisition, statement=object()),
            "generated bundle statement",
        ),
        (
            lambda: replace(
                acquisition,
                capture_limits=replace(
                    acquisition.capture_limits, maximum_records=acquisition.capture_limits.maximum_records - 1
                ),
            ),
            "capture limits",
        ),
        (lambda: replace(acquisition, stream_captures=acquisition.stream_captures[:-1]), "exactly cover"),
        (lambda: replace(acquisition, stream_captures=acquisition.stream_captures[::-1]), "declared stream order"),
        (lambda: replace(acquisition.stream_captures[0], schema=()), "capture schema"),
        (lambda: replace(acquisition.stream_captures[0], captures=()), "Parquet partitions"),
    ):
        with pytest.raises(SnowflakeBundleError, match=message):
            callback()

    object.__setattr__(request, "request_sha256", "0" * 64)
    with pytest.raises(SnowflakeBundleError, match="stale identity seal"):
        snowflake_bundle._validated_bundle_request(request)
    object.__setattr__(statement, "statement_sha256", "0" * 64)
    with pytest.raises(SnowflakeBundleError, match="stale identity seal"):
        snowflake_bundle._validated_bundle_statement(statement)
    with pytest.raises(SnowflakeBundleError, match="declared bundle request"):
        snowflake_bundle._validated_bundle_request(object())
    with pytest.raises(SnowflakeBundleError, match="bundle statement is invalid"):
        snowflake_bundle._validated_bundle_statement(object())

    schema_mismatch = replace(
        acquisition.stream_captures[0],
        schema=(SnowflakeResultColumn("wrong", "TEXT", False),),
    )
    with pytest.raises(SnowflakeBundleError, match="capture schema does not match"):
        snowflake_bundle._validated_stream_capture_manifests(
            connector.build_statement(connector.prepare_request(_definition(), bindings=_bindings())),
            (schema_mismatch, acquisition.stream_captures[1]),
            acquisition.source_snapshot_token,
            acquisition.capture_limits,
        )


def test_bundle_connector_rejects_invalid_runtime_dependencies_and_mappings():
    adapter = _Adapter(lambda: _result(query_id="query-runtime")[0])
    for options, message in (
        ({"approved_relations": (), "credential_provider": _Credentials(), "adapter": adapter}, "from 1 through"),
        (
            {"approved_relations": (object(),), "credential_provider": _Credentials(), "adapter": adapter},
            "declared relation",
        ),
        (
            {
                "approved_relations": (_relations()[0], _relations()[0]),
                "credential_provider": _Credentials(),
                "adapter": adapter,
            },
            "identifiers must be unique",
        ),
        (
            {"approved_relations": _relations(), "credential_provider": object(), "adapter": adapter},
            "credential provider",
        ),
        ({"approved_relations": _relations(), "credential_provider": _Credentials(), "adapter": object()}, "adapter"),
    ):
        with pytest.raises(SnowflakeBundleError, match=message):
            SnowflakeBundleAcquisitionConnector(**options)

    connector = _connector(adapter)
    request = connector.prepare_request(_definition(), bindings=_bindings())
    with pytest.raises(SnowflakeBundleError, match="selected field is not declared"):
        connector._approved_columns(request, replace(request.bindings[0], selected_field_ids=("unknown",)))
    with pytest.raises(SnowflakeBundleError, match="snapshot token column is not declared"):
        connector._approved_snapshot_token_column(replace(request.bindings[0], semantic_token_metadata_key="unknown"))

    restricted_request = SnowflakeBundleRequest(
        _definition(),
        _bindings(),
        capture_limits=replace(snowflake_bundle.DEFAULT_CAPTURE_LIMITS, maximum_records=1),
    )
    with pytest.raises(SnowflakeBundleError, match="capture limits do not match"):
        connector.build_statement(restricted_request)

    connector._credential_provider = SimpleNamespace(load_key_pair=lambda: object())
    with pytest.raises(SnowflakeBundleError, match="invalid key-pair"):
        connector.acquire(request)
    invalid_result_connector = _connector(_Adapter(lambda: object()))
    with pytest.raises(SnowflakeBundleError, match="invalid result"):
        invalid_result_connector.acquire(request)


def test_bundle_replay_rejects_corrupt_in_memory_evidence():
    connector = _connector(_Adapter(lambda: _result(query_id="query-replay-contract")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    root, detail = replay.streams

    for callback, message in (
        (
            lambda: snowflake_bundle_replay.SnowflakeBundleReplayStream(root.stream_id, [], root.receipt, root.parts),
            "tuple of mappings",
        ),
        (
            lambda: snowflake_bundle_replay.SnowflakeBundleReplayStream(
                root.stream_id, root.records, detail.receipt, root.parts
            ),
            "receipt",
        ),
        (
            lambda: snowflake_bundle_replay.SnowflakeBundleReplayStream(root.stream_id, root.records, root.receipt, ()),
            "non-empty bytes",
        ),
        (
            lambda: snowflake_bundle_replay.SnowflakeBundleReplayStream(
                root.stream_id, root.records, root.receipt, (root.parts[0][:-1],)
            ),
            "byte count",
        ),
        (lambda: snowflake_bundle_replay.SnowflakeBundleReplay(object(), _SNAPSHOT, replay.streams), "definition"),
        (lambda: snowflake_bundle_replay.SnowflakeBundleReplay(_definition(), _SNAPSHOT, object()), "streams"),
        (
            lambda: snowflake_bundle_replay.SnowflakeBundleReplay(_definition(), _SNAPSHOT, replay.streams[::-1]),
            "declared order",
        ),
        (
            lambda: snowflake_bundle_replay.SnowflakeBundleReplay(_definition(), "different-snapshot", replay.streams),
            "acquisition evidence",
        ),
        (lambda: snowflake_bundle_replay._verified_bundle_acquisition(object()), "acquisition is invalid"),
    ):
        with pytest.raises(SnowflakeBundleError, match=message):
            callback()


def test_bundle_replay_rejects_invalid_field_types_and_partition_schema():
    connector = _connector(_Adapter(lambda: _result(query_id="query-replay-schema")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    root_fields = tuple(field for field in _definition().fields if field.collection is None)
    score = next(field for field in root_fields if field.field_id == "score")
    assert snowflake_bundle_replay._is_bundle_column_type_valid(pa.null(), score) is False
    assert snowflake_bundle_replay._is_bundle_column_type_valid(pa.null(), replace(score, nullable=True)) is True
    assert (
        snowflake_bundle_replay._is_bundle_column_type_valid(pa.float64(), SimpleNamespace(value_type="other")) is False
    )
    with pytest.raises(SnowflakeBundleError, match="integer result is invalid"):
        snowflake_bundle_replay._normalized_integer_replay_values({"score": Decimal("1.5")}, (score,))

    root_stream = _definition().source_streams[0]
    root_capture = acquisition.stream_captures[0]
    with pytest.raises(SnowflakeBundleError, match="schema does not match"):
        snowflake_bundle_replay._validate_replay_partition_schema(
            root_capture.captures[0], fields=(score,), limits=acquisition.capture_limits
        )
    tampered_capture = replace(root_capture)
    object.__setattr__(tampered_capture, "schema", ())
    with pytest.raises(SnowflakeBundleError, match="result schema"):
        snowflake_bundle_replay._replay_stream_records(
            tampered_capture,
            stream=root_stream,
            fields=root_fields,
            limits=acquisition.capture_limits,
            decoded_bytes=0,
        )


def test_bundle_replay_rejects_invalid_decoded_records(monkeypatch):
    connector = _connector(_Adapter(lambda: _result(query_id="query-replay-records")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    root_fields = tuple(field for field in _definition().fields if field.collection is None)
    root_stream = _definition().source_streams[0]
    root_capture = acquisition.stream_captures[0]
    valid_value_by_field = {"npi": "1003000126", "score": 7, "enabled": True}
    monkeypatch.setattr(
        snowflake_bundle_replay,
        "iter_records",
        lambda *_args, **_kwargs: iter((SimpleNamespace(values={"wrong": "value"}),)),
    )
    with pytest.raises(SnowflakeBundleError, match="record fields"):
        snowflake_bundle_replay._replay_stream_records(
            root_capture,
            stream=root_stream,
            fields=root_fields,
            limits=acquisition.capture_limits,
            decoded_bytes=0,
        )
    monkeypatch.setattr(
        snowflake_bundle_replay,
        "iter_records",
        lambda *_args, **_kwargs: iter(
            (SimpleNamespace(values=valid_value_by_field), SimpleNamespace(values=valid_value_by_field))
        ),
    )
    with pytest.raises(SnowflakeBundleError, match="aggregate record"):
        snowflake_bundle_replay._replay_stream_records(
            root_capture,
            stream=root_stream,
            fields=root_fields,
            limits=replace(acquisition.capture_limits, maximum_records=1),
            decoded_bytes=0,
        )
    monkeypatch.setattr(
        snowflake_bundle_replay,
        "_validate_replay_partition_schema",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(snowflake_bundle_replay.CaptureError("corrupt")),
    )
    with pytest.raises(SnowflakeBundleError, match="cannot be decoded"):
        snowflake_bundle_replay._replay_stream_records(
            root_capture,
            stream=root_stream,
            fields=root_fields,
            limits=acquisition.capture_limits,
            decoded_bytes=0,
        )


def test_durable_replay_rejects_stale_statement_identity():
    connector = _connector(_Adapter(lambda: _result(query_id="query-replay-stale")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    statement = connector.build_statement(request)
    captures = replayable_parquet_captures(connector.acquire(request, prepared_statement=statement))
    object.__setattr__(statement, "statement_sha256", "0" * 64)

    with pytest.raises(SnowflakeBundleError, match="stale identity seal"):
        reconstruct_replayable_parquet_bundle(statement, captures)


def test_durable_replay_rejects_corrupt_receipts_and_unsealed_acquisitions():
    connector = _connector(_Adapter(lambda: _result(query_id="query-durable-integrity")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    captures = replayable_parquet_captures(acquisition)
    first_capture = captures[0]
    different_part = b"x" * len(first_capture.parts[0])
    with pytest.raises(SnowflakeBundleError, match="receipt digest"):
        snowflake_bundle_replay.SnowflakeBundleReplayStream(
            first_capture.receipt.stream_id,
            (),
            first_capture.receipt,
            (different_part, *first_capture.parts[1:]),
        )

    object.__setattr__(acquisition, "manifest_sha256", "0" * 64)
    with pytest.raises(SnowflakeBundleError, match="stale nested identity seal"):
        snowflake_bundle_replay._verified_bundle_acquisition(acquisition)
    with pytest.raises(SnowflakeBundleError, match="acquisition seal is invalid"):
        snowflake_bundle_replay._verified_bundle_acquisition(
            object.__new__(snowflake_bundle.SnowflakeBundleAcquisition)
        )

    with pytest.raises(SnowflakeBundleError, match="captures are invalid"):
        snowflake_bundle_replay._validated_durable_captures(_definition(), ())
    with pytest.raises(SnowflakeBundleError, match="do not match the declared streams"):
        snowflake_bundle_replay._validated_durable_captures(_definition(), captures[:1])

    malformed_definition = json.loads(_definition().canonical)
    malformed_definition["streams"][0]["format"] = "json"
    with pytest.raises(SnowflakeBundleError, match="fixed Parquet result shape"):
        snowflake_bundle_replay._validated_durable_captures(
            CustomImportDefinition.from_mapping(malformed_definition), captures
        )

    oversized_capture_by_stream = {
        _definition().source_streams[0].stream_id: SimpleNamespace(
            parts=(b"x",) * (snowflake_bundle_replay.MAX_RESULT_PARTITIONS + 1)
        ),
        _definition().source_streams[1].stream_id: SimpleNamespace(parts=(b"x",)),
    }
    with pytest.raises(SnowflakeBundleError, match="stream partitions exceed"):
        snowflake_bundle_replay._validate_durable_partition_counts(_definition(), oversized_capture_by_stream)


def test_durable_replay_rechecks_receipt_schema(monkeypatch):
    connector = _connector(_Adapter(lambda: _result(query_id="query-durable-recheck")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    statement = connector.build_statement(request)
    captures = replayable_parquet_captures(connector.acquire(request))
    definition = _definition()
    captures_by_stream = {capture.receipt.stream_id: capture for capture in captures}
    durable_receipt_by_stream = {
        stream_id: snowflake_bundle._durable_bundle_receipt(capture)
        for stream_id, capture in captures_by_stream.items()
    }
    first_stream = definition.source_streams[0]
    durable_receipt_by_stream[first_stream.stream_id] = replace(
        durable_receipt_by_stream[first_stream.stream_id], result_schema=()
    )
    monkeypatch.setattr(
        snowflake_bundle_replay,
        "_validated_durable_bundle",
        lambda _statement, _captures: (
            statement,
            definition,
            captures_by_stream,
            _SNAPSHOT,
            durable_receipt_by_stream,
        ),
    )
    with pytest.raises(SnowflakeBundleError, match="receipt schema"):
        reconstruct_replayable_parquet_bundle(statement, captures)


def test_durable_replay_rechecks_capture_seals_and_record_fields(monkeypatch):
    connector = _connector(_Adapter(lambda: _result(query_id="query-durable-capture")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    first_capture = replayable_parquet_captures(connector.acquire(request))[0]
    definition = _definition()
    first_stream = definition.source_streams[0]
    fields = tuple(field for field in definition.fields if field.collection is None)
    capture_sha256s = tuple("expected" for _part in first_capture.parts)
    sealed = SimpleNamespace(manifest=SimpleNamespace(capture_sha256="different", compressed_bytes=1, decoded_bytes=1))
    monkeypatch.setattr(snowflake_bundle_replay, "capture_stream", lambda *_args, **_kwargs: sealed)
    with pytest.raises(SnowflakeBundleError, match="capture seal"):
        snowflake_bundle_replay._replay_durable_stream(
            first_capture,
            first_stream,
            fields,
            capture_sha256s=capture_sha256s,
            limits=request.capture_limits,
            capture_compressed_bytes=0,
            capture_decoded_bytes=0,
            decoded_bytes=0,
        )

    sealed.manifest.capture_sha256 = "expected"
    monkeypatch.setattr(snowflake_bundle_replay, "_validate_replay_partition_schema", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        snowflake_bundle_replay,
        "iter_records",
        lambda *_args, **_kwargs: iter((SimpleNamespace(values={"wrong": "value"}),)),
    )
    with pytest.raises(SnowflakeBundleError, match="record fields"):
        snowflake_bundle_replay._replay_durable_stream(
            first_capture,
            first_stream,
            fields,
            capture_sha256s=capture_sha256s,
            limits=request.capture_limits,
            capture_compressed_bytes=0,
            capture_decoded_bytes=0,
            decoded_bytes=0,
        )


def test_durable_replay_rechecks_arrow_accounting(monkeypatch):
    class InvalidBatchReader:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def iter_batches(self, **_kwargs):
            return iter((SimpleNamespace(nbytes=True),))

    monkeypatch.setattr(snowflake_bundle_replay, "_validate_parquet_envelope", lambda _payload: None)
    monkeypatch.setattr(snowflake_bundle_replay, "_open_parquet_reader", lambda _payload, _limits: InvalidBatchReader())
    with pytest.raises(snowflake_bundle_replay.CaptureError, match="payload is invalid"):
        snowflake_bundle_replay._aggregate_parquet_arrow_bytes(
            SimpleNamespace(payload=object()), limits=snowflake_bundle.DEFAULT_CAPTURE_LIMITS, decoded_bytes=0
        )


def test_bundle_generates_one_safe_statement_with_fixed_order_and_encoding_inputs():
    adapter = _Adapter(lambda: _result(query_id="query-a")[0])
    connector = _connector(adapter)

    request = connector.prepare_request(_definition(), bindings=_bindings())
    statement = connector.build_statement(request)

    assert tuple(binding.stream_id for binding in request.bindings) == ("root_source", "detail_source")
    assert statement.sql.startswith(
        'SELECT "__ci_bundle_row_kind", "__ci_stream_ordinal", "__ci_stream_id", "__ci_source_snapshot_token", "npi"'
    )
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOTS"' in statement.sql
    assert 'FROM "SYNTHETIC"."PUBLIC"."DETAILS"' in statement.sql
    assert '(SELECT "SNAPSHOT_TOKEN" FROM "SYNTHETIC"."PUBLIC"."SNAPSHOTS")' in statement.sql
    assert '"ROOT_NPI" AS "npi"' in statement.sql
    assert '"DETAIL_AMOUNT" AS "amount"' in statement.sql
    assert "UNION ALL" in statement.sql
    assert ";" not in statement.sql
    assert "*" not in statement.sql
    assert statement.sql.endswith(
        'ORDER BY "__ci_bundle_row_kind", "__ci_stream_ordinal", "npi" NULLS FIRST, "score" NULLS FIRST, '
        '"enabled" NULLS FIRST, "detail_npi" NULLS FIRST, "detail_id" NULLS FIRST, "amount" NULLS FIRST'
    )
    assert request.encoding.as_identity_document() == {
        "format": "parquet",
        "parquet_compression": "zstd",
        "partition_rows": 1024,
    }


def test_bundle_request_identity_normalizes_binding_and_field_order():
    connector = _connector(_Adapter(lambda: pytest.fail("request preparation must not fetch")))
    definition = _definition()
    bindings = _bindings()
    reordered_bindings = tuple(
        replace(binding, selected_field_ids=tuple(reversed(binding.selected_field_ids))) for binding in bindings
    )

    expected = connector.prepare_request(definition, bindings=bindings)
    normalized = connector.prepare_request(definition, bindings=tuple(reversed(reordered_bindings)))

    assert normalized.bindings == expected.bindings
    assert normalized.canonical_request == expected.canonical_request
    assert normalized.request_sha256 == expected.request_sha256


@pytest.mark.parametrize(
    ("bindings", "message"),
    (
        (lambda bindings: (bindings[0], bindings[0]), "exactly cover"),
        (lambda bindings: (bindings[0],), "exactly cover"),
        (lambda bindings: (replace(bindings[0], selected_field_ids=("npi",)), bindings[1]), "selected fields"),
        (
            lambda bindings: (bindings[0], replace(bindings[1], semantic_token_metadata_key="other_token")),
            "semantic token metadata must match",
        ),
    ),
)
def test_bundle_request_rejects_incomplete_or_inconsistent_stream_bindings(bindings, message):
    connector = _connector(_Adapter(lambda: pytest.fail("invalid request must not fetch")))
    with pytest.raises(SnowflakeBundleError, match=message):
        connector.prepare_request(_definition(), bindings=bindings(_bindings()))


def test_bundle_request_identity_requires_the_exact_validated_statement():
    connector = _connector(_Adapter(lambda: _result(query_id="query-a")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    statement = connector.build_statement(request)
    assert len(snowflake_candidate.bundle_request_identity_sha256(request, statement)) == 32

    changed_request = connector.prepare_request(_definition_with_revision(2), bindings=_bindings())
    with pytest.raises(SnowflakeCandidateError, match="does not match"):
        snowflake_candidate.bundle_request_identity_sha256(changed_request, statement)

    object.__setattr__(statement, "statement_sha256", "0" * 64)
    with pytest.raises(SnowflakeCandidateError, match="identity is invalid"):
        snowflake_candidate.bundle_request_identity_sha256(request, statement)


def test_bundle_rejects_missing_semantic_metadata_and_unapproved_relation():
    connector = _connector(_Adapter(lambda: _result(query_id="query-a")[0]))

    with pytest.raises(SnowflakeBundleError, match="explicit semantic token metadata"):
        connector.prepare_request(_definition(), bindings=_bindings(child_token_key=None))

    snapshot_relation, root_relation, detail_relation = _relations()
    request = connector.prepare_request(
        _definition(),
        bindings=(
            SnowflakeBundleBinding(
                stream_id="root_source",
                relation=SnowflakeRelation(database="synthetic", schema="public", name="other"),
                source_snapshot_token_relation=snapshot_relation.relation,
                selected_field_ids=("npi", "score", "enabled"),
                semantic_token_metadata_key="semantic_snapshot",
            ),
            SnowflakeBundleBinding(
                stream_id="detail_source",
                relation=detail_relation.relation,
                source_snapshot_token_relation=snapshot_relation.relation,
                selected_field_ids=("detail_npi", "detail_id", "amount"),
                semantic_token_metadata_key="semantic_snapshot",
            ),
        ),
    )
    with pytest.raises(SnowflakeBundleError, match="not approved"):
        connector.build_statement(request)

    request = connector.prepare_request(
        _definition(),
        bindings=(
            SnowflakeBundleBinding(
                stream_id="root_source",
                relation=root_relation.relation,
                source_snapshot_token_relation=SnowflakeRelation(database="synthetic", schema="public", name="other"),
                selected_field_ids=("npi", "score", "enabled"),
                semantic_token_metadata_key="semantic_snapshot",
            ),
            SnowflakeBundleBinding(
                stream_id="detail_source",
                relation=detail_relation.relation,
                source_snapshot_token_relation=snapshot_relation.relation,
                selected_field_ids=("detail_npi", "detail_id", "amount"),
                semantic_token_metadata_key="semantic_snapshot",
            ),
        ),
    )
    with pytest.raises(SnowflakeBundleError, match="source snapshot relation identifier is not approved"):
        connector.build_statement(request)
    assert root_relation.relation.name == "ROOTS"


def test_bundle_rejects_prepared_mapping_drift_before_loading_credentials():
    adapter = _Adapter(lambda: pytest.fail("mapping drift must not contact the source adapter"))
    connector = _connector(adapter)
    request = connector.prepare_request(_definition(), bindings=_bindings())
    statement = connector.build_statement(request)
    drifting_root_columns = (
        SnowflakeDeclaredColumn(field_id="npi", column_identifier="other_npi"),
        *statement.selected_columns_by_stream[0][1:],
    )
    drifting_statement = SnowflakeBundleStatement(
        request=request,
        selected_columns_by_stream=(drifting_root_columns, statement.selected_columns_by_stream[1]),
        source_snapshot_token_columns_by_stream=statement.source_snapshot_token_columns_by_stream,
    )
    connector._credential_provider = SimpleNamespace(
        load_key_pair=lambda: pytest.fail("mapping drift must not load credentials")
    )

    with pytest.raises(SnowflakeBundleError, match="prepared bundle statement does not match"):
        connector.acquire(request, prepared_statement=drifting_statement)

    assert adapter.statements == []


@pytest.mark.parametrize(
    ("aliases", "should_fail"),
    (
        (
            {
                "root_source": {"ROOT_NPI": "npi"},
                "detail_source": {"DETAIL_AMOUNT": "amount"},
            },
            False,
        ),
        (
            {
                "root_source": {"ROOT_NPI": "score"},
                "detail_source": {},
            },
            True,
        ),
        (
            {
                "root_source": {},
                "detail_source": {"UNSELECTED_DETAIL": "amount"},
            },
            True,
        ),
    ),
    ids=("matching", "root-field-mismatch", "child-column-mismatch"),
)
def test_bundle_binds_definition_aliases_to_allowlisted_physical_columns(aliases, should_fail):
    connector = _connector(_Adapter(lambda: _result(query_id="query-aliases")[0]))
    request = connector.prepare_request(_definition_with_aliases(aliases), bindings=_bindings())

    if should_fail:
        with pytest.raises(SnowflakeBundleError, match="source aliases"):
            connector.build_statement(request)
    else:
        assert connector.build_statement(request).request == request


def test_bundle_seals_empty_child_and_ignores_query_identity():
    first_result, first_sources = _result(query_id="query-a")
    second_result, second_sources = _result(query_id="query-b")
    results = iter((first_result, second_result))
    adapter = _Adapter(lambda: next(results))
    connector = _connector(adapter)
    request = connector.prepare_request(_definition(), bindings=_bindings())

    first = connector.acquire(request, prepared_statement=connector.build_statement(request))
    second = connector.acquire(request)

    assert len(adapter.statements) == 2
    assert first.source_snapshot_token == _SNAPSHOT
    assert first.diagnostic_query_id == "query-a"
    assert second.diagnostic_query_id == "query-b"
    assert first.manifest_sha256 == second.manifest_sha256
    assert first.canonical_manifest == second.canonical_manifest
    assert "query-a" not in first.canonical_manifest
    assert "query-b" not in second.canonical_manifest
    assert [capture.stream_id for capture in first.stream_captures] == ["root_source", "detail_source"]
    assert [len(capture.captures) for capture in first.stream_captures] == [1, 1]
    records = list(iter_records(first.stream_captures[0].captures[0], _definition().source_streams[0]))
    assert dict(records[0].values) == {"npi": "1003000126", "score": 7, "enabled": True}
    assert all(sources.close_count == 1 for sources in (*first_sources, *second_sources))


def test_bundle_preserves_exact_scalar_values_without_sql_text_coercion():
    result, _sources = _result(query_id="query-scalars", options=_ResultOptions(detail_rows=True))
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    acquisition = connector.acquire(request)
    detail_records = list(iter_records(acquisition.stream_captures[1].captures[0], _definition().source_streams[1]))

    assert dict(detail_records[0].values) == {
        "detail_npi": "1003000126",
        "detail_id": "synthetic-detail",
        "amount": Decimal("12.500000000000"),
    }
    assert "TO_VARCHAR" not in acquisition.statement.sql
    assert "TO_JSON" not in acquisition.statement.sql


def test_bundle_replays_fixed38_integer_as_bounded_python_int():
    result, _sources = _result(
        query_id="query-fixed38-integer",
        options=_ResultOptions(
            root_readers=(_Reader(_fixed38_root_payload(Decimal(2**63 - 1))),),
            root_score_source_type="FIXED(38,0)",
        ),
    )
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    durable_replay = _reconstruct(connector, request, replayable_parquet_captures(acquisition))

    for candidate in (replay, durable_replay):
        assert candidate.roots == ({"npi": "1003000126", "score": 2**63 - 1, "enabled": True},)
        assert type(candidate.roots[0]["score"]) is int


def test_bundle_rejects_fixed38_integer_outside_signed_64_bit_range():
    result, _sources = _result(
        query_id="query-fixed38-overflow",
        options=_ResultOptions(
            root_readers=(_Reader(_fixed38_root_payload(Decimal(2**63))),),
            root_score_source_type="FIXED(38,0)",
        ),
    )
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    captures = replayable_parquet_captures(acquisition)

    with pytest.raises(SnowflakeBundleError, match="signed 64-bit range"):
        prepare_bundle_replay(acquisition)
    with pytest.raises(SnowflakeBundleError, match="signed 64-bit range"):
        _reconstruct(connector, request, captures)


def test_bundle_retains_null_for_a_nullable_fixed38_integer():
    definition = _definition(score_nullable=True)
    result, _sources = _result(
        query_id="query-fixed38-null",
        options=_ResultOptions(
            root_readers=(_Reader(_fixed38_root_payload(None)),),
            root_score_nullable=True,
            root_score_source_type="FIXED(38,0)",
        ),
    )
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(definition, bindings=_bindings())

    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    durable_replay = _reconstruct(connector, request, replayable_parquet_captures(acquisition))

    assert replay.roots == durable_replay.roots == ({"npi": "1003000126", "score": None, "enabled": True},)


@pytest.mark.parametrize(
    ("root_tokens", "detail_tokens"),
    (
        ((), (_SNAPSHOT,)),
        ((None,), (_SNAPSHOT,)),
        ((_SNAPSHOT, _SNAPSHOT), (_SNAPSHOT,)),
        ((_SNAPSHOT, "synthetic-release-other"), (_SNAPSHOT,)),
        ((_SNAPSHOT,), ("synthetic-release-other",)),
    ),
    ids=("missing", "null", "duplicate-identical", "multi-valued", "cross-stream-mismatch"),
)
def test_bundle_rejects_invalid_semantic_token_metadata(root_tokens, detail_tokens):
    result, sources = _result(
        query_id="query-invalid",
        options=_ResultOptions(root_tokens=root_tokens, detail_tokens=detail_tokens),
    )
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match="one shared semantic snapshot token"):
        connector.acquire(request)

    assert all(source.close_count == 1 for source in sources)


@pytest.mark.parametrize(
    ("mismatch", "message"),
    (
        ("stream_order", "configured stream order"),
        ("metadata_key", "semantic token metadata does not match"),
        ("parquet_token", "Parquet result token does not match"),
        ("schema", "result schema does not match"),
    ),
)
def test_bundle_rejects_inconsistent_adapter_evidence_and_closes_sources(mismatch, message):
    result, sources = _result(query_id="synthetic-evidence")
    root, detail = result.stream_results
    if mismatch == "stream_order":
        result.stream_results = (detail, root)
    elif mismatch == "metadata_key":
        result.stream_results = (
            replace(root, metadata=replace(root.metadata, semantic_token_metadata_key="other_token")),
            detail,
        )
    elif mismatch == "parquet_token":
        root.parquet_result.source_snapshot_token = "synthetic-other-release"
    else:
        root.parquet_result.schema = tuple(reversed(root.parquet_result.schema))
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match=message):
        connector.acquire(request)

    assert all(source.close_count == 1 for source in sources)
    assert all(reader.close_count == 1 for source in sources for reader in source.readers)


def test_bundle_keeps_primary_failure_when_bounded_cleanup_also_fails():
    close_counts_by_event = {"bundle": 0}

    def on_close() -> None:
        close_counts_by_event["bundle"] += 1

    result, sources = _result(
        query_id="query-cleanup",
        options=_ResultOptions(
            root_tokens=(),
            root_close_error=RuntimeError("synthetic close failure"),
            on_close=on_close,
        ),
    )
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match="one shared semantic snapshot token"):
        connector.acquire(request)

    assert close_counts_by_event["bundle"] == 1
    assert sources[0].readers[0].close_count == 1


def test_bundle_cleanup_preserves_process_interrupts():
    def interrupt() -> None:
        raise KeyboardInterrupt

    result, sources = _result(query_id="query-cleanup-interrupt", options=_ResultOptions(on_close=interrupt))

    with pytest.raises(KeyboardInterrupt):
        result.close()

    assert all(source.close_count == 1 for source in sources)


def test_bundle_does_not_expose_lower_level_seal_errors(monkeypatch):
    result, sources = _result(query_id="query-private-error")
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    def fail_seal(*_arguments):
        raise SnowflakeConnectorError("synthetic private detail")

    monkeypatch.setattr(snowflake_bundle, "_seal_bundle", fail_seal)
    with pytest.raises(SnowflakeBundleError, match="bundle result cannot be sealed") as failure:
        connector.acquire(request)

    assert "synthetic private detail" not in str(failure.value)
    assert all(source.close_count == 1 for source in sources)


def test_bundle_capture_bounds_match_storage_and_admit_exact_low_cost_boundary(monkeypatch):
    assert snowflake_bundle.MAX_BUNDLE_PARTITIONS == 8_192
    assert snowflake_bundle.MAX_STREAM_CAPTURE_BYTES == 64 * 1024 * 1024
    assert snowflake_bundle.MAX_BUNDLE_CAPTURE_BYTES == 128 * 1024 * 1024

    result, sources = _result(query_id="query-byte-boundary")
    root_bytes, detail_bytes = (len(source.readers[0]._payload) for source in sources)
    stream_limit = max(root_bytes, detail_bytes)
    bundle_limit = root_bytes + detail_bytes
    monkeypatch.setattr(snowflake_bundle, "MAX_STREAM_CAPTURE_BYTES", stream_limit)
    monkeypatch.setattr(snowflake_bundle, "MAX_BUNDLE_CAPTURE_BYTES", bundle_limit)
    connector = _connector(
        _Adapter(lambda: result),
        capture_limits=_capture_limits(stream_limit),
    )
    request = connector.prepare_request(_definition(), bindings=_bindings())

    acquisition = connector.acquire(request)

    assert (
        sum(
            capture.manifest.compressed_bytes
            for stream_capture in acquisition.stream_captures
            for capture in stream_capture.captures
        )
        == bundle_limit
    )
    assert all(source.close_count == 1 for source in sources)


def test_bundle_rejects_aggregate_capture_byte_overflow_and_closes_sources(monkeypatch):
    result, sources = _result(query_id="query-budget")
    root_bytes, detail_bytes = (len(source.readers[0]._payload) for source in sources)
    monkeypatch.setattr(snowflake_bundle, "MAX_STREAM_CAPTURE_BYTES", max(root_bytes, detail_bytes))
    monkeypatch.setattr(snowflake_bundle, "MAX_BUNDLE_CAPTURE_BYTES", root_bytes + detail_bytes - 1)
    connector = _connector(
        _Adapter(lambda: result),
        capture_limits=_capture_limits(max(root_bytes, detail_bytes)),
    )
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match="bundle result partition cannot be captured"):
        connector.acquire(request)

    assert all(source.close_count == 1 for source in sources)


def test_bundle_rejects_stream_capture_byte_overflow_and_closes_sources(monkeypatch):
    result, sources = _result(query_id="query-stream-budget", options=_ResultOptions(root_partition_count=2))
    root_bytes = len(sources[0].readers[0]._payload)
    detail_bytes = len(sources[1].readers[0]._payload)
    stream_limit = (2 * root_bytes) - 1
    monkeypatch.setattr(snowflake_bundle, "MAX_STREAM_CAPTURE_BYTES", stream_limit)
    monkeypatch.setattr(snowflake_bundle, "MAX_BUNDLE_CAPTURE_BYTES", (2 * root_bytes) + detail_bytes)
    connector = _connector(
        _Adapter(lambda: result),
        capture_limits=_capture_limits(stream_limit),
    )
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match="bundle result partition cannot be captured"):
        connector.acquire(request)

    assert all(source.close_count == 1 for source in sources)


def test_durable_replay_enforces_acquired_record_limit_across_parquet_parts():
    result, _sources = _result(query_id="query-record-limit", options=_ResultOptions(root_partition_count=2))
    connector = _connector(
        _Adapter(lambda: result),
        capture_limits=_capture_limits(1_000_000, maximum_records=1),
    )
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    captures = replayable_parquet_captures(acquisition)
    durable_captures = tuple(
        ReplayableParquetCapture(receipt=capture.receipt, parts=capture.parts) for capture in captures
    )

    assert '"maximum_records":1' in captures[0].receipt.canonical_manifest
    with pytest.raises(SnowflakeBundleError, match="aggregate record limit"):
        _reconstruct(connector, request, durable_captures)


def test_bundle_replay_aggregates_actual_arrow_bytes_across_parquet_parts():
    root_parquet_payload = _parquet(
        pa.table(
            {
                "npi": pa.array(["x" * 50] * 2_500, type=pa.string()),
                "score": pa.array([7] * 2_500, type=pa.int64()),
                "enabled": pa.array([True] * 2_500, type=pa.bool_()),
            }
        )
    )
    assert len(root_parquet_payload) < 200_000
    bundle_result, _sources = _result(
        query_id="query-decoded-budget",
        options=_ResultOptions(root_readers=(_Reader(root_parquet_payload), _Reader(root_parquet_payload))),
    )
    connector = _connector(
        _Adapter(lambda: bundle_result),
        capture_limits=_capture_limits(1_000_000, maximum_decoded_bytes=200_000, maximum_records=6_000),
    )
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    captures = replayable_parquet_captures(acquisition)
    assert (
        sum(
            capture.manifest.decoded_bytes
            for stream_capture in acquisition.stream_captures
            for capture in stream_capture.captures
        )
        < 200_000
    )

    with pytest.raises(SnowflakeBundleError, match="aggregate decoded-byte limit"):
        prepare_bundle_replay(acquisition)
    with pytest.raises(SnowflakeBundleError, match="aggregate decoded-byte limit"):
        _reconstruct(connector, request, captures)


def test_durable_replay_rejects_receipts_from_different_acquisitions_with_one_snapshot():
    first_result, _first_sources = _result(query_id="query-first")
    second_root_payload = _parquet(
        pa.table(
            {
                "npi": pa.array(["1003000126"], type=pa.string()),
                "score": pa.array([8], type=pa.int64()),
                "enabled": pa.array([True], type=pa.bool_()),
            }
        )
    )
    second_result, _second_sources = _result(
        query_id="query-second",
        options=_ResultOptions(root_readers=(_Reader(second_root_payload),)),
    )
    results = iter((first_result, second_result))
    connector = _connector(_Adapter(lambda: next(results)))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    first_captures = replayable_parquet_captures(connector.acquire(request))
    second_captures = replayable_parquet_captures(connector.acquire(request))
    captures_by_stream = {capture.receipt.stream_id: capture for capture in second_captures}
    captures_by_stream[first_captures[0].receipt.stream_id] = first_captures[0]
    mixed_captures = tuple(captures_by_stream[stream.stream_id] for stream in _definition().source_streams)

    with pytest.raises(SnowflakeBundleError, match="acquisition identity"):
        _reconstruct(connector, request, mixed_captures)


def test_durable_replay_rejects_receipts_for_a_different_configured_request():
    connector = _connector(_Adapter(lambda: _result(query_id="query-request-identity")[0]))
    first_request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(first_request))
    second_request = connector.prepare_request(_definition_with_revision(2), bindings=_bindings())

    assert first_request.request_sha256 != second_request.request_sha256
    with pytest.raises(SnowflakeBundleError, match="configured request"):
        _reconstruct(connector, second_request, captures)


def test_durable_replay_rejects_tampered_capture_limits_with_the_original_receipt_digest():
    connector = _connector(_Adapter(lambda: _result(query_id="query-receipt-tamper")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))
    original = captures[0]
    document = json.loads(original.receipt.canonical_manifest)
    document["capture_limits"]["maximum_records"] = 999
    tampered_receipt = CaptureReceipt(
        stream_id=original.receipt.stream_id,
        source_snapshot_token=original.receipt.source_snapshot_token,
        byte_count=original.receipt.byte_count,
        content_sha256=original.receipt.content_sha256,
        canonical_manifest=json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        manifest_sha256=original.receipt.manifest_sha256,
    )
    tampered_captures = (
        ReplayableParquetCapture(receipt=tampered_receipt, parts=original.parts),
        *captures[1:],
    )

    with pytest.raises(SnowflakeBundleError, match="replay receipt is invalid"):
        _reconstruct(connector, request, tampered_captures)


def test_durable_replay_rejects_a_reissued_receipt_with_a_tampered_capture_seal():
    connector = _connector(_Adapter(lambda: _result(query_id="query-capture-seal-tamper")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))
    original = captures[0]
    document = json.loads(original.receipt.canonical_manifest)
    assert document["captures"][0]["capture_sha256"] != "0" * 64
    document["captures"][0]["capture_sha256"] = "0" * 64
    canonical_manifest, manifest_sha256 = snowflake_bundle._canonical_identity("durable-stream-capture", document)
    tampered_receipt = CaptureReceipt(
        stream_id=original.receipt.stream_id,
        source_snapshot_token=original.receipt.source_snapshot_token,
        byte_count=original.receipt.byte_count,
        content_sha256=original.receipt.content_sha256,
        canonical_manifest=canonical_manifest,
        manifest_sha256=manifest_sha256,
    )
    tampered_captures = (
        ReplayableParquetCapture(receipt=tampered_receipt, parts=original.parts),
        *captures[1:],
    )

    with pytest.raises(SnowflakeBundleError, match="acquisition identity"):
        _reconstruct(connector, request, tampered_captures)


def test_durable_replay_rejects_reissued_capture_limits_that_exceed_the_request_policy():
    result, _sources = _result(query_id="query-reissued-limits", options=_ResultOptions(root_partition_count=2))
    connector = _connector(
        _Adapter(lambda: result),
        capture_limits=_capture_limits(1_000_000, maximum_records=1),
    )
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))
    reissued_captures = _reissue_captures(
        captures,
        lambda document: document["capture_limits"].__setitem__("maximum_records", 2),
    )

    with pytest.raises(SnowflakeBundleError, match="configured capture limits"):
        _reconstruct(connector, request, reissued_captures)


@pytest.mark.parametrize(
    "malformed_field",
    (
        "extra_key",
        "missing_limit_field",
        "empty_schema",
        "incomplete_schema_entry",
        "duplicate_schema",
        "missing_captures",
        "incomplete_capture_entry",
        "wrong_capture_size",
    ),
)
def test_durable_replay_rejects_reissued_receipts_with_invalid_shape(malformed_field):
    connector = _connector(_Adapter(lambda: _result(query_id="synthetic-receipt-shape")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))

    with pytest.raises(SnowflakeBundleError, match="replay receipt is invalid"):
        _reconstruct(
            connector,
            request,
            _reissue_captures(captures, lambda document: _mutate_invalid_receipt_shape(document, malformed_field)),
        )


def test_durable_replay_rederives_the_acquisition_identity():
    connector = _connector(_Adapter(lambda: _result(query_id="query-reissued-identity")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))
    reissued_captures = _reissue_captures(
        captures,
        lambda document: document.__setitem__("acquisition_manifest_sha256", "0" * 64),
    )

    with pytest.raises(SnowflakeBundleError, match="acquisition identity"):
        _reconstruct(connector, request, reissued_captures)


def test_durable_replay_requires_the_same_resolved_statement():
    connector = _connector(_Adapter(lambda: _result(query_id="query-statement-binding")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))
    statement = connector.build_statement(request)
    drifting_statement = SnowflakeBundleStatement(
        request=request,
        selected_columns_by_stream=(
            (
                SnowflakeDeclaredColumn(field_id="npi", column_identifier="other_npi"),
                *statement.selected_columns_by_stream[0][1:],
            ),
            statement.selected_columns_by_stream[1],
        ),
        source_snapshot_token_columns_by_stream=statement.source_snapshot_token_columns_by_stream,
    )

    with pytest.raises(SnowflakeBundleError, match="configured statement"):
        reconstruct_replayable_parquet_bundle(drifting_statement, captures)


def test_durable_replay_requires_a_sealed_statement():
    connector = _connector(_Adapter(lambda: _result(query_id="query-statement-contract")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))

    with pytest.raises(SnowflakeBundleError, match="sealed bundle statement"):
        reconstruct_replayable_parquet_bundle(request, captures)


def test_durable_replay_enforces_capture_bytes_across_streams(monkeypatch):
    result, sources = _result(query_id="query-durable-capture-budget")
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))
    captures_by_stream = {capture.receipt.stream_id: capture for capture in captures}
    first_stream = _definition().source_streams[0]
    first_stream_bytes = sum(len(part) for part in captures_by_stream[first_stream.stream_id].parts)
    monkeypatch.setattr(snowflake_bundle, "MAX_BUNDLE_CAPTURE_BYTES", first_stream_bytes)

    with pytest.raises(SnowflakeBundleError, match="aggregate byte"):
        _reconstruct(connector, request, captures)

    assert all(source.close_count == 1 for source in sources)


def test_durable_replay_enforces_partitions_across_streams(monkeypatch):
    result, sources = _result(
        query_id="query-durable-partition-budget",
        options=_ResultOptions(detail_partition_count=2),
    )
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    captures = replayable_parquet_captures(connector.acquire(request))
    monkeypatch.setattr(snowflake_bundle_replay, "MAX_BUNDLE_PARTITIONS", 2)

    with pytest.raises(SnowflakeBundleError, match="manifest limit"):
        _reconstruct(connector, request, captures)

    assert all(source.close_count == 1 for source in sources)


def test_bundle_rejects_upward_compressed_byte_override():
    with pytest.raises(SnowflakeBundleError, match="compressed-byte"):
        _connector(
            _Adapter(lambda: _result(query_id="query-override")[0]),
            capture_limits=_capture_limits(snowflake_bundle.MAX_STREAM_CAPTURE_BYTES + 1),
        )


@pytest.mark.parametrize("name", ("maximum_records", "maximum_fields_per_record"))
def test_bundle_rejects_upward_capture_shape_override(name):
    limits = snowflake_bundle.DEFAULT_CAPTURE_LIMITS
    oversized = replace(limits, **{name: getattr(limits, name) + 1})
    with pytest.raises(SnowflakeBundleError, match="record or field bounds"):
        _connector(_Adapter(lambda: _result(query_id="query-shape-override")[0]), capture_limits=oversized)


def test_bundle_caps_parts_across_streams_without_materializing_8193_parquet_readers(monkeypatch):
    result, sources = _result(query_id="query-parts", options=_ResultOptions(detail_partition_count=2))
    monkeypatch.setattr(snowflake_bundle, "MAX_BUNDLE_PARTITIONS", 2)
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match="manifest limit"):
        connector.acquire(request)

    assert all(source.close_count == 1 for source in sources)
    assert all(reader.close_count == 1 for source in sources for reader in source.readers)


def test_bundle_caps_parts_per_stream_without_materializing_4097_parquet_readers(monkeypatch):
    assert snowflake_bundle.MAX_RESULT_PARTITIONS == 4_096
    assert snowflake_bundle.MAX_BUNDLE_PARTITIONS == 8_192
    result, sources = _result(query_id="query-stream-parts", options=_ResultOptions(root_partition_count=3))
    monkeypatch.setattr(snowflake_bundle, "MAX_RESULT_PARTITIONS", 2)
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match="result limit"):
        connector.acquire(request)

    assert 3 + 1 < snowflake_bundle.MAX_BUNDLE_PARTITIONS
    assert sources[0].iterator is not None and sources[0].iterator.close_count == 1
    assert all(source.close_count == 1 for source in sources)
    assert all(reader.close_count == 1 for source in sources for reader in source.readers)


def test_bundle_rejects_closeable_non_reader_and_releases_owned_resources():
    malformed = _CloseOnlyReader()
    result, sources = _result(query_id="query-non-reader", options=_ResultOptions(root_readers=(malformed,)))
    connector = _connector(_Adapter(lambda: result))
    request = connector.prepare_request(_definition(), bindings=_bindings())

    with pytest.raises(SnowflakeBundleError, match="binary reader"):
        connector.acquire(request)

    assert malformed.close_count == 1
    assert sources[0].iterator is not None and sources[0].iterator.close_count == 1
    assert all(source.close_count == 1 for source in sources)


def _python_bundle_connector(monkeypatch, rows: tuple[tuple[object, ...], ...]):
    cursor = _PythonCursor(rows)
    connection = _PythonConnection(cursor)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python._private_key_der", lambda _credentials: b"synthetic-key"
    )
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: connection,
    )
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=_relations(),
        credential_provider=_Credentials(),
        adapter=SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh"),
    )
    return connector, connector.prepare_request(_definition(), bindings=_bindings()), cursor, connection


def _python_metadata_rows(
    *,
    root_token: str | None = _SNAPSHOT,
    detail_token: str | None = _SNAPSHOT,
) -> tuple[tuple[object, ...], ...]:
    return (
        (0, 1, "root_source", root_token, None, None, None, None, None, None),
        (0, 2, "detail_source", detail_token, None, None, None, None, None, None),
    )


def test_python_adapter_replays_empty_stream_offline(monkeypatch):
    connector, request, cursor, connection = _python_bundle_connector(
        monkeypatch,
        (
            *_python_metadata_rows(),
            (1, 1, "root_source", None, "1003000126", 7, True, None, None, None),
        ),
    )

    acquisition = connector.acquire(request)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: (_ for _ in ()).throw(AssertionError("offline replay must not open Snowflake")),
    )
    replay = prepare_bundle_replay(acquisition)

    assert cursor.executed == ["USE SECONDARY ROLES NONE", acquisition.statement.sql]
    assert acquisition.source_snapshot_token == _SNAPSHOT
    assert acquisition.diagnostic_query_id == "synthetic-common-query"
    assert [stream.receipt.source_snapshot_token for stream in replay.streams] == [
        _SNAPSHOT,
        _SNAPSHOT,
    ]
    assert replay.roots == ({"npi": "1003000126", "score": 7, "enabled": True},)
    assert replay.children_by_collection == {"details": ()}
    assert all(stream.parts for stream in replay.streams)
    assert cursor.closed and connection.closed


def test_python_adapter_rejects_malformed_common_stream_evidence_and_cleans_up(monkeypatch):
    connector, request, cursor, connection = _python_bundle_connector(
        monkeypatch,
        (
            *_python_metadata_rows(),
            (1, 1, "detail_source", None, "1003000126", 7, True, None, None, None),
        ),
    )

    with pytest.raises(SnowflakeBundleError, match="bundle result cannot be sealed"):
        connector.acquire(request)

    assert cursor.closed and connection.closed


def test_python_adapter_rejects_differing_source_snapshot_tokens_and_cleans_up(monkeypatch):
    connector, request, cursor, connection = _python_bundle_connector(
        monkeypatch,
        _python_metadata_rows(detail_token="synthetic-release-other"),
    )

    with pytest.raises(SnowflakeConnectorError, match="one shared semantic snapshot token"):
        connector.acquire(request)

    assert cursor.closed and connection.closed


def test_offline_replay_rejects_corrupt_sealed_parquet(monkeypatch):
    connector, request, _cursor, _connection = _python_bundle_connector(
        monkeypatch,
        (
            *_python_metadata_rows(),
            (1, 1, "root_source", None, "1003000126", 7, True, None, None, None),
        ),
    )
    acquisition = connector.acquire(request)
    object.__setattr__(acquisition.stream_captures[0].captures[0], "payload", b"corrupt")

    with pytest.raises(SnowflakeBundleError, match="bundle capture cannot be replayed"):
        prepare_bundle_replay(acquisition)


def test_bundle_exposes_replayable_payloads_for_later_durable_storage(monkeypatch):
    connector, request, _cursor, _connection = _python_bundle_connector(
        monkeypatch,
        (
            *_python_metadata_rows(),
            (1, 1, "root_source", None, "1003000126", 7, True, None, None, None),
        ),
    )
    acquisition = connector.acquire(request)
    captures = replayable_parquet_captures(acquisition)
    monkeypatch.setattr(
        "process.custom_import.snowflake_python.snowflake.connector.connect",
        lambda **_arguments: pytest.fail("durable replay must not open Snowflake"),
    )
    replay = _reconstruct(connector, request, captures)

    assert [capture.receipt.stream_id for capture in captures] == ["root_source", "detail_source"]
    assert replay.roots == ({"npi": "1003000126", "score": 7, "enabled": True},)
    assert replay.children_by_collection == {"details": ()}


@pytest.mark.asyncio
async def test_bundle_candidate_rejects_an_acquired_request_mismatch_before_capture_registration(monkeypatch):
    """Stop a mismatched acquisition before either capture registration or replay."""

    connector = _connector(_Adapter(lambda: _result(query_id="query-request-mismatch")[0]))
    acquired_request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(acquired_request)
    alternate_request = replace(
        acquired_request,
        bindings=(
            replace(
                acquired_request.bindings[0],
                relation=SnowflakeRelation(database="synthetic", schema="public", name="other_roots"),
            ),
            acquired_request.bindings[1],
        ),
    )
    prepared_statement = SnowflakeBundleStatement(
        request=alternate_request,
        selected_columns_by_stream=acquisition.statement.selected_columns_by_stream,
        source_snapshot_token_columns_by_stream=acquisition.statement.source_snapshot_token_columns_by_stream,
    )

    async def unexpected_capture_registration(*_arguments):
        pytest.fail("request mismatch must stop before durable capture registration")

    session_factory, transitions = _fenced_failure_dependencies(monkeypatch, transition_state="failed")

    monkeypatch.setattr(snowflake_candidate, "_register_bundle_captures", unexpected_capture_registration)

    with pytest.raises(SnowflakeCandidateError, match="does not match the prepared bundle statement"):
        await run_snowflake_bundle_candidate(
            session_factory,
            SimpleNamespace(
                build_statement=lambda _request: prepared_statement,
                acquire=lambda _request, *, prepared_statement: acquisition,
            ),
            SnowflakeBundleCandidateRequest(
                dataset_id=1,
                definition_revision_id=1,
                schema_revision_id=1,
                definition=_definition(),
                bundle_request=alternate_request,
                idempotency_key="synthetic-request-mismatch",
                lease_token="synthetic-request-mismatch-lease",
            ),
        )

    _assert_fenced_failure(transitions, "synthetic-request-mismatch-lease")


@pytest.mark.asyncio
async def test_bundle_candidate_decodes_before_durable_capture_registration(monkeypatch):
    """Reject invalid Parquet in the worker before registering a capture."""

    invalid_root_payload = _parquet(
        pa.table(
            {
                "npi": pa.array(["1003000126"], type=pa.string()),
                "score": pa.array(["not-an-integer"], type=pa.string()),
                "enabled": pa.array([True], type=pa.bool_()),
            }
        )
    )
    bundle_result, _sources = _result(
        query_id="query-pre-registration-decode",
        options=_ResultOptions(root_readers=(_Reader(invalid_root_payload),)),
    )
    connector = _connector(_Adapter(lambda: bundle_result))
    bundle_request = connector.prepare_request(_definition(), bindings=_bindings())
    event_loop_thread = current_thread()
    prepare_captures = snowflake_candidate._prepared_bundle_captures

    def prepare_in_worker(acquisition):
        assert current_thread() is not event_loop_thread
        return prepare_captures(acquisition)

    async def unexpected_capture_registration(*_arguments):
        pytest.fail("invalid Parquet must stop before durable capture registration")

    session_factory, transitions = _fenced_failure_dependencies(monkeypatch, transition_state="failed")

    monkeypatch.setattr(snowflake_candidate, "_register_bundle_captures", unexpected_capture_registration)
    monkeypatch.setattr(snowflake_candidate, "_prepared_bundle_captures", prepare_in_worker)

    with pytest.raises(SnowflakeCandidateError, match="cannot be retained"):
        await run_snowflake_bundle_candidate(
            session_factory,
            connector,
            SnowflakeBundleCandidateRequest(
                dataset_id=1,
                definition_revision_id=1,
                schema_revision_id=1,
                definition=_definition(),
                bundle_request=bundle_request,
                idempotency_key="synthetic-pre-registration-decode",
                lease_token="synthetic-pre-registration-decode-lease",
            ),
        )

    _assert_fenced_failure(transitions, "synthetic-pre-registration-decode-lease")


@pytest.mark.asyncio
async def test_bundle_candidate_seals_prepared_captures_in_worker(monkeypatch):
    connector = _connector(_Adapter(lambda: _result(query_id="query-worker-capture")[0]))
    request = connector.prepare_request(_definition(), bindings=_bindings())
    acquisition = connector.acquire(request)
    event_loop_thread = current_thread()
    seal_captures = snowflake_candidate.replayable_parquet_captures

    def seal_in_worker(acquired):
        assert current_thread() is not event_loop_thread
        return seal_captures(acquired)

    monkeypatch.setattr(snowflake_candidate, "replayable_parquet_captures", seal_in_worker)
    captures = await asyncio.to_thread(snowflake_candidate._prepared_bundle_captures, acquisition)
    assert tuple(capture.receipt.stream_id for capture in captures) == ("root_source", "detail_source")


@pytest.mark.asyncio
async def test_bundle_candidate_replays_durable_captures_in_worker(monkeypatch):
    event_loop_thread = current_thread()
    prepared_statement = object()
    durable_captures = object()
    replay = object()

    @asynccontextmanager
    async def session_factory():
        yield object()

    async def load_captures(_session, **_kwargs):
        return durable_captures

    def replay_captures(statement, captures):
        assert current_thread() is not event_loop_thread
        assert statement is prepared_statement and captures is durable_captures
        return replay

    monkeypatch.setattr(snowflake_candidate, "load_replayable_parquet_bundle", load_captures)
    monkeypatch.setattr(snowflake_candidate, "reconstruct_replayable_parquet_bundle", replay_captures)

    assert (
        await snowflake_candidate._load_bundle_replay(
            session_factory,
            SimpleNamespace(dataset_id=1, definition_revision_id=1, schema_revision_id=1),
            prepared_statement,
            1,
        )
        is replay
    )


@pytest.mark.asyncio
async def test_bundle_candidate_does_not_reacquire_an_unbound_reservation(monkeypatch):
    connector = _connector(_Adapter(lambda: _result(query_id="query-unbound-reservation")[0]))
    bundle_request = connector.prepare_request(_definition(), bindings=_bindings())
    connector_calls = []
    grant = SimpleNamespace(execution_id=1, fence=2, state="running")

    async def reserved_execution(*_arguments):
        return SimpleNamespace(execution_id=1, capture_bundle_id=None, created=False, state="running"), grant

    async def reject_unbound(*_arguments):
        raise SnowflakeCandidateError("unbound source capture")

    def unexpected_session_factory() -> None:
        raise AssertionError("unbound reservation must stop before a source operation")

    def acquire(request, *, prepared_statement):
        connector_calls.append(request)
        raise AssertionError("unbound reservation must not reacquire Snowflake")

    monkeypatch.setattr(snowflake_candidate, "_reserve_bundle_execution", reserved_execution)
    monkeypatch.setattr(snowflake_candidate, "_unbound_capture_result", reject_unbound)

    with pytest.raises(SnowflakeCandidateError, match="unbound source capture"):
        await run_snowflake_bundle_candidate(
            unexpected_session_factory,
            SimpleNamespace(build_statement=connector.build_statement, acquire=acquire),
            SnowflakeBundleCandidateRequest(
                dataset_id=1,
                definition_revision_id=1,
                schema_revision_id=1,
                definition=_definition(),
                bundle_request=bundle_request,
                idempotency_key="synthetic-unbound-reservation",
                lease_token="synthetic-unbound-reservation-lease",
            ),
        )

    assert connector_calls == []


@pytest.mark.asyncio
async def test_bundle_candidate_rejects_invalid_connector_before_reservation(monkeypatch):
    connector = _connector(_Adapter(lambda: _result(query_id="query-invalid-connector")[0]))
    bundle_request = connector.prepare_request(_definition(), bindings=_bindings())
    reservations = []

    async def unexpected_reservation(*_arguments):
        reservations.append(True)
        raise AssertionError("invalid connector must not reserve an execution")

    monkeypatch.setattr(snowflake_candidate, "_reserve_bundle_execution", unexpected_reservation)
    with pytest.raises(SnowflakeCandidateError, match="requires a bundle acquisition connector"):
        await run_snowflake_bundle_candidate(
            lambda: pytest.fail("invalid connector must not open a session"),
            object(),
            SnowflakeBundleCandidateRequest(
                dataset_id=1,
                definition_revision_id=1,
                schema_revision_id=1,
                definition=_definition(),
                bundle_request=bundle_request,
                idempotency_key="synthetic-invalid-connector",
                lease_token="synthetic-invalid-connector-lease",
            ),
        )

    assert reservations == []


@pytest.mark.asyncio
async def test_bundle_candidate_rejects_an_acquire_signature_without_prepared_statement(monkeypatch):
    connector = _connector(_Adapter(lambda: _result(query_id="query-invalid-acquire-signature")[0]))
    bundle_request = connector.prepare_request(_definition(), bindings=_bindings())
    reservations = []

    async def unexpected_reservation(*_arguments):
        reservations.append(True)
        raise AssertionError("incompatible acquisition must not reserve an execution")

    monkeypatch.setattr(snowflake_candidate, "_reserve_bundle_execution", unexpected_reservation)
    with pytest.raises(SnowflakeCandidateError, match="must accept a prepared statement"):
        await run_snowflake_bundle_candidate(
            lambda: pytest.fail("incompatible acquisition must not open a session"),
            SimpleNamespace(build_statement=connector.build_statement, acquire=lambda _request: None),
            SnowflakeBundleCandidateRequest(
                dataset_id=1,
                definition_revision_id=1,
                schema_revision_id=1,
                definition=_definition(),
                bundle_request=bundle_request,
                idempotency_key="synthetic-invalid-acquire-signature",
                lease_token="synthetic-invalid-acquire-signature-lease",
            ),
        )

    assert reservations == []


@pytest.mark.asyncio
async def test_bundle_candidate_rejects_a_stale_request_seal_before_reservation(monkeypatch):
    adapter = _Adapter(lambda: pytest.fail("stale preflight must not contact the source adapter"))
    connector = _connector(adapter)
    bundle_request = connector.prepare_request(_definition(), bindings=_bindings())
    object.__setattr__(bundle_request, "request_sha256", "0" * 64)
    reservations = []

    async def unexpected_reservation(*_arguments):
        reservations.append(True)
        raise AssertionError("stale preflight must not reserve an execution")

    monkeypatch.setattr(snowflake_candidate, "_reserve_bundle_execution", unexpected_reservation)
    with pytest.raises(SnowflakeCandidateError, match="statement cannot be prepared"):
        await run_snowflake_bundle_candidate(
            lambda: pytest.fail("stale preflight must not open a session"),
            connector,
            SnowflakeBundleCandidateRequest(
                dataset_id=1,
                definition_revision_id=1,
                schema_revision_id=1,
                definition=_definition(),
                bundle_request=bundle_request,
                idempotency_key="synthetic-stale-request-seal",
                lease_token="synthetic-stale-request-seal-lease",
            ),
        )

    assert reservations == []
    assert adapter.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_class",
    (SnowflakeBundleError, SnowflakeConnectorError, SnowflakeCredentialError),
)
@pytest.mark.parametrize("reservation_created", (True, False))
async def test_bundle_candidate_wraps_acquisition_failure(monkeypatch, error_class, reservation_created):
    connector = _connector(_Adapter(lambda: _result(query_id="query-acquisition-failure")[0]))
    bundle_request = connector.prepare_request(_definition(), bindings=_bindings())

    def raise_acquisition_error(
        _request: SnowflakeBundleRequest, *, prepared_statement: SnowflakeBundleStatement
    ) -> None:
        raise error_class("synthetic acquisition failure")

    to_thread_calls = []

    async def inline_to_thread(function, *arguments):
        to_thread_calls.append(function)
        return function(*arguments)

    session_factory, transitions = _fenced_failure_dependencies(
        monkeypatch, transition_state="canceled", reservation_created=reservation_created
    )

    monkeypatch.setattr(snowflake_candidate.asyncio, "to_thread", inline_to_thread)

    with pytest.raises(SnowflakeCandidateError, match="cannot be acquired") as failure:
        await run_snowflake_bundle_candidate(
            session_factory,
            SimpleNamespace(build_statement=connector.build_statement, acquire=raise_acquisition_error),
            SnowflakeBundleCandidateRequest(
                dataset_id=1,
                definition_revision_id=1,
                schema_revision_id=1,
                definition=_definition(),
                bundle_request=bundle_request,
                idempotency_key="synthetic-acquisition-failure",
                lease_token="synthetic-acquisition-failure-lease",
            ),
        )

    assert isinstance(failure.value.__cause__, error_class)
    assert to_thread_calls == [snowflake_candidate._acquired_bundle]
    _assert_fenced_failure(transitions, "synthetic-acquisition-failure-lease")
