# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit statement snapshots retain every stream under one query identity."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import snowflake_candidate as candidate
from process.custom_import import snowflake_capture as capture
from process.custom_import.definition import canonical_json
from process.custom_import.snowflake import SnowflakeApprovedRelation, SnowflakeRelation
from process.custom_import.snowflake_binding import SnowflakeSourceBinding, SnowflakeSourceBindingError
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleError,
    SnowflakeBundleStatementBuilder,
    _retained_statement_query_id,
    _validated_bundle_request,
    prepare_bundle_replay,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_bundle_replay import _rebuilt_bundle_statement
from process.custom_import.snowflake_operator_cli import _retained_bundle_request
from process.custom_import.snowflake_preflight import SnowflakePreflightLimits, _prepare_preflight
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_capture import _Harness
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row
from tests.test_custom_import_snowflake_source_binding import _binding_document, _definition

_MODE = "statement_query_id"
_LEGACY_DIGESTS = {
    1: (
        "0fa5896a1f329396f287547c2269e4bddf03ee01ce9522289a38fb9d705bd1d5",
        "35782c35bf9060ff034a4edfc840acd6579daf7be78e23d1bcfe64c3e4a95c7d",
        "618d96eaab702063007e642eab142bb6627fb5890f03185c08a04616f8dbffaa",
    ),
    2: (
        "70550162e3a20c409eeddd3c8bbdd610798706caeef7b3581c9adf65cbe68fd5",
        "457f4d4b0c798eaedde341c41d7b86c970624605312adb9f611e0d26a462619b",
        "bdfd0b9a8e0cf59469a3dd7b5317bf50e534703a03f9d49453aa29367d381efa",
    ),
}


def _statement_binding(version=1, *, opted_in=True):
    definition = _definition()
    document = _binding_document(definition)
    if version == 2:
        document.update(contract="custom-import/source-binding/v2", processing_policy=_policy_document())
    if opted_in:
        document["snapshot_token_mode"] = _MODE
        for stream in document["streams"]:
            stream["source_snapshot_token_relation"] = None
            stream["source_snapshot_token_column_identifier"] = None
    binding = SnowflakeSourceBinding.from_mapping(document)
    approved, streams = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved)
    request = builder.prepare_request(
        definition,
        bindings=streams,
        processing_policy=binding.processing_policy,
        snapshot_token_mode=binding.snapshot_token_mode,
    )
    return binding, builder, request


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_bytes_and_opted_in_identity(version):
    legacy, _, old_request = _statement_binding(version, opted_in=False)
    old_statement = SnowflakeBundleStatementBuilder(
        approved_relations=legacy.bundle_components(old_request.definition)[0]
    ).build_statement(old_request)
    assert (legacy.digest, old_request.request_sha256, old_statement.statement_sha256) == _LEGACY_DIGESTS[version]
    assert "snapshot_token_mode" not in legacy.canonical + old_request.canonical_request
    binding, builder, request = _statement_binding(version)
    statement = builder.build_statement(request)
    assert SnowflakeSourceBinding.from_json(binding.canonical) == binding
    assert json.loads(binding.canonical)["snapshot_token_mode"] == _MODE
    assert json.loads(request.canonical_request)["snapshot_token_mode"] == _MODE
    assert binding.digest != legacy.digest and request.request_sha256 != old_request.request_sha256
    assert _validated_bundle_request(request) == request
    assert _rebuilt_bundle_statement(statement) == (request, statement)
    prepared = _prepare_preflight(request.definition, binding, builder, limits=SnowflakePreflightLimits())
    assert prepared.statement.bundle_statement.request == request
    loaded = SimpleNamespace(definition=request.definition, binding=binding, bundle_bindings=request.bindings)
    assert _retained_bundle_request(builder, loaded) == request
    object.__setattr__(request, "snapshot_token_mode", None)
    with pytest.raises(SnowflakeBundleError):
        builder.build_statement(request)


@pytest.mark.parametrize("value", [None, False, True, 1, "statement", "", {}, []])
def test_binding_rejects_invalid_mode(value):
    binding, _, _ = _statement_binding()
    document = json.loads(binding.canonical)
    document["snapshot_token_mode"] = value
    with pytest.raises(SnowflakeSourceBindingError):
        SnowflakeSourceBinding.from_mapping(document)


@pytest.mark.parametrize("stream_index", [0, 1])
@pytest.mark.parametrize("column_only", [False, True])
def test_statement_binding_rejects_mixed_pairs(stream_index, column_only):
    binding, _, request = _statement_binding()
    document = json.loads(binding.canonical)
    stream = document["streams"][stream_index]
    stream["source_snapshot_token_column_identifier"] = "SOURCE_TOKEN"
    if not column_only:
        stream["source_snapshot_token_relation"] = ["SYNTHETIC", "PUBLIC", "TOKENS"]
    with pytest.raises(SnowflakeSourceBindingError):
        SnowflakeSourceBinding.from_mapping(document)
    with pytest.raises(SnowflakeBundleError, match="one root"):
        replace(request, snapshot_token_mode=None)


def test_request_rejects_source_tokens_under_statement_mode():
    _, _, request = _statement_binding(opted_in=False)
    with pytest.raises(SnowflakeBundleError, match="all snapshot token relations"):
        replace(request, snapshot_token_mode=_MODE)
    with pytest.raises(SnowflakeBundleError, match="unsupported"):
        replace(request, snapshot_token_mode=True)


@pytest.mark.parametrize("rows", [(), (_shared_row(),)])
@pytest.mark.parametrize("interleaved", [False, True])
def test_one_statement_captures_empty_streams(monkeypatch, rows, interleaved):
    if interleaved:
        rows = tuple((*row, None, None, None) for row in rows)
    connector, request, _, cursor, connection = _runtime(
        monkeypatch, rows, interleaved=interleaved, snapshot_token_mode=_MODE
    )
    acquisition = connector.acquire(request)
    token = f"snowflake-query:{cursor.sfqid}"
    replay = prepare_bundle_replay(acquisition)
    assert acquisition.source_snapshot_token == token
    assert len(replay.streams) == len(request.bindings)
    assert all(stream.receipt.source_snapshot_token == token for stream in replay.streams)
    retained = replayable_parquet_captures(acquisition)
    assert reconstruct_replayable_parquet_bundle(acquisition.statement, retained) == replay
    assert all(len(stream.captures) >= 1 for stream in acquisition.stream_captures)
    assert cursor.executed == [acquisition.statement.sql]
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("query_id", [None, "", " ", "bad\nquery"])
def test_missing_statement_identity_cleans_up(monkeypatch, query_id):
    connector, request, _, cursor, connection = _runtime(monkeypatch, snapshot_token_mode=_MODE)
    cursor.sfqid = query_id
    with pytest.raises(ValueError):
        connector.acquire(request)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("token", ["synthetic-token", "snowflake-query:", "snowflake-query: ", "snowflake-query:bad\n"])
def test_retained_query_token_is_not_an_arbitrary_prefix(token):
    with pytest.raises(SnowflakeBundleError):
        _retained_statement_query_id(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [(), (_shared_row(),)])
async def test_segmented_statement_seals_all_streams(monkeypatch, rows):
    harness = _Harness(monkeypatch, rows, snapshot_token_mode=_MODE)
    result = await harness.run()
    assert result.status == "capture_sealed"
    statement = harness.builder.build_statement(harness.request.bundle_request)
    token = f"snowflake-query:{harness.cursor.sfqid}"
    capture._validate_statement_receipts(statement, token, harness.receipts)
    assert len(harness.receipts) == 2 and len(harness.parts) == 2
    assert all(
        json.loads(receipt.canonical_manifest)["source"]["query_id"] == harness.cursor.sfqid
        for receipt in harness.receipts
    )
    assert all(receipt.source_snapshot_token == token for receipt in harness.receipts)
    assert harness.cursor.executed == [statement.sql]
    assert harness.cursor.closed and harness.connection.closed


def _reseal_receipt(receipt, mutation):
    document = json.loads(receipt.canonical_manifest)
    mutation(document)
    canonical = canonical_json(document)
    return replace(
        receipt, canonical_manifest=canonical, manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [("query_id", None), ("query_id", "other-query"), ("request_sha256", "0" * 64), ("statement_sha256", "0" * 64)],
)
async def test_recomputed_receipt_cannot_change_statement(monkeypatch, field, value):
    harness = _Harness(monkeypatch, snapshot_token_mode=_MODE)
    await harness.run()
    statement = harness.builder.build_statement(harness.request.bundle_request)
    changed = _reseal_receipt(harness.receipts[1], lambda document: document["source"].update({field: value}))
    with pytest.raises(capture.SnowflakeCaptureError, match="statement metadata"):
        capture._validate_statement_receipts(
            statement, harness.pending.source_snapshot_token, (harness.receipts[0], changed)
        )
    with pytest.raises(capture.SnowflakeCaptureError, match="statement metadata"):
        capture._validate_statement_receipts(statement, harness.pending.source_snapshot_token, (harness.receipts[0],))
    with pytest.raises(capture.SnowflakeCaptureError, match="statement metadata"):
        capture._validate_statement_receipts(
            statement, harness.pending.source_snapshot_token, (harness.receipts[0],) * 2
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("source_request_sha256", "0" * 64),
        ("statement_sha256", "0" * 64),
        ("source_snapshot_token", "other"),
        ("stream_id", "other_source"),
        ("source", None),
    ],
)
async def test_recomputed_receipt_header_is_not_authority(monkeypatch, field, value):
    harness = _Harness(monkeypatch, snapshot_token_mode=_MODE)
    await harness.run()
    statement = harness.builder.build_statement(harness.request.bundle_request)
    changed = _reseal_receipt(harness.receipts[1], lambda document: document.update({field: value}))
    with pytest.raises(capture.SnowflakeCaptureError, match="statement metadata"):
        capture._validate_statement_receipts(
            statement, harness.pending.source_snapshot_token, (harness.receipts[0], changed)
        )


@pytest.mark.asyncio
async def test_already_bound_replay_checks_retained_headers(monkeypatch):
    harness = _Harness(monkeypatch, snapshot_token_mode=_MODE)
    await harness.run()
    statement = harness.builder.build_statement(harness.request.bundle_request)
    harness.submission = replace(harness.submission, capture_bundle_id=23)
    harness.bound = SimpleNamespace(
        **{
            name: getattr(harness.pending, name)
            for name in (
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "request_identity_sha256",
                "source_request_sha256",
                "statement_sha256",
                "source_binding_revision_id",
                "source_binding_sha256",
            )
        },
        capture_bundle_id=23,
        snapshot_token=harness.pending.source_snapshot_token,
        capture_state="sealed",
        payload_contract=capture.SEGMENTED_PAYLOAD_CONTRACT,
        canonical_policy=harness.policy.canonical,
        policy_sha256=bytes.fromhex(harness.policy.digest),
        producing_execution_id=9,
    )
    headers = AsyncMock(return_value=(dict(enumerate(harness.receipts)), {}, {}))
    monkeypatch.setattr(capture, "_load_parquet_part_metadata", headers)
    assert (await harness.run()).status == "capture_bound"
    assert headers.await_count == 1 and harness.cursor.executed == [statement.sql]
    changed = _reseal_receipt(harness.receipts[1], lambda document: document["source"].update(query_id="other"))
    headers.return_value = ({0: harness.receipts[0], 1: changed}, {}, {})
    with pytest.raises(capture.SnowflakeCaptureError, match="statement metadata"):
        await harness.run()
    assert harness.timeline.count("credentials") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_selected_fields", [False, True])
async def test_bound_request_cannot_invent_statement_mode(monkeypatch, reverse_selected_fields):
    binding, builder, bundle = _statement_binding()
    request = candidate.SnowflakeBundleCandidateRequest(
        1,
        2,
        3,
        bundle.definition,
        bundle,
        "synthetic",
        b"owner",
        source_binding_revision_id=4,
        source_binding_sha256=bytes.fromhex(binding.digest),
    )
    retained_bindings = (
        tuple(
            replace(stream, selected_field_ids=tuple(reversed(stream.selected_field_ids))) for stream in bundle.bindings
        )
        if reverse_selected_fields
        else bundle.bindings
    )
    if reverse_selected_fields:
        assert retained_bindings != bundle.bindings
    loaded = SimpleNamespace(
        dataset_id=1,
        schema_revision_id=3,
        definition=bundle.definition,
        source_binding_sha256=request.source_binding_sha256,
        bundle_bindings=retained_bindings,
        approved_relations=binding.bundle_components(bundle.definition)[0],
        binding=SimpleNamespace(processing_policy=None, snapshot_token_mode=None),
    )
    monkeypatch.setattr(candidate, "load_snowflake_source_binding", AsyncMock(return_value=loaded))
    with pytest.raises(candidate.SnowflakeCandidateError, match="binding identity"):
        await candidate._validate_bound_source_identity(None, request, builder.build_statement(bundle))
    loaded.binding.snapshot_token_mode = _MODE
    await candidate._validate_bound_source_identity(None, request, builder.build_statement(bundle))


@pytest.mark.asyncio
async def test_bound_source_rejects_missing_retained_stream(monkeypatch):
    binding, builder, bundle = _statement_binding()
    request = candidate.SnowflakeBundleCandidateRequest(
        1,
        2,
        3,
        bundle.definition,
        bundle,
        "synthetic",
        b"owner",
        source_binding_revision_id=4,
        source_binding_sha256=bytes.fromhex(binding.digest),
    )
    monkeypatch.setattr(
        candidate,
        "load_snowflake_source_binding",
        AsyncMock(return_value=SimpleNamespace(bundle_bindings=bundle.bindings[:-1])),
    )
    with pytest.raises(candidate.SnowflakeCandidateError, match="binding identity"):
        await candidate._validate_bound_source_identity(None, request, builder.build_statement(bundle))


@pytest.mark.parametrize("corruption", ["missing", "duplicate", "reversed", "source_token"])
def test_invalid_metadata_never_seals(monkeypatch, corruption):
    connector, request, _, cursor, connection = _runtime(monkeypatch, snapshot_token_mode=_MODE)
    if corruption == "missing":
        cursor._rows.pop()
    elif corruption == "duplicate":
        cursor._rows[1] = cursor._rows[0]
    elif corruption == "reversed":
        cursor._rows.reverse()
    else:
        first = cursor._rows[0]
        cursor._rows[0] = (*first[:3], "synthetic-source-token", *first[4:])
    with pytest.raises(ValueError):
        connector.acquire(request)
    assert cursor.closed and connection.closed


@pytest.mark.asyncio
async def test_changed_generated_receipt_cannot_seal(monkeypatch):
    harness = _Harness(monkeypatch, snapshot_token_mode=_MODE)
    render_receipt = capture._StreamReceipt.receipt

    def changed_receipt(state, request):
        receipt = render_receipt(state, request)
        if receipt.stream_id == "detail_source":
            return _reseal_receipt(receipt, lambda document: document["source"].update(query_id="other-statement"))
        return receipt

    monkeypatch.setattr(capture._StreamReceipt, "receipt", changed_receipt)
    with pytest.raises(capture.SnowflakeCaptureError, match="statement metadata"):
        await harness.run()
    assert "seal" not in harness.timeline and "eof" not in harness.timeline
    assert harness.cursor.closed and harness.connection.closed


def test_three_relations_keep_an_empty_middle_stream(monkeypatch):
    npi, amount = _shared_row()[4:6]
    rows = (
        (1, 1, "root_source", None, npi, amount, True, None, None, None, None, None, None),
        (1, 3, "detail_source", None, None, None, None, npi, "a", amount, None, None, None),
    )
    original, request, adapter, cursor, connection = _runtime(
        monkeypatch, rows, interleaved=True, snapshot_token_mode=_MODE
    )
    columns = list(cursor.description)
    columns[7] = SimpleNamespace(name="detail_npi", type_name="TEXT", is_nullable=True)
    columns[9] = SimpleNamespace(name="amount", type_name="FIXED", is_nullable=True, precision=30, scale=12)
    cursor.description = tuple(columns)
    relation = SnowflakeRelation("synthetic", "public", "third_source")
    previous = request.bindings[2].relation
    approved = SnowflakeApprovedRelation(relation, original._approved_by_relation[previous.parts].columns)
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=(*original._approved_by_relation.values(), approved),
        credential_provider=original._credential_provider,
        adapter=adapter,
    )
    request = replace(request, bindings=(*request.bindings[:2], replace(request.bindings[2], relation=relation)))
    assert len({binding.relation for binding in request.bindings}) == 3
    acquisition = connector.acquire(request)
    replay = prepare_bundle_replay(acquisition)
    assert [len(stream.records) for stream in replay.streams] == [1, 0, 1]
    assert replay.source_snapshot_token == f"snowflake-query:{cursor.sfqid}"
    assert cursor.executed == [acquisition.statement.sql]
    assert cursor.closed and connection.closed
