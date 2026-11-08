# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Format-neutral HTTPS coordination with synthetic originals and no network."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
from dataclasses import replace
from io import BytesIO, StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock
from xml.etree import ElementTree

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import https_candidate
from process.custom_import.capture import capture_stream
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.https_candidate import (
    HttpsCandidateError,
    https_request_identity_sha256,
    run_https_candidate,
)
from tests.test_custom_import_definition import _raw_definition
from tests.test_custom_import_https_candidate import _request
from tests.test_custom_import_https_candidate import coordinator as coordinator
from tests.test_custom_import_https_candidate import no_network as no_network


def _encoded_records(format_name, records):
    if format_name in {"csv", "tsv"}:
        destination = StringIO(newline="")
        writer = csv.DictWriter(
            destination, fieldnames=tuple(records[0]), delimiter="\t" if format_name == "tsv" else ","
        )
        writer.writeheader()
        writer.writerows(records)
        return destination.getvalue().encode()
    if format_name == "json":
        return json.dumps(records).encode()
    if format_name == "ndjson":
        return b"".join(json.dumps(record).encode() + b"\n" for record in records)
    if format_name == "xml":
        root = ElementTree.Element("rows")
        for record in records:
            row = ElementTree.SubElement(root, "row")
            for label, value in record.items():
                ElementTree.SubElement(row, label).text = value
        return ElementTree.tostring(root)
    destination = BytesIO()
    pq.write_table(pa.Table.from_pylist(records), destination)
    return destination.getvalue()


def _original_request(format_name, compression, *, mixed=False, empty=False):
    document = _raw_definition()
    document.update(refresh_mode="snapshot", aliases={})
    records_by_stream = {
        "providers": [{"npi": "1234567893", "display_name": "Synthetic Provider"}],
        "rates": [{"rate_npi": "1234567893", "service_code": "A100", "amount": "12.50"}],
    }
    payloads_by_stream = {}
    for index, stream in enumerate(document["streams"]):
        stream.update(format="parquet" if mixed and index == 0 else format_name, compression=compression)
        if stream["format"] == "xml":
            stream["record_path"] = "row"
        payload = b"" if empty else _encoded_records(stream["format"], records_by_stream[stream["id"]])
        payloads_by_stream[stream["id"]] = gzip.compress(payload, mtime=0) if compression == "gzip" else payload
    request, _ = _request(definition=CustomImportDefinition.from_mapping(document))
    return replace(
        request,
        maximum_dataset_retained_bytes=1024 * 1024,
        objects=tuple(
            replace(
                item,
                expected_sha256=hashlib.sha256(payloads_by_stream[item.stream_id]).hexdigest(),
                expected_bytes=len(payloads_by_stream[item.stream_id]),
            )
            for item in request.objects
        ),
    ), payloads_by_stream


def _original_io(monkeypatch, case, request, payloads_by_stream):
    sealed_captures = tuple(
        capture_stream(
            BytesIO(payloads_by_stream[stream.stream_id]), stream, source_snapshot_token=request.source_snapshot_token
        )
        for stream in request.definition.source_streams
    )
    register = AsyncMock(return_value=SimpleNamespace(capture_bundle_id=23))
    load = AsyncMock(return_value=sealed_captures)
    monkeypatch.setattr(https_candidate, "register_raw_capture_bundle", register)
    monkeypatch.setattr(https_candidate, "load_replayable_raw_bundle", load)
    case.acquire.side_effect = lambda _url, stream, **_kwargs: next(
        sealed for sealed in sealed_captures if sealed.manifest.stream_id == stream.stream_id
    )
    return register, load, sealed_captures


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("format_name", "compression"),
    [
        (format_name, compression)
        for format_name in ("csv", "tsv", "json", "ndjson", "xml", "parquet")
        for compression in ("none", "gzip")
        if (format_name, compression) != ("parquet", "none")
    ],
)
async def test_originals_bind_reload_and_reach_existing_family_runner(
    coordinator, monkeypatch, format_name, compression
):
    request, payloads = _original_request(format_name, compression)
    register, load, sealed = _original_io(monkeypatch, coordinator, request, payloads)
    result = await run_https_candidate(coordinator.session_factory, request)
    assert result.status == "no_change"
    assert register.call_args.kwargs["captures"] == sealed
    assert register.call_args.kwargs["maximum_dataset_retained_bytes"] == request.maximum_dataset_retained_bytes
    registered_session = register.call_args.args[0]
    bound_session = coordinator.mocks_by_name["bind_execution_capture_bundle"].call_args.args[0]
    assert registered_session is bound_session and registered_session.outcome == "commit"
    assert load.call_args.args[0] is not bound_session and load.call_args.args[0].outcome == "commit"
    assert load.call_args.kwargs["limits"] == request.limits
    candidate = coordinator.mocks_by_name["run_claimed_candidate"].call_args.args[1]
    admitted = coordinator.mocks_by_name["run_claimed_candidate"].call_args.args[3]
    assert candidate.roots[0]["display_name"] == "Synthetic Provider"
    assert candidate.children_by_collection["rates"][0]["amount"] == "12.50"
    assert len(admitted.families) == 1 and not admitted.rejections
    coordinator.mocks_by_name["register_replayable_parquet_bundle"].assert_not_awaited()
    coordinator.mocks_by_name["load_replayable_parquet_bundle"].assert_not_awaited()


@pytest.mark.asyncio
async def test_mixed_bundle_keeps_parquet_in_the_originals_transaction(coordinator, monkeypatch):
    request, payloads = _original_request("ndjson", "none", mixed=True)
    register, _load, sealed = _original_io(monkeypatch, coordinator, request, payloads)
    assert (await run_https_candidate(coordinator.session_factory, request)).status == "no_change"
    assert register.call_args.kwargs["captures"] == sealed
    assert tuple(capture.manifest.format for capture in sealed) == ("parquet", "ndjson")


@pytest.mark.asyncio
@pytest.mark.parametrize("compression", ("none", "gzip"))
async def test_empty_ndjson_streams_are_retained_not_treated_as_missing(coordinator, monkeypatch, compression):
    request, payloads = _original_request("ndjson", compression, empty=True)
    register, _load, sealed = _original_io(monkeypatch, coordinator, request, payloads)
    assert (await run_https_candidate(coordinator.session_factory, request)).status == "no_change"
    assert register.call_args.kwargs["captures"] == sealed
    admitted = coordinator.mocks_by_name["run_claimed_candidate"].call_args.args[3]
    assert not admitted.families and not admitted.rejections


@pytest.mark.asyncio
async def test_bound_original_resume_never_refetches_or_recharges(coordinator, monkeypatch):
    request, payloads = _original_request("csv", "gzip")
    register, load, _sealed = _original_io(monkeypatch, coordinator, request, payloads)
    result = await run_https_candidate(coordinator.session_factory, request, resume=True)
    assert result.status == "no_change"
    register.assert_not_awaited()
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["bind_execution_capture_bundle"].assert_not_awaited()
    assert load.call_args.kwargs["capture_bundle_id"] == 23
    assert coordinator.mocks_by_name["run_claimed_candidate"].call_args.args[2].fence == 2


@pytest.mark.asyncio
async def test_binding_failure_rolls_back_original_registration(coordinator, monkeypatch):
    request, payloads = _original_request("csv", "gzip")
    register, load, _sealed = _original_io(monkeypatch, coordinator, request, payloads)
    coordinator.mocks_by_name["bind_execution_capture_bundle"].return_value = None
    result = await run_https_candidate(coordinator.session_factory, request)
    assert result.status == "lease_lost"
    assert register.call_args.args[0].outcome == "rollback"
    load.assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cap", (None, True, -1, 1.5, 2**63))
async def test_original_cap_is_validated_before_database_or_network(coordinator, cap):
    request, _payloads = _original_request("csv", "none")
    with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
        await run_https_candidate(coordinator.session_factory, replace(request, maximum_dataset_retained_bytes=cap))
    assert not coordinator.sessions
    coordinator.acquire.assert_not_awaited()


def test_cap_changes_raw_identity_but_none_keeps_legacy_identity():
    raw, _payloads = _original_request("csv", "none")
    assert https_request_identity_sha256(raw) != https_request_identity_sha256(
        replace(raw, maximum_dataset_retained_bytes=raw.maximum_dataset_retained_bytes + 1)
    )
    legacy, _payloads = _request()
    assert https_request_identity_sha256(legacy) == https_request_identity_sha256(
        replace(legacy, maximum_dataset_retained_bytes=None)
    )
    with pytest.raises(HttpsCandidateError, match="^https_request_invalid$"):
        https_request_identity_sha256(replace(legacy, maximum_dataset_retained_bytes=1024 * 1024))


def test_uncompressed_parquet_request_preserves_the_legacy_digest():
    legacy, _payloads = _request()
    legacy = replace(
        legacy,
        objects=tuple(replace(item, expected_sha256="0" * 64, expected_bytes=32) for item in legacy.objects),
    )
    assert https_request_identity_sha256(legacy).hex() == (
        "2002fb881d8a97beab9e9d25348a852c729160932e458d553867aba43ab294d2"
    )


@pytest.mark.asyncio
async def test_original_record_budget_is_shared_by_all_streams(coordinator, monkeypatch):
    request, payloads = _original_request("ndjson", "gzip")
    request = replace(request, limits=replace(request.limits, maximum_records=1))
    _original_io(monkeypatch, coordinator, request, payloads)
    with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
        await run_https_candidate(coordinator.session_factory, request)
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()


@pytest.mark.asyncio
async def test_original_aliases_are_applied_before_family_admission(coordinator, monkeypatch):
    request, payloads = _original_request("json", "none")
    document = json.loads(request.definition.canonical)
    document["aliases"] = _raw_definition()["aliases"]
    for stream_id, aliases in document["aliases"].items():
        original_record = json.loads(payloads[stream_id])[0]
        payloads[stream_id] = json.dumps(
            [{source: original_record[field] for source, field in aliases.items()}]
        ).encode()
    request = replace(
        request,
        definition=CustomImportDefinition.from_mapping(document),
        objects=tuple(
            replace(
                item,
                expected_sha256=hashlib.sha256(payloads[item.stream_id]).hexdigest(),
                expected_bytes=len(payloads[item.stream_id]),
            )
            for item in request.objects
        ),
    )
    _original_io(monkeypatch, coordinator, request, payloads)
    assert (await run_https_candidate(coordinator.session_factory, request)).status == "no_change"
    candidate = coordinator.mocks_by_name["run_claimed_candidate"].call_args.args[1]
    assert candidate.roots[0] == {"npi": "1234567893", "display_name": "Synthetic Provider"}
    assert candidate.children_by_collection["rates"][0]["amount"] == "12.50"


@pytest.mark.asyncio
async def test_replayed_original_snapshot_mismatch_cannot_reach_runner(coordinator, monkeypatch):
    request, payloads = _original_request("ndjson", "gzip")
    _register, load, retained = _original_io(monkeypatch, coordinator, request, payloads)
    other_snapshot = capture_stream(
        BytesIO(payloads["rates"]), request.definition.source_streams[1], source_snapshot_token="other-snapshot"
    )
    load.return_value = (retained[0], other_snapshot)
    with pytest.raises(HttpsCandidateError, match="^https_candidate_failed$"):
        await run_https_candidate(coordinator.session_factory, request, resume=True)
    coordinator.acquire.assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
