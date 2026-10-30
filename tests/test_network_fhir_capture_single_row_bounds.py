# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline native-bound parity and COPY protocol tests; no Source/PG authority claim."""

import asyncio
import hashlib
import importlib
import json
import re
import struct
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_retained_capture as capture
from process.network_bootstrap_sources import _CopyBatchTooLarge
from process.network_fhir_retained_capture_contract import TABLES, RetainedFHIRCaptureError
from process.provider_directory_resource_hash import legacy_resource_payload_sha256
from tests.test_network_fhir_retained_capture import retained

_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _row(identity="site-one", kind="Location", **fields):
    payload = {"source_id": "source-one", "resource_id": identity, **fields}
    return {
        "dataset_id": "dataset-one",
        "resource_type": kind,
        "resource_id": identity,
        "payload_json": json.dumps(payload, separators=(",", ":")),
        "payload_hash": legacy_resource_payload_sha256(payload),
        "acquired_resource_sha256": None,
    }


def _origin(rows):
    origin = retained().origin
    roots_by_kind = {kind: hashlib.sha256() for kind in sorted({row["resource_type"] for row in rows})}
    counts, root = dict.fromkeys(roots_by_kind, 0), hashlib.sha256()
    capture._fold_rows(rows, origin, roots_by_kind, counts, root, 0)
    return replace(
        origin,
        dataset_sha256=root.hexdigest(),
        resource_counts=tuple(sorted(counts.items())),
        resource_hashes=tuple((kind, value.hexdigest()) for kind, value in roots_by_kind.items()),
        descriptor_counts=tuple((kind, count + 1) for kind, count in counts.items()),
    )


class _Rows:
    def __init__(self, rows, *, raw_bytes=None):
        self.rows, self.raw_bytes, self.fetches, self.pages = rows, raw_bytes, 0, []

    def selected(self, query, arguments):
        limit = int(re.findall(r"LIMIT (\d+)", query)[-1])
        after = tuple(arguments[1:]) if len(arguments) > 1 else None
        return [row for row in self.rows if after is None or (row["resource_type"], row["resource_id"]) > after][:limit]

    async def fetchrow(self, query, *arguments):
        rows = self.selected(query, arguments)
        limit = int(re.findall(r"LIMIT (\d+)", query)[-1])
        self.pages.append((tuple(arguments[1:]) or None, limit))
        return {
            "count": len(rows),
            "bytes": self.raw_bytes
            if self.raw_bytes is not None
            else sum(len(row["payload_json"].encode()) + 256 for row in rows),
        }

    async def fetch(self, query, *arguments):
        self.fetches += 1
        return self.selected(query, arguments)


class _Writer:
    def __init__(self):
        self.events, self.batches, self.raw = [], [], []
        self.row_hash = hashlib.sha256()
        self.pause, self.release, self.started = False, asyncio.Event(), asyncio.Event()
        self.accounting = True

    async def fetchval(self, _query):
        return _AT

    async def copy_records_to_table(self, table, *, schema_name, records):
        self.events.append("copy-start")
        self.started.set()
        if self.pause:
            await self.release.wait()
        size = 0
        for record in records:
            encoded = capture.canonical([str(value) for value in record]).encode()
            size += len(encoded)
            self.row_hash.update(encoded + b"\n")
        self.batches.append((table, len(records), size))
        self.events.append("copy-finish")
        return f"COPY {len(records) if self.accounting else 0}"

    async def copy_to_table(self, table, *, schema_name, source, **copy_options_by_name):
        data, offset, count = bytes(source), 19, 0
        assert copy_options_by_name == {"format": "binary"} and data.startswith(b"PGCOPY\n\xff\r\n\0")
        while struct.unpack_from("!h", data, offset)[0] != -1:
            columns = struct.unpack_from("!h", data, offset)[0]
            offset += 2
            for _ in range(columns):
                size = struct.unpack_from("!i", data, offset)[0]
                offset += 4 + max(size, 0)
            count += 1
        assert offset + 2 == len(data)
        return f"COPY {count}"

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            self.raw.clear()
            self.batches.clear()
            raise


def _pair(rows, *, raw_bytes=None):
    writer = _Writer()
    return SimpleNamespace(reader=_Rows(rows, raw_bytes=raw_bytes), writer=writer, verify=AsyncMock())


def _raw_copy(monkeypatch):
    async def copy(pair, query, arguments, schema, table, count):
        assert table == TABLES[1]
        rows = pair.reader.selected(query, arguments)
        assert len(rows) == count
        pair.writer.raw.extend((row["resource_type"], row["resource_id"]) for row in rows)

    operation = AsyncMock(side_effect=copy)
    monkeypatch.setattr(capture, "_copy_native_batch", operation)
    return operation


@pytest.mark.parametrize("bound", ["collection_limit", "output_limit"])
async def test_real_compiled_limit_preserves_one_reference_valid_streamed_resource(monkeypatch, bound):
    extension = importlib.import_module("ptg2_address_canon")
    encoder = extension.encode_retained_fhir_capture_batch
    failures = []

    def observed_encoder(document):
        try:
            return encoder(document)
        except ValueError as error:
            failures.append(json.loads(str(error))["code"])
            raise

    monkeypatch.setattr(extension, "encode_retained_fhir_capture_batch", observed_encoder)
    resource_row = (
        _row(extra=[0] * 1_000_001)
        if bound == "collection_limit"
        else _row(
            "plan-one",
            "InsurancePlan",
            name="x" * 700_000,
            network_refs=[f"Organization/network-{index}" for index in range(100)],
        )
    )
    resource_rows = [resource_row]
    if bound == "output_limit":
        resource_rows.extend(_row(f"network-{index}", "Organization") for index in range(100))
    resource_rows.sort(key=lambda resource: (resource["resource_type"], resource["resource_id"]))
    origin = _origin(resource_rows)
    assert len(resource_row["payload_json"].encode()) + 277 < 64 * 1024**2
    # This is the unchanged original validator and individually bounded streaming derivation.
    reference = _Writer()
    capture._validate_reference_rows(resource_rows, origin)
    await capture._derived(reference, "candidate_example", origin, "release-one", [resource_row], _AT)
    assert reference.batches and all(size <= 64 * 1024**2 for _table, _count, size in reference.batches)
    if bound == "output_limit":
        assert sum(size for _table, _count, size in reference.batches) > 64 * 1024**2
        assert len(reference.batches) > 1
    raw = _raw_copy(monkeypatch)
    pair = _pair(resource_rows)
    await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    assert failures and set(failures) == {bound} and raw.await_count == len(resource_rows)
    assert pair.writer.raw == [(resource["resource_type"], resource["resource_id"]) for resource in resource_rows]
    assert pair.writer.batches == reference.batches and pair.writer.row_hash.digest() == reference.row_hash.digest()
    accepted_cursors = [after for after, _limit in pair.reader.pages if after is not None]
    assert accepted_cursors == [(resource["resource_type"], resource["resource_id"]) for resource in resource_rows]
    prior_failure_codes = list(failures)
    await capture._verify_resources(_Rows(resource_rows), "candidate_example", origin)
    assert failures == (prior_failure_codes + [bound] if bound == "collection_limit" else prior_failure_codes)


async def test_native_bound_validates_before_copy_and_folds_after_outputs(monkeypatch):
    rows = [_row()]
    origin, pair = _origin(rows), _pair(rows)
    monkeypatch.setattr(
        capture, "_prepare_native_resources", lambda *_args: (_ for _ in ()).throw(_CopyBatchTooLarge())
    )
    raw = _raw_copy(monkeypatch)
    original_fold, folds = capture._fold_rows, []

    def fold(resource_rows, descriptor, roots, counts, root, total):
        folds.append((raw.await_count, len(pair.writer.batches)))
        return original_fold(resource_rows, descriptor, roots, counts, root, total)

    monkeypatch.setattr(capture, "_fold_rows", fold)
    await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    assert folds == [(0, 0), (1, 2)]
    assert len(pair.reader.pages) == 2


async def test_multirow_preparation_bounds_shrink_without_reference_fallback(monkeypatch):
    rows = [_row("site-one"), _row("site-two")]
    origin, pair = _origin(rows), _pair(rows)
    prepare, attempts = capture._prepare_native_resources, []

    def bounded(resource_rows, *arguments):
        attempts.append(len(resource_rows))
        if len(resource_rows) > 1:
            raise _CopyBatchTooLarge()
        return prepare(resource_rows, *arguments)

    monkeypatch.setattr(capture, "_prepare_native_resources", bounded)
    derived = AsyncMock(side_effect=AssertionError("multirow page used reference fallback"))
    monkeypatch.setattr(capture, "_derived", derived)
    raw = _raw_copy(monkeypatch)
    await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    assert attempts == [2, 1, 1] and raw.await_count == 2
    assert pair.reader.pages == [(None, 1000), (None, 1), (("Location", "site-one"), 1), (("Location", "site-two"), 1)]
    derived.assert_not_awaited()
    await capture._verify_resources(_Rows(rows), "candidate_example", origin)
    assert attempts == [2, 1, 1, 2, 1, 1]


async def test_real_collection_bound_does_not_hide_invalid_original_payload_hash(monkeypatch):
    rows = [_row(extra=[0] * 1_000_001)]
    origin, pair = _origin(rows), _pair(rows)
    rows[0]["payload_hash"] = "0" * 64
    raw = _raw_copy(monkeypatch)
    with pytest.raises(RetainedFHIRCaptureError):
        await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    raw.assert_not_awaited()
    assert pair.reader.pages == [(None, 1000)]


async def test_native_accounting_failure_is_not_a_preparation_bound(monkeypatch):
    rows = [_row()]
    origin, pair = _origin(rows), _pair(rows)
    encoder = capture._native_payload_encoder

    def invalid_accounting(document):
        result = encoder(document)
        return (*result[:-1], result[-1] + 1)

    monkeypatch.setattr(capture, "_native_payload_encoder", invalid_accounting)
    raw = _raw_copy(monkeypatch)
    with pytest.raises(RetainedFHIRCaptureError):
        await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    raw.assert_not_awaited()


async def test_single_raw_copy_overflow_never_becomes_reference_fallback(monkeypatch):
    rows, pair = [_row()], _pair([_row()])
    origin = _origin(rows)
    monkeypatch.setattr(
        capture, "_prepare_native_resources", lambda *_args: (_ for _ in ()).throw(_CopyBatchTooLarge())
    )
    raw = AsyncMock(side_effect=_CopyBatchTooLarge())
    derived = AsyncMock()
    monkeypatch.setattr(capture, "_copy_native_batch", raw)
    monkeypatch.setattr(capture, "_derived", derived)
    with pytest.raises(RetainedFHIRCaptureError):
        await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    raw.assert_awaited_once()
    derived.assert_not_awaited()
    assert pair.reader.pages == [(None, 1000)]


async def test_actual_raw_spool_limit_refuses_single_row_and_closes_spool(monkeypatch):
    from process import network_bootstrap_sources as bootstrap

    resource_rows, pair = [_row()], _pair([_row()])
    origin = _origin(resource_rows)
    monkeypatch.setattr(
        capture, "_prepare_native_resources", lambda *_args: (_ for _ in ()).throw(_CopyBatchTooLarge())
    )
    spools, temporary_file = [], bootstrap.tempfile.TemporaryFile

    def track_spool(*args, **kwargs):
        spool = temporary_file(*args, **kwargs)
        spools.append(spool)
        return spool

    monkeypatch.setattr(bootstrap.tempfile, "TemporaryFile", track_spool)
    exported = SimpleNamespace(bytes=0)

    async def export(_query, *_arguments, output, **copy_options_by_name):
        assert copy_options_by_name == {"format": "binary"}
        for _ in range(65):
            chunk = b"x" * 1024**2
            exported.bytes += len(chunk)
            await output(chunk)
        return "COPY 1"

    imported = AsyncMock(side_effect=AssertionError("oversized raw row was imported"))
    pair.transaction, pair.copy_from_query, pair.copy_to_table = pair.writer.transaction, export, imported
    derived = AsyncMock()
    monkeypatch.setattr(capture, "_derived", derived)
    with pytest.raises(RetainedFHIRCaptureError):
        await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    assert exported.bytes == 65 * 1024**2 and all(spool.closed for spool in spools) and len(spools) == 1
    assert pair.writer.events == ["rollback"]
    imported.assert_not_awaited()
    derived.assert_not_awaited()


async def test_multirow_raw_bound_shrinks_without_reinterpreting_it_as_preparation_fallback(monkeypatch):
    rows = [_row("site-one"), _row("site-two")]
    pair, origin = _pair(rows), _origin(rows)
    attempts = []

    async def raw(actual, query, arguments, _schema, _table, count):
        attempts.append(count)
        if count > 1:
            raise _CopyBatchTooLarge()
        actual.writer.raw.extend(
            (row["resource_type"], row["resource_id"]) for row in actual.reader.selected(query, arguments)
        )

    monkeypatch.setattr(capture, "_copy_native_batch", raw)
    derived = AsyncMock(side_effect=AssertionError("raw bound entered reference fallback"))
    monkeypatch.setattr(capture, "_derived", derived)
    await capture._copy_resources(pair, origin, "candidate_example", "release-one")
    assert attempts == [2, 1, 1] and pair.writer.raw == [("Location", "site-one"), ("Location", "site-two")]
    derived.assert_not_awaited()


async def test_raw_preflight_remains_first_and_refuses_oversized_single_before_fetch(monkeypatch):
    rows = [_row()]
    pair = _pair(rows, raw_bytes=64 * 1024**2)
    prepare, raw = AsyncMock(), _raw_copy(monkeypatch)
    monkeypatch.setattr(capture, "_prepare_native_resources", prepare)
    with pytest.raises(RetainedFHIRCaptureError):
        await capture._copy_resources(pair, _origin(rows), "candidate_example", "release-one")
    assert pair.reader.fetches == 0 and pair.reader.pages[-1] == (None, 1)
    prepare.assert_not_called()
    raw.assert_not_awaited()


@pytest.mark.parametrize(
    "failure", [RetainedFHIRCaptureError("semantic"), ValueError("format"), RuntimeError("authority")]
)
async def test_nonbound_native_failures_are_terminal_before_copy(monkeypatch, failure):
    rows, pair = [_row()], _pair([_row()])
    monkeypatch.setattr(capture, "_prepare_native_resources", lambda *_args: (_ for _ in ()).throw(failure))
    raw = _raw_copy(monkeypatch)
    with pytest.raises(type(failure)):
        await capture._copy_resources(pair, _origin(rows), "candidate_example", "release-one")
    raw.assert_not_awaited()


@pytest.mark.parametrize("fault", ["cancel", "accounting"])
async def test_fallback_copy_drains_before_caller_rollback_and_never_advances_row(monkeypatch, fault):
    rows, pair = [_row()], _pair([_row()])
    origin = _origin(rows)
    monkeypatch.setattr(
        capture, "_prepare_native_resources", lambda *_args: (_ for _ in ()).throw(_CopyBatchTooLarge())
    )
    _raw_copy(monkeypatch)
    pair.writer.pause = fault == "cancel"
    pair.writer.accounting = fault != "accounting"

    async def operation():
        async with pair.writer.transaction():
            await capture._copy_resources(pair, origin, "candidate_example", "release-one")

    task = asyncio.create_task(operation())
    if fault == "cancel":
        await pair.writer.started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and pair.writer.events == ["copy-start"]
        pair.writer.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(RetainedFHIRCaptureError):
            await task
    assert pair.writer.events == ["copy-start", "copy-finish", "rollback"]
    assert pair.writer.raw == [] and pair.writer.batches == []
    assert pair.reader.pages == [(None, 1000)]
