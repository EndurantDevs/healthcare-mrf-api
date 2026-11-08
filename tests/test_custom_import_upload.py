# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic staged-upload identity, mutation, resource and cancellation checks."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import os
import stat
import time
import traceback
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import upload
from process.custom_import.capture import iter_records, verify_capture
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import SourceStream

_PAYLOAD = b"id,name\n1,synthetic\n"
_STREAM = SourceStream("records", "root", None, "csv", "none", None, "declared-snapshot")
_LIMITS = CaptureLimits(
    maximum_compressed_bytes=1024,
    maximum_decoded_bytes=4096,
    maximum_record_bytes=512,
    read_chunk_bytes=4,
)


@pytest.fixture
def descriptors(monkeypatch):
    active_descriptors = set()
    opened_descriptors = []
    native_open, native_close = os.open, os.close

    def open_descriptor(*arguments, **keywords):
        descriptor = native_open(*arguments, **keywords)
        active_descriptors.add(descriptor)
        opened_descriptors.append((descriptor, arguments, keywords))
        return descriptor

    def close_descriptor(descriptor):
        active_descriptors.discard(descriptor)
        native_close(descriptor)

    monkeypatch.setattr(upload.os, "open", open_descriptor)
    monkeypatch.setattr(upload.os, "close", close_descriptor)
    yield SimpleNamespace(active=active_descriptors, opened=opened_descriptors)
    assert not active_descriptors


@pytest.fixture
def staged(tmp_path, descriptors):
    root = tmp_path / "staging"
    root.mkdir()
    path = root / "object_one"
    path.write_bytes(_PAYLOAD)
    (root / "unrelated").write_bytes(b"reusable-input")
    return SimpleNamespace(
        root=root,
        path=path,
        provider=upload.StagedUploadProvider(root),
        source=_source(_PAYLOAD),
        descriptors=descriptors,
    )


def _source(payload, **changes):
    return upload.UploadObject(
        **(
            dict(
                stream_id="records",
                object_id="object_one",
                expected_sha256=hashlib.sha256(payload).hexdigest(),
                expected_bytes=len(payload),
            )
            | changes
        )
    )


async def _acquire(staged, **changes):
    arguments_by_name = (
        dict(
            source_snapshot_token="synthetic-snapshot",
            provider=staged.provider,
            limits=_LIMITS,
            timeout_seconds=1,
        )
        | changes
    )
    return await upload.acquire_upload(staged.source, _STREAM, **arguments_by_name)


def _format_payload(format_name):
    if format_name == "parquet":
        output = BytesIO()
        pq.write_table(pa.table({"id": [1]}), output)
        return output.getvalue()
    return {
        "csv": _PAYLOAD,
        "tsv": b"id\tname\n1\tsynthetic\n",
        "json": b'[{"id":1}]',
        "ndjson": b'{"id":1}\n',
        "xml": b"<records><record><id>1</id></record></records>",
    }[format_name]


@pytest.mark.parametrize("format_name", ["csv", "tsv", "json", "ndjson", "xml", "parquet"])
@pytest.mark.parametrize("compression", ["none", "gzip"])
async def test_upload_retains_exact_original_and_uses_existing_decoders(staged, format_name, compression):
    payload = _format_payload(format_name)
    original = gzip.compress(payload, mtime=0) if compression == "gzip" else payload
    limits = replace(_LIMITS, maximum_decoded_bytes=128 * 1024) if format_name == "parquet" else _LIMITS
    staged.path.write_bytes(original)
    source = _source(original)
    stream = replace(
        _STREAM, format=format_name, compression=compression, record_path="record" if format_name == "xml" else None
    )
    capture = await upload.acquire_upload(
        source,
        stream,
        source_snapshot_token="synthetic-snapshot",
        provider=staged.provider,
        limits=limits,
        timeout_seconds=1,
    )
    assert capture.payload == original
    assert capture.manifest.compressed_sha256 == source.expected_sha256
    verify_capture(capture, stream, limits=limits)
    assert len(tuple(iter_records(capture, stream, limits=limits))) == 1
    assert not staged.descriptors.active
    assert staged.path.read_bytes() == original
    assert (staged.root / "unrelated").read_bytes() == b"reusable-input"
    assert any(arguments[1] & os.O_NOFOLLOW for _fd, arguments, _keywords in staged.descriptors.opened)
    file_opens = [entry for entry in staged.descriptors.opened if "dir_fd" in entry[2]]
    assert len(file_opens) == 1 and file_opens[0][1][0] == source.object_id
    assert file_opens[0][1][1] & os.O_NONBLOCK


async def test_empty_ndjson_original_is_valid(staged):
    staged.path.write_bytes(b"")
    capture = await upload.acquire_upload(
        _source(b""),
        replace(_STREAM, format="ndjson"),
        source_snapshot_token="synthetic-snapshot",
        provider=staged.provider,
        limits=_LIMITS,
        timeout_seconds=1,
    )
    assert capture.payload == b"" and capture.manifest.compressed_bytes == 0


def test_logical_identity_has_no_physical_location_and_survives_restaging(staged, tmp_path):
    other = tmp_path / "other-staging"
    other.mkdir()
    (other / "object_one").write_bytes(_PAYLOAD)
    second_provider = upload.StagedUploadProvider(other)
    descriptor_by_name = upload.upload_object_identity(staged.source)
    assert descriptor_by_name == dict(
        stream_id="records",
        object_id="object_one",
        expected_sha256=hashlib.sha256(_PAYLOAD).hexdigest(),
        expected_bytes=len(_PAYLOAD),
    )
    assert descriptor_by_name == upload.upload_object_identity(_source(_PAYLOAD))
    assert str(staged.root) not in repr(staged.provider) and str(other) not in repr(second_provider)
    assert descriptor_by_name != upload.upload_object_identity(_source(_PAYLOAD, object_id="object_two"))


@pytest.mark.parametrize(
    "object_id",
    [
        "",
        "..",
        "../object",
        "one/two",
        "one\\two",
        "./one",
        ".hidden",
        "one.csv",
        "$ROOT",
        "${ROOT}",
        "UPPER",
        "a" * 129,
    ],
)
def test_invalid_logical_ids_fail_without_opening_anything(staged, object_id):
    with pytest.raises(upload.UploadAcquisitionError, match="upload_request_invalid"):
        _source(_PAYLOAD, object_id=object_id)
    assert staged.descriptors.opened == []


@pytest.mark.parametrize(
    "changes_by_name",
    [
        {"expected_bytes": True},
        {"expected_bytes": -1},
        {"expected_bytes": 64 * 1024 * 1024 + 1},
        {"expected_bytes": 1.0},
        {"expected_sha256": "A" * 64},
        {"expected_sha256": "short"},
        {"stream_id": "Wrong-Stream"},
    ],
)
def test_invalid_content_descriptors_fail_without_opening_anything(staged, changes_by_name):
    with pytest.raises(upload.UploadAcquisitionError, match="upload_request_invalid"):
        _source(_PAYLOAD, **changes_by_name)
    assert staged.descriptors.opened == []


@pytest.mark.parametrize(
    "changes_by_name",
    [
        {"timeout_seconds": True},
        {"timeout_seconds": 0},
        {"timeout_seconds": -1},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": float("inf")},
        {"limits": None},
        {"limits": replace(_LIMITS, maximum_compressed_bytes=65 * 1024 * 1024)},
        {"limits": replace(_LIMITS, maximum_compressed_bytes=1)},
        {"provider": object()},
        {"source_snapshot_token": "bad\nvalue"},
    ],
)
async def test_invalid_acquisition_bounds_fail_before_open(staged, changes_by_name):
    with pytest.raises(upload.UploadAcquisitionError):
        await _acquire(staged, **changes_by_name)
    assert staged.descriptors.opened == []


async def test_mismatched_stream_and_untyped_source_fail_before_open(staged):
    for source, stream in ((staged.source, replace(_STREAM, stream_id="other")), ({"path": "arbitrary"}, _STREAM)):
        with pytest.raises(upload.UploadAcquisitionError):
            await upload.acquire_upload(
                source,
                stream,
                source_snapshot_token="synthetic-snapshot",
                provider=staged.provider,
                limits=_LIMITS,
                timeout_seconds=1,
            )
    assert staged.descriptors.opened == []


@pytest.mark.parametrize("kind", ["symlink", "directory", "fifo"])
async def test_nonregular_logical_objects_are_denied_before_file_open(staged, tmp_path, kind):
    staged.path.unlink()
    if kind == "symlink":
        target = tmp_path / "outside-original"
        target.write_bytes(_PAYLOAD)
        staged.path.symlink_to(target)
    elif kind == "directory":
        staged.path.mkdir()
    else:
        os.mkfifo(staged.path)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_source_denied"):
        await asyncio.wait_for(_acquire(staged), 0.5)
    assert not any("dir_fd" in keywords for _fd, _arguments, keywords in staged.descriptors.opened)
    assert staged.path.exists()


async def test_device_entry_is_denied_before_open(staged, monkeypatch):
    native_stat = os.stat

    def device_stat(path, **keywords):
        if keywords.get("dir_fd") is not None:
            return SimpleNamespace(st_mode=stat.S_IFCHR | 0o600)
        return native_stat(path, **keywords)

    monkeypatch.setattr(upload.os, "stat", device_stat)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_source_denied"):
        await _acquire(staged)
    assert not any("dir_fd" in keywords for _fd, _arguments, keywords in staged.descriptors.opened)


def test_provider_rejects_symlink_and_nonabsolute_roots(staged, tmp_path):
    link = tmp_path / "linked-staging"
    link.symlink_to(staged.root, target_is_directory=True)
    for root in (link, str(link) + "/", "relative-staging", "$STAGING_ROOT"):
        with pytest.raises(upload.UploadAcquisitionError, match="upload_provider_invalid"):
            upload.StagedUploadProvider(root)
    assert staged.descriptors.opened == []


async def test_provider_rejects_directory_replacement_after_composition(staged, tmp_path):
    original_root = tmp_path / "original-staging"
    staged.root.rename(original_root)
    staged.root.mkdir()
    staged.path.write_bytes(_PAYLOAD)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_source_changed"):
        await _acquire(staged)
    assert (original_root / "object_one").read_bytes() == _PAYLOAD


async def test_directory_replacement_during_read_is_denied(staged, monkeypatch, tmp_path):
    native_read = os.read
    original_root = tmp_path / "original-staging"
    state = SimpleNamespace(has_changed=False)

    def replace_root(descriptor, byte_count):
        chunk = native_read(descriptor, byte_count)
        if not state.has_changed:
            state.has_changed = True
            staged.root.rename(original_root)
            staged.root.mkdir()
        return chunk

    monkeypatch.setattr(upload.os, "read", replace_root)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_source_changed"):
        await _acquire(staged)
    assert (original_root / "object_one").read_bytes() == _PAYLOAD
    assert (original_root / "unrelated").read_bytes() == b"reusable-input"


async def test_unrelated_staging_activity_does_not_change_directory_identity(staged, monkeypatch):
    native_read = os.read

    def add_unrelated_input(descriptor, byte_count):
        chunk = native_read(descriptor, byte_count)
        (staged.root / "another_original").write_bytes(b"another-reusable-input")
        return chunk

    monkeypatch.setattr(upload.os, "read", add_unrelated_input)
    assert (await _acquire(staged)).payload == _PAYLOAD
    assert (staged.root / "another_original").read_bytes() == b"another-reusable-input"


async def test_missing_logical_object_closes_directory_and_preserves_other_inputs(staged):
    staged.path.unlink()
    with pytest.raises(upload.UploadAcquisitionError, match="upload_acquisition_failed"):
        await _acquire(staged)
    assert (staged.root / "unrelated").read_bytes() == b"reusable-input"


@pytest.mark.parametrize("mutation", ["grow", "truncate", "replace", "in_place", "restore_mtime", "symlink"])
async def test_acquisition_rejects_changes_during_read(staged, monkeypatch, mutation):
    native_read = os.read
    before = staged.path.stat()
    state = SimpleNamespace(has_mutated=False)

    def read_and_mutate(descriptor, byte_count):
        chunk = native_read(descriptor, byte_count)
        if not state.has_mutated:
            state.has_mutated = True
            _mutate_staged_object(staged, mutation, before)
        return chunk

    monkeypatch.setattr(upload.os, "read", read_and_mutate)
    with pytest.raises(upload.UploadAcquisitionError):
        await _acquire(staged)
    assert not staged.descriptors.active
    assert (staged.root / "unrelated").read_bytes() == b"reusable-input"


def _mutate_staged_object(staged, mutation, before):
    if mutation == "grow":
        with staged.path.open("ab") as writable:
            writable.write(b"extra")
    elif mutation == "truncate":
        staged.path.write_bytes(b"")
    elif mutation in {"replace", "symlink"}:
        replacement = staged.root / "replacement"
        replacement.write_bytes(_PAYLOAD)
        if mutation == "replace":
            replacement.replace(staged.path)
        else:
            staged.path.unlink()
            staged.path.symlink_to(replacement)
    else:
        with staged.path.open("r+b") as writable:
            writable.write(b"X")
        if mutation == "restore_mtime":
            os.utime(staged.path, ns=(before.st_atime_ns, before.st_mtime_ns))


async def test_same_content_replacement_between_stat_and_open_is_denied(staged, monkeypatch):
    native_open = os.open

    def replace_before_open(path, flags, **keywords):
        if "dir_fd" in keywords:
            replacement = staged.root / "replacement"
            replacement.write_bytes(_PAYLOAD)
            replacement.replace(staged.path)
        return native_open(path, flags, **keywords)

    monkeypatch.setattr(upload.os, "open", replace_before_open)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_source_changed"):
        await _acquire(staged)


@pytest.mark.parametrize(
    "payload,limits",
    [
        (b"id\n" + b"x" * 100 + b"\n", replace(_LIMITS, maximum_record_bytes=16)),
        (b"id\n1\n2\n", replace(_LIMITS, maximum_records=1)),
        (b"a,b\n1,2\n", replace(_LIMITS, maximum_fields_per_record=1)),
    ],
)
async def test_existing_record_and_field_budgets_are_enforced(staged, payload, limits):
    staged.path.write_bytes(payload)
    staged.source = _source(payload)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_acquisition_failed"):
        await _acquire(staged, limits=limits)


async def test_existing_gzip_decoded_budget_is_enforced(staged):
    payload = gzip.compress(b"id\n" + b"x" * 100 + b"\n", mtime=0)
    staged.path.write_bytes(payload)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_acquisition_failed"):
        await upload.acquire_upload(
            _source(payload),
            replace(_STREAM, compression="gzip"),
            source_snapshot_token="synthetic-snapshot",
            provider=staged.provider,
            limits=replace(_LIMITS, maximum_decoded_bytes=64, maximum_record_bytes=16),
            timeout_seconds=1,
        )


async def test_actual_content_hash_and_length_are_verified(staged):
    for source in (_source(_PAYLOAD, expected_sha256="0" * 64), _source(_PAYLOAD, expected_bytes=len(_PAYLOAD) - 1)):
        staged.source = source
        with pytest.raises(upload.UploadAcquisitionError, match="upload_identity_mismatch"):
            await _acquire(staged)
    assert staged.path.read_bytes() == _PAYLOAD


async def test_deadline_after_bounded_read_closes_owned_handles(staged, monkeypatch):
    native_read = os.read

    def slow_read(descriptor, byte_count):
        chunk = native_read(descriptor, byte_count)
        time.sleep(0.02)
        return chunk

    monkeypatch.setattr(upload.os, "read", slow_read)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_acquisition_timeout"):
        await _acquire(staged, timeout_seconds=0.01)
    assert staged.descriptors.opened and not staged.descriptors.active
    assert staged.path.read_bytes() == _PAYLOAD


async def test_sealing_uses_the_same_deadline_and_closes_handles(staged, monkeypatch):
    native_capture = upload.capture_stream

    def slow_seal(*arguments, **keywords):
        sealed = native_capture(*arguments, **keywords)
        time.sleep(0.02)
        return sealed

    monkeypatch.setattr(upload, "capture_stream", slow_seal)
    with pytest.raises(upload.UploadAcquisitionError, match="upload_acquisition_timeout"):
        await _acquire(staged, timeout_seconds=0.01)
    assert staged.descriptors.opened and not staged.descriptors.active
    assert staged.path.read_bytes() == _PAYLOAD


async def test_cancellation_closes_handles_without_removing_original(staged, monkeypatch):
    started = asyncio.Event()

    async def paused_copy(*_arguments):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(upload, "_copy_upload", paused_copy)
    task = asyncio.create_task(_acquire(staged))
    await started.wait()
    assert len(staged.descriptors.active) == 2
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not staged.descriptors.active and staged.path.read_bytes() == _PAYLOAD


async def test_errors_redact_input_and_underlying_failure(staged, monkeypatch):
    marker = "synthetic-private-marker"

    def failed_read(*_arguments):
        raise OSError(f"{marker}: {staged.path}: {_PAYLOAD!r}")

    monkeypatch.setattr(upload.os, "read", failed_read)
    with pytest.raises(upload.UploadAcquisitionError) as failed:
        await _acquire(staged)
    formatted = "".join(traceback.format_exception(failed.value))
    assert str(failed.value) == "upload_acquisition_failed"
    assert marker not in formatted and str(staged.path) not in formatted and repr(_PAYLOAD) not in formatted
    assert failed.value.__suppress_context__
