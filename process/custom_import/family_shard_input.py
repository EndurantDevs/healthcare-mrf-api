# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Partition sealed capture occurrences once into bounded, owner-owned files.

This stage grants no database authority and performs no family admission. The
invalid-key lane must join final raw-key rejection accounting. Incomplete
partition work is discarded; only completed shards are eligible for replay.
"""

import asyncio
import hashlib
import json
import os
import shutil
import stat
import struct
import threading
import uuid
from collections import OrderedDict
from contextlib import aclosing
from dataclasses import asdict, dataclass, replace
from functools import partial
from pathlib import Path
from types import SimpleNamespace

from process.custom_import import family_shard_spool as spool
from process.custom_import.build_source import (
    _close_contexts,
    _close_iterator,
    _drain_cleanup,
    _enter_context,
    _source_cleanup_failure,
)
from process.custom_import.capture import SealedCapture, iter_records
from process.custom_import.capture_pending import (
    _ACCOUNTING_FIELDS,
    _bundle_manifest,
    _receipt_document,
    _ReplayAccounting,
    _validate_stream_receipt,
)
from process.custom_import.capture_store import CaptureReceipt, _iter_replayable_parquet_parts
from process.custom_import.definition import canonical_json
from process.custom_import.family_shard_input_records import (
    PartitionPreparation,
    decode_record,
    encode_record,
    partition_records,
    shard_for,
)
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from process.custom_import.snowflake_bundle_replay import (
    _aggregate_parquet_arrow_bytes,
    _stream_fields,
    _validate_replay_partition_schema,
)

_CONTRACT = "custom-import/family-shard-input/v1"
_STORAGE = "storage.json"
_STORAGE_CONTRACT = "custom-import/family-shard-input-storage/v1"


@dataclass(frozen=True)
class CaptureInputIdentity:
    """The caller's already authorized immutable dataset and sealed source scope."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    capture_bundle_id: int
    source_binding_revision_id: int | None
    definition_sha256: str
    schema_sha256: str
    source_binding_sha256: str | None
    manifest_sha256: str

    def __post_init__(self):
        if (self.source_binding_revision_id is None) != (self.source_binding_sha256 is None):
            raise ValueError("shard capture source binding evidence differs")
        for name, value in asdict(self).items():
            if name in ("source_binding_revision_id", "source_binding_sha256") and value is None:
                continue
            if name.endswith("_id"):
                if type(value) is not int or not 0 < value < 2**63:
                    raise ValueError("shard capture identity differs")
            elif type(value) is not str or len(value) != 64 or bytes.fromhex(value).hex() != value:
                raise ValueError("shard capture digest differs")


@dataclass(frozen=True)
class SealedCaptureInput:
    """Frozen owner-verified headers; parts still require actual verified EOF."""

    identity: CaptureInputIdentity
    source_snapshot_token: str
    source_request_sha256: bytes
    statement_sha256: bytes
    canonical_manifest: str
    policy: SegmentedCapturePolicy
    streams: tuple

    def __post_init__(self):
        if type(self.identity) is not CaptureInputIdentity or type(self.policy) is not SegmentedCapturePolicy:
            raise ValueError("shard capture scope is unsupported")
        if any(
            type(value) is not bytes or len(value) != 32
            for value in (self.source_request_sha256, self.statement_sha256)
        ):
            raise ValueError("shard capture source digest differs")
        if type(self.streams) is not tuple or any(
            type(entry) is not tuple
            or len(entry) != 2
            or type(entry[0]) is not int
            or not isinstance(entry[1], CaptureReceipt)
            for entry in self.streams
        ):
            raise ValueError("shard capture stream headers must be immutable")

    def headers(self, definition):
        """Verify sealed headers and reconstruct the existing replay accounting."""
        if (
            definition.digest != self.identity.definition_sha256
            or definition.schema_digest != self.identity.schema_sha256
        ):
            raise ValueError("shard capture definition identity differs")
        receipts_by_slot = dict(self.streams)
        if (
            tuple(receipts_by_slot) != tuple(range(1, len(definition.source_streams) + 1))
            or tuple(receipt.stream_id for receipt in receipts_by_slot.values())
            != tuple(stream.stream_id for stream in definition.source_streams)
            or len(receipts_by_slot) != len(self.streams)
        ):
            raise ValueError("shard capture stream identity differs")
        if hashlib.sha256(self.canonical_manifest.encode()).hexdigest() != self.identity.manifest_sha256:
            raise ValueError("shard capture bundle manifest differs")
        bundle = SimpleNamespace(
            snapshot_token=self.source_snapshot_token,
            source_request_sha256=self.source_request_sha256,
            statement_sha256=self.statement_sha256,
            source_binding_sha256=(
                None
                if self.identity.source_binding_sha256 is None
                else bytes.fromhex(self.identity.source_binding_sha256)
            ),
        )
        documents_by_slot = {
            slot: _receipt_document(receipt, bundle, self.policy) for slot, receipt in receipts_by_slot.items()
        }
        for slot, document in documents_by_slot.items():
            header = SimpleNamespace(**{"committed_" + name: document[name] for name in _ACCOUNTING_FIELDS})
            _validate_stream_receipt(receipts_by_slot[slot], header, bundle, self.policy)
        for field in _ACCOUNTING_FIELDS:
            setattr(bundle, "committed_" + field, sum(document[field] for document in documents_by_slot.values()))
        expected = _bundle_manifest(
            bundle,
            tuple((receipt.stream_id, slot) for slot, receipt in receipts_by_slot.items()),
            {receipt.stream_id: receipt for receipt in receipts_by_slot.values()},
            self.policy,
        )
        if expected != self.canonical_manifest:
            raise ValueError("shard capture complete bundle accounting differs")
        metadata = {
            slot: (document["part_count"], bytes.fromhex(document["payload_set_sha256"]))
            for slot, document in documents_by_slot.items()
        }
        accounting_by_slot = {
            slot: _ReplayAccounting(
                self.policy,
                {name: document[name] for name in _ACCOUNTING_FIELDS},
                bytes.fromhex(document["manifest_set_sha256"]),
                self.source_snapshot_token,
            )
            for slot, document in documents_by_slot.items()
        }
        return receipts_by_slot, metadata, accounting_by_slot


@dataclass(frozen=True)
class ShardInputLimits:
    """Aggregate stored-byte budget and logical chunk targets; no admission limit."""

    max_spool_bytes: int
    shard_count: int = 256
    chunk_bytes: int = 8 * 1024**2

    def __post_init__(self):
        for name, value in asdict(self).items():
            if type(value) is not int or value <= 0 or (name == "shard_count" and value > 4096):
                raise ValueError("shard spool limits differ")


@dataclass(frozen=True)
class PartitionedCapture:
    """Persist this exact completed reference outside the spool for shard replay."""

    root: Path
    sha256: str
    owner: str
    device: int
    inode: int
    identity: CaptureInputIdentity
    storage_sha256: str | None = None


def _json_bytes(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _open_owned(root, name, flags):
    if Path(name).name != name:
        raise ValueError("shard spool path is not owned")
    return os.open(root / name, flags | os.O_NOFOLLOW, 0o600)


def _root(partition):
    root = Path(partition.root)
    observed = root.lstat()
    if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (partition.device, partition.inode):
        raise ValueError("shard spool directory custody differs")
    with os.fdopen(_open_owned(root, "owner.json", os.O_RDONLY), "rb") as source:
        owner = json.load(source)
    if owner != {"contract": _CONTRACT, "owner": partition.owner}:
        raise ValueError("shard spool owner differs")
    return root


def manifest_document(partition):
    """Require exact owned files and the trusted completed manifest commitment."""
    root = _root(partition)
    with os.fdopen(_open_owned(root, "manifest.json", os.O_RDONLY), "rb") as source:
        payload = source.read()
    if hashlib.sha256(payload).hexdigest() != partition.sha256:
        raise ValueError("shard spool manifest digest differs")
    document = json.loads(payload)
    if (
        document["contract"] != _CONTRACT
        or document["owner"] != partition.owner
        or document["complete"] is not True
        or document["scope"]["identity"] != asdict(partition.identity)
    ):
        raise ValueError("shard spool manifest identity differs")
    expected_names = {"owner.json", "manifest.json", *(entry["name"] for entry in document["files"])}
    if partition.storage_sha256 is not None:
        _storage_files(partition, document)
        expected_names.add(_STORAGE)
    if {path.name for path in root.iterdir()} != expected_names:
        raise ValueError("shard spool file census differs")
    return document


def _storage_stamp(observed):
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o400
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
    ):
        raise ValueError("shard storage file owner, mode or link differs")
    return observed.st_dev, observed.st_ino, observed.st_mode, observed.st_uid, observed.st_size


def _storage_files(partition, document):
    """Authenticate stored identities separately from the unchanged logical manifest."""
    if partition.storage_sha256 is None:
        return {}
    path = Path(partition.root) / _STORAGE
    stamp = _storage_stamp(path.lstat())
    if stamp[-1] > 256 + 384 * len(document["files"]):
        raise ValueError("shard storage descriptor exceeds its bound")
    with os.fdopen(_open_owned(partition.root, _STORAGE, os.O_RDONLY), "rb") as descriptor_file:
        if _storage_stamp(os.fstat(descriptor_file.fileno())) != stamp:
            raise ValueError("shard storage descriptor was replaced")
        descriptor_bytes = descriptor_file.read(stamp[-1] + 1)
        if _storage_stamp(os.fstat(descriptor_file.fileno())) != stamp or _storage_stamp(path.lstat()) != stamp:
            raise ValueError("shard storage descriptor changed while reading")
    if hashlib.sha256(descriptor_bytes).hexdigest() != partition.storage_sha256:
        raise ValueError("shard storage descriptor digest differs")
    storage = json.loads(descriptor_bytes)
    if (
        type(storage) is not dict
        or set(storage) != {"contract", "manifest_sha256", "files"}
        or (
            storage["contract"] != _STORAGE_CONTRACT
            or storage["manifest_sha256"] != partition.sha256
            or type(storage["files"]) is not list
        )
    ):
        raise ValueError("shard storage descriptor scope differs")
    files, total = {}, 0
    for logical, stored in zip(document["files"], storage["files"], strict=True):
        if type(stored) is not list or len(stored) != 3:
            raise ValueError("shard storage descriptor file shape differs")
        name, byte_count, digest = stored
        if (
            name != logical["name"]
            or name in files
            or type(byte_count) is not int
            or byte_count <= 0
            or type(digest) is not str
            or len(digest) != 64
            or bytes.fromhex(digest).hex() != digest
        ):
            raise ValueError("shard storage descriptor file differs")
        files[name] = byte_count, digest
        total += byte_count
    if total > document["limits"]["max_spool_bytes"]:
        raise ValueError("shard storage aggregate byte budget differs")
    return files


def cleanup_spool(partition):
    """Remove only the exact exclusively created and externally registered root."""
    shutil.rmtree(_root(partition))


class _Spool:
    def __init__(self, root, capture_source, limits, register_cleanup, preparations=None):
        self.preparations = preparations or {}
        self.root, self.limits = Path(root).absolute(), limits
        self.identity = capture_source.identity
        self.handles, self.digests, self.buffers, self.current = OrderedDict(), {}, {}, {}
        self.stored_digests, self.stored_bytes = {}, 0
        self.pending_bytes = 0
        if self.root.exists() or self.root.is_symlink():
            raise FileExistsError("shard spool directory already exists")
        register_cleanup(self.root)
        self.root.mkdir(mode=0o700)
        self.directory = self.root.stat()
        self.owner = uuid.uuid4().hex
        try:
            self._write_new("owner.json", _json_bytes({"contract": _CONTRACT, "owner": self.owner}))
        except BaseException:
            self.discard()
            raise
        self.document = {
            "contract": _CONTRACT,
            "owner": self.owner,
            "scope": {"identity": asdict(capture_source.identity), "policy_sha256": capture_source.policy.digest},
            "limits": asdict(limits),
            "parts": 0,
            "source_records": {},
            "records": 0,
            "invalid_key_records": 0,
            "spool_bytes": 0,
            "files": [],
            "complete": False,
        }

    def _write_new(self, name, payload):
        self._charge_stored(len(payload))
        with os.fdopen(_open_owned(self.root, name, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as target:
            target.write(payload)
            target.flush()
            if payload:
                os.fsync(target.fileno())

    def _charge_stored(self, count):
        while self.stored_bytes + self.pending_bytes + count > self.limits.max_spool_bytes and self.buffers:
            self._flush(next(iter(self.buffers)))
        if self.stored_bytes + self.pending_bytes + count > self.limits.max_spool_bytes:
            raise OSError("shard spool byte budget is exhausted")
        self.stored_bytes += count

    def append(self, lane, occurrence):
        """Append an original occurrence under the existing spool resource budget."""
        encoded = encode_record(occurrence)
        framed = struct.pack(">Q", len(encoded)) + encoded
        if len(framed) > self.limits.max_spool_bytes:
            raise OSError("shard spool byte budget is exhausted")
        chunk_map = self.current.get(lane)
        if chunk_map is None or chunk_map["bytes"] and chunk_map["bytes"] + len(framed) > self.limits.chunk_bytes:
            if chunk_map is not None:
                self._flush(chunk_map["name"])
            number = 0 if chunk_map is None else chunk_map["number"] + 1
            name = f"shard-{lane if lane is not None else 'invalid'}-{number:06}.mpk"
            self._write_new(name, b"")
            chunk_map = {
                "name": name,
                "lane": lane,
                "number": number,
                "bytes": 0,
                "records": 0,
                "sha256": hashlib.sha256().hexdigest(),
            }
            self.document["files"].append(chunk_map)
            self.current[lane] = chunk_map
            self.digests[name] = hashlib.sha256()
            self.stored_digests[name] = hashlib.sha256()
            self._append_stored(name, spool.MAGIC)
        name = chunk_map["name"]
        self._buffer_frame(name, framed)
        self.digests[name].update(framed)
        chunk_map["bytes"] += len(framed)
        chunk_map["records"] += 1
        chunk_map["sha256"] = self.digests[name].hexdigest()
        self.document["records"] += 1
        self.document["spool_bytes"] += len(framed)
        self.document["invalid_key_records"] += int(lane is None)

    def _buffer_frame(self, name, framed):
        """Reserve aggregate pending storage and flush pressure without limiting logical volume."""
        remaining = memoryview(framed)
        while remaining:
            buffer = self.buffers.get(name)
            overhead = spool.BLOCK_OVERHEAD if buffer is None else 0
            available = self.limits.max_spool_bytes - self.stored_bytes - self.pending_bytes - overhead
            if available <= 0:
                if not self.buffers:
                    raise OSError("shard spool byte budget is exhausted")
                self._flush(next(iter(self.buffers)))
                continue
            if buffer is None:
                buffer = self.buffers[name] = bytearray()
            count = min(len(remaining), spool.BLOCK_BYTES - len(buffer), available)
            self.pending_bytes += count + overhead
            buffer.extend(remaining[:count])
            remaining = remaining[count:]
            if len(buffer) == spool.BLOCK_BYTES:
                self._flush(name)

    def _flush(self, name):
        buffer = self.buffers.pop(name, None)
        if not buffer:
            return
        encoded = spool.encoded_block(bytes(buffer))
        self.pending_bytes -= len(buffer) + spool.BLOCK_OVERHEAD
        self._append_stored(name, encoded)

    def _append_stored(self, name, payload):
        self._charge_stored(len(payload))
        target = self.handles.pop(name, None)
        if target is None:
            target = os.fdopen(_open_owned(self.root, name, os.O_WRONLY | os.O_APPEND), "wb", buffering=0)
        self.handles[name] = target
        if len(self.handles) > 8:
            self.handles.popitem(last=False)[1].close()
        remaining = memoryview(payload)
        while remaining:
            written = target.write(remaining)
            if not written:
                raise OSError("shard spool write did not advance")
            remaining = remaining[written:]
        self.stored_digests[name].update(payload)

    def complete(self):
        """Publish the manifest only after every owned file is closed and synced."""
        for name in tuple(self.buffers):
            self._flush(name)
        for entry in self.document["files"]:
            self._append_stored(entry["name"], spool.footer(entry["bytes"], self.digests[entry["name"]].digest()))
        self.close()
        stored_files = []
        for entry in self.document["files"]:
            with os.fdopen(_open_owned(self.root, entry["name"], os.O_RDONLY), "rb") as spool_file:
                os.fchmod(spool_file.fileno(), 0o400)
                os.fsync(spool_file.fileno())
                stored_files.append(
                    (
                        entry["name"],
                        _storage_stamp(os.fstat(spool_file.fileno()))[-1],
                        self.stored_digests[entry["name"]].hexdigest(),
                    )
                )
        self.document["complete"] = True
        manifest_bytes = _json_bytes(self.document)
        self._write_new("manifest.pending", manifest_bytes)
        os.link(self.root / "manifest.pending", self.root / "manifest.json", follow_symlinks=False)
        (self.root / "manifest.pending").unlink()
        manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        stored_payload = _json_bytes(
            {"contract": _STORAGE_CONTRACT, "manifest_sha256": manifest_sha256, "files": stored_files}
        )
        self._write_new(_STORAGE, stored_payload)
        for name in ("owner.json", "manifest.json", _STORAGE):
            with os.fdopen(_open_owned(self.root, name, os.O_RDONLY), "rb") as spool_file:
                os.fchmod(spool_file.fileno(), 0o400)
                os.fsync(spool_file.fileno())
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return PartitionedCapture(
            self.root,
            manifest_sha256,
            self.owner,
            self.directory.st_dev,
            self.directory.st_ino,
            self.identity,
            hashlib.sha256(stored_payload).hexdigest(),
        )

    def close(self):
        """Close every open spool file while retaining the primary cleanup error."""
        primary = None
        while self.handles:
            target = self.handles.popitem()[1]
            try:
                target.close()
            except BaseException as error:
                primary = _source_cleanup_failure(primary, error)
        self.buffers.clear()
        self.pending_bytes = 0
        if primary is not None:
            raise primary

    def discard(self):
        """Remove only this spool's original directory after draining file handles."""
        primary = None
        try:
            self.close()
        except BaseException as error:
            primary = error
        try:
            observed = self.root.lstat()
            if not stat.S_ISDIR(observed.st_mode) or (observed.st_dev, observed.st_ino) != (
                self.directory.st_dev,
                self.directory.st_ino,
            ):
                raise ValueError("shard spool directory custody differs")
            shutil.rmtree(self.root)
        except BaseException as error:
            primary = _source_cleanup_failure(primary, error)
        if primary is not None:
            raise primary


def _verify_file(root, entry, check=None, stored=None):
    digest = hashlib.sha256()
    with os.fdopen(_open_owned(root, entry["name"], os.O_RDONLY), "rb") as source:
        if stored is not None:
            _verify_stored_file(root, entry["name"], source, stored, check)
            logical = spool.BlockReader(source, check=check, maximum_logical_bytes=entry["bytes"])
        else:
            logical = source
        remaining = entry["bytes"]
        while remaining:
            if check is not None:
                check()
            block = logical.read(min(65536, remaining))
            if not block:
                raise ValueError("shard spool file is incomplete")
            digest.update(block)
            remaining -= len(block)
        if logical.read(1):
            raise ValueError("shard spool file has an uncommitted suffix")
    if digest.hexdigest() != entry["sha256"]:
        raise ValueError("shard spool file digest differs")


def _verify_stored_file(root, name, source, stored, check):
    byte_count, digest = stored
    identity = _storage_stamp((root / name).lstat())
    if identity[-1] != byte_count or _storage_stamp(os.fstat(source.fileno())) != identity:
        raise ValueError("shard stored file custody differs")
    source.seek(0)
    actual = hashlib.sha256()
    while chunk := source.read(65536):
        if check is not None:
            check()
        actual.update(chunk)
    if (
        actual.hexdigest() != digest
        or _storage_stamp(os.fstat(source.fileno())) != identity
        or (_storage_stamp((root / name).lstat()) != identity)
    ):
        raise ValueError("shard stored file digest or custody changed")
    source.seek(0)


def read_shard(partition, shard):
    """Read one completed shard without rereading the source or joining families."""
    root, document = _root(partition), manifest_document(partition)
    if shard is not None and (type(shard) is not int or not 0 <= shard < document["limits"]["shard_count"]):
        raise ValueError("shard spool shard differs")
    entries = [entry for entry in document["files"] if entry["lane"] == shard]
    yield from _read_entries(root, document, entries, stored_files=_storage_files(partition, document))


def read_partition(partition, *, check=None):
    """Verify one complete file census for owner-side global reconciliation."""
    root, document = _root(partition), manifest_document(partition)
    yield from _read_entries(
        root, document, document["files"], check=check, stored_files=_storage_files(partition, document)
    )


def _read_entries(root, document, entries, *, check=None, stored_files=None):
    stored_files = stored_files or {}
    for entry in entries:
        _verify_file(root, entry, check, stored_files.get(entry["name"]))
    for entry in entries:
        with os.fdopen(_open_owned(root, entry["name"], os.O_RDONLY), "rb") as stored_stream:
            logical = stored_stream
            stored = stored_files.get(entry["name"])
            if stored is not None:
                observed = _storage_stamp(os.fstat(stored_stream.fileno()))
                if observed[-1] != stored[0] or _storage_stamp((root / entry["name"]).lstat()) != observed:
                    raise ValueError("shard stored file was replaced before replay")
                logical = spool.BlockReader(stored_stream, check=check, maximum_logical_bytes=entry["bytes"])
            count = consumed = 0
            while consumed < entry["bytes"]:
                if check is not None:
                    check()
                header = logical.read(8)
                if len(header) != 8:
                    raise ValueError("shard spool frame is incomplete")
                length = struct.unpack(">Q", header)[0]
                if not 0 < length <= entry["bytes"] - consumed - 8:
                    raise ValueError("shard spool frame length differs")
                record_bytes = logical.read(length)
                if len(record_bytes) != length:
                    raise ValueError("shard spool frame is incomplete")
                consumed += length + 8
                count += 1
                occurrence = decode_record(record_bytes)
                if shard_for(occurrence, document["limits"]["shard_count"]) != entry["lane"]:
                    raise ValueError("shard spool record routing differs")
                yield occurrence
            if count != entry["records"]:
                raise ValueError("shard spool record count differs")
            if logical.read(1):
                raise ValueError("shard spool file has an uncommitted suffix")
            if stored is not None:
                _verify_stored_file(root, entry["name"], stored_stream, stored, check)


async def _part_rows(supplied, source, receipts):
    by_id = {receipt.stream_id: slot for slot, receipt in receipts.items()}
    async for part in supplied:
        slot = by_id.get(part.receipt.stream_id)
        if slot is None or part.receipt != receipts[slot]:
            raise ValueError("shard capture part receipt differs")
        canonical = canonical_json(asdict(part.capture.manifest))
        yield (
            source.identity.capture_bundle_id,
            slot,
            part.ordinal,
            len(part.capture.payload),
            part.capture.payload,
            bytes.fromhex(part.capture.manifest.compressed_sha256),
            canonical,
            hashlib.sha256(canonical.encode()).digest(),
            part.capture.manifest.decoded_bytes,
            part.arrow_byte_count,
            part.record_count,
        )


def _partition_part(spool, part, definition, stream, slot, collection_slot, policy, source_start):
    preparation = spool.preparations.get(slot)
    capture = SealedCapture(part.payload, part.capture_manifest)
    _validate_replay_partition_schema(capture, fields=_stream_fields(definition, stream), limits=policy.part_limits)
    count = 0
    decoder = iter_records(capture, stream, limits=policy.part_limits)
    decoded = partition_records(
        definition,
        stream,
        decoder,
        slot,
        collection_slot,
        part.ordinal,
        source_start,
        preparation,
    )
    primary = None
    try:
        for occurrence in decoded:
            if preparation is not None:
                preparation.check()
            count += 1
            spool.append(shard_for(occurrence, spool.limits.shard_count), occurrence)
    except BaseException as error:
        primary = error
    primary = _close_iterator(decoded, primary)
    primary = _close_iterator(decoder, primary)
    if primary is not None:
        raise primary
    if count != part.record_count:
        raise ValueError("shard capture decoded record count differs")
    arrow_limits = replace(
        policy.part_limits,
        maximum_decoded_bytes=max(policy.part_limits.maximum_decoded_bytes, policy.maximum_part_arrow_bytes),
    )
    # Asserted allocation stays sealed metadata; match the existing1024-batch decode budget.
    actual_arrow_bytes = _aggregate_parquet_arrow_bytes(capture, limits=arrow_limits, decoded_bytes=0)
    if actual_arrow_bytes > policy.maximum_part_arrow_bytes:
        raise ValueError("shard capture decoded Arrow bytes exceed the admitted replay limit")
    return count


class _PartitionStopped(InterruptedError):
    """The owning async operation stopped its native partition worker."""


async def _partition_part_cpu(operation, preparation):
    stopped = threading.Event()

    def _check():
        if stopped.is_set():
            raise _PartitionStopped("SOURCE partition preparation stopped")

    preparation.check = _check
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except BaseException as failure:
        if task.done() and not task.cancelled() and task.exception() is failure:
            raise
        stopped.set()

        async def _drain():
            try:
                await task
            except _PartitionStopped:
                return

        raise await _drain_cleanup(_drain(), failure)


async def _partition_occurrences(spool, part_source, definition, capture_source, receipts, metadata, accounting):
    source_counts = dict.fromkeys(receipts, 0)
    by_id = {receipt.stream_id: slot for slot, receipt in receipts.items()}
    stream_by_id = {stream.stream_id: stream for stream in definition.source_streams}
    collection_slots_by_name = {
        collection.name: slot for slot, collection in enumerate(definition.child_collections, 1)
    }
    contexts = []
    primary = None
    try:
        supplied = await _enter_context(contexts, part_source)
        stored = await _enter_context(contexts, aclosing(_part_rows(supplied, capture_source, receipts)))
        verified = _iter_replayable_parquet_parts(
            stored, capture_source.identity.capture_bundle_id, receipts, metadata, accounting
        )
        await _enter_context(contexts, aclosing(verified))
        async for part in verified:
            slot = by_id[part.receipt.stream_id]
            stream = stream_by_id[part.receipt.stream_id]
            preparation = spool.preparations.get(slot)
            operation = partial(
                _partition_part,
                spool,
                part,
                definition,
                stream,
                slot,
                collection_slots_by_name.get(stream.child_collection, 0),
                capture_source.policy,
                source_counts[slot],
            )
            source_counts[slot] += (
                await _partition_part_cpu(operation, preparation)
                if preparation is not None and preparation.native is not None
                else operation()
            )
            spool.document["parts"] += 1
    except BaseException as error:
        primary = error
    primary = await _close_contexts(contexts, primary)
    if primary is not None:
        raise primary
    return source_counts


def _partition_preparations(definition, source, preparations):
    if preparations is None:
        return {}
    if set(preparations) != set(range(1, len(definition.source_streams) + 1)):
        raise ValueError("shard native preparation context differs")
    for slot, preparation in preparations.items():
        if type(preparation) is not PartitionPreparation:
            raise ValueError("shard native preparation context differs")
        context = preparation.context
        request = context.request
        if (
            context.build_id <= 0
            or preparation.memory_bytes <= 0
            or (request.dataset_id, request.definition_revision_id, request.schema_revision_id)
            != (source.identity.dataset_id, source.identity.definition_revision_id, source.identity.schema_revision_id)
            or request.definition != definition
            or context.stream != definition.source_streams[slot - 1]
            or dict(context.registry.stream_slots)
            != {stream.stream_id: ordinal for ordinal, stream in enumerate(definition.source_streams, 1)}
        ):
            raise ValueError("shard native preparation context differs")
    return preparations


async def partition_capture(part_source, *, definition, source, root, register_cleanup, limits, preparations=None):
    """Complete real reader EOF and close before exposing any replayable shard.

    The owner registers exact cleanup before creation. Failure discards only
    that exclusive root; retry starts again from the immutable sealed capture.
    """
    receipts, metadata, accounting = source.headers(definition)
    preparations = _partition_preparations(definition, source, preparations)
    spool = _Spool(root, source, limits, register_cleanup, preparations)
    try:
        source_counts = await _partition_occurrences(
            spool, part_source, definition, source, receipts, metadata, accounting
        )
        if sum(source_counts.values()) != spool.document["records"]:
            raise ValueError("shard spool complete occurrence accounting differs")
        spool.document["source_records"] = {str(key): value for key, value in source_counts.items()}
        return spool.complete()
    except BaseException as error:
        try:
            spool.discard()
        except BaseException as cleanup:
            raise _source_cleanup_failure(error, cleanup) from cleanup
        raise
