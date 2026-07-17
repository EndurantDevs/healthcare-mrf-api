# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded-memory conversion of V3 membership sidecars to shared graph blocks."""

from __future__ import annotations

import hashlib
import heapq
import mmap
import shutil
import struct
import tempfile
from contextlib import closing
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import BinaryIO, Callable, Iterable, Iterator, Mapping, Sequence

from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_VERSION,
    PTG2ManifestArtifactError,
)
from process.ptg_parts.ptg2_shared_blocks import SharedBlock, SharedBlockReference


PTG2_V3_GRAPH_CHUNK_BYTES = 64 * 1024
PTG2_V3_GRAPH_NPI_TO_GROUP = 1
PTG2_V3_GRAPH_GROUP_TO_NPI = 2
PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET = 3
PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP = 4

_UINT32_MAX = 2**32 - 1
_STANDARD_HEADER = struct.Struct("<8sIQ")
_DENSE_HEADER = struct.Struct("<8sIQQ")
_OWNER_RECORD = struct.Struct("<16sQI")
_DENSE_MEMBER = struct.Struct("<I")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_DENSE_MAP_RECORD = struct.Struct(">16sI")
_OWNER_SPOOL_RECORD = struct.Struct(">BQIIQ")
_BLOCK_SPOOL_HEADER = struct.Struct(">BQQI")
_REFERENCE_RECORD = struct.Struct(">BQQ32sQ")
_PG_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
_PG_COPY_TRAILER = struct.pack(">h", -1)

_DIRECTION_LAYOUT = {
    PTG2_V3_GRAPH_NPI_TO_GROUP: ("graph_npi_groups_v1", _U32),
    PTG2_V3_GRAPH_GROUP_TO_NPI: ("graph_group_npis_v1", _U64),
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET: ("graph_group_provider_sets_v1", _U32),
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP: ("graph_provider_set_groups_v1", _U32),
}


@dataclass(frozen=True)
class MembershipArtifact:
    path: Path
    metadata: Mapping[str, object]


@dataclass(frozen=True)
class SharedGraphShardBundle:
    """One named source shard containing every graph direction."""

    shard_id: str
    group_npi: MembershipArtifact
    npi_group: MembershipArtifact
    group_provider_set: MembershipArtifact
    provider_set_group: MembershipArtifact


@dataclass(frozen=True)
class SharedGraphOwnerRow:
    direction: int
    owner_key: int
    first_chunk: int
    member_offset: int
    member_count: int


@dataclass(frozen=True)
class SharedGraphDirectionMetrics:
    direction: int
    object_kind: str
    member_width: int
    owner_count: int
    member_count: int
    empty_owner_count: int
    block_count: int
    raw_byte_count: int


@dataclass(frozen=True)
class SharedGraphIntegrityMetrics:
    shard_count: int
    artifact_count: int
    checksum_byte_count: int
    reciprocal_pair_count: int
    reciprocal_edge_count: int
    input_edge_count: int
    unique_edge_count: int
    duplicate_edge_count: int


@dataclass(frozen=True)
class SharedGraphEdgeMetrics:
    """Logical forward-edge counts; the validated reverse count is identical."""

    edge_kind: str
    input_edge_count: int
    unique_edge_count: int
    duplicate_edge_count: int


@dataclass(frozen=True)
class SharedGraphConversionResult:
    scratch_directory: Path
    block_copy_path: Path
    owner_copy_path: Path
    group_copy_path: Path
    npi_copy_path: Path
    block_spool_path: Path
    owner_spool_path: Path
    group_map_path: Path
    reference_path: Path
    block_count: int
    owner_count: int
    provider_group_count: int
    npi_count: int
    support_digest: bytes
    direction_metrics: tuple[SharedGraphDirectionMetrics, ...]
    edge_metrics: tuple[SharedGraphEdgeMetrics, ...]
    input_byte_count: int
    raw_block_byte_count: int
    stored_block_byte_count: int
    integrity: SharedGraphIntegrityMetrics

    def iter_shared_blocks(self) -> Iterator[SharedBlock]:
        """Yield encoded graph blocks in deterministic publication order."""
        with self.block_spool_path.open("rb") as block_spool:
            while True:
                header = block_spool.read(_BLOCK_SPOOL_HEADER.size)
                if not header:
                    return
                if len(header) != _BLOCK_SPOOL_HEADER.size:
                    raise RuntimeError("temporary graph block spool is truncated")
                direction, block_key, entry_count, raw_byte_count = _BLOCK_SPOOL_HEADER.unpack(header)
                block_payload = block_spool.read(raw_byte_count)
                if len(block_payload) != raw_byte_count:
                    raise RuntimeError("temporary graph block payload is truncated")
                object_kind, _member_struct = _DIRECTION_LAYOUT[direction]
                yield SharedBlock(
                    object_kind=object_kind,
                    block_key=block_key,
                    fragment_no=0,
                    entry_count=entry_count,
                    codec="none",
                    raw_byte_count=raw_byte_count,
                    payload=block_payload,
                )

    def iter_owner_rows(self) -> Iterator[SharedGraphOwnerRow]:
        """Yield owner locators in direction and owner-key order."""
        for encoded_owner in _iter_fixed_records(
            self.owner_spool_path, _OWNER_SPOOL_RECORD.size
        ):
            yield SharedGraphOwnerRow(*_OWNER_SPOOL_RECORD.unpack(encoded_owner))

    def iter_group_key_items(self) -> Iterator[tuple[bytes, int]]:
        """Yield provider-group global IDs paired with their dense keys."""
        for encoded_pair in _iter_fixed_records(
            self.group_map_path, _DENSE_MAP_RECORD.size
        ):
            global_id, key = _DENSE_MAP_RECORD.unpack(encoded_pair)
            yield global_id, key

    def iter_references(self) -> Iterator[SharedBlockReference]:
        """Yield immutable block references in graph publication order."""
        for encoded_reference in _iter_fixed_records(
            self.reference_path, _REFERENCE_RECORD.size
        ):
            direction, block_key, entry_count, block_hash, raw_byte_count = (
                _REFERENCE_RECORD.unpack(encoded_reference)
            )
            object_kind, _member_struct = _DIRECTION_LAYOUT[direction]
            yield SharedBlockReference(
                object_kind=object_kind,
                block_key=block_key,
                fragment_no=0,
                entry_count=entry_count,
                block_hash=block_hash,
                raw_byte_count=raw_byte_count,
        )

    def cleanup(self) -> None:
        """Remove all cardinality-dependent conversion scratch artifacts."""
        shutil.rmtree(self.scratch_directory, ignore_errors=True)


@dataclass(frozen=True)
class _OwnerEvent:
    owner: bytes
    member_count: int


@dataclass(frozen=True)
class _MemberEvent:
    member: bytes


class _ValidatedArtifact:
    def __init__(self, artifact: MembershipArtifact) -> None:
        self.path = Path(artifact.path)
        self.metadata = artifact.metadata
        self.is_dense = False
        self.owner_count = 0
        self.member_count = 0
        self.member_global_count = 0
        self.header_size = 0
        self.index_start = 0
        self.dictionary_start = 0
        self.members_start = 0
        self.byte_count = 0
        self._validate()

    @staticmethod
    def _metadata_integer(metadata: Mapping[str, object], name: str) -> int:
        value = metadata.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise PTG2ManifestArtifactError(
                f"global membership sidecar metadata requires non-negative {name}"
            )
        return value

    def _validate(self) -> None:
        expected_sha = self._validate_metadata()
        self._validate_file(expected_sha)
        header_version, header_owner_count = self._read_header()
        if header_version != PTG2_MANIFEST_VERSION:
            raise PTG2ManifestArtifactError(
                f"unsupported global membership sidecar version: {header_version}"
            )
        if header_owner_count != self.owner_count:
            raise PTG2ManifestArtifactError("global membership sidecar owner count mismatch")
        self._validate_layout()
        self._validate_dictionary()
        # Eager traversal guarantees structural failures precede graph publication.
        for _event in self.iter_events():
            continue

    def _validate_metadata(self) -> str:
        if not isinstance(self.metadata, Mapping):
            raise PTG2ManifestArtifactError("global membership sidecar metadata must be a mapping")
        expected_sha = self.metadata.get("sha256")
        if not isinstance(expected_sha, str) or len(expected_sha) != 64:
            raise PTG2ManifestArtifactError("global membership sidecar metadata requires sha256")
        self.byte_count = self._metadata_integer(self.metadata, "byte_count")
        self.owner_count = self._metadata_integer(self.metadata, "owner_count")
        self.member_count = self._metadata_integer(self.metadata, "member_count")
        return expected_sha

    def _validate_file(self, expected_sha: str) -> None:
        try:
            stat_size = self.path.stat().st_size
        except OSError as exc:
            raise PTG2ManifestArtifactError(f"global membership sidecar is unavailable: {self.path}") from exc
        if stat_size != self.byte_count:
            raise PTG2ManifestArtifactError(
                f"global membership sidecar byte count mismatch: expected {self.byte_count}, got {stat_size}"
            )
        digest = hashlib.sha256()
        with self.path.open("rb") as artifact_file:
            for chunk in iter(lambda: artifact_file.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected_sha.lower():
            raise PTG2ManifestArtifactError("global membership sidecar checksum mismatch")

    def _read_header(self) -> tuple[int, int]:
        record_format = self.metadata.get("record_format")
        with self.path.open("rb") as artifact_file:
            magic = artifact_file.read(8)
            artifact_file.seek(0)
            if magic == PTG2_MANIFEST_MEMBERSHIP_MAGIC:
                if record_format != PTG2_MANIFEST_MEMBERSHIP_FORMAT:
                    raise PTG2ManifestArtifactError("standard membership sidecar format metadata mismatch")
                header = artifact_file.read(_STANDARD_HEADER.size)
                if len(header) != _STANDARD_HEADER.size:
                    raise PTG2ManifestArtifactError("global membership sidecar is missing its header")
                _magic, version, header_owner_count = _STANDARD_HEADER.unpack(header)
                self.header_size = _STANDARD_HEADER.size
            elif magic == PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC:
                if record_format != PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT:
                    raise PTG2ManifestArtifactError("dense membership sidecar format metadata mismatch")
                header = artifact_file.read(_DENSE_HEADER.size)
                if len(header) != _DENSE_HEADER.size:
                    raise PTG2ManifestArtifactError("dense membership sidecar is missing its header")
                _magic, version, header_owner_count, self.member_global_count = _DENSE_HEADER.unpack(header)
                expected_globals = self._metadata_integer(self.metadata, "member_global_count")
                if self.member_global_count != expected_globals:
                    raise PTG2ManifestArtifactError("dense membership dictionary count mismatch")
                self.header_size = _DENSE_HEADER.size
                self.is_dense = True
            else:
                raise PTG2ManifestArtifactError("global membership sidecar has an invalid magic header")
        return version, header_owner_count

    def _validate_layout(self) -> None:
        self.index_start = self.header_size
        self.dictionary_start = self.index_start + self.owner_count * _OWNER_RECORD.size
        self.members_start = self.dictionary_start + self.member_global_count * 16
        member_width = _DENSE_MEMBER.size if self.is_dense else 16
        expected_size = self.members_start + self.member_count * member_width
        if expected_size != self.byte_count:
            raise PTG2ManifestArtifactError(
                "global membership sidecar layout size mismatch: "
                f"expected {expected_size}, got {self.byte_count}"
            )

    def _validate_dictionary(self) -> None:
        if not self.is_dense:
            return
        previous: bytes | None = None
        with self.path.open("rb") as dictionary_file:
            dictionary_file.seek(self.dictionary_start)
            for _ in range(self.member_global_count):
                global_id = dictionary_file.read(16)
                if len(global_id) != 16:
                    raise PTG2ManifestArtifactError("dense membership dictionary is truncated")
                if previous is not None and global_id <= previous:
                    raise PTG2ManifestArtifactError(
                        "dense membership dictionary must be sorted and unique"
                    )
                previous = global_id

    def iter_events(self) -> Iterator[_OwnerEvent | _MemberEvent]:
        """Yield validated owner/member events without retaining graph cardinality."""
        with self.path.open("rb") as index_file, self.path.open("rb") as member_file:
            dictionary_file = self.path.open("rb") if self.is_dense else None
            try:
                yield from self._iter_file_events(
                    index_file, member_file, dictionary_file
                )
            finally:
                if dictionary_file is not None:
                    dictionary_file.close()

    def _iter_file_events(
        self,
        index_file: BinaryIO,
        member_file: BinaryIO,
        dictionary_file: BinaryIO | None,
    ) -> Iterator[_OwnerEvent | _MemberEvent]:
        previous_owner: bytes | None = None
        expected_member_offset = 0
        observed_members = 0
        index_file.seek(self.index_start)
        member_file.seek(self.members_start)
        for _ in range(self.owner_count):
            encoded_owner = index_file.read(_OWNER_RECORD.size)
            if len(encoded_owner) != _OWNER_RECORD.size:
                raise PTG2ManifestArtifactError("global membership owner index is truncated")
            owner, member_offset, member_count = _OWNER_RECORD.unpack(encoded_owner)
            if previous_owner is not None and owner <= previous_owner:
                raise PTG2ManifestArtifactError(
                    "global membership owners must be sorted and unique"
                )
            if member_offset != expected_member_offset:
                raise PTG2ManifestArtifactError(
                    "global membership member offsets are not contiguous"
                )
            previous_owner = owner
            expected_member_offset += member_count
            observed_members += member_count
            yield _OwnerEvent(owner, member_count)
            yield from self._iter_owner_members(
                member_file, dictionary_file, member_count
            )
        if observed_members != self.member_count:
            raise PTG2ManifestArtifactError("global membership member count mismatch")

    def _iter_owner_members(
        self,
        member_file: BinaryIO,
        dictionary_file: BinaryIO | None,
        member_count: int,
    ) -> Iterator[_MemberEvent]:
        previous_member: bytes | None = None
        for _ in range(member_count):
            member_global_id = self._read_member_global_id(
                member_file, dictionary_file
            )
            if previous_member is not None and member_global_id <= previous_member:
                raise PTG2ManifestArtifactError(
                    "global membership members must be sorted and unique"
                )
            previous_member = member_global_id
            yield _MemberEvent(member_global_id)

    def _read_member_global_id(
        self,
        member_file: BinaryIO,
        dictionary_file: BinaryIO | None,
    ) -> bytes:
        if not self.is_dense:
            member_global_id = member_file.read(16)
        else:
            encoded_local_id = member_file.read(_DENSE_MEMBER.size)
            if len(encoded_local_id) != _DENSE_MEMBER.size:
                raise PTG2ManifestArtifactError(
                    "dense membership member block is truncated"
                )
            local_id = _DENSE_MEMBER.unpack(encoded_local_id)[0]
            if local_id >= self.member_global_count:
                raise PTG2ManifestArtifactError(
                    "dense membership member id is out of range"
                )
            assert dictionary_file is not None
            dictionary_file.seek(self.dictionary_start + local_id * 16)
            member_global_id = dictionary_file.read(16)
        if len(member_global_id) != 16:
            raise PTG2ManifestArtifactError(
                "global membership member block is truncated"
            )
        return member_global_id


def _normalize_global_id(value: bytes | bytearray | memoryview | str) -> bytes:
    if isinstance(value, str):
        try:
            raw = bytes.fromhex(value)
        except ValueError as exc:
            raise ValueError("provider-set dictionary IDs must be 32-character hex") from exc
    else:
        raw = bytes(value)
    if len(raw) != 16:
        raise ValueError("provider-set dictionary IDs must contain 16 bytes")
    return raw


def _npi_from_global_id(global_id: bytes) -> int:
    if len(global_id) != 16 or global_id[:8] != b"\x00" * 8:
        raise PTG2ManifestArtifactError("provider NPI membership uses an invalid global ID")
    npi = int.from_bytes(global_id[8:], "big", signed=False)
    if npi <= 0:
        raise PTG2ManifestArtifactError("provider NPI membership uses a non-positive NPI")
    return npi


def _iter_fixed_records(path: Path, record_size: int) -> Iterator[bytes]:
    with path.open("rb") as source:
        while True:
            record = source.read(record_size)
            if not record:
                return
            if len(record) != record_size:
                raise RuntimeError("temporary graph edge run is truncated")
            yield record


def _write_sorted_run(path: Path, records: list[bytes]) -> int:
    records.sort()
    unique_count = 0
    previous: bytes | None = None
    with path.open("wb") as destination:
        for record in records:
            if record == previous:
                continue
            destination.write(record)
            previous = record
            unique_count += 1
    return unique_count


def _merge_runs(paths: Sequence[Path], destination: Path, record_size: int) -> int:
    streams = [_iter_fixed_records(path, record_size) for path in paths]
    unique_count = 0
    previous: bytes | None = None
    with destination.open("wb") as output:
        for record in heapq.merge(*streams):
            if record == previous:
                continue
            output.write(record)
            previous = record
            unique_count += 1
    return unique_count


def _external_sorted_records(
    encoded_records: Iterable[bytes],
    *,
    record_size: int,
    directory: Path,
    prefix: str,
    chunk_bytes: int,
    merge_fan_in: int = 32,
) -> tuple[Path, int, int]:
    records_per_chunk = max(1, chunk_bytes // record_size)
    buffered_records: list[bytes] = []
    runs: list[Path] = []
    input_count = 0
    for encoded_record in encoded_records:
        if len(encoded_record) != record_size:
            raise RuntimeError("external graph sorter received the wrong record width")
        buffered_records.append(encoded_record)
        input_count += 1
        if len(buffered_records) >= records_per_chunk:
            run_path = directory / f"{prefix}.{len(runs):08d}.run"
            _write_sorted_run(run_path, buffered_records)
            runs.append(run_path)
            buffered_records = []
    if buffered_records or not runs:
        run_path = directory / f"{prefix}.{len(runs):08d}.run"
        _write_sorted_run(run_path, buffered_records)
        runs.append(run_path)

    pass_no = 0
    while len(runs) > 1:
        merged_runs: list[Path] = []
        for start in range(0, len(runs), merge_fan_in):
            batch = runs[start : start + merge_fan_in]
            merged = directory / f"{prefix}.merge{pass_no}.{len(merged_runs):08d}.run"
            _merge_runs(batch, merged, record_size)
            merged_runs.append(merged)
            for path in batch:
                path.unlink()
        runs = merged_runs
        pass_no += 1
    unique_count = runs[0].stat().st_size // record_size
    return runs[0], input_count, unique_count


def _artifact_records(
    artifact: _ValidatedArtifact,
    *,
    encode: Callable[[bytes, bytes], bytes],
) -> Iterator[bytes]:
    owner: bytes | None = None
    for event in artifact.iter_events():
        if isinstance(event, _OwnerEvent):
            owner = event.owner
        else:
            if owner is None:
                raise PTG2ManifestArtifactError("global membership member has no owner")
            yield encode(owner, event.member)


def _artifact_owner_records(
    artifact: _ValidatedArtifact,
    *,
    encode: Callable[[bytes], bytes],
) -> Iterator[bytes]:
    for event in artifact.iter_events():
        if isinstance(event, _OwnerEvent):
            yield encode(event.owner)


def _assert_sorted_runs_equal(
    left_path: Path,
    right_path: Path,
    *,
    left_count: int,
    right_count: int,
    description: str,
) -> None:
    if left_count != right_count:
        raise PTG2ManifestArtifactError(
            f"{description} membership directions have different edge counts"
        )
    with left_path.open("rb") as left_file, right_path.open("rb") as right_file:
        while True:
            left_chunk = left_file.read(1024 * 1024)
            right_chunk = right_file.read(1024 * 1024)
            if left_chunk != right_chunk:
                raise PTG2ManifestArtifactError(
                    f"{description} membership directions are not reciprocal"
                )
            if not left_chunk:
                break


def _merge_sorted_run_files(
    paths: Sequence[Path],
    *,
    destination: Path,
    record_size: int,
    merge_fan_in: int = 32,
) -> int:
    if not paths:
        destination.write_bytes(b"")
        return 0
    runs = [(Path(path), False) for path in paths]
    pass_no = 0
    while len(runs) > merge_fan_in:
        merged_runs: list[tuple[Path, bool]] = []
        for start in range(0, len(runs), merge_fan_in):
            batch = runs[start : start + merge_fan_in]
            merged = destination.with_name(
                f"{destination.name}.merge{pass_no}.{len(merged_runs):08d}.run"
            )
            _merge_runs([path for path, _owned in batch], merged, record_size)
            merged_runs.append((merged, True))
            for path, owned in batch:
                if owned:
                    path.unlink()
        runs = merged_runs
        pass_no += 1
    if len(runs) == 1:
        shutil.copyfile(runs[0][0], destination)
        unique_count = destination.stat().st_size // record_size
    else:
        unique_count = _merge_runs(
            [path for path, _owned in runs], destination, record_size
        )
    for path, owned in runs:
        if owned:
            path.unlink()
    return unique_count


def _transpose_sorted_run(
    source: Path,
    *,
    source_owner_width: int,
    record_size: int,
    directory: Path,
    prefix: str,
    chunk_bytes: int,
) -> tuple[Path, int]:
    def records() -> Iterator[bytes]:
        """Yield fixed records with their owner and member fields transposed."""

        for record in _iter_fixed_records(source, record_size):
            yield record[source_owner_width:] + record[:source_owner_width]

    path, _input_count, unique_count = _external_sorted_records(
        records(),
        record_size=record_size,
        directory=directory,
        prefix=prefix,
        chunk_bytes=chunk_bytes,
    )
    return path, unique_count


class _PostgresBinaryCopyWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.output: BinaryIO | None = None
        self.row_count = 0

    def __enter__(self) -> "_PostgresBinaryCopyWriter":
        self.output = self.path.open("wb")
        self.output.write(_PG_COPY_HEADER)
        return self

    def write_row(self, fields: Sequence[bytes | None]) -> None:
        """Append one row in PostgreSQL binary COPY format."""

        if self.output is None:
            raise RuntimeError("PostgreSQL COPY writer is not open")
        self.output.write(struct.pack(">h", len(fields)))
        for field in fields:
            if field is None:
                self.output.write(struct.pack(">i", -1))
            else:
                value = bytes(field)
                self.output.write(struct.pack(">i", len(value)))
                self.output.write(value)
        self.row_count += 1

    def __exit__(self, exc_type, exc, traceback) -> None:
        assert self.output is not None
        if exc_type is None:
            self.output.write(_PG_COPY_TRAILER)
        self.output.close()
        self.output = None


class _DiskDenseMap:
    """Sorted global-id to uint32 map backed by a fixed-record file."""

    def __init__(self, path: Path, *, label: str, expected_start: tuple[int, ...]) -> None:
        self.path = path
        self.label = label
        size = path.stat().st_size
        if size % _DENSE_MAP_RECORD.size:
            raise ValueError(f"{label} dictionary fixed-record file is truncated")
        self.count = size // _DENSE_MAP_RECORD.size
        previous_global: bytes | None = None
        first_key: int | None = None
        with path.open("rb") as map_input:
            for index in range(self.count):
                packed_record = map_input.read(_DENSE_MAP_RECORD.size)
                global_id, key = _DENSE_MAP_RECORD.unpack(packed_record)
                if previous_global is not None and global_id <= previous_global:
                    raise ValueError(f"{label} dictionary global IDs must be sorted and unique")
                if first_key is None:
                    first_key = key
                    if key not in expected_start:
                        raise ValueError(
                            f"{label} dictionary keys must be dense and follow sorted global IDs"
                        )
                if key != first_key + index:
                    raise ValueError(f"{label} dictionary keys must be dense and follow sorted global IDs")
                previous_global = global_id
        self._source = path.open("rb")
        self._mapped = (
            mmap.mmap(self._source.fileno(), 0, access=mmap.ACCESS_READ)
            if self.count
            else None
        )

    def key(self, global_id: bytes) -> int:
        """Return a global ID's dense key, raising KeyError when it is absent."""

        target = _normalize_global_id(global_id)
        mapped = self._mapped
        if mapped is None:
            raise KeyError(target)
        low = 0
        high = self.count
        while low < high:
            middle = (low + high) // 2
            offset = middle * _DENSE_MAP_RECORD.size
            candidate = mapped[offset : offset + 16]
            if candidate < target:
                low = middle + 1
            else:
                high = middle
        if low >= self.count:
            raise KeyError(target)
        offset = low * _DENSE_MAP_RECORD.size
        candidate, key = _DENSE_MAP_RECORD.unpack_from(mapped, offset)
        if candidate != target:
            raise KeyError(target)
        return key

    def close(self) -> None:
        """Close the memory map and its backing file."""

        if self._mapped is not None:
            self._mapped.close()
            self._mapped = None
        self._source.close()


def _write_provider_set_map(
    provider_set_source: Mapping[bytes | bytearray | memoryview | str, int] | Path,
    destination: Path,
    *,
    chunk_bytes: int,
) -> _DiskDenseMap:
    if isinstance(provider_set_source, (str, Path)):
        source_path = Path(provider_set_source)

        def records() -> Iterator[bytes]:
            """Parse exported provider-set rows into fixed dense-map records."""

            with source_path.open("rt", encoding="ascii", newline="") as rows:
                for line in rows:
                    fields = line.rstrip("\r\n").split("\t")
                    if len(fields) != 2:
                        raise ValueError("provider-set dictionary export has an invalid row")
                    try:
                        global_id = bytes.fromhex(fields[0])
                        key = int(fields[1])
                    except ValueError as exc:
                        raise ValueError("provider-set dictionary export has an invalid row") from exc
                    if len(global_id) != 16 or key < 0 or key > _UINT32_MAX:
                        raise ValueError("provider-set dictionary contains invalid entries")
                    yield _DENSE_MAP_RECORD.pack(global_id, key)
    else:
        def records() -> Iterator[bytes]:
            """Encode provider-set mapping entries as fixed dense-map records."""

            for raw_global_id, raw_key in provider_set_source.items():
                global_id = _normalize_global_id(raw_global_id)
                if isinstance(raw_key, bool) or not isinstance(raw_key, int):
                    raise ValueError("provider-set keys must be uint32 integers")
                if raw_key < 0 or raw_key > _UINT32_MAX:
                    raise ValueError("provider-set dictionary contains invalid entries")
                yield _DENSE_MAP_RECORD.pack(global_id, raw_key)

    sorted_path, _input_count, _unique_count = _external_sorted_records(
        records(),
        record_size=_DENSE_MAP_RECORD.size,
        directory=destination.parent,
        prefix="provider-set-map",
        chunk_bytes=chunk_bytes,
    )
    shutil.move(sorted_path, destination)
    return _DiskDenseMap(destination, label="provider-set", expected_start=(0, 1))


class _BlockStream:
    def __init__(
        self,
        *,
        direction: int,
        block_copy: _PostgresBinaryCopyWriter,
        owner_copy: _PostgresBinaryCopyWriter,
        block_spool: BinaryIO,
        owner_spool: BinaryIO,
        reference_spool: BinaryIO,
        support_hasher: object,
    ) -> None:
        self.direction = direction
        self.object_kind, self.member_struct = _DIRECTION_LAYOUT[direction]
        self.payload = bytearray()
        self.block_copy = block_copy
        self.owner_copy = owner_copy
        self.block_spool = block_spool
        self.owner_spool = owner_spool
        self.reference_spool = reference_spool
        self.support_hasher = support_hasher
        self.block_count = 0
        self.owner_count = 0
        self.member_count = 0
        self.empty_owner_count = 0

    def record_owner(self, owner_key: int, first_member: int, member_count: int) -> None:
        """Write one owner index row to the COPY and integrity spools."""

        absolute_offset = first_member * self.member_struct.size
        row = SharedGraphOwnerRow(
            direction=self.direction,
            owner_key=owner_key,
            first_chunk=absolute_offset // PTG2_V3_GRAPH_CHUNK_BYTES,
            member_offset=absolute_offset % PTG2_V3_GRAPH_CHUNK_BYTES,
            member_count=member_count,
        )
        encoded = _OWNER_SPOOL_RECORD.pack(
            row.direction, row.owner_key, row.first_chunk, row.member_offset, row.member_count
        )
        self.owner_spool.write(encoded)
        self.support_hasher.update(encoded)
        self.owner_copy.write_row(
            (
                struct.pack(">h", row.direction),
                struct.pack(">q", row.owner_key),
                struct.pack(">i", row.first_chunk),
                struct.pack(">i", row.member_offset),
                struct.pack(">q", row.member_count),
            )
        )
        self.owner_count += 1
        if member_count == 0:
            self.empty_owner_count += 1

    def append(self, member: int) -> None:
        """Append one validated member key, flushing each full graph chunk."""

        if member < 0 or member > (2 ** (self.member_struct.size * 8) - 1):
            raise ValueError(f"graph member does not fit uint{self.member_struct.size * 8}")
        self.payload.extend(self.member_struct.pack(member))
        self.member_count += 1
        if len(self.payload) == PTG2_V3_GRAPH_CHUNK_BYTES:
            self._flush()

    def _flush(self) -> None:
        if not self.payload:
            return
        block_payload = bytes(self.payload)
        block = SharedBlock(
            object_kind=self.object_kind,
            block_key=self.block_count,
            fragment_no=0,
            entry_count=len(block_payload) // self.member_struct.size,
            codec="none",
            raw_byte_count=len(block_payload),
            payload=block_payload,
        )
        self.block_copy.write_row(
            (
                block.block_hash,
                struct.pack(">h", block.format_version),
                block.object_kind.encode("utf-8"),
                struct.pack(">q", block.block_key),
                struct.pack(">i", block.fragment_no),
                struct.pack(">q", block.entry_count),
                block.codec.encode("ascii"),
                struct.pack(">q", block.raw_byte_count),
                struct.pack(">q", block.stored_byte_count),
                block.payload,
            )
        )
        self.block_spool.write(
            _BLOCK_SPOOL_HEADER.pack(
                self.direction, block.block_key, block.entry_count, block.raw_byte_count
            )
        )
        self.block_spool.write(block.payload)
        self.reference_spool.write(
            _REFERENCE_RECORD.pack(
                self.direction,
                block.block_key,
                block.entry_count,
                block.block_hash,
                block.raw_byte_count,
            )
        )
        self.block_count += 1
        self.payload.clear()

    def finish(self) -> SharedGraphDirectionMetrics:
        """Flush the remaining chunk and return direction metrics."""

        self._flush()
        raw_bytes = self.member_count * self.member_struct.size
        metrics = SharedGraphDirectionMetrics(
            direction=self.direction,
            object_kind=self.object_kind,
            member_width=self.member_struct.size,
            owner_count=self.owner_count,
            member_count=self.member_count,
            empty_owner_count=self.empty_owner_count,
            block_count=self.block_count,
            raw_byte_count=raw_bytes,
        )
        return metrics


def _emit_sorted_direction(
    edge_path: Path,
    owner_path: Path,
    *,
    direction: int,
    owner_key: Callable[[bytes], int],
    member_key: Callable[[bytes], int],
    owner_width: int,
    member_width: int,
    block_copy: _PostgresBinaryCopyWriter,
    owner_copy: _PostgresBinaryCopyWriter,
    block_spool: BinaryIO,
    owner_spool: BinaryIO,
    reference_spool: BinaryIO,
    support_hasher: object,
) -> SharedGraphDirectionMetrics:
    stream = _BlockStream(
        direction=direction,
        block_copy=block_copy,
        owner_copy=owner_copy,
        block_spool=block_spool,
        owner_spool=owner_spool,
        reference_spool=reference_spool,
        support_hasher=support_hasher,
    )
    edge_iterator = _iter_fixed_records(edge_path, owner_width + member_width)
    current_edge = next(edge_iterator, None)
    for owner in _iter_fixed_records(owner_path, owner_width):
        if current_edge is not None and current_edge[:owner_width] < owner:
            raise PTG2ManifestArtifactError("merged graph edge has no corresponding owner")
        first_member = stream.member_count
        member_count = 0
        while current_edge is not None and current_edge[:owner_width] == owner:
            stream.append(member_key(current_edge[owner_width:]))
            member_count += 1
            current_edge = next(edge_iterator, None)
        stream.record_owner(owner_key(owner), first_member, member_count)
    if current_edge is not None:
        raise PTG2ManifestArtifactError("merged graph edge has no corresponding owner")
    return stream.finish()


@dataclass(frozen=True)
class _ValidatedShard:
    shard_id: str
    group_npi: _ValidatedArtifact
    npi_group: _ValidatedArtifact
    group_provider_set: _ValidatedArtifact
    provider_set_group: _ValidatedArtifact

    @property
    def artifacts(self) -> tuple[_ValidatedArtifact, ...]:
        """Return the shard's four reciprocal membership artifacts."""

        return (
            self.group_npi,
            self.npi_group,
            self.group_provider_set,
            self.provider_set_group,
        )


@dataclass(frozen=True)
class _ShardRuns:
    group_npi: Path
    npi_group: Path
    group_provider_set: Path
    provider_set_group: Path
    owner_npi: Path
    owner_group_npi: Path
    owner_group_provider_set: Path
    owner_provider_set: Path
    group_npi_input_edges: int
    group_provider_set_input_edges: int


def _metadata_shard_id(metadata: Mapping[str, object]) -> str | None:
    if not isinstance(metadata, Mapping):
        raise PTG2ManifestArtifactError("membership artifact metadata must be a mapping")
    identities = {
        str(metadata[field]).strip()
        for field in ("source_shard_id", "shard_id")
        if metadata.get(field) is not None and str(metadata[field]).strip()
    }
    if len(identities) > 1:
        raise PTG2ManifestArtifactError("membership artifact has contradictory shard identities")
    return next(iter(identities), None)


def _validate_shard_bundles(
    bundles: Sequence[SharedGraphShardBundle],
) -> tuple[_ValidatedShard, ...]:
    if not bundles:
        raise PTG2ManifestArtifactError("shared graph conversion requires at least one complete shard")
    validated_shards: list[_ValidatedShard] = []
    seen_ids: set[str] = set()
    expected_names = (
        ("group_npi", "provider_group_npi"),
        ("npi_group", "provider_npi_group"),
        ("group_provider_set", "provider_inverted"),
        ("provider_set_group", "provider_forward"),
    )
    for bundle in bundles:
        if not isinstance(bundle, SharedGraphShardBundle):
            raise PTG2ManifestArtifactError("shared graph shard bundle has an invalid type")
        shard_id = str(bundle.shard_id or "").strip()
        if not shard_id:
            raise PTG2ManifestArtifactError("shared graph shard identity is required")
        if shard_id in seen_ids:
            raise PTG2ManifestArtifactError(f"duplicate shared graph shard identity: {shard_id}")
        seen_ids.add(shard_id)
        raw_artifacts = tuple(getattr(bundle, field) for field, _expected_name in expected_names)
        if any(not isinstance(artifact, MembershipArtifact) for artifact in raw_artifacts):
            raise PTG2ManifestArtifactError(
                f"shared graph shard {shard_id} is incomplete; all four directions are required"
            )
        resolved_paths = [Path(artifact.path).resolve() for artifact in raw_artifacts]
        if len(set(resolved_paths)) != 4:
            raise PTG2ManifestArtifactError(
                f"shared graph shard {shard_id} reuses an artifact for multiple directions"
            )
        for artifact, (_field, expected_name) in zip(raw_artifacts, expected_names):
            metadata_id = _metadata_shard_id(artifact.metadata)
            if metadata_id is not None and metadata_id != shard_id:
                raise PTG2ManifestArtifactError(
                    f"shared graph shard identity mismatch: bundle={shard_id}, artifact={metadata_id}"
                )
            artifact_name = artifact.metadata.get("name")
            if artifact_name is not None and str(artifact_name) != expected_name:
                raise PTG2ManifestArtifactError(
                    f"shared graph shard {shard_id} has the wrong {expected_name} artifact"
                )
        checked_artifacts = tuple(
            _ValidatedArtifact(artifact) for artifact in raw_artifacts
        )
        validated_shards.append(_ValidatedShard(shard_id, *checked_artifacts))
    return tuple(validated_shards)


def _npi_sort_key(global_id: bytes) -> bytes:
    _npi_from_global_id(global_id)
    return global_id[8:]


def _build_shard_runs(
    shard: _ValidatedShard,
    *,
    shard_index: int,
    directory: Path,
    chunk_bytes: int,
) -> _ShardRuns:
    """Validate reciprocal shard artifacts and build sorted edge and owner runs."""

    prefix = f"shard-{shard_index:08d}"

    def edge_run(
        artifact: _ValidatedArtifact,
        suffix: str,
        record_size: int,
        encoder: Callable[[bytes, bytes], bytes],
    ) -> tuple[Path, int, int]:
        """Sort encoded edges and return the run path and record counts."""

        return _external_sorted_records(
            _artifact_records(artifact, encode=encoder),
            record_size=record_size,
            directory=directory,
            prefix=f"{prefix}.{suffix}",
            chunk_bytes=chunk_bytes,
        )

    group_npi, group_npi_input, group_npi_unique = edge_run(
        shard.group_npi,
        "group-npi",
        24,
        lambda group, npi: group + _npi_sort_key(npi),
    )
    npi_group, npi_group_input, npi_group_unique = edge_run(
        shard.npi_group,
        "npi-group-canonical",
        24,
        lambda npi, group: group + _npi_sort_key(npi),
    )
    if group_npi_input != npi_group_input:
        raise PTG2ManifestArtifactError(
            f"shared graph shard {shard.shard_id} group-npi directions have different edge counts"
        )
    _assert_sorted_runs_equal(
        group_npi,
        npi_group,
        left_count=group_npi_unique,
        right_count=npi_group_unique,
        description=f"shared graph shard {shard.shard_id} group-npi",
    )

    group_provider, group_provider_input, group_provider_unique = edge_run(
        shard.group_provider_set,
        "group-provider-set",
        32,
        lambda group, provider_set: group + provider_set,
    )
    provider_group, provider_group_input, provider_group_unique = edge_run(
        shard.provider_set_group,
        "provider-set-group-canonical",
        32,
        lambda provider_set, group: group + provider_set,
    )
    if group_provider_input != provider_group_input:
        raise PTG2ManifestArtifactError(
            f"shared graph shard {shard.shard_id} group-provider-set directions have different edge counts"
        )
    _assert_sorted_runs_equal(
        group_provider,
        provider_group,
        left_count=group_provider_unique,
        right_count=provider_group_unique,
        description=f"shared graph shard {shard.shard_id} group-provider-set",
    )

    def owner_run(
        artifact: _ValidatedArtifact,
        suffix: str,
        width: int,
        encoder: Callable[[bytes], bytes],
    ) -> Path:
        """Sort encoded owners and return their deduplicated run path."""

        path, _input_count, _unique_count = _external_sorted_records(
            _artifact_owner_records(artifact, encode=encoder),
            record_size=width,
            directory=directory,
            prefix=f"{prefix}.{suffix}",
            chunk_bytes=chunk_bytes,
        )
        return path

    return _ShardRuns(
        group_npi=group_npi,
        npi_group=npi_group,
        group_provider_set=group_provider,
        provider_set_group=provider_group,
        owner_npi=owner_run(shard.npi_group, "owners-npi", 8, _npi_sort_key),
        owner_group_npi=owner_run(shard.group_npi, "owners-group-npi", 16, lambda value: value),
        owner_group_provider_set=owner_run(
            shard.group_provider_set,
            "owners-group-provider-set",
            16,
            lambda value: value,
        ),
        owner_provider_set=owner_run(
            shard.provider_set_group,
            "owners-provider-set",
            16,
            lambda value: value,
        ),
        group_npi_input_edges=group_npi_input,
        group_provider_set_input_edges=group_provider_input,
    )


def convert_v3_provider_membership_shards_to_shared_graph(
    *,
    shards: Sequence[SharedGraphShardBundle],
    provider_set_key_by_global_id: Mapping[bytes | bytearray | memoryview | str, int] | str | Path,
    spill_directory: str | Path | None = None,
    external_sort_chunk_bytes: int = 8 * 1024 * 1024,
) -> SharedGraphConversionResult:
    """Merge complete V3 shard graphs into one deterministic shared graph."""

    if external_sort_chunk_bytes < 32:
        raise ValueError("external_sort_chunk_bytes must be at least 32")
    validated_shards = _validate_shard_bundles(shards)

    temporary_parent = Path(spill_directory) if spill_directory is not None else None
    if temporary_parent is not None:
        temporary_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix="ptg2-shared-graph-", dir=temporary_parent))
    provider_set_keys: _DiskDenseMap | None = None
    group_keys: _DiskDenseMap | None = None
    try:
        provider_map_path = temp_dir / "provider-set.map"
        provider_set_keys = _write_provider_set_map(
            provider_set_key_by_global_id,
            provider_map_path,
            chunk_bytes=external_sort_chunk_bytes,
        )
        shard_runs = tuple(
            _build_shard_runs(
                shard,
                shard_index=index,
                directory=temp_dir,
                chunk_bytes=external_sort_chunk_bytes,
            )
            for index, shard in enumerate(validated_shards)
        )

        group_npi_path = temp_dir / "global.group-npi.run"
        group_npi_unique = _merge_sorted_run_files(
            [runs.group_npi for runs in shard_runs],
            destination=group_npi_path,
            record_size=24,
        )
        npi_group_canonical_path = temp_dir / "global.npi-group-canonical.run"
        npi_group_unique = _merge_sorted_run_files(
            [runs.npi_group for runs in shard_runs],
            destination=npi_group_canonical_path,
            record_size=24,
        )
        _assert_sorted_runs_equal(
            group_npi_path,
            npi_group_canonical_path,
            left_count=group_npi_unique,
            right_count=npi_group_unique,
            description="globally merged group-npi",
        )

        group_provider_path = temp_dir / "global.group-provider-set.run"
        group_provider_unique = _merge_sorted_run_files(
            [runs.group_provider_set for runs in shard_runs],
            destination=group_provider_path,
            record_size=32,
        )
        provider_group_canonical_path = temp_dir / "global.provider-set-group-canonical.run"
        provider_group_unique = _merge_sorted_run_files(
            [runs.provider_set_group for runs in shard_runs],
            destination=provider_group_canonical_path,
            record_size=32,
        )
        _assert_sorted_runs_equal(
            group_provider_path,
            provider_group_canonical_path,
            left_count=group_provider_unique,
            right_count=provider_group_unique,
            description="globally merged group-provider-set",
        )

        npi_group_path, transposed_npi_group_count = _transpose_sorted_run(
            group_npi_path,
            source_owner_width=16,
            record_size=24,
            directory=temp_dir,
            prefix="global.npi-group",
            chunk_bytes=external_sort_chunk_bytes,
        )
        provider_group_path, transposed_provider_group_count = _transpose_sorted_run(
            group_provider_path,
            source_owner_width=16,
            record_size=32,
            directory=temp_dir,
            prefix="global.provider-set-group",
            chunk_bytes=external_sort_chunk_bytes,
        )
        if transposed_npi_group_count != group_npi_unique:
            raise PTG2ManifestArtifactError("transposed group-npi edge count changed")
        if transposed_provider_group_count != group_provider_unique:
            raise PTG2ManifestArtifactError("transposed group-provider-set edge count changed")

        owner_npi_path = temp_dir / "global.owners-npi.run"
        _merge_sorted_run_files(
            [runs.owner_npi for runs in shard_runs],
            destination=owner_npi_path,
            record_size=8,
        )
        owner_group_npi_path = temp_dir / "global.owners-group-npi.run"
        _merge_sorted_run_files(
            [runs.owner_group_npi for runs in shard_runs],
            destination=owner_group_npi_path,
            record_size=16,
        )
        owner_group_provider_path = temp_dir / "global.owners-group-provider-set.run"
        _merge_sorted_run_files(
            [runs.owner_group_provider_set for runs in shard_runs],
            destination=owner_group_provider_path,
            record_size=16,
        )
        owner_provider_path = temp_dir / "global.owners-provider-set.run"
        _merge_sorted_run_files(
            [runs.owner_provider_set for runs in shard_runs],
            destination=owner_provider_path,
            record_size=16,
        )

        group_ids_path = temp_dir / "provider-group.ids"
        group_count = _merge_sorted_run_files(
            [
                *[runs.owner_group_npi for runs in shard_runs],
                *[runs.owner_group_provider_set for runs in shard_runs],
            ],
            destination=group_ids_path,
            record_size=16,
        )
        if group_count > _UINT32_MAX + 1:
            raise PTG2ManifestArtifactError("provider group dictionary exceeds uint32 capacity")
        group_map_path = temp_dir / "provider-group.map"
        with (
            group_ids_path.open("rb") as group_id_input,
            group_map_path.open("wb") as destination,
        ):
            for key, global_id in enumerate(iter(lambda: group_id_input.read(16), b"")):
                if len(global_id) != 16:
                    raise RuntimeError("temporary provider-group ID run is truncated")
                destination.write(_DENSE_MAP_RECORD.pack(global_id, key))
        group_keys = _DiskDenseMap(group_map_path, label="provider-group", expected_start=(0,))

        def provider_set_key(global_id: bytes) -> int:
            """Resolve a provider-set key, raising a manifest error when absent."""

            try:
                return provider_set_keys.key(global_id)
            except KeyError as exc:
                raise PTG2ManifestArtifactError(
                    "provider membership references a provider set absent from the "
                    "authoritative dictionary"
                ) from exc

        block_copy_path = temp_dir / "graph-blocks.copy"
        owner_copy_path = temp_dir / "graph-owners.copy"
        group_copy_path = temp_dir / "provider-groups.copy"
        npi_copy_path = temp_dir / "npi-scope.copy"
        block_spool_path = temp_dir / "graph-blocks.spool"
        owner_spool_path = temp_dir / "graph-owners.spool"
        reference_path = temp_dir / "graph-references.run"
        support_hasher = hashlib.sha256(b"PTG2V3GRAPHSUPPORT\x01")
        with (
            _PostgresBinaryCopyWriter(block_copy_path) as block_copy,
            _PostgresBinaryCopyWriter(owner_copy_path) as owner_copy,
            block_spool_path.open("wb") as block_spool,
            owner_spool_path.open("wb") as owner_spool,
            reference_path.open("wb") as reference_spool,
        ):
            outputs = (
                _emit_sorted_direction(
                    npi_group_path,
                    owner_npi_path,
                    direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
                    owner_key=lambda value: int.from_bytes(value, "big", signed=False),
                    member_key=group_keys.key,
                    owner_width=8,
                    member_width=16,
                    block_copy=block_copy,
                    owner_copy=owner_copy,
                    block_spool=block_spool,
                    owner_spool=owner_spool,
                    reference_spool=reference_spool,
                    support_hasher=support_hasher,
                ),
                _emit_sorted_direction(
                    group_npi_path,
                    owner_group_npi_path,
                    direction=PTG2_V3_GRAPH_GROUP_TO_NPI,
                    owner_key=group_keys.key,
                    member_key=lambda value: int.from_bytes(value, "big", signed=False),
                    owner_width=16,
                    member_width=8,
                    block_copy=block_copy,
                    owner_copy=owner_copy,
                    block_spool=block_spool,
                    owner_spool=owner_spool,
                    reference_spool=reference_spool,
                    support_hasher=support_hasher,
                ),
                _emit_sorted_direction(
                    group_provider_path,
                    owner_group_provider_path,
                    direction=PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
                    owner_key=group_keys.key,
                    member_key=provider_set_key,
                    owner_width=16,
                    member_width=16,
                    block_copy=block_copy,
                    owner_copy=owner_copy,
                    block_spool=block_spool,
                    owner_spool=owner_spool,
                    reference_spool=reference_spool,
                    support_hasher=support_hasher,
                ),
                _emit_sorted_direction(
                    provider_group_path,
                    owner_provider_path,
                    direction=PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
                    owner_key=provider_set_key,
                    member_key=group_keys.key,
                    owner_width=16,
                    member_width=16,
                    block_copy=block_copy,
                    owner_copy=owner_copy,
                    block_spool=block_spool,
                    owner_spool=owner_spool,
                    reference_spool=reference_spool,
                    support_hasher=support_hasher,
                ),
            )

        with _PostgresBinaryCopyWriter(group_copy_path) as group_copy:
            for global_id, key in (
                _DENSE_MAP_RECORD.unpack(dense_map_record)
                for dense_map_record in _iter_fixed_records(
                    group_map_path,
                    _DENSE_MAP_RECORD.size,
                )
            ):
                group_copy.write_row((struct.pack(">i", key), global_id))
                support_hasher.update(struct.pack(">I", key))
                support_hasher.update(global_id)
        with _PostgresBinaryCopyWriter(npi_copy_path) as npi_copy:
            for raw_npi in _iter_fixed_records(owner_npi_path, 8):
                npi = int.from_bytes(raw_npi, "big", signed=False)
                npi_copy.write_row((struct.pack(">q", npi),))
                support_hasher.update(struct.pack(">Q", npi))
        provider_set_keys.close()
        group_keys.close()

        group_npi_input = sum(runs.group_npi_input_edges for runs in shard_runs)
        group_provider_input = sum(runs.group_provider_set_input_edges for runs in shard_runs)
        edge_metrics = (
            SharedGraphEdgeMetrics(
                edge_kind="group_npi",
                input_edge_count=group_npi_input,
                unique_edge_count=group_npi_unique,
                duplicate_edge_count=group_npi_input - group_npi_unique,
            ),
            SharedGraphEdgeMetrics(
                edge_kind="group_provider_set",
                input_edge_count=group_provider_input,
                unique_edge_count=group_provider_unique,
                duplicate_edge_count=group_provider_input - group_provider_unique,
            ),
        )
        direction_metrics = tuple(outputs)
        input_bytes = sum(
            artifact.byte_count for shard in validated_shards for artifact in shard.artifacts
        )
        raw_block_bytes = sum(metric.raw_byte_count for metric in direction_metrics)
        input_edges = group_npi_input + group_provider_input
        unique_edges = group_npi_unique + group_provider_unique
        return SharedGraphConversionResult(
            scratch_directory=temp_dir,
            block_copy_path=block_copy_path,
            owner_copy_path=owner_copy_path,
            group_copy_path=group_copy_path,
            npi_copy_path=npi_copy_path,
            block_spool_path=block_spool_path,
            owner_spool_path=owner_spool_path,
            group_map_path=group_map_path,
            reference_path=reference_path,
            block_count=sum(metric.block_count for metric in direction_metrics),
            owner_count=sum(metric.owner_count for metric in direction_metrics),
            provider_group_count=group_count,
            npi_count=owner_npi_path.stat().st_size // 8,
            support_digest=support_hasher.digest(),
            direction_metrics=direction_metrics,
            edge_metrics=edge_metrics,
            input_byte_count=input_bytes,
            raw_block_byte_count=raw_block_bytes,
            stored_block_byte_count=raw_block_bytes,
            integrity=SharedGraphIntegrityMetrics(
                shard_count=len(validated_shards),
                artifact_count=len(validated_shards) * 4,
                checksum_byte_count=input_bytes,
                reciprocal_pair_count=2,
                reciprocal_edge_count=unique_edges,
                input_edge_count=input_edges,
                unique_edge_count=unique_edges,
                duplicate_edge_count=input_edges - unique_edges,
            ),
        )
    except BaseException:
        if provider_set_keys is not None:
            provider_set_keys.close()
        if group_keys is not None:
            group_keys.close()
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


def convert_v3_provider_memberships_to_shared_graph(
    *,
    group_npi: MembershipArtifact,
    npi_group: MembershipArtifact,
    group_provider_set: MembershipArtifact,
    provider_set_group: MembershipArtifact,
    provider_set_key_by_global_id: Mapping[bytes | bytearray | memoryview | str, int],
    spill_directory: str | Path | None = None,
    external_sort_chunk_bytes: int = 8 * 1024 * 1024,
) -> SharedGraphConversionResult:
    """Convert one complete V3 shard using the global shard merge path."""

    artifacts = (group_npi, npi_group, group_provider_set, provider_set_group)
    metadata_ids = {
        identity
        for artifact in artifacts
        if isinstance(artifact, MembershipArtifact)
        for identity in (_metadata_shard_id(artifact.metadata),)
        if identity is not None
    }
    if len(metadata_ids) > 1:
        raise PTG2ManifestArtifactError("single shared graph bundle has contradictory shard identities")
    shard_id = next(iter(metadata_ids), "single-shard")
    return convert_v3_provider_membership_shards_to_shared_graph(
        shards=(
            SharedGraphShardBundle(
                shard_id=shard_id,
                group_npi=group_npi,
                npi_group=npi_group,
                group_provider_set=group_provider_set,
                provider_set_group=provider_set_group,
            ),
        ),
        provider_set_key_by_global_id=provider_set_key_by_global_id,
        spill_directory=spill_directory,
        external_sort_chunk_bytes=external_sort_chunk_bytes,
    )


__all__ = [
    "MembershipArtifact",
    "SharedGraphConversionResult",
    "SharedGraphDirectionMetrics",
    "SharedGraphEdgeMetrics",
    "SharedGraphIntegrityMetrics",
    "SharedGraphOwnerRow",
    "SharedGraphShardBundle",
    "convert_v3_provider_membership_shards_to_shared_graph",
    "convert_v3_provider_memberships_to_shared_graph",
]
