# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Group a bulk export's prepared merged roles on local disk, so each merged role is staged once."""

from __future__ import annotations

import json
import os
import shutil
import zlib
from collections.abc import Iterable, Iterator
from compression import zstd
from pathlib import Path
from typing import Any

import orjson

from process.provider_directory_role_merge import RoleObservationUnion, finish_merged_roles

# 510 million source roles / 2048 = about 250 thousand per partition, few enough to reduce in memory.
PARTITIONS = 2048
# A partition's buffer becomes one independent zstd frame at this size; larger frames compress repeats better.
FRAME_BYTES = 1 << 20
# Bounds one writer's memory; reaching it flushes every buffer, so the cost stays one pass per cap.
BUFFER_CAP_BYTES = 256 << 20
_ZSTD_LEVEL = 3


def partition_of(role_id: str, partitions: int = PARTITIONS) -> int:
    """Stable across processes and runs, unlike hash()."""
    return zlib.crc32(role_id.encode()) % partitions


def _encoded(prepared_row: dict[str, Any]) -> bytes:
    """Source values keep their exact JSON: orjson would turn wide integers into floats and NaN into null."""
    try:
        # ASCII escapes keep a lone surrogate, which UTF-8 cannot encode.
        return json.dumps(prepared_row, allow_nan=False, separators=(",", ":")).encode() + b"\n"
    except ValueError as exc:
        raise ValueError("provider_directory_bulk_group_non_finite_number") from exc


def _write_atomically(path: Path, content: bytes) -> None:
    pending_path = path.with_name(path.name + ".pending")
    with open(pending_path, "wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(pending_path, path)
    _fsync_directory(path.parent)


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _done_path(directory: Path, output_index: int) -> Path:
    return Path(directory) / f"{output_index}.done"


def is_grouped(directory: Path, output_index: int) -> bool:
    """Whether an output's group files are complete and durable."""
    return _done_path(directory, output_index).exists()


def mark_loaded(directory: Path, partition: int) -> None:
    """Record, after its commit, that a partition's merged roles are staged."""
    _write_atomically(Path(directory) / f"{partition}.loaded", b"")


def is_loaded(directory: Path, partition: int) -> bool:
    """Whether a partition's merged roles were staged and committed."""
    return (Path(directory) / f"{partition}.loaded").exists()


def discard(directory: Path) -> None:
    """Delete every group file and marker of one acquisition."""
    shutil.rmtree(directory, ignore_errors=True)


class GroupWriter:
    """Write the prepared roles of ONE saved output into per-partition files; one writer per worker task."""

    def __init__(self, directory: Path, output_index: int, partitions: int = PARTITIONS) -> None:
        self.directory = Path(directory)
        self.output_index = output_index
        self.partitions = partitions
        self.output_directory = self.directory / str(output_index)
        if is_grouped(self.directory, output_index):
            raise ValueError("provider_directory_bulk_group_output_already_grouped")
        # Frames are appended, so a crashed attempt's files must go before this one writes.
        shutil.rmtree(self.output_directory, ignore_errors=True)
        self.output_directory.mkdir(parents=True)
        self.buffer_by_partition: dict[int, bytearray] = {}
        self.written_partitions: set[int] = set()
        self.buffered_bytes = 0
        self.row_count = 0

    def write(self, prepared_rows: Iterable[dict[str, Any]]) -> None:
        """Buffer prepared rows by partition; full buffers become frames on disk."""
        for prepared_row in prepared_rows:
            partition = partition_of(prepared_row["resource_id"], self.partitions)
            line = _encoded(prepared_row)
            buffer = self.buffer_by_partition.setdefault(partition, bytearray())
            buffer += line
            self.buffered_bytes += len(line)
            self.row_count += 1
            if len(buffer) >= FRAME_BYTES:
                self._flush(partition)
            if self.buffered_bytes >= BUFFER_CAP_BYTES:
                self._flush_all()

    def _flush_all(self) -> None:
        for partition in list(self.buffer_by_partition):
            self._flush(partition)

    def _flush(self, partition: int) -> None:
        buffer = self.buffer_by_partition.pop(partition)
        # Open per frame: one handle per partition would exceed common open-file limits.
        with open(self.output_directory / f"{partition}.zst", "ab") as handle:
            handle.write(zstd.compress(bytes(buffer), _ZSTD_LEVEL))
        self.buffered_bytes -= len(buffer)
        self.written_partitions.add(partition)

    def finish(self, source_lines: int) -> None:
        """Make every frame durable, then mark the output grouped."""
        self._flush_all()
        for partition in self.written_partitions:
            with open(self.output_directory / f"{partition}.zst", "rb+") as handle:
                os.fsync(handle.fileno())
        _fsync_directory(self.output_directory)
        marker_by_field = {"rows": self.row_count, "source_lines": source_lines, "partitions": self.partitions}
        _write_atomically(_done_path(self.directory, self.output_index), orjson.dumps(marker_by_field))


def _grouped_lines(directory: Path, output_index: int, partition: int, partitions: int) -> Iterator[bytes]:
    """Lines one fully grouped output holds for one partition."""

    if not is_grouped(directory, output_index):
        raise ValueError("provider_directory_bulk_group_output_not_grouped")
    if orjson.loads(_done_path(directory, output_index).read_bytes())["partitions"] != partitions:
        raise ValueError("provider_directory_bulk_group_partitions_changed")
    path = directory / str(output_index) / f"{partition}.zst"
    if path.exists():
        with zstd.open(path, "rb") as handle:
            yield from handle


def reduce_partition(
    directory: Path,
    partition: int,
    output_indexes: Iterable[int],
    resource_hash_contract: str,
    partitions: int = PARTITIONS,
) -> list[dict[str, Any]]:
    """One row per merged role of one partition, sorted by role id whatever the order of the outputs."""

    template_row: dict[str, Any] | None = None
    union_by_role_id: dict[str, RoleObservationUnion] = {}
    for output_index in output_indexes:
        for line in _grouped_lines(Path(directory), output_index, partition, partitions):
            prepared_row = json.loads(line)
            observation_by_field = prepared_row.pop("payload_json")
            del prepared_row["resource_id"], prepared_row["payload_hash"]
            if template_row is None:
                template_row = prepared_row
            elif prepared_row != template_row:
                raise ValueError("provider_directory_bulk_group_dataset_changed")
            union = union_by_role_id.get(observation_by_field["resource_id"])
            if union is None:
                union_by_role_id[observation_by_field["resource_id"]] = RoleObservationUnion(observation_by_field)
            else:
                union.add(observation_by_field)
    payload_by_role_id = {role_id: union.merged_payload() for role_id, union in union_by_role_id.items()}
    return finish_merged_roles(template_row or {}, payload_by_role_id, resource_hash_contract)
