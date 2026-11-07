# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One global raw-key pass, followed by immutable inputs for parallel reducers."""

import hashlib
import itertools
import os
import stat
import struct
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from process.custom_import import family_shard_spool as spool
from process.custom_import.family_shard_facts import (
    SourceFactBudgetExceeded,
    SourceFactLimits,
    _family_order,
    _pack,
    _prepared_families,
    _routed_records,
    _sorted,
    _Work,
)
from process.custom_import.family_shard_input import manifest_document, read_partition
from process.custom_import.family_shard_source_plan import _plan_digest


@dataclass(frozen=True)
class RoutedShard:
    """Picklable closed-file custody; no database or publication authority."""

    root: str
    root_device: int
    root_inode: int
    name: str
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    records: int
    shard_id: int
    shard_count: int
    partition_sha256: str
    definition_sha256: str
    max_record_bytes: int


def _bucket(row, count):
    typed = row[0]
    return 0 if typed is None else int.from_bytes(typed[1][:8], "big") % count


def _write_shard(work, routed_rows, shard_id, shard_count, partition, definition):
    path, occurrence_count = work.path(), 0

    def charge(count):
        """Reserve physical bytes in this producer's existing aggregate budget."""
        work.charge(count)
        work.paths[path] += count

    with path.open("xb") as spool_output:
        stored = spool.BlockWriter(spool_output, charge)
        for routed_row in routed_rows:
            if work._io_check is not None:
                work._io_check()
            record_bytes = _pack(routed_row)
            if len(record_bytes) > work.limits.max_record_bytes:
                raise SourceFactBudgetExceeded("source fact singleton exceeds the resource budget")
            stored.write(struct.pack(">Q", len(record_bytes)))
            stored.write(record_bytes)
            occurrence_count += 1
        stored.finish()
        spool_output.flush()
        os.fchmod(spool_output.fileno(), 0o400)
        os.fsync(spool_output.fileno())
    observed = path.stat()
    with path.open("rb") as spool_file:
        digest = _plan_digest(spool_file, work._io_check)
    return RoutedShard(
        str(work.root),
        work.identity.st_dev,
        work.identity.st_ino,
        path.name,
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        digest,
        occurrence_count,
        shard_id,
        shard_count,
        partition.sha256,
        definition.digest,
        work.limits.max_record_bytes,
    )


@contextmanager
def route_capture(partition, definition, *, root, register_cleanup, limits, check=None):
    """Finish global raw-key reconciliation before exposing any worker input.

    Invalid raw-equal records must join valid parents before typed partitioning.
    Empty shards are retained so every scheduled shard has a completion witness.
    The caller keeps this context alive until every worker has stopped reading.
    Run off the owner loop with a stop/deadline check and drain before cleanup.
    """
    work, routed = _produce_route(partition, definition, root, register_cleanup, limits, check)
    try:
        yield routed
    finally:
        work.close()


def prepare_routed_capture(partition, definition, *, root, register_cleanup, limits, check=None):
    """Return closed canonical files retained by the registered terminal owner."""
    _work, routed = _produce_route(partition, definition, root, register_cleanup, limits, check)
    return routed


def _produce_route(partition, definition, root, register_cleanup, limits, check):
    if type(limits) is not SourceFactLimits:
        raise ValueError("routed source budgets differ")
    document = manifest_document(partition)
    if partition.identity.definition_sha256 != definition.digest:
        raise ValueError("routed source definition differs")
    count = document["limits"]["shard_count"]
    work = _Work(root, limits, register_cleanup, check=check)
    ordered = _sorted(
        work,
        _routed_records(work, read_partition(partition, check=check), document["records"], route_merge=True),
        lambda row: (_bucket(row, count), _family_order(row)),
        route_key="family",
    )
    routed_shards, total, is_complete = [], 0, False
    try:
        groups = iter(itertools.groupby(ordered, lambda row: _bucket(row, count)))
        pending = next(groups, None)
        for shard_id in range(count):
            routed_rows = ()
            if pending is not None and pending[0] == shard_id:
                routed_rows = pending[1]
            shard = _write_shard(work, routed_rows, shard_id, count, partition, definition)
            routed_shards.append(shard)
            total += shard.records
            if pending is not None and pending[0] == shard_id:
                pending = next(groups, None)
        if pending is not None or total != document["records"]:
            raise ValueError("routed source complete census differs")
        ordered.close()
        is_complete = True
        return work, tuple(routed_shards)
    finally:
        if not is_complete:
            try:
                ordered.close()
            finally:
                work.close()


def read_routed_shard(shard, work):
    """Verify the same opened file before and after consuming its sorted rows."""
    if type(shard) is not RoutedShard or not 0 <= shard.shard_id < shard.shard_count:
        raise ValueError("routed shard identity differs")
    root = Path(shard.root)
    observed = root.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or (observed.st_dev, observed.st_ino) != (shard.root_device, shard.root_inode)
        or Path(shard.name).name != shard.name
    ):
        raise ValueError("routed shard directory custody differs")
    descriptor = os.open(root / shard.name, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as spool_input:
        observed = os.fstat(spool_input.fileno())
        if not stat.S_ISREG(observed.st_mode) or (
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            stat.S_IMODE(observed.st_mode),
        ) != (shard.file_device, shard.file_inode, shard.file_bytes, 0o400):
            raise ValueError("routed shard file custody differs")
        if hashlib.file_digest(spool_input, "sha256").digest() != shard.sha256:
            raise ValueError("routed shard digest differs")
        spool_input.seek(0)
        previous, count = None, 0
        logical = spool.logical_reader(spool_input, check=getattr(work, "_io_check", None))
        for routed_row in work.read(logical):
            order = _family_order(routed_row)
            if _bucket(routed_row, shard.shard_count) != shard.shard_id or (previous is not None and order <= previous):
                raise ValueError("routed shard family order differs")
            previous, count = order, count + 1
            yield routed_row
        if count != shard.records:
            raise ValueError("routed shard occurrence count differs")
        spool_input.seek(0)
        if hashlib.file_digest(spool_input, "sha256").digest() != shard.sha256:
            raise ValueError("routed shard changed while reading")


@contextmanager
def prepare_routed_families(shard, definition, *, root, register_cleanup, limits):
    """Run the unchanged complete family reducer inside one CPU worker."""
    if shard.definition_sha256 != definition.digest or limits.max_record_bytes < shard.max_record_bytes:
        raise ValueError("routed family scope or record budget differs")
    work = _Work(root, limits, register_cleanup)
    families = _prepared_families(work, definition, read_routed_shard(shard, work))
    try:
        yield families
    finally:
        try:
            families.close()
        finally:
            work.close()
