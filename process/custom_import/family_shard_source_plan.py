# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded original-order SOURCE plans; no database authority or ID allocation."""

import hashlib
import os
import shutil
import stat
import struct
from compression import zstd
from contextlib import closing
from dataclasses import asdict, dataclass
from operator import itemgetter
from pathlib import Path
from uuid import UUID

import msgpack

from process.custom_import.build_source import _SourcePageReducer, _StreamContext
from process.custom_import.bulk_page_codec import (
    LandingBatch,
    LandingRow,
    encode_landing_batch,
    landing_manifest_sha256,
)
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits, _sorted, _Work
from process.custom_import.family_shard_input import manifest_document, read_partition
from process.custom_import.family_shard_input_records import decode_record, encode_record
from process.custom_import.source_decoded_preparation import DecodedSourcePreparation

_CONTRACT = "custom-import/source-allocation-plan/v1"
_FILE = "source-plan.mpk"
_UUID = UUID(int=0)  # Neither original pack nor landing manifest hashes include it.
_LENGTH = struct.Struct("!Q")
_COMPRESSED_PACK = "source-pack-zstd/v1"


@dataclass(frozen=True)
class SourcePlanDescriptor:
    """Only primitive, process-picklable custody/identity fields; no borrowed iterators."""

    root: str
    device: int
    inode: int
    file_device: int
    file_inode: int
    file_bytes: int
    sha256: bytes
    partition_sha256: str
    scope: tuple
    stream_slots: tuple
    collection_slots: tuple
    source_counts: tuple
    counts: tuple
    max_frame_bytes: int


@dataclass(frozen=True)
class PlannedSourcePack:
    """One ORIGINAL bounded SOURCE page, including all initially rejected rows."""

    pack_index: int
    stream_slot: int
    collection_slot: int
    first_rejection_ordinal: int
    records: tuple
    landing: LandingBatch

    @property
    def pack_ordinal(self):
        """Return the original stream-local SOURCE page ordinal."""
        return self.landing.records[0].pack_ordinal

    @property
    def pack_sha256(self):
        """Return the unchanged original SOURCE pack commitment."""
        return self.landing.records[0].pack_sha256

    @property
    def allocation_kinds(self):
        """Every initially valid revision, without admission/collapse filtering."""
        kind = 1 if self.collection_slot == 0 else 2
        return tuple(kind if row.rejection_code is None else None for row in self.landing.records)

    @property
    def initial_rejection_ordinals(self):
        """Keep initial rejection ordinals in original occurrence order."""
        ordinal = self.first_rejection_ordinal
        ordinals = []
        for row in self.landing.records:
            ordinals.append(None if row.rejection_code is None else ordinal)
            ordinal += int(row.rejection_code is not None)
        return tuple(ordinals)


def _scope(partition, request):
    identity = partition.identity
    if (
        (identity.dataset_id, identity.definition_revision_id, identity.schema_revision_id)
        != (request.dataset_id, request.definition_revision_id, request.schema_revision_id)
        or identity.definition_sha256 != request.definition.digest
        or identity.schema_sha256 != request.definition.schema_digest
    ):
        raise ValueError("SOURCE plan capture/request identity differs")
    return (
        tuple(sorted(asdict(identity).items())),
        request.execution_id,
        request.fence,
        producer_token_sha256(request),
        request.page_row_limit,
        request.page_byte_limit,
    )


def _inputs(partition, check=None):
    # Include the invalid typed-key lane once. Raw-key reconciliation happens
    # elsewhere; SOURCE allocation/rejection order may never omit this lane.
    with closing(read_partition(partition, check=check)) as rows:
        for record in rows:
            yield (record.stream_slot, record.part_ordinal, record.row_ordinal), encode_record(record)


def _page_entry(context, page, originals, counters, pack_ordinal):
    landing = encode_landing_batch(
        context,
        (page,),
        batch_id=_UUID,
        first_pack_ordinal=pack_ordinal,
        row_limit=context.request.page_row_limit,
        byte_limit=context.request.page_byte_limit,
    )
    pack_index, first_rejection = counters[1], counters[4]
    for row in landing.records:
        counters[0] += 1
        if row.rejection_code is None:
            counters[2 if context.collection_slot == 0 else 3] += 1
        else:
            counters[4] += 1
    counters[1] += 1
    return (
        "pack",
        pack_index,
        context.stream_slot,
        context.collection_slot,
        first_rejection,
        landing.byte_count,
        landing.manifest_sha256,
        tuple(encode_record(record) for record in originals),
        tuple(tuple(row[1:]) for row in landing.records),
    )


def _stored_entry(entry, limits):
    if entry[0] != "pack":
        return entry
    raw = msgpack.packb(entry, use_bin_type=True)
    if len(raw) > limits.max_record_bytes:
        raise SourceFactBudgetExceeded("source plan singleton exceeds the resource budget")
    frame = zstd.compress(
        raw,
        options={zstd.CompressionParameter.compression_level: 3, zstd.CompressionParameter.checksum_flag: True},
    )
    stored = (_COMPRESSED_PACK, len(raw), frame)
    return stored if len(msgpack.packb(stored, use_bin_type=True)) < len(raw) else entry


def _decoded_entry(entry, maximum):
    if not isinstance(entry, tuple) or not entry or entry[0] != _COMPRESSED_PACK:
        return entry
    if len(entry) != 3 or type(entry[1]) is not int or not 0 < entry[1] <= maximum or type(entry[2]) is not bytes:
        raise ValueError("SOURCE compressed plan bounds differ")
    raw_size, frame = entry[1:]
    window_log = min(31, max(10, (raw_size - 1).bit_length()))
    decoder = zstd.ZstdDecompressor(options={zstd.DecompressionParameter.window_log_max: window_log})
    try:
        raw = decoder.decompress(frame, max_length=raw_size + 1)
    except zstd.ZstdError as error:
        raise ValueError("SOURCE compressed plan is malformed") from error
    if len(raw) != raw_size or not decoder.eof or decoder.unused_data:
        raise ValueError("SOURCE compressed plan is incomplete or oversized")
    decoded = msgpack.unpackb(raw, raw=False, use_list=False, strict_map_key=True)
    if not isinstance(decoded, tuple) or not decoded or decoded[0] != "pack":
        raise ValueError("SOURCE compressed plan record kind differs")
    return decoded


def _source_contexts(request, registry):
    contexts_by_slot = {
        slot: _StreamContext(request, registry, 0, stream)
        for slot, stream in enumerate(request.definition.source_streams, 1)
    }
    if registry.stream_slots != {context.stream.stream_id: slot for slot, context in contexts_by_slot.items()}:
        raise ValueError("SOURCE plan stream registry differs")
    collection_slots_by_name = {
        collection.name: slot for slot, collection in enumerate(request.definition.child_collections, 1)
    }
    root_slot = next(slot for slot, context in contexts_by_slot.items() if context.stream.child_collection is None)
    native_slots = tuple(registry.child_collection_slots.values())
    if (
        set(registry.child_collection_slots) != set(collection_slots_by_name)
        or any(type(slot) is not int or slot <= 0 for slot in native_slots)
        or len(native_slots) != len(set(native_slots))
        or registry.root_stream_slot != root_slot
    ):
        raise ValueError("SOURCE plan collection registry differs")
    return contexts_by_slot, collection_slots_by_name


def _check_part(part_evidence, previous_part, contexts_by_slot):
    slot, part, count = part_evidence
    if any(type(number) is not int or number < minimum for number, minimum in ((slot, 1), (part, 1), (count, 0))):
        raise ValueError("SOURCE part evidence is malformed")
    if slot not in contexts_by_slot or (previous_part is not None and (slot, part) <= previous_part):
        raise ValueError("SOURCE part scope/order differs")
    expected_part = previous_part[1] + 1 if previous_part is not None and previous_part[0] == slot else 1
    if part != expected_part:
        raise ValueError("SOURCE part evidence has a gap")


def _check_prepared_occurrence(occurrence, prepared):
    if (
        prepared.raw_key != occurrence.raw_parent_key
        or prepared.typed_key != occurrence.typed_parent_key
        or prepared.child_key != occurrence.child_key
        or (None if prepared.rejection is None else prepared.rejection.code) != occurrence.rejection_code
    ):
        raise ValueError("SOURCE partition row differs from original preparation")


def _part_occurrences(ordered, cursor_entries, part_scope, check):
    slot, part, count, source_start, collection_slot = part_scope
    for ordinal in range(count):
        if check is not None:
            check()
        current = cursor_entries[0]
        if current is None or tuple(current[0]) != (slot, part, ordinal):
            raise ValueError("SOURCE occurrence/part coverage differs")
        occurrence = decode_record(current[1])
        if occurrence.source_ordinal != source_start + ordinal or occurrence.collection_slot != collection_slot:
            raise ValueError("SOURCE occurrence stream or ordinal differs")
        yield occurrence, len(current[1])
        cursor_entries[0] = next(ordered, None)


def _planned_part_entries(context, preparation, entries, part, source_start, counters, pack_ordinals):
    slot = context.stream_slot
    reducer = _SourcePageReducer(context, part, 0, source_start)
    originals = []
    with closing(preparation.rows(entries)) as prepared_rows:
        for occurrence, prepared in prepared_rows:
            _check_prepared_occurrence(occurrence, prepared)
            page = reducer.before_append(prepared)
            if page is not None:
                yield _page_entry(context, page, originals, counters, pack_ordinals[slot])
                pack_ordinals[slot] += 1
                originals = []
            reducer.append(prepared)
            originals.append(occurrence)
    if reducer.records:
        yield _page_entry(context, reducer.page(), originals, counters, pack_ordinals[slot])
        pack_ordinals[slot] += 1


def _planned_entries(work, partition, document, contexts_by_slot, collection_slots_by_name, parts, counters, check):
    source_counts_by_slot = dict.fromkeys(contexts_by_slot, 0)
    pack_ordinals = dict.fromkeys(contexts_by_slot, 0)
    previous_part = None
    preparation = None
    previous_slot = None
    with closing(_sorted(work, _inputs(partition, check), key=itemgetter(0))) as ordered:
        cursor_entries = [next(ordered, None)]
        for slot, part, count in parts:
            _check_part((slot, part, count), previous_part, contexts_by_slot)
            context = contexts_by_slot[slot]
            if slot != previous_slot:
                preparation = DecodedSourcePreparation(context, memory_bytes=work.limits.memory_bytes, check=check)
                previous_slot = slot
            yield "part", slot, part, count
            part_scope = (
                slot,
                part,
                count,
                source_counts_by_slot[slot],
                collection_slots_by_name.get(context.stream.child_collection, 0),
            )
            entries = _part_occurrences(ordered, cursor_entries, part_scope, check)
            yield from _planned_part_entries(
                context, preparation, entries, part, source_counts_by_slot[slot], counters, pack_ordinals
            )
            source_counts_by_slot[slot] += count
            counters[5] += 1
            previous_part = slot, part
        _check_source_census(cursor_entries[0], source_counts_by_slot, counters, document)


def _check_source_census(current, source_counts_by_slot, counters, document):
    declared_counts_by_slot = {int(slot): count for slot, count in document["source_records"].items()}
    if (
        current is not None
        or source_counts_by_slot != declared_counts_by_slot
        or counters[0] != document["records"]
        or counters[5] != document["parts"]
    ):
        raise ValueError("SOURCE complete capture census differs")


def build_source_plan(partition, request, registry, parts, *, root, register_cleanup, limits, check=None):
    """Restore original SOURCE order and publish only a fully completed spool.

    Parts are the pinned sealed (stream_slot,part_ordinal,record_count) metadata,
    including zero-row parts. Limits govern temporary resources, never different
    accepted record/page limits. The caller owns allocation and database mutations.
    """
    if type(limits) is not SourceFactLimits:
        raise ValueError("SOURCE plan requires existing explicit spool budgets")
    if check is not None:
        if not callable(check):
            raise ValueError("SOURCE plan cancellation check differs")
        check()
    document = manifest_document(partition)
    scope = _scope(partition, request)
    stream_slots = tuple(sorted(registry.stream_slots.items()))
    collection_slots = tuple(sorted(registry.child_collection_slots.items()))
    source_counts = tuple(sorted((int(slot), count) for slot, count in document["source_records"].items()))
    work = _Work(root, limits, register_cleanup, check=check)
    counters = [0] * 6  # occurrences, packs, root allocations, child allocations, initial rejections, parts
    try:
        parts = iter(parts)

        def _entries_to_write():
            yield "header", _CONTRACT, partition.sha256, scope, stream_slots, collection_slots, source_counts
            contexts_by_slot, collection_slots_by_name = _source_contexts(request, registry)
            for entry in _planned_entries(
                work, partition, document, contexts_by_slot, collection_slots_by_name, parts, counters, check
            ):
                yield _stored_entry(entry, limits)
            yield "complete", tuple(counters)

        try:
            path = work.write(_entries_to_write())
        finally:
            close = getattr(parts, "close", None)
            if close is not None:
                close()
        ready, digest = _complete_plan_file(work, path, check)
        info = ready.stat()
        return SourcePlanDescriptor(
            str(work.root),
            work.identity.st_dev,
            work.identity.st_ino,
            info.st_dev,
            info.st_ino,
            info.st_size,
            digest,
            partition.sha256,
            scope,
            stream_slots,
            collection_slots,
            source_counts,
            tuple(counters),
            limits.max_record_bytes,
        )
    except BaseException:
        work.close()
        raise


def _complete_plan_file(work, path, check):
    ready = work.root / _FILE
    os.chmod(path, 0o400)
    with path.open("rb") as plan_file:
        os.fsync(plan_file.fileno())
        digest = _plan_digest(plan_file, check)
    os.link(path, ready, follow_symlinks=False)
    path.unlink()
    work.paths[ready] = work.paths.pop(path)
    fd = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return ready, digest


def _plan_digest(source, check):
    digest = hashlib.sha256()
    while True:
        if check is not None:
            check()
        block = source.read(65536)
        if not block:
            return digest.digest()
        digest.update(block)


def _owned_file(plan):
    if type(plan) is not SourcePlanDescriptor:
        raise ValueError("SOURCE plan descriptor differs")
    root = Path(plan.root)
    directory = root.lstat()
    if not stat.S_ISDIR(directory.st_mode) or (directory.st_dev, directory.st_ino) != (plan.device, plan.inode):
        raise ValueError("SOURCE plan root custody differs")
    target = root / _FILE
    observed = target.lstat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or (observed.st_dev, observed.st_ino, observed.st_size) != (plan.file_device, plan.file_inode, plan.file_bytes)
        or {entry.name for entry in root.iterdir()} != {_FILE}
    ):
        raise ValueError("SOURCE plan file custody differs")
    return target


def _entries(plan, *, check=None):
    if check is not None and not callable(check):
        raise ValueError("SOURCE plan replay cancellation check differs")
    path = _owned_file(plan)
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as plan_file:
        info = os.fstat(plan_file.fileno())
        if (info.st_dev, info.st_ino, info.st_size) != (plan.file_device, plan.file_inode, plan.file_bytes):
            raise ValueError("SOURCE plan open identity differs")
        if _plan_digest(plan_file, check) != plan.sha256:
            raise ValueError("SOURCE plan bytes differ")
        plan_file.seek(0)
        yield from _framed_entries(plan_file, plan, check=check)
        plan_file.seek(0)
        if _plan_digest(plan_file, check) != plan.sha256:
            raise ValueError("SOURCE plan changed during replay")


def _framed_entries(plan_file, plan, *, check=None):
    has_header = False
    is_complete = False
    while prefix := plan_file.read(8):
        if check is not None:
            check()
        if len(prefix) != 8:
            raise ValueError("SOURCE plan frame prefix is truncated")
        size = _LENGTH.unpack(prefix)[0]
        if not 0 < size <= plan.max_frame_bytes:
            raise ValueError("SOURCE plan frame exceeds its resource budget")
        frame = plan_file.read(size)
        if len(frame) != size or is_complete:
            raise ValueError("SOURCE plan frame/trailing bytes differ")
        entry = _decoded_entry(
            msgpack.unpackb(frame, raw=False, use_list=False, strict_map_key=True), plan.max_frame_bytes
        )
        if not has_header:
            if entry != (
                "header",
                _CONTRACT,
                plan.partition_sha256,
                plan.scope,
                plan.stream_slots,
                plan.collection_slots,
                plan.source_counts,
            ):
                raise ValueError("SOURCE plan header differs")
            has_header = True
        elif entry[0] == "complete":
            if entry != ("complete", plan.counts):
                raise ValueError("SOURCE plan completion census differs")
            is_complete = True
        elif entry[0] in ("part", "pack"):
            yield entry
        else:
            raise ValueError("SOURCE plan record kind differs")
    if not has_header or not is_complete:
        raise ValueError("SOURCE plan is incomplete")


def read_source_plan(plan, *, check=None):
    """Replay bounded packs; caller must exhaust before claiming full plan proof."""
    counters = [0] * 6
    with closing(_entries(plan, check=check)) as entries:
        for entry in entries:
            if entry[0] == "part":
                counters[5] += 1
                continue
            _, index, stream, collection, rejection, byte_count, digest, originals, landing_cells = entry
            occurrences = tuple(decode_record(encoded) for encoded in originals)
            landing_rows = tuple(LandingRow(_UUID, *cells) for cells in landing_cells)
            if (
                index != counters[1]
                or rejection != counters[4]
                or not landing_rows
                or len(occurrences) != len(landing_rows)
            ):
                raise ValueError("SOURCE plan pack/rejection sequence differs")
            if (
                len(occurrences) > plan.scope[4]
                or byte_count > plan.scope[5]
                or landing_manifest_sha256(landing_rows) != digest
            ):
                raise ValueError("SOURCE plan page budget or exact landing values differ")
            for original, stored in zip(occurrences, landing_rows, strict=True):
                if check is not None:
                    check()
                if (original.stream_slot, original.part_ordinal, original.row_ordinal, original.source_ordinal) != (
                    stream,
                    stored.source_part_ordinal,
                    stored.part_row_ordinal,
                    stored.source_ordinal,
                ):
                    raise ValueError("SOURCE plan occurrence coordinate differs")
                counters[0] += 1
                if stored.rejection_code is None:
                    counters[2 if collection == 0 else 3] += 1
                else:
                    counters[4] += 1
            counters[1] += 1
            yield PlannedSourcePack(
                index, stream, collection, rejection, occurrences, LandingBatch(landing_rows, byte_count, 1, digest)
            )
    if tuple(counters) != plan.counts:
        raise ValueError("SOURCE plan replay census differs")


def read_source_parts(plan):
    """Retain zero-row part evidence as well as every original nonempty part."""
    with closing(_entries(plan)) as entries:
        for row in entries:
            if row[0] == "part":
                yield row[1:]


def cleanup_source_plan(plan):
    """Only the owner removes its exact registered completed plan root."""
    _owned_file(plan)
    shutil.rmtree(plan.root)
