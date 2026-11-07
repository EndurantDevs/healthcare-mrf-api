# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Advance proven retained pack tails before opening a provisional generation.

The caller first finishes allocation, retained home registration/comparison and
original/stored/COPY proofs. After this operation it must not re-enter allocator
or home-registration guards requiring the old SOURCE prefix. Retry reuses the
same closed artifacts and native receipts; it never allocates another ID.
Complete native role/home equality and closed verification remain mandatory.
"""

import hashlib
import os
import stat
from contextlib import closing
from dataclasses import dataclass

from sqlalchemy import Integer, SmallInteger, column, func, select, update, values

from db.models.custom_import import CustomImportBuildAttempt, CustomImportBuildStream
from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import.build_source import _call, _page_session, _source_cleanup_failure
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_records import _integer, _sha256
from process.custom_import.family_block_store import (
    ShardOutputProof,
    _authority,
    _candidate,
    _checked_receipt,
    _descriptor_state,
    _execute,
)
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_control import _source_capture_counts
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.family_shard_retained_homes import _destination_scope
from process.custom_import.family_shard_retained_output import _owned_destination, read_retained_destination
from process.custom_import.family_shard_retained_storage_validation import RetainedStoredComparison, _output_inputs
from process.custom_import.family_shard_retained_validation import RetainedOriginalComparison

_COPY_FIELDS = ("shard_id", "partition_sha256", "result_sha256", "source_record_count", "role_counts")


@dataclass(frozen=True)
class _PackEvidence:
    signature: tuple
    stamps: tuple
    prefix: tuple
    tail: tuple
    receipts: tuple


def _scope(context, destination, original, stored, encoded, work):
    scope = _destination_scope(context, destination, work)
    if type(original) is not RetainedOriginalComparison or type(stored) is not RetainedStoredComparison:
        raise FamilyBlockUnavailable("retained pack completion requires original and stored comparisons")
    for digest in (
        destination.sha256,
        original.destination_sha256,
        stored.destination_sha256,
        stored.original_input_sha256,
        stored.sha256,
        context.retained_input.sha256,
    ):
        _sha256(digest)
    if (
        type(original.scope) is not tuple
        or type(stored.scope) is not tuple
        or _pack_row(original.scope) != _pack_row(scope)
        or _pack_row(stored.scope) != _pack_row(scope)
        or original.destination_sha256 != destination.sha256
        or stored.destination_sha256 != destination.sha256
        or stored.original_input_sha256 != context.retained_input.sha256
        or original.counts != context.retained_input.counts
        or stored.counts != original.counts
        or (stored.root_scalar_count, stored.child_scalar_count)
        != (original.root_scalar_count, original.child_scalar_count)
        or stored.output_inputs != _output_inputs(context, encoded)
    ):
        raise FamilyBlockUnavailable("retained pack completion original proof scope differs")
    for _role, count in original.counts:
        _integer(count, "retained pack original count", zero=True)
    for count in (original.root_scalar_count, original.child_scalar_count):
        _integer(count, "retained pack scalar count", zero=True)
    if original.root_scalar_count + original.child_scalar_count != dict(original.counts)["scalar"]:
        raise FamilyBlockUnavailable("retained pack original scalar split differs")
    return (
        scope,
        tuple(vars(destination).items()),
        tuple(vars(original).items()),
        tuple(vars(stored).items()),
        stored.output_inputs,
    )


def _file_stamp(path):
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode) or stat.S_IMODE(observed.st_mode) != 0o400:
        raise FamilyBlockUnavailable("retained pack output file custody differs")
    return tuple(getattr(observed, "st_" + name) for name in ("dev", "ino", "size", "mtime_ns", "ctime_ns", "mode"))


def _artifact_custody(destination, encoded):
    path = _owned_destination(destination)
    root, observed = encoded._work.root.lstat(), encoded._identity
    if (
        encoded._closed
        or not stat.S_ISDIR(root.st_mode)
        or (root.st_dev, root.st_ino)
        != (
            encoded._work.identity.st_dev,
            encoded._work.identity.st_ino,
        )
    ):
        raise FamilyBlockUnavailable("retained pack encoded owner custody differs")
    identity = _file_stamp(encoded.path)
    if identity[:3] != (observed.st_dev, observed.st_ino, observed.st_size):
        raise FamilyBlockUnavailable("retained pack encoded file custody differs")
    return (_file_stamp(path), identity)


def _tails(context, destination):
    prefix, tail = destination.source_pack_prefix, destination.destination_pack_tail
    slots = tuple(slot for slot, _count in context.common.source.source.source_counts)
    for declaration in (prefix, tail):
        if type(declaration) is not tuple or tuple(slot for slot, _count in declaration) != slots:
            raise FamilyBlockUnavailable("retained pack stream census differs")
        for slot, count in declaration:
            if type(slot) is not int or type(count) is not int or not 0 <= count < 2**31:
                raise FamilyBlockUnavailable("retained pack ordinal exceeds its native counter")
    if any(last < first for (_slot, first), (_same, last) in zip(prefix, tail, strict=True)) or (
        sum(count for _slot, count in prefix) != context.common.source.source.counts[1]
        or sum(last - first for (_slot, first), (_same, last) in zip(prefix, tail, strict=True))
        != destination.role_counts["pack"]
    ):
        raise FamilyBlockUnavailable("retained pack prefix or destination tail differs")
    return prefix, tail


def _proof(context, receipt):
    checked = _checked_receipt(context.common.layout, receipt)
    if (
        checked["partition_sha256"] != bytes.fromhex(context.common.source.source.partition_sha256)
        or checked["source_record_count"] != 0
    ):
        raise FamilyBlockUnavailable("retained pack COPY receipt scope differs")
    return checked, ShardOutputProof(context.common.layout, checked["shard_id"], checked["partition_sha256"], 0)


def _common_proof(context, destination, receipt, work):
    checked, proof = _proof(context, receipt)
    with closing(read_retained_destination(destination, check=work.current)) as entries:
        for entry in entries:
            work.current()
            if entry[0] == "copy":
                proof.add(entry[1], (_unpack_row(entry[2]),))
    if proof.receipt() != checked:
        raise FamilyBlockUnavailable("retained pack common COPY commitment differs")
    return checked


class _DigestInput:
    def __init__(self, source):
        self.source, self.digest = source, hashlib.sha256()

    def read(self, size):
        """Hash the exact framed producer bytes consumed by the bounded decoder."""
        payload = self.source.read(size)
        self.digest.update(payload)
        return payload


def _encoded_proof(context, encoded, work):
    checked, proof = _proof(context, encoded.receipt)
    identity = _file_stamp(encoded.path)
    with os.fdopen(os.open(encoded.path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        observed = os.fstat(source.fileno())
        if (observed.st_dev, observed.st_ino, observed.st_size) != identity[:3]:
            raise FamilyBlockUnavailable("retained pack open producer custody differs")
        reader = _DigestInput(source)
        for role, packed in work.read(reader):
            work.current()
            proof.add(role, (_unpack_row(packed),))
        if (
            reader.digest.digest() != encoded.sha256
            or proof.receipt() != checked
            or _file_stamp(encoded.path) != identity
        ):
            raise FamilyBlockUnavailable("retained pack encoded COPY commitment differs")
    return checked


def _prepare(context, destination, original, stored, encoded, receipt, owner_work, work):
    signature = _scope(context, destination, original, stored, encoded, owner_work)
    stamps = _artifact_custody(destination, encoded)
    prefix, tail = _tails(context, destination)
    receipts = _common_proof(context, destination, receipt, work), _encoded_proof(context, encoded, work)
    if receipts[0]["shard_id"] == receipts[1]["shard_id"]:
        raise FamilyBlockUnavailable("retained pack COPY receipts repeat a shard")
    _current_inputs(context, destination, original, stored, encoded, owner_work, signature, stamps)
    return _PackEvidence(signature, stamps, prefix, tail, receipts)


def _current_inputs(context, destination, original, stored, encoded, work, signature, stamps):
    if (
        _scope(context, destination, original, stored, encoded, work) != signature
        or _artifact_custody(destination, encoded) != stamps
    ):
        raise FamilyBlockUnavailable("retained pack completed producer inputs changed")


def _graph_state(context, build):
    if (
        build is None
        or build.phase != "graph"
        or build.source_frozen_at is None
        or build.candidate_error_count != 0
        or build.source_occurrence_count != context.common.source.source.counts[0]
        or build.capture_bundle_id != dict(context.common.source.source.scope[0])["capture_bundle_id"]
        or build.plan_stage != "base"
        or build.plan_complete_at is not None
        or (build.selected_family_count, build.completed_family_count) != (0, 0)
        or any(
            getattr(build, name) is not None
            for name in ("graph_frozen_at", "output_frozen_at", "generation_id", "verified_at")
        )
    ):
        raise FamilyBlockUnavailable("retained pack completion requires the unfrozen GRAPH owner")
    return allocation._model_cells(build)


async def _streams(session, context, evidence):
    table = CustomImportBuildStream.__table__
    rows = (
        (
            await _execute(
                session, select(table).where(table.c.build_id == context.build_id).order_by(table.c.stream_slot)
            )
        )
        .mappings()
        .all()
    )
    slots = tuple(slot for slot, _tail in evidence.tail)
    if tuple(row["stream_slot"] for row in rows) != slots:
        raise FamilyBlockUnavailable("retained pack native stream census differs")
    counts_by_stream = {}
    for row in rows:
        slot = row["stream_slot"]
        if (
            type(row["next_part_ordinal"]) is not int
            or row["next_part_ordinal"] <= 0
            or row["next_part_row_ordinal"] != 0
            or row["next_source_ordinal"] != dict(context.common.source.source.source_counts)[slot]
            or row["replay_verified_at"] is None
        ):
            raise FamilyBlockUnavailable("retained pack frozen SOURCE cursor differs")
        counts_by_stream[slot] = [row["next_part_ordinal"] - 1, row["next_source_ordinal"]]
    await _source_capture_counts(session, context.common, counts_by_stream)
    return tuple(dict(row) for row in rows)


async def _receipts(session, binding, expected):
    table = binding.tables_by_role["shard_receipt"]
    rows = (
        (await _execute(session, select(table).where(table.c.shard_id.in_([row["shard_id"] for row in expected]))))
        .mappings()
        .all()
    )
    actual_by_shard = {row["shard_id"]: row for row in rows}
    if len(rows) != len(expected) or set(actual_by_shard) != {row["shard_id"] for row in expected}:
        raise FamilyBlockUnavailable("retained pack completed COPY receipt is missing")
    for receipt in expected:
        actual = actual_by_shard[receipt["shard_id"]]
        completed = actual["completed_at"]
        if (
            completed is None
            or completed.utcoffset() is None
            or _checked_receipt(binding.layout, {name: actual[name] for name in _COPY_FIELDS}) != receipt
        ):
            raise FamilyBlockUnavailable("retained pack native COPY receipt differs")


async def _pack_census(session, context, binding, tail):
    table, request = binding.tables_by_role["pack"], context.common.request
    scope_by_name = dict(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=dict(context.common.source.source.scope[0])["capture_bundle_id"],
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
    )
    statement = select(
        table.c.stream_slot,
        func.count(),
        func.count(func.distinct(table.c.pack_ordinal)),
        func.min(table.c.pack_ordinal),
        func.max(table.c.pack_ordinal),
        func.count().filter(*(table.c[name] == cell for name, cell in scope_by_name.items())),
    ).group_by(table.c.stream_slot)
    actual_rows = tuple(sorted(tuple(row) for row in (await _execute(session, statement)).all()))
    expected_rows = tuple((slot, count, count, 0, count - 1, count) for slot, count in tail if count)
    if actual_rows != expected_rows:
        raise FamilyBlockUnavailable("retained pack native dense ordinal or producer census differs")


async def _advance(session, context, rows, evidence):
    actual_rows = tuple((row["stream_slot"], row["next_pack_ordinal"]) for row in rows)
    if actual_rows not in (evidence.prefix, evidence.tail):
        raise FamilyBlockUnavailable("retained pack counters are neither original prefix nor completed tail")
    changed_rows = [
        (slot, first, last)
        for (slot, first), (_same, last) in zip(actual_rows, evidence.tail, strict=True)
        if first != last
    ]
    if changed_rows:
        table = CustomImportBuildStream.__table__
        tails = values(
            column("slot", SmallInteger), column("previous", Integer), column("target", Integer), name="retained_tails"
        ).data(changed_rows)
        statement = (
            update(table)
            .where(
                table.c.build_id == context.build_id,
                table.c.stream_slot == tails.c.slot,
                table.c.next_pack_ordinal == tails.c.previous,
            )
            .values(next_pack_ordinal=tails.c.target)
            .returning(table.c.stream_slot, table.c.next_pack_ordinal)
        )
        observed_rows = tuple(sorted(tuple(row) for row in (await _execute(session, statement)).all()))
        if observed_rows != tuple((slot, last) for slot, _first, last in changed_rows):
            raise FamilyBlockUnavailable("retained pack conditional counter transition differs")


async def _finish(session, context, binding, rows, evidence, build_before):
    after = await _streams(session, context, evidence)
    expected_rows = tuple(dict(row, next_pack_ordinal=dict(evidence.tail)[row["stream_slot"]]) for row in rows)
    build_table = CustomImportBuildAttempt.__table__
    build = (
        (await _execute(session, select(build_table).where(build_table.c.build_id == context.build_id)))
        .mappings()
        .one()
    )
    if after != expected_rows or dict(build) != build_before:
        raise FamilyBlockUnavailable("retained pack transition changed frozen SOURCE or build state")
    await _receipts(session, binding, evidence.receipts)
    await _pack_census(session, context, binding, evidence.tail)


async def _open_candidate(session, context):
    return await _candidate(
        session, context.common.request, context.common.source.snapshot_family_id, context.common.layout, {"loading"}
    )


async def complete_retained_pack_counters(
    factory, context, destination, original, stored, encoded, common_receipt, *, work
):
    """Advance once after retained proofs/homes/COPY; exact GRAPH retry preserves counters.

    Allocator and home-registration prefix guards must finish before this call;
    never re-enter them after a committed tail. Reuse the same closed artifacts
    after uncertain commit. This transition grants no structural acceptance.
    """
    _scope(context, destination, original, stored, encoded, work)
    deadline = _read_deadline(context.common.request)

    def current():
        """Retain original time bounds and caller cancellation through producer EOF."""
        _require_deadline(deadline)
        if work.check is not None:
            work.check()

    scratch, primary, completed_tail = (
        _Scratch(PlanWork(work.root, work.limits.spool, work.register_cleanup, current)),
        None,
        None,
    )
    try:
        await allocation._check_base(context)
        evidence = await _cpu(
            scratch, _prepare, context, destination, original, stored, encoded, common_receipt, work, scratch
        )
        async with _page_session(factory, context.common.request, context.build_id) as (session, build):
            await _call(session, "lock_custom_import_snapshot_attempt", _authority(context.common.request))
            descriptor, binding = await _open_candidate(session, context)
            closed_before, build_before = _descriptor_state(descriptor), _graph_state(context, build)
            await allocation._check_base(context)
            scratch.current()
            _current_inputs(context, destination, original, stored, encoded, work, evidence.signature, evidence.stamps)
            stream_rows = await _streams(session, context, evidence)
            await _receipts(session, binding, evidence.receipts)
            await _pack_census(session, context, binding, evidence.tail)
            await _advance(session, context, stream_rows, evidence)
            await _finish(session, context, binding, stream_rows, evidence, build_before)
            descriptor, _binding = await _open_candidate(session, context)
            if _descriptor_state(descriptor) != closed_before:
                raise FamilyBlockUnavailable("retained pack candidate closure changed")
            await allocation._check_base(context)
            _current_inputs(context, destination, original, stored, encoded, work, evidence.signature, evidence.stamps)
            scratch.current()
            completed_tail = evidence.tail
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return completed_tail
