# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Encode caller-proven retained assignments; native and stored proof is separate."""

from collections.abc import Mapping
from contextlib import closing
from dataclasses import replace
from itertools import zip_longest
from pathlib import Path

from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import.build_source import _source_cleanup_failure
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer, record_parts
from process.custom_import.family_block_validate import check_key_dictionary
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_facts import _LENGTH, SourceFactLimits, _pack, _sorted
from process.custom_import.family_shard_output import FamilyOutputWork, _input_iterator, _pack_row, build_family_output
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from process.custom_import.family_shard_retained_homes import _destination_scope
from process.custom_import.family_shard_retained_output import read_retained_destination, retained_destination_families
from process.custom_import.family_shard_retained_validation import RetainedOriginalComparison
from process.custom_import.family_shard_tasks import _resolved
from process.custom_import.family_shard_worker import _key_evidence

_MISSING = object()


def _original_dictionary(original_query_keys, work):
    if not callable(original_query_keys):
        raise ValueError("retained encoding requires a fresh original dictionary factory")
    with closing(_input_iterator(original_query_keys())) as entries:
        for entry in entries:
            work.current()
            if type(entry) is not tuple or len(entry) != 4:
                raise ValueError("retained original dictionary row differs")
            digest, text, rank, variant = entry
            _key_evidence((text, digest), "child-key", work.limits.max_record_bytes)
            yield rank, variant, digest, text


def _common_dictionary(context, work):
    expected = context.common_output.role_counts
    counts_by_role = dict.fromkeys(expected, 0)
    with closing(
        context.common_output.batches(
            maximum_rows=4096, maximum_bytes=min(work.limits.max_record_bytes, work.limits.memory_bytes)
        )
    ) as batches:
        for batch in batches:
            work.current()
            for role, rows in batch.items():
                if role not in counts_by_role:
                    raise FamilyBlockUnavailable("retained common role differs")
                counts_by_role[role] += len(rows)
                if role == "key_dictionary":
                    yield from rows
    if counts_by_role != expected:
        raise FamilyBlockUnavailable("retained common complete role census differs")


def _stage_global_dictionary(context, original_query_keys, work):
    """Bind a fresh complete dictionary to exact closed common rows in one Scratch."""
    path = work.write(_original_dictionary(original_query_keys, work))
    with closing(work.rows(path)) as entries:
        count = check_key_dictionary(entries)
    with closing(work.rows(path)) as entries, closing(_common_dictionary(context, work)) as common:
        for original, observed in zip_longest(entries, common, fillvalue=_MISSING):
            work.current()
            if original is _MISSING or observed is _MISSING or original != observed:
                raise FamilyBlockUnavailable("retained original global dictionary differs from closed common output")
    return path, count


def _checked_scope(context, destination, comparison, work):
    if type(work) is not FamilyOutputWork or type(work.limits) is not FamilyBlockLimits:
        raise ValueError("retained encoding requires explicit family output work")
    limits = work.limits
    facts = SourceFactLimits(limits.max_spool_bytes, limits.max_record_bytes, limits.memory_bytes)
    owner_work = allocation.RetainedAllocationWork(
        work.root, SourceAllocationLimits(facts, 4096, limits.max_spool_bytes), work.register_cleanup, work.check
    )
    scope = _destination_scope(context, destination, owner_work)
    if (
        type(comparison) is not RetainedOriginalComparison
        or type(comparison.scope) is not tuple
        or type(comparison.destination_sha256) is not bytes
        or _pack_row(comparison.scope) != _pack_row(scope)
        or comparison.destination_sha256 != destination.sha256
        or comparison.counts != context.retained_input.counts
        or any(
            type(count) is not int or count < 0
            for count in (
                *dict(comparison.counts).values(),
                comparison.root_scalar_count,
                comparison.child_scalar_count,
            )
        )
        or comparison.root_scalar_count + comparison.child_scalar_count != dict(comparison.counts)["scalar"]
    ):
        raise FamilyBlockUnavailable("retained encoding original comparison scope differs")
    partition = context.common.source.source.partition_sha256
    if type(partition) is not str or len(partition) != 64 or bytes.fromhex(partition).hex() != partition:
        raise ValueError("retained encoding original partition differs")
    return bytes.fromhex(partition)


def _requests(context, destination, work, counts_by_kind):
    names_by_slot = {0: None} | {slot: name for name, slot in destination.collection_slots}
    definition = context.common.request.definition
    family, size, previous = None, 0, None
    with closing(read_retained_destination(destination, check=work.current)) as entries:
        for entry in entries:
            if entry[0] == "family":
                family, size = entry[1], 0
                if previous is not None and family <= previous:
                    raise FamilyBlockUnavailable("retained encoding selected family order differs")
                previous = family
                counts_by_kind["family"] += 1
                continue
            if entry[0] != "record":
                continue
            if family is None or entry[1] != family or entry[2] not in names_by_slot:
                raise FamilyBlockUnavailable("retained encoding record family differs")
            collection = names_by_slot[entry[2]]
            if collection is None:
                counts_by_kind["root"] += 1
            else:
                counts_by_kind["child"] += 1
                frame = _LENGTH.size + len(_pack((entry[4], entry[5], entry[6])))
                size += frame
                counts_by_kind["replay"] = max(counts_by_kind["replay"], size)
                counts_by_kind["frame"] = max(counts_by_kind["frame"], frame - _LENGTH.size)
            if collection is not None and collection == definition.query.child_collection:
                child_record = allocation._record(definition, collection, entry[5], retained=True)
                _wire, parts = record_parts(definition, collection, child_record, retained=True)
                yield 2, parts[:2], 1, family


def _joined_keys(work, dictionary, requests):
    with closing(work.rows(dictionary)) as keys:
        for rank, variant, digest, text in keys:
            yield 2, (digest, text), 0, (rank, variant)
    yield from work.rows(requests)


def _lookup_rows(work, dictionary, requests):
    counts_by_kind = dict(missing_query=0, missing_family=0)
    ordered = _sorted(work, _joined_keys(work, dictionary, requests), lambda row: row)
    for family, _kind, key, assignment in _resolved(ordered, counts_by_kind):
        yield family, key, assignment
    if counts_by_kind["missing_query"] or counts_by_kind["missing_family"]:
        raise FamilyBlockUnavailable("retained query key is absent from the original global dictionary")


class _FamilyKeys(Mapping):
    """Private ordered encoder lookup; only the current family and key are live."""

    def __init__(self, work, path):
        self.work, self.path, self.rows = work, path, work.rows(path)
        self.pending = next(self.rows, None)
        self.family = self.previous = self.cached = None

    def begin(self, family):
        """Bind the next borrowed family without collecting its dictionary."""
        if self.family is not None or self.previous is not None and family <= self.previous:
            raise FamilyBlockUnavailable("retained lookup family order differs")
        self.family, self.cached = family, None

    def end(self):
        """Require complete current-family lookup coverage before advancing."""
        if self.pending is not None and self.pending[0] <= self.family:
            raise FamilyBlockUnavailable("retained lookup has unconsumed or foreign current-family keys")
        self.previous, self.family = self.family, None

    def __getitem__(self, key):
        if self.family is None:
            raise ValueError("retained key lookup outlived its current family")
        self.work.current()
        if self.cached is not None:
            if key == self.cached[0]:
                return self.cached[1]
            if key < self.cached[0]:
                raise FamilyBlockUnavailable("retained encoder query keys moved backwards")
        if self.pending is None or self.pending[:2] != (self.family, key):
            raise KeyError(key)
        self.cached = key, self.pending[2]
        self.pending = next(self.rows, None)
        return self.cached[1]

    def __iter__(self):
        if self.family is None:
            raise ValueError("retained key lookup outlived its current family")
        with closing(self.work.rows(self.path)) as rows:
            for family, key, _assignment in rows:
                if family == self.family:
                    yield key
                elif family > self.family:
                    break

    def __len__(self):
        return sum(1 for _key in self)

    def finish(self):
        """Refuse any lookup tail after the complete retained family stream."""
        if self.family is not None or self.pending is not None:
            raise FamilyBlockUnavailable("retained lookup did not reach complete EOF")

    def close(self):
        """Close the borrowed lookup reader without removing the caller's file."""
        self.rows.close()


def _families(context, destination, work, keys, replay_limits):
    replay_work = PlanWork(work.root / "family-replay", replay_limits, lambda _path: None, work.current)
    with retained_destination_families(destination, context.common.request.definition, work=replay_work) as families:
        for plan, _original, assigned in families:
            keys.begin((plan.typed_key[1], plan.typed_key[0]))
            yield assigned
            keys.end()
    keys.finish()


def _prepare(context, destination, comparison, original_query_keys, scratch):
    dictionary, _count = _stage_global_dictionary(context, original_query_keys, scratch)
    counts_by_kind = dict(family=0, root=0, child=0, replay=0, frame=0)
    requests = scratch.write(_requests(context, destination, scratch, counts_by_kind))
    if any(counts_by_kind[name] != dict(comparison.counts)[name] for name in ("family", "root", "child")):
        raise FamilyBlockUnavailable("retained encoding complete selected census differs")
    joined = _sorted(scratch, _lookup_rows(scratch, dictionary, requests), lambda row: row[:2])
    with closing(joined):
        lookup = scratch.write(joined)
    scratch.remove(dictionary)
    scratch.remove(requests)
    return lookup, counts_by_kind


def _encode(context, destination, comparison, original_query_keys, work, scratch, shard_id, partition):
    lookup, counts_by_kind = _prepare(context, destination, comparison, original_query_keys, scratch)
    replay_bytes = max(
        counts_by_kind["replay"], counts_by_kind["frame"], work.limits.max_record_bytes, scratch.limits.memory_bytes
    )
    output_bytes = scratch.remaining_bytes - replay_bytes
    if output_bytes < max(work.limits.max_record_bytes, work.limits.fragment_bytes, work.limits.memory_bytes):
        raise FamilyBlockResourceLimit("retained lookup/replay/output exceeds the total spool budget")
    replay_limits = SourceFactLimits(
        replay_bytes, max(counts_by_kind["frame"], work.limits.max_record_bytes), scratch.limits.memory_bytes
    )
    scratch.charge(replay_bytes + output_bytes)
    output = None
    try:
        keys = _FamilyKeys(scratch, lookup)
        with closing(keys), closing(_families(context, destination, scratch, keys, replay_limits)) as families:
            output = build_family_output(
                context.common.layout,
                context.common.source.snapshot_family_id,
                families,
                keys,
                work=replace(
                    work,
                    limits=replace(
                        work.limits, max_spool_bytes=output_bytes, memory_bytes=max(1, work.limits.memory_bytes // 2)
                    ),
                ),
                shard_id=shard_id,
                partition_sha256=partition,
                source_record_count=0,
            )
        return output
    except BaseException as error:
        if output is not None:
            try:
                output.close()
            except BaseException as cleanup:
                raise _source_cleanup_failure(error, cleanup)
        raise
    finally:
        scratch.used -= replay_bytes + output_bytes


def build_retained_output(context, destination, retained_comparison, original_query_keys, *, work, shard_id):
    """Encode complete proven assignments, with no SQL or new record identities.

    Run off the owner event loop under its heartbeat and drained cancellation.
    The caller checks the actual live base before and after this pure CPU pass.
    Original owners remain live through separate stored/native proof and COPY.
    """
    partition = _checked_scope(context, destination, retained_comparison, work)
    _integer(shard_id, "retained output shard", zero=True)
    deadline = _read_deadline(context.common.request)

    def current():
        """Preserve the caller stop callback and original immutable deadline."""
        if work.check is not None:
            work.check()
        _require_deadline(deadline)

    checked = replace(work, check=current)
    limits = SourceFactLimits(
        work.limits.max_spool_bytes, work.limits.max_record_bytes, max(1, work.limits.memory_bytes // 4)
    )
    root = Path(work.root)
    scratch = _Scratch(PlanWork(root.with_name(root.name + "-retained-inputs"), limits, work.register_cleanup, current))
    primary, output = None, None
    try:
        output = _encode(
            context, destination, retained_comparison, original_query_keys, checked, scratch, shard_id, partition
        )
        current()
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        if output is not None:
            try:
                output.close()
            except BaseException as cleanup:
                primary = _source_cleanup_failure(primary, cleanup)
        raise primary
    return output
