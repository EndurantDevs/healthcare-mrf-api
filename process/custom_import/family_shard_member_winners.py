# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded original members and complete raw streams for the existing reducer.

These closed producer files remain provisional. Independent original winner,
retained, native role and publication verification are separate owner duties.
"""

import hashlib
import os
import stat
from contextlib import ExitStack, closing, contextmanager
from dataclasses import replace

from process.custom_import import family_shard_winners as winners
from process.custom_import import materialization as selection
from process.custom_import.build_source import _source_cleanup_failure
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_shard_facts import SourceFactLimits, _sorted
from process.custom_import.family_shard_output import (
    FamilyShardOutput,
    _generation_scope,
    _input_iterator,
    _pack_row,
    _unpack_row,
    generation_member_batches,
)
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from process.custom_import.family_shard_selection import family_candidates
from process.custom_import.materialization import ValidatedWinnerCandidateStream


def _scope(common, generation, final_plan, shard_count, member_shard_id, work):
    if type(work) is not PlanWork:
        raise ValueError("original member/winner production requires accounted PlanWork")
    _generation_scope(common, generation, final_plan)
    _integer(shard_count, "original winner factory cohort", zero=True)
    _integer(member_shard_id, "original member receipt shard", zero=True)
    if (shard_count + 1) * 4096 > work.limits.memory_bytes // 4:
        raise FamilyBlockResourceLimit("original factory cohort exceeds owner metadata memory")
    partition = common.source.source.partition_sha256
    if type(partition) is not str or len(partition) != 64 or bytes.fromhex(partition).hex() != partition:
        raise ValueError("original member partition commitment differs")
    return bytes.fromhex(partition)


def _raw_families(layout, entries, shard_count, work, target, path, maxima):
    sequence = 0
    with closing(_input_iterator(entries)) as families:
        for entry in families:
            work.current()
            if type(entry) is not tuple or len(entry) != 3:
                raise ValueError("original member family entry shape differs")
            shard, plan, family = entry
            _integer(shard, "original winner factory stream", zero=True)
            if shard >= shard_count:
                raise ValueError("original family lies outside its factory cohort")
            with closing(family_candidates(layout, family, check=work.current)) as candidates:
                for candidate in candidates:
                    packed = _candidate_bytes(candidate, work)
                    work.frame(target, (shard, sequence, packed), path)
                    maxima[shard] = max(maxima[shard], len(packed))
                    sequence += 1
            yield plan, family


def _candidate_bytes(candidate, work):
    packed = _pack_row(winners._candidate_cells(candidate))
    if 4096 + 8 * len(packed) > work.limits.memory_bytes:
        raise FamilyBlockResourceLimit("original candidate exceeds owner stage memory")
    return packed


def _stage_members(common, generation, final_plan, entries, shard_count, work, proof):
    raw_path, member_path = work.path(), work.path()
    maxima = [0] * shard_count
    with raw_path.open("xb") as raw_file, member_path.open("xb") as member_file:
        supplied = _raw_families(common.layout, entries, shard_count, work, raw_file, raw_path, maxima)
        batches = generation_member_batches(
            common,
            generation,
            final_plan,
            supplied,
            maximum_rows=max(1, work.limits.memory_bytes // 4096),
            maximum_bytes=work.limits.memory_bytes,
            check=work.current,
        )
        with closing(batches):
            for batch in batches:
                for cells in batch["generation_family"]:
                    proof.add("generation_family", (cells,))
                    work.frame(member_file, ("generation_family", _pack_row(cells)), member_path)
    return raw_path, member_path, tuple(maxima)


def _profile_key(definition, profile, scope, contract, row):
    shard, sequence, packed = row
    candidate = winners._candidate_from_cells(_unpack_row(packed))
    normalized = selection._normalize_winner_candidate(candidate, contract)
    is_matching = candidate.context_collection_slot == scope.collection_slot
    canonical, digest = "", b""
    if is_matching:
        selection._validate_selection_values(profile, normalized, contract.fields_by_id)
        canonical, digest = selection._context_key(profile, normalized, contract.fields_by_id)
    return shard, int(is_matching), candidate.entity_binding_id if is_matching else 0, digest, canonical, sequence


def _ordered_profile(layout, raw, slot, cohort, work):
    scopes = selection._profile_scopes(layout.definition, layout.collection_slots)
    contract = selection._winner_candidate_contract(layout.definition, scopes)
    profile, scope = layout.definition.selection_profiles[slot - 1], scopes[slot - 1]
    path, ranges = work.path(), [(0, 0, 0)] * cohort
    key = lambda row: _profile_key(layout.definition, profile, scope, contract, row)
    with path.open("xb") as target, closing(_sorted(work, work.rows(raw), key)) as ordered:
        for shard, _sequence, packed in ordered:
            work.current()
            start, _end, count = ranges[shard]
            start = target.tell() if count == 0 else start
            work.frame(target, (shard, packed), path)
            ranges[shard] = start, target.tell(), count + 1
    os.chmod(path, 0o400)
    observed = path.stat()
    return path, tuple(ranges), (observed.st_dev, observed.st_ino, observed.st_size), _file_digest(path, work.current)


def _file_digest(path, check):
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while True:
            check()
            block = source.read(4096)
            if not block:
                return digest.digest()
            digest.update(block)


class _Heads:
    def __init__(self, maximum):
        self.maximum, self.used = maximum, 0

    def reserve(self, packed):
        """Bound simultaneously live raw and normalized reducer heads."""
        size = 4096 + 8 * len(packed)
        if self.used + size > self.maximum:
            raise FamilyBlockResourceLimit("original winner heads exceed owner memory")
        self.used += size
        return size


def _range_candidates(work, declaration, shard, heads, exhausted):
    path, ranges, identity, _digest = declaration
    start, end, count = ranges[shard]
    if not count:
        exhausted.add(shard)
        return
    observed = path.lstat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_IMODE(observed.st_mode) != 0o400
        or (observed.st_dev, observed.st_ino, observed.st_size) != identity
    ):
        raise ValueError("original ordered profile custody differs")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb", buffering=0) as input_file:
        if tuple(getattr(os.fstat(input_file.fileno()), name) for name in ("st_dev", "st_ino", "st_size")) != identity:
            raise ValueError("original ordered profile open identity differs")
        input_file.seek(start)
        with closing(work.read(input_file)) as frames:
            for _ordinal in range(count):
                actual, packed = next(frames, (None, None))
                if actual != shard or input_file.tell() > end:
                    raise ValueError("original ordered profile stream range differs")
                reserved = heads.reserve(packed)
                try:
                    yield winners._candidate_from_cells(_unpack_row(packed))
                finally:
                    heads.used -= reserved
        if input_file.tell() != end:
            raise ValueError("original ordered profile stream EOF differs")
    exhausted.add(shard)


def _complete_ranges(declaration):
    _path, ranges, identity, _digest = declaration
    previous_end = 0
    for start, end, count in ranges:
        if any(type(value) is not int or value < 0 for value in (start, end, count)):
            raise ValueError("original ordered profile range census differs")
        if count == 0:
            if (start, end) != (0, 0):
                raise ValueError("empty original stream range differs")
            continue
        if start != previous_end or not start < end <= identity[2]:
            raise ValueError("original ordered profile complete ranges differ")
        previous_end = end
    if previous_end != identity[2]:
        raise ValueError("original ordered profile file EOF is uncovered")


@contextmanager
def _profile_streams(work, generation, declaration, head_bytes):
    _complete_ranges(declaration)
    cohort = len(declaration[1])
    exhausted, heads = set(), _Heads(head_bytes)
    with ExitStack() as stack:
        streams = tuple(
            ValidatedWinnerCandidateStream(
                generation, stack.enter_context(closing(_range_candidates(work, declaration, shard, heads, exhausted)))
            )
            for shard in range(cohort)
        )
        yield streams
        if exhausted != set(range(cohort)) or heads.used:
            raise ValueError("original ordered profile factory lacks complete cohort EOF")
        if _file_digest(declaration[0], work.current) != declaration[3]:
            raise ValueError("original ordered profile complete bytes differ")


def _builder_limits(total, work, maxima, profile_count):
    # Ordered inputs stop writing before the builder starts: both live sizes are
    # reserved, while the builder's own Work enforces only the remaining budget.
    remaining = min(total.max_spool_bytes - work.used, work.remaining_bytes)
    metadata = 4096 + (len(maxima) + 1) * (profile_count + 1) * 512
    heads = sum(4096 + 8 * maximum for maximum in maxima if maximum)
    # Include existing closed-output hashing's 256 KiB buffer and framing.
    memory = total.memory_bytes - metadata - heads - work.limits.memory_bytes - 512 * 1024
    if remaining <= 0 or memory <= 0:
        raise FamilyBlockResourceLimit("original winner builder lacks remaining owner resources")
    return SourceFactLimits(remaining, min(remaining, total.max_record_bytes), min(remaining, memory)), heads


def _seal_members(work, path, proof):
    ready = work.root / "output.mpk"
    os.chmod(path, 0o400)
    with path.open("rb") as source:
        os.fsync(source.fileno())
    os.link(path, ready, follow_symlinks=False)
    path.unlink()
    work.paths[ready] = work.paths.pop(path)
    directory = os.open(work.root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return FamilyShardOutput(work, ready, proof.receipt())


def _build_outputs(common, generation, final_plan, entries, cohort, work, receipt, policy):
    layout = common.layout
    raw, member_path, maxima = _stage_members(common, generation, final_plan, entries, cohort, work, receipt)
    profiles = tuple(
        _ordered_profile(layout, raw, slot, cohort, work)
        for slot in range(1, len(layout.definition.selection_profiles) + 1)
    )
    work.remove(raw)
    limits, head_bytes = _builder_limits(policy.limits, work, maxima, len(profiles))

    @contextmanager
    def factory(slot):
        """Borrow fresh complete cohort ranges for the unchanged global reducer."""
        with _profile_streams(work, generation, profiles[slot - 1], head_bytes) as streams:
            yield streams

    descriptor = winners.build_winner_output(
        layout,
        generation,
        factory,
        work=winners.WinnerWork(
            work.root.with_name(work.root.name + "-winners"), limits, policy.register_cleanup, work.current
        ),
    )
    try:
        for path, _ranges, _identity, _digest in profiles:
            work.remove(path)
        return _seal_members(work, member_path, receipt), descriptor
    except BaseException as primary:
        try:
            winners.cleanup_winner_output(descriptor)
        except BaseException as error:
            raise _source_cleanup_failure(primary, error)
        raise


def build_original_member_winners(
    common, generation, final_plan, family_entries, shard_count, *, work, member_shard_id
):
    """Stage exact members and every raw context once, then use the global reducer.

    Run off the coordinator loop. Caller-owned original family readers stay live
    through EOF; returned real outputs remain owned until explicit cleanup.
    Independent original/native proof is mandatory before accepting either file.
    """
    partition = _scope(common, generation, final_plan, shard_count, member_shard_id, work)
    bounded = replace(work, limits=replace(work.limits, memory_bytes=max(1, work.limits.memory_bytes // 8)))
    scratch, outputs, primary = _Scratch(bounded), None, None
    try:
        proof = ShardOutputProof(common.layout, member_shard_id, partition, 0)
        outputs = _build_outputs(common, generation, final_plan, family_entries, shard_count, scratch, proof, work)
        if _scope(common, generation, final_plan, shard_count, member_shard_id, work) != partition:
            raise ValueError("original member producer scope changed")
        scratch.current()
    except BaseException as error:
        primary = error
    if primary is not None:
        if outputs is not None:
            try:
                winners.cleanup_winner_output(outputs[1])
            except BaseException as error:
                primary = _source_cleanup_failure(primary, error)
        try:
            scratch.close()
        except BaseException as error:
            primary = _source_cleanup_failure(primary, error)
        raise primary
    return outputs
