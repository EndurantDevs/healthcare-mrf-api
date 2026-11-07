# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bind every raw winner context to original families and the global reducer.

Family inputs belong to the caller's independent original readers, never the
candidate database. This comparison grants no native or publication authority.
"""

import hashlib
from contextlib import ExitStack, closing
from dataclasses import astuple, dataclass, replace
from itertools import groupby, zip_longest

from process.custom_import import family_shard_winners as output
from process.custom_import import materialization as selection
from process.custom_import.build_source import _source_cleanup_failure
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_records import _integer
from process.custom_import.family_block_validate import FamilyBlockValidationError, _add, _equal_native
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_base_values import read_retained_input
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_original_validation import SourceOriginalComparison, _producer
from process.custom_import.family_shard_output import AllocatedFamily, _input_iterator, _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _plan_cells, _Scratch, read_family_plan
from process.custom_import.family_shard_retained_allocate import RetainedAllocationContext, RetainedAllocationWork
from process.custom_import.family_shard_retained_allocate import _scope as retained_context_scope
from process.custom_import.family_shard_retained_output import (
    RetainedDestinationDescriptor,
    _replay_families,
)
from process.custom_import.family_shard_retained_validation import RetainedOriginalComparison
from process.custom_import.family_shard_selection import family_candidates, iter_shard_profile_winners
from process.custom_import.materialization import GenerationIdentity, ValidatedWinnerCandidateStream

_MISSING = object()


@dataclass(frozen=True)
class RetainedWinnerInput:
    """Real retained assignment with its independent completed original comparison."""

    destination: RetainedDestinationDescriptor
    comparison: RetainedOriginalComparison


@dataclass(frozen=True)
class WinnerOriginalComparison:
    """Immutable raw-context/reducer evidence, without structural acceptance."""

    producer: tuple
    generation: tuple
    source_sha256: bytes
    retained_sha256: bytes | None
    winner_sha256: bytes
    retained_original_counts: tuple | None
    retained_scalar_counts: tuple | None
    profiles: tuple
    family_counts: tuple
    winner_count: int
    sha256: bytes


def _profile_scope(layout, generation, winners, shard_count):
    slots = tuple(range(1, len(layout.definition.selection_profiles) + 1))
    scopes = selection._profile_scopes(layout.definition, layout.collection_slots)
    expected_scopes = tuple((slot, scope.collection_slot) for slot, scope in zip(slots, scopes, strict=True))
    if (
        winners.layout_sha256,
        winners.definition_sha256,
        winners.generation,
        winners.profile_slots,
        winners.profile_scopes,
    ) != (layout.digest, layout.definition.digest, astuple(generation), slots, expected_scopes):
        raise FamilyBlockValidationError("winner comparison requires every configured profile and scope")
    if tuple(slot for slot, _streams, _count in winners.profiles) != slots or any(
        len(streams) != shard_count for _slot, streams, _count in winners.profiles
    ):
        raise FamilyBlockValidationError("winner output stream cohort differs")
    return scopes, selection._winner_candidate_contract(layout.definition, scopes)


def _retained_scope(context, original_scope, retained):
    if retained is None:
        if context.final_plan.counts_by_kind["retained"]:
            raise FamilyBlockValidationError("winner retained families require their proven original destination")
        return
    if type(retained) is not RetainedWinnerInput:
        raise ValueError("winner retained input requires its real descriptor and independent comparison")
    destination, comparison = retained.destination, retained.comparison
    if (
        type(destination) is not RetainedDestinationDescriptor
        or destination.scope != original_scope
        or destination.definition_sha256 != context.common.layout.definition.digest
        or destination.collection_slots != tuple(sorted(context.common.layout.collection_slots.items()))
        or destination.columns_by_role
        != tuple((role, context.common.columns_by_role[role]) for role in ("root_record", "entity_binding", "pack"))
        or type(comparison) is not RetainedOriginalComparison
        or comparison.scope != original_scope
        or comparison.destination_sha256 != destination.sha256
        or comparison.counts != context.retained_input.counts
        or any(
            type(count) is not int or count < 0
            for count in (comparison.root_scalar_count, comparison.child_scalar_count)
        )
        or comparison.root_scalar_count + comparison.child_scalar_count != dict(comparison.counts)["scalar"]
    ):
        raise FamilyBlockValidationError("winner retained original comparison scope differs")


def _scope(context, source_comparison, generation, winners, shard_count, retained, work):
    if type(context) is not RetainedAllocationContext or type(source_comparison) is not SourceOriginalComparison:
        raise ValueError("winner comparison requires real original context and SOURCE evidence")
    if type(generation) is not GenerationIdentity or type(winners) is not output.WinnerOutputDescriptor:
        raise ValueError("winner comparison requires exact generation and completed output")
    _integer(shard_count, "winner cohort size", zero=True)
    if shard_count * 4096 > work.limits.memory_bytes:
        raise FamilyBlockResourceLimit("winner cohort exceeds its owner memory reservation")
    for identity in astuple(generation):
        _integer(identity, "winner generation identity")
    common, plan = context.common, context.final_plan
    expected_inputs = (
        common.source.source.sha256,
        common.source.sha256,
        context.admission.sha256,
        plan.sha256,
        common.layout.digest,
    )
    if (
        source_comparison.producer != _producer(common)
        or source_comparison.inputs != expected_inputs
        or source_comparison.retained_family_count_unproven != plan.counts_by_kind["retained"]
    ):
        raise FamilyBlockValidationError("winner original SOURCE provenance differs")
    if (
        source_comparison.counts_by_name["family_count"] != plan.counts_by_kind["source"]
        or source_comparison.source_record_count != common.source.source.counts[0]
    ):
        raise FamilyBlockValidationError("winner original SOURCE census differs")
    if astuple(generation)[1:] != (
        common.request.dataset_id,
        common.request.definition_revision_id,
        common.request.schema_revision_id,
    ):
        raise FamilyBlockValidationError("winner generation producer differs")
    allocation_work = RetainedAllocationWork(
        work.root,
        SourceAllocationLimits(work.limits, 1, work.limits.max_record_bytes),
        work.register_cleanup,
        work.check,
    )
    original_scope = retained_context_scope(context, allocation_work)
    _retained_scope(context, original_scope, retained)
    return _profile_scope(common.layout, generation, winners, shard_count)


def _cells(contract, candidate):
    selection._normalize_winner_candidate(candidate, contract)
    cells = output._candidate_cells(candidate)
    return (*cells[:-1], tuple(sorted(cells[-1])))


def _family_identity(plan, family):
    if type(family) is not AllocatedFamily or family.root_record_id != plan.root_record_id:
        raise FamilyBlockValidationError("winner family original root or type differs")
    if plan.selection_kind == "source":
        if family.retained is not False or (
            family.family_revision_id,
            family.entity_binding_id,
            family.root.revision_id,
        ) != (*plan.source_metadata, plan.source_root.revision_id):
            raise FamilyBlockValidationError("winner SOURCE original native identity differs")
    elif (
        family.retained is not True
        or family.entity_binding_id != plan.base_header.entity_binding_id
        or family.expected_family_sha256 != plan.base_header.family_sha256
    ):
        raise FamilyBlockValidationError("winner retained original identity differs")


def _family_entries(context, entries, retained, work, counts):
    with (
        closing(read_family_plan(context.final_plan, check=work.current)) as plans,
        closing(_input_iterator(entries)) as families,
    ):
        for plan in plans:
            if plan.selection_kind == "absent":
                continue
            supplied = next(families, _MISSING)
            if supplied is _MISSING or type(supplied) is not tuple or len(supplied) != 3:
                raise FamilyBlockValidationError("winner original selected family coverage differs")
            shard, actual_plan, family = supplied
            if _plan_cells(actual_plan) != _plan_cells(plan):
                raise FamilyBlockValidationError("winner original selected plan differs")
            _family_identity(plan, family)
            if plan.selection_kind == "retained":
                original = next(retained, _MISSING)
                if original is _MISSING or _plan_cells(original[0]) != _plan_cells(plan):
                    raise FamilyBlockValidationError("winner retained destination coverage differs")
                yield shard, plan, family, original[2]
            else:
                yield shard, plan, family, None
            counts[plan.selection_kind] += 1
        if next(families, _MISSING) is not _MISSING or next(retained, _MISSING) is not _MISSING:
            raise FamilyBlockValidationError("winner original families contain trailing selections")


def _write_family(layout, family, assigned, shard, contract, work, target, path):
    candidates = family_candidates(layout, family, check=work.current)
    assigned_candidates = None if assigned is None else family_candidates(layout, assigned, check=work.current)
    with closing(candidates), closing(_input_iterator(()) if assigned_candidates is None else assigned_candidates):
        for candidate in candidates:
            cells = _cells(contract, candidate)
            if assigned_candidates is not None:
                actual = next(assigned_candidates, _MISSING)
                if actual is _MISSING or cells != _cells(contract, actual):
                    raise FamilyBlockValidationError(
                        "winner retained candidate differs from original destination bytes"
                    )
            work.frame(target, (shard, _pack_row(cells)), path)
        if assigned_candidates is not None and next(assigned_candidates, _MISSING) is not _MISSING:
            raise FamilyBlockValidationError("winner retained candidate coverage is incomplete")


def _stage_originals(context, entries, destination, shard_count, contract, work, digest):
    path, counts = work.path(), dict(source=0, retained=0)
    with ExitStack() as stack:
        retained = (
            iter(())
            if destination is None
            else stack.enter_context(closing(_replay_families(destination, context.common.layout.definition, work)))
        )
        candidate_file = stack.enter_context(path.open("xb"))
        families = stack.enter_context(closing(_family_entries(context, entries, retained, work, counts)))
        for shard, plan, family, assigned in families:
            work.current()
            _integer(shard, "winner original stream index", zero=True)
            if shard >= shard_count:
                raise FamilyBlockValidationError("winner original family stream index lies outside its cohort")
            _write_family(context.common.layout, family, assigned, shard, contract, work, candidate_file, path)
            _add(digest, "family", (shard, plan.typed_key, family.family_revision_id, family.expected_family_sha256))
    if tuple(counts.values()) != tuple(context.final_plan.counts_by_kind[name] for name in counts):
        raise FamilyBlockValidationError("winner original complete family census differs")
    return path, tuple(counts.items())


def _stage_audit(contract, descriptor, work):
    paths_by_profile = {slot: work.path() for slot in descriptor.profile_slots}
    with ExitStack() as stack:
        targets_by_profile = {slot: stack.enter_context(path.open("xb")) for slot, path in paths_by_profile.items()}
        candidates = stack.enter_context(closing(output.read_winner_candidates(descriptor, check=work.current)))
        for slot, shard, ordinal, candidate in candidates:
            work.current()
            work.frame(
                targets_by_profile[slot],
                (shard, ordinal, _pack_row(_cells(contract, candidate))),
                paths_by_profile[slot],
            )
    return paths_by_profile


def _compare_raw(descriptor, original_path, audit_paths, work, digest):
    counts = []
    for slot in descriptor.profile_slots:
        expected = work.rows(original_path)
        observed = _sorted(
            work, ((shard, packed) for shard, _ordinal, packed in work.rows(audit_paths[slot])), lambda row: row
        )
        count = 0
        with closing(expected), closing(observed):
            for original, actual in zip_longest(expected, observed, fillvalue=_MISSING):
                work.current()
                if original is _MISSING or actual is _MISSING or original != actual:
                    raise FamilyBlockValidationError("winner raw candidate membership or multiplicity differs")
                _add(digest, "candidate", (slot, *original))
                count += 1
        counts.append((slot, count))
    return tuple(counts)


def _audit_paths(audit_path, shard_count, work):
    empty = work.write(())
    paths_by_stream = dict.fromkeys(range(shard_count), empty)
    with closing(_sorted(work, work.rows(audit_path), lambda row: row[:2])) as ordered:
        for shard, rows in groupby(ordered, lambda row: row[0]):
            work.current()
            paths_by_stream[shard] = work.write((ordinal, packed) for _shard, ordinal, packed in rows)
    return paths_by_stream


class _StreamHeads:
    def __init__(self, work):
        self.work, self.reserved = work, 0

    def reserve(self, packed):
        """Charge the live raw and normalized candidate head before decoding it."""
        size = 4096 + 8 * len(packed)
        if self.reserved + size > self.work.limits.memory_bytes:
            raise FamilyBlockResourceLimit("winner reducer heads exceed the owner buffer")
        self.reserved += size
        return size


def _candidates(work, path, heads):
    with closing(work.rows(path)) as rows:
        for ordinal, (actual, packed) in enumerate(rows):
            work.current()
            if actual != ordinal:
                raise FamilyBlockValidationError("winner audited candidate stream order differs")
            reserved = heads.reserve(packed)
            try:
                yield output._candidate_from_cells(_unpack_row(packed))
            finally:
                heads.reserved -= reserved


def _reduced_profile(context, generation, slot, paths, scopes, work):
    heads = _StreamHeads(work)
    with ExitStack() as stack:
        shards = tuple(
            ValidatedWinnerCandidateStream(
                generation, stack.enter_context(closing(_candidates(work, paths[shard], heads)))
            )
            for shard in range(len(paths))
        )
        winners = iter_shard_profile_winners(
            context.common.layout.definition,
            generation=generation,
            profile_slot=slot,
            shards=shards,
            child_collection_slots=context.common.layout.collection_slots,
        )
        with closing(winners):
            for winner in winners:
                work.current()
                yield output._winner_cells(winner, context.common.layout.definition, scopes)


def _reduced_rows(context, generation, descriptor, audit_paths, shard_count, scopes, work):
    for slot in descriptor.profile_slots:
        paths = _audit_paths(audit_paths[slot], shard_count, work)
        try:
            yield from _reduced_profile(context, generation, slot, paths, scopes, work)
        finally:
            for path in {*paths.values(), audit_paths[slot]}:
                work.remove(path)


def _compare_winners(context, generation, descriptor, audit, shard_count, scopes, work, digest):
    count = 0
    with (
        closing(_reduced_rows(context, generation, descriptor, audit, shard_count, scopes, work)) as expected,
        closing(output.read_winner_rows(descriptor, check=work.current)) as actual,
    ):
        for left, right in zip_longest(expected, actual, fillvalue=_MISSING):
            work.current()
            if left is _MISSING or right is _MISSING or not _equal_native(left, right):
                raise FamilyBlockValidationError("winner output differs from its complete original global reduction")
            _add(digest, "winner", left)
            count += 1
    return count


def _compare_contexts(context, descriptor, families, shard_count, retained, contract, work, digest):
    with closing(read_retained_input(context.retained_input, check=work.current)) as original_entries:
        for _record in original_entries:
            work.current()
    originals, counts = _stage_originals(context, families, retained, shard_count, contract, work, digest)
    with closing(_sorted(work, work.rows(originals), lambda row: row)) as ordered:
        ordered_originals = work.write(ordered)
    work.remove(originals)
    audit = _stage_audit(contract, descriptor, work)
    profiles = _compare_raw(descriptor, ordered_originals, audit, work, digest)
    work.remove(ordered_originals)
    return profiles, counts, audit


def prove_original_winners(
    context, source_comparison, generation, descriptor, family_entries, shard_count, *, retained_originals=None, work
):
    """Compare all raw contexts, then replay the original complete shard reduction.

    Entries are real original-reader (factory stream index, plan, family) tuples.
    Keep the original source/base/destination contexts live through full EOF.
    """
    if type(work) is not PlanWork:
        raise ValueError("winner comparison requires explicit accounted PlanWork")
    scopes, contract = _scope(context, source_comparison, generation, descriptor, shard_count, retained_originals, work)
    retained_destination = None if retained_originals is None else retained_originals.destination
    bounded = replace(work, limits=replace(work.limits, memory_bytes=max(1, work.limits.memory_bytes // 16)))
    scratch, comparison, primary = _Scratch(bounded), None, None
    try:
        digest = hashlib.sha256(b"custom-import/winner-original-comparison/v1\0")
        inputs = (
            source_comparison.sha256,
            None if retained_destination is None else retained_destination.sha256,
            descriptor.sha256,
        )
        _add(digest, "inputs", (_producer(context.common), astuple(generation), inputs, shard_count))
        retained_counts = None if retained_originals is None else retained_originals.comparison.counts
        scalar_counts = (
            None
            if retained_originals is None
            else (retained_originals.comparison.root_scalar_count, retained_originals.comparison.child_scalar_count)
        )
        _add(digest, "retained_original_counts", retained_counts)
        _add(digest, "retained_scalar_counts", scalar_counts)
        profiles, counts, audit = _compare_contexts(
            context, descriptor, family_entries, shard_count, retained_destination, contract, scratch, digest
        )
        winner_count = _compare_winners(context, generation, descriptor, audit, shard_count, scopes, scratch, digest)
        _scope(context, source_comparison, generation, descriptor, shard_count, retained_originals, work)
        comparison = WinnerOriginalComparison(
            _producer(context.common),
            astuple(generation),
            *inputs,
            retained_counts,
            scalar_counts,
            profiles,
            counts,
            winner_count,
            digest.digest(),
        )
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return comparison
