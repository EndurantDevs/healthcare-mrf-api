# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Old logical page decisions over bounded owner-supplied original cost views.

This module performs no SQL, allocation, COPY or authority transition. Its
previews are not native receipts. Current original membership and all declared
byte/row inputs must already have been proved by the owner input readers.
"""

from collections import deque
from dataclasses import dataclass
from itertools import islice

from db.models.custom_import import CustomImportBuildFamily, CustomImportFamilyRevision
from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as old
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.runner_types import CandidateRunnerError


@dataclass(frozen=True)
class LegacyFamilyView:
    """Exact old read/reservation models and independent completed input ranges."""

    source: object
    pending_models: tuple
    child_start: int
    child_end: int
    original_cells: tuple = ()
    owned_bytes: int = 0

    @property
    def root_id(self):
        """Return the observed stable dictionary identity used by original root ordering."""
        return self.source.plan.root_record_id

    @property
    def pending_bytes(self):
        """Retain the original selected-model byte reservation without native ID inference."""
        return graph._model_bytes(model for model in self.pending_models if model is not None)


@dataclass(frozen=True)
class LegacyChildAttempt:
    """Inert old arrays and groups; completion is only a pure cost preview."""

    kind: str
    groups: tuple
    arguments: tuple
    pages: tuple
    consumed: int


def root_cohort_limit(request, registry):
    """Keep the actual old physical root-prefix limit, independent of input kind."""
    return MAX_BATCH_ROWS // old._root_fanout(request, registry)


def root_cohorts(request, registry, families, *, check=None, memory_bytes=None):
    """Yield original mixed root-ID prefixes admitted under the same half budget."""
    count_limit = root_cohort_limit(request, registry)
    cohort, used_bytes, owned_bytes, previous = [], 0, 0, None
    for family in families:
        if check is not None:
            check()
        if previous is not None and family.root_id <= previous:
            raise CandidateRunnerError("legacy cost roots repeat or are unordered")
        charge = family.pending_bytes
        if charge > MAX_BATCH_BYTES // 2:
            raise CandidateRunnerError("one build record exceeds the admitted byte page")
        if cohort and (len(cohort) == count_limit or used_bytes + charge > MAX_BATCH_BYTES // 2):
            yield tuple(cohort)
            cohort, used_bytes, owned_bytes = [], 0, 0
        owned_bytes += family.owned_bytes
        if memory_bytes is not None and owned_bytes > memory_bytes:
            raise FamilyBlockResourceLimit("legacy physical root cohort exceeds the owner buffer")
        cohort.append(family)
        used_bytes += charge
        previous = family.root_id
    if cohort:
        yield tuple(cohort)


def started_view(source, *, family_id, revision_id, entity_id):
    """Reconstruct exactly the old post-root in-memory shape, never a DB receipt."""
    original = source.plan
    current = CustomImportBuildFamily(
        build_id=original.build_id,
        root_record_id=original.root_record_id,
        selection_kind=original.selection_kind,
        base_family_revision_id=original.base_family_revision_id,
        family_revision_id=family_id,
        attached_child_count=0,
        last_child_collection_slot=None,
        last_child_key_sha256=None,
        last_input_child_revision_id=None,
    )
    # The old _root_receipts in-memory family intentionally has no producer token
    # or dataset columns populated. Adding them changes its byte reservation.
    family = CustomImportFamilyRevision(
        family_revision_id=family_id,
        root_record_id=original.root_record_id,
        root_revision_id=revision_id,
        entity_binding_id=entity_id,
        family_sha256=source.family_sha256,
        child_count=source.child_count,
    )
    return old._Started(source, current, family, source.child_count == 0)


def initial_limits(request, registry):
    """Use the exact old shared SOURCE/retained logical and physical row limits."""
    return old._child_limit(request, registry), MAX_BATCH_ROWS // old._child_fanout(request, registry)


def smaller_limits(limits, attempt=None):
    """Match old native work-cost rejection without committing any preview tip."""
    logical, physical = limits
    count = sum(attempt.arguments[-1][1]) if attempt is not None else logical
    if count < 2:
        raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")
    largest = max(attempt.arguments[-1][1]) if attempt is not None else logical
    return max(1, min(logical, largest) // 2), min(physical, max(1, count // 2))


def physical_child_limit(request, registry, states, limits):
    """Use the original shared root-window physical fanout reservation."""
    return min(limits[1], (MAX_BATCH_ROWS - len(states)) // old._child_fanout(request, registry))


def admitted_children(request, inputs, states, candidates):
    """Apply the old physical metadata byte prefix before preparing logical pages."""
    _tips, reservations, _spans = old._child_reservations(request, inputs, states)
    held_bytes = sum(reservations.values())
    metadata = [(position, old._child_model_bytes(child)) for position, (_root, child, _scope) in enumerate(candidates)]
    admitted = graph._admitted_keys(metadata, (MAX_BATCH_BYTES - held_bytes) // 2)
    return candidates[: len(admitted)]


def child_attempt(request, registry, inputs, states, kind, candidates, limits, *, check=None):
    """Reuse every original pure logical grouping/argument/cost decision once."""
    candidate_rows = tuple(islice(candidates, physical_child_limit(request, registry, states, limits)))
    child_rows = admitted_children(request, inputs, states, candidate_rows)
    if not child_rows:
        raise CandidateRunnerError("legacy cost child input ended before its complete family")
    tips, reservations, spans = old._child_reservations(request, inputs, states)
    held_bytes = sum(reservations.values()) + sum(old._child_model_bytes(child) for _root, child, _scope in child_rows)
    pages, consumed, retained_bytes = [], 0, 0
    while consumed < len(child_rows):
        if check is not None:
            check()
        groups = old._logical_child_page(request, registry, spans, reservations, tips, child_rows, limits[0], consumed)
        _name, arguments = old._child_arguments(request, registry, groups, retained_bytes=retained_bytes)
        page_bytes = old._child_argument_bytes(arguments)
        if kind == "source":
            retained_bytes += old._retained_page_bytes(arguments, page_bytes)
        page_bytes += sum(
            len(group.children[-1].child_key_sha256 or b"") - len(group.state.current.last_child_key_sha256 or b"")
            for group in groups
            if group.children and kind == "source"
        )
        if not arguments[-1][1][0]:
            raise CandidateRunnerError("child set returned inconsistent durable progress")
        if held_bytes + page_bytes > MAX_BATCH_BYTES:
            if not pages:
                raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")
            break
        pages.append((groups, arguments))
        held_bytes += page_bytes
        consumed += arguments[-1][1][0]
        old._advance_child_tips(groups, tips, reservations)
    if not pages:
        raise CandidateRunnerError("record projection fanout exceeds the admitted row page")
    groups, arguments = old._child_batch_arguments(pages)
    return LegacyChildAttempt(kind, groups, arguments, tuple(pages), consumed)


def advance_preview(states, attempt):
    """Update only CPU previews after actual work bounds; no durable receipt minted."""
    updates_by_root = {group.state.current.root_record_id: old._preview_child_state(group) for group in attempt.groups}
    return tuple(updates_by_root.get(state.current.root_record_id, state) for state in states)


def root_attempts(request, registry, inputs):
    """Retain original logical root spans and physical batching as inert arrays."""
    yield from old._root_batches(request, registry, inputs)


def split_root_attempt(request, registry, chunk):
    """Keep the old failed-root midpoint/rebatch order without running old controls."""
    if len(chunk) < 2:
        raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")
    midpoint = len(chunk) // 2
    return deque(
        tuple(old._root_batches(request, registry, chunk[:midpoint]))
        + tuple(old._root_batches(request, registry, chunk[midpoint:]))
    )
