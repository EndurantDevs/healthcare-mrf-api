# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Preserve SOURCE response counts without reading legacy occurrence tables."""

from collections import namedtuple
from contextlib import closing
from dataclasses import replace

from process.custom_import import build_counts as legacy
from process.custom_import.build_source import _source_cleanup_failure
from process.custom_import.family_shard_admission import read_admission_occurrences, read_admission_resolutions
from process.custom_import.family_shard_base import _read_deadline, _require_deadline
from process.custom_import.family_shard_build import SourceAdmissionFacts
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_events import _scope as _admission_scope
from process.custom_import.family_shard_facts import _sorted
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from process.custom_import.runner_types import CandidateRunnerError

_Partial = namedtuple(
    "_Partial", (*legacy._FamilySummary.__dataclass_fields__, "bad_collection", "bad_code", "malformed")
)


def _has_invalid_code(is_root, raw, initial, resolved):
    if is_root:
        return initial is not None and (
            initial not in legacy._ROOT_CODES or (raw is not None and initial in legacy._UNKEYED_ROOT_CODES)
        )
    return (
        (initial is not None and initial not in legacy._CHILD_CODES - legacy._CHILD_RESOLVED_CODES)
        or (resolved is not None and resolved not in legacy._CHILD_CODES)
        or (
            initial is None
            and resolved is not None
            and resolved not in legacy._CHILD_RESOLVED_CODES | legacy._CHILD_CANDIDATE_CODES
        )
    )


def _outcome_row(occurrence, resolved, order, collection_slots):
    record = occurrence.record
    is_root = record.collection_slot == 0
    raw_key, raw_hash = (None, None) if record.raw_parent_key is None else record.raw_parent_key
    initial = record.rejection_code
    facts = SourceAdmissionFacts(**occurrence.facts_by_name)
    child_code = initial if initial is not None else resolved
    if initial in legacy._CHILD_CANDIDATE_CODES or resolved in legacy._CHILD_CANDIDATE_CODES:
        child_code = None
    return _Partial(
        raw_hash,
        raw_key,
        False,
        int(is_root),
        order if is_root else None,
        initial if is_root else None,
        occurrence.root_record_id if is_root else None,
        occurrence.revision_id if is_root else None,
        is_root and facts.typed_root_count > 1,
        0 if is_root else legacy._CODE_BITS.get(child_code, 0),
        not is_root and occurrence.collection_slot not in collection_slots,
        _has_invalid_code(is_root, raw_hash, initial, resolved)
        or ((initial is None) != (occurrence.initial_rejection_id is None)),
        int(is_root and initial in legacy._UNKEYED_ROOT_CODES),
    )


def _admitted_partials(work, context, admission):
    collection_slots = frozenset(context.layout.collection_slots.values())
    with (
        closing(read_admission_occurrences(admission, check=work.current)) as occurrences,
        closing(read_admission_resolutions(admission, check=work.current)) as resolutions,
    ):
        resolution, previous = next(resolutions, None), None
        for order, occurrence in enumerate(occurrences, 1):
            work.current()
            if previous is not None and occurrence.coordinate <= previous:
                raise CandidateRunnerError("source outcome occurrence order differs")
            if resolution is not None and resolution.target < occurrence.coordinate:
                raise CandidateRunnerError("source outcome resolution lacks an occurrence")
            code = None
            if resolution is not None and resolution.target == occurrence.coordinate:
                code = resolution.event.code
                resolution = next(resolutions, None)
            yield _outcome_row(occurrence, code, order, collection_slots)
            previous = occurrence.coordinate
        if resolution is not None:
            raise CandidateRunnerError("source outcome resolution has trailing targets")


def count_admitted_source_outcomes(context, admission, *, work):
    """Count the complete immutable admission through the unchanged legacy reducer.

    Run off the owner loop under its existing heartbeat and fresh authority.
    These source-only values are provisional until the owner completes native
    admission; they neither record acceptance nor include retained families.
    """
    if type(context) is not SourceCommonContext or type(work) is not PlanWork:
        raise ValueError("source outcomes require the original context and bounded work")
    scope = _admission_scope(context.request, context.layout, context.source, admission)
    deadline = _read_deadline(context.request)

    def current():
        """Keep the original absolute deadline and the caller's stop check."""
        _require_deadline(deadline)
        if work.check is not None:
            work.check()

    scratch, primary, outcomes = _Scratch(replace(work, check=current)), None, None
    try:
        scratch.current()
        staged = scratch.write(_admitted_partials(scratch, context, admission))
        ordered = _sorted(scratch, scratch.rows(staged), lambda cells: (cells[0] is None, cells[0] or b""))
        total = legacy._OutcomeTotal()
        with closing(ordered):
            for cells in ordered:
                scratch.current()
                total.add(_Partial(*cells))
        outcomes = total.result()
        if _admission_scope(context.request, context.layout, context.source, admission) != scope:
            raise CandidateRunnerError("source outcome immutable scope changed")
        scratch.current()
    except BaseException as error:
        primary = error
    try:
        scratch.close()
    except BaseException as error:
        primary = _source_cleanup_failure(primary, error)
    if primary is not None:
        raise primary
    return outcomes
