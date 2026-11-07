# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete original admission with aggregate owner metadata, without page mirrors."""

from contextlib import closing
from dataclasses import replace

from sqlalchemy import select

from process.custom_import import family_shard_admission as admission_output
from process.custom_import import family_shard_events as events_output
from process.custom_import.admission_sql import AdmissionError
from process.custom_import.build_source import _page_session
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_store import ShardOutputProof, _candidate, _checked_receipt, _execute
from process.custom_import.family_shard_common import SourceCommonContext, _offload
from process.custom_import.family_shard_control import _source_capture_counts
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.family_shard_source_plan import read_source_parts


def _event_match(expected, actual):
    if (
        type(actual.rejection_id) is not int
        or not 0 < actual.rejection_id < 2**63
        or replace(actual, rejection_id=expected.rejection_id) != expected
        or (expected.origin == "source" and actual.rejection_id != expected.rejection_id)
    ):
        raise FamilyBlockUnavailable("admission completion event identity differs")


def _matched_entry(original, actual):
    if actual is None or actual[0] != original[0]:
        raise FamilyBlockUnavailable("admission completion event/target coverage differs")
    if original[0] == "event":
        _event_match(admission_output.AdmissionEvent(*original[1]), admission_output.AdmissionEvent(*actual[1]))
    else:
        if original[1] != actual[1]:
            raise FamilyBlockUnavailable("admission completion earliest target differs")
        _event_match(admission_output.AdmissionEvent(*original[2]), admission_output.AdmissionEvent(*actual[2]))


def _original_admission(admission, events, check):
    errors, records, fatal = 0, 0, None
    with (
        closing(admission_output._checked_entries(admission, check)) as originals,
        closing(events_output._checked_entries(events)) as allocated,
    ):
        for entry in originals:
            if check is not None:
                check()
            if entry[0] == "occurrence":
                occurrence = admission_output.AdmissionOccurrence(*entry[1])
                decision = occurrence.decision
                errors += int(decision.is_candidate_error)
                records += 1
                if decision.fatal_problem is not None and (fatal is None or occurrence.coordinate < fatal[0]):
                    fatal = occurrence.coordinate, decision.fatal_problem
            elif entry[0] in {"event", "resolution"}:
                _matched_entry(entry, next(allocated, None))
        if next(allocated, None) != ("complete", events.counts) or next(allocated, None) is not None:
            raise FamilyBlockUnavailable("admission completion events require complete EOF")
    if records != admission.counts_by_kind["records"] or errors != admission.counts_by_kind["candidate_errors"]:
        raise FamilyBlockUnavailable("admission completion original decision totals differ")
    if fatal is not None:
        raise AdmissionError(fatal[1])
    return errors


def _admission_completion(context, admission, events, receipt, limits, check):
    scope = events_output._scope(context.request, context.layout, context.source, admission)
    if type(events) is not events_output.AdmissionEventsDescriptor or _pack_row(events.scope) != _pack_row(scope):
        raise FamilyBlockUnavailable("admission completion allocated scope differs")
    expected = _checked_receipt(context.layout, receipt)
    partition = bytes.fromhex(context.source.source.partition_sha256)
    if expected["source_record_count"] != 0 or expected["partition_sha256"] != partition:
        raise FamilyBlockUnavailable("admission completion COPY scope differs")
    errors = _original_admission(admission, events, check)
    errors += int(context.request.definition.refresh_mode == "snapshot" and not context.request.complete_scope)
    if events.counts[:2] != (
        admission.counts_by_kind["source_events"],
        admission.counts_by_kind["admission_events"],
    ):
        raise FamilyBlockUnavailable("admission completion rejection census differs")
    proof = ShardOutputProof(context.layout, expected["shard_id"], partition, 0)
    with closing(
        events_output.admission_event_batches(
            events, maximum_rows=limits.maximum_batch_records, maximum_bytes=limits.maximum_batch_bytes
        )
    ) as batches:
        for batch in batches:
            if check is not None:
                check()
            for role, copy_values in batch.items():
                proof.add(role, copy_values)
    if proof.receipt() != expected:
        raise FamilyBlockUnavailable("admission completion COPY bytes differ")
    counts_by_stream = {slot: [0, 0] for slot, _count in context.source.source.source_counts}
    with closing(read_source_parts(context.source.source)) as parts:
        for slot, ordinal, count in parts:
            if check is not None:
                check()
            counts_by_stream[slot][0] = ordinal
            counts_by_stream[slot][1] += count
    return expected, errors, counts_by_stream


async def complete_shard_admission(
    session_factory, context, build_id, admission, events, receipt, *, limits, check=None
):
    """Record genuine admission/rejection totals; GRAPH selection and proof follow."""
    if type(context) is not SourceCommonContext:
        raise FamilyBlockUnavailable("admission completion requires original owner scope")
    expected, errors, counts_by_stream = await _offload(
        _admission_completion, context, admission, events, receipt, limits, check
    )
    total_events = sum(events.counts[:2])
    phase = "rejected" if errors else "graph"
    async with _page_session(session_factory, context.request, build_id) as (session, build):
        _, binding = await _candidate(
            session, context.request, context.source.snapshot_family_id, context.layout, {"loading"}
        )
        table = binding.tables_by_role["shard_receipt"]
        actual = (
            (await _execute(session, select(table).where(table.c.shard_id == expected["shard_id"])))
            .mappings()
            .one_or_none()
        )
        if actual is None or {name: actual[name] for name in expected} != expected:
            raise FamilyBlockUnavailable("admission completion COPY is not committed")
        await _source_capture_counts(session, context, counts_by_stream)
        if build.source_frozen_at is None or build.source_occurrence_count != context.source.source.counts[0]:
            raise FamilyBlockUnavailable("admission completion requires frozen original SOURCE")
        if build.phase == phase:
            if (build.candidate_error_count, build.next_rejection_ordinal) != (errors, total_events):
                raise FamilyBlockUnavailable("admission completion retry totals differ")
            return phase
        if build.phase != "admission" or (build.candidate_error_count, build.next_rejection_ordinal) != (
            0,
            context.source.source.counts[4],
        ):
            raise FamilyBlockUnavailable("admission completion phase or initial totals differ")
        build.candidate_error_count, build.next_rejection_ordinal, build.phase = errors, total_events, phase
        return phase
