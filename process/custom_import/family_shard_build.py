# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure SOURCE/admission decisions over complete, externally grouped facts.

This is the decision kernel, not a replacement for complete-source fact
aggregation. Callers must finish both raw and typed key passes, child duplicate
and membership passes, then supply ordered streams. Nothing here authorizes
COPY, treats raw keys as typed identities, or publishes provisional families.
"""

from dataclasses import dataclass

from process.custom_import.family import CandidateRejected

_CANDIDATE_ERRORS = frozenset(("root_not_object", "root_key_missing", "child_not_object", "orphan_child"))


def occurrence_order(record):
    """Retain original SOURCE stream order, independent of shard scheduling."""
    return record.stream_slot, record.source_ordinal


@dataclass(frozen=True)
class SourceAdmissionFacts:
    """Required complete-set facts; unknown counts are refused, never zero-filled.

    Counts include rejected rows wherever the SQL does. Child duplicate and
    last-record facts use only initially valid child revisions; the raw parent
    points to the first SOURCE root, even when that root is initially invalid.
    """

    raw_root_count: int
    typed_root_count: int
    first_raw_root: tuple | None
    parent_raw_differs: bool = False
    child_count: int = 0
    collapse_identical: bool = False
    last_child: tuple | None = None
    payload_differs_from_last: bool | None = None
    membership_problem: str | None = None
    raw_key_collision: bool = False
    child_key_collision: bool = False

    def __post_init__(self):
        for count in (self.raw_root_count, self.typed_root_count, self.child_count):
            if type(count) is not int or count < 0:
                raise ValueError("admission facts are incomplete")
        if (self.first_raw_root is None) != (self.raw_root_count == 0):
            raise ValueError("admission raw parent facts differ")
        for key in (self.first_raw_root, self.last_child):
            if key is not None:
                _order(key)
        for value in (
            self.parent_raw_differs,
            self.collapse_identical,
            self.raw_key_collision,
            self.child_key_collision,
        ):
            if type(value) is not bool:
                raise ValueError("admission equality facts differ")
        if self.payload_differs_from_last is not None and type(self.payload_differs_from_last) is not bool:
            raise ValueError("admission payload equality witness differs")


@dataclass(frozen=True)
class RejectionEvent:
    origin: str
    occurrence: tuple
    code: str

    def __post_init__(self):
        _order(self.occurrence)
        if self.origin not in ("source", "admission") or type(self.code) is not str or not self.code:
            raise ValueError("admission rejection event differs")


@dataclass(frozen=True)
class RejectionTarget:
    occurrence: tuple
    event: RejectionEvent


@dataclass(frozen=True)
class AdmissionDecision:
    occurrence: tuple
    code: str | None
    error_code: str | None
    event: RejectionEvent | None
    targets: tuple
    fatal_problem: str | None

    @property
    def is_candidate_error(self):
        """Report whether this decision rejects the candidate source occurrence."""
        return self.error_code in _CANDIDATE_ERRORS


def _order(value):
    if type(value) is not tuple or len(value) != 2 or any(type(cell) is not int for cell in value):
        raise ValueError("admission occurrence order differs")
    if value[0] <= 0 or value[1] < 0:
        raise ValueError("admission occurrence order differs")
    return value


def initial_rejection(record):
    """SOURCE events exist even if later admission replaces their resolution."""
    return (
        None
        if record.rejection_code is None
        else RejectionEvent("source", occurrence_order(record), record.rejection_code)
    )


def primary_code(source_record, facts):
    """Match admission.sql primary_codes without changing its priority."""
    if type(facts) is not SourceAdmissionFacts:
        raise ValueError("admission requires complete source facts")
    initial = source_record.rejection_code
    if source_record.collection_slot == 0:
        if source_record.raw_parent_key is not None and facts.raw_root_count == 0:
            raise ValueError("admission raw root count is incomplete")
        if source_record.typed_parent_key is not None and facts.typed_root_count == 0:
            raise ValueError("admission typed root count is incomplete")
        return initial or ("duplicate_root_key" if facts.raw_root_count > 1 or facts.typed_root_count > 1 else None)
    if initial == "child_not_object":
        return initial
    if facts.first_raw_root is None or facts.parent_raw_differs:
        return "orphan_child"
    if initial is None and source_record.child_key is not None and facts.child_count == 0:
        raise ValueError("admission child count is incomplete")
    if (
        initial is None
        and facts.child_count > 1
        and facts.collapse_identical
        and facts.last_child is not None
        and facts.payload_differs_from_last is None
    ):
        raise ValueError("admission last payload witness is incomplete")
    if (
        initial is None
        and facts.child_count > 1
        and (not facts.collapse_identical or facts.last_child is None or facts.payload_differs_from_last)
    ):
        return "duplicate_child_key"
    return initial


def decide_occurrence(source_record, facts, *, earlier_child_target=None):
    """Replay original occurrence decisions after complete fact passes.

    An earlier child suppresses only its exact first raw root's own action.
    Pass the earliest rejecting child's target from a complete ordered reduction;
    a count-only or code-set comparison cannot establish this relationship.
    """
    order = _order(occurrence_order(source_record))
    code = primary_code(source_record, facts)
    if code is None and facts.membership_problem == "child_membership_missing":
        code = facts.membership_problem
    is_suppressed = False
    if earlier_child_target is not None:
        if type(earlier_child_target) is not RejectionTarget or earlier_child_target.occurrence != order:
            raise ValueError("admission root propagation target differs")
        is_suppressed = source_record.collection_slot == 0 and earlier_child_target.event.occurrence < order
    problem = None
    if not is_suppressed:
        if source_record.raw_parent_key is not None and facts.raw_key_collision:
            problem = "custom_import_build_structure_mismatch: raw key digest collision"
        elif facts.membership_problem not in (None, "child_membership_missing"):
            problem = facts.membership_problem
        elif source_record.collection_slot and source_record.child_key is not None and facts.child_key_collision:
            problem = "custom_import_build_structure_mismatch: child key digest collision"
    event = (
        None
        if code is None or is_suppressed
        else RejectionEvent("source" if code == source_record.rejection_code else "admission", order, code)
    )
    rejection_targets = () if event is None else (RejectionTarget(order, event),)
    if (
        event is not None
        and source_record.collection_slot
        and code != "orphan_child"
        and facts.first_raw_root is not None
    ):
        rejection_targets += (RejectionTarget(facts.first_raw_root, event),)
    return AdmissionDecision(
        order, code, source_record.rejection_code if is_suppressed else code, event, rejection_targets, problem
    )


def first_resolutions(ordered_targets):
    """Reduce an external (target occurrence, event occurrence) sort in O(1) RAM."""
    previous = None
    for target in ordered_targets:
        if type(target) is not RejectionTarget:
            raise ValueError("admission resolution target differs")
        key = _order(target.occurrence), _order(target.event.occurrence)
        if previous is not None and key < previous:
            raise ValueError("admission resolution targets are not ordered")
        if previous is None or key[0] != previous[0]:
            yield target
        previous = key


def rejection_ordinals(source_events, admission_events, *, first_ordinal=0):
    """Initial SOURCE events precede every new ADMISSION event, as before."""
    if type(first_ordinal) is not int or first_ordinal < 0:
        raise ValueError("admission rejection ordinal differs")
    ordinal = first_ordinal
    for origin, events in (("source", source_events), ("admission", admission_events)):
        previous = None
        for event in events:
            if type(event) is not RejectionEvent or event.origin != origin:
                raise ValueError("admission rejection phase differs")
            if previous is not None and event.occurrence <= previous:
                raise ValueError("admission rejection events are not ordered uniquely")
            yield ordinal, event
            ordinal += 1
            previous = event.occurrence


def family_selection(source_root, resolution, *, has_base, refresh_mode, complete_scope, source_facts=None):
    """Match native planning by typed identity, never Python raw-key equality."""
    if refresh_mode not in ("snapshot", "upsert") or type(has_base) is not bool or type(complete_scope) is not bool:
        raise ValueError("family selection scope differs")
    if refresh_mode == "snapshot" and not complete_scope:
        raise CandidateRejected("snapshot_scope_incomplete")
    if source_root is not None:
        if source_root.collection_slot != 0 or source_root.typed_parent_key is None:
            raise ValueError("family selection requires an exact typed source root")
        if type(source_facts) is not SourceAdmissionFacts:
            raise ValueError("family selection requires complete source facts")
        if resolution is not None and (
            type(resolution) is not RejectionTarget or resolution.occurrence != occurrence_order(source_root)
        ):
            raise ValueError("family source resolution target differs")
        if primary_code(source_root, source_facts) is not None and resolution is None:
            raise ValueError("family source rejection has no complete resolution")
        if resolution is None:
            return "source"
    elif resolution is not None or source_facts is not None:
        raise ValueError("absent typed source cannot have a raw-key resolution")
    return "retained" if has_base and (refresh_mode == "upsert" or resolution is not None) else "absent"
