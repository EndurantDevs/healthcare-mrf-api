# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reduce bounded edition metadata without admitting or publishing source rows."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

MAX_SOURCE_EVENTS = 100
MAX_SOURCE_SCOPES = 100
_MAX_SEQUENCE = 2**63 - 1
_EVENT_KINDS = frozenset({"discovered", "prepared", "validated", "unavailable", "failed"})
_CANDIDATE_KINDS = frozenset({"discovered", "prepared", "validated"})
_DECISIONS = frozenset({"noop", "prepare", "validate", "ready_for_admission", "review", "unavailable", "failed"})


class RegistrySourceEventError(ValueError):
    """Reject invalid control metadata before returning any partial batch."""

    def __init__(self):
        super().__init__("Registry source events or state are invalid")


def _text(value, limit):
    return type(value) is str and value == value.strip() and 0 < len(value) <= limit and value.isprintable()


def _scope(value):
    if not _text(value.source_system, 64) or not _text(value.source_id, 128):
        raise RegistrySourceEventError()
    return value.source_system, value.source_id


@dataclass(frozen=True)
class RegistrySourceAcceptedEdition:
    """Immutable edition evidence; only a state's accepted field denotes acceptance."""

    source_system: str
    source_id: str
    edition_id: str
    artifact_sha256: str
    input_sha256: str
    parser_version: str
    header_sha256: str

    def __post_init__(self):
        _scope(self)
        if not _text(self.edition_id, 128) or not _text(self.parser_version, 128):
            raise RegistrySourceEventError()
        for digest in (self.artifact_sha256, self.input_sha256, self.header_sha256):
            if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise RegistrySourceEventError()


@dataclass(frozen=True)
class RegistrySourceEvent:
    """A source-local monotone sequence and explicit candidate identity."""

    source_system: str
    source_id: str
    event_kind: str
    event_sequence: int
    edition: RegistrySourceAcceptedEdition | None = None
    reviewed: bool = False

    def __post_init__(self):
        scope = _scope(self)
        if type(self.event_kind) is not str or self.event_kind not in _EVENT_KINDS:
            raise RegistrySourceEventError()
        if type(self.event_sequence) is not int or not 1 <= self.event_sequence <= _MAX_SEQUENCE:
            raise RegistrySourceEventError()
        if type(self.reviewed) is not bool or (self.reviewed and self.event_kind != "prepared"):
            raise RegistrySourceEventError()
        if self.edition is None:
            if self.event_kind in _CANDIDATE_KINDS:
                raise RegistrySourceEventError()
        elif type(self.edition) is not RegistrySourceAcceptedEdition or _scope(replace(self.edition)) != scope:
            raise RegistrySourceEventError()


@dataclass(frozen=True)
class RegistrySourceEventState:
    """Retain accepted evidence, candidate progress and the last source event."""

    source_system: str
    source_id: str
    accepted: RegistrySourceAcceptedEdition | None = None
    candidate: RegistrySourceEvent | None = None
    last_event: RegistrySourceEvent | None = None
    decision: str = "noop"
    review_required: bool = False

    def __post_init__(self):
        scope = _scope(self)
        if type(self.decision) is not str or self.decision not in _DECISIONS or type(self.review_required) is not bool:
            raise RegistrySourceEventError()
        if self.accepted is not None and (
            type(self.accepted) is not RegistrySourceAcceptedEdition or _scope(replace(self.accepted)) != scope
        ):
            raise RegistrySourceEventError()
        for event in (self.candidate, self.last_event):
            if event is not None and (type(event) is not RegistrySourceEvent or _scope(replace(event)) != scope):
                raise RegistrySourceEventError()
        if self.last_event is None and (self.candidate is not None or self.decision != "noop"):
            raise RegistrySourceEventError()
        if self.candidate is None:
            if self.review_required:
                raise RegistrySourceEventError()
        elif (
            self.candidate.event_kind not in _CANDIDATE_KINDS
            or self.candidate.event_sequence > self.last_event.event_sequence
            or (self.review_required and self.candidate.event_kind != "discovered")
        ):
            raise RegistrySourceEventError()


@dataclass(frozen=True)
class RegistrySourceEventDecision:
    source_system: str
    source_id: str
    event_sequence: int
    decision: str


@dataclass(frozen=True)
class RegistrySourceEventBatch:
    states: tuple[RegistrySourceEventState, ...]
    decisions: tuple[RegistrySourceEventDecision, ...]


def _validated_batch(states, events):
    if type(states) not in {list, tuple} or len(states) > MAX_SOURCE_SCOPES:
        raise RegistrySourceEventError()
    if type(events) not in {list, tuple} or len(events) > MAX_SOURCE_EVENTS:
        raise RegistrySourceEventError()
    states_by_scope = {}
    for state in tuple(states):
        if type(state) is not RegistrySourceEventState or _scope(state) in states_by_scope:
            raise RegistrySourceEventError()
        states_by_scope[_scope(state)] = replace(state)
    events_by_sequence = {}
    validated_events = []
    for event in tuple(events):
        if type(event) is not RegistrySourceEvent:
            raise RegistrySourceEventError()
        event = replace(event)
        key = (*_scope(event), event.event_sequence)
        previous = events_by_sequence.get(key)
        state = states_by_scope.get(_scope(event))
        if (previous is not None and previous != event) or (
            state is not None
            and state.last_event is not None
            and state.last_event.event_sequence == event.event_sequence
            and state.last_event != event
        ):
            raise RegistrySourceEventError()
        events_by_sequence[key] = event
        validated_events.append(event)
    if len(set(states_by_scope) | {_scope(event) for event in validated_events}) > MAX_SOURCE_SCOPES:
        raise RegistrySourceEventError()
    return states_by_scope, validated_events


def _has_drift(previous, edition):
    if previous is None:
        return False
    return previous.header_sha256 != edition.header_sha256 or (
        previous.edition_id == edition.edition_id and previous != edition
    )


def _candidate_decision(state, event):
    candidate = state.candidate
    has_matching_candidate = candidate is not None and candidate.edition == event.edition
    if event.event_kind == "discovered":
        if has_matching_candidate:
            return "noop", candidate, state.review_required
        needs_review = _has_drift(state.accepted, event.edition) or (
            candidate is not None and _has_drift(candidate.edition, event.edition)
        )
        return "review" if needs_review else "prepare", event, needs_review
    if not has_matching_candidate:
        return "review", candidate, state.review_required
    if event.event_kind == "prepared":
        if candidate.event_kind in {"prepared", "validated"}:
            return "noop", candidate, state.review_required
        if state.review_required and not event.reviewed:
            return "review", candidate, True
        return "validate", event, False
    if candidate.event_kind == "validated":
        return "noop", candidate, state.review_required
    if candidate.event_kind != "prepared" or state.review_required:
        return "review", candidate, state.review_required
    return "ready_for_admission", event, False


def _reduce_event(state, event):
    if state.last_event is not None:
        if event.event_sequence < state.last_event.event_sequence:
            return state, "stale"
        if event.event_sequence == state.last_event.event_sequence:
            return state, "noop"
    if event.event_kind in {"unavailable", "failed"}:
        return replace(state, last_event=event, decision=event.event_kind), event.event_kind
    if event.edition == state.accepted:
        return replace(state, last_event=event, decision="noop"), "noop"
    decision, candidate, review_required = _candidate_decision(state, event)
    return replace(
        state, candidate=candidate, last_event=event, decision=decision, review_required=review_required
    ), decision


def reduce_registry_source_events(
    states: list[RegistrySourceEventState] | tuple[RegistrySourceEventState, ...],
    events: list[RegistrySourceEvent] | tuple[RegistrySourceEvent, ...],
) -> RegistrySourceEventBatch:
    """Validate the whole batch, then reduce events in their supplied order.

    Each sequence is unique within its source. Exact last-event or batch replay
    is idempotent; older historical sequences are stale without a replay journal.
    Drift needs reviewed preparation bound to all candidate evidence, including
    both artifact and input hashes. Validation produces an admission intention;
    it never replaces accepted evidence. Callers persist acceptance separately.
    """
    states_by_scope, validated_events = _validated_batch(states, events)
    decisions = []
    for event in validated_events:
        scope = _scope(event)
        state = states_by_scope.get(scope) or RegistrySourceEventState(*scope)
        state, decision = _reduce_event(state, event)
        states_by_scope[scope] = state
        decisions.append(RegistrySourceEventDecision(*scope, event.event_sequence, decision))
    return RegistrySourceEventBatch(
        tuple(states_by_scope[scope] for scope in sorted(states_by_scope)), tuple(decisions)
    )
