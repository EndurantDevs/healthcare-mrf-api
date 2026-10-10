# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-local lifecycle metadata cannot replace accepted data or bypass review."""

from dataclasses import FrozenInstanceError, replace

import pytest

from process.registry_source_events import (
    MAX_SOURCE_EVENTS,
    MAX_SOURCE_SCOPES,
    RegistrySourceAcceptedEdition,
    RegistrySourceEvent,
    RegistrySourceEventError,
    RegistrySourceEventState,
    reduce_registry_source_events,
)


def _edition(**changes):
    edition = RegistrySourceAcceptedEdition(
        "cms", "commercial-mlr", "edition-a", "a" * 64, "b" * 64, "parser-v1", "c" * 64
    )
    return replace(edition, **changes)


def _event(kind="discovered", sequence=1, edition=None, **changes):
    edition = _edition() if edition is None and kind not in {"unavailable", "failed"} else edition
    event = RegistrySourceEvent("cms", "commercial-mlr", kind, sequence, edition)
    return replace(event, **changes)


def _state(accepted=None, **changes):
    return replace(RegistrySourceEventState("cms", "commercial-mlr", accepted), **changes)


def _decisions(batch):
    return [outcome.decision for outcome in batch.decisions]


def test_complete_candidate_retains_accepted():
    """Only matching preparation followed by validation produces an intention."""
    accepted = _edition()
    candidate = _edition(edition_id="edition-b", artifact_sha256="d" * 64, input_sha256="e" * 64)
    events = [_event(kind, index, candidate) for index, kind in enumerate(("discovered", "prepared", "validated"), 1)]
    initial = _state(accepted)
    batch = reduce_registry_source_events([initial], events)
    assert _decisions(batch) == ["prepare", "validate", "ready_for_admission"]
    assert batch.states[0].accepted == accepted and batch.states[0].candidate == events[-1]
    assert batch.states[0].last_event == events[-1] and not batch.states[0].review_required
    assert initial == _state(accepted) and initial.candidate is None
    assert not hasattr(batch, "published")


def test_initial_edition_requires_preparation():
    """A first edition has no inferred accepted header or source version."""
    events = [_event(kind, index) for index, kind in enumerate(("discovered", "prepared", "validated"), 1)]
    batch = reduce_registry_source_events([], events)
    assert _decisions(batch) == ["prepare", "validate", "ready_for_admission"]
    assert batch.states[0].accepted is None


@pytest.mark.parametrize("kind", ["discovered", "prepared", "validated"])
def test_exact_accepted_evidence_is_noop(kind):
    """Full edition, parser, header, artifact and input identity is replayed."""
    accepted = _edition()
    batch = reduce_registry_source_events([_state(accepted)], [_event(kind, edition=accepted)])
    assert _decisions(batch) == ["noop"] and batch.states[0].accepted == accepted
    assert batch.states[0].candidate is None


@pytest.mark.parametrize(
    "change",
    [
        {"header_sha256": "d" * 64},
        {"artifact_sha256": "d" * 64},
        {"input_sha256": "d" * 64},
        {"parser_version": "parser-v2"},
    ],
)
def test_drift_requires_bound_review(change):
    """Drift cannot validate until its exact candidate has reviewed preparation."""
    accepted = _edition()
    candidate = _edition(**change)
    events = [
        _event("discovered", 1, candidate),
        _event("prepared", 2, candidate),
        _event("validated", 3, candidate),
        _event("prepared", 4, candidate, reviewed=True),
        _event("validated", 5, candidate),
    ]
    batch = reduce_registry_source_events([_state(accepted)], events)
    assert _decisions(batch) == ["review", "review", "review", "validate", "ready_for_admission"]
    assert batch.states[0].accepted == accepted and not batch.states[0].review_required


def test_new_edition_header_drift_review():
    """A new edition label cannot conceal an unexpected layout fingerprint."""
    changed = _edition(edition_id="edition-b", header_sha256="d" * 64)
    batch = reduce_registry_source_events([_state(_edition())], [_event(edition=changed)])
    assert _decisions(batch) == ["review"] and batch.states[0].review_required


def test_pending_candidate_drift_survives_failure():
    """Review obligations survive source-local failure even before first acceptance."""
    changed = _edition(input_sha256="d" * 64)
    events = [
        _event(),
        _event(sequence=2, edition=changed),
        _event("unavailable", 3),
        _event("prepared", 4, changed),
        _event("prepared", 5, changed, reviewed=True),
    ]
    batch = reduce_registry_source_events([], events)
    assert _decisions(batch) == ["prepare", "review", "unavailable", "review", "validate"]
    assert batch.states[0].accepted is None and batch.states[0].candidate == events[-1]


@pytest.mark.parametrize("kind", ["prepared", "validated"])
def test_unseen_candidates_cannot_advance(kind):
    """A reviewed flag is bound to an already discovered candidate identity."""
    event = _event(kind, reviewed=kind == "prepared")
    batch = reduce_registry_source_events([], [event])
    assert _decisions(batch) == ["review"] and batch.states[0].candidate is None


def test_preparation_cannot_switch_candidate_input():
    """Even reviewed preparation cannot substitute the candidate input hash."""
    discovered = _event()
    different = _edition(input_sha256="d" * 64)
    events = [discovered, _event("prepared", 2, different, reviewed=True), _event("validated", 3, different)]
    batch = reduce_registry_source_events([], events)
    assert _decisions(batch) == ["prepare", "review", "review"]
    assert batch.states[0].candidate == discovered


def test_validation_needs_matching_preparation():
    """Discovery alone cannot mark its candidate ready for admission."""
    events = [_event(), _event("validated", 2), _event("prepared", 3), _event("validated", 4)]
    batch = reduce_registry_source_events([], events)
    assert _decisions(batch) == ["prepare", "review", "validate", "ready_for_admission"]


def test_batch_order_is_explicit():
    """A later high-water mark makes an earlier input stale rather than reordering it."""
    batch = reduce_registry_source_events([], [_event(sequence=3), _event("prepared", 2)])
    assert _decisions(batch) == ["prepare", "stale"] and batch.states[0].last_event.event_sequence == 3


@pytest.mark.parametrize("kind", ["unavailable", "failed"])
def test_source_failure_retains_candidate(kind):
    """Failures change only source-local status and retain accepted and prepared evidence."""
    accepted = _edition(edition_id="accepted")
    prepared = _event("prepared", 2)
    initial = reduce_registry_source_events([_state(accepted)], [_event(), prepared]).states[0]
    batch = reduce_registry_source_events([initial], [_event(kind, 3)])
    assert _decisions(batch) == [kind]
    assert batch.states[0].accepted == accepted and batch.states[0].candidate == prepared
    assert batch.states[0].decision == kind
    resumed = reduce_registry_source_events(batch.states, [_event("validated", 4)])
    assert _decisions(resumed) == ["ready_for_admission"] and resumed.states[0].accepted == accepted


def test_replay_and_stale_do_not_regress():
    """Repeated events are harmless; historical order never rewinds candidate progress."""
    events = [_event(), _event("prepared", 2), _event("validated", 3)]
    batch = reduce_registry_source_events([], [*events, events[-1], events[0], events[1]])
    assert _decisions(batch) == ["prepare", "validate", "ready_for_admission", "noop", "stale", "stale"]
    replayed = reduce_registry_source_events(batch.states, [events[-1]])
    assert replayed.states == batch.states and _decisions(replayed) == ["noop"]
    stale = reduce_registry_source_events(batch.states, [events[0]])
    assert stale.states == batch.states and _decisions(stale) == ["stale"]


def test_fresh_metadata_replay_preserves_progress():
    """Later discovery/preparation of identical evidence does not rewind validated progress."""
    events = [
        _event(),
        _event("prepared", 2),
        _event("validated", 3),
        _event(sequence=4),
        _event("prepared", 5),
        _event("validated", 6),
    ]
    batch = reduce_registry_source_events([], events)
    assert _decisions(batch) == ["prepare", "validate", "ready_for_admission", "noop", "noop", "noop"]
    assert batch.states[0].candidate == events[2] and batch.states[0].last_event == events[-1]


def test_conflict_rejects_entire_batch():
    """No input state changes when a later event conflicts with the same source sequence."""
    initial = _state(_edition(edition_id="accepted"))
    event = _event()
    conflict = _event("prepared", 1)
    with pytest.raises(RegistrySourceEventError):
        reduce_registry_source_events([initial], [event, conflict])
    assert initial.candidate is None and initial.last_event is None
    current = reduce_registry_source_events([initial], [event]).states
    with pytest.raises(RegistrySourceEventError):
        reduce_registry_source_events(current, [_event("failed", 2), conflict])
    assert current[0].last_event == event


def test_duplicate_events_accounted_once_per_input():
    """Duplicates produce no repeated intention while preserving bounded input accounting."""
    event = _event()
    batch = reduce_registry_source_events([], [event, event])
    assert _decisions(batch) == ["prepare", "noop"] and len(batch.decisions) == 2
    assert len(batch.states) == 1


def test_scope_isolation_and_stable_output():
    """Unavailable input does not stop an independent candidate and no names are merged."""
    naic = RegistrySourceEventState(
        "naic", "company-directory", _edition(source_system="naic", source_id="company-directory")
    )
    missing = RegistrySourceEvent("naic", "company-directory", "unavailable", 1)
    batch = reduce_registry_source_events([naic], [missing, _event()])
    assert _decisions(batch) == ["unavailable", "prepare"]
    assert [(state.source_system, state.source_id) for state in batch.states] == [
        ("cms", "commercial-mlr"),
        ("naic", "company-directory"),
    ]
    assert batch.states[1].accepted == naic.accepted


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_system", " cms"),
        ("source_id", ""),
        ("edition_id", "edition\n"),
        ("parser_version", ""),
        ("artifact_sha256", "A" * 64),
        ("input_sha256", "short"),
        ("header_sha256", 7),
    ],
)
def test_edition_metadata_invalid(field, value):
    """Malformed evidence cannot become candidate or accepted state."""
    with pytest.raises(RegistrySourceEventError):
        _edition(**{field: value})


@pytest.mark.parametrize(
    "changes",
    [
        {"event_sequence": True},
        {"event_sequence": 0},
        {"event_sequence": 2**63},
        {"event_kind": "accepted"},
        {"reviewed": 1},
        {"reviewed": True},
        {"edition": None},
        {"source_id": "other"},
    ],
)
def test_event_metadata_invalid(changes):
    """Only bounded typed sequences, known kinds and matching source evidence are accepted."""
    with pytest.raises(RegistrySourceEventError):
        replace(_event(), **changes)


def test_frozen_and_revalidated_control():
    """Frozen inputs are revalidated rather than trusted after unsafe construction."""
    event = _event()
    with pytest.raises(FrozenInstanceError):
        event.event_sequence = 2
    object.__setattr__(event.edition, "input_sha256", "malformed")
    with pytest.raises(RegistrySourceEventError):
        reduce_registry_source_events([], [event])


@pytest.mark.parametrize(
    "changes",
    [
        {"accepted": _edition(source_id="other")},
        {"accepted": object()},
        {"candidate": _event()},
        {"last_event": object()},
        {"decision": "stale"},
        {"review_required": True},
        {"review_required": 1},
        {"candidate": _event("failed"), "last_event": _event("failed")},
        {"candidate": _event(sequence=2), "last_event": _event(sequence=1)},
        {"candidate": _event("prepared"), "last_event": _event("prepared"), "review_required": True},
    ],
)
def test_state_invariants_are_checked(changes):
    """Source, progress, sequence and review state must be internally consistent."""
    with pytest.raises(RegistrySourceEventError):
        _state(**changes)


def test_maximum_sequence_and_text_bounds():
    """The explicit identifier and sequence boundaries accept only canonical values."""
    edition = _edition(source_system="s" * 64, source_id="i" * 128, edition_id="e" * 128, parser_version="p" * 128)
    event = RegistrySourceEvent(edition.source_system, edition.source_id, "discovered", 2**63 - 1, edition)
    assert reduce_registry_source_events([], [event]).states[0].last_event == event
    for field, value in (
        ("source_system", "s" * 65),
        ("source_id", "i" * 129),
        ("edition_id", "e" * 129),
        ("parser_version", "p" * 129),
    ):
        with pytest.raises(RegistrySourceEventError):
            replace(edition, **{field: value})


@pytest.mark.parametrize(
    "states,events", [(None, []), ([], None), ({}, []), ([], iter(())), ([object()], []), ([], [object()])]
)
def test_batch_types_invalid(states, events):
    """The bounded trust boundary rejects implicit iterators and foreign DTOs."""
    with pytest.raises(RegistrySourceEventError):
        reduce_registry_source_events(states, events)


def test_duplicate_scopes_and_union_bounds():
    """Duplicate initial state and scope unions beyond the exact bound reject the whole batch."""
    state = _state()
    with pytest.raises(RegistrySourceEventError):
        reduce_registry_source_events([state, state], [])
    states = [RegistrySourceEventState("cms", "synthetic-" + str(index)) for index in range(MAX_SOURCE_SCOPES)]
    assert len(reduce_registry_source_events(states, []).states) == MAX_SOURCE_SCOPES
    with pytest.raises(RegistrySourceEventError):
        reduce_registry_source_events(states, [_event()])
    event = _event()
    assert len(reduce_registry_source_events([], [event] * MAX_SOURCE_EVENTS).decisions) == MAX_SOURCE_EVENTS
    with pytest.raises(RegistrySourceEventError):
        reduce_registry_source_events([], [event] * (MAX_SOURCE_EVENTS + 1))


def test_empty_batch_is_pure():
    """Empty input returns empty immutable output without inferred source state."""
    batch = reduce_registry_source_events([], [])
    assert batch.states == () and batch.decisions == ()
