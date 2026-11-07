# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed winner COPY cells retain the complete existing reducer evidence."""

import hashlib
import os
import pickle
import random
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from process.custom_import import family_shard_winners as output
from process.custom_import import materialization as selection
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.materialization import WinnerCandidate, WinnerMaterializationError
from tests.test_custom_import_family_shard_selection import _GENERATION, _SLOTS, _candidate, _definition, _sort, _stream


class _Factory:
    def __init__(self, definition, shards):
        self.definition, self.shards = definition, shards
        self.opened, self.closed, self.exhausted = [], [], []
        self.inputs_by_profile = {}
        self.is_active = False

    @contextmanager
    def __call__(self, slot):
        assert not self.is_active
        self.is_active = True
        self.opened.append(slot)
        self.inputs_by_profile[slot] = tuple(_sort(self.definition, shard, slot) for shard in self.shards)
        iterators = []

        def candidates(shard, rows):
            try:
                yield from rows
                self.exhausted.append((slot, shard))
            finally:
                self.closed.append((slot, shard))

        try:
            for shard, rows in enumerate(self.inputs_by_profile[slot]):
                iterators.append(candidates(shard, rows))
            yield tuple(_stream(iterator) for iterator in iterators)
        finally:
            for iterator in iterators:
                iterator.close()
            self.is_active = False


def _build(tmp_path, definition, shards, *, slots=None, check=None, limits=None):
    layout = FamilyBlockLayout(definition, _SLOTS)
    factory = _Factory(definition, shards)
    registered_roots = []
    work = output.WinnerWork(
        tmp_path / "winners", limits or SourceFactLimits(16 * 1024**2, 65536, 256), registered_roots.append, check
    )
    descriptor = output.build_winner_output(layout, _GENERATION, factory, work=work, profile_slots=slots)
    assert registered_roots == [Path(descriptor.root)]
    return descriptor, factory


def _expected_cells(definition, candidates, slots=None):
    materialized = selection.materialize_winners(
        definition, generation=_GENERATION, candidates=_stream(candidates), child_collection_slots=_SLOTS
    )
    return tuple(
        tuple(getattr(model, name) for name in output._COLUMNS)
        for model in selection.winner_materialization_models(materialized)
        if slots is None or model.profile_slot in slots
    )


@pytest.mark.parametrize("seed", (7, 31, 149))
@pytest.mark.parametrize("slots", (None, (1,), (2,), (3,)))
def test_profiles_preserve_original_winners_and_complete_candidate_audit(tmp_path, seed, slots):
    definition = _definition(multiple=True)
    candidates = [
        _candidate(
            index, entity=41 + index % 3, context=str(index % 5), amount=None if index % 7 == 0 else Decimal(index % 11)
        )
        for index in range(90)
    ]
    candidates.extend(
        WinnerCandidate(
            41 + index % 3,
            3000 + index,
            bytes((index,)) * 32,
            0,
            None,
            None,
            {"npi": "1234567893", "display_name": f"Clinic {index}"},
        )
        for index in range(3)
    )
    rng = random.Random(seed)
    rng.shuffle(candidates)
    shards = [[], [], [], []]
    for candidate in candidates:
        shards[rng.randrange(len(shards))].append(candidate)
    rng.shuffle(shards)
    descriptor, factory = _build(tmp_path, definition, shards, slots=slots)
    try:
        assert tuple(output.read_winner_rows(descriptor)) == _expected_cells(definition, candidates, slots)
        assert descriptor == pickle.loads(pickle.dumps(descriptor))
        assert descriptor.role_counts == {"winner": len(_expected_cells(definition, candidates, slots))}
        audit_by_coordinate = {
            (slot, shard, ordinal): candidate
            for slot, shard, ordinal, candidate in output.read_winner_candidates(descriptor)
        }
        candidate_by_coordinate = {
            (slot, shard, ordinal): candidate
            for slot, shards in factory.inputs_by_profile.items()
            for shard, candidates in enumerate(shards)
            for ordinal, candidate in enumerate(candidates)
        }
        assert audit_by_coordinate == candidate_by_coordinate
        assert (
            set(factory.closed)
            == set(factory.exhausted)
            == {(slot, shard) for slot in descriptor.profile_slots for shard in range(4)}
        )
        assert Path(descriptor.root, output._FILE).stat().st_size > 256
    finally:
        output.cleanup_winner_output(descriptor)
    assert not Path(descriptor.root).exists()


@pytest.mark.parametrize("is_better", (False, True))
@pytest.mark.parametrize("is_physical", (False, True))
def test_selected_tie_conflicts_clear_only_for_later_better_candidate(tmp_path, is_better, is_physical):
    definition = _definition()
    first = _candidate(1)
    conflicting = (
        replace(first, family_revision_id=1003, context_child_revision_id=2003)
        if is_physical
        else replace(first, values_by_field={**first.values_by_field, "display_name": "different"})
    )
    third = replace(
        first,
        family_revision_id=1002,
        context_child_revision_id=2002,
        family_sha256=(b"\x00" if is_better else b"\xff") * 32,
    )
    candidates = (first, conflicting, third)
    if is_better:
        descriptor, _factory = _build(tmp_path, definition, ((first, conflicting), (third,)))
        try:
            assert tuple(output.read_winner_rows(descriptor)) == _expected_cells(definition, candidates)
            assert len(tuple(output.read_winner_candidates(descriptor))) == 3
        finally:
            output.cleanup_winner_output(descriptor)
    else:
        with pytest.raises(WinnerMaterializationError):
            _expected_cells(definition, candidates)
        with pytest.raises(WinnerMaterializationError):
            _build(tmp_path, definition, ((first, conflicting), (third,)))
        assert not (tmp_path / "winners").exists()


def test_eliminated_context_collision_still_refuses_and_cleans_partial_winner_spool(tmp_path, monkeypatch):
    definition = _definition()
    monkeypatch.setattr(selection, "_context_digest", lambda _context: b"c" * 32)
    first, losing, collision = (
        _candidate(1),
        _candidate(2, amount=Decimal("9")),
        _candidate(3, context="B", amount=Decimal("20")),
    )
    with pytest.raises(WinnerMaterializationError, match="context digest collision"):
        _expected_cells(definition, (first, losing, collision))
    with pytest.raises(WinnerMaterializationError, match="context digest collision"):
        _build(tmp_path, definition, ((losing, collision), (first,)))
    assert not (tmp_path / "winners").exists()


def test_missing_explicit_null_and_decimal_lexical_values_are_retained_independently(tmp_path):
    import json

    document = json.loads(_definition().canonical)
    document["selection_profiles"][0]["context_dimensions"] = ["amount"]
    definition = CustomImportDefinition.from_mapping(document)
    candidates = (
        replace(_candidate(1), values_by_field={"service_code": "A"}),
        _candidate(2, amount=None),
        _candidate(3, amount=Decimal("-0.000")),
        _candidate(4, amount=Decimal("1.2000")),
    )
    descriptor, _factory = _build(
        tmp_path, definition, ((candidates[0], candidates[2]), (candidates[1], candidates[3]))
    )
    try:
        assert tuple(output.read_winner_rows(descriptor)) == _expected_cells(definition, candidates)
        by_id = {
            candidate.family_revision_id: candidate
            for *_provenance, candidate in output.read_winner_candidates(descriptor)
        }
        assert by_id[1001].values_by_field == {"service_code": "A"}
        assert by_id[1002].values_by_field["amount"] is None
        assert by_id[1003].values_by_field["amount"].as_tuple() == Decimal("-0.000").as_tuple()
        assert by_id[1004].values_by_field["amount"].as_tuple() == Decimal("1.2000").as_tuple()
    finally:
        output.cleanup_winner_output(descriptor)


def test_raw_timestamp_keeps_original_offset_while_original_reducer_orders_utc(tmp_path):
    import json

    document = json.loads(_definition(multiple=True).canonical)
    document["schema"]["root"]["fields"][1]["type"] = "timestamp"
    definition = CustomImportDefinition.from_mapping(document)
    timestamp = datetime(2026, 1, 1, 13, 2, 3, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30), "Synthetic"))
    candidate = WinnerCandidate(41, 3001, b"f" * 32, 0, None, None, {"npi": "1234567893", "display_name": timestamp})
    descriptor, _factory = _build(tmp_path, definition, ((candidate,),), slots=(3,))
    try:
        assert tuple(output.read_winner_rows(descriptor)) == _expected_cells(definition, (candidate,), (3,))
        ((_slot, _shard, _ordinal, actual),) = output.read_winner_candidates(descriptor)
        assert actual.values_by_field["display_name"].isoformat() == timestamp.isoformat()
        assert actual.values_by_field["display_name"].tzname() == timestamp.tzname()
    finally:
        output.cleanup_winner_output(descriptor)


@pytest.mark.parametrize("failure", ("cancel", "budget", "late_stream", "reuse"))
def test_late_failure_never_publishes_output_and_closes_borrowed_contexts(tmp_path, failure):
    definition = _definition(multiple=True)
    candidates = tuple(_candidate(index, entity=41 + index) for index in range(40))
    factory = _Factory(definition, (candidates,))
    count_by_phase = {"checks": 0}

    def check():
        count_by_phase["checks"] += 1
        if failure == "cancel" and count_by_phase["checks"] == 30:
            raise RuntimeError("owner cancelled")

    @contextmanager
    def contexts(slot):
        with factory(slot) as streams:
            if failure == "late_stream" and slot == 2:
                raise RuntimeError("candidate source failed")
            yield previous if failure == "reuse" and slot > 1 else streams
            previous[:] = streams

    previous = []
    unit = max(512, os.statvfs(tmp_path).f_frsize)
    limits = (
        SourceFactLimits(6 * unit, 1024, 256) if failure == "budget" else SourceFactLimits(16 * 1024**2, 65536, 256)
    )
    registered_roots = []
    work = output.WinnerWork(tmp_path / "winners", limits, registered_roots.append, check)
    with pytest.raises((RuntimeError, SourceFactBudgetExceeded, WinnerMaterializationError)):
        output.build_winner_output(FamilyBlockLayout(definition, _SLOTS), _GENERATION, contexts, work=work)
    assert registered_roots == [tmp_path / "winners"] and not (tmp_path / "winners").exists()
    assert not factory.is_active


@pytest.mark.parametrize("damage", ("bytes", "missing_complete", "trailing", "inode", "declaration"))
def test_replay_checks_same_file_bytes_full_eof_and_declared_scope(tmp_path, damage):
    descriptor, _factory = _build(tmp_path, _definition(), ((_candidate(1), _candidate(2)),))
    original = descriptor
    path = Path(descriptor.root, output._FILE)
    saved = path.read_bytes()
    path.chmod(0o600)
    if damage == "inode":
        backup = tmp_path / "original"
        path.rename(backup)
        path.write_bytes(saved)
    elif damage == "declaration":
        descriptor = replace(descriptor, generation=(102, *descriptor.generation[1:]))
    else:
        if damage == "bytes":
            changed = saved[:-1] + bytes((saved[-1] ^ 1,))
        elif damage == "trailing":
            changed = saved + output._LENGTH.pack(1) + b"\xc0"
        else:
            prefix = saved.rfind(
                output._pack_row(
                    ("complete", descriptor.profiles, descriptor.winner_count, descriptor.winner_rows_sha256)
                )
            )
            changed = saved[: prefix - 8]
        path.write_bytes(changed)
        if damage != "bytes":
            descriptor = replace(descriptor, file_bytes=len(changed), sha256=hashlib.sha256(changed).digest())
    try:
        with pytest.raises((ValueError, TypeError, IndexError)):
            tuple(output.read_winner_rows(descriptor))
    finally:
        if damage == "inode":
            path.unlink()
            backup.rename(path)
        elif damage != "declaration":
            path.write_bytes(saved)
        output.cleanup_winner_output(original)
    assert not Path(original.root).exists()


def test_wrong_generation_or_invalid_profile_is_refused_without_false_consumption(tmp_path):
    definition = _definition()
    stream = _stream((_candidate(1),), replace(_GENERATION, generation_id=102))

    @contextmanager
    def contexts(_slot):
        yield (stream,)

    registered_roots = []
    work = output.WinnerWork(tmp_path / "winners", SourceFactLimits(1024**2, 65536, 256), registered_roots.append)
    with pytest.raises(WinnerMaterializationError, match="generation does not match"):
        output.build_winner_output(FamilyBlockLayout(definition, _SLOTS), _GENERATION, contexts, work=work)
    assert not stream._is_consumed and not (tmp_path / "winners").exists()
    with pytest.raises(ValueError, match="profile declaration"):
        output.build_winner_output(
            FamilyBlockLayout(definition, _SLOTS), _GENERATION, contexts, work=work, profile_slots=(True,)
        )
    assert len(registered_roots) == 1


def test_late_global_order_error_cannot_publish_a_valid_winner_prefix(tmp_path):
    close_events = []

    def candidates():
        try:
            yield _candidate(1, entity=41)
            yield _candidate(2, entity=43)
            yield _candidate(3, entity=42)
        finally:
            close_events.append(True)

    @contextmanager
    def contexts(_slot):
        iterator = candidates()
        try:
            yield (_stream(iterator),)
        finally:
            iterator.close()

    work = output.WinnerWork(tmp_path / "winners", SourceFactLimits(1024**2, 65536, 256), lambda _path: None)
    with pytest.raises(WinnerMaterializationError, match="shard candidates.*order"):
        output.build_winner_output(FamilyBlockLayout(_definition(), _SLOTS), _GENERATION, contexts, work=work)
    assert close_events == [True] and not (tmp_path / "winners").exists()


@pytest.mark.parametrize("raw_amount", ("1.2000", Decimal("1.2000"), "invalid", True))
def test_raw_scalar_evidence_matches_existing_acceptance_without_canonical_fallback(tmp_path, raw_amount):
    definition = _definition()
    candidate = _candidate(1, amount=raw_amount)
    if raw_amount == "invalid" or raw_amount is True:
        with pytest.raises(WinnerMaterializationError):
            _expected_cells(definition, (candidate,))
        with pytest.raises(WinnerMaterializationError):
            _build(tmp_path, definition, ((candidate,),))
        assert not (tmp_path / "winners").exists()
        return
    descriptor, _factory = _build(tmp_path, definition, ((candidate,),))
    try:
        assert tuple(output.read_winner_rows(descriptor)) == _expected_cells(definition, (candidate,))
        ((_slot, _shard, _ordinal, replay),) = output.read_winner_candidates(descriptor)
        assert type(replay.values_by_field["amount"]) is type(raw_amount)
        assert str(replay.values_by_field["amount"]) == str(raw_amount)
    finally:
        output.cleanup_winner_output(descriptor)


def test_replay_cancellation_interrupts_file_digest_before_exposing_copy_cells(tmp_path):
    descriptor, _factory = _build(tmp_path, _definition(), ((_candidate(1),),))
    count_by_phase = {"check": 0}

    def check():
        count_by_phase["check"] += 1
        if count_by_phase["check"] == 2:
            raise RuntimeError("owner cancelled during digest")

    try:
        with pytest.raises(RuntimeError, match="during digest"):
            next(output.read_winner_rows(descriptor, check=check))
        assert count_by_phase["check"] == 2
    finally:
        output.cleanup_winner_output(descriptor)


def test_empty_shards_still_complete_every_declared_profile_without_invented_contexts(tmp_path):
    descriptor, factory = _build(tmp_path, _definition(multiple=True), ())
    try:
        assert descriptor.profile_slots == (1, 2, 3)
        assert descriptor.profiles == ((1, (), 0), (2, (), 0), (3, (), 0))
        assert tuple(output.read_winner_rows(descriptor)) == ()
        assert tuple(output.read_winner_candidates(descriptor)) == ()
        assert descriptor.role_counts == {"winner": 0}
        assert factory.opened == [1, 2, 3] and not factory.is_active
    finally:
        output.cleanup_winner_output(descriptor)


@pytest.mark.parametrize("failure", ("late_sha", "late_eof"))
def test_provisional_copy_prefix_never_produces_an_accepted_receipt_on_late_failure(tmp_path, failure):
    descriptor, _factory = _build(tmp_path, _definition(), ((_candidate(1), _candidate(2, context="B")),))
    original = descriptor
    path = Path(descriptor.root, output._FILE)
    saved = path.read_bytes()
    if failure == "late_sha":
        descriptor = replace(descriptor, sha256=b"x" * 32)
    else:
        footer = output._pack_row(
            ("complete", descriptor.profiles, descriptor.winner_count, descriptor.winner_rows_sha256)
        )
        truncated = saved[: -(8 + len(footer))]
        path.chmod(0o600)
        path.write_bytes(truncated)
        descriptor = replace(descriptor, file_bytes=len(truncated), sha256=hashlib.sha256(truncated).digest())
    count_by_status = {"copied": 0}
    accepted_receipts = []
    try:
        with pytest.raises(ValueError, match="EOF|incomplete"):
            for _copy_cells in output.read_winner_rows(descriptor):
                count_by_status["copied"] += 1
            accepted_receipts.append(descriptor.role_counts)
        assert count_by_status["copied"] == descriptor.winner_count > 0
        assert accepted_receipts == []
    finally:
        if failure == "late_eof":
            path.write_bytes(saved)
        output.cleanup_winner_output(original)
