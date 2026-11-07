# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real original-reader composition; native IDs/transport remain pure fixtures."""

import json
import os
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_member_winners as producer
from process.custom_import import family_shard_winner_validation as proof
from process.custom_import import family_shard_winners as output
from process.custom_import import materialization as selection
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_output import generation_member_batches
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_family_shard_original_validation import _case as original_case
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_winner_validation import (
    _case,
    _definition,
    _entries,
    _retained_case,
    _retained_entries,
)

_LIMITS = SourceFactLimits(64 * 1024**2, 4 * 1024**2, 2 * 1024**2)


def _build(tmp_path, case, *, entries=None, cohort=2, check=None, limits=_LIMITS, generation=None, member_shard=99):
    native, _context, _source, identity, _winner, _candidates = case
    return producer.build_original_member_winners(
        native.context,
        identity if generation is None else generation,
        native.plan,
        _entries(tmp_path, native) if entries is None else entries,
        cohort,
        work=PlanWork(tmp_path / "produced", limits, lambda _path: None, check),
        member_shard_id=member_shard,
    )


def _cleanup(artifacts):
    members, descriptor = artifacts
    members.close()
    output.cleanup_winner_output(descriptor)


def _member_rows(members):
    return tuple(
        row for batch in members.batches(maximum_rows=2, maximum_bytes=4096) for row in batch["generation_family"]
    )


def _compare(tmp_path, case, artifacts, *, entries=None, retained=None, cohort=2):
    native, context, source, generation, _winner, _candidates = case
    return proof.prove_original_winners(
        context,
        source,
        generation,
        artifacts[1],
        _entries(tmp_path, native) if entries is None else entries,
        cohort,
        retained_originals=retained,
        work=PlanWork(tmp_path / "independent-proof", _LIMITS, lambda _path: None),
    )


async def test_real_members_all_contexts_and_global_winners_pass_independent_proof(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        artifacts = _build(tmp_path, case)
        try:
            members, descriptor = artifacts
            expected_rows = tuple(
                member_cells
                for batch in generation_member_batches(
                    case[0].context,
                    case[3],
                    case[0].plan,
                    ((plan, family) for _shard, plan, family in _entries(tmp_path, case[0])),
                    maximum_rows=2,
                    maximum_bytes=4096,
                )
                for member_cells in batch["generation_family"]
            )
            assert _member_rows(members) == expected_rows and len(expected_rows) == 1
            assert members.receipt["partition_sha256"].hex() == case[0].context.source.source.partition_sha256
            assert members.receipt["source_record_count"] == 0 and members.receipt["shard_id"] == 99
            assert {role: count for role, count in members.receipt["role_counts"].items() if count} == {
                "generation_family": 1
            }
            assert tuple(output.read_winner_rows(descriptor)) == tuple(output.read_winner_rows(case[4]))
            comparison = _compare(tmp_path, case, artifacts)
            assert comparison.profiles == ((1, 3), (2, 3), (3, 3)) and comparison.winner_count == 4
            assert set(path.name for path in Path(members._work.root).iterdir()) == {"output.mpk"}
            assert not hasattr(comparison, "verified")
        finally:
            _cleanup(artifacts)
        assert not (tmp_path / "produced").exists() and not (tmp_path / "produced-winners").exists()


async def test_each_selected_family_and_query_child_factory_are_traversed_once(tmp_path, monkeypatch):
    calls = []

    def supplied(native):
        with closing(_entries(tmp_path, native)) as families:
            for shard, plan, family in families:
                original = family.children_by_collection["details"]

                def children():
                    calls.append("children")
                    with closing(original()) as rows:
                        yield from rows

                calls.append("family")
                yield shard, plan, replace(family, children_by_collection={"details": children})

    async with _case(tmp_path, monkeypatch) as case:
        artifacts = _build(tmp_path, case, entries=supplied(case[0]))
        try:
            assert calls == ["family", "children"]
            assert len(tuple(output.read_winner_candidates(artifacts[1]))) == 9
        finally:
            _cleanup(artifacts)


async def test_explicit_factory_assignment_and_empty_streams_survive(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        artifacts = _build(tmp_path, case, entries=_entries(tmp_path, case[0], shard=2), cohort=3)
        try:
            assert {shard for _slot, shard, _ordinal, _candidate in output.read_winner_candidates(artifacts[1])} == {2}
            result = _compare(tmp_path, case, artifacts, entries=_entries(tmp_path, case[0], shard=2), cohort=3)
            assert result.profiles == ((1, 3), (2, 3), (3, 3))
        finally:
            _cleanup(artifacts)


@pytest.mark.parametrize("cohort", [0, 2])
async def test_empty_plan_still_produces_closed_zero_roles_and_every_profile(tmp_path, monkeypatch, cohort):
    async with _case(tmp_path, monkeypatch, empty=True) as case:
        artifacts = _build(tmp_path, case, cohort=cohort)
        try:
            assert _member_rows(artifacts[0]) == () and all(
                count == 0 for count in artifacts[0].receipt["role_counts"].values()
            )
            assert len(artifacts[1].profiles) == 3 and artifacts[1].winner_count == 0
            assert _compare(tmp_path, case, artifacts, cohort=cohort).profiles == ((1, 0), (2, 0), (3, 0))
        finally:
            _cleanup(artifacts)


async def test_actual_retained_destination_composes_with_source_members_and_winners(tmp_path, monkeypatch):
    async with _retained_case(tmp_path, monkeypatch) as (case, retained):
        artifacts = _build(tmp_path, case, entries=_retained_entries(tmp_path, case[0], retained.destination))
        try:
            assert len(_member_rows(artifacts[0])) == 2
            result = _compare(
                tmp_path,
                case,
                artifacts,
                entries=_retained_entries(tmp_path, case[0], retained.destination),
                retained=retained,
            )
            assert result.profiles == ((1, 5), (2, 5), (3, 5)) and result.winner_count == 7
            assert result.retained_scalar_counts == (1, 2)
        finally:
            _cleanup(artifacts)


def _bad_entries(tmp_path, native, damage):
    if damage == "missing":
        return
    with closing(_entries(tmp_path, native)) as families:
        for shard, plan, family in families:
            if damage == "shard":
                shard = 2
            if damage == "native_id":
                family = replace(family, family_revision_id=family.family_revision_id + 1)
            if damage == "kind":
                family = replace(family, retained=True)
            yield shard, plan, family
    if damage == "extra":
        yield from _entries(tmp_path, native)


@pytest.mark.parametrize("damage", ["missing", "extra", "shard", "native_id", "kind"])
async def test_complete_selection_scope_and_native_ids_are_required(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        with pytest.raises(ValueError):
            _build(tmp_path, case, entries=_bad_entries(tmp_path, case[0], damage))
        assert not (tmp_path / "produced").exists() and not (tmp_path / "produced-winners").exists()
        assert Path(case[0].plan.root).exists()


@pytest.mark.parametrize("damage", ["generation", "cohort", "member_shard", "memory"])
async def test_owner_scope_and_resource_refusals_do_not_expose_outputs(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        options_by_damage = {
            "generation": dict(generation=replace(case[3], dataset_id=999)),
            "cohort": dict(cohort=True),
            "member_shard": dict(member_shard=-1),
            "memory": dict(limits=replace(_LIMITS, memory_bytes=4096)),
        }
        with pytest.raises((ValueError, FamilyBlockResourceLimit)):
            _build(tmp_path, case, **options_by_damage[damage])
        assert not (tmp_path / "produced").exists() and not (tmp_path / "produced-winners").exists()


async def test_builder_receives_only_budget_left_after_all_live_files(tmp_path, monkeypatch):
    original = producer.winners.build_winner_output
    reservations = []

    def observed(*arguments, **options):
        root = tmp_path / "produced"
        entries = tuple(root.iterdir())
        unit = max(512, os.statvfs(root).f_frsize)
        live = sum(path.stat().st_size for path in entries)
        entry_reservation = 2 * unit * (len(entries) + 1)
        reservations.append((live, entry_reservation, options["work"].limits))
        return original(*arguments, **options)

    async with _case(tmp_path, monkeypatch) as case:
        monkeypatch.setattr(producer.winners, "build_winner_output", observed)
        artifacts = _build(tmp_path, case)
        try:
            ((live, entry_reservation, bounds),) = reservations
            assert live + entry_reservation + bounds.max_spool_bytes == _LIMITS.max_spool_bytes
            assert 0 < bounds.memory_bytes < _LIMITS.memory_bytes
            assert Path(artifacts[0].path).stat().st_size + artifacts[1].file_bytes <= _LIMITS.max_spool_bytes
        finally:
            _cleanup(artifacts)


def test_builder_disk_and_live_head_reservations_are_not_double_spent():
    total = SourceFactLimits(2 * 1024**2, 4096, 2 * 1024**2)
    work = SimpleNamespace(
        used=total.max_spool_bytes - 4096,
        remaining_bytes=2048,
        limits=replace(total, memory_bytes=65536),
    )
    limits, heads = producer._builder_limits(total, work, (200, 0, 400), 3)
    assert limits.max_spool_bytes == limits.memory_bytes == 2048
    assert heads == 2 * 4096 + 8 * 600
    work.used = total.max_spool_bytes
    with pytest.raises(FamilyBlockResourceLimit):
        producer._builder_limits(total, work, (200, 0, 400), 3)


@pytest.mark.parametrize("stage", ["members", "ordering", "builder", "seal"])
async def test_failed_or_canceled_production_closes_inputs_and_all_owned_files(tmp_path, monkeypatch, stage):
    target_name = {
        "members": "_stage_members",
        "ordering": "_ordered_profile",
        "builder": "_profile_streams",
        "seal": "_seal_members",
    }[stage]
    original = getattr(producer, target_name)
    events_by_name = {"stopped": False, "closed": False}

    def entries(native):
        try:
            yield from _entries(tmp_path, native)
        finally:
            events_by_name["closed"] = True

    def check():
        if events_by_name["stopped"]:
            raise RuntimeError("owner stop")

    def stop_after(*arguments, **options):
        value = original(*arguments, **options)
        events_by_name["stopped"] = True
        return value

    def refuse(*_arguments, **_options):
        raise RuntimeError("owner stop")

    async with _case(tmp_path, monkeypatch) as case:
        monkeypatch.setattr(producer, target_name, stop_after if stage in {"members", "ordering"} else refuse)
        with pytest.raises(RuntimeError, match="owner stop"):
            _build(tmp_path, case, entries=entries(case[0]), check=check)
        assert events_by_name["closed"]
        assert not (tmp_path / "produced").exists() and not (tmp_path / "produced-winners").exists()
        assert Path(case[0].plan.root).exists() and Path(case[4].root).exists()


async def test_partial_factory_eof_cannot_publish_a_valid_looking_prefix(tmp_path, monkeypatch):
    original = producer._range_candidates

    def prefix(*arguments):
        with closing(original(*arguments)) as candidates:
            first = next(candidates, None)
            if first is not None:
                yield first

    async with _case(tmp_path, monkeypatch) as case:
        monkeypatch.setattr(producer, "_range_candidates", prefix)
        with pytest.raises(ValueError, match="complete cohort EOF"):
            _build(tmp_path, case)
        assert not (tmp_path / "produced").exists() and not (tmp_path / "produced-winners").exists()


async def test_foreign_winner_directory_is_preserved_after_refusal(tmp_path, monkeypatch):
    foreign = tmp_path / "produced-winners"
    foreign.mkdir()
    sentinel = foreign / "keep"
    sentinel.write_text("unrelated")
    async with _case(tmp_path, monkeypatch) as case:
        with pytest.raises(FileExistsError):
            _build(tmp_path, case)
        assert sentinel.read_text() == "unrelated" and not (tmp_path / "produced").exists()


async def test_root_only_query_preserves_all_configured_profiles(tmp_path, monkeypatch):
    document = json.loads(_definition().canonical)
    document["query"].pop("child")
    document["selection_profiles"] = document["selection_profiles"][:1]
    async with _case(tmp_path, monkeypatch, definition=CustomImportDefinition.from_mapping(document)) as case:
        artifacts = _build(tmp_path, case)
        try:
            assert len(_member_rows(artifacts[0])) == 1
            assert len(tuple(output.read_winner_candidates(artifacts[1]))) == 1
            assert _compare(tmp_path, case, artifacts).profiles == ((1, 1),)
        finally:
            _cleanup(artifacts)


@pytest.mark.parametrize(
    "ranges,size", [(((0, 0, 0),), 1), (((1, 4, 1),), 4), (((0, 4, 1), (2, 8, 1)), 8), (((0, 8, True),), 8)]
)
def test_profile_ranges_must_cover_exact_file_once(ranges, size):
    with pytest.raises(ValueError):
        producer._complete_ranges((None, ranges, (1, 2, size), b"x" * 32))


def test_raw_candidate_and_combined_head_memory_have_operational_bounds():
    candidate = selection.WinnerCandidate(1, 2, b"s" * 32, 0, None, None, {"text": "x" * 1000})
    with pytest.raises(FamilyBlockResourceLimit, match="stage memory"):
        producer._candidate_bytes(candidate, SimpleNamespace(limits=replace(_LIMITS, memory_bytes=8192)))
    heads = producer._Heads(9000)
    first = heads.reserve(b"x")
    with pytest.raises(FamilyBlockResourceLimit, match="heads"):
        heads.reserve(b"x" * 1000)
    assert heads.used == first


def _repeat_child(factory):
    first = None
    with closing(factory()) as children:
        for child in children:
            first = child if first is None else first
            yield child
    if first is not None:
        yield first


def _repeated_candidate_entries(tmp_path, native):
    with closing(_entries(tmp_path, native)) as families:
        for shard, plan, family in families:
            factory = family.children_by_collection["details"]
            yield shard, plan, replace(family, children_by_collection={"details": lambda: _repeat_child(factory)})


async def test_raw_multiplicity_remains_provisional_until_independent_original_proof(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        # Deliberately damage this caller's records; producer files grant no
        # original-membership proof and must retain the duplicate occurrence.
        artifacts = _build(tmp_path, case, entries=_repeated_candidate_entries(tmp_path, case[0]))
        try:
            audit_entries = tuple(output.read_winner_candidates(artifacts[1]))
            assert len(audit_entries) == 12 and all(
                streams[0][1] == 4 for _slot, streams, _count in artifacts[1].profiles
            )
            with pytest.raises(proof.FamilyBlockValidationError, match="multiplicity"):
                _compare(tmp_path, case, artifacts)
        finally:
            _cleanup(artifacts)


async def test_absent_original_families_create_no_members_or_contexts(tmp_path, monkeypatch):
    rows_by_stream = {
        "root_source": [[_root(), _root("1234567893", score=None)]],
        "detail_source": [[_child(), _child("1234567893")]],
    }
    async with original_case(tmp_path, monkeypatch, rows_by_stream, definition=_definition()) as native:
        request = native.context.request
        generation = selection.GenerationIdentity(
            501, request.dataset_id, request.definition_revision_id, request.schema_revision_id
        )
        assert native.plan.counts_by_kind["source"] == native.plan.counts_by_kind["absent"] == 1
        artifacts = producer.build_original_member_winners(
            native.context,
            generation,
            native.plan,
            _entries(tmp_path, native),
            2,
            work=PlanWork(tmp_path / "produced", _LIMITS, lambda _path: None),
            member_shard_id=99,
        )
        try:
            assert len(_member_rows(artifacts[0])) == 1
            assert len(tuple(output.read_winner_candidates(artifacts[1]))) == 6
        finally:
            _cleanup(artifacts)
