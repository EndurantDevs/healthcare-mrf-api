# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original readers, real winner artifacts and reducer; native IDs/lookup are fake."""

import hashlib
import json
import struct
import time
from contextlib import asynccontextmanager, closing, contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import family_shard_base_values as retained
from process.custom_import import family_shard_retained_allocate as allocate
from process.custom_import import family_shard_retained_validation as retained_validation
from process.custom_import import family_shard_winner_validation as validation
from process.custom_import import family_shard_winners as output
from process.custom_import import materialization as selection
from process.custom_import import runner_codec as codec
from process.custom_import.build_source import _WINDOW, _PageWindow
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_validate import FamilyBlockValidationError
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork, _plan, read_family_plan
from process.custom_import.family_shard_retained_allocate import RetainedAllocationContext
from process.custom_import.family_shard_selection import _record_values, family_candidates
from process.custom_import.family_shard_source_values import source_family_inputs
from process.custom_import.materialization import GenerationIdentity, ValidatedWinnerCandidateStream
from process.custom_import.runner_codec import digest_text, root_key_contract_hash
from process.custom_import.runner_types import CandidateRegistry
from tests import test_custom_import_family_shard_admission as admission_fixture
from tests import test_custom_import_family_shard_source_plan as capture_fixture
from tests.test_custom_import_family_shard_base_values import _legacy_rows, _pinned_context, _records, _transport
from tests.test_custom_import_family_shard_base_values import _pack_row as native_pack_row
from tests.test_custom_import_family_shard_common import (
    _ENTITY_STAMP,
    _ROOT_STAMP,
    _build_common,
    _entity_row,
    _lookup_fixture,
    _root_rows,
)
from tests.test_custom_import_family_shard_original_validation import _case as source_case
from tests.test_custom_import_family_shard_original_validation import _prove as prove_source
from tests.test_custom_import_family_shard_retained_allocate import _ids, _produce_destination
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_source_values import _definition as source_definition

_LIMITS = SourceFactLimits(64 * 1024**2, 4 * 1024**2, 8 * 1024**2)


def _definition():
    document = json.loads(source_definition().canonical)
    document["selection_profiles"] = [
        {
            "id": "default",
            "selection": [{"field": "score", "direction": "desc", "nulls": "last"}],
            "context_dimensions": [],
        },
        {
            "id": "small",
            "selection": [{"field": "amount", "direction": "asc", "nulls": "last"}],
            "context_dimensions": [],
        },
        {
            "id": "split",
            "selection": [{"field": "amount", "direction": "desc", "nulls": "last"}],
            "context_dimensions": ["detail_id"],
        },
    ]
    return CustomImportDefinition.from_mapping(document)


def _ordered(layout, candidates, slot):
    scopes = selection._profile_scopes(layout.definition, layout.collection_slots)
    contract = selection._winner_candidate_contract(layout.definition, scopes)
    profile, scope = layout.definition.selection_profiles[slot - 1], scopes[slot - 1]
    other, matching = [], []
    for raw in candidates:
        normalized = selection._normalize_winner_candidate(raw, contract)
        if raw.context_collection_slot != scope.collection_slot:
            other.append(raw)
        else:
            text, digest = selection._context_key(profile, normalized, contract.fields_by_id)
            matching.append(((raw.entity_binding_id, digest, text), raw))
    return (*other, *(raw for _key, raw in sorted(matching, key=lambda item: item[0])))


def _entries(tmp_path, native, *, shard=0):
    with source_family_inputs(
        native.context, native.admission, native.plan, work=PlanWork(tmp_path / "values", _LIMITS, lambda _path: None)
    ) as families:
        for plan, family in families:
            yield shard, plan, family


def _build_winners(tmp_path, native, generation, shards, *, profile_slots=None):
    @contextmanager
    def factory(slot):
        with _streams(native.context.layout, generation, shards, slot) as streams:
            yield streams

    return output.build_winner_output(
        native.context.layout,
        generation,
        factory,
        work=output.WinnerWork(tmp_path / "winners", _LIMITS, lambda _path: None),
        profile_slots=profile_slots,
    )


@contextmanager
def _streams(layout, generation, shards, slot):
    yield tuple(ValidatedWinnerCandidateStream(generation, _ordered(layout, candidates, slot)) for candidates in shards)


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, profile_slots=None, shards=None, empty=False, definition=None):
    rows_by_stream = {
        "root_source": [[_root()]],
        "detail_source": [[_child(key="a", amount="1.00"), _child(key="b", amount="9.00")]],
    }
    if empty:
        rows_by_stream = {name: [[]] for name in rows_by_stream}
    async with source_case(tmp_path, monkeypatch, rows_by_stream, definition=definition or _definition()) as native:
        source_comparison = prove_source(tmp_path, native)
        originals = await retained.build_retained_input(
            None, native.plan, work=retained.RetainedWork(tmp_path / "retained", _LIMITS, lambda _path: None)
        )
        _lookup_fixture(monkeypatch, native.context)
        plans = list(read_family_plan(native.plan))
        common = await _build_common(
            tmp_path,
            native.context,
            selected=tuple(plan.source_metadata[1] for plan in plans if plan.selection_kind == "source"),
            keys=native.keys,
        )
        context = RetainedAllocationContext(native.context, 777, native.admission, native.plan, originals, None, common)
        generation = GenerationIdentity(
            501,
            native.context.request.dataset_id,
            native.context.request.definition_revision_id,
            native.context.request.schema_revision_id,
        )
        candidates = []
        for _shard, _plan, family in _entries(tmp_path, native):
            candidates.extend(family_candidates(native.context.layout, family))
        winner = _build_winners(
            tmp_path,
            native,
            generation,
            (candidates, ()) if shards is None else shards(candidates),
            profile_slots=profile_slots,
        )
        try:
            yield native, context, source_comparison, generation, winner, candidates
        finally:
            output.cleanup_winner_output(replace(winner, file_bytes=Path(winner.root, output._FILE).stat().st_size))
            common.close()
            retained.cleanup_retained_input(originals)


def _prove(tmp_path, case, *, entries=None, descriptor=None, generation=None, source=None, check=None, shard_count=2):
    native, context, original_source, identity, winner, _candidates = case
    return validation.prove_original_winners(
        context,
        original_source if source is None else source,
        identity if generation is None else generation,
        winner if descriptor is None else descriptor,
        _entries(tmp_path, native) if entries is None else entries,
        shard_count,
        work=PlanWork(tmp_path / "winner-proof", _LIMITS, lambda _path: None, check),
    )


async def test_all_profiles_losers_and_other_scopes_match_originals(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch) as case:
        result = _prove(tmp_path, case)
        assert result.profiles == ((1, 3), (2, 3), (3, 3))
        assert result.winner_count == 4 and dict(result.family_counts) == {"source": 1, "retained": 0}
        assert result.source_sha256 == case[2].sha256 and result.winner_sha256 == case[4].sha256
        assert len(result.sha256) == 32 and not hasattr(result, "verified")
        assert not (tmp_path / "winner-proof").exists()
        assert Path(case[4].root).is_dir()


@pytest.mark.parametrize(
    "damage", ["missing_loser", "missing_other_scope", "duplicate", "foreign_id", "scalar", "wrong_shard"]
)
async def test_changed_raw_candidates_refuse_against_originals(monkeypatch, tmp_path, damage):
    async with _case(tmp_path, monkeypatch, shards=lambda values: _changed_candidates(values, damage)) as case:
        with pytest.raises(FamilyBlockValidationError, match="membership or multiplicity"):
            _prove(tmp_path, case)
        assert not (tmp_path / "winner-proof").exists()


@pytest.mark.parametrize(
    "damage", ["profile_subset", "generation", "source_provenance", "cohort", "missing_family", "extra_family"]
)
async def test_full_scope_and_selected_plan_coverage_are_required(monkeypatch, tmp_path, damage):
    async with _case(tmp_path, monkeypatch, profile_slots=(1,) if damage == "profile_subset" else None) as case:
        native, _context, source_comparison, generation, _winner, _candidates = case
        options_by_damage = {
            "profile_subset": {},
            "generation": {"generation": replace(generation, generation_id=999)},
            "source_provenance": {"source": replace(source_comparison, inputs=())},
            "cohort": {"shard_count": 1},
            "missing_family": {"entries": iter(())},
            "extra_family": {"entries": _double_entries(tmp_path, native)},
        }
        with pytest.raises(FamilyBlockValidationError):
            _prove(tmp_path, case, **options_by_damage[damage])
        assert not (tmp_path / "winner-proof").exists()


def _changed_candidates(values, damage):
    candidates = list(values)
    if damage == "missing_loser":
        candidates.pop()
        return candidates, ()
    if damage == "missing_other_scope":
        return candidates[1:], ()
    if damage == "duplicate":
        candidates.append(candidates[0])
        return candidates, ()
    if damage == "foreign_id":
        candidates[0] = replace(candidates[0], family_revision_id=999999)
        return candidates, ()
    if damage == "scalar":
        candidates[0] = replace(candidates[0], values_by_field={**candidates[0].values_by_field, "score": 99})
        return candidates, ()
    if damage == "wrong_shard":
        return (), candidates
    return candidates, ()


def _double_entries(tmp_path, native):
    yield from _entries(tmp_path, native)
    yield from _entries(tmp_path, native)


def _profile_commitment(entries, descriptor, slot):
    count = len(descriptor.profiles[slot - 1][1])
    censuses = [[0, 0, hashlib.sha256(output._STREAM_DOMAIN)] for _index in range(count)]
    rewritten_entries = []
    winner_count = 0
    for entry in entries:
        if entry[0] == "candidate":
            _kind, _slot, shard, _ordinal, packed = entry
            cells = _unpack_row(packed)
            census = censuses[shard]
            rewritten_entries.append(("candidate", slot, shard, census[0], packed))
            output._feed(census[2], cells)
            census[0] += 1
            census[1] += int(cells[3] == dict(descriptor.profile_scopes)[slot])
        else:
            rewritten_entries.append(entry)
            winner_count += int(entry[0] == "winner")
    streams = tuple((index, count, matches, digest.digest()) for index, (count, matches, digest) in enumerate(censuses))
    census = slot, streams, winner_count
    return [("profile", slot, count), *rewritten_entries, ("profile_complete", *census)], census


def _rewrite_winners(descriptor, change):
    bodies = dict.fromkeys(descriptor.profile_slots)
    for slot in bodies:
        bodies[slot] = []
    for entry in output._checked_entries(descriptor):
        if entry[0] in ("candidate", "winner"):
            bodies[entry[1]].append(entry)
    entries, profiles = [], []
    role_digest = hashlib.sha256(output._ROLE_DOMAIN)
    for slot, body in bodies.items():
        profile_entries, census = _profile_commitment(change(slot, body), descriptor, slot)
        entries.extend(profile_entries)
        profiles.append(census)
        for entry in profile_entries:
            if entry[0] == "winner":
                output._feed(role_digest, ("winner", entry[2]))
    winner_count = sum(profile[2] for profile in profiles)
    frames = [
        (
            "header",
            output._CONTRACT,
            descriptor.layout_sha256,
            descriptor.definition_sha256,
            descriptor.generation,
            descriptor.profile_slots,
            descriptor.profile_scopes,
            output._COLUMNS,
        ),
        *entries,
        ("complete", tuple(profiles), winner_count, role_digest.digest()),
    ]
    packed = b"".join(struct.pack("!Q", len(frame)) + frame for frame in map(_pack_row, frames))
    path = Path(descriptor.root, output._FILE)
    path.chmod(0o600)
    path.write_bytes(packed)
    path.chmod(0o400)
    return replace(
        descriptor,
        file_bytes=len(packed),
        sha256=hashlib.sha256(packed).digest(),
        profiles=tuple(profiles),
        winner_count=winner_count,
        winner_rows_sha256=role_digest.digest(),
    )


def _audit_change(slot, entries, damage):
    if damage == "loser" and slot == 2:
        return [
            entry
            for entry in entries
            if not (entry[0] == "candidate" and dict(_unpack_row(entry[4])[-1]).get("amount") == "9.00")
        ]
    if damage == "other_scope" and slot == 1:
        has_skipped = False
        changed_entries = []
        for entry in entries:
            if not has_skipped and entry[0] == "candidate" and _unpack_row(entry[4])[3] != 0:
                has_skipped = True
            else:
                changed_entries.append(entry)
        return changed_entries
    if damage == "duplicate" and slot == 1:
        candidate = next(entry for entry in entries if entry[0] == "candidate")
        return [candidate, *entries]
    if damage == "order" and slot == 3:
        matching_entries = [entry for entry in entries if entry[0] == "candidate" and _unpack_row(entry[4])[3]]
        changed = iter(reversed(matching_entries))
        return [next(changed) if entry in matching_entries else entry for entry in entries]
    if damage == "winner_id" and slot == 2:
        changed_entries = []
        for entry in entries:
            if entry[0] == "winner":
                cells = list(entry[2])
                cells[output._COLUMNS.index("family_revision_id")] += 1
                entry = (*entry[:2], tuple(cells))
            changed_entries.append(entry)
        return changed_entries
    return entries


@pytest.mark.parametrize("damage", ["loser", "other_scope", "duplicate", "order", "winner_id"])
async def test_valid_file_commitments_cannot_replace_original_membership_or_reducer(monkeypatch, tmp_path, damage):
    async with _case(tmp_path, monkeypatch) as case:
        original_winners = list(output.read_winner_rows(case[4]))
        changed = _rewrite_winners(case[4], lambda slot, entries: _audit_change(slot, entries, damage))
        assert len(list(output.read_winner_candidates(changed))) > 0  # File/census commitments are all valid.
        if damage != "winner_id":
            assert list(output.read_winner_rows(changed)) == original_winners
        with pytest.raises((FamilyBlockValidationError, selection.WinnerMaterializationError)):
            _prove(tmp_path, case, descriptor=changed)
        assert not (tmp_path / "winner-proof").exists()


def _owner_window_boundary(monkeypatch, common):
    def bounded_window(original=None):
        deadline = allocate._read_deadline(common.request)
        lease_deadline = time.monotonic() + common.request.lease_seconds if original is None else original.deadline
        return _PageWindow(
            lease_deadline if deadline is None else min(deadline, lease_deadline), common.request.statement_timeout_ms
        )

    @asynccontextmanager
    async def session(*_arguments):
        window = bounded_window()
        allocate._require_deadline(window.deadline)
        native = SimpleNamespace(info={_WINDOW: window})
        yield native, None
        assert native.info[_WINDOW] is window
        allocate._require_deadline(window.deadline)

    _ids(monkeypatch)
    original_observe = allocate._observe_ids

    async def observe(*arguments, remembered=None, cursor=()):
        callback = None if remembered is None else lambda window: remembered(bounded_window(window))
        return await original_observe(*arguments, remembered=callback, cursor=cursor)

    monkeypatch.setattr(allocate, "_page_session", session)
    monkeypatch.setattr(allocate, "_observe_ids", observe)


def _dictionary_boundary(monkeypatch, common):
    roots = _root_rows(common)

    async def lookup(_factory, context, batch, _prefix):
        root_rows, entity_rows = [], {}
        for header in batch:
            root_id, kind, plan_cells, _family, _revision, entity_id = header[0][:6]
            plan = _plan(plan_cells)
            root_rows.append(
                (
                    roots[root_id]
                    if kind == "source"
                    else dict(
                        root_record_id=root_id,
                        dataset_id=common.request.dataset_id,
                        key_contract_sha256=root_key_contract_hash(common.request.definition),
                        canonical_logical_key=plan.typed_key[0],
                        logical_key_sha256=plan.typed_key[1],
                        created_at=_ROOT_STAMP,
                    ),
                )
            )
            entity_rows[entity_id] = (
                _entity_row(common, entity_id)
                if kind == "source"
                else dict(
                    entity_binding_id=entity_id,
                    dataset_id=common.request.dataset_id,
                    adapter_id="npi",
                    canonical_value="1234567893",
                    value_sha256=digest_text("entity:npi", "1234567893"),
                    created_at=_ENTITY_STAMP,
                )
            )
        return tuple(root_rows), tuple(entity_rows[key] for key in sorted(entity_rows))

    monkeypatch.setattr(allocate, "_lookup_dictionaries", lookup)
    _owner_window_boundary(monkeypatch, common)
    monkeypatch.setattr(allocate, "_guard", AsyncMock())
    monkeypatch.setattr(
        allocate,
        "load_registry",
        AsyncMock(return_value=CandidateRegistry({"details": 7}, {"root_source": 1, "detail_source": 2}, 1)),
    )


def _retained_entries(tmp_path, native, destination):
    with (
        source_family_inputs(
            native.context,
            native.admission,
            native.plan,
            work=PlanWork(tmp_path / "source-values", _LIMITS, lambda _path: None),
        ) as sources,
        allocate.retained_destination_families(
            destination,
            native.context.layout.definition,
            work=PlanWork(tmp_path / "destination-values", _LIMITS, lambda _path: None),
        ) as retained_families,
    ):
        source_item, retained_item = next(sources, None), next(retained_families, None)
        while source_item is not None or retained_item is not None:
            if retained_item is None or (
                source_item is not None and source_item[0].typed_key[::-1] < retained_item[0].typed_key[::-1]
            ):
                yield 0, *source_item
                source_item = next(sources, None)
            else:
                yield 1, retained_item[0], retained_item[2]
                retained_item = next(retained_families, None)


@asynccontextmanager
async def _retained_case(tmp_path, monkeypatch):
    """Use real original/destination readers with native transport faked."""
    monkeypatch.setattr(admission_fixture, "_case", _wide_capture_case)
    async with source_case(tmp_path, monkeypatch, definition=_definition(), base=True) as native:
        source_comparison = prove_source(tmp_path, native)
        base = _pinned_context(native.context.request, monkeypatch, False)
        plans = list(read_family_plan(native.plan))
        retained_header = next(plan.base_header for plan in plans if plan.selection_kind == "retained")
        root, children = _records()
        root = replace(root, revision_id=retained_header.root_revision_id)
        rows_by_role = _legacy_rows(base, retained_header, root, children)
        rows_by_role["pack"] = [native_pack_row(base, 201), native_pack_row(base, 202, child=True)]
        _transport(monkeypatch, rows_by_role, [])
        originals = await retained.build_retained_input(
            base, native.plan, work=retained.RetainedWork(tmp_path / "retained", _LIMITS, lambda _path: None)
        )
        _lookup_fixture(monkeypatch, native.context)
        common = await _build_common(
            tmp_path,
            native.context,
            selected=tuple(plan.source_metadata[1] for plan in plans if plan.selection_kind == "source"),
            keys=native.keys,
        )
        context = RetainedAllocationContext(native.context, 777, native.admission, native.plan, originals, base, common)
        _dictionary_boundary(monkeypatch, native.context)
        destination = await _produce_destination(None, context, work=_allocation_work(tmp_path, "destination"))
        comparison = await retained_validation.verify_retained_destination(
            context,
            destination,
            work=_allocation_work(tmp_path, "retained-comparison"),
        )
        generation = GenerationIdentity(
            501,
            native.context.request.dataset_id,
            native.context.request.definition_revision_id,
            native.context.request.schema_revision_id,
        )
        candidate_streams = [[], []]
        for shard, _plan_item, family in _retained_entries(tmp_path, native, destination):
            candidate_streams[shard].extend(family_candidates(native.context.layout, family))
        winner = _build_winners(tmp_path, native, generation, candidate_streams)
        try:
            yield (
                (native, context, source_comparison, generation, winner, candidate_streams),
                validation.RetainedWinnerInput(destination, comparison),
            )
        finally:
            output.cleanup_winner_output(replace(winner, file_bytes=Path(winner.root, output._FILE).stat().st_size))
            allocate.cleanup_retained_destination(destination)
            common.close()
            retained.cleanup_retained_input(originals)


async def _wide_capture_case(*arguments, **options):
    return await capture_fixture._case(*arguments, **options, row_limit=128, byte_limit=1024**2)


def _allocation_work(tmp_path, label):
    return allocate.RetainedAllocationWork(
        tmp_path / label, SourceAllocationLimits(_LIMITS, 10000, 32 * 1024**2), lambda _path: None
    )


async def test_retained_original_destination_ids_and_sparse_payload_values_are_bound(monkeypatch, tmp_path):
    async with _retained_case(tmp_path, monkeypatch) as (case, retained_originals):
        destination = retained_originals.destination
        native, context, source_comparison, generation, winner, candidates = case
        assert candidates[1][0].values_by_field["score"] == 7
        assert {str(candidate.values_by_field["amount"]) for candidate in candidates[1][1:]} == {"1.23"}
        assert source_comparison.retained_family_count_unproven == 1
        native_queries = context.base.session.scalar.await_count
        result = validation.prove_original_winners(
            context,
            source_comparison,
            generation,
            winner,
            _retained_entries(tmp_path, native, destination),
            2,
            retained_originals=retained_originals,
            work=PlanWork(tmp_path / "winner-proof", _LIMITS, lambda _path: None),
        )
        assert dict(result.family_counts) == {"source": 1, "retained": 1}
        assert result.profiles == ((1, 5), (2, 5), (3, 5)) and result.winner_count == 7
        assert result.retained_original_counts == context.retained_input.counts
        assert (
            result.retained_sha256 == destination.sha256 and context.base.session.scalar.await_count == native_queries
        )


@pytest.mark.parametrize("cohort", [0, 2])
async def test_empty_originals_still_complete_all_profiles_and_cohort(monkeypatch, tmp_path, cohort):
    async with _case(tmp_path, monkeypatch, empty=True, shards=lambda _values: [()] * cohort) as case:
        result = _prove(tmp_path, case, shard_count=cohort)
        assert result.profiles == ((1, 0), (2, 0), (3, 0)) and result.winner_count == 0
        assert dict(result.family_counts) == {"source": 0, "retained": 0}
        assert not (tmp_path / "winner-proof").exists()


async def test_root_only_profile_still_consumes_nonquery_original_children(monkeypatch, tmp_path):
    document = json.loads(_definition().canonical)
    document["query"].pop("child")
    document["selection_profiles"] = document["selection_profiles"][:1]
    async with _case(tmp_path, monkeypatch, definition=CustomImportDefinition.from_mapping(document)) as case:
        result = _prove(tmp_path, case)
        assert result.profiles == ((1, 1),) and result.winner_count == 1
        assert case[2].counts_by_name["family_child_count"] == 2


@pytest.mark.parametrize("phase", ["originals", "audit", "reducer"])
async def test_cancellation_closes_replays_and_removes_only_proof_scratch(monkeypatch, tmp_path, phase):
    cancel_events = []

    def check():
        if cancel_events:
            raise RuntimeError("owner cancellation")

    target = {"originals": "_stage_originals", "audit": "_stage_audit", "reducer": "_reduced_profile"}[phase]
    operation = getattr(validation, target)

    def stage(*arguments):
        value = operation(*arguments)
        cancel_events.append(True)
        return value

    def reducer(*arguments):
        with closing(operation(*arguments)) as winners:
            yield next(winners)
            cancel_events.append(True)
            yield from winners

    async with _case(tmp_path, monkeypatch) as case:
        monkeypatch.setattr(validation, target, reducer if phase == "reducer" else stage)
        with pytest.raises(RuntimeError, match="owner cancellation"):
            _prove(tmp_path, case, check=check)
        assert not (tmp_path / "winner-proof").exists()
        assert Path(case[4].root, output._FILE).exists() and Path(case[0].plan.root).exists()
        assert not (tmp_path / "values").exists()


def test_reducer_live_heads_are_charged_and_released(tmp_path):
    scratch = validation._Scratch(PlanWork(tmp_path / "heads", replace(_LIMITS, memory_bytes=8000), lambda _path: None))
    try:
        candidate = selection.WinnerCandidate(1, 2, b"s" * 32, 0, None, None, {"score": 7})
        path = scratch.write([(0, _pack_row(output._candidate_cells(candidate)))])
        heads = validation._StreamHeads(scratch)
        with closing(validation._candidates(scratch, path, heads)) as first:
            assert next(first) == candidate and heads.reserved > 0
            with closing(validation._candidates(scratch, path, heads)) as second:
                with pytest.raises(validation.FamilyBlockResourceLimit, match="reducer heads"):
                    next(second)
            assert heads.reserved > 0
        assert heads.reserved == 0
    finally:
        scratch.close()
    assert not (tmp_path / "heads").exists()


async def test_complete_winner_footer_and_file_eof_are_mandatory(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch) as case:
        descriptor = case[4]
        path = Path(descriptor.root, output._FILE)
        frame = _pack_row(("extra",))
        packed = path.read_bytes() + struct.pack("!Q", len(frame)) + frame
        path.chmod(0o600)
        path.write_bytes(packed)
        path.chmod(0o400)
        changed = replace(descriptor, file_bytes=len(packed), sha256=hashlib.sha256(packed).digest())
        with pytest.raises(ValueError, match="trailing records"):
            _prove(tmp_path, case, descriptor=changed)
        assert not (tmp_path / "winner-proof").exists()


def _prove_retained(tmp_path, case, originals, *, entries=None):
    native, context, source_comparison, generation, winner, _candidates = case
    return validation.prove_original_winners(
        context,
        source_comparison,
        generation,
        winner,
        _retained_entries(tmp_path, native, originals.destination) if entries is None else entries,
        2,
        retained_originals=originals,
        work=PlanWork(tmp_path / "winner-proof", _LIMITS, lambda _path: None),
    )


def _different_child_ids(factory):
    with closing(factory()) as children:
        for child in children:
            yield replace(child, revision_id=child.revision_id + 100)


def _different_root_payload(definition, root):
    values_by_field = dict(_record_values(definition, None, root), score=123)
    if root.sparse is None:
        return replace(root, values_by_field=values_by_field)
    payload = codec.record_payload(definition.root_fields, values_by_field)
    return replace(
        root,
        sparse=replace(root.sparse, payload_text=payload, payload_sha256=codec.digest_text("root-payload", payload)),
    )


def _different_retained_entries(tmp_path, native, destination, damage):
    with closing(_retained_entries(tmp_path, native, destination)) as families:
        for shard, plan, family in families:
            if plan.selection_kind != "retained":
                yield shard, plan, family
                continue
            if damage == "family_id":
                family = replace(family, family_revision_id=family.family_revision_id + 100)
            if damage == "child_id":
                original = family.children_by_collection["details"]
                family = replace(family, children_by_collection={"details": lambda: _different_child_ids(original)})
            if damage == "payload":
                family = replace(family, root=_different_root_payload(native.context.layout.definition, family.root))
            if damage == "missing":
                continue
            yield 0 if damage == "stream" else shard, plan, family


@pytest.mark.parametrize("damage", ["family_id", "child_id", "payload", "missing", "stream"])
async def test_retained_winner_inputs_must_match_real_destination_and_selection(monkeypatch, tmp_path, damage):
    async with _retained_case(tmp_path, monkeypatch) as (case, originals):
        entries = _different_retained_entries(tmp_path, case[0], originals.destination, damage)
        with pytest.raises(FamilyBlockValidationError):
            _prove_retained(tmp_path, case, originals, entries=entries)
        assert not (tmp_path / "winner-proof").exists()
        assert Path(originals.destination.root).exists() and Path(case[1].retained_input.root).exists()


@pytest.mark.parametrize("damage", ["scope", "sha", "counts", "root_count", "child_count", "boolean_count"])
async def test_retained_original_proof_is_required_and_fully_bound(monkeypatch, tmp_path, damage):
    async with _retained_case(tmp_path, monkeypatch) as (case, originals):
        proof = originals.comparison
        changes_by_damage = {
            "scope": dict(scope=()),
            "sha": dict(destination_sha256=b"x" * 32),
            "counts": dict(counts=()),
            "root_count": dict(root_scalar_count=proof.root_scalar_count + 1),
            "child_count": dict(child_scalar_count=-1),
            "boolean_count": dict(root_scalar_count=True),
        }
        changed = replace(originals, comparison=replace(proof, **changes_by_damage[damage]))
        with pytest.raises(FamilyBlockValidationError, match="comparison scope"):
            _prove_retained(tmp_path, case, changed)
        assert not (tmp_path / "winner-proof").exists()


async def test_retained_cannot_use_a_destination_without_its_original_comparison(monkeypatch, tmp_path):
    async with _retained_case(tmp_path, monkeypatch) as (case, _originals):
        with pytest.raises(FamilyBlockValidationError, match="proven original destination"):
            _prove(tmp_path, case)
        assert not (tmp_path / "winner-proof").exists()


async def test_cohort_assignment_comes_from_actual_factory_streams(monkeypatch, tmp_path):
    async with _case(tmp_path, monkeypatch, shards=lambda candidates: ((), candidates)) as case:
        result = _prove(tmp_path, case, entries=_entries(tmp_path, case[0], shard=1))
        assert result.profiles == ((1, 3), (2, 3), (3, 3)) and result.winner_count == 4


async def test_original_field_mapping_order_preserves_membership(monkeypatch, tmp_path):
    def reversed_fields(candidates):
        return [
            replace(candidate, values_by_field=dict(reversed(tuple(candidate.values_by_field.items()))))
            for candidate in candidates
        ], ()

    async with _case(tmp_path, monkeypatch, shards=reversed_fields) as case:
        result = _prove(tmp_path, case)
        assert result.profiles == ((1, 3), (2, 3), (3, 3)) and result.winner_count == 4
