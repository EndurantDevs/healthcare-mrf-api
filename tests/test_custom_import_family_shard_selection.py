# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete shard merges retain the existing winner semantics and refusals."""

import hashlib
import itertools
import json
import random
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from process.custom_import import materialization as selection
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_shard_selection import iter_shard_profile_winners
from process.custom_import.materialization import (
    GenerationIdentity,
    ValidatedWinnerCandidateStream,
    WinnerCandidate,
    WinnerMaterializationError,
)

_GENERATION = GenerationIdentity(101, 11, 21, 31)
_SLOTS = {"rates": 7}


def _definition(*, nulls="last", multiple=False):
    document = json.loads((Path(__file__).parent / "fixtures/custom_import/v1_valid.json").read_text())
    document["selection_profiles"][0]["selection"][0]["nulls"] = nulls
    if multiple:
        document["selection_profiles"].extend(
            [
                {
                    "id": "largest",
                    "selection": [{"field": "amount", "direction": "desc", "nulls": "first"}],
                    "context_dimensions": [],
                },
                {
                    "id": "named",
                    "selection": [{"field": "display_name", "direction": "asc", "nulls": "last"}],
                    "context_dimensions": ["npi"],
                },
            ]
        )
    return CustomImportDefinition.from_mapping(document)


def _digest(value):
    return hashlib.sha256(str(value).encode()).digest()


def _candidate(index, *, entity=41, context="A", amount=Decimal("1")):
    return WinnerCandidate(
        entity,
        1000 + index,
        _digest(index),
        7,
        2000 + index,
        _digest((index, "key")),
        {"service_code": context, "amount": amount},
    )


def _stream(rows, generation=_GENERATION):
    return ValidatedWinnerCandidateStream(generation, rows)


def _sort(definition, rows, profile_slot=1):
    scopes = selection._profile_scopes(definition, _SLOTS)
    contract = selection._winner_candidate_contract(definition, scopes)
    profile, scope = definition.selection_profiles[profile_slot - 1], scopes[profile_slot - 1]
    matching, other = [], []
    for raw in rows:
        candidate = selection._normalize_winner_candidate(raw, contract)
        if raw.context_collection_slot != scope.collection_slot:
            other.append(raw)
            continue
        canonical, digest = selection._context_key(profile, candidate, contract.fields_by_id)
        matching.append(((raw.entity_binding_id, digest, canonical), raw))
    return (*other, *(raw for _key, raw in sorted(matching, key=lambda item: item[0])))


def _merged(definition, shards, profile_slot=1):
    return tuple(
        iter_shard_profile_winners(
            definition,
            generation=_GENERATION,
            profile_slot=profile_slot,
            shards=tuple(_stream(_sort(definition, shard, profile_slot)) for shard in shards),
            child_collection_slots=_SLOTS,
        )
    )


def _monolithic(definition, rows):
    return selection.materialize_winners(
        definition, generation=_GENERATION, candidates=_stream(rows), child_collection_slots=_SLOTS
    ).winners


@pytest.mark.parametrize("seed", range(8))
@pytest.mark.parametrize("nulls", ("first", "last"))
def test_random_shards_profiles_and_input_orders_match_monolithic(seed, nulls):
    definition = _definition(nulls=nulls, multiple=True)
    rng = random.Random(seed)
    rows = [
        _candidate(
            index, entity=41 + index % 4, context=str(index % 7), amount=None if index % 5 == 0 else Decimal(index % 9)
        )
        for index in range(90)
    ]
    rows.extend(
        WinnerCandidate(
            41 + index % 4,
            3000 + index,
            _digest(("root", index)),
            0,
            None,
            None,
            {"npi": "1234567893", "display_name": f"Name {index % 3}"},
        )
        for index in range(15)
    )
    rng.shuffle(rows)
    shards = [[] for _ in range(7)]
    for row in rows:
        shards[rng.randrange(len(shards))].append(row)
    rng.shuffle(shards)
    actual_items = tuple(winner for slot in range(1, 4) for winner in _merged(definition, shards, slot))
    assert actual_items == _monolithic(definition, rows)


@pytest.mark.parametrize("order", itertools.permutations(range(3)))
@pytest.mark.parametrize("physical", (False, True))
@pytest.mark.parametrize("better", (False, True))
def test_later_better_candidate_clears_cross_shard_tie(order, physical, better):
    definition = _definition()
    first = _candidate(1)
    conflict = (
        replace(first, family_revision_id=1003, context_child_revision_id=2003)
        if physical
        else replace(first, values_by_field={**first.values_by_field, "display_name": "different"})
    )
    third = replace(
        first,
        family_revision_id=1002,
        context_child_revision_id=2002,
        family_sha256=(b"\x00" if better else b"\xff") * 32,
    )
    rows = tuple((first, conflict, third)[index] for index in order)
    shards = ((rows[0], rows[1]), (rows[2],), ())
    if better:
        assert _merged(definition, shards) == _monolithic(definition, rows)
        assert _merged(definition, shards)[0].family_revision_id == 1002
    else:
        message = "conflicting physical identity" if physical else "conflicting typed values"
        with pytest.raises(WinnerMaterializationError, match=message):
            _monolithic(definition, rows)
        with pytest.raises(WinnerMaterializationError, match=message):
            _merged(definition, shards)


def test_child_hash_tie_break_uses_equal_hashes_without_allocation_order():
    definition = _definition()
    first = replace(_candidate(1), family_sha256=b"f" * 32, context_child_key_sha256=b"z" * 32)
    better = replace(_candidate(9), family_sha256=b"f" * 32, context_child_key_sha256=b"a" * 32)
    assert _merged(definition, ((first,), (better,))) == _monolithic(definition, (better, first))
    assert _merged(definition, ((first,), (better,)))[0].family_revision_id == better.family_revision_id


def test_context_digest_collision_from_a_losing_candidate_is_not_discarded(monkeypatch):
    definition = _definition()
    monkeypatch.setattr(selection, "_context_digest", lambda _canonical: b"c" * 32)
    best_a = _candidate(1, context="A", amount=Decimal("1"))
    losing_a = _candidate(2, context="A", amount=Decimal("9"))
    other_context = _candidate(3, context="B", amount=Decimal("20"))
    rows = (losing_a, best_a, other_context)
    with pytest.raises(WinnerMaterializationError, match="context digest collision"):
        _monolithic(definition, rows)
    with pytest.raises(WinnerMaterializationError, match="context digest collision"):
        _merged(definition, ((losing_a, other_context), (best_a,)))


def test_missing_and_explicit_null_contexts_remain_distinct():
    document = json.loads(_definition().canonical)
    document["selection_profiles"][0]["context_dimensions"] = ["amount"]
    definition = CustomImportDefinition.from_mapping(document)
    first = replace(_candidate(1), values_by_field={"service_code": "A"})
    second = _candidate(2, amount=None)
    third = _candidate(3, amount=Decimal("0"))
    assert _merged(definition, ((first, third), (second,))) == _monolithic(definition, (second, first, third))
    assert len(_merged(definition, ((first, third), (second,)))) == 3


def test_each_shard_order_is_checked_before_merge():
    definition = _definition()
    rows = (_candidate(1, entity=41), _candidate(2, entity=43), _candidate(3, entity=42))
    result = iter_shard_profile_winners(
        definition,
        generation=_GENERATION,
        profile_slot=1,
        shards=(_stream(rows), _stream((_candidate(4, entity=44),))),
        child_collection_slots=_SLOTS,
    )
    assert next(result).entity_binding_id == 41
    with pytest.raises(WinnerMaterializationError, match="shard candidates.*order"):
        tuple(result)


def test_membership_generation_and_profile_are_checked_before_consumption():
    definition = _definition()
    stream = _stream(())
    for changes, message in (
        ({"generation": replace(_GENERATION, generation_id=102)}, "generation does not match"),
        ({"profile_slot": True}, "profile slot"),
        ({"shards": (object(),)}, "validated generation"),
        ({"shards": iter((stream,))}, "bounded sequence"),
    ):
        arguments_by_name = dict(
            generation=_GENERATION, profile_slot=1, shards=(stream,), child_collection_slots=_SLOTS
        )
        with pytest.raises(WinnerMaterializationError, match=message):
            tuple(iter_shard_profile_winners(definition, **(arguments_by_name | changes)))
        assert not stream._is_consumed


def test_other_scopes_are_validated_and_streams_are_single_use():
    definition = _definition(multiple=True)
    root = WinnerCandidate(41, 100, b"r" * 32, 0, None, None, {"display_name": "Name", "npi": "1234567893"})
    stream = _stream((root, _candidate(1)))
    arguments_by_name = dict(generation=_GENERATION, profile_slot=1, shards=(stream,), child_collection_slots=_SLOTS)
    assert len(tuple(iter_shard_profile_winners(definition, **arguments_by_name))) == 1
    with pytest.raises(WinnerMaterializationError, match="already consumed"):
        tuple(iter_shard_profile_winners(definition, **arguments_by_name))
    invalid = replace(root, values_by_field={"display_name": 99})
    with pytest.raises(WinnerMaterializationError):
        _merged(definition, ((invalid,), (_candidate(1),)))


def test_merge_reads_only_one_head_per_shard_before_producing_a_prefix():
    definition = _definition()
    consumed_items = [0, 0, 0]

    def rows(shard):
        for ordinal in range(100):
            consumed_items[shard] += 1
            yield _candidate(ordinal * 3 + shard, entity=41 + ordinal * 3 + shard)

    streams = tuple(_stream(rows(shard)) for shard in range(3))
    result = iter_shard_profile_winners(
        definition, generation=_GENERATION, profile_slot=1, shards=streams, child_collection_slots=_SLOTS
    )
    assert next(result).entity_binding_id == 41
    assert consumed_items == [2, 1, 1]
    assert len(tuple(result)) == 299
    assert consumed_items == [100, 100, 100]


def test_empty_shard_plan_has_no_winners():
    assert _merged(_definition(), ()) == ()
