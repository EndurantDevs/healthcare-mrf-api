# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact decision-priority cases; full native fact aggregation remains separate."""

import json
from dataclasses import replace

import pytest

from process.custom_import import family_shard_build as build
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import CandidateRejected, assemble_root_families
from process.custom_import.family_shard_input_records import prepare_record
from tests.test_custom_import_snowflake_bundle import _definition


def _records(definition, roots, children):
    observed_result_items = []
    for slot, stream in enumerate(definition.source_streams, 1):
        for ordinal, values in enumerate(roots if stream.record_kind == "root" else children):
            observed_result_items.append(
                prepare_record(
                    definition,
                    stream,
                    values,
                    stream_slot=slot,
                    collection_slot=0 if stream.record_kind == "root" else 1,
                    part_ordinal=1,
                    row_ordinal=ordinal,
                    source_ordinal=ordinal,
                )
            )
    return observed_result_items


def _root(npi="1003000126", score=7):
    return dict(npi=npi, score=score, enabled=True)


def _child(npi="1003000126", amount="2.0"):
    return dict(detail_npi=npi, detail_id="a", amount=amount)


def _facts(**changes):
    return build.SourceAdmissionFacts(
        **(
            dict(
                raw_root_count=1,
                typed_root_count=1,
                first_raw_root=(1, 0),
                child_count=1,
                payload_differs_from_last=False,
            )
            | changes
        )
    )


def _compound_definition(value_type="integer"):
    document = json.loads(_definition().canonical)
    document["schema"]["root"]["logical_key"] = ["npi", "score"]
    document["schema"]["root"]["fields"][1]["type"] = value_type
    child = document["schema"]["children"][0]
    child["parent_key"].append({"child": "detail_score", "root": "score"})
    child["fields"].append({"id": "detail_score", "slot": 7, "type": value_type, "nullable": False})
    return CustomImportDefinition.from_mapping(document)


def test_decimal_raw_key_variants_are_typed_duplicate_roots():
    definition = _compound_definition("decimal")
    roots = [_root(score="1.0"), _root(score="1.00")]
    pure = assemble_root_families(definition, roots, {"details": []})
    assert len(pure.families) == 2 and not pure.rejections
    records = _records(definition, roots, [])
    assert records[0].typed_parent_key == records[1].typed_parent_key
    assert records[0].raw_parent_key != records[1].raw_parent_key
    for record in records:
        facts = _facts(first_raw_root=build.occurrence_order(record), typed_root_count=2)
        decision = build.decide_occurrence(record, facts)
        assert decision.code == "duplicate_root_key"
        assert decision.event.origin == "admission"


def test_raw_equal_invalid_boolean_keeps_its_initial_rejection_priority():
    definition = _compound_definition()
    records = _records(definition, [_root(score=1), _root(score=True)], [])
    assert records[0].raw_parent_key == records[1].raw_parent_key
    assert records[1].typed_parent_key is None
    facts = _facts(raw_root_count=2)
    assert build.decide_occurrence(records[0], facts).code == "duplicate_root_key"
    invalid = build.decide_occurrence(records[1], facts)
    assert invalid.code == "field_type_invalid" and invalid.event.origin == "source"


def test_global_admission_golden_codes_and_initial_then_new_event_order():
    # Exact final codes of the existing native global-admission regression case.
    definition = _definition()
    source_records = _records(
        definition,
        [_root(), _root("1234567893", None), _root()],
        [_child(), _child("1234567893", None), _child(), _child("absent-parent", None)],
    )
    facts = [
        _facts(raw_root_count=2, typed_root_count=2),
        _facts(first_raw_root=(1, 1)),
        _facts(raw_root_count=2, typed_root_count=2),
        _facts(raw_root_count=2, typed_root_count=2, child_count=2),
        _facts(first_raw_root=(1, 1)),
        _facts(raw_root_count=2, typed_root_count=2, child_count=2),
        _facts(raw_root_count=0, typed_root_count=0, first_raw_root=None),
    ]
    decisions = [
        build.decide_occurrence(source_record, complete)
        for source_record, complete in zip(source_records, facts, strict=True)
    ]
    rejection_targets = sorted(
        (rejection_target for decision in decisions for rejection_target in decision.targets),
        key=lambda target: (target.occurrence, target.event.occurrence),
    )
    resolutions_by_key = {
        rejection_target.occurrence: rejection_target.event
        for rejection_target in build.first_resolutions(iter(rejection_targets))
    }
    assert [resolutions_by_key[build.occurrence_order(source_record)].code for source_record in source_records] == [
        "duplicate_root_key",
        "required_field_null",
        "duplicate_root_key",
        "duplicate_child_key",
        "required_field_null",
        "duplicate_child_key",
        "orphan_child",
    ]
    assert sum(decision.is_candidate_error for decision in decisions) == 1
    initial_items = [
        event for source_record in source_records if (event := build.initial_rejection(source_record)) is not None
    ]
    added_items = [
        decision.event for decision in decisions if decision.event is not None and decision.event.origin == "admission"
    ]
    events = list(build.rejection_ordinals(iter(initial_items), iter(added_items)))
    assert [ordinal for ordinal, _event in events] == list(range(8))
    assert [event.origin for _ordinal, event in events] == ["source"] * 3 + ["admission"] * 5
    assert resolutions_by_key[(1, 0)].occurrence == (
        1,
        0,
    )  # Earlier occurrence wins even though its new ID allocates later.
    assert initial_items[-1].code == "required_field_null" and resolutions_by_key[(2, 3)].code == "orphan_child"


def test_earlier_child_event_suppresses_only_its_exact_first_raw_root():
    document = json.loads(_definition().canonical)
    document["streams"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    child, first_root, duplicate = _records(definition, [_root(), _root()], [_child(amount=None)])
    facts = _facts(raw_root_count=2, typed_root_count=2, first_raw_root=(2, 0))
    child_decision = build.decide_occurrence(child, facts)
    propagation = next(target for target in child_decision.targets if target.occurrence == (2, 0))
    first = build.decide_occurrence(first_root, facts, earlier_child_target=propagation)
    assert first.code == "duplicate_root_key" and first.error_code is None and first.event is None
    assert build.decide_occurrence(duplicate, facts).code == "duplicate_root_key"
    with pytest.raises(ValueError, match="target differs"):
        build.decide_occurrence(duplicate, facts, earlier_child_target=propagation)


@pytest.mark.parametrize(
    "collapse,last,differs,expected",
    [
        (False, None, False, "duplicate_child_key"),
        (True, None, False, "duplicate_child_key"),
        (True, (2, 2), True, "duplicate_child_key"),
        (True, (2, 2), False, None),
    ],
)
def test_child_duplicate_priority_and_complete_last_payload_witness(collapse, last, differs, expected):
    child = _records(_definition(), [], [_child()])[0]
    facts = _facts(child_count=2, collapse_identical=collapse, last_child=last, payload_differs_from_last=differs)
    assert build.primary_code(child, facts) == expected


def test_orphan_precedes_initial_child_error_and_membership_runs_only_after_primary():
    invalid = _records(_definition(), [], [_child(amount=None)])[0]
    orphan = _facts(raw_root_count=0, typed_root_count=0, first_raw_root=None)
    decision = build.decide_occurrence(invalid, orphan)
    assert decision.event == build.RejectionEvent("admission", (2, 0), "orphan_child")
    assert len(decision.targets) == 1
    assert (
        build.decide_occurrence(invalid, _facts(membership_problem="child_membership_missing")).code
        == "required_field_null"
    )
    valid = replace(invalid, rejection_code=None, child_key="synthetic")
    assert (
        build.decide_occurrence(valid, _facts(membership_problem="child_membership_missing")).code
        == "child_membership_missing"
    )


def test_collision_failure_priority_matches_native_stages():
    child = _records(_definition(), [], [_child()])[0]
    facts = _facts(
        raw_key_collision=True,
        child_key_collision=True,
        membership_problem="custom_import_build_structure_mismatch: membership key differs",
    )
    assert build.decide_occurrence(child, facts).fatal_problem.endswith("raw key digest collision")
    facts = replace(facts, raw_key_collision=False)
    assert build.decide_occurrence(child, facts).fatal_problem.endswith("membership key differs")
    assert build.decide_occurrence(child, replace(facts, membership_problem=None)).fatal_problem.endswith(
        "child key digest collision"
    )


@pytest.mark.parametrize(
    "mode,base,expected",
    [
        ("snapshot", True, "absent"),
        ("upsert", True, "retained"),
        ("snapshot", False, "absent"),
        ("upsert", False, "absent"),
    ],
)
def test_untypable_raw_root_never_becomes_a_typed_rejected_base_replacement(mode, base, expected):
    invalid = _records(_compound_definition(), [_root(score=True)], [])[0]
    assert invalid.typed_parent_key is None
    assert build.family_selection(None, None, has_base=base, refresh_mode=mode, complete_scope=True) == expected
    with pytest.raises(ValueError, match="exact typed source root"):
        build.family_selection(
            invalid, None, source_facts=_facts(), has_base=base, refresh_mode=mode, complete_scope=True
        )


def test_typed_source_selection_retains_rejected_base_and_refuses_incomplete_scope():
    root = _records(_definition(), [_root()], [])[0]
    facts = _facts()
    assert (
        build.family_selection(
            root, None, source_facts=facts, has_base=True, refresh_mode="snapshot", complete_scope=True
        )
        == "source"
    )
    rejection = build.RejectionTarget((1, 0), build.RejectionEvent("admission", (2, 0), "required_field_null"))
    assert (
        build.family_selection(
            root, rejection, source_facts=facts, has_base=True, refresh_mode="snapshot", complete_scope=True
        )
        == "retained"
    )
    with pytest.raises(CandidateRejected, match="scope_incomplete"):
        build.family_selection(None, None, has_base=True, refresh_mode="snapshot", complete_scope=False)
    with pytest.raises(ValueError, match="complete resolution"):
        build.family_selection(
            root,
            None,
            source_facts=replace(facts, typed_root_count=2),
            has_base=True,
            refresh_mode="snapshot",
            complete_scope=True,
        )


def test_unknown_or_unordered_facts_and_events_are_refused():
    with pytest.raises(ValueError, match="incomplete"):
        _facts(typed_root_count=None)
    first = build.RejectionEvent("admission", (1, 0), "duplicate_root_key")
    last = build.RejectionEvent("admission", (2, 0), "duplicate_child_key")
    with pytest.raises(ValueError, match="not ordered"):
        list(build.rejection_ordinals((), (last, first)))
    with pytest.raises(ValueError, match="phase differs"):
        list(build.rejection_ordinals((first,), ()))
    with pytest.raises(ValueError, match="not ordered"):
        list(build.first_resolutions((build.RejectionTarget((1, 0), last), build.RejectionTarget((1, 0), first))))


def test_missing_child_count_and_last_payload_witness_never_default_to_acceptance():
    child = _records(_definition(), [], [_child()])[0]
    with pytest.raises(ValueError, match="child count is incomplete"):
        build.primary_code(child, _facts(child_count=0))
    with pytest.raises(ValueError, match="payload witness is incomplete"):
        build.primary_code(
            child, _facts(child_count=2, collapse_identical=True, last_child=(2, 2), payload_differs_from_last=None)
        )
