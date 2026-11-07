# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original OUTPUT candidate values over fresh and exact sparse family inputs."""

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import runner_codec, runner_graph
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_shard_selection import family_candidates
from process.custom_import.runner_types import CandidateRunnerError, PublishedCandidateChild, PublishedCandidateFamily
from tests.test_custom_import_family_block_codec import _sparse
from tests.test_custom_import_family_shard_output import prepared


def _original(layout, family, children):
    definition = layout.definition

    def values(record, fields):
        return (
            record.values_by_field
            if record.sparse is None
            else runner_codec.payload_values(fields, record.sparse.payload_text, label="winner original payload")
        )

    published = PublishedCandidateFamily(
        family.root_record_id,
        family.root.revision_id,
        family.family_revision_id,
        family.entity_binding_id,
        family.expected_family_sha256,
        values(family.root, definition.root_fields),
        tuple(
            PublishedCandidateChild(
                "rates",
                child.revision_id,
                record_parts(definition, "rates", child, retained=family.retained)[1][0],
                values(child, runner_codec.fields_by_collection(definition)["rates"]),
            )
            for child in children
        ),
    )
    return runner_graph.winner_candidates(
        SimpleNamespace(definition=definition),
        SimpleNamespace(child_collection_slots=layout.collection_slots),
        (published,),
    )


@pytest.mark.parametrize("sparse", [False, True])
def test_family_contexts_match_original_payload_candidates_with_native_slots(sparse):
    layout, family, child_records, _keys = prepared()
    if sparse:
        root = _sparse(family.root, layout.definition, None)
        child_records = tuple(_sparse(child, layout.definition, "rates") for child in child_records)
        family = replace(
            family, root=root, children_by_collection={"rates": lambda: iter(child_records)}, retained=True
        )
    candidate_rows = tuple(family_candidates(layout, family))
    assert candidate_rows == _original(layout, family, child_records)
    assert candidate_rows[0].context_collection_slot == 0
    assert all(candidate.context_collection_slot == 7 for candidate in candidate_rows[1:])
    assert candidate_rows[0].values_by_field["display_name"] == "Synthetic Clinic"
    assert family.root.scalars["display_name"].string_value == "Independent projection"


def test_root_profile_does_not_read_unneeded_child_contexts():
    layout, family, _children, _keys = prepared()
    document = json.loads(layout.definition.canonical)
    document["selection_profiles"] = [
        {"id": "named", "selection": [{"field": "display_name", "direction": "asc", "nulls": "last"}]}
    ]
    layout = FamilyBlockLayout(CustomImportDefinition.from_mapping(document), layout.collection_slots)

    def unexpected():
        raise AssertionError("root-only OUTPUT must not open child candidate input")

    family = replace(family, children_by_collection={"rates": unexpected})
    (candidate,) = family_candidates(layout, family)
    assert candidate.context_collection_slot == 0


@pytest.mark.parametrize("payload", ["opaque original payload", " "])
def test_sparse_payload_failure_keeps_original_output_refusal(payload):
    layout, family, _children, _keys = prepared()
    payload = (
        payload
        if payload.strip()
        else payload + runner_codec.record_payload(layout.definition.root_fields, family.root.values_by_field)
    )
    root = _sparse(family.root, layout.definition, None, payload=payload)
    family = replace(family, root=root, retained=True)
    with pytest.raises(CandidateRunnerError):
        tuple(family_candidates(layout, family))


def test_interrupted_child_candidates_close_original_iterator():
    layout, family, children, _keys = prepared()
    closed, calls = [], []

    def records():
        try:
            yield from children
        finally:
            closed.append(True)

    failure = InterruptedError("synthetic original candidate stop")

    def check():
        calls.append(True)
        if len(calls) == 3:
            raise failure

    family = replace(family, children_by_collection={"rates": records})
    with pytest.raises(InterruptedError) as caught:
        tuple(family_candidates(layout, family, check=check))
    assert caught.value is failure and closed == [True]
