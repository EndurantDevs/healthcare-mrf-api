# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable numeric query reducers without changing physical source fields."""

from __future__ import annotations

import copy

import pytest
from sqlalchemy import literal

from process.custom_import.definition import CustomImportDefinition, DefinitionError
from process.custom_import.derived_query import MAX_DERIVED_QUERY_FIELDS, reduce_expression
from tests import custom_import_grouped_child_support as fixture


def definition_document():
    document = fixture.definition_document()
    document["schema"]["root"]["fields"].append(
        {"id": "weight", "slot": 12, "type": "integer", "nullable": True, "projection_slot": 9}
    )
    document["schema"]["children"][0]["fields"].append(
        {"id": "quantity", "slot": 13, "type": "integer", "nullable": True, "projection_slot": 10}
    )
    document["query"]["root_fields"].append("weight")
    document["query"]["child"]["fields"].append("quantity")
    return document


def derived_document():
    document = definition_document()
    document["query"]["derived_fields"] = [
        {
            "id": "combined_score",
            "expression": {
                "type": "group_weighted_mean",
                "scope": "root",
                "value_field": "score",
                "weight_field": "weight",
            },
        },
        {
            "id": "combined_quality",
            "collection": "rates",
            "complete_child_key": ["service_code"],
            "expression": {
                "type": "group_weighted_mean",
                "scope": "child",
                "value_field": "quality",
                "weight_field": "quantity",
                "group_values": ["segment_a", "segment_b"],
            },
        },
        {
            "id": "merged_cost",
            "collection": "rates",
            "complete_child_key": ["service_code"],
            "expression": {
                "type": "preferred_non_null",
                "scope": "child",
                "field_id": "amount",
                "group_values": ["segment_a", "segment_b"],
            },
        },
    ]
    document["query"]["sortable_fields"] += ["combined_score", "combined_quality", "merged_cost"]
    document["query"]["aliases"]["combined_alias"] = "combined_score"
    return document


def definition():
    return CustomImportDefinition.from_mapping(derived_document())


def test_output_bindings_are_revision_owned_metadata_without_changing_scalar_sources():
    document = derived_document()
    document["query"]["derived_fields"][0]["output_path"] = ["weighted", "score"]
    document["query"]["derived_fields"][1]["output_path"] = ["rates", {"each": "rates"}, "quality"]
    parsed = CustomImportDefinition.from_mapping(document)
    assert parsed.query.derived_by_id["combined_score"].output_path == ("weighted", "score")
    assert parsed.query.derived_by_id["combined_quality"].output_path == ("rates", ("each", "rates"), "quality")
    assert parsed.schema_digest == definition().schema_digest
    document["revision"]["definition"] += 1
    document["query"]["derived_fields"][0]["output_path"] = ["different", "score"]
    changed = CustomImportDefinition.from_mapping(document, previous=parsed)
    assert changed.query.derived_by_id["combined_score"].output_path == ("different", "score")
    assert parsed.query.derived_by_id["combined_score"].output_path == ("weighted", "score")


@pytest.mark.parametrize(
    "output_path",
    [None, [], ["a"] * 17, [0], ["\x00"], ["a" * 1025], [{"each": "other"}], [{"each": "rates", "extra": True}]],
)
def test_output_bindings_reject_malformed_or_wrong_scope_steps(output_path):
    document = derived_document()
    document["query"]["derived_fields"][1]["output_path"] = output_path
    with pytest.raises(DefinitionError):
        CustomImportDefinition.from_mapping(document)


def test_filtering_whitelist_distinguishes_legacy_from_explicit_empty_and_uses_canonical_ids():
    assert definition().query.filterable_fields is None
    document = derived_document()
    document["query"]["filterable_fields"] = []
    assert CustomImportDefinition.from_mapping(document).query.filterable_fields == ()
    document["query"]["filterable_fields"] = ["combined_score", "score"]
    assert CustomImportDefinition.from_mapping(document).query.filterable_fields == ("combined_score", "score")
    for invalid in [None, ["combined_alias"], ["absent"], ["score", "score"]]:
        document["query"]["filterable_fields"] = invalid
        with pytest.raises(DefinitionError):
            CustomImportDefinition.from_mapping(document)


def test_derived_query_changes_only_definition_identity_and_keeps_stored_ids():
    original = CustomImportDefinition.from_mapping(definition_document())
    declared = definition()
    assert declared.schema_digest == original.schema_digest
    assert declared.digest != original.digest
    assert declared.fields == original.fields
    assert declared.query.resolve_field_id("score") == "score"
    assert declared.query.resolve_field_id("combined_alias") == "combined_score"
    assert declared.query.resolve_field_id("merged_cost") == "merged_cost"
    assert "combined_score" not in declared.fields_by_id
    assert declared.query.derived_by_id["combined_score"].group_values == ("segment_a", "segment_b")
    merged = declared.query.derived_by_id["merged_cost"]
    assert (merged.value_type, merged.collection, merged.child_key) == ("decimal", "rates", ("service_code",))
    assert merged.nullable


def test_weighted_query_can_use_a_configured_family_subset():
    document = derived_document()
    document["query"]["derived_fields"][0]["expression"]["group_values"] = ["segment_b"]
    assert CustomImportDefinition.from_mapping(document).query.derived_by_id["combined_score"].group_values == (
        "segment_b",
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda doc: doc["query"].pop("entity_selection"),
        lambda doc: doc["query"].update(derived_fields={}),
        lambda doc: doc["query"]["derived_fields"].append(copy.deepcopy(doc["query"]["derived_fields"][0])),
        lambda doc: doc["query"]["derived_fields"][0].update(id="score"),
        lambda doc: doc["query"]["derived_fields"][0].update(id=[]),
        lambda doc: doc["query"]["derived_fields"][0].update(sql="SELECT 1"),
        lambda doc: doc["query"]["derived_fields"][0].pop("expression"),
        lambda doc: doc["query"]["derived_fields"][0].update(collection="rates"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(type=[]),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(type="sql"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(scope=[]),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(scope="associated"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(value_field="quality"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(value_field="npi"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(value_field="combined_score"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(weight_field="absent"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(weight_field=[]),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].pop("weight_field"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(field_id="score"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(rounding="half_up"),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(group_values=[]),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(group_values=["segment_a", "segment_a"]),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(group_values=["weighted"]),
        lambda doc: doc["query"]["derived_fields"][0]["expression"].update(group_values=[{}]),
        lambda doc: doc["query"]["derived_fields"][1].update(collection="other"),
        lambda doc: doc["query"]["derived_fields"][1].update(complete_child_key=[]),
        lambda doc: doc["query"]["derived_fields"][1].update(complete_child_key=["quality"]),
        lambda doc: doc["query"]["derived_fields"][1].pop("complete_child_key"),
        lambda doc: doc["query"]["child"]["fields"].remove("service_code"),
        lambda doc: doc["query"]["derived_fields"][2]["expression"].pop("group_values"),
        lambda doc: doc["query"]["aliases"].update(merged_cost="amount"),
        lambda doc: doc["selection_profiles"][0].update(context_dimensions=["combined_score"]),
    ],
)
def test_derived_query_rejects_unsafe_or_ambiguous_declarations(mutate):
    document = derived_document()
    mutate(document)
    with pytest.raises(DefinitionError):
        CustomImportDefinition.from_mapping(document)


def test_derived_query_declarations_are_finitely_bounded():
    document = derived_document()
    document["query"]["derived_fields"] = [
        {**copy.deepcopy(document["query"]["derived_fields"][0]), "id": f"metric_{index}"}
        for index in range(MAX_DERIVED_QUERY_FIELDS + 1)
    ]
    with pytest.raises(DefinitionError, match="field count"):
        CustomImportDefinition.from_mapping(document)


@pytest.mark.parametrize("rebind", [False, True])
def test_metric_revision_keeps_existing_reader_immutable(rebind):
    original = definition()
    document = derived_document()
    document["revision"]["definition"] += 1
    if rebind:
        document["query"]["derived_fields"][0]["expression"]["group_values"] = ["segment_b"]
    else:
        document["query"]["derived_fields"].pop(1)
        document["query"]["sortable_fields"].remove("combined_quality")
    changed = CustomImportDefinition.from_mapping(document, previous=original)
    assert original.query.derived_by_id["combined_score"].group_values == ("segment_a", "segment_b")
    assert "combined_quality" in original.query.derived_by_id
    assert changed.digest != original.digest


def test_metric_evolution_preserves_published_alias_mapping_and_explicit_default_order_grant():
    original = definition()
    document = derived_document()
    document["revision"]["definition"] += 1
    document["query"]["aliases"].pop("combined_alias")
    with pytest.raises(DefinitionError, match="published query aliases"):
        CustomImportDefinition.from_mapping(document, previous=original)
    document = derived_document()
    document["query"]["filterable_fields"] = []
    document["query"]["order"] = [{"field": "combined_score", "direction": "asc", "nulls": "last"}]
    document["query"]["sortable_fields"].remove("combined_score")
    with pytest.raises(DefinitionError, match="sortable fields"):
        CustomImportDefinition.from_mapping(document)


def test_declared_grouped_query_supports_four_ordered_metrics():
    document = derived_document()
    document["query"]["order"] = [
        {"field": field, "direction": "desc", "nulls": "last"}
        for field in ("score", "combined_score", "combined_quality", "merged_cost")
    ]
    assert len(CustomImportDefinition.from_mapping(document).query.order_terms) == 4


def test_weighted_expression_rejects_missing_weight_mapping():
    field = definition().query.derived_by_id["combined_score"]
    with pytest.raises(ValueError, match="requires source weights"):
        reduce_expression(field, {"segment_a": literal(1)})
