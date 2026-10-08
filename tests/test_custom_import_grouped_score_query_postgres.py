# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared derived-score definition for unit and admitted native regressions."""

from process.custom_import.definition import CustomImportDefinition
from tests import custom_import_grouped_child_support as fixture


def _definition():
    document = fixture.definition_document()
    document["schema"]["root"]["fields"].append(
        {"id": "weight", "slot": 12, "type": "integer", "nullable": True, "projection_slot": 9}
    )
    document["query"]["root_fields"].append("weight")
    document["query"]["sortable_fields"].append("weight")
    declarations = []
    for field_id, source_field, weight, collection in (
        ("root_cost", "score", "weight", None),
        ("root_quality", "weight", None, None),
        ("child_cost", "amount", "quality", "rates"),
        ("child_quality", "quality", None, "rates"),
    ):
        expression_map = {
            "type": "group_weighted_mean" if weight else "preferred_non_null",
            "scope": "child" if collection else "root",
        }
        expression_map.update(
            {"value_field": source_field, "weight_field": weight}
            if weight
            else {"field_id": source_field, "group_values": ["segment_a", "segment_b"]}
        )
        declaration_map = {"id": field_id, "expression": expression_map}
        if collection:
            declaration_map.update(collection=collection, complete_child_key=["service_code"])
        declarations.append(declaration_map)
    document["query"]["derived_fields"] = declarations
    document["query"]["sortable_fields"] += [field["id"] for field in declarations]
    return CustomImportDefinition.from_mapping(document)
