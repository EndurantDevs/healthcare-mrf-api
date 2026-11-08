# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic root-grain definition shared by grouped-read proofs."""

import json
from pathlib import Path

from process.custom_import.definition import CustomImportDefinition
from process.custom_import.read_core import NpiEntityRelationQuery, PinnedReadTarget, _ReadContext

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


def selection_document():
    return {
        "contract": "custom-import/grouped-entity-selection/v1",
        "field": "period",
        "default": "max_per_entity",
        "family_dimension": {"field": "segment", "values": ["segment_a", "segment_b"]},
        "by_value_profile": "families_by_period",
        "default_profile": "latest_period",
    }


def definition_document():
    document = json.loads(_FIXTURE.read_text())
    root = document["schema"]["root"]
    root["logical_key"] = ["npi", "period", "segment"]
    root["fields"].extend(
        [
            {"id": "period", "slot": 6, "type": "integer", "nullable": False, "projection_slot": 5},
            {"id": "segment", "slot": 7, "type": "string", "nullable": False, "projection_slot": 6},
            {"id": "score", "slot": 8, "type": "decimal", "nullable": True, "projection_slot": 7},
        ]
    )
    child = document["schema"]["children"][0]
    child["parent_key"].extend(
        [
            {"child": "rate_period", "root": "period"},
            {"child": "rate_segment", "root": "segment"},
        ]
    )
    child["fields"].extend(
        [
            {"id": "rate_period", "slot": 9, "type": "integer", "nullable": False},
            {"id": "rate_segment", "slot": 10, "type": "string", "nullable": False},
        ]
    )
    document["query"] = {
        "root_fields": ["npi", "period", "segment", "score"],
        "order": [],
        "sortable_fields": ["score"],
        "aliases": {"year_alias": "period", "panel_alias": "segment", "metric_alias": "score"},
        "entity_selection": selection_document(),
    }
    document["selection_profiles"] = [
        {"id": "families_by_period", "context_dimensions": ["period", "segment"], "selection": []},
        {
            "id": "latest_period",
            "context_dimensions": [],
            "selection": [
                {"field": "period", "direction": "desc", "nulls": "last"},
            ],
        },
    ]
    return document


def definition():
    return CustomImportDefinition.from_mapping(definition_document())


def context():
    return _ReadContext(
        PinnedReadTarget(1, 2, 3, 4, "families_by_period"),
        definition(),
        1,
        0,
        {"rates": 1},
        {1: "rates"},
        default_profile_slot=2,
    )


def query(**changes):
    return NpiEntityRelationQuery(grouped_entity_selection=selection_document(), **changes)
