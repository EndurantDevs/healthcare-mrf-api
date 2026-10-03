# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic complete-key children in the existing grouped family fixture."""

from dataclasses import replace

from process.custom_import.definition import CustomImportDefinition
from tests import custom_import_grouped_support as grouped


def definition_document():
    document = grouped.definition_document()
    document["schema"]["children"][0]["fields"].append(
        {"id": "quality", "slot": 11, "type": "decimal", "nullable": True, "projection_slot": 8}
    )
    document["query"]["child"] = {"collection": "rates", "fields": ["service_code", "amount", "quality"]}
    document["query"]["sortable_fields"] += ["amount", "quality"]
    document["query"]["aliases"].update({"child_alias": "service_code", "cost_alias": "amount"})
    return document


def definition():
    return CustomImportDefinition.from_mapping(definition_document())


def child_descriptor():
    return {
        "contract": "custom-import/grouped-child-query/v1",
        "collection": "rates",
        "key_field_id": "service_code",
    }


def query(**changes):
    return grouped.query(grouped_child_query=child_descriptor(), **changes)


def context():
    return replace(grouped.context(), definition=definition())
