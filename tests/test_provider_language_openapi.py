# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical provider-language OpenAPI contract."""

from pathlib import Path

import yaml


def test_language_openapi_documents_primary_and_fallback_contracts():
    openapi_by_field = yaml.safe_load(
        (Path(__file__).parents[1] / "doc" / "openapi.yaml").read_text()
    )
    operation_by_field = openapi_by_field["paths"]["/npi/id/{npi}/profile"][
        "get"
    ]
    schemas_by_name = openapi_by_field["components"]["schemas"]

    assert "`category=languages` is the canonical provider-level language list" in (
        operation_by_field["description"]
    )
    operation_description = operation_by_field["description"].replace("\n", " ")
    assert "never that the provider speaks only English" in operation_description
    language_value_by_field = schemas_by_name["ProviderLanguageValue"]
    code_by_field = language_value_by_field["properties"]["codes"]["items"][
        "properties"
    ]
    assert code_by_field["system"]["enum"] == ["urn:ietf:bcp:47"]
    assert language_value_by_field["properties"]["normalization_warning"][
        "enum"
    ] == [
        "source_code_display_mismatch",
        "multiple_source_language_codes",
    ]
    assert language_value_by_field["oneOf"] == [
        {"required": ["codes"]},
        {"required": ["text"]},
    ]
    assert language_value_by_field["properties"]["text"]["minLength"] == 1
    fact_properties_by_name = schemas_by_name["ProviderProfileFact"]["properties"]
    assert {
        "source_record_id",
        "source_record_ids",
        "source_ids",
        "source_count",
        "independent_source_count",
        "assertion_count",
    } <= fact_properties_by_name.keys()
    assert fact_properties_by_name["source_count"]["minimum"] == 1
    assert fact_properties_by_name["independent_source_count"]["minimum"] == 1
    assert "logical Provider Directory feeds" in (
        fact_properties_by_name["source_count"]["description"]
    )
    assert "independent FHIR endpoints" in (
        fact_properties_by_name["independent_source_count"]["description"]
    )
    plans_operation_by_field = openapi_by_field["paths"][
        "/npi/plans_by_npi/{npi}"
    ]["get"]
    assert "not normalized" in plans_operation_by_field["description"]
    assert "provider facts" in plans_operation_by_field["description"]
