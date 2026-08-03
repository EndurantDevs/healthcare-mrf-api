# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""OpenAPI contracts for atomic PTG negotiated-rate options."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def _openapi_schemas():
    return yaml.safe_load(OPENAPI_PATH.read_text())["components"]["schemas"]


def test_openapi_exposes_atomic_ptg_rate_options():
    """Keep every negotiated price tied to its opaque serving references."""

    schemas = _openapi_schemas()
    option_schema = schemas["PtgRateOption"]

    assert set(option_schema["required"]) == {
        "provider_set_ref",
        "price_set_ref",
        "rate_pack_ref",
        "prices",
    }
    assert option_schema["properties"]["prices"]["items"] == {
        "$ref": "#/components/schemas/PtgNegotiatedPrice"
    }
    provider_properties = schemas["PricingProcedureProviderRecord"][
        "properties"
    ]
    assert provider_properties["rate_options"]["items"] == {
        "$ref": "#/components/schemas/PtgRateOption"
    }
    for count_field in (
        "rate_option_count",
        "provider_set_count",
        "price_set_count",
        "rate_pack_count",
    ):
        assert provider_properties[count_field] == {
            "type": "integer",
            "minimum": 0,
        }


def test_openapi_exposes_exact_billing_associations():
    schemas = _openapi_schemas()
    option_properties = schemas["PtgRateOption"]["properties"]
    assert option_properties["billing_associations"]["items"] == {
        "$ref": "#/components/schemas/PtgBillingAssociation"
    }
    assert option_properties["billing_association_status"]["enum"] == [
        "resolved",
        "partially_resolved",
        "unresolved",
        "unavailable",
    ]
    association_schema = schemas["PtgBillingAssociation"]
    assert set(association_schema["required"]) == {
        "association_ordinal",
        "tax_identity_status",
    }
    assert association_schema["properties"]["association_ordinal"] == {
        "type": "integer",
        "minimum": 1,
        "description": "One-based position within this rate option only.",
    }
    assert "provider_group_ref" not in association_schema["properties"]
    assert association_schema["properties"]["billing_entity_ref"][
        "pattern"
    ] == "^be1_[A-Za-z0-9_-]{64}$"
    provider_properties = schemas["PricingProcedureProviderRecord"]["properties"]
    assert provider_properties["billing_association_count"]["minimum"] == 0
    for nullable_count in (
        "resolved_billing_entity_count",
        "billing_entity_count",
    ):
        assert provider_properties[nullable_count]["minimum"] == 0
        assert provider_properties[nullable_count]["nullable"] is True
    assert provider_properties["billing_entity_count_status"]["enum"] == [
        "exact",
        "lower_bound",
        "unavailable",
    ]
