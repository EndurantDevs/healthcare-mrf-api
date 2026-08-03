# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""OpenAPI contracts for atomic PTG negotiated-rate options."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def test_openapi_exposes_atomic_ptg_rate_options():
    """Keep every negotiated price tied to its opaque serving references."""

    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    schemas = spec["components"]["schemas"]
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
