# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from pathlib import Path

import yaml


def test_procedure_pricing_documents_postal_box_mailing_semantics():
    specification = yaml.safe_load(Path("doc/openapi.yaml").read_text())
    schemas = specification["components"]["schemas"]
    provider_records = (
        "PricingProcedureProviderRecord",
        "PricingProviderProcedureRecord",
    )

    for record_name in provider_records:
        provider_properties = schemas[record_name]["properties"]
        assert provider_properties["address_kind"]["enum"] == [
            "physical",
            "postal_box",
            "unknown",
        ]
        assert provider_properties["address"] == {
            "$ref": "#/components/schemas/PtgProviderAddress"
        }
        assert provider_properties["address_verification"] == {
            "$ref": "#/components/schemas/PtgAddressVerification"
        }
    exact_npi_response = specification["paths"][
        "/pricing/providers/{npi}/procedures"
    ]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert exact_npi_response == {
        "$ref": "#/components/schemas/PricingProviderProcedureListResponse"
    }
    list_item = schemas["PricingProviderProcedureListResponse"]["properties"][
        "items"
    ]["items"]
    assert list_item == {
        "$ref": "#/components/schemas/PricingProviderProcedureRecord"
    }
    address_description = schemas["PtgProviderAddress"]["description"]
    assert "mailing" in address_description
    assert "facility" in address_description
    verification = schemas["PtgAddressVerification"]["properties"]
    assert "postal_box_provider_address" in verification[
        "address_evidence_level"
    ]["description"]
    assert "not_applicable_postal_box" in verification[
        "address_network_binding"
    ]["description"]
