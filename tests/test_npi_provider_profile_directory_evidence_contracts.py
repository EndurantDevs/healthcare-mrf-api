# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from api.endpoint import npi as npi_module


def _canonical_contact(_contact):
    return {
        "phone_number": "5550100",
        "phone_extension": "9",
        "fax_number_digits": "5550199",
        "fax_extension": "8",
    }


def _directory_address_maps():
    primary_map = {
        "type": "primary",
        "address_key": "address-a",
        "first_line": "100 Main Street",
        "telephone_number": "555-0100 ext 9",
        "address_sources": ["nppes"],
        "source_record_ids": ["record-a"],
        "source_count": 1,
    }
    duplicate_map = {
        "type": "secondary",
        "address_key": "ADDRESS-A",
        "fax_number": "555-0199 ext 8",
        "formatted_address": "100 Main Street, Example City",
        "address_sources": ["nppes", "provider_directory_fhir"],
        "source_record_ids": ["record-a", "record-b"],
        "source_count": 2,
    }
    separate_site_map = {
        "type": "site",
        "address_key": "address-a",
        npi_module.PUBLIC_ADDRESS_SITE_KEY: "site-b",
        "first_line": "Suite B",
    }
    unkeyed_map = {"type": "mail", "first_line": "PO Box 7"}
    return [unkeyed_map, duplicate_map, "ignored", separate_site_map, primary_map]


def test_provider_directory_address_merge_preserves_best_row_and_all_evidence(
    monkeypatch,
):
    monkeypatch.setattr(
        npi_module,
        "canonicalize_contact_one",
        _canonical_contact,
    )
    merged_addresses = npi_module._dedupe_addresses_by_key(
        _directory_address_maps()
    )

    assert merged_addresses[0]["type"] == "primary"
    assert merged_addresses[0]["address_sources"] == [
        "nppes",
        "provider_directory_fhir",
    ]
    assert merged_addresses[0]["source_record_ids"] == ["record-a", "record-b"]
    assert merged_addresses[0]["source_count"] == 2
    assert merged_addresses[0]["independent_source_count"] == 2
    assert merged_addresses[0]["multi_source_confirmed"] is True
    assert (
        merged_addresses[0]["formatted_address"]
        == "100 Main Street, Example City"
    )
    assert merged_addresses[0]["phone_number"] == "5550100"
    assert merged_addresses[0]["fax_number_digits"] == "5550199"
    assert [address["type"] for address in merged_addresses] == ["primary", "mail"]


def test_provider_directory_address_helpers_handle_empty_and_duplicate_values():
    class TextValue:
        def __str__(self) -> str:
            return "same"

    assert npi_module._merge_unique_list_values(
        [None, "", {"code": "A"}],
        [{"code": "A"}, "second"],
    ) == [{"code": "A"}, "second"]
    assert npi_module._merge_unique_list_values(
        ["1", 1, True, "same"],
        [1, "1", True, TextValue()],
    ) == ["1", 1, True, "same"]
    assert not npi_module._has_contact_value(None)
    assert not npi_module._has_contact_value("  ")
    assert npi_module._has_contact_value(0)
    untouched_map = {}
    assert (
        npi_module._add_canonical_contact_fields_to_address(untouched_map)
        is untouched_map
    )


def test_provider_directory_endpoint_details_preserve_typed_public_fields():
    details = npi_module._provider_directory_endpoint_details(
        """[
          {
            "source_id": "source-a",
            "resource_id": "endpoint-a",
            "status": "active",
            "name": "Directory endpoint",
            "managing_organization_ref": "Organization/org-a",
            "contact": [{"system": "phone", "value": "555-0100"}],
            "payload_type_codes": [{"code": "any"}],
            "payload_mime_types": ["application/fhir+json"],
            "connection_type_system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
            "connection_type_code": "hl7-fhir-rest",
            "connection_type_display": "HL7 FHIR",
            "period_start": "2026-01-01",
            "period_end": "2026-12-31",
            "address": "https://example.test/fhir",
            "fhir_fetch_mode": "read"
          },
          {
            "source_id": "source-a",
            "resource_id": "endpoint-minimal"
          }
        ]"""
    )

    assert npi_module._provider_directory_endpoint_details("not-json") == []
    assert details[0]["connection_type"] == {
        "system": "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
        "code": "hl7-fhir-rest",
        "display": "HL7 FHIR",
    }
    assert details[0]["period"] == {
        "start": "2026-01-01",
        "end": "2026-12-31",
    }
    assert details[0]["address"] == "https://example.test/fhir"
    assert details[0]["fhir_provenance"]["fetch_mode"] == "read"
    assert details[1] == {
        "resource_type": "Endpoint",
        "source_id": "source-a",
        "resource_id": "endpoint-minimal",
    }
