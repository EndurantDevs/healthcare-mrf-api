"""Lightweight factories used across API and importer tests."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict


def issuer_payload(**overrides: Any) -> Dict[str, Any]:
    payload = {
        "state": "CA",
        "issuer_id": 1001,
        "issuer_name": "Example Issuer",
        "issuer_marketing_name": "Example Health",
        "mrf_url": "https://example.com/mrf.json",
        "data_contact_email": "contact@example.com",
    }
    payload.update(overrides)
    return payload


def plan_payload(**overrides: Any) -> Dict[str, Any]:
    payload = {
        "plan_id": "12345678901234",
        "year": 2024,
        "issuer_id": 1001,
        "state": "CA",
        "plan_id_type": "HIOS",
        "marketing_name": "Example Silver Plan",
        "summary_url": "https://example.com/summary.pdf",
        "marketing_url": "https://example.com",
        "formulary_url": "https://example.com/formulary.pdf",
        "plan_contact": "800-555-0100",
        "network": [],
        "benefits": [],
        "last_updated_on": dt.datetime.utcnow(),
        "checksum": 123456,
    }
    payload.update(overrides)
    return payload


def npi_payload(**overrides: Any) -> Dict[str, Any]:
    payload = {
        "npi": 1518379601,
        "entity_type_code": 1,
        "provider_first_name": "Jane",
        "provider_last_name": "Doe",
        "provider_organization_name": None,
        "provider_enumeration_date": dt.date(2020, 1, 1),
        "last_update_date": dt.date(2024, 1, 1),
    }
    payload.update(overrides)
    return payload


def npi_address_payload(**overrides: Any) -> Dict[str, Any]:
    payload = {
        "npi": 1518379601,
        "type": "primary",
        "checksum": 123456,
        "first_line": "123 Main St",
        "second_line": None,
        "city_name": "San Francisco",
        "state_name": "CA",
        "postal_code": "94105",
        "country_code": "US",
        "telephone_number": "800-555-0100",
        "fax_number": None,
        "formatted_address": "123 Main St, San Francisco, CA 94105",
        "lat": None,
        "long": None,
        "date_added": dt.date.today(),
    }
    payload.update(overrides)
    return payload
