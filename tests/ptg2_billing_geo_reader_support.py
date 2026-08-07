# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared synthetic fixtures for exact billing geo-reader tests."""

from __future__ import annotations

import json

from api import ptg2_billing_geo_reader as geo_reader
from api.ptg2_billing_exact_reader import BillingRateOccurrenceWitness
from api.ptg2_types import PTG2ServingTables

GROUP_A = "aa" * 16
GROUP_B = "bb" * 16
NPI_A = 1000000004
NPI_B = 1000000012
NPI_C = 1234567893


def _tables() -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id="ptg2:synthetic",
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=2,
    )


def _rate(
    *,
    group_ref: str = GROUP_A,
    source_key: int = 0,
    provider_set_key: int = 3,
    price_key: int = 10,
    occurrence_ordinal: int = 0,
) -> BillingRateOccurrenceWitness:
    return BillingRateOccurrenceWitness(
        snapshot_key=17,
        code_key=5,
        source_key=source_key,
        source_record_ordinal=source_key,
        provider_group_ref=group_ref,
        provider_set_key=provider_set_key,
        price_key=price_key,
        occurrence_ordinal=occurrence_ordinal,
    )


def _member(npi: int) -> str:
    return geo_reader.ptg2_serving._ptg2_npi_member_id(npi)


def _provider_rate(
    *,
    npi: int = NPI_A,
    group_ref: str = GROUP_A,
    source_key: int = 0,
    provider_set_key: int = 3,
    price_key: int = 10,
    occurrence_ordinal: int = 0,
) -> geo_reader.BillingProviderRateWitness:
    return geo_reader.BillingProviderRateWitness(
        snapshot_key=17,
        code_key=5,
        source_key=source_key,
        source_record_ordinal=source_key,
        provider_group_ref=group_ref,
        provider_set_key=provider_set_key,
        price_key=price_key,
        occurrence_ordinal=occurrence_ordinal,
        npi=npi,
    )


def _location_row(
    npi: int,
    *,
    distance: float | None = None,
    address_key: str = "00000000-0000-0000-0000-000000000001",
) -> dict[str, object]:
    evidence_level = "nppes_registry_address"
    location_key = f"{npi:064x}"
    address_payload_by_field = {
        "first_line": "10 Example Ave",
        "second_line": "Suite 2",
        "city": "EXAMPLE",
        "state": "WV",
        "postal_code": "25000",
        "country_code": "US",
        "lat": 38.0,
        "long": -82.0,
        "address_key": address_key,
        "address_site_key": "00000000-0000-0000-0000-000000000002",
        "location_key": location_key,
        "source_record_ids": ["must-not-leak"],
        "address_sources": ["internal"],
        "geo_evidence_level": evidence_level,
        "address_provenance": [
            {
                "dataset_id": "cms_nppes_registry",
                "source_id": 1,
                "source_record_id": f"synthetic:{npi}",
                "record_version_id": "20260101",
                "record_version_ids": ["20260101"],
                "retrieved_at": "2026-01-01T00:00:00+00:00",
            }
        ],
    }
    return {
        "npi": npi,
        "location_hash": f"entity_address_unified:{location_key}",
        "distance_miles": distance,
        "type": "practice",
        "address_payload": json.dumps(address_payload_by_field),
    }


def _replace_location_payload(
    row: dict[str, object],
    **updates: object,
) -> dict[str, object]:
    updated_row_by_field = dict(row)
    payload = json.loads(str(updated_row_by_field["address_payload"]))
    for key, value in updates.items():
        if value is None:
            payload.pop(key, None)
        else:
            payload[key] = value
    updated_row_by_field["address_payload"] = json.dumps(payload)
    return updated_row_by_field
