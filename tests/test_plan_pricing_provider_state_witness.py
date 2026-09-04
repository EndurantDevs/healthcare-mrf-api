# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen provider-state witness contracts for pricing projection v4."""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock

import orjson
import pytest

from api import plan_pricing_projection as projection
from api import plan_pricing_projection_source as projection_source
from api import plan_pricing_projection_v3_provider_cells as provider_cells
from api import plan_pricing_state_scan_hydration as hydration
from api import ptg2_serving as serving
from api.plan_pricing_projection_v3_types import _BuildState

from .test_plan_pricing_projection import PROJECTION_ID, _Session


NPI = 1000000001


def _state_address(
    zip5: str,
    first_line: str,
) -> bytes:
    location_key = f"location-{zip5}"
    return orjson.dumps(
        {
            "npi": NPI,
            "type": "practice",
            "first_line": first_line,
            "second_line": None,
            "city": "Ann Arbor",
            "state": "MI",
            "postal_code": zip5,
            "country_code": "US",
            "address_key": f"address-{zip5}",
            "location_key": location_key,
            "address_precision": "street",
            "address_sources": ["nppes"],
            "source_record_ids": [f"record-{zip5}"],
            "source_count": 1,
            "multi_source_confirmed": False,
            "source_mask": 1,
            "address_source_mask": 1,
            "location_confidence_id": 1,
            "geo_evidence_level": "nppes_registry_address",
            "address_provenance": [
                {
                    "source_id": 1,
                    "dataset_id": "synthetic",
                    "source_record_id": f"record-{zip5}",
                    "record_version_id": "version-1",
                    "retrieved_at": "2026-01-01T00:00:00Z",
                }
            ],
            "lat": 42.28,
            "long": -83.74,
        }
    )


def _state_provider(
    zip5: str,
    rank: int,
    *,
    first_line: str = "1 Frozen Street",
) -> dict:
    return {
        "npi": NPI,
        "provider_name": "Frozen Synthetic Provider",
        "entity_type_code": 1,
        "credential": "MD",
        "provider_sex_code": "U",
        "taxonomy_codes": ["207Q00000X"],
        "specialties": ["Family Medicine"],
        "primary_specialty": "Family Medicine",
        "classifications": ["Family Medicine"],
        "specializations": [],
        "primary_specialization": None,
        "state": "MI",
        "city": "Ann Arbor",
        "zip5": zip5,
        "location_hash": f"entity_address_unified:location-{zip5}",
        "location_source": "entity_address_unified",
        "location_confidence_code": "entity_address_unified",
        "state_address_rank": rank,
        "address_payload": _state_address(zip5, first_line),
    }


@pytest.mark.asyncio
async def test_provider_projection_keeps_each_zip_and_one_state_witness(
    monkeypatch,
) -> None:
    provider_rows = [_state_provider("48104", 1), _state_provider("48105", 2)]
    session = _Session(provider_rows)
    hydrate_provenance = AsyncMock(return_value="available")
    monkeypatch.setattr(
        serving,
        "_hydrate_address_provenance",
        hydrate_provenance,
    )

    providers_by_npi = await projection._projection_provider_rows_for_npis(
        session, [NPI]
    )

    assert [
        provider_by_field["zip5"]
        for provider_by_field in providers_by_npi[NPI]
    ] == ["48104", "48105"]
    provider_sql = session.statements[0][0]
    assert "PARTITION BY addr.npi, addr.projected_zip5" in provider_sql
    assert "PARTITION BY addr.npi, addr.projected_state" in provider_sql
    assert "addr.state_address_rank" in provider_sql
    assert "address_payload" in provider_sql
    hydrate_provenance.assert_awaited_once()
    assert hydrate_provenance.await_args.args[1] == [provider_rows[0]]
    assert hydrate_provenance.await_args.kwargs == {
        "include_response_evidence": True,
        "use_stored_only": True,
    }
    assert session.statements[0][1]["provider_row_limit"] == (
        projection_source.MAX_PROVIDER_ROWS_PER_BATCH + 1
    )


@pytest.mark.asyncio
async def test_provider_state_provenance_hydration_boundaries(monkeypatch) -> None:
    hydrate_provenance = AsyncMock(return_value="unavailable")
    monkeypatch.setattr(serving, "_hydrate_address_provenance", hydrate_provenance)

    await projection_source._hydrate_state_address_provenance(
        object(), [_state_provider("48104", 2)]
    )
    hydrate_provenance.assert_not_awaited()

    with pytest.raises(ValueError, match="provenance is incomplete"):
        await projection_source._hydrate_state_address_provenance(
            object(), [_state_provider("48104", 1)]
        )


def test_v4_provider_state_witness_is_single_bounded_and_digest_bound() -> None:
    providers = [_state_provider("48104", 1), _state_provider("48105", 2)]
    first_state = _BuildState(hashlib.sha256())
    provider_cell_rows = provider_cells._provider_cell_rows(
        PROJECTION_ID,
        first_state,
        [NPI],
        {NPI: providers},
    )

    state_fragments = [
        provider_cell_row["state_fragment"]
        for provider_cell_row in provider_cell_rows
        if provider_cell_row["state_fragment"]
    ]
    witness_by_field = orjson.loads(state_fragments[0])
    assert [
        provider_cell_row["geo_cell"]
        for provider_cell_row in provider_cell_rows
    ] == ["48104", "48105"]
    assert len(state_fragments) == 1
    assert first_state.provider_state_count == 1
    assert first_state.provider_fragment_byte_count == sum(
        len(provider_cell_row["fragment"])
        + len(provider_cell_row["state_fragment"] or b"")
        for provider_cell_row in provider_cell_rows
    )
    assert witness_by_field["version"] == (
        provider_cells.PROVIDER_STATE_FRAGMENT_VERSION
    )
    assert witness_by_field["provider"]["address_payload"]["first_line"] == (
        "1 Frozen Street"
    )

    changed_state = _BuildState(hashlib.sha256())
    provider_cells._provider_cell_rows(
        PROJECTION_ID,
        changed_state,
        [NPI],
        {
            NPI: [
                _state_provider("48104", 1, first_line="2 Mutated Street"),
                _state_provider("48105", 2),
            ]
        },
    )
    assert first_state.content_digest.digest() != changed_state.content_digest.digest()


@pytest.mark.parametrize(
    "evidence_flag",
    ("include_evidence", "include_debug", "include_details"),
)
def test_v4_provider_state_witness_round_trips_nullable_address_identity(
    evidence_flag,
) -> None:
    provider = _state_provider("48104", 1)
    address = orjson.loads(provider["address_payload"])
    address["address_key"] = None
    address["location_confidence_id"] = 0
    provider["address_payload"] = orjson.dumps(address)

    provider_cell_rows = provider_cells._provider_cell_rows(
        PROJECTION_ID,
        _BuildState(hashlib.sha256()),
        [NPI],
        {NPI: [provider]},
    )
    state_fragment = next(
        row["state_fragment"] for row in provider_cell_rows if row["state_fragment"]
    )
    hydrated = hydration._validated_providers_by_npi(
        {NPI: state_fragment},
        {"state": "MI", evidence_flag: "true"},
        serving,
    )[NPI]

    assert hydrated["address_payload"]["address_key"] is None
    assert hydrated["address_payload"]["location_confidence_id"] == 0
    assert hydrated["address_payload"]["geo_evidence_level"] == "nppes_registry_address"
    assert hydrated["address_payload"]["address_provenance"]


def test_v4_provider_state_witness_fails_closed(monkeypatch) -> None:
    incomplete_providers = [_state_provider("48104", 2)]
    with pytest.raises(ValueError, match="witness is incomplete"):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            _BuildState(hashlib.sha256()),
            [NPI],
            {NPI: incomplete_providers},
        )

    with pytest.raises(ValueError, match="witness is duplicated"):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            _BuildState(hashlib.sha256()),
            [NPI],
            {NPI: [_state_provider("48104", 1), _state_provider("48105", 1)]},
        )

    monkeypatch.setattr(provider_cells, "MAX_PROVIDER_STATE_FRAGMENT_BYTES", 2)
    with pytest.raises(ValueError, match="fragment bound exceeded"):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            _BuildState(hashlib.sha256()),
            [NPI],
            {NPI: [_state_provider("48104", 1)]},
        )


@pytest.mark.parametrize(
    ("provider_update", "message"),
    [
        ({"address_payload": b"[]"}, "address is invalid"),
        ({"state": "Michigan"}, "code is invalid"),
        ({"state": ""}, "code is missing"),
        ({"state_address_rank": 0}, "rank is invalid"),
    ],
)
def test_v4_provider_state_witness_rejects_malformed_fields(
    provider_update,
    message,
) -> None:
    provider = _state_provider("48104", 1)
    provider.update(provider_update)

    with pytest.raises(ValueError, match=message):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            _BuildState(hashlib.sha256()),
            [NPI],
            {NPI: [provider]},
        )


@pytest.mark.parametrize(
    ("field", "value", "in_provenance"),
    [
        ("source_id", True, True),
        ("source_count", True, False),
        ("lat", None, False),
        ("lat", True, False),
    ],
)
def test_v4_provider_state_witness_rejects_inconsistent_address(
    field,
    value,
    in_provenance,
) -> None:
    provider = _state_provider("48104", 1)
    address = orjson.loads(provider["address_payload"])
    target = address["address_provenance"][0] if in_provenance else address
    target[field] = value
    provider["address_payload"] = orjson.dumps(address)

    with pytest.raises(ValueError, match="address is inconsistent"):
        provider_cells._provider_cell_rows(
            PROJECTION_ID,
            _BuildState(hashlib.sha256()),
            [NPI],
            {NPI: [provider]},
        )
