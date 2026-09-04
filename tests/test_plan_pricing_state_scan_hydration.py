# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact response hydration for release-bound state scans."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import plan_pricing_state_scan as scan
from api import plan_pricing_state_scan_hydration as hydration


PROVIDER_STATE_FRAGMENT_VERSION = "plan_pricing_provider_state_v1"


def _binding():
    return SimpleNamespace(
        binding_ordinal=0,
        snapshot_id="snapshot-1",
        source_key="source-1",
        plan_id="plan-1",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


def _selection():
    binding = _binding()
    return SimpleNamespace(
        in_network_bindings=(binding,),
        serving_tables_for_snapshot=lambda _snapshot_id: SimpleNamespace(),
    )


def _args(**overrides):
    return {
        "plan_release_id": "hprelease_01J00000000000000000000000",
        "state": "MI",
        "include_evidence": "true",
        **overrides,
    }


def _occurrence(npi: int) -> dict:
    return {
        "npi": npi,
        "binding_ordinal": 0,
        "occurrence_ordinal": 0,
        "provider_set_key": 7,
        "provider_set_ref": "1" * 32,
        "price_key": 9,
        "price_set_ref": "2" * 32,
        "rate_pack_ref": "3" * 32,
        "source_artifact_key": 11,
        "provider_count": 2,
        "group_fragment": {
            "reported_code_system": "CPT",
            "reported_code": "27447",
            "plan_id": "plan-1",
            "plan_market_type": "group",
            "negotiation_arrangement": "ffs",
            "billing_code_type_version": "2026",
            "source_procedure_name": "Synthetic procedure",
            "source_procedure_description": "Synthetic description",
            "network_names": ["Synthetic Network"],
        },
        "occurrence_multiplicity": 1,
    }


def _address(
    first_line: str,
    city: str,
    postal_code: str,
    location_key: str,
) -> dict:
    return {
        "first_line": first_line,
        "city": city,
        "state": "MI",
        "postal_code": postal_code,
        "country_code": "US",
        "location_key": location_key,
        "address_provenance": [
            {
                "source_id": 1,
                "dataset_id": "synthetic",
                "source_record_id": "record-1",
                "record_version_id": "version-1",
                "retrieved_at": "2026-01-01T00:00:00Z",
            }
        ],
    }


def _provider_fragment(
    npi: int,
    address_by_field: dict,
    *,
    taxonomy_codes: list[str] | None = None,
    entity_type_code: int = 1,
) -> bytes:
    return orjson.dumps(
        {
            "version": PROVIDER_STATE_FRAGMENT_VERSION,
            "provider": {
                "npi": npi,
                "provider_name": "Frozen Synthetic Provider",
                "entity_type_code": entity_type_code,
                "credential": "MD",
                "provider_sex_code": "U",
                "taxonomy_codes": taxonomy_codes or ["207Q00000X"],
                "specialties": ["Family Medicine"],
                "primary_specialty": "Family Medicine",
                "classifications": ["Family Medicine"],
                "specializations": [],
                "primary_specialization": None,
                "state": "MI",
                "city": address_by_field["city"],
                "zip5": address_by_field["postal_code"],
                "location_hash": f"entity_address_unified:{address_by_field['location_key']}",
                "location_source": "entity_address_unified",
                "location_confidence_code": "entity_address_unified",
                "address_payload": {
                    "npi": npi,
                    "type": "practice",
                    "second_line": None,
                    "address_key": f"address-{npi}",
                    "address_precision": "street",
                    "address_sources": ["nppes"],
                    "source_record_ids": ["record-1"],
                    "source_count": 1,
                    "multi_source_confirmed": False,
                    "source_mask": 1,
                    "address_source_mask": 1,
                    "location_confidence_id": 1,
                    "geo_evidence_level": "nppes_registry_address",
                    "lat": 42.28,
                    "long": -83.74,
                    **address_by_field,
                },
            },
        }
    )


def test_state_scan_applies_inferred_taxonomy_to_frozen_witnesses():
    first_address = _address(
        "1 GI Street", "Ann Arbor", "48104", "gi-location"
    )
    second_address = _address(
        "2 Family Street", "Lansing", "48933", "family-location"
    )
    third_address = _address(
        "3 GI Facility", "Detroit", "48201", "gi-facility-location"
    )

    eligible_npis = scan._eligible_provider_npis(
        {
            1000000001: _provider_fragment(
                1000000001,
                first_address,
                taxonomy_codes=["207RG0100X"],
            ),
            1000000002: _provider_fragment(1000000002, second_address),
            1000000003: _provider_fragment(
                1000000003,
                third_address,
                taxonomy_codes=["207RG0100X"],
                entity_type_code=2,
            ),
        },
        {"code_system": "CPT", "code": "45378", "state": "MI"},
    )

    assert eligible_npis == (1000000001,)


def _install_price_mocks(monkeypatch) -> None:
    from api import ptg2_serving as serving

    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(
            return_value={
                9: [{"negotiated_rate": "10.00"}],
                10: [{"negotiated_rate": "12.00"}],
            }
        ),
    )
    monkeypatch.setattr(serving, "_procedure_details_for_rows", AsyncMock(return_value={}))


@pytest.mark.asyncio
async def test_selected_group_hydration_preserves_address_evidence_and_options(
    monkeypatch,
):
    first_occurrence = _occurrence(1000000001)
    second_occurrence_by_field = {
        **first_occurrence,
        "occurrence_ordinal": 1,
        "price_key": 10,
        "price_set_ref": "4" * 32,
        "rate_pack_ref": "5" * 32,
    }
    other_provider_occurrence_by_field = {**first_occurrence, "npi": 1000000002}
    addresses_by_npi = {
        1000000001: _address("1 Example Street", "Ann Arbor", "48104", "synthetic-location"),
        1000000002: _address("2 Example Street", "Lansing", "48933", "synthetic-location-2"),
    }
    _install_price_mocks(monkeypatch)

    hydrated_items = await scan._hydrate_selected_groups(
        object(),
        _selection(),
        _args(),
        [first_occurrence, second_occurrence_by_field, other_provider_occurrence_by_field],
        {
            npi: _provider_fragment(npi, address_by_field)
            for npi, address_by_field in addresses_by_npi.items()
        },
    )

    assert [hydrated_item["npi"] for hydrated_item in hydrated_items] == [
        1000000001,
        1000000002,
    ]
    assert len(hydrated_items[0]["rate_options"]) == 2
    assert {price_by_field["negotiated_rate"] for price_by_field in hydrated_items[0]["prices"]} == {10, 12}
    assert hydrated_items[0]["address"]["first_line"] == "1 Example Street"
    assert hydrated_items[0]["address_verification"]["address_provenance"][0]["dataset_id"] == "synthetic"
    assert hydrated_items[1]["address"]["first_line"] == "2 Example Street"


@pytest.mark.asyncio
async def test_state_scan_hydrates_only_from_frozen_provider_witness(monkeypatch):
    from api import ptg2_serving as serving

    address = _address("1 Frozen Street", "Ann Arbor", "48104", "frozen-location")
    never_live = AsyncMock(side_effect=AssertionError("live provider read"))
    for function_name in (
        "_membership_location_rows",
        "_selected_provider_rows_by_set",
        "_enriched_provider_rows_for_npis",
        "_taxonomy_rows_for_npis",
        "_overlay_provider_directory_corroboration",
    ):
        monkeypatch.setattr(serving, function_name, never_live)
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(return_value={9: [{"negotiated_rate": "10.00"}]}),
    )
    monkeypatch.setattr(serving, "_procedure_details_for_rows", AsyncMock(return_value={}))

    hydrated_items = await scan._hydrate_selected_groups(
        object(),
        _selection(),
        _args(),
        [_occurrence(1000000001)],
        {1000000001: _provider_fragment(1000000001, address)},
    )

    assert hydrated_items[0]["provider_name"] == "Frozen Synthetic Provider"
    assert hydrated_items[0]["address"]["first_line"] == "1 Frozen Street"
    assert hydrated_items[0]["address_verification"]["network_bound_address"] is False
    assert hydrated_items[0]["address_verification"]["address_network_binding"] == (
        "inferred_from_provider_identity"
    )
    never_live.assert_not_awaited()


@pytest.mark.asyncio
async def test_state_scan_strips_frozen_address_provenance_without_evidence(
    monkeypatch,
):
    address = _address("1 Frozen Street", "Ann Arbor", "48104", "frozen-location")
    _install_price_mocks(monkeypatch)

    items = await scan._hydrate_selected_groups(
        object(),
        _selection(),
        _args(include_evidence="false"),
        [_occurrence(1000000001)],
        {1000000001: _provider_fragment(1000000001, address)},
    )

    assert "address_provenance" not in items[0]["address"]
    assert "address_provenance" not in items[0]["address_verification"]


def _mutated_fragment(path: tuple[str, ...], value) -> bytes:
    address = _address("1 Frozen Street", "Ann Arbor", "48104", "frozen-location")
    fragment = orjson.loads(_provider_fragment(1000000001, address))
    target = fragment
    for field_name in path[:-1]:
        target = target[field_name]
    target[path[-1]] = value
    return orjson.dumps(fragment)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("version",), "plan_pricing_provider_state_v0"),
        (("provider", "npi"), 1000000002),
        (("provider", "state"), "OH"),
        (("provider", "address_payload", "npi"), 1000000002),
        (("provider", "address_payload", "state"), "OH"),
        (("provider", "address_payload", "location_key"), "changed-location"),
        (("provider", "address_payload", "source_mask"), "1"),
        (
            (
                "provider",
                "address_payload",
                "address_provenance",
            ),
            [],
        ),
    ),
)
async def test_state_scan_rejects_inconsistent_frozen_provider_witness(
    path,
    value,
):
    with pytest.raises(scan.PTG2ManifestArtifactError):
        await scan._hydrate_selected_groups(
            object(),
            _selection(),
            _args(),
            [_occurrence(1000000001)],
            {1000000001: _mutated_fragment(path, value)},
        )


def test_state_scan_witness_primitive_boundaries():
    assert hydration._fragment_bytes(memoryview(b"{}")) == b"{}"
    assert hydration._positive_source_id(True) is None
    assert hydration._state_code("Michigan") is None

    with pytest.raises(scan.PTG2ManifestArtifactError):
        hydration._fragment_bytes(b"")


@pytest.mark.parametrize(
    "address",
    (
        {"lat": 42.0, "long": None},
        {"lat": True, "long": -83.0},
        {"lat": float("nan"), "long": -83.0},
    ),
)
def test_state_scan_rejects_invalid_coordinate_pairs(address):
    with pytest.raises(scan.PTG2ManifestArtifactError):
        hydration._validated_coordinates(address)


def test_state_scan_accepts_absent_coordinate_pair():
    hydration._validated_coordinates({"lat": None, "long": None})


def test_state_scan_rejects_invalid_hydration_references():
    occurrence = _occurrence(1000000001)

    with pytest.raises(scan.PTG2ManifestArtifactError):
        hydration._validated_providers_by_npi({True: b"{}"}, _args(), object())
    with pytest.raises(scan.PTG2ManifestArtifactError):
        hydration._provider_payload_for_npi({}, 1000000001, object())
    with pytest.raises(scan.PTG2ManifestArtifactError):
        hydration._serving_row({**occurrence, "group_fragment": None}, _binding())
    with pytest.raises(scan.PTG2ManifestArtifactError):
        hydration._binding_scope(_selection(), _args(), [occurrence], 1)

    selection = _selection()
    selection.serving_tables_for_snapshot = lambda _snapshot_id: None
    with pytest.raises(hydration.PlanPricingProjectionUnavailable):
        hydration._binding_scope(selection, _args(), [occurrence], 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("prices", "remaining_atoms", "error"),
    (
        ({}, 1, scan.PTG2ManifestArtifactError),
        ({9: [{"negotiated_rate": "10.00"}]}, 0, hydration.PlanPricingStateScanBudgetExceeded),
    ),
)
async def test_state_scan_rejects_incomplete_or_over_budget_prices(
    prices,
    remaining_atoms,
    error,
):
    serving = SimpleNamespace(
        _version_three_bounded_prices_by_key=AsyncMock(return_value=prices)
    )
    scope = hydration._BindingScope(
        _binding(),
        object(),
        [_occurrence(1000000001)],
        _args(),
    )

    with pytest.raises(error):
        await hydration._prices_for_scope(object(), scope, remaining_atoms, serving)


def test_state_scan_response_items_apply_available_source_and_provider_fields():
    occurrence = _occurrence(1000000001)
    serving_row = hydration._serving_row(occurrence, _binding())
    serving = SimpleNamespace(
        _request_local_provider_payload=lambda provider: provider,
        _item_source_provenance=lambda provenance: {"source": provenance},
        _catalog_key=lambda *_args: ("CPT", "27447"),
        _ptg2_manifest_provider_procedure_item=lambda **kwargs: {
            "npi": kwargs["npi"],
            "source": kwargs["serving_data"].get("source"),
        },
    )
    scope = hydration._BindingScope(_binding(), object(), [occurrence], _args())

    response_items = hydration._response_items(
        scope,
        [serving_row],
        {
            1000000001: {
                "entity_type_code": None,
                "credential": "MD",
                "provider_sex_code": "U",
            }
        },
        {"2" * 32: [{"negotiated_rate": "10.00"}]},
        {},
        {11: "synthetic-source"},
        serving,
    )

    assert response_items == [
        {
            "npi": 1000000001,
            "source": "synthetic-source",
            "credential": "MD",
            "provider_sex_code": "U",
        }
    ]


@pytest.mark.asyncio
async def test_state_scan_rejects_missing_provider_witness():
    with pytest.raises(scan.PTG2ManifestArtifactError):
        await scan._hydrate_selected_groups(
            object(),
            _selection(),
            _args(),
            [_occurrence(1000000001)],
            {},
        )
