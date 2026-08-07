# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact billing negotiated-price hydration tests."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_geo_reader as geo_reader
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

GROUP_A = "aa" * 16
GROUP_B = "bb" * 16
NPI_A = 1000000004
NPI_B = 1000000012


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


def _provider_rate(
    *,
    npi: int = NPI_A,
    source_key: int = 0,
    group_ref: str = GROUP_A,
) -> geo_reader.BillingProviderRateWitness:
    return geo_reader.BillingProviderRateWitness(
        snapshot_key=17,
        code_key=5,
        source_key=source_key,
        source_record_ordinal=source_key,
        provider_group_ref=group_ref,
        provider_set_key=3,
        price_key=10,
        occurrence_ordinal=0,
        npi=npi,
    )


def _address(
    npi: int,
    distance: float | None = None,
) -> geo_reader.BillingProviderAddress:
    return geo_reader.BillingProviderAddress(
        npi=npi,
        location_hash=f"entity_address_unified:{npi:064x}",
        distance_miles=distance,
        address_key="00000000-0000-0000-0000-000000000001",
        address_site_key="00000000-0000-0000-0000-000000000002",
        location_key=f"{npi:064x}",
        address_purpose="practice",
        display={"first_line": "10 Example Ave"},
        geo_evidence_level="nppes_registry_address",
        geo_evidence_source_id=1,
        provenance=(
            geo_reader.BillingAddressProvenance(
                dataset_id="cms_nppes_registry",
                source_id=1,
                source_record_id=f"synthetic:{npi}",
                record_version_id="20260101",
                record_version_ids=("20260101",),
                retrieved_at="2026-01-01T00:00:00+00:00",
                issuer_names=(),
                source_urls=(),
            ),
        ),
    )


def _geo_witness(
    *,
    npi: int = NPI_A,
    source_key: int = 0,
    group_ref: str = GROUP_A,
) -> geo_reader.BillingProviderGeoWitness:
    return geo_reader.BillingProviderGeoWitness(
        provider_rate=_provider_rate(
            npi=npi,
            source_key=source_key,
            group_ref=group_ref,
        ),
        address=_address(npi),
    )


@pytest.mark.asyncio
async def test_price_hydration_rejects_duplicate_geo_witness_keys(
    monkeypatch,
) -> None:
    hydrate = AsyncMock(return_value={10: [{"negotiated_rate": 20}]})
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )
    witness = _geo_witness()

    with pytest.raises(PTG2ManifestArtifactError, match="canonical and unique"):
        await geo_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(witness, witness),
        )

    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_hydration_rejects_invalid_nested_provider_witness(
    monkeypatch,
) -> None:
    hydrate = AsyncMock(return_value={10: [{"negotiated_rate": 20}]})
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="provider/rate scope"):
        await geo_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(
                geo_reader.BillingProviderGeoWitness(
                    provider_rate=object(),
                    address=_address(NPI_A),
                ),
            ),
        )

    hydrate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_address",
    (
        replace(_address(NPI_A), selection_contract="unexpected"),
        replace(_address(NPI_A), distance_miles=float("nan")),
    ),
    ids=("selection-contract", "non-finite-distance"),
)
async def test_price_hydration_rejects_forged_address_stable_fields(
    monkeypatch,
    invalid_address,
) -> None:
    hydrate = AsyncMock(return_value={10: [{"negotiated_rate": 20}]})
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="address scope is invalid"):
        await geo_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(
                geo_reader.BillingProviderGeoWitness(
                    provider_rate=_provider_rate(),
                    address=invalid_address,
                ),
            ),
        )

    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_hydration_preserves_source_witness_multiplicity(
    monkeypatch,
) -> None:
    hydrate = AsyncMock(
        return_value={
            10: [
                {"negotiated_rate": 20, "billing_code_modifier": ["AA"]},
                {"negotiated_rate": 30, "billing_code_modifier": ["BB"]},
            ]
        }
    )
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )
    hydrated = await geo_reader.hydrate_exact_billing_geo_prices(
        object(),
        _tables(),
        geo_witnesses=(
            _geo_witness(),
            _geo_witness(source_key=1, group_ref=GROUP_B),
        ),
        price_filter_args={"modifiers": ["AA"]},
    )

    assert len(hydrated) == 2
    assert [witness.geo_witness.provider_rate.source_key for witness in hydrated] == [
        0,
        1,
    ]
    assert [tuple(witness.prices) for witness in hydrated] == [
        ({"negotiated_rate": 20, "billing_code_modifier": ["AA"]},),
        ({"negotiated_rate": 20, "billing_code_modifier": ["AA"]},),
    ]
    assert hydrate.await_args.args[2] == (10,)
    assert hydrate.await_args.kwargs == {"maximum_atom_count": 256}


@pytest.mark.asyncio
async def test_price_hydration_rejects_missing_keys_and_atom_overflow(
    monkeypatch,
) -> None:
    hydrate = AsyncMock(return_value={})
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="incomplete"):
        await geo_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(_geo_witness(),),
        )

    hydrate.return_value = {10: [{"negotiated_rate": 20}]}
    monkeypatch.setattr(
        geo_reader.ptg2_billing_price_reader,
        "MAX_PRICE_ATOMS",
        1,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="atom limit"):
        await geo_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(
                _geo_witness(),
                _geo_witness(source_key=1, group_ref=GROUP_B),
            ),
        )


@pytest.mark.asyncio
async def test_price_hydration_accepts_256_atoms_and_rejects_257(
    monkeypatch,
) -> None:
    price_reader = AsyncMock(
        return_value={
            10: [{"negotiated_rate": atom_ordinal} for atom_ordinal in range(256)]
        }
    )
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        price_reader,
    )

    hydrated = await geo_reader.hydrate_exact_billing_geo_prices(
        object(),
        _tables(),
        geo_witnesses=(_geo_witness(),),
    )

    assert len(hydrated[0].prices) == 256
    price_reader.return_value[10].append({"negotiated_rate": 256})
    with pytest.raises(PTG2ManifestArtifactError, match="atom limit"):
        await geo_reader.hydrate_exact_billing_geo_prices(
            object(),
            _tables(),
            geo_witnesses=(_geo_witness(),),
        )


@pytest.mark.asyncio
async def test_price_hydration_rejects_cross_snapshot_or_cross_npi_witness(
    monkeypatch,
) -> None:
    hydrate = AsyncMock(return_value={10: [{"negotiated_rate": 20}]})
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        hydrate,
    )
    cross_snapshot = geo_reader.BillingProviderGeoWitness(
        provider_rate=geo_reader.BillingProviderRateWitness(
            snapshot_key=18,
            code_key=5,
            source_key=0,
            source_record_ordinal=0,
            provider_group_ref=GROUP_A,
            provider_set_key=3,
            price_key=10,
            occurrence_ordinal=0,
            npi=NPI_A,
        ),
        address=_address(NPI_A),
    )
    cross_npi = geo_reader.BillingProviderGeoWitness(
        provider_rate=_provider_rate(npi=NPI_A),
        address=_address(NPI_B),
    )
    for invalid_witness in (cross_snapshot, cross_npi):
        with pytest.raises(
            PTG2ManifestArtifactError,
            match="crossed its snapshot or NPI scope",
        ):
            await geo_reader.hydrate_exact_billing_geo_prices(
                object(),
                _tables(),
                geo_witnesses=(invalid_witness,),
            )
    hydrate.assert_not_awaited()


@pytest.mark.asyncio
async def test_price_payloads_are_independent_across_source_witnesses(
    monkeypatch,
) -> None:
    nested_price_by_field = {"negotiated_rate": 20, "service_code": ["11"]}
    monkeypatch.setattr(
        geo_reader.ptg2_serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(return_value={10: [nested_price_by_field]}),
    )
    hydrated = await geo_reader.hydrate_exact_billing_geo_prices(
        object(),
        _tables(),
        geo_witnesses=(
            _geo_witness(),
            _geo_witness(source_key=1, group_ref=GROUP_B),
        ),
    )
    hydrated[0].prices[0]["service_code"].append("12")
    assert hydrated[1].prices[0]["service_code"] == ["11"]
    assert nested_price_by_field["service_code"] == ["11"]
