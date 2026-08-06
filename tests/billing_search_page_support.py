# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic constructors shared by exact billing provider-page tests."""

from __future__ import annotations

from api import ptg2_billing_search_page as page
from api.plan_release_serving import PlanReleaseSnapshotBinding
from api.ptg2_billing_code_reader import BillingCodeWitness
from api.ptg2_billing_geo_contract import (
    BillingAddressProvenance,
    BillingProviderAddress,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
    BillingProviderRateWitness,
)
from api.ptg2_billing_search_contract import BillingSearchProviderCandidate
from api.ptg2_types import PTG2ServingTables

GROUP_A = "aa" * 16
GROUP_B = "bb" * 16
SNAPSHOT_ID = "ptg2:synthetic-page"
NPI_VALUES = (
    1000000004,
    1000000012,
    1000000020,
    1000000038,
    1000000046,
    1000000053,
)


def binding(
    ordinal: int = 0,
    *,
    snapshot_id: str = SNAPSHOT_ID,
) -> PlanReleaseSnapshotBinding:
    return PlanReleaseSnapshotBinding(
        binding_ordinal=ordinal,
        snapshot_id=snapshot_id,
        source_key="synthetic-source",
        plan_id="synthetic-plan",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


def serving_tables(
    *,
    snapshot_id: str = SNAPSHOT_ID,
    snapshot_key: int = 17,
    plan_id: str = "synthetic-plan",
    plan_market_type: str = "group",
    source_key: str = "synthetic-source",
) -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id=snapshot_id,
        arch_version="postgres_binary_v3",
        shared_snapshot_key=snapshot_key,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=2,
        plan_id=plan_id,
        plan_market_type=plan_market_type,
        source_key=source_key,
    )


def code_witness(code_key: int = 5) -> BillingCodeWitness:
    return BillingCodeWitness(
        code_key=code_key,
        code_system="CPT",
        code="99213",
        negotiation_arrangement="ffs",
        billing_code_type_version="2026",
        source_name="synthetic-source",
        source_description="Synthetic source",
    )


def address(
    npi: int,
    *,
    distance: float | None = None,
    address_suffix: int = 1,
) -> BillingProviderAddress:
    location_key = f"{npi:064x}"
    return BillingProviderAddress(
        npi=npi,
        location_hash=f"entity_address_unified:{location_key}",
        distance_miles=distance,
        address_key=("00000000-0000-0000-0000-" f"{address_suffix:012d}"),
        address_site_key="00000000-0000-0000-0000-000000000099",
        location_key=location_key,
        address_purpose="practice",
        display={"first_line": "10 Example Ave", "postal_code": "25000"},
        geo_evidence_level="nppes_registry_address",
        geo_evidence_source_id=1,
        provenance=(
            BillingAddressProvenance(
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


def geo_witness(
    *,
    npi: int = NPI_VALUES[0],
    distance: float | None = None,
    price_key: int = 10,
    source_key: int = 0,
    group_ref: str = GROUP_A,
    occurrence_ordinal: int = 0,
    selected_address: BillingProviderAddress | None = None,
    snapshot_key: int = 17,
) -> BillingProviderGeoWitness:
    provider_rate = BillingProviderRateWitness(
        snapshot_key=snapshot_key,
        code_key=5,
        source_key=source_key,
        source_record_ordinal=source_key,
        provider_group_ref=group_ref,
        provider_set_key=3 + source_key,
        price_key=price_key,
        occurrence_ordinal=occurrence_ordinal,
        npi=npi,
    )
    return BillingProviderGeoWitness(
        provider_rate=provider_rate,
        address=selected_address or address(npi, distance=distance),
    )


def hydrated_price(
    witness: BillingProviderGeoWitness,
    *,
    rate: int = 20,
) -> BillingProviderGeoPriceWitness:
    return BillingProviderGeoPriceWitness(
        witness,
        ({"negotiated_rate": rate, "negotiated_type": "negotiated"},),
    )


def candidate(
    *,
    npi: int = NPI_VALUES[0],
    distance: float | None = None,
    binding_ordinal: int = 0,
    price_keys: tuple[int, ...] = (10,),
) -> BillingSearchProviderCandidate:
    """Build one provider/address candidate with exact synthetic witnesses."""

    selected_address = address(npi, distance=distance)
    witnesses = tuple(
        geo_witness(
            npi=npi,
            distance=distance,
            price_key=price_key,
            source_key=source_key,
            group_ref=("aa" if source_key == 0 else "bb") * 16,
            occurrence_ordinal=source_key,
            selected_address=selected_address,
        )
        for source_key, price_key in enumerate(price_keys)
    )
    return page.group_billing_geo_candidates(
        binding=binding(binding_ordinal),
        serving_tables=serving_tables(),
        code_witnesses=(code_witness(),),
        geo_witnesses=witnesses,
    )[0]
