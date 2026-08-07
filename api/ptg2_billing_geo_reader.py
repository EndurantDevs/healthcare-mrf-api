# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Expand exact billing rates through provider-owned GEO evidence."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from api import (
    ptg2_billing_address_lineage,
    ptg2_billing_price_reader,
    ptg2_serving,
)
from api.ptg2_billing_exact_contract import BillingRateOccurrenceWitness
from api.ptg2_billing_geo_contract import (
    BILLING_ADDRESS_SELECTION_CONTRACT,
    MAX_PROVIDER_GROUPS,
    MAX_PROVIDER_RATE_WITNESSES,
    BillingAddressProvenance,
    BillingGeoSelection,
    BillingProviderAddress,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
    BillingProviderRateWitness,
    validated_geo_args as _validated_geo_args,
    validated_provider_npi,
    validated_provider_rate_witnesses,
    validated_rate_witnesses,
)
from api.ptg2_types import PTG2ServingTables
from process.provider_directory_profile import is_valid_npi
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

PTG2_BILLING_ADDRESS_SELECTION_CONTRACT = BILLING_ADDRESS_SELECTION_CONTRACT
_MAX_PROVIDER_GROUPS = MAX_PROVIDER_GROUPS
_MAX_PROVIDER_NPIS_PER_GROUP = 2048
_MAX_GROUP_NPI_EDGES = 8192
_MAX_PROVIDER_RATE_WITNESSES = MAX_PROVIDER_RATE_WITNESSES


def _npi_members_by_group(
    member_ids_by_group: Mapping[str, tuple[str, ...]],
    *,
    provider_group_refs: tuple[str, ...],
) -> dict[str, tuple[int, ...]]:
    if set(member_ids_by_group) != set(provider_group_refs):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing group-to-NPI projection is incomplete"
        )
    total_member_count = 0
    npis_by_group: dict[str, tuple[int, ...]] = {}
    for provider_group_ref in provider_group_refs:
        member_ids = member_ids_by_group[provider_group_ref]
        if type(member_ids) is not tuple:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-NPI projection is malformed"
            )
        if len(member_ids) > _MAX_PROVIDER_NPIS_PER_GROUP:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing provider group exceeds its NPI limit"
            )
        total_member_count += len(member_ids)
        if total_member_count > _MAX_GROUP_NPI_EDGES:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-NPI projection exceeds its edge limit"
            )
        npis = tuple(
            (
                ptg2_serving._ptg2_npi_from_member_id(member_id)
                if type(member_id) is str
                else None
            )
            for member_id in member_ids
        )
        if any(
            npi is None
            or not is_valid_npi(npi)
            or member_id != ptg2_serving._ptg2_npi_member_id(npi)
            for member_id, npi in zip(member_ids, npis, strict=True)
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group contains an invalid NPI member"
            )
        if npis != tuple(sorted(npis)) or len(npis) != len(set(npis)):
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-NPI projection is inconsistent"
            )
        npis_by_group[provider_group_ref] = npis
    return npis_by_group


def _expanded_provider_rates(
    rate_witnesses: tuple[BillingRateOccurrenceWitness, ...],
    npis_by_group: Mapping[str, tuple[int, ...]],
    provider_npi: int | None,
) -> tuple[BillingProviderRateWitness, ...]:
    provider_rates: list[BillingProviderRateWitness] = []
    for rate_witness in rate_witnesses:
        for npi in npis_by_group[rate_witness.provider_group_ref]:
            if provider_npi is not None and npi != provider_npi:
                continue
            provider_rates.append(
                BillingProviderRateWitness(
                    snapshot_key=rate_witness.snapshot_key,
                    code_key=rate_witness.code_key,
                    source_key=rate_witness.source_key,
                    source_record_ordinal=rate_witness.source_record_ordinal,
                    provider_group_ref=rate_witness.provider_group_ref,
                    provider_set_key=rate_witness.provider_set_key,
                    price_key=rate_witness.price_key,
                    occurrence_ordinal=rate_witness.occurrence_ordinal,
                    npi=npi,
                )
            )
            if len(provider_rates) > _MAX_PROVIDER_RATE_WITNESSES:
                raise PTG2ManifestArtifactError(
                    "PTG2 exact billing provider/rate scope exceeds its limit"
                )
    return tuple(
        sorted(
            provider_rates,
            key=lambda witness: (witness.npi, *witness.stable_rate_key),
        )
    )


async def expand_billing_rate_witnesses_to_npis(
    session,
    serving_tables: PTG2ServingTables,
    *,
    rate_witnesses: Iterable[BillingRateOccurrenceWitness],
    provider_npi: int | None = None,
) -> tuple[BillingProviderRateWitness, ...]:
    """Expand only through each rate witness's exact provider group."""

    if not serving_tables.uses_v4_graph:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider expansion requires the sealed V4 graph"
        )
    normalized_provider_npi = validated_provider_npi(provider_npi, optional=True)
    normalized_rate_witnesses = validated_rate_witnesses(rate_witnesses)
    if not normalized_rate_witnesses:
        return ()
    snapshot_key = ptg2_serving._required_shared_snapshot_key(serving_tables)
    if any(
        witness.snapshot_key != snapshot_key for witness in normalized_rate_witnesses
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing rate witness belongs to another snapshot"
        )
    provider_group_refs = tuple(
        sorted({witness.provider_group_ref for witness in normalized_rate_witnesses})
    )
    if len(provider_group_refs) > _MAX_PROVIDER_GROUPS:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider expansion exceeds its group limit"
        )
    npis_by_group = _npi_members_by_group(
        await ptg2_serving._shared_graph_members_by_id(
            session,
            serving_tables,
            "provider_group_npi",
            provider_group_refs,
            max_members=_MAX_PROVIDER_NPIS_PER_GROUP + 1,
            max_projection_members=_MAX_GROUP_NPI_EDGES + 1,
        ),
        provider_group_refs=provider_group_refs,
    )
    return _expanded_provider_rates(
        normalized_rate_witnesses,
        npis_by_group,
        normalized_provider_npi,
    )


async def load_exact_billing_geo_witnesses(
    session,
    serving_tables: PTG2ServingTables,
    *,
    provider_rate_witnesses: Iterable[BillingProviderRateWitness],
    geo_args: Mapping[str, Any],
) -> BillingGeoSelection:
    """Select each eligible NPI's own address within one bounded GEO scope."""

    normalized_geo_args = _validated_geo_args(geo_args)
    provider_rates = validated_provider_rate_witnesses(provider_rate_witnesses)
    if not provider_rates:
        return BillingGeoSelection(True, ())
    snapshot_key = ptg2_serving._required_shared_snapshot_key(serving_tables)
    if any(witness.snapshot_key != snapshot_key for witness in provider_rates):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider witness belongs to another snapshot"
        )
    candidate_npis = tuple(sorted({witness.npi for witness in provider_rates}))
    location_rows = await ptg2_serving._membership_location_rows(
        session,
        serving_tables,
        normalized_geo_args,
        candidate_npis=candidate_npis,
        limit=len(candidate_npis),
        offset=0,
        stored_address_provenance_only=True,
    )
    if location_rows is None:
        return BillingGeoSelection(False, ())
    addresses_by_npi = ptg2_billing_address_lineage.provider_addresses_by_npi(
        location_rows,
        candidate_npis=frozenset(candidate_npis),
    )
    witnesses = tuple(
        BillingProviderGeoWitness(provider_rate, addresses_by_npi[provider_rate.npi])
        for provider_rate in provider_rates
        if provider_rate.npi in addresses_by_npi
    )
    return BillingGeoSelection(
        True,
        tuple(sorted(witnesses, key=lambda witness: witness.stable_sort_key)),
    )


hydrate_exact_billing_geo_prices = (
    ptg2_billing_price_reader.hydrate_exact_billing_geo_prices
)

__all__ = [
    "PTG2_BILLING_ADDRESS_SELECTION_CONTRACT",
    "BillingAddressProvenance",
    "BillingGeoSelection",
    "BillingProviderAddress",
    "BillingProviderGeoPriceWitness",
    "BillingProviderGeoWitness",
    "BillingProviderRateWitness",
    "expand_billing_rate_witnesses_to_npis",
    "hydrate_exact_billing_geo_prices",
    "load_exact_billing_geo_witnesses",
]
