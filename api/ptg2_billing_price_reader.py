# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Hydrate exact billing GEO witnesses with bounded negotiated prices."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from typing import Any

from api import ptg2_serving
from api.ptg2_billing_geo_contract import (
    MAX_PROVIDER_RATE_WITNESSES,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
    bounded_tuple,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

MAX_PRICE_KEYS = 256
MAX_PRICE_ATOMS = 256


def _validated_prices_by_key(
    prices_by_key: Mapping[int, list[dict[str, Any]]],
    *,
    price_keys: tuple[int, ...],
) -> dict[int, list[dict[str, Any]]]:
    if set(prices_by_key) != set(price_keys):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing price hydration is incomplete"
        )
    validated_prices_by_key: dict[int, list[dict[str, Any]]] = {}
    for price_key in price_keys:
        price_payloads = prices_by_key[price_key]
        if type(price_payloads) is not list or any(
            type(price_payload) is not dict for price_payload in price_payloads
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing price hydration is malformed"
            )
        validated_prices_by_key[price_key] = price_payloads
    return validated_prices_by_key


def _normalized_geo_witnesses(
    serving_tables: PTG2ServingTables,
    geo_witnesses: Iterable[BillingProviderGeoWitness],
) -> tuple[BillingProviderGeoWitness, ...]:
    normalized_witnesses = bounded_tuple(
        geo_witnesses,
        maximum_count=MAX_PROVIDER_RATE_WITNESSES,
        error_message="PTG2 exact billing geo witness scope is invalid",
    )
    if any(
        type(witness) is not BillingProviderGeoWitness
        for witness in normalized_witnesses
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing geo witness scope is invalid"
        )
    if normalized_witnesses != tuple(
        sorted(normalized_witnesses, key=lambda witness: witness.stable_sort_key)
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing geo witnesses are not canonically ordered"
        )
    snapshot_key = ptg2_serving._required_shared_snapshot_key(serving_tables)
    if any(
        witness.provider_rate.snapshot_key != snapshot_key
        or witness.address.npi != witness.provider_rate.npi
        for witness in normalized_witnesses
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing geo witness crossed its snapshot or NPI scope"
        )
    return normalized_witnesses


def _hydrated_price_witnesses(
    geo_witnesses: tuple[BillingProviderGeoWitness, ...],
    prices_by_key: Mapping[int, list[dict[str, Any]]],
    price_filter_args: Mapping[str, Any],
    *,
    atom_budget: int,
) -> tuple[BillingProviderGeoPriceWitness, ...]:
    hydrated_witnesses: list[BillingProviderGeoPriceWitness] = []
    retained_price_atom_count = 0
    for geo_witness in geo_witnesses:
        filtered_prices = ptg2_serving._ptg2_manifest_filter_prices(
            prices_by_key[geo_witness.provider_rate.price_key],
            price_filter_args,
        )
        if not filtered_prices:
            continue
        retained_price_atom_count += len(filtered_prices)
        if retained_price_atom_count > atom_budget:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing price hydration exceeds its atom limit"
            )
        hydrated_witnesses.append(
            BillingProviderGeoPriceWitness(
                geo_witness=geo_witness,
                prices=tuple(deepcopy(price) for price in filtered_prices),
            )
        )
    return tuple(hydrated_witnesses)


async def hydrate_exact_billing_geo_prices(
    session,
    serving_tables: PTG2ServingTables,
    *,
    geo_witnesses: Iterable[BillingProviderGeoWitness],
    price_filter_args: Mapping[str, Any] | None = None,
    atom_budget: int | None = None,
) -> tuple[BillingProviderGeoPriceWitness, ...]:
    """Hydrate selected price keys without merging source-local witnesses."""

    effective_atom_budget = MAX_PRICE_ATOMS if atom_budget is None else atom_budget
    if (
        type(effective_atom_budget) is not int
        or not 0 <= effective_atom_budget <= MAX_PRICE_ATOMS
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing price hydration has an invalid atom budget"
        )
    normalized_witnesses = _normalized_geo_witnesses(
        serving_tables,
        geo_witnesses,
    )
    price_keys = tuple(
        sorted({witness.provider_rate.price_key for witness in normalized_witnesses})
    )
    if len(price_keys) > MAX_PRICE_KEYS:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing price hydration exceeds its key limit"
        )
    if not price_keys:
        return ()
    prices_by_key = _validated_prices_by_key(
        await ptg2_serving._version_three_prices_by_key(
            session,
            serving_tables,
            price_keys,
            maximum_atom_count=effective_atom_budget,
        ),
        price_keys=price_keys,
    )
    return _hydrated_price_witnesses(
        normalized_witnesses,
        prices_by_key,
        dict(price_filter_args or {}),
        atom_budget=effective_atom_budget,
    )


__all__ = [
    "BillingProviderGeoPriceWitness",
    "hydrate_exact_billing_geo_prices",
]
