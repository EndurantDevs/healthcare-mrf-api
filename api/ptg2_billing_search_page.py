# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-level grouping and bounded hydration for exact billing search."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any
from uuid import UUID

from api import ptg2_billing_price_reader
from api.billing_search_request import BILLING_SEARCH_MAX_LIMIT
from api.plan_release_serving import (
    PLAN_RELEASE_IN_NETWORK_ROLE,
    PlanReleaseSnapshotBinding,
)
from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.ptg2_billing_code_reader import BillingCodeWitness
from api.ptg2_billing_geo_contract import (
    LOCATION_KEY_PATTERN,
    MAX_PROVIDER_RATE_WITNESSES,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
    bounded_tuple,
)
from api.ptg2_billing_price_reader import MAX_PRICE_ATOMS, MAX_PRICE_KEYS
from api.ptg2_billing_search_contract import (
    BillingSearchMatchedProvider,
    BillingSearchProviderCandidate,
    BillingSearchProviderPage,
    serving_unavailable,
)
from api.ptg2_types import PTG2ServingTables
from process.provider_directory_profile import is_valid_npi

MAX_HYDRATION_CANDIDATES = 128
MAX_HYDRATION_PARTITIONS = 8
MAX_PAGE_HYDRATION_CALLS = 8
MAX_PAGE_SCOPED_PRICE_KEYS = MAX_PRICE_KEYS
_BillingSearchSortKey = tuple[int | float | str, ...]


def validate_billing_search_sort_key(sort_key: object) -> _BillingSearchSortKey:
    """Validate the exact seven-member provider cursor coordinate."""

    if type(sort_key) not in {tuple, list} or len(sort_key) != 7:
        raise serving_unavailable()
    (
        missing_distance,
        distance,
        binding_ordinal,
        snapshot_id,
        npi,
        address_key,
        location_key,
    ) = sort_key
    if (
        type(missing_distance) is not int
        or missing_distance not in {0, 1}
        or type(distance) is not float
        or not math.isfinite(distance)
        or distance < 0
        or (missing_distance == 1 and distance != 0.0)
        or type(binding_ordinal) is not int
        or not 0 <= binding_ordinal < 2**31
        or type(snapshot_id) is not str
        or not 1 <= len(snapshot_id) <= 256
        or not snapshot_id.isascii()
        or not snapshot_id.isprintable()
        or type(npi) is not int
        or not is_valid_npi(npi)
        or type(address_key) is not str
        or type(location_key) is not str
        or LOCATION_KEY_PATTERN.fullmatch(location_key) is None
    ):
        raise serving_unavailable()
    try:
        canonical_address_key = str(UUID(address_key))
    except (AttributeError, ValueError):
        raise serving_unavailable() from None
    if canonical_address_key != address_key:
        raise serving_unavailable()
    return (
        missing_distance,
        0.0 if distance == 0.0 else distance,
        binding_ordinal,
        snapshot_id,
        npi,
        address_key,
        location_key,
    )


def _validated_code_witnesses(
    code_witnesses: Iterable[BillingCodeWitness],
) -> dict[int, BillingCodeWitness]:
    retained = bounded_tuple(
        code_witnesses,
        maximum_count=256,
        error_message="PTG2 exact billing code scope is invalid",
    )
    if any(type(witness) is not BillingCodeWitness for witness in retained):
        raise serving_unavailable()
    witnesses_by_key = {witness.code_key: witness for witness in retained}
    if len(witnesses_by_key) != len(retained):
        raise serving_unavailable()
    return witnesses_by_key


def _geo_witnesses_by_provider(
    geo_witnesses: Iterable[BillingProviderGeoWitness],
) -> dict[tuple[int, str, str], tuple[BillingProviderGeoWitness, ...]]:
    retained_geo_witnesses = bounded_tuple(
        geo_witnesses,
        maximum_count=MAX_PROVIDER_RATE_WITNESSES,
        error_message="PTG2 exact billing geo scope is invalid",
    )
    if any(
        type(witness) is not BillingProviderGeoWitness
        for witness in retained_geo_witnesses
    ):
        raise serving_unavailable()
    grouped_witnesses_by_provider: dict[
        tuple[int, str, str], list[BillingProviderGeoWitness]
    ] = {}
    address_by_group: dict[tuple[int, str, str], object] = {}
    for witness in retained_geo_witnesses:
        group_key = (
            witness.address.npi,
            witness.address.address_key,
            witness.address.location_key,
        )
        grouped_witnesses_by_provider.setdefault(group_key, []).append(witness)
        prior_address = address_by_group.setdefault(group_key, witness.address)
        if prior_address != witness.address:
            raise serving_unavailable()
    return {
        group_key: tuple(sorted(witnesses, key=lambda witness: witness.stable_sort_key))
        for group_key, witnesses in grouped_witnesses_by_provider.items()
    }


def group_billing_geo_candidates(
    *,
    binding: PlanReleaseSnapshotBinding,
    serving_tables: PTG2ServingTables,
    code_witnesses: Iterable[BillingCodeWitness],
    geo_witnesses: Iterable[BillingProviderGeoWitness],
) -> tuple[BillingSearchProviderCandidate, ...]:
    """Group every exact rate occurrence before provider-level pagination."""

    if (
        type(binding) is not PlanReleaseSnapshotBinding
        or binding.role != PLAN_RELEASE_IN_NETWORK_ROLE
        or type(serving_tables) is not PTG2ServingTables
        or serving_tables.snapshot_id != binding.snapshot_id
        or not is_release_binding_serving_scope_exact(serving_tables, binding)
    ):
        raise serving_unavailable()
    code_witnesses_by_key = _validated_code_witnesses(code_witnesses)
    candidates: list[BillingSearchProviderCandidate] = []
    for sorted_geo_witnesses in _geo_witnesses_by_provider(geo_witnesses).values():
        referenced_code_keys = {
            witness.provider_rate.code_key for witness in sorted_geo_witnesses
        }
        if not referenced_code_keys.issubset(code_witnesses_by_key):
            raise serving_unavailable()
        candidates.append(
            BillingSearchProviderCandidate(
                binding_ordinal=binding.binding_ordinal,
                snapshot_id=binding.snapshot_id,
                serving_tables=serving_tables,
                address=sorted_geo_witnesses[0].address,
                geo_witnesses=sorted_geo_witnesses,
                code_witnesses_by_key=tuple(
                    (code_key, code_witnesses_by_key[code_key])
                    for code_key in sorted(referenced_code_keys)
                ),
            )
        )
    return tuple(sorted(candidates, key=lambda candidate: candidate.sort_key))


def _validated_candidates(
    candidates: Iterable[BillingSearchProviderCandidate],
) -> tuple[BillingSearchProviderCandidate, ...]:
    retained = bounded_tuple(
        candidates,
        maximum_count=MAX_PROVIDER_RATE_WITNESSES,
        error_message="PTG2 exact billing candidate scope is invalid",
    )
    if any(
        type(candidate) is not BillingSearchProviderCandidate for candidate in retained
    ):
        raise serving_unavailable()
    candidate_keys = tuple(candidate.sort_key for candidate in retained)
    validated_candidate_keys = tuple(
        validate_billing_search_sort_key(candidate_key)
        for candidate_key in candidate_keys
    )
    if candidate_keys != validated_candidate_keys or candidate_keys != tuple(
        sorted(set(candidate_keys))
    ):
        raise serving_unavailable()
    return retained


def _next_hydration_chunk(
    candidates: tuple[BillingSearchProviderCandidate, ...],
    start: int,
    maximum_candidates: int,
) -> tuple[tuple[BillingSearchProviderCandidate, ...], int]:
    if (
        type(MAX_HYDRATION_PARTITIONS) is not int
        or MAX_HYDRATION_PARTITIONS < 1
        or type(maximum_candidates) is not int
        or maximum_candidates < 1
    ):
        raise serving_unavailable()
    selected_candidates: list[BillingSearchProviderCandidate] = []
    price_keys_by_scope: dict[tuple[int, str], set[int]] = {}
    next_index = start
    while (
        next_index < len(candidates) and len(selected_candidates) < maximum_candidates
    ):
        candidate = candidates[next_index]
        scope = (candidate.binding_ordinal, candidate.snapshot_id)
        if (
            scope not in price_keys_by_scope
            and len(price_keys_by_scope) >= MAX_HYDRATION_PARTITIONS
        ):
            break
        combined_price_keys = price_keys_by_scope.get(scope, set()) | set(
            candidate.price_keys
        )
        aggregate_price_key_count = sum(
            len(scope_price_keys)
            for retained_scope, scope_price_keys in price_keys_by_scope.items()
            if retained_scope != scope
        ) + len(combined_price_keys)
        if (
            len(combined_price_keys) > MAX_PRICE_KEYS
            or aggregate_price_key_count > MAX_PRICE_KEYS
        ):
            if not selected_candidates:
                raise serving_unavailable()
            break
        price_keys_by_scope[scope] = combined_price_keys
        selected_candidates.append(candidate)
        next_index += 1
    return tuple(selected_candidates), next_index


def _partition_candidates(
    candidates: tuple[BillingSearchProviderCandidate, ...],
) -> tuple[tuple[BillingSearchProviderCandidate, ...], ...]:
    candidates_by_scope: dict[tuple[int, str], list[BillingSearchProviderCandidate]] = (
        {}
    )
    for candidate in candidates:
        scope = (candidate.binding_ordinal, candidate.snapshot_id)
        scope_candidates = candidates_by_scope.setdefault(scope, [])
        if (
            scope_candidates
            and scope_candidates[0].serving_tables != candidate.serving_tables
        ):
            raise serving_unavailable()
        scope_candidates.append(candidate)
    if len(candidates_by_scope) > MAX_HYDRATION_PARTITIONS:
        raise serving_unavailable()
    return tuple(tuple(scope) for scope in candidates_by_scope.values())


async def _hydrate_partition(
    session,
    candidates: tuple[BillingSearchProviderCandidate, ...],
    price_filter_args: Mapping[str, Any],
    *,
    atom_budget: int,
) -> dict[int, tuple[BillingProviderGeoPriceWitness, ...]]:
    witness_owner_by_id: dict[int, int] = {}
    geo_witnesses: list[BillingProviderGeoWitness] = []
    for candidate_index, candidate in enumerate(candidates):
        for geo_witness in candidate.geo_witnesses:
            witness_id = id(geo_witness)
            if witness_id in witness_owner_by_id:
                raise serving_unavailable()
            witness_owner_by_id[witness_id] = candidate_index
            geo_witnesses.append(geo_witness)
    ordered_geo_witnesses = tuple(
        sorted(geo_witnesses, key=lambda witness: witness.stable_sort_key)
    )
    hydrated = await ptg2_billing_price_reader.hydrate_exact_billing_geo_prices(
        session,
        candidates[0].serving_tables,
        geo_witnesses=ordered_geo_witnesses,
        price_filter_args=price_filter_args,
        atom_budget=atom_budget,
    )
    if type(hydrated) is not tuple:
        raise serving_unavailable()
    input_position_by_witness_id = {
        id(witness): position for position, witness in enumerate(ordered_geo_witnesses)
    }
    hydrated_positions = tuple(
        input_position_by_witness_id.get(id(witness.geo_witness))
        for witness in hydrated
        if type(witness) is BillingProviderGeoPriceWitness
    )
    if (
        len(hydrated_positions) != len(hydrated)
        or any(position is None for position in hydrated_positions)
        or hydrated_positions != tuple(sorted(set(hydrated_positions)))
    ):
        raise serving_unavailable()
    hydrated_by_candidate: dict[int, list[BillingProviderGeoPriceWitness]] = {}
    for price_witness in hydrated:
        candidate_index = witness_owner_by_id[id(price_witness.geo_witness)]
        hydrated_by_candidate.setdefault(candidate_index, []).append(price_witness)
    return {
        candidate_index: tuple(price_witnesses)
        for candidate_index, price_witnesses in hydrated_by_candidate.items()
    }


async def _hydrate_chunk(
    session,
    candidates: tuple[BillingSearchProviderCandidate, ...],
    price_filter_args: Mapping[str, Any],
    *,
    atom_budget: int,
) -> tuple[BillingSearchMatchedProvider, ...]:
    if type(atom_budget) is not int or atom_budget < 0:
        raise serving_unavailable()
    matched_by_key: dict[
        tuple[int | float | str, ...], BillingSearchMatchedProvider
    ] = {}
    retained_atom_count = 0
    for partition in _partition_candidates(candidates):
        hydrated_by_candidate = await _hydrate_partition(
            session,
            partition,
            price_filter_args,
            atom_budget=atom_budget - retained_atom_count,
        )
        for candidate_index, price_witnesses in hydrated_by_candidate.items():
            candidate = partition[candidate_index]
            matched_provider = BillingSearchMatchedProvider(
                candidate,
                price_witnesses,
            )
            retained_atom_count += matched_provider.price_atom_count
            if retained_atom_count > atom_budget:
                raise serving_unavailable()
            matched_by_key[candidate.sort_key] = matched_provider
    return tuple(
        matched_by_key[candidate.sort_key]
        for candidate in candidates
        if candidate.sort_key in matched_by_key
    )


def _candidate_window(
    candidates: tuple[BillingSearchProviderCandidate, ...],
    after_sort_key: object,
) -> tuple[BillingSearchProviderCandidate, ...]:
    validated_after_key = (
        None
        if after_sort_key is None
        else validate_billing_search_sort_key(after_sort_key)
    )
    candidate_keys = {candidate.sort_key for candidate in candidates}
    if validated_after_key is not None and validated_after_key not in candidate_keys:
        raise serving_unavailable()
    return tuple(
        candidate
        for candidate in candidates
        if validated_after_key is None or candidate.sort_key > validated_after_key
    )


def _admit_chunk_work(
    chunk: tuple[BillingSearchProviderCandidate, ...],
    *,
    prior_call_count: int,
    prior_scoped_price_keys: set[tuple[int, str, int]],
) -> tuple[int, set[tuple[int, str, int]]]:
    chunk_scopes = {
        (candidate.binding_ordinal, candidate.snapshot_id) for candidate in chunk
    }
    chunk_scoped_price_keys = {
        (candidate.binding_ordinal, candidate.snapshot_id, price_key)
        for candidate in chunk
        for price_key in candidate.price_keys
    }
    call_count = prior_call_count + len(chunk_scopes)
    scoped_price_keys = prior_scoped_price_keys | chunk_scoped_price_keys
    if (
        call_count > MAX_PAGE_HYDRATION_CALLS
        or len(scoped_price_keys) > MAX_PAGE_SCOPED_PRICE_KEYS
    ):
        raise serving_unavailable()
    return call_count, scoped_price_keys


async def _hydrated_page_matches(
    session,
    *,
    candidates: tuple[BillingSearchProviderCandidate, ...],
    limit: int,
    price_filter_args: Mapping[str, Any],
) -> tuple[BillingSearchMatchedProvider, ...]:
    matched_providers: list[BillingSearchMatchedProvider] = []
    retained_atom_count = 0
    hydration_call_count = 0
    retained_scoped_price_keys: set[tuple[int, str, int]] = set()
    candidate_index = 0
    hydration_candidate_limit = min(
        MAX_HYDRATION_CANDIDATES,
        max(32, limit + 1),
    )
    while candidate_index < len(candidates):
        chunk, candidate_index = _next_hydration_chunk(
            candidates,
            candidate_index,
            hydration_candidate_limit,
        )
        hydration_call_count, retained_scoped_price_keys = _admit_chunk_work(
            chunk,
            prior_call_count=hydration_call_count,
            prior_scoped_price_keys=retained_scoped_price_keys,
        )
        hydrated_chunk = await _hydrate_chunk(
            session,
            chunk,
            dict(price_filter_args),
            atom_budget=MAX_PRICE_ATOMS - retained_atom_count,
        )
        retained_atom_count += sum(
            provider.price_atom_count for provider in hydrated_chunk
        )
        matched_providers.extend(hydrated_chunk)
        if len(matched_providers) > limit:
            break
    return tuple(matched_providers)


def _provider_page(
    matched_providers: tuple[BillingSearchMatchedProvider, ...],
    limit: int,
) -> BillingSearchProviderPage:
    page_providers = matched_providers[:limit]
    has_more = len(matched_providers) > limit
    return BillingSearchProviderPage(
        providers=page_providers,
        has_more=has_more,
        next_sort_key=(page_providers[-1].candidate.sort_key if has_more else None),
    )


async def hydrate_billing_search_page(
    session,
    *,
    candidates: Iterable[BillingSearchProviderCandidate],
    after_sort_key: object,
    limit: int,
    price_filter_args: Mapping[str, Any],
) -> BillingSearchProviderPage:
    """Hydrate bounded chunks until one complete provider page is known."""

    retained_candidates = _validated_candidates(candidates)
    if type(limit) is not int or not 1 <= limit <= BILLING_SEARCH_MAX_LIMIT:
        raise serving_unavailable()
    if not isinstance(price_filter_args, Mapping):
        raise serving_unavailable()
    if (
        type(MAX_PAGE_HYDRATION_CALLS) is not int
        or MAX_PAGE_HYDRATION_CALLS < 1
        or type(MAX_PAGE_SCOPED_PRICE_KEYS) is not int
        or MAX_PAGE_SCOPED_PRICE_KEYS < 1
    ):
        raise serving_unavailable()
    remaining_candidates = _candidate_window(
        retained_candidates,
        after_sort_key,
    )
    matched_providers = await _hydrated_page_matches(
        session,
        candidates=remaining_candidates,
        limit=limit,
        price_filter_args=price_filter_args,
    )
    return _provider_page(matched_providers, limit)


__all__ = [
    "group_billing_geo_candidates",
    "hydrate_billing_search_page",
    "validate_billing_search_sort_key",
]
