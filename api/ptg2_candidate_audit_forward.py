# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Budgeted forward-index transformations for candidate audits."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Sequence

from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    retain_unique_integer_key_set,
)
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_price_load import (
    CandidatePriceLoad,
    build_candidate_price_load,
)
from api.ptg2_candidate_audit_occurrences import (
    required_candidate_occurrence_keys,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


_PRELOADED_FORWARD_MAP_BYTES = 224
_PRELOADED_FORWARD_OCCURRENCE_BYTES = 256
_PRELOADED_FORWARD_PRICE_BYTES = 48


@dataclass(frozen=True)
class CandidatePriceLoaders:
    """Call seams required by the candidate price-loading pipeline."""

    forward_price_keys: Callable[..., Awaitable[dict[Any, tuple[int, ...]]]]
    hydrate_prices: Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class _CandidatePriceScope:
    provider_sets_by_npi_code: Mapping[tuple[int, int], tuple[int, ...]]
    provider_filters_by_code_key: Mapping[int, tuple[int, ...]]
    persisted_audit_occurrences: Sequence[PersistedAuditOccurrence]


def _candidate_price_scope(scope_arguments: tuple[Any, ...]) -> _CandidatePriceScope:
    """Normalize the compatibility call shape into one explicit scope."""

    if len(scope_arguments) not in {2, 3}:
        raise TypeError("candidate price scope requires provider indexes")
    return _CandidatePriceScope(
        provider_sets_by_npi_code=scope_arguments[0],
        provider_filters_by_code_key=scope_arguments[1],
        persisted_audit_occurrences=(
            scope_arguments[2] if len(scope_arguments) == 3 else ()
        ),
    )


async def _hydrate_candidate_prices(
    session: Any,
    serving_tables: PTG2ServingTables,
    price_keys_by_occurrence: Mapping[Any, tuple[int, ...]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    hydration_loader: Callable[..., Awaitable[Any]],
) -> Any:
    retained_price_key_set, retained_price_key_bytes = retain_unique_integer_key_set(
        (
            price_key
            for occurrence_price_keys in price_keys_by_occurrence.values()
            for price_key in occurrence_price_keys
        ),
        retention_budget,
        category="candidate hydration price",
    )
    hydration_argument_map: dict[str, Any] = {"copy_payloads": False}
    if retention_budget is not None:
        hydration_argument_map["retention_budget"] = retention_budget
    try:
        return await hydration_loader(
            session,
            serving_tables,
            retained_price_key_set,
            **hydration_argument_map,
        )
    finally:
        if retention_budget is not None:
            retention_budget.release(retained_price_key_bytes)


async def _candidate_price_index(
    session: Any,
    serving_tables: PTG2ServingTables,
    price_scope: _CandidatePriceScope,
    required_occurrence_keys: frozenset[tuple[int, int, int]],
    preloaded_price_keys_by_occurrence: Mapping[Any, tuple[int, ...]] | None,
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    forward_loader: Callable[..., Awaitable[dict[Any, tuple[int, ...]]]],
) -> dict[tuple[int, int, int], tuple[int, ...]]:
    if preloaded_price_keys_by_occurrence is not None:
        return filter_preloaded_price_index(
            preloaded_price_keys_by_occurrence,
            required_occurrence_keys,
            retention_budget,
        )
    return await forward_loader(
        session,
        serving_tables,
        price_scope.provider_filters_by_code_key,
        required_occurrence_keys,
        retention_budget,
    )


async def load_candidate_price_data(
    session: Any,
    serving_tables: PTG2ServingTables,
    challenges: Sequence[AuditBatchChallenge],
    code_records_by_pair: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    scope_arguments: tuple[Any, ...],
    preloaded_price_keys_by_occurrence: Mapping[Any, tuple[int, ...]] | None,
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
    price_loaders: CandidatePriceLoaders,
) -> CandidatePriceLoad:
    """Retain exact forward rows, then hydrate each retained price once."""

    price_scope = _candidate_price_scope(scope_arguments)
    required_keys = required_candidate_occurrence_keys(
        challenges,
        code_records_by_pair,
        price_scope.provider_sets_by_npi_code,
        price_scope.persisted_audit_occurrences,
        retention_budget,
    )
    price_keys_by_occurrence = await _candidate_price_index(
        session,
        serving_tables,
        price_scope,
        required_keys,
        preloaded_price_keys_by_occurrence,
        retention_budget,
        price_loaders.forward_price_keys,
    )
    if any(
        occurrence_key not in required_keys
        for occurrence_key in price_keys_by_occurrence
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 candidate forward read escaped its exact occurrence scope"
        )
    hydration = await _hydrate_candidate_prices(
        session,
        serving_tables,
        price_keys_by_occurrence,
        retention_budget,
        price_loaders.hydrate_prices,
    )
    return build_candidate_price_load(
        price_keys_by_occurrence,
        required_keys,
        hydration,
    )


def filter_preloaded_price_index(
    preloaded_price_keys_by_occurrence: Mapping[tuple[int, int, int], tuple[int, ...]],
    required_occurrence_keys: frozenset[tuple[int, int, int]],
    retention_budget: CandidateAuditDecodedRetentionBudget | None,
) -> dict[tuple[int, int, int], tuple[int, ...]]:
    """Retain only exact preloaded coordinates after claiming each row."""

    retained_result_bytes = _PRELOADED_FORWARD_MAP_BYTES
    if retention_budget is not None:
        retention_budget.claim(
            retained_result_bytes,
            category="the filtered preloaded forward map",
        )
    filtered_price_keys_by_occurrence: dict[tuple[int, int, int], tuple[int, ...]] = {}
    try:
        for occurrence_key, price_keys in preloaded_price_keys_by_occurrence.items():
            if occurrence_key not in required_occurrence_keys:
                continue
            occurrence_bytes = (
                _PRELOADED_FORWARD_OCCURRENCE_BYTES
                + len(price_keys) * _PRELOADED_FORWARD_PRICE_BYTES
            )
            if retention_budget is not None:
                retention_budget.claim(
                    occurrence_bytes,
                    category="a filtered preloaded forward occurrence",
                )
                retained_result_bytes += occurrence_bytes
            filtered_price_keys_by_occurrence[occurrence_key] = tuple(price_keys)
    except BaseException:
        if retention_budget is not None:
            retention_budget.release(retained_result_bytes)
        raise
    return filtered_price_keys_by_occurrence


__all__ = [
    "CandidatePriceLoaders",
    "filter_preloaded_price_index",
    "load_candidate_price_data",
]
