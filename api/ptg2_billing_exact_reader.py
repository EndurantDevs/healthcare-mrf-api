# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact source/group/set/rate witnesses for one billing identity."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from api import ptg2_db_sidecars, ptg2_serving
from api.ptg2_billing_entity_source_resolution import (
    ResolvedBillingEntitySourceScope,
)
from api.ptg2_billing_exact_contract import (
    MAX_PRICE_KEY as _MAX_PRICE_KEY,
    BillingRateOccurrenceWitness,
    ExactGroupProjection as _ExactGroupProjection,
    ExactSetProjection as _ExactSetProjection,
    billing_rate_occurrence_sort_key as _billing_rate_occurrence_sort_key,
    canonical_ref as _canonical_ref,
    distinct_dense_keys as _distinct_dense_keys,
    normalized_dense_key as _normalized_dense_key,
    source_groups as _source_groups,
)
from api.ptg2_candidate_audit_capacity import CandidateAuditDecodedRetentionBudget
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

_MAX_CODE_KEYS = 64
_MAX_ASSOCIATION_EDGES = 8192
_MAX_SOURCE_GROUP_SET_EDGES = 8192
_MAX_FORWARD_FILTER_COORDINATES = 16384
_MAX_FORWARD_OCCURRENCES = 32768
_MAX_RATE_WITNESSES = 32768


def _validated_dictionary(
    values_by_id: Mapping[str, int],
    *,
    expected_ids: Iterable[str],
    category: str,
) -> dict[str, int]:
    expected_id_set = {
        _canonical_ref(identifier, category=category) for identifier in expected_ids
    }
    if set(values_by_id) != expected_id_set:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing references an unknown {category}"
        )
    normalized_values = _distinct_dense_keys(
        values_by_id.values(),
        category=category,
        maximum_count=_MAX_ASSOCIATION_EDGES,
    )
    if len(normalized_values) != len(values_by_id):
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing {category} keys are inconsistent"
        )
    return dict(values_by_id)


def _validated_sets_by_group(
    sets_by_group: Mapping[str, tuple[str, ...]],
    *,
    provider_group_refs: tuple[str, ...],
) -> dict[str, tuple[str, ...]]:
    if set(sets_by_group) != set(provider_group_refs):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing group-to-set projection is incomplete"
        )
    association_edge_count = 0
    for provider_set_ids in sets_by_group.values():
        if type(provider_set_ids) is not tuple:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-set projection is malformed"
            )
        association_edge_count += len(provider_set_ids)
        if association_edge_count > _MAX_ASSOCIATION_EDGES:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-set projection exceeds its edge limit"
            )
        normalized_provider_set_ids = tuple(
            _canonical_ref(provider_set_id, category="provider set")
            for provider_set_id in provider_set_ids
        )
        if len(normalized_provider_set_ids) != len(set(normalized_provider_set_ids)):
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-set projection contains duplicates"
            )
    return dict(sets_by_group)


def _group_refs_by_key(
    group_keys_by_id: Mapping[str, int],
) -> dict[int, str]:
    group_ref_by_key = {
        group_key: group_ref for group_ref, group_key in group_keys_by_id.items()
    }
    if len(group_ref_by_key) != len(group_keys_by_id):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider-group keys are inconsistent"
        )
    return group_ref_by_key


def _validated_exact_groups_by_set(
    group_keys_by_set: Mapping[int, tuple[int, ...]],
    *,
    sets_by_group: Mapping[str, tuple[str, ...]],
    provider_set_keys_by_id: Mapping[str, int],
    group_keys_by_id: Mapping[str, int],
) -> dict[int, tuple[int, ...]]:
    expected_groups_by_set: dict[int, set[int]] = {
        provider_set_key: set() for provider_set_key in provider_set_keys_by_id.values()
    }
    for provider_group_ref, provider_set_ids in sets_by_group.items():
        provider_group_key = group_keys_by_id[provider_group_ref]
        for provider_set_id in provider_set_ids:
            expected_groups_by_set[provider_set_keys_by_id[provider_set_id]].add(
                provider_group_key
            )
    expected_group_keys_by_set = {
        provider_set_key: tuple(sorted(provider_group_keys))
        for provider_set_key, provider_group_keys in expected_groups_by_set.items()
    }
    actual_group_keys_by_set: dict[int, tuple[int, ...]] = {}
    for provider_set_key, provider_group_keys in group_keys_by_set.items():
        normalized_set_key = _normalized_dense_key(
            provider_set_key,
            category="provider set",
        )
        if normalized_set_key in actual_group_keys_by_set:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-set projections are inconsistent"
            )
        if type(provider_group_keys) is not tuple:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing group-to-set projections are inconsistent"
            )
        actual_group_keys_by_set[normalized_set_key] = _distinct_dense_keys(
            provider_group_keys,
            category="provider group",
            maximum_count=_MAX_ASSOCIATION_EDGES,
        )
    if actual_group_keys_by_set != expected_group_keys_by_set:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing group-to-set projections are inconsistent"
        )
    return actual_group_keys_by_set


def _source_set_coordinates(
    *,
    group_refs_by_source: Mapping[int, Mapping[str, int]],
    sets_by_group: Mapping[str, tuple[str, ...]],
    provider_set_keys_by_id: Mapping[str, int],
    code_keys: tuple[int, ...],
) -> tuple[tuple[int, int, int], ...]:
    source_group_set_count = 0
    set_source_coordinates: set[tuple[int, int]] = set()
    for source_key in sorted(group_refs_by_source):
        for provider_group_ref in sorted(group_refs_by_source[source_key]):
            for provider_set_id in sets_by_group[provider_group_ref]:
                source_group_set_count += 1
                if source_group_set_count > _MAX_SOURCE_GROUP_SET_EDGES:
                    raise PTG2ManifestArtifactError(
                        "PTG2 exact billing source/group/set scope exceeds its edge limit"
                    )
                set_source_coordinates.add(
                    (provider_set_keys_by_id[provider_set_id], source_key)
                )
    coordinate_count = len(code_keys) * len(set_source_coordinates)
    if coordinate_count > _MAX_FORWARD_FILTER_COORDINATES:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing forward filter scope exceeds its coordinate limit"
        )
    return tuple(
        (code_key, provider_set_key, source_key)
        for code_key in code_keys
        for provider_set_key, source_key in sorted(set_source_coordinates)
    )


def _validated_occurrences_by_code(
    occurrences_by_code: Mapping[int, tuple[tuple[int, int, int], ...]],
    *,
    code_keys: tuple[int, ...],
    provider_set_keys: frozenset[int],
    source_keys: frozenset[int],
    allowed_set_source_coordinates: frozenset[tuple[int, int]],
    price_item_count: int,
) -> dict[int, tuple[tuple[int, int, int], ...]]:
    normalized_result_code_keys = _distinct_dense_keys(
        occurrences_by_code,
        category="forward occurrence code",
        maximum_count=_MAX_CODE_KEYS,
    )
    if normalized_result_code_keys != code_keys:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing forward occurrence projection is incomplete"
        )
    validated_by_code: dict[int, tuple[tuple[int, int, int], ...]] = {}
    for code_key in code_keys:
        normalized_occurrences: list[tuple[int, int, int]] = []
        code_occurrences = occurrences_by_code[code_key]
        if type(code_occurrences) is not tuple:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing forward occurrence projection is malformed"
            )
        for occurrence in code_occurrences:
            if type(occurrence) is not tuple or len(occurrence) != 3:
                raise PTG2ManifestArtifactError(
                    "PTG2 exact billing forward occurrence is malformed"
                )
            provider_set_key = _normalized_dense_key(
                occurrence[0],
                category="forward occurrence provider set",
            )
            price_key = _normalized_dense_key(
                occurrence[1],
                category="forward occurrence price",
                maximum=_MAX_PRICE_KEY,
            )
            source_key = _normalized_dense_key(
                occurrence[2],
                category="forward occurrence source",
            )
            if (
                provider_set_key not in provider_set_keys
                or source_key not in source_keys
                or (provider_set_key, source_key) not in allowed_set_source_coordinates
                or price_key >= price_item_count
            ):
                raise PTG2ManifestArtifactError(
                    "PTG2 exact billing forward occurrence escaped its scope"
                )
            normalized_occurrences.append((provider_set_key, price_key, source_key))
        validated_by_code[code_key] = tuple(sorted(normalized_occurrences))
    return validated_by_code


def _materialize_rate_witnesses(
    *,
    snapshot_key: int,
    occurrences_by_code: Mapping[int, tuple[tuple[int, int, int], ...]],
    group_keys_by_set: Mapping[int, tuple[int, ...]],
    group_ref_by_key: Mapping[int, str],
    group_refs_by_source: Mapping[int, Mapping[str, int]],
) -> tuple[BillingRateOccurrenceWitness, ...]:
    witnesses: list[BillingRateOccurrenceWitness] = []
    for code_key in sorted(occurrences_by_code):
        for occurrence_ordinal, (
            provider_set_key,
            price_key,
            source_key,
        ) in enumerate(occurrences_by_code[code_key]):
            exact_group_refs = {
                group_ref_by_key[group_key]
                for group_key in group_keys_by_set[provider_set_key]
            }
            for provider_group_ref in sorted(
                exact_group_refs & set(group_refs_by_source[source_key])
            ):
                witnesses.append(
                    BillingRateOccurrenceWitness(
                        snapshot_key=snapshot_key,
                        code_key=code_key,
                        source_key=source_key,
                        source_record_ordinal=group_refs_by_source[source_key][
                            provider_group_ref
                        ],
                        provider_group_ref=provider_group_ref,
                        provider_set_key=provider_set_key,
                        price_key=price_key,
                        occurrence_ordinal=occurrence_ordinal,
                    )
                )
                if len(witnesses) > _MAX_RATE_WITNESSES:
                    raise PTG2ManifestArtifactError(
                        "PTG2 exact billing rate witness scope exceeds its limit"
                    )
    return tuple(sorted(witnesses, key=_billing_rate_occurrence_sort_key))


async def _load_group_projection(
    session,
    serving_tables: PTG2ServingTables,
    *,
    source_scope: ResolvedBillingEntitySourceScope,
) -> _ExactGroupProjection:
    """Load and validate the source-local provider-group projection."""

    snapshot_key = ptg2_serving._required_shared_snapshot_key(serving_tables)
    source_count = ptg2_serving._required_source_count(serving_tables)
    group_refs_by_source = _source_groups(
        source_scope,
        snapshot_key=snapshot_key,
        source_count=source_count,
        source_publication=(
            serving_tables.provider_tax_identity_source_publication
        ),
    )
    provider_group_refs = tuple(
        sorted(
            {
                provider_group_ref
                for group_refs in group_refs_by_source.values()
                for provider_group_ref in group_refs
            }
        )
    )
    sets_by_group = _validated_sets_by_group(
        await ptg2_serving._manifest_sets_by_group(
            session,
            serving_tables,
            provider_group_refs,
            max_members=_MAX_ASSOCIATION_EDGES,
        ),
        provider_group_refs=provider_group_refs,
    )
    group_keys_by_id = _validated_dictionary(
        await ptg2_serving._shared_provider_group_keys_for_ids(
            session,
            serving_tables,
            provider_group_refs,
        ),
        expected_ids=provider_group_refs,
        category="provider group",
    )
    return _ExactGroupProjection(
        snapshot_key=snapshot_key,
        source_count=source_count,
        group_refs_by_source=group_refs_by_source,
        provider_group_refs=provider_group_refs,
        sets_by_group=sets_by_group,
        group_keys_by_id=group_keys_by_id,
    )


async def _load_set_projection(
    session,
    serving_tables: PTG2ServingTables,
    group_projection: _ExactGroupProjection,
) -> _ExactSetProjection | None:
    """Load exact provider-set dictionaries and reverse group membership."""

    provider_set_ids = tuple(
        sorted(
            {
                provider_set_id
                for group_set_ids in group_projection.sets_by_group.values()
                for provider_set_id in group_set_ids
            }
        )
    )
    if not provider_set_ids:
        return None
    provider_set_keys_by_id = _validated_dictionary(
        await ptg2_serving._provider_set_keys_for_ids(
            session,
            serving_tables,
            provider_set_ids,
        ),
        expected_ids=provider_set_ids,
        category="provider set",
    )
    group_keys_by_set = _validated_exact_groups_by_set(
        await ptg2_serving._v4_exact_groups_by_set(
            session,
            serving_tables,
            provider_set_keys=provider_set_keys_by_id.values(),
            exact_group_keys=group_projection.group_keys_by_id.values(),
        ),
        sets_by_group=group_projection.sets_by_group,
        provider_set_keys_by_id=provider_set_keys_by_id,
        group_keys_by_id=group_projection.group_keys_by_id,
    )
    return _ExactSetProjection(provider_set_keys_by_id, group_keys_by_set)


def _price_item_count(forward_lookup_hints: Mapping[str, Any]) -> int:
    price_item_count = forward_lookup_hints["price_dictionary_item_count"]
    if (
        type(price_item_count) is not int
        or not 0 <= price_item_count <= _MAX_PRICE_KEY + 1
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing price dictionary size is invalid"
        )
    return price_item_count


async def _load_forward_occurrences(
    session,
    serving_tables: PTG2ServingTables,
    *,
    code_keys: tuple[int, ...],
    group_projection: _ExactGroupProjection,
    set_projection: _ExactSetProjection,
) -> dict[int, tuple[tuple[int, int, int], ...]]:
    """Read and validate only the requested source/set/code coordinates."""

    provider_set_keys_by_id = set_projection.provider_set_keys_by_id
    provider_set_keys = frozenset(provider_set_keys_by_id.values())
    source_keys = frozenset(group_projection.group_refs_by_source)
    lookup_hints = ptg2_serving._version_three_forward_lookup_hints(serving_tables)
    occurrence_keys = _source_set_coordinates(
        group_refs_by_source=group_projection.group_refs_by_source,
        sets_by_group=group_projection.sets_by_group,
        provider_set_keys_by_id=provider_set_keys_by_id,
        code_keys=code_keys,
    )
    retention_budget = CandidateAuditDecodedRetentionBudget()
    lookup_forward = ptg2_db_sidecars.lookup_forward_occurrences_batch_from_db
    occurrences_by_code = await lookup_forward(
        session,
        code_keys,
        shared_snapshot_key=group_projection.snapshot_key,
        source_count=group_projection.source_count,
        provider_set_keys_by_code={
            code_key: provider_set_keys for code_key in code_keys
        },
        source_keys_by_code={code_key: source_keys for code_key in code_keys},
        occurrence_keys=occurrence_keys,
        max_occurrences=_MAX_FORWARD_OCCURRENCES,
        retention_budget=retention_budget,
        schema_name=ptg2_serving.PTG2_SCHEMA,
        **lookup_hints,
    )
    try:
        return _validated_occurrences_by_code(
            occurrences_by_code,
            code_keys=code_keys,
            provider_set_keys=provider_set_keys,
            source_keys=source_keys,
            allowed_set_source_coordinates=frozenset(
                (provider_set_key, source_key)
                for _code_key, provider_set_key, source_key in occurrence_keys
            ),
            price_item_count=_price_item_count(lookup_hints),
        )
    finally:
        retained_bytes = ptg2_db_sidecars.forward_occurrence_batch_retained_bytes(
            occurrences_by_code
        )
        retention_budget.release(retained_bytes)


async def load_exact_billing_rate_occurrence_witnesses(
    session,
    serving_tables: PTG2ServingTables,
    *,
    source_scope: ResolvedBillingEntitySourceScope,
    code_keys: Iterable[int],
) -> tuple[BillingRateOccurrenceWitness, ...]:
    """Traverse one sealed billing scope to its exact code/rate occurrences."""

    if not serving_tables.uses_v4_graph:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing search requires the sealed V4 provider graph"
        )
    normalized_code_keys = _distinct_dense_keys(
        code_keys,
        category="code",
        maximum_count=_MAX_CODE_KEYS,
    )
    if not normalized_code_keys:
        return ()
    group_projection = await _load_group_projection(
        session,
        serving_tables,
        source_scope=source_scope,
    )
    set_projection = await _load_set_projection(
        session,
        serving_tables,
        group_projection,
    )
    if set_projection is None:
        return ()
    occurrences_by_code = await _load_forward_occurrences(
        session,
        serving_tables,
        code_keys=normalized_code_keys,
        group_projection=group_projection,
        set_projection=set_projection,
    )
    return _materialize_rate_witnesses(
        snapshot_key=group_projection.snapshot_key,
        occurrences_by_code=occurrences_by_code,
        group_keys_by_set=set_projection.group_keys_by_set,
        group_ref_by_key=_group_refs_by_key(group_projection.group_keys_by_id),
        group_refs_by_source=group_projection.group_refs_by_source,
    )


__all__ = [
    "BillingRateOccurrenceWitness",
    "load_exact_billing_rate_occurrence_witnesses",
]
