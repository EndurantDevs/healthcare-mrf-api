# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dataset selection helpers for one locked Provider Directory Profile view."""

from __future__ import annotations

from typing import Any, Mapping

from process import provider_directory_profile as profile_artifact
from process.provider_directory_profile_selection_contract import _clean_text
from process.provider_directory_profile_uhc_flex import (
    is_uhc_flex_dataset_row_ready,
    is_uhc_flex_dataset_variant_matching,
)


def _metadata_source_ids(metadata_map: Any) -> tuple[str, ...] | None:
    if not isinstance(metadata_map, Mapping):
        return None
    raw_source_ids = metadata_map.get("source_ids")
    if not isinstance(raw_source_ids, list) or not raw_source_ids:
        return None
    source_ids = tuple(
        sorted(
            source_id
            for raw_source_id in raw_source_ids
            if (source_id := _clean_text(raw_source_id))
        )
    )
    if len(source_ids) != len(raw_source_ids) or len(source_ids) != len(
        set(source_ids)
    ):
        return None
    return source_ids


def _source_selection_indexes(
    source_rows: list[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, set[str]]]:
    source_by_id: dict[str, Mapping[str, Any]] = {}
    source_ids_by_endpoint: dict[str, set[str]] = {}
    for source_row in source_rows:
        source_id = _clean_text(source_row.get("source_id"))
        endpoint_id = _clean_text(source_row.get("endpoint_id"))
        if source_id is None or source_id in source_by_id:
            raise RuntimeError("provider_directory_profile_selection_source_invalid")
        source_by_id[source_id] = source_row
        if endpoint_id is not None:
            source_ids_by_endpoint.setdefault(endpoint_id, set()).add(source_id)
    return source_by_id, source_ids_by_endpoint


def _assert_dataset_variant_registry_coordinates(
    source_by_id: Mapping[str, Mapping[str, Any]],
) -> None:
    """Fail until every reviewed logical-generation coordinate is registered."""

    for (
        source_id,
        endpoint_id,
    ) in profile_artifact.configured_dataset_scoped_profile_endpoints():
        source_row = source_by_id.get(source_id)
        if (
            source_row is None
            or _clean_text(source_row.get("endpoint_id")) != endpoint_id
            or profile_artifact.profile_reviewed_source_authority_id(source_id) is None
        ):
            raise RuntimeError(
                "provider_directory_profile_selection_dataset_variant_registry_invalid"
            )


def _variant_source_ids(
    variant_source_groups: tuple[tuple[str, ...], ...],
) -> frozenset[str]:
    return frozenset(
        source_id
        for variant_source_group in variant_source_groups
        for source_id in variant_source_group
    )


def _ordinary_dataset_selection_by_group(
    dataset_rows: list[Mapping[str, Any]],
    source_groups: tuple[tuple[str, ...], ...],
    source_ids_by_endpoint: Mapping[str, set[str]],
    variant_source_groups: tuple[tuple[str, ...], ...],
) -> dict[tuple[str, ...], Mapping[str, Any]]:
    dataset_by_group: dict[tuple[str, ...], Mapping[str, Any]] = {}
    variant_source_ids = _variant_source_ids(variant_source_groups)
    dataset_scoped_source_ids = frozenset(
        profile_artifact.configured_dataset_scoped_profile_source_ids()
    )
    for dataset_row in dataset_rows:
        source_group = _metadata_source_ids(
            dataset_row.get("publication_metadata_json")
        )
        endpoint_id = _clean_text(dataset_row.get("endpoint_id"))
        endpoint_source_ids = source_ids_by_endpoint.get(endpoint_id or "", set())
        if variant_source_ids.intersection(
            source_group or ()
        ) or variant_source_ids.intersection(endpoint_source_ids):
            continue
        is_dataset_scoped = bool(
            source_group and set(source_group).issubset(dataset_scoped_source_ids)
        )
        if (
            source_group in source_groups
            and endpoint_id is not None
            and set(source_group).issubset(endpoint_source_ids)
            and (not is_dataset_scoped or is_uhc_flex_dataset_row_ready(dataset_row))
        ):
            dataset_by_group.setdefault(source_group, dataset_row)
    return dataset_by_group


def _current_variant_dataset(
    dataset_rows: list[Mapping[str, Any]],
    variant_source_group: tuple[str, ...],
    reviewed_endpoint_by_source_id: Mapping[str, str],
) -> tuple[str, Mapping[str, Any]] | None:
    current_rows: list[tuple[str, Mapping[str, Any]]] = []
    for dataset_row in dataset_rows:
        endpoint_id = _clean_text(dataset_row.get("endpoint_id"))
        endpoint_variant_sources = {
            source_id
            for source_id in variant_source_group
            if reviewed_endpoint_by_source_id.get(source_id) == endpoint_id
        }
        if not endpoint_variant_sources:
            continue
        if len(endpoint_variant_sources) != 1:
            raise RuntimeError(
                "provider_directory_profile_selection_dataset_variant_invalid"
            )
        source_id = next(iter(endpoint_variant_sources))
        source_group = _metadata_source_ids(
            dataset_row.get("publication_metadata_json")
        )
        if source_group != (source_id,) or not is_uhc_flex_dataset_variant_matching(
            source_id,
            dataset_row.get("dataset_id"),
        ):
            raise RuntimeError(
                "provider_directory_profile_selection_dataset_variant_invalid"
            )
        current_rows.append((source_id, dataset_row))
    if len(current_rows) > 1:
        raise RuntimeError(
            "provider_directory_profile_selection_dataset_variant_ambiguous"
        )
    return current_rows[0] if current_rows else None


def _dataset_selection_by_group(
    dataset_rows: list[Mapping[str, Any]],
    source_groups: tuple[tuple[str, ...], ...],
    source_ids_by_endpoint: Mapping[str, set[str]],
    variant_source_groups: tuple[tuple[str, ...], ...] = (),
) -> dict[tuple[str, ...], Mapping[str, Any]]:
    """Select at most one ready current dataset for every logical group."""

    dataset_by_group = _ordinary_dataset_selection_by_group(
        dataset_rows,
        source_groups,
        source_ids_by_endpoint,
        variant_source_groups,
    )
    reviewed_endpoint_by_source_id = dict(
        profile_artifact.configured_dataset_scoped_profile_endpoints()
    )
    for variant_source_group in variant_source_groups:
        current_variant = _current_variant_dataset(
            dataset_rows,
            variant_source_group,
            reviewed_endpoint_by_source_id,
        )
        if current_variant is None:
            continue
        source_id, dataset_row = current_variant
        if is_uhc_flex_dataset_row_ready(dataset_row):
            dataset_by_group[(source_id,)] = dataset_row
    return dataset_by_group


__all__ = (
    "_assert_dataset_variant_registry_coordinates",
    "_dataset_selection_by_group",
    "_metadata_source_ids",
    "_source_selection_indexes",
)
