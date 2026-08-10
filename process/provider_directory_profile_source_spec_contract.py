# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed validation for the reviewed Provider Directory Profile source spec."""

from __future__ import annotations

import re
from typing import Any, Mapping


_SPEC_INVALID = "provider_directory_profile_source_spec_invalid"


def _fail_spec_validation() -> None:
    raise RuntimeError(_SPEC_INVALID)


def _is_unique_list(raw_values: object) -> bool:
    return isinstance(raw_values, list) and len(raw_values) == len(set(raw_values))


def _is_source_id_list(source_ids: object) -> bool:
    return bool(
        _is_unique_list(source_ids)
        and source_ids
        and all(
            isinstance(source_id, str)
            and source_id.startswith("pdfhir_")
            and len(source_id) > len("pdfhir_")
            for source_id in source_ids
        )
    )


def _is_entry_id_list(entry_ids: object) -> bool:
    return bool(
        _is_unique_list(entry_ids)
        and entry_ids
        and all(isinstance(entry_id, str) and entry_id for entry_id in entry_ids)
    )


def _is_entry_subset(raw_entry_ids: object, entry_ids: list[str]) -> bool:
    return bool(
        _is_unique_list(raw_entry_ids)
        and all(
            isinstance(entry_id, str) and entry_id in entry_ids
            for entry_id in raw_entry_ids
        )
    )


def _is_variant_group(
    raw_group: object,
    dataset_scoped_entry_ids: list[str],
) -> bool:
    if not isinstance(raw_group, dict) or set(raw_group) != {"group_id", "entry_ids"}:
        return False
    group_id = raw_group.get("group_id")
    group_entry_ids = raw_group.get("entry_ids")
    return bool(
        isinstance(group_id, str)
        and group_id
        and group_id == group_id.strip()
        and len(group_id) <= 96
        and _is_entry_subset(group_entry_ids, dataset_scoped_entry_ids)
        and len(group_entry_ids) >= 2
    )


def _is_variant_group_collection_valid(
    variant_groups: object,
    dataset_scoped_entry_ids: list[str],
) -> bool:
    if not isinstance(variant_groups, list) or not all(
        _is_variant_group(group, dataset_scoped_entry_ids) for group in variant_groups
    ):
        return False
    group_ids = [group["group_id"] for group in variant_groups]
    grouped_entry_ids = [
        entry_id for group in variant_groups for entry_id in group["entry_ids"]
    ]
    return bool(
        len(group_ids) == len(set(group_ids))
        and len(grouped_entry_ids) == len(set(grouped_entry_ids))
        and set(grouped_entry_ids) == set(dataset_scoped_entry_ids)
    )


def _is_authority_map_valid(
    authority_by_source_id: object,
    source_ids: list[str],
) -> bool:
    return bool(
        isinstance(authority_by_source_id, dict)
        and all(
            isinstance(source_id, str)
            and source_id in source_ids
            and isinstance(authority_id, str)
            and authority_id
            and authority_id == authority_id.strip()
            and len(authority_id) <= 96
            for source_id, authority_id in authority_by_source_id.items()
        )
    )


def _is_dataset_endpoint_map_valid(
    endpoint_by_source_id: object,
    source_ids: list[str],
) -> bool:
    return bool(
        isinstance(endpoint_by_source_id, dict)
        and all(
            isinstance(source_id, str)
            and source_id in source_ids
            and isinstance(endpoint_id, str)
            and re.fullmatch(r"[0-9a-f]{64}", endpoint_id) is not None
            for source_id, endpoint_id in endpoint_by_source_id.items()
        )
    )


def _source_rows(source_spec_map: Mapping[str, Any]) -> list[dict[str, Any]]:
    matrix = source_spec_map.get("verification_matrix")
    source_rows = matrix.get("sources") if isinstance(matrix, dict) else None
    if not isinstance(source_rows, list) or any(
        not isinstance(source_row, dict) for source_row in source_rows
    ):
        _fail_spec_validation()
    return source_rows


def _dataset_source_by_entry_id(
    source_rows: list[dict[str, Any]],
    dataset_scoped_entry_ids: list[str],
) -> dict[str, Any]:
    return {
        source_row.get("entry_id"): source_row.get("source_id")
        for source_row in source_rows
        if source_row.get("entry_id") in dataset_scoped_entry_ids
    }


def _assert_dataset_source_coordinates(
    source_spec_map: Mapping[str, Any],
    source_ids: list[str],
    dataset_scoped_entry_ids: list[str],
    variant_groups: list[dict[str, Any]],
) -> None:
    source_id_by_entry_id = _dataset_source_by_entry_id(
        _source_rows(source_spec_map),
        dataset_scoped_entry_ids,
    )
    endpoint_by_source_id = source_spec_map["dataset_scoped_endpoint_ids_by_source_id"]
    if (
        set(source_id_by_entry_id) != set(dataset_scoped_entry_ids)
        or any(
            not isinstance(source_id, str) or source_id not in source_ids
            for source_id in source_id_by_entry_id.values()
        )
        or set(endpoint_by_source_id) != set(source_id_by_entry_id.values())
    ):
        _fail_spec_validation()
    _assert_variant_group_coordinates(
        source_spec_map,
        source_id_by_entry_id,
        variant_groups,
    )


def _assert_variant_group_coordinates(
    source_spec_map: Mapping[str, Any],
    source_id_by_entry_id: Mapping[str, str],
    variant_groups: list[dict[str, Any]],
) -> None:
    authority_by_source_id = source_spec_map["authority_ids_by_source_id"]
    endpoint_by_source_id = source_spec_map["dataset_scoped_endpoint_ids_by_source_id"]
    for group in variant_groups:
        group_source_ids = tuple(
            source_id_by_entry_id[entry_id] for entry_id in group["entry_ids"]
        )
        group_authority_ids = tuple(
            authority_by_source_id.get(source_id) for source_id in group_source_ids
        )
        group_endpoint_ids = tuple(
            endpoint_by_source_id.get(source_id) for source_id in group_source_ids
        )
        if (
            len(set(group_source_ids)) != len(group_source_ids)
            or None in group_authority_ids
            or len(set(group_authority_ids)) != 1
            or None in group_endpoint_ids
            or len(set(group_endpoint_ids)) != len(group_endpoint_ids)
        ):
            _fail_spec_validation()


def validated_profile_source_spec(raw_source_spec: object) -> dict[str, Any]:
    """Return one source spec only after all reviewed coordinates validate."""

    if (
        not isinstance(raw_source_spec, dict)
        or raw_source_spec.get("schema_version") != 1
    ):
        _fail_spec_validation()
    source_ids = raw_source_spec.get("source_ids")
    entry_ids = raw_source_spec.get("entry_ids")
    retained_entry_ids = raw_source_spec.get("retained_entry_ids", [])
    dataset_scoped_entry_ids = raw_source_spec.get("dataset_scoped_entry_ids", [])
    variant_groups = raw_source_spec.get("dataset_scoped_variant_groups", [])
    authority_by_source_id = raw_source_spec.get("authority_ids_by_source_id", {})
    endpoint_by_source_id = raw_source_spec.get(
        "dataset_scoped_endpoint_ids_by_source_id", {}
    )
    if not (
        _is_source_id_list(source_ids)
        and _is_entry_id_list(entry_ids)
        and _is_entry_subset(retained_entry_ids, entry_ids)
        and _is_entry_subset(dataset_scoped_entry_ids, entry_ids)
        and _is_variant_group_collection_valid(
            variant_groups,
            dataset_scoped_entry_ids,
        )
        and _is_authority_map_valid(authority_by_source_id, source_ids)
        and _is_dataset_endpoint_map_valid(endpoint_by_source_id, source_ids)
    ):
        _fail_spec_validation()
    _assert_dataset_source_coordinates(
        raw_source_spec,
        source_ids,
        dataset_scoped_entry_ids,
        variant_groups,
    )
    return raw_source_spec


__all__ = ("validated_profile_source_spec",)
