# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed parsing contracts for Provider Directory Profile capacity geometry."""

from __future__ import annotations

import datetime
from typing import Any, Mapping

from process.provider_directory_profile_capacity_types import (
    ProviderDirectoryProfileCapacityError,
    ProviderDirectoryProfileRelationByteCaps,
    _DATE_PATTERN,
    _HASH_PATTERN,
    _MAX_SIGNED_BIGINT,
    _MAX_UNSIGNED_BIGINT,
    _RELATION_FIELDS,
    _RELATION_NAMES,
    _SCRATCH_RELATION_NAMES,
    _SYSTEM_IDENTIFIER_PATTERN,
    _TARGET_RELATION_NAMES,
)


def _error(reason: str) -> ProviderDirectoryProfileCapacityError:
    return ProviderDirectoryProfileCapacityError(
        f"provider_directory_profile_capacity_{reason}"
    )


def _exact_fields(
    value_map: Mapping[str, Any],
    expected_fields: frozenset[str],
    *,
    name: str,
) -> None:
    if not isinstance(value_map, Mapping) or set(value_map) != expected_fields:
        raise _error(f"{name}_fields_invalid")


def _exact_text(
    value_map: Mapping[str, Any],
    name: str,
    *,
    maximum_length: int,
) -> str:
    field_value = value_map.get(name)
    if (
        not isinstance(field_value, str)
        or not field_value
        or field_value != field_value.strip()
        or len(field_value) > maximum_length
    ):
        raise _error(f"{name}_invalid")
    return field_value


def _exact_hash(value_map: Mapping[str, Any], name: str) -> str:
    field_value = _exact_text(value_map, name, maximum_length=64)
    if not _HASH_PATTERN.fullmatch(field_value):
        raise _error(f"{name}_invalid")
    return field_value


def _bounded_integer(
    value_map: Mapping[str, Any],
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    field_value = value_map.get(name)
    if (
        not isinstance(field_value, int)
        or isinstance(field_value, bool)
        or not minimum <= field_value <= maximum
    ):
        raise _error(f"{name}_invalid")
    return field_value


def _positive_bigint(value_map: Mapping[str, Any], name: str) -> int:
    return _bounded_integer(
        value_map,
        name,
        minimum=1,
        maximum=_MAX_SIGNED_BIGINT,
    )


def _nonnegative_bigint(value_map: Mapping[str, Any], name: str) -> int:
    return _bounded_integer(
        value_map,
        name,
        minimum=0,
        maximum=_MAX_SIGNED_BIGINT,
    )


def _database_system_identifier(
    geometry_map: Mapping[str, Any],
) -> str:
    identifier = _exact_text(
        geometry_map,
        "database_system_identifier",
        maximum_length=20,
    )
    if (
        not _SYSTEM_IDENTIFIER_PATTERN.fullmatch(identifier)
        or int(identifier) > _MAX_UNSIGNED_BIGINT
    ):
        raise _error("database_system_identifier_invalid")
    return identifier


def _profile_as_of(geometry_map: Mapping[str, Any]) -> str:
    profile_date = _exact_text(
        geometry_map,
        "profile_as_of",
        maximum_length=10,
    )
    if not _DATE_PATTERN.fullmatch(profile_date):
        raise _error("profile_as_of_invalid")
    try:
        parsed_date = datetime.date.fromisoformat(profile_date)
    except ValueError as error:
        raise _error("profile_as_of_invalid") from error
    if parsed_date.isoformat() != profile_date:
        raise _error("profile_as_of_invalid")
    return profile_date


def _validated_relation_cap_sequence(
    relation_maps: Any,
) -> tuple[ProviderDirectoryProfileRelationByteCaps, ...]:
    if not isinstance(relation_maps, list) or len(relation_maps) != len(
        _RELATION_NAMES
    ):
        raise _error("relation_byte_caps_invalid")
    return tuple(
        _validated_single_relation_cap(
            relation_map,
            expected_name=expected_name,
        )
        for relation_map, expected_name in zip(
            relation_maps,
            _RELATION_NAMES,
            strict=True,
        )
    )


def _validated_single_relation_cap(
    relation_map: Any,
    *,
    expected_name: str,
) -> ProviderDirectoryProfileRelationByteCaps:
    _exact_fields(relation_map, _RELATION_FIELDS, name="relation")
    relation_name = _exact_text(
        relation_map,
        "relation_name",
        maximum_length=64,
    )
    if relation_name != expected_name:
        raise _error("relation_order_invalid")
    cap_values_by_name = {
        field_name: _nonnegative_bigint(relation_map, field_name)
        for field_name in _RELATION_FIELDS - {"relation_name"}
    }
    _assert_relation_cap_shape(relation_name, cap_values_by_name)
    return ProviderDirectoryProfileRelationByteCaps(
        relation_name=relation_name,
        **cap_values_by_name,
    )


def _assert_relation_cap_shape(
    relation_name: str,
    cap_values_by_name: Mapping[str, int],
) -> None:
    if cap_values_by_name["max_temp_bytes"] < 1:
        raise _error("relation_temp_cap_invalid")
    if cap_values_by_name["max_wal_bytes"] < 1:
        raise _error("relation_wal_cap_invalid")
    if relation_name in _SCRATCH_RELATION_NAMES:
        if cap_values_by_name["max_scratch_bytes"] < 1:
            raise _error("scratch_relation_cap_invalid")
        if (
            cap_values_by_name["max_target_growth_bytes"] != 0
            or cap_values_by_name["max_deleted_logical_bytes"] != 0
        ):
            raise _error("scratch_relation_target_cap_invalid")
        return
    if relation_name not in _TARGET_RELATION_NAMES:
        raise _error("relation_name_invalid")
    if cap_values_by_name["max_scratch_bytes"] != 0:
        raise _error("target_relation_scratch_cap_invalid")
    if (
        cap_values_by_name["max_target_growth_bytes"] < 1
        or cap_values_by_name["max_deleted_logical_bytes"] < 1
    ):
        raise _error("target_relation_cap_invalid")
