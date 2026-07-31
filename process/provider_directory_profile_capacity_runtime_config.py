# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parse deployment limits and signed-lease trust configuration."""

from __future__ import annotations

import json
import os
from typing import Any, Mapping

from process import provider_directory_profile_capacity as capacity
from process.provider_directory_profile_capacity_runtime_types import (
    CAPACITY_LIMITS_CONTRACT_ID,
    CAPACITY_LIMITS_ENV,
    ProviderDirectoryProfileCapacityConfigurationError,
    ProviderDirectoryProfileCapacityLimits,
    _LIMIT_FIELDS,
    _MAX_SIGNED_BIGINT,
    _POSITIVE_LIMIT_FIELDS,
    _RELATION_FIELDS,
    _RELATION_NAMES,
)
from process.provider_directory_profile_capacity_trust_config import (
    configured_capacity_lease_trust,
    validated_capacity_lease_trust,
)


def _configuration_error(reason: str) -> (
    ProviderDirectoryProfileCapacityConfigurationError
):
    return ProviderDirectoryProfileCapacityConfigurationError(
        "provider_directory_profile_capacity_configuration_" + reason
    )


def _positive_integer(value: Any, *, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= _MAX_SIGNED_BIGINT
    ):
        raise _configuration_error(field_name + "_invalid")
    return value


def _zero_integer(value: Any, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value != 0:
        raise _configuration_error(field_name + "_invalid")
    return value


def _relation_byte_caps(
    raw_caps: Any,
) -> tuple[capacity.ProviderDirectoryProfileRelationByteCaps, ...]:
    if not isinstance(raw_caps, list) or len(raw_caps) != len(
        _RELATION_NAMES
    ):
        raise _configuration_error("relation_byte_caps_invalid")
    return tuple(
        _validated_relation_cap(raw_cap, relation_name)
        for raw_cap, relation_name in zip(
            raw_caps,
            _RELATION_NAMES,
            strict=True,
        )
    )


def _validated_relation_cap(
    raw_cap: Any,
    relation_name: str,
) -> capacity.ProviderDirectoryProfileRelationByteCaps:
    """Validate one ordered deployment relation ceiling."""

    if (
        not isinstance(raw_cap, Mapping)
        or set(raw_cap) != _RELATION_FIELDS
        or raw_cap.get("relation_name") != relation_name
    ):
        raise _configuration_error("relation_byte_caps_invalid")
    is_scratch_relation = relation_name in _RELATION_NAMES[:4]
    return capacity.ProviderDirectoryProfileRelationByteCaps(
        relation_name=relation_name,
        max_scratch_bytes=_relation_limit_value(
            raw_cap,
            relation_name,
            "max_scratch_bytes",
            positive=is_scratch_relation,
        ),
        max_target_growth_bytes=_relation_limit_value(
            raw_cap,
            relation_name,
            "max_target_growth_bytes",
            positive=not is_scratch_relation,
        ),
        max_deleted_logical_bytes=_relation_limit_value(
            raw_cap,
            relation_name,
            "max_deleted_logical_bytes",
            positive=not is_scratch_relation,
        ),
        max_temp_bytes=_relation_limit_value(
            raw_cap,
            relation_name,
            "max_temp_bytes",
            positive=True,
        ),
        max_wal_bytes=_relation_limit_value(
            raw_cap,
            relation_name,
            "max_wal_bytes",
            positive=True,
        ),
    )


def _relation_limit_value(
    raw_cap: Mapping[str, Any],
    relation_name: str,
    field_name: str,
    *,
    positive: bool,
) -> int:
    parser = _positive_integer if positive else _zero_integer
    return parser(
        raw_cap[field_name],
        field_name=relation_name + "_" + field_name,
    )


def validated_capacity_limits(
    raw_limits: Mapping[str, Any],
) -> ProviderDirectoryProfileCapacityLimits:
    """Validate a closed, explicit set of hard deployment ceilings."""

    if (
        not isinstance(raw_limits, Mapping)
        or set(raw_limits) != _LIMIT_FIELDS
        or raw_limits.get("contract_id") != CAPACITY_LIMITS_CONTRACT_ID
    ):
        raise _configuration_error("fields_invalid")
    positive_values_by_name = {
        field_name: _positive_integer(
            raw_limits[field_name],
            field_name=field_name,
        )
        for field_name in _POSITIVE_LIMIT_FIELDS
    }
    return ProviderDirectoryProfileCapacityLimits(
        **positive_values_by_name,
        relation_byte_caps=_relation_byte_caps(
            raw_limits["relation_byte_caps"]
        ),
    )


def configured_capacity_limits(
    raw_json: str | None = None,
) -> ProviderDirectoryProfileCapacityLimits:
    """Load required non-secret limits; never synthesize production defaults."""

    encoded_limits = (
        raw_json if raw_json is not None else os.getenv(CAPACITY_LIMITS_ENV)
    )
    if not isinstance(encoded_limits, str) or not encoded_limits.strip():
        raise _configuration_error("missing")
    try:
        raw_limits = json.loads(encoded_limits)
    except (TypeError, ValueError) as exc:
        raise _configuration_error("json_invalid") from exc
    return validated_capacity_limits(raw_limits)
