# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compatibility facade for Provider Directory Profile capacity configuration."""

from __future__ import annotations

import json
import os

from process.provider_directory_profile_capacity_runtime_types import (
    CAPACITY_LIMITS_CONTRACT_ID,
    CAPACITY_LIMITS_ENV,
    CAPACITY_TRUST_CONTRACT_ID,
    CAPACITY_TRUST_ENV,
    ProviderDirectoryProfileCapacityConfigurationError,
    ProviderDirectoryProfileCapacityGeometryInputs,
    ProviderDirectoryProfileCapacityLimits,
    _LIMIT_FIELDS,
    _MAX_SIGNED_BIGINT,
    _POSITIVE_LIMIT_FIELDS,
    _RELATION_FIELDS,
    _RELATION_NAMES,
    _TRUST_FIELDS,
    _TRUST_KEY_FIELDS,
    _TRUST_TABLESPACE_FIELDS,
    _TRUST_VOLUME_FIELDS,
)
from process.provider_directory_profile_capacity_runtime_config import (
    _configuration_error,
    _positive_integer,
    _relation_byte_caps,
    _zero_integer,
    configured_capacity_lease_trust,
    configured_capacity_limits,
    validated_capacity_lease_trust,
    validated_capacity_limits,
)
from process.provider_directory_profile_capacity_runtime_geometry import (
    build_capacity_geometry,
)

__all__ = (
    "CAPACITY_LIMITS_CONTRACT_ID",
    "CAPACITY_LIMITS_ENV",
    "CAPACITY_TRUST_CONTRACT_ID",
    "CAPACITY_TRUST_ENV",
    "ProviderDirectoryProfileCapacityConfigurationError",
    "ProviderDirectoryProfileCapacityGeometryInputs",
    "ProviderDirectoryProfileCapacityLimits",
    "build_capacity_geometry",
    "configured_capacity_lease_trust",
    "configured_capacity_limits",
    "validated_capacity_lease_trust",
    "validated_capacity_limits",
)
