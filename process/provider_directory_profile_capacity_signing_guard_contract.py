# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed fields and primitive validators for the signed replay guard."""

from __future__ import annotations

import datetime
import json
import re
from collections.abc import Mapping
from typing import Any, NoReturn

from process.provider_directory_profile_capacity_preflight_contract import (
    CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD,
    ProviderDirectoryProfileCapacityPreflightError,
    canonical_preflight_json,
    utc_second_text,
)


CAPACITY_SIGNING_PREFLIGHT_GUARD_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-signing-preflight-guard.v1"
)
CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN = (
    CAPACITY_SIGNING_PREFLIGHT_GUARD_CONTRACT_ID
)
CONTROL_PLANE_PREFLIGHT_REQUEST_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-control-plane-"
    "preflight-request.v1"
)
CONTROL_PLANE_PREFLIGHT_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-control-plane-preflight.v1"
)
CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-signing-intent.v1"
)
CONTROL_PLANE_REQUEST_DIGEST_DOMAIN = (
    "healthporta.provider-directory-profile-capacity-control-plane-"
    "preflight-request-digest.v1"
)
CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN = (
    "healthporta.provider-directory-profile-capacity-storage-observation-digest.v1"
)
CONTROL_PLANE_QUIESCENCE_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-control-plane-quiescence.v1"
)
CONTROL_PLANE_QUIESCENCE_DIGEST_DOMAIN = (
    "healthporta.provider-directory-profile-capacity-control-plane-"
    "quiescence-digest.v1"
)

CAPACITY_BUNDLE_VALIDATION_PATH = (
    "/v1/provider-directory/profile-capacity-bundle-validation"
)
CONTROL_PLANE_PREFLIGHT_PATH = (
    "/v1/provider-directory/profile-capacity-signing-preflight"
)
HEALTHCARE_PREFLIGHT_PATH = "/control/provider-directory/profile-capacity-preflight"

CONTROL_PLANE_PREFLIGHT_PATH_FIELD = "control_plane_preflight_path"
CONTROL_PLANE_REQUEST_FIELD = "control_plane_request"
CONTROL_PLANE_REQUEST_SHA256_FIELD = "control_plane_request_sha256"
CONTROL_PLANE_RECEIPT_FIELD = "control_plane_receipt"

_GUARD_FIELDS = frozenset(
    {
        "contract_id",
        "bundle_validation_path",
        CONTROL_PLANE_PREFLIGHT_PATH_FIELD,
        CONTROL_PLANE_REQUEST_FIELD,
        CONTROL_PLANE_REQUEST_SHA256_FIELD,
        CONTROL_PLANE_RECEIPT_FIELD,
        CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD,
        "healthcare_preflight_path",
        "healthcare_request",
        "healthcare_request_sha256",
        "healthcare_receipt",
        "healthcare_receipt_sha256",
        "capacity_limits_sha256",
        "storage_observation_sha256",
        "held_followup_preimage_sha256",
    }
)
_CONTROL_PLANE_REQUEST_FIELDS = frozenset(
    {
        "contract_id",
        "profile_execution",
        "provider_directory_profile_capacity_limits",
        "storage_observation",
        "signing_intent",
    }
)
_SIGNING_INTENT_FIELDS = frozenset({"contract_id", "request_nonce"})
_STORAGE_OBSERVATION_FIELDS = frozenset(
    {
        "observed_at",
        "issued_at",
        "expires_at",
        "max_build_deadline",
        "temp_tablespace",
        "volumes",
    }
)
_TEMP_TABLESPACE_FIELDS = frozenset(
    {"tablespace_name", "tablespace_oid", "volume_digest"}
)
_VOLUME_FIELDS = frozenset(
    {
        "volume_class",
        "volume_digest",
        "available_bytes",
        "available_after_all_reservations_bytes",
    }
)
_CONTROL_PLANE_RECEIPT_FIELDS = frozenset(
    {
        "contract_id",
        "request_contract_id",
        "request_sha256",
        "request_nonce",
        "issued_at",
        "expires_at",
        "max_build_deadline",
        "profile_execution_identity",
        "capacity_limits_sha256",
        "storage_observation_sha256",
        "held_followup",
        "quiescence",
        "quiescence_sha256",
        "receipt_sha256",
    }
)
_HELD_FOLLOWUP_FIELDS = frozenset(
    {
        "profile_key",
        "node_id",
        "desired_generation",
        "applied_generation",
        "authority_epoch",
        "status",
        "hold_until",
        "descriptor_sha256",
        "followup_preimage_sha256",
    }
)
_CONTROL_PLANE_QUIESCENCE_FIELDS = frozenset(
    {
        "contract_id",
        "followup_preimage_sha256",
        "active_profile_run_count",
        "active_held_dispatch_count",
    }
)
_HEALTHCARE_RECEIPT_FIELDS = frozenset(
    {
        "contract_id",
        "request_contract_id",
        "request_sha256",
        "request_nonce",
        CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD,
        "issued_at",
        "expires_at",
        "profile_execution_identity",
        "capacity_limits",
        "capacity_limits_sha256",
        "capacity_geometry_hash",
        "capacity_geometry",
        "required_reservation_bytes_by_storage_class",
        "artifact_scope_projection",
        "runtime_observation",
        "serving_generation_preflight",
        "serving_generation_preflight_sha256",
        "quiescence",
        "quiescence_sha256",
        "preflight_receipt_storage",
        "receipt_sha256",
    }
)
_HEALTHCARE_QUIESCENCE_FIELDS = frozenset(
    {
        "contract_id",
        "active_profile_run_count",
        "claimed_profile_checkpoint_count",
        "unexpired_capacity_consumption_count",
        "outstanding_preflight_receipt_count",
        "active_profile_run_statuses",
        "claimed_checkpoint_states",
        "capacity_consumption_boundary",
        "preflight_receipt_boundary",
    }
)

_LOWER_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_OPAQUE_TEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_MAX_SIGNED_BIGINT = (1 << 63) - 1
_MAX_OID = (1 << 32) - 1


class ProfileCapacitySigningGuardError(ValueError):
    """Report malformed or inconsistently linked signed replay material."""


def _fail(reason: str) -> NoReturn:
    raise ProfileCapacitySigningGuardError(
        "provider_directory_profile_capacity_signing_guard_" + reason
    )


def _exact(value: Any, fields: frozenset[str], reason: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        _fail(reason)
    return dict(value)


def _hex(value: Any, reason: str) -> str:
    if not isinstance(value, str) or _LOWER_HEX_64.fullmatch(value) is None:
        _fail(reason)
    return value


def _integer(
    value: Any,
    reason: str,
    *,
    minimum: int = 0,
    maximum: int = _MAX_SIGNED_BIGINT,
) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not minimum <= value <= maximum
    ):
        _fail(reason)
    return value


def _timestamp(value: Any, reason: str) -> datetime.datetime:
    if not isinstance(value, str):
        _fail(reason)
    try:
        parsed_at = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc
        )
    except ValueError:
        _fail(reason)
    if utc_second_text(parsed_at) != value:
        _fail(reason)
    return parsed_at


def _plain_json(value: Any, reason: str) -> Any:
    try:
        return json.loads(canonical_preflight_json(value))
    except (ProviderDirectoryProfileCapacityPreflightError, ValueError):
        _fail(reason)
