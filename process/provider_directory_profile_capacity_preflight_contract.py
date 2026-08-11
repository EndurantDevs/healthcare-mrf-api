# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed authenticated request and receipt identities for Profile capacity."""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

from process.provider_directory_profile_capacity_runtime_config import (
    validated_capacity_limits,
)
from process.provider_directory_profile_capacity_runtime_types import (
    CAPACITY_LIMITS_CONTRACT_ID,
    ProviderDirectoryProfileCapacityLimits,
)
from process.provider_directory_profile_selection_contract import (
    PROFILE_EXECUTION_CONTRACT_ID,
    ProviderDirectoryProfileExecution,
    _GLOBAL_PROFILE_PARAMS,
    validated_profile_execution,
)


CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-preflight-request.v3"
)
CAPACITY_PREFLIGHT_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-preflight.v3"
)
CAPACITY_SIGNING_GUARD_REQUEST_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-signing-guard-request.v1"
)
CAPACITY_LIMITS_DIGEST_DOMAIN = (
    "healthporta.provider-directory-profile-capacity-limits-digest.v1"
)
CAPACITY_SERVING_PREFLIGHT_CONTRACT_ID = (
    "healthporta.provider-directory-profile-serving-generation-preflight.v1"
)
CAPACITY_QUIESCENCE_CONTRACT_ID = (
    "healthporta.provider-directory-profile-capacity-signing-quiescence.v1"
)
CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN = (
    "healthporta.provider-directory-profile-serving-generation-preflight.v1"
)
CAPACITY_QUIESCENCE_DIGEST_DOMAIN = (
    "healthporta.provider-directory-profile-capacity-signing-quiescence.v1"
)
# Lease-v3 uses a source-neutral controller identity. Historical lease-v2 field
# names remain isolated in the coordinator's version-aware replay parser.
CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD = "control_plane_receipt_sha256"

_REQUEST_FIELDS = frozenset(
    {
        "contract_id",
        "profile_execution",
        "provider_directory_profile_capacity_limits",
        "signing_guard",
    }
)
_EXECUTION_FIELDS = frozenset(
    {
        *_GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation",
        "provider_directory_profile_selection_attestation",
        "provider_directory_profile_capacity_attestation",
    }
)
_SIGNING_GUARD_FIELDS = frozenset(
    {
        "contract_id",
        "request_nonce",
        CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD,
        "expires_at",
    }
)
_LOWER_HEX_64 = re.compile(r"[0-9a-f]{64}\Z")
_UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T" r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z"
)
_MAX_RECEIPT_LIFETIME_SECONDS = 24 * 60 * 60


class ProviderDirectoryProfileCapacityPreflightError(ValueError):
    """Report a malformed or unsafe signing-preflight request."""


@dataclass(frozen=True)
class ProviderDirectoryProfileCapacityPreflightRequest:
    """Validated exact execution, limits, and replay-fence challenge."""

    execution: ProviderDirectoryProfileExecution
    execution_payload: dict[str, Any]
    limits: ProviderDirectoryProfileCapacityLimits
    limits_payload: dict[str, Any]
    limits_sha256: str
    request_nonce: str
    control_plane_receipt_sha256: str
    expires_at: datetime.datetime
    request_payload: dict[str, Any]
    request_sha256: str


def canonical_preflight_json(value: Any) -> str:
    """Serialize a closed preflight value deterministically."""

    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_json_invalid"
        ) from exc


def preflight_domain_sha256(domain: str, value: Any) -> str:
    """Hash canonical JSON with an explicit ASCII/NUL domain boundary."""

    return hashlib.sha256(
        domain.encode("ascii") + b"\0" + canonical_preflight_json(value).encode("ascii")
    ).hexdigest()


def canonical_capacity_limits_payload(
    limits: ProviderDirectoryProfileCapacityLimits,
) -> dict[str, Any]:
    """Return the exact public limits document from validated semantics."""

    if not isinstance(limits, ProviderDirectoryProfileCapacityLimits):
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_limits_invalid"
        )
    values_by_name = dataclasses.asdict(limits)
    values_by_name["relation_byte_caps"] = list(values_by_name["relation_byte_caps"])
    return {
        "contract_id": CAPACITY_LIMITS_CONTRACT_ID,
        **values_by_name,
    }


def capacity_limits_sha256(
    limits_payload: Mapping[str, Any],
) -> str:
    """Bind the complete closed limits document, including spare ceilings."""

    return preflight_domain_sha256(
        CAPACITY_LIMITS_DIGEST_DOMAIN,
        dict(limits_payload),
    )


def _exact_mapping(
    value: Any,
    fields: frozenset[str],
    *,
    reason: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_" + reason
        )
    return value


def _hex_digest(value: Any, *, reason: str) -> str:
    if not isinstance(value, str) or _LOWER_HEX_64.fullmatch(value) is None:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_" + reason
        )
    return value


def _utc_timestamp(value: Any) -> datetime.datetime:
    if not isinstance(value, str) or _UTC_TIMESTAMP.fullmatch(value) is None:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_expires_at_invalid"
        )
    try:
        parsed = datetime.datetime.strptime(
            value,
            "%Y-%m-%dT%H:%M:%SZ",
        ).replace(tzinfo=datetime.timezone.utc)
    except ValueError as exc:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_expires_at_invalid"
        ) from exc
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_expires_at_invalid"
        )
    return parsed


def _validated_execution_payload(
    raw_execution: Any,
) -> tuple[ProviderDirectoryProfileExecution, dict[str, Any]]:
    execution_map = dict(
        _exact_mapping(
            raw_execution,
            _EXECUTION_FIELDS,
            reason="execution_fields_invalid",
        )
    )
    if execution_map["provider_directory_profile_capacity_attestation"] != {}:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_attestation_not_empty"
        )
    return validated_profile_execution(execution_map), execution_map


def _validated_limits_payload(
    raw_limits: Any,
) -> tuple[ProviderDirectoryProfileCapacityLimits, dict[str, Any], str]:
    if not isinstance(raw_limits, Mapping):
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_limits_invalid"
        )
    limits = validated_capacity_limits(raw_limits)
    limits_payload = canonical_capacity_limits_payload(limits)
    if dict(raw_limits) != limits_payload:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_limits_not_canonical"
        )
    return limits, limits_payload, capacity_limits_sha256(limits_payload)


def _validated_signing_guard(
    raw_guard: Any,
) -> tuple[str, str, datetime.datetime]:
    """Return the canonical request nonce, receipt link, and expiry."""

    signing_guard_by_field = dict(
        _exact_mapping(
            raw_guard,
            _SIGNING_GUARD_FIELDS,
            reason="signing_guard_fields_invalid",
        )
    )
    if (
        signing_guard_by_field.get("contract_id")
        != CAPACITY_SIGNING_GUARD_REQUEST_CONTRACT_ID
    ):
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_signing_guard_contract_invalid"
        )
    request_nonce = _hex_digest(
        signing_guard_by_field.get("request_nonce"),
        reason="request_nonce_invalid",
    )
    control_plane_receipt = _hex_digest(
        signing_guard_by_field.get(CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD),
        reason="control_plane_receipt_invalid",
    )
    expires_at = _utc_timestamp(signing_guard_by_field.get("expires_at"))
    return request_nonce, control_plane_receipt, expires_at


def validated_capacity_preflight_request(
    raw_request: Any,
) -> ProviderDirectoryProfileCapacityPreflightRequest:
    """Validate the exact authenticated signing-preflight request schema."""

    request_map = dict(
        _exact_mapping(
            raw_request,
            _REQUEST_FIELDS,
            reason="request_fields_invalid",
        )
    )
    if request_map.get("contract_id") != CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_request_contract_invalid"
        )
    execution, execution_payload = _validated_execution_payload(
        request_map["profile_execution"]
    )
    limits, limits_payload, limits_digest = _validated_limits_payload(
        request_map["provider_directory_profile_capacity_limits"]
    )
    request_nonce, control_plane_receipt, expires_at = _validated_signing_guard(
        request_map["signing_guard"]
    )
    normalized_by_field = {
        "contract_id": CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "profile_execution": execution_payload,
        "provider_directory_profile_capacity_limits": limits_payload,
        "signing_guard": {
            "contract_id": CAPACITY_SIGNING_GUARD_REQUEST_CONTRACT_ID,
            "request_nonce": request_nonce,
            CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD: control_plane_receipt,
            "expires_at": expires_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    }
    if request_map != normalized_by_field:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_request_not_canonical"
        )
    return ProviderDirectoryProfileCapacityPreflightRequest(
        execution=execution,
        execution_payload=execution_payload,
        limits=limits,
        limits_payload=limits_payload,
        limits_sha256=limits_digest,
        request_nonce=request_nonce,
        control_plane_receipt_sha256=control_plane_receipt,
        expires_at=expires_at,
        request_payload=normalized_by_field,
        request_sha256=preflight_domain_sha256(
            CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
            normalized_by_field,
        ),
    )


def assert_preflight_expiry(
    request: ProviderDirectoryProfileCapacityPreflightRequest,
    *,
    issued_at: datetime.datetime,
) -> None:
    """Require a future deployment-grade expiry within the lease-v2 ceiling."""

    normalized_issued_at = issued_at.astimezone(datetime.timezone.utc)
    lifetime = request.expires_at - normalized_issued_at
    if (
        not datetime.timedelta(0)
        < lifetime
        <= datetime.timedelta(seconds=_MAX_RECEIPT_LIFETIME_SECONDS)
    ):
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_expiry_invalid"
        )


def profile_execution_identity_payload(
    request: ProviderDirectoryProfileCapacityPreflightRequest,
) -> dict[str, Any]:
    """Return the exact v6/source-delta identity exposed to the signer."""

    attestation = request.execution.attestation
    return {
        "execution_contract_id": PROFILE_EXECUTION_CONTRACT_ID,
        "selection_proof_id": attestation.proof_id,
        "selection_fingerprint": attestation.selection_fingerprint,
        "profile_input_digest": attestation.profile_input_digest,
        "source_context_digest": attestation.source_context_digest,
        "generation": request.execution.generation,
        "operation": attestation.operation,
        "profile_schema_version": attestation.profile_schema_version,
        "profile_strategy_version": attestation.profile_strategy_version,
        "materialization_mode": "source_delta",
    }


def utc_second_text(value: datetime.datetime) -> str:
    """Render an aware PostgreSQL timestamp as canonical UTC seconds."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_clock_invalid"
        )
    normalized = value.astimezone(datetime.timezone.utc)
    if normalized.microsecond != 0:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_profile_capacity_preflight_clock_invalid"
        )
    return normalized.strftime("%Y-%m-%dT%H:%M:%SZ")


__all__ = (
    "CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD",
    "CAPACITY_LIMITS_DIGEST_DOMAIN",
    "CAPACITY_PREFLIGHT_CONTRACT_ID",
    "CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID",
    "CAPACITY_QUIESCENCE_CONTRACT_ID",
    "CAPACITY_QUIESCENCE_DIGEST_DOMAIN",
    "CAPACITY_SERVING_PREFLIGHT_CONTRACT_ID",
    "CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN",
    "CAPACITY_SIGNING_GUARD_REQUEST_CONTRACT_ID",
    "ProviderDirectoryProfileCapacityPreflightError",
    "ProviderDirectoryProfileCapacityPreflightRequest",
    "assert_preflight_expiry",
    "canonical_capacity_limits_payload",
    "canonical_preflight_json",
    "capacity_limits_sha256",
    "preflight_domain_sha256",
    "profile_execution_identity_payload",
    "utc_second_text",
    "validated_capacity_preflight_request",
)
