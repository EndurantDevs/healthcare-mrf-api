# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed receipt validation for the Profile capacity signing guard."""

from __future__ import annotations

import datetime
from collections.abc import Mapping, Sequence
from typing import Any

from process.provider_directory_profile_capacity_preflight_contract import (
    CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD,
    CAPACITY_PREFLIGHT_CONTRACT_ID,
    CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
    CAPACITY_QUIESCENCE_CONTRACT_ID,
    CAPACITY_QUIESCENCE_DIGEST_DOMAIN,
    CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN,
    preflight_domain_sha256,
    profile_execution_identity_payload,
    utc_second_text,
)
from process.provider_directory_profile_capacity_signing_guard_contract import (
    CONTROL_PLANE_PREFLIGHT_CONTRACT_ID,
    CONTROL_PLANE_PREFLIGHT_REQUEST_CONTRACT_ID,
    CONTROL_PLANE_QUIESCENCE_CONTRACT_ID,
    CONTROL_PLANE_QUIESCENCE_DIGEST_DOMAIN,
    CONTROL_PLANE_REQUEST_DIGEST_DOMAIN,
    CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID,
    CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN,
    _HEALTHCARE_QUIESCENCE_FIELDS,
    _HEALTHCARE_RECEIPT_FIELDS,
    _HELD_FOLLOWUP_FIELDS,
    _CONTROL_PLANE_QUIESCENCE_FIELDS,
    _CONTROL_PLANE_RECEIPT_FIELDS,
    _CONTROL_PLANE_REQUEST_FIELDS,
    _MAX_OID,
    _OPAQUE_TEXT,
    _SIGNING_INTENT_FIELDS,
    _STORAGE_OBSERVATION_FIELDS,
    _TEMP_TABLESPACE_FIELDS,
    _VOLUME_FIELDS,
    _exact,
    _fail,
    _hex,
    _integer,
    _timestamp,
)


def _validated_storage_observation(raw: Any) -> dict[str, Any]:
    """Validate one closed external storage observation and lease interval."""

    observation = _exact(
        raw, _STORAGE_OBSERVATION_FIELDS, "storage_observation_invalid"
    )
    storage_time_by_name = {
        field: _timestamp(observation[field], f"storage_{field}_invalid")
        for field in ("observed_at", "issued_at", "expires_at", "max_build_deadline")
    }
    if not (
        storage_time_by_name["observed_at"]
        <= storage_time_by_name["issued_at"]
        < storage_time_by_name["max_build_deadline"]
        <= storage_time_by_name["expires_at"]
    ):
        _fail("storage_interval_invalid")
    tablespace = _exact(
        observation["temp_tablespace"],
        _TEMP_TABLESPACE_FIELDS,
        "temp_tablespace_invalid",
    )
    name = tablespace.get("tablespace_name")
    if (
        not isinstance(name, str)
        or len(name) > 63
        or _OPAQUE_TEXT.fullmatch(name) is None
    ):
        _fail("temp_tablespace_invalid")
    _integer(
        tablespace.get("tablespace_oid"),
        "temp_tablespace_invalid",
        minimum=1,
        maximum=_MAX_OID,
    )
    _hex(tablespace.get("volume_digest"), "temp_tablespace_invalid")
    _assert_storage_volumes(observation.get("volumes"))
    return observation


def _assert_storage_volumes(raw_volumes: object) -> None:
    if (
        not isinstance(raw_volumes, Sequence)
        or isinstance(raw_volumes, (str, bytes, bytearray))
        or len(raw_volumes) != 3
    ):
        _fail("volumes_invalid")
    for expected_class, raw_volume in zip(
        ("data", "temp", "wal"), raw_volumes, strict=True
    ):
        volume = _exact(raw_volume, _VOLUME_FIELDS, "volume_invalid")
        if volume.get("volume_class") != expected_class:
            _fail("volume_order_invalid")
        _hex(volume.get("volume_digest"), "volume_digest_invalid")
        available = _integer(volume.get("available_bytes"), "volume_bytes_invalid")
        remaining = _integer(
            volume.get("available_after_all_reservations_bytes"),
            "volume_bytes_invalid",
        )
        if remaining > available:
            _fail("volume_accounting_invalid")


def _validated_control_plane_request(
    raw: Any,
    healthcare_request: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    request = _exact(
        raw, _CONTROL_PLANE_REQUEST_FIELDS, "control_plane_request_invalid"
    )
    intent = _exact(
        request.get("signing_intent"),
        _SIGNING_INTENT_FIELDS,
        "control_plane_intent_invalid",
    )
    if (
        request.get("contract_id") != CONTROL_PLANE_PREFLIGHT_REQUEST_CONTRACT_ID
        or intent.get("contract_id") != CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID
    ):
        _fail("control_plane_request_contract_invalid")
    _hex(intent.get("request_nonce"), "request_nonce_invalid")
    if (
        request.get("profile_execution") != healthcare_request.execution_payload
        or request.get("provider_directory_profile_capacity_limits")
        != healthcare_request.limits_payload
    ):
        _fail("execution_or_limits_mismatch")
    storage = _validated_storage_observation(request.get("storage_observation"))
    return request, storage


def _validated_held_followup(
    raw: Any,
    healthcare_request: Any,
    expires_at: datetime.datetime,
) -> dict[str, Any]:
    followup = _exact(raw, _HELD_FOLLOWUP_FIELDS, "held_followup_invalid")
    for field in ("descriptor_sha256", "followup_preimage_sha256"):
        _hex(followup.get(field), f"{field}_invalid")
    desired = _integer(
        followup.get("desired_generation"),
        "held_followup_generation_invalid",
        minimum=1,
    )
    applied = _integer(
        followup.get("applied_generation"), "held_followup_generation_invalid"
    )
    _integer(
        followup.get("authority_epoch"),
        "held_followup_authority_invalid",
        minimum=1,
    )
    node_id = followup.get("node_id")
    hold_until = _timestamp(followup.get("hold_until"), "held_followup_hold_invalid")
    if (
        followup.get("profile_key") != "provider-directory-global-profile"
        or node_id != healthcare_request.execution.attestation.node_id
        or desired != healthcare_request.execution.generation
        or applied >= desired
        or followup.get("status") != "queued"
        or hold_until <= expires_at
    ):
        _fail("held_followup_binding_invalid")
    return followup


def _expected_control_plane_receipt_fields(
    receipt_by_field: Mapping[str, Any],
    request: Mapping[str, Any],
    storage: Mapping[str, Any],
    healthcare_request: Any,
    quiescence: Mapping[str, Any],
) -> dict[str, Any]:
    receipt_digest_by_field = {
        field_name: field_value
        for field_name, field_value in receipt_by_field.items()
        if field_name != "receipt_sha256"
    }
    return {
        "contract_id": CONTROL_PLANE_PREFLIGHT_CONTRACT_ID,
        "request_contract_id": CONTROL_PLANE_PREFLIGHT_REQUEST_CONTRACT_ID,
        "request_sha256": preflight_domain_sha256(
            CONTROL_PLANE_REQUEST_DIGEST_DOMAIN, request
        ),
        "request_nonce": request["signing_intent"]["request_nonce"],
        "issued_at": storage["issued_at"],
        "expires_at": storage["expires_at"],
        "max_build_deadline": storage["max_build_deadline"],
        "profile_execution_identity": profile_execution_identity_payload(
            healthcare_request
        ),
        "capacity_limits_sha256": healthcare_request.limits_sha256,
        "storage_observation_sha256": preflight_domain_sha256(
            CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN, storage
        ),
        "quiescence_sha256": preflight_domain_sha256(
            CONTROL_PLANE_QUIESCENCE_DIGEST_DOMAIN, quiescence
        ),
        "receipt_sha256": preflight_domain_sha256(
            CONTROL_PLANE_PREFLIGHT_CONTRACT_ID, receipt_digest_by_field
        ),
    }


def _validated_control_plane_receipt(
    raw: Any,
    request: Mapping[str, Any],
    storage: Mapping[str, Any],
    healthcare_request: Any,
) -> dict[str, Any]:
    """Validate one control_plane receipt and its held follow-up proof."""

    receipt_by_field = _exact(
        raw, _CONTROL_PLANE_RECEIPT_FIELDS, "control_plane_receipt_invalid"
    )
    expires_at = _timestamp(
        receipt_by_field.get("expires_at"), "control_plane_expiry_invalid"
    )
    followup = _validated_held_followup(
        receipt_by_field.get("held_followup"), healthcare_request, expires_at
    )
    quiescence = _exact(
        receipt_by_field.get("quiescence"),
        _CONTROL_PLANE_QUIESCENCE_FIELDS,
        "control_plane_quiescence_invalid",
    )
    if (
        quiescence.get("contract_id") != CONTROL_PLANE_QUIESCENCE_CONTRACT_ID
        or quiescence.get("followup_preimage_sha256")
        != followup["followup_preimage_sha256"]
        or quiescence.get("active_profile_run_count") != 0
        or quiescence.get("active_held_dispatch_count") != 0
    ):
        _fail("control_plane_quiescence_invalid")
    expected_by_field = _expected_control_plane_receipt_fields(
        receipt_by_field, request, storage, healthcare_request, quiescence
    )
    if any(
        receipt_by_field.get(field_name) != expected_value
        for field_name, expected_value in expected_by_field.items()
    ):
        _fail("control_plane_receipt_binding_invalid")
    return receipt_by_field


def _expected_healthcare_receipt_fields(
    receipt_by_field: Mapping[str, Any],
    healthcare_request: Any,
    control_plane_receipt: Mapping[str, Any],
    serving: Mapping[str, Any],
    quiescence: Mapping[str, Any],
) -> dict[str, Any]:
    receipt_digest_by_field = {
        field_name: field_value
        for field_name, field_value in receipt_by_field.items()
        if field_name != "receipt_sha256"
    }
    return {
        "contract_id": CAPACITY_PREFLIGHT_CONTRACT_ID,
        "request_contract_id": CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "request_sha256": healthcare_request.request_sha256,
        "request_nonce": healthcare_request.request_nonce,
        CAPACITY_CONTROL_PLANE_RECEIPT_SHA256_FIELD: control_plane_receipt[
            "receipt_sha256"
        ],
        "expires_at": utc_second_text(healthcare_request.expires_at),
        "profile_execution_identity": profile_execution_identity_payload(
            healthcare_request
        ),
        "capacity_limits": healthcare_request.limits_payload,
        "capacity_limits_sha256": healthcare_request.limits_sha256,
        "serving_generation_preflight_sha256": preflight_domain_sha256(
            CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN, dict(serving)
        ),
        "quiescence_sha256": preflight_domain_sha256(
            CAPACITY_QUIESCENCE_DIGEST_DOMAIN, quiescence
        ),
        "receipt_sha256": preflight_domain_sha256(
            CAPACITY_PREFLIGHT_CONTRACT_ID, receipt_digest_by_field
        ),
    }


def _validated_healthcare_receipt(
    raw: Any,
    healthcare_request: Any,
    control_plane_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate one durable healthcare receipt and every embedded digest."""

    receipt_by_field = _exact(
        raw, _HEALTHCARE_RECEIPT_FIELDS, "healthcare_receipt_invalid"
    )
    quiescence = _exact(
        receipt_by_field.get("quiescence"),
        _HEALTHCARE_QUIESCENCE_FIELDS,
        "healthcare_quiescence_invalid",
    )
    count_fields = (
        "active_profile_run_count",
        "claimed_profile_checkpoint_count",
        "unexpired_capacity_consumption_count",
        "outstanding_preflight_receipt_count",
    )
    if quiescence.get("contract_id") != CAPACITY_QUIESCENCE_CONTRACT_ID or any(
        quiescence.get(field) != 0 for field in count_fields
    ):
        _fail("healthcare_quiescence_invalid")
    serving = receipt_by_field.get("serving_generation_preflight")
    if not isinstance(serving, Mapping):
        _fail("healthcare_serving_preflight_invalid")
    issued_at = _timestamp(
        receipt_by_field.get("issued_at"), "healthcare_issued_at_invalid"
    )
    expires_at = _timestamp(
        receipt_by_field.get("expires_at"), "healthcare_expires_at_invalid"
    )
    expected_by_field = _expected_healthcare_receipt_fields(
        receipt_by_field,
        healthcare_request,
        control_plane_receipt,
        serving,
        quiescence,
    )
    if not issued_at < expires_at or any(
        receipt_by_field.get(field_name) != expected_value
        for field_name, expected_value in expected_by_field.items()
    ):
        _fail("healthcare_receipt_binding_invalid")
    _hex(
        receipt_by_field.get("capacity_geometry_hash"),
        "capacity_geometry_hash_invalid",
    )
    return receipt_by_field


__all__ = (
    "_validated_healthcare_receipt",
    "_validated_control_plane_receipt",
    "_validated_control_plane_request",
)
