# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed ordinary serving requests paired with existing signed Profile authority."""

from __future__ import annotations

import dataclasses
import datetime
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

from process import provider_directory_cms_capacity_contract as cms
from process import provider_directory_profile_capacity_preflight_contract as base
from process import provider_directory_serving_capacity_contract as serving

_ADMISSION_FIELDS = frozenset({"contract_id", "admission_purpose", "paired_profile_lease", "limits", "ordinary_scope"})


def _invalid(reason: str) -> None:
    """Reject malformed ordinary authority without including supplied contents."""
    raise base.ProviderDirectoryProfileCapacityPreflightError("provider_directory_serving_capacity_" + reason)


def _validated_limits(raw: Any) -> dict[str, Any]:
    """Reuse the existing closed integer, byte, and worker ceiling rules."""
    limits_by_field = dict(base._exact_mapping(raw, cms.CMS_LIMIT_FIELDS, reason="serving_limits_fields_invalid"))
    if limits_by_field["contract_id"] != serving.LIMITS_CONTRACT:
        _invalid("limits_contract_invalid")
    limits_by_field["contract_id"] = cms.CMS_LIMITS_CONTRACT
    validated = cms.validated_cms_capacity_limits(limits_by_field)
    return {**validated, "contract_id": serving.LIMITS_CONTRACT}


def serving_profile_materialization(admission: Mapping[str, Any]) -> str:
    """Infer only the existing explicit paired initial request, never head absence."""
    from process.provider_directory_profile_initial_contract import execution_initial_requested

    paired = SimpleNamespace(capacity_attestation=admission["paired_profile_lease"])
    return "full_swap" if execution_initial_requested(paired) else "source_delta"


def validated_serving_capacity_admission(raw: Any) -> dict[str, Any]:
    """Decode one exact Profile pair, ordinary retention scope, and bounded ceilings."""
    admission_by_field = dict(base._exact_mapping(raw, _ADMISSION_FIELDS, reason="serving_admission_fields_invalid"))
    if (
        admission_by_field["contract_id"] != serving.ADMISSION_CONTRACT
        or admission_by_field["admission_purpose"] != serving.PURPOSE
    ):
        _invalid("admission_contract_invalid")
    admission_by_field["limits"] = _validated_limits(admission_by_field["limits"])
    admission_by_field["paired_profile_lease"] = cms._paired_profile_envelope(
        admission_by_field["paired_profile_lease"]
    )
    try:
        admission_by_field["ordinary_scope"] = serving.validate_scope(admission_by_field["ordinary_scope"])
        serving.require_materialization_scope(
            admission_by_field["ordinary_scope"],
            {"materialization_mode": serving_profile_materialization(admission_by_field)},
        )
    except (ValueError, TypeError, KeyError) as error:
        raise base.ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_serving_capacity_scope_invalid"
        ) from error
    return admission_by_field


def _require_request_pair(request: Any, admission: Mapping[str, Any]) -> None:
    """Keep the complete ordinary selection and exact execution inside the signed pair."""
    try:
        serving.require_ordinary_selection(request.execution.attestation.payload)
    except (ValueError, TypeError, KeyError) as error:
        raise base.ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_serving_capacity_ordinary_publish_required"
        ) from error
    if admission["ordinary_scope"]["source_vector_sha256"] != serving.source_vector_sha256(
        request.execution.attestation.payload["pairs"]
    ):
        _invalid("source_vector_changed")
    paired_request = admission["paired_profile_lease"]["lease"]["signing_preflight_guard"]["healthcare_request"]
    if serving.ADMISSION_FIELD in paired_request or paired_request["profile_execution"] != request.execution_payload:
        _invalid("paired_profile_execution_changed")


def validated_serving_preflight_request(raw: Any, *, projection: bool = False):
    """Extend only the explicit ordinary versions; preserve existing base parsing."""
    fields = base._AUTHORITY_PROJECTION_REQUEST_FIELDS if projection else base._REQUEST_FIELDS
    request_by_field = dict(
        base._exact_mapping(raw, fields | {serving.ADMISSION_FIELD}, reason="serving_request_fields_invalid")
    )
    contract = serving.PROJECTION_REQUEST if projection else serving.PREFLIGHT_REQUEST
    if request_by_field["contract_id"] != contract:
        _invalid("request_contract_invalid")
    admission = validated_serving_capacity_admission(request_by_field.pop(serving.ADMISSION_FIELD))
    request_by_field["contract_id"] = (
        base.CAPACITY_AUTHORITY_PROJECTION_REQUEST_CONTRACT_ID
        if projection
        else base.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID
    )
    validate = (
        base._validated_profile_projection_request if projection else base._validated_profile_capacity_preflight_request
    )
    validated = validate(request_by_field)
    _require_request_pair(validated, admission)
    normalized_by_field = {**validated.request_payload, "contract_id": contract, serving.ADMISSION_FIELD: admission}
    if raw != normalized_by_field:
        _invalid("request_not_canonical")
    return dataclasses.replace(
        validated,
        request_payload=normalized_by_field,
        request_sha256=base.preflight_domain_sha256(contract, normalized_by_field),
        serving_nonprofile_admission=admission,
    )


def verified_serving_paired_profile_lease(request: Any, *, trust: Any, now: datetime.datetime):
    """Verify the configured key, database, validity window, exact execution, and mode."""
    from process.provider_directory_profile_capacity_attestation import verify_database_capacity_lease
    from process.provider_directory_profile_capacity_trust import CapacityLeaseTrust

    admission = request.serving_nonprofile_admission
    if not isinstance(trust, CapacityLeaseTrust) or not isinstance(admission, Mapping):
        _invalid("paired_profile_trust_required")
    envelope = admission["paired_profile_lease"]
    verified = verify_database_capacity_lease(
        envelope,
        trust=trust,
        now=now,
        expected_capacity_geometry_hash=envelope["lease"]["capacity_geometry_hash"],
        expected_database_system_identifier=trust.database_system_identifier,
        expected_database_oid=trust.database_oid,
        expected_database_name=trust.database_name,
    )
    if verified.signing_preflight_guard["healthcare_request"]["profile_execution"] != request.execution_payload:
        _invalid("paired_profile_execution_changed")
    try:
        geometry = verified.signing_preflight_guard["healthcare_receipt"]["capacity_geometry"]
        serving.require_materialization_scope(admission["ordinary_scope"], geometry)
    except (ValueError, TypeError, KeyError) as error:
        raise base.ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_serving_capacity_paired_profile_materialization_changed"
        ) from error
    return verified


def validated_serving_execution_capacity(raw, *, profile_envelope, attestation, generation):
    """Retain the closed ordinary pair; admission verifies both signatures and runtime."""
    from process.provider_directory_cms_capacity_contract import _validated_execution_capacity

    return _validated_execution_capacity(
        raw,
        profile_envelope=profile_envelope,
        attestation=attestation,
        generation=generation,
        purpose=serving.PURPOSE,
    )
