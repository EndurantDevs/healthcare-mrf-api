# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed additional capacity requests for complete directory preparation."""

from __future__ import annotations

import dataclasses
import datetime
import json
from collections.abc import Mapping
from typing import Any

from process.provider_directory_profile_capacity_preflight_contract import (
    CAPACITY_AUTHORITY_PROJECTION_REQUEST_CONTRACT_ID,
    CAPACITY_PREFLIGHT_CONTRACT_ID,
    CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
    ProviderDirectoryProfileCapacityPreflightError,
    _exact_mapping,
    canonical_preflight_json,
    preflight_domain_sha256,
)

CMS_ADMISSION_REQUEST_CONTRACT = "provider-directory-cms-nonprofile-admission-request.v1"
CMS_LIMITS_CONTRACT = "provider-directory-cms-nonprofile-limits.v1"
CMS_GEOMETRY_CONTRACT = "provider-directory-cms-nonprofile-capacity.v1"
CMS_PREFLIGHT_REQUEST_CONTRACT = "healthporta.provider-directory-profile-capacity-preflight-request.v4"
CMS_PREFLIGHT_CONTRACT = "healthporta.provider-directory-profile-capacity-preflight.v4"
CMS_PROJECTION_REQUEST_CONTRACT = "healthporta.provider-directory-profile-capacity-authority-projection-request.v2"
CMS_PROJECTION_CONTRACT = "healthporta.provider-directory-profile-capacity-authority-projection.v2"
CMS_CONTROL_REQUEST_CONTRACT = "healthporta.provider-directory-profile-capacity-control-plane-preflight-request.v2"
CMS_CONTROL_CONTRACT = "healthporta.provider-directory-profile-capacity-control-plane-preflight.v2"
CMS_ADMISSION_FIELD = "cms_nonprofile_admission"
CMS_LIMIT_FIELDS = frozenset(
    {
        "contract_id",
        "batch_size",
        "worker_count",
        "temp_file_limit_bytes_per_backend",
        "minimum_remaining_bytes",
        "required_build_seconds",
        "max_data_bytes",
        "max_temp_bytes",
        "max_wal_bytes",
    }
)


_RETENTION_POLICY_FIELDS = frozenset(
    {
        "version",
        "capture_id",
        "source_pin",
        "binding_coordinates",
        "selection_proof_id",
        "expected_admission_sha256",
        "expected_metadata_sha256",
        "raw_tables",
        "address_tables",
        "owner_role",
        "runtime_roles",
        "extra_data_upper_bound_bytes",
        "extra_wal_upper_bound_bytes",
    }
)
_RETENTION_SOURCE_PIN_FIELDS = frozenset(
    {
        "schema_name",
        "source_id",
        "endpoint_id",
        "dataset_id",
        "dataset_sha256",
        "release_id",
        "resource_table_oid",
        "alias_scope",
        "as_of",
    }
)
_RETENTION_BINDING_FIELDS = frozenset(
    {
        "source_system",
        "source_id",
        "dataset_schema",
        "dataset_id",
        "producer_id",
        "edition_id",
    }
)


def _invalid(reason: str) -> None:
    """Reject unsafe additional requests without disclosing their contents."""
    raise ProviderDirectoryProfileCapacityPreflightError("provider_directory_cms_capacity_" + reason)


def validated_registry_source_retention_policy(raw: Any) -> dict[str, Any]:
    """Decode the original source policy through its native preparation DTO."""
    from uuid import UUID

    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_membership_source import PinnedFHIRMembershipSource
    from process.network_registry_cms_prepared_pair import (
        RegistryCMSRetentionRequest,
        _validate_preparation_request,
    )

    try:
        policy_by_field = dict(_exact_mapping(raw, _RETENTION_POLICY_FIELDS, reason="retention_policy_invalid"))
        pin_by_field = dict(
            _exact_mapping(
                policy_by_field["source_pin"], _RETENTION_SOURCE_PIN_FIELDS, reason="retention_policy_invalid"
            )
        )
        coordinates_by_field = dict(
            _exact_mapping(
                policy_by_field["binding_coordinates"], _RETENTION_BINDING_FIELDS, reason="retention_policy_invalid"
            )
        )
        if (
            type(policy_by_field["version"]) is not int
            or policy_by_field["version"] != 1
            or type(policy_by_field["capture_id"]) is not str
            or type(policy_by_field["runtime_roles"]) is not list
            or not 1 <= len(policy_by_field["runtime_roles"]) <= 64
            or any(type(role) is not str for role in policy_by_field["runtime_roles"])
            or type(policy_by_field["owner_role"]) is not str
            or type(pin_by_field["schema_name"]) is not str
            or type(policy_by_field["raw_tables"]) is not list
            or type(policy_by_field["address_tables"]) is not list
        ):
            _invalid("retention_policy_invalid")
        capture_id = UUID(policy_by_field["capture_id"])
        request = RegistryCMSRetentionRequest(
            PinnedFHIRMembershipSource(**pin_by_field),
            RegistryNetworkSourceCoordinates(**coordinates_by_field),
            policy_by_field["selection_proof_id"],
            policy_by_field["expected_admission_sha256"],
            policy_by_field["expected_metadata_sha256"],
            policy_by_field["extra_data_upper_bound_bytes"],
            policy_by_field["extra_wal_upper_bound_bytes"],
        )
        roles = tuple(policy_by_field["runtime_roles"])
        _validate_preparation_request(request, capture_id, policy_by_field["owner_role"], roles)
        normalized = request.policy(capture_id, policy_by_field["owner_role"], roles)
        if policy_by_field != normalized:
            _invalid("retention_policy_invalid")
        return json.loads(canonical_preflight_json(normalized))
    except ValueError, TypeError, KeyError, AttributeError:
        _invalid("retention_policy_invalid")


def require_profile_capacity_request(request: Any) -> None:
    """Prevent the Profile-only producer from signing additional CMS costs."""
    if request.cms_nonprofile_admission is not None:
        _invalid("native_projection_required")


def validated_cms_execution_capacity(
    raw: Any, *, profile_envelope: Any, attestation: Any, generation: int
) -> dict[str, Any]:
    """Retain the exact desired-publish pair; admission still verifies signatures and runtime."""
    from process import provider_directory_profile_capacity_attestation as lease
    from process.provider_directory_profile_capacity_attestation_contract import _SIGNED_BODY_FIELDS
    from process.provider_directory_profile_capacity_preflight_contract import validated_capacity_preflight_request

    if attestation.operation != "publish" or attestation.desired_cms_dataset is None:
        _invalid("execution_desired_publish_required")
    envelope_by_field = dict(
        _exact_mapping(raw, frozenset({"lease", "signature"}), reason="cms_execution_fields_invalid")
    )
    body_by_field = dict(
        _exact_mapping(envelope_by_field["lease"], _SIGNED_BODY_FIELDS, reason="cms_execution_body_invalid")
    )
    guard = body_by_field.get("signing_preflight_guard")
    receipt = guard.get("healthcare_receipt") if isinstance(guard, Mapping) else None
    request_by_field = guard.get("healthcare_request") if isinstance(guard, Mapping) else None
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("contract_id") != CMS_PREFLIGHT_CONTRACT
        or (
            not isinstance(request_by_field, Mapping)
            or request_by_field.get("contract_id") != CMS_PREFLIGHT_REQUEST_CONTRACT
        )
    ):
        _invalid("execution_purpose_invalid")
    request = validated_capacity_preflight_request(request_by_field)
    if (
        request.execution.attestation.payload != attestation.payload
        or request.execution.generation != generation
        or (request.cms_nonprofile_admission["paired_profile_lease"] != profile_envelope)
    ):
        _invalid("execution_pair_changed")
    try:
        lease._decode_signature(envelope_by_field["signature"])
        parsed = lease._parse_capacity_lease_fields(body_by_field)
        lease._parse_capacity_signing_guard_fields(body_by_field, parsed)
        lease._assert_attestation_id(body_by_field)
    except (ValueError, KeyError, TypeError) as error:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_cms_capacity_execution_envelope_invalid"
        ) from error
    return json.loads(canonical_preflight_json(envelope_by_field))


def capacity_preflight_receipt_row_values(
    request: Any, receipt: Mapping[str, Any], *, issued_at: datetime.datetime
) -> dict[str, Any]:
    """Retain the exact matching receipt/request version and immutable row identity."""
    from process import provider_directory_profile_initial_contract as initial

    is_cms = request.cms_nonprofile_admission is not None
    is_initial = initial.is_initial_request(request)
    receipt_contract = (
        CMS_PREFLIGHT_CONTRACT
        if is_cms
        else (initial.RECEIPT_CONTRACT if is_initial else CAPACITY_PREFLIGHT_CONTRACT_ID)
    )
    request_contract = (
        CMS_PREFLIGHT_REQUEST_CONTRACT
        if is_cms
        else (initial.REQUEST_CONTRACT if is_initial else CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID)
    )
    if (
        receipt["contract_id"] != receipt_contract
        or receipt["request_contract_id"] != request_contract
        or (
            request.request_payload["contract_id"] != request_contract
            or receipt["request_sha256"] != request.request_sha256
        )
    ):
        _invalid("receipt_version_or_request_changed")
    identity = receipt["profile_execution_identity"]
    return {
        "receipt_sha256": receipt["receipt_sha256"],
        "request_nonce": request.request_nonce,
        "request_sha256": request.request_sha256,
        "control_plane_receipt_sha256": request.control_plane_receipt_sha256,
        "contract_id": receipt_contract,
        "request_contract_id": request_contract,
        "limits_contract_id": request.limits_payload["contract_id"],
        "selection_proof_id": identity["selection_proof_id"],
        "profile_input_digest": identity["profile_input_digest"],
        "control_generation": identity["generation"],
        "profile_schema_version": identity["profile_schema_version"],
        "profile_strategy_version": identity["profile_strategy_version"],
        "materialization_mode": identity["materialization_mode"],
        "limits_sha256": receipt["capacity_limits_sha256"],
        "capacity_geometry_hash": receipt["capacity_geometry_hash"],
        "serving_preflight_sha256": receipt["serving_generation_preflight_sha256"],
        "quiescence_sha256": receipt["quiescence_sha256"],
        "receipt_json": canonical_preflight_json(receipt),
        "issued_at": issued_at,
        "expires_at": request.expires_at,
        "created_at": issued_at,
    }


def read_capacity_preflight_receipt(receipt_row: Mapping[str, Any], lease: Any) -> dict[str, Any]:
    """Check stored bytes and CMS metadata against the already verified signed guard."""
    from process import provider_directory_profile_capacity_preflight_contract as base
    from process import provider_directory_profile_initial_contract as initial
    from process.provider_directory_profile_capacity_signing_guard_contract import _HEALTHCARE_RECEIPT_FIELDS

    signed_receipt = lease.signing_preflight_guard["healthcare_receipt"]
    is_cms = signed_receipt["contract_id"] == CMS_PREFLIGHT_CONTRACT
    is_initial = signed_receipt["contract_id"] == initial.RECEIPT_CONTRACT
    fields = _HEALTHCARE_RECEIPT_FIELDS | {"database_binding"} if is_cms else _HEALTHCARE_RECEIPT_FIELDS
    if is_initial:
        fields = fields | {"profile_materialization"}
    raw_receipt = receipt_row.get("receipt_json")
    if isinstance(raw_receipt, str):
        raw_receipt = json.loads(raw_receipt)
    receipt_by_field = dict(_exact_mapping(raw_receipt, fields, reason="stored_receipt_fields_invalid"))
    contract = (
        CMS_PREFLIGHT_CONTRACT
        if is_cms
        else (initial.RECEIPT_CONTRACT if is_initial else CAPACITY_PREFLIGHT_CONTRACT_ID)
    )
    digest = preflight_domain_sha256(
        contract, {name: field_value for name, field_value in receipt_by_field.items() if name != "receipt_sha256"}
    )
    if (
        receipt_by_field != signed_receipt
        or receipt_by_field["receipt_sha256"] != digest
        or (digest != lease.nonce or receipt_row.get("receipt_sha256") != digest)
    ):
        _invalid("stored_receipt_changed")
    if is_cms or is_initial:
        request = base.validated_capacity_preflight_request(lease.signing_preflight_guard["healthcare_request"])
        expected_by_field = capacity_preflight_receipt_row_values(
            request, receipt_by_field, issued_at=base._utc_timestamp(receipt_by_field["issued_at"])
        )
        if any(
            receipt_row.get(name) != field_value
            for name, field_value in expected_by_field.items()
            if name != "receipt_json"
        ):
            _invalid("stored_receipt_metadata_changed")
    return receipt_by_field


def validated_cms_capacity_limits(raw: Any) -> dict[str, Any]:
    """Treat supplied byte maxima as ceilings, never a physical projection."""
    limits_by_field = dict(_exact_mapping(raw, CMS_LIMIT_FIELDS, reason="cms_limits_fields_invalid"))
    if limits_by_field["contract_id"] != CMS_LIMITS_CONTRACT:
        _invalid("limits_contract_invalid")
    if any(
        type(limits_by_field[name]) is not int or not 0 < limits_by_field[name] < 2**63
        for name in CMS_LIMIT_FIELDS - {"contract_id"}
    ):
        _invalid("limits_invalid")
    if limits_by_field["temp_file_limit_bytes_per_backend"] % 1024 or (
        limits_by_field["worker_count"] * limits_by_field["temp_file_limit_bytes_per_backend"]
        > limits_by_field["max_temp_bytes"]
    ):
        _invalid("temporary_worker_ceiling_invalid")
    return limits_by_field


def _paired_profile_envelope(raw: Any) -> dict[str, Any]:
    """Parse a current Profile pair without admitting a recursive CMS envelope."""
    from process import provider_directory_profile_capacity_attestation as lease
    from process import provider_directory_profile_initial_contract as initial
    from process.provider_directory_profile_capacity_attestation_contract import _SIGNED_BODY_FIELDS

    envelope_by_field = dict(_exact_mapping(raw, frozenset({"lease", "signature"}), reason="cms_pair_fields_invalid"))
    body_by_field = dict(
        _exact_mapping(envelope_by_field["lease"], _SIGNED_BODY_FIELDS, reason="cms_pair_body_invalid")
    )
    guard = body_by_field.get("signing_preflight_guard")
    receipt = guard.get("healthcare_receipt") if isinstance(guard, Mapping) else None
    request = guard.get("healthcare_request") if isinstance(guard, Mapping) else None
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("contract_id") not in {CAPACITY_PREFLIGHT_CONTRACT_ID, initial.RECEIPT_CONTRACT}
        or (
            not isinstance(request, Mapping)
            or request.get("contract_id")
            != (
                initial.REQUEST_CONTRACT
                if receipt.get("contract_id") == initial.RECEIPT_CONTRACT
                else CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID
            )
            or CMS_ADMISSION_FIELD in request
        )
    ):
        _invalid("paired_profile_purpose_invalid")
    geometry = receipt.get("capacity_geometry")
    if (
        not isinstance(geometry, Mapping)
        or geometry.get("admission_purpose") is not None
        or (geometry.get("contract_id") == CMS_GEOMETRY_CONTRACT)
    ):
        _invalid("paired_profile_purpose_invalid")
    try:
        lease._decode_signature(envelope_by_field["signature"])
        parsed = lease._parse_capacity_lease_fields(body_by_field)
        lease._parse_capacity_signing_guard_fields(body_by_field, parsed)
        lease._assert_attestation_id(body_by_field)
    except (ValueError, KeyError, TypeError) as error:
        raise ProviderDirectoryProfileCapacityPreflightError("provider_directory_cms_capacity_pair_invalid") from error
    return json.loads(canonical_preflight_json(envelope_by_field))


def validated_cms_capacity_admission(raw: Any) -> dict[str, Any]:
    """Bind one exact Profile envelope and closed reviewed execution ceilings."""
    fields = frozenset({"contract_id", "admission_purpose", "paired_profile_lease", "limits"})
    if isinstance(raw, Mapping) and "registry_source_retention" in raw:
        fields |= {"registry_source_retention"}
    admission_by_field = dict(_exact_mapping(raw, fields, reason="cms_admission_fields_invalid"))
    if (
        admission_by_field["contract_id"] != CMS_ADMISSION_REQUEST_CONTRACT
        or admission_by_field["admission_purpose"] != "cms_nonprofile"
    ):
        _invalid("admission_contract_invalid")
    admission_by_field["limits"] = validated_cms_capacity_limits(admission_by_field["limits"])
    admission_by_field["paired_profile_lease"] = _paired_profile_envelope(admission_by_field["paired_profile_lease"])
    if "registry_source_retention" in admission_by_field:
        admission_by_field["registry_source_retention"] = validated_registry_source_retention_policy(
            admission_by_field["registry_source_retention"]
        )
    return admission_by_field


def validated_cms_preflight_request(raw: Any, *, projection: bool = False):
    """Reuse the existing exact execution/limits/challenge validator for CMS."""
    from process import provider_directory_profile_capacity_preflight_contract as base

    fields = base._AUTHORITY_PROJECTION_REQUEST_FIELDS if projection else base._REQUEST_FIELDS
    request_by_field = dict(_exact_mapping(raw, fields | {CMS_ADMISSION_FIELD}, reason="cms_request_fields_invalid"))
    contract = CMS_PROJECTION_REQUEST_CONTRACT if projection else CMS_PREFLIGHT_REQUEST_CONTRACT
    if request_by_field["contract_id"] != contract:
        _invalid("request_contract_invalid")
    admission = validated_cms_capacity_admission(request_by_field.pop(CMS_ADMISSION_FIELD))
    request_by_field["contract_id"] = (
        CAPACITY_AUTHORITY_PROJECTION_REQUEST_CONTRACT_ID if projection else CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID
    )
    validate = (
        base.validated_capacity_authority_projection_request
        if projection
        else base.validated_capacity_preflight_request
    )
    validated = validate(request_by_field)
    if (
        validated.execution.attestation.operation != "publish"
        or validated.execution.attestation.desired_cms_dataset is None
    ):
        _invalid("desired_cms_publish_required")
    normalized_by_field = {**validated.request_payload, "contract_id": contract, CMS_ADMISSION_FIELD: admission}
    if raw != normalized_by_field:
        _invalid("request_not_canonical")
    return dataclasses.replace(
        validated,
        request_payload=normalized_by_field,
        request_sha256=preflight_domain_sha256(contract, normalized_by_field),
        cms_nonprofile_admission=admission,
    )


def verified_cms_paired_profile_lease(request: Any, *, trust: Any, now: datetime.datetime):
    """Verify the configured authority, current validity and exact execution."""
    from process.provider_directory_profile_capacity_attestation import verify_database_capacity_lease
    from process.provider_directory_profile_capacity_trust import CapacityLeaseTrust

    admission = request.cms_nonprofile_admission
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
    return verified


def validated_cms_capacity_geometry(raw: Any):
    """Parse the existing producer's complete plan and enforce its validation."""
    from process.provider_directory_cms_nonprofile_capacity import _validate_plan
    from process.provider_directory_cms_preparation import NonprofileAdmissionPlan

    fields = frozenset(field.name for field in dataclasses.fields(NonprofileAdmissionPlan))
    geometry_by_field = dict(
        _exact_mapping(raw, fields | {"contract_id", "admission_purpose"}, reason="cms_geometry_fields_invalid")
    )
    if (
        geometry_by_field.pop("contract_id") != CMS_GEOMETRY_CONTRACT
        or geometry_by_field.pop("admission_purpose") != "cms_nonprofile"
    ):
        _invalid("geometry_contract_invalid")
    for name in ("publish_targets", "resource_types", "native_address_targets"):
        if not isinstance(geometry_by_field[name], list):
            _invalid("geometry_scope_invalid")
        geometry_by_field[name] = tuple(geometry_by_field[name])
    reservations = geometry_by_field["reservation_bytes"]
    if not isinstance(reservations, list) or any(
        not isinstance(entry, list) or len(entry) != 2 for entry in reservations
    ):
        _invalid("geometry_reservations_invalid")
    geometry_by_field["reservation_bytes"] = tuple(tuple(entry) for entry in reservations)
    try:
        semantic_date = datetime.date.fromisoformat(geometry_by_field["desired_profile_as_of"])
        if semantic_date.isoformat() != geometry_by_field["desired_profile_as_of"]:
            _invalid("geometry_date_invalid")
        plan = NonprofileAdmissionPlan(**geometry_by_field)
        _validate_plan(plan)
    except (ValueError, TypeError, RuntimeError) as error:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_cms_capacity_geometry_invalid"
        ) from error
    return plan


def validated_cms_database_binding(raw: Any) -> dict[str, Any]:
    """Validate the authenticated database and effective data tablespace pins."""
    from process.provider_directory_profile_capacity_attestation_contract import (
        _database_name_value,
        _database_system_identifier,
        _integer,
        _opaque_id,
    )

    fields = frozenset(
        {"database_system_identifier", "database_oid", "database_name", "tablespace_oid", "tablespace_name"}
    )
    binding_by_field = dict(_exact_mapping(raw, fields, reason="cms_database_binding_fields_invalid"))
    try:
        _database_system_identifier(binding_by_field["database_system_identifier"])
        _database_name_value(binding_by_field["database_name"], field="database_name")
        _opaque_id(binding_by_field["tablespace_name"], field="tablespace_name", maximum_length=63)
        for name in ("database_oid", "tablespace_oid"):
            _integer(binding_by_field[name], field=name, minimum=1, maximum=2**32 - 1)
    except ValueError as error:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_cms_capacity_database_binding_invalid"
        ) from error
    return binding_by_field


def assert_cms_geometry_matches_request(plan: Any, request: Any) -> None:
    """Check engine-pinned monitored budgets against ceilings and the exact paired lease."""
    from process.provider_directory_profile_capacity_attestation_contract import (
        CAPACITY_LEASE_DIGEST_DOMAIN,
        _domain_hash,
    )

    admission = request.cms_nonprofile_admission
    limits = admission["limits"]
    exact_fields = (
        "batch_size",
        "worker_count",
        "temp_file_limit_bytes_per_backend",
        "minimum_remaining_bytes",
        "required_build_seconds",
    )
    reservation_by_class = dict(plan.reservation_bytes)
    attestation = request.execution.attestation
    if (
        any(getattr(plan, name) != limits[name] for name in exact_fields)
        or any(reservation_by_class[name] > limits["max_" + name + "_bytes"] for name in ("data", "temp", "wal"))
        or (
            plan.paired_profile_lease_digest
            != _domain_hash(CAPACITY_LEASE_DIGEST_DOMAIN, admission["paired_profile_lease"])
            or plan.selection_proof_id != attestation.proof_id
            or plan.desired_profile_as_of != attestation.desired_profile_as_of
        )
    ):
        _invalid("geometry_request_changed")
