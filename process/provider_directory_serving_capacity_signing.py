# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind ordinary serving receipts to closed plans and their exact Profile pair."""

from __future__ import annotations

import datetime
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from process import provider_directory_serving_capacity_contract as ordinary
from process.provider_directory_profile_capacity_preflight_contract import (
    ProviderDirectoryProfileCapacityPreflightError,
)

if TYPE_CHECKING:
    from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan


def _invalid(reason: str) -> None:
    """Reject a changed signed plan without exposing supplied material."""
    raise ProviderDirectoryProfileCapacityPreflightError("provider_directory_serving_capacity_" + reason)


def validated_serving_capacity_geometry(raw: Any) -> OrdinaryServingAdmissionPlan:
    """Parse an ordinary plan directly; no CMS lease or geometry is relabelled."""
    from process.provider_directory_cms_nonprofile_capacity import _validate_plan
    from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan

    try:
        values_by_field = ordinary.validate_geometry(raw)
        values_by_field.pop("contract_id")
        values_by_field.pop("admission_purpose")
        scope_json = ordinary.canonical(values_by_field.pop("ordinary_scope"))
        for name in ("publish_targets", "resource_types", "native_address_targets"):
            if type(values_by_field[name]) is not list:
                _invalid("geometry_scope_invalid")
            values_by_field[name] = tuple(values_by_field[name])
        values_by_field["reservation_bytes"] = tuple(tuple(entry) for entry in values_by_field["reservation_bytes"])
        day = values_by_field["desired_profile_as_of"]
        if type(day) is not str or datetime.date.fromisoformat(day).isoformat() != day:
            _invalid("geometry_date_invalid")
        plan = OrdinaryServingAdmissionPlan(**values_by_field, ordinary_scope_json=scope_json)
        _validate_plan(plan)
        return plan
    except (ValueError, TypeError, KeyError, RuntimeError) as error:
        raise ProviderDirectoryProfileCapacityPreflightError(
            "provider_directory_serving_capacity_geometry_invalid"
        ) from error


def _expected_budgets(limits: Mapping[str, Any], scope: Mapping[str, Any], paired_receipt):
    """Add retained data and phase WAL exactly once; temporary capacity is unchanged."""
    if limits["max_wal_bytes"] < 2:
        _invalid("phase_wal_budget_too_small")
    preparation = limits["max_wal_bytes"] // 2
    paired_wal = ordinary.paired_profile_wal_bytes(paired_receipt)
    reservations = (
        ("data", limits["max_data_bytes"] + scope["max_retention_data_bytes"] - scope["resident_retention_data_bytes"]),
        ("temp", limits["max_temp_bytes"]),
        (
            "wal",
            limits["max_wal_bytes"] + scope["max_retention_wal_bytes"] + scope["max_commit_wal_bytes"] + paired_wal,
        ),
    )
    return (
        reservations,
        preparation + scope["max_retention_wal_bytes"],
        limits["max_wal_bytes"] - preparation + scope["max_commit_wal_bytes"] + paired_wal,
    )


def assert_serving_geometry_matches_request(plan: Any, request: Any) -> None:
    """Bind all reviewed ceilings, retained scope, selection, and paired lease bytes."""
    from process.provider_directory_profile_capacity_attestation_contract import (
        CAPACITY_LEASE_DIGEST_DOMAIN,
        _domain_hash,
    )
    from process.provider_directory_serving_capacity_plan import OrdinaryServingAdmissionPlan

    if type(plan) is not OrdinaryServingAdmissionPlan or request.serving_nonprofile_admission is None:
        _invalid("geometry_request_changed")
    admission = request.serving_nonprofile_admission
    limits, scope = admission["limits"], admission["ordinary_scope"]
    exact_fields = (
        "batch_size",
        "worker_count",
        "temp_file_limit_bytes_per_backend",
        "minimum_remaining_bytes",
        "required_build_seconds",
    )
    attestation = request.execution.attestation
    budgets = _expected_budgets(
        limits, scope, admission["paired_profile_lease"]["lease"]["signing_preflight_guard"]["healthcare_receipt"]
    )
    ordinary.require_plan_selection(plan.payload, attestation.payload, scope)
    if (
        any(getattr(plan, name) != limits[name] for name in exact_fields)
        or (plan.reservation_bytes, plan.logging_wal_upper_bound_bytes, plan.cutover_wal_upper_bound_bytes) != budgets
        or json.loads(plan.ordinary_scope_json) != scope
        or plan.paired_profile_lease_digest
        != _domain_hash(CAPACITY_LEASE_DIGEST_DOMAIN, admission["paired_profile_lease"])
        or plan.selection_proof_id != attestation.proof_id
        or plan.desired_profile_as_of != ordinary.require_ordinary_selection(attestation.payload)
    ):
        _invalid("geometry_request_changed")


def assert_serving_receipt_geometry(receipt: Mapping[str, Any], request: Any) -> None:
    """Verify complete plan, projection, database, and the paired serving predecessor."""
    from process.provider_directory_cms_capacity_contract import validated_cms_database_binding
    from process.provider_directory_profile_capacity_signing_guard_contract import _fail

    try:
        plan = validated_serving_capacity_geometry(receipt["capacity_geometry"])
        validated_cms_database_binding(receipt["database_binding"])
        assert_serving_geometry_matches_request(plan, request)
    except ValueError, TypeError, KeyError:
        _fail("serving_geometry_invalid")
    projection = receipt["artifact_scope_projection"]
    paired = request.serving_nonprofile_admission["paired_profile_lease"]["lease"]["signing_preflight_guard"][
        "healthcare_receipt"
    ]
    if (
        not isinstance(projection, Mapping)
        or receipt["capacity_geometry_hash"] != plan.capacity_geometry_hash
        or receipt["required_reservation_bytes_by_storage_class"] != dict(plan.reservation_bytes)
        or projection.get("projection_hash") != plan.artifact_scope_projection_hash
        or receipt["serving_generation_preflight"] != paired["serving_generation_preflight"]
        or receipt["serving_generation_preflight_sha256"] != paired["serving_generation_preflight_sha256"]
    ):
        _fail("serving_geometry_binding_invalid")
