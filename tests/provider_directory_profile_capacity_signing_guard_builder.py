# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Assembly of the synthetic closed Profile capacity signing chain."""

from __future__ import annotations

import datetime
from typing import Any

from process import provider_directory_profile_capacity_preflight_contract as preflight
from tests import (
    provider_directory_profile_capacity_signing_guard_test_support as support,
)


def _capacity_receipt_material(
    *,
    capacity_geometry_hash: str | None,
    observed_at: datetime.datetime,
    issued_at: datetime.datetime,
    expires_at: datetime.datetime,
    max_build_deadline: datetime.datetime,
    request_nonce: str,
    execution=None,
) -> tuple[dict[str, object], ...]:
    timing = support._GuardTiming(observed_at, issued_at, expires_at, max_build_deadline, request_nonce)
    execution = execution or support.synthetic_profile_execution(
        frozen_profile_input_digest=support.GOLDEN_PROFILE_INPUT_DIGEST,
    )
    execution_by_field = support._execution_by_field(execution)
    limits_by_field = support._limits_payload()
    storage_by_field = support._storage_observation(timing)
    import_request_by_field = support._control_plane_request(
        execution_by_field, limits_by_field, storage_by_field, timing
    )
    execution_request = support._execution_request(execution_by_field, limits_by_field, timing)
    execution_identity_by_field = preflight.profile_execution_identity_payload(execution_request)
    followup_by_field = support._held_followup(execution, timing)
    import_receipt_by_field = support._control_plane_receipt(
        import_request_by_field,
        execution_request,
        execution_identity_by_field,
        storage_by_field,
        followup_by_field,
        timing,
    )
    healthcare_request_by_field = support._healthcare_request(
        execution_by_field, limits_by_field, import_receipt_by_field, timing
    )
    validated_healthcare_request = support._validated_request(healthcare_request_by_field)
    healthcare_receipt_by_field = support._healthcare_receipt(
        validated_healthcare_request,
        import_receipt_by_field,
        execution_identity_by_field,
        limits_by_field,
        capacity_geometry_hash,
        timing,
    )
    return (
        import_request_by_field,
        import_receipt_by_field,
        healthcare_request_by_field,
        validated_healthcare_request,
        healthcare_receipt_by_field,
        followup_by_field,
    )


def build_capacity_signing_guard(
    *,
    capacity_geometry_hash: str | None,
    observed_at: datetime.datetime,
    issued_at: datetime.datetime,
    expires_at: datetime.datetime,
    max_build_deadline: datetime.datetime,
    request_nonce: str,
) -> tuple[dict[str, object], str, str]:
    """Return guard payload, guard digest, and its healthcare receipt ID."""

    (
        import_request_by_field,
        import_receipt_by_field,
        healthcare_request_by_field,
        validated_healthcare_request,
        healthcare_receipt_by_field,
        followup_by_field,
    ) = _capacity_receipt_material(
        capacity_geometry_hash=capacity_geometry_hash,
        observed_at=observed_at,
        issued_at=issued_at,
        expires_at=expires_at,
        max_build_deadline=max_build_deadline,
        request_nonce=request_nonce,
    )
    guard_by_field = support._guard_payload(
        import_request_by_field,
        import_receipt_by_field,
        healthcare_request_by_field,
        validated_healthcare_request,
        healthcare_receipt_by_field,
        followup_by_field,
    )
    guard_sha256 = preflight.preflight_domain_sha256(
        support.guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN,
        guard_by_field,
    )
    return (
        guard_by_field,
        guard_sha256,
        str(healthcare_receipt_by_field["receipt_sha256"]),
    )


def _current_capacity_geometry(limits_by_field, execution_identity_by_field):
    """Reuse the complete current geometry fixture with this selection and runtime."""
    from process import provider_directory_profile_capacity as capacity
    from tests.provider_directory_profile_capacity_runtime_test_support import golden_runtime_witnesses
    from tests.test_provider_directory_profile_capacity import _geometry_payload

    witness_by_field, _legacy_deployment = golden_runtime_witnesses()
    geometry_by_field = _geometry_payload(
        **{name: limit_value for name, limit_value in limits_by_field.items() if name != "contract_id"}
    )
    geometry_by_field.update(
        selection_proof_id=execution_identity_by_field["selection_proof_id"],
        profile_input_digest=execution_identity_by_field["profile_input_digest"],
        profile_schema_version=witness_by_field["profile_schema_version"],
        profile_strategy_version=witness_by_field["profile_strategy_version"],
        postgres_server_version_num=witness_by_field["postgres_server_version_num"],
    )
    geometry = capacity.validated_capacity_geometry(geometry_by_field)
    runtime_by_field = {
        name: witness_by_field[name]
        for name in (
            "healthcare_source_commit",
            "profile_migration_revision",
            "profile_schema_version",
            "profile_strategy_version",
            "postgres_server_version_num",
        )
    }
    return geometry, geometry_by_field, runtime_by_field


def _serving_generation_payload(geometry_by_field):
    """Keep the incumbent's named targets and retained semantic selection."""
    target_by_role = {
        role: {name: geometry_by_field["relations"][role + "_target"][name] for name in ("schema", "name")}
        for role in ("evidence", "profile")
    }
    return {
        "contract_id": "healthporta.provider-directory-profile-serving-generation-preflight.v2",
        "resolution": "existing",
        "generation_id": "pdprofile_" + "44" * 16,
        "authority_revision": 7,
        "control_generation": 10,
        "status": "published",
        "operation": "publish",
        "selection_proof_id": geometry_by_field["selection_proof_id"],
        "profile_schema_version": geometry_by_field["profile_schema_version"],
        "profile_strategy_version": geometry_by_field["profile_strategy_version"],
        "source_vector_hash": geometry_by_field["current_source_vector_hash"],
        "source_context_vector_hash": geometry_by_field["current_context_vector_hash"],
        "executable_plan_hash": geometry_by_field["executable_plan_hash"],
        "capacity_geometry_hash": None,
        "capacity_geometry_status": "legacy_unavailable",
        "evidence_rows": 67,
        "profile_rows": 45,
        "published_at": "2026-07-29T08:00:00+00:00",
        "profile_as_of": "2026-07-29",
        "targets": target_by_role,
    }


def _preflight_storage_payload():
    """Use declared relation coordinates and numeric projection bounds."""
    return {
        "contract_id": "healthporta.provider-directory-profile-capacity-preflight-storage.v2",
        "schema": "synthetic_profile",
        "name": "provider_directory_profile_capacity_preflight_receipt",
        "main_index_pages": [3, 3, 3],
        "toast_index_pages": [2],
        "toastable_column_count": 2,
    }


def build_healthcare_receipt(
    validated_request,
    import_receipt_by_field,
    execution_identity_by_field,
    limits_by_field,
    capacity_geometry_hash,
    timing,
):
    """Derive actual geometry by default; retain explicit hashes for mismatch cases."""
    from process import provider_directory_profile_capacity as capacity

    geometry, geometry_by_field, runtime_by_field = _current_capacity_geometry(
        limits_by_field, execution_identity_by_field
    )
    desired_date = validated_request.execution.attestation.desired_profile_as_of
    if desired_date is not None:
        geometry_by_field["profile_as_of"] = desired_date
        geometry = capacity.validated_capacity_geometry(geometry_by_field)
    serving_by_field = _serving_generation_payload(geometry_by_field)
    quiescence_by_field = support._healthcare_quiescence()
    receipt_by_field = {
        "contract_id": preflight.CAPACITY_PREFLIGHT_CONTRACT_ID,
        "request_contract_id": preflight.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "request_sha256": validated_request.request_sha256,
        "request_nonce": timing.request_nonce,
        "control_plane_receipt_sha256": import_receipt_by_field["receipt_sha256"],
        "issued_at": support._utc(timing.issued_at + datetime.timedelta(seconds=1)),
        "expires_at": support._utc(timing.expires_at),
        "profile_execution_identity": execution_identity_by_field,
        "capacity_limits": limits_by_field,
        "capacity_limits_sha256": validated_request.limits_sha256,
        "capacity_geometry_hash": capacity.capacity_geometry_hash(geometry)
        if capacity_geometry_hash is None
        else capacity_geometry_hash,
        "capacity_geometry": capacity.capacity_geometry_payload(geometry),
        "required_reservation_bytes_by_storage_class": geometry.reservation_bytes_by_storage_class,
        "artifact_scope_projection": {
            "projected_rows": 1,
            "projected_logical_bytes": geometry_by_field["artifact_scope_projected_logical_bytes"],
            "projection_hash": geometry_by_field["artifact_scope_projection_hash"],
        },
        "runtime_observation": {
            "contract_id": "healthporta.provider-directory-profile-runtime-observation.v1",
            **runtime_by_field,
        },
        "serving_generation_preflight": serving_by_field,
        "serving_generation_preflight_sha256": preflight.preflight_domain_sha256(
            preflight.CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN, serving_by_field
        ),
        "quiescence": quiescence_by_field,
        "quiescence_sha256": preflight.preflight_domain_sha256(
            preflight.CAPACITY_QUIESCENCE_DIGEST_DOMAIN, quiescence_by_field
        ),
        "preflight_receipt_storage": _preflight_storage_payload(),
    }
    receipt_by_field["receipt_sha256"] = preflight.preflight_domain_sha256(
        preflight.CAPACITY_PREFLIGHT_CONTRACT_ID, receipt_by_field
    )
    return receipt_by_field


__all__ = ("build_capacity_signing_guard", "build_healthcare_receipt")
