# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic closed lease-v3 signing guard_by_field shared by capacity tests."""

from __future__ import annotations

import datetime
import os
from dataclasses import dataclass

from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_capacity_signing_guard as guard_contract
from process import provider_directory_profile as profile_artifact
from process import provider_directory_profile_selection as selection
from tests.test_provider_directory_profile_capacity_runtime import (
    _limits_payload,
)


SYNTHETIC_PROFILE_SOURCE_ID = "synthetic_profile_source"


def _utc(value: datetime.datetime) -> str:
    return value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validated_request(raw_request: dict[str, object]):
    previous_node_id = os.environ.get("HLTHPRT_IMPORT_NODE_ID")
    os.environ["HLTHPRT_IMPORT_NODE_ID"] = "dev-node"
    try:
        return preflight.validated_capacity_preflight_request(raw_request)
    finally:
        if previous_node_id is None:
            os.environ.pop("HLTHPRT_IMPORT_NODE_ID", None)
        else:
            os.environ["HLTHPRT_IMPORT_NODE_ID"] = previous_node_id


def synthetic_profile_execution() -> selection.ProviderDirectoryProfileExecution:
    """Build the neutral exact selection used by the cross-repository golden."""

    computed = selection._computed_selection_from_rows(
        _synthetic_profile_catalog(),
        node_id="dev-node",
        source_rows=_synthetic_profile_source_rows(),
        dataset_rows=[_synthetic_profile_dataset_row()],
    )
    identity_by_field = {**computed.identity_payload, "authority_revision": 7}
    attestation_by_field = {
        **identity_by_field,
        "proof_id": selection._proof_id(identity_by_field),
    }
    return selection.ProviderDirectoryProfileExecution(
        attestation=selection.validated_profile_selection_attestation(
            attestation_by_field
        ),
        generation=11,
    )


def _synthetic_profile_catalog() -> dict[str, object]:
    return {
        "catalog_digest": "a" * 64,
        "items": [
            {
                "entry_id": "synthetic-profile",
                "runnable": True,
                "profile_enabled": True,
                "source_ids": [SYNTHETIC_PROFILE_SOURCE_ID],
            }
        ],
    }


def _synthetic_profile_source_rows() -> list[dict[str, object]]:
    return [
        {
            "source_id": SYNTHETIC_PROFILE_SOURCE_ID,
            "endpoint_id": "synthetic-endpoint-1",
            "canonical_api_base": "https://synthetic.invalid/fhir",
            "org_name": "Synthetic Organization",
            "plan_name": "Synthetic Plan",
        },
        *[
            {
                "source_id": source_id,
                "endpoint_id": endpoint_id,
                "canonical_api_base": "https://synthetic.invalid/R4",
                "org_name": "Synthetic dataset variant",
                "plan_name": None,
            }
            for source_id, endpoint_id in (
                profile_artifact.configured_dataset_scoped_profile_endpoints()
            )
        ],
    ]


def _synthetic_profile_dataset_row() -> dict[str, object]:
    return {
        "endpoint_id": "synthetic-endpoint-1",
        "dataset_id": "synthetic-dataset-1",
        "acquisition_root_run_id": "synthetic-run-root-1",
        "dataset_hash": "b" * 64,
        "status": "published",
        "is_current": True,
        "resource_count": 42,
        "validated_at": "2026-07-20 10:00:00",
        "published_at": "2026-07-20 11:00:00",
        "superseded_at": None,
        "publication_metadata_json": {
            "source_ids": [SYNTHETIC_PROFILE_SOURCE_ID],
            "selected_resources": ["Practitioner"],
        },
    }


def synthetic_profile_task(
    capacity_attestation: dict[str, object],
) -> dict[str, object]:
    """Return the complete neutral execution document frozen cross-repository."""

    execution = synthetic_profile_execution()
    return {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": execution.generation,
        "provider_directory_profile_selection_attestation": (
            execution.attestation.payload
        ),
        "provider_directory_profile_capacity_attestation": capacity_attestation,
    }


@dataclass(frozen=True)
class _GuardTiming:
    observed_at: datetime.datetime
    issued_at: datetime.datetime
    expires_at: datetime.datetime
    max_build_deadline: datetime.datetime
    request_nonce: str


def _execution_by_field(
    execution: selection.ProviderDirectoryProfileExecution,
) -> dict[str, object]:
    return {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": execution.generation,
        "provider_directory_profile_selection_attestation": (
            execution.attestation.payload
        ),
        "provider_directory_profile_capacity_attestation": {},
    }


def _storage_observation(timing: _GuardTiming) -> dict[str, object]:
    common_volume_by_field = {
        "available_bytes": 1_000_000_000_000,
        "available_after_all_reservations_bytes": 650_000_000_000,
    }
    return {
        "observed_at": _utc(timing.observed_at),
        "issued_at": _utc(timing.issued_at),
        "expires_at": _utc(timing.expires_at),
        "max_build_deadline": _utc(timing.max_build_deadline),
        "temp_tablespace": {
            "tablespace_name": "pg_default",
            "tablespace_oid": 1663,
            "volume_digest": "33" * 32,
        },
        "volumes": [
            {
                "volume_class": "data",
                "volume_digest": "33" * 32,
                **common_volume_by_field,
            },
            {
                "volume_class": "temp",
                "volume_digest": "33" * 32,
                **common_volume_by_field,
            },
            {
                "volume_class": "wal",
                "volume_digest": "44" * 32,
                **common_volume_by_field,
            },
        ],
    }


def _control_plane_request(
    execution_by_field: dict[str, object],
    limits_by_field: dict[str, object],
    storage_by_field: dict[str, object],
    timing: _GuardTiming,
) -> dict[str, object]:
    return {
        "contract_id": guard_contract.CONTROL_PLANE_PREFLIGHT_REQUEST_CONTRACT_ID,
        "profile_execution": execution_by_field,
        "provider_directory_profile_capacity_limits": limits_by_field,
        "storage_observation": storage_by_field,
        "signing_intent": {
            "contract_id": guard_contract.CONTROL_PLANE_SIGNING_INTENT_CONTRACT_ID,
            "request_nonce": timing.request_nonce,
        },
    }


def _execution_request(
    execution_by_field: dict[str, object],
    limits_by_field: dict[str, object],
    timing: _GuardTiming,
):
    return _validated_request(
        {
            "contract_id": preflight.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
            "profile_execution": execution_by_field,
            "provider_directory_profile_capacity_limits": limits_by_field,
            "signing_guard": {
                "contract_id": preflight.CAPACITY_SIGNING_GUARD_REQUEST_CONTRACT_ID,
                "request_nonce": timing.request_nonce,
                "control_plane_receipt_sha256": "00" * 32,
                "expires_at": _utc(timing.expires_at),
            },
        }
    )


def _held_followup(
    execution: selection.ProviderDirectoryProfileExecution,
    timing: _GuardTiming,
) -> dict[str, object]:
    return {
        "profile_key": "provider-directory-global-profile",
        "node_id": execution.attestation.node_id,
        "desired_generation": execution.generation,
        "applied_generation": execution.generation - 1,
        "authority_epoch": 7,
        "status": "queued",
        "hold_until": _utc(timing.expires_at + datetime.timedelta(minutes=5)),
        "descriptor_sha256": "66" * 32,
        "followup_preimage_sha256": "77" * 32,
    }


def _control_plane_receipt(
    request_by_field: dict[str, object],
    execution_request,
    execution_identity_by_field: dict[str, object],
    storage_by_field: dict[str, object],
    followup_by_field: dict[str, object],
    timing: _GuardTiming,
) -> dict[str, object]:
    quiescence_by_field = {
        "contract_id": guard_contract.CONTROL_PLANE_QUIESCENCE_CONTRACT_ID,
        "followup_preimage_sha256": followup_by_field["followup_preimage_sha256"],
        "active_profile_run_count": 0,
        "active_held_dispatch_count": 0,
    }
    receipt_by_field = {
        "contract_id": guard_contract.CONTROL_PLANE_PREFLIGHT_CONTRACT_ID,
        "request_contract_id": (
            guard_contract.CONTROL_PLANE_PREFLIGHT_REQUEST_CONTRACT_ID
        ),
        "request_sha256": preflight.preflight_domain_sha256(
            guard_contract.CONTROL_PLANE_REQUEST_DIGEST_DOMAIN,
            request_by_field,
        ),
        "request_nonce": timing.request_nonce,
        "issued_at": _utc(timing.issued_at),
        "expires_at": _utc(timing.expires_at),
        "max_build_deadline": _utc(timing.max_build_deadline),
        "profile_execution_identity": execution_identity_by_field,
        "capacity_limits_sha256": execution_request.limits_sha256,
        "storage_observation_sha256": preflight.preflight_domain_sha256(
            guard_contract.CONTROL_PLANE_STORAGE_OBSERVATION_DIGEST_DOMAIN,
            storage_by_field,
        ),
        "held_followup": followup_by_field,
        "quiescence": quiescence_by_field,
        "quiescence_sha256": preflight.preflight_domain_sha256(
            guard_contract.CONTROL_PLANE_QUIESCENCE_DIGEST_DOMAIN,
            quiescence_by_field,
        ),
    }
    receipt_by_field["receipt_sha256"] = preflight.preflight_domain_sha256(
        guard_contract.CONTROL_PLANE_PREFLIGHT_CONTRACT_ID,
        receipt_by_field,
    )
    return receipt_by_field


def _healthcare_request(
    execution_by_field: dict[str, object],
    limits_by_field: dict[str, object],
    import_receipt_by_field: dict[str, object],
    timing: _GuardTiming,
) -> dict[str, object]:
    return {
        "contract_id": preflight.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "profile_execution": execution_by_field,
        "provider_directory_profile_capacity_limits": limits_by_field,
        "signing_guard": {
            "contract_id": preflight.CAPACITY_SIGNING_GUARD_REQUEST_CONTRACT_ID,
            "request_nonce": timing.request_nonce,
            "control_plane_receipt_sha256": import_receipt_by_field["receipt_sha256"],
            "expires_at": _utc(timing.expires_at),
        },
    }


def _healthcare_quiescence() -> dict[str, object]:
    return {
        "contract_id": preflight.CAPACITY_QUIESCENCE_CONTRACT_ID,
        "active_profile_run_count": 0,
        "claimed_profile_checkpoint_count": 0,
        "unexpired_capacity_consumption_count": 0,
        "outstanding_preflight_receipt_count": 0,
        "active_profile_run_statuses": [
            "queued",
            "starting",
            "running",
            "finalizing",
            "canceling",
        ],
        "claimed_checkpoint_states": [
            "building_evidence",
            "evidence_complete",
            "building_profile",
            "ready",
        ],
        "capacity_consumption_boundary": "unexpired",
        "preflight_receipt_boundary": "unconsumed_and_unexpired",
    }


def _healthcare_receipt(
    validated_request,
    import_receipt_by_field: dict[str, object],
    execution_identity_by_field: dict[str, object],
    limits_by_field: dict[str, object],
    capacity_geometry_hash: str,
    timing: _GuardTiming,
) -> dict[str, object]:
    serving_by_field = {
        "contract_id": preflight.CAPACITY_SERVING_PREFLIGHT_CONTRACT_ID,
        "resolution": "existing",
    }
    quiescence_by_field = _healthcare_quiescence()
    receipt_by_field = {
        "contract_id": preflight.CAPACITY_PREFLIGHT_CONTRACT_ID,
        "request_contract_id": preflight.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "request_sha256": validated_request.request_sha256,
        "request_nonce": timing.request_nonce,
        "control_plane_receipt_sha256": import_receipt_by_field["receipt_sha256"],
        "issued_at": _utc(timing.issued_at + datetime.timedelta(seconds=1)),
        "expires_at": _utc(timing.expires_at),
        "profile_execution_identity": execution_identity_by_field,
        "capacity_limits": limits_by_field,
        "capacity_limits_sha256": validated_request.limits_sha256,
        "capacity_geometry_hash": capacity_geometry_hash,
        "capacity_geometry": {"contract_id": "synthetic-geometry.v1"},
        "required_reservation_bytes_by_storage_class": {
            "data": 180_000_000_000,
            "temp": 20_000_000_000,
            "wal": 150_000_000_000,
        },
        "artifact_scope_projection": {
            "projected_rows": 1,
            "projected_logical_bytes": 1,
            "projection_hash": "88" * 32,
        },
        "runtime_observation": {
            "contract_id": "healthporta.provider-directory-profile-runtime-observation.v1"
        },
        "serving_generation_preflight": serving_by_field,
        "serving_generation_preflight_sha256": preflight.preflight_domain_sha256(
            preflight.CAPACITY_SERVING_PREFLIGHT_DIGEST_DOMAIN,
            serving_by_field,
        ),
        "quiescence": quiescence_by_field,
        "quiescence_sha256": preflight.preflight_domain_sha256(
            preflight.CAPACITY_QUIESCENCE_DIGEST_DOMAIN,
            quiescence_by_field,
        ),
        "preflight_receipt_storage": {
            "contract_id": "healthporta.provider-directory-profile-capacity-preflight-storage.v1"
        },
    }
    receipt_by_field["receipt_sha256"] = preflight.preflight_domain_sha256(
        preflight.CAPACITY_PREFLIGHT_CONTRACT_ID,
        receipt_by_field,
    )
    return receipt_by_field


def _guard_payload(
    import_request_by_field: dict[str, object],
    import_receipt_by_field: dict[str, object],
    healthcare_request_by_field: dict[str, object],
    validated_healthcare_request,
    healthcare_receipt_by_field: dict[str, object],
    followup_by_field: dict[str, object],
) -> dict[str, object]:
    return {
        "contract_id": guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_CONTRACT_ID,
        "bundle_validation_path": guard_contract.CAPACITY_BUNDLE_VALIDATION_PATH,
        "control_plane_preflight_path": guard_contract.CONTROL_PLANE_PREFLIGHT_PATH,
        "control_plane_request": import_request_by_field,
        "control_plane_request_sha256": import_receipt_by_field["request_sha256"],
        "control_plane_receipt": import_receipt_by_field,
        "control_plane_receipt_sha256": import_receipt_by_field["receipt_sha256"],
        "healthcare_preflight_path": guard_contract.HEALTHCARE_PREFLIGHT_PATH,
        "healthcare_request": healthcare_request_by_field,
        "healthcare_request_sha256": validated_healthcare_request.request_sha256,
        "healthcare_receipt": healthcare_receipt_by_field,
        "healthcare_receipt_sha256": healthcare_receipt_by_field["receipt_sha256"],
        "capacity_limits_sha256": validated_healthcare_request.limits_sha256,
        "storage_observation_sha256": import_receipt_by_field[
            "storage_observation_sha256"
        ],
        "held_followup_preimage_sha256": followup_by_field["followup_preimage_sha256"],
    }


def capacity_signing_guard(
    *,
    capacity_geometry_hash: str,
    observed_at: datetime.datetime,
    issued_at: datetime.datetime,
    expires_at: datetime.datetime,
    max_build_deadline: datetime.datetime,
    request_nonce: str = "22" * 32,
) -> tuple[dict[str, object], str, str]:
    """Return guard payload, guard digest, and its healthcare receipt ID."""

    from tests.provider_directory_profile_capacity_signing_guard_builder import (
        build_capacity_signing_guard,
    )

    return build_capacity_signing_guard(
        capacity_geometry_hash=capacity_geometry_hash,
        observed_at=observed_at,
        issued_at=issued_at,
        expires_at=expires_at,
        max_build_deadline=max_build_deadline,
        request_nonce=request_nonce,
    )


__all__ = (
    "SYNTHETIC_PROFILE_SOURCE_ID",
    "capacity_signing_guard",
    "synthetic_profile_execution",
    "synthetic_profile_task",
)
