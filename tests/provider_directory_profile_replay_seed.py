# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Committed receipt and serving-generation seed fixtures."""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import importlib
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import null
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import MetaData

from db.models.system import (
    ProviderDirectoryProfileBuildCheckpoint,
    ProviderDirectoryProfileCapacityLeaseConsumption,
    ProviderDirectoryProfileDeltaReceipt,
    ProviderDirectoryProfileServingGeneration,
)
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_attestation as lease
from process.provider_directory_profile_capacity_attestation_contract import (
    CapacityLeaseConsumptionBinding,
)
from process.provider_directory_profile_selection_contract import (
    ProviderDirectoryProfileExecution,
    ProviderDirectoryProfileSelectionAttestation,
)
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_capacity_attestation import (
    _signed_envelope,
)
from tests.provider_directory_profile_capacity_trust_fixtures import (
    capacity_trust_from_envelope,
)
from tests.provider_directory_profile_delta_test_support import _delta_database


importer = importlib.import_module("process.provider_directory_fhir")
UTC = datetime.timezone.utc
from tests.provider_directory_profile_replay_test_support import _utc_text


@dataclasses.dataclass(frozen=True)
class _ReplaySeed:
    """Inputs required to seed one committed profile-delta replay."""

    database: object
    tables: tuple[object, object, object]
    run_id: str
    build_id: str
    geometry: object
    envelope: dict[str, object]
    accepted_at: datetime.datetime
    committed_at: datetime.datetime
    source_vector: tuple[tuple[str, str], ...]
    source_context_vector: tuple[tuple[str, str], ...]

    @property
    def geometry_hash(self) -> str:
        return capacity.capacity_geometry_hash(self.geometry)

    @property
    def generation_id(self) -> str:
        digest = hashlib.sha256(
            f"{self.build_id}:{self.geometry.profile_as_of}".encode("utf-8")
        ).hexdigest()
        return "pdprofile_" + digest[:32]


def _replay_capacity_consumption(seed: _ReplaySeed) -> dict[str, object]:
    """Verify and bind the signed capacity lease to the seeded replay."""
    geometry = seed.geometry
    verified_lease = lease.verify_database_capacity_lease(
        seed.envelope,
        trust=capacity_trust_from_envelope(seed.envelope),
        now=seed.accepted_at,
        expected_capacity_geometry_hash=seed.geometry_hash,
        expected_database_system_identifier=(
            geometry.database_system_identifier
        ),
        expected_database_oid=geometry.database_oid,
        expected_database_name=geometry.database_name,
    )
    return lease.capacity_lease_consumption_values(
        verified_lease,
        CapacityLeaseConsumptionBinding(
            run_id=seed.run_id,
            build_id=seed.build_id,
            executable_plan_hash=geometry.executable_plan_hash,
            selection_proof_id=geometry.selection_proof_id,
            source_vector_hash=geometry.desired_source_vector_hash,
            source_context_vector_hash=geometry.desired_context_vector_hash,
            profile_as_of=geometry.profile_as_of,
        ),
        accepted_at=seed.accepted_at,
    )


def _metadata_layout_by_field(
    exact_fingerprint: str,
    *,
    main_index_oid: int,
    toast_index_oid: int,
) -> dict[str, object]:
    """Return one deterministic metadata relation layout."""
    return {
        "exact_fingerprint": exact_fingerprint,
        "main_index_oids": [main_index_oid],
        "main_index_pages": [1],
        "toast_index_oids": [toast_index_oid],
        "toast_index_pages": [1],
        "deleted_toast_chunks": 0,
    }


def _replay_metadata_projection(seed: _ReplaySeed):
    """Project the three immutable metadata mutations for replay."""
    return capacity.project_profile_delta_metadata_capacity(
        seed.geometry,
        (
            capacity.ProviderDirectoryProfileMetadataMutationInput(
                relation_name="build_checkpoint",
                operation="update",
                payload_upper_bytes=64 * 1024,
                deleted_toast_chunks=0,
                main_index_pages=(1,),
                toast_index_pages=(1,),
            ),
            capacity.ProviderDirectoryProfileMetadataMutationInput(
                relation_name="serving_generation",
                operation="update",
                payload_upper_bytes=4_096,
                deleted_toast_chunks=0,
                main_index_pages=(1,),
                toast_index_pages=(1,),
            ),
            capacity.ProviderDirectoryProfileMetadataMutationInput(
                relation_name="delta_receipt",
                operation="insert",
                payload_upper_bytes=64 * 1024,
                deleted_toast_chunks=0,
                main_index_pages=(1,),
                toast_index_pages=(1,),
            ),
        ),
        pending_commit_items=0,
    )


def _empty_target_layout(
    exact_fingerprint: str,
    *,
    main_index_oid: int,
    toast_index_oid: int,
) -> dict[str, object]:
    """Return an unchanged target relation layout."""
    return {
        "exact_fingerprint": exact_fingerprint,
        "main_index_oids": [main_index_oid],
        "main_index_pages": [1],
        "toast_index_oids": [toast_index_oid],
        "toast_index_pages": [1],
        "inserted_toast_chunks": 0,
        "deleted_toast_chunks": 0,
    }


def _empty_target_projection() -> dict[str, object]:
    """Return the zero-row target growth forecast."""
    return {
        "targets": [
            {
                "relation_name": relation_name,
                "target_growth_bytes": 0,
                "deleted_logical_bytes": 0,
                "wal_bytes": 0,
            }
            for relation_name in ("evidence_target", "profile_target")
        ],
        "target_data_bytes": 0,
        "wal_bytes": 0,
    }


def _replay_forecast(seed: _ReplaySeed, metadata_projection):
    """Return the exact committed replay cutover forecast."""
    geometry = seed.geometry
    forecast_by_field = {
        "contract_id": capacity.CUTOVER_FORECAST_CONTRACT_ID,
        "build_id": seed.build_id,
        "run_id": seed.run_id,
        "capacity_geometry_hash": seed.geometry_hash,
        "target_projection": _empty_target_projection(),
        "metadata_projection": dataclasses.asdict(metadata_projection),
        "wal_start_lsn": "0/1",
        "wal_bytes_before": 0,
        "evidence_target_bytes_before": 0,
        "profile_target_bytes_before": 0,
        "evidence_target_layout": _empty_target_layout(
            geometry.evidence_target_storage_fingerprint,
            main_index_oid=1,
            toast_index_oid=2,
        ),
        "profile_target_layout": _empty_target_layout(
            geometry.profile_target_storage_fingerprint,
            main_index_oid=3,
            toast_index_oid=4,
        ),
        "build_checkpoint_layout": _metadata_layout_by_field(
            geometry.build_checkpoint_storage_fingerprint,
            main_index_oid=5,
            toast_index_oid=6,
        ),
        "serving_generation_layout": _metadata_layout_by_field(
            geometry.serving_generation_storage_fingerprint,
            main_index_oid=7,
            toast_index_oid=8,
        ),
        "delta_receipt_layout": _metadata_layout_by_field(
            geometry.delta_receipt_storage_fingerprint,
            main_index_oid=9,
            toast_index_oid=10,
        ),
        "build_checkpoint_payload_upper_bytes": 64 * 1024,
        "serving_payload_upper_bytes": 4_096,
        "receipt_payload_upper_bytes": 64 * 1024,
        "pending_commit_items": 0,
    }
    return forecast_by_field


def _replay_forecast_hash(forecast_by_field: dict[str, object]) -> str:
    """Hash a replay forecast with its immutable contract identity."""
    return importer._identity_hash(
        {
            "contract": "provider-directory-profile-cutover-forecast-hash-v1",
            "forecast": forecast_by_field,
        }
    )


def _replay_actual(
    forecast_hash: str,
    metadata_projection,
) -> dict[str, object]:
    """Return the exact zero-row cutover observations."""
    actual_by_field = {
        "contract_id": capacity.CUTOVER_ACTUAL_CONTRACT_ID,
        "forecast_hash": forecast_hash,
        "wal_start_lsn": "0/1",
        "target_wal_start_lsn": "0/1",
        "wal_observed_lsn": "0/1",
        "cutover_wal_bytes": 0,
        "evidence_target_bytes_before": 0,
        "evidence_target_bytes_after": 0,
        "evidence_target_growth_bytes": 0,
        "profile_target_bytes_before": 0,
        "profile_target_bytes_after": 0,
        "profile_target_growth_bytes": 0,
        "metadata_wal_forecast_bytes": metadata_projection.wal_bytes,
        "commit_envelope_bytes": metadata_projection.commit_envelope_bytes,
    }
    return actual_by_field


def _replay_actual_hash(actual_by_field: dict[str, object]) -> str:
    """Hash replay observations with their immutable contract identity."""
    return importer._identity_hash(
        {
            "contract": "provider-directory-profile-cutover-actual-hash-v1",
            "actual": actual_by_field,
        }
    )


def _replay_receipt_values(
    seed: _ReplaySeed,
    forecast_by_field: dict[str, object],
    actual_by_field: dict[str, object],
) -> dict[str, object]:
    """Return the immutable committed delta receipt row."""
    geometry = seed.geometry
    return {
        "build_id": seed.build_id,
        "executable_plan_hash": geometry.executable_plan_hash,
        "from_capacity_geometry_status": "legacy_unavailable",
        "from_capacity_geometry_hash": None,
        "from_capacity_geometry_json": null(),
        "capacity_geometry_status": "verified",
        "capacity_geometry_hash": seed.geometry_hash,
        "capacity_geometry_json": capacity.capacity_geometry_payload(geometry),
        "from_source_vector_hash": geometry.current_source_vector_hash,
        "to_source_vector_hash": geometry.desired_source_vector_hash,
        "from_source_context_vector_hash": (
            geometry.current_context_vector_hash
        ),
        "to_source_context_vector_hash": (
            geometry.desired_context_vector_hash
        ),
        "from_generation_id": "pdprofile_" + "1" * 32,
        "generation_id": seed.generation_id,
        "operation": "publish",
        "profile_as_of": geometry.profile_as_of,
        "selection_proof_id": geometry.selection_proof_id,
        "control_generation": 7,
        "authority_revision": 7,
        "evidence_target_oid": geometry.evidence_target_oid,
        "profile_target_oid": geometry.profile_target_oid,
        "evidence_rows": 0,
        "profile_rows": 0,
        "evidence_inserted": 0,
        "evidence_deleted": 0,
        "profile_inserted": 0,
        "profile_deleted": 0,
        "cutover_forecast_hash": _replay_forecast_hash(forecast_by_field),
        "cutover_forecast_json": forecast_by_field,
        "cutover_actual_hash": _replay_actual_hash(actual_by_field),
        "cutover_actual_json": actual_by_field,
        "cutover_wal_start_lsn": "0/1",
        "cutover_wal_observed_lsn": "0/1",
        "cutover_wal_bytes": 0,
        "evidence_target_bytes_before": 0,
        "evidence_target_bytes_after": 0,
        "evidence_target_growth_bytes": 0,
        "profile_target_bytes_before": 0,
        "profile_target_bytes_after": 0,
        "profile_target_growth_bytes": 0,
        "committed_at": seed.committed_at,
    }


def _replay_serving_values(
    seed: _ReplaySeed,
    forecast_by_field: dict[str, object],
) -> dict[str, object]:
    """Return the serving-generation row corresponding to the receipt."""
    geometry = seed.geometry
    return {
        "singleton_key": "global",
        "status": "published",
        "operation": "publish",
        "control_generation": 7,
        "generation_id": seed.generation_id,
        "selection_proof_id": geometry.selection_proof_id,
        "authority_revision": 7,
        "profile_schema_version": profile.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "source_vector_hash": geometry.desired_source_vector_hash,
        "source_vector_json": (
            importer._provider_directory_profile_source_vector_json(
                seed.source_vector
            )
        ),
        "source_context_vector_hash": geometry.desired_context_vector_hash,
        "source_context_vector_json": (
            importer._provider_directory_profile_source_context_vector_json(
                seed.source_context_vector
            )
        ),
        "executable_plan_hash": geometry.executable_plan_hash,
        "capacity_geometry_status": "verified",
        "capacity_geometry_hash": seed.geometry_hash,
        "capacity_geometry_json": capacity.capacity_geometry_payload(geometry),
        "cutover_forecast_hash": _replay_forecast_hash(forecast_by_field),
        "evidence_target_oid": geometry.evidence_target_oid,
        "profile_target_oid": geometry.profile_target_oid,
        "evidence_rows": 0,
        "profile_rows": 0,
        "profile_as_of": geometry.profile_as_of,
        "published_at": seed.committed_at,
        "created_at": seed.accepted_at,
        "updated_at": seed.committed_at,
    }


async def _insert_replay_rows(seed: _ReplaySeed) -> None:
    """Seed the signed lease, immutable receipt, and serving generation."""
    serving_table, receipt_table, consumption_table = seed.tables
    metadata_projection = _replay_metadata_projection(seed)
    forecast_by_field = _replay_forecast(seed, metadata_projection)
    actual_by_field = _replay_actual(
        _replay_forecast_hash(forecast_by_field),
        metadata_projection,
    )
    await seed.database.insert(consumption_table).values(
        **_replay_capacity_consumption(seed)
    ).status()
    await seed.database.insert(receipt_table).values(
        **_replay_receipt_values(seed, forecast_by_field, actual_by_field)
    ).status()
    await seed.database.insert(serving_table).values(
        **_replay_serving_values(seed, forecast_by_field)
    ).status()
