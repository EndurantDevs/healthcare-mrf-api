# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cutover forecast and actual-capacity replay contracts."""

from __future__ import annotations

import copy
import dataclasses

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_capacity_projection import _projection_geometry

def _cutover_target_projection(geometry):
    """Project one evidence insert and one no-op profile target."""
    target_inputs = (
        capacity.ProviderDirectoryProfileTargetDeltaInput(
            relation_name="evidence_target",
            inserted_rows=1,
            inserted_toast_chunks=0,
            deleted_rows=0,
            deleted_logical_bytes=0,
            deleted_toast_chunks=0,
            main_index_pages=(1,),
            toast_index_pages=(1,),
        ),
        capacity.ProviderDirectoryProfileTargetDeltaInput(
            relation_name="profile_target",
            inserted_rows=0,
            inserted_toast_chunks=0,
            deleted_rows=0,
            deleted_logical_bytes=0,
            deleted_toast_chunks=0,
            main_index_pages=(1,),
            toast_index_pages=(1,),
        ),
    )
    return capacity.project_profile_delta_capacity(geometry, target_inputs)


def _cutover_metadata_projection(geometry):
    """Project checkpoint, serving-generation, and receipt metadata writes."""
    metadata_inputs = (
        capacity.ProviderDirectoryProfileMetadataMutationInput(
            relation_name="build_checkpoint",
            operation="update",
            payload_upper_bytes=1_024,
            deleted_toast_chunks=0,
            main_index_pages=(1,),
            toast_index_pages=(1,),
        ),
        capacity.ProviderDirectoryProfileMetadataMutationInput(
            relation_name="serving_generation",
            operation="update",
            payload_upper_bytes=1_024,
            deleted_toast_chunks=0,
            main_index_pages=(1,),
            toast_index_pages=(1,),
        ),
        capacity.ProviderDirectoryProfileMetadataMutationInput(
            relation_name="delta_receipt",
            operation="insert",
            payload_upper_bytes=1_024,
            deleted_toast_chunks=0,
            main_index_pages=(1,),
            toast_index_pages=(1,),
        ),
    )
    return capacity.project_profile_delta_metadata_capacity(
        geometry,
        metadata_inputs,
        pending_commit_items=1,
    )


def _metadata_layout_by_field(fingerprint: str, oid: int) -> dict[str, object]:
    """Return one exact metadata relation layout."""
    return {
        "exact_fingerprint": fingerprint,
        "main_index_oids": [oid],
        "main_index_pages": [1],
        "toast_index_oids": [oid + 1],
        "toast_index_pages": [1],
        "deleted_toast_chunks": 0,
    }


def _cutover_layout_by_name(geometry) -> dict[str, dict[str, object]]:
    """Return exact target and metadata layouts keyed by relation role."""
    evidence_target_by_field = {
        "exact_fingerprint": (
            geometry.evidence_target_storage_fingerprint
        ),
        "main_index_oids": [1],
        "main_index_pages": [1],
        "toast_index_oids": [2],
        "toast_index_pages": [1],
        "inserted_toast_chunks": 0,
        "deleted_toast_chunks": 0,
    }
    profile_target_by_field = {
        **evidence_target_by_field,
        "exact_fingerprint": (
            geometry.profile_target_storage_fingerprint
        ),
        "main_index_oids": [3],
        "toast_index_oids": [4],
    }
    return {
        "evidence_target": evidence_target_by_field,
        "profile_target": profile_target_by_field,
        "build_checkpoint": _metadata_layout_by_field(
            geometry.build_checkpoint_storage_fingerprint, 5
        ),
        "serving_generation": _metadata_layout_by_field(
            geometry.serving_generation_storage_fingerprint, 7
        ),
        "delta_receipt": _metadata_layout_by_field(
            geometry.delta_receipt_storage_fingerprint, 9
        ),
    }


def _cutover_forecast_by_field(
    geometry,
    target_projection,
    metadata_projection,
) -> dict[str, object]:
    """Return the complete signed cutover forecast fixture."""
    layout_by_name = _cutover_layout_by_name(geometry)
    return {
        "contract_id": capacity.CUTOVER_FORECAST_CONTRACT_ID,
        "build_id": "pdpb_" + "a" * 32,
        "run_id": "run_" + "b" * 32,
        "capacity_geometry_hash": capacity.capacity_geometry_hash(geometry),
        "target_projection": dataclasses.asdict(target_projection),
        "metadata_projection": dataclasses.asdict(metadata_projection),
        "wal_start_lsn": "0/1",
        "wal_bytes_before": 0,
        "evidence_target_bytes_before": 0,
        "profile_target_bytes_before": 0,
        "evidence_target_layout": layout_by_name["evidence_target"],
        "profile_target_layout": layout_by_name["profile_target"],
        "build_checkpoint_layout": layout_by_name["build_checkpoint"],
        "serving_generation_layout": layout_by_name["serving_generation"],
        "delta_receipt_layout": layout_by_name["delta_receipt"],
        "build_checkpoint_payload_upper_bytes": 1_024,
        "serving_payload_upper_bytes": 1_024,
        "receipt_payload_upper_bytes": 1_024,
        "pending_commit_items": 1,
    }


def _cutover_actual_by_field(
    metadata_projection,
    forecast_hash: str,
) -> dict[str, object]:
    """Return zero-growth cutover observations bound to the forecast."""
    return {
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


def _cutover_evidence_fixture():
    """Return geometry plus matching cutover forecast and observations."""
    geometry = _projection_geometry()
    target_projection = _cutover_target_projection(geometry)
    metadata_projection = _cutover_metadata_projection(geometry)
    forecast_hash = "f" * 64
    forecast_by_field = _cutover_forecast_by_field(
        geometry,
        target_projection,
        metadata_projection,
    )
    actual_by_field = _cutover_actual_by_field(
        metadata_projection,
        forecast_hash,
    )
    return geometry, forecast_by_field, actual_by_field, forecast_hash


def test_cutover_replay_recomputes_target_and_metadata_formulas():
    geometry, forecast, actual, forecast_hash = (
        _cutover_evidence_fixture()
    )

    capacity.validate_profile_delta_cutover_evidence(
        geometry,
        forecast,
        actual,
        build_id=forecast["build_id"],
        run_id=forecast["run_id"],
        forecast_hash=forecast_hash,
        evidence_inserted=1,
        evidence_deleted=0,
        profile_inserted=0,
        profile_deleted=0,
    )


@pytest.mark.parametrize("tamper", ["target", "metadata"])
def test_cutover_replay_rejects_self_consistent_underforecast(tamper):
    geometry, forecast, actual, forecast_hash = (
        _cutover_evidence_fixture()
    )
    tampered_forecast = copy.deepcopy(forecast)
    tampered_actual = copy.deepcopy(actual)
    if tamper == "target":
        evidence_projection = tampered_forecast["target_projection"][
            "targets"
        ][0]
        evidence_projection["target_growth_bytes"] = 0
        evidence_projection["wal_bytes"] = 0
        tampered_forecast["target_projection"]["target_data_bytes"] = 0
        tampered_forecast["target_projection"]["wal_bytes"] = 0
        expected_reason = "cutover_target_projection_formula_changed"
    else:
        tampered_forecast["metadata_projection"] = {
            "data_bytes": 0,
            "wal_bytes": 0,
            "commit_envelope_bytes": 0,
        }
        tampered_actual["metadata_wal_forecast_bytes"] = 0
        tampered_actual["commit_envelope_bytes"] = 0
        expected_reason = "cutover_metadata_projection_formula_changed"

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match=expected_reason,
    ):
        capacity.validate_profile_delta_cutover_evidence(
            geometry,
            tampered_forecast,
            tampered_actual,
            build_id=forecast["build_id"],
            run_id=forecast["run_id"],
            forecast_hash=forecast_hash,
            evidence_inserted=1,
            evidence_deleted=0,
            profile_inserted=0,
            profile_deleted=0,
        )


@pytest.mark.parametrize(
    "overrides",
    (
        {"postgres_server_version_num": 170006},
        {"postgres_block_size_bytes": 16_384},
        {"postgres_toast_max_chunk_size_bytes": 1995},
        {"postgres_btree_version": 3},
    ),
)
def test_geometry_rejects_unproved_postgres_storage_abi(overrides):
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="postgres_storage_abi_unsupported",
    ):
        capacity.validated_capacity_geometry(
            _geometry_payload(**overrides)
        )
