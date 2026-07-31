# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Recompute and compare bounded Profile cutover projections."""

from __future__ import annotations

import dataclasses
import json
from typing import Any, Mapping

from process.provider_directory_profile_capacity_cutover_contract import (
    _cutover_nonnegative_integer,
)
from process.provider_directory_profile_capacity_geometry import _error
from process.provider_directory_profile_capacity_physical import (
    project_profile_delta_metadata_capacity,
)
from process.provider_directory_profile_capacity_target import (
    project_profile_delta_capacity,
)
from process.provider_directory_profile_capacity_types import (
    ProviderDirectoryProfileCapacityGeometry,
    ProviderDirectoryProfileDeltaProjection,
    ProviderDirectoryProfileMetadataMutationInput,
    ProviderDirectoryProfileTargetDeltaInput,
)


@dataclasses.dataclass(frozen=True)
class _TargetEvidence:
    projection_by_field: Mapping[str, Any]
    target_value_tuples: tuple[tuple[int, int, int], ...]
    counts_by_name: Mapping[str, int]
    wal_bytes: int


@dataclasses.dataclass(frozen=True)
class _MetadataEvidence:
    projection_by_field: Mapping[str, Any]
    wal_bytes: int
    commit_envelope_bytes: int


@dataclasses.dataclass(frozen=True)
class _CutoverLayouts:
    evidence_target: tuple[tuple[int, ...], tuple[int, ...], int, int]
    profile_target: tuple[tuple[int, ...], tuple[int, ...], int, int]
    build_checkpoint: tuple[tuple[int, ...], tuple[int, ...], int, int]
    serving_generation: tuple[tuple[int, ...], tuple[int, ...], int, int]
    delta_receipt: tuple[tuple[int, ...], tuple[int, ...], int, int]


def _recomputed_target_projection(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    target_evidence: _TargetEvidence,
    layouts: _CutoverLayouts,
) -> ProviderDirectoryProfileDeltaProjection:
    counts_by_name = target_evidence.counts_by_name
    target_value_tuples = target_evidence.target_value_tuples
    return project_profile_delta_capacity(
        geometry,
        (
            ProviderDirectoryProfileTargetDeltaInput(
                relation_name="evidence_target",
                inserted_rows=counts_by_name["evidence_inserted"],
                inserted_toast_chunks=layouts.evidence_target[2],
                deleted_rows=counts_by_name["evidence_deleted"],
                deleted_logical_bytes=target_value_tuples[0][1],
                deleted_toast_chunks=layouts.evidence_target[3],
                main_index_pages=layouts.evidence_target[0],
                toast_index_pages=layouts.evidence_target[1],
            ),
            ProviderDirectoryProfileTargetDeltaInput(
                relation_name="profile_target",
                inserted_rows=counts_by_name["profile_inserted"],
                inserted_toast_chunks=layouts.profile_target[2],
                deleted_rows=counts_by_name["profile_deleted"],
                deleted_logical_bytes=target_value_tuples[1][1],
                deleted_toast_chunks=layouts.profile_target[3],
                main_index_pages=layouts.profile_target[0],
                toast_index_pages=layouts.profile_target[1],
            ),
        ),
    )


def _assert_target_formula(
    target_evidence: _TargetEvidence,
    recomputed_target: ProviderDirectoryProfileDeltaProjection,
) -> None:
    retained_projection = json.loads(
        json.dumps(
            target_evidence.projection_by_field,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    recomputed_projection = json.loads(
        json.dumps(
            dataclasses.asdict(recomputed_target),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    if retained_projection != recomputed_projection:
        raise _error("cutover_target_projection_formula_changed")


def _metadata_mutation_batch(
    forecast: Mapping[str, Any],
    layouts: _CutoverLayouts,
) -> tuple[ProviderDirectoryProfileMetadataMutationInput, ...]:
    return (
        _build_metadata_mutation(
            forecast,
            layouts.build_checkpoint,
            relation_name="build_checkpoint",
            operation="update",
            payload_field="build_checkpoint_payload_upper_bytes",
        ),
        _build_metadata_mutation(
            forecast,
            layouts.serving_generation,
            relation_name="serving_generation",
            operation="update",
            payload_field="serving_payload_upper_bytes",
        ),
        _build_metadata_mutation(
            forecast,
            layouts.delta_receipt,
            relation_name="delta_receipt",
            operation="insert",
            payload_field="receipt_payload_upper_bytes",
        ),
    )


def _build_metadata_mutation(
    forecast: Mapping[str, Any],
    layout: tuple[tuple[int, ...], tuple[int, ...], int, int],
    *,
    relation_name: str,
    operation: str,
    payload_field: str,
) -> ProviderDirectoryProfileMetadataMutationInput:
    return ProviderDirectoryProfileMetadataMutationInput(
        relation_name=relation_name,
        operation=operation,
        payload_upper_bytes=int(forecast[payload_field]),
        deleted_toast_chunks=layout[3],
        main_index_pages=layout[0],
        toast_index_pages=layout[1],
    )


def _assert_metadata_formula(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    forecast: Mapping[str, Any],
    metadata_evidence: _MetadataEvidence,
    layouts: _CutoverLayouts,
    pending_commit_items: int,
) -> None:
    recomputed_metadata = project_profile_delta_metadata_capacity(
        geometry,
        _metadata_mutation_batch(forecast, layouts),
        pending_commit_items=pending_commit_items,
    )
    if (
        metadata_evidence.projection_by_field
        != dataclasses.asdict(recomputed_metadata)
    ):
        raise _error("cutover_metadata_projection_formula_changed")


def _assert_total_wal_forecast(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    forecast: Mapping[str, Any],
    target_evidence: _TargetEvidence,
    metadata_evidence: _MetadataEvidence,
) -> None:
    wal_bytes_before = _cutover_nonnegative_integer(
        forecast,
        "wal_bytes_before",
    )
    for field_name in (
        "evidence_target_bytes_before",
        "profile_target_bytes_before",
    ):
        _cutover_nonnegative_integer(forecast, field_name)
    if (
        wal_bytes_before
        + target_evidence.wal_bytes
        + metadata_evidence.wal_bytes
        + metadata_evidence.commit_envelope_bytes
        > geometry.reservation_bytes_by_storage_class["wal"]
    ):
        raise _error("cutover_total_wal_projection_exceeded")


def _assert_actual_within_forecast(
    actual: Mapping[str, Any],
    forecast: Mapping[str, Any],
    target_evidence: _TargetEvidence,
    metadata_evidence: _MetadataEvidence,
    recomputed_target: ProviderDirectoryProfileDeltaProjection,
) -> None:
    actual_values_by_name = {
        field_name: _cutover_nonnegative_integer(actual, field_name)
        for field_name in (
            "cutover_wal_bytes",
            "evidence_target_bytes_before",
            "evidence_target_bytes_after",
            "evidence_target_growth_bytes",
            "profile_target_bytes_before",
            "profile_target_bytes_after",
            "profile_target_growth_bytes",
            "metadata_wal_forecast_bytes",
            "commit_envelope_bytes",
        )
    }
    if (
        actual_values_by_name["cutover_wal_bytes"]
        > target_evidence.wal_bytes
        or actual_values_by_name["evidence_target_growth_bytes"]
        > recomputed_target.targets[0].target_growth_bytes
        or actual_values_by_name["profile_target_growth_bytes"]
        > recomputed_target.targets[1].target_growth_bytes
        or actual_values_by_name["metadata_wal_forecast_bytes"]
        != metadata_evidence.wal_bytes
        or actual_values_by_name["commit_envelope_bytes"]
        != metadata_evidence.commit_envelope_bytes
        or actual_values_by_name["evidence_target_bytes_before"]
        != forecast.get("evidence_target_bytes_before")
        or actual_values_by_name["profile_target_bytes_before"]
        != forecast.get("profile_target_bytes_before")
    ):
        raise _error("cutover_actual_exceeded_forecast")
