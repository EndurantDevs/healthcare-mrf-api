# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Re-prove immutable forecast and actual delta-cutover evidence."""

from __future__ import annotations

from typing import Any, Mapping

from process.provider_directory_profile_capacity_cutover_contract import (
    _assert_cutover_layout,
    _cutover_nonnegative_integer,
    _cutover_target_projection,
)
from process.provider_directory_profile_capacity_cutover_projection import (
    _CutoverLayouts,
    _MetadataEvidence,
    _TargetEvidence,
    _assert_actual_within_forecast,
    _assert_metadata_formula,
    _assert_target_formula,
    _assert_total_wal_forecast,
    _recomputed_target_projection,
)
from process.provider_directory_profile_capacity_geometry import (
    _error,
    _exact_fields,
    capacity_geometry_hash,
    revalidate_capacity_geometry,
)
from process.provider_directory_profile_capacity_types import (
    CUTOVER_ACTUAL_CONTRACT_ID,
    CUTOVER_FORECAST_CONTRACT_ID,
    METADATA_PAYLOAD_UPPER_BOUND_BYTES,
    ProviderDirectoryProfileCapacityGeometry,
)


_FORECAST_FIELDS = frozenset(
    {
        "contract_id",
        "build_id",
        "run_id",
        "capacity_geometry_hash",
        "target_projection",
        "metadata_projection",
        "wal_start_lsn",
        "wal_bytes_before",
        "evidence_target_bytes_before",
        "profile_target_bytes_before",
        "evidence_target_layout",
        "profile_target_layout",
        "build_checkpoint_layout",
        "serving_generation_layout",
        "delta_receipt_layout",
        "build_checkpoint_payload_upper_bytes",
        "serving_payload_upper_bytes",
        "receipt_payload_upper_bytes",
        "pending_commit_items",
    }
)
_ACTUAL_FIELDS = frozenset(
    {
        "contract_id",
        "forecast_hash",
        "wal_start_lsn",
        "target_wal_start_lsn",
        "wal_observed_lsn",
        "cutover_wal_bytes",
        "evidence_target_bytes_before",
        "evidence_target_bytes_after",
        "evidence_target_growth_bytes",
        "profile_target_bytes_before",
        "profile_target_bytes_after",
        "profile_target_growth_bytes",
        "metadata_wal_forecast_bytes",
        "commit_envelope_bytes",
    }
)
_COORDINATE_FIELDS = frozenset(
    {
        "build_id",
        "run_id",
        "forecast_hash",
        "evidence_inserted",
        "evidence_deleted",
        "profile_inserted",
        "profile_deleted",
    }
)
_LAYOUT_FIELDS = (
    (
        "evidence_target_layout",
        "evidence_target_storage_fingerprint",
        True,
    ),
    (
        "profile_target_layout",
        "profile_target_storage_fingerprint",
        True,
    ),
    (
        "build_checkpoint_layout",
        "build_checkpoint_storage_fingerprint",
        False,
    ),
    (
        "serving_generation_layout",
        "serving_generation_storage_fingerprint",
        False,
    ),
    (
        "delta_receipt_layout",
        "delta_receipt_storage_fingerprint",
        False,
    ),
)


def validate_profile_delta_cutover_evidence(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    forecast: Mapping[str, Any],
    actual: Mapping[str, Any],
    **coordinates_by_name: Any,
) -> None:
    """Re-prove immutable forecast and actual semantics during replay."""

    coordinates_by_name = _validated_cutover_coordinates(
        coordinates_by_name
    )
    verified_geometry = revalidate_capacity_geometry(geometry)
    _assert_cutover_identity(
        verified_geometry,
        forecast,
        actual,
        coordinates_by_name,
    )
    target_evidence = _validated_target_evidence(
        verified_geometry,
        forecast,
        coordinates_by_name,
    )
    metadata_evidence = _validated_metadata_evidence(forecast)
    layouts = _validated_cutover_layouts(verified_geometry, forecast)
    pending_commit_items = _validated_metadata_payloads(forecast)
    recomputed_target = _recomputed_target_projection(
        verified_geometry,
        target_evidence,
        layouts,
    )
    _assert_target_formula(target_evidence, recomputed_target)
    _assert_metadata_formula(
        verified_geometry,
        forecast,
        metadata_evidence,
        layouts,
        pending_commit_items,
    )
    _assert_total_wal_forecast(
        verified_geometry,
        forecast,
        target_evidence,
        metadata_evidence,
    )
    _assert_actual_within_forecast(
        actual,
        forecast,
        target_evidence,
        metadata_evidence,
        recomputed_target,
    )


def _validated_cutover_coordinates(
    coordinates_by_name: Mapping[str, Any],
) -> Mapping[str, Any]:
    if set(coordinates_by_name) != _COORDINATE_FIELDS:
        raise TypeError("cutover evidence coordinates are incomplete")
    return coordinates_by_name


def _assert_cutover_identity(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    forecast: Mapping[str, Any],
    actual: Mapping[str, Any],
    coordinates_by_name: Mapping[str, Any],
) -> None:
    _exact_fields(forecast, _FORECAST_FIELDS, name="cutover_forecast")
    _exact_fields(actual, _ACTUAL_FIELDS, name="cutover_actual")
    if (
        forecast.get("contract_id") != CUTOVER_FORECAST_CONTRACT_ID
        or actual.get("contract_id") != CUTOVER_ACTUAL_CONTRACT_ID
        or forecast.get("build_id") != coordinates_by_name["build_id"]
        or forecast.get("run_id") != coordinates_by_name["run_id"]
        or forecast.get("capacity_geometry_hash")
        != capacity_geometry_hash(geometry)
        or actual.get("forecast_hash")
        != coordinates_by_name["forecast_hash"]
        or actual.get("wal_start_lsn") != forecast.get("wal_start_lsn")
        or not isinstance(actual.get("target_wal_start_lsn"), str)
        or not actual.get("target_wal_start_lsn")
    ):
        raise _error("cutover_evidence_identity_changed")


def _validated_target_evidence(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    forecast: Mapping[str, Any],
    coordinates_by_name: Mapping[str, Any],
) -> _TargetEvidence:
    """Validate retained target counts, ordering, and aggregate bounds."""

    projection_by_field = forecast.get("target_projection")
    if not isinstance(projection_by_field, Mapping):
        raise _error("cutover_target_projection_invalid")
    _exact_fields(
        projection_by_field,
        frozenset({"targets", "target_data_bytes", "wal_bytes"}),
        name="cutover_target_projection",
    )
    target_maps = projection_by_field.get("targets")
    if not isinstance(target_maps, (list, tuple)) or len(target_maps) != 2:
        raise _error("cutover_target_projection_invalid")
    counts_by_name = _validated_cutover_counts(coordinates_by_name)
    target_value_tuples = _target_projection_value_tuples(
        geometry,
        target_maps,
    )
    target_data_bytes = _cutover_nonnegative_integer(
        projection_by_field,
        "target_data_bytes",
    )
    target_wal_bytes = _cutover_nonnegative_integer(
        projection_by_field,
        "wal_bytes",
    )
    if (
        target_data_bytes
        != sum(
            target_value_tuple[0]
            for target_value_tuple in target_value_tuples
        )
        or target_wal_bytes
        != sum(
            target_value_tuple[2]
            for target_value_tuple in target_value_tuples
        )
    ):
        raise _error("cutover_target_projection_sum_changed")
    return _TargetEvidence(
        projection_by_field=projection_by_field,
        target_value_tuples=target_value_tuples,
        counts_by_name=counts_by_name,
        wal_bytes=target_wal_bytes,
    )


def _validated_cutover_counts(
    coordinates_by_name: Mapping[str, Any],
) -> Mapping[str, int]:
    count_fields = (
        "evidence_inserted",
        "evidence_deleted",
        "profile_inserted",
        "profile_deleted",
    )
    return {
        field_name: _cutover_nonnegative_integer(
            coordinates_by_name,
            field_name,
        )
        for field_name in count_fields
    }


def _target_projection_value_tuples(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    target_maps: list[Any] | tuple[Any, ...],
) -> tuple[tuple[int, int, int], ...]:
    return tuple(
        _cutover_target_projection(
            geometry,
            target_map,
            expected_name,
        )
        for target_map, expected_name in zip(
            target_maps,
            ("evidence_target", "profile_target"),
            strict=True,
        )
    )


def _validated_metadata_evidence(
    forecast: Mapping[str, Any],
) -> _MetadataEvidence:
    projection_by_field = forecast.get("metadata_projection")
    if not isinstance(projection_by_field, Mapping):
        raise _error("cutover_metadata_projection_invalid")
    _exact_fields(
        projection_by_field,
        frozenset({"data_bytes", "wal_bytes", "commit_envelope_bytes"}),
        name="cutover_metadata_projection",
    )
    _cutover_nonnegative_integer(
        projection_by_field,
        "data_bytes",
    )
    metadata_wal_bytes = _cutover_nonnegative_integer(
        projection_by_field,
        "wal_bytes",
    )
    commit_envelope_bytes = _cutover_nonnegative_integer(
        projection_by_field,
        "commit_envelope_bytes",
    )
    return _MetadataEvidence(
        projection_by_field=projection_by_field,
        wal_bytes=metadata_wal_bytes,
        commit_envelope_bytes=commit_envelope_bytes,
    )


def _validated_cutover_layouts(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    forecast: Mapping[str, Any],
) -> _CutoverLayouts:
    layouts_by_name = {
        layout_field: _assert_cutover_layout(
            forecast[layout_field],
            getattr(geometry, fingerprint_field),
            includes_inserted_toast_chunks=includes_inserted_chunks,
        )
        for (
            layout_field,
            fingerprint_field,
            includes_inserted_chunks,
        ) in _LAYOUT_FIELDS
    }
    return _CutoverLayouts(
        evidence_target=layouts_by_name["evidence_target_layout"],
        profile_target=layouts_by_name["profile_target_layout"],
        build_checkpoint=layouts_by_name["build_checkpoint_layout"],
        serving_generation=layouts_by_name["serving_generation_layout"],
        delta_receipt=layouts_by_name["delta_receipt_layout"],
    )


def _validated_metadata_payloads(forecast: Mapping[str, Any]) -> int:
    for field_name in (
        "build_checkpoint_payload_upper_bytes",
        "serving_payload_upper_bytes",
        "receipt_payload_upper_bytes",
    ):
        if (
            _cutover_nonnegative_integer(forecast, field_name)
            > METADATA_PAYLOAD_UPPER_BOUND_BYTES
        ):
            raise _error("cutover_metadata_payload_exceeded")
    return _cutover_nonnegative_integer(
        forecast,
        "pending_commit_items",
    )
