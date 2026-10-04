# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Re-prove immutable forecast and actual delta-cutover evidence."""

from __future__ import annotations

from dataclasses import dataclass
import re
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
        geometry=verified_geometry,
    )
    if verified_geometry.bounded_admission:
        _assert_bounded_cutover(verified_geometry, forecast, actual, target_evidence, metadata_evidence)


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
    _exact_fields(forecast, _FORECAST_FIELDS | (
        {"admission_wal_start_lsn", "admission_wal_offset_bytes"} if geometry.bounded_admission else set()
    ), name="cutover_forecast")
    _exact_fields(actual, _ACTUAL_FIELDS | (
        {"target_windows", "wal_ledger"} if geometry.bounded_admission else set()
    ), name="cutover_actual")
    if (
        forecast.get("contract_id") != geometry.cutover_forecast_contract_id
        or actual.get("contract_id") != geometry.cutover_actual_contract_id
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


def _lsn_bytes(value: Any) -> int:
    if not isinstance(value, str) or re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", value) is None:
        raise _error("cutover_lsn_invalid")
    high, low = value.split("/")
    return (int(high, 16) << 32) + int(low, 16)


def _bounded_cutover_ledger(geometry, forecast, actual, metadata_evidence):
    """Prove settled WAL against its admission origin and remaining reserves."""
    origin = _lsn_bytes(forecast["admission_wal_start_lsn"])
    start = _lsn_bytes(forecast["wal_start_lsn"])
    target_start = _lsn_bytes(actual["target_wal_start_lsn"])
    observed = _lsn_bytes(actual["wal_observed_lsn"])
    offset = _cutover_nonnegative_integer(forecast, "admission_wal_offset_bytes")
    if (not origin <= start <= target_start <= observed
        or start - origin + offset != forecast["wal_bytes_before"]
        or observed - target_start != actual["cutover_wal_bytes"]):
        raise _error("cutover_wal_origin_changed")
    ledger = actual.get("wal_ledger")
    if not isinstance(ledger, Mapping):
        raise _error("cutover_wal_ledger_invalid")
    _exact_fields(ledger, frozenset({"settled_relation_wal_bytes", "pending_relation_wal_bytes",
                                    "accounted_control_wal_bytes", "pending_control_wal_bytes",
                                    "accounted_metadata_wal_bytes", "pending_metadata_wal_bytes"}), name="cutover_wal_ledger")
    caps_by_name = {cap.relation_name: cap for cap in geometry.relation_byte_caps}
    settled, pending = ledger["settled_relation_wal_bytes"], ledger["pending_relation_wal_bytes"]
    if not isinstance(settled, Mapping) or not isinstance(pending, Mapping):
        raise _error("cutover_wal_ledger_invalid")
    _exact_fields(settled, frozenset(caps_by_name), name="cutover_settled_wal")
    _exact_fields(pending, frozenset(caps_by_name), name="cutover_pending_wal")
    remaining_relation = 0
    settled_relation = 0
    for name, cap in caps_by_name.items():
        charged = _cutover_nonnegative_integer(settled, name)
        outstanding = _cutover_nonnegative_integer(pending, name)
        if charged > cap.max_wal_bytes or outstanding != 0:
            raise _error("cutover_window_unresolved_or_exceeded")
        settled_relation += charged
        remaining_relation += cap.max_wal_bytes - charged
    if settled_relation > observed - origin + offset:
        raise _error("cutover_settled_wal_exceeds_observed")
    control = _cutover_nonnegative_integer(ledger, "accounted_control_wal_bytes")
    control_pending = _cutover_nonnegative_integer(ledger, "pending_control_wal_bytes")
    metadata = _cutover_nonnegative_integer(ledger, "accounted_metadata_wal_bytes")
    metadata_pending = _cutover_nonnegative_integer(ledger, "pending_metadata_wal_bytes")
    if (control > geometry.control_wal_upper_bound_bytes or control_pending != 0
        or metadata != metadata_evidence.wal_bytes + metadata_evidence.commit_envelope_bytes
        or metadata_pending != metadata or metadata > geometry.metadata_wal_upper_bound_bytes):
        raise _error("cutover_finish_reservation_invalid")
    required_wal = (observed - origin + offset + metadata_pending + remaining_relation
                    + geometry.control_wal_upper_bound_bytes - control
                    + geometry.metadata_wal_upper_bound_bytes - metadata)
    if required_wal > geometry.reservation_bytes_by_storage_class["wal"]:
        raise _error("cutover_total_wal_projection_exceeded")
    return caps_by_name, settled


def _assert_target_window(geometry, forecast, actual, target_evidence, caps_by_name, settled, coordinate, totals):
    """Reconcile one finite-window summary with its frozen count and layout."""
    name, count_prefix = coordinate
    if not isinstance(totals, Mapping):
        raise _error("cutover_target_windows_invalid")
    _exact_fields(totals, frozenset({"window_count", "inserted_rows", "deleted_rows",
                                    "inserted_toast_chunks", "deleted_toast_chunks", "deleted_logical_bytes",
                                    "projected_wal_bytes", "projected_growth_bytes", "observed_wal_bytes",
                                    "windows_hash"}), name="cutover_target_window_totals")
    metrics_by_name = {field: _cutover_nonnegative_integer(totals, field) for field in totals if field != "windows_hash"}
    expected_windows = sum((target_evidence.counts_by_name[f"{count_prefix}_{operation}"]
                            + geometry.artifact_scope_batch_size - 1) // geometry.artifact_scope_batch_size
                           for operation in ("inserted", "deleted"))
    layout = forecast[name + "_layout"]
    index = 0 if name == "evidence_target" else 1
    before = _cutover_nonnegative_integer(actual, name + "_bytes_before")
    after = _cutover_nonnegative_integer(actual, name + "_bytes_after")
    growth = _cutover_nonnegative_integer(actual, name + "_growth_bytes")
    if (metrics_by_name["window_count"] != expected_windows
        or metrics_by_name["inserted_rows"] != target_evidence.counts_by_name[count_prefix + "_inserted"]
        or metrics_by_name["deleted_rows"] != target_evidence.counts_by_name[count_prefix + "_deleted"]
        or metrics_by_name["inserted_toast_chunks"] != layout["inserted_toast_chunks"]
        or metrics_by_name["deleted_toast_chunks"] != layout["deleted_toast_chunks"]
        or metrics_by_name["deleted_logical_bytes"] != target_evidence.target_value_tuples[index][1]
        or metrics_by_name["observed_wal_bytes"] != settled[name]
        or metrics_by_name["observed_wal_bytes"] > metrics_by_name["projected_wal_bytes"]
        or growth != max(after - before, 0) or growth > caps_by_name[name].max_target_growth_bytes
        or growth > metrics_by_name["projected_growth_bytes"]
        or not isinstance(totals["windows_hash"], str)
        or re.fullmatch(r"[0-9a-f]{64}", totals["windows_hash"]) is None):
        raise _error("cutover_window_reconciliation_changed")


def _assert_bounded_cutover(geometry, forecast, actual, target_evidence, metadata_evidence):
    """Validate compact window reconciliation without reinterpreting v1."""
    caps_by_name, settled = _bounded_cutover_ledger(geometry, forecast, actual, metadata_evidence)
    windows = actual.get("target_windows")
    if not isinstance(windows, Mapping):
        raise _error("cutover_target_windows_invalid")
    _exact_fields(windows, frozenset({"evidence_target", "profile_target"}), name="cutover_target_windows")
    target_observed = 0
    for name, count_prefix in (("evidence_target", "evidence"), ("profile_target", "profile")):
        _assert_target_window(geometry, forecast, actual, target_evidence, caps_by_name, settled, (name, count_prefix), windows[name])
        target_observed += settled[name]
    if target_observed > actual["cutover_wal_bytes"]:
        raise _error("cutover_window_reconciliation_changed")


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


@dataclass(frozen=True)
class _TargetWindowPlan:
    """Bind immutable SQL coordinates and mutable accounting for one target."""

    admission: Any
    relation_name: str
    target_ref: str
    stage_ref: str
    target_oid: int
    stage_oid: int
    column_sql: str
    key_sql: str
    key_type: str
    cursor_sql: str
    delete_predicate: str
    params_by_name: Mapping[str, Any]
    totals_by_metric: dict[str, Any]


def _target_window_totals(fhir, admission, relation_name):
    return admission.wal_tracker.target_window_totals.setdefault(relation_name, {
        "window_count": 0, "inserted_rows": 0, "deleted_rows": 0,
        "inserted_toast_chunks": 0, "deleted_toast_chunks": 0,
        "deleted_logical_bytes": 0, "projected_wal_bytes": 0,
        "projected_growth_bytes": 0, "observed_wal_bytes": 0,
        "windows_hash": fhir._identity_hash({
            "geometry_hash": fhir.profile_capacity.capacity_geometry_hash(admission.geometry),
            "relation_name": relation_name,
        }),
    })


async def _target_window_plan(fhir, relation_name, target_ref, stage_ref, selection_by_name):
    if fhir.db._transaction_binding() is None:
        raise RuntimeError("provider_directory_profile_target_requires_transaction")
    admission = fhir._provider_directory_profile_capacity_admission()
    target_oid = getattr(admission.geometry, relation_name + "_oid")
    stage_oid = int(await fhir.db.scalar(
        "SELECT to_regclass(:relation_ref)::oid::bigint;", relation_ref=stage_ref,
    ) or 0)
    totals_by_metric = _target_window_totals(fhir, admission, relation_name)
    column_sql = ", ".join(fhir._q(column) for column in selection_by_name["columns"])
    key_sql = "target." + fhir._q(selection_by_name["key_column"])
    key_type = selection_by_name["key_type"]
    cursor_sql = (f"(CAST(:capacity_after_key AS {key_type}) IS NULL OR "
                  f"{key_sql} > CAST(:capacity_after_key AS {key_type}))")
    return _TargetWindowPlan(
        admission, relation_name, target_ref, stage_ref, target_oid, stage_oid,
        column_sql, key_sql, key_type, cursor_sql,
        selection_by_name["delete_predicate"], selection_by_name["params"], totals_by_metric,
    )


async def _select_target_window(fhir, plan, relation_ref, predicate, params_by_name):
    key_row = await fhir.db.first(
        f"SELECT count(*)::bigint AS row_count, MAX(window_key) AS last_key FROM ("
        f"SELECT {plan.key_sql} AS window_key FROM {relation_ref} AS target "
        f"WHERE ({predicate}) AND {plan.cursor_sql} ORDER BY {plan.key_sql} "
        "LIMIT :capacity_window_size) AS selected;", **params_by_name,
    )
    if key_row is None:
        raise RuntimeError("provider_directory_profile_target_window_missing")
    key_by_field = fhir._pagination_checkpoint_row_mapping(key_row)
    row_count, last_key = int(key_by_field["row_count"]), key_by_field["last_key"]
    if row_count == 0:
        return row_count, last_key
    after_key = params_by_name["capacity_after_key"]
    if (not 0 < row_count <= plan.admission.geometry.artifact_scope_batch_size
        or last_key is None or (after_key is not None and last_key <= after_key)):
        raise RuntimeError("provider_directory_profile_target_window_cursor_invalid")
    return row_count, last_key


async def _project_target_window(fhir, plan, operation, relation_ref, bound, params_by_name, row_count):
    target_layout, stage_layout = await fhir._profile_cutover_layout_pair(
        plan.admission, plan.relation_name, plan.target_oid, plan.stage_oid,
    )
    layout = target_layout if operation == "delete" else stage_layout
    observed_rows, logical_bytes = await fhir._profile_delta_deletion_projection(
        f"SELECT count(*)::bigint AS row_count, COALESCE(sum(pg_column_size(target)), 0)::bigint AS logical_bytes "
        f"FROM {relation_ref} AS target WHERE {bound};",
        "provider_directory_profile_target_window_missing", params_by_name,
    )
    if observed_rows != row_count:
        raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_profile_target_window_changed")
    chunks = await fhir._provider_directory_profile_toast_chunk_count(
        source_sql=f"SELECT target.* FROM {relation_ref} AS target WHERE {bound}",
        relation_oid=layout.relation_oid, toast_oid=layout.toast_oid,
        toastable_columns=layout.toastable_columns,
        expected_compression=plan.admission.geometry.postgres_default_toast_compression,
        params=params_by_name,
    )
    delta_input = fhir.profile_capacity.ProviderDirectoryProfileTargetDeltaInput(
        relation_name=plan.relation_name,
        inserted_rows=row_count if operation == "insert" else 0,
        inserted_toast_chunks=chunks if operation == "insert" else 0,
        deleted_rows=row_count if operation == "delete" else 0,
        deleted_logical_bytes=logical_bytes if operation == "delete" else 0,
        deleted_toast_chunks=chunks if operation == "delete" else 0,
        main_index_pages=target_layout.main_index_pages,
        toast_index_pages=target_layout.toast_index_pages,
    )
    return delta_input, fhir.profile_capacity._target_delta_projection(plan.admission.geometry, delta_input)


def _record_target_window(fhir, plan, operation, params_by_name, delta_input, projection, actual_wal):
    totals_by_metric = plan.totals_by_metric
    totals_by_metric["window_count"] += 1
    for field_name in ("inserted_rows", "deleted_rows", "inserted_toast_chunks",
                       "deleted_toast_chunks", "deleted_logical_bytes"):
        totals_by_metric[field_name] += getattr(delta_input, field_name)
    totals_by_metric["projected_wal_bytes"] += projection.wal_bytes
    totals_by_metric["projected_growth_bytes"] += projection.target_growth_bytes
    totals_by_metric["observed_wal_bytes"] += actual_wal
    totals_by_metric["windows_hash"] = fhir._identity_hash({
        "previous": totals_by_metric["windows_hash"], "operation": operation,
        "after_key": params_by_name["capacity_after_key"],
        "last_key": params_by_name["capacity_last_key"],
        "input": fhir.asdict(delta_input), "projection": fhir.asdict(projection),
        "observed_wal_bytes": actual_wal,
    })


async def _apply_target_window(fhir, plan, operation, bound, params_by_name, row_count):
    relation_ref = plan.target_ref if operation == "delete" else plan.stage_ref
    tracker = plan.admission.wal_tracker
    settled_before = tracker.accounted_relation_wal_bytes.get(plan.relation_name, 0)
    async with fhir._profile_capacity_mutation_window(plan.relation_name, (plan.target_ref,)):
        delta_input, projection = await _project_target_window(
            fhir, plan, operation, relation_ref, bound, params_by_name, row_count,
        )
        await fhir._reserve_profile_capacity_growth(plan.admission, plan.relation_name,
                                                    projection.target_growth_bytes)
        await fhir._reserve_provider_directory_profile_wal_budget(
            plan.admission, relation_wal_bytes={plan.relation_name: projection.wal_bytes},
        )
        if operation == "delete":
            statement = f"DELETE FROM {plan.target_ref} AS target WHERE {bound};"
        else:
            statement = (f"INSERT INTO {plan.target_ref} ({plan.column_sql}) SELECT {plan.column_sql} "
                         f"FROM {plan.stage_ref} AS target WHERE {bound} ORDER BY {plan.key_sql};")
        changed = fhir._coerce_rowcount(await fhir.db.status(statement, **params_by_name))
        if changed != row_count:
            raise fhir.ProviderDirectoryArtifactBuildStale("provider_directory_profile_delta_rowcount_changed")
    actual_wal = tracker.accounted_relation_wal_bytes[plan.relation_name] - settled_before
    _record_target_window(fhir, plan, operation, params_by_name, delta_input, projection, actual_wal)


async def _replace_target_operation(fhir, plan, operation):
    relation_ref = plan.target_ref if operation == "delete" else plan.stage_ref
    predicate = plan.delete_predicate if operation == "delete" else "TRUE"
    after_key, total_rows = None, 0
    while True:
        params_by_name = {
            **plan.params_by_name, "capacity_after_key": after_key,
            "capacity_window_size": plan.admission.geometry.artifact_scope_batch_size,
        }
        row_count, last_key = await _select_target_window(fhir, plan, relation_ref, predicate, params_by_name)
        if row_count == 0:
            return total_rows
        params_by_name["capacity_last_key"] = last_key
        bound = (f"({predicate}) AND {plan.cursor_sql} AND "
                 f"{plan.key_sql} <= CAST(:capacity_last_key AS {plan.key_type})")
        await _apply_target_window(fhir, plan, operation, bound, params_by_name, row_count)
        total_rows += row_count
        after_key = last_key


async def replace_target_windows(fhir, relation_name, target_ref, stage_ref, **selection_by_name):
    """Complete every bounded delete before replacement inserts on one target."""
    plan = await _target_window_plan(fhir, relation_name, target_ref, stage_ref, selection_by_name)
    counts = []
    for operation in ("delete", "insert"):
        counts.append(await _replace_target_operation(fhir, plan, operation))
    return tuple(counts)


def _actual_wal_ledger(admission):
    tracker = admission.wal_tracker
    return {
        "settled_relation_wal_bytes": {
            cap.relation_name: tracker.accounted_relation_wal_bytes.get(cap.relation_name, 0)
            for cap in admission.geometry.relation_byte_caps
        },
        "pending_relation_wal_bytes": {
            cap.relation_name: tracker.pending_relation_wal_bytes.get(cap.relation_name, 0)
            for cap in admission.geometry.relation_byte_caps
        },
        "accounted_control_wal_bytes": sum(
            operation.wal_bytes_per_operation * tracker.accounted_control_operation_counts.get(operation.operation_name, 0)
            for operation in admission.control_wal_projection.operations
        ),
        "pending_control_wal_bytes": sum(tracker.pending_control_wal_bytes.values()),
        "accounted_metadata_wal_bytes": tracker.accounted_metadata_wal_bytes,
        "pending_metadata_wal_bytes": tracker.pending_metadata_wal_bytes,
    }


async def _observe_cutover_wal(fhir, capacity_forecast, target_wal_start_lsn):
    wal_observed_row = await fhir.db.first(
        """
        WITH observed AS MATERIALIZED (
            SELECT pg_current_wal_insert_lsn() AS wal_lsn
        )
        SELECT observed.wal_lsn::text AS wal_observed_lsn,
               pg_wal_lsn_diff(
                   observed.wal_lsn,
                   CAST(CAST(:wal_start_lsn AS text) AS pg_lsn)
               )::bigint AS cutover_wal_bytes
          FROM observed;
        """,
        wal_start_lsn=target_wal_start_lsn,
    )
    if wal_observed_row is None:
        raise RuntimeError("provider_directory_profile_cutover_actual_wal_missing")
    wal_observation = fhir._pagination_checkpoint_row_mapping(wal_observed_row)
    target_wal_bytes = int(wal_observation["cutover_wal_bytes"])
    admission = fhir._provider_directory_profile_capacity_admission()
    is_bounded = admission is not None and admission.geometry.bounded_admission
    target_wal_limit = (
        sum(cap.max_wal_bytes for cap in admission.geometry.relation_byte_caps
            if cap.relation_name in {"evidence_target", "profile_target"})
        if is_bounded else capacity_forecast.target_projection.wal_bytes
    )
    if not 0 <= target_wal_bytes <= target_wal_limit:
        raise RuntimeError(
            "provider_directory_profile_capacity_target_wal_exceeded:"
            f"observed={target_wal_bytes}:"
            f"projected={capacity_forecast.target_projection.wal_bytes}"
        )
    return wal_observation, target_wal_bytes, admission, is_bounded


async def cutover_actual(fhir, capacity_forecast, target_wal_start_lsn, target_bytes):
    """Bind observed target WAL and settlement to the immutable forecast."""
    wal_observation, target_wal_bytes, admission, is_bounded = await _observe_cutover_wal(
        fhir, capacity_forecast, target_wal_start_lsn,
    )
    actual_by_field = fhir._profile_delta_cutover_actual_values(
        capacity_forecast, target_wal_start_lsn, wal_observation, target_wal_bytes, target_bytes,
    )
    if is_bounded:
        actual_by_field["target_windows"] = dict(admission.wal_tracker.target_window_totals)
        actual_by_field["wal_ledger"] = _actual_wal_ledger(admission)
    actual_json = fhir.json.dumps(
        actual_by_field, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
    )
    return {
        **actual_by_field,
        "cutover_forecast_hash": capacity_forecast.forecast_hash,
        "cutover_forecast_json": capacity_forecast.forecast_json,
        "cutover_actual_hash": fhir._identity_hash({
            "contract": fhir._profile_cutover_hash_domain(actual_by_field, "actual"),
            "actual": actual_by_field,
        }),
        "cutover_actual_json": actual_json,
    }
