# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed physical, WAL, and cutover projection edge coverage."""

from __future__ import annotations

import dataclasses
import types

import pytest

from process import provider_directory_profile_capacity as capacity
from process import (
    provider_directory_profile_capacity_control_budget as control_budget,
)
from process import (
    provider_directory_profile_capacity_control_projection
    as control_projection,
)
from process import provider_directory_profile_capacity_cutover as cutover
from process import (
    provider_directory_profile_capacity_cutover_contract as cutover_contract,
)
from process import (
    provider_directory_profile_capacity_cutover_projection
    as cutover_projection,
)
from process import provider_directory_profile_capacity_physical as physical
from process import provider_directory_profile_capacity_target as target
from process.provider_directory_profile_capacity_types import (
    _MAX_SIGNED_BIGINT,
)
from tests.test_provider_directory_profile_capacity_projection import (
    _projection_geometry,
    _projection_inputs,
)
from tests.test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
    _control_metadata_input,
    _control_wal_plan_input,
)


def _assert_projection_errors(failure_operations):
    for failure_operation in failure_operations:
        with pytest.raises(capacity.ProviderDirectoryProfileCapacityError):
            failure_operation()


def test_target_projection_rejects_overflow_shape_order_and_named_caps():
    geometry_state = _projection_geometry()
    failure_operations = (
        lambda: target._checked_add(_MAX_SIGNED_BIGINT, 1),
        lambda: target._ceil_log2(0),
        lambda: target._validate_target_delta_input(None),
        lambda: target._target_relation_cap(
            geometry_state,
            "unknown_target",
        ),
        lambda: target.project_profile_delta_capacity(
            geometry_state,
            tuple(reversed(_projection_inputs())),
        ),
    )
    _assert_projection_errors(failure_operations)


def test_target_projection_enforces_deleted_and_wal_caps():
    relation_cap = next(
        cap
        for cap in _projection_geometry().relation_byte_caps
        if cap.relation_name == "evidence_target"
    )
    projection_values = (
        {
            "target_growth_bytes": 0,
            "deleted_logical_bytes": (
                relation_cap.max_deleted_logical_bytes + 1
            ),
            "wal_bytes": 0,
        },
        {
            "target_growth_bytes": 0,
            "deleted_logical_bytes": 0,
            "wal_bytes": relation_cap.max_wal_bytes + 1,
        },
    )
    for projection_by_field in projection_values:
        with pytest.raises(capacity.ProviderDirectoryProfileCapacityError):
            target._assert_target_projection_caps(
                relation_cap,
                **projection_by_field,
            )


def test_scratch_and_metadata_projection_reject_invalid_inputs():
    geometry_state = _projection_geometry()
    failure_operations = (
        lambda: physical._validate_scratch_input(None),
        lambda: physical._scratch_relation_cap(
            geometry_state,
            "unknown_stage",
        ),
        lambda: physical._validate_metadata_mutation(None),
        lambda: physical.project_profile_delta_metadata_capacity(
            geometry_state,
            (),
            pending_commit_items=0,
        ),
    )
    _assert_projection_errors(failure_operations)


def _metadata_mutation_batch():
    return (
        _control_metadata_input("build_checkpoint", "update"),
        _control_metadata_input("serving_generation", "update"),
        _control_metadata_input("delta_receipt", "insert"),
    )


def test_metadata_projection_enforces_data_bound(monkeypatch):
    geometry_state = _projection_geometry()
    monkeypatch.setattr(
        physical,
        "_metadata_mutation_projection",
        lambda *_args: (
            geometry_state.metadata_data_upper_bound_bytes,
            0,
        ),
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="metadata_projection_data_exceeded",
    ):
        physical.project_profile_delta_metadata_capacity(
            geometry_state,
            _metadata_mutation_batch(),
            pending_commit_items=0,
        )


def test_metadata_projection_enforces_wal_bound(monkeypatch):
    geometry_state = _projection_geometry()
    monkeypatch.setattr(
        physical,
        "_metadata_mutation_projection",
        lambda *_args: (
            0,
            geometry_state.metadata_wal_upper_bound_bytes,
        ),
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="metadata_projection_wal_exceeded",
    ):
        physical.project_profile_delta_metadata_capacity(
            geometry_state,
            _metadata_mutation_batch(),
            pending_commit_items=0,
        )


def test_control_budget_rejects_overflow_and_invalid_plan():
    invalid_mutation = dataclasses.replace(
        _control_metadata_input("build_checkpoint", "insert"),
        relation_name="other",
    )
    invalid_plan = dataclasses.replace(
        _control_wal_plan_input(),
        artifact_scope_recovery_contract_id="wrong-contract",
    )
    failure_operations = (
        lambda: control_budget._control_wal_product(-1),
        lambda: control_budget._control_wal_product(
            _MAX_SIGNED_BIGINT,
            2,
        ),
        lambda: control_budget._validate_control_wal_metadata_input(
            invalid_mutation,
            relation_name="build_checkpoint",
            operation="insert",
        ),
        lambda: control_budget._validate_control_wal_plan_input(None),
        lambda: control_budget._validate_control_wal_plan_input(invalid_plan),
    )
    _assert_projection_errors(failure_operations)


def test_control_budget_returns_exact_zero_work_envelopes():
    geometry_state = _projection_geometry()
    assert control_budget._control_metadata_projection_per_operation(
        geometry_state,
        _control_metadata_input("build_checkpoint", "insert"),
        sequence_operation_count=0,
    ) == (0, 0)

    empty_operation = control_budget._control_wal_operation(
        geometry_state,
        ("pre_cutover", "zero_work"),
        operation_count=0,
    )
    assert empty_operation.operation_count == 0
    assert empty_operation.wal_bytes == 0


def test_control_projection_rejects_shape_and_total_drift():
    _, projection = _bound_control_wal_projection()
    first_operation = projection.operations[0]
    invalid_operation = dataclasses.replace(
        first_operation,
        wal_bytes=first_operation.wal_bytes + 1,
    )
    invalid_projection = dataclasses.replace(
        projection,
        contract_id="wrong-contract",
    )
    invalid_totals = dataclasses.replace(
        projection,
        total_control_wal_bytes=projection.total_control_wal_bytes + 1,
    )
    failure_operations = (
        lambda: control_projection._validate_control_operation_shape(
            invalid_operation
        ),
        lambda: control_projection._assert_control_wal_projection_shape(
            invalid_projection
        ),
        lambda: control_projection._assert_control_wal_projection_shape(
            invalid_totals
        ),
    )
    _assert_projection_errors(failure_operations)


def test_control_projection_rejects_runtime_operation_reordering(monkeypatch):
    geometry_state, projection = _bound_control_wal_projection()
    monkeypatch.setattr(
        control_projection,
        "_control_wal_operation_ledger",
        lambda *_args: tuple(reversed(projection.operations)),
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="operation_order_invalid",
    ):
        control_projection.project_profile_control_wal_capacity(
            geometry_state,
            projection.plan_input,
        )


def test_cutover_contract_rejects_numeric_order_and_cap_drift():
    geometry_state = _projection_geometry()
    target_by_field = {
        "relation_name": "profile_target",
        "target_growth_bytes": 0,
        "deleted_logical_bytes": 0,
        "wal_bytes": 0,
    }
    target_cap_exceeded_by_field = {
        **target_by_field,
        "relation_name": "evidence_target",
        "target_growth_bytes": 10_000_001,
    }
    failure_operations = (
        lambda: cutover_contract._cutover_nonnegative_integer(
            {"count": -1},
            "count",
        ),
        lambda: cutover_contract._cutover_target_projection(
            geometry_state,
            target_by_field,
            "evidence_target",
        ),
        lambda: cutover_contract._cutover_target_projection(
            geometry_state,
            target_cap_exceeded_by_field,
            "evidence_target",
        ),
    )
    _assert_projection_errors(failure_operations)


def test_cutover_contract_rejects_layout_identity_drift():
    invalid_layout_by_field = {
        "exact_fingerprint": "wrong",
        "main_index_oids": [1],
        "main_index_pages": [1],
        "toast_index_oids": [],
        "toast_index_pages": [],
        "deleted_toast_chunks": 0,
    }
    failure_operations = (
        lambda: cutover_contract._assert_cutover_layout(
            invalid_layout_by_field,
            "expected",
            includes_inserted_toast_chunks=False,
        ),
        lambda: cutover_contract._validated_index_pages(
            {
                "main_index_oids": [],
                "main_index_pages": [],
            },
            oid_field="main_index_oids",
            page_field="main_index_pages",
            required=True,
        ),
    )
    _assert_projection_errors(failure_operations)


def _cutover_coordinates():
    return {
        "build_id": "pdpb_" + "1" * 32,
        "run_id": "run_" + "2" * 32,
        "forecast_hash": "3" * 64,
        "evidence_inserted": 0,
        "evidence_deleted": 0,
        "profile_inserted": 0,
        "profile_deleted": 0,
    }


def test_cutover_evidence_rejects_incomplete_coordinates_and_identity():
    geometry_state = _projection_geometry()
    forecast_by_field = dict.fromkeys(cutover._FORECAST_FIELDS)
    actual_by_field = dict.fromkeys(cutover._ACTUAL_FIELDS)

    with pytest.raises(TypeError, match="coordinates are incomplete"):
        cutover._validated_cutover_coordinates({})
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="identity_changed",
    ):
        cutover._assert_cutover_identity(
            geometry_state,
            forecast_by_field,
            actual_by_field,
            _cutover_coordinates(),
        )


def test_cutover_evidence_rejects_target_shape_and_sum_drift():
    geometry_state = _projection_geometry()
    coordinates_by_name = _cutover_coordinates()
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="target_projection_invalid",
    ):
        cutover._validated_target_evidence(
            geometry_state,
            {"target_projection": None},
            coordinates_by_name,
        )
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="target_projection_invalid",
    ):
        cutover._validated_target_evidence(
            geometry_state,
            {
                "target_projection": {
                    "targets": [],
                    "target_data_bytes": 0,
                    "wal_bytes": 0,
                }
            },
            coordinates_by_name,
        )

    zero_target_by_field = {
        "relation_name": "evidence_target",
        "target_growth_bytes": 0,
        "deleted_logical_bytes": 0,
        "wal_bytes": 0,
    }
    target_projection_by_field = {
        "targets": [
            zero_target_by_field,
            {
                **zero_target_by_field,
                "relation_name": "profile_target",
            },
        ],
        "target_data_bytes": 1,
        "wal_bytes": 0,
    }
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="target_projection_sum_changed",
    ):
        cutover._validated_target_evidence(
            geometry_state,
            {"target_projection": target_projection_by_field},
            coordinates_by_name,
        )


def test_cutover_evidence_rejects_metadata_shape_and_payload_drift():
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="metadata_projection_invalid",
    ):
        cutover._validated_metadata_evidence(
            {"metadata_projection": None}
        )
    metadata_by_field = {
        "build_checkpoint_payload_upper_bytes": (
            capacity.METADATA_PAYLOAD_UPPER_BOUND_BYTES + 1
        ),
        "serving_payload_upper_bytes": 0,
        "receipt_payload_upper_bytes": 0,
        "pending_commit_items": 0,
    }
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="metadata_payload_exceeded",
    ):
        cutover._validated_metadata_payloads(metadata_by_field)


def _empty_cutover_evidence():
    target_evidence = cutover_projection._TargetEvidence(
        projection_by_field={},
        target_value_tuples=((0, 0, 0), (0, 0, 0)),
        counts_by_name={},
        wal_bytes=1,
    )
    metadata_evidence = cutover_projection._MetadataEvidence(
        projection_by_field={},
        wal_bytes=1,
        commit_envelope_bytes=1,
    )
    return target_evidence, metadata_evidence


def test_cutover_projection_rejects_total_wal_overage():
    geometry_state = _projection_geometry()
    target_evidence, metadata_evidence = _empty_cutover_evidence()
    forecast_by_field = {
        "wal_bytes_before": (
            geometry_state.reservation_bytes_by_storage_class["wal"]
        ),
        "evidence_target_bytes_before": 0,
        "profile_target_bytes_before": 0,
    }

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="total_wal_projection_exceeded",
    ):
        cutover_projection._assert_total_wal_forecast(
            geometry_state,
            forecast_by_field,
            target_evidence,
            metadata_evidence,
        )


def test_cutover_projection_rejects_actual_overage():
    target_evidence, metadata_evidence = _empty_cutover_evidence()
    recomputed_target = types.SimpleNamespace(
        targets=(
            types.SimpleNamespace(target_growth_bytes=0),
            types.SimpleNamespace(target_growth_bytes=0),
        )
    )
    actual_by_field = {
        "cutover_wal_bytes": 2,
        "evidence_target_bytes_before": 0,
        "evidence_target_bytes_after": 0,
        "evidence_target_growth_bytes": 0,
        "profile_target_bytes_before": 0,
        "profile_target_bytes_after": 0,
        "profile_target_growth_bytes": 0,
        "metadata_wal_forecast_bytes": 1,
        "commit_envelope_bytes": 1,
    }
    forecast_by_field = {
        "evidence_target_bytes_before": 0,
        "profile_target_bytes_before": 0,
    }

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="actual_exceeded_forecast",
    ):
        cutover_projection._assert_actual_within_forecast(
            actual_by_field,
            forecast_by_field,
            target_evidence,
            metadata_evidence,
            recomputed_target,
        )
