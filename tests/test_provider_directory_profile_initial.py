# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit initial identity, finite planning and uncertain-completion accounting."""

import asyncio
import copy
import importlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from process import provider_directory_profile_selection as selection
from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution
from tests.test_provider_directory_profile_bounded_capacity import admitted_window
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_control_capacity import _control_wal_plan_input

fhir = importlib.import_module("process.provider_directory_fhir")


def _geometry():
    return capacity.validated_capacity_geometry(
        _geometry_payload(
            contract_id=contract.GEOMETRY_CONTRACT,
            materialization_mode="full_swap",
            physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID,
            current_source_vector_hash=None,
            current_context_vector_hash=None,
            initial_target_state_sha256="ab" * 32,
            initial_receipt_oid=29000,
            initial_receipt_storage_fingerprint="cd" * 32,
        )
    )


def _target():
    return {
        "contract_id": contract.TARGET_CONTRACT,
        "resolution": "empty",
        "serving_singleton_absent": True,
        "initial_commit_receipt_absent": True,
        "evidence_target_oid": 20001,
        "profile_target_oid": 20002,
        "evidence_target_storage_fingerprint": "aa" * 32,
        "profile_target_storage_fingerprint": "bb" * 32,
        "evidence_target_bytes": 8192,
        "profile_target_bytes": 8192,
        "evidence_rows": 0,
        "profile_rows": 0,
        "historical_publication": None,
    }


def test_initial_geometry_keeps_delta_wire_contract_closed():
    initial_geometry = _geometry()
    assert isinstance(initial_geometry, contract.InitialCapacityGeometry)
    assert capacity.revalidate_capacity_geometry(initial_geometry) == initial_geometry
    raw = capacity.capacity_geometry_payload(initial_geometry)
    raw["contract_id"] = capacity.CAPACITY_GEOMETRY_CONTRACT_ID
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError):
        capacity.validated_capacity_geometry(raw)
    raw = _geometry_payload(materialization_mode="full_swap")
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError):
        capacity.validated_capacity_geometry(raw)
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="initial_physical"):
        capacity.revalidate_capacity_geometry(
            replace(
                initial_geometry,
                physical_projection_contract_id="healthporta.provider-directory-profile-physical-projection.v1",
            )
        )


@pytest.mark.parametrize("tablespaces", [(), (20001,), (1663, 20001)])
def test_initial_geometry_refuses_unfunded_receipt_storage(tablespaces):
    with pytest.raises(RuntimeError, match="initial_receipt_tablespace_unsupported"):
        initial.geometry_inputs(
            fhir, None, SimpleNamespace(tablespace_oid=1663), SimpleNamespace(effective_tablespace_oids=tablespaces)
        )


def test_initial_control_plan_has_finite_swap_attempts():
    geometry = _geometry()
    plan = _control_wal_plan_input()
    geometry = replace(geometry, control_wal_plan_input_hash=capacity.profile_control_wal_plan_input_hash(plan))
    projection = capacity.project_profile_control_wal_capacity(geometry, plan)
    operations_by_name = {op.operation_name: op for op in projection.operations}
    assert operations_by_name["affected_npi_payload"].operation_count == 0
    assert operations_by_name["affected_npi_analyze"].operation_count == 0
    assert operations_by_name["initial_cutover"].operation_count == contract.INITIAL_CUTOVER_ATTEMPTS
    assert (
        operations_by_name["initial_cutover"].wal_bytes
        >= contract.INITIAL_CUTOVER_ATTEMPTS * 13 * capacity.CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT
    )
    geometry = replace(
        geometry,
        control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
    )
    projection = capacity.project_profile_control_wal_capacity(geometry, plan)
    assert capacity.revalidate_profile_control_wal_projection(geometry, projection) == projection


def test_legacy_result_is_validated_without_a_manufactured_date():
    execution = synthetic_profile_execution()
    legacy_result = selection.profile_selection_result(
        execution,
        profile_generation_id="pdprofile_" + "a" * 32,
        profile_rows=1,
        profile_source_evidence_rows=2,
        profile_as_of="2026-08-09",
    )
    legacy_result.pop("profile_as_of")
    before = copy.deepcopy(legacy_result)
    assert contract.validated_legacy_result(legacy_result) == before
    target_snapshot = _target()
    target_snapshot.update(
        resolution="legacy_as_of_unknown",
        evidence_rows=2,
        profile_rows=1,
        historical_publication={
            "run_id": "run_" + "b" * 32,
            "result_sha256": contract.result_sha256(legacy_result),
            "result": legacy_result,
            "profile_as_of": None,
            "temporal_metadata": "not_recorded_by_producer",
        },
    )
    assert contract.validated_target_state(target_snapshot) == target_snapshot
    assert legacy_result == before and "profile_as_of" not in legacy_result
    target_snapshot["historical_publication"]["profile_as_of"] = "2026-08-09"
    with pytest.raises(ValueError, match="legacy_history"):
        contract.validated_target_state(target_snapshot)


@pytest.mark.parametrize(
    "change",
    [
        {"serving_singleton_absent": False},
        {"initial_commit_receipt_absent": False},
        {"evidence_rows": 1},
        {"profile_target_oid": 20001},
    ],
)
def test_target_snapshot_refuses_ambiguous_absence(change):
    with pytest.raises(ValueError):
        contract.validated_target_state({**_target(), **change})


def test_missing_serving_state_is_not_an_initial_mode_selector():
    assert not initial.requested(fhir)
    assert not contract.execution_initial_requested(SimpleNamespace(capacity_attestation={}))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "cancel", "wal", "deadline"])
async def test_initial_final_metadata_settles_only_after_every_guard(admitted_window, monkeypatch, failure):
    admission, state = admitted_window
    projection = capacity.ProviderDirectoryProfileMetadataProjection(
        data_bytes=100, wal_bytes=500, commit_envelope_bytes=100
    )
    monkeypatch.setattr(fhir, "_validate_profile_delta_total_wal", AsyncMock())

    async def exercise():
        async with initial.metadata_window(fhir, admission, projection):
            state.wal = 501 if failure == "wal" else 100
            if failure == "cancel":
                raise asyncio.CancelledError()
            if failure == "deadline":
                state.expired = True

    if failure:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await exercise()
        assert admission.wal_tracker.pending_metadata_wal_bytes == 600
        assert admission.wal_tracker.unresolved_window
    else:
        await exercise()
        assert admission.wal_tracker.pending_metadata_wal_bytes == 100
        assert not admission.wal_tracker.unresolved_window
