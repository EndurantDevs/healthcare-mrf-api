# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for admitted progress and WAL capacity consumption."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import importlib
import inspect
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution
from .test_provider_directory_profile_capacity import _geometry_payload
from .test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
)
from .provider_directory_profile_execution_test_support import (
    _capacity_consumption_row,
    _capacity_geometry_identity,
    _profile_serving_state,
    _published_dataset_state,
    _wal_tracker_admission,
)

importer = importlib.import_module("process.provider_directory_fhir")
capacity = importlib.import_module("process.provider_directory_profile_capacity")


@pytest.mark.parametrize(
    "sampler",
    (
        importer._provider_directory_profile_current_wal_bytes,
        importer._profile_cutover_observation,
        importer._profile_delta_target_wal_start_lsn,
        importer._profile_delta_cutover_actual,
        importer._validate_profile_delta_final_wal,
        importer._PROFILE_CAPACITY_DATABASE_IDENTITY_SQL,
    ),
)
def test_capacity_wal_samplers_use_one_insert_location(sampler):
    """Bind each observation to one backend WAL insert position."""
    source = sampler if isinstance(sampler, str) else inspect.getsource(sampler)
    assert "pg_current_wal_lsn()" not in source
    assert source.count("pg_current_wal_insert_lsn()") == 1


@pytest.mark.asyncio
async def test_capacity_admitted_progress_failure_is_fail_closed(
    monkeypatch,
):
    @asynccontextmanager
    async def capacity_transaction():
        yield

    monkeypatch.setattr(
        importer,
        "mark_control_run",
        AsyncMock(side_effect=RuntimeError("control unavailable")),
    )
    monkeypatch.setattr(
        importer,
        "_reserve_provider_directory_profile_wal_budget",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_transaction",
        capacity_transaction,
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(
            return_value=SimpleNamespace(exact_fingerprint="f" * 64)
        ),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(
            geometry=SimpleNamespace(
                import_run_oid=123,
                import_run_storage_fingerprint="f" * 64,
            )
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="capacity_progress_update_failed",
    ):
        await importer._mark_provider_directory_progress(
            "run_" + "b" * 32,
            phase="provider-directory profile evidence batches",
            done=1,
            total=2,
            message="bounded progress",
        )


def _wal_tracker_admission():
    geometry, control_projection = _bound_control_wal_projection()
    return importer._ProviderDirectoryProfileCapacityAdmission(
        geometry=geometry,
        control_wal_projection=control_projection,
        lease=SimpleNamespace(
            max_build_deadline=(
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(minutes=10)
            )
        ),
        database_identity=SimpleNamespace(),
        build_id="pdpb_" + "a" * 32,
        run_id="run_" + "b" * 32,
        initial_wal_lsn="0/1",
        wal_tracker=importer._ProviderDirectoryProfileWalTracker(
            accounted_control_operation_counts={
                "admission_row_lock": 2,
                "capacity_consumption_insert": 1,
            }
        ),
    )


def _capacity_consumption_row(admission):
    return {
        "attestation_id": admission.lease.attestation_id,
        "lease_digest": admission.lease.lease_digest,
        "capacity_geometry_hash": capacity.capacity_geometry_hash(
            admission.geometry
        ),
        "executable_plan_hash": admission.geometry.executable_plan_hash,
        "selection_proof_id": admission.geometry.selection_proof_id,
        "source_vector_hash": (
            admission.geometry.desired_source_vector_hash
        ),
        "source_context_vector_hash": (
            admission.geometry.desired_context_vector_hash
        ),
        "run_id": admission.run_id,
        "build_id": admission.build_id,
        "profile_as_of": admission.geometry.profile_as_of,
    }


@pytest.mark.asyncio
async def test_capacity_wal_observation_uses_insert_location(monkeypatch):
    scalar = AsyncMock(return_value=17)
    monkeypatch.setattr(importer.db, "scalar", scalar)

    observed = await importer._provider_directory_profile_current_wal_bytes(
        SimpleNamespace(initial_wal_lsn="0/1")
    )

    query = scalar.await_args.args[0]
    assert observed == 17
    assert "pg_current_wal_insert_lsn()" in query
    assert "pg_current_wal_lsn()" not in query


def test_capacity_admission_retains_prelock_wal_baseline():
    base = _wal_tracker_admission()
    admission = importer._profile_admission_result(
        base.run_id,
        base.build_id,
        SimpleNamespace(),
        SimpleNamespace(
            geometry=base.geometry,
            control_wal_projection=base.control_wal_projection,
        ),
        base.lease,
        SimpleNamespace(wal_lsn="0/9"),
        "0/1",
    )

    assert admission.database_identity.wal_lsn == "0/9"
    assert admission.initial_wal_lsn == "0/1"


@pytest.mark.asyncio
async def test_capacity_consumption_allows_terminal_prior_same_build(
    monkeypatch,
):
    admission = dataclasses.replace(
        _wal_tracker_admission(),
        lease=SimpleNamespace(
            attestation_id="c" * 64,
            lease_digest="d" * 64,
            max_build_deadline=(
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(minutes=10)
            ),
        ),
    )
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            side_effect=[
                [_capacity_consumption_row(admission)],
                [{"run_id": "run_" + "e" * 32, "status": "canceled"}],
            ]
        ),
    )

    await importer._assert_provider_directory_profile_capacity_consumption(
        admission,
        SimpleNamespace(schema="mrf"),
    )


@pytest.mark.asyncio
async def test_capacity_consumption_rejects_nonterminal_prior_same_build(
    monkeypatch,
):
    admission = dataclasses.replace(
        _wal_tracker_admission(),
        lease=SimpleNamespace(
            attestation_id="c" * 64,
            lease_digest="d" * 64,
            max_build_deadline=(
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(minutes=10)
            ),
        ),
    )
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(
            side_effect=[
                [_capacity_consumption_row(admission)],
                [{"run_id": "run_" + "e" * 32, "status": "running"}],
            ]
        ),
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="capacity_competing_owner",
    ):
        await (
            importer
            ._assert_provider_directory_profile_capacity_consumption(
                admission,
                SimpleNamespace(schema="mrf"),
            )
        )


@pytest.mark.asyncio
async def test_capacity_wal_tracker_reserves_two_worker_wave_atomically(
    monkeypatch,
):
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_current_wal_bytes",
        AsyncMock(return_value=0),
    )

    await importer._gather_provider_directory_profile_tasks(
        [
            asyncio.create_task(
                importer._reserve_provider_directory_profile_wal_budget(
                    admission,
                    control_operation_counts={"evidence_payload": 1},
                )
            )
            for _ in range(2)
        ]
    )

    assert (
        admission.wal_tracker.accounted_control_operation_counts[
            "evidence_payload"
        ]
        == 2
    )


@pytest.mark.asyncio
async def test_capacity_wal_tracker_refuses_unforecast_external_wal(
    monkeypatch,
):
    admission = _wal_tracker_admission()
    capacity_operation = next(
        operation
        for operation in admission.control_wal_projection.operations
        if operation.operation_name == "capacity_consumption_insert"
    )
    admission_lock_operation = next(
        operation
        for operation in admission.control_wal_projection.operations
        if operation.operation_name == "admission_row_lock"
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_current_wal_bytes",
        AsyncMock(
            return_value=(
                capacity_operation.wal_bytes_per_operation
                + admission_lock_operation.wal_bytes
                + 1
            )
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="capacity_total_wal_exceeded",
    ):
        await importer._assert_provider_directory_profile_wal_budget(
            admission
        )
