# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Purpose-scoped immutable capacity consumption and retained Profile WAL."""

import importlib
import importlib.util
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import MetaData

from db.models.system import ImportRun, ProviderDirectoryProfileCapacityLeaseConsumption
from process import provider_directory_cms_nonprofile_capacity as capacity
from process.provider_directory_profile_capacity_attestation import (
    CapacityLeaseConsumptionBinding,
    capacity_lease_consumption_values,
)
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _signed_envelope, _verify
from tests.test_provider_directory_profile_capacity_attestation_schema import _OperationsRecorder

fhir = importlib.import_module("process.provider_directory_fhir")


def _migration():
    """Load only the new purpose migration without invoking the full chain."""
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20260930110000_cms_npd_nonprofile_capacity.py"
    spec = importlib.util.spec_from_file_location("cms_capacity_purpose_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _values(run_id, purpose, reservation):
    """Create fresh synthetic signed values bound to one exact real control row."""
    envelope = _signed_envelope(body_mutator=lambda body: body.update(reservation_id=reservation))
    binding = CapacityLeaseConsumptionBinding(
        run_id, "pdpb_" + uuid.uuid4().hex, "ab" * 32, "cd" * 32, "ef" * 32, "ba" * 32, "2026-07-30"
    )
    return {
        "admission_purpose": purpose,
        **capacity_lease_consumption_values(
            _verify(envelope),
            binding,
            accepted_at=VALIDATION_TIME,
        ),
    }


def test_purpose_migration_locks_and_preserves_global_uniqueness(monkeypatch):
    migration, recorder = _migration(), _OperationsRecorder()
    migration.op = recorder
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_schema")
    migration.upgrade()
    statements = "\n".join(recorder.statements)
    assert "ACCESS EXCLUSIVE" in statements
    assert "UNIQUE (run_id,admission_purpose)" in statements
    assert "DROP CONSTRAINT pd_profile_capacity_consumption_run_key" in statements
    assert "DROP CONSTRAINT pd_profile_capacity_consumption_reservation_key" not in statements
    assert "DROP TRIGGER" not in statements
    recorder.statements.clear()
    migration.downgrade()
    assert "ACCESS EXCLUSIVE" in recorder.statements[1]
    assert "capacity_nonprofile_history_requires_retention" in recorder.statements[2]


@pytest.mark.asyncio
async def test_native_two_purposes_share_one_run_and_reject_conflicts(monkeypatch):
    from tests.test_provider_directory_dataset_artifact_db import _dataset_database
    from tests.test_provider_directory_profile_capacity_attestation_postgres import (
        _apply_profile_delta_migration,
        _install_immutable_guards,
    )

    async with _dataset_database(monkeypatch) as (database, schema):
        ledger = ProviderDirectoryProfileCapacityLeaseConsumption.__table__.to_metadata(MetaData(), schema=schema)
        control = ImportRun.__table__.to_metadata(MetaData(), schema=schema)
        await database.create_table(ledger)
        await database.create_table(control)
        table = fhir._unscoped_qt(schema, ledger.name)
        await database.status(f"ALTER TABLE {table} DROP CONSTRAINT pd_profile_capacity_consumption_run_purpose_key;")
        await database.status(f"ALTER TABLE {table} DROP COLUMN admission_purpose;")
        await database.status(
            f"ALTER TABLE {table} ADD CONSTRAINT pd_profile_capacity_consumption_run_key UNIQUE(run_id);"
        )
        await _install_immutable_guards(database, schema)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        migration = _migration()
        await _apply_profile_delta_migration(database, migration, "upgrade")
        run_id = "run_" + uuid.uuid4().hex
        await (
            database.insert(control)
            .values(run_id=run_id, importer="provider-directory-fhir", status="running", params={})
            .status()
        )
        async with database.transaction():
            await fhir._lock_provider_directory_profile_capacity_control_run(schema=schema, run_id=run_id)
        profile, nonprofile = (
            _values(run_id, "profile", "synthetic-profile"),
            _values(run_id, "cms_nonprofile", "synthetic-full"),
        )
        await fhir._consume_provider_directory_profile_capacity_lease(schema=schema, values_by_name=profile)
        await fhir._consume_provider_directory_profile_capacity_lease(schema=schema, values_by_name=nonprofile)
        await fhir._consume_provider_directory_profile_capacity_lease(schema=schema, values_by_name=nonprofile)
        await _assert_native_conflicts(database, schema, run_id, table, nonprofile)
        current = await fhir._replay_current_consumption(table, run_id)
        assert fhir._pagination_checkpoint_row_mapping(current)["admission_purpose"] == "profile"
        with pytest.raises(DBAPIError, match="capacity_nonprofile_history_requires_retention"):
            await _apply_profile_delta_migration(database, migration, "downgrade")
        assert await database.scalar(f"SELECT count(*) FROM {table};") == 2


async def _assert_native_conflicts(database, schema, run_id, table, nonprofile):
    """Keep conflicting reuse and every immutable-history mutation forbidden."""
    conflict = _values(run_id, "cms_nonprofile", "synthetic-conflict")
    with pytest.raises(RuntimeError, match="capacity_lease_already_consumed"):
        await fhir._consume_provider_directory_profile_capacity_lease(schema=schema, values_by_name=conflict)
    with pytest.raises(RuntimeError, match="capacity_lease_already_consumed"):
        await fhir._consume_provider_directory_profile_capacity_lease(
            schema=schema, values_by_name={**nonprofile, "admission_purpose": "profile"}
        )
    for statement in (
        f"UPDATE {table} SET admission_purpose=admission_purpose",
        f"DELETE FROM {table}",
        f"TRUNCATE {table}",
    ):
        with pytest.raises(DBAPIError):
            await database.status(statement)


@pytest.mark.asyncio
async def test_profile_wal_offset_retains_admission_spend(monkeypatch):
    admission = SimpleNamespace(initial_wal_lsn="0/20", initial_wal_offset_bytes=73)
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=11))
    assert await fhir._provider_directory_profile_current_wal_bytes(admission) == 84
    admission.initial_wal_offset_bytes = -1
    with pytest.raises(RuntimeError, match="wal_offset_invalid"):
        await fhir._provider_directory_profile_current_wal_bytes(admission)


@pytest.mark.asyncio
async def test_cutover_wal_observation_includes_retained_admission_offset(monkeypatch):
    admission = SimpleNamespace(initial_wal_lsn="0/20", initial_wal_offset_bytes=73)
    monkeypatch.setattr(fhir.db, "first", AsyncMock(return_value={"wal_start_lsn": "0/30", "wal_bytes_before": 11}))
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_relation_bytes", AsyncMock(return_value=123))
    observed = await fhir._profile_cutover_observation(
        admission, SimpleNamespace(evidence_target="evidence", profile_target="profile")
    )
    assert observed.wal_bytes == 84 and observed.wal_start_lsn == "0/30"
    admission.initial_wal_offset_bytes = -1
    with pytest.raises(RuntimeError, match="wal_offset_invalid"):
        await fhir._profile_cutover_observation(
            admission, SimpleNamespace(evidence_target="evidence", profile_target="profile")
        )


def _resume_fixture():
    """Build a frozen admission while preserving its one original tracker."""
    identity = SimpleNamespace(serving_state="existing")
    geometry, tracker = SimpleNamespace(reservation_bytes_by_storage_class={"wal": 1000}), SimpleNamespace()
    admission = fhir._ProviderDirectoryProfileCapacityAdmission(
        geometry, "projection", "lease", "database", "pdpb_" + "a" * 32, "run_" + "b" * 32, "0/10", tracker, identity
    )
    return admission, identity


def _resume_backend(admission, identity):
    """Keep source identity and geometry checks on one synthetic transaction backend."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def transaction():
        """Keep source checks and resumption in one synthetic transaction."""
        yield

    return SimpleNamespace(
        db=SimpleNamespace(transaction=transaction),
        _profile_admission_identity=AsyncMock(return_value=identity),
        _profile_admission_workload=AsyncMock(return_value="workload"),
        _profile_admission_inputs=lambda *args: "inputs",
        _profile_admission_geometry=lambda *args: SimpleNamespace(
            geometry=admission.geometry, control_wal_projection="projection"
        ),
        assert_profile_selection_current_in_transaction=AsyncMock(),
        _provider_directory_profile_selection_catalog=lambda: {},
        _lock_and_verify_artifact_dataset_fence=AsyncMock(),
        _schema=lambda: "synthetic_schema",
        _provider_directory_profile_serving_state=AsyncMock(return_value="existing"),
        _assert_provider_directory_profile_capacity_serving_state=lambda expected, observed: None,
        _profile_admission_runtime_state=AsyncMock(return_value=(SimpleNamespace(wal_lsn="0/9000"), {})),
        _provider_directory_profile_capacity_acceptance_time=AsyncMock(),
        _assert_provider_directory_profile_wal_budget=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_resume_retains_identity_tracker_and_measured_wal(monkeypatch):
    """Resumption preserves measured WAL and rejects changed identity, geometry or deadline."""
    import asyncio

    admission, identity = _resume_fixture()
    admission.wal_tracker.lock = asyncio.Lock()
    admission.wal_tracker.accounted_control_operation_counts = {
        "admission_row_lock": 1,
        "capacity_consumption_insert": 3,
    }
    admission.wal_tracker.accounted_relation_wal_bytes = {"artifact_scope": 0}
    admission.wal_tracker.accounted_metadata_wal_bytes = 0
    monkeypatch.setattr(fhir, "_provider_directory_profile_current_wal_bytes", AsyncMock(return_value=73))
    paused = await capacity.pause_profile_capacity(fhir, admission)

    fake = _resume_backend(admission, identity)
    execution = SimpleNamespace(attestation="selection")
    resumed = await capacity.resume_profile_capacity(
        fake, paused, execution, "fence", "delta", frozenset({"Practitioner"})
    )
    assert resumed.initial_wal_lsn == "0/9000" and resumed.initial_wal_offset_bytes == 73
    assert resumed.wal_tracker is admission.wal_tracker and resumed.admitted_identity is identity
    assert resumed.wal_tracker.accounted_control_operation_counts == {
        "admission_row_lock": 1,
        "capacity_consumption_insert": 3,
    }
    fake._assert_provider_directory_profile_wal_budget.assert_awaited_once_with(resumed)
    fake._profile_admission_identity.return_value = "changed"
    with pytest.raises(RuntimeError, match="profile_build_identity_changed"):
        await capacity.resume_profile_capacity(fake, paused, execution, "fence", "delta", frozenset({"Practitioner"}))
    fake._profile_admission_identity.return_value = identity
    fake._profile_admission_geometry = lambda *args: SimpleNamespace(
        geometry="changed", control_wal_projection="projection"
    )
    with pytest.raises(RuntimeError, match="profile_geometry_changed"):
        await capacity.resume_profile_capacity(fake, paused, execution, "fence", "delta", frozenset({"Practitioner"}))
    fake._profile_admission_geometry = lambda *args: SimpleNamespace(
        geometry=admission.geometry, control_wal_projection="projection"
    )
    fake._provider_directory_profile_capacity_acceptance_time.side_effect = RuntimeError("deadline_reached")
    with pytest.raises(RuntimeError, match="deadline_reached"):
        await capacity.resume_profile_capacity(fake, paused, execution, "fence", "delta", frozenset({"Practitioner"}))
    assert admission.initial_wal_lsn == "0/10" and admission.initial_wal_offset_bytes == 0
