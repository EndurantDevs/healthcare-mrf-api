# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Purpose-scoped immutable capacity consumption and retained Profile WAL."""

import importlib
import importlib.util
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import MetaData

from db.models.system import ImportRun, ProviderDirectoryProfileCapacityLeaseConsumption
from process import provider_directory_cms_nonprofile_capacity as capacity
from process import provider_directory_profile_capacity as profile_capacity
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as initial_contract
from process.provider_directory_profile_capacity_attestation import (
    CapacityLeaseConsumptionBinding,
    capacity_lease_consumption_values,
)
from tests.provider_directory_profile_capacity_trust_fixtures import capacity_trust_from_envelope
from tests.provider_directory_profile_initial_test_support import signed_initial_envelope
from tests.provider_directory_profile_replay_test_support import _capacity_envelope, _serving_state_for_identity
from tests.test_provider_directory_profile_bounded_capacity import _bounded_geometry
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _signed_envelope, _verify
from tests.test_provider_directory_profile_capacity_attestation_schema import _OperationsRecorder
from tests.test_provider_directory_profile_control_capacity import _control_wal_plan_input
from tests.test_provider_directory_profile_initial import _geometry as _initial_geometry
from tests.test_provider_directory_profile_initial import _target
from tests.test_provider_directory_profile_initial_replay import _selection

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
    identity = SimpleNamespace(serving_state="existing", initial_targets=None)
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


def _resume_geometry(initial_mode):
    """Reuse the signed geometry contracts and their real control-WAL projections."""
    geometry = (
        replace(_initial_geometry(), initial_target_state_sha256=initial_contract.target_state_sha256(_target()))
        if initial_mode
        else _bounded_geometry()[0]
    )
    plan = _control_wal_plan_input(admission_row_lock_count=fhir.PROVIDER_DIRECTORY_PROFILE_ADMISSION_ROW_LOCK_COUNT)
    geometry = replace(geometry, control_wal_plan_input_hash=profile_capacity.profile_control_wal_plan_input_hash(plan))
    projection = profile_capacity.project_profile_control_wal_capacity(geometry, plan)
    geometry = replace(
        geometry,
        control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
    )
    return geometry, profile_capacity.project_profile_control_wal_capacity(geometry, plan)


def _resume_source_identity(geometry, initial_mode):
    """Supply a typed source observation to the real initial/delta identity dispatcher."""
    return fhir._ProviderDirectoryProfileIdentityInputs(
        source_ids=[],
        retained_source_ids=[],
        dataset_ids=[],
        resume_lineage_hash="ab" * 32,
        batch_plan=fhir._ProviderDirectoryProfileBatchPlan(
            not initial_mode, False, (), (), geometry.executable_plan_hash
        ),
        materialization_mode=geometry.materialization_mode,
        current_source_vector=(),
        desired_source_vector=(),
        current_source_vector_hash=geometry.current_source_vector_hash,
        desired_source_vector_hash=geometry.desired_source_vector_hash,
        current_source_context_vector=(),
        desired_source_context_vector=(),
        current_source_context_vector_hash=geometry.current_context_vector_hash,
        desired_source_context_vector_hash=geometry.desired_context_vector_hash,
        removed_source_ids=(),
        serving_state=None if initial_mode else _serving_state_for_identity(20001, 20002),
    )


def _resume_authority(geometry, execution, initial_mode):
    """Verify a real synthetic signature without issuing a runtime reservation."""
    database_identity = SimpleNamespace(
        database_name=geometry.database_name,
        database_oid=geometry.database_oid,
        database_system_identifier=geometry.database_system_identifier,
        tablespace_oid=geometry.tablespace_oid,
        tablespace_name=geometry.tablespace_name,
        temp_tablespace_oid=geometry.tablespace_oid,
        temp_tablespace_name=geometry.tablespace_name,
    )
    if initial_mode:
        envelope, _receipt = signed_initial_envelope(geometry, database_identity, _target(), execution, VALIDATION_TIME)
    else:
        envelope = _capacity_envelope(
            profile_capacity.capacity_geometry_hash(geometry), database_identity, VALIDATION_TIME
        )
    lease = _verify(
        envelope,
        trust=capacity_trust_from_envelope(envelope),
        expected_capacity_geometry_hash=profile_capacity.capacity_geometry_hash(geometry),
        expected_database_name=geometry.database_name,
        expected_database_oid=geometry.database_oid,
        expected_database_system_identifier=geometry.database_system_identifier,
    )
    return replace(execution, capacity_attestation=envelope), lease


async def _admitted_resume_fixture(monkeypatch, initial_mode):
    """Run admission and identity binding with only database/planning boundaries stubbed."""
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    geometry, projection = _resume_geometry(initial_mode)
    source_identity = _resume_source_identity(geometry, initial_mode)
    execution, lease = _resume_authority(geometry, _selection(geometry.profile_as_of), initial_mode)
    admission_state = (
        initial_contract.InitialTargets(20001, 20002, _target()) if initial_mode else source_identity.serving_state
    )
    database_identity = SimpleNamespace(wal_lsn="0/10")
    template, _ = _resume_fixture()
    template = replace(template, geometry=geometry)
    backend = _resume_backend(template, source_identity)
    backend._provider_directory_profile_serving_state.return_value = source_identity.serving_state
    for name, boundary in vars(backend).items():
        if name not in {"_profile_admission_identity", "_assert_provider_directory_profile_capacity_serving_state"}:
            monkeypatch.setattr(fhir, name, boundary)
    monkeypatch.setattr(fhir, "_profile_build_identity_inputs", AsyncMock(return_value=source_identity))
    monkeypatch.setattr(fhir, "_assert_profile_capacity_run_unconsumed", AsyncMock())
    monkeypatch.setattr(
        fhir, "_profile_capacity_preflight_serving", AsyncMock(return_value=SimpleNamespace(state=admission_state))
    )
    monkeypatch.setattr(
        fhir,
        "_profile_admission_workload",
        AsyncMock(return_value=SimpleNamespace(database_identity=database_identity)),
    )
    monkeypatch.setattr(
        fhir,
        "_profile_admission_geometry",
        lambda *_: SimpleNamespace(geometry=geometry, control_wal_projection=projection),
    )
    monkeypatch.setattr(fhir, "_verified_admission_lease", lambda *_: lease)
    monkeypatch.setattr(fhir, "_consume_admission_transaction", AsyncMock(return_value=database_identity))
    _install_initial_resume_observations(monkeypatch, admission_state, geometry)
    admission = await fhir._admit_provider_directory_profile_capacity(
        run_id=template.run_id,
        control_run_id=template.run_id,
        execution=execution,
        fence="fence",
        resource_fence="resource-fence",
        artifact_resource_types=frozenset({"Practitioner"}),
    )
    monkeypatch.setattr(fhir, "_provider_directory_profile_current_wal_bytes", AsyncMock(return_value=73))
    paused = await capacity.pause_profile_capacity(fhir, admission)
    return paused, execution


def _install_initial_resume_observations(monkeypatch, target, geometry):
    """Keep physical observations replaceable while retaining the production state comparison."""
    monkeypatch.setattr(fhir, "_lock_profile_capacity_preflight_state", AsyncMock())
    monkeypatch.setattr(initial, "capture_targets", AsyncMock(return_value=target))
    monkeypatch.setattr(
        initial,
        "receipt_layout",
        AsyncMock(
            return_value=SimpleNamespace(
                relation_oid=getattr(geometry, "initial_receipt_oid", None),
                exact_fingerprint=getattr(geometry, "initial_receipt_storage_fingerprint", None),
            )
        ),
    )


def _resume_guard_order(monkeypatch):
    """Record the transaction boundary and real resume's ordered observation calls."""
    guard_calls = Mock()

    @asynccontextmanager
    async def transaction():
        guard_calls.begin()
        try:
            yield
        finally:
            guard_calls.end()

    monkeypatch.setattr(fhir.db, "transaction", transaction)
    for boundary, name in (
        (fhir._lock_profile_capacity_preflight_state, "lock"),
        (fhir.assert_profile_selection_current_in_transaction, "selection"),
        (fhir._lock_and_verify_artifact_dataset_fence, "fence"),
        (initial.capture_targets, "targets"),
        (initial.receipt_layout, "receipt"),
        (fhir._provider_directory_profile_serving_state, "serving"),
        (fhir._profile_admission_runtime_state, "runtime"),
        (fhir._provider_directory_profile_capacity_acceptance_time, "deadline"),
        (fhir._assert_provider_directory_profile_wal_budget, "wal"),
    ):
        guard_calls.attach_mock(boundary, name)
    return guard_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_mode", [False, True], ids=["delta", "initial"])
async def test_admitted_profile_pause_resume_keeps_signed_identity(monkeypatch, initial_mode):
    """An admitted initial build resumes through its actual dispatcher, like a delta."""
    paused, execution = await _admitted_resume_fixture(monkeypatch, initial_mode)
    original = paused.admission
    guard_calls = _resume_guard_order(monkeypatch)
    resumed = await capacity.resume_profile_capacity(
        fhir, paused, execution, "fence", "resource-fence", frozenset({"Practitioner"})
    )
    assert resumed == replace(original, initial_wal_lsn="0/9000", initial_wal_offset_bytes=73)
    assert resumed.admitted_identity is original.admitted_identity
    assert resumed.wal_tracker is original.wal_tracker and resumed.lease is original.lease
    fhir._consume_admission_transaction.assert_awaited_once()
    fhir._provider_directory_profile_capacity_acceptance_time.assert_awaited_once_with(original.lease)
    fhir._assert_provider_directory_profile_wal_budget.assert_awaited_once_with(resumed)
    state_guards = (
        ["lock", "selection", "fence", "targets", "receipt"] if initial_mode else ["selection", "fence", "serving"]
    )
    assert [call[0] for call in guard_calls.mock_calls] == ["begin", *state_guards, "runtime", "deadline", "end", "wal"]
    if initial_mode:
        admission_state = original.admitted_identity.initial_targets
        assert original.admitted_identity.serving_state is None
        receipt = original.lease.signing_preflight_guard["healthcare_receipt"]
        fhir._assert_profile_capacity_receipt_serving(receipt, original.admitted_identity)
        assert original.geometry.initial_target_state_sha256 == initial_contract.target_state_sha256(
            admission_state.payload
        )
        fhir._lock_profile_capacity_preflight_state.assert_awaited_once_with(fhir._schema())
        initial.capture_targets.assert_awaited_once_with(fhir, fhir._schema())
        initial.receipt_layout.assert_awaited_once_with(fhir, fhir._schema())
        fhir._provider_directory_profile_serving_state.assert_not_awaited()
    else:
        fhir._provider_directory_profile_serving_state.assert_awaited_once_with(fhir._schema(), for_update=True)
        fhir._lock_profile_capacity_preflight_state.assert_not_awaited()
        initial.capture_targets.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["target_oid", "target_fingerprint", "receipt_oid", "receipt_fingerprint"])
async def test_initial_resume_refuses_fresh_locked_target_or_receipt_drift(monkeypatch, drift):
    """Changed physical identities cannot reuse the original signed initial admission."""
    paused, execution = await _admitted_resume_fixture(monkeypatch, True)
    original = paused.admission
    if drift.startswith("target"):
        target = original.admitted_identity.initial_targets
        payload = dict(target.payload)
        if drift == "target_oid":
            payload["profile_target_oid"] += 1
        else:
            payload["profile_target_storage_fingerprint"] = "ee" * 32
        initial.capture_targets.return_value = replace(
            target, profile_target_oid=payload["profile_target_oid"], payload=payload
        )
    else:
        layout = initial.receipt_layout.return_value
        if drift == "receipt_oid":
            layout.relation_oid += 1
        else:
            layout.exact_fingerprint = "ee" * 32
    with pytest.raises(RuntimeError, match="initial_target_changed"):
        await capacity.resume_profile_capacity(
            fhir, paused, execution, "fence", "resource-fence", frozenset({"Practitioner"})
        )
    fhir._profile_admission_runtime_state.assert_not_awaited()
    fhir._provider_directory_profile_capacity_acceptance_time.assert_not_awaited()
    assert original.initial_wal_lsn == "0/10" and original.initial_wal_offset_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_mode", [False, True], ids=["delta", "initial"])
@pytest.mark.parametrize("failure", ["source_lineage", "deadline"])
async def test_admitted_resume_retains_source_and_deadline_refusals(monkeypatch, initial_mode, failure):
    """Neither mode can change resume lineage or extend the original admitted deadline."""
    paused, execution = await _admitted_resume_fixture(monkeypatch, initial_mode)
    original = paused.admission
    if failure == "source_lineage":
        identity = fhir._profile_build_identity_inputs.return_value
        fhir._profile_build_identity_inputs.return_value = replace(identity, resume_lineage_hash="ef" * 32)
        expected_error = "profile_build_identity_changed"
    else:
        fhir._provider_directory_profile_capacity_acceptance_time.side_effect = RuntimeError("deadline_reached")
        expected_error = "deadline_reached"
    with pytest.raises(RuntimeError, match=expected_error):
        await capacity.resume_profile_capacity(
            fhir, paused, execution, "fence", "resource-fence", frozenset({"Practitioner"})
        )
    assert original.initial_wal_lsn == "0/10" and original.initial_wal_offset_bytes == 0
    fhir._assert_provider_directory_profile_wal_budget.assert_not_awaited()


@pytest.mark.asyncio
async def test_delta_resume_keeps_serving_predecessor_refusal(monkeypatch):
    """The ordinary path still checks its existing predecessor through the real comparator."""
    paused, execution = await _admitted_resume_fixture(monkeypatch, False)
    fhir._provider_directory_profile_serving_state.return_value = None
    with pytest.raises(RuntimeError, match="capacity_serving_generation_changed"):
        await capacity.resume_profile_capacity(
            fhir, paused, execution, "fence", "resource-fence", frozenset({"Practitioner"})
        )
    fhir._profile_admission_runtime_state.assert_not_awaited()
    fhir._provider_directory_profile_capacity_acceptance_time.assert_not_awaited()
    initial.capture_targets.assert_not_awaited()
