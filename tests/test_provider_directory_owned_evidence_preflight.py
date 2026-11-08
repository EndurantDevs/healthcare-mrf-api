# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preflight reader custody without deriving stage counts from progress."""

import asyncio
import contextlib
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_provider_directory_owned_evidence_wave import Session, custody, fhir, owned_wave


class ReaderSession(Session):
    """Execute the existing aggregate reads on a watched native owner."""

    async def execute(self, statement, parameters):
        assert self.active and "limits" in self.bind.events
        state = self.bind.state
        sql = str(statement)
        self.bind.events.append("read")
        state.reads.append((sql, dict(parameters), self.bind.driver.pid))
        if "projected_rows" in sql:
            return await self.read_projection()
        assert sql == 'SELECT count(*) FROM "synthetic"."synthetic_evidence";'
        assert parameters == {}
        assert all(connection.closed for connection in state.connections[:-1])
        assert state.checkedout == 1
        state.stage_counts += 1
        if state.mode == "stage_postcommit_sample":
            self.bind.driver.fail_command = "sample"
            self.bind.driver.error = OSError("synthetic stage observation failure")
        return SimpleNamespace(scalar=lambda: state.existing_rows)

    async def read_projection(self):
        """Coordinate synthetic reader overlap and post-commit observation failures."""
        state = self.bind.state
        state.projection_entered += 1
        if state.projection_entered == 2:
            state.projections_started.set()
        if state.mode == "first_reader_failure":
            await state.projections_started.wait()
            if self.bind.driver.pid == 4321:
                raise ValueError("synthetic_projection_failure")
            await state.release_projections.wait()
        elif state.mode == "block_readers":
            await state.release_projections.wait()
        if state.mode == "postcommit_sample":
            self.bind.driver.fail_command = "sample"
            self.bind.driver.error = OSError("synthetic observation failure")
        return SimpleNamespace(first=lambda: {"projected_rows": 2, "projected_logical_bytes": 256})


@pytest.fixture
def owned_preflight(owned_wave, monkeypatch):
    """Reuse the pool/driver fixture with the signed bounded execution mode."""
    state, database, admission, build, batches, projection, _ledger = owned_wave
    state.pool = asyncio.Semaphore(2)
    state.reads, state.projection_entered, state.stage_counts = [], 0, 0
    state.projections_started, state.release_projections = asyncio.Event(), asyncio.Event()
    state.existing_rows = 7
    admission = replace(
        admission,
        geometry=replace(
            admission.geometry,
            physical_projection_contract_id=fhir.profile_capacity.BOUNDED_ADMISSION_CONTRACT_ID,
        ),
    )
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    monkeypatch.setattr(custody, "AsyncSession", ReaderSession)
    monkeypatch.setattr(
        fhir.profile_artifact,
        "profile_evidence_count_sql",
        Mock(return_value="SELECT projected_rows, projected_logical_bytes FROM synthetic_source;"),
    )
    monkeypatch.setattr(fhir, "_require_provider_directory_profile_stage_oid", AsyncMock(return_value=101))
    monkeypatch.setattr(fhir, "_project_provider_directory_profile_scratch_window", AsyncMock())
    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", AsyncMock())
    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", lambda *_args: contextlib.nullcontext())
    window_token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), "evidence_stage"))
    try:
        yield state, database, admission, build, batches, projection
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(window_token)


async def preflight(fixture):
    """Run the actual caller with two exact immutable coordinates."""
    _state, _database, _admission, build, batches, _projection = fixture
    return await fhir._preflight_profile_evidence_window_capacity(build, list(enumerate(batches)), {})


async def bounded_wave(fixture):
    """Reach reader admission before the payload dispatcher."""
    _state, _database, _admission, build, batches, _projection = fixture
    return await fhir._execute_bounded_evidence_window(build, list(enumerate(batches)), "COPY unused", {})


def current_capture(fixture):
    """Return only the current source-bound reader diagnostic."""
    captures = fixture[2].wal_tracker.owned_evidence_preflight_outcomes
    assert len(captures) == 1
    return captures[0]


@pytest.mark.asyncio
async def test_two_projection_readers_drain_before_parent_stage_owner(owned_preflight):
    """Parallel projection drivers close before the parent can check out."""
    state, _database, admission, build, batches, _projection = owned_preflight
    state.mode = "block_readers"
    build.evidence_next_batch = 0
    build.evidence_rows = 999999
    task = asyncio.create_task(preflight(owned_preflight))
    await asyncio.wait_for(state.projections_started.wait(), 1)
    assert len(state.connections) == state.checkedout == state.max_checkedout == 2
    assert state.stage_counts == 0
    state.release_projections.set()
    projections = await task
    assert set(projections) == {0, 1} and state.stage_counts == 1
    assert len(state.connections) == 3 and state.max_checkedout == 2 and state.checkedout == 0
    capture = current_capture(owned_preflight)
    assert capture["projection_readers"]["expected_readers"] == capture["projection_readers"]["drained_readers"] == 2
    assert capture["status"] == "committed_measured_mixed_unclassified"
    assert not capture["accounting_authority"] and not capture["reservation_refund"]
    assert capture["stage_reader"]["committed"]
    assert all(
        reader["wal_classification"] == "mixed_unclassified" for reader in capture["projection_readers"]["readers"]
    )
    assert len({reader["measurement"]["backend_pid"] for reader in capture["projection_readers"]["readers"]}) == 2
    json.dumps(capture)
    for (_sql, params, _pid), batch in zip(state.reads[:2], batches, strict=True):
        assert params == {
            "source_ids": [batch.source_id],
            "dataset_ids": [batch.dataset_id],
            "profile_as_of": build.profile_as_of,
        }
    fhir._project_provider_directory_profile_scratch_window.assert_awaited_once_with(
        "evidence_stage",
        '"synthetic"."synthetic_evidence"',
        101,
        inserted_rows=4,
        inserted_logical_bytes=512,
        expected_persistence="p",
    )
    fhir._reserve_provider_directory_profile_wal_budget.assert_awaited_once_with(
        admission, control_operation_counts={"evidence_payload": 2}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("overrun", [False, True])
async def test_resume_uses_real_stage_rows_and_keeps_original_cap(owned_preflight, overrun):
    """Committed rows count even when the durable batch cursor has not advanced."""
    state, _database, admission, build, _batches, _projection = owned_preflight
    build.evidence_next_batch = 0
    build.evidence_rows = 0
    state.existing_rows = admission.geometry.max_evidence_rows - (3 if overrun else 4)
    if overrun:
        with pytest.raises(RuntimeError, match="provider_directory_profile_capacity_evidence_rows_projected"):
            await preflight(owned_preflight)
        fhir._reserve_provider_directory_profile_wal_budget.assert_not_awaited()
        assert current_capture(owned_preflight)["stage_reader"]["commit_state"] == "rolled_back"
    else:
        assert set(await preflight(owned_preflight)) == {0, 1}
    assert state.stage_counts == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["postcommit_sample", "stage_postcommit_sample"])
async def test_reader_commit_without_measurement_stops_before_payload(owned_preflight, monkeypatch, mode):
    """Retain confirmed commits when final native observation is unavailable."""
    state = owned_preflight[0]
    state.mode = mode
    payload = AsyncMock(side_effect=AssertionError("incomplete reader cannot dispatch payload"))
    monkeypatch.setattr(fhir, "_run_profile_evidence_window", payload)
    with pytest.raises(custody.OwnedWalTransactionError):
        await bounded_wave(owned_preflight)
    payload.assert_not_awaited()
    capture = current_capture(owned_preflight)
    assert capture["status"] == "accounting_incomplete"
    readers = capture["projection_readers"]["readers"] if mode == "postcommit_sample" else [capture["stage_reader"]]
    assert any(reader["committed"] and reader["status"] == "committed_accounting_incomplete" for reader in readers)
    assert all(reader["measurement"] is None for reader in readers)
    assert state.stage_counts == (0 if mode == "postcommit_sample" else 1)
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_missing_projection_capture_rejects_before_parent_checkout(owned_preflight, monkeypatch):
    """A returned projection does not manufacture native reader evidence."""
    monkeypatch.setattr(fhir, "_project_owned_profile_evidence_batch", AsyncMock(return_value=owned_preflight[-1]))
    payload = AsyncMock()
    monkeypatch.setattr(fhir, "_run_profile_evidence_window", payload)
    with pytest.raises(RuntimeError, match="preflight_capture_incomplete"):
        await bounded_wave(owned_preflight)
    capture = current_capture(owned_preflight)
    assert capture["projection_readers"]["drained_readers"] == 2
    assert all(reader["committed"] is None for reader in capture["projection_readers"]["readers"])
    assert capture["stage_reader"]["commit_state"] == "unobserved"
    assert owned_preflight[0].connections == []
    payload.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_reader_failure_drains_blocked_sibling(owned_preflight, monkeypatch):
    """The original reader error survives cleanup and no parent owner starts."""
    state = owned_preflight[0]
    state.mode = "first_reader_failure"
    payload = AsyncMock()
    monkeypatch.setattr(fhir, "_run_profile_evidence_window", payload)
    with pytest.raises(ValueError, match="synthetic_projection_failure"):
        await asyncio.wait_for(bounded_wave(owned_preflight), 2)
    capture = current_capture(owned_preflight)
    assert capture["projection_readers"]["drained_readers"] == 2
    assert len(state.connections) == 2 and state.checkedout == state.stage_counts == 0
    assert all(connection.closed for connection in state.connections)
    payload.assert_not_awaited()


@pytest.mark.asyncio
async def test_repeated_caller_cancellation_drains_all_readers(owned_preflight):
    """Cancellation leaves neither a reader task nor a checked-out driver."""
    state = owned_preflight[0]
    state.mode = "block_readers"
    task = asyncio.create_task(preflight(owned_preflight))
    await asyncio.wait_for(state.projections_started.wait(), 1)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    capture = current_capture(owned_preflight)
    assert capture["projection_readers"]["drained_readers"] == 2
    assert capture["stage_reader"]["commit_state"] == "unobserved"
    assert state.checkedout == state.stage_counts == 0 and len(state.connections) == 2
    assert all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_inherited_parent_borrowed_session_cannot_be_used_by_readers(owned_preflight):
    """Child reader ownership fails before borrowing the parent connection."""
    state, database, _admission, _build, _batches, _projection = owned_preflight
    async with custody.registry_owned_wal_transaction(database):
        task = asyncio.create_task(preflight(owned_preflight))
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await task
        assert len(state.connections) == 1 and state.checkedout == 1
    assert state.checkedout == 0 and state.stage_counts == 0
    assert current_capture(owned_preflight)["status"] == "accounting_incomplete"


@pytest.mark.asyncio
async def test_current_reader_capture_drops_earlier_wave_and_task_objects(owned_preflight):
    """The retained diagnostic stays bounded across repeated waves."""
    await preflight(owned_preflight)
    earlier = current_capture(owned_preflight)
    await preflight(owned_preflight)
    current = current_capture(owned_preflight)
    assert current is not earlier
    assert len(current["projection_readers"]["readers"]) == 2
    assert current["stage_reader"]["measurement_status"] == "complete"
    json.dumps(current)


@pytest.mark.asyncio
async def test_unadmitted_preflight_preserves_legacy_without_owner(monkeypatch):
    """No observation checkout is introduced for an unadmitted build."""
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)
    owner = Mock(side_effect=AssertionError("unadmitted reader must keep its original path"))
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", owner)
    assert await fhir._preflight_profile_evidence_window_capacity("build", [], {}) == {}
    owner.assert_not_called()


@pytest.mark.asyncio
async def test_larger_frozen_wave_never_opens_more_than_two_readers(owned_preflight):
    """Queued coordinates are fully drained before the stage reader begins."""
    state, _database, _admission, build, batches, _projection = owned_preflight
    state.pool = asyncio.Semaphore(8)
    state.mode = "block_readers"
    coordinates = list(enumerate([batches[0]] * 5))
    task = asyncio.create_task(fhir._preflight_profile_evidence_window_capacity(build, coordinates, {}))
    await asyncio.wait_for(state.projections_started.wait(), 1)
    await asyncio.sleep(0)
    assert len(state.connections) == state.checkedout == 2
    state.release_projections.set()
    assert set(await task) == set(range(5))
    assert state.max_checkedout == 2 and len(state.connections) == 6
    assert current_capture(owned_preflight)["projection_readers"]["drained_readers"] == 5
    assert state.stage_counts == 1 and state.checkedout == 0


@pytest.mark.asyncio
async def test_duplicate_coordinates_drain_every_reader_before_rejection(owned_preflight):
    """Conflicting coordinates cannot hide an opened task or its native capture."""
    state, _database, _admission, build, batches, _projection = owned_preflight
    with pytest.raises(RuntimeError, match="preflight_capture_incomplete"):
        await fhir._preflight_profile_evidence_window_capacity(build, [(0, batches[0]), (0, batches[1])], {})
    capture = current_capture(owned_preflight)
    assert capture["projection_readers"]["expected_readers"] == 2
    assert capture["projection_readers"]["drained_readers"] == 2
    assert len(capture["projection_readers"]["readers"]) == 2
    assert len(state.connections) == 2 and state.stage_counts == state.checkedout == 0
    assert all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_unbounded_admission_keeps_original_projection_path(owned_preflight, monkeypatch):
    """Legacy admission preserves count, capacity checks and its existing helper."""
    state, _database, admission, _build, _batches, projection = owned_preflight
    legacy_admission = replace(
        admission, geometry=replace(admission.geometry, physical_projection_contract_id="legacy")
    )
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: legacy_admission)
    project = AsyncMock(return_value=projection)
    monkeypatch.setattr(fhir, "_project_profile_evidence_batch_with_capacity", project)
    stage = AsyncMock(return_value={0: projection, 1: projection})
    monkeypatch.setattr(fhir, "_preflight_profile_evidence_stage_capacity", stage)
    owner = Mock(side_effect=AssertionError("legacy preflight must retain its original path"))
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", owner)
    assert set(await preflight(owned_preflight)) == {0, 1}
    assert project.await_count == 2
    stage.assert_awaited_once()
    owner.assert_not_called()
    assert state.connections == [] and admission.wal_tracker.owned_evidence_preflight_outcomes == []


class CompactReaderSession(ReaderSession):
    """Run original compact aggregates on the existing retained fake driver."""

    async def execute(self, statement, parameters):
        assert self.active and "limits" in self.bind.events
        state = self.bind.state
        sql = str(statement)
        self.bind.events.append("read")
        state.reads.append((sql, dict(parameters), self.bind.driver.pid))
        if "projected_rows" in sql:
            return await self.read_projection()
        assert sql == 'SELECT count(*) FROM "synthetic"."synthetic_profile";'
        assert parameters == {} and all(connection.closed for connection in state.connections[:-1])
        assert state.checkedout == 1
        state.stage_counts += 1
        if state.mode == "stage_postcommit_sample":
            self.bind.driver.fail_command = "sample"
            self.bind.driver.error = OSError("synthetic stage observation failure")
        return SimpleNamespace(scalar=lambda: state.existing_rows)


@pytest.fixture
def owned_compact_preflight(owned_preflight, monkeypatch):
    """Use the same native owner fixture with compact SQL and pool one."""
    state, database, admission, build, _batches, projection = owned_preflight
    state.pool = asyncio.Semaphore(1)
    build.profile_stage = "synthetic_profile"
    build.build_id, build.generation_id = "synthetic_build", "synthetic_generation"
    # Supplied by the coordinated worker candidate's tracker field addition.
    admission.wal_tracker.owned_compact_preflight_outcomes = []
    monkeypatch.setattr(custody, "AsyncSession", CompactReaderSession)
    batches = [
        fhir._ProviderDirectoryProfileCompactBatch(kind="npi", npi_start=1000000000 + index, npi_end=1000000001 + index)
        for index in range(2)
    ]
    token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), "profile_stage"))
    try:
        yield state, database, admission, build, batches, projection
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


async def compact_preflight(fixture, coordinates=None):
    """Reach the real compact caller and original generated SQL arguments."""
    _state, _database, _admission, build, batches, _projection = fixture
    return await fhir._preflight_profile_compact_window_capacity(
        build,
        list(enumerate(batches)) if coordinates is None else coordinates,
        profile_count_sql=lambda **_kwargs: "SELECT projected_rows, projected_logical_bytes FROM synthetic_compact;",
        profile_sql_args_by_name={"profile_ref": "synthetic_profile"},
        profile_params_by_name={"source_ids": ["synthetic_source"], "profile_as_of": "2026-01-01"},
    )


def compact_capture(fixture):
    captures = fixture[2].wal_tracker.owned_compact_preflight_outcomes
    assert len(captures) == 1
    return captures[0]


@pytest.mark.asyncio
async def test_compact_readers_commit_before_stage_with_pool_one(owned_compact_preflight):
    state, _database, admission, _build, batches, _projection = owned_compact_preflight
    projections = await compact_preflight(owned_compact_preflight)
    assert set(projections) == {0, 1} and all(projection.projected_rows == 2 for projection in projections.values())
    assert len(state.connections) == 3 and state.max_checkedout == 1 and state.checkedout == 0
    capture = compact_capture(owned_compact_preflight)
    assert capture["coordinate"] == {"batch_start": 0, "batch_end": 2}
    assert capture["status"] == "committed_measured_mixed_unclassified"
    assert not capture["accounting_authority"] and not capture["reservation_refund"]
    assert capture["stage_reader"]["reader_scope"] == "compact_stage_preflight"
    for connection in state.connections:
        assert connection.events.index("read") < connection.events.index("commit")
        assert connection.events.index("commit") < len(connection.events) - 1 - connection.events[::-1].index("sample")
        assert connection.closed and connection.driver.settings == connection.driver.original
    for (sql, params, _backend_pid), batch in zip(state.reads[:2], batches, strict=True):
        assert sql == "SELECT projected_rows, projected_logical_bytes FROM synthetic_compact;"
        assert params == {
            "source_ids": ["synthetic_source"],
            "profile_as_of": "2026-01-01",
            "profile_npi_start": batch.npi_start,
            "profile_npi_end": batch.npi_end,
        }
    fhir._project_provider_directory_profile_scratch_window.assert_awaited_once_with(
        "profile_stage",
        '"synthetic"."synthetic_profile"',
        101,
        inserted_rows=4,
        inserted_logical_bytes=512,
        expected_persistence="p",
    )
    fhir._reserve_provider_directory_profile_wal_budget.assert_awaited_once_with(
        admission, control_operation_counts={"profile_payload": 2}
    )
    json.dumps(capture)


@pytest.mark.asyncio
async def test_compact_stage_overrun_rolls_back_before_reservation(owned_compact_preflight):
    state, _database, admission, _build, _batches, _projection = owned_compact_preflight
    state.existing_rows = admission.geometry.max_profile_rows - 3
    with pytest.raises(RuntimeError, match="profile_rows_projected"):
        await compact_preflight(owned_compact_preflight)
    fhir._reserve_provider_directory_profile_wal_budget.assert_not_awaited()
    assert compact_capture(owned_compact_preflight)["stage_reader"]["commit_state"] == "rolled_back"
    assert "commit" not in state.connections[-1].events and state.checkedout == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["postcommit_sample", "stage_postcommit_sample", "unknown_commit"])
async def test_compact_reader_measurement_failure_preserves_commit(owned_compact_preflight, mode):
    state = owned_compact_preflight[0]
    state.mode = mode
    with pytest.raises(custody.OwnedWalTransactionError):
        await compact_preflight(owned_compact_preflight)
    capture = compact_capture(owned_compact_preflight)
    assert capture["status"] == "accounting_incomplete" and not capture["reservation_refund"]
    readers = (
        [capture["stage_reader"]] if mode == "stage_postcommit_sample" else capture["projection_readers"]["readers"]
    )
    if mode == "unknown_commit":
        assert any(
            reader["commit_state"] == "attempted" and reader["status"] == "commit_uncertain_accounting_incomplete"
            for reader in readers
        )
    else:
        assert any(reader["status"] == "committed_accounting_incomplete" and reader["committed"] for reader in readers)
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_compact_duplicate_coordinate_rejects_after_drain(owned_compact_preflight):
    state, _database, _admission, _build, batches, _projection = owned_compact_preflight
    with pytest.raises(RuntimeError, match="compact_preflight_capture_incomplete"):
        await compact_preflight(owned_compact_preflight, [(0, batches[0]), (0, batches[1])])
    capture = compact_capture(owned_compact_preflight)
    assert capture["projection_readers"]["drained_readers"] == 2
    assert capture["stage_reader"]["commit_state"] == "unobserved"
    assert state.stage_counts == state.checkedout == 0


@pytest.mark.asyncio
async def test_compact_missing_native_capture_blocks_stage(owned_compact_preflight, monkeypatch):
    monkeypatch.setattr(
        fhir, "_project_owned_profile_compact_batch", AsyncMock(return_value=owned_compact_preflight[-1])
    )
    with pytest.raises(RuntimeError, match="compact_preflight_capture_incomplete"):
        await compact_preflight(owned_compact_preflight)
    assert owned_compact_preflight[0].connections == []
    assert compact_capture(owned_compact_preflight)["stage_reader"]["commit_state"] == "unobserved"


@pytest.mark.asyncio
async def test_compact_failure_drains_sibling_before_stage(owned_compact_preflight):
    state = owned_compact_preflight[0]
    state.pool, state.mode = asyncio.Semaphore(2), "first_reader_failure"
    with pytest.raises(ValueError, match="synthetic_projection_failure"):
        await asyncio.wait_for(compact_preflight(owned_compact_preflight), 2)
    assert compact_capture(owned_compact_preflight)["projection_readers"]["drained_readers"] == 2
    assert state.checkedout == state.stage_counts == 0 and all(connection.closed for connection in state.connections)


@pytest.mark.asyncio
async def test_compact_repeated_cancellation_drains_readers(owned_compact_preflight):
    state = owned_compact_preflight[0]
    state.pool, state.mode = asyncio.Semaphore(2), "block_readers"
    task = asyncio.create_task(compact_preflight(owned_compact_preflight))
    await asyncio.wait_for(state.projections_started.wait(), 1)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert state.checkedout == state.stage_counts == 0 and all(connection.closed for connection in state.connections)
    assert compact_capture(owned_compact_preflight)["projection_readers"]["drained_readers"] == 2


@pytest.mark.asyncio
async def test_compact_capture_retains_only_current_wave(owned_compact_preflight):
    await compact_preflight(owned_compact_preflight)
    earlier = compact_capture(owned_compact_preflight)
    await compact_preflight(owned_compact_preflight)
    assert compact_capture(owned_compact_preflight) is not earlier
    assert len(owned_compact_preflight[2].wal_tracker.owned_compact_preflight_outcomes) == 1


@pytest.mark.asyncio
async def test_compact_unadmitted_path_has_no_owner(monkeypatch):
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)
    owner = Mock(side_effect=AssertionError("no observation checkout for unadmitted compact"))
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", owner)
    assert (
        await fhir._preflight_profile_compact_window_capacity(
            "build", [], profile_count_sql=Mock(), profile_sql_args_by_name={}, profile_params_by_name={}
        )
        == {}
    )
    owner.assert_not_called()


@pytest.mark.asyncio
async def test_compact_parent_binding_cannot_checkout_child_owner(owned_compact_preflight):
    state, database, _admission, _build, _batches, _projection = owned_compact_preflight
    async with custody.registry_owned_wal_transaction(database):
        task = asyncio.create_task(compact_preflight(owned_compact_preflight))
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await asyncio.wait_for(task, 1)
        assert len(state.connections) == state.checkedout == 1
    assert state.checkedout == state.stage_counts == 0
    assert compact_capture(owned_compact_preflight)["status"] == "accounting_incomplete"


@pytest.mark.asyncio
async def test_compact_direct_projection_preserves_original_unbound_sql(monkeypatch):
    """No borrowed option or native owner is introduced on the original path."""
    batch = fhir._ProviderDirectoryProfileCompactBatch(kind="npi", npi_start=1000000000, npi_end=1000000001)
    sql = Mock(return_value="SELECT original_compact_projection;")
    database = SimpleNamespace(first=AsyncMock(return_value={"projected_rows": 3, "projected_logical_bytes": 99}))
    transaction = Mock(return_value=contextlib.nullcontext())
    owner = Mock(side_effect=AssertionError("unbound projection must keep original path"))
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_transaction", transaction)
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", owner)
    result = await fhir._project_provider_directory_profile_compact_batch(
        "build",
        batch,
        profile_count_sql=sql,
        profile_sql_args_by_name={"profile_ref": "original_ref"},
        profile_params_by_name={"source_ids": ["source"]},
    )
    assert (result.projected_rows, result.projected_logical_bytes) == (3, 99)
    sql.assert_called_once_with(profile_ref="original_ref", npi_start=batch.npi_start, npi_end=batch.npi_end)
    database.first.assert_awaited_once_with(
        "SELECT original_compact_projection;",
        source_ids=["source"],
        profile_npi_start=batch.npi_start,
        profile_npi_end=batch.npi_end,
    )
    transaction.assert_called_once_with()
    owner.assert_not_called()


@pytest.mark.asyncio
async def test_compact_unbounded_admission_keeps_legacy_helpers(owned_compact_preflight, monkeypatch):
    fixture = owned_compact_preflight
    admission = replace(fixture[2], geometry=replace(fixture[2].geometry, physical_projection_contract_id="legacy"))
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: admission)
    projection = fixture[-1]
    project = AsyncMock(return_value=projection)
    stage = AsyncMock(return_value={0: projection, 1: projection})
    monkeypatch.setattr(fhir, "_project_provider_directory_profile_compact_batch", project)
    monkeypatch.setattr(fhir, "_preflight_profile_compact_stage_capacity", stage)
    owner = Mock(side_effect=AssertionError("legacy admission cannot add retained reader checkout"))
    monkeypatch.setattr(custody, "registry_owned_wal_transaction", owner)
    assert set(await compact_preflight(fixture)) == {0, 1}
    assert project.await_count == 2 and all("reuse_borrowed" not in call.kwargs for call in project.await_args_list)
    stage.assert_awaited_once()
    owner.assert_not_called()
    assert fixture[0].connections == [] and admission.wal_tracker.owned_compact_preflight_outcomes == []


@pytest.fixture(params=[False, True])
def native_preflight(request):
    compact = request.param
    return compact, request.getfixturevalue("owned_compact_preflight" if compact else "owned_preflight")


@pytest.mark.asyncio
async def test_preflight_retains_actual_native_objects_and_json_separation(native_preflight, monkeypatch):
    compact, fixture = native_preflight
    actual_outcomes = []
    original = fhir._profile_owned_transaction_capture

    def capture(outcome, failure):
        if outcome is not None:
            actual_outcomes.append(outcome)
        return original(outcome, failure)

    monkeypatch.setattr(fhir, "_profile_owned_transaction_capture", capture)
    run = compact_preflight if compact else preflight
    await run(fixture)
    tracker = fixture[2].wal_tracker
    retained_list = (
        tracker.owned_compact_preflight_native_outcomes if compact else tracker.owned_evidence_preflight_native_outcomes
    )
    retained = retained_list[0]
    assert retained["status"] == "complete" and retained["build"] is fixture[3]
    assert retained["admission"] is fixture[2] and not retained["accounting_authority"]
    assert [entry["outcome"] for entry in retained["readers"]] == actual_outcomes
    assert all(entry["outcome"] is outcome for entry, outcome in zip(retained["readers"], actual_outcomes, strict=True))
    assert [entry["coordinate"] for entry in retained["readers"]] == [0, 1, "stage"]
    assert all(reader["task"].done() for reader in retained["readers"][:-1])
    json.dumps(compact_capture(fixture) if compact else current_capture(fixture))
    earlier = retained
    await run(fixture)
    assert len(retained_list) == 1 and retained_list[0] is not earlier
    assert len(retained_list[0]["readers"]) == 3


@pytest.mark.asyncio
async def test_preflight_native_retention_keeps_committed_missing_measurement(native_preflight):
    compact, fixture = native_preflight
    fixture[0].mode = "postcommit_sample"
    with pytest.raises(custody.OwnedWalTransactionError):
        await (compact_preflight(fixture) if compact else preflight(fixture))
    tracker = fixture[2].wal_tracker
    retained = (
        tracker.owned_compact_preflight_native_outcomes if compact else tracker.owned_evidence_preflight_native_outcomes
    )[0]
    assert retained["status"] == "incomplete" and not retained["reservation_refund"]
    assert any(
        reader["outcome"] is not None and reader["outcome"].is_committed and reader["outcome"].measurement is None
        for reader in retained["readers"]
    )


def _invalid_native_reader_inputs(build, phase, failure):
    """Alter identity inputs while preserving the original native outcomes."""
    readers = [list(entry) for entry in phase["native_readers"]]
    for reader in readers:
        reader[-1] = dict(reader[-1])
    current_build = build
    if failure == "stale_build":
        current_build = SimpleNamespace()
    elif failure == "duplicate":
        readers[1] = readers[0]
    elif failure == "late":
        # A hashable task-shaped fake; the real outcome itself is unchanged.
        readers[0][3] = Mock(done=Mock(return_value=False))
        readers[0][-1]["reader_task"] = readers[0][3]
    elif failure == "missing":
        readers[0][-1]["native_outcome"] = None
    else:
        readers[0][-1]["reader_window"] = (object(), "profile_stage")
    return current_build, readers


@pytest.mark.asyncio
async def test_native_preflight_rejects_stale_duplicate_late_missing_inputs(owned_compact_preflight, monkeypatch):
    arguments = []
    original = fhir._retain_profile_preflight_native_outcomes

    def retain(*args, **kwargs):
        arguments.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(fhir, "_retain_profile_preflight_native_outcomes", retain)
    await compact_preflight(owned_compact_preflight)
    args, kwargs = arguments[0]
    build, coordinates, admission, phase, stage = args
    for failure in ("stale_build", "duplicate", "late", "missing", "stale_window"):
        current_build, readers = _invalid_native_reader_inputs(build, phase, failure)
        original(current_build, coordinates, admission, {**phase, "native_readers": readers}, stage, **kwargs)
        assert admission.wal_tracker.owned_compact_preflight_native_outcomes[0]["status"] == "incomplete"
