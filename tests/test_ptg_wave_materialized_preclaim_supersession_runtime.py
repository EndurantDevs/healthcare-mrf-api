"""GET-only runtime and route tests for materialized-preclaim recovery."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import control_wave_routes as routes
from api import control_import_wave_materialized_preclaim as preclaim
from api import ptg_wave_kubernetes_attestation as kubernetes_attestation
from process import ptg_wave_materialized_preclaim_supersession_runtime as runtime
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from tests.test_ptg_wave_materialized_preclaim_supersession import (
    _attest,
    _materialized_wave,
)
from tests.test_ptg_wave_preclaim_supersession import (
    _actual_job,
    _empty_redis_attestation,
    _intents_and_runs,
)


class _Result:
    def __init__(self, value=None, values=()):
        self.value = value
        self.values = list(values)

    def scalar_one_or_none(self):
        return self.value

    def scalars(self):
        return self

    def all(self):
        return list(self.values)


class _ReadSession:
    def __init__(self, *results):
        self.results = list(results)
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return self.results.pop(0)


class _Context:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _WriteSession(_ReadSession):
    def __init__(self, *results):
        super().__init__(*results)
        self.added = []
        self.flush_count = 0

    def add(self, row):
        self.added.append(row)

    async def flush(self):
        self.flush_count += 1


class _RedisAttestation:
    def __init__(self, mapping):
        self.mapping = mapping

    def as_mapping(self):
        return self.mapping


class _RepeatedArguments(dict):
    def getlist(self, field_name):
        return [self[field_name], self[field_name]]


class _Blueprint:
    def __init__(self, registered_routes):
        self.registered_routes = registered_routes

    def listener(self, _name):
        return lambda function: function

    def get(self, path):
        return lambda function: self.registered_routes.append(
            ("GET", path, function)
        ) or function

    def post(self, path):
        return lambda function: self.registered_routes.append(
            ("POST", path, function)
        ) or function


def _snapshot(
    *,
    claims=(),
    outcomes=(),
    worker_events=(),
) -> runtime._MaterializedDatabaseSnapshot:
    wave = _materialized_wave()
    intents, runs = _intents_and_runs(wave)
    return runtime._MaterializedDatabaseSnapshot(
        wave=wave,
        intents=tuple(intents),
        runs=tuple(runs),
        claims=tuple(claims),
        outcomes=tuple(outcomes),
        worker_start_event_ordinals=tuple(worker_events),
        logical_supersession=wave.logical_recovery,
        admission_rollback=wave.rollback_recovery,
    )


@pytest.mark.asyncio
async def test_get_replays_stored_v5_proof_without_external_reads(monkeypatch):
    proof = _attest()
    session = _ReadSession(_Result(SimpleNamespace(
        recovery_basis="materialized_preclaim_failure",
        successor_wave_id="successor-wave",
        recovery_evidence=proof,
    )))
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))
    get_job = Mock(side_effect=AssertionError("replay must not read Kubernetes"))
    redis_attest = AsyncMock(
        side_effect=AssertionError("replay must not read Redis")
    )
    monkeypatch.setattr(runtime, "get_wave_job", get_job)
    monkeypatch.setattr(
        runtime,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        redis_attest,
    )

    observed = await runtime.get_materialized_preclaim_supersession_candidate(
        "materialized-wave",
        "successor-wave",
        redis=object(),
    )

    assert observed == proof
    get_job.assert_not_called()
    redis_attest.assert_not_awaited()
    assert len(session.statements) == 1


@pytest.mark.asyncio
async def test_fresh_get_observes_job_and_redis_once_without_dml(monkeypatch):
    snapshot = _snapshot()
    session = _ReadSession(_Result(None))
    monkeypatch.setattr(runtime.db, "session", lambda: _Context(session))
    loader = AsyncMock(return_value=snapshot)
    get_job = Mock(
        return_value=_actual_job(snapshot.wave.kubernetes_manifest)
    )
    restored_manifest = object()
    restore = Mock(return_value=restored_manifest)
    redis_attest = AsyncMock(return_value=_RedisAttestation(
        _empty_redis_attestation(snapshot.wave)
    ))
    monkeypatch.setattr(runtime, "_load_snapshot", loader)
    monkeypatch.setattr(runtime, "get_wave_job", get_job)
    monkeypatch.setattr(runtime, "restore_wave_manifest", restore)
    monkeypatch.setattr(
        runtime,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        redis_attest,
    )

    proof = await runtime.get_materialized_preclaim_supersession_candidate(
        "materialized-wave",
        "successor-wave",
        redis="redis-observer",
    )

    assert proof["successor_wave_id"] == "successor-wave"
    loader.assert_awaited_once_with(
        session,
        "materialized-wave",
        lock_rows=False,
    )
    get_job.assert_called_once_with(snapshot.wave.wave_digest)
    restore.assert_called_once()
    redis_attest.assert_awaited_once_with("redis-observer", restored_manifest)
    assert all(
        str(statement).lstrip().upper().startswith("SELECT")
        for statement in session.statements
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_source", ("job", "manifest", "redis"))
async def test_external_observer_failures_are_normalized(monkeypatch, failure_source):
    snapshot = _snapshot()
    if failure_source == "job":
        monkeypatch.setattr(
            runtime,
            "get_wave_job",
            Mock(side_effect=RuntimeError("Kubernetes unavailable")),
        )
    else:
        monkeypatch.setattr(
            runtime,
            "get_wave_job",
            Mock(return_value=_actual_job(snapshot.wave.kubernetes_manifest)),
        )
    if failure_source == "manifest":
        monkeypatch.setattr(
            runtime,
            "restore_wave_manifest",
            Mock(side_effect=RuntimeError("manifest drift")),
        )
    else:
        monkeypatch.setattr(runtime, "restore_wave_manifest", Mock(return_value=object()))
    redis_effect = (
        RuntimeError("Redis unavailable")
        if failure_source == "redis"
        else _RedisAttestation(_empty_redis_attestation(snapshot.wave))
    )
    monkeypatch.setattr(
        runtime,
        "attest_ptg_small_wave_unclaimed_failure_redis",
        AsyncMock(
            side_effect=redis_effect
            if isinstance(redis_effect, Exception)
            else None,
            return_value=(
                None if isinstance(redis_effect, Exception) else redis_effect
            ),
        ),
    )

    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="materialized preclaim observation failed",
    ) as exc_info:
        await runtime._observe(snapshot, "successor-wave", redis=object())
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert str(exc_info.value.__cause__) not in str(exc_info.value)


@pytest.mark.asyncio
async def test_locked_reobservation_rejects_existing_row_or_drift(monkeypatch):
    proof = _attest()
    existing = AsyncMock(return_value=SimpleNamespace())
    monkeypatch.setattr(runtime, "_supersession_row", existing)

    with pytest.raises(
        PTGWaveMaterializedPreclaimConflict,
        match="immutable supersession",
    ):
        await runtime.attest_locked_materialized_preclaim_supersession(
            object(),
            "materialized-wave",
            "successor-wave",
            proof,
            redis=object(),
        )

    snapshot = _snapshot()
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    monkeypatch.setattr(runtime, "_load_snapshot", AsyncMock(return_value=snapshot))
    drifted_proof_map = dict(proof)
    drifted_proof_map["proof_digest"] = "0" * 64
    monkeypatch.setattr(
        runtime,
        "_observe",
        AsyncMock(return_value=drifted_proof_map),
    )

    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="differs"):
        await runtime.attest_locked_materialized_preclaim_supersession(
            object(),
            "materialized-wave",
            "successor-wave",
            proof,
            redis=object(),
        )


@pytest.mark.asyncio
async def test_locked_reobservation_returns_the_matching_materialized_witness(
    monkeypatch,
):
    """The locked V5 handoff returns only its freshly matched witness."""

    proof = _attest()
    snapshot = _snapshot()
    monkeypatch.setattr(runtime, "_supersession_row", AsyncMock(return_value=None))
    loader = AsyncMock(return_value=snapshot)
    observe = AsyncMock(return_value=proof)
    monkeypatch.setattr(runtime, "_load_snapshot", loader)
    monkeypatch.setattr(runtime, "_observe", observe)

    observed = await runtime.attest_locked_materialized_preclaim_supersession(
        object(),
        "materialized-wave",
        "successor-wave",
        proof,
        redis="redis-observer",
    )

    assert observed == proof
    loader.assert_awaited_once_with(
        ANY,
        "materialized-wave",
        lock_rows=True,
    )
    observe.assert_awaited_once_with(
        snapshot,
        "successor-wave",
        redis="redis-observer",
    )


@pytest.mark.asyncio
async def test_locked_snapshot_reads_every_materialized_boundary_in_lock_order(
    monkeypatch,
):
    """V5 reobservation locks wave, work, and prior-recovery evidence together."""

    wave = SimpleNamespace(wave_id="materialized-wave")
    intent = SimpleNamespace(run_id="materialized-run")
    run = SimpleNamespace(run_id="materialized-run")
    claim = SimpleNamespace(ordinal=0)
    outcome = SimpleNamespace(ordinal=0)
    logical = SimpleNamespace(successor_wave_id="materialized-wave")
    rollback = SimpleNamespace(successor_wave_id="materialized-wave")
    session = _ReadSession(
        _Result(wave),
        _Result(None),
        _Result(values=(intent,)),
        _Result(values=(run,)),
        _Result(values=(claim,)),
        _Result(values=(outcome,)),
        _Result(logical),
        _Result(rollback),
    )
    worker_events = AsyncMock(return_value=(0,))
    monkeypatch.setattr(runtime, "_worker_start_event_ordinals", worker_events)

    snapshot = await runtime._load_snapshot(
        session,
        "materialized-wave",
        lock_rows=True,
    )

    assert snapshot == runtime._MaterializedDatabaseSnapshot(
        wave=wave,
        intents=(intent,),
        runs=(run,),
        claims=(claim,),
        outcomes=(outcome,),
        worker_start_event_ordinals=(0,),
        logical_supersession=logical,
        admission_rollback=rollback,
    )
    worker_events.assert_awaited_once_with(session, (intent,))
    assert len(session.statements) == 8
    assert all("FOR UPDATE" in str(statement) for statement in session.statements)


@pytest.mark.parametrize(
    ("template", "desired_template"),
    (
        ({"spec": None}, {"spec": {}}),
        ({"spec": {"containers": {}}}, {"spec": {"containers": []}}),
        ({"spec": {"containers": ["not-a-container"]}}, {"spec": {"containers": []}}),
    ),
)
def test_materialized_job_template_normalization_preserves_malformed_inputs(
    template,
    desired_template,
):
    """V5 Job evidence must not normalize malformed Kubernetes templates."""

    assert (
        kubernetes_attestation._normalize_kubernetes_defaulted_template(
            template,
            desired_template,
        )
        is template
    )


@pytest.mark.parametrize(
    ("volumes", "desired_volumes"),
    (
        ({}, []),
        ([{"name": "worker"}], []),
        (["not-a-volume"], [{"name": "worker"}]),
    ),
)
def test_materialized_job_volume_normalization_preserves_noncanonical_shapes(
    volumes,
    desired_volumes,
):
    """A V5 Job receipt only permits the one bounded default-mode rewrite."""

    assert kubernetes_attestation._normalize_kubernetes_defaulted_volumes(
        volumes,
        desired_volumes,
    ) is volumes


@pytest.mark.parametrize("environment", ({}, ["not-an-entry"]))
def test_materialized_job_environment_normalization_preserves_noncanonical_shapes(
    environment,
):
    """Malformed environment entries remain visible to the V5 Job comparator."""

    normalized = kubernetes_attestation._normalize_kubernetes_defaulted_environment(
        environment
    )

    assert normalized == environment
    if not isinstance(environment, list):
        assert normalized is environment


@pytest.mark.asyncio
async def test_materialized_route_is_get_only_and_requires_one_exact_query(monkeypatch):
    """Register only GET and reject missing, extra, or repeated fields."""

    registered_routes = []
    routes.register_control_wave_routes(_Blueprint(registered_routes))
    route_path = "/import-waves/<wave_id>/materialized-preclaim-supersession"
    assert ("GET", route_path) in [
        (method, path) for method, path, _function in registered_routes
    ]
    assert not any(
        method == "POST" and path == route_path
        for method, path, _function in registered_routes
    )

    candidate = AsyncMock(return_value={"proof_digest": "a" * 64})
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_materialized_preclaim_supersession_candidate",
        candidate,
    )
    request = SimpleNamespace(
        args={"successor_wave_id": "successor-wave"},
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis="redis")),
    )

    response = await routes.control_get_materialized_preclaim_supersession(
        request,
        "materialized-wave",
    )

    assert response.status == 200
    candidate.assert_awaited_once_with(
        "materialized-wave",
        "successor-wave",
        redis="redis",
    )
    for invalid_arguments in (
        {},
        {"successor_wave_id": "successor-wave", "extra": "x"},
        _RepeatedArguments(successor_wave_id="successor-wave"),
    ):
        with pytest.raises(BadRequest):
            await routes.control_get_materialized_preclaim_supersession(
                SimpleNamespace(args=invalid_arguments, app=request.app),
                "materialized-wave",
            )


@pytest.mark.asyncio
async def test_materialized_route_maps_observation_drift_to_conflict(monkeypatch):
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "get_materialized_preclaim_supersession_candidate",
        AsyncMock(side_effect=PTGWaveMaterializedPreclaimConflict("drift")),
    )
    request = SimpleNamespace(
        args={"successor_wave_id": "successor-wave"},
        app=SimpleNamespace(ctx=SimpleNamespace(ptg_wave_redis=object())),
    )

    with pytest.raises(SanicException) as exc_info:
        await routes.control_get_materialized_preclaim_supersession(
            request,
            "materialized-wave",
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_runtime_internal_read_guards_cover_locked_and_invalid_paths(monkeypatch):
    snapshot = _snapshot()
    loaded = await runtime._load_predecessor_wave(
        _ReadSession(_Result(snapshot.wave), _Result(None)),
        "materialized-wave",
        lock_rows=True,
    )
    assert loaded is snapshot.wave

    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="missing"):
        await runtime._load_predecessor_wave(
            _ReadSession(_Result(None), _Result(None)),
            "materialized-wave",
            lock_rows=False,
        )

    session = _ReadSession(
        _Result(values=snapshot.intents),
        _Result(values=snapshot.runs),
        _Result(values=()),
        _Result(values=()),
    )
    monkeypatch.setattr(
        runtime,
        "_worker_start_event_ordinals",
        AsyncMock(return_value=()),
    )
    work_rows = await runtime._load_work_rows(
        session, "materialized-wave", lock_rows=True
    )
    assert work_rows[-1] == ()

    monkeypatch.setattr(
        runtime,
        "_worker_start_event_ordinals",
        AsyncMock(side_effect=RuntimeError("synthetic worker event failure")),
    )
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="worker events"):
        await runtime._load_work_rows(
            _ReadSession(_Result(values=()), _Result(values=()), _Result(values=()), _Result(values=())),
            "materialized-wave",
            lock_rows=False,
        )


@pytest.mark.asyncio
async def test_runtime_observation_and_storage_guards_reject_absent_or_conflicting_state(monkeypatch):
    snapshot = _snapshot()
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="Redis observer"):
        await runtime._observe(snapshot, "successor-wave", redis=None)

    monkeypatch.setattr(runtime, "get_wave_job", Mock(return_value=None))
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="Job is unavailable"):
        await runtime._observe(snapshot, "successor-wave", redis=object())

    locked_row = await runtime._supersession_row(
        _ReadSession(_Result(None)), "materialized-wave", lock_row=True
    )
    assert locked_row is None

    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="another recovery"):
        runtime._existing_proof(
            SimpleNamespace(recovery_basis="other", successor_wave_id="successor-wave"),
            predecessor_wave_id="materialized-wave",
            successor_wave_id="successor-wave",
        )
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="invalid"):
        runtime._wave_id(" ", "synthetic wave ID")


@pytest.mark.asyncio
async def test_materialized_preclaim_persistence_covers_empty_and_attested_requests(monkeypatch):
    no_proof_session = _WriteSession()
    await preclaim.persist_materialized_preclaim_supersession(
        no_proof_session,
        {"wave_id": "successor-wave"},
        now=dt.datetime.now(dt.UTC),
        redis=object(),
    )
    assert no_proof_session.added == []

    proof = _attest()
    session = _WriteSession()
    monkeypatch.setattr(
        preclaim,
        "attest_locked_materialized_preclaim_supersession",
        AsyncMock(return_value=proof),
    )
    await preclaim.persist_materialized_preclaim_supersession(
        session,
        {"wave_id": "successor-wave", "materialized_preclaim_supersession": proof},
        now=dt.datetime.now(dt.UTC),
        redis=object(),
    )
    assert session.flush_count == 2
    assert len(session.added) == 2


@pytest.mark.asyncio
async def test_materialized_preclaim_persistence_read_and_validation_boundaries(monkeypatch):
    retired = await preclaim.is_materialized_preclaim_retired(
        _ReadSession(_Result("retired-wave")),
        "retired-wave",
        lock_row=True,
    )
    assert retired is True

    monkeypatch.setattr(
        preclaim,
        "is_materialized_preclaim_retired",
        AsyncMock(return_value=True),
    )
    with pytest.raises(PTGWaveMaterializedPreclaimConflict, match="retired"):
        await preclaim.require_materialized_preclaim_replay_allowed(
            object(), "retired-wave"
        )

    assert preclaim.validate_materialized_preclaim_supersession(
        {"schema_version": "older"}, wave_id="successor-wave"
    ) is None
