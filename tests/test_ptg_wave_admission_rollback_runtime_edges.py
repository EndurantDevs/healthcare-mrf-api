"""Fail-closed edge coverage for admission-rollback observations."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_wave_routes
from process import ptg_wave_admission_rollback_supersession_runtime as runtime
from process.ptg_wave_admission_rollback_supersession import (
    DATABASE_FIELDS,
    PTGWaveAdmissionRollbackConflict,
)
from tests.test_ptg_wave_admission_rollback_supersession_runtime import (
    _Redis,
    _proof,
    _stored_retirement,
)


class _ScalarRows:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self.rows


class _ScalarRow:
    def __init__(self, row):
        self.row = row

    def scalar_one_or_none(self):
        return self.row


@pytest.mark.asyncio
async def test_locked_reobservation_returns_exact_proof_and_rejects_drift(
    monkeypatch,
):
    proof = _proof()
    predecessor = proof["predecessor"]
    session = object()
    monkeypatch.setattr(runtime, "_retirement_row", AsyncMock(return_value=None))
    monkeypatch.setattr(
        runtime,
        "_database_absence_observation",
        AsyncMock(return_value={name: 0 for name in DATABASE_FIELDS}),
    )
    monkeypatch.setattr(
        runtime,
        "_external_absence_observation",
        AsyncMock(return_value=(proof["kubernetes"], proof["redis"])),
    )

    observed_proof = await runtime.attest_locked_admission_rollback_supersession(
        session,
        predecessor,
        "successor-wave",
        proof,
        redis=object(),
    )
    assert observed_proof == proof

    drifted_proof_map = dict(proof, proof_digest="0" * 64)
    monkeypatch.setattr(
        runtime,
        "build_admission_rollback_supersession_proof",
        Mock(return_value=drifted_proof_map),
    )
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="differs from current observation",
    ):
        await runtime.attest_locked_admission_rollback_supersession(
            session,
            predecessor,
            "successor-wave",
            proof,
            redis=object(),
        )


@pytest.mark.asyncio
async def test_retirement_collision_query_honors_lock_and_cardinality():
    proof = _proof()
    request = proof["predecessor"]
    retirement_row = _stored_retirement(proof)
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=(
                _ScalarRows([retirement_row]),
                _ScalarRows([retirement_row]),
            )
        )
    )

    assert await runtime.find_admission_retirement_collision(
        session,
        request,
        lock_row=False,
    ) is retirement_row
    assert await runtime.find_admission_retirement_collision(
        session,
        request,
        lock_row=True,
    ) is retirement_row
    unlocked_statement = session.execute.await_args_list[0].args[0]
    locked_statement = session.execute.await_args_list[1].args[0]
    assert "FOR UPDATE" not in str(unlocked_statement)
    assert "FOR UPDATE" in str(locked_statement)

    collision_session = SimpleNamespace(
        execute=AsyncMock(return_value=_ScalarRows([retirement_row, object()]))
    )
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="different tombstones",
    ):
        await runtime.find_admission_retirement_collision(
            collision_session,
            request,
        )


@pytest.mark.asyncio
async def test_database_observer_rejects_invalid_rows_and_schema_drift(
    monkeypatch,
):
    proof = _proof()
    observation_by_field = {name: 0 for name in DATABASE_FIELDS}
    observation_by_field["wave_id_count"] = "not-an-integer"
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(one=lambda: observation_by_field)
        )
    )
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="database observation is invalid",
    ):
        await runtime._database_absence_observation(
            session,
            proof["predecessor"],
        )

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setenv("DB_SCHEMA", "other")
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="schema configuration is inconsistent",
    ):
        runtime._database_tables_by_name()


@pytest.mark.asyncio
async def test_external_observer_requires_redis_and_maps_kubernetes_failures(
    monkeypatch,
):
    predecessor = _proof()["predecessor"]
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="requires the exact-wave Redis observer",
    ):
        await runtime._external_absence_observation(predecessor, redis=None)

    monkeypatch.setattr(
        runtime,
        "wave_absence_observation",
        Mock(side_effect=runtime.PTGWaveContractError("synthetic drift")),
    )
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="Kubernetes observation failed",
    ):
        await runtime._external_absence_observation(
            predecessor,
            redis=object(),
        )


@pytest.mark.asyncio
async def test_external_observer_projects_exact_absence(monkeypatch):
    proof = _proof()
    predecessor = proof["predecessor"]
    monkeypatch.setattr(
        runtime,
        "wave_absence_observation",
        Mock(return_value={"job_absent": True, "pod_count": 0}),
    )
    redis_observer = AsyncMock(return_value=proof["redis"])
    monkeypatch.setattr(runtime, "_redis_absence_observation", redis_observer)

    kubernetes_map, redis_map = await runtime._external_absence_observation(
        predecessor,
        redis="redis-observer",
    )

    assert kubernetes_map == proof["kubernetes"]
    assert redis_map == proof["redis"]
    redis_observer.assert_awaited_once_with("redis-observer", predecessor)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_values",
    (
        "not-a-sequence",
        [0],
        [True, 0, None, None],
        [0, False, None, None],
    ),
)
async def test_redis_observer_rejects_malformed_atomic_reads(raw_values):
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="Redis observation is invalid",
    ):
        await runtime._redis_absence_observation(
            _Redis(raw_values),
            _proof()["predecessor"],
        )


@pytest.mark.asyncio
async def test_retirement_lookup_honors_lock_and_descriptor_binding():
    proof = _proof()
    retirement_row = _stored_retirement(proof)
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=(
                _ScalarRow(retirement_row),
                _ScalarRow(retirement_row),
            )
        )
    )

    assert await runtime._retirement_row(
        session,
        retirement_row.predecessor_wave_id,
        lock_row=False,
    ) is retirement_row
    assert await runtime._retirement_row(
        session,
        retirement_row.predecessor_wave_id,
        lock_row=True,
    ) is retirement_row
    assert "FOR UPDATE" not in str(session.execute.await_args_list[0].args[0])
    assert "FOR UPDATE" in str(session.execute.await_args_list[1].args[0])

    retirement_row.predecessor_request_digest = "b" * 64
    with pytest.raises(
        PTGWaveAdmissionRollbackConflict,
        match="descriptor conflicts",
    ):
        runtime._validated_existing_retirement(
            retirement_row,
            proof["predecessor"],
            "successor-wave",
        )


def test_single_query_argument_accepts_one_multidict_value():
    arguments = SimpleNamespace(getlist=lambda _field: ["one-value"])
    assert control_wave_routes._single_query_argument(
        arguments,
        "field",
    ) == "one-value"
