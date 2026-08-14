# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")


def _sample(
    wal_lsn: str,
    *,
    wal_bytes: int = 0,
    temp_bytes: int = 100,
    relation_bytes: int = 1_000,
):
    return {
        "wal_lsn": wal_lsn,
        "wal_bytes": wal_bytes,
        "temp_bytes": temp_bytes,
        "relation_bytes": {"stage": relation_bytes},
    }


def _logged_observations(caplog):
    return [
        json.loads(record.message)
        for record in caplog.records
        if "profile-clone-capacity-observation.v1" in record.message
    ]


@pytest.mark.asyncio
async def test_clone_capacity_observation_reports_partial_counter_deltas(
    monkeypatch,
    caplog,
):
    sampler = AsyncMock(
        side_effect=(
            _sample("0/10"),
            _sample(
                "0/40",
                wal_bytes=48,
                temp_bytes=132,
                relation_bytes=1_120,
            ),
        )
    )
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_PROFILE_CLONE_CAPACITY_OBSERVATION_ENV,
        "1",
    )
    monkeypatch.setattr(
        importer, "_profile_capacity_observation_sample", sampler
    )
    monkeypatch.setattr(
        importer,
        "time",
        SimpleNamespace(monotonic=Mock(side_effect=(10.0, 12.5))),
    )
    caplog.set_level(logging.INFO, logger=importer.__name__)

    async with importer._observe_profile_capacity_wave(
        "evidence",
        {"evidence_stage": '"fixture"."evidence_stage"'},
        coordinate_by_field={"batch_start": 0, "batch_end": 1},
    ):
        await asyncio.sleep(0)

    observation_by_field = _logged_observations(caplog)[0]
    assert observation_by_field["status"] == "succeeded"
    assert observation_by_field["elapsed_seconds"] == 2.5
    assert observation_by_field["measurement_status"] == "partial"
    assert observation_by_field["wal_observation"]["bytes"] == 48
    assert observation_by_field["relation_observation"]["growth"] == {"stage": 120}
    assert observation_by_field["temp_observation"]["observed_delta"] == 32
    assert observation_by_field["temp_observation"]["status"] == (
        "delayed_database_aggregate"
    )
    assert caplog.records[-1].message == json.dumps(
        observation_by_field, sort_keys=True, separators=(",", ":")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_type", (RuntimeError, asyncio.CancelledError))
async def test_clone_capacity_observation_preserves_operation_failure(
    monkeypatch,
    caplog,
    failure_type,
):
    sampler = AsyncMock(return_value=_sample("0/10"))
    monkeypatch.setenv(
        importer.PROVIDER_DIRECTORY_PROFILE_CLONE_CAPACITY_OBSERVATION_ENV,
        "1",
    )
    monkeypatch.setattr(
        importer, "_profile_capacity_observation_sample", sampler
    )
    caplog.set_level(logging.INFO, logger=importer.__name__)
    expected = failure_type("profile wave failed")

    with pytest.raises(failure_type) as raised:
        async with importer._observe_profile_capacity_wave(
            "compact",
            {"profile_stage": '"fixture"."profile_stage"'},
            coordinate_by_field={"batch_start": 0, "batch_end": 1},
        ):
            raise expected

    assert raised.value is expected
    sampler.assert_awaited_once()
    observation_by_field = _logged_observations(caplog)[0]
    assert observation_by_field["status"] == "failed"
    assert observation_by_field["measurement_status"] == "incomplete"
    assert observation_by_field["failure_type"] == failure_type.__name__


@pytest.mark.asyncio
async def test_clone_capacity_observation_is_inert_when_disabled(monkeypatch):
    sampler = AsyncMock(side_effect=AssertionError("unexpected sample"))
    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_CLONE_CAPACITY_OBSERVATION",
        raising=False,
    )
    monkeypatch.setattr(
        importer, "_profile_capacity_observation_sample", sampler,
        raising=False,
    )

    async with importer._observe_profile_capacity_wave(
        "artifact",
        {"scope": '"fixture"."scope"'},
        coordinate_by_field={"wave": 1},
    ):
        await asyncio.sleep(0)

    sampler.assert_not_awaited()


@pytest.mark.parametrize(
    ("function", "phase", "coordinates"),
    (
        (importer._materialize_artifact_scope_payload, "artifact", ("wave",)),
        (importer._materialize_artifact_resource_waves, "artifact", ("wave", "job_start")),
        (importer._run_profile_evidence_window, "evidence", ("batch_start", "batch_end")),
        (importer._populate_affected_npi_stage, "affected_npi", ("wave",)),
        (importer._populate_provider_directory_profile_compact_stage, "compact", ("batch_start", "batch_end")),
        (importer._apply_provider_directory_profile_delta, "target", ("wave",)),
    ),
)
def test_clone_capacity_observes_only_existing_write_waves(
    function,
    phase,
    coordinates,
):
    source = inspect.getsource(function)
    assert source.count("_observe_profile_capacity_wave(") == 1
    assert f'"{phase}"' in source
    assert all(f'"{coordinate}"' in source for coordinate in coordinates)
    if phase == "evidence":
        source = inspect.getsource(
            importer._execute_bounded_profile_evidence_plan
        )
        assert source.index("_preflight_profile") < source.index(
            "_run_profile_evidence_window("
        )
    elif phase == "compact":
        assert source.index("_preflight_profile") < source.index(
            "_observe_profile_capacity_wave("
        )
