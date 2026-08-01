"""Branch contracts for legacy-V3 lineage and operational absence."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from process.ptg_parts import ptg2_legacy_v3_metadata_lineage as lineage
from process.ptg_parts import ptg2_legacy_v3_operational_absence as absence


_SOURCE_IMPORT_ID = "synthetic-source-import-v3-coverage"
_SNAPSHOT_ID = "ptg2:202607:synthetic-v3-coverage"
_OUTER_RUN_ID = "run_synthetic_v3_coverage"


def _source_identity() -> dict[str, Any]:
    return {
        "source_file_import_id": _SOURCE_IMPORT_ID,
        "import_id": _SOURCE_IMPORT_ID,
        "params": {
            "source_file_import_id": _SOURCE_IMPORT_ID,
            "import_id": _SOURCE_IMPORT_ID,
        },
    }


def _outer_run(
    run_id: str = _OUTER_RUN_ID,
    *,
    retry_of_run_id: str | None = None,
) -> dict[str, Any]:
    return {
        **_source_identity(),
        "run_id": run_id,
        "importer": "ptg",
        "status": "failed",
        "retry_of_run_id": retry_of_run_id,
        "finished_at": "2026-07-01T00:03:00+00:00",
    }


def _mirror(run: dict[str, Any]) -> dict[str, Any]:
    return {
        **_source_identity(),
        "run_id": run["run_id"],
        "importer": "ptg",
        "status": run["status"],
        "finished_at": run["finished_at"],
    }


def _observation(
    runs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    run_rows = runs or [_outer_run()]
    target = run_rows[-1]
    return {
        "outer_runs": copy.deepcopy(run_rows),
        "outer_target": copy.deepcopy(target),
        "control_run_mirrors": [_mirror(run) for run in run_rows],
        "source_import_rows": [
            {
                "payload": {
                    "source_file_import_id": _SOURCE_IMPORT_ID,
                    "status": "failed",
                    "engine_run_id": target["run_id"],
                    "snapshot_id": _SNAPSHOT_ID,
                    "removed_at": None,
                }
            }
        ],
        "event_rows": [],
        "placement_rows": [],
    }


def _lineage_reasons(observation: dict[str, Any]) -> set[str]:
    return set(
        lineage.legacy_v3_outer_lineage_reasons(
            observation,
            outer_run_id=_OUTER_RUN_ID,
            source_file_import_id=_SOURCE_IMPORT_ID,
            snapshot_id=_SNAPSHOT_ID,
        )
    )


@pytest.mark.asyncio
async def test_close_redis_accepts_missing_sync_and_async_close() -> None:
    class NoClose:
        pass

    class SyncClose:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class AsyncClose:
        def __init__(self) -> None:
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    sync_redis = SyncClose()
    async_redis = AsyncClose()
    await absence._close_redis(NoClose())
    await absence._close_redis(sync_redis)
    await absence._close_redis(async_redis)
    assert sync_redis.closed is True
    assert async_redis.closed is True


def test_operational_identities_are_exact_and_deduplicated() -> None:
    outer_runs = [
        {"run_id": "", "params": "not-a-mapping"},
        {
            "run_id": "run-a",
            "status": "failed",
            "params": {"import_id": "import-from-params"},
            "metrics": {"job_id": "recorded-job"},
        },
        {
            "run_id": "run-a",
            "status": "failed",
            "import_id": "import-from-row",
            "metrics": "not-a-mapping",
        },
    ]
    event_rows = [
        {"outer_run_id": "run-a", "attempt_id": "recorded-job"},
        {"outer_run_id": "run-b", "attempt_id": "event-job"},
        {"outer_run_id": "", "attempt_id": ""},
    ]

    run_rows = absence._operational_run_rows(outer_runs, event_rows)
    assert run_rows == [outer_runs[-1], {"run_id": "run-b"}]
    assert absence._operational_job_ids(
        [{"run_id": "", "metrics": {}}, *run_rows, outer_runs[1]],
        event_rows,
    ) == [
        "event-job",
        "ptg_start_run-a",
        "ptg_start_run-b",
        "recorded-job",
    ]
    assert absence._worker_identity_payload(
        outer_runs[0], worker_class="process.PTG"
    ) == {
        "importer": "ptg",
        "worker_class": "process.PTG",
    }
    assert absence._worker_identity_payload(
        outer_runs[1], worker_class="process.PTGSmall"
    ) == {
        "run_id": "run-a",
        "importer": "ptg",
        "status": "failed",
        "worker_class": "process.PTGSmall",
        "import_id": "import-from-params",
    }


@pytest.mark.asyncio
async def test_redis_operational_counts_use_only_exact_keys(
    monkeypatch,
) -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self.closed = False

        async def zscore(self, queue_name: str, job_id: str) -> int | None:
            if queue_name == "arq:PTG" and job_id == "job-a":
                return 0
            return None

        async def exists(self, *keys: str) -> int:
            return int("arq:job:job-b" in keys)

        async def aclose(self) -> None:
            self.closed = True

    redis = FakeRedis()

    async def create_pool(*_args, **_kwargs):
        return redis

    monkeypatch.setattr(absence, "create_pool", create_pool)
    monkeypatch.setattr(absence, "build_redis_settings", object)

    assert await absence._redis_operational_counts(["job-a", "job-b"]) == (
        1,
        1,
    )
    assert redis.closed is True


@pytest.mark.asyncio
async def test_worker_operational_counts_distinguish_presence_and_running(
    monkeypatch,
) -> None:
    state_by_worker = {
        "process.PTG": None,
        "process.PTGSmall": {"items": "not-a-list"},
        "process.PTGNormal": {
            "items": [{"running": True, "job_status": "running"}]
        },
        "process.PTGLarge": {
            "items": [{"running": False, "job_status": "missing"}]
        },
        "process.PTGHuge": {
            "items": [{"running": False, "job_status": "complete"}]
        },
        "process.PTGCandidateAudit": {
            "items": [{"running": True, "job_status": None}]
        },
    }

    def worker_state(worker_payload: dict[str, Any]):
        return state_by_worker[worker_payload["worker_class"]]

    async def to_thread(function, *args):
        return function(*args)

    monkeypatch.setattr(absence, "worker_state", worker_state)
    monkeypatch.setattr(absence.asyncio, "to_thread", to_thread)

    assert await absence._worker_operational_counts([_outer_run()]) == (2, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("redis_counts", "worker_counts", "expected_absence"),
    (
        ((0, 0), (0, 0), True),
        ((1, 0), (0, 0), False),
    ),
)
async def test_kubernetes_absence_requires_every_exact_probe_to_be_empty(
    monkeypatch,
    redis_counts: tuple[int, int],
    worker_counts: tuple[int, int],
    expected_absence: bool,
) -> None:
    async def redis_probe(_job_ids):
        return redis_counts

    async def worker_probe(_run_rows):
        return worker_counts

    monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
    monkeypatch.setattr(absence, "_redis_operational_counts", redis_probe)
    monkeypatch.setattr(absence, "_worker_operational_counts", worker_probe)

    evidence = await absence.load_exact_operational_absence(
        [_outer_run()],
        None,
    )
    assert evidence["worker_probe_supported"] is True
    assert evidence["exact_external_absence"] is expected_absence
    assert evidence["queue_memberships"] == redis_counts[0]
    assert evidence["redis_exact_key_count"] == redis_counts[1]


def test_payload_envelopes_fail_closed() -> None:
    assert lineage._payload(None) == {}
    assert lineage._payload({"payload": "not-a-mapping"}) == {}
    assert lineage._payload({"payload": {"status": "failed"}}) == {
        "status": "failed"
    }
    assert lineage._exact_source_identity(
        {
            "source_file_import_id": _SOURCE_IMPORT_ID,
            "import_id": "conflicting-source-import",
        }
    ) == (None, True)

    missing_target = _observation()
    missing_target["outer_target"] = None
    assert "outer_target_missing" in _lineage_reasons(missing_target)


def test_exact_retry_chain_and_mirrors_are_accepted() -> None:
    previous_run_id = "run_synthetic_v3_coverage_previous"
    observation = _observation(
        [
            _outer_run(previous_run_id),
            _outer_run(_OUTER_RUN_ID, retry_of_run_id=previous_run_id),
        ]
    )
    assert _lineage_reasons(observation) == set()


def _invalid_lineage_observation() -> dict[str, Any]:
    conflicting_identity_by_field = {
        "source_file_import_id": _SOURCE_IMPORT_ID,
        "import_id": "conflicting-source-import",
    }
    observation_by_field = _observation()
    observation_by_field["outer_runs"] = [
        {
            **conflicting_identity_by_field,
            "run_id": "",
            "importer": "not-ptg",
            "status": "running",
            "finished_at": None,
            "snapshot_id": "ptg2:202607:other-snapshot",
        }
    ]
    observation_by_field["outer_target"] = {
        **conflicting_identity_by_field,
        "run_id": "run-other",
        "importer": "not-ptg",
        "status": "running",
        "finished_at": None,
    }
    observation_by_field["control_run_mirrors"] = [
        {
            **conflicting_identity_by_field,
            "run_id": "run-other",
            "importer": "not-ptg",
            "status": "running",
            "finished_at": None,
            "snapshot_id": "ptg2:202607:other-snapshot",
        }
    ]
    observation_by_field["source_import_rows"] = [
        {
            "payload": {
                "source_file_import_id": "other-source-import",
                "status": "planned",
                "engine_run_id": "run-other",
                "snapshot_id": "ptg2:202607:other-snapshot",
                "removed_at": "2026-07-01T00:04:00+00:00",
            }
        }
    ]
    observation_by_field["event_rows"] = None
    observation_by_field["placement_rows"] = [{"path": "TEST_ONLY"}]
    return observation_by_field


def test_invalid_outer_mirror_source_and_event_views_fail_closed() -> None:
    """Reject divergent outer, mirror, source, event, and placement views."""

    assert {
        "outer_run_cardinality_changed",
        "outer_source_importer_changed",
        "outer_source_identity_changed",
        "outer_source_attempt_not_failed",
        "outer_source_attempt_not_finished",
        "outer_source_snapshot_changed",
        "outer_retry_lineage_not_single_chain",
        "outer_target_not_retry_leaf",
        "outer_target_identity_changed",
        "outer_target_not_ptg",
        "outer_target_not_failed",
        "outer_target_not_finished",
        "outer_target_source_changed",
        "control_mirror_lineage_changed",
        "control_mirror_importer_changed",
        "control_mirror_source_changed",
        "control_mirror_not_failed",
        "control_mirror_status_changed",
        "control_mirror_not_finished",
        "control_mirror_snapshot_changed",
        "source_import_lineage_changed",
        "source_import_not_failed",
        "source_event_lineage_changed",
        "file_placement_present",
    }.issubset(_lineage_reasons(_invalid_lineage_observation()))


@pytest.mark.parametrize(
    ("run_rows", "outer_run_id", "expected_reasons"),
    (
        (
            [
                {"run_id": "run-a"},
                {"run_id": "run-b", "retry_of_run_id": "run-a"},
            ],
            "run-b",
            set(),
        ),
        (
            [{"run_id": "run-a", "retry_of_run_id": "run-missing"}],
            "run-a",
            {
                "outer_retry_lineage_incomplete",
                "outer_retry_lineage_not_single_chain",
            },
        ),
        (
            [
                {"run_id": "run-a"},
                {"run_id": "run-b", "retry_of_run_id": "run-a"},
                {"run_id": "run-c", "retry_of_run_id": "run-a"},
            ],
            "run-c",
            {
                "outer_retry_lineage_not_single_chain",
                "outer_target_not_retry_leaf",
            },
        ),
        (
            [
                {"run_id": "run-a", "retry_of_run_id": "run-b"},
                {"run_id": "run-b", "retry_of_run_id": "run-a"},
            ],
            "run-b",
            {
                "outer_retry_lineage_not_single_chain",
                "outer_target_not_retry_leaf",
            },
        ),
    ),
)
def test_retry_lineage_is_complete_linear_acyclic_and_target_terminated(
    run_rows: list[dict[str, Any]],
    outer_run_id: str,
    expected_reasons: set[str],
) -> None:
    run_ids = {run["run_id"] for run in run_rows}
    assert set(
        lineage._retry_lineage_reasons(
            run_rows,
            run_ids=run_ids,
            outer_run_id=outer_run_id,
        )
    ) == expected_reasons


def test_source_control_shapes_and_cardinality_fail_closed() -> None:
    missing_source_import = _observation()
    missing_source_import["source_import_rows"] = "not-a-list"
    assert "source_import_cardinality_changed" in _lineage_reasons(
        missing_source_import
    )

    malformed_events = _observation()
    malformed_events["event_rows"] = [None]
    assert "source_event_lineage_changed" in _lineage_reasons(
        malformed_events
    )

    wrong_event = _observation()
    wrong_event["event_rows"] = [
        {
            "outer_run_id": " run-with-whitespace ",
            "attempt_id": "job-synthetic",
        }
    ]
    assert "source_event_outer_lineage_changed" in _lineage_reasons(
        wrong_event
    )
