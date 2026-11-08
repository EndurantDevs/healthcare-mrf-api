# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text, update

from api import control_imports
from api import control_workers as workers
from api import provider_directory_source_worker as source
from db.models import ImportRun, db
from process import control_lifecycle
from process.control_lifecycle import mark_control_run
from tests.control_imports_cancel_race_support import (
    finish_cancel_race_run,
    insert_queued_cancel_race_run,
)
from tests.test_provider_directory_source_worker_launch import (
    JOB_ID,
    ORDINARY_IMAGE,
    PROFILE_IMAGE,
    RUN_ID,
    SPEC,
    _signed_profile_run,
)
from tests.test_provider_directory_source_worker_launch import (
    original_job_before_default_change as original_job_before_default_change,
)
from tests.test_provider_directory_source_worker_launch import secret_selection as secret_selection
from tests.test_provider_directory_source_worker_launch import source_runtime_mount as source_runtime_mount

pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.filterwarnings("ignore:coroutine 'Connection._cancel' was never awaited:RuntimeWarning"),
]


def _require_disposable_database() -> None:
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database.rsplit("/", 1)[-1].lower():
        pytest.skip("DB-backed control tests require HLTHPRT_DB_DATABASE to contain 'test'")


async def _reset_import_run_schema() -> None:
    _require_disposable_database()
    await db.disconnect()
    await asyncio.sleep(0)
    try:
        await db.connect()
    except Exception as exc:
        pytest.skip(f"Postgres is not available for DB-backed control tests: {exc}")
    schema = ImportRun.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if not schema.replace("_", "").isalnum():
        raise AssertionError(f"unsafe schema name for test cleanup: {schema!r}")
    assert db.engine is not None
    async with db.engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        await conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{ImportRun.__tablename__}" CASCADE'))
    control_imports._IMPORT_RUN_ENSURE_STATE.ensured = False
    await control_imports.ensure_import_run_table()


async def _drop_import_run_schema() -> None:
    schema = ImportRun.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if db.engine is not None:
        async with db.engine.begin() as conn:
            await conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{ImportRun.__tablename__}" CASCADE'))
    await db.disconnect()
    await asyncio.sleep(0)
    control_imports._IMPORT_RUN_ENSURE_STATE.ensured = False


async def _fake_enqueue(row: dict) -> dict:
    return {
        "status": "queued",
        "phase_detail": "enqueued",
        "heartbeat_at": control_imports.utc_now(),
        "progress": {"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"},
        "metrics": {"enqueue_adapter": "arq_single_job", "queue": f"arq:{row['importer'].upper()}"},
        "error": None,
    }


def _acquisition_params(source_id: str, endpoint_scope: str, **overrides) -> dict:
    params_by_name = {
        "import_resources": True,
        "source_ids": [source_id],
        "source_concurrency": 1,
        "provider_directory_endpoint_scope": endpoint_scope,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
    }
    params_by_name.update(overrides)
    return params_by_name


def _artifact_params(source_id: str, **overrides) -> dict:
    params_by_name = {
        "publish_artifacts_only": True,
        "source_ids": [source_id],
        "import_resources": False,
        "canonical_backfill_only": False,
        "contact_backfill_only": False,
        "seed_only": False,
        "stale_cleanup": False,
        "publish_after_acquisition": False,
        "publish_artifacts": False,
    }
    params_by_name.update(overrides)
    return params_by_name


async def _create_fhir_run(run_id: str, params: dict) -> tuple[dict, bool]:
    return await control_imports.create_import_run(
        {
            "run_id": run_id,
            "importer": "provider-directory-fhir",
            "params": params,
        }
    )


async def _assert_fhir_blocked(run_id: str, params: dict, expected_run_id: str) -> None:
    blocked_run, created = await _create_fhir_run(run_id, params)
    assert created is False
    assert blocked_run["run_id"] == expected_run_id


async def test_fhir_admission_matrix(monkeypatch):
    await _reset_import_run_schema()
    try:
        monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_ACTIVE", "2")
        monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
        first_acquisition, first_created = await _create_fhir_run(
            "run_acquisition_one",
            _acquisition_params("pdfhir_one", "https://one.example.org/fhir"),
        )
        artifact_run, artifact_created = await _create_fhir_run(
            "run_artifact",
            _artifact_params("pdfhir_artifact"),
        )
        await _assert_fhir_blocked(
            "run_artifact_overlap",
            _artifact_params("pdfhir_one"),
            "run_acquisition_one",
        )
        await _assert_fhir_blocked(
            "run_acquisition_artifact_overlap",
            _acquisition_params("pdfhir_artifact", "https://artifact-overlap.example.org/fhir"),
            "run_artifact",
        )
        await _assert_fhir_blocked(
            "run_acquisition_overlap",
            _acquisition_params("pdfhir_one", "https://acquisition-overlap.example.org/fhir"),
            "run_acquisition_one",
        )
        second_acquisition, second_created = await _create_fhir_run(
            "run_acquisition_two",
            _acquisition_params("pdfhir_two", "https://two.example.org/fhir"),
        )

        await _assert_fhir_blocked(
            "run_artifact_two",
            _artifact_params("pdfhir_other"),
            "run_artifact",
        )
        await _assert_fhir_blocked(
            "run_mixed",
            _artifact_params("pdfhir_mixed", import_resources=True),
            "run_acquisition_one",
        )

        assert (first_created, artifact_created, second_created) == (True, True, True)
        assert [first_acquisition["run_id"], artifact_run["run_id"], second_acquisition["run_id"]] == [
            "run_acquisition_one",
            "run_artifact",
            "run_acquisition_two",
        ]
        active_rows = (
            (await db.execute(select(ImportRun).where(ImportRun.status.in_(control_imports.ACTIVE_STATUSES))))
            .scalars()
            .all()
        )
        assert len(active_rows) == 3
    finally:
        await _drop_import_run_schema()


async def test_fhir_artifact_admission_race(monkeypatch):
    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
        requests = [
            control_imports.create_import_run(
                {
                    "run_id": f"run_artifact_{index}",
                    "importer": "provider-directory-fhir",
                    "params": _artifact_params(f"pdfhir_artifact_{index}"),
                }
            )
            for index in range(2)
        ]

        admission_results = await asyncio.gather(*requests)

        assert sorted(created for _, created in admission_results) == [False, True]
        active_rows = (
            (await db.execute(select(ImportRun).where(ImportRun.status.in_(control_imports.ACTIVE_STATUSES))))
            .scalars()
            .all()
        )
        active_artifacts = [
            active_row
            for active_row in active_rows
            if control_imports._provider_directory_operation(active_row.params)[0]
            == control_imports._PROVIDER_DIRECTORY_SCOPED_ARTIFACT
        ]
        assert len(active_artifacts) == 1
        assert len(active_rows) == 1
    finally:
        await _drop_import_run_schema()


async def test_fhir_disjoint_relation_artifact_admission_race(monkeypatch):
    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(
            control_imports,
            "_enqueue_import_start",
            _fake_enqueue,
        )
        relation_targets = "dataset_network_plan,dataset_affiliation_organization"
        admission_requests = [
            control_imports.create_import_run(
                {
                    "run_id": f"run_summary_{index}",
                    "importer": "provider-directory-fhir",
                    "params": _artifact_params(
                        f"pdfhir_summary_{index}",
                        publish_artifacts_targets=relation_targets,
                    ),
                }
            )
            for index in range(2)
        ]

        admission_results = await asyncio.gather(*admission_requests)

        assert sorted(created for _, created in admission_results) == [True, True]
        active_rows = (
            (await db.execute(select(ImportRun).where(ImportRun.status.in_(control_imports.ACTIVE_STATUSES))))
            .scalars()
            .all()
        )
        assert len(active_rows) == 2
        assert {control_imports._provider_directory_operation(active_row.params)[0] for active_row in active_rows} == {
            control_imports._PROVIDER_DIRECTORY_SCOPED_RELATION_ARTIFACT
        }
    finally:
        await _drop_import_run_schema()


async def test_fhir_overlapping_relation_artifact_admission_race(monkeypatch):
    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(
            control_imports,
            "_enqueue_import_start",
            _fake_enqueue,
        )
        admission_requests = [
            control_imports.create_import_run(
                {
                    "run_id": f"run_overlapping_summary_{index}",
                    "importer": "provider-directory-fhir",
                    "params": _artifact_params(
                        "pdfhir_shared_summary",
                        publish_artifacts_targets="dataset_network_plan",
                    ),
                }
            )
            for index in range(2)
        ]

        admission_results = await asyncio.gather(*admission_requests)

        assert sorted(created for _, created in admission_results) == [False, True]
        active_rows = (
            (await db.execute(select(ImportRun).where(ImportRun.status.in_(control_imports.ACTIVE_STATUSES))))
            .scalars()
            .all()
        )
        assert len(active_rows) == 1
    finally:
        await _drop_import_run_schema()


async def test_fhir_general_and_relation_artifact_admission_race(monkeypatch):
    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(
            control_imports,
            "_enqueue_import_start",
            _fake_enqueue,
        )
        admission_requests = [
            control_imports.create_import_run(
                {
                    "run_id": "run_relation_summary",
                    "importer": "provider-directory-fhir",
                    "params": _artifact_params(
                        "pdfhir_relation_summary",
                        publish_artifacts_targets=("dataset_affiliation_organization"),
                    ),
                }
            ),
            control_imports.create_import_run(
                {
                    "run_id": "run_general_artifact",
                    "importer": "provider-directory-fhir",
                    "params": _artifact_params("pdfhir_general_artifact"),
                }
            ),
        ]

        admission_results = await asyncio.gather(*admission_requests)

        assert sorted(created for _, created in admission_results) == [False, True]
        active_rows = (
            (await db.execute(select(ImportRun).where(ImportRun.status.in_(control_imports.ACTIVE_STATUSES))))
            .scalars()
            .all()
        )
        assert len(active_rows) == 1
    finally:
        await _drop_import_run_schema()


async def test_terminal_status_write_does_not_clobber_canceling_run():
    await _reset_import_run_schema()
    try:
        await db.execute(
            control_imports.insert(ImportRun).values(
                run_id="run_canceling",
                engine=control_imports.ENGINE_NAME,
                importer="npi",
                family="provider",
                status="canceling",
                phase_detail="cancel requested",
                params={},
                created_at=control_imports.utc_now(),
                heartbeat_at=control_imports.utc_now(),
                progress={"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "cancel requested"},
                metrics={"cancel_signal": {"redis": True}},
            )
        )

        await mark_control_run(
            "run_canceling",
            status="succeeded",
            phase_detail="npi succeeded",
            progress_message="succeeded",
            metrics={"rows": 10},
        )

        stored_run = (await db.execute(select(ImportRun).where(ImportRun.run_id == "run_canceling"))).scalar_one()
        assert stored_run.status == "canceling"
        assert stored_run.phase_detail == "cancel requested"
        assert stored_run.metrics == {"cancel_signal": {"redis": True}}
        assert stored_run.finished_at is None
    finally:
        await _drop_import_run_schema()


async def test_cancel_request_does_not_clobber_concurrent_terminal_run(
    monkeypatch,
):
    """Preserve worker success when it commits before cancellation persistence."""
    await _reset_import_run_schema()
    try:
        await insert_queued_cancel_race_run()
        monkeypatch.setattr(
            control_imports,
            "_cancel_signal_for_run",
            finish_cancel_race_run,
        )

        cancel_result_map = await control_imports.request_cancel("run_cancel_terminal_race")

        assert cancel_result_map["status"] == "succeeded"
        assert cancel_result_map["phase_detail"] == "worker succeeded"
        assert cancel_result_map["progress"]["message"] == "succeeded"
        assert cancel_result_map["metrics"] == {"rows": 10}
    finally:
        await _drop_import_run_schema()


async def test_running_status_clears_previous_finished_at(monkeypatch):
    await _reset_import_run_schema()
    try:
        monkeypatch.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "0")
        finished_at = control_imports.utc_now()
        await db.execute(
            control_imports.insert(ImportRun).values(
                run_id="run_retry",
                engine=control_imports.ENGINE_NAME,
                importer="ptg",
                family="pricing",
                status="queued",
                phase_detail="ptg import queued",
                params={},
                created_at=control_imports.utc_now(),
                started_at=control_imports.utc_now(),
                finished_at=finished_at,
                heartbeat_at=finished_at,
                progress={"unit": "run", "total": 1, "done": 0, "pct": 0, "message": "queued"},
                error=None,
            )
        )

        await mark_control_run(
            "run_retry",
            status="running",
            phase_detail="ptg import running",
            progress_message="running",
        )

        stored_run = (await db.execute(select(ImportRun).where(ImportRun.run_id == "run_retry"))).scalar_one()
        assert stored_run.status == "running"
        assert stored_run.phase_detail == "ptg import running"
        assert stored_run.finished_at is None
        assert stored_run.error is None
    finally:
        await _drop_import_run_schema()


async def _persist_source_run(recorded):
    await db.execute(
        control_imports.insert(ImportRun).values(
            **recorded,
            engine=control_imports.ENGINE_NAME,
            family="provider-directory",
            created_at=control_imports.utc_now(),
            heartbeat_at=control_imports.utc_now(),
            progress={},
        )
    )


async def _persist_original_and_retry(recorded):
    root_id = recorded["run_id"]
    await _persist_source_run({**deepcopy(recorded), "status": "failed"})
    recorded["run_id"] = RUN_ID
    recorded["retry_of_run_id"] = root_id
    recorded["metrics"]["job_id"] = JOB_ID
    recorded["params"].update(retry_of_run_id=root_id, provider_directory_pagination_root_run_id=root_id)
    await _persist_source_run(recorded)
    request = workers._AdmittedWorkerRequest(run_id=RUN_ID, importer="provider-directory-fhir")
    workers._bind_admitted_job(request, recorded)
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded)
    job = workers._worker_job_manifest(SPEC, request, ORDINARY_IMAGE)
    job["status"] = {"active": 1}
    assert job["metadata"]["annotations"]["healthporta.com/source-root-run-id"] == root_id
    assert job["spec"]["template"]["spec"]["containers"][0]["image"] == ORDINARY_IMAGE
    return job


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [False, True])
async def test_native_original_and_retry_keep_reviewed_runtime(monkeypatch, original_job_before_default_change, retry):
    """Changing the default never replaces a matching original or its genuine retry."""
    recorded, job = original_job_before_default_change
    root_id = recorded["run_id"]
    await _reset_import_run_schema()
    try:
        if retry:
            job = await _persist_original_and_retry(recorded)
        else:
            await _persist_source_run(recorded)
        monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
        monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
        api_calls = []
        monkeypatch.setattr(workers, "_kubernetes_request", lambda *args: api_calls.append(args) or {"items": [job]})
        before = (await db.execute(select(ImportRun.__table__).order_by(ImportRun.run_id))).mappings().all()
        outcome = await workers.guarded_ensure_worker(
            {"run_id": recorded["run_id"], "importer": "provider-directory-fhir"}
        )
        assert outcome["status"] == "already_running"
        assert len(api_calls) == 1 and api_calls[0][0] == "GET"
        after = (await db.execute(select(ImportRun.__table__).order_by(ImportRun.run_id))).mappings().all()
        assert after == before
        await db.execute(
            update(ImportRun)
            .where(ImportRun.run_id == root_id)
            .values(params={**recorded["params"], "source_ids": ["other-source"]})
        )
        refused = await workers.guarded_ensure_worker(
            {"run_id": recorded["run_id"], "importer": "provider-directory-fhir"}
        )
        assert refused["status"] == "failed"
        assert len(api_calls) == 1
    finally:
        await _drop_import_run_schema()


async def test_native_profile_uses_persisted_job(monkeypatch, secret_selection, source_runtime_mount):
    """Verify the stored signed Profile when creating or reusing its ordinary Job."""
    await _reset_import_run_schema()
    try:
        database_identity_by_field = {
            "database_system_identifier": str(await db.scalar("SELECT system_identifier FROM pg_control_system()")),
            "database_oid": int(await db.scalar("SELECT oid FROM pg_database WHERE datname=current_database()")),
            "database_name": str(await db.scalar("SELECT current_database()")),
        }
        recorded_run = _signed_profile_run(monkeypatch, database_identity_by_field)
        await _persist_source_run(recorded_run)
        monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
        monkeypatch.setenv("HLTHPRT_WORKER_LAUNCHER", "kubernetes")
        observed_jobs, kubernetes_methods = [], []

        def kubernetes(method, path, body=None):
            kubernetes_methods.append(method)
            if method == "POST":
                observed_jobs.append({**body, "status": {"active": 1}})
            return {"items": observed_jobs}

        monkeypatch.setattr(workers, "_kubernetes_request", kubernetes)
        original_runs = (await db.execute(select(ImportRun.__table__))).mappings().all()
        ensure_request_by_field = {
            "run_id": RUN_ID,
            "importer": "provider-directory-fhir",
            "params": {"import_resources": True},
        }
        assert (await workers.guarded_ensure_worker(ensure_request_by_field))["status"] == "started"
        from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME

        advanced_time = VALIDATION_TIME + timedelta(seconds=304)
        monkeypatch.setattr(source, "datetime", SimpleNamespace(now=lambda _zone: advanced_time))
        assert (await workers.guarded_ensure_worker(ensure_request_by_field))["status"] == "already_running"
        assert kubernetes_methods.count("POST") == len(observed_jobs) == 1
        existing_job = observed_jobs.pop()
        assert (await workers.guarded_ensure_worker(ensure_request_by_field))["status"] == "failed"
        assert kubernetes_methods.count("POST") == 1
        observed_jobs.append(existing_job)
        worker_container = observed_jobs[0]["spec"]["template"]["spec"]["containers"][0]
        assert worker_container["image"] == PROFILE_IMAGE
        assert worker_container["command"] == workers._worker_command(workers._worker_python(), SPEC)
        assert "SOURCE_USER" not in {entry["name"] for entry in worker_container["env"]}
        assert {entry["name"]: entry.get("value") for entry in worker_container["env"]}[
            "HLTHPRT_WORKER_ONCE_TARGET_JOB_ID"
        ] == JOB_ID
        assert (await db.execute(select(ImportRun.__table__))).mappings().all() == original_runs
        changed_params = deepcopy(recorded_run["params"])
        changed_params["provider_directory_profile_generation"] += 1
        await db.execute(update(ImportRun).where(ImportRun.run_id == RUN_ID).values(params=changed_params))
        prior_kubernetes_methods = list(kubernetes_methods)
        assert (await workers.guarded_ensure_worker(ensure_request_by_field))["status"] == "failed"
        assert kubernetes_methods == prior_kubernetes_methods
    finally:
        await _drop_import_run_schema()
