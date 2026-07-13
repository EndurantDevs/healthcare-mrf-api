# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os

import pytest
from sqlalchemy import select, text, update

from api import control_imports
from db.models import ImportRun, db
from process.control_lifecycle import mark_control_run


pytestmark = [
    pytest.mark.asyncio(loop_scope="module"),
    pytest.mark.filterwarnings(
        "ignore:coroutine 'Connection._cancel' was never awaited:RuntimeWarning"
    ),
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
            await db.execute(select(ImportRun).where(ImportRun.status.in_(control_imports.ACTIVE_STATUSES)))
        ).scalars().all()
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
            await db.execute(select(ImportRun).where(ImportRun.status.in_(control_imports.ACTIVE_STATUSES)))
        ).scalars().all()
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


async def test_duplicate_idempotency_key_uses_real_partial_unique_index(monkeypatch):
    await _reset_import_run_schema()
    try:
        monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
        first, first_created = await control_imports.create_import_run(
            {
                "run_id": "run_first",
                "importer": "npi",
                "idempotency_key": "idem-db",
            }
        )
        assert first_created is True
        assert first["run_id"] == "run_first"

        real_find = control_imports.find_active_run_by_idempotency_key
        calls = {"count": 0}

        async def race_miss_then_real(idempotency_key: str):
            calls["count"] += 1
            if calls["count"] == 1:
                return None
            return await real_find(idempotency_key)

        monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", race_miss_then_real)
        second, second_created = await control_imports.create_import_run(
            {
                "run_id": "run_second",
                "importer": "nucc",
                "idempotency_key": "idem-db",
            }
        )

        assert second_created is False
        assert second["run_id"] == "run_first"
        assert calls["count"] == 2

        await db.execute(
            update(ImportRun)
            .where(ImportRun.run_id == "run_first")
            .values(status="succeeded", finished_at=control_imports.utc_now())
        )
        monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", real_find)
        third, third_created = await control_imports.create_import_run(
            {
                "run_id": "run_after_terminal",
                "importer": "nucc",
                "idempotency_key": "idem-db",
            }
        )

        assert third_created is True
        assert third["run_id"] == "run_after_terminal"
        rows = (await db.execute(select(ImportRun).order_by(ImportRun.run_id))).scalars().all()
        assert [row.run_id for row in rows] == ["run_after_terminal", "run_first"]
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

        row = (
            await db.execute(select(ImportRun).where(ImportRun.run_id == "run_canceling"))
        ).scalar_one()
        assert row.status == "canceling"
        assert row.phase_detail == "cancel requested"
        assert row.metrics == {"cancel_signal": {"redis": True}}
        assert row.finished_at is None
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
                status="failed",
                phase_detail="ptg import failed",
                params={},
                created_at=control_imports.utc_now(),
                started_at=control_imports.utc_now(),
                finished_at=finished_at,
                heartbeat_at=finished_at,
                progress={"unit": "run", "total": 1, "done": 1, "pct": 100, "message": "failed"},
                error={"code": "low_memory_pause"},
            )
        )

        await mark_control_run(
            "run_retry",
            status="running",
            phase_detail="ptg import running",
            progress_message="running",
        )

        row = (await db.execute(select(ImportRun).where(ImportRun.run_id == "run_retry"))).scalar_one()
        assert row.status == "running"
        assert row.phase_detail == "ptg import running"
        assert row.finished_at is None
        assert row.error is None
    finally:
        await _drop_import_run_schema()
