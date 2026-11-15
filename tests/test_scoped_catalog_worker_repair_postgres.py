# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native prepared attempts survive ordinary repair and cancellation requests."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api import control_workers as workers
from api import scoped_catalog_publisher_worker as launcher
from db.connection import ConnectionProxy
from db.models import ImportRun
from process.scoped_catalog_publisher import parse_attempt, run_publisher
from tests.test_named_scoped_catalog_postgres import _prepare, _resources
from tests.test_scoped_catalog_worker_repair import _ordinary_job_id, _repair_request, _transport


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize("status", ["finalizing", "canceling"])
async def test_persisted_repair_launches_protected_publication_and_observes_terminal(monkeypatch, importer, status):
    async with _resources(monkeypatch) as (actors, settings):
        prepared = await _prepare(actors, settings.schema, importer)
        candidate = prepared[launcher.publication.METRIC]
        run_id = candidate["run_id"]
        async with actors.builder.begin() as session:
            await session.execute(
                text(
                    f'UPDATE "{settings.schema}".import_run SET status=:status, '
                    "metrics=(metrics::jsonb||CAST(:extra AS jsonb))::json WHERE run_id=:run"
                ),
                {"status": status, "extra": json.dumps({"job_id": _ordinary_job_id(importer, run_id)}), "run": run_id},
            )
            run_by_field = dict(
                (
                    await session.execute(
                        text(f'SELECT * FROM "{settings.schema}".import_run WHERE run_id=:run'), {"run": run_id}
                    )
                )
                .mappings()
                .one()
            )

        @asynccontextmanager
        async def acquire():
            async with actors.builder.begin() as session:
                yield ConnectionProxy(workers.db, session, None)

        monkeypatch.setattr(workers, "db", SimpleNamespace(acquire=acquire))
        monkeypatch.setattr(ImportRun.__table__, "schema", settings.schema)
        api, jobs = _transport(monkeypatch)
        request = _repair_request(run_by_field)
        assert (await workers.guarded_ensure_worker(request))["status"] == "started"
        assert (await workers.guarded_ensure_worker(request))["status"] == "already_running"
        job = next(iter(jobs.values()))
        assert job["metadata"]["name"] != run_by_field["metrics"]["job_id"]
        assert job["spec"]["template"]["spec"]["serviceAccountName"] == launcher.SERVICE_ACCOUNT
        attempt = parse_attempt(job["spec"]["template"]["spec"]["containers"][0]["args"])
        terminal = "succeeded" if status == "finalizing" else "canceled"
        assert (await run_publisher(settings, attempt))["status"] == terminal
        calls_before_terminal = api.call_count
        terminal_result = await workers.guarded_ensure_worker(request)
        assert terminal_result["status"] == "blocked" and terminal_result["items"][0]["job_status"] == terminal
        assert api.call_count == calls_before_terminal
        async with actors.builder.begin() as session:
            saved = (
                (
                    await session.execute(
                        text(f'SELECT status,metrics FROM "{settings.schema}".import_run WHERE run_id=:run'),
                        {"run": run_id},
                    )
                )
                .mappings()
                .one()
            )
            assert saved["status"] == terminal
            assert saved["metrics"]["scoped_catalog_handoff"] == candidate
