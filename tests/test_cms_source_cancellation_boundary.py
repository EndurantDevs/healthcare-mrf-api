# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the native fixture's commit boundary through the actual dispatcher."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests import test_cms_npd_admission_postgres as native_case
from tests.test_provider_directory_cms_npd import _source_dispatch_fixture, cms, fhir


def _cancellation_dispatch(monkeypatch, tmp_path):
    """Keep real readers, two worker tasks and drain while substituting database I/O."""
    fixture = _source_dispatch_fixture(monkeypatch, tmp_path)
    fixture.committed_rows = []
    fixture.worker_tasks = []
    original_gather = fixture.fhir._gather_provider_directory_profile_tasks

    async def gather(tasks):
        fixture.worker_tasks.extend(tasks)
        return await original_gather(tasks)

    async def persist(_fhir, _model, rows, _raw, _candidate, _kind):
        fixture.committed_rows.extend(rows)
        await asyncio.sleep(0)

    async def probe(*args):
        return await fhir._raise_if_resource_import_cancelled(*args)

    fixture.fhir._gather_provider_directory_profile_tasks = gather
    fixture.fhir._raise_if_resource_import_cancelled = probe
    monkeypatch.setattr(cms, "_persist_source_batch", persist)
    return fixture


@pytest.mark.asyncio
async def test_cancellation_fixture_stops_after_third_acknowledged_commit(monkeypatch, tmp_path):
    """A sibling cannot make a fourth commit; the original replay assertions still run."""
    fixture = _cancellation_dispatch(monkeypatch, tmp_path)
    original_persist = cms._persist_source_batch
    probe = AsyncMock()
    monkeypatch.setattr(fhir, "_raise_if_resource_import_cancelled", probe)
    database = SimpleNamespace(
        first=AsyncMock(return_value=(fixture.candidate.dataset_id,)),
        scalar=AsyncMock(side_effect=lambda *_args, **_kwargs: len(fixture.committed_rows)),
    )

    @asynccontextmanager
    async def database_context(_monkeypatch):
        yield database

    async def admit(_directory, _receipt, run_id):
        if run_id == "cms-test-incumbent":
            return {"dataset_id": "incumbent"}
        if run_id == "cms-test-resumed":
            return {"dataset_id": fixture.candidate.dataset_id, "resource_count": 9}
        return await cms._stage_and_validate_candidate(
            fixture.fhir, fixture.directory, fixture.candidate, fixture.identity, fixture.receipt, {}, {}
        )

    current = AsyncMock()
    monkeypatch.setattr(native_case, "admission_database", database_context)
    monkeypatch.setattr(native_case, "retained_release", lambda *_args, **_kwargs: (fixture.directory, fixture.receipt))
    monkeypatch.setattr(native_case, "_admit_legacy_source", admit)
    monkeypatch.setattr(native_case, "_assert_current", current)
    await native_case.test_committed_batch_cancellation_keeps_incumbent_and_replays_safely(monkeypatch, tmp_path)
    assert cms._persist_source_batch is original_persist
    assert len(fixture.committed_rows) == 3
    assert len(fixture.worker_tasks) == 2 and all(task.done() for task in fixture.worker_tasks)
    assert not fixture.opened and fixture.events == []
    assert fhir._raise_if_resource_import_cancelled is probe and probe.await_count == 2
    assert [call.args[1] for call in current.await_args_list] == ["incumbent", fixture.candidate.dataset_id]
