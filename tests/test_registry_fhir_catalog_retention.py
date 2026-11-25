# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog candidate retention: run lock, listing, per-candidate deferral and arguments."""

import json
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_fhir_catalog_retention as retention


class _Connection:
    """Record statements; answer the lock, the eligible list and each guarded preparation delete."""

    def __init__(self, *, locked=True, candidates=(), busy=(), gone=()):
        self.locked, self.candidates, self.busy, self.gone = locked, list(candidates), set(busy), set(gone)
        self.statements = []
        self.rolled_back = 0

    async def fetchval(self, sql, *args):
        self.statements.append(sql)
        if "pg_try_advisory_lock" in sql:
            return self.locked
        if args[-1] in self.busy:
            raise asyncpg.LockNotAvailableError("candidate is busy")
        return None if args[-1] in self.gone else True

    async def fetch(self, sql, *args):
        self.statements.append(sql)
        if "pg_class" in sql:
            return [{"name": '"candidate"."registry_fhir_catalog_network"'}]
        return [{"preparation_id": candidate} for candidate in self.candidates]

    async def execute(self, sql, *args):
        self.statements.append(sql)

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.rolled_back += 1
            raise


def _drops(connection):
    return [sql for sql in connection.statements if sql.startswith(("DROP", "DELETE"))]


@pytest.mark.asyncio
async def test_running_catalog_sync_skips_the_sweep():
    connection = _Connection(locked=False, candidates=[uuid4()])
    report = await retention.retire_unpublished_fhir_catalog_candidates(
        connection, control_schema="registry", execute=True
    )
    assert report == {"state": "catalog_run_active", "schemas": []}
    assert len(connection.statements) == 1


@pytest.mark.asyncio
async def test_listing_changes_nothing_and_releases_the_lock():
    candidate = uuid4()
    connection = _Connection(candidates=[candidate])
    report = await retention.retire_unpublished_fhir_catalog_candidates(connection, control_schema="registry")
    assert report == {"state": "listed", "schemas": ["fhir_catalog_" + candidate.hex]}
    assert not _drops(connection)
    assert "pg_advisory_unlock" in connection.statements[-1]


@pytest.mark.asyncio
async def test_busy_candidate_is_deferred_and_the_rest_retired():
    busy, idle = uuid4(), uuid4()
    connection = _Connection(candidates=[busy, idle], busy={busy})
    report = await retention.retire_unpublished_fhir_catalog_candidates(
        connection, control_schema="registry", execute=True
    )
    assert report == {
        "state": "retired",
        "schemas": ["fhir_catalog_" + idle.hex],
        "deferred": ["fhir_catalog_" + busy.hex],
    }
    drops = _drops(connection)
    assert drops[0].startswith("DELETE FROM hp_snapshot_retention.preparation_relation")
    assert drops[1].startswith("DELETE FROM hp_snapshot_retention.preparation_claim")
    assert drops[2].startswith("DELETE FROM hp_snapshot_retention.preparation p")
    assert 'DROP TABLE "candidate"."registry_fhir_catalog_network"' in drops
    assert f'DROP SCHEMA IF EXISTS "fhir_catalog_{idle.hex}"' in drops
    assert "pg_advisory_unlock" in connection.statements[-1]


@pytest.mark.asyncio
async def test_candidate_no_longer_eligible_is_left_untouched():
    candidate = uuid4()
    connection = _Connection(candidates=[candidate], gone={candidate})
    report = await retention.retire_unpublished_fhir_catalog_candidates(
        connection, control_schema="registry", execute=True
    )
    assert report == {"state": "retired", "schemas": [], "deferred": ["fhir_catalog_" + candidate.hex]}
    assert connection.rolled_back == 1
    assert [sql for sql in _drops(connection) if not sql.startswith("DELETE FROM hp_snapshot_retention")] == []


def test_command_sweeps_the_configured_registry_schema(monkeypatch, capsys):
    from db.connection import db

    driver = object()

    @asynccontextmanager
    async def acquire_driver():
        yield driver

    report_by_field = {"state": "retired", "schemas": [], "deferred": []}
    sweep = AsyncMock(return_value=report_by_field)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "registry_fixture")
    monkeypatch.setattr(db, "connect", AsyncMock())
    monkeypatch.setattr(db, "disconnect", AsyncMock())
    monkeypatch.setattr(db, "acquire_driver", acquire_driver)
    monkeypatch.setattr(retention, "retire_unpublished_fhir_catalog_candidates", sweep)
    retention.main(["--stale-hours", "6", "--execute"])
    sweep.assert_awaited_once_with(
        driver, control_schema="registry_fixture", stale_after=timedelta(hours=6), execute=True
    )
    db.disconnect.assert_awaited_once()
    assert json.loads(capsys.readouterr().out) == report_by_field


def test_stale_bound_must_be_positive():
    with pytest.raises(SystemExit) as exited:
        retention.main(["--stale-hours", "0"])
    assert exited.value.code == 2
