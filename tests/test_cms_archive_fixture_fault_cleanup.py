# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unused archive acknowledgement faults cannot replace a precommit failure."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tests import test_cms_archive_publication_postgres as archive_case


def _archive_fixture_io(monkeypatch, engine, failure, events, outcome):
    """Double only native setup and database I/O around the original fixture."""

    @asynccontextmanager
    async def database_context(_monkeypatch):
        yield engine, "synthetic"

    @asynccontextmanager
    async def transaction(_database):
        yield object()
        events.append("cleanup_commit")

    async def prepare(_monkeypatch, database, _schema, _outcome, phase_checks):
        async def ready():
            phase_checks.append(SimpleNamespace(phase="readiness"))

        async def cleanup(fhir):
            events.append(database.acknowledgement_failure)
            assert fhir.db is database
            async with database.transaction():
                events.append("cleanup_body")

        return SimpleNamespace(
            assert_ready=ready, archive_delta=SimpleNamespace(cleanup=cleanup), execution=object()
        ), None

    predecessor_by_field = {
        "payload": {
            "cms": dict.fromkeys(
                ("dataset_id", "endpoint_id", "dataset_hash", "release_id", "proof_version"), "synthetic"
            )
        }
    }
    monkeypatch.setattr(archive_case.native, "_database", database_context)
    monkeypatch.setattr(archive_case.native, "_publish_initial", AsyncMock(return_value=predecessor_by_field))
    monkeypatch.setattr(archive_case.common, "_enable_candidate_checks", AsyncMock())
    monkeypatch.setattr(archive_case.common, "_serving_state", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(archive_case.Database, "transaction", transaction)
    monkeypatch.setattr(archive_case, "_prepared_serving", prepare)

    async def fail_precommit(*_args, **_kwargs):
        fault_type = asyncio.CancelledError if outcome == "cancel_after" else OSError
        assert isinstance(archive_case.fhir.db.acknowledgement_failure, fault_type)
        assert archive_case.fhir.db._transaction_binding() is None
        events.append("armed_fault")
        raise failure

    monkeypatch.setattr(
        archive_case.publication, "commit_prepared_serving_generation", AsyncMock(side_effect=fail_precommit)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["lost_ack", "cancel_after"])
async def test_original_archive_fixture_disarms_fault_before_cleanup(monkeypatch, outcome):
    """Run the original fault injector, owner acknowledgement wrapper and fixture finally."""
    engine = create_async_engine("postgresql+asyncpg://synthetic@127.0.0.1/synthetic")
    failure = RuntimeError("synthetic early precommit failure")
    events = []
    previous_profile = archive_case.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get()
    previous_preparation = archive_case.preparation._ACTIVE.get()
    _archive_fixture_io(monkeypatch, engine, failure, events, outcome)
    try:
        with pytest.raises(RuntimeError) as caught:
            await archive_case.test_actual_archive_readiness_and_common_publication(monkeypatch, outcome)
        assert caught.value is failure
        assert events == ["armed_fault", None, "cleanup_body", "cleanup_commit"]
        assert archive_case.publication.commit_prepared_serving_generation.await_count == 1
        assert archive_case.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.get() is previous_profile
        assert archive_case.preparation._ACTIVE.get() is previous_preparation
    finally:
        await engine.dispose()
