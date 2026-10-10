# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained-refresh INSERT uses the actual admitted worker/capture path."""

from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_owned_evidence_wave import Session, custody, fhir, owned_wave


async def copy_wave(fixture):
    _, _, _, build, *_ = fixture
    build.source_ids = ("refresh_source",)
    build.retained_source_ids = ("retained_source",)
    batch = fhir._ProviderDirectoryProfileEvidenceBatch(kind="copy")
    sql = fhir.profile_artifact.copy_existing_evidence_sql(
        source_ref='"fixture"."current_evidence"', target_ref='"fixture"."new_evidence"'
    )
    return await fhir._run_profile_evidence_window(build, [(0, batch)], sql, {}, {}), sql


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,expected_rows", [("success", 1), ("overrun", 3)])
async def test_retained_insert_keeps_original_count_and_observes_once(owned_wave, monkeypatch, mode, expected_rows):
    state, _, admission, build, *_ = owned_wave
    state.mode = mode
    original = Session.execute
    calls = []

    async def execute(self, statement, parameters):
        calls.append((str(statement), parameters))
        return await original(self, statement, parameters)

    monkeypatch.setattr(Session, "execute", execute)
    affected_rows_by_batch, sql = await copy_wave(owned_wave)
    assert affected_rows_by_batch == [expected_rows]
    assert calls == [
        (
            fhir.profile_statement_wal._EXPLAIN + sql,
            {
                "source_ids": ["refresh_source"],
                "retained_source_ids": ["retained_source"],
                "profile_as_of": build.profile_as_of,
            },
        )
    ]
    capture = admission.wal_tracker.owned_evidence_wave_outcomes[0]["workers"][0]
    assert (
        capture["status"] == "committed_measured" and capture["statement_measurement"]["affected_rows"] == expected_rows
    )
    assert (
        capture["statement_reconciliation_status"] == "complete"
        and capture["unclassified_record_residual"]["wal_record_bytes"] == 32
    )
    assert state.connections[0].events.count("write") == 1 and state.connections[0].events.count("commit") == 1
    assert not capture["reservation_refund"] and not capture["accounting_authority"]


@pytest.mark.asyncio
async def test_unowned_copy_keeps_status_and_exact_original_parameters(owned_wave, monkeypatch):
    _, database, _, build, *_ = owned_wave
    build.source_ids, build.retained_source_ids = ("refresh_source",), ("retained_source",)
    sql = fhir.profile_artifact.copy_existing_evidence_sql(
        source_ref='"fixture"."current_evidence"', target_ref='"fixture"."new_evidence"'
    )
    status = AsyncMock(return_value=4)
    monkeypatch.setattr(database, "status", status)
    monkeypatch.setattr(database, "scalar", AsyncMock(side_effect=AssertionError("no EXPLAIN without owner")))
    batch = fhir._ProviderDirectoryProfileEvidenceBatch(kind="copy")
    assert await fhir._execute_profile_evidence_batch_statement(build, batch, sql, {}, None) == 4
    status.assert_awaited_once_with(
        sql, source_ids=["refresh_source"], retained_source_ids=["retained_source"], profile_as_of=build.profile_as_of
    )


@pytest.mark.asyncio
async def test_missing_fact_witness_still_requires_owner_rollback(owned_wave, monkeypatch):
    original = Session.execute

    async def missing(self, statement, parameters):
        result = await original(self, statement, parameters)
        del result.scalar()[0]["Plan"]["Tuples Inserted"]
        return result

    monkeypatch.setattr(Session, "execute", missing)
    state, _, admission, build, batches, projection, _ = owned_wave
    with pytest.raises(custody.OwnedWalTransactionError):
        await fhir._run_profile_evidence_window(build, [(0, batches[0])], "COPY unused", {}, {0: projection})
    assert "commit" not in state.connections[0].events
    assert admission.wal_tracker.owned_evidence_wave_outcomes[0]["status"] == "accounting_incomplete"
