# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Store and facade tests for reviewed mixed-terminal disposition."""

from __future__ import annotations

from dataclasses import fields
import inspect

import pytest

from process import provider_directory_fhir_subset_terminal_disposition as facade
from process import provider_directory_fhir_subset_terminal_disposition_store
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    EXPECTED_RESOURCE_TYPES,
    TERMINAL_DISPOSITION_ENABLED_ENV,
    TERMINAL_DISPOSITION_METADATA_KEY,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
    validated_terminal_disposition_marker,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    selected_reviewed_subset_terminal_disposition,
)
from process.provider_directory_fhir_subset_terminal_disposition_store import (
    sync_reviewed_subset_terminal_disposition_transaction,
)
from tests.provider_directory_fhir_subset_terminal_disposition_support import (
    TerminalDispositionDatabase,
)


@pytest.mark.asyncio
async def test_store_cas_order_and_exact_retry_are_idempotent():
    database = TerminalDispositionDatabase()

    first = await sync_reviewed_subset_terminal_disposition_transaction(
        database,
        "source-a",
    )
    second = await sync_reviewed_subset_terminal_disposition_transaction(
        database,
        "source-a",
    )

    assert first == ReviewedSubsetTerminalDispositionResult(disposed=True)
    assert second == ReviewedSubsetTerminalDispositionResult(disposed=False)
    updates = [
        statement
        for method, statement, _parameters in database.calls
        if method == "status" and "UPDATE" in statement
    ]
    assert len(updates) == len(EXPECTED_RESOURCE_TYPES) + 1
    assert all("pagination_checkpoint" in sql for sql in updates[:-1])
    assert "endpoint_dataset" in updates[-1]
    marker = database.candidate_row["publication_metadata_json"][
        TERMINAL_DISPOSITION_METADATA_KEY
    ]
    assert validated_terminal_disposition_marker(marker) == marker


@pytest.mark.asyncio
async def test_marker_rejects_zero_retained_progress():
    selection, _checkpoint_rows = (
        await selected_reviewed_subset_terminal_disposition(
            TerminalDispositionDatabase(),
            "source-a",
        )
    )
    marker = selection.marker_by_field
    for resource in marker["resource_dispositions"].values():
        resource["retained_rows"] = 0
        resource["returned_unique"] = 0
    marker.update(
        checkpoint_rows_processed=0,
        resource_count=0,
        proof_row_count=0,
        proof_shard_count=0,
    )

    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        validated_terminal_disposition_marker(marker)


@pytest.mark.asyncio
async def test_replay_does_not_depend_on_mutable_source_import_snapshot():
    """Keep sealed evidence valid after a later campaign updates the source."""

    database = TerminalDispositionDatabase()
    first = await sync_reviewed_subset_terminal_disposition_transaction(
        database,
        "source-a",
    )
    database.source_row["metadata_json"]["last_resource_import"] = {
        "run_id": "owner-fresh",
        "observed_at": "2026-08-10T00:00:00Z",
        "resources": {"Synthetic": {"complete": True}},
    }
    database.source_row["metadata_json"][
        "provider_directory_configured_endpoint_id"
    ] = "endpoint-fresh"

    replay = await sync_reviewed_subset_terminal_disposition_transaction(
        database,
        "source-a",
    )

    assert first.disposed is True
    assert replay.is_already_applied is True


@pytest.mark.asyncio
async def test_store_rejects_cas_or_guard_failure(monkeypatch):
    database = TerminalDispositionDatabase()
    original_status = database.status

    async def failed_candidate_cas(statement, **parameters):
        if "provider_directory_endpoint_dataset" in statement:
            database.calls.append(("status", statement, parameters))
            return 0
        return await original_status(statement, **parameters)

    monkeypatch.setattr(database, "status", failed_candidate_cas)
    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await sync_reviewed_subset_terminal_disposition_transaction(
            database,
            "source-a",
        )
    assert error.value.code == "state"


@pytest.mark.asyncio
async def test_operator_facade_is_selector_free(monkeypatch):
    database = TerminalDispositionDatabase()
    monkeypatch.setenv(TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: "source-a",
    )

    result = await facade.dispose_reviewed_subset_census_drift_root(
        database=database
    )

    assert result.disposed is True
    assert [field.name for field in fields(result)] == ["disposed"]


def test_new_and_expired_cursor_markers_are_mutually_exclusive_in_store_sql():
    source = inspect.getsource(
        provider_directory_fhir_subset_terminal_disposition_store._seal_candidate
    )
    assert "abandonment_key" in source
    assert "disposition_key" in source
