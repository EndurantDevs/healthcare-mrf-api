# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Runtime branch buffer for semantic-content acquisition safeguards."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")
manual_catalog = importlib.import_module(
    "process.provider_directory_fhir_manual_catalog"
)
abandonment_store = importlib.import_module(
    "process.provider_directory_fhir_subset_abandonment_store"
)


@pytest.mark.asyncio
async def test_empty_deferred_resource_batch_is_a_noop() -> None:
    assert (
        await importer._upsert_deferred_resource_rows(
            object,
            [],
            dataset_id=None,
            track_seen=False,
        )
        == 0
    )


def test_progress_details_allow_absent_optional_fields() -> None:
    assert importer._provider_directory_progress_details() == {}


@pytest.mark.asyncio
async def test_inadmitted_progress_phase_stays_in_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(),
    )
    monkeypatch.setattr(
        importer,
        "_is_profile_progress_phase_admitted",
        lambda _admission, _phase: False,
    )

    await importer._mark_provider_directory_progress(
        "root-edge",
        phase="acquiring resources",
        done=0,
        total=1,
        message="waiting",
    )


@pytest.mark.asyncio
async def test_admitted_progress_requires_owned_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importer,
        "_is_admitted_profile_progress_written",
        AsyncMock(return_value=False),
    )

    with pytest.raises(RuntimeError, match="progress_ownership_lost"):
        await importer._persist_provider_directory_progress(
            "root-edge",
            SimpleNamespace(),
            phase="acquiring resources",
            message="waiting",
            progress_by_name={},
            metrics=None,
        )


def test_census_stats_ignore_noninteger_optional_counts() -> None:
    stats_by_name = {
        "current_version_census_sources": 0,
        "current_version_census_verified_sources": 0,
    }
    result = importer.ResourceFetchResult(
        model=object,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=0,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
        fetch_mode=importer.CURRENT_VERSION_CENSUS_FETCH_MODE,
        fetch_diagnostic={"verified": True},
    )

    importer._record_current_version_census_stats(stats_by_name, result)

    assert stats_by_name == {
        "current_version_census_sources": 1,
        "current_version_census_verified_sources": 1,
    }


@pytest.mark.asyncio
async def test_reviewed_subset_abandonment_rejects_wrong_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = SimpleNamespace(
        is_server_issued_subset_v3=True,
        resources=("Practitioner",),
    )
    guard_lease = SimpleNamespace(
        is_held=True,
        context=SimpleNamespace(
            canonical_api_base="https://example.invalid/fhir"
        ),
        database=SimpleNamespace(),
        lock_key="guard-edge",
    )
    sync_abandonment = AsyncMock()
    monkeypatch.setattr(
        importer,
        "current_version_census_contract",
        lambda _source: contract,
    )
    monkeypatch.setattr(
        manual_catalog,
        "reviewed_manual_census_source_id",
        lambda: "source-expected",
    )
    monkeypatch.setattr(
        abandonment_store,
        "sync_reviewed_subset_abandonment_transaction",
        sync_abandonment,
    )

    await importer._abandon_terminal_reviewed_subset_without_masking(
        [
            {
                "source_id": "source-edge",
                "canonical_api_base": "https://example.invalid/fhir",
            }
        ],
        SimpleNamespace(
            diagnostics_by_resource={"Practitioner": "incomplete"}
        ),
        guard_lease,
    )

    sync_abandonment.assert_not_awaited()
