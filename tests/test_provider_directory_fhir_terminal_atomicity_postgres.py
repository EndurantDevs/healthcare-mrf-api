# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for terminal-page and census-checkpoint atomicity."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_fhir_census_binding import (
    current_version_census_count_url,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_RETRYABLE_ERROR,
)
from tests.provider_directory_current_census_postgres_support import (
    NEXT_URL,
    RESOURCE_TYPE,
    ROOT_RUN_ID,
    candidate_resource_ids,
    census_contract,
    census_database,
    census_source_record,
    checkpoint_context,
    checkpoint_record,
    count_bundle,
    fetch_practitioners,
    fetch_sequence,
    importer,
    practitioner_bundle,
    proof_shard_counts,
)


def _count_bundle(total: int) -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
    }


async def _seed_resumable_page(
    monkeypatch: pytest.MonkeyPatch,
    source_record: dict[str, Any],
    start_url: str,
    count_url: str,
) -> None:
    requested_urls: list[str] = []
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fetch_sequence(
            [
                (200, count_bundle(), None, 1),
                (
                    200,
                    practitioner_bundle("practitioner-1", next_url=NEXT_URL),
                    None,
                    1,
                ),
                (500, None, None, 1),
            ],
            requested_urls,
        ),
    )
    fetch_result = await fetch_practitioners(
        source_record,
        checkpoint_context(
            source_record,
            owner_run_id=ROOT_RUN_ID,
            retry_of_run_id=None,
        ),
    )
    assert requested_urls == [count_url, start_url, NEXT_URL]
    assert fetch_result.error == (
        f"{CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:http_500"
    )


def _install_terminal_fetch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    post_count: int,
) -> list[str]:
    requested_urls: list[str] = []
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fetch_sequence(
            [
                (200, practitioner_bundle("practitioner-2"), None, 1),
                (200, _count_bundle(post_count), None, 1),
            ],
            requested_urls,
        ),
    )
    return requested_urls


async def _assert_terminal_page_rolled_back(
    database: Any,
    schema: str,
    *,
    owner_run_id: str,
) -> dict[str, Any]:
    checkpoint = await checkpoint_record(database, schema)
    assert checkpoint["state"] == importer.PAGINATION_CHECKPOINT_ACTIVE
    assert checkpoint["next_url"] == NEXT_URL
    assert checkpoint["pages_processed"] == 1
    assert checkpoint["rows_processed"] == 1
    assert checkpoint["owner_run_id"] == owner_run_id
    assert await candidate_resource_ids(database, schema) == ["practitioner-1"]
    assert await proof_shard_counts(database, schema) == (1, 1)
    return checkpoint


@pytest.mark.asyncio
async def test_rejected_proof_rolls_back_page_before_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain a rejected proof without advancing its page or checkpoint."""

    source_record = census_source_record(census_contract())
    start_url = importer._resource_start_url(
        source_record, RESOURCE_TYPE, page_count=1
    )
    assert start_url is not None
    count_url = current_version_census_count_url(start_url)
    owner_run_id = "run-current-census-rejected-terminal"
    async with census_database(monkeypatch) as (database, schema):
        await _seed_resumable_page(
            monkeypatch, source_record, start_url, count_url
        )
        requested_urls = _install_terminal_fetch(monkeypatch, post_count=1)
        fetch_result = await fetch_practitioners(
            source_record,
            checkpoint_context(
                source_record,
                owner_run_id=owner_run_id,
                retry_of_run_id=ROOT_RUN_ID,
            ),
        )
        checkpoint = await _assert_terminal_page_rolled_back(
            database, schema, owner_run_id=owner_run_id
        )

    assert requested_urls == [NEXT_URL, count_url]
    assert fetch_result.complete is False
    assert fetch_result.error == (
        f"{importer.CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:census_drift"
    )
    completeness = checkpoint["completeness_json"]
    assert completeness["verified"] is False
    assert completeness["failure"] == "census_drift"
    assert completeness["pre_count"] == 2
    assert completeness["post_count"] == 1
    assert completeness["processed_rows"] == 2
    assert completeness["unique_candidate_rows"] == 2


@pytest.mark.asyncio
async def test_checkpoint_failure_rolls_back_accepted_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep an accepted terminal page atomic with its checkpoint write."""

    source_record = census_source_record(census_contract())
    start_url = importer._resource_start_url(
        source_record, RESOURCE_TYPE, page_count=1
    )
    assert start_url is not None
    count_url = current_version_census_count_url(start_url)
    owner_run_id = "run-current-census-checkpoint-failure"
    async with census_database(monkeypatch) as (database, schema):
        await _seed_resumable_page(
            monkeypatch, source_record, start_url, count_url
        )
        requested_urls = _install_terminal_fetch(monkeypatch, post_count=2)
        monkeypatch.setattr(
            importer,
            "_save_pagination_checkpoint",
            AsyncMock(side_effect=RuntimeError("synthetic_checkpoint_failure")),
        )
        with pytest.raises(RuntimeError, match="synthetic_checkpoint_failure"):
            await fetch_practitioners(
                source_record,
                checkpoint_context(
                    source_record,
                    owner_run_id=owner_run_id,
                    retry_of_run_id=ROOT_RUN_ID,
                ),
            )
        checkpoint = await _assert_terminal_page_rolled_back(
            database, schema, owner_run_id=owner_run_id
        )

    assert requested_urls == [NEXT_URL, count_url]
    assert checkpoint["completeness_json"]["verified"] is False
    assert "post_count" not in checkpoint["completeness_json"]
