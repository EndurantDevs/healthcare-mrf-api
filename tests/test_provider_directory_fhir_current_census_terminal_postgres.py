# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for terminal current-version census cursors."""

from __future__ import annotations

from typing import Any

import pytest

from process.provider_directory_fhir_census_binding import (
    current_version_census_count_url,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_FETCH_MODE,
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
from tests.test_provider_directory_fhir_current_census_postgres import (
    _run_initial_interrupted_phase,
)


async def _run_expired_cursor_phase(
    monkeypatch: pytest.MonkeyPatch,
    source_by_field: dict[str, Any],
    database: Any,
    schema: str,
    start_url: str,
    count_url: str,
) -> None:
    """Prove one direct retry replaces an expired exact cursor from its start."""

    requested_urls: list[str] = []
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fetch_sequence(
            [
                (410, None, None, 1),
                (200, count_bundle(), None, 1),
                (200, practitioner_bundle("practitioner-1", next_url=NEXT_URL), None, 1),
                (200, practitioner_bundle("practitioner-2"), None, 1),
                (200, count_bundle(), None, 1),
            ],
            requested_urls,
        ),
    )
    fetch_result = await fetch_practitioners(
        source_by_field,
        checkpoint_context(
            source_by_field,
            owner_run_id="run-current-census-retry-1",
            retry_of_run_id=ROOT_RUN_ID,
        ),
    )
    checkpoint_by_field = await checkpoint_record(database, schema)

    assert requested_urls == [NEXT_URL, count_url, start_url, NEXT_URL, count_url]
    assert fetch_result.fetch_mode == CURRENT_VERSION_CENSUS_FETCH_MODE
    assert fetch_result.complete is True
    assert fetch_result.error is None
    assert fetch_result.next_url_remaining is False
    assert checkpoint_by_field["state"] == importer.PAGINATION_CHECKPOINT_COMPLETE
    assert checkpoint_by_field["owner_run_id"] == "run-current-census-retry-1"
    assert checkpoint_by_field["retry_of_run_id"] == ROOT_RUN_ID
    assert await candidate_resource_ids(database, schema) == [
        "practitioner-1",
        "practitioner-2",
    ]
    assert await proof_shard_counts(database, schema) == (2, 2)


@pytest.mark.asyncio
async def test_postgres_direct_retry_restarts_expired_exact_census_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A verified direct retry reuses no stale opaque cursor."""

    contract = census_contract()
    source_by_field = census_source_record(contract)
    start_url = importer._resource_start_url(
        source_by_field,
        RESOURCE_TYPE,
        page_count=1,
    )
    assert start_url is not None
    count_url = current_version_census_count_url(start_url)

    async with census_database(monkeypatch) as (database, schema):
        await _run_initial_interrupted_phase(
            monkeypatch,
            source_by_field,
            database,
            schema,
            start_url,
            count_url,
            contract,
        )
        await _run_expired_cursor_phase(
            monkeypatch,
            source_by_field,
            database,
            schema,
            start_url,
            count_url,
        )
