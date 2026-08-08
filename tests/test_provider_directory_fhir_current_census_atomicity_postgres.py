# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL proof for exact-census page/checkpoint atomicity."""

from __future__ import annotations

from typing import Any

import pytest

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


async def _run_failed_checkpoint_page(
    monkeypatch: pytest.MonkeyPatch,
    database: Any,
    schema: str,
    source_record: dict[str, Any],
    context: Any,
    start_url: str,
) -> Any:
    """Fail after a visible page write and prove its transaction rolled back."""

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
            ],
            [],
        ),
    )

    async def fail_checkpoint(*_args: Any, **_kwargs: Any) -> None:
        assert await candidate_resource_ids(database, schema) == [
            "practitioner-1"
        ]
        raise RuntimeError("checkpoint failed")

    monkeypatch.setattr(importer, "_save_pagination_checkpoint", fail_checkpoint)
    with pytest.raises(RuntimeError, match="checkpoint failed"):
        await fetch_practitioners(source_record, context)
    assert await candidate_resource_ids(database, schema) == []
    assert await proof_shard_counts(database, schema) == (0, 0)
    failed_checkpoint = await checkpoint_record(database, schema)
    assert failed_checkpoint["next_url"] == start_url
    assert failed_checkpoint["pages_processed"] == 0
    assert failed_checkpoint["rows_processed"] == 0


async def _resume_rolled_back_page(
    monkeypatch: pytest.MonkeyPatch,
    source_record: dict[str, Any],
    context: Any,
    checkpoint_write: Any,
) -> Any:
    """Resume the unchanged page and complete both exact logical windows."""

    monkeypatch.setattr(
        importer,
        "_save_pagination_checkpoint",
        checkpoint_write,
    )
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fetch_sequence(
            [
                (
                    200,
                    practitioner_bundle("practitioner-1", next_url=NEXT_URL),
                    None,
                    1,
                ),
                (200, practitioner_bundle("practitioner-2"), None, 1),
                (200, count_bundle(), None, 1),
            ],
            [],
        ),
    )
    return await fetch_practitioners(source_record, context)


@pytest.mark.asyncio
async def test_postgres_rolls_back_rows_when_checkpoint_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Roll back an exact page and resume it from the unchanged checkpoint."""

    source_record = census_source_record(census_contract())
    start_url = importer._resource_start_url(
        source_record,
        RESOURCE_TYPE,
        page_count=1,
    )
    assert start_url is not None

    async with census_database(monkeypatch) as (database, schema):
        real_checkpoint_write = importer._save_pagination_checkpoint
        context = checkpoint_context(
            source_record,
            owner_run_id=ROOT_RUN_ID,
            retry_of_run_id=None,
        )
        await _run_failed_checkpoint_page(
            monkeypatch,
            database,
            schema,
            source_record,
            context,
            start_url,
        )
        completed = await _resume_rolled_back_page(
            monkeypatch,
            source_record,
            context,
            real_checkpoint_write,
        )
        assert completed.complete is True
        assert await candidate_resource_ids(database, schema) == [
            "practitioner-1",
            "practitioner-2",
        ]
        assert await proof_shard_counts(database, schema) == (2, 2)
