# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL proof for current-version FHIR census resumption."""

from __future__ import annotations

from typing import Any
import pytest

from process.provider_directory_fhir_census_binding import (
    current_version_census_count_url,
)
from process.provider_directory_fhir_census_execution import (
    CURRENT_VERSION_CENSUS_FETCH_MODE,
    CURRENT_VERSION_CENSUS_RETRYABLE_ERROR,
    current_version_census_proof_identity,
)
from tests.provider_directory_current_census_postgres_support import (
    CUTOFF,
    ENDPOINT_ID,
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
    endpoint_dataset_record,
    fetch_practitioners,
    fetch_sequence,
    importer,
    practitioner_bundle,
    proof_shard_counts,
)

def checkpoint_page_geometry() -> dict[str, int]:
    """Return the exact durable geometry after the first full window."""

    return {
        "version": 2,
        "page_count": 1,
        "checkpointed_pages": 1,
        "checkpointed_rows": 1,
        "logical_next_offset": 1,
        "sparse_pages": 0,
        "empty_pages": 0,
    }


def assert_initial_checkpoint(checkpoint: dict[str, Any], contract: Any) -> None:
    assert checkpoint["next_url"] == NEXT_URL
    assert checkpoint["state"] == importer.PAGINATION_CHECKPOINT_ACTIVE
    assert checkpoint["pages_processed"] == 1
    assert checkpoint["rows_processed"] == 1
    assert checkpoint["owner_run_id"] == ROOT_RUN_ID
    assert checkpoint["retry_of_run_id"] is None
    assert checkpoint["completeness_json"] == {
        "strategy_version": contract.strategy_version,
        "contract_identity": current_version_census_proof_identity(contract),
        "cutoff": CUTOFF,
        "resource_type": RESOURCE_TYPE,
        "pre_count": 2,
        "verified": False,
        "page_geometry": checkpoint_page_geometry(),
    }


def assert_completed_checkpoint(
    checkpoint: dict[str, Any],
    contract: Any,
    *,
    owner_run_id: str,
    retry_of_run_id: str,
) -> None:
    assert checkpoint["next_url"] is None
    assert checkpoint["state"] == importer.PAGINATION_CHECKPOINT_COMPLETE
    assert checkpoint["pages_processed"] == 2
    assert checkpoint["rows_processed"] == 2
    assert checkpoint["owner_run_id"] == owner_run_id
    assert checkpoint["retry_of_run_id"] == retry_of_run_id
    assert checkpoint["completed_at"] is not None
    assert checkpoint["completeness_json"] == {
        "strategy_version": contract.strategy_version,
        "contract_identity": current_version_census_proof_identity(contract),
        "cutoff": CUTOFF,
        "resource_type": RESOURCE_TYPE,
        "pre_count": 2,
        "post_count": 2,
        "processed_rows": 2,
        "unique_candidate_rows": 2,
        "unreturned_count": 0,
        "verified": True,
        "page_geometry": checkpoint_page_geometry(),
        "terminal_page_geometry": {
            "version": 2,
            "page_count": 1,
            "pages_processed": 2,
            "processed_rows": 2,
            "terminal_page_start_offset": 1,
            "logical_window_end_offset": 2,
            "terminal_page_entries": 1,
            "sparse_pages": 0,
            "empty_pages": 0,
        },
    }


async def _run_initial_interrupted_phase(
    monkeypatch: pytest.MonkeyPatch,
    source_record: dict[str, Any],
    database: Any,
    schema: str,
    start_url: str,
    count_url: str,
    contract: Any,
) -> dict[str, Any]:
    """Persist one page and its proof before a retryable source failure."""

    requested_urls: list[str] = []
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fetch_sequence(
            [
                (200, count_bundle(), None, 1),
                (
                    200,
                    practitioner_bundle(
                        "practitioner-1",
                        next_url=NEXT_URL,
                    ),
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
    checkpoint = await checkpoint_record(database, schema)

    assert requested_urls == [count_url, start_url, NEXT_URL]
    assert fetch_result.fetch_mode == CURRENT_VERSION_CENSUS_FETCH_MODE
    assert fetch_result.complete is False
    assert fetch_result.error == (
        f"{CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:http_500"
    )
    assert fetch_result.next_url_remaining is True
    assert_initial_checkpoint(checkpoint, contract)
    assert await candidate_resource_ids(database, schema) == [
        "practitioner-1"
    ]
    assert await proof_shard_counts(database, schema) == (1, 1)
    return checkpoint


async def _run_completed_phase(
    monkeypatch: pytest.MonkeyPatch,
    source_record: dict[str, Any],
    database: Any,
    schema: str,
    count_url: str,
    contract: Any,
) -> None:
    """Resume one transient failure and persist the verified terminal proof."""

    requested_urls: list[str] = []
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fetch_sequence(
            [
                (
                    200,
                    practitioner_bundle("practitioner-2"),
                    None,
                    1,
                ),
                (200, count_bundle(), None, 1),
            ],
            requested_urls,
        ),
    )
    fetch_result = await fetch_practitioners(
        source_record,
        checkpoint_context(
            source_record,
            owner_run_id="run-current-census-retry-1",
            retry_of_run_id=ROOT_RUN_ID,
        ),
    )
    checkpoint = await checkpoint_record(database, schema)

    assert requested_urls == [NEXT_URL, count_url]
    assert fetch_result.fetch_mode == CURRENT_VERSION_CENSUS_FETCH_MODE
    assert fetch_result.complete is True
    assert fetch_result.error is None
    assert fetch_result.next_url_remaining is False
    assert_completed_checkpoint(
        checkpoint,
        contract,
        owner_run_id="run-current-census-retry-1",
        retry_of_run_id=ROOT_RUN_ID,
    )
    assert await candidate_resource_ids(database, schema) == [
        "practitioner-1",
        "practitioner-2",
    ]
    assert await proof_shard_counts(database, schema) == (2, 2)


@pytest.mark.asyncio
async def test_postgres_resumes_transient_exact_census_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify retry ownership and completion after a transient status."""

    contract = census_contract()
    source_record = census_source_record(contract)
    start_url = importer._resource_start_url(
        source_record,
        RESOURCE_TYPE,
        page_count=1,
    )
    assert start_url is not None
    count_url = current_version_census_count_url(start_url)

    async with census_database(monkeypatch) as (database, schema):
        await _run_initial_interrupted_phase(
            monkeypatch,
            source_record,
            database,
            schema,
            start_url,
            count_url,
            contract,
        )
        await _run_completed_phase(
            monkeypatch,
            source_record,
            database,
            schema,
            count_url,
            contract,
        )


async def _run_outer_import(
    source_record: dict[str, Any],
    run_id: str,
    *,
    retry_of_run_id: str | None = None,
    pagination_resume_required: set[str] | None = None,
) -> dict[str, int]:
    return await importer._import_resources(
        [source_record],
        resources=[RESOURCE_TYPE],
        per_resource_limit=0,
        page_limit=0,
        page_count=1,
        timeout=3,
        run_id=run_id,
        stream_batch_size=1,
        is_pagination_checkpointing_enabled=True,
        defer_typed_materialization=True,
        retry_of_run_id=retry_of_run_id,
        pagination_root_run_id=(ROOT_RUN_ID if retry_of_run_id else None),
        pagination_resume_required=pagination_resume_required,
    )


async def _run_outer_resumable_phase(
    monkeypatch: pytest.MonkeyPatch,
    source_record: dict[str, Any],
    database: Any,
    schema: str,
    count_url: str,
    start_url: str,
    dataset_id: str,
) -> None:
    """Retain an acquiring candidate's exact cursor and durable page proof."""

    requested_urls: list[str] = []
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        fetch_sequence(
            [
                (200, count_bundle(), None, 1),
                (200, practitioner_bundle("practitioner-1", next_url=NEXT_URL), None, 1),
                (500, None, None, 1),
            ],
            requested_urls,
        ),
    )
    resume_required_entries: set[str] = set()
    await _run_outer_import(source_record, ROOT_RUN_ID, pagination_resume_required=resume_required_entries)

    dataset = await endpoint_dataset_record(database, schema)
    checkpoint = await checkpoint_record(database, schema)
    assert requested_urls == [count_url, start_url, NEXT_URL]
    assert resume_required_entries == {f"{source_record['source_id']}:{RESOURCE_TYPE}"}
    assert dataset["dataset_id"] == dataset_id
    assert dataset["status"] == importer.ENDPOINT_DATASET_ACQUIRING
    assert dataset["import_run_id"] == dataset["acquisition_root_run_id"] == ROOT_RUN_ID
    assert dataset["resource_count"] == 0
    assert checkpoint["dataset_id"] == dataset_id
    assert_initial_checkpoint(checkpoint, census_contract())
    assert await candidate_resource_ids(database, schema, dataset_id) == [
        "practitioner-1"
    ]
    assert await proof_shard_counts(database, schema, dataset_id) == (1, 1)


async def _run_outer_retry_phase(
    monkeypatch: pytest.MonkeyPatch,
    source_record: dict[str, Any],
    database: Any,
    schema: str,
    count_url: str,
    dataset_id: str,
) -> None:
    """Observe acquiring reuse before completing normal validation."""

    requested_urls: list[str] = []
    observed_resume_states: list[tuple[str, str, int, int]] = []

    async def fetch_retry(
        _source_record: dict[str, Any],
        request_url: str,
        *,
        timeout: int,
    ) -> tuple[int, dict[str, Any], None, int]:
        del timeout
        requested_urls.append(request_url)
        if request_url == NEXT_URL:
            dataset = await endpoint_dataset_record(database, schema)
            checkpoint = await checkpoint_record(database, schema)
            shard_count, proof_rows = await proof_shard_counts(
                database, schema, dataset_id
            )
            observed_resume_states.append(
                (dataset["status"], dataset["import_run_id"], shard_count, proof_rows)
            )
            assert dataset["dataset_id"] == checkpoint["dataset_id"] == dataset_id
            assert dataset["acquisition_root_run_id"] == ROOT_RUN_ID
            assert checkpoint["owner_run_id"] == "run-current-census-retry-outer"
            assert checkpoint["retry_of_run_id"] == ROOT_RUN_ID
            assert checkpoint["next_url"] == NEXT_URL
            assert checkpoint["pages_processed"] == checkpoint["rows_processed"] == 1
            assert await candidate_resource_ids(database, schema, dataset_id) == [
                "practitioner-1"
            ]
            return 200, practitioner_bundle("practitioner-2"), None, 1
        assert request_url == count_url
        return 200, count_bundle(), None, 1

    monkeypatch.setattr(importer, "_fetch_source_json", fetch_retry)
    counts = await _run_outer_import(
        source_record,
        "run-current-census-retry-outer",
        retry_of_run_id=ROOT_RUN_ID,
    )
    assert counts == {RESOURCE_TYPE: 1}
    assert requested_urls == [NEXT_URL, count_url]
    assert observed_resume_states == [
        (
            importer.ENDPOINT_DATASET_ACQUIRING,
            "run-current-census-retry-outer",
            1,
            1,
        )
    ]
    await _assert_outer_finalized(database, schema, dataset_id)


async def _assert_outer_finalized(
    database: Any,
    schema: str,
    dataset_id: str,
) -> None:
    """Assert immutable validation and retirement of transient proof state."""

    dataset = await endpoint_dataset_record(database, schema)
    assert dataset["dataset_id"] == dataset_id
    assert dataset["status"] == importer.ENDPOINT_DATASET_VALIDATED
    assert dataset["import_run_id"] == "run-current-census-retry-outer"
    assert dataset["acquisition_root_run_id"] == ROOT_RUN_ID
    assert dataset["resource_count"] == 2
    assert dataset["validated_at"] is not None
    assert dataset["is_current"] is False
    assert dataset["published_at"] is None
    assert len(dataset["dataset_hash"]) == 64
    diagnostic = dataset["publication_metadata_json"]["resource_diagnostics"][RESOURCE_TYPE]
    assert diagnostic["current_version_census_completeness"]["verified"] is True
    assert await checkpoint_record(database, schema) == {}
    assert await candidate_resource_ids(database, schema, dataset_id) == [
        "practitioner-1",
        "practitioner-2",
    ]
    assert await proof_shard_counts(database, schema, dataset_id) == (0, 0)


@pytest.mark.asyncio
async def test_postgres_outer_lifecycle_resumes_acquiring_census_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove acquiring reuse and normal immutable finalization."""

    async def ignore_source_metadata(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def relation_proofs(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY: {
                "complete": True,
                "edge_count": 0,
            },
            importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY: {
                "complete": True,
                "edge_count": 0,
            },
        }

    monkeypatch.setattr(importer, "_update_source_resource_import_metadata", ignore_source_metadata)
    monkeypatch.setattr(importer, "_build_endpoint_dataset_serving_relations", relation_proofs)
    contract = census_contract()
    source_record = census_source_record(contract)
    start_url = importer._resource_start_url(source_record, RESOURCE_TYPE, page_count=1)
    assert start_url is not None
    count_url = current_version_census_count_url(start_url)
    dataset_id = importer._endpoint_dataset_candidate_id(
        ENDPOINT_ID, (RESOURCE_TYPE,), ROOT_RUN_ID
    )

    async with census_database(monkeypatch, seed_dataset=False) as (database, schema):
        await _run_outer_resumable_phase(
            monkeypatch, source_record, database, schema, count_url, start_url, dataset_id
        )
        await _run_outer_retry_phase(
            monkeypatch, source_record, database, schema, count_url, dataset_id
        )
