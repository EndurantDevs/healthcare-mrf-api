# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL partition acquisition for semantic-v4 Organizations."""

from __future__ import annotations

import asyncio
import urllib.parse
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
)
from tests.provider_directory_organization_union_v4_postgres_support import (
    stored_dataset_row,
    stored_proof,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    CANONICAL_BASE,
    PROJECTION_AS_OF,
    SOURCE_ID,
    _insert_parent,
    _semantic_database,
    importer,
)


DATASET_ID = "dataset-organization-partition-v4"


def _organization(name: str, last_updated: str) -> dict[str, object]:
    """Return one raw Organization observation inside the root window."""

    return {
        "resourceType": "Organization",
        "id": "organization-semantic-union",
        "meta": {
            "versionId": "1",
            "source": CANONICAL_BASE,
            "lastUpdated": last_updated,
        },
        "active": True,
        "identifier": [
            {
                "system": "urn:example",
                "value": "organization-semantic-union",
            }
        ],
        "name": name,
    }


def _source(
    *,
    ceiling: int = 10,
    page_count: int = 10,
) -> dict[str, object]:
    """Return a complete partition-enabled semantic-v4 source."""

    return {
        "source_id": SOURCE_ID,
        "endpoint_id": f"endpoint-{DATASET_ID}"[:64],
        "api_base": CANONICAL_BASE,
        "canonical_api_base": CANONICAL_BASE,
        "_resource_hash_contract": (
            SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
        ),
        "_semantic_projection_as_of": PROJECTION_AS_OF,
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_coverage_mode": "probe_only",
            "provider_directory_fully_enumerable_resources": [],
            importer.LAST_UPDATED_PARTITION_METADATA_KEY: {
                "enabled": True,
                "resources": {
                    "Organization": {
                        "start": "2024-01-01T00:00:00Z",
                        "end": "2024-01-03T00:00:00Z",
                        "ceiling": ceiling,
                        "minimum_width_seconds": 3600,
                        "page_count": page_count,
                        "volatile_metadata_paths": ["/meta/lastUpdated"],
                    }
                },
            },
        },
    }


def _context() -> importer.PaginationCheckpointContext:
    """Return the exact parent dataset checkpoint lineage."""

    return importer.PaginationCheckpointContext(
        canonical_api_base=CANONICAL_BASE,
        source_scope_hash="scope-organization-partition-v4",
        source_ids=(SOURCE_ID,),
        owner_run_id="run-organization-partition-v4",
        acquisition_root_run_id=f"root-{DATASET_ID}"[:64],
        endpoint_id=f"endpoint-{DATASET_ID}"[:64],
        dataset_id=DATASET_ID,
        lineage_verified=True,
    )


def _bundle(resources=(), *, total=None) -> dict[str, object]:
    """Wrap raw resources in one strict searchset Bundle."""

    bundle_by_field: dict[str, object] = {
        "resourceType": "Bundle",
        "type": "searchset",
    }
    if resources:
        bundle_by_field["entry"] = [
            {"resource": resource_by_field}
            for resource_by_field in resources
        ]
    if total is not None:
        bundle_by_field["total"] = total
    return bundle_by_field


class _StablePartitionEndpoint:
    """Serve exact census totals and reversed twin-pass observations."""

    def __init__(self) -> None:
        self.page_count = 0
        self.observations = (
            _organization(
                "Community Health Center",
                "2024-01-01T01:00:00Z",
            ),
            _organization(
                "COMMUNITY HEALTH SERVICES",
                "2024-01-01T02:00:00Z",
            ),
        )

    async def fetch(self, _source_by_field, request_url, *, timeout):
        """Return one complete window or one exact count observation."""

        assert timeout == 3
        if "_summary=count" in request_url:
            return 200, _bundle(total=2), None, 1
        self.page_count += 1
        observations = (
            self.observations
            if self.page_count == 1
            else self.observations[::-1]
        )
        return 200, _bundle(observations), None, 1


class _SplitPartitionEndpoint:
    """Serve the same logical Organization from two exact leaf windows."""

    def __init__(self) -> None:
        self.page_count = 0

    @staticmethod
    def _bounds(request_url: str) -> tuple[str, ...]:
        query_by_name = urllib.parse.parse_qs(
            urllib.parse.urlsplit(request_url).query
        )
        return tuple(query_by_name.get("_lastUpdated", ()))

    def _resources(self, bounds: tuple[str, ...]):
        if bounds[0].startswith("ge2024-01-01") and bounds[1].startswith(
            "lt2024-01-02"
        ):
            return (
                _organization(
                    "Community Health Center",
                    "2024-01-01T01:00:00Z",
                ),
            )
        return (
            _organization(
                "COMMUNITY HEALTH SERVICES",
                "2024-01-02T01:00:00Z",
            ),
        )

    async def fetch(self, _source_by_field, request_url, *, timeout):
        """Return root-two, leaf-one counts and one row per leaf pass."""

        assert timeout == 3
        bounds = self._bounds(request_url)
        if "_summary=count" in request_url:
            return 200, _bundle(total=1 if len(bounds) == 2 and (
                bounds[1].startswith("lt2024-01-02")
                or bounds[0].startswith("ge2024-01-02")
            ) else 2), None, 1
        self.page_count += 1
        return 200, _bundle(self._resources(bounds)), None, 1


class _InterruptedPartitionEndpoint(_StablePartitionEndpoint):
    """Stop after pass one has committed its occurrence proof."""

    async def fetch(self, source_by_field, request_url, *, timeout):
        """Cancel before the second data pass while preserving count calls."""

        if "_summary=count" not in request_url and self.page_count == 1:
            raise asyncio.CancelledError
        return await super().fetch(
            source_by_field,
            request_url,
            timeout=timeout,
        )


def _resume(config) -> importer.LastUpdatedPartitionResume:
    """Return one fresh immutable planner rooted at the test window."""

    return importer.LastUpdatedPartitionResume(
        importer.PartitionPlan.create(
            config.start,
            config.end,
            ceiling=config.ceiling,
            minimum_width=config.minimum_width,
            volatile_metadata_paths=config.volatile_metadata_paths,
            boundary_precision=config.boundary_precision,
        )
    )


async def _run_partition(
    monkeypatch,
    *,
    source_by_field=None,
    endpoint=None,
    resume=None,
    save_callback=None,
):
    """Execute the full partition fetch with only network/checkpoint stubs."""

    source_by_field = source_by_field or _source()
    config, error = importer._last_updated_partition_config(
        source_by_field,
        "Organization",
    )
    assert config is not None and error is None
    endpoint = endpoint or _StablePartitionEndpoint()
    monkeypatch.setattr(
        importer,
        "_load_partition_plan",
        AsyncMock(return_value=resume or _resume(config)),
    )
    save_plan = save_callback or AsyncMock()
    monkeypatch.setattr(
        importer,
        "_save_last_updated_partition_plan",
        save_plan,
    )
    monkeypatch.setattr(importer, "_fetch_source_json", endpoint.fetch)
    output_batches: list[list[dict[str, object]]] = []

    async def write_rows(_model, output_rows):
        output_batches.append(output_rows)
        return len(output_rows)

    fetch_result = await importer._fetch_last_updated_partition_resource_rows(
        source_by_field,
        "Organization",
        importer.ProviderDirectoryOrganization,
        config,
        importer.LastUpdatedPartitionFetchOptions(
            per_resource_limit=0,
            page_limit=0,
            timeout=3,
            run_id="run-organization-partition-v4",
            row_batch_handler=write_rows,
            row_batch_size=10,
            retain_rows=False,
            cancel_ctx=None,
            cancel_task=None,
            deadline_seconds=0,
            pagination_checkpoint=_context(),
        ),
    )
    return fetch_result, output_batches, endpoint, save_plan


async def _partition_counts(database, schema: str) -> tuple[int, int, int]:
    """Return unique candidates and raw occurrence proof counts."""

    counts = await database.first(
        f'SELECT (SELECT count(*) FROM "{schema}".'
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
        "AND resource_type='Organization'), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
        "AND resource_type='LU:Organization:pass:1'), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
        "AND resource_type='LU:Organization:pass:2');",
        dataset_id=DATASET_ID,
    )
    return tuple(int(value) for value in counts)


@pytest.mark.asyncio
async def test_postgres_v4_partition_unions_occurrences(monkeypatch) -> None:
    """Complete reversed twin passes with raw-two to unique-one proof parity."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(
            database,
            schema,
            DATASET_ID,
            resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
            selected_resources=("Organization",),
        )
        fetch_result, output_batches, endpoint, save_plan = await _run_partition(
            monkeypatch
        )
        assert fetch_result.complete is True
        assert fetch_result.rows_fetched == 2
        assert fetch_result.rows_written == 1
        assert endpoint.page_count == 2
        assert len(output_batches) == 1
        assert output_batches[0][0]["name_variants"] == [
            "Community Health Center",
            "COMMUNITY HEALTH SERVICES",
        ]
        assert await _partition_counts(database, schema) == (1, 2, 2)
        _payload_hash, payload_by_field = await stored_dataset_row(
            database,
            schema,
            DATASET_ID,
        )
        assert payload_by_field["name_variants"] == [
            "Community Health Center",
            "COMMUNITY HEALTH SERVICES",
        ]
        proof = await stored_proof(database, schema, DATASET_ID)
        assert proof.resource_count == 1
        direct_proof = await importer._endpoint_dataset_content_proof(
            database,
            DATASET_ID,
            ("Organization",),
            verify_payload_hashes=True,
            resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
        )
        assert direct_proof.dataset_hash == proof.dataset_hash
        assert direct_proof.resource_hashes == proof.resource_hashes
        assert direct_proof.resource_count == proof.resource_count
        assert proof.metadata["semantic_union"] == {
            "added_name_count": 1,
            "collision_identities": 1,
            "observation_variants": 2,
            "union_name_count": 2,
        }
        assert save_plan.await_count >= 2


@pytest.mark.asyncio
async def test_postgres_v4_partition_unions_across_windows(monkeypatch) -> None:
    """Compose one canonical Organization from two immutable leaf proofs."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(
            database,
            schema,
            DATASET_ID,
            resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
            selected_resources=("Organization",),
        )
        endpoint = _SplitPartitionEndpoint()
        fetch_result, output_batches, _endpoint, _save_plan = await _run_partition(
            monkeypatch,
            source_by_field=_source(ceiling=1, page_count=1),
            endpoint=endpoint,
        )

        assert fetch_result.complete is True
        assert fetch_result.rows_fetched == 2
        assert fetch_result.rows_written == 1
        assert endpoint.page_count == 4
        assert len(output_batches) == 1
        assert await _partition_counts(database, schema) == (1, 2, 2)
        _payload_hash, payload_by_field = await stored_dataset_row(
            database,
            schema,
            DATASET_ID,
        )
        assert payload_by_field["name_variants"] == [
            "Community Health Center",
            "COMMUNITY HEALTH SERVICES",
        ]


@pytest.mark.asyncio
async def test_postgres_v4_partition_resumes_pass_one(monkeypatch) -> None:
    """Reuse committed pass-one occurrences before completing pass two."""

    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(
            database,
            schema,
            DATASET_ID,
            resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
            selected_resources=("Organization",),
        )
        source_by_field = _source()
        config, error = importer._last_updated_partition_config(
            source_by_field,
            "Organization",
        )
        assert config is not None and error is None
        resume = _resume(config)
        with pytest.raises(asyncio.CancelledError):
            await _run_partition(
                monkeypatch,
                source_by_field=source_by_field,
                endpoint=_InterruptedPartitionEndpoint(),
                resume=resume,
            )
        assert await _partition_counts(database, schema) == (0, 2, 0)

        fetch_result, output_batches, endpoint, _save_plan = await _run_partition(
            monkeypatch,
            source_by_field=source_by_field,
            endpoint=_StablePartitionEndpoint(),
            resume=resume,
        )
        assert fetch_result.complete is True
        assert fetch_result.rows_fetched == 2
        assert fetch_result.rows_written == 1
        assert endpoint.page_count == 1
        assert len(output_batches) == 1
        assert await _partition_counts(database, schema) == (1, 2, 2)
