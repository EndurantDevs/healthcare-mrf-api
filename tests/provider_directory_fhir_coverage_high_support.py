# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
from unittest.mock import AsyncMock

from db.models import ProviderDirectoryPractitioner


importer = importlib.import_module("process.provider_directory_fhir")


def pagination_checkpoint_context(**overrides):
    field_map = {
        "canonical_api_base": "https://example.test/fhir",
        "source_scope_hash": "scope-hash",
        "source_ids": ("source-1",),
        "owner_run_id": "run-new",
        "acquisition_root_run_id": "root-run",
        "retry_of_run_id": "run-old",
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "lineage_verified": True,
    }
    field_map.update(overrides)
    return importer.PaginationCheckpointContext(**field_map)


def endpoint_dataset_candidate(**overrides):
    field_map = {
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "root-run",
        "source_ids": ("source-1",),
        "selected_resources": ("Practitioner",),
        "expected_resources": ("Practitioner",),
        "import_run_id": "run-new",
        "previous_dataset_id": None,
    }
    field_map.update(overrides)
    return importer.EndpointDatasetCandidate(**field_map)


def last_updated_partition_config():
    return importer.LastUpdatedPartitionConfig(
        start=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        end=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        end_mode="exclusive",
        ceiling=10,
        minimum_width=datetime.timedelta(hours=1),
        boundary_precision=datetime.timedelta(microseconds=1),
        page_count=10,
        volatile_metadata_paths=(),
    )


def last_updated_partition_plan():
    partition_config = last_updated_partition_config()
    return importer.PartitionPlan.create(
        partition_config.start,
        partition_config.end,
        ceiling=partition_config.ceiling,
        minimum_width=partition_config.minimum_width,
        volatile_metadata_paths=partition_config.volatile_metadata_paths,
        boundary_precision=partition_config.boundary_precision,
    )


def last_updated_partition_state(*, plan=None, census=None, deadline_at=None):
    return importer.LastUpdatedPartitionState(
        context=pagination_checkpoint_context(),
        plan=plan or last_updated_partition_plan(),
        census=census or importer.LastUpdatedCompletenessCensus(),
        start_url="https://example.test/fhir/Practitioner",
        pages_fetched=0,
        rows_fetched=0,
        deadline_at=deadline_at,
    )


def last_updated_partition_fetch_options(**overrides):
    field_map = {
        "per_resource_limit": 0,
        "page_limit": 0,
        "timeout": 3,
        "run_id": "root-run",
        "row_batch_handler": AsyncMock(return_value=0),
        "row_batch_size": 10,
        "retain_rows": False,
        "cancel_ctx": None,
        "cancel_task": None,
        "deadline_seconds": 0,
        "pagination_checkpoint": pagination_checkpoint_context(),
    }
    field_map.update(overrides)
    return importer.LastUpdatedPartitionFetchOptions(**field_map)


def resource_fetch_result(*, error="expected-error"):
    return importer.ResourceFetchResult(
        model=ProviderDirectoryPractitioner,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=0,
        complete=False,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
        error=error,
    )
