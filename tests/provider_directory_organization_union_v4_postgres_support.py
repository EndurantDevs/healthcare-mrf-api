# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL fixtures for semantic-v4 Organization unions."""

from __future__ import annotations

import datetime
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from db.connection import Database
from db.models import ProviderDirectoryOrganization
from process.provider_directory_proof_store import (
    ProviderDirectoryStoredProofOptions,
    build_stored_dataset_proof,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    CANONICAL_BASE,
    PROJECTION_AS_OF,
    SOURCE_ID,
    importer,
)


RESOURCE_ID = "organization-semantic-union"
V4_DATASET_FORWARD = "dataset-organization-union-forward"
V4_DATASET_REVERSE = "dataset-organization-union-reverse"
V4_DATASET_CONCURRENT = "dataset-organization-union-concurrent"
V4_DATASET_ROLLBACK = "dataset-organization-union-rollback"


def organization_observation(
    name: str,
    *,
    aliases: list[str] | None = None,
    page_number: int,
    transport_host: str,
) -> dict[str, object]:
    """Return one complete Organization observation with volatile provenance."""

    observed_at = datetime.datetime(2026, 8, 9, 13, 0, page_number)
    return {
        "source_id": SOURCE_ID,
        "resource_id": RESOURCE_ID,
        "name": name,
        "aliases": aliases or [],
        "active": True,
        "identifiers": [
            {"system": "urn:example", "value": RESOURCE_ID}
        ],
        "resource_url": f"https://{transport_host}/{RESOURCE_ID}",
        "fhir_self_url": f"https://{transport_host}/self/{RESOURCE_ID}",
        "fhir_fetch_url": f"https://{transport_host}/page/{page_number}",
        "fhir_fetch_mode": "rest_bundle",
        "fhir_meta": {
            "versionId": "1",
            "lastUpdated": f"2026-08-09T13:00:0{page_number}Z",
            "source": CANONICAL_BASE,
        },
        "last_seen_run_id": "run-organization-union",
        "observed_at": observed_at,
        "updated_at": observed_at,
    }


def write_scope(dataset_id: str) -> importer.EndpointDatasetWriteScope:
    """Bind one Organization write to the exact v4 root identity."""

    return importer.EndpointDatasetWriteScope(
        dataset_id=dataset_id,
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
    )


async def write_page(dataset_id: str, row_by_field: dict[str, object]) -> int:
    """Write one normal Organization page through all representations."""

    return await importer._upsert_resource_rows(
        ProviderDirectoryOrganization,
        [row_by_field],
        options=importer.ProviderDirectoryResourceWriteOptions(
            run_id="run-organization-union",
            track_seen=False,
            canonical_api_base=CANONICAL_BASE,
            source_ids=[SOURCE_ID],
            dataset_scope=write_scope(dataset_id),
        ),
    )


def dataset_row(
    dataset_id: str,
    row_by_field: dict[str, object],
) -> dict[str, object]:
    """Build one exact v4 retained row for a partition write."""

    return importer._endpoint_dataset_resource_rows(
        ProviderDirectoryOrganization,
        [row_by_field],
        dataset_id=dataset_id,
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
    )[0]


async def write_partition_page(
    monkeypatch,
    dataset_id: str,
    row_by_field: dict[str, object],
) -> None:
    """Write one Organization observation through the partition path."""

    retained_row = dataset_row(dataset_id, row_by_field)
    stage = importer.LastUpdatedPartitionStage(
        rows=(retained_row,),
        candidate_hashes_by_id={
            str(retained_row["resource_id"]): str(retained_row["payload_hash"])
        },
        resource_hash_contract=SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
    )
    state = SimpleNamespace(
        plan=SimpleNamespace(failure=None),
        context=object(),
        census=object(),
        pages_fetched=1,
        rows_fetched=1,
    )
    window = SimpleNamespace(passes={1: object()})
    monkeypatch.setattr(importer, "_store_partition_pass_proof", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_save_last_updated_partition_plan",
        AsyncMock(),
    )
    await importer._persist_partition_pass(
        "Organization",
        object(),
        state,
        window,
        2,
        stage,
    )


async def stored_dataset_row(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[str, dict[str, object]]:
    """Return the retained v4 hash and decoded payload."""

    record = await database.first(
        f'SELECT payload_hash, payload_json FROM "{schema}".'
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
        "AND resource_type='Organization' AND resource_id=:resource_id;",
        dataset_id=dataset_id,
        resource_id=RESOURCE_ID,
    )
    payload_by_field = record[1]
    if isinstance(payload_by_field, str):
        payload_by_field = json.loads(payload_by_field)
    return str(record[0]), payload_by_field


async def stored_proof(
    database: Database,
    schema: str,
    dataset_id: str,
):
    """Build one sealed v4 Organization proof from durable shards."""

    return await build_stored_dataset_proof(
        database,
        schema,
        dataset_id=dataset_id,
        endpoint_id=f"endpoint-{dataset_id}"[:64],
        acquisition_root_run_id=f"root-{dataset_id}"[:64],
        source_ids=[SOURCE_ID],
        selected_resources=["Organization"],
        options=ProviderDirectoryStoredProofOptions(
            proof_resource_scope=["Organization"],
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of=PROJECTION_AS_OF,
        ),
    )


async def materialized_organization(
    database: Database,
    schema: str,
) -> tuple[dict[str, object], str]:
    """Return typed name state and canonical hash."""

    typed_record = await database.first(
        f'SELECT name, name_variants, aliases, resource_url FROM "{schema}".'
        "provider_directory_organization WHERE source_id=:source_id "
        "AND resource_id=:resource_id;",
        source_id=SOURCE_ID,
        resource_id=RESOURCE_ID,
    )
    canonical_hash = await database.scalar(
        f'SELECT payload_hash FROM "{schema}".'
        "provider_directory_canonical_resource "
        "WHERE canonical_api_base=:canonical_base "
        "AND resource_type='Organization' AND resource_id=:resource_id;",
        canonical_base=CANONICAL_BASE,
        resource_id=RESOURCE_ID,
    )
    return {
        "name": typed_record[0],
        "name_variants": list(typed_record[1]),
        "aliases": list(typed_record[2]),
        "resource_url": typed_record[3],
    }, str(canonical_hash)
