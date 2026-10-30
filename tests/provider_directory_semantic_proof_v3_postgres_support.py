# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared PostgreSQL fixtures for semantic Provider Directory proof tests."""

from __future__ import annotations

import datetime
import hashlib
import importlib
import json
import zlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from db.connection import Database
from db.models import (
    ProviderDirectoryOrganization,
    ProviderDirectoryPractitioner,
)
from process.provider_directory_dataset_rehydrate_types import RehydrationRequest, RehydrationRuntime
from process.provider_directory_generic_source_adapters import completed_source_scope, source_family_rows
from process.provider_directory_generic_source_policy import source_transaction
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    ProviderDirectoryStoredProofOptions,
    build_stored_dataset_proof,
)
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    canonical_practitioner_payload,
    is_semantic_resource_hash_contract,
)
from tests.cms_output_snapshot_postgres_support import provision_native_outputs
from tests.provider_directory_dataset_rehydrate_postgres_support import _migrations as rehydration_migrations
from tests.provider_directory_source_postgres_support import source_component_database, source_component_templates

importer = importlib.import_module("process.provider_directory_fhir")


SOURCE_ID = "source-semantic-proof"
RESOURCE_ID = "practitioner-semantic-proof"
CANONICAL_BASE = "https://directory.example.test/fhir"
PROJECTION_AS_OF = "2026-08-09"
V3_DATASET_FORWARD = "dataset-semantic-forward"
V3_DATASET_REVERSE = "dataset-semantic-reverse"
V3_DATASET_CONCURRENT = "dataset-semantic-concurrent"
V3_DATASET_ROLLBACK = "dataset-semantic-rollback"


@pytest.fixture(scope="module", autouse=True)
async def semantic_source_template():
    """Reuse closed migrations while each test provisions fresh Source custody."""
    async with source_component_templates():
        yield


@asynccontextmanager
async def _semantic_database(monkeypatch, *, resource_json_type="jsonb"):
    """Use the genuine Source Factory, Publisher, Owner, and native migrations."""
    assert resource_json_type in {"json", "jsonb"}

    async def prepare_inputs(component):
        await provision_native_outputs(component)
        if resource_json_type == "json":
            # Match the current resource model before Source custody or candidates exist.
            relation = importer._unscoped_qt(component.policy.schema, "provider_directory_dataset_resource")
            async with component.admin_engine.begin() as connection:
                assert await connection.scalar(text(f"SELECT count(*) FROM {relation}")) == 0
                await connection.execute(
                    text(f"ALTER TABLE {relation} ALTER COLUMN payload_json TYPE json USING payload_json::json")
                )
                assert await connection.scalar(
                    text(
                        "SELECT atttypid='pg_catalog.json'::regtype AND attnotnull FROM pg_attribute "
                        "WHERE attrelid=to_regclass(:relation) AND attname='payload_json' AND NOT attisdropped"
                    ),
                    {"relation": relation},
                )

    async with source_component_database(
        monkeypatch,
        before_provision=prepare_inputs,
        migration_prefixes=rehydration_migrations(),
    ) as component:
        database, schema = component.database, component.policy.schema
        database.semantic_candidates = {}
        database.semantic_component = component
        database.semantic_published = set()
        monkeypatch.setattr(importer, "db", database)
        async with source_transaction(database):
            await database.status(
                f'INSERT INTO "{schema}".provider_directory_source '
                "(source_id,org_name,canonical_api_base,requires_registration,requires_api_key) "
                "VALUES (:source_id,'Semantic fixture',:base,false,false)",
                source_id=SOURCE_ID,
                base=CANONICAL_BASE,
            )
        yield database, schema


async def _insert_parent(
    database: Database,
    schema: str,
    dataset_id: str,
    *,
    resource_hash_contract: str = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    semantic_projection_as_of: str | None = PROJECTION_AS_OF,
    selected_resources: tuple[str, ...] = ("Practitioner",),
) -> None:
    """Admit a real private Source candidate with the original semantic identity."""
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id=f"endpoint-{dataset_id}"[:64],
        dataset_id=dataset_id,
        acquisition_root_run_id=f"root-{dataset_id}"[:64],
        source_ids=(SOURCE_ID,),
        selected_resources=selected_resources,
        expected_resources=selected_resources,
        import_run_id="run-semantic-proof",
        previous_dataset_id=None,
        resource_hash_contract=resource_hash_contract,
        semantic_projection_as_of=semantic_projection_as_of,
        proof_resource_scope=(
            importer._provider_directory_proof_resource_scope(selected_resources)
            if is_semantic_resource_hash_contract(resource_hash_contract)
            else None
        ),
    )
    async with source_transaction(database):
        await database.status(
            f'INSERT INTO "{schema}".provider_directory_api_endpoint '
            "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
            "VALUES (:endpoint_id,:base,:credential_hash,repeat('d',64))",
            endpoint_id=candidate.endpoint_id,
            base=CANONICAL_BASE,
            credential_hash=hashlib.sha256(dataset_id.encode()).hexdigest(),
        )
        database.semantic_candidates[dataset_id] = await importer._ensure_endpoint_dataset_candidate(candidate)


@asynccontextmanager
async def _completed_semantic_scope(database, dataset_id):
    """Observe real completed heaps, then roll preparation back so intake can continue."""
    async with source_transaction(database, isolation="repeatable read") as session:
        if dataset_id in database.semantic_published:
            yield
            return
        savepoint = await session.begin_nested()
        try:
            async with completed_source_scope(importer, database.semantic_candidates[dataset_id]):
                yield
        finally:
            await savepoint.rollback()


async def _publish_semantic_source(database, dataset_id):
    """Validate and publish the acquired Source with the real admission and attach path."""
    candidate = database.semantic_candidates[dataset_id]
    proof = await _candidate_stored_proof(database, candidate)
    async with source_transaction(database):
        await database.status(
            f'UPDATE "{importer._schema()}".provider_directory_source SET endpoint_id=:endpoint WHERE source_id=:source',
            endpoint=candidate.endpoint_id,
            source=SOURCE_ID,
        )
    diagnostics_by_resource = {
        resource: {"rows_fetched": proof.resource_counts.get(resource, 0), "complete": True}
        for resource in candidate.selected_resources
    }
    validated = await importer._validate_endpoint_dataset_candidate(candidate, diagnostics_by_resource)
    assert validated["validated"] and not validated["published"]
    async with source_transaction(database):
        await importer._publish_validated_artifact_dataset(
            importer.ProviderDirectoryArtifactDataset(
                SOURCE_ID,
                candidate.endpoint_id,
                dataset_id,
                candidate.acquisition_root_run_id,
                dataset_hash=validated["dataset_hash"],
                selected_resources=candidate.selected_resources,
                retained_resources=tuple(sorted(proof.resource_counts)),
            )
        )
    database.semantic_published.add(dataset_id)


async def _materialize_semantic_dataset(database, dataset_id):
    """Build and atomically publish typed, canonical, and source-edge output stages."""
    if dataset_id not in database.semantic_published:
        await _publish_semantic_source(database, dataset_id)
    candidate = database.semantic_candidates[dataset_id]
    runtime = RehydrationRuntime(
        database=database,
        schema=importer._schema(),
        models_by_type=importer.RESOURCE_MODELS_BY_TYPE,
        upsert_batch=importer._reject_rehydration_upsert,
        directory=importer,
    )
    request = RehydrationRequest(
        SOURCE_ID,
        dataset_id,
        candidate.acquisition_root_run_id,
        "semantic-output-" + hashlib.sha256(dataset_id.encode()).hexdigest()[:32],
        batch_size=1,
    )
    return await importer._rehydrate_prepared_dataset(runtime, request)


async def _candidate_stored_proof(database, candidate):
    """Read all original proof fields under the genuine Source transaction."""
    async with source_transaction(database, isolation="repeatable read"):
        return await build_stored_dataset_proof(
            database,
            importer._schema(),
            dataset_id=candidate.dataset_id,
            endpoint_id=candidate.endpoint_id,
            acquisition_root_run_id=candidate.acquisition_root_run_id,
            source_ids=[SOURCE_ID],
            selected_resources=candidate.selected_resources,
            options=ProviderDirectoryStoredProofOptions(
                proof_resource_scope=candidate.proof_resource_scope,
                expected_resource_hash_contract=candidate.resource_hash_contract,
                expected_semantic_projection_as_of=candidate.semantic_projection_as_of,
            ),
        )


async def _admin_update_semantic_input(database, dataset_id, statement, parameters):
    """Inject native input drift as the explicit independent administrator fault actor."""
    async with source_transaction(database):
        binding = await database.scalar(
            f'SELECT candidate_storage_json FROM "{importer._schema()}".provider_directory_endpoint_dataset '
            "WHERE dataset_id=:dataset_id",
            dataset_id=dataset_id,
        )
    binding = json.loads(binding) if isinstance(binding, str) else binding
    relation = importer._unscoped_qt(importer._schema(), binding["tables"]["resource_input"]["name"])
    async with database.semantic_component.admin_engine.begin() as connection:
        await connection.execute(text(f"UPDATE {relation} {statement}"), parameters)


def _observation(
    full_name: str,
    *,
    page_number: int,
) -> dict[str, object]:
    family_name = full_name.split()[-1]
    given_name = full_name.split()[0]
    practitioner_payload_by_field = canonical_practitioner_payload(
        {
            "resource_id": RESOURCE_ID,
            "npi": 1000000000,
            "active": True,
            "names": [
                {
                    "use": "official",
                    "family": family_name,
                    "given": [given_name],
                    "text": full_name,
                }
            ],
            "fhir_meta": {
                "versionId": "1",
                "lastUpdated": (f"2026-08-09T12:00:0{page_number}Z"),
                "source": CANONICAL_BASE,
            },
            "resource_url": f"{CANONICAL_BASE}/Practitioner/{RESOURCE_ID}",
            "fhir_self_url": f"{CANONICAL_BASE}/Practitioner/{RESOURCE_ID}",
            "fhir_fetch_url": (f"{CANONICAL_BASE}/Practitioner?page={page_number}"),
            "fhir_fetch_mode": "rest_bundle",
        }
    )
    observed_at = datetime.datetime(
        2026,
        8,
        9,
        12,
        0,
        page_number,
    )
    return {
        **practitioner_payload_by_field,
        "source_id": SOURCE_ID,
        "last_seen_run_id": "run-semantic-proof",
        "observed_at": observed_at,
        "updated_at": observed_at,
    }


def _organization_observation(
    transport_host: str,
    *,
    page_number: int,
) -> dict[str, object]:
    resource_id = "organization-semantic-proof"
    observed_at = datetime.datetime(2026, 8, 9, 13, 0, page_number)
    return {
        "source_id": SOURCE_ID,
        "resource_id": resource_id,
        "name": "Example Organization",
        "active": True,
        "resource_url": f"https://{transport_host}/{resource_id}",
        "fhir_self_url": f"https://{transport_host}/self/{resource_id}",
        "fhir_fetch_url": f"https://{transport_host}/page/{page_number}",
        "fhir_fetch_mode": "rest_bundle",
        "fhir_meta": {
            "versionId": "1",
            "lastUpdated": f"2026-08-09T13:00:0{page_number}Z",
            "source": CANONICAL_BASE,
        },
        "last_seen_run_id": "run-semantic-proof",
        "observed_at": observed_at,
        "updated_at": observed_at,
    }


def _write_scope(dataset_id: str) -> importer.EndpointDatasetWriteScope:
    return importer.EndpointDatasetWriteScope(
        dataset_id=dataset_id,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
    )


async def _write_page(dataset_id: str, row: dict[str, object]) -> int:
    return await importer._upsert_resource_rows(
        ProviderDirectoryPractitioner,
        [row],
        options=importer.ProviderDirectoryResourceWriteOptions(
            run_id="run-semantic-proof",
            track_seen=False,
            canonical_api_base=CANONICAL_BASE,
            source_ids=[SOURCE_ID],
            dataset_scope=_write_scope(dataset_id),
        ),
    )


async def _write_organization_page(
    dataset_id: str,
    row: dict[str, object],
) -> int:
    return await importer._upsert_resource_rows(
        ProviderDirectoryOrganization,
        [row],
        options=importer.ProviderDirectoryResourceWriteOptions(
            run_id="run-semantic-proof",
            track_seen=False,
            canonical_api_base=CANONICAL_BASE,
            source_ids=[SOURCE_ID],
            dataset_scope=_write_scope(dataset_id),
        ),
    )


def _dataset_row(
    dataset_id: str,
    row: dict[str, object],
) -> dict[str, object]:
    return importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitioner,
        [row],
        dataset_id=dataset_id,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )[0]


async def _write_partition_page(
    monkeypatch,
    dataset_id: str,
    resource_row_by_field: dict[str, object],
) -> None:
    dataset_row = _dataset_row(dataset_id, resource_row_by_field)
    stage = importer.LastUpdatedPartitionStage(
        rows=(dataset_row,),
        candidate_hashes_by_id={str(dataset_row["resource_id"]): str(dataset_row["payload_hash"])},
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
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
    monkeypatch.setattr(
        importer,
        "_store_partition_pass_proof",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_save_last_updated_partition_plan",
        AsyncMock(),
    )
    await importer._persist_partition_pass(
        "Practitioner",
        object(),
        state,
        window,
        2,
        stage,
    )


async def _stored_dataset_row(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[str, dict[str, object]]:
    async with _completed_semantic_scope(database, dataset_id):
        record = await database.first(
            f"SELECT payload_hash, payload_json FROM {importer._qt(schema, 'provider_directory_dataset_resource')} "
            "WHERE dataset_id=:dataset_id "
            "AND resource_type='Practitioner' AND resource_id=:resource_id;",
            dataset_id=dataset_id,
            resource_id=RESOURCE_ID,
        )
        payload = record[1]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return str(record[0]), payload


async def _shard_records(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[list[str], list[list[object]]]:
    async with source_transaction(database, isolation="repeatable read"):
        records = await database.all(
            f'SELECT input_sha256, payload_bytes FROM "{schema}".'
            f'"{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
            "WHERE dataset_id=:dataset_id ORDER BY shard_id;",
            dataset_id=dataset_id,
        )
        input_hashes: list[str] = []
        proof_records: list[list[object]] = []
        for input_hash, payload_bytes in records:
            input_hashes.append(str(input_hash))
            proof_records.extend(json.loads(line) for line in zlib.decompress(bytes(payload_bytes)).splitlines())
        return sorted(input_hashes), proof_records


async def _proof(
    database: Database,
    schema: str,
    dataset_id: str,
):
    return await _candidate_stored_proof(database, database.semantic_candidates[dataset_id])


async def _dataset_and_shard_counts(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[int, int]:
    if dataset_id in database.semantic_published:
        async with source_transaction(database):
            count = await database.scalar(
                f'SELECT count(*) FROM "{schema}".provider_directory_dataset_resource WHERE dataset_id=:dataset_id',
                dataset_id=dataset_id,
            )
    else:
        count = 0
        for family in database.semantic_candidates[dataset_id].selected_resources:
            count += len(
                await source_family_rows(importer, dataset_id, (family, f"LU:{family}:pass:1", f"LU:{family}:pass:2"))
            )
    async with source_transaction(database):
        shards = await database.scalar(
            f'SELECT count(*) FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" WHERE dataset_id=:dataset_id',
            dataset_id=dataset_id,
        )
    return int(count), int(shards)


async def _compatibility_counts(
    database: Database,
    schema: str,
) -> tuple[int, int, int]:
    async with source_transaction(database):
        counts = await database.first(
            f'SELECT (SELECT count(*) FROM "{schema}".'
            "provider_directory_canonical_resource), "
            f'(SELECT count(*) FROM "{schema}".'
            "provider_directory_source_resource), "
            f'(SELECT count(*) FROM "{schema}".'
            "provider_directory_practitioner);"
        )
        return int(counts[0]), int(counts[1]), int(counts[2])
