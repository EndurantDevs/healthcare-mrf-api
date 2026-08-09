# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL transaction proof for semantic Provider Directory content."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import datetime
import importlib
import json
from types import SimpleNamespace
import uuid
from unittest.mock import AsyncMock
import zlib

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from db.models import (
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryOrganization,
    ProviderDirectoryPractitioner,
    ProviderDirectorySourceResource,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    build_stored_dataset_proof,
    ensure_dataset_proof_shard_table,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    canonical_practitioner_payload,
    resource_payload_sha256_for_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


SOURCE_ID = "source-semantic-proof"
RESOURCE_ID = "practitioner-semantic-proof"
CANONICAL_BASE = "https://directory.example.test/fhir"
PROJECTION_AS_OF = "2026-08-09"
V3_DATASET_FORWARD = "dataset-semantic-forward"
V3_DATASET_REVERSE = "dataset-semantic-reverse"
V3_DATASET_CONCURRENT = "dataset-semantic-concurrent"
V3_DATASET_ROLLBACK = "dataset-semantic-rollback"


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("semantic proof tests need disposable PostgreSQL")
    if "test" not in database_name.lower():
        pytest.skip("semantic proof tests refuse a non-test database")


def _fixture_models() -> tuple[type, ...]:
    return (
        ProviderDirectoryDatasetResource,
        ProviderDirectoryPractitioner,
        ProviderDirectoryOrganization,
        ProviderDirectoryCanonicalResource,
        ProviderDirectorySourceResource,
    )


async def _create_tables(database: Database, schema: str) -> None:
    await database.status(f'CREATE SCHEMA "{schema}";')
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64) NOT NULL,
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            publication_metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_pagination_checkpoint (
            canonical_api_base text NOT NULL,
            resource_type varchar(64) NOT NULL,
            source_scope_hash varchar(64) NOT NULL,
            dataset_id varchar(96),
            source_ids jsonb NOT NULL,
            acquisition_root_run_id varchar(64) NOT NULL,
            owner_run_id varchar(64) NOT NULL,
            retry_of_run_id varchar(64),
            start_url_hash varchar(64) NOT NULL,
            next_url text,
            state varchar(32) NOT NULL,
            pages_processed bigint NOT NULL DEFAULT 0,
            rows_processed bigint NOT NULL DEFAULT 0,
            recent_cursor_hashes jsonb NOT NULL DEFAULT '[]'::jsonb,
            completeness_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            PRIMARY KEY (
                canonical_api_base,
                resource_type,
                source_scope_hash,
                acquisition_root_run_id
            )
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL REFERENCES
                "{schema}".provider_directory_endpoint_dataset(dataset_id),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            acquired_resource_sha256 varchar(64),
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        """
    )
    for model in _fixture_models()[1:]:
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model,
                schema,
                model.__tablename__,
            )
        )
        for primary_key_statement in importer._artifact_scope_pk_sql(
            model,
            schema,
            model.__tablename__,
        ):
            await database.status(primary_key_statement)
    await ensure_dataset_proof_shard_table(database, schema)


@asynccontextmanager
async def _semantic_database(monkeypatch):
    schema = f"provider_directory_semantic_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "6")
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        for model in _fixture_models():
            monkeypatch.setattr(model.__table__, "schema", schema)
        await _create_tables(database, schema)
        is_schema_created = True
        monkeypatch.setattr(importer, "db", database)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()


async def _insert_parent(
    database: Database,
    schema: str,
    dataset_id: str,
    *,
    resource_hash_contract: str = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    semantic_projection_as_of: str | None = PROJECTION_AS_OF,
    selected_resources: tuple[str, ...] = ("Practitioner",),
) -> None:
    metadata = {
        "source_ids": [SOURCE_ID],
        "selected_resources": list(selected_resources),
        "resource_hash_contract": resource_hash_contract,
        **(
            {"semantic_projection_as_of": semantic_projection_as_of}
            if semantic_projection_as_of is not None
            else {}
        ),
        **(
            {
                "proof_resource_scope": list(
                    importer._provider_directory_proof_resource_scope(
                        selected_resources
                    )
                )
            }
            if resource_hash_contract
            == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            else {}
        ),
    }
    await database.status(
        f"INSERT INTO \"{schema}\".provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, acquisition_root_run_id, status, "
        "is_current, publication_metadata_json) VALUES ("
        ":dataset_id, :endpoint_id, :root_run_id, :status, false, "
        "CAST(:metadata_json AS jsonb));",
        dataset_id=dataset_id,
        endpoint_id=f"endpoint-{dataset_id}"[:64],
        root_run_id=f"root-{dataset_id}"[:64],
        status=importer.ENDPOINT_DATASET_ACQUIRING,
        metadata_json=json.dumps(metadata),
    )


def _observation(
    full_name: str,
    *,
    page_number: int,
) -> dict[str, object]:
    family_name = full_name.split()[-1]
    given_name = full_name.split()[0]
    payload = canonical_practitioner_payload(
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
                "lastUpdated": (
                    f"2026-08-09T12:00:0{page_number}Z"
                ),
                "source": CANONICAL_BASE,
            },
            "resource_url": f"{CANONICAL_BASE}/Practitioner/{RESOURCE_ID}",
            "fhir_self_url": f"{CANONICAL_BASE}/Practitioner/{RESOURCE_ID}",
            "fhir_fetch_url": (
                f"{CANONICAL_BASE}/Practitioner?page={page_number}"
            ),
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
        **payload,
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
        run_id="run-semantic-proof",
        track_seen=False,
        canonical_api_base=CANONICAL_BASE,
        source_ids=[SOURCE_ID],
        dataset_scope=_write_scope(dataset_id),
    )


async def _write_organization_page(
    dataset_id: str,
    row: dict[str, object],
) -> int:
    return await importer._upsert_resource_rows(
        ProviderDirectoryOrganization,
        [row],
        run_id="run-semantic-proof",
        track_seen=False,
        canonical_api_base=CANONICAL_BASE,
        source_ids=[SOURCE_ID],
        dataset_scope=_write_scope(dataset_id),
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
    row: dict[str, object],
) -> None:
    dataset_row = _dataset_row(dataset_id, row)
    stage = importer.LastUpdatedPartitionStage(
        rows=(dataset_row,),
        candidate_hashes_by_id={
            str(dataset_row["resource_id"]): str(
                dataset_row["payload_hash"]
            )
        },
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
    record = await database.first(
        f"SELECT payload_hash, payload_json FROM \"{schema}\"."
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
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
    records = await database.all(
        f"SELECT input_sha256, payload_bytes FROM \"{schema}\"."
        f'"{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
        "WHERE dataset_id=:dataset_id ORDER BY shard_id;",
        dataset_id=dataset_id,
    )
    input_hashes: list[str] = []
    proof_records: list[list[object]] = []
    for input_hash, payload_bytes in records:
        input_hashes.append(str(input_hash))
        proof_records.extend(
            json.loads(line)
            for line in zlib.decompress(bytes(payload_bytes)).splitlines()
        )
    return sorted(input_hashes), proof_records


async def _proof(
    database: Database,
    schema: str,
    dataset_id: str,
):
    return await build_stored_dataset_proof(
        database,
        schema,
        dataset_id=dataset_id,
        endpoint_id=f"endpoint-{dataset_id}"[:64],
        acquisition_root_run_id=f"root-{dataset_id}"[:64],
        source_ids=[SOURCE_ID],
        selected_resources=["Practitioner"],
        proof_resource_scope=["Practitioner"],
        expected_resource_hash_contract=(
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
        expected_semantic_projection_as_of=PROJECTION_AS_OF,
    )


async def _dataset_and_shard_counts(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[int, int]:
    counts = await database.first(
        f"SELECT (SELECT count(*) FROM \"{schema}\"."
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id), "
        f'(SELECT count(*) FROM "{schema}".'
        f'"{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
        "WHERE dataset_id=:dataset_id);",
        dataset_id=dataset_id,
    )
    return int(counts[0]), int(counts[1])


async def _compatibility_counts(
    database: Database,
    schema: str,
) -> tuple[int, int, int]:
    counts = await database.first(
        f'SELECT (SELECT count(*) FROM "{schema}".'
        "provider_directory_canonical_resource), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_source_resource), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_practitioner);"
    )
    return int(counts[0]), int(counts[1]), int(counts[2])


@pytest.mark.asyncio
async def test_postgres_v3_union_is_page_order_independent_across_write_paths(
    monkeypatch,
):
    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(database, schema, V3_DATASET_FORWARD)
        await _insert_parent(database, schema, V3_DATASET_REVERSE)
        first = _observation("Alex Example", page_number=1)
        second = _observation("Avery Sample", page_number=2)

        await _write_page(V3_DATASET_FORWARD, first)
        await _write_page(V3_DATASET_FORWARD, second)
        await _write_partition_page(
            monkeypatch,
            V3_DATASET_REVERSE,
            second,
        )
        await _write_partition_page(
            monkeypatch,
            V3_DATASET_REVERSE,
            first,
        )

        forward_hash, forward_payload = await _stored_dataset_row(
            database,
            schema,
            V3_DATASET_FORWARD,
        )
        reverse_hash, reverse_payload = await _stored_dataset_row(
            database,
            schema,
            V3_DATASET_REVERSE,
        )
        assert forward_payload == reverse_payload
        assert forward_hash == reverse_hash
        assert len(forward_payload["names"]) == 2

        forward_proof = await _proof(
            database,
            schema,
            V3_DATASET_FORWARD,
        )
        reverse_proof = await _proof(
            database,
            schema,
            V3_DATASET_REVERSE,
        )
        assert (
            forward_proof.dataset_hash,
            forward_proof.resource_hashes,
            forward_proof.resource_counts,
        ) == (
            reverse_proof.dataset_hash,
            reverse_proof.resource_hashes,
            reverse_proof.resource_counts,
        )
        assert forward_proof.metadata["semantic_union"] == {
            "added_name_count": 1,
            "collision_identities": 1,
            "observation_variants": 2,
            "union_name_count": 2,
        }
        direct_proof = await importer._endpoint_dataset_content_proof(
            database,
            V3_DATASET_FORWARD,
            ("Practitioner",),
            verify_payload_hashes=True,
            resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
        )
        assert direct_proof.dataset_hash == forward_proof.dataset_hash

        forward_inputs, forward_records = await _shard_records(
            database,
            schema,
            V3_DATASET_FORWARD,
        )
        reverse_inputs, reverse_records = await _shard_records(
            database,
            schema,
            V3_DATASET_REVERSE,
        )
        assert forward_inputs == reverse_inputs
        assert len(forward_records) == len(reverse_records) == 2
        assert all(
            len(record) == 10
            and record[7] == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            and len(record[9]) == 1
            for record in [*forward_records, *reverse_records]
        )

        typed_names = await database.scalar(
            f"SELECT names FROM \"{schema}\".provider_directory_practitioner "
            "WHERE source_id=:source_id AND resource_id=:resource_id;",
            source_id=SOURCE_ID,
            resource_id=RESOURCE_ID,
        )
        canonical_record = await database.first(
            f"SELECT payload_hash, payload_json FROM \"{schema}\"."
            "provider_directory_canonical_resource "
            "WHERE canonical_api_base=:canonical_base "
            "AND resource_type='Practitioner' AND resource_id=:resource_id;",
            canonical_base=CANONICAL_BASE,
            resource_id=RESOURCE_ID,
        )
        assert list(typed_names) == forward_payload["names"]
        assert canonical_record[0] == forward_hash
        assert canonical_record[1] is None


@pytest.mark.asyncio
async def test_postgres_v3_concurrent_first_insert_is_serialized(monkeypatch):
    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(database, schema, V3_DATASET_CONCURRENT)
        first_at_typed_write = asyncio.Event()
        release_first_write = asyncio.Event()
        original_upsert_rows = importer._upsert_rows
        block_first_typed_write = True

        async def controlled_upsert(model, rows, **kwargs):
            nonlocal block_first_typed_write
            if model is ProviderDirectoryPractitioner and block_first_typed_write:
                block_first_typed_write = False
                first_at_typed_write.set()
                await release_first_write.wait()
            return await original_upsert_rows(model, rows, **kwargs)

        monkeypatch.setattr(importer, "_upsert_rows", controlled_upsert)
        first_task = asyncio.create_task(
            _write_page(
                V3_DATASET_CONCURRENT,
                _observation("Alex Example", page_number=1),
            )
        )
        second_task: asyncio.Task | None = None
        try:
            await asyncio.wait_for(first_at_typed_write.wait(), timeout=2)
            second_task = asyncio.create_task(
                _write_page(
                    V3_DATASET_CONCURRENT,
                    _observation("Avery Sample", page_number=2),
                )
            )
            await asyncio.sleep(0.1)
            assert second_task.done() is False
            release_first_write.set()
            await asyncio.gather(first_task, second_task)
        finally:
            release_first_write.set()
            await asyncio.gather(
                first_task,
                *([second_task] if second_task is not None else []),
                return_exceptions=True,
            )

        _payload_hash, dataset_payload = await _stored_dataset_row(
            database,
            schema,
            V3_DATASET_CONCURRENT,
        )
        assert len(dataset_payload["names"]) == 2
        assert await _dataset_and_shard_counts(
            database,
            schema,
            V3_DATASET_CONCURRENT,
        ) == (1, 2)
        typed_names = await database.scalar(
            f"SELECT names FROM \"{schema}\".provider_directory_practitioner "
            "WHERE source_id=:source_id AND resource_id=:resource_id;",
            source_id=SOURCE_ID,
            resource_id=RESOURCE_ID,
        )
        canonical_record = await database.first(
            f"SELECT payload_hash, payload_json FROM \"{schema}\"."
            "provider_directory_canonical_resource "
            "WHERE canonical_api_base=:canonical_base "
            "AND resource_type='Practitioner' AND resource_id=:resource_id;",
            canonical_base=CANONICAL_BASE,
            resource_id=RESOURCE_ID,
        )
        assert list(typed_names) == dataset_payload["names"]
        assert canonical_record[0] == _payload_hash
        assert canonical_record[1] is None


@pytest.mark.asyncio
async def test_postgres_v3_distinct_resource_families_remain_parallel(
    monkeypatch,
):
    """Keep one slow semantic family from serializing sibling families."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-parallel-families"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            selected_resources=("Organization", "Practitioner"),
        )
        first_family_locked = asyncio.Event()
        release_first_family = asyncio.Event()
        second_family_complete = asyncio.Event()
        original_existing_resources = (
            importer._existing_endpoint_dataset_resources
        )

        async def controlled_existing_resources(
            executor,
            observed_dataset_id,
            resource_type,
            resource_ids,
        ):
            if resource_type == "Practitioner":
                first_family_locked.set()
                await release_first_family.wait()
            return await original_existing_resources(
                executor,
                observed_dataset_id,
                resource_type,
                resource_ids,
            )

        monkeypatch.setattr(
            importer,
            "_existing_endpoint_dataset_resources",
            controlled_existing_resources,
        )
        practitioner_row = _dataset_row(
            dataset_id,
            _observation("Alex Example", page_number=1),
        )
        organization_payload = {
            "resource_id": "organization-semantic-proof",
            "name": "Example Organization",
            "fhir_meta": {
                "versionId": "1",
                "lastUpdated": "2026-08-09T12:00:01Z",
            },
        }
        organization_row = {
            "dataset_id": dataset_id,
            "resource_type": "Organization",
            "resource_id": organization_payload["resource_id"],
            "payload_hash": resource_payload_sha256_for_contract(
                organization_payload,
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            ),
            "payload_json": organization_payload,
            "acquired_resource_sha256": None,
        }

        async def write_rows(rows):
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    rows,
                    persist_content_proof=True,
                    resource_hash_contract=(
                        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                    ),
                    semantic_projection_as_of=PROJECTION_AS_OF,
                )

        async def write_second_family():
            await write_rows([organization_row])
            second_family_complete.set()

        first_task = asyncio.create_task(write_rows([practitioner_row]))
        second_task: asyncio.Task | None = None
        try:
            await asyncio.wait_for(first_family_locked.wait(), timeout=2)
            second_task = asyncio.create_task(write_second_family())
            await asyncio.wait_for(second_family_complete.wait(), timeout=2)
            assert first_task.done() is False
            release_first_family.set()
            await asyncio.gather(first_task, second_task)
        finally:
            release_first_family.set()
            await asyncio.gather(
                first_task,
                *([second_task] if second_task is not None else []),
                return_exceptions=True,
            )

        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (2, 2)


@pytest.mark.asyncio
async def test_postgres_v3_non_practitioner_materialization_uses_retained_winner(
    monkeypatch,
):
    """Keep typed and canonical provenance aligned with deterministic v3 data."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-organization-winner"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            selected_resources=("Organization",),
        )
        winning_observation = _organization_observation(
            "z.example.test",
            page_number=2,
        )
        losing_observation = _organization_observation(
            "a.example.test",
            page_number=1,
        )

        await _write_organization_page(dataset_id, winning_observation)
        await _write_organization_page(dataset_id, losing_observation)

        dataset_record = await database.first(
            f'SELECT payload_hash, payload_json FROM "{schema}".'
            "provider_directory_dataset_resource "
            "WHERE dataset_id=:dataset_id AND resource_type='Organization' "
            "AND resource_id=:resource_id;",
            dataset_id=dataset_id,
            resource_id="organization-semantic-proof",
        )
        dataset_payload = dataset_record[1]
        if isinstance(dataset_payload, str):
            dataset_payload = json.loads(dataset_payload)
        typed_record = await database.first(
            f'SELECT resource_url, fhir_meta FROM "{schema}".'
            "provider_directory_organization WHERE source_id=:source_id "
            "AND resource_id=:resource_id;",
            source_id=SOURCE_ID,
            resource_id="organization-semantic-proof",
        )
        canonical_record = await database.first(
            f'SELECT resource_url, fhir_meta, payload_hash FROM "{schema}".'
            "provider_directory_canonical_resource "
            "WHERE canonical_api_base=:canonical_base "
            "AND resource_type='Organization' AND resource_id=:resource_id;",
            canonical_base=CANONICAL_BASE,
            resource_id="organization-semantic-proof",
        )

        assert dataset_payload["resource_url"] == (
            "https://z.example.test/organization-semantic-proof"
        )
        assert typed_record[0] == dataset_payload["resource_url"]
        assert typed_record[1] == dataset_payload["fhir_meta"]
        assert canonical_record[0] == dataset_payload["resource_url"]
        assert canonical_record[1] == dataset_payload["fhir_meta"]
        assert canonical_record[2] == dataset_record[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_hash_contract",
    [
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    ],
)
async def test_postgres_checkpoint_clear_waits_for_same_family_writer(
    monkeypatch,
    resource_hash_contract,
):
    """Serialize reset cleanup with both historical and semantic writers."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = f"dataset-clear-{resource_hash_contract}"
        projection_as_of = (
            PROJECTION_AS_OF
            if resource_hash_contract
            == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            else None
        )
        await _insert_parent(
            database,
            schema,
            dataset_id,
            resource_hash_contract=resource_hash_contract,
            semantic_projection_as_of=projection_as_of,
        )
        incoming_row = importer._endpoint_dataset_resource_rows(
            ProviderDirectoryPractitioner,
            [_observation("Alex Example", page_number=1)],
            dataset_id=dataset_id,
            resource_hash_contract=resource_hash_contract,
        )[0]
        writer_has_family_lock = asyncio.Event()
        release_writer = asyncio.Event()
        original_family_lock = (
            importer._lock_endpoint_dataset_resource_family
        )

        async def controlled_family_lock(executor, observed_id, resource_type):
            await original_family_lock(executor, observed_id, resource_type)
            if asyncio.current_task().get_name() == "semantic-writer":
                writer_has_family_lock.set()
                await release_writer.wait()

        monkeypatch.setattr(
            importer,
            "_lock_endpoint_dataset_resource_family",
            controlled_family_lock,
        )

        async def write_candidate():
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [incoming_row],
                    persist_content_proof=True,
                    resource_hash_contract=resource_hash_contract,
                    semantic_projection_as_of=projection_as_of,
                )

        checkpoint_context = importer.PaginationCheckpointContext(
            canonical_api_base=CANONICAL_BASE,
            source_scope_hash="scope-semantic-clear",
            source_ids=(SOURCE_ID,),
            owner_run_id="run-semantic-proof",
            acquisition_root_run_id=f"root-{dataset_id}"[:64],
            endpoint_id=f"endpoint-{dataset_id}"[:64],
            dataset_id=dataset_id,
            lineage_verified=True,
        )
        writer_task = asyncio.create_task(
            write_candidate(),
            name="semantic-writer",
        )
        clear_task: asyncio.Task | None = None
        try:
            await asyncio.wait_for(writer_has_family_lock.wait(), timeout=2)
            clear_task = asyncio.create_task(
                importer._clear_checkpoint_dataset_resource_type(
                    checkpoint_context,
                    "Practitioner",
                ),
                name="checkpoint-clear",
            )
            await asyncio.sleep(0.1)
            assert clear_task.done() is False
            release_writer.set()
            await asyncio.gather(writer_task, clear_task)
        finally:
            release_writer.set()
            await asyncio.gather(
                writer_task,
                *([clear_task] if clear_task is not None else []),
                return_exceptions=True,
            )

        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (0, 0)


@pytest.mark.asyncio
async def test_postgres_v3_proof_failure_rolls_back_row_and_shard(monkeypatch):
    async with _semantic_database(monkeypatch) as (database, schema):
        await _insert_parent(database, schema, V3_DATASET_ROLLBACK)
        original_persist_shard = importer.persist_dataset_proof_shard

        async def fail_after_shard(*args, **kwargs):
            await original_persist_shard(*args, **kwargs)
            raise RuntimeError("synthetic-proof-failure")

        monkeypatch.setattr(
            importer,
            "persist_dataset_proof_shard",
            fail_after_shard,
        )
        with pytest.raises(RuntimeError, match="synthetic-proof-failure"):
            await importer._persist_endpoint_dataset_rows(
                ProviderDirectoryPractitioner,
                [_observation("Alex Example", page_number=1)],
                V3_DATASET_ROLLBACK,
                resource_hash_contract=(
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                semantic_projection_as_of=PROJECTION_AS_OF,
            )
        assert await _dataset_and_shard_counts(
            database,
            schema,
            V3_DATASET_ROLLBACK,
        ) == (0, 0)


@pytest.mark.asyncio
async def test_postgres_v3_typed_failure_rolls_back_all_representations(
    monkeypatch,
):
    """Hold the parent transaction through proof, edges, and typed rows."""

    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-semantic-typed-rollback"
        await _insert_parent(database, schema, dataset_id)
        original_upsert_rows = importer._upsert_rows

        async def fail_typed_write(model, rows, **kwargs):
            if model is ProviderDirectoryPractitioner:
                raise RuntimeError("synthetic-typed-failure")
            return await original_upsert_rows(model, rows, **kwargs)

        monkeypatch.setattr(importer, "_upsert_rows", fail_typed_write)
        with pytest.raises(RuntimeError, match="synthetic-typed-failure"):
            await _write_page(
                dataset_id,
                _observation("Alex Example", page_number=1),
            )

        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (0, 0)
        assert await _compatibility_counts(database, schema) == (0, 0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("parent_contract", "parent_date", "expected_error"),
    [
        (
            TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
            None,
            "hash_contract_changed",
        ),
        (
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            "2026-08-10",
            "projection_date_changed",
        ),
    ],
)
async def test_postgres_v3_parent_contract_and_projection_date_are_fenced(
    monkeypatch,
    parent_contract,
    parent_date,
    expected_error,
):
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = f"dataset-fence-{expected_error}"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            resource_hash_contract=parent_contract,
            semantic_projection_as_of=parent_date,
        )
        incoming_row = _dataset_row(
            dataset_id,
            _observation("Alex Example", page_number=1),
        )
        with pytest.raises(RuntimeError, match=expected_error):
            async with database.acquire() as connection:
                await importer._upsert_dataset_resource_rows_on_connection(
                    connection,
                    [incoming_row],
                    persist_content_proof=True,
                    resource_hash_contract=(
                        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                    ),
                    semantic_projection_as_of=PROJECTION_AS_OF,
                )
        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_hash_contract",
    [
        LEGACY_RESOURCE_HASH_CONTRACT,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    ],
)
async def test_postgres_historical_hash_contracts_remain_writable(
    monkeypatch,
    resource_hash_contract,
):
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = f"dataset-{resource_hash_contract}"
        await _insert_parent(
            database,
            schema,
            dataset_id,
            resource_hash_contract=resource_hash_contract,
            semantic_projection_as_of=None,
        )
        payload = {
            "resource_id": "organization-historical",
            "name": "Example Organization",
            "resource_url": f"{CANONICAL_BASE}/Organization/historical",
        }
        dataset_row = {
            "dataset_id": dataset_id,
            "resource_type": "Organization",
            "resource_id": payload["resource_id"],
            "payload_hash": resource_payload_sha256_for_contract(
                payload,
                resource_hash_contract,
            ),
            "payload_json": payload,
            "acquired_resource_sha256": None,
        }
        async with database.acquire() as connection:
            await importer._upsert_dataset_resource_rows_on_connection(
                connection,
                [dataset_row],
                persist_content_proof=True,
                resource_hash_contract=resource_hash_contract,
                semantic_projection_as_of=None,
            )
        stored_proof = await build_stored_dataset_proof(
            database,
            schema,
            dataset_id=dataset_id,
            endpoint_id=f"endpoint-{dataset_id}"[:64],
            acquisition_root_run_id=f"root-{dataset_id}"[:64],
            source_ids=[SOURCE_ID],
            selected_resources=["Organization"],
            expected_resource_hash_contract=resource_hash_contract,
        )
        assert stored_proof.resource_count == 1
        assert await _dataset_and_shard_counts(
            database,
            schema,
            dataset_id,
        ) == (1, 1)
