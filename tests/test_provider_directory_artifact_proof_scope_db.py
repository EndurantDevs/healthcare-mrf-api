# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for global Provider Directory artifact source scope."""

from __future__ import annotations

import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from tests.provider_directory_subset_completion_pg_setup import (
    install_subset_canonical_functions,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _install_admission_seal_fixture_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


async def _database() -> Database:
    database = Database()
    try:
        await database.connect()
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError) as exc:
        await database.disconnect()
        pytest.skip(f"Postgres is unavailable for artifact scope proof: {exc}")
    if "test" not in database_name.lower():
        await database.disconnect()
        pytest.skip("Artifact scope proof requires a disposable test database")
    return database


async def _create_tables(database: Database, schema: str) -> None:
    """Create the minimal tables used by the production selection SQL."""

    await database.status(f"CREATE SCHEMA {schema};")
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            import_run_id varchar(64),
            acquisition_root_run_id varchar(64),
            previous_dataset_id varchar(96),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL,
            resource_count bigint NOT NULL,
            created_at timestamp,
            validated_at timestamp,
            published_at timestamp,
            superseded_at timestamp,
            publication_metadata_json jsonb,
            artifact_selection_receipt_json jsonb,
            publication_metadata_summary_json jsonb,
            publication_metadata_sha256 varchar(64),
            content_proof_admission_version smallint,
            content_proof_admission_kind varchar(32),
            content_proof_admission_sha256 varchar(64),
            content_proof_resource_types varchar(64)[],
            completion_proof_required_version integer,
            completion_proof_json jsonb,
            completion_proof_sha256 varchar(64)
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_source (
            source_id varchar(64) PRIMARY KEY,
            endpoint_id varchar(64),
            metadata_json jsonb
        );
        """
    )
    await install_subset_canonical_functions(database, schema)
    await _install_admission_seal_fixture_contract(database, schema)


async def _insert_fixture(database: Database, schema: str) -> None:
    """Insert two aliases and one proof-bound current dataset."""

    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_source
            (source_id, endpoint_id, metadata_json)
        VALUES
            ('source_a', 'endpoint_a', '{{}}'::jsonb),
            ('source_b', 'endpoint_a', '{{}}'::jsonb);
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
            status, is_current, resource_count, publication_metadata_json
        ) VALUES (
            'dataset_a', 'endpoint_a', 'root_a', repeat('a', 64),
            :published, true, 1, CAST(:metadata AS jsonb)
        );
        """,
        published=importer.ENDPOINT_DATASET_PUBLISHED,
        metadata=json.dumps(
            {
                "source_ids": ["source_a"],
                "selected_resources": ["Location"],
                "expected_resources": ["Location"],
                importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: {
                    "contract_id": (
                        "healthporta.provider-directory.content-proof.v1"
                    )
                },
            }
        ),
    )


async def _selected_rows(
    database: Database,
    *,
    select_validated_candidates: bool = False,
) -> list[tuple[str, str, str]]:
    rows = await database.all(
        importer._provider_directory_artifact_dataset_selection_sql(
            None,
            should_select_validated_candidates=select_validated_candidates,
        ),
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
        select_validated_candidates=select_validated_candidates,
    )
    return [
        (
            str(row._mapping["source_id"]),
            str(row._mapping["endpoint_id"]),
            str(row._mapping["dataset_id"]),
        )
        for row in rows
    ]


@pytest.mark.asyncio
async def test_global_artifact_scope_is_proof_bound(monkeypatch):
    """Filter sibling aliases, preserve fallback, and reject an empty scope."""
    schema = f"provider_directory_artifact_scope_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = await _database()
    try:
        await _create_tables(database, schema)
        await _insert_fixture(database, schema)
        assert await _selected_rows(database) == [("source_a", "endpoint_a", "dataset_a")]
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = publication_metadata_json
                   - '{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}';
            """
        )
        assert await _selected_rows(database) == [
            ("source_a", "endpoint_a", "dataset_a"),
            ("source_b", "endpoint_a", "dataset_a"),
        ]
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = publication_metadata_json
                   || jsonb_build_object(
                       '{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}',
                       jsonb_build_object('contract_id', 'content-proof.v1')
                   );
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_source
                (source_id, endpoint_id, metadata_json)
            VALUES ('source_unbound', 'endpoint_missing', '{{}}'::jsonb);
            """
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
                status, is_current, resource_count, publication_metadata_json
            ) VALUES (
                'dataset_missing', 'endpoint_missing', 'root_missing',
                repeat('b', 64), '{importer.ENDPOINT_DATASET_PUBLISHED}', true,
                1, jsonb_build_object(
                    'source_ids', jsonb_build_array('source_missing'),
                    'selected_resources', jsonb_build_array('Location'),
                    'expected_resources', jsonb_build_array('Location'),
                    '{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}',
                    jsonb_build_object('contract_id', 'content-proof.v1')
                )
            );
            """
        )
        assert await _selected_rows(database) == []
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


@pytest.mark.asyncio
async def test_global_artifact_scope_is_sealed_proof_bound(monkeypatch):
    """Keep a generic admission receipt bound to its source aliases."""

    schema = f"provider_directory_artifact_scope_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = await _database()
    try:
        await _create_tables(database, schema)
        await _insert_fixture(database, schema)
        proof_sha256 = "a" * 64
        summary_by_field = {
            "source_ids": ["source_a"],
            "selected_resources": ["Location"],
            "expected_resources": ["Location"],
            importer.ADMISSION_GENERIC_PROOF_SUMMARY_KEY: {
                "contract_id": "healthporta.provider-directory.content-proof.v1",
                "proof_sha256": proof_sha256,
            },
        }
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_summary_json = CAST(:summary AS jsonb),
                   content_proof_admission_version =
                       CAST(:admission_version AS smallint),
                   content_proof_admission_kind =
                       CAST(:admission_kind AS varchar),
                   content_proof_admission_sha256 =
                       CAST(:proof_sha256 AS varchar),
                   content_proof_resource_types = ARRAY['Location']::varchar[],
                   publication_metadata_sha256 =
                       {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
                           CAST(:summary AS jsonb),
                           CAST(:admission_version AS smallint),
                           CAST(:admission_kind AS text),
                           CAST(:proof_sha256 AS text),
                           ARRAY['Location']::varchar[]
                       );
            """,
            summary=json.dumps(summary_by_field),
            admission_version=importer.ADMISSION_SEAL_VERSION,
            admission_kind=importer.ADMISSION_KIND_GENERIC,
            proof_sha256=proof_sha256,
        )
        assert await database.scalar(
            f"SELECT ({importer._artifact_admission_seal_valid_sql('dataset')}) "
            f"FROM {schema}.provider_directory_endpoint_dataset AS dataset"
        ) is True

        assert await _selected_rows(database) == [
            ("source_a", "endpoint_a", "dataset_a")
        ]
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


@pytest.mark.asyncio
async def test_global_artifact_scope_preserves_cross_endpoint_candidate(
    monkeypatch,
):
    """Resolve an alias to its validated replacement endpoint."""

    schema = f"provider_directory_artifact_scope_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = await _database()
    try:
        await _create_tables(database, schema)
        await _insert_fixture(database, schema)
        await _install_cross_endpoint_candidate(database, schema)
        assert await _selected_rows(
            database,
            select_validated_candidates=True,
        ) == [("source_a", "endpoint_a", "dataset_a")]
        await _seal_cross_endpoint_candidate(database, schema)
        assert await _selected_rows(
            database,
            select_validated_candidates=True,
        ) == [("source_a", "endpoint_b", "dataset_b")]
        explicit_rows = await database.all(
            importer._provider_directory_artifact_dataset_selection_sql(
                ["source_a"],
                should_select_validated_candidates=True,
            ),
            published_status=importer.ENDPOINT_DATASET_PUBLISHED,
            validated_status=importer.ENDPOINT_DATASET_VALIDATED,
            select_validated_candidates=True,
            source_ids=["source_a"],
        )
        assert [
            (
                str(selected_record._mapping["source_id"]),
                str(selected_record._mapping["endpoint_id"]),
                str(selected_record._mapping["dataset_id"]),
            )
            for selected_record in explicit_rows
        ] == [("source_a", "endpoint_b", "dataset_b")]
    finally:
        await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _install_cross_endpoint_candidate(database: Database, schema: str) -> None:
    await database.status(
        f"DELETE FROM {schema}.provider_directory_source "
        "WHERE source_id = 'source_b';"
    )
    await database.status(
        f"""INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
            status, is_current, resource_count, publication_metadata_json
        ) VALUES (
            'dataset_b', 'endpoint_b', 'root_b', repeat('c', 64),
            '{importer.ENDPOINT_DATASET_VALIDATED}', false, 1,
            jsonb_build_object(
                'source_ids', jsonb_build_array('source_a'),
                'selected_resources', jsonb_build_array('Location'),
                'expected_resources', jsonb_build_array('Location'),
                '{importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY}',
                jsonb_build_object('contract_id', 'content-proof.v1')
            )
        );"""
    )


async def _seal_cross_endpoint_candidate(database: Database, schema: str) -> None:
    await database.status(
        f"""UPDATE {schema}.provider_directory_endpoint_dataset
           SET publication_metadata_summary_json = publication_metadata_json,
               content_proof_admission_version = {importer.ADMISSION_SEAL_VERSION},
               content_proof_admission_kind = '{importer.ADMISSION_KIND_GENERIC}',
               content_proof_admission_sha256 = repeat('a', 64),
               content_proof_resource_types = ARRAY['Location']::varchar[],
               publication_metadata_sha256 =
                   {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
                       publication_metadata_json,
                       {importer.ADMISSION_SEAL_VERSION}::smallint,
                       '{importer.ADMISSION_KIND_GENERIC}'::text,
                       repeat('a', 64), ARRAY['Location']::varchar[])
         WHERE dataset_id = 'dataset_b';"""
    )
