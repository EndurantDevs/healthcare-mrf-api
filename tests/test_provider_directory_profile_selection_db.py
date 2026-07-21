# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_snapshot as snapshot


def _catalog_map() -> dict[str, object]:
    return {
        "catalog_digest": "a" * 64,
        "items": [
            {
                "entry_id": "payer",
                "runnable": True,
                "profile_enabled": True,
                "source_ids": ["pdfhir_test_payer"],
            }
        ],
    }


async def _profile_test_database() -> Database:
    database = Database()
    try:
        await database.connect()
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError) as exc:
        await database.disconnect()
        pytest.skip(f"Postgres is unavailable for Profile proof: {exc}")
    is_schema_test_enabled = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_ALLOW_SCHEMA_TESTS",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    if "test" not in database_name.lower() and not is_schema_test_enabled:
        await database.disconnect()
        pytest.skip("Profile proof requires a disposable test database")
    return database


async def _create_selection_tables(database: Database, schema: str) -> None:
    """Create the narrow selection and authority schema used by this proof."""

    await database.status(f"CREATE SCHEMA {schema};")
    await _create_selection_source_tables(database, schema)
    await _create_selection_authority_tables(database, schema)


async def _create_selection_source_tables(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_api_endpoint (
            endpoint_id varchar(64) PRIMARY KEY
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_source (
            source_id varchar(64) PRIMARY KEY,
            endpoint_id varchar(64),
            canonical_api_base text,
            org_name varchar(256),
            plan_name varchar(512)
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL,
            resource_count bigint NOT NULL,
            validated_at timestamp,
            published_at timestamp,
            superseded_at timestamp,
            publication_metadata_json jsonb
        );
        """
    )


async def _create_selection_authority_tables(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_profile_selection_authority (
            authority_key varchar(16) PRIMARY KEY,
            last_revision bigint NOT NULL,
            created_at timestamp NOT NULL,
            updated_at timestamp NOT NULL
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_profile_selection_proof (
            input_identity_digest varchar(64) PRIMARY KEY,
            proof_id varchar(64) NOT NULL UNIQUE,
            identity_json jsonb NOT NULL,
            created_at timestamp NOT NULL
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_profile_selection_observation (
            authority_revision bigint PRIMARY KEY,
            input_identity_digest varchar(64) NOT NULL REFERENCES
                {schema}.provider_directory_profile_selection_proof
                (input_identity_digest),
            payload_json jsonb NOT NULL,
            created_at timestamp NOT NULL
        );
        """
    )


async def _seed_current_selection(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_api_endpoint (endpoint_id)
        VALUES ('endpoint-1');
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base, org_name, plan_name
        ) VALUES (
            'pdfhir_test_payer', 'endpoint-1',
            'https://payer.example/fhir', 'Payer', 'Payer Plan'
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
            status, is_current, resource_count, validated_at, published_at,
            superseded_at, publication_metadata_json
        ) VALUES (
            'dataset-1', 'endpoint-1', 'run-root-1', :dataset_hash,
            'published', true, 42, now(), now(), NULL,
            CAST(:publication_metadata AS jsonb)
        );
        """,
        dataset_hash="b" * 64,
        publication_metadata=json.dumps(
            {
                "source_ids": ["pdfhir_test_payer"],
                "selected_resources": ["Practitioner"],
            }
        ),
    )


@contextlib.asynccontextmanager
async def _selection_database(monkeypatch):
    schema = f"profile_selection_{uuid.uuid4().hex[:12]}"
    database = await _profile_test_database()
    is_schema_created = False
    try:
        await _create_selection_tables(database, schema)
        is_schema_created = True
        await _seed_current_selection(database, schema)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "test-node")
        monkeypatch.setattr(selection, "db", database)
        monkeypatch.setattr(snapshot, "db", database)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _attest_current(request_map, catalog_map):
    return await selection.attest_profile_selection(request_map, catalog_map)


async def _set_source_name(database: Database, schema: str, name: str) -> None:
    await database.status(
        f"UPDATE {schema}.provider_directory_source "
        "SET org_name = :org_name WHERE source_id = 'pdfhir_test_payer';",
        org_name=name,
    )


async def _assert_old_observation_rejected(first_map, catalog_map) -> None:
    with pytest.raises(
        selection.ProviderDirectoryProfileSelectionStale,
        match="proof_not_registered",
    ):
        await selection.assert_registered_profile_selection_current(
            selection.validated_profile_selection_attestation(first_map),
            catalog_map,
        )


async def _assert_authority_rows(database: Database, schema: str) -> None:
    proof_count = await database.scalar(
        f"SELECT COUNT(*) FROM {schema}.provider_directory_profile_selection_proof;"
    )
    observation_count = await database.scalar(
        f"SELECT COUNT(*) FROM "
        f"{schema}.provider_directory_profile_selection_observation;"
    )
    revision = await database.scalar(
        f"SELECT last_revision FROM "
        f"{schema}.provider_directory_profile_selection_authority "
        "WHERE authority_key = 'global';"
    )
    assert (proof_count, observation_count, revision) == (2, 3, 3)


@pytest.mark.asyncio
async def test_parallel_attestation_observes_a_b_a_and_rejects_old_revisions(
    monkeypatch,
):
    """Prove concurrent replay and monotonic revisions in disposable Postgres."""

    async with _selection_database(monkeypatch) as (database, schema):
        catalog_map = _catalog_map()
        computed_selection = await snapshot._compute_current_selection(
            catalog_map,
            node_id="test-node",
            lock_selection=False,
        )
        request_map = selection._expected_request(computed_selection, "test-node")
        first_map, replay_map = await asyncio.gather(
            _attest_current(request_map, catalog_map),
            _attest_current(request_map, catalog_map),
        )

        assert first_map == replay_map
        assert first_map["authority_revision"] == 1
        await _set_source_name(database, schema, "Renamed Payer")
        second_map = await _attest_current(request_map, catalog_map)

        assert second_map["authority_revision"] == 2
        assert second_map["proof_id"] != first_map["proof_id"]
        await _set_source_name(database, schema, "Payer")
        third_map = await _attest_current(request_map, catalog_map)
        third_replay_map = await _attest_current(request_map, catalog_map)

        assert third_map["authority_revision"] == 3
        assert third_map["proof_id"] == first_map["proof_id"]
        assert third_replay_map == third_map
        await _assert_old_observation_rejected(first_map, catalog_map)
        await _assert_authority_rows(database, schema)
