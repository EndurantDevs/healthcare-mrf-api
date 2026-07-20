# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import dataclasses
import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")


def _candidate() -> importer.EndpointDatasetCandidate:
    resources = ("Organization", "Practitioner")
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_1",
        dataset_id="dataset_candidate",
        acquisition_root_run_id="root_candidate",
        source_ids=("source_a", "source_b"),
        selected_resources=resources,
        expected_resources=resources,
        import_run_id="root_candidate",
        previous_dataset_id="dataset_current",
        requires_twin_root_verification=True,
        verification_campaign_id="reviewed-candidates-v1",
        verification_source_scope_hash="scope-v1",
    )


def _baseline(candidate: importer.EndpointDatasetCandidate) -> dict[str, object]:
    content = importer.EndpointDatasetContentProof(
        dataset_hash="a" * 64,
        resource_count=2,
        resource_hashes={"Organization": "b" * 64, "Practitioner": "c" * 64},
        resource_counts={"Organization": 1, "Practitioner": 1},
    )
    proof = importer._twin_root_content_proof(candidate, content)
    proof["acquisition_root_run_id"] = "root_baseline"
    return {
        "dataset_id": "dataset_baseline",
        "endpoint_id": candidate.endpoint_id,
        "acquisition_root_run_id": "root_baseline",
        "dataset_hash": content.dataset_hash,
        "status": importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        "resource_count": content.resource_count,
        "publication_metadata_json": {
            "source_ids": list(candidate.source_ids),
            "selected_resources": list(candidate.selected_resources),
            "expected_resources": list(candidate.expected_resources),
            "requires_twin_root_verification": True,
            importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: (
                candidate.verification_campaign_id
            ),
            importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
                candidate.verification_source_scope_hash
            ),
            importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
                importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
            ),
            importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: None,
            importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
                "role": "baseline",
                "admission_role": importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
                "result": "baseline_recorded",
                "proof": proof,
            }
        },
    }


async def _disposable_database() -> Database:
    database = Database()
    try:
        await database.connect()
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError) as exc:
        await database.disconnect()
        pytest.skip(f"Postgres is unavailable for twin-root DB proof: {exc}")
    if "test" not in database_name.lower():
        await database.disconnect()
        pytest.skip("Twin-root DB proof requires a disposable test database")
    return database


async def _create_dataset_table(database: Database, schema: str) -> None:
    await database.status(f"CREATE SCHEMA {schema};")
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64),
            previous_dataset_id varchar(96),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            resource_count bigint NOT NULL DEFAULT 0,
            created_at timestamp DEFAULT now(),
            validated_at timestamp,
            published_at timestamp,
            superseded_at timestamp,
            publication_metadata_json jsonb
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


async def _insert_baseline(
    database: Database,
    schema: str,
    baseline: dict[str, object],
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id,
            dataset_hash, status, resource_count,
            publication_metadata_json
        ) VALUES (
            :dataset_id, :endpoint_id, :root_run_id,
            :dataset_hash, :status, :resource_count,
            CAST(:metadata AS jsonb)
        );
        """,
        dataset_id=baseline["dataset_id"],
        endpoint_id=baseline["endpoint_id"],
        root_run_id=baseline["acquisition_root_run_id"],
        dataset_hash=baseline["dataset_hash"],
        status=baseline["status"],
        resource_count=baseline["resource_count"],
        metadata=json.dumps(baseline["publication_metadata_json"]),
    )


async def _insert_terminal_successor(
    database: Database,
    schema: str,
    candidate: importer.EndpointDatasetCandidate,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, status,
            publication_metadata_json
        ) VALUES (
            'dataset_terminal', :endpoint_id, 'root_terminal', :status,
            CAST(:metadata AS jsonb)
        );
        """,
        endpoint_id=candidate.endpoint_id,
        status=importer.ENDPOINT_DATASET_FAILED,
        metadata=json.dumps(
            {
                "requires_twin_root_verification": True,
                importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: (
                    candidate.verification_campaign_id
                ),
                importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
                    candidate.verification_source_scope_hash
                ),
                importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
                    importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
                ),
                importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: (
                    "dataset_baseline"
                ),
            }
        ),
    )


async def _set_terminal_successor_status(
    database: Database,
    schema: str,
    status: str,
    is_current: bool,
) -> None:
    await database.status(
        f"""
        UPDATE {schema}.provider_directory_endpoint_dataset
           SET status = :status, is_current = :is_current
         WHERE dataset_id = 'dataset_terminal';
        """,
        status=status,
        is_current=is_current,
    )


async def _assert_terminal_successor_fence(
    database: Database,
    schema: str,
    candidate: importer.EndpointDatasetCandidate,
) -> None:
    await _insert_terminal_successor(database, schema, candidate)
    for terminal_status, is_current in (
        (importer.ENDPOINT_DATASET_FAILED, False),
        (importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH, False),
        (importer.ENDPOINT_DATASET_VALIDATED, False),
        (importer.ENDPOINT_DATASET_PUBLISHED, True),
        (importer.ENDPOINT_DATASET_SUPERSEDED, False),
    ):
        await _set_terminal_successor_status(
            database, schema, terminal_status, is_current
        )
        async with database.acquire() as connection:
            with pytest.raises(RuntimeError, match="active_conflict"):
                await importer._assert_no_conflicting_endpoint_candidate(
                    connection, candidate
                )

    retry_candidate = dataclasses.replace(
        candidate,
        dataset_id="dataset_terminal",
        acquisition_root_run_id="root_terminal",
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
        verification_baseline_dataset_id="dataset_baseline",
    )
    await _set_terminal_successor_status(
        database, schema, importer.ENDPOINT_DATASET_FAILED, False
    )
    async with database.acquire() as connection:
        await importer._assert_no_conflicting_endpoint_candidate(
            connection, retry_candidate
        )

    established_candidate = dataclasses.replace(
        candidate, requires_twin_root_verification=False
    )
    async with database.acquire() as connection:
        await importer._assert_no_conflicting_endpoint_candidate(
            connection, established_candidate
        )


async def _assert_failed_first_root_allows_match(
    database: Database,
    schema: str,
    candidate: importer.EndpointDatasetCandidate,
) -> None:
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, status,
            publication_metadata_json
        ) VALUES (
            'dataset_failed_first', :endpoint_id, 'root_failed_first', :status,
            CAST(:metadata AS jsonb)
        );
        """,
        endpoint_id=candidate.endpoint_id,
        status=importer.ENDPOINT_DATASET_FAILED,
        metadata=json.dumps(
            {
                "requires_twin_root_verification": True,
                importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: (
                    candidate.verification_campaign_id
                ),
                importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
                    candidate.verification_source_scope_hash
                ),
                importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
                    importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
                ),
                importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: None,
            }
        ),
    )
    async with database.acquire() as connection:
        admitted = await importer._assert_no_conflicting_endpoint_candidate(
            connection, candidate
        )
    assert admitted.verification_role == (
        importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
    )
    assert admitted.verification_baseline_dataset_id == "dataset_baseline"


async def _assert_generation_isolation(
    database: Database,
    schema: str,
    candidate: importer.EndpointDatasetCandidate,
) -> None:
    next_generation = dataclasses.replace(
        candidate,
        verification_campaign_id="reviewed-candidates-v2",
    )
    async with database.acquire() as connection:
        admitted = await importer._assert_no_conflicting_endpoint_candidate(
            connection, next_generation
        )
    assert admitted.verification_role == (
        importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, status,
            publication_metadata_json
        ) VALUES (
            'dataset_active_old_generation', :endpoint_id,
            'root_active_old_generation', :status, '{{}}'::jsonb
        );
        """,
        endpoint_id=candidate.endpoint_id,
        status=importer.ENDPOINT_DATASET_ACQUIRING,
    )
    async with database.acquire() as connection:
        with pytest.raises(RuntimeError, match="active_conflict"):
            await importer._assert_no_conflicting_endpoint_candidate(
                connection, next_generation
            )
    await database.status(
        f"""
        DELETE FROM {schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset_active_old_generation';
        """
    )


async def _assert_baseline_row_lock(
    database: Database,
    schema: str,
    candidate: importer.EndpointDatasetCandidate,
    baseline: dict[str, object],
) -> None:
    async with database.acquire() as holder:
        state = await importer._locked_endpoint_verification_state(
            holder, candidate
        )
        assert importer._compatible_twin_root_baseline(candidate, state)
        with pytest.raises(Exception) as lock_error:
            async with database.acquire() as contender:
                await contender.status("SET LOCAL lock_timeout = '100ms';")
                await contender.status(
                    f"""
                    UPDATE {schema}.provider_directory_endpoint_dataset
                       SET resource_count = resource_count + 1
                     WHERE dataset_id = :dataset_id;
                    """,
                    dataset_id=baseline["dataset_id"],
                )
        assert "lock" in str(lock_error.value).lower()


async def _artifact_option_ids(database: Database, schema: str) -> list[str]:
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, status,
            is_current, resource_count
        ) VALUES
            ('artifact_current', 'artifact_endpoint', 'artifact_root_0',
             :published, true, 1),
            ('artifact_baseline', 'artifact_endpoint', 'artifact_root_1',
             :baseline, false, 1),
            ('artifact_validated', 'artifact_endpoint', 'artifact_root_2',
             :validated, false, 1),
            ('artifact_mismatch', 'artifact_endpoint', 'artifact_root_3',
             :mismatch, false, 1);
        """,
        published=importer.ENDPOINT_DATASET_PUBLISHED,
        baseline=importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        validated=importer.ENDPOINT_DATASET_VALIDATED,
        mismatch=importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
    )
    option_rows = await database.all(
        f"""
        WITH {importer._artifact_dataset_options_cte(
            f'{schema}.provider_directory_endpoint_dataset'
        )}
        SELECT dataset_id FROM dataset_options
         WHERE endpoint_id = 'artifact_endpoint'
         ORDER BY dataset_id;
        """,
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
    )
    return [
        dataset_record._mapping["dataset_id"]
        for dataset_record in option_rows
    ]


async def test_postgres_baseline_lock_and_artifact_selection_are_fail_closed(
    monkeypatch,
):
    """Prove the baseline row lock and non-publishable status filtering."""
    schema = f"provider_directory_twin_root_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = await _disposable_database()

    is_schema_created = False
    candidate = _candidate()
    baseline = _baseline(candidate)
    try:
        await _create_dataset_table(database, schema)
        is_schema_created = True
        await _insert_baseline(database, schema, baseline)
        await _assert_baseline_row_lock(database, schema, candidate, baseline)
        await _assert_failed_first_root_allows_match(
            database, schema, candidate
        )
        await _assert_terminal_successor_fence(database, schema, candidate)
        await _assert_generation_isolation(
            database, schema, candidate
        )
        assert await _artifact_option_ids(database, schema) == [
            "artifact_current",
            "artifact_validated",
        ]
        status_length = await database.scalar(
            """
            SELECT character_maximum_length
              FROM information_schema.columns
             WHERE table_schema = :schema_name
               AND table_name = 'provider_directory_endpoint_dataset'
               AND column_name = 'status';
            """,
            schema_name=schema,
        )
        assert int(status_length) == 32
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def test_postgres_stores_verification_baseline_without_status_parameter_ambiguity(
    monkeypatch,
):
    """Execute the terminal dataset update through asyncpg's real type inference."""
    schema = f"provider_directory_status_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = await _disposable_database()

    is_schema_created = False
    candidate = _candidate()
    try:
        await _create_dataset_table(database, schema)
        is_schema_created = True
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                status, is_current, resource_count
            ) VALUES (:dataset_id, :endpoint_id, :acquisition_root_run_id,
                      :status, false, 0);
            """,
            dataset_id=candidate.dataset_id,
            endpoint_id=candidate.endpoint_id,
            acquisition_root_run_id=candidate.acquisition_root_run_id,
            status=importer.ENDPOINT_DATASET_ACQUIRING,
        )
        await importer._store_validated_endpoint_dataset(
            database,
            candidate,
            candidate.previous_dataset_id,
            "d" * 64,
            288_056,
            {"verification": "baseline"},
            status=importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        )
        stored = await database.first(
            f"""
            SELECT previous_dataset_id, dataset_hash, status, resource_count,
                   validated_at, publication_metadata_json
            FROM {schema}.provider_directory_endpoint_dataset
            WHERE dataset_id = :dataset_id;
            """,
            dataset_id=candidate.dataset_id,
        )
        assert stored is not None
        stored_map = stored._mapping
        assert stored_map["previous_dataset_id"] == candidate.previous_dataset_id
        assert stored_map["dataset_hash"] == "d" * 64
        assert stored_map["status"] == importer.ENDPOINT_DATASET_VERIFICATION_BASELINE
        assert stored_map["resource_count"] == 288_056
        assert stored_map["validated_at"] is None
        assert stored_map["publication_metadata_json"] == {"verification": "baseline"}
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()
