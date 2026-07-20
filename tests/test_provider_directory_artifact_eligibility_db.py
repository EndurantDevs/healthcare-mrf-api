# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import copy
import importlib
import json
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")

ENDPOINT_ID = "candidate_endpoint"
SERVING_ENDPOINT_ID = "serving_endpoint_old"
CAMPAIGN_ID = "reviewed-candidate-v1"
SOURCE_SCOPE_HASH = "scope-v1"
DATASET_HASH = "a" * 64
RESOURCE_COUNT = 2


async def _disposable_database() -> Database:
    database = Database()
    try:
        await database.connect()
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError) as exc:
        await database.disconnect()
        pytest.skip(f"Postgres is unavailable for artifact gate proof: {exc}")
    if "test" not in database_name.lower():
        await database.disconnect()
        pytest.skip("Artifact gate proof requires a disposable test database")
    return database


async def _create_tables(database: Database, schema: str) -> None:
    await database.status(f"CREATE SCHEMA {schema};")
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            resource_count bigint NOT NULL DEFAULT 0,
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


def _source_metadata(
    status: str | None = importer.PROVIDER_DIRECTORY_TWIN_ROOT_PENDING,
    *,
    campaign_id: str | None = CAMPAIGN_ID,
    configured_endpoint_id: str = ENDPOINT_ID,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        importer.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY: (
            configured_endpoint_id
        ),
        "provider_directory_supported_resources": [
            "Organization",
            "Practitioner",
        ],
        "provider_directory_fully_enumerable_resources": [
            "Organization",
            "Practitioner",
        ],
    }
    if status is not None:
        metadata["provider_directory_candidate_status"] = status
    if campaign_id is not None:
        metadata[
            importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
        ] = campaign_id
    return metadata


def _content_proof(root_run_id: str) -> dict[str, object]:
    return {
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": root_run_id,
        "source_ids": ["source_a", "source_b"],
        "selected_resources": ["Organization", "Practitioner"],
        "expected_resources": ["Organization", "Practitioner"],
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: CAMPAIGN_ID,
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: SOURCE_SCOPE_HASH,
        "dataset_hash": DATASET_HASH,
        "resource_count": RESOURCE_COUNT,
        "resource_hashes": {
            "Organization": "b" * 64,
            "Practitioner": "c" * 64,
        },
        "resource_counts": {"Organization": 1, "Practitioner": 1},
    }


def _baseline_metadata() -> dict[str, object]:
    return {
        "source_ids": ["source_a", "source_b"],
        "selected_resources": ["Organization", "Practitioner"],
        "expected_resources": ["Organization", "Practitioner"],
        "requires_twin_root_verification": True,
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: CAMPAIGN_ID,
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: SOURCE_SCOPE_HASH,
        importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
            importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE
        ),
        importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: None,
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "role": "baseline",
            "admission_role": importer.TWIN_ROOT_BASELINE_CANDIDATE_ROLE,
            "result": "baseline_recorded",
            "proof": _content_proof("root_baseline"),
        },
    }


def _matched_metadata() -> dict[str, object]:
    return {
        "source_ids": ["source_a", "source_b"],
        "selected_resources": ["Organization", "Practitioner"],
        "expected_resources": ["Organization", "Practitioner"],
        "requires_twin_root_verification": True,
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: CAMPAIGN_ID,
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: SOURCE_SCOPE_HASH,
        importer.TWIN_ROOT_VERIFICATION_ROLE_KEY: (
            importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
        ),
        importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: (
            "dataset_baseline"
        ),
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "role": importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
            "admission_role": (
                importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE
            ),
            "result": "matched",
            "mismatch_fields": [],
            "baseline_dataset_id": "dataset_baseline",
            "baseline_acquisition_root_run_id": "root_baseline",
            "proof": _content_proof("root_matched"),
        },
    }


def _ordinary_metadata() -> dict[str, object]:
    return {
        "source_ids": ["source_a", "source_b"],
        "selected_resources": ["Organization", "Practitioner"],
        "expected_resources": ["Organization", "Practitioner"],
        "requires_twin_root_verification": False,
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: CAMPAIGN_ID,
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: SOURCE_SCOPE_HASH,
        "dataset_hash": DATASET_HASH,
        "resource_count": RESOURCE_COUNT,
        "completion_proof_v1": {
            importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: CAMPAIGN_ID,
            importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: (
                SOURCE_SCOPE_HASH
            ),
        },
    }


async def _insert_sources(database: Database, schema: str) -> None:
    metadata = json.dumps(_source_metadata())
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_source (
            source_id, endpoint_id, metadata_json
        ) VALUES
            ('source_a', :serving_endpoint_id, CAST(:metadata AS jsonb)),
            ('source_b', :serving_endpoint_id, CAST(:metadata AS jsonb));
        """,
        serving_endpoint_id=SERVING_ENDPOINT_ID,
        metadata=metadata,
    )


async def _insert_core_datasets(database: Database, schema: str) -> None:
    matched_metadata = _matched_metadata()
    wrong_campaign_metadata = copy.deepcopy(matched_metadata)
    wrong_campaign_metadata[
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY
    ] = "reviewed-candidate-v2"
    wrong_campaign_metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY][
        "proof"
    ][importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY] = "reviewed-candidate-v2"
    missing_source_ids_metadata = copy.deepcopy(matched_metadata)
    missing_source_ids_metadata.pop("source_ids")
    nonarray_source_ids_metadata = copy.deepcopy(matched_metadata)
    nonarray_source_ids_metadata["source_ids"] = {"source_a": True}
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash,
            status, is_current, resource_count, publication_metadata_json
        ) VALUES
            ('dataset_current', :endpoint_id, 'root_current', :dataset_hash,
             :published, true, :resource_count, '{{}}'::jsonb),
            ('dataset_baseline', :endpoint_id, 'root_baseline', :dataset_hash,
             :baseline, false, :resource_count, CAST(:baseline_metadata AS jsonb)),
            ('dataset_exact_matched', :endpoint_id, 'root_matched', :dataset_hash,
             :validated, false, :resource_count, CAST(:matched_metadata AS jsonb)),
            ('dataset_legacy', :endpoint_id, 'root_legacy', :dataset_hash,
             :validated, false, :resource_count,
             CAST(:legacy_metadata AS jsonb)),
            ('dataset_wrong_campaign', :endpoint_id, 'root_wrong', :dataset_hash,
             :validated, false, :resource_count, CAST(:wrong_metadata AS jsonb)),
            ('dataset_missing_sources', :endpoint_id, 'root_missing', :dataset_hash,
             :validated, false, :resource_count, CAST(:missing_metadata AS jsonb)),
            ('dataset_nonarray_sources', :endpoint_id, 'root_nonarray', :dataset_hash,
             :validated, false, :resource_count, CAST(:nonarray_metadata AS jsonb));
        """,
        endpoint_id=ENDPOINT_ID,
        dataset_hash=DATASET_HASH,
        published=importer.ENDPOINT_DATASET_PUBLISHED,
        baseline=importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        validated=importer.ENDPOINT_DATASET_VALIDATED,
        resource_count=RESOURCE_COUNT,
        baseline_metadata=json.dumps(_baseline_metadata()),
        matched_metadata=json.dumps(matched_metadata),
        legacy_metadata=json.dumps(
            {
                "requires_twin_root_verification": True,
                "source_ids": ["source_a"],
            }
        ),
        wrong_metadata=json.dumps(wrong_campaign_metadata),
        missing_metadata=json.dumps(missing_source_ids_metadata),
        nonarray_metadata=json.dumps(nonarray_source_ids_metadata),
    )


@contextlib.asynccontextmanager
async def _candidate_database(monkeypatch):
    schema = f"provider_directory_artifact_gate_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = await _disposable_database()
    is_schema_created = False
    try:
        await _create_tables(database, schema)
        is_schema_created = True
        await _insert_sources(database, schema)
        await _insert_core_datasets(database, schema)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _artifact_options(
    database: Database,
    schema: str,
    endpoint_id: str = ENDPOINT_ID,
) -> list[dict]:
    rows = await database.all(
        f"""
        WITH {importer._artifact_dataset_options_cte(
            f'{schema}.provider_directory_endpoint_dataset',
            f'{schema}.provider_directory_source',
        )}
        SELECT dataset_id, status, validated_candidate_count
          FROM dataset_options
         WHERE endpoint_id = :endpoint_id
         ORDER BY dataset_id;
        """,
        endpoint_id=endpoint_id,
        published_status=importer.ENDPOINT_DATASET_PUBLISHED,
        validated_status=importer.ENDPOINT_DATASET_VALIDATED,
    )
    return [dict(row._mapping) for row in rows]


async def _set_source_metadata(
    database: Database,
    schema: str,
    source_id: str,
    metadata: dict[str, object],
) -> None:
    await database.status(
        f"""
        UPDATE {schema}.provider_directory_source
           SET metadata_json = CAST(:metadata AS jsonb)
         WHERE source_id = :source_id;
        """,
        source_id=source_id,
        metadata=json.dumps(metadata),
    )


async def _set_all_source_profiles(
    database: Database,
    schema: str,
    status: str,
) -> None:
    metadata = _source_metadata(status=status)
    for source_id in ("source_a", "source_b"):
        await _set_source_metadata(database, schema, source_id, metadata)


def _option_ids(options: list[dict]) -> list[str]:
    return [str(option["dataset_id"]) for option in options]


@pytest.mark.asyncio
async def test_pending_gate_keeps_only_the_published_incumbent(
    monkeypatch,
):
    async with _candidate_database(monkeypatch) as (database, schema):
        options = await _artifact_options(database, schema)
        assert _option_ids(options) == ["dataset_current"]
        assert int(options[0]["validated_candidate_count"]) == 0


@pytest.mark.asyncio
async def test_verified_gate_keeps_incumbent_and_exact_matched_candidate(
    monkeypatch,
):
    async with _candidate_database(monkeypatch) as (database, schema):
        await _set_all_source_profiles(
            database,
            schema,
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        )
        artifact_option_rows = await _artifact_options(database, schema)
        assert _option_ids(artifact_option_rows) == [
            "dataset_current",
            "dataset_exact_matched",
        ]
        assert {
            int(option_row["validated_candidate_count"])
            for option_row in artifact_option_rows
        } == {1}
        serving_endpoints = await database.all(
            f"SELECT endpoint_id FROM {schema}.provider_directory_source;"
        )
        assert {
            source_endpoint_row._mapping["endpoint_id"]
            for source_endpoint_row in serving_endpoints
        } == {SERVING_ENDPOINT_ID}
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = jsonb_set(
                   publication_metadata_json,
                   '{{{importer.TWIN_ROOT_VERIFICATION_METADATA_KEY},proof,resource_hashes,Organization}}',
                   '"tampered"'::jsonb
               )
             WHERE dataset_id = 'dataset_baseline';
            """
        )
        assert _option_ids(await _artifact_options(database, schema)) == [
            "dataset_current"
        ]


@pytest.mark.asyncio
async def test_verified_gate_rejects_config_and_profile_drift(monkeypatch):
    async with _candidate_database(monkeypatch) as (database, schema):
        await _set_all_source_profiles(
            database,
            schema,
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        )
        await _set_source_metadata(
            database,
            schema,
            "source_a",
            _source_metadata(
                status=importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
                configured_endpoint_id="replacement_endpoint",
            ),
        )
        assert _option_ids(await _artifact_options(database, schema)) == [
            "dataset_current"
        ]
        for invalid_metadata in (
            _source_metadata(status="unknown"),
            _source_metadata(status=None),
        ):
            await _set_source_metadata(
                database, schema, "source_a", invalid_metadata
            )
            assert _option_ids(await _artifact_options(database, schema)) == [
                "dataset_current"
            ]
        await _set_source_metadata(database, schema, "source_a", _source_metadata())
        await _set_source_metadata(database, schema, "source_b", {})
        assert _option_ids(await _artifact_options(database, schema)) == [
            "dataset_current"
        ]


@pytest.mark.asyncio
async def test_removed_review_profile_cannot_reclassify_twin_datasets(
    monkeypatch,
):
    async with _candidate_database(monkeypatch) as (database, schema):
        profile_absent_metadata = _source_metadata(
            status=None,
            campaign_id=None,
        )
        for source_id in ("source_a", "source_b"):
            await _set_source_metadata(
                database,
                schema,
                source_id,
                profile_absent_metadata,
            )
        assert _option_ids(await _artifact_options(database, schema)) == [
            "dataset_current"
        ]


@pytest.mark.asyncio
async def test_genuine_established_candidate_keeps_profile_absent_path(
    monkeypatch,
):
    async with _candidate_database(monkeypatch) as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_source (
                source_id, endpoint_id, metadata_json
            ) VALUES (
                'established_source', 'established_endpoint',
                CAST(:source_metadata AS jsonb)
            );
            """,
            source_metadata=json.dumps(
                {
                    "provider_directory_supported_resources": ["Organization"],
                    "provider_directory_fully_enumerable_resources": [
                        "Organization"
                    ],
                }
            ),
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                dataset_hash, status, is_current, resource_count,
                publication_metadata_json
            ) VALUES
                ('established_current', 'established_endpoint', 'root_current',
                 :dataset_hash, :published, true, 1, '{{}}'::jsonb),
                ('established_candidate', 'established_endpoint', 'root_candidate',
                 :dataset_hash, :validated, false, 1,
                 CAST(:candidate_metadata AS jsonb));
            """,
            dataset_hash=DATASET_HASH,
            published=importer.ENDPOINT_DATASET_PUBLISHED,
            validated=importer.ENDPOINT_DATASET_VALIDATED,
            candidate_metadata=json.dumps(
                {
                    "requires_twin_root_verification": False,
                    "source_ids": ["established_source"],
                }
            ),
        )
        options = await _artifact_options(
            database,
            schema,
            "established_endpoint",
        )
        assert _option_ids(options) == [
            "established_candidate",
            "established_current",
        ]
