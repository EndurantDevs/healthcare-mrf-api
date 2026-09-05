# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from contextlib import asynccontextmanager
import datetime as dt
import json
import os
from unittest.mock import AsyncMock
import uuid

import pytest
from sqlalchemy.exc import OperationalError

from api import provider_directory_source_outcomes as outcomes
from api import provider_directory_source_dataset_selection as selection
from api.provider_directory_source_dataset_selection import (
    _source_local_current_published_dataset_statement,
)
from db.connection import Database
from tests.test_provider_directory_source_outcomes import (
    DATASET_ID,
    RESOURCE_COUNTS,
    SOURCE_IDS,
    TOTAL_RESOURCES,
    _catalog,
    _dataset_row,
    _identity_proof,
    _MappingResult,
    _metadata,
    _relation_metadata,
)


async def _disposable_outcome_database() -> Database:
    database = Database()
    try:
        await database.connect()
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError) as error:
        await database.disconnect()
        pytest.skip(f"PostgreSQL source outcome proof unavailable: {error}")
    is_schema_test_allowed = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_OUTCOME_ALLOW_SCHEMA_TESTS",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    if "test" not in database_name.lower() and not is_schema_test_allowed:
        await database.disconnect()
        pytest.skip("source outcome proof requires a disposable database")
    return database


async def _create_outcome_selection_tables(
    database: Database,
    schema: str,
) -> None:
    await database.status(f"CREATE SCHEMA {schema};")
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_source ("
        "source_id varchar(64) PRIMARY KEY, endpoint_id varchar(64), "
        "metadata_json jsonb);"
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_endpoint_dataset ("
        "dataset_id varchar(96) PRIMARY KEY, endpoint_id varchar(64) NOT NULL, "
        "acquisition_root_run_id varchar(64), previous_dataset_id varchar(96), "
        "dataset_hash varchar(64), "
        "status varchar(32) NOT NULL, is_current boolean NOT NULL, "
        "validated_at timestamp, published_at timestamp, superseded_at timestamp, "
        "resource_count bigint NOT NULL, publication_metadata_json jsonb, "
        "publication_metadata_summary_json jsonb, "
        "publication_metadata_sha256 varchar(64), "
        "content_proof_admission_version smallint, "
        "content_proof_admission_kind varchar(32), "
        "content_proof_admission_sha256 varchar(64), "
        "content_proof_resource_types varchar(64)[], "
        "artifact_selection_receipt_json jsonb);"
    )
    await database.status(
        f"CREATE INDEX provider_directory_endpoint_dataset_endpoint_idx "
        f"ON {schema}.provider_directory_endpoint_dataset (endpoint_id);"
    )
    await database.status(
        f"CREATE FUNCTION {schema}."
        "provider_directory_endpoint_dataset_admission_metadata_sha256("
        "metadata_summary jsonb, admission_version smallint, "
        "admission_kind text, proof_sha256 text, resource_types varchar[]) "
        "RETURNS varchar LANGUAGE sql IMMUTABLE STRICT AS $$ "
        "SELECT encode(sha256(convert_to(jsonb_build_array("
        "metadata_summary, admission_version, admission_kind, proof_sha256, "
        "to_jsonb(resource_types))::text, 'UTF8')), 'hex')::varchar $$;"
    )


async def _insert_outcome_selection_sources(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source "
        "(source_id, endpoint_id, metadata_json) VALUES "
        "('source-published', 'endpoint-published', NULL), "
        "('source-validated', 'endpoint-validated', NULL), "
        "('source-reassigned', 'endpoint-new', NULL), "
        "('source-rotated', 'endpoint-rotated-serving', "
        " '{\"provider_directory_configured_endpoint_id\": "
        "\"endpoint-rotated-configured\"}'::jsonb), "
        "('source-multi-a', 'endpoint-multi', NULL), "
        "('source-multi-b', 'endpoint-multi', NULL), "
        "('source-shared', 'endpoint-published', NULL), "
        "('source-unrequested', 'endpoint-unrequested', NULL);"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source "
        "(source_id, endpoint_id) SELECT 'bulk-source-' || value, "
        "'bulk-unrequested-' || value FROM generate_series(1, 2000) AS value;"
    )


async def _insert_outcome_dataset(
    database: Database,
    schema: str,
    *,
    dataset_state_tuple: tuple[
        str,
        str,
        str,
        str,
        bool,
        dt.datetime,
        dt.datetime | None,
        dt.datetime | None,
    ],
) -> None:
    (
        dataset_id,
        endpoint_id,
        source_id,
        status,
        is_current,
        validated_at,
        published_at,
        superseded_at,
    ) = dataset_state_tuple
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash, "
        "status, is_current, validated_at, published_at, superseded_at, "
        "resource_count, "
        "publication_metadata_json) VALUES ("
        ":dataset_id, :endpoint_id, :root_run_id, :dataset_hash, :status, "
        ":is_current, CAST(:validated_at AS timestamp), "
        "CAST(:published_at AS timestamp), CAST(:superseded_at AS timestamp), "
        "1, CAST(:metadata AS jsonb));",
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        root_run_id="root-" + dataset_id,
        dataset_hash="a" * 64,
        status=status,
        is_current=is_current,
        validated_at=validated_at,
        published_at=published_at,
        superseded_at=superseded_at,
        metadata=json.dumps(
            {
                "source_ids": [source_id],
                "selected_resources": ["Practitioner"],
            }
        ),
    )


def _outcome_selection_dataset_states():
    return (
        ("published-old", "endpoint-published", "source-published",
         "superseded", False, dt.datetime(2026, 7, 20, 7),
         dt.datetime(2026, 7, 20, 8), dt.datetime(2026, 7, 20, 8, 5)),
        ("published-current", "endpoint-published", "source-published",
         "published", True, dt.datetime(2026, 7, 20, 8, 30),
         dt.datetime(2026, 7, 20, 9), None),
        ("validated-incumbent", "endpoint-validated", "source-validated",
         "published", True, dt.datetime(2026, 7, 20, 7),
         dt.datetime(2026, 7, 20, 8), None),
        ("validated-candidate", "endpoint-validated", "source-validated",
         "validated", False, dt.datetime(2026, 7, 20, 10), None, None),
        ("reassigned-current", "endpoint-new", "source-reassigned",
         "published", True, dt.datetime(2026, 7, 20, 6, 30),
         dt.datetime(2026, 7, 20, 7), None),
        ("reassigned-stale", "endpoint-old", "source-reassigned",
         "validated", False, dt.datetime(2026, 7, 20, 11), None, None),
        ("rotated-incumbent", "endpoint-rotated-serving", "source-rotated",
         "published", True, dt.datetime(2026, 7, 20, 6),
         dt.datetime(2026, 7, 20, 6, 30), None),
        ("rotated-candidate", "endpoint-rotated-configured", "source-rotated",
         "validated", False, dt.datetime(2026, 7, 20, 11), None, None),
        ("unrequested-current", "endpoint-unrequested", "source-unrequested",
         "published", True, dt.datetime(2026, 7, 20, 11, 30),
         dt.datetime(2026, 7, 20, 12), None),
        ("invalid-validated-superseded", "endpoint-validated", "source-validated",
         "validated", False, dt.datetime(2026, 7, 20, 13), None,
         dt.datetime(2026, 7, 20, 13, 5)),
        ("invalid-published-superseded", "endpoint-published", "source-published",
         "published", True, dt.datetime(2026, 7, 20, 13),
         dt.datetime(2026, 7, 20, 13, 5), dt.datetime(2026, 7, 20, 13, 10)),
        ("invalid-superseded-unsealed", "endpoint-new", "source-reassigned",
         "superseded", False, dt.datetime(2026, 7, 20, 13),
         dt.datetime(2026, 7, 20, 13, 5), None),
    )


async def _seed_outcome_selection_datasets(
    database: Database,
    schema: str,
) -> None:
    for dataset_state_tuple in _outcome_selection_dataset_states():
        await _insert_outcome_dataset(
            database,
            schema,
            dataset_state_tuple=dataset_state_tuple,
        )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash, "
        "status, is_current, validated_at, published_at, superseded_at, "
        "resource_count, publication_metadata_json) VALUES ("
        "'multi-current', 'endpoint-multi', 'root-multi', :dataset_hash, "
        "'published', true, now(), now(), NULL, 1, CAST(:metadata AS jsonb));",
        dataset_hash="a" * 64,
        metadata=json.dumps({
            "source_ids": ["source-multi-b", "source-multi-a"],
            "selected_resources": ["Practitioner"],
        }),
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id, endpoint_id, status, is_current, validated_at, "
        "published_at, resource_count, publication_metadata_json) "
        "SELECT 'unrequested-' || value, 'bulk-unrequested-' || value, "
        "'published', true, now(), now(), 1, "
        "json_build_object('source_ids', "
        "json_build_array('bulk-source-' || value)) "
        "FROM generate_series(1, 2000) AS value;"
    )
    await database.status(
        f"ANALYZE {schema}.provider_directory_source;"
    )
    await database.status(
        f"ANALYZE {schema}.provider_directory_endpoint_dataset;"
    )


async def _seal_outcome_selection_datasets(database: Database, schema: str) -> None:
    digest = (
        f'"{schema}".'
        "provider_directory_endpoint_dataset_admission_metadata_sha256"
    )
    proof_sha256 = "b" * 64
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
        "publication_metadata_summary_json = publication_metadata_json, "
        "content_proof_admission_version = 1, "
        "content_proof_admission_kind = 'generic', "
        "content_proof_admission_sha256 = :stored_proof_sha256, "
        "content_proof_resource_types = ARRAY[]::varchar[], "
        f"publication_metadata_sha256 = {digest}("
        "publication_metadata_json, 1::smallint, 'generic'::text, "
        "CAST(:digest_proof_sha256 AS text), ARRAY[]::varchar[]) "
        "WHERE dataset_id NOT LIKE 'unrequested-%';",
        stored_proof_sha256=proof_sha256,
        digest_proof_sha256=proof_sha256,
    )


@asynccontextmanager
async def _outcome_selection_schema(monkeypatch):
    database = await _disposable_outcome_database()
    schema = f"provider_directory_outcomes_{uuid.uuid4().hex[:12]}"
    is_created = False
    try:
        monkeypatch.setattr(selection, "_ADMISSION_SCHEMA", schema)
        await _create_outcome_selection_tables(database, schema)
        is_created = True
        await _insert_outcome_selection_sources(database, schema)
        await _seed_outcome_selection_datasets(database, schema)
        await _seal_outcome_selection_datasets(database, schema)
        yield database, schema
    finally:
        if is_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


def test_source_local_query_is_bounded_ranked_and_metadata_only():
    source_id_groups = {SOURCE_IDS} | {
        (f"synthetic-source-{index}",) for index in range(39)
    }
    statement = outcomes._current_published_dataset_statement(source_id_groups)
    selected_sql = str(statement)
    bound_values = statement.compile().params.values()

    assert "provider_directory_endpoint_dataset" in selected_sql
    assert "publication_metadata_json" not in selected_sql
    assert "publication_metadata_summary_json" in selected_sql
    assert "jsonb_build_object" not in selected_sql
    assert "artifact_selection_receipt_json" not in selected_sql
    assert "admission_metadata_sha256" in selected_sql
    assert "provider_directory_dataset_resource" not in selected_sql
    assert "provider_directory_api_endpoint" not in selected_sql
    assert "provider_directory_source" in selected_sql
    assert "endpoint_id IN (SELECT" in selected_sql
    assert "provider_directory_configured_endpoint_id" in bound_values
    assert "array_agg" in selected_sql.lower()
    assert "jsonb_array_length" in selected_sql.lower()
    assert "CASE WHEN" in selected_sql
    assert selected_sql.count("@>") == 2 * len(source_id_groups)
    assert "ORDER BY" in selected_sql
    assert "LIMIT" in selected_sql
    assert selected_sql.count("LIMIT") == 40
    assert any(bound_value == list(SOURCE_IDS) for bound_value in bound_values)
    assert "resource_count" in selected_sql
    assert "superseded_at" in selected_sql
    assert "GROUP BY" in selected_sql


def test_current_source_local_query_uses_exact_profile_publication_state():
    source_id_groups = {SOURCE_IDS} | {
        (f"synthetic-source-{index}",) for index in range(39)
    }
    statement = _source_local_current_published_dataset_statement(source_id_groups)
    selected_sql = str(statement)
    bound_values = statement.compile().params.values()

    assert "status =" in selected_sql
    assert "is_current IS true" in selected_sql
    assert "published_at IS NOT NULL" in selected_sql
    assert "superseded_at IS NULL" in selected_sql
    assert "validated_at IS NOT NULL" not in selected_sql
    assert "LIMIT" in selected_sql
    assert selected_sql.count("LIMIT") == 40
    assert "publication_metadata_json" not in selected_sql
    assert "artifact_selection_receipt_json" not in selected_sql
    assert "admission_metadata_sha256" in selected_sql
    assert "jsonb_build_object" in selected_sql
    assert "provider_directory_dataset_resource" not in selected_sql
    assert "published" in bound_values
    assert "provider_directory_configured_endpoint_id" not in bound_values


@pytest.mark.asyncio
async def test_newer_validated_source_local_dataset_is_reported(monkeypatch):
    validated_at = dt.datetime(2026, 7, 21, 10, 0, tzinfo=dt.UTC)
    publication_metadata = _metadata(
        outcome_resource_counts_v1=_identity_proof(),
        **_relation_metadata(),
    )
    publication_metadata["outcome_resource_counts_v1"][
        "dataset_id"
    ] = "pdds-validated"
    for relation_proof in (
        publication_metadata["dataset_network_plan"],
        publication_metadata["dataset_affiliation_organization"],
    ):
        relation_proof["dataset_id"] = "pdds-validated"
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(),
                    _dataset_row(
                        dataset_id="pdds-validated",
                        status="validated",
                        is_current=False,
                        validated_at=validated_at,
                        published_at=None,
                        publication_metadata=publication_metadata,
                        current_source_ids=SOURCE_IDS,
                    ),
                ]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary == {
        "dataset_id": "pdds-validated",
        "status": "validated",
        "is_current": False,
        "validated_at": "2026-07-21T10:00:00+00:00",
        "total_resources": TOTAL_RESOURCES,
        "resource_counts": dict(sorted(RESOURCE_COUNTS.items())),
        "network_plan_links": 5,
        "organization_affiliation_links": 1,
    }


@pytest.mark.asyncio
async def test_superseded_outcome_keeps_historic_state_truthful(monkeypatch):
    monkeypatch.setattr(
        outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                [
                    _dataset_row(
                        status="superseded",
                        is_current=False,
                        superseded_at=dt.datetime(2026, 7, 20, 9),
                    )
                ]
            )
        ),
    )

    summary = (
        await outcomes.enrich_provider_directory_source_catalog(_catalog())
    )["items"][0]["outcome_summary"]

    assert summary == {
        "dataset_id": DATASET_ID,
        "status": "superseded",
        "is_current": False,
        "validated_at": "2026-07-20T08:00:00+00:00",
        "published_at": "2026-07-20T08:30:00+00:00",
        "total_resources": TOTAL_RESOURCES,
    }
