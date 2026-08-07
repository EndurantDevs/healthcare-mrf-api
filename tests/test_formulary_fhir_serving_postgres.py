# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for source-hidden FHIR formulary serving."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
import uuid

import orjson
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import formulary_fhir_serving as serving
from api.endpoint import formulary_fhir as endpoint
from db.models import FHIRFormularyDataset
from db.models import db
from process.formulary_fhir.synthetic_canary_contract import CANARY_SOURCE_ID
from process.formulary_fhir.synthetic_canary_contract import expected_evidence
from process.formulary_fhir.synthetic_seed_publisher import publish_synthetic_seed
from tests.test_formulary_fhir_repository_postgres import _configure_database
from tests.test_formulary_fhir_storage_postgres import _connect
from tests.test_formulary_fhir_storage_postgres import _database_url
from tests.test_formulary_fhir_storage_postgres import _drop_schema
from tests.test_formulary_fhir_storage_postgres import _load_migration
from tests.test_formulary_fhir_storage_postgres import _quoted
from tests.test_formulary_fhir_storage_postgres import _run_migration_action
from tests.test_formulary_fhir_storage_postgres import TABLE_NAMES
from tests.test_formulary_fhir_synthetic_seed_publisher_postgres import (
    _prepare_schema as _prepare_seed_schema,
)


SOURCE_A = "source-alpha"
SOURCE_B = "source-beta"
PLAN_CURRENT = "fhir_" + "a" * 26
PLAN_BUILDING = "fhir_" + "b" * 26
PLAN_VERIFIED = "fhir_" + "c" * 26
PLAN_NONCURRENT = "fhir_" + "d" * 26
PLAN_FOREIGN = "fhir_" + "e" * 26
PLAN_UNKNOWN = "fhir_" + "z" * 26
CANARY_PLAN_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
LAST_UPDATED = dt.datetime(2026, 8, 5, 10, tzinfo=dt.UTC)
AS_OF = dt.datetime(2026, 8, 6, 11, tzinfo=dt.UTC)
VERIFIED_AT = dt.datetime(2026, 8, 7, 18, tzinfo=dt.UTC)
PUBLISHED_AT = dt.datetime(2026, 8, 7, 19, tzinfo=dt.UTC)
PERIOD_START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
DATASET_ROWS = (
    (
        "dataset-a-current",
        SOURCE_A,
        "run-a-current",
        AS_OF,
        "published",
        "a" * 64,
        "1" * 64,
        VERIFIED_AT,
        PUBLISHED_AT,
    ),
    (
        "dataset-a-building",
        SOURCE_A,
        "run-a-building",
        AS_OF,
        "building",
        None,
        None,
        None,
        None,
    ),
    (
        "dataset-a-verified",
        SOURCE_A,
        "run-a-verified",
        AS_OF,
        "verified",
        "b" * 64,
        "2" * 64,
        VERIFIED_AT,
        None,
    ),
    (
        "dataset-a-noncurrent",
        SOURCE_A,
        "run-a-noncurrent",
        AS_OF,
        "published",
        "c" * 64,
        "3" * 64,
        VERIFIED_AT,
        PUBLISHED_AT - dt.timedelta(days=1),
    ),
    (
        "dataset-b-current",
        SOURCE_B,
        "run-b-current",
        AS_OF,
        "published",
        "d" * 64,
        "4" * 64,
        VERIFIED_AT,
        PUBLISHED_AT,
    ),
)


async def _create_migrated_schema(engine, schema_name: str) -> None:
    async with engine.begin() as engine_connection:
        await engine_connection.exec_driver_sql(
            f"CREATE SCHEMA {_quoted(schema_name)}"
        )
    await _run_migration_action(engine, _load_migration(), "upgrade")


async def _insert_sources(connection, schema_name: str) -> None:
    schema = _quoted(schema_name)
    await connection.executemany(
        f"""INSERT INTO {schema}.fhir_formulary_source
            (source_id, canonical_base, display_name, enabled,
             runtime_config_json, metadata_json)
        VALUES ($1, $2, $3, false, $4::jsonb, $5::jsonb)""",
        (
            (
                SOURCE_A,
                "https://source-alpha.example.invalid/fhir",
                "Synthetic Source Alpha",
                "{}",
                '{"synthetic": true}',
            ),
            (
                SOURCE_B,
                "https://source-beta.example.invalid/fhir",
                "Synthetic Source Beta",
                "{}",
                '{"synthetic": true}',
            ),
        ),
    )


async def _insert_datasets(connection, schema_name: str) -> None:
    schema = _quoted(schema_name)
    await connection.executemany(
        f"""INSERT INTO {schema}.fhir_formulary_dataset
            (dataset_id, source_id, run_id, cutoff_at, status,
             publish_requested, seed_eligible, coverage_hash,
             membership_hash, verified_at, published_at)
        VALUES ($1, $2, $3, $4, $5, true, false, $6, $7, $8, $9)""",
        DATASET_ROWS,
    )


async def _insert_plans(connection, schema_name: str) -> None:
    schema = _quoted(schema_name)
    plan_rows = (
        (PLAN_CURRENT, SOURCE_A, "list-current", "identity-current"),
        (PLAN_BUILDING, SOURCE_A, "list-building", "identity-building"),
        (PLAN_VERIFIED, SOURCE_A, "list-verified", "identity-verified"),
        (PLAN_NONCURRENT, SOURCE_A, "list-noncurrent", "identity-noncurrent"),
        (PLAN_FOREIGN, SOURCE_B, "list-foreign", "identity-foreign"),
    )
    await connection.executemany(
        f"""INSERT INTO {schema}.fhir_formulary_coverage_plan
            (public_id, source_id, upstream_list_id, canonical_identity)
        VALUES ($1, $2, $3, $4)""",
        plan_rows,
    )
    await connection.executemany(
        f"""INSERT INTO {schema}.fhir_formulary_coverage_plan_version
            (coverage_version_id, public_id, upstream_last_updated, status,
             title, name, period_start, period_end, content_hash,
             metadata_json)
        VALUES ($1, $2, $3, 'current', $4, $5, $6, NULL, $7, $8::jsonb)""",
        tuple(
            (
                f"version-{index}",
                public_id,
                LAST_UPDATED,
                title,
                name,
                PERIOD_START,
                str(index) * 64,
                "{}",
            )
            for index, (public_id, title, name) in enumerate(
                (
                    (PLAN_CURRENT, "Current Alpha Plan", "Alpha Formulary"),
                    (PLAN_BUILDING, "Building Alpha Plan", "Building Formulary"),
                    (PLAN_VERIFIED, "Verified Alpha Plan", "Verified Formulary"),
                    (PLAN_NONCURRENT, "Prior Alpha Plan", "Prior Formulary"),
                    (PLAN_FOREIGN, "Current Beta Plan", "Beta Formulary"),
                ),
                start=1,
            )
        ),
    )
    await connection.executemany(
        f"""INSERT INTO {schema}.fhir_formulary_dataset_coverage_plan
            (source_id, dataset_id, public_id, coverage_version_id)
        VALUES ($1, $2, $3, $4)""",
        (
            (SOURCE_A, "dataset-a-current", PLAN_CURRENT, "version-1"),
            (SOURCE_A, "dataset-a-building", PLAN_BUILDING, "version-2"),
            (SOURCE_A, "dataset-a-verified", PLAN_VERIFIED, "version-3"),
            (SOURCE_A, "dataset-a-noncurrent", PLAN_NONCURRENT, "version-4"),
            (SOURCE_B, "dataset-b-current", PLAN_FOREIGN, "version-5"),
        ),
    )


async def _insert_current_pointers(connection, schema_name: str) -> None:
    schema = _quoted(schema_name)
    await connection.executemany(
        f"""INSERT INTO {schema}.fhir_formulary_current
            (source_id, dataset_id, generation, published_at)
        VALUES ($1, $2, 1, $3)""",
        (
            (SOURCE_A, "dataset-a-current", PUBLISHED_AT),
            (SOURCE_B, "dataset-b-current", PUBLISHED_AT),
        ),
    )


async def _seed_serving_graph(connection, schema_name: str) -> None:
    async with connection.transaction():
        await _insert_sources(connection, schema_name)
        await _insert_datasets(connection, schema_name)
        await _insert_plans(connection, schema_name)
        await _insert_current_pointers(connection, schema_name)


async def _table_fingerprints(
    connection,
    schema_name: str,
) -> dict[str, tuple[int, str]]:
    schema = _quoted(schema_name)
    fingerprints_by_table: dict[str, tuple[int, str]] = {}
    for table in sorted(TABLE_NAMES):
        fingerprint = await connection.fetchrow(
            f"SELECT count(*) AS row_count, "
            f"md5(COALESCE(string_agg(to_jsonb(stored)::text, E'\\n' "
            f"ORDER BY to_jsonb(stored)::text), '')) AS content_hash "
            f"FROM {schema}.{_quoted(table)} AS stored"
        )
        assert fingerprint is not None
        fingerprints_by_table[table] = (
            int(fingerprint["row_count"]),
            str(fingerprint["content_hash"]),
        )
    return fingerprints_by_table


async def _request_detail(session_factory, formulary_id: str):
    async with session_factory() as session:
        request = SimpleNamespace(ctx=SimpleNamespace(sa_session=session))
        return await endpoint.get_current_formulary_detail(request, formulary_id)


def _payload(http_response) -> dict[str, object]:
    return orjson.loads(http_response.body)


def _assert_not_found(http_response) -> None:
    assert http_response.status == 404
    assert http_response.headers.get("Cache-Control") == "private, no-store"
    assert _payload(http_response) == {
        "error": {
            "code": "formulary_fhir_not_found",
            "message": "FHIR formulary not found.",
        }
    }


async def _assert_current_detail(session_factory):
    current_response = await _request_detail(session_factory, PLAN_CURRENT)
    assert current_response.status == 200
    assert current_response.headers.get("Cache-Control") == "private, no-store"
    assert _payload(current_response) == {
        "formulary_id": PLAN_CURRENT,
        "status": "current",
        "title": "Current Alpha Plan",
        "name": "Alpha Formulary",
        "period": {"start": "2026-01-01T00:00:00Z", "end": None},
        "last_updated": "2026-08-05T10:00:00Z",
        "as_of": "2026-08-06T11:00:00Z",
        "published_at": "2026-08-07T19:00:00Z",
    }
    return current_response


async def _assert_independent_source(session_factory, current_response) -> None:
    foreign_response = await _request_detail(session_factory, PLAN_FOREIGN)
    assert foreign_response.status == 200
    assert _payload(foreign_response)["formulary_id"] == PLAN_FOREIGN
    assert _payload(foreign_response)["title"] == "Current Beta Plan"
    repeated_response = await _request_detail(session_factory, PLAN_CURRENT)
    assert _payload(repeated_response) == _payload(current_response)


async def _assert_hidden_plans(session_factory) -> None:
    for hidden_id in (
        PLAN_BUILDING,
        PLAN_VERIFIED,
        PLAN_NONCURRENT,
        PLAN_UNKNOWN,
        "fhir_malformed",
    ):
        _assert_not_found(await _request_detail(session_factory, hidden_id))


def _utc_text(timestamp: dt.datetime) -> str:
    return timestamp.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


@pytest.mark.asyncio
async def test_verified_seed_publication_serves_with_source_disabled(monkeypatch):
    """Prove the real fixed seed path is compatible with the read contract."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    migration_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    model_schema = FHIRFormularyDataset.__table__.schema
    serving_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"),
        execution_options={"schema_translate_map": {model_schema: schema_name}},
    )
    session_factory = async_sessionmaker(serving_engine, expire_on_commit=False)
    connection = None
    try:
        await _prepare_seed_schema(
            monkeypatch,
            database_url,
            schema_name,
            migration_engine,
        )
        publication = await publish_synthetic_seed()
        monkeypatch.setenv(serving.FHIR_FORMULARY_SERVING_ENABLED_ENV, "true")
        connection = await _connect(database_url)
        baseline_fingerprints = await _table_fingerprints(connection, schema_name)

        http_response = await _request_detail(session_factory, CANARY_PLAN_ID)

        assert _payload(http_response) == {
            "formulary_id": CANARY_PLAN_ID,
            "status": "current",
            "title": "Synthetic Coverage",
            "name": "Synthetic Coverage A",
            "period": None,
            "last_updated": "2026-08-01T12:00:00Z",
            "as_of": "2026-08-06T00:00:00Z",
            "published_at": _utc_text(publication.published_at),
        }
        assert publication.dataset_id == expected_evidence()["dataset_id"]
        assert await connection.fetchval(
            f"SELECT enabled FROM {_quoted(schema_name)}.fhir_formulary_source "
            "WHERE source_id = $1",
            CANARY_SOURCE_ID,
        ) is False
        assert await _table_fingerprints(
            connection,
            schema_name,
        ) == baseline_fingerprints
    finally:
        await db.disconnect()
        if connection is not None:
            await connection.close()
        await serving_engine.dispose()
        await _drop_schema(migration_engine, schema_name)
        await migration_engine.dispose()


@pytest.mark.asyncio
async def test_serving_postgres_current_isolation_and_read_only(
    monkeypatch,
):
    """Serve exact current rows without writes or cross-source contamination."""

    database_url = _database_url()
    schema_name = f"fhir_formulary_test_{uuid.uuid4().hex}"
    migration_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg")
    )
    model_schema = FHIRFormularyDataset.__table__.schema
    serving_engine = create_async_engine(
        database_url.set(drivername="postgresql+asyncpg"),
        execution_options={
            "schema_translate_map": {model_schema: schema_name},
        },
    )
    session_factory = async_sessionmaker(serving_engine, expire_on_commit=False)
    connection = None
    _configure_database(monkeypatch, database_url, schema_name)
    monkeypatch.setenv(serving.FHIR_FORMULARY_SERVING_ENABLED_ENV, "true")
    try:
        await _create_migrated_schema(migration_engine, schema_name)
        connection = await _connect(database_url)
        await _seed_serving_graph(connection, schema_name)
        assert await connection.fetchval(
            f"SELECT enabled FROM {_quoted(schema_name)}.fhir_formulary_source "
            "WHERE source_id = $1",
            SOURCE_A,
        ) is False
        baseline_fingerprints = await _table_fingerprints(connection, schema_name)

        current_response = await _assert_current_detail(session_factory)
        await _assert_independent_source(session_factory, current_response)
        await _assert_hidden_plans(session_factory)

        assert await _table_fingerprints(
            connection,
            schema_name,
        ) == baseline_fingerprints
    finally:
        if connection is not None:
            await connection.close()
        await serving_engine.dispose()
        await _drop_schema(migration_engine, schema_name)
        await migration_engine.dispose()
