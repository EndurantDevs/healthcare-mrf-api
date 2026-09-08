# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import MetaData, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import npi as npi_module
from db.connection import Database

florida = importlib.import_module("process.florida_mqa_profile")


PROFILE_SERVING_POSTGRES_FIXTURE_SQL = """
    CREATE TEMP TABLE pd_profile (
        npi bigint PRIMARY KEY,
        profile_json jsonb NOT NULL,
        evidence_json jsonb NOT NULL,
        generation_id text NOT NULL,
        published_at timestamp NOT NULL
    ) ON COMMIT DROP;
    CREATE TEMP TABLE pd_evidence (
        evidence_key text PRIMARY KEY
    ) ON COMMIT DROP;
    CREATE TEMP TABLE pd_serving_generation (
        singleton_key text PRIMARY KEY,
        generation_id text,
        published_at timestamp,
        profile_as_of varchar(10),
        status text,
        operation text,
        control_generation bigint,
        profile_target_oid bigint,
        evidence_target_oid bigint
    ) ON COMMIT DROP;
"""


def _profile_serving_asyncpg_query():
    query_template = (
        npi_module.PROVIDER_DIRECTORY_PROFILE_SERVING_QUERY_TEMPLATE
    )
    return query_template.format(
        serving_generation_ref="pg_temp.pd_serving_generation",
        profile_table_ref="pg_temp.pd_profile",
        evidence_select="",
    ).replace(
        ":npis", "$1"
    ).replace(
        ":profile_table_ref", "$2"
    ).replace(
        ":evidence_table_ref", "$3"
    )


async def _create_profile_serving_postgres_fixture(connection):
    await connection.execute(PROFILE_SERVING_POSTGRES_FIXTURE_SQL)
    await connection.execute(
        """
        INSERT INTO pd_profile
        VALUES ($1, $2::jsonb, $3::jsonb, $4, $5);
        """,
        1588616783,
        "{}",
        "{}",
        "pdprofile_11111111111111111111111111111111",
        datetime(2026, 7, 13, 20, 0),
    )
    profile_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint",
        "pg_temp.pd_profile",
    )
    evidence_oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint",
        "pg_temp.pd_evidence",
    )
    return profile_oid, evidence_oid


@pytest.mark.asyncio
async def test_profile_serving_query_fences_transition_in_postgresql():
    """Prove fallback, adoption, and OID mismatch against PostgreSQL tableoid."""
    database_dsn = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN"
    )
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN to run serving truth table")
    connection = await asyncpg.connect(database_dsn)
    transaction = connection.transaction()
    await transaction.start()
    try:
        profile_oid, evidence_oid = (
            await _create_profile_serving_postgres_fixture(connection)
        )
        query = _profile_serving_asyncpg_query()
        query_args = (
            [1588616783],
            "pg_temp.pd_profile",
            "pg_temp.pd_evidence",
        )
        fallback_rows = await connection.fetch(query, *query_args)
        await connection.execute(
            """
            INSERT INTO pd_serving_generation
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
            """,
            "global",
            "pdprofile_11111111111111111111111111111111",
            datetime(2026, 7, 30, 15, 0),
            "2026-07-29",
            "published",
            "publish",
            6,
            profile_oid,
            evidence_oid,
        )
        adopted_rows = await connection.fetch(query, *query_args)
        await connection.execute(
            "UPDATE pd_serving_generation SET profile_as_of = NULL"
        )
        missing_as_of_rows = await connection.fetch(query, *query_args)
        await connection.execute(
            "UPDATE pd_serving_generation "
            "SET profile_as_of = '2026-07-29', "
            "profile_target_oid = profile_target_oid + 1"
        )
        mismatch_rows = await connection.fetch(query, *query_args)
    finally:
        await transaction.rollback()
        await connection.close()

    assert fallback_rows[0]["published_at"] == datetime(2026, 7, 13, 20, 0)
    assert adopted_rows[0]["published_at"] == datetime(2026, 7, 30, 15, 0)
    assert adopted_rows[0]["serving_generation_key"] == "global"
    assert adopted_rows[0]["profile_as_of"] == "2026-07-29"
    assert missing_as_of_rows == []
    assert mismatch_rows == []


def _retention_schema_models(monkeypatch, schema):
    """Copy production tables without changing shared model metadata."""
    metadata = MetaData()
    for model_name in (
        "ProviderProfileImportRun", "ProviderProfileFact",
        "ProviderProfileSourceRecord", "ProviderProfileArtifact",
        "ProviderProfileProjection",
    ):
        table = getattr(florida, model_name).__table__.to_metadata(
            metadata, schema=schema,
        )
        monkeypatch.setattr(florida, model_name, SimpleNamespace(
            __table__=table, __tablename__=table.name,
            **{column.name: column for column in table.columns},
        ))
    florida.ProviderProfileProjection.__table__.to_metadata(
        metadata, schema=schema, name="provider_profile_projection_old",
    )
    return metadata


@asynccontextmanager
async def _retention_database(monkeypatch):
    """Use the existing native CI DSN with an exactly owned disposable schema."""
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN to run retention isolation")
    schema = f"profile_retention_{uuid.uuid4().hex}"
    engine = create_async_engine(make_url(database_dsn).set(drivername="postgresql+asyncpg"))
    database = Database(
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )
    metadata = _retention_schema_models(monkeypatch, schema)
    is_schema_created = False
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        is_schema_created = True
        async with engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
        monkeypatch.setattr(florida, "db", database)
        yield database, metadata
    finally:
        try:
            if is_schema_created:
                await database.status(f"DROP SCHEMA {schema} CASCADE")
        finally:
            await database.disconnect()


def _retention_run_cases():
    """Cover source ownership separately from status, age, and projection pins."""
    observed_at = datetime(2026, 9, 8, 12)
    run_cases = (
        ("a", "completed", "florida-mqa", 30),
        ("b", "failed", "florida-mqa", 30),
        ("c", "completed", "florida-mqa", 30),
        ("d", "completed", "florida-mqa", 30),
        ("e", "completed", "florida-mqa", 30),
        ("f", "failed", "florida-mqa", 1),
        ("1", "running", "florida-mqa", 30),
        ("2", "completed", "massachusetts-board", 30),
        ("3", "failed", "massachusetts-board", 30),
        ("4", "completed", "another-florida-source", 30),
    )
    return observed_at, [
        {
            "run_id": token * 32, "status": status, "source_key": source_key,
            "jurisdiction": "MA" if source_key == "massachusetts-board" else "FL",
            "schema_version": "provider-profile/v1",
            "started_at": observed_at - timedelta(days=age + 1),
            "finished_at": observed_at - timedelta(days=age),
        }
        for token, status, source_key, age in run_cases
    ]


async def _seed_retention_payloads(database, run_rows, artifact_root):
    """Retain distinguishable facts, source records and artifacts for every run."""
    for run_row in run_rows:
        run_id = run_row["run_id"]
        source_key = run_row["source_key"]
        sentinel_by_field = {"run_id": run_id, "source_key": source_key}
        payload_by_model = {
            "ProviderProfileArtifact": {
                "artifact_id": run_id, "source_key": source_key,
                "file_name": "source.csv", "source_url": "https://example.test/source.csv",
                "category": "education", "content_sha256": run_id * 2,
                "content_bytes": 8, "metadata_json": sentinel_by_field,
            },
            "ProviderProfileSourceRecord": {
                "record_id": run_id, "artifact_id": run_id, "source_key": source_key,
                "source_record_key": "one", "raw_payload": sentinel_by_field,
            },
            "ProviderProfileFact": {
                "fact_id": run_id, "source_record_id": run_id,
                "logical_fact_key": run_id, "category": "education",
                "fact_type": "education_history", "display": "Example medical school",
                "value_json": sentinel_by_field, "source_json": sentinel_by_field,
                "assertion_type": "self_reported",
                "verification_status": "not_independently_verified",
            },
        }
        for model_name, payload_fields in payload_by_model.items():
            await database.insert(getattr(florida, model_name).__table__).values(
                run_id=run_id, **payload_fields,
            ).status()
        (artifact_root / run_id).mkdir()
        (artifact_root / run_id / "source.csv").write_text(run_id)


async def _seed_retention_generations(database, metadata, observed_at):
    for suffix, generation_id in (("", "c" * 32), ("_old", "d" * 32)):
        projection = florida.ProviderProfileProjection.__table__
        table = metadata.tables[f"{projection.schema}.{projection.name}{suffix}"]
        await database.insert(table).values(
            npi=1000000004, generation_id=generation_id,
            schema_version="provider-profile/v1", profile_json={},
            evidence_json={}, source_keys=["florida-mqa"], published_at=observed_at,
        ).status()


async def _retention_payload_snapshot(database):
    snapshots_by_table = {}
    for model in (
        florida.ProviderProfileFact, florida.ProviderProfileSourceRecord,
        florida.ProviderProfileArtifact,
    ):
        snapshots_by_table[model.__tablename__] = {
            retained_row._mapping["run_id"]: dict(retained_row._mapping)
            for retained_row in await database.all(select(model.__table__))
        }
    return snapshots_by_table


@pytest.mark.asyncio
async def test_florida_retention_preserves_other_sources_in_postgresql(monkeypatch, tmp_path):
    """Delete only obsolete Florida payloads while retaining every audit run."""
    observed_at, run_rows = _retention_run_cases()
    deleted_run_ids = {"a" * 32, "b" * 32}
    monkeypatch.setattr(florida, "_utcnow", lambda: observed_at)
    async with _retention_database(monkeypatch) as (database, metadata):
        await database.insert(florida.ProviderProfileImportRun.__table__).values(run_rows).status()
        await _seed_retention_payloads(database, run_rows, tmp_path)
        await _seed_retention_generations(database, metadata, observed_at)
        before_by_table = await _retention_payload_snapshot(database)
        retention = await florida._post_success_retention(
            run_id="e" * 32, artifact_root=tmp_path, failed_retention_days=7,
        )
        after_by_table = await _retention_payload_snapshot(database)
        assert retention["deleted_run_ids"] == sorted(deleted_run_ids)
        assert retention["protected_audit_run_ids"] == ["c" * 32, "d" * 32]
        assert retention["deleted_rows"] == {"facts": 2, "source_records": 2, "artifacts": 2}
        for table_name, before_by_run in before_by_table.items():
            assert after_by_table[table_name] == {
                run_id: retained_payload for run_id, retained_payload in before_by_run.items()
                if run_id not in deleted_run_ids
            }
        audit_runs = await database.all(select(florida.ProviderProfileImportRun.run_id))
        assert {audit_run._mapping["run_id"] for audit_run in audit_runs} == {
            run_row["run_id"] for run_row in run_rows
        }
    for run_row in run_rows:
        artifact_path = tmp_path / run_row["run_id"] / "source.csv"
        if run_row["run_id"] in deleted_run_ids:
            assert not artifact_path.parent.exists()
        else:
            assert artifact_path.read_text() == run_row["run_id"]
