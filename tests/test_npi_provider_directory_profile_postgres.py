# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import MetaData, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import npi as npi_module
from api.provider_profile_snapshot import lock_reader_relations
from db.connection import Database
from db.models import ProviderDirectoryProfileServingGeneration, ProviderProfileSourcePin
from process.source_profile_result_pins import statement_pin_guard_statements

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
        targets_json jsonb
    ) ON COMMIT DROP;
"""


def _profile_serving_asyncpg_query():
    query_template = npi_module.PROVIDER_DIRECTORY_PROFILE_SERVING_QUERY_TEMPLATE
    return (
        query_template.format(
            serving_generation_ref="pg_temp.pd_serving_generation",
            profile_table_ref="pg_temp.pd_profile",
            evidence_select="",
            serving_targets_select="targets_json",
            canonical_targets_sql=npi_module.PROFILE_CANONICAL_TARGETS_SQL,
        )
        .replace(":npis", "$1")
        .replace(":profile_table_ref", "$2")
        .replace(":evidence_table_ref", "$3")
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


async def _adopt_profile_serving_fixture(connection):
    await connection.execute(
        """
        INSERT INTO pd_serving_generation
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb);
        """,
        "global",
        "pdprofile_11111111111111111111111111111111",
        datetime(2026, 7, 30, 15, 0),
        "2026-07-29",
        "published",
        "publish",
        6,
        json.dumps(
            {
                "profile": {"schema": "pg_temp", "name": "pd_profile"},
                "evidence": {"schema": "pg_temp", "name": "pd_evidence"},
            }
        ),
    )


@pytest.mark.asyncio
async def test_profile_serving_query_fences_transition_in_postgresql():
    """Prove fallback, copied snapshots and logical publication refusal in PostgreSQL."""
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN to run serving truth table")
    connection = await asyncpg.connect(
        make_url(database_dsn).set(drivername="postgresql").render_as_string(hide_password=False)
    )
    transaction = connection.transaction()
    await transaction.start()
    try:
        await _create_profile_serving_postgres_fixture(connection)
        query = _profile_serving_asyncpg_query()
        query_args = (
            [1588616783],
            "pg_temp.pd_profile",
            "pg_temp.pd_evidence",
        )
        fallback_rows = await connection.fetch(query, *query_args)
        await _adopt_profile_serving_fixture(connection)
        adopted_rows = await connection.fetch(query, *query_args)
        await connection.execute("ALTER TABLE pd_profile RENAME TO prior_profile")
        await connection.execute("CREATE TEMP TABLE pd_profile (LIKE prior_profile INCLUDING ALL) ON COMMIT DROP")
        await connection.execute("INSERT INTO pd_profile SELECT * FROM prior_profile")
        copied_rows = await connection.fetch(query, *query_args)
        await connection.execute("UPDATE pd_serving_generation SET profile_as_of = NULL")
        missing_as_of_rows = await connection.fetch(query, *query_args)
        for targets_by_role in (None, {}, {"profile": {"schema": "pg_temp", "name": "pd_profile"}}):
            await connection.execute(
                "UPDATE pd_serving_generation SET profile_as_of='2026-07-29', targets_json=$1::jsonb",
                None if targets_by_role is None else json.dumps(targets_by_role),
            )
            assert await connection.fetch(query, *query_args) == []
        await connection.execute(
            "UPDATE pd_serving_generation SET targets_json=$1::jsonb",
            json.dumps(
                {
                    role: {"schema": "pg_temp", "name": name}
                    for role, name in (("profile", "pd_profile"), ("evidence", "pd_evidence"))
                }
            ),
        )
        await connection.execute(
            "UPDATE pd_serving_generation "
            "SET profile_as_of = '2026-07-29', "
            "targets_json = jsonb_set(targets_json, '{profile,schema}', '\"other\"'::jsonb)"
        )
        mismatch_rows = await connection.fetch(query, *query_args)
    finally:
        await transaction.rollback()
        await connection.close()

    assert fallback_rows[0]["published_at"] == datetime(2026, 7, 13, 20, 0)
    assert adopted_rows[0]["published_at"] == datetime(2026, 7, 30, 15, 0)
    assert adopted_rows[0]["serving_generation_key"] == "global"
    assert adopted_rows[0]["profile_as_of"] == "2026-07-29"
    assert copied_rows == adopted_rows
    assert missing_as_of_rows == []
    assert mismatch_rows == []


def _install_serving_edition(connection, schema, named):
    """Use the original serving migration or its current named model."""
    if named:
        metadata = MetaData()
        ProviderDirectoryProfileServingGeneration.__table__.to_metadata(metadata, schema=schema)
        metadata.create_all(connection)
        return
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).resolve().parents[1] / "alembic"))
    migration = ScriptDirectory.from_config(config).get_revision("20260730110000").module
    with Operations.context(MigrationContext.configure(connection)):
        migration._create_serving_generation_table(schema)


def _serving_edition_row(schema, named):
    """Keep global publication newer than unchanged materialized rows."""
    serving_values_by_name = {
        "singleton_key": "global",
        "status": "published",
        "operation": "publish",
        "control_generation": 6,
        "generation_id": "pdprofile_" + "2" * 32,
        "selection_proof_id": "a" * 64,
        "authority_revision": 1,
        "profile_schema_version": 1,
        "profile_strategy_version": "fixture",
        "source_vector_hash": "a" * 64,
        "source_vector_json": "[]",
        "source_context_vector_hash": "b" * 64,
        "source_context_vector_json": "[]",
        "executable_plan_hash": "c" * 64,
        "capacity_geometry_status": "legacy_unavailable",
        "evidence_rows": 1,
        "profile_rows": 1,
        "profile_as_of": "2026-07-29",
        "published_at": datetime(2026, 7, 30, 15, tzinfo=timezone.utc),
        "created_at": datetime(2026, 7, 30, 15, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 7, 30, 15, tzinfo=timezone.utc),
    }
    if named:
        serving_values_by_name["targets_json"] = json.dumps(
            {
                role: {"schema": schema, "name": "provider_directory_profile" + suffix}
                for role, suffix in (("profile", ""), ("evidence", "_evidence"))
            }
        )
    else:
        serving_values_by_name.update(evidence_target_oid=1, profile_target_oid=2)
    return serving_values_by_name


@asynccontextmanager
async def _serving_edition_database(monkeypatch, named):
    """Give an exact task-owned Reader only the columns this public read needs."""
    dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set the profile PostgreSQL DSN to run edition compatibility")
    url = make_url(dsn).set(drivername="postgresql+asyncpg")
    schema = "profile_serving_" + uuid.uuid4().hex
    role = "profile_reader_" + uuid.uuid4().hex
    password = "pw_" + uuid.uuid4().hex
    admin = create_async_engine(url)
    reader = create_async_engine(url.set(username=role, password=password))
    try:
        async with admin.begin() as connection:
            await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
            await connection.exec_driver_sql(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
            await connection.run_sync(lambda sync: _install_serving_edition(sync, schema, named))
            for name, columns in (
                (
                    "provider_directory_profile",
                    "npi bigint PRIMARY KEY, profile_json jsonb, evidence_json jsonb, generation_id text, published_at timestamp",
                ),
                ("provider_directory_profile_evidence", "evidence_key text PRIMARY KEY"),
            ):
                await connection.exec_driver_sql(f'CREATE TABLE "{schema}"."{name}" ({columns})')
            await connection.execute(
                text(
                    f'INSERT INTO "{schema}".provider_directory_profile VALUES (1588616783, :profile, :evidence, :generation, :published)'
                ),
                {
                    "profile": "{}",
                    "evidence": "{}",
                    "generation": "pdprofile_" + "1" * 32,
                    "published": datetime(2026, 7, 13, 20),
                },
            )
            await connection.exec_driver_sql(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
            await connection.exec_driver_sql(
                f'GRANT SELECT ON "{schema}".provider_directory_profile, "{schema}".provider_directory_profile_evidence TO "{role}"'
            )
            columns = "singleton_key,generation_id,published_at,profile_as_of,status,operation,control_generation"
            await connection.exec_driver_sql(
                f'GRANT SELECT ({columns}{",targets_json" if named else ""}) ON "{schema}".provider_directory_profile_serving_generation TO "{role}"'
            )
        monkeypatch.setattr(npi_module, "_schema_cache_key", lambda name: f"{schema}.{name}")
        monkeypatch.setattr(npi_module, "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN", set())
        yield schema, admin, reader
    finally:
        await reader.dispose()
        async with admin.begin() as connection:
            await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await connection.exec_driver_sql(f'DROP ROLE IF EXISTS "{role}"')
        await admin.dispose()


async def _fetch_serving_edition(reader, schema):
    """Run the real loader under native named pins and a read-only snapshot."""
    async with async_sessionmaker(reader)() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        names = ("provider_directory_profile", "provider_directory_profile_evidence")
        await lock_reader_relations(session, ((schema, name) for name in names))
        return await npi_module._fetch_provider_directory_profile_map([1588616783], session=session)


async def _insert_serving_edition(admin, schema, named):
    serving_values_by_name = _serving_edition_row(schema, named)
    values = ",".join(
        f"CAST(:{name} AS jsonb)" if name.endswith("_json") else f":{name}" for name in serving_values_by_name
    )
    async with admin.begin() as connection:
        await connection.execute(
            text(
                f'INSERT INTO "{schema}".provider_directory_profile_serving_generation ({",".join(serving_values_by_name)}) VALUES ({values})'
            ),
            serving_values_by_name,
        )


async def _assert_serving_reader_denials(reader, schema, named):
    """No immutable artifact writes or undeclared legacy-column reads are granted."""
    async with async_sessionmaker(reader)() as session, session.begin():
        statements = [f"UPDATE \"{schema}\".provider_directory_profile SET profile_json='{{}}'"]
        if not named:
            statements.append(
                f'SELECT profile_target_oid FROM "{schema}".provider_directory_profile_serving_generation'
            )
        for statement in statements:
            with pytest.raises(DBAPIError) as refused:
                async with session.begin_nested():
                    await session.execute(text(statement))
            assert refused.value.orig.sqlstate == "42501"


@pytest.mark.parametrize("named", (False, True), ids=("legacy", "named"))
@pytest.mark.asyncio
async def test_profile_loader_reads_both_serving_editions_with_narrow_grants(monkeypatch, named):
    """Read both metadata editions using actual native permissions."""
    async with _serving_edition_database(monkeypatch, named) as (schema, admin, reader):
        fallback = await _fetch_serving_edition(reader, schema)
        await _insert_serving_edition(admin, schema, named)
        adopted = await _fetch_serving_edition(reader, schema)
        assert fallback[1588616783]["profile"]["generation_id"] == "pdprofile_" + "1" * 32
        assert adopted[1588616783]["profile"]["generation_id"] == "pdprofile_" + "2" * 32
        assert adopted[1588616783]["profile"]["profile_as_of"] == "2026-07-29"
        await _assert_serving_reader_denials(reader, schema, named)
        async with admin.begin() as connection:
            await connection.exec_driver_sql(
                f"UPDATE \"{schema}\".provider_directory_profile_serving_generation SET status='purged',operation='purge'"
            )
        assert await _fetch_serving_edition(reader, schema) == {}
        if named:
            async with admin.begin() as connection:
                await connection.execute(
                    text(
                        f"UPDATE \"{schema}\".provider_directory_profile_serving_generation SET status='published',operation='publish',targets_json=jsonb_set(targets_json::jsonb, '{{profile,schema}}', CAST(:other AS jsonb))"
                    ),
                    {"other": json.dumps("other")},
                )
            assert await _fetch_serving_edition(reader, schema) == {}


def _retention_schema_models(monkeypatch, schema):
    """Copy production tables without changing shared model metadata."""
    metadata = MetaData()
    for model_name in (
        "ProviderProfileImportRun",
        "ProviderProfileFact",
        "ProviderProfileSourceRecord",
        "ProviderProfileArtifact",
        "ProviderProfileProjection",
    ):
        table = getattr(florida, model_name).__table__.to_metadata(
            metadata,
            schema=schema,
        )
        monkeypatch.setattr(
            florida,
            model_name,
            SimpleNamespace(
                __table__=table,
                __tablename__=table.name,
                **{column.name: column for column in table.columns},
            ),
        )
    florida.ProviderProfileProjection.__table__.to_metadata(
        metadata,
        schema=schema,
        name="provider_profile_projection_old",
    )
    ProviderProfileSourcePin.__table__.to_metadata(metadata, schema=schema)
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
            for statement in statement_pin_guard_statements(schema):
                await connection.exec_driver_sql(statement)
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
            "run_id": token * 32,
            "status": status,
            "source_key": source_key,
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
                "artifact_id": run_id,
                "source_key": source_key,
                "file_name": "source.csv",
                "source_url": "https://example.test/source.csv",
                "category": "education",
                "content_sha256": run_id * 2,
                "content_bytes": 8,
                "metadata_json": sentinel_by_field,
            },
            "ProviderProfileSourceRecord": {
                "record_id": run_id,
                "artifact_id": run_id,
                "source_key": source_key,
                "source_record_key": "one",
                "raw_payload": sentinel_by_field,
            },
            "ProviderProfileFact": {
                "fact_id": run_id,
                "source_record_id": run_id,
                "logical_fact_key": run_id,
                "category": "education",
                "fact_type": "education_history",
                "display": "Example medical school",
                "value_json": sentinel_by_field,
                "source_json": sentinel_by_field,
                "assertion_type": "self_reported",
                "verification_status": "not_independently_verified",
            },
        }
        for model_name, payload_fields in payload_by_model.items():
            await (
                database.insert(getattr(florida, model_name).__table__)
                .values(
                    run_id=run_id,
                    **payload_fields,
                )
                .status()
            )
        (artifact_root / run_id).mkdir()
        (artifact_root / run_id / "source.csv").write_text(run_id)


async def _seed_retention_generations(database, metadata, observed_at):
    for suffix, generation_id in (("", "c" * 32), ("_old", "d" * 32)):
        projection = florida.ProviderProfileProjection.__table__
        table = metadata.tables[f"{projection.schema}.{projection.name}{suffix}"]
        await (
            database.insert(table)
            .values(
                npi=1000000004,
                generation_id=generation_id,
                schema_version="provider-profile/v1",
                profile_json={},
                evidence_json={},
                source_keys=["florida-mqa"],
                published_at=observed_at,
            )
            .status()
        )


async def _retention_payload_snapshot(database):
    snapshots_by_table = {}
    for model in (
        florida.ProviderProfileFact,
        florida.ProviderProfileSourceRecord,
        florida.ProviderProfileArtifact,
    ):
        snapshots_by_table[model.__tablename__] = {
            retained_row._mapping["run_id"]: dict(retained_row._mapping)
            for retained_row in await database.all(select(model.__table__))
        }
    return snapshots_by_table


@pytest.mark.asyncio
async def test_florida_retention_preserves_other_sources_in_postgresql(monkeypatch, tmp_path):
    """Skip a pinned Florida audit and clean an unrelated eligible generation."""
    observed_at, run_rows = _retention_run_cases()
    deleted_run_ids = {"b" * 32}
    monkeypatch.setattr(florida, "_utcnow", lambda: observed_at)
    async with _retention_database(monkeypatch) as (database, metadata):
        await database.insert(florida.ProviderProfileImportRun.__table__).values(run_rows).status()
        await _seed_retention_payloads(database, run_rows, tmp_path)
        await _seed_retention_generations(database, metadata, observed_at)
        await (
            database.insert(
                metadata.tables[f"{florida.ProviderProfileProjection.__table__.schema}.provider_profile_source_pin"]
            )
            .values(
                pin_id=str(uuid.uuid4()),
                source_key="florida-mqa",
                run_id="a" * 32,
                purpose="export",
                authority_json={},
            )
            .status()
        )
        before_by_table = await _retention_payload_snapshot(database)
        retention = await florida._post_success_retention(
            run_id="e" * 32,
            artifact_root=tmp_path,
            failed_retention_days=7,
        )
        after_by_table = await _retention_payload_snapshot(database)
        assert retention["deleted_run_ids"] == sorted(deleted_run_ids)
        assert retention["protected_audit_run_ids"] == ["a" * 32, "c" * 32, "d" * 32]
        assert retention["deleted_rows"] == {"facts": 1, "source_records": 1, "artifacts": 1}
        for table_name, before_by_run in before_by_table.items():
            assert after_by_table[table_name] == {
                run_id: retained_payload
                for run_id, retained_payload in before_by_run.items()
                if run_id not in deleted_run_ids
            }
        audit_runs = await database.all(select(florida.ProviderProfileImportRun.run_id))
        assert {audit_run._mapping["run_id"] for audit_run in audit_runs} == {run_row["run_id"] for run_row in run_rows}
    for run_row in run_rows:
        artifact_path = tmp_path / run_row["run_id"] / "source.csv"
        if run_row["run_id"] in deleted_run_ids:
            assert not artifact_path.parent.exists()
        else:
            assert artifact_path.read_text() == run_row["run_id"]
