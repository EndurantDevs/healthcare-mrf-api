# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exercise the RI retained-evidence predicates against a small native database."""

import copy
import json
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from process import provider_profile_source_store as shared
from process import rhode_island_profile as worker
from process.rhode_island_profile_store import store
from tests.test_rhode_island_profile_acquisition import synthetic_schema_fingerprint
from tests.test_rhode_island_profile_managed import acquired, counts, run_row
from tests.test_rhode_island_profile_registry import snapshot


@asynccontextmanager
async def database():
    database_dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not database_dsn:
        pytest.skip("set the profile PostgreSQL DSN for native RI publication predicates")
    connection = await asyncpg.connect(
        make_url(database_dsn).set(drivername="postgresql").render_as_string(hide_password=False)
    )
    schema = "ri_store_" + uuid.uuid4().hex
    is_created = False
    try:
        assert "test" in (await connection.fetchval("SELECT current_database()")).lower()
        await connection.execute(f"CREATE SCHEMA {schema}")
        is_created = True
        tables_by_model = {
            model: f'"{schema}"."{model.__table__.name}"'
            for model in (
                shared.ProviderProfileSourceRecord,
                shared.ProviderProfileFact,
                shared.ProviderProfileArtifact,
                shared.ProviderProfileImportRun,
            )
        }
        await connection.execute(f"""CREATE TABLE {tables_by_model[shared.ProviderProfileSourceRecord]} (
            record_id text, run_id text, source_key text, artifact_id text, license_number text,
            source_record_key text, match_status text, raw_payload json, normalized_payload json, match_evidence json);
            CREATE TABLE {tables_by_model[shared.ProviderProfileFact]} (run_id text, source_record_id text, source_json json);
            CREATE TABLE {tables_by_model[shared.ProviderProfileArtifact]} (artifact_id text, run_id text, source_key text, metadata_json json);
            CREATE TABLE {tables_by_model[shared.ProviderProfileImportRun]} (run_id text, source_manifest json);""")
        yield connection, tables_by_model
    finally:
        try:
            if is_created:
                await connection.execute(f"DROP SCHEMA {schema} CASCADE")
                assert await connection.fetchval("SELECT to_regnamespace($1::text)", schema) is None
        finally:
            await connection.close()
            assert connection.is_closed()


async def populate(connection, tables, cohort, profiles, artifact, directory):
    worker.write_new_json(directory / "snapshot.json", snapshot())
    bound = worker.registry.bind_snapshot_profiles(
        worker._profile_inputs(cohort, profiles, directory, artifact),
        snapshot_path=directory / "snapshot.json",
        snapshot_sha256=run_row()["source_manifest"]["snapshot_sha256"],
    )
    record_columns = (
        "record_id",
        "run_id",
        "source_key",
        "artifact_id",
        "license_number",
        "source_record_key",
        "match_status",
        "raw_payload",
        "normalized_payload",
        "match_evidence",
    )
    for root, (source_record, facts) in zip(cohort["roots"], bound, strict=True):
        worker._retain_roster(source_record, root, artifact)
        await connection.execute(
            f"INSERT INTO {tables[shared.ProviderProfileSourceRecord]} VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            *(
                json.dumps(source_record[key]) if isinstance(source_record[key], dict) else source_record[key]
                for key in record_columns
            ),
        )
        for fact in facts:
            await connection.execute(
                f"INSERT INTO {tables[shared.ProviderProfileFact]} VALUES ($1,$2,$3)",
                fact["run_id"],
                fact["source_record_id"],
                json.dumps(fact["source_json"]),
            )
    await connection.execute(
        f"INSERT INTO {tables[shared.ProviderProfileArtifact]} VALUES ($1,$2,$3,$4)",
        artifact["artifact_id"],
        artifact["run_id"],
        artifact["source_key"],
        json.dumps(artifact["metadata_json"]),
    )
    await connection.execute(
        f"INSERT INTO {tables[shared.ProviderProfileImportRun]} VALUES ($1,$2)",
        run_row()["run_id"],
        json.dumps(run_row()["source_manifest"]),
    )


async def test_native_capture_and_roster_lineage_refusals(tmp_path, monkeypatch, synthetic_schema_fingerprint):
    cohort, profiles, metrics, artifact = await acquired(tmp_path, monkeypatch)
    async with database() as (connection, tables):
        await populate(connection, tables, cohort, profiles, artifact, tmp_path)
        monkeypatch.setattr(type(store), "_table", lambda self, model: tables[model])
        monkeypatch.setattr(shared.SourceProfileStore, "retained_counts", AsyncMock(return_value=counts(metrics)))
        monkeypatch.setattr(type(store), "_read_run", AsyncMock(return_value=run_row()))

        async def first(query, **parameters):
            returned = await connection.fetchrow(str(query).replace(":run_id", "$1"), parameters["run_id"])
            return SimpleNamespace(_mapping=dict(returned))

        monkeypatch.setattr(shared.db, "first", first)
        monkeypatch.setattr(shared.db, "all", AsyncMock(return_value=[SimpleNamespace(_mapping=artifact)]))
        retained = await store.retained_counts(run_row()["run_id"])
        assert store._completion_metrics(run_row(), metrics, retained)["requested_licenses"] == 2
        source_table, fact_table = tables[shared.ProviderProfileSourceRecord], tables[shared.ProviderProfileFact]
        mutations = [
            (
                f"UPDATE {fact_table} SET source_json=(source_json::jsonb - 'schema_page')::json",
                "invalid_capture_facts",
            ),
            (
                f"UPDATE {source_table} SET raw_payload=jsonb_set(raw_payload::jsonb,'{{roster_occurrences}}','[]')::json",
                "invalid_bundle_records",
            ),
            (
                f"UPDATE {source_table} SET match_evidence=jsonb_set(match_evidence::jsonb,'{{registry_binding,retained_snapshot,snapshot_sha256}}','\"changed\"')::json",
                "invalid_bundle_records",
            ),
            (
                f"UPDATE {source_table} SET normalized_payload=(normalized_payload::jsonb - 'profile_capture')::json",
                "invalid_bundle_records",
            ),
        ]
        for query, field in mutations:
            transaction = connection.transaction()
            await transaction.start()
            try:
                await connection.execute(query)
                invalid = await store.retained_counts(run_row()["run_id"])
                assert invalid[field] > 0
                with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
                    store._completion_metrics(run_row(), metrics, invalid)
            finally:
                await transaction.rollback()
        assert (
            store._completion_metrics(run_row(), metrics, await store.retained_counts(run_row()["run_id"]))[
                "requested_licenses"
            ]
            == 2
        )
