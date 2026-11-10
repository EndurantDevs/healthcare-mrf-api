# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bounded resource projection; publication authority is tested separately."""

import importlib
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateTable

from process import provider_directory_generic_source_artifacts as artifacts
from tests.cms_npd_admission_postgres_support import _owned_database
from tests.provider_directory_entities_postgres_support import _database_url

fhir = importlib.import_module("process.provider_directory_fhir")
MODEL = fhir.ProviderDirectoryPractitioner
DATASET = SimpleNamespace(source_id="source_a", dataset_id="dataset_a", evidence_run_id="root_a")


def enrichment():
    """Keep the ordinary overlay's original NPI correction and decision timestamp."""
    return dict(
        plan=dict(publish_targets=["address_overlay"], decision_at=datetime(2026, 1, 2).isoformat()),
        metrics=dict(resource_id_npis_backfilled=dict(Practitioner=0, Organization=0)),
    )


async def seed(connection, rows, payload_bytes):
    """Mix origins and resource types sharing IDs without sharing their payloads."""
    for statement in (
        "CREATE SCHEMA mrf",
        "CREATE TABLE mrf.provider_directory_endpoint_dataset (dataset_id varchar PRIMARY KEY, "
        "published_at timestamp, validated_at timestamp, created_at timestamp)",
        "CREATE TABLE mrf.provider_directory_dataset_resource (dataset_id varchar NOT NULL, "
        "resource_type varchar NOT NULL, resource_id varchar NOT NULL, payload_json json NOT NULL, "
        "PRIMARY KEY(dataset_id,resource_type,resource_id))",
        "INSERT INTO mrf.provider_directory_endpoint_dataset VALUES "
        "('dataset_a',NULL,NULL,'2026-01-01'),('dataset_b',NULL,NULL,'2026-01-01')",
    ):
        await connection.execute(text(statement))
    await connection.execute(
        text(
            "INSERT INTO mrf.provider_directory_dataset_resource "
            "SELECT d,t,(1000000000+n)::text,json_build_object('full_name',repeat('x',:bytes), "
            "'names',json_build_array(json_build_object('family',d)), 'active',true, "
            "'addresses',json_build_array(json_build_object('line',json_build_array(repeat('y',:bytes))))) "
            "FROM generate_series(1,CAST(:rows AS integer)) n "
            "CROSS JOIN (VALUES('dataset_a'),('dataset_b')) ds(d) "
            "CROSS JOIN (VALUES('Practitioner'),('Organization')) types(t)"
        ),
        dict(rows=rows, bytes=payload_bytes),
    )
    for name in ("old_stage", "new_stage"):
        table = MODEL.__table__.to_metadata(MetaData(), schema="mrf", name=name)
        await connection.execute(CreateTable(table))
    await connection.execute(text("ANALYZE mrf.provider_directory_dataset_resource"))


@asynccontextmanager
async def native_case(monkeypatch, *, rows=1030, payload_bytes=128):
    """Own one disposable database; use the same original connection for reads and COPY."""
    monkeypatch.setattr(artifacts.source, "is_source_storage_required", lambda: False)
    async with _owned_database(_database_url()) as (url, _admin):
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await seed(connection, rows, payload_bytes)
            async with engine.connect() as connection:

                async def all_rows(statement, **params):
                    return (await connection.execute(text(statement), params)).all()

                directory = SimpleNamespace(
                    db=SimpleNamespace(all=all_rows),
                    _schema=lambda: "mrf",
                    _q=fhir._q,
                    _artifact_resource_batch_select_sql=fhir._artifact_resource_batch_select_sql,
                    RESOURCE_TYPES_BY_MODEL=fhir.RESOURCE_TYPES_BY_MODEL,
                    CMS_NPD_SOURCE_ID=fhir.CMS_NPD_SOURCE_ID,
                    ProviderDirectorySource=fhir.ProviderDirectorySource,
                    _json_columns=fhir._json_columns,
                    _copy_record=fhir._copy_record,
                )
                raw = await connection.get_raw_connection()
                yield SimpleNamespace(connection=connection, driver=raw.driver_connection, directory=directory)
        finally:
            await engine.dispose()


def copy_inputs(case, prepared):
    statement = fhir._provider_directory_artifact_resource_select_sql(MODEL, "mrf")
    params_by_name = dict(
        source_ids=[DATASET.source_id],
        dataset_ids=[DATASET.dataset_id],
        evidence_run_ids=[DATASET.evidence_run_id],
        resource_type="Practitioner",
    )
    return artifacts._loading_projection(case.directory, MODEL.__tablename__, statement, params_by_name, prepared)


def copy_admission(prepared):
    """Keep native copy inputs under one ordinary, immutable dataset selector."""
    return SimpleNamespace(factory=None, fence=SimpleNamespace(datasets=(DATASET,)), prepared=prepared)
