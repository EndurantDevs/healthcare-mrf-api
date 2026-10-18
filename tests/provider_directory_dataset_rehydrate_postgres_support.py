# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Acquire real retained SOURCE before exercising atomic output rehydration."""

import importlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from process.provider_directory_dataset_rehydrate_types import RehydrationRequest, RehydrationRuntime
from process.provider_directory_generic_source_policy import source_transaction
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.cms_output_snapshot_postgres_support import provision_native_outputs
from tests.cms_registry_complete_publication_postgres_support import source_component_native_migrations
from tests.provider_directory_source_postgres_support import (
    bootstrap_source_component,
    native_source_component,
    provision_source_component,
    source_component_migrations,
)

fhir = importlib.import_module("process.provider_directory_fhir")
SOURCE_ID = "pdfhir_directory_fixture"
ENDPOINT_ID = "endpoint_directory_fixture"
DATASET_ID = "pdds_directory_fixture"
ROOT_RUN_ID = "run_directory_fixture_root"
CANONICAL_BASE = "https://directory.fixture.test/fhir"


def _migrations():
    """Add only current output-model prerequisites to the original SOURCE slice, in DAG order."""
    selected = set(source_component_migrations()) | {
        "20260628143000",
        "20260629160000",
        "20260729120000",
        "20261019030000",
    }
    ordered_prefixes = tuple(prefix for prefix in source_component_native_migrations() if prefix in selected)
    assert set(ordered_prefixes) == selected and len(ordered_prefixes) == len(selected) == 103
    return ordered_prefixes


@asynccontextmanager
async def _component(monkeypatch):
    """Use the existing disposable lifecycle, real roles, migrations and both original provisioners."""
    async with _owned_database(_database_url()) as (url, _admin):
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text("CREATE SCHEMA mrf"))
            settings_by_name = dict(
                HOST=url.host,
                PORT=url.port,
                DATABASE=url.database,
                USER=url.username,
                PASSWORD=url.password or "",
                SCHEMA="mrf",
            )
            for name, setting_value in settings_by_name.items():
                monkeypatch.setenv("HLTHPRT_DB_" + name, str(setting_value))
            monkeypatch.setenv("DB_SCHEMA", "mrf")
            monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
            async with native_source_component(engine, "mrf", monkeypatch, legacy_builder=True) as component:
                await bootstrap_source_component(engine, component.policy, migration_prefixes=_migrations())
                await provision_native_outputs(component)
                await provision_source_component(engine, component.policy)
                monkeypatch.setenv("HLTHPRT_DB_USER", component.publisher)
                monkeypatch.setenv("HLTHPRT_DB_READER_USER", component.reader)
                monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", url.password or "")
                await component.database._connect_reader()
                yield component
        finally:
            await engine.dispose()


def candidate_for(resources, *, semantic=False):
    """Declare the real acquisition contract before any input COPY."""
    selected = ("Practitioner",) if semantic else ("Location",)
    return fhir.EndpointDatasetCandidate(
        endpoint_id=ENDPOINT_ID,
        dataset_id=DATASET_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=(SOURCE_ID,),
        selected_resources=selected,
        expected_resources=selected,
        import_run_id=ROOT_RUN_ID,
        previous_dataset_id=None,
        resource_hash_contract=(
            fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT if semantic else fhir.LEGACY_RESOURCE_HASH_CONTRACT
        ),
        semantic_projection_as_of="2026-08-09" if semantic else None,
        proof_resource_scope=tuple(sorted({row["resource_type"] for row in resources})) if semantic else None,
    )


async def _seed_identity(database):
    """Create the exact endpoint and logical source under the original native owner."""
    await database.status(
        "INSERT INTO mrf.provider_directory_api_endpoint "
        "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
        "VALUES (:endpoint,:base,repeat('c',64),repeat('d',64))",
        endpoint=ENDPOINT_ID,
        base=CANONICAL_BASE,
    )
    await database.status(
        "INSERT INTO mrf.provider_directory_source "
        "(source_id,org_name,endpoint_id,canonical_api_base,requires_registration,requires_api_key) "
        "VALUES (:source,'Rehydration fixture',:endpoint,:base,false,false)",
        source=SOURCE_ID,
        endpoint=ENDPOINT_ID,
        base=CANONICAL_BASE,
    )


async def _publish_candidate(database, candidate, resources, validated):
    """Promote only the original completed native candidate using the production header fence."""
    async with source_transaction(database):
        await fhir._publish_validated_artifact_dataset(
            fhir.ProviderDirectoryArtifactDataset(
                SOURCE_ID,
                ENDPOINT_ID,
                DATASET_ID,
                ROOT_RUN_ID,
                dataset_hash=validated["dataset_hash"],
                selected_resources=candidate.selected_resources,
                retained_resources=tuple(sorted({row["resource_type"] for row in resources})),
            )
        )


async def _acquire(database, resources, candidate):
    """Use actual resource/proof COPY, validation, attachment and protected header promotion."""
    async with source_transaction(database), database.acquire() as connection:
        await _seed_identity(database)
        await fhir._ensure_endpoint_dataset_candidate(candidate)
        for resource_type in sorted({row["resource_type"] for row in resources}):
            await fhir._upsert_dataset_resource_rows_on_connection(
                connection,
                [row for row in resources if row["resource_type"] == resource_type],
                persist_content_proof=True,
                resource_hash_contract=candidate.resource_hash_contract,
                semantic_projection_as_of=candidate.semantic_projection_as_of,
            )
    diagnostics_by_resource = {
        resource: {"rows_fetched": sum(row["resource_type"] == resource for row in resources), "complete": True}
        for resource in candidate.selected_resources
    }
    validated = await fhir._validate_endpoint_dataset_candidate(candidate, diagnostics_by_resource)
    assert validated["validated"] and not validated["published"]
    await _publish_candidate(database, candidate, resources, validated)
    return validated


async def source_rows(database):
    """Read exact committed immutable payloads through the real SOURCE parent."""
    async with source_transaction(database):
        rows = await database.all(
            "SELECT resource_type,resource_id,payload_hash,payload_json FROM mrf.provider_directory_dataset_resource "
            "WHERE dataset_id=:dataset ORDER BY resource_type,resource_id",
            dataset=DATASET_ID,
        )
        return tuple(
            (row[0], row[1], row[2], json.loads(row[3]) if isinstance(row[3], str) else row[3]) for row in rows
        )


@asynccontextmanager
async def rehydration_case(monkeypatch, resources, *, semantic=False):
    """Keep genuine operator provisioning and SOURCE custody through every assertion."""
    async with _component(monkeypatch) as component:
        database, schema = component.database, component.policy.schema
        monkeypatch.setattr(fhir, "db", database)
        candidate = candidate_for(resources, semantic=semantic)
        validated = await _acquire(database, resources, candidate)
        runtime = RehydrationRuntime(
            database=database,
            schema=schema,
            models_by_type=fhir.RESOURCE_MODELS_BY_TYPE,
            upsert_batch=fhir._reject_rehydration_upsert,
            directory=fhir,
        )
        request = RehydrationRequest(SOURCE_ID, DATASET_ID, ROOT_RUN_ID, "run_fixture_rehydrate", batch_size=1)
        yield SimpleNamespace(
            database=database,
            schema=schema,
            component=component,
            runtime=runtime,
            request=request,
            candidate=candidate,
            validated=validated,
            original_source=await source_rows(database),
        )
