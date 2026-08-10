# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL fixture builders shared by proof-store integration tests."""

from __future__ import annotations

import importlib
import json

from db.connection import Database
from process.provider_directory_proof_store import (
    ensure_dataset_proof_shard_table,
)


importer = importlib.import_module("process.provider_directory_fhir")


async def _create_endpoint_dataset_table(
    database: Database,
    schema: str,
) -> None:
    """Create the minimal parent table required by proof shards."""

    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            publication_metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb
        );
        """
    )


async def _create_dataset_resource_table(
    database: Database,
    schema: str,
) -> None:
    """Create the retained mapped-resource input table."""

    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL REFERENCES
                "{schema}".provider_directory_endpoint_dataset(dataset_id),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            acquired_resource_sha256 varchar(64),
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        """
    )


async def _insert_candidate_parent(
    database: Database,
    schema: str,
    *,
    dataset_id: str,
    endpoint_id: str,
    root_run_id: str,
    source_ids: tuple[str, ...],
    selected_resources: tuple[str, ...],
) -> None:
    """Insert one acquiring parent with its immutable source scope."""

    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, acquisition_root_run_id,
            status, is_current, publication_metadata_json
        ) VALUES (
            :dataset_id, :endpoint_id, :root_run_id,
            :status, false, CAST(:metadata_json AS jsonb)
        );
        """,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        root_run_id=root_run_id,
        status=importer.ENDPOINT_DATASET_ACQUIRING,
        metadata_json=json.dumps(
            {
                "source_ids": list(source_ids),
                "selected_resources": list(selected_resources),
            }
        ),
    )


async def _create_legacy_mirror_tables(
    database: Database,
    schema: str,
    mirror_models: tuple[type, ...],
) -> None:
    """Create the typed and compatibility tables used by the fixture."""

    for model in mirror_models:
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model,
                schema,
                model.__tablename__,
            )
        )
        for statement in importer._artifact_scope_pk_sql(
            model,
            schema,
            model.__tablename__,
        ):
            await database.status(statement)


async def create_proof_store_tables(
    database: Database,
    schema: str,
    *,
    dataset_id: str,
    endpoint_id: str,
    root_run_id: str,
    source_ids: tuple[str, ...],
    selected_resources: tuple[str, ...],
    mirror_models: tuple[type, ...],
) -> None:
    """Create one isolated proof-store schema and seeded acquiring parent."""

    await database.status(f'CREATE SCHEMA "{schema}";')
    await _create_endpoint_dataset_table(database, schema)
    await _create_dataset_resource_table(database, schema)
    await _insert_candidate_parent(
        database,
        schema,
        dataset_id=dataset_id,
        endpoint_id=endpoint_id,
        root_run_id=root_run_id,
        source_ids=source_ids,
        selected_resources=selected_resources,
    )
    await _create_legacy_mirror_tables(database, schema, mirror_models)
    await ensure_dataset_proof_shard_table(database, schema)
