# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL schema for semantic proof transaction tests."""

from __future__ import annotations

import importlib

from db.connection import Database
from db.models import (
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryOrganization,
    ProviderDirectoryPractitioner,
    ProviderDirectorySourceResource,
)
from process.provider_directory_proof_store import (
    ensure_dataset_proof_shard_table,
)

importer = importlib.import_module("process.provider_directory_fhir")


def _fixture_models() -> tuple[type, ...]:
    return (
        ProviderDirectoryDatasetResource,
        ProviderDirectoryPractitioner,
        ProviderDirectoryOrganization,
        ProviderDirectoryCanonicalResource,
        ProviderDirectorySourceResource,
    )


async def _create_parent_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64) NOT NULL,
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            publication_metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb
        );
        """
    )


async def _create_checkpoint_table(database: Database, schema: str) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_pagination_checkpoint (
            canonical_api_base text NOT NULL,
            resource_type varchar(64) NOT NULL,
            source_scope_hash varchar(64) NOT NULL,
            dataset_id varchar(96),
            source_ids jsonb NOT NULL,
            acquisition_root_run_id varchar(64) NOT NULL,
            owner_run_id varchar(64) NOT NULL,
            retry_of_run_id varchar(64),
            start_url_hash varchar(64) NOT NULL,
            next_url text,
            state varchar(32) NOT NULL,
            pages_processed bigint NOT NULL DEFAULT 0,
            rows_processed bigint NOT NULL DEFAULT 0,
            recent_cursor_hashes jsonb NOT NULL DEFAULT '[]'::jsonb,
            completeness_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            PRIMARY KEY (
                canonical_api_base,
                resource_type,
                source_scope_hash,
                acquisition_root_run_id
            )
        );
        """
    )


async def _create_dataset_resource_table(
    database: Database,
    schema: str,
) -> None:
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


async def _create_compatibility_tables(
    database: Database,
    schema: str,
) -> None:
    for model in _fixture_models()[1:]:
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model,
                schema,
                model.__tablename__,
            )
        )
        for primary_key_statement in importer._artifact_scope_pk_sql(
            model,
            schema,
            model.__tablename__,
        ):
            await database.status(primary_key_statement)


async def _create_tables(database: Database, schema: str) -> None:
    """Create the exact parent, child, proof, and compatibility fixture."""

    await database.status(f'CREATE SCHEMA "{schema}";')
    await _create_parent_table(database, schema)
    await _create_checkpoint_table(database, schema)
    await _create_dataset_resource_table(database, schema)
    await _create_compatibility_tables(database, schema)
    await ensure_dataset_proof_shard_table(database, schema)
