# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Low-level PostgreSQL fixtures for artifact-scope capacity tests."""

from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from db.connection import Database


importer = importlib.import_module("process.provider_directory_fhir")

_POSTGRES_DSN_ENV = "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN"
_RUN_ID = "run_" + "a" * 32
_SELECTED_RESOURCE_TYPES = frozenset(
    {"OrganizationAffiliation", "PractitionerRole"}
)

@dataclass(frozen=True)
class _ArtifactFixture:
    database: Database
    schema: str
    fence: importer.ProviderDirectoryArtifactDatasetFence
    projection: importer._ProviderDirectoryArtifactScopeExactProjection
    relation_by_table: dict[str, str]
    tablespace_oid: int


def _configure_database(monkeypatch: pytest.MonkeyPatch, dsn: str) -> None:
    url = make_url(dsn)
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host or "127.0.0.1"))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username or "postgres"))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database or "postgres"))
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "8")


async def _require_postgresql_18(database: Database) -> None:
    try:
        server_version_num = int(
            await database.scalar(
                "SELECT current_setting('server_version_num')::integer;"
            )
            or 0
        )
    except (OSError, OperationalError):
        pytest.skip("artifact capacity tests need disposable PostgreSQL 18")
    if server_version_num < 180000:
        pytest.skip("artifact capacity tests require PostgreSQL 18")


def _incompressible_text(seed: str, *, repetitions: int = 320) -> str:
    """Return deterministic high-entropy text that PostgreSQL must TOAST."""

    return "".join(
        hashlib.sha256(f"{seed}:{index}".encode("utf-8")).hexdigest()
        for index in range(repetitions)
    )


async def _create_artifact_fixture_tables(
    database: Database,
    schema: str,
) -> tuple[str, str]:
    """Create source, dataset, and retained-resource fixture tables."""
    source_ref = importer._unscoped_qt(
        schema,
        importer.ProviderDirectorySource.__tablename__,
    )
    dataset_ref = importer._unscoped_qt(
        schema,
        importer.ProviderDirectoryEndpointDataset.__tablename__,
    )
    resource_ref = importer._unscoped_qt(
        schema,
        importer.ProviderDirectoryDatasetResource.__tablename__,
    )
    await database.status(
        importer._provider_directory_artifact_scope_table_sql(
            importer.ProviderDirectorySource,
            schema,
            importer.ProviderDirectorySource.__tablename__,
        )
    )
    await database.status(
        f"""
        CREATE TABLE {dataset_ref} (
            dataset_id varchar(96) PRIMARY KEY,
            published_at timestamp,
            validated_at timestamp,
            created_at timestamp
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {resource_ref} (
            dataset_id varchar(96) NOT NULL,
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_json json NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        """
    )
    return source_ref, resource_ref


async def _insert_artifact_source_and_dataset(
    database: Database,
    schema: str,
    source_ref: str,
) -> None:
    """Insert the selected source plus its current dataset."""
    dataset_ref = importer._unscoped_qt(
        schema,
        importer.ProviderDirectoryEndpointDataset.__tablename__,
    )
    await database.status(
        f"""
        INSERT INTO {source_ref} (
            source_id, org_name, requires_registration, requires_api_key,
            metadata_json, created_at, updated_at
        )
        VALUES (
            'source-a', 'Synthetic UHC acceptance source', false, false,
            CAST(:metadata_json AS json), now(), now()
        );
        """,
        metadata_json=json.dumps(
            {"catalog_evidence": _incompressible_text("source")}
        ),
    )
    await database.status(
        f"""
        INSERT INTO {dataset_ref} (
            dataset_id, published_at, validated_at, created_at
        )
        VALUES ('dataset-a', now(), now(), now());
        """
    )


def _role_resource_payloads() -> tuple[tuple[str, str, dict[str, object]], ...]:
    """Return two high-entropy PractitionerRole resource fixtures."""
    return (
        (
            "PractitionerRole",
            "role-a",
            {
                "active": True,
                "npi": 1000000001,
                "practitioner_ref": "Practitioner/practitioner-a",
                "organization_ref": "Organization/organization-a",
                "network_refs": ["Network/network-a"],
                "availability_exceptions": _incompressible_text("role-a"),
                "plan_scope": {
                    "payer_reported": _incompressible_text(
                        "role-a-plan",
                        repetitions=160,
                    )
                },
            },
        ),
        (
            "PractitionerRole",
            "role-b",
            {
                "active": True,
                "npi": 1000000002,
                "practitioner_ref": "Practitioner/practitioner-b",
                "organization_ref": "Organization/organization-b",
                "network_refs": ["Network/network-b"],
                "availability_exceptions": _incompressible_text("role-b"),
                "plan_scope": {
                    "payer_reported": _incompressible_text(
                        "role-b-plan",
                        repetitions=160,
                    )
                },
            },
        ),
    )


def _affiliation_resource_payloads() -> tuple[
    tuple[str, str, dict[str, object]],
    ...,
]:
    """Return explicit non-ownership plan-membership fixtures."""
    return (
        (
            "OrganizationAffiliation",
            "affiliation-a",
            {
                "active": True,
                "organization_ref": "InsurancePlan/plan-a",
                "participating_organization_ref": (
                    "Organization/organization-a"
                ),
                "insurance_plan_refs": ["InsurancePlan/plan-a"],
                "relationship_type": "payer_reported_provider_plan_membership",
                "ownership_status": "not_asserted",
                "source_lineage": {
                    "raw_evidence": _incompressible_text("affiliation-a")
                },
            },
        ),
        (
            "OrganizationAffiliation",
            "affiliation-b",
            {
                "active": True,
                "organization_ref": "InsurancePlan/plan-a",
                "participating_organization_ref": (
                    "Organization/organization-b"
                ),
                "insurance_plan_refs": ["InsurancePlan/plan-a"],
                "relationship_type": "payer_reported_provider_plan_membership",
                "ownership_status": "not_asserted",
                "source_lineage": {
                    "raw_evidence": _incompressible_text("affiliation-b")
                },
            },
        ),
    )


async def _insert_artifact_resources(
    database: Database,
    resource_ref: str,
) -> None:
    """Insert all retained resource payload fixtures."""
    resource_payloads = (
        *_role_resource_payloads(),
        *_affiliation_resource_payloads(),
    )
    for (
        resource_type,
        resource_id,
        resource_payload_by_field,
    ) in resource_payloads:
        await database.status(
            f"""
            INSERT INTO {resource_ref} (
                dataset_id, resource_type, resource_id, payload_json
            )
            VALUES (
                'dataset-a', :resource_type, :resource_id,
                CAST(:payload_json AS json)
            );
            """,
            resource_type=resource_type,
            resource_id=resource_id,
            payload_json=json.dumps(resource_payload_by_field),
        )


async def _create_source_and_resource_fixture(
    database: Database,
    schema: str,
) -> importer.ProviderDirectoryArtifactDatasetFence:
    """Create the source dataset and return its immutable build fence."""
    source_ref, resource_ref = await _create_artifact_fixture_tables(
        database,
        schema,
    )
    await _insert_artifact_source_and_dataset(database, schema, source_ref)
    await _insert_artifact_resources(database, resource_ref)
    return importer.ProviderDirectoryArtifactDatasetFence(
        (
            importer.ProviderDirectoryArtifactDataset(
                source_id="source-a",
                endpoint_id="endpoint-a",
                dataset_id="dataset-a",
                evidence_run_id=_RUN_ID,
                selected_resources=tuple(sorted(_SELECTED_RESOURCE_TYPES)),
            ),
        )
    )


async def _create_complete_empty_scope_layout(
    database: Database,
    schema: str,
) -> dict[str, str]:
    relation_by_table: dict[str, str] = {}
    models = (importer.ProviderDirectorySource, *importer.RESOURCE_MODELS)
    for model in models:
        scope_table = (
            importer._provider_directory_artifact_scope_table_prefix(
                model.__tablename__
            )
            + "_pg"
        )
        relation_by_table[model.__tablename__] = scope_table
        await importer._create_provider_directory_artifact_scope_layout(
            model,
            schema,
            scope_table,
        )

    for model in models:
        scope_table = relation_by_table[model.__tablename__]
        scope_ref = importer._unscoped_qt(schema, scope_table)
        assert int(await database.scalar(f"SELECT count(*) FROM {scope_ref};")) == 0
        primary_key_count = int(
            await database.scalar(
                """
                SELECT count(*)
                  FROM pg_constraint
                 WHERE conrelid = to_regclass(:relation_ref)
                   AND contype = 'p';
                """,
                relation_ref=scope_ref,
            )
            or 0
        )
        assert primary_key_count == 1
        if model in {
            importer.ProviderDirectoryPractitionerRole,
            importer.ProviderDirectoryOrganizationAffiliation,
        }:
            bucket_index_count = int(
                await database.scalar(
                    """
                    SELECT count(*)
                      FROM pg_indexes
                     WHERE schemaname = :schema
                       AND tablename = :table_name
                       AND indexdef LIKE '%hashtextextended%';
                    """,
                    schema=schema,
                    table_name=scope_table,
                )
                or 0
            )
            assert bucket_index_count == 1
    return relation_by_table
