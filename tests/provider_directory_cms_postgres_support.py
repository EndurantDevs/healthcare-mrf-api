# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic dependency rows with real admission digest and identity migration SQL."""

import importlib.util
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from uuid import UUID

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import provider_directory_source_dataset_selection as selection
from api.provider_directory_cms_generation import RESOURCE_FILES, RESOURCE_TYPES
from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpointDataset,
    ProviderDirectorySource,
)
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
)
from process.provider_directory_cms_serving_coverage import build_cms_coverage
from process.provider_directory_resource_identity import bind_resource_identity_batch
from tests.provider_directory_entities_postgres_support import directory_database

ORG_ID = "00000000-0000-0000-0000-000000000010"
SITE_ID = "00000000-0000-0000-0000-000000000020"
NETWORK_ID = "00000000-0000-0000-0000-000000000030"
_RELEASE = "a" * 64
_HASH = "b" * 64


def migration_module(prefix):
    path = next((Path(__file__).resolve().parents[1] / "alembic" / "versions").glob(prefix + "*.py"))
    spec = importlib.util.spec_from_file_location("migration_" + prefix, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _create_catalog(session, schema):
    """Install actual core columns and database digest, not a mocked acceptance check."""
    connection = await session.connection()
    for model in (
        ProviderDirectoryAPIEndpoint,
        ProviderDirectorySource,
        ProviderDirectoryEndpointDataset,
        ProviderDirectoryDatasetResource,
    ):
        await connection.run_sync(lambda sync, table=model.__table__: table.create(sync))
    canonical = migration_module("20260808190000")
    seal = migration_module("20260812020000")
    for sql in (
        canonical._payload_canonical_json_function_sql(schema),
        canonical._payload_sha256_function_sql(schema),
        seal._digest_function_sql(schema),
    ):
        await session.execute(text(sql))
    migration = migration_module("20260930010000")

    def upgrade(sync):
        with Operations.context(MigrationContext.configure(sync)):
            migration.upgrade()

    await connection.run_sync(upgrade)


async def _create_dependencies(session):
    statements = (
        "CREATE TABLE mrf_payer (payer_id varchar(64) PRIMARY KEY)",
        "CREATE TABLE provider_directory_mrf_payer_review_decision (decision_id uuid PRIMARY KEY, action text, "
        "source_id text, resource_type text, resource_id text, payer_id text, source_payload_sha256 text, prior_decision_id uuid)",
        "CREATE TABLE provider_directory_mrf_payer_binding (source_id text, resource_type text, resource_id text, "
        "payer_id text, binding_decision_id uuid, PRIMARY KEY(source_id, resource_id))",
        "CREATE TABLE provider_directory_entity_source_binding (source_id text, resource_type text, resource_id text, "
        "organization_id uuid, site_id uuid, PRIMARY KEY (source_id, resource_type, resource_id))",
        "CREATE INDEX ON provider_directory_entity_source_binding (organization_id)",
        "CREATE INDEX ON provider_directory_entity_source_binding (site_id)",
        "CREATE TABLE provider_directory_entity_release_evidence (source_id text, resource_type text, resource_id text, "
        "release_id text, payload_sha256 text, PRIMARY KEY (source_id, resource_type, resource_id, release_id))",
        "CREATE TABLE provider_directory_insurance_network_source_binding (source_id text, resource_type text, "
        "resource_id text, network_id uuid, PRIMARY KEY(source_id, resource_id))",
        "CREATE INDEX ON provider_directory_insurance_network_source_binding (network_id)",
        "CREATE TABLE provider_directory_insurance_network_plan_evidence (source_id text, release_id text, "
        "network_resource_id text, insurance_plan_resource_id text, plan_payload_sha256 text, "
        "PRIMARY KEY(source_id, release_id, network_resource_id, insurance_plan_resource_id))",
        "CREATE TABLE provider_directory_cms_npd_relationship (dataset_id text, source_id text, release_id text, "
        "resource_type text, resource_id text, source_payload_hash text, raw_payload_sha256 text, "
        "reference_field text, parent_ordinal integer, reference_ordinal integer, target_reference text, "
        "resolution_status text, period_start text, period_end text)",
    )
    for statement in statements:
        await session.execute(text(statement))


def resource_payloads():
    return {
        "Organization": {
            "resource_id": "org-example",
            "name": "Example Organization",
            "active": True,
            "part_of_ref": "https://example.invalid/Organization/external",
            "tax_id": "synthetic-tax-value",
        },
        "Location": {
            "resource_id": "site-example",
            "name": "Example Site",
            "status": "active",
            "managing_organization_ref": "Organization/org-example",
        },
        "InsurancePlan": {
            "resource_id": "plan-example",
            "name": "Example Plan",
            "status": "active",
            "owned_by_ref": "Organization/org-example",
            "administered_by_ref": "Organization/missing",
            "network_refs": ["Organization/org-example", "https://example.invalid/Organization/external"],
            "period_start": "2026-01-01",
            "period_end": "2026-12-31",
        },
        "PractitionerRole": {
            "resource_id": "role-example",
            "active": False,
            "organization_ref": "Organization/org-example",
            "location_refs": ["Location/site-example"],
            "network_refs": ["Organization/org-example"],
            "insurance_plan_refs": ["InsurancePlan/plan-example"],
            "practitioner_ref": "Practitioner/1234567890",
        },
        **{
            kind: {"resource_id": kind.lower() + "-example"}
            for kind in ("Endpoint", "HealthcareService", "Practitioner", "OrganizationAffiliation")
        },
    }


def publication_summary():
    return {
        "source_ids": ["cms-npd"],
        "source_release": {
            "source_id": "cms-npd",
            "generated_at": "2026-01-01",
            "manifest_sha256": "c" * 64,
            "vector_sha256": _RELEASE,
            "files": {
                name: {
                    "sha256": _HASH,
                    "compressed_bytes": 100,
                    "original_bytes": 200,
                    "row_count": 1,
                    "distinct_count": 1,
                }
                for name in RESOURCE_FILES
            },
        },
        ADMISSION_GENERIC_PROOF_SUMMARY_KEY: {
            "dataset_hash": _HASH,
            "resource_count": 8,
            "resource_counts": dict.fromkeys(RESOURCE_TYPES, 1),
            "resource_hashes": dict.fromkeys(RESOURCE_TYPES, _HASH),
        },
    }


async def reseal(session):
    """Use the real SQL digest after an intentional synthetic catalog mutation."""
    await session.execute(
        text("""UPDATE provider_directory_endpoint_dataset SET publication_metadata_sha256 =
        provider_directory_endpoint_dataset_admission_metadata_sha256(publication_metadata_summary_json,
            content_proof_admission_version, content_proof_admission_kind,
            content_proof_admission_sha256, content_proof_resource_types)""")
    )


async def _seed_catalog(session):
    await session.execute(
        ProviderDirectoryAPIEndpoint.__table__.insert().values(
            endpoint_id="synthetic-endpoint",
            canonical_api_base="https://example.invalid/fhir",
            credential_descriptor_hash=_HASH,
            endpoint_signature_hash=_HASH,
        )
    )
    await session.execute(
        ProviderDirectorySource.__table__.insert().values(
            source_id="cms-npd",
            org_name="Example Directory",
            endpoint_id="synthetic-endpoint",
        )
    )
    await session.execute(
        ProviderDirectoryEndpointDataset.__table__.insert().values(
            dataset_id="synthetic-dataset",
            endpoint_id="synthetic-endpoint",
            acquisition_root_run_id="synthetic-root",
            dataset_hash=_HASH,
            status="published",
            is_current=True,
            resource_count=8,
            validated_at=datetime(2026, 1, 1),
            published_at=datetime(2026, 1, 1),
            publication_metadata_summary_json=publication_summary(),
            content_proof_admission_version=1,
            content_proof_admission_kind="generic",
            content_proof_admission_sha256=_HASH,
            content_proof_resource_types=sorted(RESOURCE_TYPES),
        )
    )
    await reseal(session)
    for kind, resource_payload in resource_payloads().items():
        await session.execute(
            ProviderDirectoryDatasetResource.__table__.insert().values(
                dataset_id="synthetic-dataset",
                resource_type=kind,
                resource_id=resource_payload["resource_id"],
                payload_hash=_HASH,
                acquired_resource_sha256=_HASH,
                payload_json=resource_payload,
            )
        )
    for kind in ("InsurancePlan", "PractitionerRole"):
        await bind_resource_identity_batch(
            session, source_id="cms-npd", resource_type=kind, resource_ids=[resource_payloads()[kind]["resource_id"]]
        )


async def _seed_dependencies(session):
    await session.execute(
        text("""INSERT INTO provider_directory_entity_source_binding VALUES
        ('cms-npd', 'Organization', 'org-example', :org_id, NULL),
        ('cms-npd', 'Location', 'site-example', NULL, :site_id),
        ('cms-npd', 'Organization', 'candidate-only', '00000000-0000-0000-0000-000000000099', NULL)"""),
        {"org_id": UUID(ORG_ID), "site_id": UUID(SITE_ID)},
    )
    for kind, resource_id in (("Organization", "org-example"), ("Location", "site-example")):
        await session.execute(
            text(
                "INSERT INTO provider_directory_entity_release_evidence VALUES "
                "('cms-npd', :kind, :resource_id, :release, :hash)"
            ),
            {"kind": kind, "resource_id": resource_id, "release": _RELEASE, "hash": _HASH},
        )
    await session.execute(
        text(
            "INSERT INTO provider_directory_insurance_network_source_binding VALUES "
            "('cms-npd', 'Organization', 'org-example', :network_id)"
        ),
        {"network_id": UUID(NETWORK_ID)},
    )
    await session.execute(
        text(
            "INSERT INTO provider_directory_insurance_network_plan_evidence VALUES "
            "('cms-npd', :release, 'org-example', 'plan-example', :hash)"
        ),
        {"release": _RELEASE, "hash": _HASH},
    )


async def _seed_relationships(session):
    """Give the read tests exact synthetic ledger rows rather than a JSON fallback."""

    fields_by_kind = {
        "Organization": (("part_of_ref", "partOf"),),
        "Location": (("managing_organization_ref", "managingOrganization"), ("part_of_ref", "partOf")),
        "InsurancePlan": (
            ("owned_by_ref", "ownedBy"),
            ("administered_by_ref", "administeredBy"),
            ("network_refs", "network"),
            ("coverage_area_refs", "coverageArea"),
        ),
        "PractitionerRole": (
            ("organization_ref", "organization"),
            ("location_refs", "location"),
            ("network_refs", "network"),
            ("insurance_plan_refs", "insurancePlan"),
        ),
    }
    resource_rows = (
        await session.execute(
            text("SELECT resource_type, resource_id, payload_json FROM provider_directory_dataset_resource")
        )
    ).all()
    present_resources = {(kind, resource_id) for kind, resource_id, _ in resource_rows}
    for kind, resource_id, resource_payload in resource_rows:
        for normalized_field, reference_field in fields_by_kind.get(kind, ()):
            raw_refs = resource_payload.get(normalized_field)
            if raw_refs is None:
                continue
            for ordinal, reference in enumerate(raw_refs if isinstance(raw_refs, list) else [raw_refs], 1):
                target_type, _, target_id = reference.partition("/")
                is_resolved = (
                    target_type,
                    target_id,
                ) in present_resources and reference == f"{target_type}/{target_id}"
                await session.execute(
                    text(
                        "INSERT INTO provider_directory_cms_npd_relationship VALUES "
                        "('synthetic-dataset','cms-npd',:release,:kind,:resource_id,:hash,:hash,"
                        ":field,0,:ordinal,:reference,:status,NULL,NULL)"
                    ),
                    {
                        "release": _RELEASE,
                        "kind": kind,
                        "resource_id": resource_id,
                        "hash": _HASH,
                        "field": reference_field,
                        "ordinal": ordinal,
                        "reference": reference,
                        "status": "resolved" if is_resolved else "unresolved",
                    },
                )


@asynccontextmanager
async def cms_database(monkeypatch, *, prepare=None, seal=True):
    """Extend the guarded disposable fixture without changing any shared database."""
    async with directory_database(monkeypatch) as original_sessions:
        schema = os.environ["HLTHPRT_DB_SCHEMA"]
        monkeypatch.setattr(selection, "_ADMISSION_SCHEMA", schema)
        engine = original_sessions.kw["bind"].execution_options(schema_translate_map={"mrf": schema})
        sessions = async_sessionmaker(engine)
        async with sessions() as session, session.begin():
            await _create_catalog(session, schema)
            await _create_dependencies(session)
            await _seed_catalog(session)
            await _seed_dependencies(session)
            if prepare is not None:
                await prepare(session)
            await _seed_relationships(session)
            connection = await session.connection()
            migration = migration_module("20260930020000")

            def upgrade(sync):
                with Operations.context(MigrationContext.configure(sync)):
                    migration.upgrade()

            await connection.run_sync(upgrade)
        if seal:
            async with sessions() as session:
                await build_cms_coverage(session)
        yield sessions
