# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData
from sqlalchemy.exc import IntegrityError, OperationalError

from api.endpoint import npi as npi_endpoint
from db.connection import Database
from process import provider_directory_profile as profile
from process.uhc_provider_file_identity import (
    PROVIDER_MEMBERSHIP,
    UHCSourceFileDescriptor,
    logical_scope_for_file,
)
from process.uhc_retained_dataset import _provider_resource_rows


FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures"
FHIR_FIXTURE_PATH = (
    FIXTURE_DIRECTORY / "provider_directory_profile_affiliations.json"
)
SQL_FIXTURE_PATH = (
    FIXTURE_DIRECTORY / "provider_directory_profile_affiliations.sql"
)
importer = importlib.import_module("process.provider_directory_fhir")
LOGGER = logging.getLogger(__name__)


def _json_default(raw_value: Any) -> str:
    if isinstance(raw_value, (date, datetime)):
        return raw_value.isoformat()
    raise TypeError(
        f"unsupported fixture value: {type(raw_value).__name__}"
    )


def _decoded(json_value: Any) -> Any:
    return (
        json.loads(json_value)
        if isinstance(json_value, str)
        else json_value
    )


def _plan_relation_nodes(
    raw_plan: Any,
    relation_name: str,
) -> list[dict[str, Any]]:
    if isinstance(raw_plan, dict):
        matches = (
            [raw_plan]
            if raw_plan.get("Relation Name") == relation_name
            else []
        )
        return matches + [
            node
            for child in raw_plan.values()
            for node in _plan_relation_nodes(child, relation_name)
        ]
    if isinstance(raw_plan, list):
        return [
            node
            for child in raw_plan
            for node in _plan_relation_nodes(child, relation_name)
        ]
    return []


def _plan_index_nodes(
    raw_plan: Any,
    index_name: str,
) -> list[dict[str, Any]]:
    if isinstance(raw_plan, dict):
        matches = (
            [raw_plan] if raw_plan.get("Index Name") == index_name else []
        )
        return matches + [
            node
            for child in raw_plan.values()
            for node in _plan_index_nodes(child, index_name)
        ]
    if isinstance(raw_plan, list):
        return [
            node
            for child in raw_plan
            for node in _plan_index_nodes(child, index_name)
        ]
    return []


def _plan_metric_values(raw_plan: Any, metric_name: str) -> list[float]:
    if isinstance(raw_plan, dict):
        values = (
            [float(raw_plan[metric_name])]
            if raw_plan.get(metric_name) is not None
            else []
        )
        return values + [
            value
            for child in raw_plan.values()
            for value in _plan_metric_values(child, metric_name)
        ]
    if isinstance(raw_plan, list):
        return [
            value
            for child in raw_plan
            for value in _plan_metric_values(child, metric_name)
        ]
    return []


def _plan_execution_ms(raw_plan: Any) -> float:
    decoded_plan = _decoded(raw_plan)
    if (
        not isinstance(decoded_plan, list)
        or not decoded_plan
        or not isinstance(decoded_plan[0], dict)
    ):
        raise AssertionError("PostgreSQL EXPLAIN JSON root is invalid")
    return float(decoded_plan[0]["Execution Time"])


def _plan_temp_blocks(raw_plan: Any) -> int:
    return int(
        max(
            [
                *_plan_metric_values(raw_plan, "Temp Read Blocks"),
                *_plan_metric_values(raw_plan, "Temp Written Blocks"),
            ],
            default=0,
        )
    )


async def _require_profile_database(database: Database) -> None:
    """Skip unless the configured PostgreSQL database is disposable."""
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("profile affiliation tests need disposable PostgreSQL")
    is_schema_test_opted_in = os.getenv(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_ALLOW_SCHEMA_TESTS",
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    if "test" not in database_name.lower() and not is_schema_test_opted_in:
        pytest.skip("profile affiliation tests need a test database")


async def _create_fixture_tables(
    database: Database,
    schema: str,
) -> None:
    """Create the narrow typed schema needed by generated profile SQL."""
    fixture_sql = SQL_FIXTURE_PATH.read_text(encoding="utf-8").replace(
        "{{SCHEMA}}",
        schema,
    )
    for sql_statement in fixture_sql.split("-- statement"):
        if sql_statement := sql_statement.strip():
            await database.status(sql_statement)
    await database.status(
        profile.profile_evidence_table_sql(
            schema,
            "profile_evidence",
            logged=True,
        )
    )
    await database.status(
        profile.profile_table_sql(schema, "profile", logged=True)
    )
    checkpoint_table = (
        importer.ProviderDirectoryProfileBuildCheckpoint.__table__.to_metadata(
            MetaData(),
            schema=schema,
        )
    )
    await database.create_table(checkpoint_table)


async def _insert_typed_resource(
    database: Database,
    schema: str,
    source_id: str,
    raw_resource: dict[str, Any],
) -> None:
    """Parse and insert one raw FHIR resource into its typed table."""
    parsed_resource = importer.parse_fhir_resource(
        source_id,
        raw_resource,
        run_id=f"run-{source_id}",
    )
    assert parsed_resource is not None
    model, typed_fields = parsed_resource
    table_ref = profile.qualified_table(schema, model.__tablename__)
    await database.status(
        f"INSERT INTO {table_ref} SELECT * FROM jsonb_populate_record("
        f"NULL::{table_ref}, CAST(:typed_fields AS jsonb));",
        typed_fields=json.dumps(typed_fields, default=_json_default),
    )


async def _insert_source_resources(
    database: Database,
    schema: str,
    resources_by_source_id: dict[str, list[dict[str, Any]]],
) -> None:
    """Insert source lineage and every normalized raw-FHIR fixture row."""
    source_ref = profile.qualified_table(
        schema,
        "provider_directory_source",
    )
    for source_number, (source_id, source_resources) in enumerate(
        sorted(resources_by_source_id.items()),
        start=1,
    ):
        await database.status(
            f"""
            INSERT INTO {source_ref} (
                source_id, endpoint_id, canonical_api_base, org_name, plan_name
            ) VALUES (
                :source_id, :endpoint_id, :api_base, :org_name, :plan_name
            );
            """,
            source_id=source_id,
            endpoint_id=f"profile-endpoint-{source_number}",
            api_base=f"https://payer-{source_number}.test/fhir",
            org_name="Example Health Plan",
            plan_name=f"Example Plan {source_number}",
        )
        for raw_resource in source_resources:
            await _insert_typed_resource(
                database,
                schema,
                source_id,
                raw_resource,
            )


async def _insert_raw_fhir_fixture(
    database: Database,
    schema: str,
) -> None:
    """Insert typed FHIR rows plus current and stale dataset edges."""
    fixture_payload = json.loads(
        FHIR_FIXTURE_PATH.read_text(encoding="utf-8")
    )
    await _insert_source_resources(
        database,
        schema,
        fixture_payload["sources"],
    )
    edge_ref = profile.qualified_table(
        schema,
        "provider_directory_dataset_affiliation_organization",
    )
    await database.status(
        f"""
        INSERT INTO {edge_ref} VALUES
            ('profile-dataset-a', 'clinic', 'aff-positive'),
            ('profile-dataset-a', 'other-clinic', 'aff-primary-match'),
            ('profile-dataset-b', 'clinic', 'aff-positive'),
            ('stale-dataset-a', 'clinic', 'aff-stale');
        """
    )
    await _insert_uhc_facility_fixture(database, schema)


def _uhc_logical_scope():
    return logical_scope_for_file(
        UHCSourceFileDescriptor(
            "ifp",
            PROVIDER_MEMBERSHIP,
            "JSON_Providers_ILIEX.json",
        )
    )


def _uhc_lineage_by_field(
    *,
    source_file_id: str = "f" * 64,
    artifact_sha256: str = "a" * 64,
    catalog_set_sha256: str = "c" * 64,
    record_ordinal: int = 17,
) -> dict[str, Any]:
    return {
        "catalog_set_sha256": catalog_set_sha256,
        "source_file_id": source_file_id,
        "file_name": "JSON_Providers_ILIEX.json",
        "artifact_sha256": artifact_sha256,
        "record_ordinal": record_ordinal,
        "logical_scope_id": _uhc_logical_scope().logical_scope_id,
    }


def _uhc_facility_semantic_fact(
    *,
    npi: str = "1000000491",
    name: str = "Example UHC Facility",
) -> dict[str, Any]:
    return {
        "type": "FACILITY",
        "npi": npi,
        "name": None,
        "facility_name": name,
        "facility_type": ["Clinic"],
        "gender": None,
        "accepting": "accepting",
        "addresses": [
            {
                "address": "1 Main St",
                "city": "Chicago",
                "state": "IL",
                "zip": "60601",
                "phone": "3125551212",
            }
        ],
        "plans": [
            {
                "plan_id_type": "HIOS-PLAN-ID",
                "plan_id": "12345IL0010001",
                "years": [2026],
                "network_tier": "PREFERRED",
            }
        ],
        "specialty": ["Family Medicine"],
        "last_updated_on": "2026-07-01",
    }


async def _insert_uhc_profile_source(
    database: Database,
    schema: str,
) -> None:
    source_ref = profile.qualified_table(
        schema,
        "provider_directory_source",
    )
    await database.status(
        f"""
        INSERT INTO {source_ref} (
            source_id, endpoint_id, canonical_api_base, org_name, plan_name
        ) VALUES (
            'profile-source-uhc', 'profile-endpoint-uhc',
            'https://providermrf.uhc.com', 'UnitedHealthcare',
            'Official provider files'
        );
        """
    )


async def _insert_canonical_uhc_resources(
    database: Database,
    schema: str,
    *,
    npi: str,
    facility_name: str,
    source_file_id: str,
    record_ordinal: int,
    lineage_by_field: dict[str, Any],
) -> tuple[str, str]:
    """Land production canonical UHC payloads into their typed relations."""
    assert profile.is_valid_npi(npi)
    canonical_rows, _membership_keys = _provider_resource_rows(
        _uhc_facility_semantic_fact(npi=npi, name=facility_name),
        source_file_id=source_file_id,
        ordinal=record_ordinal,
        logical_scope=_uhc_logical_scope(),
        source_lineage=lineage_by_field,
    )
    resource_id_by_type: dict[str, str] = {}
    for (
        resource_type,
        resource_id,
        _payload_hash,
        payload_json,
        _source_rank,
    ) in canonical_rows:
        if resource_type not in {"Organization", "OrganizationAffiliation"}:
            continue
        resource_id_by_type[resource_type] = resource_id
        model = importer.RESOURCE_MODELS_BY_TYPE[resource_type]
        table_ref = profile.qualified_table(schema, model.__tablename__)
        typed_fields_by_name = {
            **json.loads(payload_json),
            "source_id": "profile-source-uhc",
            "updated_at": datetime(2026, 7, 19),
        }
        await database.status(
            f"INSERT INTO {table_ref} SELECT * FROM jsonb_populate_record("
            f"NULL::{table_ref}, CAST(:typed_fields AS jsonb));",
            typed_fields=json.dumps(
                typed_fields_by_name,
                default=_json_default,
                sort_keys=True,
            ),
        )
    assert set(resource_id_by_type) == {
        "Organization",
        "OrganizationAffiliation",
    }
    return (
        resource_id_by_type["Organization"],
        resource_id_by_type["OrganizationAffiliation"],
    )


async def _insert_uhc_membership_edges(
    database: Database,
    schema: str,
    *,
    organization_resource_id: str,
    affiliation_resource_id: str,
    dataset_id: str,
) -> None:
    edge_ref = profile.qualified_table(
        schema,
        "provider_directory_dataset_affiliation_organization",
    )
    await database.status(
        f"INSERT INTO {edge_ref} VALUES "
        "(:dataset_id, :organization_resource_id, "
        ":affiliation_resource_id);",
        dataset_id=dataset_id,
        organization_resource_id=organization_resource_id,
        affiliation_resource_id=affiliation_resource_id,
    )


async def _insert_non_uhc_direct_organization(
    database: Database,
    schema: str,
) -> None:
    """Install a generic NPI Organization that must not become a direct fact."""
    assert profile.is_valid_npi("1003821380")
    organization_ref = profile.qualified_table(
        schema,
        "provider_directory_organization",
    )
    await database.status(
        f"""
        INSERT INTO {organization_ref} (
            source_id, resource_id, npi, name, active, type_codes,
            telecom, address_json, updated_at
        ) VALUES (
            'profile-source-a', 'generic-direct-organization', 1003821380,
            'Generic FHIR Facility', TRUE, '["Clinic"]'::jsonb,
            '[{{"system":"phone","value":"3125550199"}}]'::jsonb,
            '[{{"city":"Chicago","state":"IL"}}]'::jsonb, now()
        );
        """
    )


async def _insert_uhc_facility_with_edge(
    database: Database,
    schema: str,
    *,
    npi: str,
    facility_name: str,
    lineage_by_field: dict[str, Any],
    dataset_id: str,
) -> tuple[str, str]:
    organization_id, affiliation_id = await _insert_canonical_uhc_resources(
        database,
        schema,
        npi=npi,
        facility_name=facility_name,
        source_file_id=str(lineage_by_field["source_file_id"]),
        record_ordinal=int(lineage_by_field["record_ordinal"]),
        lineage_by_field=lineage_by_field,
    )
    await _insert_uhc_membership_edges(
        database,
        schema,
        organization_resource_id=organization_id,
        affiliation_resource_id=affiliation_id,
        dataset_id=dataset_id,
    )
    return organization_id, affiliation_id


async def _insert_current_uhc_facility(
    database: Database,
    schema: str,
) -> None:
    await _insert_uhc_facility_with_edge(
        database,
        schema,
        npi="1000000491",
        facility_name="Example UHC Facility",
        lineage_by_field=_uhc_lineage_by_field(),
        dataset_id="profile-dataset-uhc",
    )


async def _insert_stale_uhc_facility(
    database: Database,
    schema: str,
) -> None:
    stale_lineage = _uhc_lineage_by_field(
        source_file_id="1" * 64,
        artifact_sha256="2" * 64,
        catalog_set_sha256="3" * 64,
        record_ordinal=18,
    )
    await _insert_uhc_facility_with_edge(
        database,
        schema,
        npi="1234567893",
        facility_name="Stale UHC Facility",
        lineage_by_field=stale_lineage,
        dataset_id="stale-dataset-uhc",
    )


async def _insert_self_referential_uhc_facility(
    database: Database,
    schema: str,
) -> None:
    self_ref_lineage = _uhc_lineage_by_field(
        source_file_id="4" * 64,
        artifact_sha256="5" * 64,
        catalog_set_sha256="6" * 64,
        record_ordinal=19,
    )
    self_ref_organization_id, self_ref_affiliation_id = (
        await _insert_uhc_facility_with_edge(
            database,
            schema,
            npi="1000000004",
            facility_name="Ownership-looking UHC Facility",
            lineage_by_field=self_ref_lineage,
            dataset_id="profile-dataset-uhc",
        )
    )
    affiliation_ref = profile.qualified_table(
        schema,
        "provider_directory_organization_affiliation",
    )
    await database.status(
        f"UPDATE {affiliation_ref} "
        "SET organization_ref=:organization_ref "
        "WHERE source_id='profile-source-uhc' "
        "AND resource_id=:affiliation_resource_id;",
        organization_ref=f"Organization/{self_ref_organization_id}",
        affiliation_resource_id=self_ref_affiliation_id,
    )


async def _insert_mismatched_scope_uhc_facility(
    database: Database,
    schema: str,
) -> None:
    mismatched_scope_lineage = _uhc_lineage_by_field(
        source_file_id="7" * 64,
        artifact_sha256="8" * 64,
        catalog_set_sha256="9" * 64,
        record_ordinal=20,
    )
    _organization_id, affiliation_id = (
        await _insert_uhc_facility_with_edge(
            database,
            schema,
            npi="1000000012",
            facility_name="Mismatched-scope UHC Facility",
            lineage_by_field=mismatched_scope_lineage,
            dataset_id="profile-dataset-uhc",
        )
    )
    affiliation_ref = profile.qualified_table(
        schema,
        "provider_directory_organization_affiliation",
    )
    await database.status(
        f"UPDATE {affiliation_ref} "
        "SET plan_scope=jsonb_set("
        "plan_scope, '{logical_scope_id}', "
        "to_jsonb(CAST(:wrong_scope AS text))"
        ") WHERE source_id='profile-source-uhc' "
        "AND resource_id=:affiliation_resource_id;",
        wrong_scope="0" * 64,
        affiliation_resource_id=affiliation_id,
    )


async def _insert_malformed_plan_refs_uhc_facility(
    database: Database,
    schema: str,
) -> None:
    malformed_plan_refs_lineage = _uhc_lineage_by_field(
        source_file_id="a" * 64,
        artifact_sha256="b" * 64,
        catalog_set_sha256="d" * 64,
        record_ordinal=21,
    )
    _organization_id, affiliation_id = (
        await _insert_uhc_facility_with_edge(
            database,
            schema,
            npi="1000000020",
            facility_name="Malformed-plan-refs UHC Facility",
            lineage_by_field=malformed_plan_refs_lineage,
            dataset_id="profile-dataset-uhc",
        )
    )
    affiliation_ref = profile.qualified_table(
        schema,
        "provider_directory_organization_affiliation",
    )
    await database.status(
        f"UPDATE {affiliation_ref} "
        "SET insurance_plan_refs="
        "CAST(:malformed_plan_refs AS jsonb) "
        "WHERE source_id='profile-source-uhc' "
        "AND resource_id=:affiliation_resource_id;",
        malformed_plan_refs=json.dumps({"unexpected": "shape"}),
        affiliation_resource_id=affiliation_id,
    )


async def _insert_uhc_facility_fixture(
    database: Database,
    schema: str,
) -> None:
    """Land positive and fail-closed semantic-to-canonical UHC evidence."""
    await _insert_uhc_profile_source(database, schema)
    await _insert_current_uhc_facility(database, schema)
    await _insert_stale_uhc_facility(database, schema)
    await _insert_self_referential_uhc_facility(database, schema)
    await _insert_mismatched_scope_uhc_facility(database, schema)
    await _insert_malformed_plan_refs_uhc_facility(database, schema)
    await _insert_non_uhc_direct_organization(database, schema)


async def _archive_profile_dataset_a(database: Database, schema: str) -> None:
    """Retain exact typed payloads for the attested A dataset."""
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            created_at timestamp,
            validated_at timestamp,
            published_at timestamp
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE {schema}.provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL,
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset
            (dataset_id, created_at, validated_at, published_at)
        VALUES ('profile-dataset-a', now(), now(), now());
        """
    )
    for resource_type, table_name in (
        ("PractitionerRole", "provider_directory_practitioner_role"),
        ("Organization", "provider_directory_organization"),
        (
            "OrganizationAffiliation",
            "provider_directory_organization_affiliation",
        ),
    ):
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_dataset_resource (
                dataset_id, resource_type, resource_id,
                payload_hash, payload_json
            )
            SELECT 'profile-dataset-a', :resource_type, resource_id,
                   repeat('a', 64), to_jsonb(resource)
              FROM {schema}.{table_name} AS resource
             WHERE source_id = 'profile-source-a';
            """,
            resource_type=resource_type,
        )


@asynccontextmanager
async def _profile_database(monkeypatch):
    """Yield an isolated schema and remove it after the DB regression."""
    schema = f"provider_directory_profile_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_profile_database(database)
        await database.status(
            f"CREATE SCHEMA {profile.quote_identifier(schema)};"
        )
        is_schema_created = True
        await _create_fixture_tables(database, schema)
        await _insert_raw_fhir_fixture(database, schema)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(
                f"DROP SCHEMA IF EXISTS "
                f"{profile.quote_identifier(schema)} CASCADE;"
            )
        await database.disconnect()


async def _create_profile_bucket_probe_scopes(
    database: Database,
    schema: str,
    *,
    row_count: int,
    include_affiliation_edges: bool = False,
) -> tuple[str, str]:
    """Create production-shaped scoped relations with high source fanout."""
    role_relation = importer.ProviderDirectoryPractitionerRole.__tablename__
    affiliation_relation = (
        importer.ProviderDirectoryOrganizationAffiliation.__tablename__
    )
    role_scope = importer._provider_directory_artifact_scope_table_name(
        role_relation,
        "profile-bucket-probe",
    )
    affiliation_scope = (
        importer._provider_directory_artifact_scope_table_name(
            affiliation_relation,
            "profile-bucket-probe",
        )
    )
    await _create_profile_bucket_probe_tables(
        database,
        schema,
        role_scope,
        affiliation_scope,
    )
    await _seed_profile_bucket_probe_rows(
        database,
        schema,
        role_scope,
        affiliation_scope,
        row_count=row_count,
        include_affiliation_edges=include_affiliation_edges,
    )
    for model, scope_table in (
        (importer.ProviderDirectoryPractitionerRole, role_scope),
        (
            importer.ProviderDirectoryOrganizationAffiliation,
            affiliation_scope,
        ),
    ):
        await importer._build_artifact_scope_pk(model, schema, scope_table)
        await database.status(
            f"ANALYZE {profile.qualified_table(schema, scope_table)};"
        )
    return role_scope, affiliation_scope


async def _create_profile_bucket_probe_tables(
    database: Database,
    schema: str,
    role_scope: str,
    affiliation_scope: str,
) -> None:
    """Create the two scoped relations used by the bucket-plan proof."""
    for model, scope_table in (
        (importer.ProviderDirectoryPractitionerRole, role_scope),
        (
            importer.ProviderDirectoryOrganizationAffiliation,
            affiliation_scope,
        ),
    ):
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model,
                schema,
                scope_table,
            )
        )


async def _seed_role_bucket_rows(database, role_scope_ref, row_count) -> None:
    await database.status(
        f"""
        INSERT INTO {role_scope_ref} (
            source_id, resource_id, npi, active, organization_ref, updated_at
        )
        SELECT 'profile-source-a',
               'bucket-role-' || generated.value,
               1588616783,
               true,
               'Organization/clinic',
               now()
          FROM generate_series(1, :row_count) AS generated(value);
        """,
        row_count=row_count,
    )


async def _seed_affiliation_bucket_rows(
    database,
    affiliation_scope_ref,
    row_count,
) -> None:
    await database.status(
        f"""
        INSERT INTO {affiliation_scope_ref} (
            source_id, resource_id, active, organization_ref,
            participating_organization_ref, updated_at
        ) VALUES (
            'profile-source-a', 'aff-positive', true,
            'Organization/parent', 'Organization/clinic', now()
        );
        """
    )
    await database.status(
        f"""
        INSERT INTO {affiliation_scope_ref} (
            source_id, resource_id, active, organization_ref,
            participating_organization_ref, updated_at
        )
        SELECT 'profile-source-a',
               'bucket-affiliation-' || generated.value,
               true,
               'Organization/parent',
               'Organization/clinic',
               now()
          FROM generate_series(1, :row_count) AS generated(value);
        """,
        row_count=row_count,
    )


async def _seed_affiliation_bucket_edges(
    database,
    schema,
    row_count,
) -> None:
    affiliation_edge_ref = profile.qualified_table(
        schema,
        "provider_directory_dataset_affiliation_organization",
    )
    await database.status(
        f"""
        INSERT INTO {affiliation_edge_ref} (
            dataset_id, participating_organization_resource_id,
            affiliation_resource_id
        )
        SELECT 'profile-dataset-a', 'clinic',
               'bucket-affiliation-' || generated.value
          FROM generate_series(1, :row_count) AS generated(value);
        """,
        row_count=row_count,
    )
    await database.status(f"ANALYZE {affiliation_edge_ref};")


async def _seed_profile_bucket_probe_rows(
    database: Database,
    schema: str,
    role_scope: str,
    affiliation_scope: str,
    *,
    row_count: int,
    include_affiliation_edges: bool,
) -> None:
    """Populate production-shaped role and affiliation scope rows."""
    role_scope_ref = profile.qualified_table(schema, role_scope)
    affiliation_scope_ref = profile.qualified_table(
        schema,
        affiliation_scope,
    )
    await _seed_role_bucket_rows(database, role_scope_ref, row_count)
    await _seed_affiliation_bucket_rows(
        database,
        affiliation_scope_ref,
        row_count,
    )
    if include_affiliation_edges:
        await _seed_affiliation_bucket_edges(database, schema, row_count)


async def _build_profile_artifacts(
    database: Database,
    schema: str,
    *,
    source_ids: tuple[str, ...] = (
        "profile-source-a",
        "profile-source-b",
        "profile-source-uhc",
    ),
    dataset_ids: tuple[str, ...] = (
        "profile-dataset-a",
        "profile-dataset-b",
        "profile-dataset-uhc",
    ),
) -> None:
    """Execute evidence and compact-profile SQL for the selected datasets."""
    table_ref = lambda table_name: profile.qualified_table(schema, table_name)
    await database.status(
        profile.profile_evidence_insert_sql(
            target_ref=table_ref("profile_evidence"),
            source_ref=table_ref("provider_directory_source"),
            practitioner_ref=table_ref("provider_directory_practitioner"),
            role_ref=table_ref("provider_directory_practitioner_role"),
            organization_ref=table_ref("provider_directory_organization"),
            service_ref=table_ref("provider_directory_healthcare_service"),
            endpoint_ref=table_ref("provider_directory_endpoint"),
        ),
        source_ids=list(source_ids),
        dataset_ids=list(dataset_ids),
        profile_as_of="2026-07-19",
    )
    await database.status(
        profile.profile_insert_sql(
            evidence_ref=table_ref("profile_evidence"),
            target_ref=table_ref("profile"),
            old_evidence_ref=None,
            rebuild_all=True,
        ),
        generation_id="profile-affiliation-test",
        profile_as_of="2026-07-19",
    )


async def _seed_old_only_profile(
    database: Database,
    schema: str,
) -> int:
    """Add one serving NPI whose selected-source evidence later disappears."""
    removed_npi = 1000000491
    evidence_ref = profile.qualified_table(
        schema,
        profile.PROFILE_EVIDENCE_TABLE,
    )
    profile_ref = profile.qualified_table(schema, profile.PROFILE_TABLE)
    await database.status(
        f"""
        INSERT INTO {evidence_ref} (
            evidence_key, npi, fact_type, fact_key, value_json,
            source_id, endpoint_id, dataset_id, canonical_api_base,
            source_org_name, source_plan_name, resource_type,
            resource_id, role_resource_id, active, effective_start,
            effective_end, observed_at
        ) VALUES (
            md5('old-only-evidence'), :npi, 'specialty',
            md5('old-only-fact'), '{{"code": "OLD-ONLY"}}'::jsonb,
            'profile-source-a', 'profile-endpoint-1',
            'profile-dataset-a', 'https://payer-1.test/fhir',
            'Example Health Plan', 'Example Plan 1',
            'PractitionerRole', 'old-only-role', 'old-only-role',
            TRUE, '2026-01-01', '2026-12-31',
            '2026-07-01'::timestamp
        );
        """,
        npi=removed_npi,
    )
    await database.status(
        profile.profile_insert_sql(
            evidence_ref=evidence_ref,
            target_ref=profile_ref,
            old_evidence_ref=None,
            rebuild_all=True,
            npi_start=removed_npi,
            npi_end=removed_npi + 1,
        ),
        generation_id="old-only-generation",
        profile_as_of="2026-07-19",
        profile_npi_start=removed_npi,
        profile_npi_end=removed_npi + 1,
    )
    return removed_npi


async def _install_fixture_as_serving_profile(
    database: Database,
    schema: str,
) -> None:
    """Copy the monolithic fixture into production-named serving tables."""
    serving_evidence_ref = profile.qualified_table(
        schema,
        profile.PROFILE_EVIDENCE_TABLE,
    )
    serving_profile_ref = profile.qualified_table(
        schema,
        profile.PROFILE_TABLE,
    )
    await database.status(
        profile.profile_evidence_table_sql(
            schema,
            profile.PROFILE_EVIDENCE_TABLE,
            logged=True,
        )
    )
    await database.status(
        profile.profile_table_sql(
            schema,
            profile.PROFILE_TABLE,
            logged=True,
        )
    )
    await database.status(
        f"INSERT INTO {serving_evidence_ref} "
        f"SELECT * FROM {profile.qualified_table(schema, 'profile_evidence')};"
    )
    await database.status(
        f"INSERT INTO {serving_profile_ref} "
        f"SELECT * FROM {profile.qualified_table(schema, 'profile')};"
    )


async def _global_refresh_baseline(database: Database, schema: str):
    """Capture changed, deleted, and unaffected serving evidence."""
    evidence_ref = profile.qualified_table(
        schema,
        profile.PROFILE_EVIDENCE_TABLE,
    )
    changed = await database.first(
        f"SELECT evidence_key, value_json FROM {evidence_ref} "
        "WHERE source_id = 'profile-source-a' "
        "AND fact_type = 'affiliation' "
        "AND resource_id = 'aff-positive';"
    )
    removed = await database.first(
        f"SELECT evidence_key FROM {evidence_ref} "
        "WHERE source_id = 'profile-source-b' "
        "AND fact_type = 'affiliation' "
        "AND resource_id = 'aff-positive';"
    )
    stable = await database.first(
        f"SELECT evidence_key, fact_key, value_json FROM {evidence_ref} "
        "WHERE source_id = 'profile-source-b' "
        "AND fact_type = 'role_identifier' AND resource_id = 'role-b';"
    )
    assert changed is not None
    assert removed is not None
    assert stable is not None
    return SimpleNamespace(changed=changed, removed=removed, stable=stable)


async def _mutate_global_profile_fixture(
    database: Database,
    schema: str,
) -> None:
    """Change one selected fact and delete another before a global refresh."""
    await database.status(
        f"UPDATE {schema}.provider_directory_organization_affiliation "
        "SET network_refs = '[\"Organization/network-updated\"]'::jsonb "
        "WHERE source_id = 'profile-source-a' "
        "AND resource_id = 'aff-positive';"
    )
    await database.status(
        f"DELETE FROM {schema}.provider_directory_organization_affiliation "
        "WHERE source_id = 'profile-source-b' "
        "AND resource_id = 'aff-positive';"
    )


def _existing_global_profile_build(schema: str):
    """Return a selected-equals-retained build and its immutable plan."""
    source_ids = ("profile-source-a", "profile-source-b")
    dataset_ids = ("profile-dataset-a", "profile-dataset-b")
    batch_plan = importer._provider_directory_profile_batch_plan(
        source_ids,
        source_ids,
        dataset_ids,
        has_existing_artifacts=True,
    )
    build = importer._ProviderDirectoryProfileBuild(
        schema=schema,
        generation_id="profile-global-refresh",
        source_ids=source_ids,
        retained_source_ids=source_ids,
        dataset_ids=dataset_ids,
        profile_as_of="2026-07-19",
        evidence_stage="profile_evidence_global_refresh_stage",
        profile_stage="profile_global_refresh_stage",
        build_id="profile-global-refresh-build",
        owner_run_id="profile-global-refresh-run",
        batch_plan=batch_plan,
    )
    return build, batch_plan


def _global_refresh_copy_statements(build) -> set[str]:
    """Return the two physical copy statements forbidden for this plan."""
    evidence_ref = profile.qualified_table(
        build.schema,
        profile.PROFILE_EVIDENCE_TABLE,
    )
    profile_ref = profile.qualified_table(
        build.schema,
        profile.PROFILE_TABLE,
    )
    evidence_stage_ref = profile.qualified_table(
        build.schema,
        build.evidence_stage,
    )
    profile_stage_ref = profile.qualified_table(
        build.schema,
        build.profile_stage,
    )
    return {
        profile.copy_existing_evidence_sql(
            source_ref=evidence_ref,
            target_ref=evidence_stage_ref,
        ),
        profile.copy_unaffected_profiles_sql(
            profile_source_ref=profile_ref,
            evidence_source_ref=evidence_ref,
            evidence_stage_ref=evidence_stage_ref,
            profile_stage_ref=profile_stage_ref,
        ),
    }


async def _assert_existing_global_refresh(
    database: Database,
    build,
    baseline,
    removed_npi: int,
) -> None:
    """Require changed, deleted, removed-NPI, and stable-row correctness."""
    evidence_ref = profile.qualified_table(
        build.schema,
        build.evidence_stage,
    )
    profile_ref = profile.qualified_table(build.schema, build.profile_stage)
    changed = await database.first(
        f"SELECT evidence_key, value_json FROM {evidence_ref} "
        "WHERE source_id = 'profile-source-a' "
        "AND fact_type = 'affiliation' "
        "AND resource_id = 'aff-positive';"
    )
    stable = await database.first(
        f"SELECT evidence_key, fact_key, value_json FROM {evidence_ref} "
        "WHERE source_id = 'profile-source-b' "
        "AND fact_type = 'role_identifier' AND resource_id = 'role-b';"
    )
    stale_evidence_count = await database.scalar(
        f"SELECT count(*) FROM {evidence_ref} "
        "WHERE evidence_key = :changed_evidence_key "
        "OR evidence_key = :removed_evidence_key "
        "OR npi = :removed_npi;",
        changed_evidence_key=baseline.changed.evidence_key,
        removed_evidence_key=baseline.removed.evidence_key,
        removed_npi=removed_npi,
    )
    removed_profile_count = await database.scalar(
        f"SELECT count(*) FROM {profile_ref} WHERE npi = :removed_npi;",
        removed_npi=removed_npi,
    )
    refreshed_profile = await database.first(
        f"SELECT profile_json, source_ids FROM {profile_ref} "
        "WHERE npi = 1588616783;"
    )
    _assert_refreshed_global_evidence(changed, stable, baseline)
    assert stale_evidence_count == 0
    assert removed_profile_count == 0
    _assert_refreshed_global_profile(refreshed_profile)


def _assert_refreshed_global_evidence(changed, stable, baseline) -> None:
    """Require replacement plus byte-equivalent unaffected evidence."""
    assert changed is not None
    assert changed.evidence_key != baseline.changed.evidence_key
    assert _decoded(changed.value_json) != _decoded(
        baseline.changed.value_json
    )
    assert stable is not None
    assert (
        stable.evidence_key,
        stable.fact_key,
        _decoded(stable.value_json),
    ) == (
        baseline.stable.evidence_key,
        baseline.stable.fact_key,
        _decoded(baseline.stable.value_json),
    )


def _assert_refreshed_global_profile(refreshed_profile) -> None:
    """Require the surviving NPI to use only refreshed evidence."""
    assert refreshed_profile is not None
    serialized_profile = json.dumps(
        _decoded(refreshed_profile.profile_json),
        sort_keys=True,
    )
    assert "network-updated" in serialized_profile
    assert "Organization/network-1" not in serialized_profile
    assert refreshed_profile.source_ids == [
        "profile-source-a",
        "profile-source-b",
    ]


def _attested_a_fence():
    return importer.ProviderDirectoryArtifactDatasetFence(
        (
            importer.ProviderDirectoryArtifactDataset(
                source_id="profile-source-a",
                endpoint_id="profile-endpoint-1",
                dataset_id="profile-dataset-a",
                evidence_run_id="run-profile-source-a",
                selected_resources=(
                    "Organization",
                    "OrganizationAffiliation",
                    "PractitionerRole",
                ),
            ),
        )
    )


async def _materialize_attested_a_scope(database, schema, monkeypatch):
    async def materialize_fixture_source(
        scope_schema: str,
        table_name: str,
        source_ids: list[str],
    ) -> None:
        await database.status(
            f"CREATE UNLOGGED TABLE {scope_schema}.{table_name} AS "
            f"SELECT * FROM {scope_schema}.provider_directory_source "
            "WHERE source_id = ANY(CAST(:source_ids AS varchar[]));",
            source_ids=source_ids,
        )

    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_source_scope",
        materialize_fixture_source,
    )
    return await importer._materialize_artifact_scope_tables(
        schema,
        "attested-a",
        _attested_a_fence(),
        importer.PROVIDER_DIRECTORY_ARTIFACT_TARGET_RESOURCE_TYPES["profile"],
    )


async def _overwrite_live_b_and_continue_disjoint(database, schema):
    await database.status(
        f"UPDATE {schema}.provider_directory_organization_affiliation "
        "SET network_refs = '[\"Organization/network-b\"]'::jsonb "
        "WHERE source_id = 'profile-source-a' "
        "AND resource_id = 'aff-positive';"
    )
    await asyncio.wait_for(
        database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET plan_name = 'Disjoint C continued' "
            "WHERE source_id = 'profile-source-b';"
        ),
        timeout=1,
    )


def _attested_a_profile_build(schema):
    return importer._ProviderDirectoryProfileBuild(
        schema=schema,
        generation_id="attested-a",
        source_ids=("profile-source-a",),
        retained_source_ids=("profile-source-a",),
        dataset_ids=("profile-dataset-a",),
        profile_as_of="2026-07-19",
        evidence_stage="profile_evidence",
        profile_stage="profile",
    )


async def _assert_attested_a_evidence(database, schema):
    evidence_row = await database.first(
        f"SELECT value_json FROM {schema}.profile_evidence "
        "WHERE source_id = 'profile-source-a' "
        "AND fact_type = 'affiliation' "
        "AND resource_id = 'aff-positive';"
    )
    assert evidence_row is not None
    assert _decoded(evidence_row.value_json)["network_refs"] == [
        "Organization/network-1"
    ]


def _attested_a_source_context_digest():
    return importer._source_context_digest(
        [
            {
                "source_id": "profile-source-a",
                "endpoint_id": "profile-endpoint-1",
                "canonical_api_base": "https://payer-1.test/fhir",
                "org_name": "Example Health Plan",
                "plan_name": "Example Plan 1",
            }
        ]
    )


async def _set_profile_source_labels(database, schema, org_name, plan_name):
    await database.status(
        f"UPDATE {schema}.provider_directory_source "
        "SET org_name = :org_name, plan_name = :plan_name "
        "WHERE source_id = 'profile-source-a';",
        org_name=org_name,
        plan_name=plan_name,
    )


@pytest.mark.asyncio
async def test_attested_a_resume_ignores_live_b_overwrite_and_disjoint_work(
    monkeypatch,
):
    """Resume from A staging after mutable typed rows already contain B."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        await _archive_profile_dataset_a(database, schema)
        relation_overrides, created_tables = await _materialize_attested_a_scope(
            database,
            schema,
            monkeypatch,
        )
        try:
            await _overwrite_live_b_and_continue_disjoint(database, schema)
            with importer._provider_directory_artifact_relation_scope(
                relation_overrides
            ):
                await importer._populate_provider_directory_profile_evidence_stage(
                    _attested_a_profile_build(schema),
                    has_evidence_target=False,
                )
        finally:
            await importer._drop_artifact_scope_tables(schema, created_tables)
        await _assert_attested_a_evidence(database, schema)


@pytest.mark.asyncio
async def test_source_context_aba_is_rejected_before_profile_evidence(
    monkeypatch,
):
    """Reject B labels staged between an A proof and restored A live rows."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        monkeypatch.setattr(
            profile,
            "configured_profile_source_ids",
            lambda: ("profile-source-a",),
        )
        await _archive_profile_dataset_a(database, schema)
        await _set_profile_source_labels(database, schema, "Candidate B", "B")
        overrides, created_tables = await _materialize_attested_a_scope(
            database,
            schema,
            monkeypatch,
        )
        await _set_profile_source_labels(
            database,
            schema,
            "Example Health Plan",
            "Example Plan 1",
        )
        execution = SimpleNamespace(
            attestation=SimpleNamespace(
                source_context_digest=_attested_a_source_context_digest()
            )
        )
        execution_token = (
            importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
                execution
            )
        )
        try:
            with importer._provider_directory_artifact_relation_scope(overrides):
                with pytest.raises(
                    importer.ProviderDirectoryArtifactBuildStale,
                    match="source_context_attestation_changed",
                ):
                    await importer._provider_directory_profile_scope_source_ids(
                        schema,
                        {"profile-source-a"},
                    )
        finally:
            importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(
                execution_token
            )
            await importer._drop_artifact_scope_tables(schema, created_tables)
        assert await database.scalar(
            f"SELECT count(*) FROM {schema}.profile_evidence;"
        ) == 0


async def _populate_bounded_profile_evidence(
    database: Database,
    evidence_sql_refs_by_name: dict[str, str],
) -> None:
    """Populate bounded evidence one source, fact, and role bucket at a time."""
    for source_id, dataset_id in (
        ("profile-source-a", "profile-dataset-a"),
        ("profile-source-b", "profile-dataset-b"),
        ("profile-source-uhc", "profile-dataset-uhc"),
    ):
        for fact_type in profile.PROFILE_EVIDENCE_FACT_TYPES:
            role_bucket_count = (
                2
                if fact_type
                in {"affiliation", "organization", "plan_membership"}
                else 1
            )
            for role_bucket in range(role_bucket_count):
                params_by_name = {
                    "source_ids": [source_id],
                    "dataset_ids": [dataset_id],
                    "profile_as_of": "2026-07-19",
                }
                if role_bucket_count > 1:
                    params_by_name.update(
                        {
                            "profile_role_bucket_count": role_bucket_count,
                            "profile_role_bucket": role_bucket,
                        }
                    )
                await database.status(
                    profile.profile_evidence_insert_sql(
                        **evidence_sql_refs_by_name,
                        fact_type=fact_type,
                        role_bucket_count=role_bucket_count,
                        role_bucket=role_bucket,
                    ),
                    **params_by_name,
                )


async def _populate_bounded_profiles(
    database: Database,
    evidence_ref: str,
    profile_ref: str,
) -> None:
    """Populate the compact profile table through deterministic NPI ranges."""
    for npi_start in (1_000_000_000, 2_000_000_000):
        await database.status(
            profile.profile_insert_sql(
                evidence_ref=evidence_ref,
                target_ref=profile_ref,
                old_evidence_ref=None,
                rebuild_all=True,
                npi_start=npi_start,
                npi_end=npi_start + 1_000_000_000,
            ),
            generation_id="profile-affiliation-test",
            profile_as_of="2026-07-19",
            profile_npi_start=npi_start,
            profile_npi_end=npi_start + 1_000_000_000,
        )


async def _build_bounded_profile_artifacts(
    database: Database,
    schema: str,
) -> tuple[str, str]:
    """Build the same fixture through production-style bounded statements."""
    table_ref = lambda table_name: profile.qualified_table(schema, table_name)
    evidence_table = "profile_evidence_bounded"
    profile_table = "profile_bounded"
    evidence_ref = table_ref(evidence_table)
    profile_ref = table_ref(profile_table)
    await database.status(
        profile.profile_evidence_table_sql(
            schema,
            evidence_table,
            logged=True,
        )
    )
    await database.status(
        profile.profile_table_sql(schema, profile_table, logged=True)
    )
    await _populate_bounded_profile_evidence(
        database,
        {
            "target_ref": evidence_ref,
            "source_ref": table_ref("provider_directory_source"),
            "practitioner_ref": table_ref("provider_directory_practitioner"),
            "role_ref": table_ref("provider_directory_practitioner_role"),
            "organization_ref": table_ref("provider_directory_organization"),
            "service_ref": table_ref("provider_directory_healthcare_service"),
            "endpoint_ref": table_ref("provider_directory_endpoint"),
        },
    )
    await _populate_bounded_profiles(database, evidence_ref, profile_ref)
    return evidence_ref, profile_ref


def _assert_evidence_rows(evidence_rows: list[Any]) -> None:
    """Require only positive current-dataset affiliation witnesses."""
    assert [evidence_row.resource_id for evidence_row in evidence_rows] == [
        "aff-positive",
        "aff-positive",
    ]
    assert [evidence_row.dataset_id for evidence_row in evidence_rows] == [
        "profile-dataset-a",
        "profile-dataset-b",
    ]
    assert {evidence_row.resource_type for evidence_row in evidence_rows} == {
        "OrganizationAffiliation"
    }
    assert {
        evidence_row.role_resource_id for evidence_row in evidence_rows
    } == {"role-a", "role-b"}


def _assert_affiliation_value(affiliation_value: dict[str, Any]) -> None:
    """Require the safe typed context exposed by one affiliation fact."""
    assert affiliation_value["primary_organization"] == {
        "resource_id": "parent",
        "name": "Example Health Plan",
        "active": True,
        "type_codes": [],
    }
    assert affiliation_value["participating_organization"] == {
        "resource_id": "clinic",
        "name": "Example Medical Group",
        "active": True,
        "type_codes": [],
    }
    assert affiliation_value["network_refs"] == ["Organization/network-1"]
    assert affiliation_value["healthcare_service_refs"] == [
        "HealthcareService/primary-care"
    ]
    assert affiliation_value["location_refs"] == ["Location/main-clinic"]
    assert affiliation_value["specialty_codes"][0]["code"] == "207Q00000X"
    assert affiliation_value["telecom"][0]["value"] == "312-555-0100"
    assert affiliation_value["period_start"] == "2026-01-01"
    assert affiliation_value["period_end"] == "2026-12-31"
    assert affiliation_value["active"] is True


def _assert_deduplicated_profiles(profile_row: Any) -> None:
    """Require one fact, two witnesses, and no false network context."""
    compact_profile = _decoded(profile_row.profile_json)
    evidence_profile = _decoded(profile_row.evidence_json)
    compact_affiliations = compact_profile["facts"]["affiliation"]
    evidence_affiliations = evidence_profile["facts"]["affiliation"]
    assert compact_affiliations["total"] == 1
    assert len(compact_affiliations["items"]) == 1
    assert compact_affiliations["items"][0]["source_count"] == 2
    assert evidence_affiliations["items"][0]["evidence_count"] == 2
    assert {
        witness["source_id"]
        for witness in evidence_affiliations["items"][0]["evidence"]
    } == {"profile-source-a", "profile-source-b"}
    _assert_affiliation_value(compact_affiliations["items"][0]["value"])

    serialized_profiles = json.dumps(
        [compact_profile, evidence_profile],
        sort_keys=True,
    )
    assert "network-primary-only" not in serialized_profiles
    assert "network-false-primary-match" not in serialized_profiles
    assert "network-stale" not in serialized_profiles


@pytest.mark.asyncio
async def test_affiliation_profile_requires_participating_org_and_deduplicates_sources(
    monkeypatch,
):
    """Prove normalized participation, current lineage, and source dedup."""
    async with _profile_database(monkeypatch) as (database, schema):
        await _build_profile_artifacts(database, schema)
        evidence_ref = profile.qualified_table(schema, "profile_evidence")
        profile_ref = profile.qualified_table(schema, "profile")
        evidence_rows = await database.all(
            f"""
            SELECT source_id, dataset_id, resource_type, resource_id,
                   role_resource_id, value_json
              FROM {evidence_ref}
             WHERE fact_type = 'affiliation'
             ORDER BY source_id;
            """
        )
        profile_row = await database.first(
            f"SELECT profile_json, evidence_json FROM {profile_ref} "
            "WHERE npi = 1588616783;"
        )

    assert profile_row is not None
    _assert_evidence_rows(evidence_rows)
    _assert_deduplicated_profiles(profile_row)


@pytest.mark.asyncio
async def test_uhc_facility_profile_preserves_membership_without_ownership(
    monkeypatch,
):
    """Semantic UHC input reaches Profile and the public HTTP contract."""
    async with _profile_database(monkeypatch) as (database, schema):
        await _build_profile_artifacts(database, schema)
        evidence_ref = profile.qualified_table(schema, "profile_evidence")
        profile_ref = profile.qualified_table(schema, "profile")
        evidence_rows = await database.all(
            f"""
            SELECT fact_type, dataset_id, role_resource_id, value_json
              FROM {evidence_ref}
             WHERE npi = 1000000491
             ORDER BY fact_type;
            """
        )
        profile_row = await database.first(
            f"SELECT profile_json, evidence_json FROM {profile_ref} "
            "WHERE npi = 1000000491;"
        )

    _assert_uhc_facility_profile_rows(evidence_rows, profile_row)
    await _assert_uhc_facility_profile_endpoint(monkeypatch, profile_row)


@pytest.mark.asyncio
async def test_direct_organization_requires_current_uhc_nonownership_witness(
    monkeypatch,
):
    """Reject generic, stale-only, and ownership-looking direct facilities."""
    async with _profile_database(monkeypatch) as (database, schema):
        await _build_profile_artifacts(database, schema)
        evidence_ref = profile.qualified_table(schema, "profile_evidence")
        rejected_rows = await database.all(
            f"""
            SELECT npi, fact_type, resource_id
              FROM {evidence_ref}
             WHERE npi = ANY(
                       CAST(
                           ARRAY[
                               1003821380,
                               1234567893,
                               1000000004,
                               1000000012,
                               1000000020
                           ] AS bigint[]
                       )
                   )
               AND fact_type IN ('organization', 'plan_membership')
             ORDER BY npi, fact_type, resource_id;
            """
        )

    assert rejected_rows == []


def test_uhc_profile_fixture_npis_are_valid():
    """Keep acceptance fixtures inside the production NPI checksum gate."""
    for npi in (
        1000000491,
        1003821380,
        1234567893,
        1000000004,
        1000000012,
        1000000020,
    ):
        assert profile.is_valid_npi(npi)


def _assert_uhc_facility_profile_rows(
    evidence_rows: list[Any],
    profile_row: Any,
) -> None:
    """Require exact UHC dataset, organization, plan, and file lineage."""
    assert profile_row is not None
    assert [
        evidence_row.fact_type for evidence_row in evidence_rows
    ] == [
        "organization",
        "plan_membership",
    ]
    assert {
        evidence_row.dataset_id for evidence_row in evidence_rows
    } == {
        "profile-dataset-uhc"
    }
    assert all(
        evidence_row.role_resource_id is None
        for evidence_row in evidence_rows
    )
    organization = _decoded(evidence_rows[0].value_json)
    membership = _decoded(evidence_rows[1].value_json)
    assert organization["npi"] == 1000000491
    assert organization["name"] == "Example UHC Facility"
    assert organization["type_codes"] == ["Clinic"]
    assert organization["address_status"] == "payer_directory_candidate"
    assert organization["candidate_addresses"][0]["city"] == "Chicago"
    assert organization["tax_id"] is None
    assert (
        organization["tin_status"]
        == "unavailable_from_uhc_source"
    )
    assert organization["source_lineage"]["record_ordinal"] == 17
    assert len(membership["insurance_plan_refs"]) == 1
    assert membership["insurance_plan_refs"][0].startswith(
        "InsurancePlan/uhcplan-"
    )
    assert (
        membership["relationship_type"]
        == "payer_reported_provider_plan_membership"
    )
    assert membership["ownership_status"] == "not_asserted"
    assert membership["plan_scope"]["plan_id"] == "12345IL0010001"
    assert membership["source_lineage"] == organization["source_lineage"]
    compact_profile = _decoded(profile_row.profile_json)
    assert compact_profile["sources"] == [
        {
            "source_id": "profile-source-uhc",
            "endpoint_id": "profile-endpoint-uhc",
            "dataset_id": "profile-dataset-uhc",
            "api_base": "https://providermrf.uhc.com",
            "org_name": "UnitedHealthcare",
            "plan_name": "Official provider files",
        }
    ]


async def _assert_uhc_facility_profile_endpoint(
    monkeypatch,
    profile_row: Any,
) -> None:
    """Serve the SQL-built UHC Profile through the real HTTP handler."""
    compact_profile = _decoded(profile_row.profile_json)
    evidence_profile = _decoded(profile_row.evidence_json)
    monkeypatch.setattr(
        npi_endpoint,
        "fetch_state_profile_projection",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        npi_endpoint,
        "_fetch_provider_directory_profile_map",
        AsyncMock(
            return_value={
                1000000491: {
                    "profile": compact_profile,
                    "evidence": evidence_profile,
                }
            }
        ),
    )

    operation_result = await npi_endpoint.get_provider_profile(
        SimpleNamespace(args={"include_evidence": "true"}),
        "1000000491",
    )
    payload_by_field = json.loads(operation_result.body)

    assert operation_result.status == 200
    public_profile = payload_by_field["provider_profile"]
    organization_value = public_profile["categories"]["organizations"][
        "items"
    ][0]["value"]
    membership_value = public_profile["categories"][
        "network_participation"
    ]["items"][0]["value"]
    assert organization_value["tax_id"] is None
    assert (
        organization_value["tin_status"]
        == "unavailable_from_uhc_source"
    )
    assert organization_value["address_status"] == "payer_directory_candidate"
    assert (
        membership_value["relationship_type"]
        == "payer_reported_provider_plan_membership"
    )
    assert membership_value["ownership_status"] == "not_asserted"
    assert public_profile["sources"][0]["dataset_id"] == "profile-dataset-uhc"
    assert "provider_profile_evidence" in payload_by_field


@pytest.mark.asyncio
async def test_bounded_profile_build_matches_monolithic_sql_exactly(monkeypatch):
    """Prove source/fact and NPI batches preserve the existing contract."""
    async with _profile_database(monkeypatch) as (database, schema):
        await _build_profile_artifacts(database, schema)
        bounded_evidence_ref, bounded_profile_ref = (
            await _build_bounded_profile_artifacts(database, schema)
        )
        baseline_evidence_ref = profile.qualified_table(
            schema,
            "profile_evidence",
        )
        baseline_profile_ref = profile.qualified_table(schema, "profile")
        evidence_difference = await database.scalar(
            f"""
            SELECT count(*)
              FROM (
                    (SELECT * FROM {baseline_evidence_ref}
                     EXCEPT ALL
                     SELECT * FROM {bounded_evidence_ref})
                    UNION ALL
                    (SELECT * FROM {bounded_evidence_ref}
                     EXCEPT ALL
                     SELECT * FROM {baseline_evidence_ref})
              ) AS difference;
            """
        )
        profile_difference = await database.scalar(
            f"""
            SELECT count(*)
              FROM (
                    (SELECT npi, profile_json, evidence_json, source_ids,
                            endpoint_ids, dataset_ids, source_count,
                            independent_source_count, fact_count, generation_id
                       FROM {baseline_profile_ref}
                     EXCEPT ALL
                     SELECT npi, profile_json, evidence_json, source_ids,
                            endpoint_ids, dataset_ids, source_count,
                            independent_source_count, fact_count, generation_id
                       FROM {bounded_profile_ref})
                    UNION ALL
                    (SELECT npi, profile_json, evidence_json, source_ids,
                            endpoint_ids, dataset_ids, source_count,
                            independent_source_count, fact_count, generation_id
                       FROM {bounded_profile_ref}
                     EXCEPT ALL
                     SELECT npi, profile_json, evidence_json, source_ids,
                            endpoint_ids, dataset_ids, source_count,
                            independent_source_count, fact_count, generation_id
                       FROM {baseline_profile_ref})
              ) AS difference;
            """
        )

    assert evidence_difference == 0
    assert profile_difference == 0


@pytest.mark.asyncio
async def test_existing_global_refresh_replaces_without_copy_batches(
    monkeypatch,
):
    """Rebuild selected-equals-retained artifacts without stale materialization."""
    async with _profile_database(monkeypatch) as (database, schema):
        await _build_profile_artifacts(database, schema)
        await _install_fixture_as_serving_profile(database, schema)
        removed_npi = await _seed_old_only_profile(database, schema)
        baseline = await _global_refresh_baseline(database, schema)
        await _mutate_global_profile_fixture(database, schema)
        build, batch_plan = _existing_global_profile_build(schema)
        assert len(batch_plan.evidence_batches) == 230
        assert len(batch_plan.compact_batches) == 400
        assert {batch.kind for batch in batch_plan.evidence_batches} == {
            "fact"
        }
        assert {batch.kind for batch in batch_plan.compact_batches} == {
            "npi"
        }
        copy_statements = _global_refresh_copy_statements(build)
        executed_copy_statements: list[str] = []
        original_status = database.status

        async def recording_status(sql: Any, **params: Any):
            if str(sql) in copy_statements:
                executed_copy_statements.append(str(sql))
            return await original_status(sql, **params)

        monkeypatch.setattr(database, "status", recording_status)
        monkeypatch.setattr(importer, "db", database)
        fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)
        metrics, _stages = (
            await importer._build_provider_directory_profile_stages(
                build,
                fence,
                fence,
                has_existing_artifacts=True,
            )
        )

        assert metrics["incremental"] is True
        assert executed_copy_statements == []
        await _assert_existing_global_refresh(
            database,
            build,
            baseline,
            removed_npi,
        )


async def _create_empty_profile_targets(
    database: Database,
    schema: str,
) -> None:
    """Create an empty serving pair for the bounded first-build contract."""
    await database.status(
        profile.profile_evidence_table_sql(
            schema,
            profile.PROFILE_EVIDENCE_TABLE,
            logged=True,
        )
    )
    await database.status(
        profile.profile_table_sql(
            schema,
            profile.PROFILE_TABLE,
            logged=True,
        )
    )


def _empty_target_profile_build(
    schema: str,
) -> importer._ProviderDirectoryProfileBuild:
    """Return the deterministic build used against an empty serving pair."""
    return importer._ProviderDirectoryProfileBuild(
        schema=schema,
        generation_id="profile-affiliation-test",
        source_ids=("profile-source-a", "profile-source-b"),
        retained_source_ids=("profile-source-a", "profile-source-b"),
        dataset_ids=("profile-dataset-a", "profile-dataset-b"),
        profile_as_of="2026-07-19",
        evidence_stage="profile_evidence_empty_target_stage",
        profile_stage="profile_empty_target_stage",
        build_id="profile-empty-target-build",
        owner_run_id="profile-empty-target-run",
    )


async def _profile_artifact_difference_counts(
    database: Database,
    schema: str,
    build: importer._ProviderDirectoryProfileBuild,
) -> tuple[int, int]:
    """Compare bounded artifacts with the monolithic fixture outputs."""
    evidence_ref = profile.qualified_table(schema, "profile_evidence")
    bounded_evidence_ref = profile.qualified_table(schema, build.evidence_stage)
    profile_ref = profile.qualified_table(schema, "profile")
    bounded_profile_ref = profile.qualified_table(schema, build.profile_stage)
    evidence_difference = await database.scalar(
        f"""
        SELECT count(*) FROM (
            (SELECT * FROM {evidence_ref}
             EXCEPT ALL SELECT * FROM {bounded_evidence_ref})
            UNION ALL
            (SELECT * FROM {bounded_evidence_ref}
             EXCEPT ALL SELECT * FROM {evidence_ref})
        ) AS difference;
        """
    )
    profile_columns = (
        "npi, profile_json, evidence_json, source_ids, endpoint_ids, "
        "dataset_ids, source_count, independent_source_count, fact_count, "
        "generation_id"
    )
    profile_difference = await database.scalar(
        f"""
        SELECT count(*) FROM (
            (SELECT {profile_columns} FROM {profile_ref}
             EXCEPT ALL SELECT {profile_columns} FROM {bounded_profile_ref})
            UNION ALL
            (SELECT {profile_columns} FROM {bounded_profile_ref}
             EXCEPT ALL SELECT {profile_columns} FROM {profile_ref})
        ) AS difference;
        """
    )
    return int(evidence_difference or 0), int(profile_difference or 0)


@pytest.mark.asyncio
async def test_bounded_build_populates_an_initial_empty_serving_pair(
    monkeypatch,
):
    """Treat an empty serving pair as a fresh, complete global rebuild."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        await _create_empty_profile_targets(database, schema)
        build = _empty_target_profile_build(schema)
        fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)
        metrics, _stages = (
            await importer._build_provider_directory_profile_stages(
                build,
                fence,
                fence,
            )
        )

        checkpoint_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_build_checkpoint",
        )
        checkpoint_record = await database.first(
            f"SELECT has_existing_artifacts, state FROM {checkpoint_ref} "
            "WHERE build_id = :build_id;",
            build_id=build.build_id,
        )
        assert checkpoint_record is not None
        assert checkpoint_record.has_existing_artifacts is False
        assert checkpoint_record.state == "ready"
        assert metrics["incremental"] is False
        await _build_profile_artifacts(
            database,
            schema,
            source_ids=build.source_ids,
            dataset_ids=build.dataset_ids,
        )
        evidence_difference, profile_difference = (
            await _profile_artifact_difference_counts(
                database,
                schema,
                build,
            )
        )

    assert evidence_difference == 0
    assert profile_difference == 0


@pytest.mark.asyncio
async def test_bounded_fact_plan_prunes_unrelated_resource_branches(monkeypatch):
    """Keep each fact statement limited to the tables that can produce it."""
    async with _profile_database(monkeypatch) as (database, schema):
        table_ref = lambda table_name: profile.qualified_table(
            schema,
            table_name,
        )
        plan = await database.scalar(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
            + profile.profile_evidence_insert_sql(
                target_ref=table_ref("profile_evidence"),
                source_ref=table_ref("provider_directory_source"),
                practitioner_ref=table_ref(
                    "provider_directory_practitioner"
                ),
                role_ref=table_ref("provider_directory_practitioner_role"),
                organization_ref=table_ref(
                    "provider_directory_organization"
                ),
                service_ref=table_ref(
                    "provider_directory_healthcare_service"
                ),
                endpoint_ref=table_ref("provider_directory_endpoint"),
                fact_type="name",
            ),
            source_ids=["profile-source-a"],
            dataset_ids=["profile-dataset-a"],
            profile_as_of="2026-07-19",
        )

    practitioner_nodes = _plan_relation_nodes(
        plan,
        "provider_directory_practitioner",
    )
    assert practitioner_nodes
    assert any(node["Actual Loops"] > 0 for node in practitioner_nodes)
    for unrelated_relation in (
        "provider_directory_practitioner_role",
        "provider_directory_organization_affiliation",
        "provider_directory_healthcare_service",
        "provider_directory_endpoint",
    ):
        unused_nodes = _plan_relation_nodes(plan, unrelated_relation)
        assert unused_nodes
        assert all(node["Actual Loops"] == 0 for node in unused_nodes)
        assert all(node["Shared Hit Blocks"] == 0 for node in unused_nodes)
        assert all(node["Shared Read Blocks"] == 0 for node in unused_nodes)


def _profile_bucket_probe_evidence_sql(
    schema: str,
    role_scope: str,
    affiliation_scope: str,
    *,
    fact_type: str = "affiliation",
) -> str:
    """Return one exact bounded resource-bucket SQL statement."""
    table_ref = lambda table_name: profile.qualified_table(schema, table_name)
    return profile.profile_evidence_insert_sql(
        target_ref=table_ref("profile_evidence"),
        source_ref=table_ref("provider_directory_source"),
        practitioner_ref=table_ref("provider_directory_practitioner"),
        role_ref=table_ref(role_scope),
        organization_ref=table_ref("provider_directory_organization"),
        affiliation_ref=table_ref(affiliation_scope),
        affiliation_organization_ref=table_ref(
            "provider_directory_dataset_affiliation_organization"
        ),
        service_ref=table_ref("provider_directory_healthcare_service"),
        endpoint_ref=table_ref("provider_directory_endpoint"),
        fact_type=fact_type,
        role_bucket_count=profile.PROFILE_AFFILIATION_ROLE_BUCKETS,
        role_bucket=7,
    )


async def _capture_profile_bucket_probe(
    database: Database,
    schema: str,
    role_scope: str,
    affiliation_scope: str,
    *,
    fact_type: str = "affiliation",
) -> SimpleNamespace:
    """Capture before/after plans plus exact scoped-index metrics."""
    table_ref = lambda table_name: profile.qualified_table(schema, table_name)
    explain_sql = (
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
        + _profile_bucket_probe_evidence_sql(
            schema,
            role_scope,
            affiliation_scope,
            fact_type=fact_type,
        )
    )
    explain_params_by_name = {
        "source_ids": ["profile-source-a"],
        "dataset_ids": ["profile-dataset-a"],
        "profile_as_of": "2026-07-19",
        "profile_role_bucket_count": profile.PROFILE_AFFILIATION_ROLE_BUCKETS,
        "profile_role_bucket": 7,
    }
    before_plan = await database.scalar(explain_sql, **explain_params_by_name)
    await database.status(f"TRUNCATE TABLE {table_ref('profile_evidence')};")
    role_metrics = await importer._prepare_provider_directory_profile_bucket_index(
        schema,
        importer.ProviderDirectoryPractitionerRole.__tablename__,
    )
    affiliation_metrics = (
        await importer._prepare_provider_directory_profile_bucket_index(
            schema,
            importer.ProviderDirectoryOrganizationAffiliation.__tablename__,
        )
    )
    after_plan = await database.scalar(explain_sql, **explain_params_by_name)
    affiliation_plan = await database.scalar(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
        f"SELECT resource_id FROM {table_ref(affiliation_scope)} "
        "WHERE source_id = :source_id AND "
        f"{importer._provider_directory_profile_bucket_expression()} "
        "= CAST(:profile_role_bucket AS bigint);",
        source_id="profile-source-a",
        profile_role_bucket=7,
    )
    return SimpleNamespace(
        role_scope=role_scope,
        affiliation_scope=affiliation_scope,
        fact_type=fact_type,
        before_plan=before_plan,
        after_plan=after_plan,
        affiliation_plan=affiliation_plan,
        role_metrics=role_metrics,
        affiliation_metrics=affiliation_metrics,
    )


def _assert_profile_bucket_probe(probe: SimpleNamespace) -> None:
    """Assert index use, bounded work, and spill-free execution."""
    assert probe.role_metrics is not None
    assert probe.affiliation_metrics is not None
    role_index_name = str(probe.role_metrics["index_name"])
    affiliation_index_name = str(probe.affiliation_metrics["index_name"])
    assert not _plan_index_nodes(probe.before_plan, role_index_name)
    role_index_nodes = _plan_index_nodes(probe.after_plan, role_index_name)
    affiliation_index_nodes = _plan_index_nodes(
        probe.affiliation_plan,
        affiliation_index_name,
    )
    assert role_index_nodes
    assert affiliation_index_nodes
    assert all(
        node["Node Type"] in {"Index Scan", "Bitmap Index Scan"}
        for node in [*role_index_nodes, *affiliation_index_nodes]
    )
    before_role_nodes = _plan_relation_nodes(
        probe.before_plan,
        probe.role_scope,
    )
    after_role_nodes = _plan_relation_nodes(probe.after_plan, probe.role_scope)
    before_rows_inspected = max(
        int(node.get("Actual Rows", 0))
        + int(node.get("Rows Removed by Filter", 0))
        for node in before_role_nodes
    )
    after_rows_inspected = max(
        int(node.get("Actual Rows", 0))
        + int(node.get("Rows Removed by Filter", 0))
        for node in after_role_nodes
    )
    assert before_rows_inspected >= 190_000
    assert 0 < after_rows_inspected < before_rows_inspected // 8
    assert _plan_temp_blocks(probe.after_plan) == 0
    assert _plan_temp_blocks(probe.affiliation_plan) == 0
    for metrics_by_name in (
        probe.role_metrics,
        probe.affiliation_metrics,
    ):
        assert int(metrics_by_name["index_bytes"]) > 0
        assert float(metrics_by_name["elapsed_seconds"]) >= 0
        assert metrics_by_name["temp_bytes_delta"] is None or (
            int(metrics_by_name["temp_bytes_delta"]) >= 0
        )


@pytest.mark.asyncio
async def test_role_bucket_plan_uses_scoped_expression_indexes_without_spill(
    monkeypatch,
):
    """Prove exact 32-way SQL avoids serial scoped-relation rescans."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        role_scope, affiliation_scope = (
            await _create_profile_bucket_probe_scopes(
                database,
                schema,
                row_count=200_000,
            )
        )
        relation_scope_by_name = {
            importer.ProviderDirectoryPractitionerRole.__tablename__: (
                role_scope
            ),
            importer.ProviderDirectoryOrganizationAffiliation.__tablename__: (
                affiliation_scope
            ),
        }
        with importer._provider_directory_artifact_relation_scope(
            relation_scope_by_name
        ):
            probe = await _capture_profile_bucket_probe(
                database,
                schema,
                role_scope,
                affiliation_scope,
            )
        _assert_profile_bucket_probe(probe)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fact_type", "requires_role_index"),
    (
        ("plan_membership", False),
        ("organization", True),
    ),
)
async def test_affiliation_resource_bucket_plan_uses_index_without_spill(
    monkeypatch,
    fact_type,
    requires_role_index,
):
    """Prove UHC membership-backed branches avoid full affiliation scans."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        role_scope, affiliation_scope = (
            await _create_profile_bucket_probe_scopes(
                database,
                schema,
                row_count=200_000,
                include_affiliation_edges=True,
            )
        )
        relation_scope_by_name = {
            importer.ProviderDirectoryPractitionerRole.__tablename__: (
                role_scope
            ),
            importer.ProviderDirectoryOrganizationAffiliation.__tablename__: (
                affiliation_scope
            ),
        }
        with importer._provider_directory_artifact_relation_scope(
            relation_scope_by_name
        ):
            probe = await _capture_profile_bucket_probe(
                database,
                schema,
                role_scope,
                affiliation_scope,
                fact_type=fact_type,
            )

        affiliation_index_name = str(
            probe.affiliation_metrics["index_name"]
        )
        affiliation_index_nodes = _plan_index_nodes(
            probe.after_plan,
            affiliation_index_name,
        )
        assert affiliation_index_nodes, _decoded(probe.after_plan)
        assert all(
            node["Node Type"] in {"Index Scan", "Bitmap Index Scan"}
            for node in affiliation_index_nodes
        )
        role_index_nodes = _plan_index_nodes(
            probe.after_plan,
            str(probe.role_metrics["index_name"]),
        )
        role_index_executed = any(
            int(node.get("Actual Loops", 0)) > 0
            for node in role_index_nodes
        )
        assert role_index_executed is requires_role_index
        assert _plan_temp_blocks(probe.after_plan) == 0


async def _seed_profile_evidence_plan(
    database: Database,
    schema: str,
) -> tuple[str, str, str]:
    """Create and index a broad evidence fixture for a late NPI range."""
    evidence_table = "profile_evidence_plan"
    profile_table = "profile_plan"
    evidence_ref = profile.qualified_table(schema, evidence_table)
    profile_ref = profile.qualified_table(schema, profile_table)
    await database.status(
        profile.profile_evidence_table_sql(schema, evidence_table, logged=True)
    )
    await database.status(
        profile.profile_table_sql(schema, profile_table, logged=True)
    )
    await database.status(
        f"""
        INSERT INTO {evidence_ref} (
            evidence_key, npi, fact_type, fact_key, value_json,
            source_id, endpoint_id, dataset_id, canonical_api_base,
            source_org_name, source_plan_name, resource_type,
            resource_id, role_resource_id, active, effective_start,
            effective_end, observed_at
        )
        SELECT md5(value::text), 1000000000 + value * 10000,
               'name', md5(('fact-' || value)::text),
               jsonb_build_object('text', 'Provider ' || value),
               'profile-source-a', 'profile-endpoint-a',
               'profile-dataset-a', 'https://payer.test/fhir',
               'Example Health Plan', 'Example Plan', 'Practitioner',
               'practitioner-' || value, NULL, true, NULL, NULL, now()
          FROM generate_series(1, 100000) AS value;
        """
    )
    for index_sql in profile.profile_index_statements(
        schema,
        evidence_table,
        evidence=True,
    ):
        await database.status(index_sql)
    await database.status(f"ANALYZE {evidence_ref};")
    return evidence_table, evidence_ref, profile_ref


async def _explain_late_profile_npi_range(
    database: Database,
    evidence_ref: str,
    profile_ref: str,
) -> Any:
    """Explain one late bounded NPI insertion with broad joins disabled."""
    npi_start = profile.NPI_MIN + 995_000_000
    npi_end = npi_start + profile.PROFILE_NPI_BATCH_SIZE
    async with database.transaction():
        await database.status("SET LOCAL enable_nestloop = off;")
        await database.status("SET LOCAL enable_hashjoin = off;")
        return await database.scalar(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
            + profile.profile_insert_sql(
                evidence_ref=evidence_ref,
                target_ref=profile_ref,
                old_evidence_ref=None,
                rebuild_all=True,
                npi_start=npi_start,
                npi_end=npi_end,
            ),
            generation_id="profile-index-plan",
            profile_as_of="2026-07-19",
            profile_npi_start=npi_start,
            profile_npi_end=npi_end,
        )


@pytest.mark.asyncio
async def test_five_million_npi_batch_uses_evidence_range_indexes(monkeypatch):
    """Prevent every compact-profile range from rescanning all evidence."""
    async with _profile_database(monkeypatch) as (database, schema):
        evidence_table, evidence_ref, profile_ref = (
            await _seed_profile_evidence_plan(database, schema)
        )
        plan = await _explain_late_profile_npi_range(
            database,
            evidence_ref,
            profile_ref,
        )

    evidence_nodes = _plan_relation_nodes(plan, evidence_table)
    assert evidence_nodes
    assert all(node["Node Type"] != "Seq Scan" for node in evidence_nodes)
    assert any("Index Cond" in node for node in evidence_nodes)
    assert max(node["Actual Rows"] for node in evidence_nodes) < 2_000


@pytest.mark.asyncio
async def test_profile_build_resumes_after_committed_batch_interruption(
    monkeypatch,
):
    """Resume exact logged stages without replaying completed fact batches."""
    async with _profile_database(monkeypatch) as (database, schema):
        await _build_profile_artifacts(database, schema)
        monkeypatch.setattr(importer, "db", database)
        progress_events: list[tuple[str | None, dict[str, Any]]] = []

        async def record_progress(
            run_id: str | None,
            **progress_by_name: Any,
        ) -> None:
            progress_events.append((run_id, progress_by_name))

        monkeypatch.setattr(
            importer,
            "_mark_provider_directory_progress",
            record_progress,
        )
        build = importer._ProviderDirectoryProfileBuild(
            schema=schema,
            generation_id="profile-affiliation-test",
            source_ids=(
                "profile-source-a",
                "profile-source-b",
                "profile-source-uhc",
            ),
            retained_source_ids=(
                "profile-source-a",
                "profile-source-b",
                "profile-source-uhc",
            ),
            dataset_ids=(
                "profile-dataset-a",
                "profile-dataset-b",
                "profile-dataset-uhc",
            ),
            profile_as_of="2026-07-19",
            evidence_stage="profile_evidence_resume_stage",
            profile_stage="profile_resume_stage",
            build_id="profile-resume-build",
            owner_run_id="profile-run-first",
        )
        expected_evidence_total = len(
            importer._provider_directory_profile_batch_plan(
                build.source_ids,
                build.retained_source_ids,
                build.dataset_ids,
                has_existing_artifacts=True,
            ).evidence_batches
        )
        assert expected_evidence_total == 345
        fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)
        original_status = database.status
        fact_statement_starts: list[None] = []

        async def interrupting_status(sql: Any, **params: Any):
            if (
                f'INSERT INTO "{schema}"."{build.evidence_stage}"' in str(sql)
                and "ON CONFLICT (evidence_key) DO NOTHING" in str(sql)
            ):
                if len(fact_statement_starts) == 1:
                    raise RuntimeError("forced resumable interruption")
                fact_statement_starts.append(None)
            return await original_status(sql, **params)

        monkeypatch.setattr(database, "status", interrupting_status)
        with pytest.raises(RuntimeError, match="forced resumable interruption"):
            await importer._build_provider_directory_profile_stages(
                build,
                fence,
                fence,
            )

        checkpoint_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_build_checkpoint",
        )
        interrupted_checkpoint = await database.first(
            f"SELECT state, evidence_next_batch, profile_next_batch "
            f"FROM {checkpoint_ref} WHERE build_id = :build_id;",
            build_id=build.build_id,
        )
        assert interrupted_checkpoint is not None
        assert interrupted_checkpoint.state == "failed"
        assert interrupted_checkpoint.evidence_next_batch == 1
        assert interrupted_checkpoint.profile_next_batch == 0
        first_run_progress = [
            progress
            for progress_run_id, progress in progress_events
            if progress_run_id == "profile-run-first"
        ]
        assert [
            (progress["done"], progress["total"])
            for progress in first_run_progress
        ] == [
            (0, expected_evidence_total),
            (1, expected_evidence_total),
        ]
        assert {
            progress["phase"] for progress in first_run_progress
        } == {importer._PROFILE_EVIDENCE_PROGRESS_PHASE}
        assert {
            progress["details"]["_progress_unit"]
            for progress in first_run_progress
        } == {"batches"}
        interrupted_evidence_count = int(
            await database.scalar(
                f"SELECT count(*) FROM "
                f"{profile.qualified_table(schema, build.evidence_stage)};"
            )
            or 0
        )

        resumed_fact_statements: list[str] = []
        compact_statement_starts: list[None] = []

        async def tracking_status(sql: Any, **params: Any):
            if (
                f'INSERT INTO "{schema}"."{build.evidence_stage}"' in str(sql)
                and "ON CONFLICT (evidence_key) DO NOTHING" in str(sql)
            ):
                resumed_fact_statements.append(str(sql))
            if (
                f'INSERT INTO "{schema}"."{build.profile_stage}"' in str(sql)
                and "ON CONFLICT (npi) DO NOTHING" in str(sql)
            ):
                if len(compact_statement_starts) == 0:
                    raise RuntimeError("forced compact interruption")
                compact_statement_starts.append(None)
            return await original_status(sql, **params)

        monkeypatch.setattr(database, "status", tracking_status)
        resumed_build = replace(
            build,
            owner_run_id="profile-run-retry",
        )
        with pytest.raises(RuntimeError, match="forced compact interruption"):
            await importer._build_provider_directory_profile_stages(
                resumed_build,
                fence,
                fence,
            )
        compact_checkpoint = await database.first(
            f"SELECT state, evidence_next_batch, evidence_total_batches, "
            f"profile_next_batch FROM {checkpoint_ref} "
            f"WHERE build_id = :build_id;",
            build_id=build.build_id,
        )
        assert compact_checkpoint is not None
        assert compact_checkpoint.state == "failed"
        assert (
            compact_checkpoint.evidence_next_batch
            == compact_checkpoint.evidence_total_batches
        )
        assert compact_checkpoint.profile_next_batch == 0
        assert len(resumed_fact_statements) == (
            compact_checkpoint.evidence_total_batches - 1
        )
        retry_evidence_progress = [
            progress
            for progress_run_id, progress in progress_events
            if progress_run_id == "profile-run-retry"
            and progress["phase"]
            == importer._PROFILE_EVIDENCE_PROGRESS_PHASE
        ]
        assert [
            progress["done"] for progress in retry_evidence_progress
        ] == list(range(1, expected_evidence_total + 1))
        retry_profile_progress = [
            progress
            for progress_run_id, progress in progress_events
            if progress_run_id == "profile-run-retry"
            and progress["phase"]
            == importer._PROFILE_COMPACT_PROGRESS_PHASE
        ]
        assert [
            (progress["done"], progress["total"])
            for progress in retry_profile_progress
        ] == [(0, 400)]

        evidence_population = (
            importer._populate_provider_directory_profile_evidence_stage
        )
        compact_population = (
            importer._populate_provider_directory_profile_compact_stage
        )
        mark_checkpoint_failed = (
            importer._mark_profile_build_checkpoint_failed
        )

        async def stage_oids() -> tuple[int, int]:
            relation_oids: list[int] = []
            for stage_table in (
                build.evidence_stage,
                build.profile_stage,
            ):
                relation_oids.append(
                    int(
                        await database.scalar(
                            "SELECT to_regclass(:relation_name)::oid;",
                            relation_name=f"{schema}.{stage_table}",
                        )
                        or 0
                    )
                )
            return relation_oids[0], relation_oids[1]

        stage_oids_before_interrupt = await stage_oids()
        evidence_reopen_attempts: list[None] = []

        async def reject_evidence_reopen(*_args: Any, **_params: Any):
            evidence_reopen_attempts.append(None)
            raise AssertionError("completed evidence phase was reopened")

        async def interrupt_before_next_compact_batch(
            *_args: Any,
            **_params: Any,
        ):
            raise RuntimeError("hard stop before next compact batch")

        async def preserve_hard_stop_state(
            *_args: Any,
            **_params: Any,
        ) -> None:
            return None

        monkeypatch.setattr(
            importer,
            "_populate_provider_directory_profile_evidence_stage",
            reject_evidence_reopen,
        )
        monkeypatch.setattr(
            importer,
            "_populate_provider_directory_profile_compact_stage",
            interrupt_before_next_compact_batch,
        )
        monkeypatch.setattr(
            importer,
            "_mark_profile_build_checkpoint_failed",
            preserve_hard_stop_state,
        )
        with pytest.raises(
            RuntimeError,
            match="hard stop before next compact batch",
        ):
            await importer._build_provider_directory_profile_stages(
                replace(
                    resumed_build,
                    owner_run_id="profile-run-boundary-stop",
                ),
                fence,
                fence,
            )
        assert evidence_reopen_attempts == []
        boundary_checkpoint = await database.first(
            f"SELECT state, evidence_next_batch, evidence_total_batches, "
            f"profile_next_batch FROM {checkpoint_ref} "
            f"WHERE build_id = :build_id;",
            build_id=build.build_id,
        )
        assert boundary_checkpoint is not None
        assert boundary_checkpoint.state == "building_profile"
        assert (
            boundary_checkpoint.evidence_next_batch
            == boundary_checkpoint.evidence_total_batches
        )
        assert boundary_checkpoint.profile_next_batch == 0
        assert await stage_oids() == stage_oids_before_interrupt
        monkeypatch.setattr(
            importer,
            "_populate_provider_directory_profile_evidence_stage",
            evidence_population,
        )
        monkeypatch.setattr(
            importer,
            "_populate_provider_directory_profile_compact_stage",
            compact_population,
        )
        monkeypatch.setattr(
            importer,
            "_mark_profile_build_checkpoint_failed",
            mark_checkpoint_failed,
        )

        last_batch_evidence_statements: list[str] = []
        last_batch_profile_statements: list[str] = []
        profile_index_interruptions: list[None] = []

        async def last_batch_status(sql: Any, **params: Any):
            if (
                f'INSERT INTO "{schema}"."{build.evidence_stage}"' in str(sql)
                and "ON CONFLICT (evidence_key) DO NOTHING" in str(sql)
            ):
                last_batch_evidence_statements.append(str(sql))
            if (
                f'INSERT INTO "{schema}"."{build.profile_stage}"' in str(sql)
                and "ON CONFLICT (npi) DO NOTHING" in str(sql)
            ):
                last_batch_profile_statements.append(str(sql))
            if (
                "CREATE INDEX IF NOT EXISTS" in str(sql)
                and f'"{build.profile_stage}_generation_idx"' in str(sql)
                and not profile_index_interruptions
            ):
                profile_index_interruptions.append(None)
                raise RuntimeError("forced profile index interruption")
            return await original_status(sql, **params)

        monkeypatch.setattr(database, "status", last_batch_status)
        with pytest.raises(
            RuntimeError,
            match="forced profile index interruption",
        ):
            await importer._build_provider_directory_profile_stages(
                replace(
                    resumed_build,
                    owner_run_id="profile-run-last-batch",
                ),
                fence,
                fence,
            )
        last_batch_checkpoint = await database.first(
            f"SELECT state, evidence_next_batch, evidence_total_batches, "
            f"profile_next_batch, profile_total_batches "
            f"FROM {checkpoint_ref} WHERE build_id = :build_id;",
            build_id=build.build_id,
        )
        assert last_batch_checkpoint is not None
        assert last_batch_evidence_statements == []
        assert len(last_batch_profile_statements) == (
            last_batch_checkpoint.profile_total_batches
        )
        assert last_batch_checkpoint.state == "failed"
        assert (
            last_batch_checkpoint.profile_next_batch
            == last_batch_checkpoint.profile_total_batches
        )
        assert await stage_oids() == stage_oids_before_interrupt

        final_evidence_statements: list[str] = []
        final_profile_statements: list[str] = []

        async def final_status(sql: Any, **params: Any):
            if (
                f'INSERT INTO "{schema}"."{build.evidence_stage}"' in str(sql)
                and "ON CONFLICT (evidence_key) DO NOTHING" in str(sql)
            ):
                final_evidence_statements.append(str(sql))
            if (
                f'INSERT INTO "{schema}"."{build.profile_stage}"' in str(sql)
                and "ON CONFLICT (npi) DO NOTHING" in str(sql)
            ):
                final_profile_statements.append(str(sql))
            return await original_status(sql, **params)

        monkeypatch.setattr(database, "status", final_status)
        _metrics, _stages = await importer._build_provider_directory_profile_stages(
            replace(resumed_build, owner_run_id="profile-run-final"),
            fence,
            fence,
        )
        completed_checkpoint = await database.first(
            f"SELECT state, evidence_next_batch, evidence_total_batches, "
            f"profile_next_batch, profile_total_batches "
            f"FROM {checkpoint_ref} WHERE build_id = :build_id;",
            build_id=build.build_id,
        )
        assert completed_checkpoint is not None
        assert final_evidence_statements == []
        assert final_profile_statements == []
        assert completed_checkpoint.state == "ready"
        assert (
            completed_checkpoint.evidence_next_batch
            == completed_checkpoint.evidence_total_batches
        )
        assert (
            completed_checkpoint.profile_next_batch
            == completed_checkpoint.profile_total_batches
        )
        last_batch_progress = [
            progress["done"]
            for progress_run_id, progress in progress_events
            if progress_run_id == "profile-run-last-batch"
            and progress["phase"]
            == importer._PROFILE_COMPACT_PROGRESS_PHASE
        ]
        assert last_batch_progress == list(range(401))
        final_progress = [
            (progress["done"], progress["total"])
            for progress_run_id, progress in progress_events
            if progress_run_id == "profile-run-final"
            and progress["phase"]
            == importer._PROFILE_COMPACT_PROGRESS_PHASE
        ]
        assert final_progress == [(400, 400)]
        assert int(
            await database.scalar(
                f"SELECT count(*) FROM "
                f"{profile.qualified_table(schema, build.evidence_stage)};"
            )
            or 0
        ) >= interrupted_evidence_count

        baseline_evidence_ref = profile.qualified_table(
            schema,
            "profile_evidence",
        )
        resumed_evidence_ref = profile.qualified_table(
            schema,
            build.evidence_stage,
        )
        baseline_profile_ref = profile.qualified_table(schema, "profile")
        resumed_profile_ref = profile.qualified_table(
            schema,
            build.profile_stage,
        )
        assert await database.scalar(
            f"""
            SELECT count(*) FROM (
                (SELECT * FROM {baseline_evidence_ref}
                 EXCEPT ALL SELECT * FROM {resumed_evidence_ref})
                UNION ALL
                (SELECT * FROM {resumed_evidence_ref}
                 EXCEPT ALL SELECT * FROM {baseline_evidence_ref})
            ) AS difference;
            """
        ) == 0
        assert await database.scalar(
            f"""
            SELECT count(*) FROM (
                (SELECT npi, profile_json, evidence_json, source_ids,
                        endpoint_ids, dataset_ids, source_count,
                        independent_source_count, fact_count, generation_id
                   FROM {baseline_profile_ref}
                 EXCEPT ALL
                 SELECT npi, profile_json, evidence_json, source_ids,
                        endpoint_ids, dataset_ids, source_count,
                        independent_source_count, fact_count, generation_id
                   FROM {resumed_profile_ref})
                UNION ALL
                (SELECT npi, profile_json, evidence_json, source_ids,
                        endpoint_ids, dataset_ids, source_count,
                        independent_source_count, fact_count, generation_id
                   FROM {resumed_profile_ref}
                 EXCEPT ALL
                 SELECT npi, profile_json, evidence_json, source_ids,
                        endpoint_ids, dataset_ids, source_count,
                        independent_source_count, fact_count, generation_id
                   FROM {baseline_profile_ref})
            ) AS difference;
            """
        ) == 0


def _reaper_profile_build(
    schema: str,
    build_id: str,
    *,
    source_id: str,
    dataset_id: str,
    owner_run_id: str,
) -> importer._ProviderDirectoryProfileBuild:
    """Return one deterministic build used by stale-stage reaper tests."""
    return importer._ProviderDirectoryProfileBuild(
        schema=schema,
        generation_id=f"generation-{build_id[-4:]}",
        source_ids=(source_id,),
        retained_source_ids=(source_id,),
        dataset_ids=(dataset_id,),
        profile_as_of="2026-07-20",
        evidence_stage=profile.profile_evidence_stage_table_name(build_id),
        profile_stage=profile.profile_stage_table_name(build_id),
        build_id=build_id,
        owner_run_id=owner_run_id,
    )


async def _seed_stale_and_current_profile_builds(
    schema: str,
) -> tuple[
    importer._ProviderDirectoryProfileBuild,
    importer._ProviderDirectoryProfileBuild,
]:
    """Claim one failed build and one current build for reaper coverage."""
    stale_build = _reaper_profile_build(
        schema,
        f"pdpb_{'1' * 32}",
        source_id="profile-source-a",
        dataset_id="profile-dataset-a",
        owner_run_id="profile-run-stale",
    )
    current_build = _reaper_profile_build(
        schema,
        f"pdpb_{'2' * 32}",
        source_id="profile-source-b",
        dataset_id="profile-dataset-b",
        owner_run_id="profile-run-current",
    )
    fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)
    await importer._claim_provider_directory_profile_build_checkpoint(
        stale_build,
        has_existing_artifacts=False,
        evidence_build_fence=fence,
        profile_build_fence=fence,
    )
    await importer._mark_profile_build_checkpoint_failed(
        stale_build,
        RuntimeError("forced stale failure"),
    )
    await importer._claim_provider_directory_profile_build_checkpoint(
        current_build,
        has_existing_artifacts=False,
        evidence_build_fence=fence,
        profile_build_fence=fence,
    )
    return stale_build, current_build


async def _assert_profile_build_relations_reaped(
    database: Database,
    schema: str,
    stale_build: importer._ProviderDirectoryProfileBuild,
    current_build: importer._ProviderDirectoryProfileBuild,
) -> None:
    """Require only the stale build's two stage relations to be removed."""
    checkpoint_ref = profile.qualified_table(
        schema,
        "provider_directory_profile_build_checkpoint",
    )
    remaining_build_ids = {
        checkpoint_record.build_id
        for checkpoint_record in await database.all(
            f"SELECT build_id FROM {checkpoint_ref};"
        )
    }
    assert remaining_build_ids == {current_build.build_id}
    for stage_table in (stale_build.evidence_stage, stale_build.profile_stage):
        assert await database.scalar(
            "SELECT to_regclass(:relation_name);",
            relation_name=f"{schema}.{stage_table}",
        ) is None
    for stage_table in (current_build.evidence_stage, current_build.profile_stage):
        assert await database.scalar(
            "SELECT to_regclass(:relation_name);",
            relation_name=f"{schema}.{stage_table}",
        ) is not None


@pytest.mark.asyncio
async def test_profile_build_reaps_failed_stages_after_lineage_changes(
    monkeypatch,
):
    """Drop only superseded logged stages when source/dataset scope changes."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        stale_build, current_build = (
            await _seed_stale_and_current_profile_builds(schema)
        )
        checkpoint_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_build_checkpoint",
        )
        with pytest.raises(
            IntegrityError,
            match="pd_profile_build_checkpoint_phase_order_check",
        ):
            await database.status(
                f"UPDATE {checkpoint_ref} SET profile_next_batch = 1 "
                "WHERE build_id = :build_id;",
                build_id=current_build.build_id,
            )
        with pytest.raises(
            IntegrityError,
            match="pd_profile_build_checkpoint_state_progress_check",
        ):
            await database.status(
                f"UPDATE {checkpoint_ref} "
                "SET evidence_next_batch = evidence_total_batches, "
                "profile_next_batch = 1, state = 'evidence_complete' "
                "WHERE build_id = :build_id;",
                build_id=current_build.build_id,
            )

        assert await importer._reap_stale_provider_directory_profile_builds(
            schema,
            current_build_id=current_build.build_id,
        ) == 1
        await _assert_profile_build_relations_reaped(
            database,
            schema,
            stale_build,
            current_build,
        )
