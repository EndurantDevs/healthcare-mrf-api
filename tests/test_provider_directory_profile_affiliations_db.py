# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_profile as profile


FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures"
FHIR_FIXTURE_PATH = (
    FIXTURE_DIRECTORY / "provider_directory_profile_affiliations.json"
)
SQL_FIXTURE_PATH = (
    FIXTURE_DIRECTORY / "provider_directory_profile_affiliations.sql"
)
importer = importlib.import_module("process.provider_directory_fhir")


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


async def _build_profile_artifacts(
    database: Database,
    schema: str,
) -> None:
    """Execute evidence and compact-profile SQL for both current datasets."""
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
        source_ids=["profile-source-a", "profile-source-b"],
        dataset_ids=["profile-dataset-a", "profile-dataset-b"],
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
