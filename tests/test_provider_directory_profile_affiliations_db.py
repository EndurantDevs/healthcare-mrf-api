# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import MetaData
from sqlalchemy.exc import IntegrityError, OperationalError

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
    evidence_sql_refs_by_name = {
        "target_ref": evidence_ref,
        "source_ref": table_ref("provider_directory_source"),
        "practitioner_ref": table_ref("provider_directory_practitioner"),
        "role_ref": table_ref("provider_directory_practitioner_role"),
        "organization_ref": table_ref("provider_directory_organization"),
        "service_ref": table_ref("provider_directory_healthcare_service"),
        "endpoint_ref": table_ref("provider_directory_endpoint"),
    }
    for source_id, dataset_id in (
        ("profile-source-a", "profile-dataset-a"),
        ("profile-source-b", "profile-dataset-b"),
    ):
        for fact_type in profile.PROFILE_EVIDENCE_FACT_TYPES:
            role_bucket_count = 2 if fact_type == "affiliation" else 1
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
async def test_bounded_build_populates_an_initial_empty_serving_pair(
    monkeypatch,
):
    """Treat an existing empty table pair as a valid incremental baseline."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
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
        build = importer._ProviderDirectoryProfileBuild(
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
        assert checkpoint_record.has_existing_artifacts is True
        assert checkpoint_record.state == "ready"
        assert metrics["incremental"] is True

        await _build_profile_artifacts(database, schema)
        baseline_evidence_ref = profile.qualified_table(
            schema,
            "profile_evidence",
        )
        bounded_evidence_ref = profile.qualified_table(
            schema,
            build.evidence_stage,
        )
        baseline_profile_ref = profile.qualified_table(schema, "profile")
        bounded_profile_ref = profile.qualified_table(
            schema,
            build.profile_stage,
        )
        evidence_difference = await database.scalar(
            f"""
            SELECT count(*) FROM (
                (SELECT * FROM {baseline_evidence_ref}
                 EXCEPT ALL SELECT * FROM {bounded_evidence_ref})
                UNION ALL
                (SELECT * FROM {bounded_evidence_ref}
                 EXCEPT ALL SELECT * FROM {baseline_evidence_ref})
            ) AS difference;
            """
        )
        profile_difference = await database.scalar(
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


@pytest.mark.asyncio
async def test_five_million_npi_batch_uses_evidence_range_indexes(monkeypatch):
    """Prevent every compact-profile range from rescanning all evidence."""
    async with _profile_database(monkeypatch) as (database, schema):
        evidence_table = "profile_evidence_plan"
        profile_table = "profile_plan"
        evidence_ref = profile.qualified_table(schema, evidence_table)
        profile_ref = profile.qualified_table(schema, profile_table)
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
        await database.status(
            f"""
            INSERT INTO {evidence_ref} (
                evidence_key, npi, fact_type, fact_key, value_json,
                source_id, endpoint_id, dataset_id, canonical_api_base,
                source_org_name, source_plan_name, resource_type,
                resource_id, role_resource_id, active, effective_start,
                effective_end, observed_at
            )
            SELECT md5(value::text),
                   1000000000 + value * 10000,
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
        late_npi_start = profile.NPI_MIN + 995_000_000
        async with database.transaction():
            await database.status("SET LOCAL enable_nestloop = off;")
            await database.status("SET LOCAL enable_hashjoin = off;")
            plan = await database.scalar(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                + profile.profile_insert_sql(
                    evidence_ref=evidence_ref,
                    target_ref=profile_ref,
                    old_evidence_ref=None,
                    rebuild_all=True,
                    npi_start=late_npi_start,
                    npi_end=(
                        late_npi_start + profile.PROFILE_NPI_BATCH_SIZE
                    ),
                ),
                generation_id="profile-index-plan",
                profile_as_of="2026-07-19",
                profile_npi_start=late_npi_start,
                profile_npi_end=(
                    late_npi_start + profile.PROFILE_NPI_BATCH_SIZE
                ),
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
        build = importer._ProviderDirectoryProfileBuild(
            schema=schema,
            generation_id="profile-affiliation-test",
            source_ids=("profile-source-a", "profile-source-b"),
            retained_source_ids=("profile-source-a", "profile-source-b"),
            dataset_ids=("profile-dataset-a", "profile-dataset-b"),
            profile_as_of="2026-07-19",
            evidence_stage="profile_evidence_resume_stage",
            profile_stage="profile_resume_stage",
            build_id="profile-resume-build",
            owner_run_id="profile-run-first",
        )
        fence = importer.ProviderDirectoryArtifactBuildFence(target_oid=None)
        original_status = database.status
        fact_statement_starts: list[None] = []

        async def interrupting_status(sql: Any, **params: Any):
            if (
                f'INSERT INTO "{schema}"."{build.evidence_stage}"' in str(sql)
                and "ON CONFLICT (evidence_key) DO NOTHING" in str(sql)
            ):
                if len(fact_statement_starts) == 6:
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
        assert interrupted_checkpoint.evidence_next_batch == 6
        assert interrupted_checkpoint.profile_next_batch == 0
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
                if len(compact_statement_starts) == 120:
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
        assert compact_checkpoint.profile_next_batch == 120
        assert len(resumed_fact_statements) == (
            compact_checkpoint.evidence_total_batches - 6
        )

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
        assert boundary_checkpoint.profile_next_batch == 120
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
            last_batch_checkpoint.profile_total_batches - 120
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


@pytest.mark.asyncio
async def test_profile_build_reaps_failed_stages_after_lineage_changes(
    monkeypatch,
):
    """Drop only superseded logged stages when source/dataset scope changes."""
    async with _profile_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        stale_build_id = f"pdpb_{'1' * 32}"
        current_build_id = f"pdpb_{'2' * 32}"

        def build(
            build_id: str,
            *,
            source_id: str,
            dataset_id: str,
            owner_run_id: str,
        ) -> importer._ProviderDirectoryProfileBuild:
            return importer._ProviderDirectoryProfileBuild(
                schema=schema,
                generation_id=f"generation-{build_id[-4:]}",
                source_ids=(source_id,),
                retained_source_ids=(source_id,),
                dataset_ids=(dataset_id,),
                profile_as_of="2026-07-20",
                evidence_stage=(
                    profile.profile_evidence_stage_table_name(build_id)
                ),
                profile_stage=profile.profile_stage_table_name(build_id),
                build_id=build_id,
                owner_run_id=owner_run_id,
            )

        stale_build = build(
            stale_build_id,
            source_id="profile-source-a",
            dataset_id="profile-dataset-a",
            owner_run_id="profile-run-stale",
        )
        current_build = build(
            current_build_id,
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
                build_id=current_build_id,
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
                build_id=current_build_id,
            )

        assert await importer._reap_stale_provider_directory_profile_builds(
            schema,
            current_build_id=current_build_id,
        ) == 1

        remaining_build_ids = {
            checkpoint_record.build_id
            for checkpoint_record in await database.all(
                f"SELECT build_id FROM {checkpoint_ref};"
            )
        }
        assert remaining_build_ids == {current_build_id}
        for stage_table in (
            stale_build.evidence_stage,
            stale_build.profile_stage,
        ):
            assert await database.scalar(
                "SELECT to_regclass(:relation_name);",
                relation_name=f"{schema}.{stage_table}",
            ) is None
        for stage_table in (
            current_build.evidence_stage,
            current_build.profile_stage,
        ):
            assert await database.scalar(
                "SELECT to_regclass(:relation_name);",
                relation_name=f"{schema}.{stage_table}",
            ) is not None
