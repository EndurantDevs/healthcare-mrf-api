# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import importlib.util
from pathlib import Path

import pytest

_FIXTURE_PATH = Path(__file__).with_name(
    "test_provider_directory_dataset_serving_relations_db.py"
)
_FIXTURE_SPEC = importlib.util.spec_from_file_location("db_fixtures", _FIXTURE_PATH)
db_fixtures = importlib.util.module_from_spec(_FIXTURE_SPEC)
assert _FIXTURE_SPEC.loader is not None
_FIXTURE_SPEC.loader.exec_module(db_fixtures)


importer = importlib.import_module("process.provider_directory_fhir")


def test_affiliation_sql_has_explicit_contra_policy_and_dataset_org_join():
    sql = importer._dataset_affiliation_organization_proof_sql(
        should_verify_acquisition_root=True,
    )

    assert "organization_ref" in sql
    assert "provider_directory_affiliation_reference_fallback" in sql
    assert importer.CONTRA_COSTA_PROVIDER_DIRECTORY_BASE in sql
    assert "resource.resource_type = 'Organization'" in sql
    assert "allow_organization_reference_fallback" in sql


def test_unresolved_fallback_is_diagnostic_and_not_complete():
    proof_by_field = {
        "target_dataset_count": 1,
        "acquisition_root_run_id": "root-run",
        "affiliation_resource_count": 1,
        "affiliation_with_participating_organization_count": 0,
        "empty_participating_organization_reference_count": 1,
        "fallback_candidate_count": 1,
        "fallback_resolved_count": 0,
        "fallback_unresolved_count": 1,
        "unresolved_reference_count": 1,
        "malformed_reference_payload_count": 0,
        "valid_reference_count": 0,
        "invalid_reference_count": 0,
        "expected_edge_count": 0,
    }
    with pytest.raises(RuntimeError, match="unresolved_references"):
        importer._validated_dataset_affiliation_organization_proof(
            proof_by_field,
            dataset_id="dataset-contra",
            build_run_id="contra-build",
            replaced_edge_count=0,
            inserted_edge_count=0,
            expected_acquisition_root_run_id="root-run",
        )


@pytest.mark.asyncio
async def test_contra_fallback_resolves_only_with_source_policy(monkeypatch):
    async with db_fixtures._dataset_database(monkeypatch) as (database, schema):
        await db_fixtures._build_baseline_dataset_a_relations(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET metadata_json = '{\""
            f"{importer.PROVIDER_DIRECTORY_AFFILIATION_REFERENCE_FALLBACK_METADATA_KEY}"
            "\":\"organization_ref\"}'::jsonb "
            "WHERE source_id = 'source-a';"
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_dataset_resource "
            "SET payload_json = '{\"organization_ref\":"
            "\"Organization/org-b\"}'::jsonb "
            "WHERE dataset_id = 'dataset-a' "
            "AND resource_type = 'OrganizationAffiliation' "
            "AND resource_id = 'affiliation-d';"
        )

        async with database.acquire() as connection:
            proof = await importer._build_provider_directory_dataset_affiliation_organization(
                connection,
                "dataset-a",
                build_run_id="contra-build",
                expected_acquisition_root_run_id="root-a",
            )

        assert proof["fallback_candidate_count"] == 1
        assert proof["fallback_resolved_count"] == 1
        assert proof["fallback_unresolved_count"] == 0
        assert proof["expected_edge_count"] == 4
        assert await database.scalar(
            f"SELECT count(*) FROM {schema}."
            "provider_directory_dataset_affiliation_organization "
            "WHERE dataset_id = 'dataset-a' "
            "AND participating_organization_resource_id = 'org-b' "
            "AND affiliation_resource_id = 'affiliation-d';"
        ) == 1


@pytest.mark.asyncio
async def test_unresolved_contra_fallback_preserves_existing_edges(monkeypatch):
    async with db_fixtures._dataset_database(monkeypatch) as (database, schema):
        original_rows = await db_fixtures._build_baseline_dataset_a_relations(
            database,
            schema,
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_source "
            "SET metadata_json = '{\""
            f"{importer.PROVIDER_DIRECTORY_AFFILIATION_REFERENCE_FALLBACK_METADATA_KEY}"
            "\":\"organization_ref\"}'::jsonb "
            "WHERE source_id = 'source-a';"
        )
        await database.status(
            f"UPDATE {schema}.provider_directory_dataset_resource "
            "SET payload_json = '{\"organization_ref\":"
            "\"Organization/org-missing\"}'::jsonb "
            "WHERE dataset_id = 'dataset-a' "
            "AND resource_type = 'OrganizationAffiliation' "
            "AND resource_id = 'affiliation-e';"
        )

        async with database.acquire() as connection:
            proof = await connection.first(
                importer._dataset_affiliation_organization_proof_sql(
                    should_verify_acquisition_root=True,
                ),
                dataset_id="dataset-a",
                acquisition_root_run_id="root-a",
            )
            assert proof["fallback_candidate_count"] == 1
            assert proof["fallback_resolved_count"] == 0
            assert proof["fallback_unresolved_count"] == 1
            assert proof["unresolved_reference_count"] == 1
            assert proof["expected_edge_count"] == 3
            with pytest.raises(RuntimeError, match="unresolved_references"):
                await importer._build_provider_directory_dataset_affiliation_organization(
                    connection,
                    "dataset-a",
                    build_run_id="bad-contra-build",
                    expected_acquisition_root_run_id="root-a",
                )

        await db_fixtures._assert_dataset_a_relations_unchanged(
            database,
            schema,
            original_rows,
        )
