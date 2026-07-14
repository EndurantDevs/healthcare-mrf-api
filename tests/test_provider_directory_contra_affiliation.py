# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import importlib.util
import json
from pathlib import Path

import pytest

from db.models.system import ProviderDirectoryOrganizationAffiliation


_DB_FIXTURE_PATH = Path(__file__).with_name(
    "test_provider_directory_dataset_serving_relations_db.py"
)
_DB_FIXTURE_SPEC = importlib.util.spec_from_file_location(
    "db_fixtures",
    _DB_FIXTURE_PATH,
)
db_fixtures = importlib.util.module_from_spec(_DB_FIXTURE_SPEC)
assert _DB_FIXTURE_SPEC.loader is not None
_DB_FIXTURE_SPEC.loader.exec_module(db_fixtures)

_FHIR_FIXTURE_PATH = Path(__file__).parent / "fixtures" / (
    "provider_directory_contra_affiliations.json"
)
importer = importlib.import_module("process.provider_directory_fhir")


def _raw_affiliations_by_id() -> dict[str, dict]:
    bundle = json.loads(_FHIR_FIXTURE_PATH.read_text(encoding="utf-8"))
    return {
        entry["resource"]["id"]: entry["resource"]
        for entry in bundle["entry"]
    }


def _dataset_payload(raw_resource: dict) -> dict:
    model, row = importer.parse_fhir_resource(
        "contra-source",
        raw_resource,
        run_id="contra-run",
    )
    assert model is ProviderDirectoryOrganizationAffiliation
    dataset_rows = importer._endpoint_dataset_resource_rows(
        model,
        [row],
        dataset_id="dataset-a",
    )
    assert len(dataset_rows) == 1
    return dataset_rows[0]["payload_json"]


def test_raw_affiliations_keep_primary_and_participating_organizations_typed():
    resources = _raw_affiliations_by_id()
    organization_only = _dataset_payload(resources["contra-organization-only"])
    explicit = _dataset_payload(resources["contra-explicit-participating"])

    assert organization_only["organization_ref"] == "Organization/org-b"
    assert organization_only["participating_organization_ref"] is None
    assert organization_only["network_refs"] == ["Organization/net-shared"]
    assert organization_only["location_refs"] == ["Location/contra-clinic"]
    assert organization_only["telecom"] == resources[
        "contra-organization-only"
    ]["telecom"]
    assert organization_only["specialty_codes"][0]["code"] == "207Q00000X"
    assert explicit["organization_ref"] == (
        "Organization/contra-costa-health-plan"
    )
    assert explicit["participating_organization_ref"] == "Organization/org-a"


def test_affiliation_relation_sql_uses_only_participating_organization():
    proof_sql = importer._dataset_affiliation_organization_proof_sql(
        should_verify_acquisition_root=True,
    )
    insert_sql = importer._dataset_affiliation_organization_edge_insert_sql(
        should_verify_acquisition_root=True,
    )

    assert "participating_organization_ref" in proof_sql
    assert "'organization_ref'" not in proof_sql
    assert "provider_directory_source" not in proof_sql
    assert "participating_organization_resource_id" in insert_sql
    assert "edge.participating_organization_resource_id" in insert_sql
    assert "'organization_ref'" not in insert_sql


async def _insert_raw_affiliation_fixtures(database, schema_name):
    await database.status(
        f"UPDATE {schema_name}.provider_directory_source "
        "SET canonical_api_base = :canonical_api_base, "
        "metadata_json = '{\"provider_directory_affiliation_reference_fallback\""
        ":\"organization_ref\"}'::jsonb "
        "WHERE source_id = 'source-a';",
        canonical_api_base=importer.CONTRA_COSTA_PROVIDER_DIRECTORY_BASE,
    )
    for resource_id, raw_resource in _raw_affiliations_by_id().items():
        await db_fixtures._insert_resource(
            database,
            schema_name,
            "dataset-a",
            "OrganizationAffiliation",
            resource_id,
            _dataset_payload(raw_resource),
        )


async def _build_fixture_relations(database):
    async with database.acquire() as connection:
        await importer._build_provider_directory_dataset_network_plan(
            connection,
            "dataset-a",
            build_run_id="contra-build",
            expected_acquisition_root_run_id="root-a",
        )
        return await (
            importer
            ._build_provider_directory_dataset_affiliation_organization(
                connection,
                "dataset-a",
                build_run_id="contra-build",
                expected_acquisition_root_run_id="root-a",
            )
        )


async def _fixture_affiliation_rows(database, schema_name):
    return await database.all(
        f"SELECT participating_organization_resource_id, "
        f"affiliation_resource_id FROM {schema_name}."
        "provider_directory_dataset_affiliation_organization "
        "WHERE dataset_id = 'dataset-a' "
        "AND affiliation_resource_id IN ("
        "'contra-organization-only', 'contra-explicit-participating') "
        "ORDER BY affiliation_resource_id;"
    )


async def _fixture_provider_network_plan_rows(database, schema_name):
    return await database.all(
        f"SELECT locator.participating_organization_resource_id, "
        "network_plan.network_resource_id, "
        f"network_plan.insurance_plan_resource_id FROM {schema_name}."
        "provider_directory_dataset_affiliation_organization AS locator "
        f"JOIN {schema_name}.provider_directory_dataset_resource AS affiliation "
        "ON affiliation.dataset_id = locator.dataset_id "
        "AND affiliation.resource_type = 'OrganizationAffiliation' "
        "AND affiliation.resource_id = locator.affiliation_resource_id "
        "CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE("
        "affiliation.payload_json->'network_refs', '[]'::jsonb"
        ")) AS network_ref(value) "
        f"JOIN {schema_name}.provider_directory_dataset_network_plan AS network_plan "
        "ON network_plan.dataset_id = locator.dataset_id "
        "AND network_plan.network_resource_id = NULLIF(regexp_replace("
        "network_ref.value, '^.*/', ''), '') "
        "WHERE locator.affiliation_resource_id IN ("
        "'contra-organization-only', 'contra-explicit-participating') "
        "ORDER BY 1, 2, 3;"
    )


@pytest.mark.asyncio
async def test_raw_organization_only_affiliation_emits_no_provider_plan_relation(
    monkeypatch,
):
    """Exclude primary organization while retaining explicit participant edges."""
    async with db_fixtures._dataset_database(monkeypatch) as (
        database,
        schema_name,
    ):
        await _insert_raw_affiliation_fixtures(database, schema_name)
        affiliation_proof = await _build_fixture_relations(database)
        affiliation_rows = await _fixture_affiliation_rows(database, schema_name)
        provider_network_plan_rows = await _fixture_provider_network_plan_rows(
            database,
            schema_name,
        )

        assert affiliation_proof["affiliation_resource_count"] == 7
        assert affiliation_proof[
            "affiliation_with_participating_organization_count"
        ] == 4
        assert affiliation_proof[
            "empty_participating_organization_reference_count"
        ] == 3
        assert affiliation_proof["edge_count"] == 4
        assert [tuple(affiliation_row) for affiliation_row in affiliation_rows] == [
            ("org-a", "contra-explicit-participating")
        ]
        assert [
            tuple(provider_network_plan_row)
            for provider_network_plan_row in provider_network_plan_rows
        ] == [
            ("org-a", "net-shared", "plan-a"),
            ("org-a", "net-shared", "plan-b"),
        ]
