# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native authority checks for exact desired CMS and retained Profile inputs."""

import json

import pytest

from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_contract as contract

from .test_provider_directory_profile_selection_db import _catalog_map, _selection_database
from .test_provider_directory_profile_selection_desired import _desired


def _cms_catalog_map():
    catalog_map = _catalog_map()
    catalog_map["items"].append(
        {"entry_id": "cms-npd", "runnable": True, "profile_enabled": True, "source_ids": ["cms-npd"]}
    )
    return catalog_map


async def _seed_cms(database, schema, *, incumbent=True):
    await database.status(f"INSERT INTO {schema}.provider_directory_api_endpoint VALUES ('cms-endpoint');")
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source "
        "(source_id, endpoint_id, org_name) VALUES ('cms-npd', 'cms-endpoint', 'Synthetic directory');"
    )
    for pair_map in [_desired()["desired_cms_dataset"], *([_desired()["expected_cms_incumbent"]] if incumbent else [])]:
        await database.status(
            f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
            "(dataset_id, endpoint_id, acquisition_root_run_id, dataset_hash, status, is_current, "
            "resource_count, validated_at, published_at, superseded_at, publication_metadata_json) "
            "VALUES (:dataset_id, :endpoint_id, :root, :hash, :status, :current, 12, now(), "
            "CASE WHEN :current THEN now() ELSE NULL END, NULL, CAST(:metadata AS jsonb));",
            dataset_id=pair_map["dataset_id"],
            endpoint_id=pair_map["endpoint_id"],
            root=pair_map["acquisition_root_run_id"],
            hash=pair_map["dataset_hash"],
            status=pair_map["publication_status"],
            current=pair_map["is_current"],
            metadata=json.dumps({"source_ids": ["cms-npd"]}),
        )


async def _registered_desired(catalog_map, desired_selection):
    request_map = await selection.current_profile_selection_request(catalog_map, **desired_selection)
    proof = contract.validated_profile_selection_attestation(
        await selection.attest_profile_selection(request_map, catalog_map)
    )
    await selection.assert_registered_profile_selection_current(proof, catalog_map)
    return proof


@pytest.mark.asyncio
async def test_desired_candidate_registry_rechecks_incumbent_and_retained_inputs(monkeypatch):
    async with _selection_database(monkeypatch) as (database, schema):
        await _seed_cms(database, schema)
        catalog_map = _cms_catalog_map()
        proof = await _registered_desired(catalog_map, _desired())
        assert [(pair["source_id"], pair["dataset_id"]) for pair in proof.pairs] == [
            ("cms-npd", "cms-next"),
            ("pdfhir_test_payer", "dataset-1"),
        ]
        assert proof.desired_cms_dataset["is_current"] is False
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET dataset_hash=:hash WHERE dataset_id='cms-current';",
            hash="d" * 64,
        )
        with pytest.raises(selection.ProviderDirectoryProfileSelectionStale):
            await selection.assert_registered_profile_selection_current(proof, catalog_map)


@pytest.mark.asyncio
async def test_current_cms_date_refresh_allocates_distinct_registered_identity(monkeypatch):
    async with _selection_database(monkeypatch) as (database, schema):
        await _seed_cms(database, schema)
        catalog_map = _cms_catalog_map()
        first = await _registered_desired(catalog_map, _desired(current=True))
        later = await _registered_desired(catalog_map, _desired(current=True, day="2026-09-30"))
        assert first.pairs == later.pairs and first.proof_id != later.proof_id
        assert later.authority_revision == first.authority_revision + 1
        with pytest.raises(selection.ProviderDirectoryProfileSelectionStale, match="proof_not_registered"):
            await selection.assert_registered_profile_selection_current(first, catalog_map)


@pytest.mark.asyncio
async def test_first_cms_candidate_does_not_enter_the_default_current_lane(monkeypatch):
    async with _selection_database(monkeypatch) as (database, schema):
        await _seed_cms(database, schema, incumbent=False)
        catalog_map = _cms_catalog_map()
        legacy_request = await selection.current_profile_selection_request(catalog_map)
        assert legacy_request["contract_id"] == contract.PROFILE_SELECTION_REQUEST_CONTRACT_ID
        assert legacy_request["datasets"] == [{"source_id": "pdfhir_test_payer", "dataset_id": "dataset-1"}]
        proof = await _registered_desired(catalog_map, _desired(incumbent=False))
        assert proof.operation == "publish" and proof.expected_cms_incumbent is None


@pytest.mark.parametrize("mutation", ["candidate_hash", "retained_source", "candidate_promoted"])
@pytest.mark.asyncio
async def test_registered_candidate_rejects_source_or_pointer_drift(monkeypatch, mutation):
    async with _selection_database(monkeypatch) as (database, schema):
        await _seed_cms(database, schema)
        catalog_map = _cms_catalog_map()
        proof = await _registered_desired(catalog_map, _desired())
        if mutation == "retained_source":
            await database.status(
                f"UPDATE {schema}.provider_directory_source "
                "SET org_name='Changed synthetic payer' WHERE source_id='pdfhir_test_payer';"
            )
        elif mutation == "candidate_hash":
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset "
                "SET dataset_hash=:hash WHERE dataset_id='cms-next';",
                hash="d" * 64,
            )
        else:
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset "
                "SET is_current=false, superseded_at=now() WHERE dataset_id='cms-current';"
            )
            await database.status(
                f"UPDATE {schema}.provider_directory_endpoint_dataset "
                "SET status='published', is_current=true, published_at=now() WHERE dataset_id='cms-next';"
            )
        with pytest.raises(selection.ProviderDirectoryProfileSelectionStale):
            await selection.assert_registered_profile_selection_current(proof, catalog_map)


@pytest.mark.parametrize(
    "assignment",
    ["published_at=NULL", "validated_at=NULL", "status='validated'", "publication_metadata_json='{}'::jsonb"],
)
@pytest.mark.asyncio
async def test_malformed_current_parent_cannot_be_treated_as_missing(monkeypatch, assignment):
    async with _selection_database(monkeypatch) as (database, schema):
        await _seed_cms(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET {assignment} WHERE dataset_id='cms-current';"
        )
        with pytest.raises(contract.ProviderDirectoryProfileSelectionDrift):
            await selection.current_profile_selection_request(_cms_catalog_map(), **_desired(incumbent=False))
