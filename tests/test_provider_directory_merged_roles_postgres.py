# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL completion of roles merged across acquisition batches."""

from __future__ import annotations

import datetime

import pytest

from db.models import ProviderDirectoryPractitionerRole
from process import provider_directory_generic_source_storage as source_storage
from process import provider_directory_role_merge as role_merge
from process.provider_directory_generic_source_completion import complete_generic_source_candidate
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    CANONICAL_BASE,
    SOURCE_ID,
    _candidate_stored_proof,
    _insert_parent,
    _semantic_database,
    _write_scope,
    importer,
)
from tests.provider_directory_semantic_proof_v3_postgres_support import (
    semantic_source_template as semantic_source_template,
)


def _role_row(role_id: str, network: str) -> dict[str, object]:
    observed_at = datetime.datetime(2026, 8, 9, 12, 0, 0)
    return {
        "resource_id": role_id,
        "active": True,
        "practitioner_ref": "Practitioner/practitioner-one",
        "organization_ref": "Organization/organization-one",
        "location_refs": ["Location/location-one"],
        "network_refs": [f"Organization/{network}"],
        "identifiers": [{"system": "urn:role", "value": role_id}],
        "specialty_codes": [{"code": "207Q00000X"}],
        "fhir_meta": {"versionId": role_id, "lastUpdated": "2026-08-09T12:00:00Z"},
        "resource_url": f"{CANONICAL_BASE}/PractitionerRole/{role_id}",
        "fhir_self_url": f"{CANONICAL_BASE}/PractitionerRole/{role_id}",
        "fhir_fetch_url": f"{CANONICAL_BASE}/PractitionerRole?page=1",
        "fhir_fetch_mode": "rest_bundle",
        "source_id": SOURCE_ID,
        "last_seen_run_id": "run-semantic-proof",
        "observed_at": observed_at,
        "updated_at": observed_at,
    }


async def _staged_payloads(database, schema: str, dataset_id: str) -> list[dict[str, object]]:
    """Read the loader's private input in arrival order."""
    async with source_transaction(database):
        binding = source_storage._json(
            await database.scalar(
                f'SELECT candidate_storage_json FROM "{schema}".provider_directory_endpoint_dataset '
                "WHERE dataset_id=:dataset_id",
                dataset_id=dataset_id,
            )
        )
        relation = importer._unscoped_qt(schema, binding["tables"]["resource_input"]["name"])
        staged_rows = await bound_publisher().connection.fetch(
            f"SELECT payload_json FROM {relation} ORDER BY batch_ordinal,row_ordinal"
        )
    return [source_storage._json(staged_row["payload_json"]) for staged_row in staged_rows]


async def _write_role_page(dataset_id: str, role_row: dict[str, object]) -> int:
    return await importer._upsert_resource_rows(
        ProviderDirectoryPractitionerRole,
        [role_row],
        options=importer.ProviderDirectoryResourceWriteOptions(
            run_id="run-semantic-proof",
            track_seen=False,
            canonical_api_base=CANONICAL_BASE,
            source_ids=[SOURCE_ID],
            dataset_scope=_write_scope(dataset_id),
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_opted_in", [True, False])
async def test_postgres_roles_appended_in_separate_batches_complete_as_one_role(monkeypatch, is_opted_in):
    """Complete two per-network roles of one provider and office that arrived in different batches."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID if is_opted_in else "")
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        await _write_role_page(dataset_id, _role_row("role-two", "network-two"))
        staged_payloads = await _staged_payloads(database, schema, dataset_id)
        assert len(staged_payloads) == 2
        if is_opted_in:
            # Each observation commits to its own source role; completion checks and drops these.
            native_roles = [staged_payload.pop(role_merge.NATIVE_ROLES) for staged_payload in staged_payloads]
            assert len({*native_roles[0], *native_roles[1]}) == 2
            # The second observation is stored as a repeat: only what the first did not already say.
            assert staged_payloads[1] == {
                "resource_id": staged_payloads[0]["resource_id"],
                role_merge._REPEAT: True,
                "identifiers": [{"system": "urn:role", "value": "role-two"}],
                "network_refs": ["Organization/network-two"],
            }
        else:
            assert role_merge._REPEAT not in staged_payloads[1]
        async with source_transaction(database):
            completed = await complete_generic_source_candidate(
                database, importer, database.semantic_candidates[dataset_id]
            )
            reduced = importer._unscoped_qt(schema, completed["tables"][source_storage.RESOURCE]["name"])
            retained_rows = await bound_publisher().connection.fetch(
                f"SELECT resource_id,payload_hash,payload_json FROM {reduced} ORDER BY resource_id"
            )
        if not is_opted_in:
            assert [retained_row["resource_id"] for retained_row in retained_rows] == ["role-one", "role-two"]
            return
        (retained_row,) = retained_rows
        payload_by_field = source_storage._json(retained_row["payload_json"])
        assert role_merge.NATIVE_ROLES not in payload_by_field
        assert (
            retained_row["resource_id"]
            == payload_by_field["resource_id"]
            == role_merge.merged_role_id(payload_by_field)
        )
        assert payload_by_field["network_refs"] == ["Organization/network-one", "Organization/network-two"]
        assert [identifier["value"] for identifier in payload_by_field["identifiers"]] == ["role-one", "role-two"]
        assert retained_row["payload_hash"] == resource_payload_sha256_for_contract(
            payload_by_field, SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT, resource_type="PractitionerRole"
        )


@pytest.mark.asyncio
async def test_postgres_merged_roles_validate_against_the_source_count_and_publish(monkeypatch):
    """Validate with the two roles the source returned, then publish the one retained role."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles-published"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        await _write_role_page(dataset_id, _role_row("role-two", "network-two"))
        candidate = database.semantic_candidates[dataset_id]
        proof = await _candidate_stored_proof(database, candidate)
        assert proof.resource_counts["PractitionerRole"] == 1
        async with source_transaction(database):
            await database.status(
                f'UPDATE "{schema}".provider_directory_source SET endpoint_id=:endpoint WHERE source_id=:source',
                endpoint=candidate.endpoint_id,
                source=SOURCE_ID,
            )
        validated = await importer._validate_endpoint_dataset_candidate(
            candidate, {"PractitionerRole": {"rows_fetched": 2, "complete": True}}
        )
        assert validated["validated"] and not validated["published"]
        async with source_transaction(database):
            await importer._publish_validated_artifact_dataset(
                importer.ProviderDirectoryArtifactDataset(
                    SOURCE_ID,
                    candidate.endpoint_id,
                    dataset_id,
                    candidate.acquisition_root_run_id,
                    dataset_hash=validated["dataset_hash"],
                    selected_resources=candidate.selected_resources,
                    retained_resources=tuple(sorted(proof.resource_counts)),
                )
            )
            stored_payload_rows = await database.all(
                f'SELECT payload_json FROM "{schema}".provider_directory_dataset_resource WHERE dataset_id=:dataset_id',
                dataset_id=dataset_id,
            )
        assert len(stored_payload_rows) == 1
        payload_by_field = source_storage._json(stored_payload_rows[0][0])
        assert payload_by_field["network_refs"] == ["Organization/network-one", "Organization/network-two"]


@pytest.mark.asyncio
async def test_postgres_role_observed_again_after_a_reset_is_stored_in_full(monkeypatch):
    """A reset discards earlier observations, so the next one must carry the whole role again."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles-reset"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        async with source_transaction(database):
            await source_storage.reset_generic_source_family(
                database, importer, database.semantic_candidates[dataset_id]
            )
        await _write_role_page(dataset_id, _role_row("role-two", "network-two"))
        staged_payloads = await _staged_payloads(database, schema, dataset_id)
        assert [role_merge._REPEAT in staged_payload for staged_payload in staged_payloads] == [False, False]
        async with source_transaction(database):
            completed = await complete_generic_source_candidate(
                database, importer, database.semantic_candidates[dataset_id]
            )
            reduced = importer._unscoped_qt(schema, completed["tables"][source_storage.RESOURCE]["name"])
            retained_payloads = await bound_publisher().connection.fetch(f"SELECT payload_json FROM {reduced}")
        assert len(retained_payloads) == 1
        assert source_storage._json(retained_payloads[0]["payload_json"])["network_refs"] == [
            "Organization/network-two"
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [{}, {"network_refs": ["Organization/network-two"]}, {"specialty_codes": [{"code": "208D00000X"}]}],
    ids=["same content", "another network", "another specialty"],
)
async def test_postgres_repeated_source_role_keeps_the_conflict_rule(monkeypatch, changes):
    """One source role may arrive twice with the same content, never with a changed network or body."""
    monkeypatch.setenv(role_merge.MERGED_ROLE_SOURCES_ENV, SOURCE_ID)
    async with _semantic_database(monkeypatch) as (database, schema):
        dataset_id = "dataset-merged-roles-native"
        await _insert_parent(database, schema, dataset_id, selected_resources=("PractitionerRole",))
        await _write_role_page(dataset_id, _role_row("role-one", "network-one"))
        await _write_role_page(dataset_id, {**_role_row("role-one", "network-one"), **changes})
        async with source_transaction(database):
            if changes:
                with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
                    await complete_generic_source_candidate(
                        database, importer, database.semantic_candidates[dataset_id]
                    )
                return
            completed = await complete_generic_source_candidate(
                database, importer, database.semantic_candidates[dataset_id]
            )
            reduced = importer._unscoped_qt(schema, completed["tables"][source_storage.RESOURCE]["name"])
            retained_payloads = await bound_publisher().connection.fetch(f"SELECT payload_json FROM {reduced}")
        (retained_payload,) = retained_payloads
        payload_by_field = source_storage._json(retained_payload["payload_json"])
        assert payload_by_field["network_refs"] == ["Organization/network-one"]
        assert role_merge.NATIVE_ROLES not in payload_by_field
