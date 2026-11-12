# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Roles that differ only per network are retained as one role."""

from __future__ import annotations

import importlib
import itertools
import json
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitionerRole

importer = importlib.import_module("process.provider_directory_fhir")
resource_hash = importlib.import_module("process.provider_directory_resource_hash")
role_merge = importlib.import_module("process.provider_directory_role_merge")
proof_store = importlib.import_module("process.provider_directory_proof_store")

SEMANTIC_CONTRACT = resource_hash.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
SOURCES_ENV = role_merge.MERGED_ROLE_SOURCES_ENV


def role(role_id: str, network: str | None, **changes: object) -> dict[str, object]:
    return {
        "resource_id": role_id,
        "practitioner_ref": "Practitioner/p1",
        "organization_ref": "Organization/o1",
        "location_refs": ["Location/l1"],
        "network_refs": [f"Organization/{network}"] if network else [],
        "identifiers": [{"system": "urn:role", "value": role_id}],
        "specialty_codes": [{"code": "207Q00000X"}],
        "accepting_patients": [{"code": "newpt"}],
        "fhir_meta": {"profile": ["plannet"], "versionId": role_id, "lastUpdated": "2026-01-01T00:00:00Z"},
        "resource_url": f"https://directory.synthetic.test/PractitionerRole/{role_id}",
        **changes,
    }


def connections(payload: dict[str, object]) -> set[tuple[object, ...]]:
    return {
        (payload["practitioner_ref"], location, network)
        for location in payload["location_refs"]
        for network in payload["network_refs"] or [None]
    }


def merged(*roles: dict[str, object]) -> dict[str, object]:
    observations = [role_merge.merged_role_payload(payload) for payload in roles]
    result = observations[0]
    for observation in observations[1:]:
        result = role_merge.merge_semantic_resource_payloads(result, observation)
    return result


def test_roles_differing_only_per_network_become_one_role_in_any_order() -> None:
    payload_by_field_without_url = {name: value for name, value in role("r5", "n2").items() if name != "resource_url"}
    roles = [role("r1", "n1"), role("r2", "n2"), role("r3", "n3"), role("r4", "n1"), payload_by_field_without_url]

    results = [merged(*order) for order in itertools.permutations(roles)]

    assert all(result == results[0] for result in results)
    assert results[0]["network_refs"] == ["Organization/n1", "Organization/n2", "Organization/n3"]
    assert len(results[0]["identifiers"]) == 5
    assert results[0]["resource_id"] == role_merge.merged_role_id(results[0])
    assert connections(results[0]) == set().union(*(connections(payload) for payload in roles))
    assert role_merge.merged_role_payload(results[0]) == results[0]
    rekeyed_roles = [role_merge.merged_role_payload(payload) for payload in roles[:2]]
    assert importer._merged_semantic_dataset_payload("PractitionerRole", *rekeyed_roles, SEMANTIC_CONTRACT) == merged(
        *roles[:2]
    )


@pytest.mark.parametrize(
    "other",
    [
        role("r2", None),
        role("r2", "n2", location_refs=["Location/l2"]),
        role("r2", "n2", specialty_codes=[{"code": "208000000X"}]),
        role("r2", "n2", accepting_patients=[{"code": "nopt"}]),
        role("r2", "n2", fhir_meta={"profile": ["other"], "versionId": "2", "lastUpdated": "2026-01-01T00:00:00Z"}),
    ],
)
def test_roles_that_differ_beyond_their_network_stay_separate(other: dict[str, object]) -> None:
    assert role_merge.merged_role_id(role("r1", "n1")) != role_merge.merged_role_id(other)


def test_acceptance_merges_only_when_each_entry_names_its_network() -> None:
    first = role("r1", "n1", accepting_patients=[{"code": "newpt", "from_network_ref": "Organization/n1"}])
    second = role("r2", "n2", accepting_patients=[{"code": "nopt", "from_network_ref": "Organization/n2"}])

    assert merged(first, second)["accepting_patients"] == [
        {"code": "newpt", "from_network_ref": "Organization/n1"},
        {"code": "nopt", "from_network_ref": "Organization/n2"},
    ]


def test_identical_roles_without_a_network_are_retained_once() -> None:
    result = merged(role("r1", None), role("r2", None))

    assert result["network_refs"] == []
    assert [identifier["value"] for identifier in result["identifiers"]] == ["r1", "r2"]
    assert result["resource_id"] != role_merge.merged_role_id(role("r3", "n1"))


def test_repeated_native_identity_with_different_networks_still_conflicts() -> None:
    with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
        role_merge.merge_semantic_resource_payloads(role("r1", "n1"), role("r1", "n2"))


def retained_rows(rows: list[dict[str, object]], contract: str = SEMANTIC_CONTRACT) -> list[dict[str, object]]:
    return importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitionerRole,
        [dict(row) for row in rows],
        dataset_id="dataset-a",
        resource_hash_contract=contract,
    )


def test_only_opted_in_sources_retain_merged_roles(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [{**role("r1", "n1"), "source_id": "source-a"}, {**role("r2", "n2"), "source_id": "source-a"}]
    legacy_contract = resource_hash.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT

    monkeypatch.delenv(SOURCES_ENV, raising=False)
    native_rows = retained_rows(rows)
    assert role_merge.merged_role_rows(native_rows, rows, SEMANTIC_CONTRACT) == native_rows

    monkeypatch.setenv(SOURCES_ENV, "source-b, source-a")
    # The shared builder also writes acquisition proofs, so it keeps native identities for every source.
    assert [row["resource_id"] for row in retained_rows(rows)] == ["r1", "r2"]
    legacy_rows = retained_rows(rows, legacy_contract)
    assert role_merge.merged_role_rows(legacy_rows, rows, legacy_contract) == legacy_rows
    (row,) = role_merge.merged_role_rows(native_rows, rows, SEMANTIC_CONTRACT)
    payload = row["payload_json"]
    assert row["resource_id"] == payload["resource_id"] == role_merge.merged_role_id(payload)
    assert payload["network_refs"] == ["Organization/n1", "Organization/n2"]
    assert row["payload_hash"] == role_merge.resource_payload_sha256_for_contract(
        payload, SEMANTIC_CONTRACT, resource_type="PractitionerRole"
    )


def test_merged_role_commits_to_each_source_role_outside_its_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    source_rows = [{**role("r1", "n1"), "source_id": "source-a"}, {**role("r2", "n2"), "source_id": "source-a"}]
    monkeypatch.setenv(SOURCES_ENV, "source-a")
    (merged_row,) = role_merge.merged_role_rows(retained_rows(source_rows), source_rows, SEMANTIC_CONTRACT)
    role_by_field = dict(merged_row["payload_json"])
    native_roles = role_by_field.pop(role_merge.NATIVE_ROLES)

    assert len(native_roles) == 2 and {len(native_role) for native_role in native_roles} == {40}
    assert merged_row["resource_id"] == role_merge.merged_role_id(role_by_field)
    assert merged_row["payload_hash"] == role_merge.resource_payload_sha256_for_contract(
        role_by_field, SEMANTIC_CONTRACT, resource_type="PractitionerRole"
    )
    # A repeat keeps the commitments its first staged observation lacks, also outside its hash.
    repeat_row = role_merge._role_repeat_row(merged_row, set())
    assert repeat_row["payload_json"][role_merge.NATIVE_ROLES] == native_roles
    assert repeat_row["payload_hash"] == merged_row["payload_hash"]


def test_opted_in_source_still_conflicts_on_one_native_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [{**role("r1", "n1"), "source_id": "source-a"}, {**role("r1", "n2"), "source_id": "source-a"}]
    monkeypatch.setenv(SOURCES_ENV, "source-a")

    with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
        retained_rows(rows)


def proof_record(payload_by_field: dict[str, object]) -> list[object]:
    return proof_store.build_dataset_proof_record(
        {
            "resource_type": "PractitionerRole",
            "resource_id": payload_by_field["resource_id"],
            "payload_hash": resource_hash.resource_payload_sha256_for_contract(
                payload_by_field, SEMANTIC_CONTRACT, resource_type="PractitionerRole"
            ),
            "payload_json": payload_by_field,
        },
        SEMANTIC_CONTRACT,
    )


def test_merged_role_proofs_from_separate_batches_compose_to_the_retained_role() -> None:
    first_role, second_role = role("r1", "n1"), role("r2", "n2")
    batch_records = [proof_record(role_merge.merged_role_payload(payload)) for payload in (first_role, second_role)]

    reduced_fields, _diagnostics = proof_store.reduce_dataset_proof_records(batch_records)

    assert reduced_fields == proof_record(merged(first_role, second_role))


def test_native_roles_keep_their_content_hash_and_proof_conflict() -> None:
    native_role = role("r1", "n1")
    assert role_merge.merged_role_hash_components(native_role) is None
    assert resource_hash.resource_payload_sha256_for_contract(
        native_role, SEMANTIC_CONTRACT, resource_type="PractitionerRole"
    ) == resource_hash.semantic_resource_payload_sha256(native_role)

    with pytest.raises(proof_store.ProviderDirectoryProofStoreError, match="proof shards conflict"):
        proof_store.reduce_dataset_proof_records([proof_record(native_role), proof_record(role("r1", "n2"))])


def test_repeat_observations_merge_into_the_observed_role_in_any_order() -> None:
    first_role, second_role, third_role = role("r1", "n1"), role("r2", "n2"), role("r3", "n3")
    observed_role = role_merge.merged_role_payload(first_role)
    repeats = [
        {
            "resource_id": observed_role["resource_id"],
            role_merge._REPEAT: True,
            "network_refs": payload_by_field["network_refs"],
            "identifiers": payload_by_field["identifiers"],
        }
        for payload_by_field in (second_role, third_role)
    ]
    assert all(role_merge.merged_role_hash_components(repeat) is not None for repeat in repeats)

    expected = merged(first_role, second_role, third_role)
    for order in itertools.permutations([observed_role, *repeats]):
        result = order[0]
        for observation in order[1:]:
            result = role_merge.merge_semantic_resource_payloads(result, observation)
        # Repeats carry no provenance, so the one observed role supplies it; the content is the same.
        assert role_merge.merged_role_hash_components(result) == role_merge.merged_role_hash_components(expected)
        assert result["fhir_meta"] == observed_role["fhir_meta"]

    only_repeats = role_merge.merge_semantic_resource_payloads(*repeats)
    assert only_repeats[role_merge._REPEAT] is True
    assert only_repeats["network_refs"] == ["Organization/n2", "Organization/n3"]


def test_repeat_with_a_field_outside_the_role_merge_conflicts() -> None:
    observed_role = role_merge.merged_role_payload(role("r1", "n1"))
    repeat_by_field = {
        "resource_id": observed_role["resource_id"],
        role_merge._REPEAT: True,
        "accepting_patients": [{"code": "nopt"}],
    }

    with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
        role_merge.merge_semantic_resource_payloads(observed_role, repeat_by_field)
    assert role_merge.merged_role_hash_components({**repeat_by_field, "specialty_codes": []}) is None


@pytest.mark.parametrize("ordered", [False, True])
async def test_staging_reduces_only_roles_with_an_ordered_previous_observation(monkeypatch, ordered) -> None:
    monkeypatch.setattr(role_merge, "_ordered_input_by_name", {})
    first_role_by_field = role_merge.merged_role_payload(role("r1", "n1"))
    second_role_by_field = role_merge.merged_role_payload(role("r2", "n2"))
    role_row_by_field = {
        "resource_type": "PractitionerRole",
        "resource_id": second_role_by_field["resource_id"],
        "payload_json": second_role_by_field,
    }
    location_row_by_field = {"resource_type": "Location", "resource_id": "l1", "payload_json": {}}
    connection = AsyncMock()
    connection.fetchval.return_value = ordered
    connection.fetch.return_value = [
        {"resource_id": first_role_by_field["resource_id"], "payload_json": json.dumps(first_role_by_field)}
    ]
    binding_by_field = {"resets": {"*": 7, "PractitionerRole": 9}}

    assert await role_merge.staged_role_rows(
        connection, "source", "input_rows", binding_by_field, [location_row_by_field]
    ) == [location_row_by_field]
    connection.fetchval.assert_not_awaited()
    staged_rows = await role_merge.staged_role_rows(
        connection, "source", "input_rows", binding_by_field, [role_row_by_field, location_row_by_field]
    )
    assert staged_rows[1] is location_row_by_field
    if not ordered:
        assert staged_rows[0] is role_row_by_field
        connection.fetch.assert_not_awaited()
        return

    _, ids, reset, family_resets = connection.fetch.await_args.args
    assert (ids, reset, json.loads(family_resets)) == (
        [second_role_by_field["resource_id"]],
        7,
        binding_by_field["resets"],
    )
    assert staged_rows[0]["payload_json"]["network_refs"] == ["Organization/n2"]
    recovered_role_by_field = role_merge.merge_semantic_resource_payloads(
        first_role_by_field, staged_rows[0]["payload_json"]
    )
    assert (
        recovered_role_by_field["fhir_meta"] == first_role_by_field["fhir_meta"]
        and recovered_role_by_field["resource_url"] == first_role_by_field["resource_url"]
    )
    assert role_merge.merged_role_hash_components(recovered_role_by_field) == role_merge.merged_role_hash_components(
        role_merge.merge_semantic_resource_payloads(first_role_by_field, second_role_by_field)
    )
    connection.fetch.return_value = [
        {"resource_id": first_role_by_field["resource_id"], "payload_json": first_role_by_field}
    ]
    assert (
        await role_merge.staged_role_rows(
            connection, "source", "input_rows", binding_by_field, [role_row_by_field, location_row_by_field]
        )
        == staged_rows
    )
    assert connection.fetchval.await_count == 1


@pytest.mark.parametrize("conflict", [False, True])
async def test_completion_checks_full_observations_and_native_identity(conflict) -> None:
    connection = AsyncMock()
    connection.fetchval.return_value = conflict
    checks = (
        (role_merge.require_observed_merged_roles, (), "provider_directory_merged_role_observation_missing"),
        (
            role_merge.require_one_content_per_native_role,
            ("WHERE batch_ordinal > $1", (3,)),
            "provider_directory_resource_payload_conflict",
        ),
    )
    for check, arguments, error in checks:
        if conflict:
            with pytest.raises(ValueError, match=error):
                await check(connection, '"source"."roles"', *arguments)
        else:
            await check(connection, '"source"."roles"', *arguments)

    observed_arguments, native_arguments = [call.args for call in connection.fetchval.await_args_list]
    assert role_merge._REPEAT in observed_arguments[0]
    assert "batch_ordinal > $1" in native_arguments[0] and native_arguments[1:] == (3,)


def test_role_without_fhir_metadata_cannot_reuse_a_different_roles_hash() -> None:
    first = role_merge.merged_role_payload(role("r1", "n1", fhir_meta=None))
    second = role_merge.merged_role_payload(role("r2", "n2", fhir_meta=None))
    assert first["resource_id"] == second["resource_id"]
    forged_role_by_field = {**second, "location_refs": ["Location/other"]}
    assert role_merge.merged_role_hash_components(forged_role_by_field) is None
    with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
        role_merge.merge_semantic_resource_payloads(first, forged_role_by_field)
