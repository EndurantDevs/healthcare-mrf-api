# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Staging and completion checks for roles retained once per provider and office."""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_proof_store as proof_store
from process import provider_directory_role_merge as role_merge
from tests.test_provider_directory_merged_roles import (
    SEMANTIC_CONTRACT,
    SOURCES_ENV,
    merged,
    proof_record,
    retained_rows,
    role,
)

SOURCE_ID = "source-a"
SCHEMA = "source_schema"


@pytest.fixture
def input_name() -> str:
    # Whether an input is ordered is remembered per input name for the life of the process.
    return f"pdgs_{uuid.uuid4().hex}_ri"


@pytest.fixture
def staged_row(monkeypatch: pytest.MonkeyPatch):
    """Build the row an opted-in batch stages for one source role."""
    monkeypatch.setenv(SOURCES_ENV, SOURCE_ID)

    def build(source_role: dict[str, object]) -> dict[str, object]:
        source_rows = [{**source_role, "source_id": SOURCE_ID}]
        (merged_row,) = role_merge.merged_role_rows(retained_rows(source_rows), source_rows, SEMANTIC_CONTRACT)
        return merged_row

    return build


def loader(first_observations: list[dict[str, object]], *, is_ordered: bool = True) -> SimpleNamespace:
    """A loader connection whose resource input already holds these first observations."""
    return SimpleNamespace(
        fetchval=AsyncMock(return_value=is_ordered), fetch=AsyncMock(return_value=first_observations)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stored", [json.dumps, dict], ids=["text", "decoded"])
async def test_role_already_staged_is_stored_as_only_its_new_entries(staged_row, input_name, stored) -> None:
    first_row, second_row = staged_row(role("r1", "n1")), staged_row(role("r2", "n2"))
    unseen_row = staged_row(role("r3", "n1", practitioner_ref="Practitioner/p2"))
    organization_by_field = {**first_row, "resource_type": "Organization", "resource_id": "o1", "payload_json": {}}
    first_payload = first_row["payload_json"]
    connection = loader([{"resource_id": first_row["resource_id"], "payload_json": stored(first_payload)}])
    cutoff_by_type = {"*": 4, "PractitionerRole": 6}

    staged_rows = await role_merge.staged_role_rows(
        connection, SCHEMA, input_name, {"resets": cutoff_by_type}, [organization_by_field, second_row, unseen_row]
    )

    assert staged_rows[0] is organization_by_field and staged_rows[2] is unseen_row
    repeat_by_field = staged_rows[1]["payload_json"]
    assert repeat_by_field == {
        "resource_id": first_row["resource_id"],
        role_merge._REPEAT: True,
        "network_refs": ["Organization/n2"],
        "identifiers": [{"system": "urn:role", "value": "r2"}],
        role_merge.NATIVE_ROLES: second_row["payload_json"][role_merge.NATIVE_ROLES],
    }
    assert staged_rows[1]["payload_hash"] == role_merge.resource_payload_sha256_for_contract(
        repeat_by_field, SEMANTIC_CONTRACT, resource_type="PractitionerRole"
    )
    # Completion composes the same role from the repeat as from the whole second observation.
    completed = role_merge.merge_semantic_resource_payloads(first_payload, repeat_by_field)
    assert completed["network_refs"] == ["Organization/n1", "Organization/n2"]
    assert role_merge.merged_role_hash_components(completed) == role_merge.merged_role_hash_components(
        role_merge.merge_semantic_resource_payloads(first_payload, second_row["payload_json"])
    )
    statement, role_ids, cutoff, encoded_resets = connection.fetch.await_args.args
    assert f'"{SCHEMA}"."{input_name}"' in statement
    assert role_ids == sorted({first_row["resource_id"], unseen_row["resource_id"]})
    assert (cutoff, json.loads(encoded_resets)) == (4, cutoff_by_type)
    connection.fetchval.assert_awaited_once()
    assert connection.fetchval.await_args.args[1] == f'"{SCHEMA}"."{input_name}_order"'


@pytest.mark.asyncio
async def test_unordered_input_keeps_roles_whole_and_is_probed_once(staged_row, input_name) -> None:
    rows = [staged_row(role("r2", "n2"))]
    connection = loader([], is_ordered=False)

    for _batch in range(2):
        assert await role_merge.staged_role_rows(connection, SCHEMA, input_name, {"resets": {}}, rows) == rows

    connection.fetchval.assert_awaited_once()
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_without_merged_roles_runs_no_statement(input_name) -> None:
    (native_row,) = retained_rows([{**role("r1", "n1"), "source_id": SOURCE_ID}])
    rows = [native_row, {**native_row, "payload_json": "{}"}, {**native_row, "resource_type": "Organization"}]

    # A connection without any method fails the test on the first statement.
    assert await role_merge.staged_role_rows(object(), SCHEMA, input_name, {"resets": {}}, rows) == rows


@pytest.mark.asyncio
async def test_completion_refuses_a_role_retained_only_as_a_repeat() -> None:
    relation = f'"{SCHEMA}"."reduced_resource"'
    connection = loader([], is_ordered=False)
    await role_merge.require_observed_merged_roles(connection, relation)
    (statement,) = connection.fetchval.await_args.args
    assert relation in statement and f"? '{role_merge._REPEAT}'" in statement

    connection.fetchval.return_value = True
    with pytest.raises(ValueError, match="provider_directory_merged_role_observation_missing"):
        await role_merge.require_observed_merged_roles(connection, relation)


@pytest.mark.asyncio
async def test_completion_refuses_one_source_role_with_two_contents() -> None:
    relation, active_where = f'"{SCHEMA}"."resource_input"', "WHERE batch_ordinal>$1 "
    connection = loader([], is_ordered=False)
    await role_merge.require_one_content_per_native_role(connection, relation, active_where, (3,))
    statement, *arguments = connection.fetchval.await_args.args
    assert f"{active_where}AND resource_type='PractitionerRole'" in statement and arguments == [3]

    connection.fetchval.return_value = True
    with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
        await role_merge.require_one_content_per_native_role(connection, relation, active_where, (3,))


@pytest.mark.parametrize(
    ("source_ids", "is_ordered"),
    [
        (["source-a"], True),
        (["source-b", "source-a"], True),
        (["source-a", "source-c"], False),
        ([], False),
        (None, False),
    ],
)
def test_input_is_ordered_only_when_every_source_retains_merged_roles(monkeypatch, source_ids, is_ordered) -> None:
    monkeypatch.setenv(SOURCES_ENV, " source-a ,source-b,")

    assert role_merge.is_role_repeat_source(source_ids) is is_ordered


def test_hashed_native_identity_keeps_the_equal_content_rule() -> None:
    native_id = "a" * 64
    first, second = role(native_id, "n1"), role(native_id, "n2")

    assert role_merge.merged_role_hash_components(first) is None
    assert role_merge.merge_semantic_resource_payloads(first, dict(first)) == first
    with pytest.raises(ValueError, match="provider_directory_resource_payload_conflict"):
        role_merge.merge_semantic_resource_payloads(first, second)


def test_roles_without_fhir_metadata_merge_apart_from_roles_with_it() -> None:
    result = merged(role("r1", "n1", fhir_meta=None), role("r2", "n2", fhir_meta=None))

    assert result["network_refs"] == ["Organization/n1", "Organization/n2"]
    assert result["resource_id"] != role_merge.merged_role_id(role("r3", "n3"))


def test_proofs_of_two_merged_roles_never_reduce_to_one() -> None:
    other_office = role("r2", "n2", location_refs=["Location/l2"])
    records = [proof_record(role_merge.merged_role_payload(payload)) for payload in (role("r1", "n1"), other_office)]

    with pytest.raises(proof_store.ProviderDirectoryProofStoreError, match="identity changed"):
        proof_store.reduce_dataset_proof_records(records)


def test_merged_rows_carry_the_commitments_a_fresh_hash_gives(staged_row) -> None:
    merged_row = staged_row(role("r1", "n1"))
    plain_payload_by_field = dict(merged_row["payload_json"])

    assert role_merge.merged_role_hash_components(merged_row["payload_json"]) == (
        role_merge.merged_role_hash_components(plain_payload_by_field)
    )
    assert merged_row["payload_hash"] == role_merge.merged_role_hash_components(plain_payload_by_field)[2]
    assert json.dumps(merged_row["payload_json"], sort_keys=True) == json.dumps(plain_payload_by_field, sort_keys=True)
