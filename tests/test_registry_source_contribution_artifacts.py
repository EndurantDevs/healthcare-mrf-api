# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct immutable source anchors preserve capture identity and forbid chains."""

from dataclasses import asdict
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import registry_source_contribution_artifacts as artifacts
from process.registry_source_contribution_store import RegistrySourceContributionError, _digest
from tests.test_registry_source_contributions import _descriptor, _recipe


def _reference():
    return artifacts.SourceFactArtifactReference(str(uuid4()), 42, "a" * 64)


@pytest.mark.parametrize(
    "field,value",
    [
        ("remove", "fact_table_oid"),
        ("fact_candidate_id", "00000000-0000-0000-0000-000000000000"),
        ("fact_table_oid", True),
        ("fact_seal_sha256", "A" * 64),
        ("predecessor", "synthetic-predecessor"),
        ("fact_table_oid", None),
    ],
)
def test_anchor_reference_is_closed_and_physical(field, value):
    descriptor_by_field = {**_descriptor(_recipe()), **asdict(_reference())}
    if field == "remove":
        descriptor_by_field.pop(value)
    else:
        descriptor_by_field[field] = value
    with pytest.raises(RegistrySourceContributionError):
        artifacts.source_fact_artifact_reference(descriptor_by_field)


def test_mapping_changes_keep_original_capture_and_direct_owner():
    descriptor = _descriptor(_recipe())
    reference = _reference()
    artifact = artifacts.ResolvedSourceFactArtifact(reference, "network_candidate_" + uuid4().hex, descriptor)
    current = descriptor
    for mapping in ("b" * 64, "c" * 64, "d" * 64):
        changed_by_field = {
            **current,
            "mapping_sha256": mapping,
            "source_generation": _digest([descriptor["capture_sha256"], mapping]),
        }
        current = artifacts.anchored_source_fact_descriptor(changed_by_field, artifact)
        assert artifacts.source_fact_artifact_reference(current) == reference
    assert current["recipe_sha256"] == descriptor["recipe_sha256"]
    assert current["fact_sha256"] == descriptor["fact_sha256"]
    for identity in ("capture_sha256", "fact_sha256", "fact_rows", "transform_version"):
        damaged_by_field = {**current, identity: 2 if identity == "fact_rows" else "f" * 64}
        with pytest.raises(RegistrySourceContributionError):
            artifacts.anchored_source_fact_descriptor(damaged_by_field, artifact)


def test_reference_owner_cannot_itself_reference_another_owner(monkeypatch):
    descriptor = _descriptor(_recipe())
    owning_by_field = {**descriptor, **asdict(_reference())}
    monkeypatch.setattr(
        artifacts, "verify_registry_source_contributions", lambda candidate: {"sources": [owning_by_field]}
    )
    with pytest.raises(RegistrySourceContributionError, match="chain_forbidden"):
        artifacts._owning_descriptor({}, descriptor)


@pytest.mark.asyncio
async def test_own_fact_dto_does_not_confer_native_authority():
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    connection.fetchval.return_value = "repeatable read"
    own_by_field = {
        **_descriptor(_recipe()),
        "fact_candidate_id": None,
        "fact_table_oid": None,
        "fact_seal_sha256": None,
    }
    assert (
        await artifacts.resolve_anchored_source_facts(connection, (own_by_field,), control_schema="synthetic_control")
        == {}
    )
    connection.fetch.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_last_owner_page_requires_exact_requested_set():
    connection = AsyncMock()
    connection.fetch.return_value = []
    with pytest.raises(RegistrySourceContributionError, match="owner_unavailable"):
        await artifacts._published_artifact_candidates(connection, "synthetic_control", {str(uuid4())})


@pytest.mark.parametrize("descriptor", [None, [], "untrusted-reference"])
def test_reference_requires_a_closed_descriptor_instead_of_an_opaque_identity(descriptor):
    with pytest.raises(RegistrySourceContributionError, match="reference_invalid"):
        artifacts.source_fact_artifact_reference(descriptor)


def test_direct_owner_requires_its_registered_contribution_receipt():
    candidate_by_field = {"source_generations": {}, "validation_json": {}, "source_recipes_json": []}
    with pytest.raises(RegistrySourceContributionError, match="owner_missing"):
        artifacts._owning_descriptor(candidate_by_field, _descriptor(_recipe()))


@pytest.mark.asyncio
async def test_missing_owner_receipt_refuses_before_inventory_or_fact_loading():
    connection = AsyncMock()
    candidate_by_field = {"source_generations": {}, "validation_json": {}, "source_recipes_json": []}
    with pytest.raises(RegistrySourceContributionError, match="owner_missing"):
        await artifacts._owner_sources(connection, candidate_by_field, {}, "synthetic_control")
    connection.fetch.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["container", "bytes", "duplicate_recipe"])
async def test_reference_scope_refuses_ambiguous_or_unbounded_input_before_owner_queries(damage):
    descriptor = _descriptor(_recipe())
    descriptors = {"untrusted": descriptor} if damage == "container" else (descriptor,)
    if damage == "bytes":
        descriptors = ({**descriptor, "untrusted": "x" * artifacts.MAX_RECEIPT_BYTES},)
    elif damage == "duplicate_recipe":
        descriptors *= 2
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    connection.fetchval.return_value = "repeatable read"
    with pytest.raises(RegistrySourceContributionError, match="reference_invalid"):
        await artifacts.authenticate_anchored_source_fact_metadata(
            connection, descriptors, control_schema="synthetic_control"
        )
    connection.fetch.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_artifact_read_requires_readonly_before_any_owner_metadata():
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    connection.fetchval.side_effect = ["repeatable read", "off"]
    with pytest.raises(RegistrySourceContributionError, match="scope_invalid"):
        await artifacts.authenticate_anchored_source_fact_metadata(
            connection, (), control_schema="synthetic_control", api_read=True
        )
    connection.fetch.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()
