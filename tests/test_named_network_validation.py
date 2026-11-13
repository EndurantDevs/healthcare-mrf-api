# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical artifact evidence and complete named inventory reads."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_inventory_store as inventory_store
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import NetworkPublicationError, named_network_preparation_roles
from process.registry_source_contribution_store import RegistrySourceContributionError
from process.registry_source_inventory_contract import RegistrySourceInventoryError


def _candidate():
    candidate_id = str(UUID(int=4))
    return {
        "dataset_id": str(UUID(int=1)),
        "schema_id": str(UUID(int=2)),
        "producer_id": str(UUID(int=3)),
        "candidate_id": candidate_id,
        "schema_name": "network_candidate_" + UUID(candidate_id).hex,
        "source_recipes_json": [],
        "source_generations": {},
        "state": "published",
    }


def test_named_permission_profile_is_copied_and_canonical():
    profile_by_field = {
        "namespace_owner_role": "snapshot_owner",
        "publisher_role": "publisher",
        "application_roles": ["reader", "publisher", "builder"],
    }
    copied = named_network_preparation_roles(profile_by_field)
    assert copied["application_roles"] == ["builder", "publisher", "reader"]
    profile_by_field["application_roles"].append("later")
    assert copied["application_roles"] == ["builder", "publisher", "reader"]


@pytest.mark.parametrize("applications", [[], ["reader"], ["publisher", "publisher"], ["publisher", "snapshot_owner"]])
def test_named_permission_profile_requires_distinct_frozen_owner_and_publisher(applications):
    with pytest.raises(NetworkPublicationError):
        named_network_preparation_roles(
            {"namespace_owner_role": "snapshot_owner", "publisher_role": "publisher", "application_roles": applications}
        )


@pytest.mark.parametrize("has_validation", [False, True])
async def test_published_artifact_reads_data_evidence_by_logical_generation(has_validation):
    candidate = _candidate()
    validation_by_field = {
        "candidate_id": candidate["candidate_id"],
        "schema_name": candidate["schema_name"],
        "complete": True,
        "semantic_validation": {"mode": "sampled"},
    }
    report = {"final_validation": validation_by_field} if has_validation else {}
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(
            return_value={
                **candidate,
                "manifest_sha256": "a" * 64,
                "validation_json": report,
            }
        ),
    )
    result = await artifacts.load_published_artifact_validation(connection, candidate, "directory_control")
    assert result == {
        "candidate_id": candidate["candidate_id"],
        "generation_id": "a" * 64,
        "validation": validation_by_field if has_validation else None,
        "semantic_validation": {"mode": "sampled"} if has_validation else None,
    }
    query = connection.fetchrow.await_args.args[0]
    assert "network_serving_manifest" in query and "c.state='published'" in query
    assert "pg_catalog" not in query and "custody" not in query


@pytest.mark.parametrize("changes", [{"complete": False}, {"candidate_id": str(UUID(int=5))}, {"schema_name": "other"}])
async def test_artifact_data_evidence_requires_complete_matching_scope(changes):
    candidate = _candidate()
    validation_by_field = {
        "candidate_id": candidate["candidate_id"],
        "schema_name": candidate["schema_name"],
        "complete": True,
    }
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(
            return_value={
                **candidate,
                "manifest_sha256": "a" * 64,
                "validation_json": {"final_validation": validation_by_field | changes},
            }
        ),
    )
    with pytest.raises(RegistrySourceContributionError, match="validation_invalid"):
        await artifacts.load_published_artifact_validation(connection, candidate, "directory_control")


@pytest.mark.parametrize("mismatch", [False, True])
async def test_named_inventory_checks_registered_root_and_complete_rows(monkeypatch, mismatch):
    candidate = _candidate()
    copy_target = MembershipCopyTarget(
        *(candidate[key] for key in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"))
    )
    inventory = SimpleNamespace(root="complete-data-root")
    other = SimpleNamespace(root="other-data-root")
    registered_rows = [inventory, other if mismatch else inventory]
    monkeypatch.setattr(
        inventory_store,
        "verify_source_recipe_metadata",
        lambda value: SimpleNamespace(inventory=registered_rows.pop(0)),
    )
    entry = object()

    async def fold(connection, relation, root, rows, byte_limit, receive):
        assert relation == '"' + candidate["schema_name"] + '".network_source_inventory'
        assert root == inventory.root and rows == 100
        assert byte_limit == inventory_store.MAX_INVENTORY_PAGE_BYTES
        receive(entry)

    folded = AsyncMock(side_effect=fold)
    monkeypatch.setattr(inventory_store, "_fold_inventory", folded)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        fetchrow=AsyncMock(return_value=candidate),
    )
    if mismatch:
        with pytest.raises(RegistrySourceInventoryError, match="root_changed"):
            await inventory_store.load_source_inventory(
                connection, copy_target, candidate, control_schema="directory_control"
            )
        folded.assert_not_awaited()
    else:
        assert await inventory_store.load_source_inventory(
            connection, copy_target, candidate, control_schema="directory_control"
        ) == (inventory, (entry,))
        folded.assert_awaited_once()
    assert "pg_catalog" not in connection.fetchrow.await_args.args[0]


@pytest.mark.parametrize("field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"])
async def test_artifact_evidence_checks_actual_complete_logical_scope(field):
    candidate = _candidate()
    actual_by_field = dict(candidate, manifest_sha256="a" * 64, validation_json={})
    actual_by_field[field] = "other_snapshot" if field == "schema_name" else str(UUID(int=99))
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=actual_by_field))
    with pytest.raises(RegistrySourceContributionError, match="owner_unavailable"):
        await artifacts.load_published_artifact_validation(connection, candidate, "directory_control")


@pytest.mark.parametrize("matches", [False, True])
async def test_final_data_evidence_keeps_actual_source_generations(matches):
    candidate = _candidate()
    actual_generations_by_name = {"source_example": "edition-one"}
    final_by_field = {
        "candidate_id": candidate["candidate_id"],
        "schema_name": candidate["schema_name"],
        "complete": True,
        "source_generations": actual_generations_by_name if matches else {"source_example": "other-edition"},
    }
    actual_by_field = dict(
        candidate,
        manifest_sha256="a" * 64,
        source_generations=actual_generations_by_name,
        validation_json={"final_validation": final_by_field},
    )
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=actual_by_field))
    if matches:
        result = await artifacts.load_published_artifact_validation(connection, candidate, "directory_control")
        assert result["validation"] == final_by_field
    else:
        with pytest.raises(RegistrySourceContributionError, match="validation_invalid"):
            await artifacts.load_published_artifact_validation(connection, candidate, "directory_control")


async def test_absent_retained_generation_returns_before_storage_access():
    from process.registry_retained_generation import resolve_registry_retained_generation

    assert (
        await resolve_registry_retained_generation(object(), {"source_generations": {}}, control_schema="control")
        is None
    )


@pytest.mark.parametrize(
    "damage", [None, "absent", "ambiguous", "unpublished", "locator", "schema", "generation", "dataset"]
)
async def test_retained_named_generation_keeps_exact_rollback_edition(damage):
    from process.registry_retained_generation import (
        RegistryRetainedGenerationError,
        resolve_registry_retained_generation,
    )

    candidate_by_field = {
        **_candidate(),
        "generation_id": 7,
        "schema_revision": 1,
        "approved_custom_revision": 0,
        "validation_json": {"serving_readiness": {"component": "full_network_serving"}},
    }
    manifest_by_field = {
        "candidate_id": candidate_by_field["candidate_id"],
        "generation_id": 7,
        "schema_revision": 1,
        "source_generations": {},
        "approved_custom_revision": 0,
        "manifest_sha256": "a" * 64,
        "eligible": False,
    }
    selected_rows = [dict(candidate_by_field)]
    match damage:
        case "absent":
            selected_rows.clear()
        case "ambiguous":
            selected_rows *= 2
        case "unpublished":
            selected_rows[0]["state"] = "ready"
        case "locator":
            manifest_by_field["manifest_sha256"] = "b" * 64
        case "schema":
            candidate_by_field["schema_name"] = "another_generation"
        case "generation":
            manifest_by_field["generation_id"] = 8
        case "dataset":
            candidate_by_field["dataset_id"] = str(UUID(int=9))
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=selected_rows),
        execute=AsyncMock(),
        fetchrow=AsyncMock(return_value={"candidate_json": candidate_by_field, "manifest_json": manifest_by_field}),
    )
    operation = resolve_registry_retained_generation(
        connection, {"source_generations": {"retained_network_source": "a" * 64}}, control_schema="directory_control"
    )
    if damage is None:
        manifest, selected = await operation
        assert manifest.generation_id == 7 and manifest.schema_name == candidate_by_field["schema_name"]
        assert selected == selected_rows[0]
        assert connection.fetchrow.await_args.args[1:] == (7, True)
        assert manifest.address_relation.schema_name == candidate_by_field["schema_name"]
    else:
        with pytest.raises(RegistryRetainedGenerationError):
            await operation
    assert "manifest_sha256=$1 LIMIT 2" in connection.fetch.await_args.args[0]
    connection.execute.assert_not_awaited()
