# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pinned logical approval scope and complete named source-fact metadata reads."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_approved_membership_source as approved
from process import network_approved_source_bindings as bindings
from process import network_serving_read as serving
from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_inventory_store as inventory
from process.network_compact_layout import CompactAddressRelation
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_network_approved_source_bindings import COORDINATES, boundary, scope_row
from tests.test_registry_source_inventory_fact_owners import CONTROL, _owner, _selected

pytestmark = pytest.mark.asyncio


def _manifest():
    identity = str(UUID(int=1))
    schema = "network_candidate_" + UUID(identity).hex
    return PinnedNetworkServingManifest(
        7,
        identity,
        schema,
        1,
        {"custom_membership": "b" * 64},
        3,
        "a" * 64,
        CompactAddressRelation(schema, "entity_address_unified"),
    )


async def test_approved_api_reuses_selected_logical_generation_and_named_control(monkeypatch):
    manifest = _manifest()
    resolver = AsyncMock(return_value=manifest)
    scoped = AsyncMock(return_value=approved.ApprovedMembershipSource(3, "b" * 64))
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(approved, "_scope", scoped)
    connection = object()
    result, relation = await approved.pin_api_retained_approved_membership_source(
        connection, replace(manifest), control_schema=CONTROL
    )
    assert result.generation_id == "b" * 64
    assert relation == CompactAddressRelation(CONTROL, "registry_approved_record")
    resolver.assert_awaited_once_with(
        connection, generation_id=manifest.generation_id, control_schema=CONTROL, api_read=True
    )
    scoped.assert_awaited_once_with(connection, manifest.approved_custom_revision, '"' + CONTROL + '"', retained=True)


@pytest.mark.parametrize("changed", ["generation", "schema", "approval", "source"])
async def test_approved_api_refuses_changed_logical_manifest_before_approval_reads(monkeypatch, changed):
    manifest = _manifest()
    current = replace(
        manifest,
        **{
            "generation": {"generation_id": 8},
            "schema": {"schema_name": "another_snapshot"},
            "approval": {"approved_custom_revision": 4},
            "source": {"source_generations": {"custom_membership": "c" * 64}},
        }[changed],
    )
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", AsyncMock(return_value=current))
    scoped = AsyncMock()
    monkeypatch.setattr(approved, "_scope", scoped)
    with pytest.raises(approved.ApprovedMembershipSourceError, match="manifest differs"):
        await approved.pin_api_retained_approved_membership_source(object(), manifest, control_schema=CONTROL)
    scoped.assert_not_awaited()


async def test_approved_api_requires_the_manifest_approved_generation(monkeypatch):
    manifest = _manifest()
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", AsyncMock(return_value=manifest))
    monkeypatch.setattr(approved, "_scope", AsyncMock(return_value=approved.ApprovedMembershipSource(3, "c" * 64)))
    with pytest.raises(approved.ApprovedMembershipSourceError, match="generation differs"):
        await approved.pin_api_retained_approved_membership_source(object(), manifest, control_schema=CONTROL)


@pytest.mark.parametrize(
    "changes",
    [
        None,
        {"approved_revision": 4},
        {"isolation": "read committed"},
        {"is_bounded": False},
        {"invalid_documents": 1},
        {"unresolved_rows": 1},
    ],
)
async def test_approval_scope_checks_complete_data_bounds_and_snapshot(changes):
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={**scope_row(3, "b" * 64), **(changes or {})}),
        execute=AsyncMock(),
    )
    operation = approved._scope(connection, 3, '"' + CONTROL + '"', retained=True)
    if changes is None:
        assert await operation == approved.ApprovedMembershipSource(3, "b" * 64)
    else:
        with pytest.raises(approved.ApprovedMembershipSourceError):
            await operation
    query, revision, limit, schema = connection.fetchrow.await_args.args
    assert '"' + CONTROL + '".registry_approved_record' in query
    assert revision == 3 and limit == approved.MAX_INPUT_BYTES and schema == CONTROL
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("invalid", [False, True])
async def test_retained_bindings_validate_all_coordinates_against_selected_approval(invalid):
    connection, calls = boundary(current_revision=3, fingerprint="b" * 64, invalid=invalid)
    operation = bindings.require_retained_approved_network_source_bindings(
        connection, approved.ApprovedMembershipSource(3, "b" * 64), COORDINATES, control_schema="sample_control"
    )
    if invalid:
        with pytest.raises(bindings.ApprovedNetworkSourceBindingError, match="binding_invalid"):
            await operation
    else:
        assert await operation == 2
    assert len(calls) == 2
    assert calls[1][1][:7] == (3, *COORDINATES.sql_parameters)
    assert "invalid_scopes" in calls[1][0] and "duplicate_digests" in calls[1][0]


@pytest.mark.parametrize("legacy", [False, True])
async def test_source_metadata_resolves_named_generation_and_complete_registered_inventory(monkeypatch, legacy):
    original = _owner(2, legacy=legacy)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(side_effect=["repeatable read", "on"]),
        fetch=AsyncMock(return_value=[original.candidate]),
        fetchrow=AsyncMock(return_value={**original.candidate, "manifest_sha256": original.generation_id}),
        execute=AsyncMock(),
    )
    loader = AsyncMock(return_value=(original.reference, original.entries))
    monkeypatch.setattr(inventory, "load_source_inventory", loader)
    selected = _selected(original, original.sources[:1])
    resolved = await artifacts.authenticate_anchored_source_fact_metadata(
        connection, selected, control_schema=CONTROL, api_read=True
    )
    result = resolved[selected[0]["recipe_sha256"]]
    assert len(resolved) == 1 and result.reference.fact_candidate_id == original.candidate["candidate_id"]
    assert result.reference.fact_schema_name == original.candidate["schema_name"]
    assert result.reference.fact_generation_id == original.generation_id
    assert result.reference.fact_table_name == artifacts.CONTRIBUTION_TABLE
    assert result.descriptor == original.sources[0]
    assert loader.await_count == int(not legacy)
    if not legacy:
        assert loader.await_args.args[2] == original.candidate
        assert loader.await_args.kwargs == {"control_schema": CONTROL}
    assert "network_serving_manifest" in connection.fetchrow.await_args.args[0]
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("field", ["dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"])
async def test_source_metadata_refuses_changed_logical_scope_before_inventory(monkeypatch, field):
    original = _owner(1)
    current_by_field = {
        **original.candidate,
        "manifest_sha256": original.generation_id,
        field: "another_snapshot" if field == "schema_name" else str(UUID(int=99)),
    }
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(side_effect=["repeatable read", "on"]),
        fetch=AsyncMock(return_value=[original.candidate]),
        fetchrow=AsyncMock(return_value=current_by_field),
    )
    loader = AsyncMock()
    monkeypatch.setattr(inventory, "load_source_inventory", loader)
    with pytest.raises(ValueError, match="owner_unavailable"):
        await artifacts.authenticate_anchored_source_fact_metadata(
            connection, _selected(original), control_schema=CONTROL, api_read=True
        )
    loader.assert_not_awaited()


async def test_empty_fact_metadata_api_still_requires_readonly():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchval=AsyncMock(side_effect=["repeatable read", "off"]), fetch=AsyncMock()
    )
    with pytest.raises(ValueError, match="scope_invalid"):
        await artifacts.authenticate_anchored_source_fact_metadata(
            connection, (), control_schema=CONTROL, api_read=True
        )
    connection.fetch.assert_not_awaited()
