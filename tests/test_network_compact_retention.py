# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Mandatory compact pins supplement both existing local cleanup callbacks."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_ownership as ownership
from process import entity_address_snapshot_preparation as protected
from process import network_cms_registry_address_capture as capture
from process import network_compact_retention as retention
from tests.test_entity_address_archive_ownership_guards import _set_owner


@pytest.mark.parametrize("blocked_call", [1, 2])
async def test_cleanup_requires_both_compact_reference_checks(monkeypatch, blocked_call):
    owner = _set_owner()
    connection = object()
    monkeypatch.setattr(protected, "_publisher_authority", AsyncMock(return_value=31))
    monkeypatch.setattr(ownership, "_validated_publication_owner", lambda *_: ("example", [], {"relation_oid": 8}))
    monkeypatch.setattr(ownership, "_require_retained_catalog", lambda *_: None)
    monkeypatch.setattr(
        ownership, "_retained_relation_catalog", AsyncMock(return_value=[{"nspname": "example", "relname": "retained"}])
    )
    monkeypatch.setattr(ownership, "_schema_oid", AsyncMock(return_value=owner.schema_oid))
    monkeypatch.setattr(ownership, "_assert_namespace_is_owned", AsyncMock())
    monkeypatch.setattr(capture, "native_driver", AsyncMock(return_value=connection))
    guard_outcomes = [retention.NetworkCompactRetentionError("retained")]
    if blocked_call == 2:
        guard_outcomes.insert(0, {"projection": "first-data-version"})
    guard = AsyncMock(side_effect=guard_outcomes)
    monkeypatch.setattr(retention, "require_network_compact_artifacts_unreferenced", guard)
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    old_hook = AsyncMock(return_value=True)
    with pytest.raises(retention.NetworkCompactRetentionError, match="retained"):
        await ownership.cleanup_entity_address_archive_publication(
            session, owner=owner, publication={}, assert_unreferenced=old_hook
        )
    assert guard.await_count == old_hook.await_count == blocked_call
    from process.network_compact_layout import CompactAddressRelation

    assert guard.await_args.args == (connection, (CompactAddressRelation("example", "retained"),))
    if blocked_call == 2:
        assert guard.await_args.kwargs == {"expected_projection": {"projection": "first-data-version"}}
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == blocked_call - 1
    assert all("ACCESS EXCLUSIVE" in statement for statement in statements)
    assert not any("DROP" in statement for statement in statements)


def _retention_connection():
    async def value(query, *arguments):
        if query == "SHOW transaction_isolation":
            return "read committed"
        if query.startswith("SELECT count(*) FROM pg_class"):
            return 3
        assert query.startswith("WITH candidates AS")
        return {"candidates": "a" * 64, "manifests": "b" * 64, "head": {"generation_id": 2}}

    return SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(side_effect=value), execute=AsyncMock())


async def test_reference_walk_precedes_short_version_check(monkeypatch):
    from process.network_compact_layout import CompactAddressRelation

    connection = _retention_connection()
    walk = AsyncMock()
    monkeypatch.setattr(retention, "_walk_references", walk)
    names = (CompactAddressRelation("retained", "entity_address_unified"),)
    projection = await retention.require_network_compact_artifacts_unreferenced(
        connection, names, control_schema="control"
    )
    connection.execute.assert_not_awaited()
    walk.assert_awaited_once()
    assert (
        await retention.require_network_compact_artifacts_unreferenced(
            connection, names, control_schema="control", expected_projection=projection
        )
        == projection
    )
    assert walk.await_count == 1
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert "statement_timeout" in statements[0] and "LEAST" in statements[0]
    assert statements[1].endswith("IN SHARE MODE NOWAIT")
    assert "snapshot_retention" not in statements[1] and "FOR UPDATE" not in statements[1]


@pytest.mark.parametrize("change", ["candidate", "manifest", "head"])
async def test_changed_reference_projection_aborts_after_target_lock_without_rewalking(monkeypatch, change):
    from process.network_compact_layout import CompactAddressRelation

    connection = _retention_connection()
    monkeypatch.setattr(retention, "_walk_references", AsyncMock())
    names = (CompactAddressRelation("retained", "entity_address_unified"),)
    projection = await retention.require_network_compact_artifacts_unreferenced(
        connection, names, control_schema="control"
    )
    actual_by_field = dict(projection["projection"])
    actual_by_field[{"candidate": "candidates", "manifest": "manifests", "head": "head"}[change]] = (
        "changed-logical-version"
    )
    monkeypatch.setattr(retention, "_projection", AsyncMock(return_value=actual_by_field))
    with pytest.raises(retention.NetworkCompactRetentionError, match="references_changed"):
        await retention.require_network_compact_artifacts_unreferenced(
            connection, names, control_schema="control", expected_projection=projection
        )
    assert connection.execute.await_args.args[0].endswith("IN SHARE MODE NOWAIT")


async def test_physical_ids_cannot_name_retirement_targets():
    connection = _retention_connection()
    with pytest.raises(retention.NetworkCompactRetentionError, match="relations_invalid"):
        await retention.require_network_compact_artifacts_unreferenced(connection, [42], control_schema="control")
    connection.fetchval.assert_not_awaited()
    connection.execute.assert_not_awaited()


async def test_declared_unwritten_fact_inventory_is_not_an_empty_dependency_set():
    from process.registry_source_contribution_store import RegistrySourceContributionError
    from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
    from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
    from process.registry_source_recipe_store import RECIPE_DIGEST_KEY
    from tests.registry_source_recipe_support import _example
    from tests.test_registry_source_contributions import _descriptor

    recipe = _example()
    root = source_inventory_root((make_source_inventory_entry(1, recipe, contribution=_descriptor(recipe)),))
    candidate_by_field = {
        "source_recipes_json": SourceInventoryReference(root).as_dict(),
        "source_generations": {INVENTORY_DIGEST_KEY: root.entries_sha256, RECIPE_DIGEST_KEY: root.recipes_sha256},
        "validation_json": {},
    }
    with pytest.raises(RegistrySourceContributionError, match="contribution_invalid"):
        await retention._source_fact_dependencies(object(), candidate_by_field, "control", None)
