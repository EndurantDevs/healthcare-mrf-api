# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed reuse authenticates the predecessor before distinguishing its source protocol."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_typed_retained_source as retained
from tests.test_registry_typed_recipe_references import _old_capture_recipe
from tests.test_registry_typed_source_recipe import _recipe


def _predecessor_reads(monkeypatch, previous_recipes):
    manifest = SimpleNamespace(generation_id=1, candidate_id="candidate")
    current = AsyncMock(return_value=manifest)
    recipes = AsyncMock(return_value=previous_recipes)
    candidates = AsyncMock(return_value={manifest.candidate_id: {"candidate_id": manifest.candidate_id}})
    owner = AsyncMock(side_effect=ValueError("required_custody_missing"))
    for name, method in (
        ("resolve_network_serving_manifest", current),
        ("resolve_retained_registry_source_recipes", recipes),
        ("_published_artifact_candidates", candidates),
        ("_verify_artifact_owner", owner),
    ):
        monkeypatch.setattr(retained, name, method)
    return manifest, current, recipes, candidates, owner


@pytest.mark.asyncio
@pytest.mark.parametrize("has_legacy_recipe", [False, True])
async def test_authenticated_predecessor_without_typed_recipes_has_no_reuse_proofs(monkeypatch, has_legacy_recipe):
    typed = _recipe()
    previous = (_old_capture_recipe(typed),) if has_legacy_recipe else ()
    manifest, current, recipes, candidates, owner = _predecessor_reads(monkeypatch, previous)
    connection = object()
    assert await retained.retained_typed_source_proofs(connection, manifest, (typed,), "registry") == {}
    current.assert_awaited_once_with(connection, generation_id=1, control_schema="registry")
    recipes.assert_awaited_once_with(connection, manifest, control_schema="registry")
    candidates.assert_not_awaited()
    owner.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["changed_manifest", "unclosed_predecessor"])
async def test_legacy_reuse_branch_cannot_skip_predecessor_authentication(monkeypatch, damage):
    typed = _recipe()
    manifest, current, recipes, candidates, owner = _predecessor_reads(monkeypatch, ())
    if damage == "changed_manifest":
        current.return_value = object()
    else:
        recipes.side_effect = ValueError("native_closure_changed")
    with pytest.raises(ValueError):
        await retained.retained_typed_source_proofs(object(), manifest, (typed,), "registry")
    candidates.assert_not_awaited()
    owner.assert_not_awaited()
    if damage == "changed_manifest":
        recipes.assert_not_awaited()


@pytest.mark.asyncio
async def test_actual_typed_predecessor_still_requires_artifact_custody(monkeypatch):
    typed = _recipe()
    manifest, _current, recipes, candidates, owner = _predecessor_reads(monkeypatch, (typed,))
    connection = object()
    with pytest.raises(ValueError, match="required_custody_missing"):
        await retained.retained_typed_source_proofs(connection, manifest, (typed,), "registry")
    recipes.assert_awaited_once()
    candidates.assert_awaited_once()
    owner.assert_awaited_once_with(connection, {"candidate_id": manifest.candidate_id}, "registry")
