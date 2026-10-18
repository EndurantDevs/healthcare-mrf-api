# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed billing resolves complete metadata before contributor readiness."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_entity_ref_resolution as selector
from api import billing_search_pagination as pagination
from api import plan_release_serving as serving
from api import plan_release_serving_resolution as resolution
from api import ptg2_billing_search_service as service
from api.billing_search_selector_contract import BillingSearchSelectorNotFoundError
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.billing_search_entity_ref_support import billing_entity_reference, resolved_source_scope
from tests.billing_search_service_support import selection
from tests.test_billing_search_network_policy import _scope
from tests.test_plan_release_serving_resolution import PLAN_RELEASE_ID, _binding_row, _Session


@pytest.mark.asyncio
async def test_metadata_resolution_preserves_full_roles_without_physical_checks(monkeypatch):
    rows = [
        _binding_row(expected_binding_count=2),
        _binding_row(
            expected_binding_count=2,
            binding_ordinal=0,
            role="allowed_amounts",
            snapshot_id="allowed-snapshot",
            source_key="allowed-source",
        ),
    ]
    ready = AsyncMock(side_effect=AssertionError("unadmitted physical readiness"))
    monkeypatch.setattr(resolution, "_validate_release_bindings", ready)
    resolved = await resolution.resolve_plan_release_serving_resolution(
        _Session(rows), PLAN_RELEASE_ID, include_billing_tax_identity_source=True, projection_only=True
    )
    assert resolved.state == "ready"
    assert tuple(binding.role for binding in resolved.selection.bindings) == ("in_network", "allowed_amounts")
    assert resolved.selection.binding_set_digest == "a" * 64
    assert not resolved.selection.includes_billing_tax_identity_source
    assert resolved.selection.network_tables_by_snapshot() is None
    ready.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", [{"is_pinned": False}, {"expected_binding_count": 2}, {"snapshot_status": "candidate"}]
)
async def test_metadata_resolution_still_requires_complete_published_pinned_recipe(monkeypatch, damage):
    ready = AsyncMock(side_effect=AssertionError("invalid recipe readiness"))
    monkeypatch.setattr(resolution, "_validate_release_bindings", ready)
    resolved = await resolution.resolve_plan_release_serving_resolution(
        _Session([_binding_row(**damage)]), PLAN_RELEASE_ID, projection_only=True
    )
    assert resolved.state == "unavailable" and resolved.selection is None
    ready.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [1, 0, None, "true"])
async def test_metadata_switch_requires_actual_boolean_before_database(value):
    session = _Session([_binding_row()])
    resolved = await resolution.resolve_plan_release_serving_resolution(session, PLAN_RELEASE_ID, projection_only=value)
    assert resolved.state == "unavailable" and session.calls == []


def _partial_release():
    full_selection = selection(binding_count=2)
    admitted = full_selection.in_network_bindings[1:]
    partial = replace(full_selection, _validated_serving_tables=full_selection._validated_serving_tables[1:])
    return partial, admitted


@pytest.mark.asyncio
async def test_managed_selector_resolves_only_admitted_source_and_keeps_all_coordinates(monkeypatch):
    partial, admitted = _partial_release()
    tables = partial.serving_tables_for_snapshot(admitted[0].snapshot_id)
    source_reader = AsyncMock(
        return_value=resolved_source_scope(
            publication=tables.provider_tax_identity_source_publication, snapshot_key=tables.shared_snapshot_key
        )
    )
    monkeypatch.setattr(selector, "resolve_billing_entity_ref_source_scope", source_reader)
    with _scope():
        resolved = await selector.resolve_billing_search_entity_ref_selector(
            object(),
            billing_entity_ref=billing_entity_reference(),
            authorized_plan_release_id=partial.plan_release_id,
            source_pinned_selection=partial,
            contributing_bindings=admitted,
        )
    assert tuple((binding.binding_ordinal, binding.state) for binding in resolved.selector_scope.bindings) == (
        (0, "no_match"),
        (1, "matched"),
    )
    source_reader.assert_awaited_once()
    assert source_reader.await_args.kwargs["snapshot_key"] == tables.shared_snapshot_key
    assert partial.bindings == selection(binding_count=2).bindings


@pytest.mark.asyncio
async def test_all_denied_billing_reference_is_private_not_found_without_source_query(monkeypatch):
    partial = replace(selection(binding_count=2), _validated_serving_tables=())
    source_reader = AsyncMock(side_effect=AssertionError("denied identity query"))
    monkeypatch.setattr(selector, "resolve_billing_entity_ref_source_scope", source_reader)
    with _scope(), pytest.raises(BillingSearchSelectorNotFoundError):
        await selector.resolve_billing_search_entity_ref_selector(
            object(),
            billing_entity_ref=billing_entity_reference(),
            authorized_plan_release_id=partial.plan_release_id,
            source_pinned_selection=partial,
            contributing_bindings=(),
        )
    source_reader.assert_not_awaited()


def test_managed_generation_keeps_denied_recipe_without_requiring_denied_tables():
    partial, admitted = _partial_release()
    with _scope():
        first = pagination.billing_search_snapshot_set_sha256(partial, contributing_bindings=admitted)
        changed = replace(
            partial, bindings=(replace(partial.bindings[0], source_key="different-denied-source"), admitted[0])
        )
        second = pagination.billing_search_snapshot_set_sha256(changed, contributing_bindings=admitted)
        assert first != second
        assert partial.network_tables_by_snapshot() is None
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.billing_search_snapshot_set_sha256(partial, contributing_bindings=admitted)


@pytest.mark.asyncio
async def test_admission_precedes_real_preparation_and_selector_on_original_bindings(monkeypatch):
    full_selection = selection(binding_count=2)
    metadata = replace(full_selection, _validated_serving_tables=(), _includes_billing_tax_identity_source=False)
    admitted = metadata.in_network_bindings[1:]
    events = []

    async def admit(_session, selected):
        assert selected is metadata and selected.bindings == full_selection.bindings
        events.append("admit")
        return admitted

    async def is_ready(_session, binding, **options):
        assert events == ["admit"] and binding is admitted[0]
        assert options["include_billing_tax_identity_source"] is True
        options["validated_serving_tables_by_snapshot_id"][binding.snapshot_id] = (
            full_selection.serving_tables_for_snapshot(binding.snapshot_id)
        )
        events.append("ready")
        return True

    async def source(_session, **options):
        assert events == ["admit", "ready"] and options["snapshot_key"] == 18
        events.append("selector")
        return resolved_source_scope(publication=options["source_publication"], snapshot_key=18)

    from types import SimpleNamespace

    monkeypatch.setattr(service.network_policy, "admitted_plan_release_bindings", admit)
    monkeypatch.setattr(serving, "is_release_binding_serving_ready", is_ready)
    monkeypatch.setattr(selector, "resolve_billing_entity_ref_source_scope", source)
    request = SimpleNamespace(billing_entity_ref=billing_entity_reference(), plan_release_id=metadata.plan_release_id)
    with _scope():
        prepared, resolved, contributors = await service._managed_billing_release(object(), metadata, request)
        assert prepared.network_tables_by_snapshot(contributing_bindings=contributors) is not None
        assert prepared.network_tables_by_snapshot() is None
    assert events == ["admit", "ready", "selector"] and contributors == admitted
    assert prepared.bindings == metadata.bindings and prepared.binding_set_digest == metadata.binding_set_digest
    assert tuple(binding.binding_ordinal for binding in resolved.selector_scope.bindings) == (0, 1)
