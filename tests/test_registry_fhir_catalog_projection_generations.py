# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Active retained sources are distinct from merely historical catalog sources."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_sync as sync


@pytest.mark.asyncio
async def test_projection_order_uses_granted_retained_sources_and_current_sources(monkeypatch):
    manifest = SimpleNamespace(manifest_sha256="a" * 64)
    resolve_manifest = AsyncMock(return_value=manifest)
    retained = AsyncMock(return_value=(SimpleNamespace(descriptor="retained-active"),))
    monkeypatch.setattr(sync, "resolve_network_serving_manifest", resolve_manifest)
    monkeypatch.setattr(sync, "_retained_catalog_sources", retained)
    monkeypatch.setattr(sync, "fhir_catalog_source_generation", lambda descriptor: descriptor)
    descriptor_by_generation = {
        "retained-active": {"profile": "retained_normalized"},
        "retained-history": {"profile": "retained_normalized"},
        "current-b": {"profile": "published_normalized"},
        "current-a": {"profile": "published_normalized"},
    }
    snapshot_by_field = {
        "network_generation": 9,
        "manifest_sha256": manifest.manifest_sha256,
        "source_vector_json": [
            {"source_generation": key, "status": "current" if key.startswith("current") else "last_good"}
            for key in descriptor_by_generation
        ],
    }
    connection, grant = object(), SimpleNamespace(control_schema="catalog_control")
    assert await sync.resolve_fhir_catalog_projection_generations(
        connection, snapshot_by_field, grant=grant, descriptors=descriptor_by_generation
    ) == ("retained-active", "current-a", "current-b")
    resolve_manifest.assert_awaited_once_with(connection, generation_id=9, control_schema=grant.control_schema)
    retained.assert_awaited_once_with(connection, grant, manifest)
    snapshot_by_field["manifest_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="manifest_changed"):
        await sync.resolve_fhir_catalog_projection_generations(
            connection, snapshot_by_field, grant=grant, descriptors=descriptor_by_generation
        )


@pytest.mark.asyncio
async def test_projection_rejects_an_omitted_actively_granted_generation(monkeypatch):
    monkeypatch.setattr(
        sync, "_retained_catalog_sources", AsyncMock(return_value=(SimpleNamespace(descriptor="active"),))
    )
    monkeypatch.setattr(sync, "fhir_catalog_source_generation", lambda descriptor: descriptor)
    snapshot_by_field = {"network_generation": None, "source_vector_json": []}
    with pytest.raises(ValueError, match="projection_source_missing"):
        await sync.resolve_fhir_catalog_projection_generations(
            object(), snapshot_by_field, grant=object(), descriptors={}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["no_generation", "missing", "sealed", "broken_custody"])
async def test_candidate_reuse_requires_a_present_snapshot_and_its_existing_custody(monkeypatch, state):
    from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant

    grant = standing_grant()
    selected = snapshot(grant)
    loader = SimpleNamespace(fetchrow=AsyncMock(return_value=None if state == "missing" else selected))
    verify = AsyncMock()
    if state == "broken_custody":
        verify.side_effect = ValueError("sealed custody changed")
    monkeypatch.setattr(sync, "verify_fhir_catalog_snapshot", verify)
    generation = None if state == "no_generation" else selected["metadata_generation"]
    if state == "broken_custody":
        with pytest.raises(ValueError, match="sealed custody changed"):
            await sync._reusable_catalog_snapshot(loader, grant, generation)
    else:
        result = await sync._reusable_catalog_snapshot(loader, grant, generation)
        assert result == (selected if state == "sealed" else None)
    assert verify.await_count == int(state in {"sealed", "broken_custody"})
    assert loader.fetchrow.await_count == int(generation is not None)
