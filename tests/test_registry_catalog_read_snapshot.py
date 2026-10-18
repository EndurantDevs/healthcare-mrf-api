# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only the active verified connection may reuse a catalog manifest."""

from dataclasses import replace

import pytest

from process import registry_network_catalog_read as catalog
from tests.test_registry_network_catalog_read import batch_boundary, legacy_selector


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True])
async def test_legacy_reuses_active_verified_manifest_once(batch_boundary, batch):
    connection, manifest, revisions = batch_boundary
    selector = (
        catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", ("42",)) if batch else legacy_selector()
    )
    async with catalog.registry_catalog_read_snapshot(connection, None, "sample_control") as pinned:
        assert pinned is manifest
        page = await catalog.resolve_registry_network_catalog_legacy(
            connection,
            selector,
            excluded_network_ids=(),
            control_schema="sample_control",
            generation_id=9,
            manifest=pinned,
        )
        assert page["generation"] == "9" and page["items"][0]["network_id"] == 7
    assert revisions == [4]
    assert sum("current_setting" in sql for sql, _ in connection.calls) == 1
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        catalog._reuse_manifest(connection, manifest, 9, "sample_control")


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["copy", "connection", "schema", "generation", "closed"])
async def test_manifest_reuse_refuses_unverified_context(batch_boundary, damage):
    connection, manifest, _ = batch_boundary
    async with catalog.registry_catalog_read_snapshot(connection, None, "sample_control"):
        supplied = replace(manifest) if damage == "copy" else manifest
        driver = object() if damage == "connection" else connection
        schema = "other_control" if damage == "schema" else "sample_control"
        generation = 10 if damage == "generation" else 9
        connection.in_transaction = damage != "closed"
        with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
            catalog._reuse_manifest(driver, supplied, generation, schema)


@pytest.mark.asyncio
async def test_nested_snapshot_restores_outer_proof_after_failure(batch_boundary):
    connection, manifest, revisions = batch_boundary
    async with catalog.registry_catalog_read_snapshot(connection, None, "sample_control"):
        with pytest.raises(RuntimeError, match="interrupted"):
            async with catalog.registry_catalog_read_snapshot(connection, 9, "sample_control"):
                raise RuntimeError("interrupted")
        assert catalog._reuse_manifest(connection, manifest, 9, "sample_control") is manifest
    assert revisions == [4, 4]
    assert catalog._PINNED_READ.get() is None
