# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only the active verified connection may reuse a catalog manifest."""

from dataclasses import replace
from unittest.mock import AsyncMock, call

import pytest

from process import registry_network_catalog_read as catalog
from tests.test_registry_network_catalog_read import batch_boundary, legacy_selector


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True])
async def test_legacy_reuses_active_verified_manifest_once(batch_boundary, monkeypatch, batch):
    connection, manifest, revisions = batch_boundary
    resolve = AsyncMock(wraps=catalog.resolve_network_serving_manifest)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", resolve)
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
    assert resolve.await_args_list == [
        call(connection, generation_id=None, control_schema="sample_control"),
        call(connection, generation_id=9, control_schema="sample_control"),
    ]
    assert sum("transaction_isolation" in sql for sql, _ in connection.calls) == 1
    assert connection.calls.count(("SELECT current_setting('search_path')", ())) == 1
    assert connection.calls.count(("SET LOCAL search_path TO pg_catalog,public", ())) == 1
    assert connection.calls.count(("SELECT set_config('search_path',$1,true)", ("sample_path",))) == 1
    locks = [sql for sql, _ in connection.calls if sql.startswith("LOCK TABLE")]
    assert locks == [
        'LOCK TABLE "candidate_sample".network_membership,"candidate_sample".provider_location_binding,'
        '"candidate_sample".entity_address_unified IN ACCESS SHARE MODE'
    ]
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
