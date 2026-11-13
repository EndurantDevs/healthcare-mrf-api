# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only the active verified connection may reuse a catalog manifest."""

from dataclasses import replace
from unittest.mock import AsyncMock, call

import pytest

from process import registry_network_catalog_read as catalog
from process.registry_serving_organization_read import (
    RegistryOrganizationCatalogQuery,
    read_registry_serving_organizations,
)
from tests.test_registry_network_catalog_read import batch_boundary, legacy_selector


@pytest.mark.asyncio
async def test_sealed_catalog_pin_resolves_the_published_metadata(batch_boundary, monkeypatch):
    connection, manifest, _ = batch_boundary
    object.__setattr__(manifest, "_custody_seal_sha256", "c" * 64)
    resolve = AsyncMock(return_value=manifest)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", resolve)
    assert await catalog.catalog_pin(connection, 9, "sample_control") is manifest
    resolve.assert_awaited_once_with(connection, generation_id=9, control_schema="sample_control", api_read=True)
    assert not any(sql.startswith("LOCK TABLE") for sql, _ in connection.calls)


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
        call(connection, generation_id=None, control_schema="sample_control", api_read=True)
    ]
    assert sum("transaction_isolation" in sql for sql, _ in connection.calls) == 1
    assert not any(sql.startswith("LOCK TABLE") for sql, _ in connection.calls)
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


async def _read_catalog_family(connection, manifest, family, *, generation=9, schema="sample_control"):
    options_by_name = {"control_schema": schema, "manifest": manifest}
    if family == "organization":
        return await read_registry_serving_organizations(
            connection, RegistryOrganizationCatalogQuery("company", (), generation_id=generation), **options_by_name
        )
    options_by_name["excluded_network_ids"] = ()
    if family == "detail":
        return await catalog.read_registry_network_catalog_detail(
            connection, 7, generation_id=generation, **options_by_name
        )
    if family == "autocomplete":
        query = catalog.RegistryNetworkAutocompleteQuery("Example", 10, generation_id=generation)
        return await catalog.read_registry_network_autocomplete(connection, query, **options_by_name)
    return await catalog.read_registry_network_catalog(
        connection, catalog.RegistryNetworkCatalogQuery(generation_id=generation), **options_by_name
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["list", "detail", "autocomplete", "organization"])
async def test_catalog_families_reuse_one_active_manifest(batch_boundary, monkeypatch, family):
    connection, manifest, revisions = batch_boundary
    pin = AsyncMock(wraps=catalog._pin)
    monkeypatch.setattr(catalog, "_pin", pin)
    if family == "organization":
        connection.page.update(total=0, rows_json="[]")
    async with catalog.registry_catalog_read_snapshot(connection, None, "sample_control") as pinned:
        assert pinned is manifest
        before_page = len(connection.calls)
        page = await _read_catalog_family(connection, pinned, family)
        assert len(connection.calls) == before_page + 1
        assert page["generation"] == "9" and page["approved_custom_revision"] == "4"
    pin.assert_awaited_once_with(connection, None, "sample_control")
    assert revisions == [4] and catalog._PINNED_READ.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["list", "detail", "autocomplete", "organization"])
@pytest.mark.parametrize("damage", ["detached", "copy", "connection", "schema", "generation", "closed"])
async def test_catalog_family_reuse_refuses_without_query_or_repin(batch_boundary, monkeypatch, family, damage):
    connection, manifest, _ = batch_boundary
    async with catalog.registry_catalog_read_snapshot(connection, None, "sample_control"):
        prior = len(connection.calls)
        pin = AsyncMock(side_effect=AssertionError("unexpected metadata verification"))
        monkeypatch.setattr(catalog, "_pin", pin)
        supplied = replace(manifest) if damage == "copy" else manifest
        driver = object() if damage == "connection" else connection
        schema = "other_control" if damage == "schema" else "sample_control"
        generation = 10 if damage == "generation" else 9
        connection.in_transaction = damage != "closed"
        detached = catalog._PINNED_READ.set(None) if damage == "detached" else None
        try:
            with pytest.raises(catalog.RegistryNetworkCatalogError):
                await _read_catalog_family(driver, supplied, family, generation=generation, schema=schema)
        finally:
            if detached is not None:
                catalog._PINNED_READ.reset(detached)
        assert len(connection.calls) == prior
        pin.assert_not_awaited()
