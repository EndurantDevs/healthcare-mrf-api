# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact client scope and source-qualified catalog replies under a real reader boundary."""

import json
from uuid import UUID

import asyncpg
import pytest

from process import registry_network_catalog_read as catalog
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.test_registry_network_catalog_read import (
    autocomplete_document,
    legacy_selector,
    network_document,
)
from tests.test_registry_network_catalog_read import (
    batch_boundary as batch_boundary,
)


def _company_document():
    """Use the ordinary closed company shape, independent of network membership."""
    return {
        "company_id": str(UUID(int=7)),
        "display_name": "Example Company",
        "roles": ["insurer"],
        "revision": 1,
        "link_revision": 1,
        "group_id": str(UUID(int=8)),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("excluded_kind", ["company", "group"])
async def test_nested_organization_scope_cannot_escape_catalog_page(batch_boundary, excluded_kind):
    connection, _, revisions = batch_boundary
    company_by_field = _company_document()
    document = network_document() | {"companies": [company_by_field]}
    connection.page["rows_json"] = json.dumps([document])
    policy = RegistryOrganizationVisibility(
        **{"excluded_" + excluded_kind + "_ids": (company_by_field[excluded_kind + "_id"],)}
    )
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="policy_invalid"):
        await catalog.read_registry_network_catalog(
            connection,
            catalog.RegistryNetworkCatalogQuery(generation_id=9),
            excluded_network_ids=(2,),
            organization_policy=policy,
            control_schema="sample_control",
        )
    assert connection.calls[-1][1][-2:] == (policy.excluded_company_ids, policy.excluded_group_ids)
    page = await catalog.read_registry_network_catalog(
        connection,
        catalog.RegistryNetworkCatalogQuery(generation_id=9),
        excluded_network_ids=(2,),
        organization_policy=RegistryOrganizationVisibility(),
        control_schema="sample_control",
    )
    assert page["items"] == (document,) and page["generation"] == "9"
    assert revisions == [4, 4] and connection.in_transaction


@pytest.mark.asyncio
async def test_untyped_organization_policy_refuses_before_metadata_query(batch_boundary):
    connection, _, _ = batch_boundary
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="policy_invalid"):
        await catalog.read_registry_network_catalog(
            connection,
            catalog.RegistryNetworkCatalogQuery(),
            excluded_network_ids=(),
            organization_policy={"excluded_company_ids": []},
            control_schema="sample_control",
        )
    assert all("filtered AS" not in statement for statement, _ in connection.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_id", [None, 8])
async def test_legacy_resolution_cannot_return_another_network(batch_boundary, returned_id):
    connection, _, revisions = batch_boundary
    connection.page.update(
        total=int(returned_id is not None),
        rows_json=json.dumps([] if returned_id is None else [network_document(returned_id)]),
    )
    selector = legacy_selector()
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="detail_denied"):
        await catalog.resolve_registry_network_catalog_legacy(
            connection, selector, excluded_network_ids=(2,), generation_id=9, control_schema="sample_control"
        )
    scoped_query, scoped_arguments = connection.calls[-2]
    assert "source_scope_json'=$3::jsonb" in scoped_query
    assert scoped_arguments == (4, selector.source.sql_parameters, selector.source_scope_json.decode(), (2,))
    assert connection.calls[-1][1][5] == (7,) and revisions == [4]


@pytest.mark.asyncio
@pytest.mark.parametrize("network_ids", [[], [7, 7], [8, 7], [True], [0], [2147483648]])
async def test_legacy_batch_rejects_uncanonical_resolver_targets_before_page(batch_boundary, network_ids):
    connection, _, _ = batch_boundary
    connection.legacy_batch["network_ids"] = network_ids
    selector = catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", ("42", "43"))
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="document_invalid"):
        await catalog.resolve_registry_network_catalog_legacy(
            connection, selector, excluded_network_ids=(), generation_id=9, control_schema="sample_control"
        )
    assert "AS complete" in connection.calls[-1][0]


@pytest.mark.asyncio
async def test_complete_legacy_batch_refuses_same_size_foreign_page(batch_boundary):
    connection, _, _ = batch_boundary
    connection.legacy_batch["network_ids"] = [7, 8]
    connection.page.update(total=2, rows_json=json.dumps([network_document(7), network_document(9)]))
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="detail_denied"):
        await catalog.resolve_registry_network_catalog_legacy(
            connection,
            catalog.RegistryNetworkCatalogLegacyBatchSelector("checksum_network", ("42", "43")),
            excluded_network_ids=(),
            generation_id=9,
            control_schema="sample_control",
        )
    assert connection.calls[-1][1][5] == (7, 8)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", [{"marketing_name": 7}, {"network_checksum": {}}, {"network_checksum": {"7": True}}]
)
async def test_autocomplete_never_returns_untyped_source_metadata(batch_boundary, changed):
    connection, _, _ = batch_boundary
    connection.autocomplete["plans_json"] = json.dumps([autocomplete_document() | changed])
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="document_invalid"):
        await catalog.read_registry_network_autocomplete(
            connection,
            catalog.RegistryNetworkAutocompleteQuery("Sample", generation_id=9),
            excluded_network_ids=(2,),
            control_schema="sample_control",
        )
    assert connection.calls[-1][1][1] == (2,) and connection.calls[-1][1][0] == 4


@pytest.mark.asyncio
async def test_zip_lookup_missing_tables_never_abort_parent_reader():
    """Execute real fallback ordering with external optional-table IO doubles."""
    from contextlib import asynccontextmanager

    events = []

    class Reader:
        @asynccontextmanager
        async def transaction(self):
            events.append("savepoint")
            try:
                yield self
            finally:
                events.append("end")

        async def fetch(self, statement, *arguments):
            events.append(arguments[0])
            if len(events) < 7:
                raise asyncpg.UndefinedTableError("optional lookup absent")
            return [{"state": "CA"}]

    assert await catalog._autocomplete_zip_states(Reader(), "12345") == ("CA",)
    assert events == ["savepoint", "12345", "end", "savepoint", "123", "end", "savepoint", "12345", "end"]
