# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed private-preparation reads, never worker-issued canonical grants."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_prepared_reads as reads


def test_read_plan_has_only_fixed_column_selects():
    statements = reads.cms_preparation_read_grants("catalog", "prepared_publisher")
    assert len(statements) == 8
    assert all(statement.startswith("GRANT SELECT(") for statement in statements)
    assert all(statement.endswith(' TO "prepared_publisher"') for statement in statements)
    assert all('ON "catalog".' in statement for statement in statements)
    for root, columns in reads.READ_COLUMNS.items():
        assert (
            f"GRANT SELECT({','.join(chr(34) + name + chr(34) for name in columns)}) "
            f'ON "catalog"."{root}" TO "prepared_publisher"'
        ) in statements
    assert reads.READ_COLUMNS["provider_directory_organization_identity"] == ("organization_id",)
    assert reads.READ_COLUMNS["provider_directory_site_identity"] == ("site_id",)
    assert reads.READ_COLUMNS["provider_directory_insurance_network_identity"] == ("network_id",)


@pytest.mark.parametrize("schema,role", [("catalog;RESET ROLE", "publisher"), ("catalog", "publisher;RESET ROLE")])
def test_read_plan_rejects_identifier_injection(schema, role):
    with pytest.raises(ValueError):
        reads.cms_preparation_read_grants(schema, role)


@pytest.mark.asyncio
@pytest.mark.parametrize("active,closed", [(False, False), (True, True)])
async def test_read_gate_requires_original_live_transaction(active, closed):
    connection = SimpleNamespace(is_in_transaction=lambda: active, is_closed=lambda: closed, fetchval=AsyncMock())
    with pytest.raises(ValueError, match="transaction_required"):
        await reads.require_cms_preparation_reads(connection, "catalog")
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, False])
async def test_read_gate_stops_on_first_native_refusal(refusal):
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, is_closed=lambda: False, fetchval=AsyncMock(return_value=refusal)
    )
    with pytest.raises(ValueError, match="missing_provider_directory_entity_source_binding"):
        await reads.require_cms_preparation_reads(connection, "catalog")
    assert connection.fetchval.await_count == 1


@pytest.mark.asyncio
async def test_read_gate_checks_every_column_on_native_relations():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, is_closed=lambda: False, fetchval=AsyncMock(return_value=True)
    )
    await reads.require_cms_preparation_reads(connection, "catalog")
    assert connection.fetchval.await_count == 8
    for call, (root, columns) in zip(connection.fetchval.await_args_list, reads.READ_COLUMNS.items(), strict=True):
        query, relation, requested = call.args
        assert relation == f'"catalog"."{root}"' and requested == list(columns)
        assert "pg_catalog.has_column_privilege" in query and "NOT c.relrowsecurity" in query
