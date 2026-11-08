# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allocation input and conflict refusals happen before exposing network identities."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_identity as identity


def _input(**changes):
    return identity.FHIRNetworkIdentityInput(
        **{
            "organization_id": "network-one",
            "endpoint_id": "endpoint-one",
            "source_aliases": ("source-one",),
            "evidence_id": "reviewed-edition",
            **changes,
        }
    )


def _connection(*, active=True):
    return SimpleNamespace(
        is_in_transaction=Mock(return_value=active),
        execute=AsyncMock(),
        copy_records_to_table=AsyncMock(),
        fetchval=AsyncMock(),
        fetch=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation", [identity.allocate_fhir_catalog_network_ids, identity.resolve_fhir_catalog_network_ids]
)
@pytest.mark.parametrize("active,revision", [(False, 0), (True, True), (True, -1)])
async def test_identity_operations_require_caller_transaction_and_valid_approval_revision(operation, active, revision):
    connection = _connection(active=active)
    with pytest.raises(ValueError, match="registry_fhir_catalog_identity_transaction_required"):
        await operation(connection, (_input(),), approved_revision=revision, control_schema="registry_example")
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"organization_id": "Organization/foreign"},
        {"endpoint_id": " padded"},
        {"evidence_id": "x" * 513},
        {"source_aliases": ()},
        {"source_aliases": ("source-b", "source-a")},
        {"source_aliases": ("source-a", "source-a")},
        {"source_aliases": (" padded",)},
        {"source_aliases": ("x" * 129,)},
        {"legacy_uuid": "00000000-0000-0000-0000-000000000000", "legacy_scope": "old-scope"},
        {"legacy_uuid": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", "legacy_scope": "old-scope"},
        {"legacy_uuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
        {"legacy_scope": "old-scope"},
    ],
)
async def test_invalid_source_identity_aliases_or_legacy_pair_refuse_before_temp_copy(changes):
    connection = _connection()
    with pytest.raises(ValueError, match="registry_fhir_catalog_identity_"):
        await identity.allocate_fhir_catalog_network_ids(
            connection, (_input(**changes),), approved_revision=0, control_schema="registry_example"
        )
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_valid_identity_rows_still_respect_complete_batch_byte_budget():
    connection = _connection()
    aliases = tuple(f"alias-{index:03d}-" + "x" * 118 for index in range(100))
    original = _input(source_aliases=aliases)
    rows = tuple(replace(original, organization_id=f"network-{index}") for index in range(identity.MAX_ROWS))
    with pytest.raises(ValueError, match="registry_fhir_catalog_identity_bounds"):
        await identity.allocate_fhir_catalog_network_ids(
            connection, rows, approved_revision=0, control_schema="registry_example"
        )
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_alias_conflict_after_append_cannot_return_resolved_identity_rows():
    connection = _connection()
    connection.fetchval.side_effect = [0, True]
    with pytest.raises(ValueError, match="registry_fhir_catalog_identity_conflict"):
        await identity.allocate_fhir_catalog_network_ids(
            connection, (_input(),), approved_revision=0, control_schema="registry_example"
        )
    connection.copy_records_to_table.assert_awaited_once()
    assert connection.fetchval.await_count == 2
    connection.fetch.assert_not_awaited()
