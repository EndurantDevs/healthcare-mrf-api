# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed catalog and exact-column contracts without resource access."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_publication_permissions as permissions


def connection():
    """Supply a complete protected catalog result and no native side effects."""
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(
            return_value={
                "closed": True,
                "table_oid": 10,
                "schema_oid": 11,
                "owner_oid": 12,
                "runtime_oid": 13,
                "owner_role": "metadata_owner",
            }
        ),
        fetchval=AsyncMock(return_value=True),
        fetch=AsyncMock(
            return_value=[
                {"attname": name, "allowed": name in permissions.LIFECYCLE_COLUMNS} for name in permissions._COLUMNS
            ]
        ),
    )


@pytest.mark.asyncio
async def test_native_owner_binding():
    native = connection()
    receipt = await permissions.verify_registry_publication_lifecycle_permissions(
        native, control_schema="control", physical_owner_role="candidate_owner"
    )
    assert receipt == {"owner_role": "metadata_owner", "table_oid": 10}
    assert native.fetchrow.await_args.args[1:] == (
        '"control".registry_publication_request',
        None,
        None,
        "candidate_owner",
    )
    assert native.fetchval.await_args.args[1:] == (
        10,
        11,
        12,
        13,
        sorted(permissions.LIFECYCLE_COLUMNS),
        sorted(permissions.INSERT_COLUMNS),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("catalog", [None, {"closed": False}, {"closed": None}])
async def test_native_catalog_required(catalog):
    native = connection()
    native.fetchrow.return_value = catalog
    with pytest.raises(ValueError, match="queue_permissions_unavailable"):
        await permissions.verify_registry_publication_lifecycle_permissions(native)
    native.fetchval.assert_not_awaited()
    native.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["audit_update", "missing_update", "missing_column", "extra_column", "global_acl"])
async def test_native_columns_refused(mutation):
    native = connection()
    columns = native.fetch.return_value
    if mutation == "global_acl":
        native.fetchval.return_value = False
    elif mutation == "extra_column":
        columns.append({"attname": "unexpected", "allowed": False})
    elif mutation == "missing_column":
        columns.remove(next(column for column in columns if column["attname"] == "created_at"))
    else:
        name = "command_json" if mutation == "audit_update" else "state"
        next(column for column in columns if column["attname"] == name)["allowed"] = mutation == "audit_update"
    with pytest.raises(ValueError, match="queue_permissions_unavailable"):
        await permissions.verify_registry_publication_lifecycle_permissions(native)


@pytest.mark.asyncio
async def test_queue_context_required():
    native = connection()
    native.is_in_transaction = lambda: False
    with pytest.raises(ValueError, match="queue_permissions_unavailable"):
        await permissions.verify_registry_publication_lifecycle_permissions(native)
    with pytest.raises(ValueError):
        await permissions.verify_registry_publication_lifecycle_permissions(native, role="bad-role")
    native.fetchrow.assert_not_awaited()
