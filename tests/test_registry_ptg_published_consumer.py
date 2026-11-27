# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure provisioning bounds; native authority evidence lives in the selected Postgres module."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_published_provisioning as provisioning
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError


def test_operator_plan_grants_only_constant_lock_and_original_pin_insert_columns():
    statements = provisioning.published_lock_provisioning_statements("synthetic_ptg", "scope_reader", "scope_approver")
    assert len(statements) == 13
    assert all("UPDATE(registry_ptg_read_lock)" in statement for statement in statements if "GRANT UPDATE" in statement)
    assert not any(
        "GRANT UPDATE ON" in statement or "DELETE" in statement or "TRIGGER" in statement for statement in statements
    )
    assert "INSERT(owner_type,owner_id,snapshot_id,reason,created_at)" in statements[-1]
    for invalid in [("bad-schema", "reader", "writer"), ("schema", "same", "same")]:
        with pytest.raises(ValueError):
            provisioning.published_lock_provisioning_statements(*invalid)


@pytest.mark.parametrize("mode", ["missing", "unsafe", "duplicate"])
@pytest.mark.asyncio
async def test_lock_column_check_refuses_missing_unsafe_or_duplicate_headers(mode):
    headers = [{"relname": table, "protected": True} for table in provisioning.HEADER_TABLES]
    if mode == "missing":
        headers.pop()
    if mode == "unsafe":
        headers[0]["protected"] = False
    if mode == "duplicate":
        headers.append(headers[0])
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: headers))),
    )
    with pytest.raises(RegistryPTGProducerScopeError):
        await provisioning.require_published_lock_privileges(session, "synthetic_ptg")


def test_lock_column_check_reads_shape_without_a_privilege_census():
    assert "convalidated" in provisioning._LOCK_COLUMN_SQL
    assert "privilege" not in provisioning._LOCK_COLUMN_SQL
    assert "current_user" not in provisioning._LOCK_COLUMN_SQL
    assert "GRANT" not in provisioning._LOCK_COLUMN_SQL
