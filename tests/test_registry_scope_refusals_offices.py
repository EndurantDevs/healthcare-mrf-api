# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Office selections cannot borrow another namespace, client or source approval."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_office_selection as offices
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
from tests.test_registry_ptg_office_selection import _pair


class _OfficeReader:
    """External catalog replies only; real permission-query and receipt code still run."""

    def __init__(self, receipts):
        self.receipts = receipts
        self.calls = []
        self.active = True
        self.isolation = "repeatable read"
        self.permission_results = iter((True, True))

    def is_in_transaction(self):
        """Expose the modeled caller's transaction without creating a database."""
        return self.active

    async def fetchval(self, statement, *arguments):
        """Refuse unrecognized SQL instead of granting arbitrary catalog queries."""
        self.calls.append((statement, arguments))
        if statement == "SHOW transaction_isolation":
            return self.isolation
        assert "pg_namespace" in statement
        return next(self.permission_results)

    async def fetch(self, statement, *arguments):
        """Return only one explicitly supplied approval/hold response page."""
        self.calls.append((statement, arguments))
        assert "registry_ptg_office_approval" in statement
        return self.receipts


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["namespace", "transaction", "isolation", "approval_acl", "hold_acl"])
async def test_office_authority_refuses_before_reading_any_receipt(failure):
    binding_by_field, receipt_by_name, _ = _pair()
    reader = _OfficeReader([receipt_by_name])
    store = RegistryPTGProducerScopeStore("office_owner", "office_approver", "office_control")
    namespace = '"foreign_control"' if failure == "namespace" else '"office_control"'
    if failure == "transaction":
        reader.active = False
    elif failure == "isolation":
        reader.isolation = "read committed"
    elif failure == "approval_acl":
        reader.permission_results = iter((False,))
    elif failure == "hold_acl":
        reader.permission_results = iter((True, False))
    with pytest.raises(ValueError, match="store_unprotected|transaction_required"):
        await offices._read_references(reader, namespace, [binding_by_field], store)
    assert all("WITH ORDINALITY" not in statement for statement, _ in reader.calls)
    if failure == "namespace":
        assert reader.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("returned_ordinals", [[], [2], [1, 1]])
async def test_office_receipt_page_cannot_shift_binding_positions(returned_ordinals):
    binding_by_field, receipt_by_name, _ = _pair()
    reader = _OfficeReader([{**receipt_by_name, "ordinal": ordinal} for ordinal in returned_ordinals])
    store = RegistryPTGProducerScopeStore("office_owner", "office_approver", "office_control")
    with pytest.raises(ValueError, match="registry_office_selection_changed"):
        await offices._read_references(reader, '"office_control"', [binding_by_field], store)
    assert "WITH ORDINALITY" in reader.calls[-1][0]
    assert json.loads(reader.calls[-1][1][0]) == [binding_by_field]


@pytest.mark.asyncio
@pytest.mark.parametrize("claim", ["client", "producer"])
async def test_selected_recipe_cannot_borrow_another_approved_identity(monkeypatch, claim):
    binding_by_field, receipt_by_name, original = _pair()
    if claim == "client":
        requested = replace(original, source_pin=replace(original.source_pin, client_id="another-client"))
    else:
        coordinates = replace(original.binding_coordinates, producer_id="another-producer")
        requested = replace(
            original, source_pin=replace(original.source_pin, coordinates=coordinates), binding_coordinates=coordinates
        )
    reader = _OfficeReader([receipt_by_name])
    native_fetch = reader.fetch

    async def approved_rows(statement, *arguments):
        if "registry_approved_record" in statement:
            reader.calls.append((statement, arguments))
            return [{"document": json.dumps(binding_by_field)}]
        return await native_fetch(statement, *arguments)

    reader.fetch = approved_rows
    approved = SimpleNamespace(approved_revision=9)
    monkeypatch.setattr(offices, "pin_approved_membership_source", AsyncMock(return_value=approved))
    store = RegistryPTGProducerScopeStore("office_owner", "office_approver", "office_control")
    with pytest.raises(ValueError, match="registry_office_selection_ambiguous"):
        await offices.require_selected_ptg_office_recipes(
            reader, (requested,), approved, control_schema="office_control", office_authority=(None, store, None)
        )
    assert offices._paired_recipe(receipt_by_name, binding_by_field) == original
    assert requested != original
