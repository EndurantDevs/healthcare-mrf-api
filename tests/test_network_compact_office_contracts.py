# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Office input and snapshot guards confer no native source authority."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_compact_offices as compact
from process import network_custom_office_rows as custom
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_copy import MembershipCopyTarget


@pytest.fixture
def office_inputs():
    candidate = uuid4()
    target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(candidate), "network_candidate_" + candidate.hex
    )
    return target, PinnedAddressSource("retained_example", "address_heap", "a" * 64)


@pytest.mark.parametrize("field", ["base", "custom", "physical_base", "carriers"])
def test_office_sources_require_exact_supported_component_types(office_inputs, field):
    _target, base = office_inputs
    options_by_name = {"base": base, field: object()}
    with pytest.raises(compact.CompactOfficeSourceError):
        compact.CompactOfficeSources(**options_by_name)


def test_physical_alias_must_remain_in_original_source_namespace(office_inputs):
    _target, base = office_inputs
    sources = compact.CompactOfficeSources(
        base, physical_base=CompactAddressRelation("other_source", "address_heap", 11)
    )
    with pytest.raises(compact.CompactOfficeSourceError, match="namespace differs"):
        compact.compact_office_relation_sql(sources)
    with pytest.raises(compact.CompactOfficeSourceError, match="Exact compact"):
        compact.compact_office_relation_sql(object())


@pytest.mark.asyncio
async def test_compact_pin_requires_transaction_and_external_retained_schema(office_inputs):
    target, base = office_inputs
    connection = SimpleNamespace(is_in_transaction=lambda: False, execute=AsyncMock())
    with pytest.raises(compact.CompactOfficeSourceError, match="transaction"):
        await compact.pin_compact_office_sources(connection, target, compact.CompactOfficeSources(base))
    connection.is_in_transaction = lambda: True
    same_candidate = compact.CompactOfficeSources(replace(base, schema_name=target.schema_name))
    with pytest.raises(compact.CompactOfficeSourceError, match="outside the candidate"):
        await compact.pin_compact_office_sources(connection, target, same_candidate)
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_compact_physical_oid_drift_refuses_before_column_access(monkeypatch, office_inputs):
    target, base = office_inputs
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, execute=AsyncMock(), fetchval=AsyncMock(return_value=12)
    )
    sources = compact.CompactOfficeSources(base, physical_base=CompactAddressRelation(base.schema_name, "old_heap", 11))
    monkeypatch.setattr(compact, "_projection_columns", AsyncMock())
    with pytest.raises(compact.CompactOfficeSourceError, match="identity changed"):
        await compact.pin_compact_office_sources(connection, target, sources)
    compact._projection_columns.assert_not_awaited()
    assert "LOCK TABLE ONLY" in connection.execute.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage,approved_revision",
    [("candidate-type", 1), ("base-type", 1), ("revision", True), ("npi-table", 1), ("source-inside-candidate", 1)],
)
async def test_direct_custom_input_refusals_precede_native_preparation(
    monkeypatch, office_inputs, damage, approved_revision
):
    target, base = office_inputs
    npi_source = None
    if damage == "candidate-type":
        target = object()
    elif damage == "base-type":
        base = object()
    elif damage == "npi-table":
        npi_source = PinnedAddressSource("retained_example", "not_npi", "b" * 64)
    elif damage == "source-inside-candidate":
        base = replace(base, schema_name=target.schema_name)
    monkeypatch.setattr(custom, "_require_transaction", AsyncMock(side_effect=AssertionError("Native stage reached")))
    with pytest.raises(custom.NetworkCustomAddressSourceError):
        await custom.prepare_custom_office_rows(
            object(),
            target,
            base,
            approved_revision=approved_revision,
            runtime_roles=("reader_example",),
            npi_source=npi_source,
        )
    custom._require_transaction.assert_not_awaited()
