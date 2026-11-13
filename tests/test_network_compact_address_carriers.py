# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed compact carrier metadata; source hashes never replace native verification."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_compact_address_carriers as carriers
from process.network_address_projection import PinnedAddressSource
from process.network_compact_address_carriers import (
    CompactAddressCarrierError,
    CompactAddressCarrierReceipt,
    parse_compact_address_carrier_receipt,
)
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_copy import MembershipCopyTarget


def _receipt():
    return CompactAddressCarrierReceipt(
        MembershipCopyTarget(*(str(UUID(int=i)) for i in range(1, 5)), "network_candidate_" + UUID(int=4).hex),
        "control_fixture",
        1,
        "a" * 64,
        PinnedAddressSource("base_fixture", "entity_address_unified", "current-base"),
        "b" * 64,
        "c" * 64,
        "d" * 64,
        "e" * 64,
        None,
        ((1, "f" * 64, 2, "a" * 64),),
        2,
        2,
        1,
        CompactAddressRelation("network_candidate_" + UUID(int=4).hex, "entity_address_unified"),
        CompactAddressRelation("network_candidate_" + UUID(int=4).hex, "provider_location_binding"),
    )


def test_carrier_receipt_roundtrip_is_closed_and_does_not_contain_office_vectors():
    receipt = _receipt()
    assert parse_compact_address_carrier_receipt(receipt.as_dict()) == receipt
    assert all("records" not in name for name in receipt.as_dict())


@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", True),
        ("revision", 3),
        ("approved_revision", -1),
        ("approved_generation", "A" * 64),
        ("materialized_address_rows", 3),
        ("selected_binding_rows", True),
        ("address_relation", None),
        ("binding_relation", {"schema_name": "foreign", "table_name": "provider_location_binding"}),
        ("control_schema", "bad.schema"),
        ("origin_references", [[1, "f" * 64, 0, "a" * 64]]),
        ("origin_references", [[1, "f" * 64, 2, "a" * 64]] * 101),
        ("unexpected", True),
    ],
)
def test_carrier_receipt_refuses_open_shapes_or_invalid_native_coordinates(field, value):
    document = deepcopy(_receipt().as_dict())
    document[field] = value
    with pytest.raises(CompactAddressCarrierError):
        parse_compact_address_carrier_receipt(document)


def test_two_source_streams_must_be_deduplicated_into_one_native_origin():
    document = _receipt().as_dict()
    document["origin_references"].append([1, "f" * 64, 3, "b" * 64])
    with pytest.raises(CompactAddressCarrierError):
        parse_compact_address_carrier_receipt(document)


def test_legacy_carrier_receipt_projects_names_and_logical_origin_scope():
    document = _receipt().as_dict()
    document.pop("address_relation")
    document.pop("binding_relation")
    document.update(revision=1, runtime_roles=["reader"], address_table_oid=987, binding_table_oid=654)
    document["origin_references"] = [[1, "f" * 64, 111, 222, 333, 444, 2, "a" * 64]]
    interpreted = parse_compact_address_carrier_receipt(document)
    assert interpreted.address_relation == _receipt().address_relation
    assert interpreted.binding_relation == _receipt().binding_relation
    assert interpreted.origin_references == _receipt().origin_references
    assert interpreted.source_recipes_sha256 == document["source_recipes_sha256"]
    assert parse_compact_address_carrier_receipt(interpreted.as_dict()) == interpreted


@pytest.mark.parametrize("failure", [None, "query", "cancellation"])
async def test_carrier_scratch_uses_transaction_cleanup_on_success_and_failure(failure):
    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=True), transaction=lambda: transaction, execute=AsyncMock()
    )
    relation = CompactAddressRelation("base_fixture", "entity_address_unified")

    async def run():
        async with carriers._scratch(connection, relation):
            if failure == "query":
                raise RuntimeError("query failed")
            if failure == "cancellation":
                raise asyncio.CancelledError()

    if failure:
        with pytest.raises(RuntimeError if failure == "query" else asyncio.CancelledError):
            await run()
        transaction.rollback.assert_awaited_once()
        transaction.commit.assert_not_awaited()
    else:
        await run()
        transaction.commit.assert_awaited_once()
        transaction.rollback.assert_not_awaited()
        assert connection.execute.await_args.args == ('DROP TABLE pg_temp."entity_address_unified"',)
    transaction.start.assert_awaited_once()


async def test_carrier_scratch_refuses_existing_temporary_names():
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False), transaction=AsyncMock())
    with pytest.raises(CompactAddressCarrierError, match="scratch_names_in_use"):
        async with carriers._scratch(connection, _receipt().address_relation):
            pytest.fail("occupied scratch names entered")
    connection.transaction.assert_not_called()


def test_custom_receipt_roundtrip_and_legacy_named_projection():
    from process.network_compact_custom_offices import CompactCustomOfficeReceipt, parse_compact_custom_office_receipt

    carrier = _receipt()
    receipt = CompactCustomOfficeReceipt(
        carrier.target,
        carrier.control_schema,
        1,
        carrier.base_source,
        "b" * 64,
        None,
        "c" * 64,
        1,
        carrier.address_relation,
        carrier.binding_relation,
    )
    assert parse_compact_custom_office_receipt(receipt.as_dict()) == receipt
    saved = receipt.as_dict()
    saved.pop("address_relation")
    saved.pop("binding_relation")
    saved.update(revision=1, runtime_roles=["reader"], npi_identity=None, address_table_oid=111, binding_table_oid=222)
    projected = parse_compact_custom_office_receipt(saved)
    assert projected.target == receipt.target and projected.binding_relation == receipt.binding_relation
    assert projected.approved_records_sha256 == receipt.approved_records_sha256
    assert parse_compact_custom_office_receipt(projected.as_dict()) == projected


@pytest.mark.parametrize(
    "field,value",
    [
        ("approved_records_sha256", "invalid"),
        ("custom_pair_count", -1),
        ("address_relation", None),
        ("binding_relation", {"schema_name": "foreign", "table_name": "provider_location_binding"}),
        ("unexpected", True),
    ],
)
def test_custom_receipt_keeps_data_accounting_and_qualified_scope_validation(field, value):
    from process.network_compact_custom_offices import CompactCustomOfficeReceipt, parse_compact_custom_office_receipt

    carrier = _receipt()
    receipt = CompactCustomOfficeReceipt(
        carrier.target,
        carrier.control_schema,
        1,
        carrier.base_source,
        "b" * 64,
        None,
        "c" * 64,
        1,
        carrier.address_relation,
        carrier.binding_relation,
    )
    document = receipt.as_dict()
    document[field] = value
    with pytest.raises(ValueError):
        parse_compact_custom_office_receipt(document)


def test_carrier_rederivation_preserves_legacy_keys_and_requires_current_data_counts():
    from dataclasses import replace

    current = _receipt()
    legacy = replace(
        current,
        revision=1,
        approved_site_sha256="1" * 64,
        ptg_carry_sha256="2" * 64,
        origin_references=((1, "f" * 64, 2, "3" * 64),),
    )
    assert carriers._has_matching_receipt_data(current, legacy)
    assert not carriers._has_matching_receipt_data(replace(current, selected_address_rows=3), legacy)
    assert not carriers._has_matching_receipt_data(replace(current, approved_generation="4" * 64), legacy)
    assert not carriers._has_matching_receipt_data(
        replace(current, origin_references=((1, "e" * 64, 2, "a" * 64),)), legacy
    )
    assert not carriers._has_matching_receipt_data(replace(current, approved_site_sha256="1" * 64), current)


async def test_carrier_scratch_rolls_back_creation_errors():
    transaction = SimpleNamespace(start=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock())
    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=True),
        transaction=lambda: transaction,
        execute=AsyncMock(side_effect=RuntimeError("creation failed")),
    )
    with pytest.raises(RuntimeError, match="creation failed"):
        async with carriers._scratch(connection, _receipt().address_relation):
            pytest.fail("failed creation entered")
    transaction.rollback.assert_awaited_once()
    transaction.commit.assert_not_awaited()
