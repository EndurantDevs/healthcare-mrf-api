# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed compact carrier metadata; source hashes never replace native verification."""

from copy import deepcopy
from uuid import UUID

import pytest

from process.network_address_projection import PinnedAddressSource
from process.network_compact_address_carriers import (
    CompactAddressCarrierError,
    CompactAddressCarrierReceipt,
    parse_compact_address_carrier_receipt,
)
from process.network_membership_copy import MembershipCopyTarget


def _receipt():
    return CompactAddressCarrierReceipt(
        MembershipCopyTarget(*(str(UUID(int=i)) for i in range(1, 5)), "network_candidate_" + UUID(int=4).hex),
        "control_fixture",
        1,
        "a" * 64,
        PinnedAddressSource("base_fixture", "entity_address_unified", "current-base"),
        "b" * 64,
        ("reader_fixture",),
        "c" * 64,
        "d" * 64,
        "e" * 64,
        None,
        ((1, "f" * 64, 1, 2, 3, 4, 2, "a" * 64),),
        2,
        2,
        1,
        5,
        6,
    )


def test_carrier_receipt_roundtrip_is_closed_and_does_not_contain_office_vectors():
    receipt = _receipt()
    assert parse_compact_address_carrier_receipt(receipt.as_dict()) == receipt
    assert all("records" not in name for name in receipt.as_dict())


@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", True),
        ("revision", 2),
        ("approved_revision", -1),
        ("approved_generation", "A" * 64),
        ("materialized_address_rows", 3),
        ("selected_binding_rows", True),
        ("address_table_oid", None),
        ("binding_table_oid", 0),
        ("runtime_roles", ["reader_fixture"] * 2),
        ("control_schema", "bad.schema"),
        ("origin_references", [[1, "f" * 64, 1, 2, 3, 4, 0, "a" * 64]]),
        ("origin_references", [[1, "f" * 64, 1, 2, 3, 4, 2, "a" * 64]] * 101),
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
    document["origin_references"].append([1, "f" * 64, 1, 2, 3, 4, 3, "b" * 64])
    with pytest.raises(CompactAddressCarrierError):
        parse_compact_address_carrier_receipt(document)
