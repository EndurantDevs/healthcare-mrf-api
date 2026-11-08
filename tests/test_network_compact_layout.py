# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compact coordinates are canonical shape, never a custody credential."""

from dataclasses import FrozenInstanceError, replace

import pytest

from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    verify_compact_layout_catalog,
)


def _layout():
    artifact = CompactEdgeArtifact(
        "00000000-0000-0000-0000-000000000001", "network_candidate_00000000000000000000000000000001", 3
    )
    return CompactNetworkLayout(
        CompactAddressRelation("generation_a", "network_slice_ref", 1),
        CompactAddressRelation("addresses_a", "entity_address_unified_retained_abc", 2),
        (artifact,),
        CompactAddressRelation("manual_a", "entity_address_unified", 4),
    )


def test_frozen_coordinates_accept_retained_names():
    layout = _layout()
    with pytest.raises(FrozenInstanceError):
        layout.base.table_oid = 7
    inventory_by_relation = {
        ("generation_a", "network_slice_ref"): 1,
        ("addresses_a", "entity_address_unified_retained_abc"): 2,
        (layout.artifacts[0].schema_name, "network_office_edge"): 3,
        ("manual_a", "entity_address_unified"): 4,
    }
    assert verify_compact_layout_catalog(layout, inventory_by_relation) is layout
    inventory_by_relation[("manual_a", "entity_address_unified")] = 5
    with pytest.raises(ValueError, match="physical relation"):
        verify_compact_layout_catalog(layout, inventory_by_relation)


@pytest.mark.parametrize("identifier", ["", "unsafe.name", 'unsafe"name', "a" * 64, "1table", None])
def test_rejects_invalid_identifiers(identifier):
    with pytest.raises(ValueError, match="identifier"):
        CompactAddressRelation(identifier, "entity_address_unified", 1)


@pytest.mark.parametrize("oid", [0, -1, True, "1", 4294967296])
def test_rejects_invalid_oids(oid):
    with pytest.raises(ValueError, match="OID"):
        CompactAddressRelation("addresses_a", "entity_address_unified", oid)


@pytest.mark.parametrize("damage", ["uuid", "schema", "duplicates", "order", "list", "ref_role", "oid_alias"])
def test_rejects_noncanonical_layouts(damage):
    layout = _layout()
    second = CompactEdgeArtifact(
        "00000000-0000-0000-0000-000000000002", "network_candidate_00000000000000000000000000000002", 5
    )
    with pytest.raises(ValueError):
        match damage:
            case "uuid":
                replace(layout.artifacts[0], candidate_id="00000000000000000000000000000001")
            case "schema":
                replace(layout.artifacts[0], schema_name="other_candidate")
            case "duplicates":
                replace(layout, artifacts=(layout.artifacts[0], layout.artifacts[0]))
            case "order":
                replace(layout, artifacts=(second, layout.artifacts[0]))
            case "list":
                replace(layout, artifacts=list(layout.artifacts))
            case "ref_role":
                replace(layout, ref_map=replace(layout.ref_map, table_name="other_refs"))
            case _:
                replace(layout, custom=replace(layout.custom, table_oid=layout.base.table_oid))
