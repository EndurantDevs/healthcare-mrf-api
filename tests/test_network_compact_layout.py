# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published network coordinates serialize using schema-qualified names."""

from dataclasses import FrozenInstanceError, asdict, replace

import pytest

from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
)


def _layout():
    artifact = CompactEdgeArtifact(
        "00000000-0000-0000-0000-000000000001", "network_candidate_00000000000000000000000000000001"
    )
    return CompactNetworkLayout(
        CompactAddressRelation("generation_a", "network_slice_ref"),
        CompactAddressRelation("addresses_a", "entity_address_unified_retained_abc"),
        (artifact,),
        CompactAddressRelation("manual_a", "entity_address_unified"),
    )


def test_frozen_coordinates_accept_retained_names():
    layout = _layout()
    with pytest.raises(FrozenInstanceError):
        layout.base.table_name = "replacement"
    assert asdict(layout.base) == {
        "schema_name": "addresses_a",
        "table_name": "entity_address_unified_retained_abc",
    }
    assert asdict(layout.artifacts[0]) == {
        "candidate_id": "00000000-0000-0000-0000-000000000001",
        "schema_name": "network_candidate_00000000000000000000000000000001",
    }


@pytest.mark.parametrize("identifier", ["", "unsafe.name", 'unsafe"name', "a" * 64, "1table", None])
def test_rejects_invalid_identifiers(identifier):
    with pytest.raises(ValueError, match="identifier"):
        CompactAddressRelation(identifier, "entity_address_unified")


@pytest.mark.parametrize("table", ["", "unsafe.name", 'unsafe"name', "a" * 64, "1table", None])
def test_rejects_invalid_table_names(table):
    with pytest.raises(ValueError, match="identifier"):
        CompactAddressRelation("addresses_a", table)


@pytest.mark.parametrize("damage", ["uuid", "schema", "duplicates", "order", "list", "ref_role", "relation_alias"])
def test_rejects_noncanonical_layouts(damage):
    layout = _layout()
    second = CompactEdgeArtifact(
        "00000000-0000-0000-0000-000000000002", "network_candidate_00000000000000000000000000000002"
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
                replace(layout, custom=layout.base)
