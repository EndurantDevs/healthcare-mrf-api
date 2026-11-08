# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Graph metadata caches distinguish authenticated interchangeable families."""

import json
from collections import OrderedDict
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_v4_graph as graph
from process.ptg_parts.ptg2_shared_blocks import SharedBlock
from process.ptg_parts.ptg2_v4_snapshot_maps import encode_v4_snapshot_map_pack
from tests.test_ptg2_physical_binding import _binding
from tests.test_ptg2_v4_graph import _heavy_owner_row


def _map_row(ordinal):
    """Use genuine encoded coordinates and physical-block authentication."""
    member = SharedBlock("v4_npi_groups_exact_members_v1", 0, 0, 1, "none", 1, bytes([ordinal]))
    payload = encode_v4_snapshot_map_pack(member.object_kind, (member.reference(),))
    block = SharedBlock(graph.PTG2_V4_MAP_BLOCK_KIND, 0, 0, 1, "none", len(payload), payload)
    return {
        "pack_no": 0,
        "coordinate_count": 1,
        "block_hash": block.block_hash,
        "object_kind": block.object_kind,
        "format_version": 2,
        "codec": "none",
        "raw_byte_count": len(payload),
        "stored_byte_count": len(payload),
        "block_entry_count": 1,
        "payload": payload,
    }


def _session(binding, catalog_sha256, ordinal):
    """Model only the cache context handed off after the native read proof."""
    root_by_field = {
        "snapshot_key": binding.payload_snapshot_key,
        "representation": "direct_v1",
        "format_version": graph.PTG2_V4_MAP_FORMAT_VERSION,
        "map_format": graph.PTG2_V4_MAP_FORMAT,
        "projection_id_scope": graph.PTG2_V4_PROJECTION_ID_SCOPE,
        "map_digest": bytes([ordinal]) * 32,
    }
    manifest_by_field = {
        "snapshot_key": binding.payload_snapshot_key,
        "relation": "npi_groups_exact",
        "member_object_kind": "v4_npi_groups_exact_members_v1",
        "locator_object_kind": "v4_npi_groups_exact_locators_v1",
        "owner_base": 0,
        "owner_count": 8,
        "logical_member_count": ordinal,
        "vector_member_count": ordinal,
        "member_width": 4,
        "member_page_bytes": 16,
        "locator_page_bytes": 24,
        "locator_owner_span": 2,
    }
    return SimpleNamespace(
        info={
            "ptg2_local_read_bindings": {binding.schema_name: binding},
            "ptg2_local_read_catalog_sha256": {binding.schema_name: catalog_sha256},
        },
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(first=lambda: root_by_field),
                SimpleNamespace(first=lambda: manifest_by_field),
                [_map_row(ordinal)],
                [dict(_heavy_owner_row(7, member_base=ordinal), snapshot_key=binding.payload_snapshot_key)],
            ]
        ),
    )


async def _metadata(session, binding):
    """Exercise every graph cache whose identity previously depended on names."""
    schema_name, snapshot_key = binding.schema_name, binding.payload_snapshot_key
    root = await graph.load_v4_graph_root(session, snapshot_key, schema_name=schema_name)
    manifest = await graph.load_v4_relation_manifest(
        session,
        snapshot_key=snapshot_key,
        relation="npi_groups_exact",
        schema_name=schema_name,
    )
    coordinates_by_pair = await graph._load_map_coordinate_pairs(
        session,
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        object_kind="v4_npi_groups_exact_members_v1",
        coordinate_pairs=((0, 0),),
    )
    owners_by_key = await graph.load_v4_heavy_owners(
        session,
        snapshot_key=snapshot_key,
        relation="npi_groups_exact",
        owner_keys=(7,),
        schema_name=schema_name,
    )
    return (
        root.map_digest,
        manifest.logical_member_count,
        coordinates_by_pair[(0, 0)].block_hash,
        owners_by_key[7].member_base,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["schema", "heap", "sequence", "catalog", "destination", "owner"])
async def test_graph_caches_reuse_only_the_exact_native_family(monkeypatch, changed):
    for cache_name in ("_ROOT_CACHE", "_RELATION_CACHE", "_HEAVY_OWNER_CACHE", "_HEAVY_OWNER_NEGATIVE_CACHE"):
        monkeypatch.setattr(graph, cache_name, OrderedDict())
    monkeypatch.setattr(graph, "_MAP_COORDINATE_CACHE", graph._ByteLRU(4096))
    first = _binding()
    changes_by_name = {
        "schema": {"schema_oid": 1001},
        "heap": {"relation_oids": ((first.relation_oids[0][0], 2001), *first.relation_oids[1:])},
        "sequence": {"sequence_oids": ((first.sequence_oids[0][0], 3001, *first.sequence_oids[0][2:]),)},
        "catalog": {},
        "destination": {"snapshot_id": "another-destination"},
        "owner": {"owner_oid": 1002},
    }
    second = replace(first, **changes_by_name[changed])
    before = _session(first, "a" * 64, 1)
    after = _session(second, ("b" if changed == "catalog" else "a") * 64, 2)
    assert first.schema_name == second.schema_name and first.payload_snapshot_key == second.payload_snapshot_key
    with graph.v4_graph_request_scope():
        old_metadata = await _metadata(before, first)
        assert await _metadata(before, first) == old_metadata
        new_metadata = await _metadata(after, second)
        assert all(old != new for old, new in zip(old_metadata, new_metadata, strict=True))
        assert await _metadata(after, second) == new_metadata
    assert before.execute.await_count == after.execute.await_count == 4
    for call in after.execute.await_args_list:
        assert f'"{second.schema_name}".' in str(call.args[0])


@pytest.mark.parametrize("catalog_sha256", [None, "", "g" * 64, 7])
def test_native_cache_context_requires_complete_catalog_identity(catalog_sha256):
    binding = _binding()
    session = _session(binding, catalog_sha256, 1)
    with pytest.raises(graph.PTG2SharedBlockError, match="catalog identity"):
        graph._graph_cache_scope(session, binding.schema_name)
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("invalid_binding", [None, "wrong-schema"])
def test_native_cache_context_refuses_untyped_or_mislocated_identity(invalid_binding):
    binding = _binding()
    session = _session(binding, "a" * 64, 1)
    session.info["ptg2_local_read_bindings"][binding.schema_name] = (
        {"schema_oid": binding.schema_oid}
        if invalid_binding is None
        else replace(binding, dataset_id=type(binding.dataset_id)(int=2))
    )
    with pytest.raises(graph.PTG2SharedBlockError, match="cache identity"):
        graph._graph_cache_scope(session, binding.schema_name)


@pytest.mark.asyncio
async def test_legacy_and_detached_cache_scope_encoding_is_unchanged(monkeypatch):
    binding = _binding()
    session = _session(binding, "a" * 64, 1)
    session.info.clear()
    monkeypatch.setattr(graph, "_ROOT_CACHE", OrderedDict())
    root = await graph.load_v4_graph_root(session, binding.payload_snapshot_key, schema_name="mrf")
    assert graph._ROOT_CACHE == {("mrf", binding.payload_snapshot_key): root}
    relations_by_table = {"ptg2_v4_snapshot_map_pack": "detached_map"}
    session.info["ptg_snapshot_candidate_reads"] = {'"mrf"': relations_by_table}
    assert graph._graph_cache_scope(session, "mrf") == json.dumps(("mrf", relations_by_table), sort_keys=True)
    assert graph._graph_cache_scope(session, "other") == "other"
