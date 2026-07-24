from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_manifest_publish as manifest


def _graph_entry(artifact_name: str, *, shard: str = "shard") -> dict[str, str]:
    return {
        "name": artifact_name,
        "source_shard_id": shard,
        "storage_uri": "db://ptg2_artifact/id",
    }


def test_sidecar_merge_preserves_every_list_envelope() -> None:
    """Merge absent, list, and legacy scalar sidecars without dropping entries."""

    assert manifest._merge_ptg2_manifest_sidecar_artifacts(None, {}) == {}
    merged_by_name = manifest._merge_ptg2_manifest_sidecar_artifacts(
        {"sidecars": "legacy"},
        {"sidecars": [{"name": "one"}]},
        {"sidecars": [{"name": "two"}]},
        {"direct": {"name": "direct"}},
    )
    assert merged_by_name["sidecars"] == [
        "legacy",
        {"name": "one"},
        {"name": "two"},
    ]
    assert merged_by_name["direct"] == {"name": "direct"}
    assert manifest._merge_ptg2_manifest_sidecar_artifacts(
        {"sidecars": [{"name": "one"}]}
    )["sidecars"] == [{"name": "one"}]


def test_graph_shard_contract_rejects_missing_duplicate_and_partial() -> None:
    """Require one complete set of graph directions per identified source shard."""

    observed_names_by_shard: dict[str, set[str]] = {}
    with pytest.raises(RuntimeError, match="lacks source shard"):
        manifest._record_v3_graph_shard(
            observed_names_by_shard,
            {},
            "provider_forward",
        )
    manifest._record_v3_graph_shard(
        observed_names_by_shard,
        {"source_shard_id": "shard"},
        "provider_forward",
    )
    with pytest.raises(RuntimeError, match="duplicate"):
        manifest._record_v3_graph_shard(
            observed_names_by_shard,
            {"source_shard_id": "shard"},
            "provider_forward",
        )
    with pytest.raises(RuntimeError, match="no source shards"):
        manifest._require_complete_v3_graph_shards({})
    with pytest.raises(RuntimeError, match="incomplete"):
        manifest._require_complete_v3_graph_shards(observed_names_by_shard)
    manifest._require_complete_v3_graph_shards(
        {"shard": set(manifest._PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES)}
    )


def test_v3_graph_retention_keeps_only_complete_graph_artifacts() -> None:
    """Retain direct and listed graph directions while omitting unrelated sidecars."""

    graph_names = sorted(manifest._PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES)
    artifacts_by_name = {
        "sidecars": [
            _graph_entry(graph_names[0]),
            _graph_entry(graph_names[1]),
            {"name": "price_forward"},
            "invalid",
        ],
        graph_names[2]: _graph_entry(graph_names[2]),
        graph_names[3]: _graph_entry(graph_names[3]),
        "unrelated": {"name": "unrelated"},
    }
    retained_by_name = manifest._retain_v3_provider_graph_artifacts(
        artifacts_by_name
    )
    retained_names = {
        manifest._ptg2_sidecar_entry_name("", entry)
        for entry in retained_by_name["sidecars"]
    }
    retained_names.update(
        name for name in retained_by_name if name != "sidecars"
    )
    assert retained_names == set(graph_names)
    assert manifest._ptg2_sidecar_entry_name(
        "fallback",
        {"kind": "kind"},
    ) == "kind"
    assert manifest._ptg2_sidecar_entry_name("fallback", "raw") == "fallback"


def test_price_forward_and_serving_binary_helpers_cover_all_shapes() -> None:
    """Remove redundant forward indexes and recognize V2/V3 price atom evidence."""

    retained_by_name = manifest._omit_price_forward_artifacts(
        {
            "sidecars": [
                {"name": "price_forward"},
                {"name": "provider_inverted"},
                [("opaque", True)],
            ],
            "direct": {"kind": "price_forward"},
            "other": {"name": "other"},
        }
    )
    assert retained_by_name == {
        "sidecars": [{"name": "provider_inverted"}, {"opaque": True}],
        "other": {"name": "other"},
    }
    assert manifest._omit_price_forward_artifacts(
        {"sidecars": [{"name": "price_forward"}]}
    ) == {}

    assert not manifest._has_serving_binary_price_atoms(None)
    v3_manifest_by_field = {
        "arch_version": manifest.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3,
        "price_set_atom_memberships_v3": {},
        "price_atoms_v3": {},
    }
    assert manifest._has_serving_binary_price_atoms(v3_manifest_by_field)
    assert not manifest._has_serving_binary_price_atoms(
        {"arch_version": "v2", "price_set_atoms": "invalid"}
    )
    assert manifest._has_serving_binary_price_atoms(
        {"arch_version": "v2", "price_set_atoms": {"price_set_count": "1"}}
    )
    assert manifest._serving_binary_atom_key_bits(None) is None
    assert manifest._serving_binary_atom_key_bits({"dense_atom_keys": "bad"}) is None
    assert manifest._serving_binary_atom_key_bits(
        {"dense_atom_keys": {"atom_key_bits": "bad"}}
    ) is None
    assert manifest._serving_binary_atom_key_bits(
        {"dense_atom_keys": {"atom_key_bits": "32"}}
    ) == 32


@pytest.mark.parametrize(
    ("raw_sidecars", "expected_sidecars"),
    (
        ([{"name": "one"}], {"sidecars": [{"name": "one"}]}),
        ({"one": {"name": "one"}}, {"one": {"name": "one"}}),
        ("opaque", {"sidecars": ["opaque"]}),
        (None, {}),
    ),
)
def test_base_artifact_split_normalizes_sidecar_envelopes(
    raw_sidecars,
    expected_sidecars,
) -> None:
    """Separate base artifacts from all supported sidecar envelope shapes."""

    base_by_name, sidecars_by_name = manifest._split_ptg2_manifest_base_artifacts(
        {"manifest_uri": "file://manifest", "sidecars": raw_sidecars}
    )
    assert base_by_name == {"manifest_uri": "file://manifest"}
    assert sidecars_by_name == expected_sidecars


def test_network_and_constant_helpers_distinguish_missing_evidence() -> None:
    """Normalize network names without inventing absent manifest constants."""

    assert manifest._coerce_network_names(None) == []
    assert manifest._coerce_network_names(["one", "", None, "two"]) == [
        "one",
        "two",
    ]
    assert manifest._coerce_network_names("") == []
    assert manifest._coerce_network_names("one") == ["one"]
    assert manifest._manifest_constants_from_artifacts(None) is None
    assert manifest._manifest_constants_from_artifacts(
        {"source_trace_set_hash": "", "network_names": []}
    ) is None
    assert manifest._manifest_constants_from_artifacts(
        {"source_trace_set_hash": "hash", "network_names": "network"}
    ) == {
        "source_trace_set_hash": "hash",
        "network_names": ["network"],
    }


@pytest.mark.asyncio
async def test_direct_lean_dictionaries_use_stage_or_serving_source(
    monkeypatch,
) -> None:
    """Prefer scanner dictionaries when present and otherwise scan serving rows."""

    status = AsyncMock()
    table_exists = AsyncMock(side_effect=(True, False, True, False))
    monkeypatch.setattr(manifest.db, "status", status)
    monkeypatch.setattr(manifest, "_table_exists", table_exists)
    for stage_table in ("code_stage", "code_stage"):
        await manifest._build_direct_lean_code_counts(
            schema_name="mrf",
            final_table="serving",
            code_count_table="codes",
            storage_mode="",
            code_count_stage_table=stage_table,
        )
    for stage_table in ("provider_stage", "provider_stage"):
        await manifest._build_direct_lean_provider_sets(
            schema_name="mrf",
            final_table="serving",
            provider_set_dictionary_table="providers",
            storage_mode="",
            provider_set_dictionary_stage_table=stage_table,
        )
    statements = "\n".join(call.args[0] for call in status.await_args_list)
    assert "SUM(rate_count)" in statements
    assert "COUNT(*)::bigint" in statements
    assert "FROM \"mrf\".\"provider_stage\"" in statements
    assert "FROM \"mrf\".\"serving\"" in statements


@pytest.mark.asyncio
async def test_direct_lean_index_can_skip_serving_lookup(monkeypatch) -> None:
    """Always index dictionaries while making the serving lookup optional."""

    status = AsyncMock()
    monkeypatch.setattr(manifest.db, "status", status)
    for should_create_lookup in (True, False):
        await manifest._index_direct_lean_tables(
            schema_name="mrf",
            final_table="serving",
            code_count_table="codes",
            provider_set_dictionary_table="providers",
            create_serving_lookup_index=should_create_lookup,
        )
    statements = "\n".join(call.args[0] for call in status.await_args_list)
    assert statements.count("lean_code_lookup_idx") == 1
    assert statements.count("lean_code_idx") == 2


@pytest.mark.asyncio
async def test_direct_lean_rewrite_covers_parallel_and_metadata_only(
    monkeypatch,
) -> None:
    """Support parallel materialization and serial metadata-only publication."""

    build_codes = AsyncMock()
    build_providers = AsyncMock()
    swap_stage = AsyncMock()
    index_tables = AsyncMock()
    monkeypatch.setattr(manifest, "_build_direct_lean_code_counts", build_codes)
    monkeypatch.setattr(manifest, "_build_direct_lean_provider_sets", build_providers)
    monkeypatch.setattr(manifest, "_swap_direct_lean_stage", swap_stage)
    monkeypatch.setattr(manifest, "_index_direct_lean_tables", index_tables)
    monkeypatch.setattr(manifest, "_use_parallel_dictionary_rewrite", lambda: True)
    materialized_by_field = await manifest._rewrite_direct_lean_manifest_stage(
        schema_name="mrf",
        final_table="serving",
        code_count_table="codes",
        provider_set_dictionary_table="providers",
        constants={"network_names": ["network"]},
    )
    assert materialized_by_field["serving_stage_layout"] == "lean_source_v1"
    swap_stage.assert_awaited_once()

    monkeypatch.setattr(manifest, "_use_parallel_dictionary_rewrite", lambda: False)
    metadata_only_by_field = await manifest._rewrite_direct_lean_manifest_stage(
        schema_name="mrf",
        final_table="serving",
        code_count_table="codes",
        provider_set_dictionary_table="providers",
        constants=None,
        code_count_stage_table="code_stage",
        materialize_serving_table=False,
    )
    assert metadata_only_by_field["serving_stage_layout"] == "natural_lean_source_v1"
    assert metadata_only_by_field["dictionary_source"] == "scanner_support"
    assert index_tables.await_args.kwargs["create_serving_lookup_index"] is False
