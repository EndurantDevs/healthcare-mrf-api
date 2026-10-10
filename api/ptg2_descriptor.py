# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact pure construction shared by singleton and native-set readers."""

from __future__ import annotations

from typing import Any

from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def _descriptor_graph_fields(serving_index, storage_generation, source_count, *, include_tax_identity):
    from api.ptg2_tables import PTG2_V4_SHARED_GENERATION, _has_valid_v4_manifest, _v4_tax_identity_source_publication
    from process.ptg_parts.ptg2_v4_taxonomy_candidates import validate_v4_inferred_taxonomy_projection_manifest

    network_names = serving_index.get("network_names")
    provider_graph_v4_hot_prefix_by_field: dict[str, Any] | None = None
    provider_graph_v4_inferred_taxonomy_candidates: dict[str, Any] | None = None
    provider_tax_identity_source_publication = None
    if storage_generation == PTG2_V4_SHARED_GENERATION:
        serving_binary = serving_index.get("serving_binary")
        provider_graph = serving_binary.get("provider_graph_v4") if isinstance(serving_binary, dict) else None
        raw_hot_prefix = provider_graph.get("hot_prefix") if isinstance(provider_graph, dict) else None
        if not _has_valid_v4_manifest(
            raw_hot_prefix,
            representation=str(provider_graph.get("representation") or "").strip().lower(),
        ):
            raise PTG2ManifestArtifactError(
                "PTG2 V4 snapshot is missing sealed hot-prefix limits; reimport the snapshot"
            )
        provider_graph_v4_hot_prefix_by_field = dict(raw_hot_prefix)
        raw_inferred_taxonomy_candidates = (
            provider_graph.get("inferred_taxonomy_candidates") if isinstance(provider_graph, dict) else None
        )
        if raw_inferred_taxonomy_candidates is not None:
            provider_graph_v4_inferred_taxonomy_candidates = validate_v4_inferred_taxonomy_projection_manifest(
                raw_inferred_taxonomy_candidates
            )
        if include_tax_identity is True:
            provider_tax_identity_source_publication = _v4_tax_identity_source_publication(
                serving_index,
                source_count=int(source_count or 0),
            )
    return {
        "network_names": network_names,
        "hot_prefix": provider_graph_v4_hot_prefix_by_field,
        "taxonomy": provider_graph_v4_inferred_taxonomy_candidates,
        "tax_identity": provider_tax_identity_source_publication,
    }


def _descriptor_geometry_fields(serving_index, network_names):
    from api.ptg2_tables import _serving_binary_section_integer, _serving_index_atom_key_bits

    return {
        "source_trace_set_hash": str(serving_index.get("source_trace_set_hash") or "").strip() or None,
        "network_names": [str(network_name) for network_name in network_names]
        if isinstance(network_names, list)
        else None,
        "price_atom_constant_values": dict(serving_index.get("price_atom_constant_values") or {})
        if isinstance(serving_index.get("price_atom_constant_values"), dict)
        else None,
        "price_dictionary_item_count": _serving_binary_section_integer(
            serving_index, "price_dictionary", "price_set_count"
        ),
        "price_dictionary_block_bytes": _serving_binary_section_integer(
            serving_index, "price_dictionary", "block_bytes"
        ),
        "provider_shard_span": _serving_binary_section_integer(
            serving_index, "assigned_encoder", "provider_shard_span"
        ),
        "atom_key_bits": _serving_index_atom_key_bits(serving_index),
        "price_key_block_span": _serving_binary_section_integer(
            serving_index, "price_set_atom_memberships_v3", "block_span"
        ),
        "atom_key_block_span": _serving_binary_section_integer(serving_index, "price_atoms_v3", "block_span"),
    }


def _serving_tables_descriptor(
    snapshot_id,
    row_fields,
    serving_index,
    *,
    layout_by_field,
    source_by_field,
    physical_binding=None,
    include_tax_identity=False,
):
    from api.ptg2_tables import (
        PTG2_V3_ARCH_VERSION,
        PTG2_V3_SERVING_LAYOUT,
        PTG2_V3_SHARED_BLOCK_LAYOUT,
        PTG2_V4_SHARED_BLOCK_LAYOUT,
        PTG2_V4_SHARED_GENERATION,
        _database_execution_evidence,
    )

    graph_by_field = _descriptor_graph_fields(
        serving_index,
        layout_by_field["storage_generation"],
        source_by_field["source_count"],
        include_tax_identity=include_tax_identity,
    )
    return PTG2ServingTables(
        physical_binding=physical_binding,
        snapshot_id=str(snapshot_id),
        arch_version=PTG2_V3_ARCH_VERSION,
        storage="manifest_snapshot",
        shared_snapshot_key=layout_by_field["shared_snapshot_key"],
        storage_generation=layout_by_field["storage_generation"],
        cold_lookup_contract=layout_by_field["cold_lookup_contract"],
        serving_table_layout=PTG2_V3_SERVING_LAYOUT,
        shared_block_layout=PTG2_V4_SHARED_BLOCK_LAYOUT
        if layout_by_field["storage_generation"] == PTG2_V4_SHARED_GENERATION
        else PTG2_V3_SHARED_BLOCK_LAYOUT,
        source_count=source_by_field["source_count"],
        code_count=source_by_field["code_count"],
        coverage_scope_id=source_by_field["coverage_scope_id"],
        plan_id=str(row_fields.get("snapshot_plan_id") or "").strip() or None,
        plan_market_type=str(row_fields.get("snapshot_plan_market_type") or "").strip() or None,
        source_key=source_by_field["source_key"],
        audit_sample=source_by_field["audit_sample"],
        source_witness=source_by_field["source_witness_by_field"],
        source_set=source_by_field["source_set_by_field"],
        database_evidence=_database_execution_evidence(row_fields),
        provider_graph_v4_hot_prefix=graph_by_field["hot_prefix"],
        provider_graph_v4_inferred_taxonomy_candidates=graph_by_field["taxonomy"],
        provider_tax_identity_source_publication=graph_by_field["tax_identity"],
        **_descriptor_geometry_fields(serving_index, graph_by_field["network_names"]),
    )
