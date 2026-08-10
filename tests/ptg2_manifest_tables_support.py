# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_tables
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from process.ptg_parts.ptg2_shared_source_set import (
    shared_source_set_metadata,
)


class FakeResult:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar(self):
        return self._scalar

    def one_or_none(self):
        return self._scalar

    def first(self):
        return self._scalar


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    async def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        value = self._results.pop(0) if self._results else None
        return value if isinstance(value, FakeResult) else FakeResult(value)


def strict_source_identity_rows():
    return [
        {
            "source_key": 0,
            "source_type": "in_network",
            "identity_kind": "logical_json_sha256_v1",
            "identity_sha256": "2" * 64,
            "raw_container_sha256": "1" * 64,
            "logical_json_sha256": "2" * 64,
            "logical_hash_deferred": False,
            "source_trace_set_hash": "5" * 64,
        },
        {
            "source_key": 1,
            "source_type": "in_network",
            "identity_kind": "raw_container_sha256_v1",
            "identity_sha256": "3" * 64,
            "raw_container_sha256": "3" * 64,
            "logical_json_sha256": None,
            "logical_hash_deferred": True,
            "source_trace_set_hash": "6" * 64,
        },
    ]


def strict_source_set():
    return shared_source_set_metadata(
        row["raw_container_sha256"]
        for row in strict_source_identity_rows()
    )


def strict_tax_identity_source_publication(*, source_count=2):
    return {
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "content_contract": "ptg2_provider_group_tax_identity_source_content_v1",
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "binding_vector_contract": "ptg2_tax_identity_source_binding_vector_v1",
        "token_policy_id": "ptg-tin-hmac-sha256-v1:test",
        "token_policy_descriptor_sha256": "1" * 64,
        "source_ordinal_map_digest": "2" * 64,
        "source_count": source_count,
        "provider_group_occurrence_count": 7,
        "matched_ein_count": 5,
        "missing_count": 1,
        "malformed_count": 1,
        "unsupported_type_count": 0,
        "content_digest": "3" * 64,
        "artifact_byte_count": 455,
        "binding_vector_digest": "4" * 64,
    }


def strict_serving_index(snapshot_key=41):
    audit_sample_map = {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 2,
        "maximum_rows": 2560,
        "complete_population": False,
        "sample_digest": "a" * 64,
        "source_count": 2,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "serving_multiplicity_semantics": "source_multiset_v1",
    }
    return {
        "storage": "manifest_snapshot",
        "type": "ptg2_shared_blocks_v3",
        "snapshot_scoped": True,
        "arch_version": "postgres_binary_v3",
        "shared_snapshot_key": snapshot_key,
        "coverage_scope_id": "c" * 64,
        "storage_generation": "shared_blocks_v3",
        "source_count": 2,
        "source_set": strict_source_set(),
        "code_count": 2,
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "serving_multiplicity_semantics": "source_multiset_v1",
        "price_membership_semantics": "multiset_v1",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": "dense_shared_blocks_v3",
        "provider_scope_strategy": "postgres_shared_graph",
        "id_storage": "binary128",
        "materialized_tables": {},
        "serving_rates": 2,
        "atom_key_bits": 24,
        "audit_sample": audit_sample_map,
        "serving_binary": {
            "format": "postgres_binary_v3",
            "assigned_encoder": {"provider_shard_span": 8192},
            "price_set_atom_memberships_v3": {"block_span": 512},
            "price_atoms_v3": {"block_span": 512},
            "price_dictionary": {
                "artifact_kind": "by_code_price_dictionary",
                "price_set_count": 29_000_000,
                "block_bytes": 65_536,
                "storage": {"compressed_records": 0},
            },
        },
    }


def _sealed_v4_hot_limits() -> dict:
    """Return the positive limits copied into the sealed test manifest."""

    return {
        "npi_prefix_target": 201,
        "max_set_patterns_per_set": 1024,
        "max_set_components_per_fallback_set": 4096,
        "max_online_group_keys_per_set": 4096,
        "max_online_source_owners_per_set": 4096,
        "max_online_source_members_per_set": 16384,
        "max_online_source_pages_per_set": 64,
        "max_online_source_bytes_per_set": 1048576,
        "online_group_npi_batch_size": 32,
        "max_online_group_npi_members_per_set": 32768,
        "max_online_group_npi_locator_pages_per_set": 16,
        "max_online_group_npi_member_pages_per_set": 128,
        "max_online_group_npi_bytes_per_set": 4194304,
        "max_online_group_npi_batches_per_set": 4,
        "provider_expansion_rate_page_rows": 64,
        "max_online_provider_expansion_rate_rows": 256,
        "max_online_provider_expansion_provider_sets": 64,
        "max_online_provider_expansion_graph_batches": 64,
    }


def _worst_v4_owner_diagnostic() -> dict:
    """Return the sparse-override canary evidence for the strict fixture."""

    return {
        "maximum_group_npi_member_work": 100,
        "maximum_group_npi_locator_page_work": 1,
        "maximum_group_npi_member_page_work": 1,
        "maximum_group_npi_byte_work": 1000,
        "maximum_group_npi_batch_work": 1,
        "group_unsafe_set_count": 1,
        "physical_unsafe_set_count": 0,
        "simulated_set_count": 1,
        "override_owner_count": 1,
        "override_member_count": 3,
        "override_raw_bytes": 12,
        "worst_provider_set_key": 1,
        "worst_groups_to_target": 4097,
        "worst_uses_override": True,
        "worst_uses_component_fallback": False,
        "worst_member_count": 3,
        "worst_member_digest": "a" * 64,
        "worst_source_owner_work": 1,
        "worst_source_member_work": 1,
        "worst_source_page_work": 1,
        "worst_source_byte_work": 16,
        "worst_group_npi_member_work": 100,
        "worst_group_npi_locator_page_work": 1,
        "worst_group_npi_member_page_work": 1,
        "worst_group_npi_byte_work": 1000,
        "worst_group_npi_batch_work": 1,
    }


def _empty_worst_v4_owner_diagnostic() -> dict:
    """Return empty highest-risk canary evidence for a zero-set graph."""

    return {
        "maximum_group_npi_member_work": 0,
        "maximum_group_npi_locator_page_work": 0,
        "maximum_group_npi_member_page_work": 0,
        "maximum_group_npi_byte_work": 0,
        "maximum_group_npi_batch_work": 0,
        "group_unsafe_set_count": 0,
        "physical_unsafe_set_count": 0,
        "simulated_set_count": 0,
        "override_owner_count": 0,
        "override_member_count": 0,
        "override_raw_bytes": 0,
        "worst_provider_set_key": None,
        "worst_groups_to_target": 0,
        "worst_uses_override": False,
        "worst_uses_component_fallback": False,
        "worst_member_count": 0,
        "worst_member_digest": None,
        "worst_source_owner_work": 0,
        "worst_source_member_work": 0,
        "worst_source_page_work": 0,
        "worst_source_byte_work": 0,
        "worst_group_npi_member_work": 0,
        "worst_group_npi_locator_page_work": 0,
        "worst_group_npi_member_page_work": 0,
        "worst_group_npi_byte_work": 0,
        "worst_group_npi_batch_work": 0,
    }


def _online_v4_owner_diagnostic() -> dict:
    """Return the bounded online canary evidence for the strict fixture."""

    return {
        "worst_online_provider_set_key": 2,
        "worst_online_groups_to_target": 2,
        "worst_online_groups_to_target_exact": True,
        "worst_online_uses_component_fallback": False,
        "worst_online_group_work_bound": 2,
        "worst_online_member_count": 3,
        "worst_online_member_digest": "b" * 64,
        "worst_online_source_owner_work": 1,
        "worst_online_source_member_work": 1,
        "worst_online_source_page_work": 1,
        "worst_online_source_byte_work": 16,
        "worst_online_group_npi_member_work": 100,
        "worst_online_group_npi_locator_page_work": 1,
        "worst_online_group_npi_member_page_work": 1,
        "worst_online_group_npi_byte_work": 1000,
        "worst_online_group_npi_batch_work": 1,
    }


def _empty_online_v4_owner_diagnostic() -> dict:
    """Return the empty ordinary-online canary required by direct coverage."""

    return {
        "worst_online_provider_set_key": None,
        "worst_online_groups_to_target": 0,
        "worst_online_groups_to_target_exact": False,
        "worst_online_uses_component_fallback": False,
        "worst_online_group_work_bound": 0,
        "worst_online_member_count": 0,
        "worst_online_member_digest": None,
        "worst_online_source_owner_work": 0,
        "worst_online_source_member_work": 0,
        "worst_online_source_page_work": 0,
        "worst_online_source_byte_work": 0,
        "worst_online_group_npi_member_work": 0,
        "worst_online_group_npi_locator_page_work": 0,
        "worst_online_group_npi_member_page_work": 0,
        "worst_online_group_npi_byte_work": 0,
        "worst_online_group_npi_batch_work": 0,
    }


def _strict_v4_hot_prefix_manifest() -> dict:
    """Build one exact sealed hot-prefix manifest for contract tests."""

    return {
        **_sealed_v4_hot_limits(),
        **_worst_v4_owner_diagnostic(),
        **_online_v4_owner_diagnostic(),
    }


def strict_v4_serving_index(snapshot_key=43):
    """Build the strict V4 serving manifest used by table-contract tests."""

    serving_index_by_field = strict_serving_index(snapshot_key)
    serving_index_by_field.update(
        {
            "type": "ptg2_shared_blocks_v4",
            "storage_generation": "shared_blocks_v4",
            "provider_scope_strategy": "postgres_packed_graph_v4",
            "shared_block_layout": "packed_snapshot_maps_v4",
        }
    )
    serving_index_by_field["serving_binary"]["provider_graph_v4"] = {
        "contract": "ptg2_provider_graph_v4",
        "representation": "pattern_v1",
        "map_format": "packed_coordinate_hash_v1",
        "projection_id_scope": "snapshot_local_v1",
        "map_digest": "d" * 64,
        "locator_page_contract": "packed_owner_locator_page_v1",
        "member_page_contract": "packed_member_page_v1",
        "npi_table": "ptg2_v4_npi_scope",
        "component_table": "ptg2_v4_provider_component",
        "pattern_table": "ptg2_v4_pattern",
        "relation_manifest_table": "ptg2_v4_relation_manifest",
        "heavy_owner_table": "ptg2_v4_heavy_owner",
        "npi_prefix_table": "ptg2_v4_provider_set_npi_prefix",
        "diagnostic_table": "ptg2_v4_provider_graph_diagnostic",
        "resource_admission": {
            "compressed_acquisition_bytes": 1024,
            "input_factor_bytes": 512,
            "factor_edge_count": 9,
            "empty_npi_tin_only_normalization_count": 0,
        },
        "hot_prefix": _strict_v4_hot_prefix_manifest(),
    }
    return serving_index_by_field


def strict_direct_v4_serving_index(snapshot_key=43):
    """Build a direct layout with one complete prefix and no unsafe sets."""

    serving_index_by_field = strict_v4_serving_index(snapshot_key)
    provider_graph_by_field = serving_index_by_field["serving_binary"][
        "provider_graph_v4"
    ]
    provider_graph_by_field["representation"] = "direct_v1"
    provider_graph_by_field["hot_prefix"].update(
        group_unsafe_set_count=0,
        physical_unsafe_set_count=0,
        **_empty_online_v4_owner_diagnostic(),
    )
    return serving_index_by_field


def empty_direct_v4_serving_index(snapshot_key=43):
    """Build a writer-shaped direct graph without provider sets."""

    serving_index_by_field = strict_direct_v4_serving_index(snapshot_key)
    hot_prefix = serving_index_by_field["serving_binary"]["provider_graph_v4"][
        "hot_prefix"
    ]
    hot_prefix.update(
        **_empty_worst_v4_owner_diagnostic(),
        **_empty_online_v4_owner_diagnostic(),
    )
    return serving_index_by_field


def strict_v4_root_row(serving_index_by_field, *, representation=None):
    """Mirror the sealed V4 root and diagnostic row for one manifest."""

    provider_graph_by_field = serving_index_by_field["serving_binary"][
        "provider_graph_v4"
    ]
    hot_prefix_by_field = dict(provider_graph_by_field["hot_prefix"])
    for digest_field in (
        "worst_member_digest",
        "worst_online_member_digest",
    ):
        digest = hot_prefix_by_field.get(digest_field)
        hot_prefix_by_field[digest_field] = (
            bytes.fromhex(digest) if digest is not None else None
        )
    return {
        "representation": (
            representation or provider_graph_by_field["representation"]
        ),
        "map_format": provider_graph_by_field["map_format"],
        "projection_id_scope": provider_graph_by_field["projection_id_scope"],
        "map_digest": provider_graph_by_field["map_digest"],
        **hot_prefix_by_field,
        **provider_graph_by_field["resource_admission"],
    }


def strict_snapshot_row(serving_index=None, **overrides):
    serving_index = dict(serving_index or strict_serving_index())
    snapshot_source_set = serving_index.pop("source_set", None)
    snapshot_source_set_digest = (snapshot_source_set or {}).get("raw_container_sha256_digest")
    coverage_scope_id = serving_index.get("coverage_scope_id")
    snapshot_row_map = {
        "layout_serving_index": serving_index,
        "snapshot_source_set": snapshot_source_set,
        "bound_snapshot_key": serving_index.get("shared_snapshot_key"),
        "snapshot_plan_id": "TEST-PLAN-001",
        "snapshot_plan_market_type": "group",
        "snapshot_coverage_scope_id": coverage_scope_id,
        "attested_source_key": "source-a",
        "attested_coverage_scope_id": coverage_scope_id,
        "attested_source_set_digest": snapshot_source_set_digest,
        "attested_audit_sample_digest": "a" * 64,
        "source_row_count": serving_index.get("source_count"),
        "distinct_source_key_count": serving_index.get("source_count"),
        "minimum_source_key": 0,
        "maximum_source_key": int(serving_index.get("source_count") or 0) - 1,
        "source_identity_rows": strict_source_identity_rows(),
        "postgres_server_version_num": 160004,
        "database_selected": True,
        "backend_session_active": True,
        "transaction_snapshot_observed": True,
    }
    snapshot_row_map.update(overrides)
    return snapshot_row_map


def strict_candidate_row(serving_index=None, **overrides):
    serving_index = dict(serving_index or strict_serving_index())
    serving_index["source_key"] = "source-a"
    coverage_scope_id = serving_index.get("coverage_scope_id")
    snapshot_row_map = {
        "candidate_serving_index": serving_index,
        "layout_audit_sample": serving_index.get("audit_sample"),
        "layout_coverage_scope_id": coverage_scope_id,
        "layout_code_count": serving_index.get("code_count"),
        "snapshot_plan_id": "TEST-PLAN-001",
        "snapshot_plan_market_type": "group",
        "snapshot_coverage_scope_id": coverage_scope_id,
        "postgres_server_version_num": 160004,
        "database_selected": True,
        "backend_session_active": True,
        "transaction_snapshot_observed": True,
    }
    snapshot_row_map.update(overrides)
    return snapshot_row_map
