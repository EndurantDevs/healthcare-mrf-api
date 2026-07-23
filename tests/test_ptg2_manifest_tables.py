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


@pytest.mark.asyncio
async def test_snapshot_serving_tables_requires_published_and_never_caches_v3_metadata():
    class RealishFakeSession(FakeSession):
        sync_session = object()

    snapshot_id = "strict-v3-cache-free"
    session = RealishFakeSession(
        [
            None,
            strict_snapshot_row(strict_serving_index(41)),
            strict_snapshot_row(strict_serving_index(42)),
        ]
    )

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError, match="published.*sealed"
    ):
        await ptg2_tables.snapshot_serving_tables(session, snapshot_id)
    first = await ptg2_tables.snapshot_serving_tables(session, snapshot_id)
    second = await ptg2_tables.snapshot_serving_tables(session, snapshot_id)

    assert first.shared_snapshot_key == 41
    assert second.shared_snapshot_key == 42
    assert len(session.calls) == 3
    sql = str(session.calls[0][0][0])
    assert "status = 'published'" in sql
    assert "ptg2_v3_snapshot_binding" in sql
    assert "ptg2_v3_snapshot_layout" in sql
    assert "ptg2_v3_snapshot_scope" in sql
    assert "ptg2_v3_candidate_audit_attestation" in sql
    assert "ptg2_v3_snapshot_source" in sql
    assert "JSON_AGG(" in sql
    assert "raw_container_sha256" in sql
    assert "logical_hash_deferred" in sql
    assert "source_trace_set_hash" in sql
    assert "pgcrypto" not in sql
    source_set_sql = "snapshot.manifest->'serving_index'->'source_set'"
    assert f"{source_set_sql} AS snapshot_source_set" in " ".join(sql.split())
    assert "SELECT snapshot.manifest," not in sql
    assert "current_setting('server_version_num')" in sql
    assert "txid_current_snapshot()" in sql
    assert "attestation.contract = ANY(" in sql
    query_params = session.calls[0][0][1]
    assert query_params["attestation_contracts"] == list(
        ptg2_tables.PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
    )
    assert "COUNT(DISTINCT code.coverage_scope_id)" not in sql
    assert "ptg2_v3_code code" not in sql
    assert not hasattr(ptg2_tables, "_PTG2_SNAPSHOT_TABLES_CACHE")


@pytest.mark.asyncio
async def test_snapshot_serving_tables_reads_strict_shared_v3_contract():
    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession([strict_snapshot_row()]),
        "strict-v3",
    )

    assert tables.arch_version == "postgres_binary_v3"
    assert tables.uses_shared_blocks is True
    assert tables.shared_snapshot_key == 41
    assert not hasattr(tables, "serving_binary_table")
    assert tables.atom_key_bits == 24
    assert tables.price_key_block_span == 512
    assert tables.atom_key_block_span == 512
    assert tables.price_dictionary_item_count == 29_000_000
    assert tables.price_dictionary_block_bytes == 65_536
    assert tables.provider_shard_span == 8192
    assert tables.source_count == 2
    assert tables.source_set == strict_serving_index()["source_set"]
    assert tables.database_evidence["server_version_num"] == 160004
    assert tables.shared_block_layout == "dense_shared_blocks_v3"


@pytest.mark.asyncio
async def test_candidate_snapshot_keeps_pre_activation_manifest_validation():
    session = FakeSession([strict_candidate_row()])
    tables = await ptg2_tables.snapshot_serving_tables(
        session,
        "candidate-v3",
        candidate_audit_access=PTG2CandidateAuditAccess(
            snapshot_id="candidate-v3",
            source_key="source-a",
            plan_id="TEST-PLAN-001",
            plan_market_type="group",
        ),
    )

    assert tables.source_key == "source-a"
    assert tables.source_set == strict_serving_index()["source_set"]
    assert tables.source_witness is None
    sql = str(session.calls[0][0][0])
    assert "snapshot.status = 'validated'" in sql
    assert "snapshot.manifest->'serving_index' AS candidate_serving_index" in sql
    assert "SELECT snapshot.manifest," not in sql
    assert "ptg2_v3_candidate_audit_attestation" not in sql
    assert "LEFT JOIN mrf.ptg2_v3_source_audit_witness" in sql


@pytest.mark.asyncio
async def test_snapshot_serving_tables_requires_database_execution_evidence():
    row = strict_snapshot_row(backend_session_active=False)

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="PostgreSQL execution evidence",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([row]),
            "strict-v3-no-db-evidence",
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_accepts_json_string_v3_layout_metadata():
    serving_index = json.dumps(strict_serving_index())
    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession([strict_snapshot_row(layout_serving_index=serving_index)]),
        "strict-v3-json",
    )

    assert tables.shared_snapshot_key == 41
    assert tables.storage_generation == "shared_blocks_v3"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "serving_index",
    [
        {"arch_version": "materialized_v1", "table": "mrf.ptg2_serving_old"},
        {"arch_version": "sidecar_scope_v1", "storage": "manifest_snapshot"},
        {"arch_version": "postgres_binary_v1", "serving_binary_table": "mrf.old"},
        {"arch_version": "postgres_binary_v2", "serving_binary_table": "mrf.old"},
        {
            **strict_serving_index(),
            "serving_binary_table": "mrf.ptg2_serving_binary_old_v3",
        },
        {
            **strict_serving_index(),
            "materialized_tables": {"serving_binary": "mrf.old"},
        },
    ],
)
async def test_snapshot_serving_tables_rejects_every_legacy_shape(serving_index):
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="reimport"):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([strict_snapshot_row(serving_index)]),
            "legacy-snapshot",
        )


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda value: value.pop("shared_block_layout"), "shared_block_layout"),
        (
            lambda value: value.update(storage_generation="shared_blocks_v1"),
            "storage_generation=shared_blocks_v3",
        ),
        (
            lambda value: value.update(shared_block_layout="dense_shared_blocks_v1"),
            "shared_block_layout=dense_shared_blocks_v3",
        ),
        (lambda value: value.pop("source_count"), "source_count"),
        (
            lambda value: value["serving_binary"].update(format="postgres_binary_v2"),
            "serving_binary format",
        ),
        (
            lambda value: value["serving_binary"]["price_dictionary"].pop(
                "block_bytes"
            ),
            "block_bytes",
        ),
        (
            lambda value: value["serving_binary"]["price_dictionary"].update(
                block_bytes=65_535
            ),
            "price dictionary metadata",
        ),
        (
            lambda value: value["serving_binary"]["price_set_atom_memberships_v3"].pop(
                "block_span"
            ),
            "block_span",
        ),
        (lambda value: value.pop("audit_sample"), "persisted audit sample"),
        (
            lambda value: value["audit_sample"].update(sample_count=2561),
            "audit sample bounds",
        ),
        (
            lambda value: value["audit_sample"].update(sample_digest="not-a-digest"),
            "audit sample digest",
        ),
        (
            lambda value: value["audit_sample"].update(source_count=1),
            "audit sample bounds",
        ),
    ],
)
def test_strict_v3_contract_rejects_cold_unsafe_metadata(mutator, message):
    serving_index = strict_serving_index()
    mutator(serving_index)

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match=message):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


def test_v4_manifest_accepts_v3_price_contract() -> None:
    shared_snapshot_key, generation, cold_contract, _audit = (
        ptg2_tables._strict_v3_manifest_fields(strict_v4_serving_index())
    )
    assert shared_snapshot_key == 43
    assert generation == "shared_blocks_v4"
    assert cold_contract == "ptg_v3_cold_v2"


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value["serving_binary"].pop("provider_graph_v4"),
        lambda value: value["serving_binary"]["provider_graph_v4"].update(
            representation="source_component_v1"
        ),
        lambda value: value["serving_binary"]["provider_graph_v4"].update(
            map_digest="not-a-digest"
        ),
        lambda value: value["serving_binary"]["provider_graph_v4"].update(
            npi_table="ptg2_v3_npi_scope"
        ),
        lambda value: value["serving_binary"]["provider_graph_v4"][
            "hot_prefix"
        ].update(max_online_group_npi_batches_per_set=0),
        lambda value: value["serving_binary"]["provider_graph_v4"][
            "hot_prefix"
        ].update(provider_expansion_rate_page_rows=0),
    ],
)
def test_strict_manifest_rejects_unservable_v4_graph_metadata(mutator) -> None:
    serving_index = strict_v4_serving_index()
    mutator(serving_index)
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="V4"):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.parametrize(
    "normalization_count",
    [-1, None],
)
def test_v4_manifest_rejects_invalid_empty_npi_resource(
    normalization_count,
) -> None:
    serving_index = strict_v4_serving_index()
    resources = serving_index["serving_binary"]["provider_graph_v4"][
        "resource_admission"
    ]
    resources["empty_npi_tin_only_normalization_count"] = normalization_count

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="V4"):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.asyncio
async def test_snapshot_serving_tables_binds_v4_manifest_to_completed_root() -> None:
    serving_index = strict_v4_serving_index()
    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(serving_index),
                strict_v4_root_row(serving_index),
            ]
        ),
        "strict-v4",
    )
    assert tables.storage_generation == "shared_blocks_v4"
    assert tables.shared_block_layout == "packed_snapshot_maps_v4"
    assert tables.uses_v4_graph is True


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_v4_root_manifest_mismatch() -> None:
    serving_index = strict_v4_serving_index()
    root_by_field = strict_v4_root_row(
        serving_index,
        representation="direct_v1",
    )
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="map root does not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession(
                [strict_snapshot_row(serving_index), root_by_field]
            ),
            "strict-v4-mismatch",
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_empty_npi_resource_tamper() -> None:
    serving_index = strict_v4_serving_index()
    root_by_field = strict_v4_root_row(serving_index)
    root_by_field["empty_npi_tin_only_normalization_count"] = 1

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="graph resources do not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession(
                [strict_snapshot_row(serving_index), root_by_field]
            ),
            "strict-v4-resource-tamper",
        )


@pytest.mark.parametrize(
    "coverage_scope_id",
    [
        None,
        "C" * 64,
        "c" * 63,
        "c" * 65,
        "g" * 64,
        f" {'c' * 64}",
    ],
)
def test_strict_v3_contract_requires_canonical_coverage_scope_id(
    coverage_scope_id,
):
    serving_index = strict_serving_index()
    serving_index["coverage_scope_id"] = coverage_scope_id

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="64-lowercase-hex coverage_scope_id",
    ):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_missing_or_mismatched_binding():
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError, match="published.*sealed"
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([None]),
            "scope-binding-mismatch",
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_rejects_attested_audit_sample_mismatch():
    row = strict_snapshot_row()
    row["attested_audit_sample_digest"] = "b" * 64

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="audit attestation does not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([row]),
            "audit-sample-mismatch",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"snapshot_coverage_scope_id": None},
            "published scope",
        ),
        (
            {"snapshot_coverage_scope_id": "d" * 64},
            "published scope",
        ),
        (
            {"attested_coverage_scope_id": None},
            "published scope",
        ),
        (
            {"attested_coverage_scope_id": "d" * 64},
            "published scope",
        ),
        (
            {"bound_snapshot_key": 99},
            "layout binding",
        ),
        (
            {"source_row_count": 1},
            "source dictionary",
        ),
        (
            {"maximum_source_key": 2},
            "source dictionary",
        ),
    ],
    ids=[
        "missing-snapshot-scope",
        "mismatched-snapshot-scope",
        "missing-attested-scope",
        "mismatched-attested-scope",
        "mismatched-binding",
        "missing-source-row",
        "non-dense-source-key",
    ],
)
async def test_snapshot_serving_tables_rejects_broken_scope_chain(
    overrides,
    message,
):
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match=message):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([strict_snapshot_row(**overrides)]),
            "broken-scope-chain",
        )


@pytest.mark.asyncio
async def test_snapshot_serving_tables_allows_no_code_rows_for_empty_layout():
    serving_index = strict_serving_index()
    serving_index["serving_rates"] = 0
    serving_index["code_count"] = 0

    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession(
            [
                strict_snapshot_row(
                    serving_index,
                    layout_code_count=0,
                )
            ]
        ),
        "empty-scope-chain",
    )

    assert tables.shared_snapshot_key == 41
