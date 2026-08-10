# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_tables
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from process.ptg_parts.ptg2_shared_source_set import (
    shared_source_set_metadata,
)

from tests.ptg2_manifest_tables_support import (
    FakeResult,
    FakeSession,
    _empty_online_v4_owner_diagnostic,
    _empty_worst_v4_owner_diagnostic,
    _online_v4_owner_diagnostic,
    _sealed_v4_hot_limits,
    _strict_v4_hot_prefix_manifest,
    _worst_v4_owner_diagnostic,
    empty_direct_v4_serving_index,
    strict_candidate_row,
    strict_direct_v4_serving_index,
    strict_serving_index,
    strict_snapshot_row,
    strict_source_identity_rows,
    strict_source_set,
    strict_tax_identity_source_publication,
    strict_v4_root_row,
    strict_v4_serving_index,
)

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


def test_v4_manifest_accepts_complete_direct_prefix() -> None:
    serving_index = strict_direct_v4_serving_index()

    _shared_snapshot_key, generation, _cold_contract, _audit = (
        ptg2_tables._strict_v3_manifest_fields(serving_index)
    )

    assert generation == "shared_blocks_v4"


def test_v4_manifest_accepts_empty_direct_prefix() -> None:
    serving_index = empty_direct_v4_serving_index()

    _shared_snapshot_key, generation, _cold_contract, _audit = (
        ptg2_tables._strict_v3_manifest_fields(serving_index)
    )

    assert generation == "shared_blocks_v4"


@pytest.mark.parametrize(
    "hot_prefix_updates",
    [
        {"override_owner_count": 0},
        {"override_owner_count": 2},
        {"group_unsafe_set_count": 2},
        {"worst_online_group_work_bound": 1},
        {"override_member_count": 202, "override_raw_bytes": 808},
        {"override_raw_bytes": 13},
        {"worst_uses_override": False},
        {"worst_provider_set_key": None, "worst_member_digest": None},
    ],
)
def test_v4_manifest_rejects_incomplete_direct_prefix(
    hot_prefix_updates,
) -> None:
    serving_index = strict_direct_v4_serving_index()
    hot_prefix = serving_index["serving_binary"]["provider_graph_v4"]["hot_prefix"]
    hot_prefix.update(hot_prefix_updates)

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="V4"):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


@pytest.mark.parametrize(
    "hot_prefix_updates",
    [
        {
            "group_unsafe_set_count": 0,
            "physical_unsafe_set_count": 0,
        },
        {"override_owner_count": 0},
        {
            "simulated_set_count": 3,
            "group_unsafe_set_count": 1,
            "physical_unsafe_set_count": 1,
            "override_owner_count": 3,
        },
    ],
)
def test_v4_manifest_retains_sparse_pattern_prefix_contract(
    hot_prefix_updates,
) -> None:
    serving_index = strict_v4_serving_index()
    hot_prefix = serving_index["serving_binary"]["provider_graph_v4"]["hot_prefix"]
    hot_prefix.update(hot_prefix_updates)

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="V4"):
        ptg2_tables._strict_v3_manifest_fields(serving_index)


def test_legacy_v4_manifest_without_taxonomy_projection_remains_servable() -> None:
    serving_index = strict_v4_serving_index()
    provider_graph = serving_index["serving_binary"]["provider_graph_v4"]
    assert "inferred_taxonomy_candidates" not in provider_graph

    _shared_snapshot_key, generation, _cold_contract, _audit = (
        ptg2_tables._strict_v3_manifest_fields(serving_index)
    )
    assert generation == "shared_blocks_v4"


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
