# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_tables


class FakeResult:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar(self):
        return self._scalar

    def one_or_none(self):
        return self._scalar


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []

    async def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        value = self._results.pop(0) if self._results else None
        return value if isinstance(value, FakeResult) else FakeResult(value)


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
        "source_set": {
            "contract": "sorted_raw_container_sha256_bytes_v1",
            "source_count": 2,
            "raw_container_sha256_digest": "b" * 64,
        },
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


def strict_snapshot_row(serving_index=None, **overrides):
    serving_index = dict(serving_index or strict_serving_index())
    coverage_scope_id = serving_index.get("coverage_scope_id")
    snapshot_row_map = {
        "manifest": {"serving_index": serving_index},
        "layout_audit_sample": serving_index.get("audit_sample"),
        "layout_coverage_scope_id": coverage_scope_id,
        "snapshot_plan_id": "TEST-PLAN-001",
        "snapshot_plan_market_type": "group",
        "snapshot_coverage_scope_id": coverage_scope_id,
        "layout_code_count": serving_index.get("code_count"),
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
    assert "current_setting('server_version_num')" in sql
    assert "txid_current_snapshot()" in sql
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
    assert tables.source_count == 2
    assert tables.source_set == strict_serving_index()["source_set"]
    assert tables.database_evidence["server_version_num"] == 160004
    assert tables.shared_block_layout == "dense_shared_blocks_v3"


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
async def test_snapshot_serving_tables_accepts_json_string_v3_manifest():
    manifest = json.dumps({"serving_index": strict_serving_index()})
    tables = await ptg2_tables.snapshot_serving_tables(
        FakeSession([strict_snapshot_row(manifest=manifest)]),
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
async def test_snapshot_serving_tables_rejects_layout_audit_sample_mismatch():
    row = strict_snapshot_row()
    row["layout_audit_sample"] = {
        **row["layout_audit_sample"],
        "sample_digest": "b" * 64,
    }

    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="layout audit sample does not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([row]),
            "audit-sample-mismatch",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"layout_coverage_scope_id": None}, "sealed layout coverage scope"),
        ({"layout_coverage_scope_id": "d" * 64}, "sealed layout coverage scope"),
        (
            {"snapshot_coverage_scope_id": None},
            "snapshot coverage scope binding",
        ),
        (
            {"snapshot_coverage_scope_id": "d" * 64},
            "snapshot coverage scope binding",
        ),
        (
            {"layout_code_count": 3},
            "code count does not match",
        ),
        (
            {"layout_code_count": None},
            "code count does not match",
        ),
    ],
    ids=[
        "missing-layout-manifest-scope",
        "mismatched-layout-manifest-scope",
        "missing-snapshot-scope",
        "mismatched-snapshot-scope",
        "mismatched-code-count",
        "missing-code-count",
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
