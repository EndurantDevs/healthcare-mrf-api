# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import (
    ptg2_candidate_audit,
    ptg2_db_sidecars,
    ptg2_db_serving_v3_pages,
    ptg2_serving,
    ptg2_shared_blocks,
    ptg2_snapshot,
    ptg2_tables,
)
from api.ptg2_shared_blocks import SharedBlockPayload


def test_shared_v3_object_kinds_and_block_format_are_fail_closed():
    assert (
        ptg2_db_sidecars._SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND
        == "by_code_provider_shard_v1"
    )
    assert (
        ptg2_db_serving_v3_pages.PTG2_SERVING_BINARY_V3_BY_CODE_PAGE_KIND
        == "by_code_price_page_v4"
    )
    assert (
        ptg2_db_serving_v3_pages.PTG2_SERVING_BINARY_V3_PROVIDER_SET_PAGE_KIND
        == "provider_set_page_v3_s2"
    )
    with pytest.raises(
        ptg2_shared_blocks.PTG2SharedBlockError,
        match="unsupported format version",
    ):
        ptg2_shared_blocks._validated_payload(
            {
                "object_kind": "by_code_provider_shard_v1",
                "format_version": 1,
                "codec": "none",
                "payload": b"",
            },
            expected_kind="by_code_provider_shard_v1",
        )


def _strict_serving_index(snapshot_key=41):
    audit_sample_by_field = {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 1,
        "maximum_rows": 2560,
        "complete_population": False,
        "sample_digest": "a" * 64,
        "source_count": 1,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "serving_multiplicity_semantics": "source_multiset_v1",
    }
    return {
        "storage": "manifest_snapshot",
        "type": "ptg2_shared_blocks_v3",
        "snapshot_scoped": True,
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "price_membership_semantics": "multiset_v1",
        "serving_multiplicity_semantics": "source_multiset_v1",
        "shared_snapshot_key": snapshot_key,
        "source_count": 1,
        "source_set": {
            "contract": "sorted_raw_container_sha256_bytes_v1",
            "source_count": 1,
            "raw_container_sha256_digest": "b" * 64,
        },
        "code_count": 1,
        "coverage_scope_id": "c" * 64,
        "provider_scope_strategy": "postgres_shared_graph",
        "id_storage": "binary128",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": "dense_shared_blocks_v3",
        "atom_key_bits": 24,
        "materialized_tables": {},
        "audit_sample": audit_sample_by_field,
        "serving_binary": {
            "format": "postgres_binary_v3",
            "price_dictionary": {
                "artifact_kind": "by_code_price_dictionary",
                "price_set_count": 2,
                "block_bytes": 65_536,
            },
            "price_set_atom_memberships_v3": {"block_span": 512},
            "price_atoms_v3": {"block_span": 512},
        },
    }


def _strict_tables(
    *,
    snapshot_id: str = "logical-snapshot",
    snapshot_key: int = 41,
) -> ptg2_serving.PTG2ServingTables:
    return ptg2_serving.PTG2ServingTables(
        snapshot_id=snapshot_id,
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_snapshot_key=snapshot_key,
        serving_table_layout="lean_provider_key_v1",
        shared_block_layout="dense_shared_blocks_v3",
        source_count=1,
        atom_key_bits=24,
        price_dictionary_item_count=2,
        price_dictionary_block_bytes=65_536,
        price_key_block_span=512,
        atom_key_block_span=512,
        database_evidence={
            "contract": "postgresql_session_v1",
            "server_version_num": 160004,
            "database_selected": True,
            "backend_session_active": True,
            "transaction_snapshot_observed": True,
        },
    )


class _Rows:
    def __init__(self, rows):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)


class _OneOrNoneResult:
    def __init__(self, row_fields):
        self.row_fields = row_fields

    def one_or_none(self):
        return self.row_fields


class _Session:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return _Rows(self.rows)


class _RecordingOneRowSession:
    sync_session = object()

    def __init__(self, row_fields):
        self.row_fields = row_fields
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return _OneOrNoneResult(self.row_fields)


def _candidate_audit_access():
    return ptg2_candidate_audit.PTG2CandidateAuditAccess(
        snapshot_id="logical-candidate",
        source_key="logical-source",
        plan_id="plan-a",
        plan_market_type="group",
    )


def _candidate_descriptor_row(manifest_source_key: str):
    serving_index = {
        **_strict_serving_index(),
        "source_key": manifest_source_key,
    }
    return {
        "candidate_serving_index": serving_index,
        "layout_audit_sample": serving_index["audit_sample"],
        "layout_coverage_scope_id": serving_index["coverage_scope_id"],
        "layout_code_count": serving_index["code_count"],
        "snapshot_plan_id": "plan-a",
        "snapshot_plan_market_type": "group",
        "snapshot_coverage_scope_id": serving_index["coverage_scope_id"],
        "postgres_server_version_num": 160004,
        "database_selected": True,
        "backend_session_active": True,
        "transaction_snapshot_observed": True,
    }


@pytest.mark.asyncio
async def test_provider_set_network_names_are_loaded_by_exact_provider_identity():
    provider_set_id = "02" * 16
    rows = [{"provider_set_global_id_128": provider_set_id, "network_names": []}]
    session = _Session(
        [
            {
                "provider_set_id": provider_set_id,
                "network_names": ["Source Network B", "Source Network A"],
            }
        ]
    )

    await ptg2_serving._hydrate_provider_set_network_names(
        session,
        _strict_tables(snapshot_key=41),
        rows,
    )

    assert rows[0]["network_names"] == ["Source Network B", "Source Network A"]
    assert len(session.calls) == 1
    assert session.calls[0][1] == {
        "shared_snapshot_key": 41,
        "provider_set_ids": [bytes.fromhex(provider_set_id)],
    }


@pytest.mark.asyncio
async def test_provider_set_network_name_hydration_fails_closed_when_metadata_is_missing():
    rows = [{"provider_set_global_id_128": "02" * 16, "network_names": []}]

    with pytest.raises(
        ptg2_serving.PTG2ManifestArtifactError,
        match="network metadata is incomplete",
    ):
        await ptg2_serving._hydrate_provider_set_network_names(
            _Session(),
            _strict_tables(snapshot_key=41),
            rows,
        )


@pytest.mark.asyncio
async def test_shared_snapshot_metadata_is_never_process_cached():
    snapshot_id = "shared-snapshot-no-cache"
    manifests = [
        {"serving_index": _strict_serving_index(snapshot_key)}
        for snapshot_key in (41, 42)
    ]

    class _ManifestResult:
        def __init__(self, value):
            self.value = value

        def one_or_none(self):
            serving_index = self.value["serving_index"]
            return {
                "layout_serving_index": serving_index,
                "bound_snapshot_key": serving_index["shared_snapshot_key"],
                "snapshot_plan_id": "TEST-PLAN-001",
                "snapshot_plan_market_type": "group",
                "snapshot_coverage_scope_id": "c" * 64,
                "attested_source_key": "source-a",
                "attested_coverage_scope_id": "c" * 64,
                "attested_source_set_digest": "b" * 64,
                "attested_audit_sample_digest": "a" * 64,
                "source_row_count": 1,
                "distinct_source_key_count": 1,
                "minimum_source_key": 0,
                "maximum_source_key": 0,
                "postgres_server_version_num": 160004,
                "database_selected": True,
                "backend_session_active": True,
                "transaction_snapshot_observed": True,
            }

    class _MetadataSession:
        sync_session = object()

        def __init__(self):
            self.calls = 0

        async def execute(self, _statement, _params):
            value = manifests[self.calls]
            self.calls += 1
            return _ManifestResult(value)

    session = _MetadataSession()
    first = await ptg2_tables.snapshot_serving_tables(session, snapshot_id)
    second = await ptg2_tables.snapshot_serving_tables(session, snapshot_id)

    assert session.calls == 2
    assert first.shared_snapshot_key == 41
    assert second.shared_snapshot_key == 42
    assert not hasattr(ptg2_tables, "_PTG2_SNAPSHOT_TABLES_CACHE")


@pytest.mark.asyncio
async def test_candidate_snapshot_descriptor_requires_manifest_source_binding():
    access = _candidate_audit_access()
    session = _RecordingOneRowSession(
        _candidate_descriptor_row(access.source_key)
    )
    tables = await ptg2_tables.snapshot_serving_tables(
        session,
        access.snapshot_id,
        candidate_audit_access=access,
    )

    assert tables.source_key == access.source_key
    assert len(session.calls) == 1
    sql, params = session.calls[0]
    assert "snapshot.manifest->'serving_index'->>'source_key'" in sql
    assert params["candidate_source_key"] == access.source_key


@pytest.mark.asyncio
async def test_candidate_snapshot_descriptor_rejects_mismatched_manifest_source():
    access = _candidate_audit_access()
    session = _RecordingOneRowSession(
        _candidate_descriptor_row("another-source")
    )
    with pytest.raises(
        ptg2_tables.PTG2ManifestArtifactError,
        match="candidate source does not match",
    ):
        await ptg2_tables.snapshot_serving_tables(
            session,
            access.snapshot_id,
            candidate_audit_access=access,
        )


@pytest.mark.asyncio
async def test_candidate_search_reuses_one_validated_snapshot_descriptor(monkeypatch):
    session = object()
    pagination = SimpleNamespace(limit=100, offset=0)
    access = _candidate_audit_access()
    query_args_by_name = {
        "snapshot_id": access.snapshot_id,
        "source_key": access.source_key,
        "plan_id": access.plan_id,
        "plan_market_type": access.plan_market_type,
        ptg2_candidate_audit.PTG2_CANDIDATE_AUDIT_ACCESS_ARG: access,
    }
    tables = _strict_tables(snapshot_id=access.snapshot_id)
    descriptor = AsyncMock(return_value=tables)
    resolver = AsyncMock(
        side_effect=AssertionError("candidate search must not resolve the snapshot twice")
    )
    one_snapshot_search = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", descriptor)
    monkeypatch.setattr(
        ptg2_serving,
        "resolve_current_ptg2_snapshot_id",
        resolver,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        one_snapshot_search,
    )

    search_response = await ptg2_serving.search_current_ptg2_index(
        session,
        query_args_by_name,
        pagination,
    )

    assert search_response == {"items": []}
    resolver.assert_not_awaited()
    descriptor.assert_awaited_once_with(
        session,
        access.snapshot_id,
        candidate_audit_access=access,
    )
    one_snapshot_search.assert_awaited_once_with(
        session,
        access.snapshot_id,
        query_args_by_name,
        pagination,
        serving_tables=tables,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("omitted_coordinate", ["snapshot_id", "source_key"])
async def test_candidate_search_rejects_missing_coordinates_before_network_lookup(
    monkeypatch,
    omitted_coordinate,
):
    access = _candidate_audit_access()
    query_args_by_name = {
        "snapshot_id": access.snapshot_id,
        "source_key": access.source_key,
        "plan_id": access.plan_id,
        "plan_market_type": access.plan_market_type,
        ptg2_candidate_audit.PTG2_CANDIDATE_AUDIT_ACCESS_ARG: access,
    }
    query_args_by_name.pop(omitted_coordinate)
    descriptor = AsyncMock()
    network_lookup = AsyncMock()
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", descriptor)
    monkeypatch.setattr(
        ptg2_serving,
        "current_network_snapshots_for_plan",
        network_lookup,
    )

    search_response = await ptg2_serving.search_current_ptg2_index(
        object(),
        query_args_by_name,
        SimpleNamespace(limit=100, offset=0),
    )

    assert search_response is None
    descriptor.assert_not_awaited()
    network_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_explicit_snapshot_search_keeps_resolver_revalidation(monkeypatch):
    session = object()
    pagination = SimpleNamespace(limit=100, offset=0)
    query_args_by_name = {"snapshot_id": "published-snapshot"}
    resolver = AsyncMock(return_value="published-snapshot")
    one_snapshot_search = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(
        ptg2_serving,
        "resolve_current_ptg2_snapshot_id",
        resolver,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_search_one_ptg2_snapshot",
        one_snapshot_search,
    )

    search_response = await ptg2_serving.search_current_ptg2_index(
        session,
        query_args_by_name,
        pagination,
    )

    assert search_response == {"items": []}
    resolver.assert_awaited_once_with(session, query_args_by_name)
    one_snapshot_search.assert_awaited_once_with(
        session,
        "published-snapshot",
        query_args_by_name,
        pagination,
    )


def test_snapshot_availability_branches_v3_to_binding_and_layout():
    sql = ptg2_snapshot._serving_relation_available_sql("published_snapshot")

    assert "ptg2_v3_snapshot_binding" in sql
    assert "ptg2_v3_snapshot_layout" in sql
    assert "shared_layout.state = 'sealed'" in sql
    assert "shared_layout.generation = 'shared_blocks_v3'" in sql
    assert "ptg2_v3_candidate_audit_attestation" in sql
    assert "shared_attestation.activated_at IS NOT NULL" in sql
    assert "manifest" not in sql
    assert "<> 'postgres_binary_v3'" not in sql
    assert "to_regclass" not in sql


@pytest.mark.asyncio
async def test_explicit_snapshot_id_is_revalidated_as_published_sealed_v3():
    class _ScalarResult:
        def scalar(self):
            return "strict-v3-snapshot"

    class _ExplicitSession:
        def __init__(self):
            self.calls = []

        async def execute(self, statement, params):
            self.calls.append((str(statement), dict(params)))
            return _ScalarResult()

    session = _ExplicitSession()
    snapshot_id = await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        session,
        {"snapshot_id": "strict-v3-snapshot"},
    )

    assert snapshot_id == "strict-v3-snapshot"
    sql, params = session.calls[0]
    assert "status = 'published'" in sql
    assert "ptg2_v3_snapshot_binding" in sql
    assert "ptg2_v3_snapshot_layout" in sql
    assert params == {"snapshot_id": "strict-v3-snapshot"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "selected_lookup"),
    [
        ({"plan_id": "plan"}, "current_source_snapshot_id_for_plan"),
        ({"source_key": "source"}, "current_source_snapshot_id"),
    ],
)
async def test_plan_and_source_resolution_never_fall_back_to_global(
    monkeypatch,
    args,
    selected_lookup,
):
    plan_lookup = AsyncMock(return_value=None)
    source_lookup = AsyncMock(return_value=None)
    global_lookup = AsyncMock(
        side_effect=AssertionError("scoped misses must fail closed")
    )
    monkeypatch.setattr(
        ptg2_snapshot, "current_source_snapshot_id_for_plan", plan_lookup
    )
    monkeypatch.setattr(ptg2_snapshot, "current_source_snapshot_id", source_lookup)
    monkeypatch.setattr(ptg2_snapshot, "current_snapshot_id", global_lookup)

    snapshot_id = await ptg2_snapshot.resolve_current_ptg2_snapshot_id(object(), args)

    assert snapshot_id is None
    selected_probe = {
        "current_source_snapshot_id_for_plan": plan_lookup,
        "current_source_snapshot_id": source_lookup,
    }[selected_lookup]
    selected_probe.assert_awaited_once()
    global_lookup.assert_not_awaited()


@pytest.mark.parametrize(
    ("field_name", "field_value", "error_text"),
    [
        ("storage_generation", None, "storage_generation=shared_blocks_v3"),
        ("cold_lookup_contract", None, "cold_lookup_contract=ptg_v3_cold_v2"),
        ("shared_snapshot_key", 0, "positive shared_snapshot_key"),
        ("shared_snapshot_key", True, "positive shared_snapshot_key"),
    ],
)
def test_strict_v3_manifest_missing_contract_fails_closed(
    field_name, field_value, error_text
):
    manifest = _strict_serving_index()
    manifest[field_name] = field_value

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match=error_text):
        ptg2_tables._strict_v3_manifest_fields(manifest)


@pytest.mark.asyncio
async def test_shared_payload_reads_have_no_binary_cache(monkeypatch):
    block_key = (7 << 31) | (5 // 1024)
    fetch = AsyncMock(
        return_value={
            block_key: (
                SharedBlockPayload(
                    block_key=block_key,
                    fragment_no=0,
                    entry_count=1,
                    payload=b"payload",
                ),
            )
        }
    )
    monkeypatch.setattr(ptg2_db_sidecars, "fetch_shared_blocks", fetch)
    assert not hasattr(ptg2_db_sidecars, "_BINARY_BLOCK_CACHE")
    assert not hasattr(ptg2_db_sidecars, "_BINARY_DICTIONARY_CACHE")

    for _ in range(2):
        payload_rows = await ptg2_db_sidecars._serving_binary_payload_rows(
            object(),
            shared_snapshot_key=41,
            artifact_kind="by_code_provider_shard_v1",
            block_key=block_key,
            schema_name="mrf",
        )
        assert payload_rows[0]["_decoded_payload"] == b"payload"

    assert fetch.await_count == 2
    assert all(call.kwargs["snapshot_key"] == 41 for call in fetch.await_args_list)
    assert all(
        call.kwargs["object_kind"] == "by_code_provider_shard_v1"
        for call in fetch.await_args_list
    )


@pytest.mark.asyncio
async def test_shared_dictionary_read_passes_snapshot_key_and_never_caches(monkeypatch):
    dictionary_bytes = bytes.fromhex("00" * 15 + "01") + bytes.fromhex(
        "00" * 15 + "02"
    )
    fetch = AsyncMock(
        return_value={
            0: (
                SharedBlockPayload(
                    block_key=0,
                    fragment_no=0,
                    entry_count=2,
                    payload=dictionary_bytes,
                ),
            )
        }
    )
    monkeypatch.setattr(ptg2_db_sidecars, "fetch_shared_blocks", fetch)

    first = await ptg2_db_sidecars._serving_binary_dictionary_values_for_keys(
        object(),
        shared_snapshot_key=41,
        artifact_kind="by_code_price_dictionary",
        item_keys=(1,),
        item_count=2,
        block_bytes=32,
        schema_name="mrf",
    )
    second = await ptg2_db_sidecars._serving_binary_dictionary_values_for_keys(
        object(),
        shared_snapshot_key=41,
        artifact_kind="by_code_price_dictionary",
        item_keys=(1,),
        item_count=2,
        block_bytes=32,
        schema_name="mrf",
    )

    assert first == second == {1: "00000000000000000000000000000002"}
    assert fetch.await_count == 2
    assert fetch.await_args.kwargs["snapshot_key"] == 41
    assert fetch.await_args.kwargs["block_keys"] == (0,)
    assert fetch.await_args.kwargs["fragment_nos"] == (0,)


@pytest.mark.asyncio
async def test_large_shared_dictionary_reads_only_the_requested_tail_fragment(
    monkeypatch,
):
    item_count = 29_000_000
    entries_per_fragment = 4096
    item_key = item_count - 1
    fragment_no = item_key // entries_per_fragment
    fragment_start = fragment_no * entries_per_fragment
    fragment_entries = item_count - fragment_start
    expected = bytes.fromhex("0123456789abcdef0123456789abcdef")
    dictionary_fragment = bytearray(fragment_entries * 16)
    offset = (item_key - fragment_start) * 16
    dictionary_fragment[offset : offset + 16] = expected
    fetch = AsyncMock(
        return_value={
            0: (
                SharedBlockPayload(
                    block_key=0,
                    fragment_no=fragment_no,
                    entry_count=fragment_entries,
                    payload=bytes(dictionary_fragment),
                ),
            )
        }
    )
    monkeypatch.setattr(ptg2_db_sidecars, "fetch_shared_blocks", fetch)

    dictionary_values = await ptg2_db_sidecars._serving_binary_dictionary_values_for_keys(
        object(),
        shared_snapshot_key=41,
        artifact_kind="by_code_price_dictionary",
        item_keys=(item_key,),
        item_count=item_count,
        block_bytes=entries_per_fragment * 16,
        schema_name="mrf",
    )

    assert dictionary_values == {item_key: expected.hex()}
    assert fetch.await_args.kwargs["block_keys"] == (0,)
    assert fetch.await_args.kwargs["fragment_nos"] == (fragment_no,)
    assert len(dictionary_fragment) <= entries_per_fragment * 16


@pytest.mark.asyncio
async def test_shared_code_and_provider_support_queries_are_snapshot_scoped():
    """Keep shared-code and provider support reads within one snapshot."""

    provider_session = _Session(
        [
            {
                "provider_set_key": 3,
                "provider_set_global_id_128": bytes.fromhex("00" * 15 + "03"),
            }
        ]
    )
    provider_ids = await ptg2_serving._provider_set_ids_for_keys(
        provider_session,
        _strict_tables(),
        (3,),
    )
    provider_sql, provider_params = provider_session.calls[0]

    code_session = _Session(
        [
            {
                "code_key": 7,
                "plan_id": "plan",
                "reported_code_system": "CPT",
                "reported_code": "99213",
                "negotiation_arrangement": "FFS",
            }
        ]
    )
    code_rows = await ptg2_serving._manifest_reverse_code_rows(
        code_session,
        _strict_tables(),
        requested_plan="plan",
        code_value="99213",
        code_system="CPT",
        q_text="",
        code_context=None,
    )
    code_sql, code_params = code_session.calls[0]

    assert provider_ids == {3: "00000000000000000000000000000003"}
    assert "ptg2_v3_provider_set" in provider_sql
    assert provider_params["shared_snapshot_key"] == 41
    assert code_rows[0]["code_key"] == 7
    assert code_rows[0]["negotiation_arrangement"] == "FFS"
    assert "ptg2_v3_code" in code_sql
    assert "JOIN mrf.ptg2_v3_snapshot_scope physical_scope" in code_sql
    assert "JOIN LATERAL (" in code_sql
    assert "mrf.ptg2_v3_snapshot_plan_scope plan_scope" in code_sql
    assert "physical_scope.snapshot_id = :logical_snapshot_id" in code_sql
    assert "plan_scope.snapshot_id = :logical_snapshot_id" in code_sql
    assert (
        "physical_scope.coverage_scope_id = code_metadata.coverage_scope_id" in code_sql
    )
    assert "logical_scope.plan_id" in code_sql
    assert "code_metadata.negotiation_arrangement" in code_sql
    assert "code_metadata.plan_id" not in code_sql
    assert "snapshot_key = :shared_snapshot_key" in code_sql
    assert "plan_id = :plan_id" in code_sql
    assert "OR COALESCE(plan_id, '') = ''" not in code_sql
    assert code_params["shared_snapshot_key"] == 41
    assert code_params["logical_snapshot_id"] == "logical-snapshot"


@pytest.mark.asyncio
async def test_forward_search_scopes_shared_layout_rows_to_each_logical_plan(
    monkeypatch,
):
    """Ensure shared rows are filtered by each logical snapshot's plan scope."""

    merge_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(ptg2_serving, "_merge_manifest_code_variant_rows", merge_rows)

    shared_layout_key = 41
    logical_plans = (
        ("logical-plan-a", "plan-a"),
        ("logical-plan-b", "plan-b"),
    )
    for logical_snapshot_id, plan_id in logical_plans:
        session = _Session(
            [
                {
                    "code_key": 7,
                    "plan_id": plan_id,
                    "plan_market_type": "group",
                    "reported_code_system": "CPT",
                    "reported_code": "99213",
                    "negotiation_arrangement": "FFS",
                    "rate_count": 1,
                }
            ]
        )
        response = await ptg2_serving._search_manifest_serving_table(
            session,
            logical_snapshot_id,
            {
                "plan_id": plan_id,
                "plan_market_type": "group",
                "code_system": "CPT",
                "code": "99213",
            },
            SimpleNamespace(limit=10, offset=0),
            _strict_tables(
                snapshot_id=logical_snapshot_id, snapshot_key=shared_layout_key
            ),
            "product_search",
        )

        assert response is None
        code_sql, code_params = session.calls[0]
        assert "FROM mrf.ptg2_v3_code code_metadata" in code_sql
        assert "JOIN mrf.ptg2_v3_snapshot_scope physical_scope" in code_sql
        assert "JOIN LATERAL (" in code_sql
        assert "mrf.ptg2_v3_snapshot_plan_scope plan_scope" in code_sql
        assert "physical_scope.snapshot_id = :logical_snapshot_id" in code_sql
        assert "plan_scope.snapshot_id = :logical_snapshot_id" in code_sql
        assert (
            "physical_scope.coverage_scope_id = code_metadata.coverage_scope_id"
            in code_sql
        )
        assert "logical_scope.plan_id" in code_sql
        assert "logical_scope.plan_market_type" in code_sql
        assert "code_metadata.plan_id" not in code_sql
        assert "COALESCE(plan_id" not in code_sql
        assert code_params["logical_snapshot_id"] == logical_snapshot_id
        assert code_params["shared_snapshot_key"] == shared_layout_key
        assert code_params["plan_id"] == plan_id
        assert code_params["plan_market_type"] == "group"

    merged_plan_ids = [
        call.kwargs["code_rows"][0]["plan_id"] for call in merge_rows.await_args_list
    ]
    assert merged_plan_ids == ["plan-a", "plan-b"]
    assert [
        call.kwargs["code_rows"][0]["negotiation_arrangement"]
        for call in merge_rows.await_args_list
    ] == ["FFS", "FFS"]


def _exact_npi_graph_scope():
    return ptg2_serving._ExplicitNpiGraphScope(
        1234567890,
        ("04" * 16,),
        (3,),
    )


def _single_code_metadata_session():
    return _Session(
        [
            {
                "code_key": 7,
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "reported_code_system": "CPT",
                "reported_code": "99213",
                "negotiation_arrangement": "FFS",
                "rate_count": 500,
            }
        ]
    )


def _stub_exact_npi_graph(monkeypatch, provider_set_id):
    match_provider_locations = AsyncMock(
        side_effect=AssertionError(
            "exact NPI lookup must not run generic location traversal"
        )
    )
    expand_provider_members = AsyncMock(
        side_effect=AssertionError("explicit NPI lookup must not expand all members")
    )
    selected_providers_by_set = {
        provider_set_id: [
            {"npi": 1234567890, "provider_name": "Selected provider"}
        ]
    }
    enrich_selected_provider_rows = AsyncMock(
        return_value=selected_providers_by_set
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=_exact_npi_graph_scope()),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        match_provider_locations,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_rows_for_sets",
        expand_provider_members,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_exact_npi_provider_rows_by_set",
        enrich_selected_provider_rows,
    )
    return (
        match_provider_locations,
        expand_provider_members,
        enrich_selected_provider_rows,
    )


def _stub_exact_npi_price_rows(monkeypatch, provider_set_id, price_set_id):
    merge_code_rows = AsyncMock(
        return_value=[
            {
                "serving_content_hash_128": "06" * 16,
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "reported_code_system": "CPT",
                "reported_code": "99213",
                "negotiation_arrangement": "FFS",
                "provider_set_global_id_128": provider_set_id,
                "provider_count": 50_000,
                "price_set_global_id_128": price_set_id,
                "price_key": 9,
                "source_key": 0,
                "network_names": [],
                "_ptg_provider_set_key": 3,
            }
        ]
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_merge_manifest_code_variant_rows",
        merge_code_rows,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_hydrate_provider_set_network_names",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={price_set_id: [{"negotiated_rate": "125.00"}]}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_procedure_details_for_rows",
        AsyncMock(return_value={}),
    )
    return merge_code_rows


@pytest.mark.asyncio
async def test_explicit_npi_search_intersects_provider_sets_before_reading_rows(
    monkeypatch,
):
    """Intersect exact NPI membership before decoding the by-code row blocks."""

    merge_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(ptg2_serving, "_merge_manifest_code_variant_rows", merge_rows)
    provider_set_id = "03" * 16
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=_exact_npi_graph_scope()),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        AsyncMock(
            side_effect=AssertionError(
                "exact NPI lookup must not run generic location traversal"
            )
        ),
    )
    session = _single_code_metadata_session()

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "npi": "1234567890",
            "include_providers": True,
        },
        SimpleNamespace(limit=100, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "product_search",
    )

    assert response is None
    merge_rows.assert_awaited_once()
    assert merge_rows.await_args.kwargs["provider_set_keys"] == [3]
    assert merge_rows.await_args.kwargs["limit"] == 101
    assert merge_rows.await_args.kwargs["offset"] == 0


@pytest.mark.asyncio
async def test_exact_npi_rate_lookup_skips_generic_provider_traversal(
    monkeypatch,
):
    """Use dense NPI membership directly when no geo or taxonomy filter exists."""

    provider_set_id = "03" * 16
    price_set_id = "05" * 16
    merge_rows = _stub_exact_npi_price_rows(
        monkeypatch,
        provider_set_id,
        price_set_id,
    )
    location_matches, broad_rows, provider_enrichment = _stub_exact_npi_graph(
        monkeypatch,
        provider_set_id,
    )

    response = await ptg2_serving._search_manifest_serving_table(
        _single_code_metadata_session(),
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "npi": "1234567890",
            "include_providers": False,
        },
        SimpleNamespace(limit=25, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "exact_source",
    )

    assert response is not None
    assert len(response["items"]) == 1
    assert response["pagination"] == {
        "total": 1,
        "total_is_exact": True,
        "total_lower_bound": 1,
        "limit": 25,
        "offset": 0,
        "page": 1,
        "has_more": False,
    }
    assert merge_rows.await_args.kwargs["provider_set_keys"] == [3]
    assert merge_rows.await_args.kwargs["limit"] == 26
    assert merge_rows.await_args.kwargs["offset"] == 0
    location_matches.assert_not_awaited()
    broad_rows.assert_not_awaited()
    provider_enrichment.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_npi_with_geo_filter_keeps_location_validation(
    monkeypatch,
):
    """Retain address validation when an exact NPI also carries a geo filter."""

    explicit_scope = _exact_npi_graph_scope()
    location_matches = AsyncMock(return_value=(set(), {}))
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=explicit_scope),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        location_matches,
    )

    response = await ptg2_serving._search_manifest_serving_table(
        _Session([]),
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "npi": "1234567890",
            "state": "TN",
            "include_providers": False,
        },
        SimpleNamespace(limit=25, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "exact_source",
    )

    assert response is not None
    assert response["items"] == []
    location_matches.assert_awaited_once()
    assert location_matches.await_args.kwargs["explicit_npi_scope"] is explicit_scope


@pytest.mark.asyncio
async def test_explicit_npi_search_does_not_expand_other_provider_set_members(
    monkeypatch,
):
    """Keep exact NPI responses independent of broad provider-set cardinality."""

    provider_set_id = "03" * 16
    price_set_id = "05" * 16
    merge_rows = _stub_exact_npi_price_rows(monkeypatch, provider_set_id, price_set_id)
    location_matches, broad_rows, selected_provider_rows = _stub_exact_npi_graph(
        monkeypatch,
        provider_set_id,
    )
    session = _single_code_metadata_session()

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "npi": "1234567890",
            "negotiated_rate": "125.00",
            "include_providers": True,
        },
        SimpleNamespace(limit=100, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "exact_source",
    )

    assert response is not None
    assert [provider_record["npi"] for provider_record in response["items"]] == [
        1234567890
    ]
    assert merge_rows.await_args.kwargs["provider_set_keys"] == [3]
    location_matches.assert_not_awaited()
    broad_rows.assert_not_awaited()
    selected_provider_rows.assert_awaited_once()


def _stub_candidate_audit_npi_without_address(
    monkeypatch,
    provider_set_id,
    price_set_id,
):
    """Stub an exact-NPI candidate audit without address matching."""

    _stub_exact_npi_price_rows(monkeypatch, provider_set_id, price_set_id)
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=_exact_npi_graph_scope()),
    )
    location_matches = AsyncMock(
        side_effect=AssertionError(
            "candidate audit must not require address-backed location matching"
        )
    )
    broad_rows = AsyncMock(
        side_effect=AssertionError(
            "candidate audit must not expand unrelated provider-set members"
        )
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        location_matches,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_rows_for_sets",
        broad_rows,
    )
    enrichment = AsyncMock(
        side_effect=AssertionError(
            "candidate audit must not query provider-directory enrichment"
        )
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_enriched_provider_rows_for_npis",
        enrichment,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_source_provenance_for_rows",
        AsyncMock(
            return_value={
                0: {
                    "source_key": 0,
                    "source_type": "in_network",
                    "identity_kind": "raw_container_sha256_v1",
                    "identity_sha256": "1" * 64,
                    "raw_container_sha256": "1" * 64,
                    "logical_json_sha256": None,
                    "logical_hash_deferred": True,
                    "source_trace_set_hash": "2" * 64,
                    "source_trace": [],
                }
            }
        ),
    )
    return location_matches, broad_rows, enrichment


def _candidate_audit_query_args():
    candidate_access = ptg2_candidate_audit.PTG2CandidateAuditAccess(
        snapshot_id="logical-plan-a",
        source_key="logical-source",
        plan_id="plan-a",
        plan_market_type="group",
    )
    return {
        "snapshot_id": "logical-plan-a",
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "source_key": "logical-source",
        "code_system": "CPT",
        "code": "99213",
        "npi": "1234567890",
        "negotiated_rate": "125.00",
        "include_providers": True,
        "include_sources": True,
        ptg2_candidate_audit.PTG2_CANDIDATE_AUDIT_ACCESS_ARG: candidate_access,
    }


@pytest.mark.asyncio
async def test_candidate_audit_exact_npi_does_not_require_an_address(
    monkeypatch,
):
    """Return exact graph evidence even when provider enrichment has no address."""

    provider_set_id = "03" * 16
    price_set_id = "05" * 16
    location_matches, broad_rows, enrichment = _stub_candidate_audit_npi_without_address(
        monkeypatch,
        provider_set_id,
        price_set_id,
    )
    session = _single_code_metadata_session()
    ptg_caches = (
        ptg2_serving._PTG2_NETWORK_SERVING_TABLES_CACHE,
        ptg2_serving._PTG2_PROVIDER_NPI_PREFIX_CACHE,
        ptg2_serving._PTG2_FILTERED_PROVIDER_PREFIX_CACHE,
        ptg2_serving._PTG2_PROVIDER_SET_IDS_BY_NPI_CACHE,
        ptg2_serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE,
    )
    for cache in ptg_caches:
        cache.clear()

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        _candidate_audit_query_args(),
        SimpleNamespace(limit=100, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "exact_source",
    )

    assert response is not None
    assert response["items"][0]["npi"] == 1234567890
    assert response["items"][0]["address_verification"][
        "displayed_address_present"
    ] is False
    location_matches.assert_not_awaited()
    broad_rows.assert_not_awaited()
    enrichment.assert_not_awaited()
    assert all(not cache for cache in ptg_caches)


@pytest.mark.asyncio
async def test_default_forward_response_skips_exact_provenance_query(monkeypatch):
    """Ensure default forward responses neither query nor expose source provenance."""

    price_set_id = "01" * 16
    provider_set_id = "02" * 16
    response_row_by_field = {
        "serving_content_hash_128": "03" * 16,
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "provider_set_global_id_128": provider_set_id,
        "provider_count": 1,
        "price_set_global_id_128": price_set_id,
        "price_key": 9,
        "source_key": 1,
        "network_names": [],
    }
    provenance = AsyncMock(
        side_effect=AssertionError("default responses must not query source provenance")
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[response_row_by_field]),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_hydrate_provider_set_network_names",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "fetch_snapshot_source_provenance",
        provenance,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={price_set_id: []}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_procedure_details_for_rows",
        AsyncMock(return_value={}),
    )
    session = _Session(
        [
            {
                "code_key": 7,
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "reported_code_system": "CPT",
                "reported_code": "99213",
                "negotiation_arrangement": "FFS",
                "rate_count": 1,
            }
        ]
    )

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
        },
        SimpleNamespace(limit=10, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "product_search",
    )

    provenance.assert_not_awaited()
    assert response is not None
    assert "source_key" not in response["items"][0]
    assert "source_artifact_key" not in response["items"][0]


@pytest.mark.asyncio
async def test_source_enabled_forward_response_separates_logical_and_artifact_keys(
    monkeypatch,
):
    """Ensure source-enabled rows distinguish logical and artifact source keys."""

    price_set_id = "01" * 16
    response_row_by_field = {
        "serving_content_hash_128": "03" * 16,
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "provider_set_global_id_128": "02" * 16,
        "provider_count": 1,
        "price_set_global_id_128": price_set_id,
        "price_key": 9,
        "source_key": 1,
        "network_names": [],
    }
    monkeypatch.setattr(
        ptg2_serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[response_row_by_field]),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_hydrate_provider_set_network_names",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_source_provenance_for_rows",
        AsyncMock(
            return_value={
                1: {
                    "source_key": 1,
                    "source_type": "in_network",
                    "identity_kind": "logical_json_sha256_v1",
                    "identity_sha256": "1" * 64,
                    "raw_container_sha256": "2" * 64,
                    "logical_json_sha256": "3" * 64,
                    "logical_hash_deferred": False,
                    "source_trace_set_hash": "4" * 64,
                    "source_trace": [],
                }
            }
        ),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={price_set_id: []}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_procedure_details_for_rows",
        AsyncMock(return_value={}),
    )
    session = _Session(
        [
            {
                "code_key": 7,
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "reported_code_system": "CPT",
                "reported_code": "99213",
                "negotiation_arrangement": "FFS",
                "rate_count": 1,
            }
        ]
    )
    query_by_name = {
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "code_system": "CPT",
        "code": "99213",
        "source_key": "logical-source",
        "include_sources": True,
    }

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        query_by_name,
        SimpleNamespace(limit=10, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "product_search",
    )

    assert response is not None
    assert response["items"][0]["source_key"] == "logical-source"
    assert response["items"][0]["source_artifact_key"] == 1


@pytest.mark.asyncio
async def test_multi_file_forward_rows_keep_per_artifact_source_provenance(
    monkeypatch,
):
    """Ensure multi-file rows retain provenance for their exact source artifact."""

    price_set_ids = ("01" * 16, "02" * 16)
    response_rows = [
        {
            "serving_content_hash_128": f"0{source_key}" * 16,
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "FFS",
            "provider_set_global_id_128": f"1{source_key}" * 16,
            "provider_count": 1,
            "price_set_global_id_128": price_set_ids[source_key - 1],
            "price_key": 8 + source_key,
            "source_key": source_key,
            "network_names": [],
        }
        for source_key in (1, 2)
    ]
    monkeypatch.setattr(
        ptg2_serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=response_rows),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_hydrate_provider_set_network_names",
        AsyncMock(),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_source_provenance_for_rows",
        AsyncMock(
            return_value={
                source_key: {
                    "source_key": source_key,
                    "source_type": "in_network",
                    "identity_kind": "raw_container_sha256_v1",
                    "identity_sha256": str(source_key) * 64,
                    "raw_container_sha256": str(source_key) * 64,
                    "logical_json_sha256": None,
                    "logical_hash_deferred": True,
                    "source_trace_set_hash": str(source_key + 2) * 64,
                    "source_trace": [
                        {"source_file_version_id": f"source-file-{source_key}"}
                    ],
                }
                for source_key in (1, 2)
            }
        ),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={price_set_id: [] for price_set_id in price_set_ids}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_procedure_details_for_rows",
        AsyncMock(return_value={}),
    )
    session = _Session(
        [
            {
                "code_key": 7,
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "reported_code_system": "CPT",
                "reported_code": "99213",
                "negotiation_arrangement": "FFS",
                "rate_count": 2,
            }
        ]
    )

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "source_key": "logical-source",
            "include_sources": True,
        },
        SimpleNamespace(limit=10, offset=0),
        _strict_tables(snapshot_id="logical-plan-a", snapshot_key=41),
        "product_search",
    )

    assert response is not None
    assert {
        (
            response_item["source_artifact_key"],
            response_item["raw_container_sha256"],
            response_item["source_trace"][0]["source_file_version_id"],
        )
        for response_item in response["items"]
    } == {
        (1, "1" * 64, "source-file-1"),
        (2, "2" * 64, "source-file-2"),
    }


def test_shared_v3_response_rows_preserve_negotiation_arrangement():
    code_by_field = {
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "BUNDLE",
    }
    response_row = ptg2_serving._shared_forward_response_row(
        SimpleNamespace(
            code_key=7,
            provider_set_key=3,
            price_set_global_id_128="01" * 16,
            provider_count=2,
            price_key=11,
            source_key=0,
        ),
        "02" * 16,
        code_by_field,
        None,
        [],
    )

    assert response_row["negotiation_arrangement"] == "BUNDLE"
    assert (
        ptg2_serving._compact_item_from_row(response_row, {})[
            "negotiation_arrangement"
        ]
        == "BUNDLE"
    )

    provider_item = ptg2_serving._ptg2_manifest_provider_procedure_item(
        npi=1234567890,
        serving_data=response_row,
        prices=[],
        procedure_detail={},
        provider_context={},
        args={},
    )
    assert provider_item["negotiation_arrangement"] == "BUNDLE"


def test_reverse_provider_items_keep_exact_source_identity_and_do_not_premerge():
    base_by_field = {
        "serving_content_hash_128": "01" * 16,
        "provider_set_global_id_128": "02" * 16,
        "price_set_global_id_128": "03" * 16,
        "provider_count": 2,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "1" * 64,
        "raw_container_sha256": "2" * 64,
        "logical_json_sha256": "3" * 64,
        "logical_hash_deferred": False,
        "source_trace_set_hash": "4" * 64,
        "source_trace": [{"source_file_version_id": "source-file-1"}],
    }
    provider_items = [
        ptg2_serving._ptg2_manifest_provider_procedure_item(
            npi=1234567890,
            serving_data={**base_by_field, "source_key": source_key},
            prices=[],
            procedure_detail={},
            provider_context={},
            args={"source_key": "logical-source"},
        )
        for source_key in (0, 1)
    ]

    assert provider_items[0]["source_key"] == "logical-source"
    assert provider_items[0]["source_artifact_key"] == 0
    assert provider_items[0]["identity_sha256"] == "1" * 64
    assert provider_items[0]["source_trace"] == [
        {"source_file_version_id": "source-file-1"}
    ]
    assert len(ptg2_serving._merge_ptg2_provider_rate_items(provider_items)) == 2


@pytest.mark.asyncio
async def test_exact_source_provenance_is_one_selected_key_query_without_union():
    session = _Session(
        [
            {
                "source_key": 1,
                "source_type": "in_network",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": "1" * 64,
                "raw_container_sha256": "2" * 64,
                "logical_json_sha256": "3" * 64,
                "logical_hash_deferred": False,
                "source_trace_set_hash": "4" * 64,
                "source_count": 2,
                "distinct_source_count": 2,
                "minimum_source_key": 0,
                "maximum_source_key": 1,
                "trace_hash_count": 1,
                "resolved_trace_count": 1,
                "source_trace": [
                    {
                        "source_file_version_id": "selected-source",
                        "json_pointer": "/in_network/1",
                    }
                ],
            }
        ]
    )

    provenance = await ptg2_shared_blocks.fetch_snapshot_source_provenance(
        session,
        schema_name="mrf",
        logical_snapshot_id="logical-snapshot",
        source_keys=(1,),
        expected_source_count=2,
    )

    assert provenance[1]["source_trace"] == [
        {
            "source_file_version_id": "selected-source",
            "json_pointer": "/in_network/1",
        }
    ]
    sql, params = session.calls[0]
    assert "ptg2_v3_snapshot_source" in sql
    assert "source_key = ANY(CAST(:source_keys AS integer[]))" in sql
    assert "ptg2_source_trace_set" in sql
    assert params == {"snapshot_id": "logical-snapshot", "source_keys": (1,)}


@pytest.mark.asyncio
async def test_shaped_row_provenance_lookup_uses_source_artifact_key(monkeypatch):
    fetch = AsyncMock(return_value={1: {"source_key": 1}})
    monkeypatch.setattr(
        ptg2_serving,
        "fetch_snapshot_source_provenance",
        fetch,
    )

    provenance = await ptg2_serving._ptg2_source_provenance_for_rows(
        object(),
        _strict_tables(),
        [{"source_key": "logical-source", "source_artifact_key": 1}],
    )

    assert provenance == {1: {"source_key": 1}}
    assert fetch.await_args.kwargs["source_keys"] == {1}


@pytest.mark.asyncio
async def test_location_rate_provider_lookup_uses_logical_plan_scope(monkeypatch):
    """Ensure location-rate candidates use the logical snapshot's plan scope."""

    provider_group_id = "00000000000000000000000000000009"
    monkeypatch.setattr(
        ptg2_serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_forward_entries_for_code_rows",
        AsyncMock(return_value=[SimpleNamespace(provider_set_key=3)]),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_group_ids_for_set_keys",
        AsyncMock(return_value=(provider_group_id,)),
    )
    expected_candidates = object()
    graph_candidates = AsyncMock(return_value=expected_candidates)
    monkeypatch.setattr(
        ptg2_serving, "_graph_candidates_for_rate_scope", graph_candidates
    )
    session = _Session(
        [
            {
                "code_key": 7,
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "reported_code_system": "CPT",
                "reported_code": "99213",
            }
        ]
    )

    candidates = await ptg2_serving._graph_candidates_for_request(
        session,
        _strict_tables(snapshot_id="logical-plan-a"),
        {"plan_market_type": "group"},
        requested_code="99213",
        requested_system="CPT",
        plan_id="plan-a",
        candidate_limit=25,
    )

    assert candidates is expected_candidates
    code_sql, code_params = session.calls[0]
    assert "FROM mrf.ptg2_v3_code code_metadata" in code_sql
    assert "JOIN mrf.ptg2_v3_snapshot_scope physical_scope" in code_sql
    assert "JOIN LATERAL (" in code_sql
    assert "mrf.ptg2_v3_snapshot_plan_scope plan_scope" in code_sql
    assert "physical_scope.snapshot_id = :logical_snapshot_id" in code_sql
    assert "plan_scope.snapshot_id = :logical_snapshot_id" in code_sql
    assert (
        "physical_scope.coverage_scope_id = code_metadata.coverage_scope_id" in code_sql
    )
    assert "logical_scope.plan_id" in code_sql
    assert "logical_scope.plan_market_type" in code_sql
    assert "code_metadata.plan_id" not in code_sql
    assert "COALESCE(plan_id" not in code_sql
    assert code_params["logical_snapshot_id"] == "logical-plan-a"
    assert code_params["shared_snapshot_key"] == 41
    assert code_params["plan_id"] == "plan-a"
    assert code_params["plan_market_type"] == "group"
    graph_candidates.assert_awaited_once()


@pytest.mark.asyncio
async def test_plan_code_proof_never_bypasses_strict_validation_for_session_shape(
    monkeypatch,
):
    plan_code_probe = AsyncMock(return_value=False)
    monkeypatch.setattr(ptg2_serving, "_has_ptg2_table_plan_code", plan_code_probe)
    session = object()
    tables = _strict_tables()

    has_route = await ptg2_serving._has_snapshot_plan_code(
        session,
        "snapshot-id",
        {"plan_id": "plan", "code_system": "CPT", "code": "99213"},
        serving_tables=tables,
    )

    assert has_route is False
    plan_code_probe.assert_awaited_once()


@pytest.mark.asyncio
async def test_route_proof_joins_logical_scope_and_fails_closed_on_no_row():
    class _ScalarResult:
        def scalar(self):
            return False

    class _ScalarSession:
        def __init__(self):
            self.calls = []

        async def execute(self, statement, params):
            self.calls.append((str(statement), dict(params)))
            return _ScalarResult()

    session = _ScalarSession()
    has_route = await ptg2_serving._has_snapshot_plan_code(
        session,
        "logical-plan-a",
        {
            "plan_id": "plan-a",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
        },
        serving_tables=_strict_tables(snapshot_id="logical-plan-a"),
    )

    assert has_route is False
    proof_sql, proof_params = session.calls[0]
    assert "FROM mrf.ptg2_v3_code code_metadata" in proof_sql
    assert "JOIN mrf.ptg2_v3_snapshot_scope physical_scope" in proof_sql
    assert "JOIN LATERAL (" in proof_sql
    assert "mrf.ptg2_v3_snapshot_plan_scope plan_scope" in proof_sql
    assert "physical_scope.snapshot_id = :logical_snapshot_id" in proof_sql
    assert "plan_scope.snapshot_id = :logical_snapshot_id" in proof_sql
    assert (
        "physical_scope.coverage_scope_id = code_metadata.coverage_scope_id" in proof_sql
    )
    assert "plan_scope.plan_id = :plan_id" in proof_sql
    assert "plan_scope.plan_market_type = :plan_market_type" in proof_sql
    assert "code_metadata.plan_id" not in proof_sql
    assert "COALESCE(plan_id" not in proof_sql
    assert proof_params == {
        "logical_snapshot_id": "logical-plan-a",
        "plan_id": "plan-a",
        "plan_market_type": "group",
        "shared_snapshot_key": 41,
        "reported_code": "99213",
        "reported_code_system": "CPT",
    }


@pytest.mark.asyncio
async def test_route_proof_does_not_swallow_schema_errors():
    class _SchemaErrorSession:
        def __init__(self):
            self.rollback = AsyncMock()

        async def execute(self, _statement, _params):
            raise RuntimeError("undefined column in strict V3 schema")

    session = _SchemaErrorSession()
    with pytest.raises(RuntimeError, match="undefined column"):
        await ptg2_serving._has_snapshot_plan_code(
            session,
            "logical-plan-a",
            {"plan_id": "plan-a", "code_system": "CPT", "code": "99213"},
            serving_tables=_strict_tables(snapshot_id="logical-plan-a"),
        )

    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_page_call_site_passes_snapshot_key(monkeypatch):
    page_lookup = AsyncMock(return_value=None)
    monkeypatch.setattr(ptg2_serving, "lookup_shared_code_page_from_db", page_lookup)
    session = object()

    result = await ptg2_serving._version_three_forward_page_rows(
        session,
        _strict_tables(),
        code_metadata={"code_key": 7, "rate_count": 1},
        source_trace_set_hash=None,
        network_names=[],
        limit=1,
        offset=0,
    )

    assert result is None
    page_lookup.assert_awaited_once_with(
        session,
        41,
        7,
        source_count=1,
        schema_name="mrf",
    )


@pytest.mark.asyncio
async def test_shared_dispatch_has_no_filesystem_manifest_loader(monkeypatch):
    db_search = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(
        ptg2_serving, "_search_manifest_serving_table", db_search
    )
    assert not hasattr(ptg2_serving, "search_ptg2_manifest_serving_snapshot")
    assert not hasattr(ptg2_serving, "_resolve_ptg2_manifest_sidecar_path")
    session = object()
    pagination = object()
    tables = _strict_tables()

    search_result = await ptg2_serving.search_ptg2_serving_table(
        session,
        "snapshot-id",
        {"plan_id": "plan", "code": "99213"},
        pagination,
        serving_tables=tables,
    )

    assert search_result == {"items": []}
    db_search.assert_awaited_once_with(
        session,
        "snapshot-id",
        {"plan_id": "plan", "code": "99213"},
        pagination,
        tables,
        "product_search",
    )


@pytest.mark.asyncio
async def test_exact_source_mode_uses_the_strict_shared_dispatch(monkeypatch):
    db_search = AsyncMock(return_value={"items": []})
    monkeypatch.setattr(
        ptg2_serving, "_search_manifest_serving_table", db_search
    )
    session = object()
    pagination = object()
    tables = _strict_tables()
    query_by_name = {
        "mode": "exact_source",
        "snapshot_id": "snapshot-id",
        "code": "99213",
    }

    search_result = await ptg2_serving.search_ptg2_serving_table(
        session,
        "snapshot-id",
        query_by_name,
        pagination,
        serving_tables=tables,
    )

    assert search_result == {"items": []}
    db_search.assert_awaited_once_with(
        session,
        "snapshot-id",
        query_by_name,
        pagination,
        tables,
        "exact_source",
    )


@pytest.mark.asyncio
async def test_shared_graph_wrapper_is_one_dense_query_and_keeps_missing_owners(
    monkeypatch,
):
    graph_fetch = AsyncMock(return_value={7: (3, 5), 9: ()})
    monkeypatch.setattr(ptg2_db_sidecars, "fetch_shared_graph_members", graph_fetch)
    session = object()

    members = await ptg2_db_sidecars.lookup_shared_graph_members_from_db(
        session,
        41,
        4,
        (7, 9),
        schema_name="mrf",
    )

    assert members == {7: (3, 5), 9: ()}
    graph_fetch.assert_awaited_once_with(
        session,
        schema_name="mrf",
        snapshot_key=41,
        direction=4,
        owner_keys=(7, 9),
    )


@pytest.mark.asyncio
async def test_all_four_serving_graph_directions_use_dense_keys(monkeypatch):
    provider_set_id = "00000000000000000000000000000007"
    provider_group_id = "00000000000000000000000000000009"
    npi = 1234567890
    npi_id = ptg2_serving._ptg2_npi_member_id(npi)
    graph_fetch = AsyncMock(
        side_effect=[
            {npi: (9,)},
            {9: (npi,)},
            {9: (7,)},
            {7: (9,)},
        ]
    )
    monkeypatch.setattr(
        ptg2_serving, "lookup_shared_graph_members_from_db", graph_fetch
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={provider_set_id: 7}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={7: provider_set_id}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_provider_group_keys_for_ids",
        AsyncMock(return_value={provider_group_id: 9}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_shared_provider_group_ids_for_keys",
        AsyncMock(return_value={9: provider_group_id}),
    )
    session = object()
    tables = _strict_tables()

    assert await ptg2_serving._shared_graph_members_many(
        session, tables, "provider_npi_group", [npi_id], max_members=None
    ) == {npi_id: (provider_group_id,)}
    assert await ptg2_serving._shared_graph_members_many(
        session, tables, "provider_group_npi", [provider_group_id], max_members=None
    ) == {provider_group_id: (npi_id,)}
    assert await ptg2_serving._shared_graph_members_many(
        session, tables, "provider_inverted", [provider_group_id], max_members=None
    ) == {provider_group_id: (provider_set_id,)}
    assert await ptg2_serving._shared_graph_members_many(
        session, tables, "provider_forward", [provider_set_id], max_members=None
    ) == {provider_set_id: (provider_group_id,)}

    assert [call.args[2] for call in graph_fetch.await_args_list] == [1, 2, 3, 4]
    assert [tuple(call.args[3]) for call in graph_fetch.await_args_list] == [
        (npi,),
        (9,),
        (9,),
        (7,),
    ]
