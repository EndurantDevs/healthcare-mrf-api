# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL and contract verification for GIN-indexed network and plan state filtering."""

from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sanic.exceptions import BadRequest, ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.endpoint.network_catalog import _catalog_reader_error
from api.endpoint.plan import find_a_plan
from api.network_catalog_fhir_query import parse_fhir_catalog_query
from process import registry_fhir_catalog_read as fhir_catalog
from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    bind_compact_network_stats,
)
from process.network_stats_contract import (
    CONTRACT,
    ROLLUP_SPECS,
    STATS_TABLE_ORDER,
    _expected_btree_index_map,
    _expected_index_map,
    _validated_descriptor,
    fetch_network_stats_index_records,
    has_network_stats_state_gin,
    stats_digest,
)
from process.network_stats_sql import create_stats_indexes_sql, create_stats_tables_sql
from process.plan_network_state_filter import (
    PlanNetworkStatsUnavailable,
    parse_plan_network_state_params,
    resolve_plan_network_state_clause,
)
from tests.network_compact_address_postgres_support import _manifest
from tests.test_network_compact_address_postgres import compact_db as compact_db
from tests.test_network_membership_slices_postgres import slice_db as slice_db


def _dummy_manifest(layout: CompactNetworkLayout | None, *, approved_revision: int = 1):
    manifest = _manifest(layout)
    object.__setattr__(manifest, "approved_custom_revision", approved_revision)
    return manifest


def test_gin_indexes_included_in_stats_ddl():
    """Verify both states and core_states GIN indexes are created on network_stats_summary."""
    ddl_statements = create_stats_indexes_sql("network_candidate_" + "1" * 32)
    assert len(ddl_statements) == 11
    assert any(
        "network_stats_summary_states_gin_idx" in stmt and "USING gin (states)" in stmt for stmt in ddl_statements
    )
    assert any(
        "network_stats_summary_core_states_gin_idx" in stmt and "USING gin (core_states)" in stmt
        for stmt in ddl_statements
    )


def _signed_stats_receipt(cid: str, schema_name: str, index_list: list[dict]) -> dict:
    unsigned_by_field = {
        "contract": CONTRACT,
        "scope": {
            "dataset_id": str(UUID(int=1)),
            "schema_id": str(UUID(int=2)),
            "producer_id": str(UUID(int=3)),
            "candidate_id": cid,
            "schema_name": schema_name,
        },
        "tables": list(STATS_TABLE_ORDER),
        "counts": {
            "specialty_dim_rows": len(ROLLUP_SPECS),
            "summary_rows": 1,
            "state_rows": 1,
            "specialty_rows": 1,
            "state_specialty_rows": 1,
        },
        "input_layout": {
            "ref_map": {"schema_name": schema_name, "table_name": "network_slice_ref"},
            "base": {"schema_name": "published_addresses", "table_name": "entity_address_unified"},
            "artifacts": [{"candidate_id": cid, "schema_name": schema_name}],
            "custom": None,
        },
        "indexes": index_list,
    }
    return {**unsigned_by_field, "sha256": stats_digest(unsigned_by_field)}


def test_nine_entry_stats_receipt_stays_readable_while_new_candidates_require_gin():
    """Verify persisted 9-entry receipts remain valid for unfiltered reads while new builds require 11."""
    cid = str(UUID(int=9))
    schema_name = "network_candidate_" + UUID(cid).hex
    nine_indexes = [
        {"table_name": tbl, "name": idx_name, "definition": idx_def}
        for idx_name, (tbl, idx_def) in sorted(_expected_btree_index_map(schema_name).items())
    ]
    eleven_indexes = [
        {"table_name": tbl, "name": idx_name, "definition": idx_def}
        for idx_name, (tbl, idx_def) in sorted(_expected_index_map(schema_name).items())
    ]
    legacy_receipt = _signed_stats_receipt(cid, schema_name, nine_indexes)
    validated_legacy = _validated_descriptor(legacy_receipt)
    assert has_network_stats_state_gin(validated_legacy) is False
    with pytest.raises(ValueError):
        _validated_descriptor(legacy_receipt, require_state_gin=True)

    full_receipt = _signed_stats_receipt(cid, schema_name, eleven_indexes)
    validated_full = _validated_descriptor(full_receipt, require_state_gin=True)
    assert has_network_stats_state_gin(validated_full) is True

    legacy_layout = bind_compact_network_stats(
        CompactNetworkLayout(
            CompactAddressRelation(schema_name, "network_slice_ref"),
            CompactAddressRelation("published_addresses", "entity_address_unified"),
            (CompactEdgeArtifact(cid, schema_name),),
        ),
        schema_name,
        state_gin=False,
    )
    assert legacy_layout.network_stats_schema == schema_name
    assert legacy_layout.network_stats_state_search is None
    with pytest.raises(fhir_catalog.RegistryNetworkCatalogError, match="stats_unavailable"):
        fhir_catalog._catalog_page_statement(
            {"schema_name": "fhir_catalog_" + "a" * 32, "metadata_generation": "c" * 64},
            _dummy_manifest(legacy_layout),
            query=fhir_catalog.FHIRCatalogQuery(state=("CA",)),
        )


def test_fhir_catalog_state_query_parsing_and_optional_modes():
    """Verify state_mode and state_scope are optional and reject orphan or malformed inputs."""
    sel_default, _ = parse_fhir_catalog_query({"view": "fhir-plans-v1", "state": ["NY", "CA"]}, "list")
    assert sel_default.query.state == ("CA", "NY")
    assert sel_default.query.state_mode == "any"
    assert sel_default.query.state_scope == "any"

    sel_csv, _ = parse_fhir_catalog_query(
        {"view": "fhir-plans-v1", "state": "tx, fl", "state_mode": "all", "state_scope": "core"},
        "list",
    )
    assert sel_csv.query.state == ("FL", "TX")
    assert sel_csv.query.state_mode == "all"
    assert sel_csv.query.state_scope == "core"

    for bad_doc in (
        {"view": "fhir-plans-v1", "state_mode": "all"},
        {"view": "fhir-plans-v1", "state_scope": "core"},
        {"view": "fhir-plans-v1", "state": []},
        {"view": "fhir-plans-v1", "state": ["CA", "CA"]},
        {"view": "fhir-plans-v1", "state": ["bad!"]},
        {"view": "fhir-plans-v1", "state": ["CA"], "state_mode": "exact"},
        {"view": "fhir-plans-v1", "state": ["CA"], "state_scope": "primary"},
    ):
        with pytest.raises((ValueError, fhir_catalog.RegistryNetworkCatalogError)):
            parse_fhir_catalog_query(bad_doc, "list")

    sel_detail, _ = parse_fhir_catalog_query(
        {
            "view": "fhir-plans-v1",
            "network_id": 5,
            "state": ["NY", "CA"],
            "relationship_offset": 5,
            "metadata_generation": "c" * 64,
            "continuation_scope_sha256": "d" * 64,
        },
        "detail",
    )
    assert sel_detail.network_id == 5
    assert sel_detail.query.state == ("CA", "NY")
    assert sel_detail.query.state_mode == "any"
    assert sel_detail.query.state_scope == "any"
    assert sel_detail.query.relationship_offset == 5
    assert sel_detail.query.limit == 1
    assert sel_detail.query.offset == 0


def test_fhir_catalog_continuation_digest_and_sql_generation():
    """Verify state-free continuation scope is unchanged while state queries bind GIN predicates."""
    from process.registry_catalog_policy import RegistryOrganizationVisibility

    cid = str(UUID(int=42))
    schema_name = "network_candidate_" + UUID(cid).hex
    g8_layout = bind_compact_network_stats(
        CompactNetworkLayout(
            CompactAddressRelation(schema_name, "network_slice_ref"),
            CompactAddressRelation("published_addresses", "entity_address_unified"),
            (CompactEdgeArtifact(cid, schema_name),),
        ),
        schema_name,
    )
    manifest = _dummy_manifest(g8_layout)
    snapshot_by_field = {"schema_name": "fhir_catalog_" + "a" * 32, "metadata_generation": "c" * 64}
    policy = RegistryOrganizationVisibility()

    unfiltered = fhir_catalog.FHIRCatalogQuery()
    filtered_any = fhir_catalog.FHIRCatalogQuery(state=("CA", "NV"))
    filtered_all_core = fhir_catalog.FHIRCatalogQuery(state=("CA", "NV"), state_mode="all", state_scope="core")

    digest_unfiltered = fhir_catalog._continuation_scope(unfiltered, manifest, snapshot_by_field, (), policy, None)
    digest_any = fhir_catalog._continuation_scope(filtered_any, manifest, snapshot_by_field, (), policy, None)
    digest_all_core = fhir_catalog._continuation_scope(filtered_all_core, manifest, snapshot_by_field, (), policy, None)
    assert len({digest_unfiltered, digest_any, digest_all_core}) == 3

    detail_cont = fhir_catalog._prepare_read_query(
        fhir_catalog.FHIRCatalogQuery(
            state=("CA", "NV"),
            relationship_offset=5,
            metadata_generation="c" * 64,
            continuation_scope_sha256=digest_any,
        ),
        101,
    )
    assert fhir_catalog._continuation_scope(detail_cont, manifest, snapshot_by_field, (), policy, None) == digest_any

    sql_any, params_any = fhir_catalog._catalog_page_statement(snapshot_by_field, manifest, query=filtered_any)
    assert 'st.states && ($20::text[] COLLATE "C")' in sql_any
    assert params_any == (["CA", "NV"],)

    sql_all_core, params_all_core = fhir_catalog._catalog_page_statement(
        snapshot_by_field, manifest, query=filtered_all_core
    )
    assert 'st.core_states @> ($20::text[] COLLATE "C")' in sql_all_core
    assert params_all_core == (["CA", "NV"],)

    pre_g8 = CompactNetworkLayout(
        CompactAddressRelation(schema_name, "network_slice_ref"),
        CompactAddressRelation("published_addresses", "entity_address_unified"),
        (CompactEdgeArtifact(cid, schema_name),),
    )
    with pytest.raises(fhir_catalog.RegistryNetworkCatalogError, match="stats_unavailable"):
        fhir_catalog._catalog_page_statement(snapshot_by_field, _dummy_manifest(pre_g8), query=filtered_any)


def test_catalog_reader_error_maps_stats_unavailable_by_endpoint_kind():
    """Verify /catalog/stats returns 404 while collection /catalog returns 503 when stats are absent."""
    err = fhir_catalog.RegistryNetworkCatalogError("registry_network_catalog_stats_unavailable")
    stats_resp = _catalog_reader_error(err, "stats", True, "d" * 64)
    assert stats_resp.status == 404
    assert json.loads(stats_resp.body) == {"error": {"code": "network_stats_unavailable"}}

    list_resp = _catalog_reader_error(err, "list", True, "d" * 64, enriched=True)
    assert list_resp.status == 503
    assert json.loads(list_resp.body) == {"error": {"code": "network_catalog_unavailable"}}


def test_plan_network_state_param_validation():
    """Verify network_state_mode and network_state_scope are optional and validate bounds."""
    assert parse_plan_network_state_params({}) == ((), "any", "any")
    assert parse_plan_network_state_params({"network_state": " ny , ca , NY "}) == (("CA", "NY"), "any", "any")
    assert parse_plan_network_state_params(
        {"network_state": "CA,TX", "network_state_mode": "ALL", "network_state_scope": "CORE"}
    ) == (("CA", "TX"), "all", "core")

    for invalid_args in (
        {"network_state": ""},
        {"network_state": "CA,,NY"},
        {"network_state": "BAD!"},
        {"network_state_mode": "all"},
        {"network_state_scope": "core"},
        {"network_state": "CA", "network_state_mode": "bogus"},
        {"network_state": "CA", "network_state_scope": "bogus"},
    ):
        with pytest.raises(BadRequest):
            parse_plan_network_state_params(invalid_args)


async def _seed_stats_and_verify_gin_plan(conn, stats_schema: str) -> None:
    """Create candidate stats tables and prove GIN Bitmap Index Scans on states and core_states."""
    for stmt in create_stats_tables_sql(stats_schema):
        await conn.execute(stmt)
    await conn.execute(
        f"""INSERT INTO "{stats_schema}".network_stats_summary (
            network_id, total_npi_count, office_npi_count, provider_only_npi_count,
            individual_npi_count, organization_npi_count, unique_address_count, unique_location_count,
            state_count, states, core_state_count, core_states, county_count, zip5_count
        ) VALUES
            (101, 200, 200, 0, 180, 20, 100, 120, 2, ARRAY['CA','NV'], 1, ARRAY['CA'], 5, 20),
            (102, 300, 300, 0, 280, 20, 150, 180, 2, ARRAY['CA','NV'], 2, ARRAY['CA','NV'], 8, 30),
            (103, 150, 150, 0, 140, 10, 80, 90, 1, ARRAY['TX'], 1, ARRAY['TX'], 4, 15)"""
    )
    for stmt in create_stats_indexes_sql(stats_schema):
        await conn.execute(stmt)
    fetched_indexes = await fetch_network_stats_index_records(conn, stats_schema)
    idx_names = {entry["name"] for entry in fetched_indexes}
    assert "network_stats_summary_states_gin_idx" in idx_names
    assert "network_stats_summary_core_states_gin_idx" in idx_names

    await conn.execute(f'ANALYZE "{stats_schema}".network_stats_summary')
    async with conn.transaction():
        await conn.execute("SET LOCAL enable_seqscan = off")
        plan_any = await conn.fetch(
            f'EXPLAIN (COSTS OFF) SELECT network_id FROM ONLY "{stats_schema}".network_stats_summary '
            'WHERE states && ($1::text[] COLLATE "C")',
            ["CA"],
        )
        plan_all_core = await conn.fetch(
            f'EXPLAIN (COSTS OFF) SELECT network_id FROM ONLY "{stats_schema}".network_stats_summary '
            'WHERE core_states @> ($1::text[] COLLATE "C")',
            ["CA", "NV"],
        )
    assert any("network_stats_summary_states_gin_idx" in explain_line[0] for explain_line in plan_any)
    assert any("network_stats_summary_core_states_gin_idx" in explain_line[0] for explain_line in plan_all_core)


async def _insert_aca_fixture_row(conn, ctl_schema: str, mrf_schema: str, fixture_row: tuple) -> None:
    """Insert one synthetic ACA plan, summary, network tier, issuer and approved binding."""
    bkey, nid, pid, pyear, issuer, st_code, csum = fixture_row
    binding_by_field = {
        "network_id": nid,
        "archived": False,
        "source_system": "aca",
        "producer_id": "registry-approved-manual-v1",
        "source_scope_json": {
            "plan_id": pid,
            "plan_year": pyear,
            "issuer_id": issuer,
            "state": st_code,
            "checksum_network": csum,
        },
    }
    await conn.execute(
        f'INSERT INTO "{ctl_schema}".registry_approved_record VALUES ($1, $2, $3, $4::jsonb)',
        1,
        "network_binding",
        bkey,
        json.dumps(binding_by_field),
    )
    await conn.execute(
        f'INSERT INTO "{mrf_schema}".plan (plan_id, year, issuer_id, state, marketing_name) VALUES ($1, $2, $3, $4, $5)',
        pid,
        pyear,
        int(issuer),
        st_code,
        f"Synthetic Plan {pid}",
    )
    await conn.execute(
        f'INSERT INTO "{mrf_schema}".plan_search_summary '
        "(plan_id, year, plan_type, metal_level, is_on_exchange, attributes, plan_benefits) "
        "VALUES ($1, $2, 'PPO', 'Silver', true, '[]'::jsonb, '[]'::jsonb)",
        pid,
        pyear,
    )
    await conn.execute(
        f'INSERT INTO "{mrf_schema}".plan_networktier (plan_id, network_tier, issuer_id, year, checksum_network) '
        "VALUES ($1, 'Standard', $2, $3, $4)",
        pid,
        int(issuer),
        pyear,
        csum,
    )
    await conn.execute(
        f'INSERT INTO "{mrf_schema}".issuer (issuer_id, issuer_name, issuer_marketing_name, state) '
        "VALUES ($1, $2, $2, $3)",
        int(issuer),
        f"Synthetic Issuer {issuer}",
        st_code,
    )


async def _seed_aca_bindings_and_plans(conn, engine, ctl_schema: str, mrf_schema: str, mrf_tables: tuple) -> None:
    """Create isolated control and MRF plan tables populated with synthetic ACA fixtures."""
    await conn.execute(
        f"""CREATE TABLE "{ctl_schema}".registry_approved_record (
            approved_revision bigint NOT NULL,
            record_kind text NOT NULL,
            record_key text NOT NULL,
            record_json jsonb NOT NULL
        )"""
    )
    for nid in (101, 102, 103):
        await conn.execute(
            f'INSERT INTO "{ctl_schema}".registry_approved_record VALUES ($1, $2, $3, $4::jsonb)',
            1,
            "network",
            str(nid),
            json.dumps({"network_id": nid, "display_name": f"Synthetic Network {nid}", "archived": False}),
        )
    async with engine.begin() as ddl_conn:
        for target_table in mrf_tables:
            await ddl_conn.run_sync(lambda sync_c, tbl=target_table: tbl.create(sync_c, checkfirst=True))
    for fixture_row in (
        ("b1", 101, "11111CA0000001", 2026, "11111", "CA", 5001),
        ("b2", 102, "22222CA0000002", 2026, "22222", "CA", 5002),
        ("b3", 103, "33333TX0000003", 2026, "33333", "TX", 5003),
    ):
        await _insert_aca_fixture_row(conn, ctl_schema, mrf_schema, fixture_row)


async def _assert_plan_search_state_cases(sa_session: AsyncSession, pin_holder, pre_g8_manifest, plan_ep) -> None:
    """Assert any/all/core state modes, facet parity and pre-G8 refusal on /plan/search."""
    await sa_session.execute(text("SELECT 1"))

    async def _call_search(query_by_name: dict) -> dict:
        req = SimpleNamespace(args=query_by_name, ctx=SimpleNamespace(sa_session=sa_session))
        resp = await find_a_plan(req)
        return json.loads(resp.body)

    any_nv_by_field = await _call_search({"network_state": "NV"})
    assert any_nv_by_field["total"] == 2
    assert [plan_row["plan_id"] for plan_row in any_nv_by_field["results"]] == ["11111CA0000001", "22222CA0000002"]
    assert any_nv_by_field["applied_filters"]["network_state"] == ["NV"]
    assert any_nv_by_field["applied_filters"]["network_state_mode"] == "any"
    assert any_nv_by_field["applied_filters"]["network_state_scope"] == "any"
    assert any_nv_by_field["facets"]["plan_types"] == [{"value": "PPO", "count": 2}]

    core_all_by_field = await _call_search(
        {"network_state": "CA,NV", "network_state_mode": "all", "network_state_scope": "core"}
    )
    assert core_all_by_field["total"] == 1
    assert [plan_row["plan_id"] for plan_row in core_all_by_field["results"]] == ["22222CA0000002"]
    assert core_all_by_field["facets"]["plan_types"] == [{"value": "PPO", "count": 1}]

    none_fl_by_field = await _call_search({"network_state": "FL"})
    assert none_fl_by_field["total"] == 0 and none_fl_by_field["results"] == []

    pin_holder.current = pre_g8_manifest
    with pytest.raises(ServiceUnavailable):
        await _call_search({"network_state": "CA"})

    with pytest.raises(PlanNetworkStatsUnavailable):
        await resolve_plan_network_state_clause(
            sa_session,
            states=("CA",),
            state_mode="any",
            state_scope="any",
            plan_table=plan_ep.plan_table,
            plan_network_tier_table=plan_ep.plan_network_tier_table,
        )


@pytest.mark.asyncio
async def test_postgres_gin_indexes_and_plan_search_state_filtering(compact_db, monkeypatch):
    """Verify GIN Bitmap Index Scan on network_stats_summary and end-to-end /plan/search state filtering."""
    import api.endpoint.plan as plan_ep
    import process.plan_network_state_filter as pnsf

    conn = compact_db.connection
    cand_uuid = uuid4()
    stats_schema = "network_candidate_" + cand_uuid.hex
    ctl_schema = "ctl_state_" + uuid4().hex[:12]
    mrf_schema = "mrf_state_" + uuid4().hex[:12]
    for schema_id in (stats_schema, ctl_schema, mrf_schema):
        await conn.execute(f'CREATE SCHEMA "{schema_id}"')
    try:
        await _seed_stats_and_verify_gin_plan(conn, stats_schema)
        mrf_tables = (
            plan_ep.plan_table,
            plan_ep.plan_search_summary_table,
            plan_ep.plan_network_tier_table,
            plan_ep.issuer_table,
            plan_ep.plan_prices_table,
        )
        for target_table in mrf_tables:
            monkeypatch.setattr(target_table, "schema", mrf_schema)
        monkeypatch.setattr(pnsf, "registry_schema", lambda: ctl_schema)
        await _seed_aca_bindings_and_plans(conn, compact_db.engine, ctl_schema, mrf_schema, mrf_tables)

        base_layout = CompactNetworkLayout(
            CompactAddressRelation(stats_schema, "network_slice_ref"),
            CompactAddressRelation("published_addresses", "entity_address_unified"),
            (CompactEdgeArtifact(str(cand_uuid), stats_schema),),
        )
        g8_manifest = _dummy_manifest(bind_compact_network_stats(base_layout, stats_schema), approved_revision=1)
        pre_g8_manifest = _dummy_manifest(
            CompactNetworkLayout(base_layout.ref_map, base_layout.base, base_layout.artifacts), approved_revision=1
        )
        pin_holder = SimpleNamespace(current=g8_manifest)

        async def _fake_pin(_driver, _gen, _schema, *, approved=False):
            return pin_holder.current, None, None

        monkeypatch.setattr(pnsf, "read_published_api_pin", _fake_pin)
        async with AsyncSession(compact_db.engine) as sa_session:
            await _assert_plan_search_state_cases(sa_session, pin_holder, pre_g8_manifest, plan_ep)
    finally:
        for schema_id in (mrf_schema, ctl_schema, stats_schema):
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_id}" CASCADE')
