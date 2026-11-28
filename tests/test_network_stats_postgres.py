# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unit, PostgreSQL and HTTP verification for precomputed G8+ network statistics."""

from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sanic import Blueprint

from api.endpoint import network_catalog as catalog_endpoint
from api.endpoint import network_serving as serving_endpoint
from api.request_lifecycle import RequestLifecycleSanic
from process import network_compact_preparation as compact_prep
from process import network_geo_search as geo_search_mod
from process import network_published_api_pin as published_pin
from process import network_stats as stats_builder
from process import network_stats_read as stats_reader
from process.network_compact_layout import (
    INDEPENDENT_STORAGE,
    INDEPENDENT_STORAGE_KEY,
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    bind_compact_geo_search_candidate,
    bind_compact_network_stats,
)
from process.network_compact_readiness import compact_index_digest, compact_serving_heaps
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_serving_indexes import verify_network_search_indexes
from process.network_provider_only_projection import TABLE_NAME as PROVIDER_ONLY_TABLE
from process.network_serving_read import NetworkServingReadUnavailable
from process.network_stats_contract import (
    STATS_TABLES,
    NetworkStatsError,
    network_stats_receipt,
    network_stats_relations,
    validate_network_stats_summary_document,
)
from process.network_stats_read import (
    NetworkStatsDenied,
    NetworkStatsQuery,
    NetworkStatsRequestInvalid,
    NetworkStatsUnavailable,
    parse_network_stats_http_query,
    parse_network_stats_post_query,
    read_network_stats,
)
from tests.network_compact_address_postgres_support import _address_heap, _manifest
from tests.network_published_manifest_support import published_entry as published_entry_with_digest
from tests.test_network_catalog_routes import context as catalog_context
from tests.test_network_catalog_routes import envelope as catalog_envelope
from tests.test_network_compact_address_postgres import compact_db as compact_db
from tests.test_network_membership_slices_postgres import slice_db as slice_db
from tests.test_registry_network_catalog_read import batch_boundary as batch_boundary


def _mock_request(query_string: str = "", body: bytes = b""):
    return SimpleNamespace(query_string=query_string, body=body)


def test_stats_query_parsers():
    """Verify GET and POST parsers, dataclass guards and fanout bounds."""
    parsed_get = parse_network_stats_http_query(
        _mock_request("network_ids=42,7&state=ca,ny&specialty_kind=rollup&specialty_key=primary_care")
    )
    assert parsed_get.network_ids == (7, 42) and parsed_get.state_codes == ("CA", "NY")
    assert parsed_get.include_states and parsed_get.include_specialties and parsed_get.include_state_specialties

    parsed_post, gen_id = parse_network_stats_post_query(
        {
            "network_ids": [99, 12],
            "network_generation": "8",
            "include_states": False,
            "state": ["ca", "tx"],
            "specialty_kind": "rollup",
            "specialty_key": ["primary_care", "dental"],
        }
    )
    assert parsed_post.network_ids == (12, 99) and gen_id == 8
    assert parsed_post.state_codes == ("CA", "TX") and parsed_post.specialty_keys == ("primary_care", "dental")
    assert parse_network_stats_post_query({"network_ids": [7], "state": "ca", "specialty_key": "dental"})[1] is None

    for invalid_qs in (
        "",
        "&&",
        "network_ids=",
        "network_ids=0",
        "network_ids=1&network_ids=2",
        "network_ids=1&unknown=1",
        "network_ids=1&network_generation=0",
        "network_ids=1&include_states=yes",
        "network_ids=1&state=",
        "network_ids=1&state=INVALID_STATE_NAME",
        "network_ids=1&specialty_kind=unknown",
        "network_ids=1&specialty_key=a,,b",
        "network_ids=1&specialty_key=%FF",
        "network_ids=" + ",".join(str(idx) for idx in range(1, 13)) + "&include_specialties=true",
        "network_ids=1,2,3,4,5,6&include_state_specialties=true",
    ):
        with pytest.raises(NetworkStatsRequestInvalid):
            parse_network_stats_http_query(_mock_request(invalid_qs))
    assert parse_network_stats_http_query(_mock_request("network_ids=1&specialty_key=%EF%BF%BD")).specialty_keys == (
        "\ufffd",
    )
    with pytest.raises(NetworkStatsRequestInvalid):
        parse_network_stats_http_query(_mock_request("network_ids=1", body=b"{}"))


def test_stats_post_query_parser():
    """Verify POST query parser, UTF-8 surrogate guard and dataclass bounds."""
    with pytest.raises(NetworkStatsRequestInvalid):
        parse_network_stats_post_query({"network_ids": [1], "specialty_key": ["\ud800"]})

    for bad_post_by_field in (
        "bad",
        {},
        {"network_ids": []},
        {"network_ids": [1, 1]},
        {"network_ids": [1], "network_generation": 8},
        {"network_ids": [1], "state": 123},
        {"network_ids": [1], "include_states": "yes"},
        {"network_ids": [1, 2], "generation_id": 0},
    ):
        with pytest.raises(NetworkStatsRequestInvalid):
            parse_network_stats_post_query(bad_post_by_field)
    with pytest.raises(NetworkStatsRequestInvalid):
        NetworkStatsQuery(network_ids=())
    with pytest.raises(NetworkStatsRequestInvalid):
        NetworkStatsQuery(network_ids=(1,), generation_id=0)


def test_summary_and_layout_contracts():
    """Verify summary document invariants, layout binding guards and G7 compatibility."""
    valid_by_field = {
        "total_npi_count": 15,
        "office_npi_count": 12,
        "provider_only_npi_count": 3,
        "individual_npi_count": 10,
        "organization_npi_count": 5,
        "unique_address_count": 8,
        "unique_location_count": 14,
        "state_count": 2,
        "states": ["CA", "NV"],
        "core_state_count": 1,
        "core_states": ["CA"],
        "county_count": 4,
        "zip5_count": 6,
    }
    assert validate_network_stats_summary_document(valid_by_field) == valid_by_field
    with pytest.raises(ValueError):
        validate_network_stats_summary_document("not-a-dict")
    for patch_by_field in (
        {"total_npi_count": -1},
        {"provider_only_npi_count": 4},
        {"organization_npi_count": 6},
        {"unique_address_count": 15},
        {"core_states": ["OR"]},
        {"states": ["CA", "CA"]},
    ):
        with pytest.raises(ValueError, match="network_stats_summary_invalid"):
            validate_network_stats_summary_document({**valid_by_field, **patch_by_field})

    with pytest.raises(ValueError):
        bind_compact_network_stats("not-layout", "schema_a")
    candidate_id = "00000000-0000-0000-0000-000000000007"
    schema_name = "network_candidate_00000000000000000000000000000007"
    layout = CompactNetworkLayout(
        CompactAddressRelation(schema_name, "network_slice_ref"),
        CompactAddressRelation("published_addresses", "entity_address_unified"),
        (CompactEdgeArtifact(candidate_id, schema_name),),
    )
    with pytest.raises(ValueError):
        bind_compact_network_stats(layout, "wrong_schema")
    bind_compact_network_stats(layout, schema_name)
    with pytest.raises(ValueError):
        bind_compact_network_stats(layout, "other_schema")

    with pytest.raises(NetworkStatsError):
        network_stats_receipt({"validation_json": {"serving_readiness": {"network_stats": {}}}})


def _frozen_historical_scope(
    candidate_id: str, schema_name: str, source_gens_by_name: dict, addr_by_field: dict
) -> dict:
    return {
        "dataset_id": str(UUID(int=1)),
        "schema_id": str(UUID(int=2)),
        "producer_id": str(UUID(int=3)),
        "candidate_id": candidate_id,
        "schema_name": schema_name,
        "validation_revision": 1,
        "source_generations": source_gens_by_name,
        "address_source": addr_by_field,
        "schema_revision": 1,
        "approved_custom_revision": 4,
        "expected_head": 0,
        "expected_rows": 0,
        "accepted_rows": 0,
    }


def _frozen_historical_candidate(historical_by_field: dict) -> dict:
    candidate_id = historical_by_field["candidate_id"]
    schema_name = historical_by_field["schema_name"]
    source_gens_by_name = {**historical_by_field["source_generations"], "unified_address": "b" * 64}
    addr_by_field = historical_by_field["validation_json"]["address_source"]
    scope_by_field = _frozen_historical_scope(candidate_id, schema_name, source_gens_by_name, addr_by_field)
    return {
        **historical_by_field,
        "source_generations": source_gens_by_name,
        "dataset_id": str(UUID(int=1)),
        "schema_id": str(UUID(int=2)),
        "producer_id": str(UUID(int=3)),
        "expected_head": 0,
        "expected_rows": 0,
        "accepted_rows": 0,
        "source_recipes_json": [],
        "index_ready": True,
        "validation_json": {
            **{key: val for key, val in scope_by_field.items() if key != "address_source"},
            "address_source": addr_by_field,
            "valid": True,
            "raw_rows": 0,
            "batch_rows": 0,
            "batch_count": 0,
            "membership_rows": 0,
            "distinct_memberships": 0,
            "projected_locations": 0,
            "orphan_bindings": 0,
            "unknown_network_rows": 0,
            "accounting_errors": [],
            "candidate_readiness": {
                "component": "compact_office_slices",
                "readiness_revision": 1,
                "ready": True,
                "scope": scope_by_field,
                "membership_rows": 0,
                "distinct_memberships": 0,
                "projected_locations": 0,
                "orphan_bindings": 0,
                "ref_map": {"schema_name": schema_name, "table_name": "network_slice_ref"},
                "edge_heap": {"schema_name": schema_name, "table_name": "network_office_edge"},
            },
            "serving_readiness": {
                **historical_by_field["validation_json"]["serving_readiness"],
                "readiness_revision": 1,
                "ready": True,
                "scope": scope_by_field,
                "index_profile": "compact_membership",
                "slice_ref_sha256": "e" * 64,
                "index_definition_sha256": "d" * 64,
            },
        },
    }


def test_historical_generation_compatibility():
    """Verify merge-base G6/G7 manifests without network_stats keep exact digests and heaps."""
    candidate_id = "00000000-0000-0000-0000-000000000007"
    schema_name = "network_candidate_00000000000000000000000000000007"
    historical_by_field = {
        "candidate_id": candidate_id,
        "schema_name": schema_name,
        "schema_revision": 1,
        "approved_custom_revision": 4,
        "source_generations": {"custom_membership": "a" * 64, INDEPENDENT_STORAGE_KEY: INDEPENDENT_STORAGE},
        "state": "published",
        "validation_json": {
            "address_source": {
                "schema_name": "published_addresses",
                "table_name": "entity_address_unified",
                "generation_id": "b" * 64,
            },
            "serving_readiness": {
                "component": "compact_network_serving",
                "address_artifact": {
                    "namespace": {"name": "published_addresses"},
                    "table": {"name": "entity_address_unified"},
                },
                "edge_artifacts": [{"candidate_id": candidate_id, "schema_name": schema_name}],
            },
        },
    }
    manifest_by_field = {
        "candidate_id": candidate_id,
        "schema_revision": 1,
        "source_generations": historical_by_field["source_generations"],
        "approved_custom_revision": 4,
        "generation_id": 7,
        "manifest_sha256": "c" * 64,
        "eligible": True,
    }
    frozen_manifest_sha256 = "75df50cbdc5bba97a5e63b048e034e458383bfc38213614c87e64fa122991221"
    frozen_candidate_by_field = _frozen_historical_candidate(historical_by_field)
    frozen_manifest_by_field = {
        **manifest_by_field,
        "source_generations": frozen_candidate_by_field["source_generations"],
        "manifest_sha256": frozen_manifest_sha256,
    }
    frozen_entry_by_field = {
        "candidate_json": json.dumps(frozen_candidate_by_field),
        "manifest_json": json.dumps(frozen_manifest_by_field),
    }
    assert network_stats_receipt(frozen_candidate_by_field) is None
    assert not (compact_serving_heaps(frozen_candidate_by_field) & STATS_TABLES)
    assert published_pin.published_network_layout(frozen_candidate_by_field).network_stats_schema is None
    pinned, _, _ = published_pin._decode_published_pin({**frozen_entry_by_field, "approved_rows": 0}, "example_control")
    assert pinned.manifest_sha256 == frozen_manifest_sha256
    assert all(rel.table_name not in STATS_TABLES for rel in pinned.candidate_relation_pins)
    regenerated_by_field = published_entry_with_digest(
        {"candidate_json": json.dumps(historical_by_field), "manifest_json": json.dumps(manifest_by_field)}
    )
    assert json.loads(regenerated_by_field["manifest_json"])["manifest_sha256"] == frozen_manifest_sha256


async def _seed_nucc_table(connection, schema_name: str) -> None:
    """Create synthetic NUCC taxonomy vocabulary rows in the isolated test schema."""
    await connection.execute(
        f"""CREATE TABLE "{schema_name}".nucc_taxonomy (
            int_code integer NOT NULL, code text NOT NULL, grouping text,
            classification text, specialization text, display_name text
        )"""
    )
    await connection.copy_records_to_table(
        "nucc_taxonomy",
        schema_name=schema_name,
        columns=("int_code", "code", "grouping", "classification", "specialization", "display_name"),
        records=[
            (101, "207Q00000X", "Allopathic & Osteopathic Physicians", "Family Medicine", None, "Family Medicine"),
            (102, "207R00000X", "Allopathic & Osteopathic Physicians", "Internal Medicine", None, "Internal Medicine"),
            (103, "207RC0000X", "Allopathic & Osteopathic Physicians", "Internal Medicine", "Cardiology", "Cardiology"),
            (
                104,
                "103TC0700X",
                "Behavioral Health & Social Service Providers",
                "Psychologist",
                "Clinical",
                "Clinical Psychologist",
            ),
            (105, "282N00000X", "Hospitals", "General Acute Care Hospital", None, "General Acute Care Hospital"),
        ],
    )


async def _seed_npi_tables(connection, schema_name: str) -> None:
    """Create synthetic NPPES entity-type and primary-taxonomy rows."""
    ns = f'"{schema_name}"'
    await connection.execute(f"CREATE TABLE {ns}.npi (npi bigint PRIMARY KEY, entity_type_code integer NOT NULL)")
    await connection.copy_records_to_table(
        "npi",
        schema_name=schema_name,
        columns=("npi", "entity_type_code"),
        records=[
            (1000000001, 1),
            (1000000002, 1),
            (1000000003, 2),
            (1000000004, 1),
            (1000000005, 1),
            *((2000000000 + offset, 1) for offset in range(1, 16)),
        ],
    )
    await connection.execute(
        f"""CREATE TABLE {ns}.npi_taxonomy (
            npi bigint NOT NULL,
            healthcare_provider_taxonomy_code varchar(16) NOT NULL,
            healthcare_provider_primary_taxonomy_switch varchar(4)
        )"""
    )
    await connection.copy_records_to_table(
        "npi_taxonomy",
        schema_name=schema_name,
        columns=("npi", "healthcare_provider_taxonomy_code", "healthcare_provider_primary_taxonomy_switch"),
        records=[
            (1000000001, "207R00000X", "N"),
            (1000000001, "207RC0000X", "Y"),
            (1000000002, "207Q00000X", "Y"),
            (1000000003, "282N00000X", "Y"),
            (1000000004, "103TC0700X", "Y"),
            (1000000005, "207Q00000X", "Y"),
            *((2000000000 + offset, "207Q00000X", "Y") for offset in range(1, 16)),
        ],
    )


async def _create_slice_candidate(
    connection, schemas_list: list[str], location_keys: list[str], edges: list[tuple[int, int]]
) -> MembershipCopyTarget:
    """Create a candidate schema with bucket-partitioned slice references and edges."""
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)),
        str(candidate_id),
        "network_candidate_" + candidate_id.hex,
    )
    schemas_list.append(copy_target.schema_name)
    ns = f'"{copy_target.schema_name}"'
    await connection.execute(f"CREATE SCHEMA {ns}")
    await connection.execute(
        f"""CREATE TABLE {ns}.network_slice_ref (
            network_id integer NOT NULL, office_bucket integer NOT NULL,
            artifact_candidate_id uuid NOT NULL, slice_id bigint NOT NULL,
            row_count integer NOT NULL, content_sha256 text COLLATE "C" NOT NULL,
            PRIMARY KEY (network_id, office_bucket)
        )"""
    )
    await connection.execute(
        f"""CREATE TABLE {ns}.network_office_edge (
            slice_id bigint NOT NULL, location_key text COLLATE "C" NOT NULL,
            PRIMARY KEY (slice_id, location_key)
        )"""
    )
    await connection.execute(
        f"CREATE INDEX network_office_edge_location_idx ON {ns}.network_office_edge (location_key, slice_id)"
    )
    keys_by_net_bucket: dict[tuple[int, int], set[str]] = {}
    for net_id, key_idx in edges:
        loc_key = location_keys[key_idx]
        bucket = hashlib.sha256(loc_key.encode("utf-8")).digest()[0] & 63
        keys_by_net_bucket.setdefault((net_id, bucket), set()).add(loc_key)
    ref_tuples = []
    edge_tuples = []
    for next_slice_id, ((net_id, bucket), net_keys) in enumerate(sorted(keys_by_net_bucket.items()), 1):
        ref_tuples.append((net_id, bucket, candidate_id, next_slice_id, len(net_keys), "a" * 64))
        edge_tuples.extend((next_slice_id, loc_key) for loc_key in sorted(net_keys))
    if ref_tuples:
        await connection.copy_records_to_table(
            "network_slice_ref", schema_name=copy_target.schema_name, records=ref_tuples
        )
        await connection.copy_records_to_table(
            "network_office_edge", schema_name=copy_target.schema_name, records=edge_tuples
        )
    return copy_target


async def _seed_stats_addresses_and_evidence(db) -> tuple[CompactAddressRelation, MembershipCopyTarget]:
    """Populate multi-network addresses and provider-only evidence for stats parity checks."""
    await _seed_nucc_table(db.connection, db.source_schema)
    await _seed_npi_tables(db.connection, db.source_schema)
    all_keys = ["loc-100-dual", "loc-100-fam", "loc-100-hosp", *(f"loc-ca-{i:02d}" for i in range(1, 15)), "loc-nv-01"]
    base = await _address_heap(
        db,
        db.source_schema,
        [
            (all_keys[0], "npi", "p1", 1000000001, "Dr Dual", "base", 1, "practice"),
            (all_keys[1], "npi", "p2", 1000000002, "Dr Family", "base", 2, "practice"),
            (all_keys[2], "npi", "p3", 1000000003, "Metro Hospital", "base", 3, "site"),
            *(
                (f"loc-ca-{i:02d}", "npi", f"r-{i}", 2000000000 + i, f"Doc {i}", "base", 10 + i, "practice")
                for i in range(1, 15)
            ),
            ("loc-nv-01", "npi", "r-15", 2000000015, "Border Doc", "base", 99, "practice"),
        ],
    )
    await db.connection.execute(
        f"""UPDATE "{base.schema_name}"."{base.table_name}" SET
            lat = 34.05, long = -118.24,
            state_name = CASE WHEN location_key = 'loc-nv-01' THEN 'nv' ELSE 'CA' END,
            zip5 = CASE WHEN location_key = 'loc-nv-01' THEN '89101' ELSE '90001' END,
            county_fips = CASE WHEN location_key = 'loc-nv-01' THEN '32003' WHEN location_key = $3 THEN '06073' ELSE '06037' END,
            address_key = CASE
                WHEN location_key IN ($1, $2) THEN '00000000-0000-0000-0000-000000000101'::uuid
                WHEN location_key = $3 THEN '00000000-0000-0000-0000-000000000102'::uuid
                ELSE '00000000-0000-0000-0000-000000000103'::uuid
            END,
            taxonomy_array = CASE WHEN location_key = $1 THEN ARRAY[102, 103] WHEN location_key = $3 THEN ARRAY[105] ELSE ARRAY[101] END""",
        all_keys[0],
        all_keys[1],
        all_keys[2],
    )
    copy_target = await _create_slice_candidate(
        db.connection, db.schemas, all_keys, [(100, 0), (100, 1), (100, 2), *((200, idx) for idx in range(3, 18))]
    )
    await db.connection.execute(
        f"""CREATE TABLE "{copy_target.schema_name}".{PROVIDER_ONLY_TABLE} (
            network_id integer NOT NULL, provider_system text COLLATE "C" NOT NULL,
            provider_id text COLLATE "C" NOT NULL, evidence_id text COLLATE "C" NOT NULL,
            PRIMARY KEY (network_id, provider_system, provider_id, evidence_id)
        )"""
    )
    await db.connection.copy_records_to_table(
        PROVIDER_ONLY_TABLE,
        schema_name=copy_target.schema_name,
        columns=("network_id", "provider_system", "provider_id", "evidence_id"),
        records=[
            (100, "npi", "1000000001", "1" * 64),
            (100, "npi", "1000000004", "2" * 64),
            (100, "npi", "1000000004", "3" * 64),
            (100, "provider_directory", "Practitioner/abc", "4" * 64),
            (300, "npi", "1000000005", "5" * 64),
        ],
    )
    return base, copy_target


def _assert_multi_network_stats(entries_by_net: dict[int, dict]) -> None:
    """Assert deduplicated specialty, state, core-state and provider-only counts."""
    assert entries_by_net[100]["summary"] == {
        "total_npi_count": 4,
        "office_npi_count": 3,
        "provider_only_npi_count": 1,
        "individual_npi_count": 3,
        "organization_npi_count": 1,
        "unique_address_count": 2,
        "unique_location_count": 3,
        "state_count": 1,
        "states": ["CA"],
        "core_state_count": 1,
        "core_states": ["CA"],
        "county_count": 2,
        "zip5_count": 1,
    }
    specs_by_pair = {(sp["specialty_kind"], sp["specialty_key"]): sp for sp in entries_by_net[100]["specialties"]}
    assert specs_by_pair[("classification", "Internal Medicine")]["office_npi_count"] == 1
    assert specs_by_pair[("classification", "Internal Medicine")]["primary_office_npi_count"] == 1
    assert specs_by_pair[("rollup", "primary_care")]["office_npi_count"] == 2
    assert specs_by_pair[("rollup", "primary_care")]["primary_office_npi_count"] == 1
    assert specs_by_pair[("rollup", "primary_care")]["unique_address_count"] == 1
    assert specs_by_pair[("rollup", "behavioral_health")]["total_npi_count"] == 1
    assert specs_by_pair[("rollup", "behavioral_health")]["office_npi_count"] == 0
    assert entries_by_net[200]["summary"]["states"] == ["CA", "NV"]
    assert entries_by_net[200]["summary"]["core_states"] == ["CA"]
    assert entries_by_net[300]["summary"]["provider_only_npi_count"] == 1
    assert entries_by_net[999]["summary"]["total_npi_count"] == 0


async def _assert_stats_reader_refusals(connection, manifest, monkeypatch) -> None:
    """Verify policy exclusion, invalid selector and response byte bound refusals."""
    with pytest.raises(NetworkStatsDenied):
        await read_network_stats(
            connection, manifest, NetworkStatsQuery(network_ids=(100, 200)), excluded_network_ids=(200,)
        )
    with pytest.raises(NetworkStatsRequestInvalid):
        await read_network_stats(connection, manifest, "not-a-query")
    monkeypatch.setattr(stats_reader, "MAX_STATS_RESPONSE_BYTES", 10)
    async with connection.transaction():
        with pytest.raises(NetworkStatsRequestInvalid, match="bounds"):
            await read_network_stats(connection, manifest, NetworkStatsQuery(network_ids=(100,)))


@pytest.mark.asyncio
async def test_network_stats_end_to_end(slice_db, monkeypatch):
    """Verify batched stats builder, receipt validation, reader filters and exclusions."""
    db = slice_db
    base, copy_target = await _seed_stats_addresses_and_evidence(db)
    layout = CompactNetworkLayout(
        CompactAddressRelation(copy_target.schema_name, "network_slice_ref"),
        base,
        (CompactEdgeArtifact(copy_target.candidate_id, copy_target.schema_name),),
    )
    monkeypatch.setattr(stats_builder, "_BATCH_EDGE_BUDGET", 2)
    async with db.connection.transaction():
        await db.connection.execute("SET LOCAL work_mem = '32MB'")
        geo_receipt_by_field = await geo_search_mod.prepare_network_geo_search(db.connection, copy_target, layout)
        stats_receipt_by_field = await stats_builder.prepare_network_stats(db.connection, copy_target, layout)
        assert await db.connection.fetchval("SHOW work_mem") == "32MB"

    scope_fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    candidate_by_field = {
        **{field: getattr(copy_target, field) for field in scope_fields},
        "index_ready": True,
        "validation_json": {
            "serving_readiness": {
                "geo_search": geo_receipt_by_field,
                "network_stats": stats_receipt_by_field,
                "edge_artifacts": [{"candidate_id": copy_target.candidate_id, "schema_name": copy_target.schema_name}],
            }
        },
    }
    assert network_stats_receipt(candidate_by_field) == stats_receipt_by_field
    assert STATS_TABLES <= compact_serving_heaps(candidate_by_field)
    manifest = _manifest(bind_compact_geo_search_candidate(layout, candidate_by_field))
    assert set(network_stats_relations(copy_target.schema_name)) <= set(manifest.candidate_relation_pins)

    async with db.connection.transaction():
        fetched = await read_network_stats(
            db.connection,
            manifest,
            NetworkStatsQuery(
                network_ids=(100, 200, 300, 999),
                include_states=True,
                include_specialties=True,
                include_state_specialties=True,
                specialty_keys=("Internal Medicine", "primary_care", "behavioral_health", "207RC0000X"),
            ),
        )
        summary_only = await read_network_stats(
            db.connection, manifest, NetworkStatsQuery(network_ids=(100,), include_states=False)
        )
        assert "states_breakdown" not in summary_only[0]
    _assert_multi_network_stats({entry["network_id"]: entry for entry in fetched})
    await _assert_stats_reader_refusals(db.connection, manifest, monkeypatch)


async def _prepare_searchable_address_indexes(connection, relations) -> None:
    """Create GiST geo and taxonomy search indexes on synthetic compact heaps."""
    geo_expr = "public.Geography(public.ST_MakePoint(long::double precision, lat::double precision))"
    pred_sql = (
        "WHERE type IN ('primary','secondary','practice','site') "
        "AND COALESCE(address_precision,'')<>'city_zip' AND lat IS NOT NULL AND long IS NOT NULL"
    )
    for relation in relations:
        rel_sql = geo_search_mod._qualified(relation)
        await connection.execute(
            f"UPDATE {rel_sql} SET lat=40.0, long=-74.0, state_name='NY', zip5='10001', county_fips='36061', taxonomy_array=ARRAY[0, 777]"
        )
        await connection.execute(f"CREATE INDEX ON {rel_sql} USING gist ({geo_expr}) {pred_sql}")
        await connection.execute(
            f"CREATE INDEX ON {rel_sql} USING gist ({geo_expr}, taxonomy_array public.gist__intbig_ops) {pred_sql}"
        )


@pytest.mark.asyncio
async def test_fallback_and_index_digest(compact_db):
    """Verify fallback integer taxonomy codes and serving index digest verification."""
    db = compact_db
    base_only_layout = CompactNetworkLayout(
        db.first_layout.ref_map, db.first_layout.base, db.first_layout.artifacts, None
    )
    entry = await db.connection.fetchrow(
        f'SELECT * FROM "{db.source_schema}".network_membership_candidate WHERE schema_name=$1',
        base_only_layout.ref_map.schema_name,
    )
    scope_fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    copy_target = MembershipCopyTarget(*(str(entry[field]) for field in scope_fields))
    async with db.connection.transaction():
        await _prepare_searchable_address_indexes(db.connection, (base_only_layout.base,))
        geo_by_field = await geo_search_mod.prepare_network_geo_search(db.connection, copy_target, base_only_layout)
        stats_by_field = await stats_builder.prepare_network_stats(db.connection, copy_target, base_only_layout)
        prospective_by_field = {
            **dict(entry),
            "index_ready": True,
            "validation_json": {
                "serving_readiness": {
                    "component": "compact_network_serving",
                    "geo_search": geo_by_field,
                    "network_stats": stats_by_field,
                    "edge_artifacts": [asdict(artifact) for artifact in base_only_layout.artifacts],
                }
            },
        }
        digest = await compact_index_digest(db.connection, copy_target.schema_name, candidate=prospective_by_field)
        prospective_by_field["validation_json"]["serving_readiness"]["index_definition_sha256"] = digest
        bound_layout = bind_compact_geo_search_candidate(base_only_layout, prospective_by_field)
        await verify_network_search_indexes(db.connection, prospective_by_field, compact_layout=bound_layout)
        fallback_entries = await read_network_stats(
            db.connection,
            _manifest(bound_layout),
            NetworkStatsQuery(network_ids=(42,), include_specialties=True, specialty_kind="taxonomy"),
        )
        assert fallback_entries[0]["specialties"][0]["specialty_key"] == "777"
        assert fallback_entries[0]["summary"]["county_count"] == 1


@pytest.mark.asyncio
async def test_custom_county_and_empty_stats(compact_db):
    """Verify custom office county_fips override and zero-network candidate handling."""
    db = compact_db
    scope_fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    second_entry = await db.connection.fetchrow(
        f'SELECT * FROM "{db.source_schema}".network_membership_candidate WHERE schema_name=$1',
        db.second_layout.ref_map.schema_name,
    )
    second_pin = MembershipCopyTarget(*(str(second_entry[field]) for field in scope_fields))
    async with db.connection.transaction():
        await _prepare_searchable_address_indexes(db.connection, (db.second_layout.base, db.second_layout.custom))
        await geo_search_mod.prepare_network_geo_search(db.connection, second_pin, db.second_layout)
        second_stats_by_field = await stats_builder.prepare_network_stats(db.connection, second_pin, db.second_layout)
        assert second_stats_by_field["counts"]["summary_rows"] >= 1

        empty_pin = await _create_slice_candidate(db.connection, db.schemas, [], [])
        empty_layout = CompactNetworkLayout(
            CompactAddressRelation(empty_pin.schema_name, "network_slice_ref"),
            db.first_layout.base,
            (CompactEdgeArtifact(empty_pin.candidate_id, empty_pin.schema_name),),
        )
        await db.connection.execute(
            f'CREATE TABLE "{empty_pin.schema_name}".network_geo_search '
            f'(LIKE "{second_pin.schema_name}".network_geo_search)'
        )
        empty_stats_by_field = await stats_builder.prepare_network_stats(db.connection, empty_pin, empty_layout)
        assert empty_stats_by_field["counts"]["summary_rows"] == 0


def _fake_stats_manifests():
    cid = str(UUID(int=8))
    schema_name = "network_candidate_" + UUID(cid).hex
    pre_g8_manifest = _manifest(
        CompactNetworkLayout(
            CompactAddressRelation(schema_name, "network_slice_ref"),
            CompactAddressRelation("published_addresses", "entity_address_unified"),
            (CompactEdgeArtifact(cid, schema_name),),
        )
    )
    g8_manifest = _manifest(
        bind_compact_network_stats(
            CompactNetworkLayout(
                CompactAddressRelation(schema_name, "network_slice_ref"),
                CompactAddressRelation("published_addresses", "entity_address_unified"),
                (CompactEdgeArtifact(cid, schema_name),),
            ),
            schema_name,
        )
    )
    return cid, schema_name, pre_g8_manifest, g8_manifest


async def _fake_stats_reader(_driver, manifest, query, *, excluded_network_ids=(), control_schema=None):
    if excluded_network_ids and any(nid in excluded_network_ids for nid in query.network_ids):
        raise NetworkStatsDenied("denied")
    if control_schema is not None and 999999 in query.network_ids:
        raise NetworkStatsDenied("unknown")
    if manifest.compact_layout.network_stats_schema is None:
        raise NetworkStatsUnavailable("network_stats_unavailable")
    return [{"network_id": nid, "summary": {"total_npi_count": 5}} for nid in query.network_ids]


@pytest.mark.asyncio
async def test_serving_stats_http_routes(monkeypatch):
    """Verify GET /serving/networks/stats auth, invalid queries, G7 404 and G8 200."""
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-stats-token")
    headers_by_name = {"Authorization": "Bearer synthetic-stats-token"}
    app = RequestLifecycleSanic("network_stats_http_" + uuid4().hex)
    app.blueprint(Blueprint.group([serving_endpoint.blueprint], version_prefix="/api/v"))
    _, _, pre_g8_manifest, g8_manifest = _fake_stats_manifests()

    @asynccontextmanager
    async def fake_context(_session):
        yield SimpleNamespace()

    async def fake_resolve(_driver, *, generation_id=None, api_read=False):
        if generation_id == 999:
            raise NetworkServingReadUnavailable("missing")
        return pre_g8_manifest if generation_id == 7 else g8_manifest

    monkeypatch.setattr(serving_endpoint, "bind_network_request_reader", AsyncMock(return_value=SimpleNamespace()))
    monkeypatch.setattr(serving_endpoint, "network_serving_read_context", fake_context)
    monkeypatch.setattr(serving_endpoint, "resolve_network_serving_manifest", fake_resolve)
    monkeypatch.setattr(serving_endpoint, "read_network_stats", _fake_stats_reader)

    assert (await app.asgi_client.get("/api/v1/registry/serving/networks/stats?network_ids=42"))[1].status == 403
    assert (
        await app.asgi_client.get("/api/v1/registry/serving/networks/stats?network_ids=bad", headers=headers_by_name)
    )[1].status == 400
    assert (
        await app.asgi_client.get(
            "/api/v1/registry/serving/networks/stats?network_ids=42&network_generation=999", headers=headers_by_name
        )
    )[1].status == 503
    _, pre_g8 = await app.asgi_client.get(
        "/api/v1/registry/serving/networks/stats?network_ids=42&network_generation=7", headers=headers_by_name
    )
    assert pre_g8.status == 404 and pre_g8.json == {"error": {"code": "network_stats_unavailable"}}
    _, ok_resp = await app.asgi_client.get(
        "/api/v1/registry/serving/networks/stats?network_ids=42,7", headers=headers_by_name
    )
    assert ok_resp.status == 200 and [net_entry["network_id"] for net_entry in ok_resp.json["items"]] == [7, 42]


@pytest.mark.asyncio
async def test_catalog_stats_and_preparation(catalog_context, monkeypatch):
    """Verify POST /serving/catalog/stats policy enforcement, G7 404 and independent-v1 gating."""
    cid, schema_name, pre_g8_manifest, g8_manifest = _fake_stats_manifests()
    object.__setattr__(catalog_context.manifest, "_compact_layout", g8_manifest.compact_layout)
    monkeypatch.setattr(stats_reader, "read_network_stats", _fake_stats_reader)
    auth_by_name = {"Authorization": "Bearer synthetic-catalog-control-token"}
    _, cat_ok = await catalog_context.app.asgi_client.post(
        "/api/v1/registry/serving/catalog/stats",
        headers=auth_by_name,
        content=json.dumps(catalog_envelope({"network_ids": [42]})).encode("utf-8"),
    )
    assert cat_ok.status == 200 and cat_ok.json["items"][0]["network_id"] == 42
    _, cat_denied = await catalog_context.app.asgi_client.post(
        "/api/v1/registry/serving/catalog/stats",
        headers=auth_by_name,
        content=json.dumps(catalog_envelope({"network_ids": [42]}, excluded_network_ids=[42])).encode("utf-8"),
    )
    assert cat_denied.status == 404 and cat_denied.json == {"error": {"code": "network_catalog_not_found"}}
    object.__setattr__(catalog_context.manifest, "_compact_layout", pre_g8_manifest.compact_layout)
    _, cat_unavail = await catalog_context.app.asgi_client.post(
        "/api/v1/registry/serving/catalog/stats",
        headers=auth_by_name,
        content=json.dumps(catalog_envelope({"network_ids": [42]})).encode("utf-8"),
    )
    assert cat_unavail.status == 404 and cat_unavail.json == {"error": {"code": "network_stats_unavailable"}}


@pytest.mark.asyncio
async def test_approved_network_guard_and_independent_prep(monkeypatch):
    """Verify canonical network existence check and independent-v1 preparation gate."""
    from process import registry_effective_network_catalog as effective_cat

    cid, schema_name, _, g8_manifest = _fake_stats_manifests()
    fake_conn = SimpleNamespace(fetchval=AsyncMock(side_effect=[1, 0, 1]))
    monkeypatch.setattr(effective_cat, "effective_catalog_networks_sql", AsyncMock(return_value="(SELECT 1)"))
    await stats_reader._require_approved_networks(fake_conn, g8_manifest, (42,), "ctl")
    with pytest.raises(NetworkStatsDenied):
        await stats_reader._require_approved_networks(fake_conn, g8_manifest, (42,), "ctl")
    monkeypatch.setattr(effective_cat, "effective_catalog_networks_sql", AsyncMock(return_value=None))
    await stats_reader._require_approved_networks(fake_conn, g8_manifest, (42,), "ctl")

    pin = MembershipCopyTarget(*(str(UUID(int=i)) for i in (1, 2, 3)), cid, schema_name)
    offices = SimpleNamespace(custom=None, physical_base=CompactAddressRelation("mrf", "entity_address_unified"))
    monkeypatch.setattr(
        compact_prep,
        "capture_compact_edge_artifacts",
        AsyncMock(return_value=[{"candidate_id": cid, "schema_name": schema_name}]),
    )
    monkeypatch.setattr(geo_search_mod, "prepare_network_geo_search", AsyncMock(return_value={"contract": "geo"}))
    monkeypatch.setattr(stats_builder, "prepare_network_stats", AsyncMock(return_value={"contract": "stats"}))
    _, _, legacy_by_field = await compact_prep._prepare_address_search(
        SimpleNamespace(), pin, offices, None, "ctl", candidate={"source_generations": {}}
    )
    assert legacy_by_field is None
    _, _, indep_by_field = await compact_prep._prepare_address_search(
        SimpleNamespace(),
        pin,
        offices,
        None,
        "ctl",
        candidate={"source_generations": {INDEPENDENT_STORAGE_KEY: INDEPENDENT_STORAGE}},
    )
    assert indep_by_field == {"contract": "stats"}


def test_fhir_catalog_stats_sql_projection():
    """Verify fhir-plans-v1 SQL attaches network_stats on G8+ by default and respects include_stats."""
    from api.network_catalog_fhir_query import parse_fhir_catalog_query
    from process import registry_fhir_catalog_read as fhir_catalog

    cid = str(UUID(int=9))
    schema_name = "network_candidate_" + UUID(cid).hex
    pre_g8_layout = CompactNetworkLayout(
        CompactAddressRelation(schema_name, "network_slice_ref"),
        CompactAddressRelation("published_addresses", "entity_address_unified"),
        (CompactEdgeArtifact(cid, schema_name),),
    )
    pre_default_sql, _ = fhir_catalog._catalog_page_statement({"schema_name": "control"}, _manifest(pre_g8_layout))
    assert "network_stats" not in pre_default_sql
    pre_true_sql, _ = fhir_catalog._catalog_page_statement(
        {"schema_name": "control"}, _manifest(pre_g8_layout), include_stats=True
    )
    assert "network_stats_summary" not in pre_true_sql and "'network_stats',NULL" in pre_true_sql

    g8_layout = bind_compact_network_stats(
        CompactNetworkLayout(
            CompactAddressRelation(schema_name, "network_slice_ref"),
            CompactAddressRelation("published_addresses", "entity_address_unified"),
            (CompactEdgeArtifact(cid, schema_name),),
        ),
        schema_name,
    )
    g8_default_sql, _ = fhir_catalog._catalog_page_statement({"schema_name": "control"}, _manifest(g8_layout))
    assert "network_stats_summary" in g8_default_sql and "'network_stats'" in g8_default_sql
    g8_off_sql, _ = fhir_catalog._catalog_page_statement(
        {"schema_name": "control"}, _manifest(g8_layout), include_stats=False
    )
    assert "network_stats" not in g8_off_sql
    parsed_sel, _ = parse_fhir_catalog_query({"view": "fhir-plans-v1", "include_stats": True}, "list")
    assert parsed_sel.query.include_stats is True


def test_fhir_catalog_stats_item_validation():
    """Verify fhir-plans-v1 item validator checks optional, null and malformed network_stats."""
    from process import registry_fhir_catalog_read as fhir_catalog
    from tests.test_registry_fhir_catalog_read import network_document

    with pytest.raises(fhir_catalog.RegistryNetworkCatalogError):
        fhir_catalog.FHIRCatalogQuery(include_stats="yes")
    default_query = fhir_catalog.FHIRCatalogQuery()
    true_query = fhir_catalog.FHIRCatalogQuery(include_stats=True)
    false_query = fhir_catalog.FHIRCatalogQuery(include_stats=False)
    base_by_field = network_document()
    assert fhir_catalog._network_item(base_by_field, default_query) == base_by_field
    assert fhir_catalog._network_item({**base_by_field, "network_stats": None}, true_query)["network_stats"] is None
    with pytest.raises(fhir_catalog.RegistryNetworkCatalogError):
        fhir_catalog._network_item({**base_by_field, "network_stats": None}, default_query)

    enriched_by_field = {
        **base_by_field,
        "network_stats": {
            "total_npi_count": 10,
            "office_npi_count": 8,
            "provider_only_npi_count": 2,
            "individual_npi_count": 9,
            "organization_npi_count": 1,
            "unique_address_count": 5,
            "unique_location_count": 8,
            "state_count": 1,
            "states": ["CA"],
            "core_state_count": 1,
            "core_states": ["CA"],
            "county_count": 2,
            "zip5_count": 3,
        },
    }
    assert fhir_catalog._network_item(enriched_by_field, default_query)["network_stats"]["total_npi_count"] == 10
    assert fhir_catalog._network_item(enriched_by_field, true_query)["network_stats"]["total_npi_count"] == 10
    with pytest.raises(fhir_catalog.RegistryNetworkCatalogError):
        fhir_catalog._network_item(enriched_by_field, false_query)
    with pytest.raises(fhir_catalog.RegistryNetworkCatalogError):
        fhir_catalog._network_item({**enriched_by_field, "network_stats": {"total_npi_count": -1}}, default_query)


@pytest.mark.asyncio
async def test_approved_record_fallback_and_empty_artifacts_postgres(slice_db, monkeypatch):
    """Verify real registry_approved_record JSON archived guard and empty-artifact provider-only builds."""
    from process import registry_effective_network_catalog as effective_cat

    db = slice_db
    ctl = f'"{db.source_schema}"'
    await db.connection.execute(
        f"""CREATE TABLE {ctl}.registry_approved_record (
            approved_revision bigint NOT NULL, record_kind varchar(16) NOT NULL,
            record_key varchar(36) NOT NULL, record_revision bigint NOT NULL,
            custom_revision bigint NOT NULL, record_json jsonb NOT NULL,
            PRIMARY KEY (approved_revision, record_kind, record_key)
        )"""
    )
    await db.connection.execute(
        f"""INSERT INTO {ctl}.registry_approved_record VALUES
            (5, 'network', '42', 1, 2, '{{"network_id": 42, "archived": false}}'::jsonb),
            (5, 'network', '43', 1, 2, '{{"network_id": 43, "archived": true}}'::jsonb)"""
    )
    monkeypatch.setattr(effective_cat, "effective_catalog_networks_sql", AsyncMock(return_value=None))
    _, _, _, g8_manifest = _fake_stats_manifests()
    object.__setattr__(g8_manifest, "approved_custom_revision", 5)
    await stats_reader._require_approved_networks(db.connection, g8_manifest, (42,), db.source_schema)
    with pytest.raises(NetworkStatsDenied):
        await stats_reader._require_approved_networks(db.connection, g8_manifest, (43,), db.source_schema)

    await _seed_npi_tables(db.connection, db.source_schema)
    base = await _address_heap(db, db.source_schema, [])
    copy_target = await _create_slice_candidate(db.connection, db.schemas, [], [])
    await db.connection.execute(
        f"""CREATE TABLE "{copy_target.schema_name}".{PROVIDER_ONLY_TABLE} (
            network_id integer NOT NULL, provider_system text COLLATE "C" NOT NULL,
            provider_id text COLLATE "C" NOT NULL, evidence_id text COLLATE "C" NOT NULL,
            PRIMARY KEY (network_id, provider_system, provider_id, evidence_id)
        )"""
    )
    await db.connection.execute(
        f"""INSERT INTO "{copy_target.schema_name}".{PROVIDER_ONLY_TABLE}
            VALUES (42, 'npi', '1000000001', repeat('a', 64))"""
    )
    empty_layout = CompactNetworkLayout(CompactAddressRelation(copy_target.schema_name, "network_slice_ref"), base, ())
    async with db.connection.transaction():
        await geo_search_mod.prepare_network_geo_search(db.connection, copy_target, empty_layout)
        receipt_by_field = await stats_builder.prepare_network_stats(db.connection, copy_target, empty_layout)
    assert receipt_by_field["counts"]["summary_rows"] == 1
    po_only_count = await db.connection.fetchval(
        f'SELECT provider_only_npi_count FROM "{copy_target.schema_name}".network_stats_summary WHERE network_id = 42'
    )
    assert po_only_count == 1


@pytest.mark.asyncio
async def test_multibyte_nucc_bounds_and_savepoint_rollback_postgres(slice_db, monkeypatch):
    """Verify UTF-8 octet bounding for NUCC text and clean savepoint rollback on index error."""
    db = slice_db
    base, copy_target = await _seed_stats_addresses_and_evidence(db)
    await db.connection.execute(
        f"""INSERT INTO "{db.source_schema}".nucc_taxonomy
            (int_code, code, grouping, classification, specialization, display_name)
            VALUES (999, '999Z00000X', 'Other', $1, NULL, $2)""",
        "é" * 200,
        "🩺" * 160,
    )
    layout = CompactNetworkLayout(
        CompactAddressRelation(copy_target.schema_name, "network_slice_ref"),
        base,
        (CompactEdgeArtifact(copy_target.candidate_id, copy_target.schema_name),),
    )
    async with db.connection.transaction():
        await db.connection.execute("SET LOCAL work_mem = '24MB'")
        await geo_search_mod.prepare_network_geo_search(db.connection, copy_target, layout)

        async def _boom(_conn, _schema):
            await _conn.execute("SELECT 1 / 0")

        with monkeypatch.context() as ctx:
            ctx.setattr(stats_builder, "_index_and_verify", _boom)
            with pytest.raises(Exception) as exc_info:
                await stats_builder.prepare_network_stats(db.connection, copy_target, layout)
            assert getattr(exc_info.value, "sqlstate", None) == "22012"
        assert await db.connection.fetchval("SHOW work_mem") == "24MB"

        await stats_builder.prepare_network_stats(db.connection, copy_target, layout)
        max_key_octets, max_label_octets = await db.connection.fetchrow(
            f"SELECT max(octet_length(specialty_key)), max(octet_length(display_name)) "
            f'FROM "{copy_target.schema_name}".network_stats_specialty_dim'
        )
        assert max_key_octets <= 256 and max_label_octets <= 512
