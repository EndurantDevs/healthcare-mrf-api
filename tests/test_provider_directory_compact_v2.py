# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure unit tests for compact-v2 encoding, DDL, edition detection, and membership SQL rewrites."""

import json
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_membership_source as membership
from process import provider_directory_compact_edition as edition
from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_v2 as compact_v2
from process import provider_directory_generic_candidate as candidate
from process.network_fhir_membership_source import FHIRMembershipSourceError, PinnedFHIRMembershipSource
from tests.test_provider_directory_compact_payload import resource


def _v2_tables(*, with_index=True):
    return compact.CompactPayloadTables(
        "compact_ed",
        edition.OUTPUT_ALIASES,
        edition.REFERENCE_SETS,
        edition.BODY_BLOCKS,
        edition.SHARED_BODIES,
        edition.REFERENCE_TOKENS,
        shared_body_index="body_index" if with_index else None,
        is_v2=True,
    )


def _sample_pin(source_id="source-example"):
    return PinnedFHIRMembershipSource(
        schema_name="compact_ed",
        source_id=source_id,
        endpoint_id="endpoint-example",
        dataset_id="edition-one",
        dataset_sha256="d" * 64,
        release_id="release-one",
        resource_relation={"schema": "compact_ed", "name": "provider_directory_dataset_resource"},
        alias_scope="scope-1",
        as_of="2026-11-26",
    )


def test_compact_v2_cache_apply_and_bounds(monkeypatch):
    """Promote committed entries into CompactV2Cache while enforcing cache size caps."""
    cache = compact_v2.CompactV2Cache()
    cache.apply(None)
    cache.apply({})
    assert not cache.is_token_cache_loaded
    monkeypatch.setattr(compact_v2, "_MAX_CACHED_BODIES", 1)
    monkeypatch.setattr(compact_v2, "_MAX_CACHED_REFS", 1)
    monkeypatch.setattr(compact_v2, "_MAX_CACHED_TOKENS", 1)
    cache.apply(
        {
            "tokens_loaded": True,
            "tokens_complete": True,
            "token_id_by_ref": {"Organization/net-1": 1, "Organization/net-2": 2},
            "next_token_id": 3,
            "next_shared_block_ordinal": 5,
            "next_set_id": 9,
            "body_loc_by_hash": {b"a" * 32: (0, 0, b"1" * 16), b"b" * 32: (0, 1, b"2" * 16)},
            "ref_set_by_hash": {b"c" * 32: (1, b"3" * 16), b"d" * 32: (2, b"4" * 16)},
        }
    )
    assert cache.is_token_cache_loaded and not cache.is_token_cache_complete
    assert cache.token_id_by_ref == {"Organization/net-1": 1}
    assert (cache.next_token_id, cache.next_shared_block_ordinal, cache.next_set_id) == (3, 5, 9)
    assert len(cache.body_loc_by_hash) == 1
    assert len(cache.ref_set_by_hash) == 1


@pytest.mark.parametrize(
    ("provenance", "expected_prefix", "expected_mask", "remaining_keys"),
    [
        (
            {
                "resource_url": "https://api.example.test/fhir/Practitioner/p1",
                "fhir_self_url": "https://api.example.test/fhir/Practitioner/p1",
            },
            "https://api.example.test/fhir",
            3,
            set(),
        ),
        (
            {
                "resource_url": "https://api.example.test/fhir/Practitioner/p1",
                "fhir_self_url": "https://other.example.test/fhir/Practitioner/p1",
            },
            "https://api.example.test/fhir",
            1,
            {"fhir_self_url"},
        ),
        (
            {
                "resource_url": None,
                "fhir_self_url": "https://api.example.test/fhir/Practitioner/p1",
            },
            "https://api.example.test/fhir",
            2,
            {"resource_url"},
        ),
        (
            {
                "resource_url": "https://api.example.test/fhir/Practitioner/other",
                "fhir_self_url": 123,
            },
            None,
            0,
            {"resource_url", "fhir_self_url"},
        ),
    ],
)
def test_extract_url_prefix_elides_matching_transport_urls(provenance, expected_prefix, expected_mask, remaining_keys):
    """Elide only exact {prefix}/{resource_type}/{resource_id} transport URLs cleanly."""
    provenance_by_field = dict(provenance)
    prefix, mask = compact_v2._extract_url_prefix(provenance_by_field, "Practitioner", "p1")
    assert (prefix, mask) == (expected_prefix, expected_mask)
    assert set(provenance_by_field) == remaining_keys


def test_split_v2_row_separates_body_provenance_and_refs():
    """Extract canonical body, optional provenance, reference arrays, and URL prefix losslessly."""
    entry = resource(
        "role-1",
        healthcare_service_refs=["HealthcareService/s1"],
        network_refs=["Organization/n1"],
        resource_url="https://api.example.test/PractitionerRole/role-1",
        fhir_self_url="https://api.example.test/PractitionerRole/role-1",
    )
    split = compact_v2._split_v2_row(entry, has_float=False)
    assert len(split[0]) == 32
    assert json.loads(split[1]) == {"location_refs": ["Location/office"]}
    assert split[2] is None
    assert split[3:7] == (2, ["HealthcareService/s1"], 2, ["Organization/n1"])
    assert split[7:] == (True, "https://api.example.test", 3)


def test_text_array_block_packer_splits_by_rows_and_copy_bytes(monkeypatch):
    """Split text[] blocks at both row count and conservative COPY frame byte bounds."""
    empty_packer = compact_v2._TextArrayBlockPacker(("dataset",), 0, compact_v2.POINT_BLOCK_ROWS)
    assert empty_packer.finish() == []

    row_packer = compact_v2._TextArrayBlockPacker(("dataset",), 10, compact_v2.POINT_BLOCK_ROWS)
    slots = [row_packer.append(json.dumps({"index": index})) for index in range(35)]
    blocks = row_packer.finish()
    assert [len(block[2]) for block in blocks] == [16, 16, 3]
    assert [block[1] for block in blocks] == [10, 11, 12]
    assert slots[0] == (10, 0) and slots[16] == (11, 0) and slots[-1] == (12, 2)

    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 1500)
    byte_packer = compact_v2._TextArrayBlockPacker(("dataset",), 0, compact_v2.SEQ_BLOCK_ROWS)
    for index in range(6):
        byte_packer.append(json.dumps({"payload": ('"\\' * 90) + str(index)}))
    byte_blocks = byte_packer.finish()
    assert len(byte_blocks) > 1
    assert sum(len(block[2]) for block in byte_blocks) == 6

    sample_entries = [json.dumps({"payload": '"\\a' * 10}), json.dumps({"payload": '"\\b' * 10})]
    exact_limit = (
        candidate.copy_record_bytes((0, *compact._copy_charge(("dataset", 0, sample_entries))))
        + candidate.COPY_FRAME_BYTES
    )
    for limit, expected_blocks in ((exact_limit, 1), (exact_limit - 1, 2)):
        monkeypatch.setattr(candidate, "COPY_MAX_BYTES", limit)
        boundary_packer = compact_v2._TextArrayBlockPacker(("dataset",), 0, compact_v2.SEQ_BLOCK_ROWS)
        for entry_text in sample_entries:
            boundary_packer.append(entry_text)
        assert len(boundary_packer.finish()) == expected_blocks


def test_compact_v2_ddl_and_resource_sql_contracts():
    """Verify native PostgreSQL DDL without FKs/triggers and optional SQL clauses."""
    with_index = compact_v2.compact_v2_ddl(_v2_tables(with_index=True))
    without_index = compact_v2.compact_v2_ddl(_v2_tables(with_index=False), defer_secondary_indexes=True)
    assert any("body_index" in stmt for stmt in with_index)
    assert not any("body_index" in stmt for stmt in without_index)
    assert len(without_index) == 5
    for statement in with_index:
        upper_sql = statement.upper()
        assert "FOREIGN KEY" not in upper_sql and "TRIGGER" not in upper_sql and "FUNCTION" not in upper_sql

    full_sql = compact_v2.compact_v2_resource_sql(_v2_tables(), observations=True, alias_cte="selected")
    assert "a.batch_ordinal,a.row_ordinal" in full_sql
    assert 'FROM "selected" a' in full_sql
    assert "healthcare_service_refs" in full_sql and "network_refs" in full_sql

    trimmed_sql = compact_v2.compact_v2_resource_sql(
        _v2_tables(), include_service_refs=False, include_network_refs=False
    )
    assert "healthcare_service_refs" not in trimmed_sql and "network_refs" not in trimmed_sql


@pytest.mark.asyncio
async def test_compact_edition_tables_detection():
    """Detect flat, v1, v14, and v2 published editions while failing closed on malformed metadata."""
    connection = AsyncMock()
    connection.fetchrow.return_value = {"format_present": False, "aliases_present": False}
    assert await edition.compact_edition_tables(connection, "flat_ed") is None

    connection.fetchrow.return_value = {"format_present": False, "aliases_present": True}
    with pytest.raises(candidate.GenericCandidateError, match="format_missing"):
        await edition.compact_edition_tables(connection, "broken_ed")

    connection.fetchrow.return_value = {"format_present": True, "aliases_present": True}
    connection.fetch.return_value = [{"format_id": compact.FORMAT}]
    v1_tables = await edition.compact_edition_tables(connection, "v1_ed")
    assert not v1_tables.is_v2 and v1_tables.shared_bodies is None

    connection.fetch.return_value = [{"format_id": compact.SHARED_BODY_FORMAT}]
    connection.fetchval.return_value = True
    v14_tables = await edition.compact_edition_tables(connection, "v14_ed")
    assert not v14_tables.is_v2 and v14_tables.shared_bodies == edition.SHARED_BODIES
    assert v14_tables.reference_tokens == edition.REFERENCE_TOKENS

    connection.fetch.return_value = [{"format_id": compact.COMPACT_V2_FORMAT}]
    v2_tables = await edition.compact_edition_tables(connection, "v2_ed")
    assert v2_tables.is_v2 and v2_tables.shared_body_index is None
    assert v2_tables.reference_tokens == edition.REFERENCE_TOKENS

    connection.fetch.return_value = [{"format_id": "unknown-format"}]
    with pytest.raises(candidate.GenericCandidateError, match="format_changed"):
        await edition.compact_edition_tables(connection, "bad_ed")


def test_compact_membership_sql_rewrites_and_anchor_guards():
    """Rewrite batch and witness SQL for compact-v2, preserve CMS provider joins, and guard anchors."""
    pin = _sample_pin()
    v2_tables = _v2_tables(with_index=False)
    v14_tables = compact.CompactPayloadTables(
        "compact_ed",
        edition.OUTPUT_ALIASES,
        edition.REFERENCE_SETS,
        edition.BODY_BLOCKS,
        edition.SHARED_BODIES,
        edition.REFERENCE_TOKENS,
    )

    flat_sql = membership._extraction_sql(pin, '"reg"', False)
    assert membership._compact_membership_sql(flat_sql, pin, None, reviewed=False) == flat_sql

    reviewed_v2_sql = membership._extraction_sql(pin, '"reg"', True, compact_tables=v2_tables)
    assert "page_network_sets AS MATERIALIZED" in reviewed_v2_sql
    assert "WHERE dataset_id=$8" in reviewed_v2_sql
    assert "provider.npi_text" in reviewed_v2_sql

    v14_sql = membership._extraction_sql(pin, '"reg"', False, compact_tables=v14_tables)
    assert "page_aliases AS MATERIALIZED" in v14_sql
    assert "page_network_sets" not in v14_sql

    witness_v2_sql = membership._source_witness_sql_v2(pin, compact_tables=v2_tables)
    assert "page_network_sets AS MATERIALIZED" in witness_v2_sql
    assert 'COLLATE "C"' in witness_v2_sql

    cms_sql = membership._extraction_sql(_sample_pin("cms-npd"), '"reg"', False, compact_tables=v2_tables)
    assert f'"compact_ed"."{edition.OUTPUT_ALIASES}" provider' not in cms_sql
    assert f'"compact_ed"."{edition.OUTPUT_ALIASES}" location' in cms_sql

    with pytest.raises(FHIRMembershipSourceError, match="page SQL anchor"):
        membership._compact_membership_sql("SELECT 1", pin, v2_tables, reviewed=False)
    with pytest.raises(FHIRMembershipSourceError, match="network SQL anchor"):
        membership._rewrite_v2_network_refs_sql("SELECT 1")
