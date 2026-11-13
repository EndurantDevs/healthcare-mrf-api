# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published pins use logical generations and schema-qualified names."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_published_api_pin as published
from process.registry_network_catalog_read import _PINNED_READ


@pytest.fixture
def published_entry():
    candidate_by_field = {
        "candidate_id": "00000000-0000-0000-0000-000000000001",
        "schema_name": "network_candidate_00000000000000000000000000000001",
        "schema_revision": 1,
        "approved_custom_revision": 4,
        "source_generations": {"custom_membership": "a" * 64},
        "state": "published",
        "validation_json": {
            "address_source": {
                "schema_name": "published_addresses",
                "table_name": "entity_address_unified_v1",
                "generation_id": "b" * 64,
            },
            "serving_readiness": {
                "component": "compact_network_serving",
                "address_artifact": {
                    "namespace": {"name": "published_addresses"},
                    "table": {"name": "entity_address_unified_v1"},
                },
                "edge_artifacts": [
                    {
                        "candidate_id": "00000000-0000-0000-0000-000000000001",
                        "schema_name": "network_candidate_00000000000000000000000000000001",
                    }
                ],
            },
        },
    }
    manifest_by_field = {
        key: candidate_by_field[key]
        for key in ("candidate_id", "schema_revision", "source_generations", "approved_custom_revision")
    }
    manifest_by_field.update(generation_id=7, manifest_sha256="c" * 64, eligible=True)
    return {
        "candidate_json": json.dumps(candidate_by_field),
        "manifest_json": json.dumps(manifest_by_field),
        "approved_rows": 3,
    }


def _candidate(entry):
    return json.loads(entry["candidate_json"])


def test_published_query_reads_pointer_and_metadata():
    query = published.published_api_pin_statement("example_control")
    assert '"example_control".network_serving_control' in query
    assert "manifest.eligible AND candidate.state='published'" in query
    assert "pg_catalog" not in query
    assert "custody" not in query
    assert "oid" not in query


@pytest.mark.asyncio
@pytest.mark.parametrize("generation", [False, True, 0, -1, 1.0, "3", 2**63])
async def test_invalid_generation_refuses_before_query(generation):
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(published.NetworkServingReadUnavailable):
        await published.read_published_api_pin(connection, generation, "example_control")
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_published_generation_is_unavailable():
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    with pytest.raises(published.NetworkServingReadUnavailable, match="generation is unavailable"):
        await published.read_published_api_pin(connection, 3, "example_control")


@pytest.mark.asyncio
async def test_published_pin_uses_stored_names(published_entry):
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=published_entry))
    manifest, approved, relation = await published.read_published_api_pin(connection, None, "example_control")
    assert manifest.generation_id == 7
    assert approved.total_rows == 3
    assert asdict(relation) == {"schema_name": "example_control", "table_name": "registry_approved_record"}
    assert asdict(manifest.compact_layout.base) == {
        "schema_name": "published_addresses",
        "table_name": "entity_address_unified_v1",
    }
    assert manifest.address_relation == manifest.compact_layout.base


@pytest.mark.asyncio
@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("generation", [None, 7])
async def test_manifest_only_read_touches_no_approved_records(published_entry, compact, generation):
    candidate = _candidate(published_entry)
    if not compact:
        candidate["validation_json"]["serving_readiness"]["component"] = "unified_address_serving"
    published_entry["candidate_json"] = json.dumps(candidate)
    del published_entry["approved_rows"]
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=published_entry))
    manifest, approved, relation = await published.read_published_api_pin(
        connection, generation, "example_control", approved=False
    )
    assert manifest.generation_id == 7 and manifest.approved_custom_revision == 4
    assert (manifest.compact_layout is not None) is compact
    assert approved is None and relation is None
    assert not hasattr(manifest, "_published_api_request")
    query, selector = connection.fetchrow.await_args.args
    assert selector == generation and "NULL::bigint AS approved_rows" in query
    assert "registry_approved_record" not in query
    assert "pg_catalog" not in query and "custody" not in query and "oid" not in query
    token = _PINNED_READ.set((manifest, "example_control"))
    try:
        assert published.active_published_api_pin(connection, manifest) is None
    finally:
        _PINNED_READ.reset(token)


def test_supplied_manifest_flag_cannot_skip_request_audits():
    manifest = SimpleNamespace(_published_api_request=True)
    token = _PINNED_READ.set(None)
    try:
        assert published.active_published_api_pin(object(), manifest) is None
    finally:
        _PINNED_READ.reset(token)


def test_historical_physical_fields_do_not_change_named_layout(published_entry):
    original = _candidate(published_entry)
    expected = published.published_network_layout(original)
    artifact = original["validation_json"]["serving_readiness"]["address_artifact"]
    artifact["namespace"].update(oid=123, owner_oid=456)
    artifact["table"].update(oid=789, relfilenode=999)
    original["validation_json"]["writer_closure"] = {"relation_oids": {"network_slice_ref": 234}}
    original["validation_json"]["serving_readiness"]["edge_artifacts"][0].update(table_oid=345, seal_sha256="d" * 64)
    assert published.published_network_layout(original) == expected
    assert "oid" not in json.dumps(asdict(expected))


@pytest.mark.parametrize("damage", ["schema", "candidate", "source_generation", "revision"])
def test_changed_logical_generation_refuses(published_entry, damage):
    candidate = _candidate(published_entry)
    match damage:
        case "schema":
            candidate["schema_name"] = 'unsafe"schema'
        case "candidate":
            candidate["candidate_id"] = "other-candidate"
        case "source_generation":
            candidate["source_generations"] = {"custom_membership": "different"}
        case "revision":
            candidate["approved_custom_revision"] += 1
    published_entry["candidate_json"] = json.dumps(candidate)
    with pytest.raises(ValueError):
        published._decode_published_pin(published_entry, "example_control")


def test_flat_published_generation_is_supported(published_entry):
    candidate = _candidate(published_entry)
    candidate["validation_json"]["serving_readiness"]["component"] = "unified_address_serving"
    published_entry["candidate_json"] = json.dumps(candidate)
    manifest, _approved, _relation = published._decode_published_pin(published_entry, "example_control")
    assert manifest.compact_layout is None
    assert manifest.address_relation.table_name == "entity_address_unified"


def test_active_pin_keeps_request_generation(published_entry):
    manifest, _approved, _relation = published._decode_published_pin(published_entry, "example_control")
    token = _PINNED_READ.set((manifest, "example_control"))
    try:
        assert published.active_published_api_pin(object(), manifest, control_schema="example_control") is manifest
        with pytest.raises(published.NetworkServingReadUnavailable):
            published.active_published_api_pin(object(), SimpleNamespace(generation_id=8))
    finally:
        _PINNED_READ.reset(token)
    assert published.active_published_api_pin(object()) is None


@pytest.mark.parametrize("field", ["generation_id", "schema_name", "approved_custom_revision", "source_generations"])
def test_active_pin_refuses_changed_logical_metadata(published_entry, field):
    from dataclasses import replace

    manifest, _approved, _relation = published._decode_published_pin(published_entry, "example_control")
    value = {
        "generation_id": 8,
        "schema_name": "another_snapshot",
        "approved_custom_revision": 5,
        "source_generations": {"custom_membership": "d" * 64},
    }[field]
    token = _PINNED_READ.set((manifest, "example_control"))
    try:
        assert published.active_published_api_pin(object(), replace(manifest)) is manifest
        with pytest.raises(published.NetworkServingReadUnavailable):
            published.active_published_api_pin(object(), replace(manifest, **{field: value}))
        with pytest.raises(published.NetworkServingReadUnavailable):
            published.active_published_api_pin(object(), manifest, control_schema="other_control")
    finally:
        _PINNED_READ.reset(token)


def test_query_relations_include_the_named_compact_search_inputs(published_entry):
    from process.network_compact_layout import CompactAddressRelation, bind_compact_geo_search

    manifest, _approved, _relation = published._decode_published_pin(published_entry, "example_control")
    layout = manifest.compact_layout
    projection = CompactAddressRelation(manifest.schema_name, "network_geo_search")
    bind_compact_geo_search(layout, projection)
    assert manifest.candidate_relation_pins == (
        CompactAddressRelation(manifest.schema_name, "network_membership"),
        CompactAddressRelation(manifest.schema_name, "provider_location_binding"),
        layout.ref_map,
        layout.base,
        CompactAddressRelation(layout.artifacts[0].schema_name, "network_office_edge"),
        projection,
    )


def test_duplicate_named_query_relations_are_refused(published_entry):
    from dataclasses import replace

    from process.network_compact_layout import CompactAddressRelation

    manifest, _approved, _relation = published._decode_published_pin(published_entry, "example_control")
    duplicate = CompactAddressRelation(manifest.schema_name, "network_membership")
    layout = replace(manifest.compact_layout, base=duplicate)
    manifest = published._pinned_manifest(published_entry, layout=layout)
    with pytest.raises(published.NetworkServingReadUnavailable, match="must be unique"):
        _ = manifest.candidate_relation_pins


def test_geo_search_metadata_projects_historical_ids_to_names(published_entry):
    from process.network_geo_search import TABLE, geo_search_receipt

    candidate = _candidate(published_entry)
    candidate.update(
        {name: "00000000-0000-0000-0000-000000000002" for name in ("dataset_id", "schema_id", "producer_id")}
    )
    layout = published.published_network_layout(candidate)
    sources_by_kind = json.loads(json.dumps(asdict(layout)))
    for name in ("base", "ref_map"):
        sources_by_kind[name]["table_oid"] = 123
    sources_by_kind["artifacts"][0]["table_oid"] = 456
    serving = candidate["validation_json"]["serving_readiness"]
    serving["geo_search"] = {
        "contract": "network_geo_search.postgres.v1",
        "scope": {
            key: candidate[key] for key in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        "table_name": TABLE,
        "table_oid": 789,
        "row_count": 1,
        "sources": sources_by_kind,
        "indexes": [
            {"name": TABLE + suffix, "index_oid": number, "table_oid": 789, "definition": "example definition"}
            for suffix, number in (("_pkey", 987), ("_gn", 654))
        ],
        "sha256": "e" * 64,
    }
    descriptor = geo_search_receipt(candidate)
    assert descriptor["row_count"] == 1
    assert "oid" not in json.dumps(descriptor)
    assert descriptor["sources"]["base"] == asdict(layout.base)


def test_nullable_legacy_validation_metadata_keeps_flat_named_generation(published_entry):
    candidate = _candidate(published_entry)
    candidate["validation_json"] = None
    published_entry["candidate_json"] = json.dumps(candidate)
    manifest, approved, _relation = published._decode_published_pin(published_entry, "example_control")
    assert manifest.compact_layout is None and manifest.address_relation.schema_name == candidate["schema_name"]
    assert manifest.address_relation.table_name == "entity_address_unified" and approved.total_rows == 3
