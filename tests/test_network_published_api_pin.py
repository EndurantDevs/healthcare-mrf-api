# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published pins use logical generations and schema-qualified names."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_published_api_pin as published
from process.registry_network_catalog_read import _PINNED_READ
from tests.network_published_manifest_support import published_entry as published_entry_with_digest


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
    return published_entry_with_digest(
        {
            "candidate_json": json.dumps(candidate_by_field),
            "manifest_json": json.dumps(manifest_by_field),
            "approved_rows": 3,
        }
    )


def _candidate(entry):
    return json.loads(entry["candidate_json"])


def _store(entry, candidate):
    """Persist a changed candidate with the digest its publication would record."""
    entry.update(published_entry_with_digest({**entry, "candidate_json": json.dumps(candidate)}))


def _connection(entry):
    return SimpleNamespace(fetchrow=AsyncMock(return_value=entry), fetchval=AsyncMock())


def test_published_query_reads_pointer_and_metadata():
    query = published.published_api_pin_statement("example_control")
    assert '"example_control".network_serving_control' in query
    assert "manifest.eligible AND candidate.state='published'" in query
    assert "pg_catalog" not in query
    assert "custody" not in query
    assert "oid" not in query
    assert "query_to_xml(format(" in query
    for field in ("local_lineage_id", "local_generation", "origin_lineage_id", "origin_generation", "published_at"):
        assert field in query


def test_independent_layout_refuses_predecessor_edges(published_entry):
    candidate = _candidate(published_entry)
    edges = candidate["validation_json"]["serving_readiness"]["edge_artifacts"]
    edges[0] = {
        "candidate_id": "00000000-0000-0000-0000-000000000002",
        "schema_name": "network_candidate_00000000000000000000000000000002",
    }
    assert published.published_network_layout(candidate).artifacts[0].schema_name == edges[0]["schema_name"]
    candidate["source_generations"]["network_storage"] = "independent-v1"
    with pytest.raises(published.NetworkServingReadUnavailable, match="references predecessor serving data"):
        published.published_network_layout(candidate)


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
    connection = _connection(published_entry)
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
    _store(published_entry, candidate)
    del published_entry["approved_rows"]
    connection = _connection(published_entry)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("custom", [False, True], ids=["base only", "custom or carrier addresses"])
async def test_compact_custom_addresses_are_served_from_the_own_table(published_entry, custom):
    candidate = _candidate(published_entry)
    candidate["validation_json"]["compact_custom_offices"] = _custom_offices_receipt(candidate, int(custom))
    _store(published_entry, candidate)
    connection = _connection(published_entry)
    manifest = (await published.read_published_api_pin(connection, 7, "example_control", approved=False))[0]
    from process.network_compact_layout import CompactAddressRelation

    own = CompactAddressRelation(candidate["schema_name"], "entity_address_unified") if custom else None
    assert manifest.compact_layout.custom == own
    # Publication refused row filtering on the frozen heaps, so the read issues only the pin query.
    connection.fetchval.assert_not_awaited()


def _custom_offices_receipt(candidate, count):
    from process.network_address_projection import PinnedAddressSource
    from process.network_compact_custom_offices import CompactCustomOfficeReceipt
    from process.network_compact_layout import CompactAddressRelation
    from process.network_membership_copy import MembershipCopyTarget

    fields = ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    target = MembershipCopyTarget(*(candidate[field] for field in fields))
    own = target.schema_name
    return CompactCustomOfficeReceipt(
        target,
        "example_control",
        candidate["approved_custom_revision"],
        PinnedAddressSource("published_addresses", "entity_address_unified", "b" * 64),
        "c" * 64,
        None,
        "d" * 64,
        count,
        CompactAddressRelation(own, "entity_address_unified") if count else None,
        CompactAddressRelation(own, "provider_location_binding") if count else None,
    ).as_dict()


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


@pytest.mark.parametrize("compact", [False, True])
def test_published_reads_do_not_replay_new_writer_receipt_validation(published_entry, monkeypatch, compact):
    from process import network_membership_publication as publication

    candidate = _candidate(published_entry)
    candidate["validation_json"]["final_validation"] = {"complete": True}
    if not compact:
        candidate["validation_json"]["serving_readiness"]["component"] = "unified_address_serving"
    _store(published_entry, candidate)
    before_by_field = dict(published_entry)

    def upgraded_writer_scope(*_args):
        raise ValueError("Historical source receipt has a different encoding")

    monkeypatch.setattr(publication, "_scope_report", upgraded_writer_scope)
    manifest = published._decode_published_pin(published_entry, "example_control")[0]
    assert manifest.generation_id == 7
    assert manifest.manifest_sha256 == json.loads(before_by_field["manifest_json"])["manifest_sha256"]
    assert published_entry == before_by_field
    assert published._pinned_manifest(published_entry).manifest_sha256 == manifest.manifest_sha256


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("field", ["validation_revision", "dataset_id"])
def test_published_read_refuses_equally_incomplete_readiness_scopes(published_entry, compact, field):
    candidate = _candidate(published_entry)
    if not compact:
        candidate["validation_json"]["serving_readiness"]["component"] = "unified_address_serving"
    candidate["validation_json"]["final_validation"] = {"complete": True}
    _store(published_entry, candidate)
    candidate = _candidate(published_entry)
    for name in ("candidate_readiness", "serving_readiness"):
        del candidate["validation_json"][name]["scope"][field]
    published_entry["candidate_json"] = json.dumps(candidate)
    with pytest.raises(published.NetworkServingReadUnavailable):
        published._decode_published_pin(published_entry, "example_control")


@pytest.mark.parametrize("compact", [False, True])
def test_published_read_rejects_changed_logical_manifest_digest(published_entry, compact):
    candidate = _candidate(published_entry)
    if not compact:
        candidate["validation_json"]["serving_readiness"]["component"] = "unified_address_serving"
    candidate["validation_json"]["final_validation"] = {"complete": True}
    _store(published_entry, candidate)
    manifest = json.loads(published_entry["manifest_json"])
    manifest["manifest_sha256"] = "0" * 64
    published_entry["manifest_json"] = json.dumps(manifest)
    with pytest.raises(published.NetworkServingReadUnavailable, match="digest differs"):
        published._decode_published_pin(published_entry, "example_control")


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize(
    "damage",
    ["validation", "indexes", "serving", "serving_scope", "candidate_scope", "accounting", "digest"],
)
def test_published_read_still_requires_matching_logical_readiness(published_entry, compact, damage):
    candidate = _candidate(published_entry)
    if not compact:
        candidate["validation_json"]["serving_readiness"]["component"] = "unified_address_serving"
    _store(published_entry, candidate)
    candidate = _candidate(published_entry)
    report = candidate["validation_json"]
    match damage:
        case "validation":
            report["valid"] = False
        case "indexes":
            candidate["index_ready"] = False
        case "serving":
            report["serving_readiness"]["ready"] = False
        case "serving_scope":
            report["serving_readiness"]["scope"]["schema_name"] = "another_snapshot"
        case "candidate_scope":
            report["candidate_readiness"]["scope"]["approved_custom_revision"] += 1
        case "accounting":
            report["membership_rows"] += 1
        case "digest":
            manifest = json.loads(published_entry["manifest_json"])
            manifest["manifest_sha256"] = "invalid"
            published_entry["manifest_json"] = json.dumps(manifest)
    published_entry["candidate_json"] = json.dumps(candidate)
    with pytest.raises(published.NetworkServingReadUnavailable):
        published._decode_published_pin(published_entry, "example_control")


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
    _store(published_entry, candidate)
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
    from process.network_geo_search import TABLE, _digest, geo_search_receipt

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
    }
    # A historical writer stored the digest of the raw receipt, OIDs included.
    serving["geo_search"]["sha256"] = _digest(serving["geo_search"])
    descriptor = geo_search_receipt(candidate)
    assert descriptor["row_count"] == 1
    assert "oid" not in json.dumps(descriptor)
    assert descriptor["sources"]["base"] == asdict(layout.base)


def test_nullable_legacy_validation_metadata_cannot_prove_its_manifest_digest(published_entry):
    candidate = _candidate(published_entry)
    candidate["validation_json"] = None
    assert published.published_network_layout(candidate) is None
    published_entry["candidate_json"] = json.dumps(candidate)
    # Without its validation report the retained digest cannot be recomputed, so the generation does not serve.
    with pytest.raises(published.NetworkServingReadUnavailable, match="digest differs"):
        published._decode_published_pin(published_entry, "example_control")


@pytest.mark.asyncio
@pytest.mark.parametrize("rotated", [False, True])
async def test_historical_report_keeps_reading_its_rotated_base_heap(published_entry, rotated):
    from process.network_compact_layout import CompactAddressRelation

    candidate = _candidate(published_entry)
    candidate["validation_json"]["serving_readiness"]["address_artifact"]["table"]["oid"] = 0x2A
    entry_by_field = {**published_entry, "candidate_json": json.dumps(candidate)}
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=entry_by_field), fetchval=AsyncMock(return_value=rotated)
    )
    manifest, _approved, _relation = await published.read_published_api_pin(connection, 7, "example_control")
    retained = "entity_address_unified_v1_retained_2a"
    expected = retained if rotated else "entity_address_unified_v1"
    assert manifest.compact_layout.base == CompactAddressRelation("published_addresses", expected)
    (lookup,) = connection.fetchval.await_args_list
    assert lookup.args[1] == '"published_addresses".' + f'"{retained}"'


@pytest.mark.asyncio
@pytest.mark.parametrize("rotated", [False, True])
@pytest.mark.parametrize("path", ["current", "retained"])
async def test_non_api_reads_of_historical_reports_follow_the_rotated_base_heap(published_entry, rotated, path):
    from process import network_serving_read as serving
    from process.network_address_projection import PinnedAddressSource
    from process.network_compact_layout import CompactAddressRelation

    candidate = _candidate(published_entry)
    candidate["validation_json"]["serving_readiness"]["address_artifact"]["table"]["oid"] = 0x2A
    entry_by_field = {**published_entry, "candidate_json": json.dumps(candidate)}
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=entry_by_field),
        fetchval=AsyncMock(side_effect=[7, rotated] if path == "retained" else [rotated]),
    )
    if path == "current":
        manifest = await serving.resolve_network_serving_manifest(connection, control_schema="example_control")
    else:
        address_source = PinnedAddressSource(candidate["schema_name"], "entity_address_unified", "c" * 64)
        manifest = await serving.resolve_retained_network_address_source(
            connection, address_source, control_schema="example_control"
        )
    expected = "entity_address_unified_v1_retained_2a" if rotated else "entity_address_unified_v1"
    assert manifest.compact_layout.base == CompactAddressRelation("published_addresses", expected)


@pytest.mark.parametrize("city_search", [None, False, True])
def test_published_layout_binds_city_search_only_when_declared(published_entry, city_search):
    candidate = _candidate(published_entry)
    serving = candidate["validation_json"]["serving_readiness"]
    serving["geo_search"] = {"table_name": "network_geo_search"}
    if city_search is not None:
        serving["city_search"] = city_search
    layout = published.published_network_layout(candidate)
    assert layout.geo_search is not None
    assert layout.city_search == (layout.geo_search if city_search is True else None)


@pytest.mark.parametrize("network_search", [None, False, True])
def test_published_layout_binds_declared_network_search(published_entry, network_search):
    candidate = _candidate(published_entry)
    serving = candidate["validation_json"]["serving_readiness"]
    serving["geo_search"] = {"table_name": "network_geo_search"}
    if network_search is not None:
        serving["network_search"] = network_search
    layout = published.published_network_layout(candidate)
    assert layout.network_search == (layout.geo_search if network_search is True else None)
